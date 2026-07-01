# 小文件读写性能优化技术介绍

## 背景

分布式文件系统在大文件顺序读写场景中，吞吐通常受对象存储带宽、网络带宽、压缩和并发度影响；但在小文件场景中，瓶颈往往提前转移到元数据路径、缓存命中率、对象存储小 IO 延迟、FUSE/VFS 调用次数和写入提交语义上。一个 4 KiB 小文件的读写如果每次都触发 `lookup -> open -> stat -> read/write -> flush/close`，实际消耗的后端请求数可能远大于数据本身。

因此，小文件优化的核心不是单纯提高对象存储带宽，而是减少每个文件操作的固定成本，并把不可避免的后端 IO 批量化、异步化和可控地缓存起来。

本文面向 BrewFS 的下一阶段性能优化，结合 JuiceFS 的成熟路径，对小文件读写的缓存、元数据、写回、背压和验证方法进行技术介绍。

## 小文件性能模型

小文件读写的单次延迟可以粗略拆成：

```text
T = metadata_rtt + data_rtt + fuse_cost + serialization_cost + consistency_cost
```

其中：

- `metadata_rtt`：`lookup`、`stat`、`open`、`get_slices`、`readdir` 等元数据请求成本。
- `data_rtt`：对象存储 `GET range`、完整 block `GET`、`PUT` 或本地 cache 访问成本。
- `fuse_cost`：用户态文件系统的 syscall、上下文切换和请求分发成本。
- `serialization_cost`：元数据编码、slice 合并、数据拷贝、压缩和校验成本。
- `consistency_cost`：close-to-open、fsync、跨客户端失效、写后读一致性带来的额外刷新。

对小文件来说，`metadata_rtt` 和 `consistency_cost` 经常比数据读写本身更贵。一个 4 KiB 读如果击中本地数据缓存，但每次 `open` 都强制刷新元数据，整体性能仍然会被 Redis 或 etcd 往返延迟压住。

## 优化目标

小文件读写优化应优先达成以下目标：

1. 热路径尽量不访问后端元数据服务。
2. 小范围读优先命中本地 page/block cache。
3. 小写先进入本地 writeback，再由后台批量上传。
4. 前台读写请求优先于后台预取、后台上传和 cache 填充。
5. 缓存失效必须有明确边界，不能用性能换取不可解释的一致性问题。
6. 每一轮优化都必须通过小文件专项 benchmark 和 xfstests/perf 组合验证。

## 读路径优化

### 元数据优先

小文件读经常不是慢在 `read`，而是慢在 `lookup`、`open`、`stat` 和 `get_slices`。因此读路径优化的第一层应是元数据缓存：

- path 到 inode 的 lookup cache。
- inode attr cache。
- open-file scoped attr cache。
- inode/chunk 到 slice list 的缓存。
- directory children cache。
- readdir 后的批量 attr 预取。

JuiceFS 在 `pkg/meta/openfile.go` 中维护 open file cache，把已打开文件的 attr 和 chunk slices 缓存在 open file 生命周期内。BrewFS 当前已有 `MetaClient` 层的 inode、children、slice 和 path cache，但 `open`/fresh stat 路径仍可能频繁访问 Redis。小文件热点场景下，应优先补齐 open-file scoped cache，并为 close-to-open 语义提供可配置边界。

### 数据缓存分层

小文件读的数据缓存可以分为三层：

```text
read request
  -> dirty overlay
  -> memory hot cache
  -> disk block/page cache
  -> object store range/full read
```

各层职责不同：

- dirty overlay：保证写后读能看到未上传或刚提交的数据。
- memory hot cache：服务重复访问的热点 block。
- disk cache：扩大缓存容量，承接进程重启后的热数据。
- page cache：优化小范围随机读，避免为了 4 KiB 读完整 4 MiB block。
- full-block cache：服务顺序读和热点复用。

BrewFS 的 `src/chunk/store.rs` 已具备 full block cache、64 KiB page cache、range read、singleflight 和后台 full-block prefetch。JuiceFS 的 `pkg/chunk/cached_store.go` 也采用类似策略：小 range 读优先直接 range GET，成功后后台预取完整 block，并通过 singleflight 合并 full block 读取。

下一步重点不是再增加一层 cache，而是控制各类读取的优先级：前台用户读必须优先，后台 prefetch 和 cache fill 不能把对象存储连接、带宽或任务队列占满。

### 小范围读策略

小文件随机读应避免每次都完整拉取 block。推荐策略是：

1. 优先查 full block cache。
2. 未命中时查 page cache。
3. 小范围读使用 object range GET。
4. range GET 成功后，按压力情况后台拉取完整 block。
5. 如果已有 full block GET 正在进行，小范围读可以 piggyback 到 singleflight，避免重复请求。

这个策略适合 4 KiB 到 256 KiB 的小读。对于大于阈值的连续读，应直接走 full block read，并让 prefetch 更积极。

### 前台读优先

小文件读的 tail latency 对用户体验和 benchmark 影响很大。后台预取如果过于积极，会和前台读争抢对象存储连接池。推荐增加如下控制：

- 统计当前 foreground read inflight。
- foreground inflight 超过阈值时暂停或降低 global prefetch。
- range read 的后台 full-block prefetch 只在低压力下触发。
- 预取队列按 inode/chunk 合并，避免相邻 range 重复提交。
- 对 page cache miss 和 full block miss 分别计数，作为调参依据。

## 写路径优化

### 小写问题

小文件写入的典型瓶颈包括：

- 每次小写都生成独立 slice，导致 slice metadata 膨胀。
- 小对象 PUT 延迟高，吞吐无法堆满。
- flush/close/fsync 频繁触发上传和提交。
- 后台上传积压时，如果没有背压，会导致内存上涨和 tail latency 抖动。
- 如果先提交元数据再上传对象，读路径需要处理未完成上传的可见性。

### Writeback staging

小文件写入优先使用 writeback：

```text
write
  -> dirty buffer
  -> local staging
  -> metadata commit
  -> background upload
  -> upload completion / cache promoted
```

其收益是把用户写延迟从远端对象存储 PUT 延迟中解耦出来。JuiceFS 在 writeback 模式下会先把 block stage 到本地 cache，再由后台 uploader 上传。BrewFS 当前也已经具备 writeback staging、pending upload 统计、dirty overlay 和背压控制，近期 seqwrite 性能已经明显提升。

对小文件场景，下一步重点是小写合并和元数据提交批处理，而不是单纯提高上传并发。

### Dirty overlay

writeback 会引入一个核心问题：用户写返回后，数据可能尚未上传到对象存储。读路径必须能够看到最新写入。解决方式是 dirty overlay：

- 同一文件句柄内，read 优先读 dirty buffer。
- 已提交但未上传完成的数据，保留 recently committed overlay。
- overlay 有明确生命周期，上传完成或失效后释放。
- fsync 必须等待必要的数据上传和元数据提交完成，或返回错误。

这能保证小文件写后立即读不会被对象存储延迟影响。

### 小写合并

小写合并可以在两个层面做：

- VFS writer 层：把相邻 offset 的小写合并为更大的 slice。
- upload 层：把多个小 block 合并为 vectored upload 或更少的 staging 操作。

合并策略要受内存、延迟和 fsync 语义约束。推荐做法是：

- 对同一 inode/chunk 的小写设置短暂 coalescing window。
- 连续写满一定阈值后立即 flush。
- fsync、close、truncate、rename 等强语义操作必须打断合并窗口。
- 合并后的 slice metadata 必须仍保持 copy-on-write 可恢复性。

### 背压控制

小文件写入在高并发下容易产生大量 pending upload。背压应分层处理：

- soft pressure：短暂 yield，让后台上传追上。
- hard pressure：等待 pending upload 降到阈值。
- critical pressure：拒绝继续扩大 dirty buffer，保护进程内存。

背压不能只看 pending byte，也要看 pending object 数量。大量 4 KiB 小对象的请求调度成本可能比总字节数更重要。

## 元数据缓存优化

### 热点操作

小文件 workload 常见热点操作是：

- `lookup(parent, name)`
- `getattr(inode)`
- `open(inode)`
- `get_slices(inode, chunk)`
- `readdir(parent)`
- `create`、`unlink`、`rename`

其中读多写少的场景非常适合缓存；写密集场景则需要更严格的失效。

### Open-file cache

open-file cache 是小文件优化的高价值点。它可以缓存：

- inode attr。
- file size。
- chunk slice list。
- 最近访问时间和版本。
- open 引用计数。

推荐设计：

```text
open(inode)
  -> check open_file_cache
  -> hit and fresh: reuse attr/slices
  -> miss or stale: backend stat/get_slices
  -> insert cache entry

close(inode)
  -> decrease refcount
  -> keep short TTL hot cache

write/truncate/setattr/unlink/rename
  -> invalidate affected attr/slices/path entries
```

这个缓存可以显著降低反复打开小文件时的 Redis 压力。为了控制一致性风险，可以先在 perf profile 下打开，或使用很短 TTL，并在跨客户端强一致场景中关闭。

### Redis client-side cache

JuiceFS 的 Redis client-side cache 会缓存 inode 和 entry，并通过 Redis push notification 做失效。这个机制对小文件 stat/lookup 有明显帮助，但实现复杂度高于普通 TTL cache。

BrewFS 可以分两步推进：

1. 先补本进程内 open-file cache 和 cache hit/miss metrics。
2. 再评估是否实现 Redis CSC，包括订阅失效、entry term、inode invalidation 和跨客户端一致性测试。

## BrewFS 与 JuiceFS 对照

| 方向 | JuiceFS 成熟做法 | BrewFS 当前状态 | 下一步建议 |
| --- | --- | --- | --- |
| open-file cache | open file 生命周期缓存 attr/slices | 有通用 inode/slice cache，open scoped cache 不明显 | 增加 open-file cache |
| Redis 元数据缓存 | Redis CSC + invalidation | 主要是本地缓存和 TTL/局部失效 | 先加观测，再设计 CSC |
| 小范围读 | range GET + full-block 后台预取 | 已有 page cache/range/full-block prefetch | 增加前台读优先 |
| full block 合并 | singleflight | 已有 singleflight | 增加命中率和 inflight 指标 |
| 写回 | local stage + background upload | 已有 writeback/backpressure/dirty overlay | 针对小写做合并和提交批处理 |
| 背压 | 上传窗口和后台队列控制 | 已有 soft/hard pressure | 同时关注 object 数量和 byte 数 |
| 可观测性 | cache/upload/read metrics 丰富 | 部分路径仍缺细粒度指标 | 先补 hit/miss/strategy metrics |

## BrewFS 推荐优化路线

### 第一阶段：补齐观测

目标是知道小文件 workload 慢在哪里，而不是凭感觉调参数。

建议新增指标：

- `meta.stat.cache_hit/cache_miss`
- `meta.lookup.cache_hit/cache_miss`
- `meta.open.fresh_stat`
- `meta.get_slices.cache_hit/cache_miss`
- `chunk.read.full_cache_hit`
- `chunk.read.page_cache_hit`
- `chunk.read.range_get`
- `chunk.read.full_get`
- `chunk.prefetch.submitted/skipped/throttled`
- `writeback.pending_objects`
- `writeback.pending_bytes`

验证命令：

```bash
bash docker/compose-xfstests/run_redis_perf.sh --writeback-throughput-profile --tools "metaperf"
```

### 第二阶段：open-file 元数据缓存

实现 per-inode open-file cache，优先覆盖小文件反复 open/stat/read 的场景。

验收目标：

- `metaperf stat/open` 提升至少 30%。
- `get_slices` miss 明显下降。
- xfstests 中 rename、unlink、truncate、setattr 相关用例不回退。

### 第三阶段：前台读优先

限制后台 prefetch 对前台读的干扰。

策略：

- foreground read inflight 高时暂停后台 prefetch。
- range read 后的 full-block prefetch 使用独立低优先级 semaphore。
- global prefetch 队列做相邻 range 合并。
- 对 randread 和 seqread 分别观察收益。

验证命令：

```bash
bash docker/compose-xfstests/run_redis_perf.sh --writeback-throughput-profile --tools "fio-randread fio-seqread"
```

### 第四阶段：小写合并

针对小文件写入增加短窗口 coalescing，减少 slice 数和对象 PUT 次数。

验收目标：

- 小文件 create/write/close workload 吞吐提升。
- `fio-seqwrite` 不明显回退。
- pending object 数下降。
- fsync 语义保持正确。

验证命令：

```bash
bash docker/compose-xfstests/run_redis_perf.sh --writeback-throughput-profile --tools "fio-seqwrite fio-bigwrite"
```

## 正确性边界

小文件优化必须守住以下边界：

- `fsync` 返回成功后，数据和元数据必须满足持久性要求。
- `close` 不能吞掉异步上传或提交错误。
- 写后读必须优先看到 dirty/recently committed 数据。
- `rename`、`unlink`、`truncate` 必须失效相关 path、attr 和 slice cache。
- 跨客户端一致性必须有明确模式：强一致、短 TTL、或 perf profile 弱化。
- cache key 应包含足够的版本信息，避免旧 slice 或旧 attr 被复用。

## Benchmark 建议

小文件优化需要专项 benchmark，不能只看大文件顺序吞吐。

推荐组合：

```bash
bash docker/compose-xfstests/run_redis_perf.sh --writeback-throughput-profile --tools "metaperf"
bash docker/compose-xfstests/run_redis_perf.sh --writeback-throughput-profile --tools "fio-randread fio-seqread"
bash docker/compose-xfstests/run_redis_perf.sh --writeback-throughput-profile --tools "fio-seqwrite fio-bigwrite"
```

额外建议增加小文件专项 workload：

- 创建 10 万个 4 KiB 文件。
- 顺序读取 10 万个 4 KiB 文件。
- 随机读取 10 万个 4 KiB 文件。
- 覆盖写 10 万个 4 KiB 文件。
- 并发 `stat/open/read/close`。
- 并发 `create/write/close/unlink`。

每轮优化至少记录：

- ops/s。
- p50/p95/p99 latency。
- Redis QPS。
- object store GET/PUT QPS。
- cache hit/miss。
- pending upload bytes/objects。
- CPU 使用率和内存峰值。

## 总结

小文件读写性能优化的主线是：先减少元数据 RTT，再提高本地缓存命中率，随后控制后台任务对前台 IO 的干扰，最后再处理小写合并和提交批处理。

对 BrewFS 来说，下一阶段最值得优先尝试的是：

1. 增加元数据和读缓存命中率指标。
2. 实现 open-file scoped 元数据缓存。
3. 给读路径增加前台优先级，限制后台 prefetch。
4. 针对小写加入 coalescing 和 pending object 背压。

这条路线与 JuiceFS 的成熟设计方向一致，同时能保持 BrewFS 当前 writeback 架构已经取得的性能收益。
