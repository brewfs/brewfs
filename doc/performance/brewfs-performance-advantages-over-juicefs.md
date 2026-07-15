# BrewFS 相对 JuiceFS 的性能优势归因

## 目的与结论

本文分析 BrewFS 在当前 Redis + RustFS、buffered FUSE 性能测试中超过
JuiceFS 的原因。核心结论是：**已观测到的大部分优势来自数据路径、缓存
策略、元数据往返次数和测试 profile，不能简单归因于 Rust 语言。** Rust 的
无 GC、所有权模型和低成本并发原语提供了更好的延迟可预测性和实现基础，但只有
当热路径避免重复 I/O、锁和拷贝时，这些特性才会转化为可观测的吞吐。

当前最可信的设计优势是：

1. BrewFS 对读缓存、dirty overlay 和对象上传进行了更积极的流水化。
2. Redis metadata 的 create 和 lookup-with-attr 使用单个 Lua 原子操作，减少客户端
   往返和 TOCTOU 窗口。
3. 用 `Bytes`、`Arc`、`DashMap` 和有界 async task 建立的数据所有权模型，使
   同一块缓存数据可以在并发请求间安全共享。
4. 针对大读和大写的 opt-in profile 更激进，但这是工作负载调优成果，不是
   默认实现的无条件优势。

## 测试边界

本文使用以下产物：

- BrewFS 完整基线：`perf-run-1783996543-179`
- BrewFS bigwrite profile：`perf-run-1783997172-5429`
- BrewFS 最终 metadata profile：`perf-run-1784087526-15058`
- JuiceFS 完整基线：`juicefs-perf-run-1783997392-15764`
- JuiceFS cached-read profile：`juicefs-perf-run-1784000369-17440`
- JuiceFS metadata profile：`juicefs-perf-run-1784000697-31335`

两边使用 Redis metadata、RustFS S3 backend、4 MiB fio block size 和 buffered FUSE。BrewFS
读基线还显式启用了 `BREWFS_FUSE_READ_DIRECT_IO=1`；该选项只对只读 handle 返回
`FOPEN_DIRECT_IO`，因此 bigread/seqread/randread 与默认 JuiceFS 不是完全同构的
mount profile。

| 工作负载 | BrewFS | JuiceFS 默认 | BrewFS / JuiceFS |
| --- | ---: | ---: | ---: |
| bigread | 2.58 GiB/s | 1020.94 MiB/s | 2.59x |
| seqread | 1.84 GiB/s | 1.05 GiB/s | 1.75x |
| randread | 2.79 GiB/s | 1.07 GiB/s | 2.61x |
| randrw read | 402.89 MiB/s | 29.63 MiB/s | 13.60x |
| randrw write | 179.84 MiB/s | 13.28 MiB/s | 13.54x |
| create | 1033.8 ops/s | 355.0 ops/s | 2.91x |
| readdir | 31258.3 ops/s | 19023.7 ops/s | 1.64x |

JuiceFS 启用 `cache-large-write`、8 GiB cache、1 GiB readahead 和 prefetch 后，bigread
提升到 1.66 GiB/s，randread 提升到 1.20 GiB/s。这证明部分 BrewFS 读优势来自
热缓存是否已经被填充，而不是语言实现速度。

## 优势归因矩阵

| 观测优势 | 主要原因 | Rust 相关性 | 信心 |
| --- | --- | --- | --- |
| bigread / seqread | BrewFS 热 block/page cache、只读 `FOPEN_DIRECT_IO`、更少 FUSE buffered 拆分 | 低 | 高 |
| randread | 内存 page/block cache、singleflight、热数据集，避免重复 S3 GET | 中 | 高 |
| randrw | dirty overlay 允许读取未落远端数据，写入上传和前台 I/O 流水化 | 中 | 中 |
| create | Redis `CREATE_ENTRY_LUA` 在服务端原子完成 inode、dentry 和 parent 更新 | 低 | 高 |
| readdir | 批量获取目录项、children/inode cache、本地排序与复用 | 中 | 中 |
| bigwrite profile | commit-before-upload、SSD staging、大 dirty budget 和上传并发 | 低 | 高 |
| 延迟稳定性 | 无 GC pause，重要 buffer 生命周期由所有权和 `Arc` 管理 | 高 | 中 |

## 数据路径设计优势

### 分层块缓存与 singleflight

BrewFS 以 64 MiB chunk、4 MiB block 和 64 KiB page 组织数据。`SingleFlight` 使用
`DashMap` 管理 per-key in-flight 请求，多个并发 miss 可以共用一次对象读。这一
设计对热随机读有直接收益，但 JuiceFS 也有对应的 singleflight 和磁盘 block
cache，所以 BrewFS 的当前领先更多说明本轮 profile 中 BrewFS 缓存更热，不能说明
该架构为 BrewFS 独有。

### Dirty overlay 与读己之写

BrewFS writer 保留正在写入的 slice/page，reader 在必要时将 committed data 与 dirty
overlay 合并。对 randrw 工作负载，这能避免为每次读强制等待所有相关写入上传
至 S3。JuiceFS 的 read path 会在读前协调 writer flush，在本轮本地 RustFS 配置中可能
放大了读写互相阻塞。

该判断仍需通过 dirty-overlay hit、flush wait 和 S3 GET/PUT 时序证明，不应只用
13.6x 吞吐比作为结论。

### 工作负载专用 FUSE profile

`BREWFS_FUSE_READ_DIRECT_IO=1` 使只读 handle 返回 `FOPEN_DIRECT_IO`。这避免了 Linux
page cache 和 FUSE buffered read 层的部分切分与拷贝，但也会禁用这些 handle 的
`mmap`。因此它是可选的大读调优，不是通用默认值。

## Redis 元数据设计优势

### 服务端原子融合

Redis store 使用 Lua 把多 key 操作收敛到单个往返：

- `CREATE_ENTRY_LUA` 原子创建 inode、增加 dentry 并更新 parent metadata。
- `lookup_with_attr` 在同一次 script 中读取 directory entry 和 child node。
- rename/link/unlink/write-slice 也使用服务端原子路径避免客户端 RMW。

这是 BrewFS create 2.91x 优势最有力的解释。收益来自协议和往返次数，与
Rust/Go 的单机执行速度关系很小。

### 本地 inode/children 状态

`InodeCache` 用 Moka 管理 TTL/容量，用 `DashMap` 保留可并发更新的 inode、children
和 slice 状态。本 client mutation 后直接更新或失效缓存，可以让紧随的 lookup/stat
避免 Redis。它有助于 create 后 lookup 和目录重复遍历，但 Redis backend 目前没有
JuiceFS client tracking 级别的跨客户端失效，因此 TTL 不能无限放大。

## Rust 带来的实际优势

### 无 GC 的延迟可预测性

Rust 不需要 tracing garbage collector。大量 64 KiB page、4 MiB block、slice descriptor 和
FUSE request 并发时，buffer 的释放时机由所有权和引用计数决定。这对 p99 的
潜在收益比对平均吞吐的收益更大。

但“无 GC”不等于“无内存开销”。BrewFS 仍有 `Arc` 原子计数、`FileAttr`
clone、`Vec` 分配、Moka async lookup 和整块重组。这些热路径开销可以抵消语言层面的
优势。

### 可控的共享与拷贝

`Bytes`/`Arc` 允许多个读请求共享同一块不可变数据，所有权系统使得零拷贝引用
在编译期受到约束。对 singleflight 返回值、页缓存和 vectored S3 upload，这是
实现激进共享路径的一个工程优势。

当前仍不是端到端零拷贝：缓存、reader、FUSE reply 之间还有数据拷贝，所以不应
把当前读吞吐直接归因于 Rust ownership。

### 并发状态的编译期约束

Rust 的 `Send`/`Sync` 和借用规则降低了在 reader、writer、cache 和 background upload
之间共享状态的正确性成本。这并不会自动产生更高吞吐，但使 BrewFS 能在不依赖
GC 和大粒度全局锁的情况下添加 dirty overlay、singleflight 和并发上传。

## 不能归因于 Rust 的数字

### 热 buffered read 延迟

BrewFS bigread/randread 报告中的微秒级 p99 说明读请求命中了内核或用户态
缓存，不是 Rust 在微秒级完成了 S3 GET。这些数字只能用于当前热缓存工作
集，不能推广到冷读、超大工作集或重启后读。

### Stat 的 680k ops/s

两边 stat 都在约 684k ops/s，这主要是 FUSE/kernel attr cache 行为，并没有证明 Redis
metadata 实现可以承载这个 QPS。验证 backend stat 时必须把 attr/entry TTL 设为 0
或使用直接 metadata client benchmark。

### Bigwrite profile

BrewFS bigwrite 从 493.02 MiB/s 提升到 808.85 MiB/s 时使用了 commit-before-upload。
这改变了前台完成与远端持久化的时序；脚本会额外 drain 到 pending/dirty 归零，
但 fio 显示的 active bandwidth 仍不是 upload-before-commit 的等价延迟。这是实现和配置
优势，不是 Rust 语言优势。

## Open 性能差距专项分析

### 直接原因

xfstests `metaperf` 的 open case 反复执行：

```c
close(open(path, O_RDWR));
```

BrewFS `open_file_cache_eligible` 只允许 `read && !write && !append`，因此 `O_RDWR`
每次调用都进入 `stat_fresh`。在启用 1 秒 open cache 的实测中，`metaperf` 结束时：

- `brewfs_meta_open_fresh_stat_total = 183812`
- `brewfs_meta_open_file_cache_hit_total = 1`
- open = 4807 ops/s

这证明当前 open 差距不是 Moka 容量或 TTL 太小，而是 write-capable open 被有意
排除在缓存之外。

JuiceFS `OpenCheck` 会在 open-cache 窗口内复用 attr 并增加 refs；其 open-file entry
还管理 chunk slices，并在 mtime 变化时失效 chunk cache。所以 JuiceFS 在该 case 中的
12747 ops/s 主要来自更广的 open-cache 语义，不是 Go 的 `open` 执行更快。

### 本轮优化结果

本轮增加了默认关闭的 `allow_write_open_cache` 配置和
`--metadata-throughput-profile`，并对 open/close 本地热路径做了两项结构优化：

- OpenFileCache 不再维护从不用于读取的第二份 `DashMap`，attr 改用短临界区
  同步 `RwLock`，close attr refresh 与 ref decrement 合并为一次 Moka lookup。
- write-capable handle 不再于 open 时立即创建 per-file writer；只有第一次真实
  `write()` 才创建 writer。从未读写的 handle 在 close 时也不再访问对应
  reader/writer registry。

| 阶段 | open ops/s | 相对原始基线 |
| --- | ---: | ---: |
| BrewFS 原始基线 | 4868 | 1.00x |
| 允许 RDWR open cache | 5584 | 1.147x |
| 简化 OpenFileCache bookkeeping | 5618 | 1.154x |
| writer 惰性初始化 | 5707 | 1.172x |
| 最终代码（含正确性修复） | 5742 | 1.180x |
| JuiceFS metadata profile | 12747 | 2.619x |

最终 BrewFS artifact 为 `perf-run-1784087526-15058`。create/open/stat/readdir/rename
相对原始 BrewFS 基线分别为 +5.5%、+18.0%、+0.4%、+12.2% 和 +2.6%。相对上一轮
优化产物，五项波动均小于 1.2%，没有超过 5% 的回退；warning、timeout、slow request
和 slow operation 均为 0。

这些数据也否定了“只要让 RDWR 命中 attr cache 就能追平 JuiceFS”的强假设。
在 Redis miss 基本消除之后，单次 BrewFS open+close 仍约为 175 us，剩余差距主要在
FUSE dispatch、handle registry、inode lifecycle 以及 close 快路径，需要用 CPU profile 继续分解，
不应再通过扩大 TTL 猜测。

### CPU flamegraph 结论

对 open-only workload 挂载 `perf record -e cpu-clock -F 99 --call-graph fp`
采样 60 秒，共获得 6645 个样本且无丢失。产物位于
`perf-run-1784100638-12495/flame/open-cpu-flame.svg`。采样开销下 open 为
5247 ops/s，结果用于归因而非吞吐比较。

| self CPU 热点 | 占比 | 解释 |
| --- | ---: | --- |
| kernel `_raw_spin_unlock_irqrestore` | 24.62% | 主要来自 futex wake 和 FUSE reply 唤醒 |
| syscall | 7.06% | FUSE/io_uring/futex 系统调用边界 |
| `finish_task_switch` | 7.04% | 等待和唤醒造成的调度成本 |
| async-fuse dispatch | 0.75% | FUSE request 分发 |
| Tokio bounded mpsc send | 0.74% | runtime 与专用 io_uring 线程之间传递请求 |
| Moka CHT lookup | 0.72% | open attr cache lookup |
| DashMap iterator | 0.38% | 无锁 workload 中 release 仍扫描 lock-owner map |
| BrewFS handle allocate | 0.26% | handle registry 本身不是主瓶颈 |

调用栈显示当前 async-fuse io_uring backend 的每次 FUSE read/write 会经过 Tokio
channel、专用 ring thread、completion/oneshot 和 Tokio task 唤醒。CPU 主要消耗在这些
线程与内核 waiter 之间的切换，而不是 Redis metadata 或 BrewFS open 业务逻辑。

两个替代 backend 也做了 A/B 验证：

- async-fuse 原有 Tokio `spawn_blocking` backend 为 5558 ops/s，比当前 io_uring
  基线低约 3.2%，因此不能靠切换默认 feature 获益。
- `/dev/fuse` 在当前 kernel/container 下不会可靠地产生 epoll read/write readiness，
  基于 Tokio `AsyncFd` 的原型无法完成 mount preflight，因此已撤销，不能作为修复。

随后在相同代码和 `--read-throughput-profile` 下完成了严格配对的全套测试。Tokio
runtime 产物为 `perf-run-1784107313-20301`，io_uring runtime 产物为
`perf-run-1784108392-15309`；两轮 11 个工具均通过且 warning/timeout 为 0。

| 项目 | io_uring | Tokio | Tokio 相对变化 |
| --- | ---: | ---: | ---: |
| fio-bigread | 2.48 GiB/s | 2.49 GiB/s | +0.4% |
| fio-bigwrite | 516.65 MiB/s | 505.43 MiB/s | -2.2% |
| fio-seqread | 1.82 GiB/s | 1.74 GiB/s | -4.4% |
| fio-seqwrite | 200.39 MiB/s | 201.01 MiB/s | +0.3% |
| fio-randread | 2.84 GiB/s | 2.87 GiB/s | +1.1% |
| fio-randwrite | 198.27 MiB/s | 200.75 MiB/s | +1.3% |
| fio-randrw read | 402.68 MiB/s | 407.81 MiB/s | +1.3% |
| fio-randrw write | 179.21 MiB/s | 181.05 MiB/s | +1.0% |
| create | 1068.0 ops/s | 1039.1 ops/s | -2.7% |
| open | 5392.8 ops/s | 5239.6 ops/s | -2.8% |
| stat | 686764.8 ops/s | 685339.3 ops/s | -0.2% |
| readdir | 33983.6 ops/s | 33761.7 ops/s | -0.7% |
| rename | 996.2 ops/s | 973.5 ops/s | -2.3% |
| dirperf 1000（越低越好） | 1.437 s | 1.531 s | 慢 6.5% |

数据吞吐除 seqread 外均在常见运行波动范围内，而 metadata 和 dirperf 一致偏向
io_uring。关闭 io_uring 不能修复当前 open 瓶颈，默认 runtime 应继续保留 io_uring。

本轮保留的低风险修复是：延迟初始化未实际写入的 writer，未初始化 reader
不进入 close registry 清理，以及 lock-owner map 为空时跳过 release 全表扫描。
后续若要显著缩小与 JuiceFS 的 open 差距，应在 async-fuse 中合并请求提交/完成唤醒，
或让同一专用线程批量处理 FUSE read 与 reply；继续削减单个 `Arc`/handle 操作的收益上限很低。

修复阶段的同口径完整 metaperf 产物为 `perf-run-1784104201-24352`：create/open/stat/
readdir/rename 分别为 1063.6、5621.0、685829.1、34634.6 和 1005.3 ops/s。
相对 profile 前产物 `perf-run-1784087526-15058` 的变化分别为 -2.4%、-2.1%、
-0.3%、-1.2% 和 -0.4%，均处于 5% 波动预算内。另三次 open-only 独立结果为
5476、5535 和 5672 ops/s。修正多句柄 writer 释放边界后的最终代码又得到
5661 ops/s（`perf-run-1784105075-23134`）；当前改动没有可重复宣称的显著 open
提升，也没有性能回退证据。

### 为什么 BrewFS 默认不应直接放开

Redis backend 尚无跨客户端 invalidation。如果 client A 在 open cache 中保留旧 size/mtime，
client B 已经 truncate 或写入扩容，A 的新 `O_RDWR` open 若直接复用 attr，就可能违反
close-to-open freshness，并使 append/truncate 基于旧 size。

因此只有两条安全路径：

1. 先实现 Redis client tracking/invalidation，再让 RDWR open 命中缓存。
2. 增加显式的 single-client/performance profile，允许 non-append、non-truncate RDWR open
   复用短 TTL attr，默认保持关闭并清楚标记 stale-risk。

第二条可以用于对齐 `metaperf` 的 single-client benchmark，但不能宣称它改善了
分布式强一致 open 语义。

## 验证与可证伪实验

| 假设 | 必须执行的实验 | 接受条件 |
| --- | --- | --- |
| 读优势主要来自 cache/profile | 同时 remount、清用户缓存、drop page cache，跑大于两边缓存的 direct=1 工作集 | 报告 cold 与 warm 两组，不混表 |
| randrw 优势来自 dirty overlay | 记录 overlay hit、flush wait、S3 GET/PUT 和 pending bytes 时序 | 吞吐优势与 overlay/flush 指标相关 |
| create 优势来自单 Lua RTT | 比较 Redis commandstats 和 tcp RTT，禁用 client cache 后重跑 | 每 create business RTT 更少 |
| open 差距来自 RDWR cache eligibility | 在显式 single-client profile 下放开 RDWR cache，对比 fresh-stat/hit 计数 | hit 大幅增加且 open 提升 |
| Rust 无 GC 改善 tail | 对齐缓存、I/O 和并发后长时间记录 CPU、allocation、p99.9 | 只在排除 profile 差异后归因 |

任何性能优化都应同时满足：

- 目标 workload 的中位数至少连续三次改善。
- 非目标 fio/metadata 项目回退不超过 5%。
- 已配置的 xfstests、pjdfstest、LTP fs 和 stress-ng 不增加新失败。
- 改变 close-to-open、fsync 或远端持久化语义的 profile 必须显式标注。

## 优先级

1. 为 RDWR open cache 增加默认关闭的 single-client profile，先验证 open 差距的归因。
2. 实现 Redis client tracking 失效，再考虑安全扩大默认 open cache 适用范围。
3. 建立 cold/warm、direct/buffered 矩阵，把“缓存更热”与“数据路径更快”分开。
4. 用 flamegraph/allocation profile 定量验证 Rust 无 GC、`Bytes` 共享和 async task 开销，
   不再使用语言标签代替性能证据。
