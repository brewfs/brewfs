# BrewFS 本地缓存读优化计划

日期: 2026-05-23  
目标: 聚焦 8-42x 读差距，将 `randread 29 -> 200 MiB/s`, `seqread 134 -> 500 MiB/s`, cache 命中率 `~20% -> 80%+`。

## 背景

JuiceFS 的读路径在 `rSlice.ReadAt()` 中按如下顺序命中缓存:

1. memory pending pages
2. disk cache / KeyIndex
3. `loadRange()` 小范围读
4. `group.Execute()` full-block singleflight
5. full block 写回本地缓存

BrewFS 已有 `ChunksCache`, `ReadPageCache`, `read_flight`, `page_flight` 和 VFS 自适应预读，但存在几个关键断点:

- 小范围页缓存不一定升到 block 级，导致跨页随机读反复 range GET。
- 写路径曾经后台插入 hot cache，写返回后读不一定命中。
- 压缩默认启用时，小范围读走 full-block 解压，但解压结果没有铺 page_cache。
- 磁盘 cache opportunistic insert 缺少 per-key 去重，可能重复写同一 key。
- slice reader 失败缺少 JuiceFS 风格指数退避重试。
- tail prefetch 和磁盘健康状态机缺失。

## Phase / Task 总览

| Phase | Task | 问题 | 目标状态 |
| ----- | ---- | ---- | ---- |
| P0-1 | 小范围读后组装全 block 进 block_cache | 页缓存的页永远升不到 block 级 | 已完成: 后台 full-block prefetch + 页齐备后同步组装 |
| P0-2 | 写路径同步 hot cache 插入 | 空缓存启动惩罚 | 已完成: upload 成功后 hot cache 返回前可见 |
| P0-3 | 压缩 block 解压后填充 page_cache | 压缩启用时范围读退化 | 已完成: full-block 解压后按 64KB 铺 page_cache |
| P1-4 | 修复 insert_opportunistic 竞争 | 并发写磁盘缓存双重插入 | 待做: per-key in-flight/CAS guard |
| P1-5 | 调优自适应晋升阈值 | 过于保守的晋升策略 | 待做: 降低 burst 阈值并加入 hit-rate 反馈 |
| P2-6 | Slice 读失败指数退避重试 | 瞬时 S3 错误 -> 永久读失败 | 待做: retry + invalidate chunk cache + backoff |
| P2-7 | 尾部预取（最后 32KB） | 对标 JuiceFS tail prefetch | 待做: 文件尾读时预取 EOF 前 32KB |
| P3-8 | 磁盘缓存 I/O 健康检测 | 无降级模式 | 待做: Normal/Unstable/Down 状态机 |

## Phase P0: 立即修复缓存闭环

### Task P0-1: 小范围读后组装 full block

**问题:** `ReadPageCache` 能缓存 64KB page，但如果一个 block 的所有 page 都已经被 range 读填满，`ChunksCache` 仍可能没有 full-block entry。

**当前实现:**

- 每次 page range miss 后调用 `try_promote_page_cache_to_block_cache()`。
- 如果该 block 的所有 page 都已在 `ReadPageCache` 中，则组装 `Vec<u8>` 并调用 `block_cache.insert_opportunistic(key, full_block)`。
- 如果 page 未齐备，则保留后台 full-block prefetch 主动补全。
- 二者互补: 页齐备路径复用已下载数据，后台 prefetch 覆盖随机小读未读完整 block 的情况。

**验证:**

- 单测: 逐页 range 读同一 block，最后一页读完后 `block_cache.get(key)` 命中。
- 回归: `cargo test -q chunk::store::tests --lib`。

### Task P0-2: 写路径同步 hot cache 插入

**问题:** 写路径如果只后台插入 hot cache，`write()` 返回后立即 `read()` 仍可能穿透到对象存储。

**当前实现:**

- `write_fresh_range` / `write_fresh_vectored` 在 PUT 成功后 `await block_cache.insert_hot()`。
- 磁盘持久化仍使用 best-effort background task，避免 foreground upload 被本地 I/O 阻塞。

**验证:**

- 单测: `test_write_fresh_range_populates_hot_cache_before_return`。

### Task P0-3: 压缩 block 解压后填充 page_cache

**问题:** 默认 LZ4 压缩时，range GET 不能直接读取压缩对象中的逻辑范围；当前必须下载 full block 并解压。如果只填 block_cache，不填 page_cache，后续小范围路径难以复用 page 级缓存。

**当前实现:**

- full-block read 解压后调用 `populate_page_cache_from_block()`。
- 以 `BlockKey + page_index` 写入 64KB pages。

**验证:**

- 单测: `test_compressed_small_read_uses_full_object` 检查 page `(7, 0, 32)` 命中。

## Phase P1: 晋升和并发写入

### Task P1-4: 修复 `insert_opportunistic` 竞争

**问题:** 多个 reader/prefetcher 同时对同一 key 调用 `insert_opportunistic` 时，可能重复写磁盘 cache，并导致 `bytes_used`/cold marker 短期偏差。

**实现方向:**

- 在 `ChunksCache` 增加 `disk_insert_inflight: DashSet<String>`。
- `insert_opportunistic` 在磁盘写之前 CAS 插入 key；已存在则只做 hot insert 并返回。
- disk store 完成或失败后 remove in-flight key。
- 写前二次检查 cold/disk 命中，避免排队期间重复写。

**验证:**

- 单测: 并发 N 次 `insert_opportunistic(same_key)`，最终 disk bytes 不超过一个 framed block。

### Task P1-5: 调优自适应晋升阈值

**问题:** 当前默认 `base_promotion_threshold=10.0` 对 randread/短 burst 偏保守，page/block 被访问多次后仍可能停留在 disk/cold 层。

**实现方向:**

- 将默认 base threshold 调整到 4-6。
- 提高 short window 权重，例如 `0.8/0.2`。
- 在 cache hit rate < 0.3 时先降低阈值，提升 warm-up 速度；在 hot utilization > 0.9 时再保守。

**验证:**

- 单测覆盖 policy threshold。
- perf 验证 `fio-randread` hit rate 和带宽。

## Phase P2: 读失败恢复和尾部预取

### Task P2-6: Slice 读失败指数退避重试

**问题:** `FileReader::background_fetch` 一次 `DataFetcher` 失败会把 slice 置 Invalid。瞬时 S3/网络错误会变成用户可见永久失败。

**实现方向:**

- 在 `background_fetch` 内包一层 retry loop。
- 分类 retryable error: timeout, connection reset, transient object store error。
- 指数退避 + jitter，最大重试次数沿用 meta retry 经验值或读配置。
- 每次失败前 invalidate chunk/slice cache，下一次重新读 meta + data。

**验证:**

- Mock BlockStore 前两次失败第三次成功，`FileReader.read()` 成功且 attempts=3。

### Task P2-7: Tail prefetch 32KB

**问题:** JuiceFS 对文件尾部读有 tail readahead，BrewFS 目前只按 session 预测后续范围，EOF 附近可能无法提前覆盖最后 32KB。

**实现方向:**

- `FileReader.read_at()` 如果读范围接近 EOF，额外 prepare `[file_size-32KB, file_size)`。
- 避免重复预取已覆盖 slice。
- 受 MemoryBudget Critical 压力限制。

**验证:**

- 单测: 读取 EOF 前小段，检查 reader slices 包含 EOF 前 32KB 范围。

## Phase P3: 磁盘缓存健康

### Task P3-8: Disk cache I/O 健康状态机

**问题:** JuiceFS cache store 有 Normal/Unstable/Down 降级恢复。BrewFS 当前磁盘 cache I/O 错误直接影响本次 cache 操作，缺少长期健康判断和降级。

**实现方向:**

- 在 `DiskStorage` 增加 `CacheHealth`:
  - Normal: 正常读写。
  - Unstable: 连续 3 次 I/O error 后进入，限制并发并开始探测。
  - Down: 持续失败后跳过 disk cache，仅保留 hot/page cache。
- 成功探测 60 次或持续一段无错误后恢复 Normal。
- 所有 disk cache 错误不得影响 foreground read/write 正确性。

**验证:**

- 单测: mock/临时目录制造 I/O error，状态 Normal -> Unstable -> Down。
- 单测: Down 状态下 `block_cache.get()` miss 不返回错误，foreground read 可继续走 object store。

## 2026-05-23 fio 反馈补充诊断

用户复测数据使用 `bs=4m`，主要命中全 block 读路径，不会明显触发 P0-1 的小范围 page cache 晋升收益。新的瓶颈定位到 `FileReader::prepare_ahead_slices()`:

- 旧行为: 一次 readahead 会把多个 4MiB block 合并成一个大 `SliceState`，例如 `(4096, 12288)`。
- 影响: 下一次同步 4MiB 顺序读落在该 slice 内时，会等待整个 8/16/32MiB 预取 slice 完成，而不是只等当前 4MiB block，造成 read P99 偏高。
- 修复: readahead slice 按 block 边界拆分，保证 foreground 4MiB read 不被后续 block 拖慢。

## Phase 4: 架构重构 — 统一缓存层 (P4 — 待规划)

**状态: 未实施。Phase 1-3 的配置级优化和局部修复无法解决该问题。**

### 问题

经过 Phase 1-3 的完整实施和多轮 benchmark 验证（2026-05-24），读性能仍然落后 JuiceFS 4-25x。根因已通过实验确认为 **VFS SliceState.page 双层缓存架构冲突**:

```
FUSE → FileReader → SliceState.page ← 优先服务 (30s TTL)
                         ↓ (仅首读时使用)
                   ObjectBlockStore → ChunksCache (4GB hot + 20GB disk)
```

在 60s benchmark 窗口内，SliceState 永不过期 → **ChunksCache 仅在首次访问每个 4MB 范围时被查询 1 次，之后形同虚设**。

详细分析见: `doc/juicefs/brewfs-vs-juicefs-full-comparison.md` § 核心架构瓶颈。

### 目标

去除 VFS 层 `SliceState.page` 数据缓存。让 SliceState 退化为纯元数据记录，所有数据读取统一经 `ObjectBlockStore.read_range → ChunksCache.get`。对标 JuiceFS: `rSlice.ReadAt → bcache.load → cache hit 直接返回`。

### 预期收益

| 指标 | Phase 3 后 | Phase 4 后 | 目标 (JuiceFS) |
|------|----------|----------|-----------------|
| bigread | 226 MiB/s | 500+ MiB/s | 983 |
| seqread | 230 MiB/s | 600+ MiB/s | 1119 |
| randread | 54 MiB/s | 300+ MiB/s | 1343 |

## 验证命令

开发期最小验证:

```bash
cargo test -q chunk::store::tests --lib
cargo test -q vfs::io::reader::tests --lib
cargo test -q chunk::singleflight::tests --lib
cargo check -q
```

性能验证:

```bash
bash docker/compose-xfstests/run_redis_perf.sh --s3 --tools "fio-randread fio-seqread"
```

目标:

- `randread`: 29 MiB/s -> 200 MiB/s
- `seqread`: 134 MiB/s -> 500 MiB/s
- cache hit rate: ~20% -> 80%+
