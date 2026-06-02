# BrewFS 性能提升路线图

基于 BrewFS vs JuiceFS 对比分析，制定以下分阶段优化计划。

---

## 当前基线 (2026-05-21)

| 工作负载 | 吞吐量 | 延迟 |
|---------|--------|------|
| Sequential Write (4MB bs) | 152 MiB/s | 25.86 ms |
| Sequential Read (4MB bs) | 205 MiB/s | 19.16 ms |
| Random Write (4j, 4MB) | 159 MiB/s | 99.81 ms |
| Random Read (4j, 4MB) | 67 MiB/s | 237.63 ms |

---

## Phase 1: 低风险高收益 (1-2 周)

### 1.1 数据压缩 (预期 +30-50% write 吞吐)

**问题**: 每个 4MB block 全量传输到 S3，网络是主要瓶颈。

**方案**:
- 在 `BlockStore::write_fresh_vectored` 上传前添加 lz4/zstd 压缩
- 在 `BlockStore::read_block` 下载后解压
- Object 键增加后缀标识压缩算法 (e.g. `.lz4`)
- 配置项: `compression: none | lz4 | zstd`

**参考 JuiceFS**: `pkg/compress/` 模块，在 PUT/GET 前后透明压缩。

**预期收益**:
- 典型文本/代码数据压缩率 40-60%
- 4MB → ~2MB 传输，S3 PUT 延迟从 26ms → ~15ms
- Sequential Write: 152 → ~220 MiB/s

---

### 1.2 磁盘读缓存 (预期 random read +200-300%)

**问题**: 冷读必须从 S3 获取 (~19ms/block)，无本地持久化。

**方案**:
- 新增 `DiskCache` 层 (文件系统路径可配)
- 结构: `{cache_dir}/chunks/{slice_id}/{block_index}`
- 读路径: memory cache → disk cache → S3
- 写路径: S3 成功后异步写入 disk cache
- LRU 淘汰 + 容量限制 (e.g. 10-50 GB)

**参考 JuiceFS**: `pkg/chunk/disk_cache.go`，LRU/2-random eviction。

**预期收益**:
- 热数据命中 disk cache: 0.1ms vs 19ms
- Random Read: 67 → ~200 MiB/s (重复访问场景)

---

### 1.3 全局内存协调 (稳定性)

**问题**: 读写 buffer 独立管理，极端并发可能导致内存溢出。

**方案**:
- 创建统一 `MemoryBudget` 结构 (AtomicU64)
- Writer 和 Reader 共享 budget，当使用量 > 80% 时 reader 降低 readahead
- 超过 budget 时 writer 触发 force-flush

**参考 JuiceFS**: `usedBufferSize()` 全局协调。

---

## Phase 2: 中等收益 (2-4 周)

### 2.1 自适应 Auto-flush 间隔

**问题**: 固定 500ms auto_flush_max_age 在大文件顺序写时产生过多小 slice。

**方案**:
- 顺序写检测: 如果最近 N 次写入是连续的，延长 max_age 到 2-5s
- 小文件检测: 如果 slice 数据量 < 1MB 且 age > max_age，立即 freeze
- 动态目标: `freeze_min_bytes` 根据写入模式在 4MB-32MB 间浮动

**预期收益**:
- 大文件: 更少的 S3 PUT (更大 slice)，写延迟降低
- 小文件: 不受影响

---

### 2.2 带宽限流与优先级

**问题**: compaction 和 foreground flush 共享 UPLOAD_SEM=256 permits，无优先级区分。

**方案**:
- 分离前台/后台 semaphore: `FG_UPLOAD_SEM(200)` + `BG_UPLOAD_SEM(56)`
- 可选: 令牌桶限流 (`governor` crate)
- 配置: `upload_limit_mbps`, `download_limit_mbps`

**参考 JuiceFS**: `upLimit` / `downLimit` token bucket。

---

### 2.3 Reader: Flush-before-Read 一致性

**问题**: 不 flush pending writes 可能导致 read-after-write 不一致 (overlay_dirty 有延迟窗口)。

**方案**:
- 在 `FUSE read()` 中，如果 inode 有 pending writes 且 reader 无 overlay state：
  - 先 await writer.flush(ino)
  - 再执行 read
- 仅在必要时触发 (check write_gen vs last_flushed_gen)

**参考 JuiceFS**: `VFS.Read()` 中 `v.writer.Flush(ctx, ino)` 在读之前调用。

---

## Phase 3: 高级优化 (1-2 月)

### 3.1 Multipart Upload for Large Blocks

**问题**: 4MB block 使用单次 PUT，无法利用 S3 multipart 并行上传。

**方案**:
- block > 8MB 时使用 multipart upload (每 part 4MB)
- 或增大 block_size 到 8-16MB + multipart
- 根据网络延迟自动选择 part 大小

**注意**: 当前 4MB block 可能太小不适合 multipart，可考虑增大 block_size。

---

### 3.2 Write Coalescing (相邻 Slice 合并)

**问题**: 多个小写入创建多个小 slice，增加 meta 和 compaction 压力。

**方案**:
- 在 commit 前检测相邻 slice（同一 chunk, 连续 offset）
- 合并为单个 slice 描述再写入 meta
- 减少 meta 层 slice 条目数

---

### 3.3 Zero-copy 优化

**问题**: `write_fresh_vectored` 中 `full_block` 需要 `flat_map().collect()` 拷贝用于 cache。

**方案**:
- 使用 `bytes::Bytes` 的 chain/scatter 能力避免 memcpy
- Cache 存储 `Vec<Bytes>` 而非连续 `Vec<u8>`
- Read 时 scatter-gather 返回

---

### 3.4 io_uring 全链路

**问题**: 当前仅 FUSE 通道使用 io_uring，S3 网络仍是 epoll + tokio。

**方案**:
- 使用 `hyper` + `io_uring` backend (实验性)
- 或评估 `monoio` / `glommio` 作为 I/O runtime
- 目标: 减少系统调用开销

---

## Phase 4: 生态与可观测性

### 4.1 完善 .stats 指标

- 补充 Meta/S3/Cache 层计数器
- 每个 S3 操作记录延迟直方图
- 暴露 Prometheus endpoint

### 4.2 自动调优

- 根据 .stats 数据自动调整:
  - prefetch 窗口 (读命中率低 → 增大)
  - freeze_min_bytes (PUT 频率高 → 增大)
  - upload_concurrency (延迟高 → 减少)

---

## 优先级排序

| 优化项 | 难度 | 预期收益 | 优先级 |
|-------|------|---------|--------|
| 数据压缩 | ⭐⭐ | +30-50% write | 🔥 P0 |
| 磁盘读缓存 | ⭐⭐⭐ | +200% random read | 🔥 P0 |
| 全局内存协调 | ⭐⭐ | 稳定性 | P1 |
| 自适应 flush | ⭐⭐ | +10-20% seq write | P1 |
| 带宽限流 | ⭐ | 可控性 | P2 |
| Flush-before-Read | ⭐ | 正确性 | P1 |
| Multipart upload | ⭐⭐ | +10% large write | P2 |
| Write coalescing | ⭐⭐⭐ | meta 效率 | P2 |
| Zero-copy | ⭐⭐⭐ | -5% CPU | P3 |
| io_uring 全链路 | ⭐⭐⭐⭐ | -10% syscall | P3 |

---

## 目标

| 指标 | 当前 | Phase 1 目标 | Phase 2 目标 |
|------|------|-------------|-------------|
| Seq Write | 152 MiB/s | 220 MiB/s | 250 MiB/s |
| Seq Read | 205 MiB/s | 250 MiB/s | 300 MiB/s |
| Rand Write | 159 MiB/s | 200 MiB/s | 220 MiB/s |
| Rand Read | 67 MiB/s | 200 MiB/s | 250 MiB/s |
