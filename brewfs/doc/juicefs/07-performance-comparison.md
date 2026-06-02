# BrewFS vs JuiceFS 性能对比

> 环境: rustfs S3 + Redis, io_uring, 4M block
> JuiceFS 数据来自 `juicefs-perf-run-1779469615` (全 11/11 通过)
> BrewFS 数据来自 `perf-run-1779435359` (全 11/11 通过)

## FIO 吞吐量

| Test | rw | jobs | JuiceFS BW | BrewFS BW | Ratio | JuiceFS P99 | BrewFS P99 |
|------|----|------|-----------|------------|-------|------------|-------------|
| seqread | read | 1 | **1.1 GiB/s** | 134 MiB/s | 8.4x | **5ms** | 75ms |
| randread | randread | 4 | **1.2 GiB/s** | 29 MiB/s | 42x | **30ms** | 1367ms |
| bigread | read | 8 | **816 MiB/s** | 77 MiB/s | 10.6x | **154ms** | 1061ms |
| seqwrite | write | 1 | **195 MiB/s** | 131 MiB/s | 1.5x | **200ms** | 633ms |
| randwrite | randwrite | 4 | **184 MiB/s** | 55 MiB/s | 3.3x | **342ms** | 1267ms |
| bigwrite | write | 8 | 206 MiB/s | **205 MiB/s** | ~1x | **287ms** | 417ms |
| randrw | randrw | 4 | **57 MiB/s** | 15 MiB/s | 3.8x | **1099ms** | 9194ms |

**关键发现**:
- 读性能差距远大于写: seqread 8.4x, randread 42x vs seqwrite 1.5x
- 唯一持平的是 bigwrite (8 并发顺序写): 两者都受限于 S3 PUT 吞吐
- BrewFS randrw P99 高达 9.2s, JuiceFS 仅 1.1s

## 元数据性能 (metaperf)

| Operation | JuiceFS ops/s | BrewFS ops/s | Ratio | 说明 |
|-----------|-------------|---------------|-------|------|
| **stat** | **1,035,890** | 3,061 | **338x** | 最大差距 |
| open | 5,385 | 1,238 | 4.3x | |
| rename | 1,288 | 533 | 2.4x | |
| create | 259 | 121 | 2.1x | 共同瓶颈: S3 |
| readdir | 27,583 | 24,634 | 1.1x | 性能接近 |

## 根因分析

### 读 8-42x

**根因**: BrewFS 无本地块缓存，每次读都走 S3。

JuiceFS 有 3 级 block cache:
1. 内存 pending pages — 刚写入或刚从 S3 下载
2. 磁盘缓存 — key→file 索引 + CRC32C 校验
3. Singleflight — 同 block 并发读者共享下载

预期: 增加 block-level 磁盘缓存可提升读 5-40x。

### stat 338x

**根因**: BrewFS 每次 stat 查 Redis。

JuiceFS 有两层 attribute cache:
1. Redis CSC (RESP3 client tracking) — 跨客户端共享，失效由 PUSH 消息驱动
2. OpenFile cache — 打开文件期间免查 Redis

预期: 实现 inline attr cache 或 Redis CSC 可提升 stat 50-300x。

### 读延迟 p99 差距

**根因**: BrewFS 无自适应预读 + 无缓存。

JuiceFS 的 fileReader:
- 2 个 session slot 追踪连续读模式
- 翻倍/减半预读窗口
- 预取文件末尾 32KB

预期: 实现自适应预读可显著降低读延迟。

### 写 3x

**根因**: 切片碎片 + 无 writeback。

JuiceFS:
- 自动压缩: 每 100 slice 触发，合并小切片为大 block
- Writeback 模式: 数据先写本地磁盘，延迟上传到 S3

预期: Slice 合并 + optional writeback 可提升随机写。

### randrw P99 9.2s

**根因**: 大量小切片导致 Redis LRange 开销 + S3 GET 次数过多。

JuiceFS 压缩将 100+ slice 合并为 1-2 个，大幅减少 I/O 次数。

---

## 优化路线图

| 优先级 | 项目 | 预期收益 | 难度 | 依赖 |
|--------|------|---------|------|------|
| **P0** | Block-level 磁盘缓存 | 读 5-40x up | 中 | chunk store 架构 |
| **P0** | Inline attr cache (或 CSC) | stat 50-300x up | 低 | meta client |
| **P1** | 自适应预读 | 读延迟显著降低 | 低 | reader 重构 |
| **P1** | Singleflight 去重 | 并发读减少 S3 请求 | 低 | chunk store |
| **P2** | Slice 自动压缩 | 随机写延迟改善 | 中 | 写路径重构 |
| **P2** | Writeback 模式 | 写延迟改善 | 中 | 磁盘缓存先决 |
| **P3** | Redis pipeline / 批量提交 | 元数据写入 TPS | 中 | meta engine |

### P0 实现建议

**Block 磁盘缓存**:
- 复用 chunk/cache_eviction.go 的 KeyIndex 接口 (已有 3 种淘汰策略)
- 复用 disk_cache 的 cacheManager + 一致性哈希多目录路由
- 复用小文件格式: data + CRC32C + tierID

**Attr 缓存**:
- 第一步: 实现进程内 LRU (对标 OpenFile Cache，不含 chunk)
- 第二步: 实现 CSC (对标 redisCache，需要 Redis RESP3 client tracking)
- 最小可用: 只缓存 `i{inode}` 的 GET → 已能覆盖 stat 调用
