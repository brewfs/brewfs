# JuiceFS Internals 文档索引

基于 vendored juicefs 源码 (`juicefs/pkg/`) `54439a2`, `2026-05-21` 深度分析。

## 文档列表

| # | 文档 | 内容 |
|---|------|------|
| 1 | [01-architecture.md](01-architecture.md) | 架构总览、Redis 28 个 Key Pattern、Mount/Format 生命周期 |
| 2 | [02-read-path.md](02-read-path.md) | 读路径完整调用链、自适应预读状态机、Singleflight 去重 |
| 3 | [03-write-path.md](03-write-path.md) | 写路径 5 级缓冲阶、10 种上传触发条件、Writeback 模式 |
| 4 | [04-cache-system.md](04-cache-system.md) | 5 层缓存体系: CSC → OpenFile → Memory → Disk → Prefetch |
| 5 | [05-transaction-engine.md](05-transaction-engine.md) | Redis WATCH/EXEC 两阶段锁、5 个关键事务伪代码、Session 管理 |
| 6 | [06-slice-compaction.md](06-slice-compaction.md) | Slice 二叉树合并 (cut/buildSlice)、S3 endpoint 解析、GC |
| 7 | [07-performance-comparison.md](07-performance-comparison.md) | BrewFS vs JuiceFS benchmark、根因分析、优化路线图 |
| 8 | [brewfs-vs-juicefs-full-comparison.md](brewfs-vs-juicefs-full-comparison.md) | **全模块逐行对比**: 读路径/写路径/缓存/事务 — 27 维度差异矩阵 |

## 实施计划

| 计划 | 文件 |
|------|------|
| 综合性能优化 | [../superpowers/plans/2026-05-23-brewfs-perf-optimization.md](../superpowers/plans/2026-05-23-brewfs-perf-optimization.md) |
| **本地缓存读优化** | [../superpowers/plans/2026-05-23-read-cache-optimization.md](../superpowers/plans/2026-05-23-read-cache-optimization.md) |

## 关键源码文件

| 文件 | 行数 | 内容 |
|------|------|------|
| `meta/redis.go` | 6249 | Redis 引擎: 全部 key schema, txn(), 28 个事务方法 |
| `meta/base.go` | ~4000 | Base engine: compactChunk, session mgmt, quota, GC |
| `meta/redis_csc.go` | ~350 | RESP3 client-side caching |
| `meta/openfile.go` | ~280 | OpenFile 缓存: 对象池, 淘汰 |
| `meta/slice.go` | ~240 | Slice 二叉树结构: cut, buildSlice, compactChunk |
| `vfs/vfs.go` | ~1400 | FUSE 入口: Read/Write/Flush/Fsync, handle, internal nodes |
| `vfs/reader.go` | ~1000 | 读路径: fileReader, sliceReader, adaptive readahead |
| `vfs/writer.go` | ~530 | 写路径: fileWriter, chunkWriter, sliceWriter, commitThread |
| `chunk/cached_store.go` | ~1200 | Chunk store: wSlice/rSlice, upload/download, staging |
| `chunk/disk_cache.go` | ~1500 | 磁盘缓存: cacheManager, 文件格式, 状态机 |
| `chunk/cache_eviction.go` | ~250 | 淘汰策略: none, 2-random, LRU |
| `chunk/singleflight.go` | ~80 | 并发去重 |
| `chunk/prefetch.go` | ~70 | 预取引擎 |
| `pkg/object/s3.go` | ~600 | S3 后端: URL 解析, 认证, multipart |

## 交互式可视化

[architecture.html](architecture.html) — 7 个 tab 的交互式 SVG 架构图。

## 对比文档

[../performance/brewfs-vs-juicefs-analysis.md](../performance/brewfs-vs-juicefs-analysis.md) — 高层面的 BrewFS vs JuiceFS 差距分析。
