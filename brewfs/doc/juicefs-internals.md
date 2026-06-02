# JuiceFS Internals — 文档导航

> 基于 vendored juicefs 源码分析。详细内容已拆分为独立文档。

## 文档列表

| # | 文档 | 内容 |
|---|------|------|
| 1 | [juicefs/01-architecture.md](juicefs/01-architecture.md) | 架构总览、I/O 栈、Redis 28 Key Pattern、Mount/Format |
| 2 | [juicefs/02-read-path.md](juicefs/02-read-path.md) | 读路径调用链、自适应预读、Singleflight |
| 3 | [juicefs/03-write-path.md](juicefs/03-write-path.md) | 写路径缓冲阶、上传触发条件、Writeback |
| 4 | [juicefs/04-cache-system.md](juicefs/04-cache-system.md) | 5 层缓存: CSC → OpenFile → Memory → Disk → Prefetch |
| 5 | [juicefs/05-transaction-engine.md](juicefs/05-transaction-engine.md) | 两阶段锁、5 个关键事务、Session/Quota |
| 6 | [juicefs/06-slice-compaction.md](juicefs/06-slice-compaction.md) | Slice 二叉树合并、S3 适配、GC |
| 7 | [juicefs/07-performance-comparison.md](juicefs/07-performance-comparison.md) | BrewFS vs JuiceFS benchmark、根因、优化路线图 |

## 交互式可视化

[点击打开架构图](juicefs-architecture.html) — 7 tab SVG 交互式架构可视化。

## 索引

详细索见 [juicefs/README.md](juicefs/README.md)。
