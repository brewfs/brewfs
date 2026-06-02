# 总体差异与优先级

## 一句话结论

BrewFS 当前已经形成了 JuiceFS 风格的核心骨架：`meta + chunk + object + vfs + fuse + compact/gc`。但 JuiceFS 是一个“完整产品化文件系统”，BrewFS 仍更像“正在快速补齐 POSIX 与生产闭环的 Rust 实现”。后续不应只补功能点，而要优先补齐 **一致性语义、元数据事务边界、运维命令、可观测性和多后端验收矩阵**。

## 关键差异矩阵

| 维度 | BrewFS 当前状态 | JuiceFS 对应能力 | 主要差距 | 建议优先级 |
|---|---|---|---|---|
| 架构分层 | `src/meta`、`src/chunk`、`src/vfs`、`src/fuse` 清晰，Rust async 化程度高 | `pkg/meta`、`pkg/chunk`、`pkg/vfs`、`pkg/fuse` 长期演进 | BrewFS 模块边界已接近，但跨模块契约还不稳定 | P0 |
| 元数据接口 | `MetaStore` trait 大而全，Database/Redis/Etcd 三类后端 | `meta.Meta` 覆盖完整文件系统生命周期 | BrewFS 仍有大量默认 `NotImplemented`，后端行为可能不齐 | P0 |
| 数据路径 | 64MiB chunk、4MiB block、slice append、异步上传与 commit | chunk/slice/page/cache/writeback 成熟 | BrewFS 写入状态机先进但复杂，需稳定 fsync/失败传播/跨客户端可见性 | P0 |
| 缓存 | inode/path cache、read page cache、block cache、prefetch 雏形 | metadata cache、data cache、open-cache、writeback、warmup/evict/check | 缺少统一配置面、跨客户端失效协议和运维控制 | P0/P1 |
| POSIX 语义 | 基础操作、link/symlink/lock/fallocate 等已有补齐痕迹 | POSIX 行为覆盖更广，边界文档与测试更充分 | rename、truncate/write 排序、fsync 成功条件、权限/ACL 仍需收敛 | P0 |
| 对象存储 | LocalFS + S3 后端 | 60+ object 相关文件，覆盖 S3/OSS/COS/Azure/GCS/HDFS/NFS/SQL 等 | 后端矩阵、multipart、restore、storage class、sharding、加密生态不足 | P1 |
| 压缩/加密 | 暂未看到稳定数据面压缩/加密闭环 | `pkg/compress`、object encryption、format 级配置 | 成本、安全、兼容性治理不足 | P2 |
| 运维命令 | `mount`、`gc`、`info` | format/config/destroy/gc/fsck/dump/load/status/stats/profile/warmup/compact/gateway/webdav/quota 等 | CLI 与故障恢复能力差距最大 | P1 |
| 控制面 | Unix socket control plane，可发 `RunGc/GetInfo/GetJob` | 内部控制文件、debug/profile/status 等较成熟 | 控制协议还很小，缺维护操作与权限边界 | P1 |
| K8s | 有 operator 雏形 | JuiceFS CSI 成熟 | 还缺 CSI 动态供给、NodePublish、缓存目录、Secret/StorageClass 生态 | P1/P2 |
| 测试 | 有 xfstests/LTP/qlean/fuzz 脚本与 bugfix 记录 | JuiceFS CI 覆盖大量命令、后端、平台与随机测试 | BrewFS 需把测试从“脚本可跑”推进到“准入矩阵” | P0 |

## 当前优势

BrewFS 不只是落后方，也有一些值得保留的方向：

- Rust 类型系统对 `ChunkLayout`、`SliceDesc`、`Span`、`MetaStore`、`BlockStore` 的边界约束更强。
- `DataWriter` 明确拆分 slice 状态机：`Writable -> Readonly -> Uploaded -> Committed`，便于做故障注入和形式化校验。
- Etcd watch、path trie、read page cache、singleflight、compaction lock TTL 等设计已经朝多客户端与高并发方向演进。
- 文档和 bugfix 记录比较细，有利于沉淀语义决策。

## 最大风险

### 1. 接口“大而未闭环”

`src/meta/store.rs` 的 `MetaStore` trait 已经吸收了很多 JuiceFS 语义，包括 quota、ACL、xattr、dump/load、compact、session、lock、fallocate 等。但大量方法仍有默认 `MetaError::NotImplemented`。这会造成两个问题：

- 上层代码很难知道某个后端到底支持哪些语义。
- 测试若只覆盖 SQLite 或某个路径，Redis/Etcd/Postgres 可能悄悄偏离。

### 2. 写路径语义比实现更复杂

BrewFS 写路径为了吞吐采用异步上传和异步 commit。这个方向合理，但它把用户可见语义压在几个边界上：

- `write()` 返回时数据处于哪个状态？
- `flush/fsync/close` 成功是否等价于对象与元数据都持久化？
- 另一个客户端看到 size 扩大时，是否一定能读到对应数据？
- truncate 与 in-flight write 如何线性化？

这些问题需要被明确成文并由测试固定。

### 3. 运维工具不足会放大线上风险

JuiceFS 很多成熟度来自 `fsck`、`dump/load`、`status`、`stats`、`warmup`、`compact`、`quota`、`config` 等命令。BrewFS 当前即使核心路径能跑，一旦出现元数据不一致、对象孤儿、缓存污染、会话泄漏、容量异常，用户侧缺少标准处理手段。

## 建议路线

### P0：先把语义和回归压住

- 定义并测试 `write/flush/fsync/close/truncate/rename/open` 的成功条件。
- 为 `MetaStore` 建立能力矩阵，去掉“上层误以为支持”的隐患。
- 固定 SQLite、Redis、Etcd 三后端的最小 POSIX 准入集。
- 将 xfstests/LTP/qlean 变为 CI 或准入脚本的稳定矩阵。

### P1：补生产运维闭环

- 增加 `format`、`status`、`fsck`、`dump`、`load`、`compact`、`warmup`、`stats`。
- 统一 volume format，记录 layout、object backend、compression/encryption、feature flags、client version。
- 扩展 control plane，支持长任务、进度、取消、权限隔离。
- 完善 Prometheus/metrics、pprof 或 Rust 等价 profiling 能力。

### P2：补生态与成本优化

- 更多对象后端、multipart 行为、storage class、restore、sharding。
- 数据压缩、加密、hash prefix、带宽限速。
- CSI 与 operator 对齐，形成 K8s 标准用法。
- warmup/evict/cache status、共享缓存目录、缓存容量治理。

