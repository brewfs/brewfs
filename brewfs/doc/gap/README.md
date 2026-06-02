# BrewFS 与 JuiceFS 差异分析

本文档集基于当前工作区源码静态分析：

- BrewFS：本仓库 `src/`、`doc/`、`docker/`、`tests/`、`operator/`
- JuiceFS：本仓库内未跟踪目录 `juicefs/`

分析重点不是逐行复刻 JuiceFS，而是按模块识别 BrewFS 当前实现与 JuiceFS 成熟实现之间的关键差距，并给出后续提升方向。

## 文档列表

| 文档 | 内容 |
|---|---|
| [00-overview.md](00-overview.md) | 总体结论、关键差异矩阵、优先级路线 |
| [01-module-map.md](01-module-map.md) | 两边源码模块映射与架构边界差异 |
| [02-metadata-gap.md](02-metadata-gap.md) | 元数据模型、事务语义、缓存、会话、配额、ACL、xattr |
| [03-data-cache-gap.md](03-data-cache-gap.md) | chunk/slice/block 数据路径、对象存储、缓存、压缩、GC/compact |
| [04-vfs-fuse-posix-gap.md](04-vfs-fuse-posix-gap.md) | VFS/FUSE、POSIX 语义、句柄、锁、跨客户端一致性 |
| [05-ops-ecosystem-gap.md](05-ops-ecosystem-gap.md) | CLI、运维、观测、部署、K8s/CSI、测试体系 |
| [06-roadmap.md](06-roadmap.md) | 面向迭代的提升路线和验收标准 |

## 总体判断

BrewFS 已经不只是早期 Demo：当前代码里已有 `MetaStore` 多后端、`MetaClient` 缓存、FUSE/VFS、异步写入、读缓存、compaction、GC、控制面和 xfstests/LTP 脚本。和 JuiceFS 的差距主要在四类地方：

1. **语义闭环**：JuiceFS 的 meta/vfs 接口承载完整 POSIX 生命周期；BrewFS 有很多接口与结构已经预留，但部分后端行为、跨客户端一致性、fsync/rename/truncate 等边界还需要系统性收敛。
2. **生产运维闭环**：JuiceFS 有 format/config/status/fsck/dump/load/warmup/gateway/quota 等完整命令；BrewFS CLI 当前核心是 `mount/gc/info`。
3. **后端生态与格式治理**：JuiceFS 的格式、对象存储、加密、压缩、分片、限速、兼容版本都在 volume format 中治理；BrewFS 当前配置还更偏挂载参数与本地实现细节。
4. **测试与发布可信度**：BrewFS 已在补 xfstests/LTP/qlean/fuzz 路径，但还缺 JuiceFS 那种多后端、多平台、长稳、故障注入和运维命令级回归体系。

