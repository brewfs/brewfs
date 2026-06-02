# 运维、部署与生态差异

## CLI 差异

BrewFS 当前 CLI：

- `mount`
- `gc`
- `info`

JuiceFS 当前 CLI 包含：

- `format`
- `config`
- `quota`
- `destroy`
- `gc`
- `fsck`
- `restore`
- `dump`
- `changelog`
- `load`
- `version`
- `status`
- `stats`
- `profile`
- `info`
- `mount`
- `umount`
- `gateway`
- `webdav`
- `bench`
- `objbench`
- `mdtest`
- `warmup`
- `rmr`
- `sync`
- `debug`
- `clone`
- `summary`
- `compact`
- `tier`

这说明 JuiceFS 的成熟度不只在读写路径，更在“出问题后如何诊断、修复、迁移、恢复”。

## 最需要补的命令

### P0：格式与状态

| 命令 | 目的 |
|---|---|
| `format` | 初始化 volume format，固定 layout/backend/feature |
| `status` | 列出 volume、session、mount、pending jobs、locks、capacity |
| `stats` | 输出缓存、读写、meta/object 请求、compact/gc 指标 |
| `version` | 输出 client/version/features/build info |

### P1：恢复与维护

| 命令 | 目的 |
|---|---|
| `dump` / `load` | 元数据备份恢复、迁移、回归测试 |
| `fsck` | meta/object 对账，检查 inode/dentry/slice 引用 |
| `compact` | 手动触发 path/chunk compact |
| `gc` 增强 | dry-run 明细、对象删除进度、错误重试 |
| `warmup` / `evict` | 缓存预热与清理 |

### P2：生态与便利性

| 命令 | 目的 |
|---|---|
| `quota` | 目录/用户/组配额管理 |
| `summary` | 目录树统计 |
| `bench` / `objbench` | 元数据与对象存储基准 |
| `debug` / `profile` | 采集运行时诊断包 |
| `gateway` / `webdav` | 协议网关，视产品方向决定是否需要 |

## 控制面差异

BrewFS 已有 Unix socket control plane：

- `Ping`
- `GetInfo`
- `RunGc`
- `GetJob`

建议把它扩成 mount 实例的标准管理通道：

- `RunCompact`
- `RunFsck`
- `RunWarmup`
- `GetStats`
- `ListSessions`
- `ListLocks`
- `DumpDebugBundle`
- `CancelJob`

设计要求：

- 所有长任务都返回 job id。
- job 有进度、阶段、错误列表、起止时间。
- CLI 与未来 operator/CSI 使用同一个协议。
- socket 文件权限与 mount owner 绑定。

## 观测差异

JuiceFS 有 Prometheus 指标、profile/debug 命令、pprof、日志与 status。BrewFS 当前主要依赖 tracing/log、可选 profiling feature、测试脚本产物。

建议指标：

### 元数据

- meta op latency/count/error by operation/backend
- transaction retry/deadlock count
- cache hit/miss/eviction
- watch invalidation lag
- session heartbeat age

### 数据面

- object get/put/range/delete latency/count/error
- upload queue depth
- write dirty bytes
- fsync wait time
- read cache hit/miss
- prefetch issued/used/wasted
- singleflight coalesced count

### 维护任务

- compact candidates/processed/skipped
- fragmentation ratio
- rewritten bytes
- delayed slices pending
- orphan objects pending
- GC delete failures

## 部署差异

### BrewFS 当前

- docker-compose 与 xfstests/LTP 运行脚本较完整。
- 有 operator 雏形，包含 CRD、reconciler、manifests。
- FUSE mount 仍主要以本地命令/容器脚本为中心。

### JuiceFS 成熟点

- CSI 生态、StorageClass/PVC、动态供给。
- mount pod/cache dir/secret/config 的标准化。
- K8s 场景的缓存共享、滚动升级、监控和最佳实践。

提升方向：

- 短期：operator 只管理静态 mount 与 config secret，明确支持矩阵。
- 中期：实现 CSI NodePublish/NodeUnpublish、ControllerPublish 可按需选择。
- 长期：支持动态 provisioning、cache PVC、mount pod sidecar、metrics scraping。

## 测试体系差异

BrewFS 已有：

- Rust unit/integration tests
- qlean smoke
- xfstests 容器/KVM 脚本
- LTP 脚本
- fuzz guide
- 多个 bugfix 复盘文档

需要补齐的是“可持续准入矩阵”：

| 维度 | 最小矩阵 |
|---|---|
| meta backend | sqlite、postgres、redis、etcd |
| object backend | localfs、s3/minio、rustfs |
| mount mode | default、writeback、cache disabled、xattr/acl on/off |
| workload | xfstests smoke、pjdfstest、LTP subset、fsstress、fio |
| failure | kill client、kill meta、kill object、network timeout、disk full |
| upgrade | old format mount、new client mount、dump/load roundtrip |

## 发布工程差异

JuiceFS 有较成熟的 release、version、兼容控制。BrewFS 需要：

- build info：git sha、feature flags、rust version。
- format version 与 migration。
- 后端 schema migration 工具。
- release notes 模板，标注 breaking changes。
- downgrade/upgrade 策略。

## 建议落地顺序

1. `format/status/stats` 先落地，建立 volume 和运行态可见性。
2. `dump/load/fsck` 随后落地，建立恢复闭环。
3. 把 compact/gc/warmup 纳入 control plane job。
4. 建立 CI 准入矩阵，先 smoke 后扩展。
5. operator/CSI 复用 CLI/control plane，而不是另造一套管理逻辑。

