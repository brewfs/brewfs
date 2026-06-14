# 提升路线

## 目标分层

BrewFS 后续可以按三个阶段推进：

1. **语义可信**：单机与多客户端 POSIX 核心语义明确，写入/缓存/元数据后端行为可测试。
2. **运维可用**：出现故障能 status、fsck、dump/load、gc/compact、debug。
3. **生产可扩展**：多后端、多对象存储、K8s/CSI、压缩/加密、限速、监控和发布兼容。

## 阶段 1：语义可信

### 任务

- 定义并文档化 `write/flush/fsync/close` 成功条件。
- 定义 close-to-open 最小一致性。
- 为 `rename`、`truncate`、`unlink-open`、`link/symlink` 建立 POSIX 回归测试。
- `MetaStore` 增加 capability 矩阵，mount 阶段 fail-fast。
- 三个元数据后端跑同一核心语义测试。
- reader/writer cache 引入 inode/chunk version 校验。

### 验收

- SQLite/Redis/Etcd 至少通过同一批核心 POSIX smoke。
- fsync 故障注入能够返回错误，而不是静默成功。
- 多客户端 close-to-open 测试稳定通过。
- `MetaError::NotImplemented` 不会在普通挂载路径上意外暴露。

## 阶段 2：运维可用

### 任务

- 新增 `format` 与 volume format 表/键。
- 新增 `status`、`stats`、`version`。
- 新增 `dump/load`，支持 roundtrip 测试。
- 新增 `fsck --dry-run`，先检查 inode/dentry/slice/object 引用。
- 增强 `gc`，输出明细、进度和错误。
- 新增 `compact <path>`，复用现有 compactor。
- 扩展 control plane job：progress、cancel、history。

### 验收

- 可以从空后端执行 `format -> mount -> write -> status -> dump -> load -> fsck`。
- 对象删除失败后，GC 下轮可重试且可观测。
- compact/gc/fsck 都能以 job 形式查询进度。
- dump/load 后 xfstests smoke 不回归。

## 阶段 3：生产可扩展

### 任务

- 对象后端 conformance test：LocalFS、S3/MinIO、RustFS。
- multipart、object list、head、copy、abort upload。
- cache warmup/evict/check/status。
- Prometheus metrics 与 debug bundle。
- 数据压缩和加密以 format feature 方式落地。
- CSI NodePublish/NodeUnpublish 与 operator 对齐。
- schema migration 与版本兼容策略。

### 验收

- 多后端性能基准可重复。
- 压缩/加密卷无法被不兼容 client 挂载。
- K8s 中可用 StorageClass/PVC 完成挂载和回收。
- 长稳测试覆盖 kill/restart/network timeout。

## 优先级清单

| 优先级 | 项目 | 原因 |
|---|---|---|
| P0 | fsync/flush/close 错误传播 | 直接影响数据可靠性 |
| P0 | close-to-open 与 cache version | 直接影响多客户端正确性 |
| P0 | rename/truncate/write 语义回归 | 应用常用且容易损坏数据 |
| P0 | MetaStore capability 与后端矩阵 | 防止不同后端悄悄偏离 |
| P1 | format/status/stats | 建立基本运维可见性 |
| P1 | dump/load/fsck | 建立恢复与迁移闭环 |
| P1 | compact/gc CLI 化 | 现有能力产品化 |
| P1 | metrics/control plane job | 支撑线上排障 |
| P2 | 对象后端生态 | 扩大场景 |
| P2 | 压缩/加密/限速 | 成本、安全与性能 |
| P2 | CSI/operator | K8s 生产落地 |

## 推荐先做的 10 个具体 PR

1. 新增 `MetaStore::capabilities()`，生成 `doc/gap` 中的后端能力表。
2. 新增 `brewfs format`，写入最小 volume format：name、uuid、chunk_size、block_size、data_backend、feature flags。
3. mount 时校验 volume format，不匹配直接拒绝。
4. 给 writer 维护 per-handle/per-inode async error，并让 fsync/flush/close 返回。
5. 引入 chunk version，writer commit/truncate/compact 后递增，reader cache 校验。
6. `rename` 原子替换跨后端统一测试。
7. `truncate + pwrite + fsync` stress 测试。
8. `brewfs status` 读取 control plane，展示 mount/session/jobs。
9. `brewfs stats` 输出 JSON，先覆盖 meta/object/cache/writer 基础计数。
10. `dump/load` SQLite 先行，然后扩展 Redis/Etcd。

## 不建议优先做的事

- 不建议先扩展大量对象存储后端。当前更缺语义和运维闭环。
- 不建议先做复杂网关。gateway/webdav 可以等核心 FS 稳定。
- 不建议把 operator 做得很厚。operator/CSI 应复用 CLI/control plane。
- 不建议在没有 volume format 前加入压缩/加密，否则兼容性成本会很高。

