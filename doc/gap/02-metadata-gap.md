# 元数据层差异

## 对照对象

- BrewFS：`src/meta/store.rs`、`src/meta/client/mod.rs`、`src/meta/stores/*`
- JuiceFS：`juicefs/pkg/meta/interface.go`、`base.go`、`redis.go`、`sql*.go`、`tkv*.go`

## 总体差异

BrewFS 的元数据层已经明显参考了 JuiceFS：同样有 inode、dentry、slice、session、lock、xattr、ACL、quota、dump/load、compact 等概念。但当前最大差异是 **BrewFS 的接口已经扩张到接近 JuiceFS，实际后端语义还没有全部闭环**。

## 接口覆盖差异

### 已具备或已有雏形

BrewFS 当前已经有：

- 基础命名空间：`lookup`、`lookup_path`、`readdir`、`mkdir`、`rmdir`、`create_file`、`unlink`、`rename`
- 属性：`stat`、`set_file_size`、`truncate`、`set_attr`、`chmod`、`chown`
- 数据映射：`get_slices`、`append_slice`、`read_slices`、`write_slice`
- 后台维护：`replace_slices_for_compact`、`delayed_slice`、`uncommitted_slice`
- session：`start_session`、`shutdown_session`、`cleanup_sessions`
- 锁：global lock、flock/plock 接口
- 扩展能力接口：xattr、ACL、quota、dump/load
- 多后端：Database、Redis、Etcd

### JuiceFS 更完整的部分

JuiceFS 的 `meta.Meta` 除了上述能力，还稳定覆盖：

- `Init/Load/Reset` 与 `Format` 管理
- `Open/Close` 与 open-file cache
- `Access/CheckSetAttr` 权限检查
- `Fallocate/CopyFileRange`
- trash、sustained inode、detached node 清理
- dir stat、user/group/project quota
- `Check`/fsck、`Summary`、`Clone`
- `DumpMeta/LoadMeta` 与 V2
- ACL 规则缓存与复用
- changelog、Kerberos token 等生态功能

## 核心 gap

### 1. `MetaStore` 可选方法过多，缺少强制分层

`src/meta/store.rs` 中大量方法默认返回 `MetaError::NotImplemented`。这在快速对齐 JuiceFS API 时很方便，但会让调用方难以判断：

- 这是“功能未开启”，还是“后端不支持”？
- 当前挂载是否允许使用这个 FUSE 操作？
- 测试通过的是 trait 默认行为还是实际后端行为？

提升方向：

- 将 trait 拆成能力子 trait，例如 `NamespaceStore`、`DataMapStore`、`SessionStore`、`QuotaStore`、`AclStore`、`DumpLoadStore`、`CompactStore`。
- 或保留大 trait，但增加 `capabilities()`，mount 时根据配置做 fail-fast。
- 对核心 POSIX 必选方法禁止默认实现，避免后端漏实现。

### 2. 缺少 volume format 与兼容性治理

JuiceFS `Format` 是元数据层的产品契约，记录 block size、storage、bucket、compression、hash prefix、capacity、inodes、encryption、trash days、ACL、quota、client version 等。

BrewFS 当前 layout、data backend、meta backend 多来自 mount config。风险是：

- 同一个元数据卷可能被不同 block size/chunk size 挂载。
- 对象存储 bucket 或 key layout 变更不易校验。
- 后续压缩/加密/feature flags 缺少兼容性门禁。

提升方向：

- 新增 `format` 命令和 `volume_format` 元数据记录。
- mount 时加载 format，并与 CLI/YAML 参数比对。
- layout、object key format、compression、encryption、feature flags 一经 format 后默认不可变。
- 加入 `min_client_version/max_client_version`。

### 3. 权限、ACL、xattr 需要从结构走向语义

BrewFS 目前有 mode/uid/gid 和 ACL/xattr 接口。文档也指出 ACL 仍偏占位，xattr 后端行为可能不齐。JuiceFS 已有 POSIX ACL 管理、ACL rule cache、xattr 命令与测试。

提升方向：

- 明确支持 POSIX ACL 还是仅支持基础 mode。
- `Access`、`Open`、`SetAttr`、`Create`、`Mkdir`、`Rename` 都应统一走权限检查。
- xattr 需要定义 namespace、size limit、错误码、禁用开关和跨后端一致行为。
- 增加 pjdfstest、xfstests xattr/acl 相关用例。

### 4. quota 与 dir stat 还没有成为写路径硬约束

JuiceFS 的 quota、dir stat、summary 与写入/创建/删除/rename 紧密结合。BrewFS trait 中已有 `Quota`、`DirStat`、`VolumeStat`、`update_dir_stat` 等接口，但需要确认各后端完整度和上层调用闭环。

提升方向：

- 先实现目录级 quota，再扩展 user/group quota。
- 所有 inode/slice 增删必须产出 accounting delta。
- rename、link、unlink、truncate、compact/gc 都要更新统计或可重建。
- 提供 `quota check/repair` 和 `summary` 命令。

### 5. session 与 crash recovery 需要产品化

BrewFS 已有 session manager、heartbeat、cleanup，并在 GC 中处理 uncommitted slice。JuiceFS 的 session 还关联 sustained inode、locks、open files、stale cleanup、status 展示。

提升方向：

- `brewfs status` 列出 session、mount point、pid、heartbeat、locks、pending writes。
- close/crash 后未完成 slice、已 unlink 但仍打开的 inode、lock 释放要有明确流程。
- session cleanup 需要进入测试矩阵：进程 kill、网络分区、元数据库重启。

### 6. 跨客户端缓存失效仍是关键短板

BrewFS 有 TTL cache、path trie 和 Etcd watch。当前差距在于跨后端、跨客户端的一致失效协议尚未统一。JuiceFS 通常通过元数据操作、open-cache TTL、close-to-open 边界和明确文档管理一致性。

提升方向：

- 定义 `inode version` / `chunk version`，写、truncate、rename、setattr 后递增。
- reader cache 绑定版本；版本不匹配必须失效。
- Redis/SQL 后端也需要可用的 invalidation 方案，不能只依赖 Etcd watch。
- open 时刷新属性，close/fsync 后发布变更，形成 close-to-open 最小保证。

## 建议验收清单

- 三个后端 SQLite/Postgres、Redis、Etcd 在同一 meta API 测试套件下通过。
- 对所有默认 `NotImplemented` 方法生成能力矩阵文档。
- `format -> mount -> status -> dump -> load -> fsck` 成为基础命令闭环。
- ACL/xattr/quota 不支持时返回稳定错误；支持时跨后端行为一致。
- crash recovery 测试覆盖：写一半 kill、compact 一半 kill、unlink-open kill、lock holder kill。

