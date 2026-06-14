# 元数据系统

BrewFS 的元数据层管理文件系统命名空间、inode 属性、Chunk/Slice 布局映射和会话生命周期。通过统一 trait 抽象支持多种后端，并在 trait 之上建立缓存层加速热点访问。

源码位置：`src/meta/`

## MetaStore trait

`src/meta/store.rs` 定义了 `MetaStore` trait，包含 70+ 个方法，覆盖以下操作类别：

| 类别 | 方法 | 说明 |
|---|---|---|
| 命名空间 | `stat`, `lookup`, `lookup_path`, `readdir`, `mkdir`, `rmdir`, `create_file`, `unlink`, `rename`, `rename_exchange` | 基本文件系统操作 |
| 属性管理 | `set_attr`, `chmod`, `chown`, `set_file_size`, `extend_file_size`, `truncate` | inode 属性修改 |
| 硬链接/软链接 | `link`, `symlink`, `read_symlink` | 链接操作 |
| Slice 数据 | `get_slices`, `append_slice`, `write`, `read_slices`, `write_slice` | Chunk Slice 读写 |
| 会话管理 | `start_session`, `shutdown_session`, `cleanup_sessions` | 客户端生命周期 |
| 锁管理 | `get_global_lock`, `is_global_lock_held`, `release_global_lock` | 全局排他锁（compact 等场景） |
| 文件锁 | `get_plock`, `set_plock`, `get_flock`, `set_flock` | POSIX 文件锁和 BSD flock |
| 压缩/GC | `list_chunk_ids`, `replace_slices_for_compact`, `replace_slices_for_compact_with_version`, `record_uncommitted_slice`, `confirm_slice_committed`, `process_delayed_slices`, `confirm_delayed_deleted`, `cleanup_orphan_uncommitted_slices`, `delete_uncommitted_slices` | Compaction 和 GC 支持 |
| xattr | `set_xattr`, `get_xattr`, `list_xattr`, `remove_xattr` | 扩展属性 |
| 统计 | `stat_fs`, `get_counter`, `incr_counter`, `update_volume_stat` | 文件系统和用量统计 |
| 目录/配额 | `get_dir_stat`, `sync_dir_stat`, `get_quota`, `set_quota`, `flush_quotas` | 目录统计和配额（预留） |
| 备份 | `dump`, `load` | 元数据导出/导入（预留） |
| 其他 | `open`, `close`, `get_dentries`, `get_names`, `get_paths`, `get_deleted_files`, `remove_file_metadata`, `next_id` | 辅助操作 |

许多高级方法有默认的 `NotImplemented` 实现，允许后端逐步支持。`MetaStoreCapabilities` 结构体声明了后端的能力集，供调用方按需检查。

### MetaError

所有方法返回 `Result<T, MetaError>`，`MetaError` 包含以下 variant：

- `NotFound`, `ParentNotFound`, `AlreadyExists`, `NotDirectory`, `DirectoryNotEmpty` — 语义错误
- `ContinueRetry(RetryReason)` — 可重试的冲突（version conflict、compact conflict、transaction conflict、lock contention）
- `LockConflict`, `LockNotFound`, `DeadlockDetected` — 文件锁相关错误
- `MaxRetriesExceeded` — 重试耗尽
- `Database`, `Io`, `Serialization`, `Config` — 底层错误
- `NotImplemented`, `NotSupported` — 功能未实现
- `Internal`, `Anyhow` — 内部错误

## 三大后端实现

### DatabaseMetaStore

`src/meta/stores/database/mod.rs`

基于 SeaORM，支持 SQLite 和 PostgreSQL。核心表：

**file_meta** — 文件和目录的 inode 属性

| 字段 | 类型 | 说明 |
|---|---|---|
| ino | BIGINT PK | inode 号 |
| size | BIGINT | 文件大小 |
| mode | INTEGER | 权限位 (0o777) |
| uid, gid | INTEGER | 所有者和组 |
| nlink | INTEGER | 硬链接计数 |
| parent | BIGINT | 父目录 inode（单链接场景，O(1) 查找） |
| atime, mtime, ctime | BIGINT | 时间戳 |
| kind | INTEGER | 类型：File/Dir/Symlink |
| deleted | BOOLEAN | 软删除标记 |

**content_meta** — 目录项（父子关系）

| 字段 | 类型 | 说明 |
|---|---|---|
| ino | BIGINT | 子节点的 inode |
| parent_inode | BIGINT | 父目录 inode |
| entry_name | TEXT | 文件名 |
| entry_type | INTEGER | 类型 |

**access_meta** — 目录访问控制和时间

| 字段 | 类型 | 说明 |
|---|---|---|
| ino | BIGINT PK | 目录 inode |
| mode | INTEGER | 目录权限 |
| uid, gid | INTEGER | 所有者 |
| atime, mtime, ctime | BIGINT | 时间戳 |
| nlink | INTEGER | 链接计数 |

**slice_meta** — Chunk 的 Slice 列表

| 字段 | 类型 | 说明 |
|---|---|---|
| chunk_id | BIGINT | Chunk ID |
| slice_id | BIGINT | Slice ID |
| offset | BIGINT | Chunk 内偏移 |
| length | BIGINT | 数据长度 |

**其他表**：`link_parent_meta`（多硬链接的父目录追踪）、`delayed_slice`（GC 两阶段删除）、`uncommitted_slice`（heavy compaction 崩溃恢复）、`xattr_meta`（扩展属性）、`counter_meta`（全局计数器）、`session_meta`（会话信息）、`plock_meta`（POSIX 文件锁）、`locks_meta`（flock 锁）

DatabaseMetaStore 使用 SeaORM 的事务保证 ACID。关键操作（如 rename、append_slice + update_size）在单个事务中完成。

### EtcdMetaStore

`src/meta/stores/etcd/mod.rs`

基于 etcd-client，将文件系统元数据映射到 KV 键空间：

| Key 模式 | 值 | 说明 |
|---|---|---|
| `I{ino}` | JSON (FileAttr) | inode 属性 |
| `C{parent_ino}/{name}` | "{ino}:{type}" | 目录项 |
| `S{chunk_id}` | JSON Vec\<SliceDesc\> | Chunk 的 Slice 列表 |
| `N{key}` | i64 string | 全局 ID 生成器 |
| `LP{ino}/{parent}/{name}` | "" | 硬链接父目录追踪 |
| `DS{id}` | 20 bytes binary | 延迟删除 Slice 记录 |
| `US{slice_id}` | JSON | 未提交 Slice 记录 |
| `PLOCK{ino}/{owner}` | JSON | POSIX 文件锁 |
| `X{ino}/{name}` | bytes | 扩展属性 |

**Etcd 事务** (`src/meta/stores/etcd/txn.rs`)：封装了 etcd 的 compare-and-swap 事务模式。`Txn` builder 支持 `compare_version`, `put`, `delete`, `get` 等操作。

**Watch 机制** (`src/meta/stores/etcd/watch.rs`)：EtcdMetaStore 支持 watch 某个 key 前缀，用于缓存失效事件的跨节点传播。

Slice 列表以完整 `Vec<SliceDesc>` 存储在单个 key 下。修改时读取 → 修改 → 事务写回。对于 concurrent modification，使用 etcd 的 version CAS 做乐观并发控制。当 `replace_slices_for_compact_with_version` 发现 version 不匹配时，返回 `ContinueRetry(VersionConflict)`。

### RedisMetaStore

`src/meta/stores/redis/mod.rs`

使用 Redis 的原子性语义实现元数据操作：

**键空间**（与 JuiceFS 类似）：

| Key 模式 | 类型 | 说明 |
|---|---|---|
| `i{ino}` | String (JSON) | inode 属性 |
| `d{ino}` | Hash | 目录子项（name → ino:type） |
| `c{ino}_{chunk_idx}` | List | Chunk Slice 列表 |
| `c{ino}_{chunk_idx}:v` | String | Chunk 版本号（CAS 并发控制） |
| `nextinode`, `nextchunk`, `nextslice` | String (int) | 全局 ID 计数器 |
| `delslices` | Hash | 延迟删除 Slice 记录 |
| `session{id}` | String (JSON) | 会话信息 |

**Version + Lua CAS**（`doc/redis-version-cas.md` 详细记录）：

早期使用 `WATCH + MULTI + EXEC` 做 chunk slice list 的并发控制，在高冲突场景（如两进程并发 truncate）下频繁重试 + 每次重试新建 TCP 连接，导致连接风暴。

改为 Version + Lua CAS 方案：
- 每个 chunk list 配套一个 version key：`c{ino}_{idx}:v`
- 修改 slice list 时必须递增 version
- 使用 Lua 脚本 `CHUNK_CAS_LUA` 做原子 CAS：仅当 version 匹配才替换数据并递增 version
- 所有运行时操作复用 `self.conn.clone()`（multiplexed 连接），不再每次新建连接

## MetaClient

`src/meta/client/mod.rs`

`MetaClient` 是 MetaStore 之上的缓存代理层，所有 VFS 的元数据操作都通过 MetaClient 进行。

### 缓存架构

**InodeCache** (`src/meta/client/cache.rs`)：

- `Moka Cache`（TTL + LRU 淘汰）作为生命周期管理器
- `DashMap<u64, Arc<RwLock<InodeEntry>>>` 作为主存储，支持高并发读写
- `InodeEntry` 包含：attr（`FileAttr`）、parent（父目录 inode）、children（子节点列表，含 `ChildrenState` 状态追踪）
- `ChildrenState`：`NotLoaded` / `Partial` / `Complete`，精确追踪目录内容的加载状态。只有 `Complete` 状态的目录才能直接返回 readdir 结果，避免不完整数据

**路径解析缓存**：

两层结构并行使用：

1. **path_cache**：`Moka Cache<String, i64>`，完整路径 → inode 号的 O(1) 映射
2. **PathTrie** (`src/meta/client/path_trie.rs`)：前缀树，关键能力是 **前缀匹配失效** — 当目录被重命名或删除时，可以在 O(depth) 时间内移除整个子树的所有路径缓存，而不是遍历扁平的 K-V 缓存（O(N)）

**缓存失效机制**：

1. `inode_to_paths: DashMap<i64, Vec<String>>` 维护反向索引 — inode → 其所有路径
2. 目录修改时触发 `invalidate_parent_path`：
   - 从反向索引找到被修改 inode 的所有路径
   - 对每个路径调用 `PathTrie::remove_by_prefix`，原子性移除该路径及其所有子孙
   - 获取被移除的 (path, ino) 对列表
   - 从 path_cache 和反向索引中逐条清理

### 会话管理

`src/meta/client/session.rs`

每个挂载实例维护一个 `Session`，包含：

- `session_id: Uuid` — 全局唯一标识
- `SessionInfo` — hostname、进程 PID、启动时间
- `heartbeat_token: CancellationToken` — 心跳取消信号
- 后台心跳任务（定期更新 `last_heartbeat` 时间戳）

会话作用：
- GC 通过比对活跃 session 判断哪些 inode 处于使用中
- Session cleanup 扫描心跳超时的旧 session，回收其残留资源
- Control plane 通过 session 定位运行中的实例

## MetaLayer

`src/meta/layer.rs`

`MetaLayer` 是对 MetaClient 的进一步封装，提供更高层语义的操作：

- `append_slice(chunk_id, SliceDesc)` — 追加一个 Slice，包含 slice_meta 插入和文件属性更新
- `get_slices(chunk_id)` — 获取 Chunk 的所有 Slice
- `next_slice_id()` — 通过 `SLICE_ID_KEY` 获取全局自增 slice_id
- `compact_*` — 封装 compaction 相关的元数据操作
- `prune_slices_for_truncate` — truncate 时裁剪 Slice

VFS 层通过 `MetaLayer` 访问元数据，而不是直接使用 `MetaClient`，保持一层间接性。

## 实体定义

`src/meta/entities/` 包含所有 SeaORM 实体定义，每个文件对应一个数据库表：

| 文件 | 对应表 | 说明 |
|---|---|---|
| `file_meta.rs` | `file_meta` | 文件/目录 inode 属性 |
| `content_meta.rs` | `content_meta` | 目录项（名称→inode 的映射） |
| `access_meta.rs` | `access_meta` | 目录访问控制 |
| `slice_meta.rs` | `slice_meta` | Chunk Slice 记录 |
| `delayed_slice.rs` | `delayed_slice` | GC 延迟删除 |
| `uncommitted_slice.rs` | `uncommitted_slice` | Compaction 崩溃恢复 |
| `link_parent_meta.rs` | `link_parent_meta` | 多硬链接父目录追踪 |
| `xattr_meta.rs` | `xattr_meta` | 扩展属性 |
| `counter_meta.rs` | `counter_meta` | 全局计数器 |
| `session_meta.rs` | `session_meta` | 客户端会话 |
| `plock_meta.rs` | `plock_meta` | POSIX 文件锁 |
| `locks_meta.rs` | `locks_meta` | BSD flock |
| `etcd.rs` | — | etcd 相关的序列化/反序列化辅助 |

### 硬链接的状态转换

`src/meta/entities/link_parent_meta.rs` 和 `doc/link_symlink.md`

BrewFS 使用**可逆转换策略**优化硬链接的父目录查找：

- **单链接文件 (nlink=1)**：`file_meta.parent` 直接存储父目录 inode，O(1) 查找
- **多链接文件 (nlink>1)**：`file_meta.parent = 0`，所有父目录记录在 `link_parent_meta` 表
- **首次创建硬链接 (1→2)**：读取原 `parent`，清零；在 `link_parent_meta` 中创建两条记录
- **退回单链接 (2→1)**：删除 link_parent 记录，从剩余的 `content_meta` 行恢复 `parent` 字段

### 软链接

- `nlink` 固定为 1（不支持对软链接创建硬链接）
- 目标路径存储在 `symlink_target` 字段
- `resolve_path` 和 `resolve_path_follow` 实现了 POSIX 语义：前者返回软链接自身，后者跟随目标

## 权限模型

`src/meta/permission.rs`

支持 POSIX 风格的 `rwxrwxrwx` 权限位：

- 文件默认 mode：`0o100644`（`-rw-r--r--`）
- 目录默认 mode：`0o040755`（`drwxr-xr-x`）
- `chmod` 会剥离 setuid/setgid/sticky 位，仅保留 0o777
- `chown` 独立修改 uid/gid，不影响权限位
- FUSE 层的 `umask` 在创建时通过 `(mode & 0o777) & !(umask & 0o777)` 应用

不支持 setuid/setgid/sticky 的语义执行，不支持 POSIX ACL。

## 配置

`src/meta/config.rs` 定义了元数据层的配置：

- `DatabaseConfig` — 数据库类型和连接参数
- `MetaClientConfig` — 客户端配置（TTL、缓存容量、compact 配置、挂载点等）
- `CacheConfig` — 元数据缓存配置（inode TTL、path TTL、容量）
- `CompactConfig` — Compaction 触发参数（min_slice_count、min_fragment_ratio、sync_threshold 等）
- `ClientOptions` — 客户端选项（挂载点路径等）

## ID 分配

全局自增 ID 通过 `MetaStore::next_id(key)` 分配，key 包括：

- `INODE_ID_KEY` = `"brewfs:next_inode_id"` — 分配新 inode 号
- `SLICE_ID_KEY` = `"brewfs:next_slice_id"` — 分配新 Slice ID

不同后端实现：
- Database：`INSERT INTO counter_meta ... ON CONFLICT UPDATE ... RETURNING`
- Etcd：事务中 `get + put` 实现原子递增
- Redis：`INCR` 命令原语

## Migrations

`src/meta/migrations.rs`

DatabaseMetaStore 使用 SeaORM 的 migration 框架管理 schema 版本。迁移脚本按版本号组织，支持：

- 创建/修改表结构
- 创建/删除索引
- 数据迁移
