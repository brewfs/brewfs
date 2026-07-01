# 一致性语义与并发控制

BrewFS 的一致性模型在不同场景下有不同保证。当前版本（截至 2026-05）的单进程语义相对明确，跨进程一致性仍在逐步完善。

## 单进程一致性

### 读写互斥

每个 `FileHandle` 内部有一个 gate（`tokio::sync::RwLock` 风格的读写互斥）：

- 读操作等待进行中的写操作完成
- 写操作等待进行中的读操作完成
- 多个读操作可以并发

这个机制保证了单进程内的 read-after-write 正确性：写完即读，不需要等待元数据提交。

### Read-after-Write 保证

读路径在 `FileReader::read_at` 开始前执行 flush-before-read：

1. 检查目标 inode 是否有 pending writes（Writable/Readonly/Uploading 状态的 Slice）
2. 如果有 → 先触发 `writer.flush(ino)`，等待 pending slice 上传并提交
3. 对于已经是 `Uploaded` 但未提交的 Slice，reader 通过 overlay 机制直接读取（无需等待提交）
4. flush 完成后，reader cache 被 invalidate，重新加载 SliceDesc

### Truncate 一致性

`truncate` 操作的执行过程：

1. 锁定所有该文件的 FileHandle（阻止并发读写）
2. flush 所有 pending writes
3. 更新 metadata 中的文件大小
4. 按 `chunk_size` 边界裁剪 Slice（`prune_slices_for_truncate`）
5. 清除 reader 和 writer 的所有 inode 缓存
6. 更新内存中的文件大小

这防止了"先收缩再扩展"场景下旧数据重新出现的可能性。

### Flush/Fsync

- `flush`：冻结当前 writable slice，触发上传和提交。同时更新 atime/mtime（`update_timestamps_on_flush`，用于 mmap-heavy 负载）
- `fsync`：同 flush，但阻塞等待所有 upload 和 commit 完成

flush 与 write 串行化：flush 进行期间，新的 write 等待。

## 跨进程一致性

### Close-to-Open 语义

当前实现为尽力而为（best-effort）：

- `open`：调用 `stat_fresh`（绕过缓存的 stat）刷新 inode 的 size 和属性
- `close`：触发 flush（如果是写句柄）
- 跨进程的缓存失效机制尚未完全实现

具体差距：
- **无跨进程 cache invalidation**：进程 A 写入后，进程 B 的 reader cache 不会自动失效。进程 B 只能在下次 open 时通过 `stat_fresh` 获取新 size
- **异步写提交窗口**：进程 A 的 write 返回后，数据可能还在 Uploading 状态。进程 B 此时读取可能看到旧数据或零字节（尽管文件 size 已更新）
- **无缓存一致性协议**：没有实现类似 JuiceFS 的分布式缓存失效（如 etcd watch 驱动的失效事件）

### 什么时候数据对读可见

从写进程的视角：

1. **写进程本身**：立即（overlay 读取 Writable Slice）+ flush 后（提交后的 SliceDesc）
2. **同一进程的其他句柄**：flush 提交后（close-to-open 语义，需重新 open）
3. **其他进程的句柄**：flush 提交 + close-to-open（重新 open 触发 stat_fresh），但 reader cache 可能保留旧数据直到 TTL 过期

### Etcd Watch 缓存失效（规划中）

`EtcdMetaStore` 已具备 watch 能力（`src/meta/stores/etcd/watch.rs`），计划用于跨节点传播缓存失效事件。`MetaStoreCapabilities::watch_invalidation` 标记此能力。

## 文件锁

`src/meta/file_lock.rs`

### POSIX 文件锁（fcntl / setlk）

`MetaStore` trait 的 `set_plock` 和 `get_plock`：

```rust
pub struct FileLockRange {
    pub start: u64,
    pub end: u64,    // inclusive
}

pub enum FileLockType {
    ReadLock,   // 共享锁
    WriteLock,  // 排他锁
    UnLock,     // 释放锁
}
```

特性：
- 读锁共享（多个进程可同时持有读锁）
- 写锁互斥（与读锁和写锁均冲突）
- 同一进程重复加锁：新锁替换旧锁（符合 POSIX 语义）
- 非重叠范围的写锁可共存
- 关闭文件时自动释放该进程持有的所有锁

### BSD flock

`MetaStore` trait 的 `set_flock` 和 `get_flock`：

- 整个文件粒度的劝告锁
- `LOCK_SH`（共享）/ `LOCK_EX`（排他）/ `LOCK_UN`（释放）
- `LOCK_NB`（非阻塞）通过 `block=false` 参数实现：冲突时立即返回错误

### Lock Conflict 处理

锁冲突时的行为：

- 阻塞模式下（`block=true`）：后端持续检查直到锁可用（通过轮询 + 退避）
- 非阻塞模式下（`block=false`）：立即返回 `MetaError::LockConflict`
- 死锁检测：`MetaError::DeadlockDetected`，携带涉及的 owner 列表

后端实现：
- **DatabaseMetaStore**：事务内 SELECT ... FOR UPDATE + 冲突检查
- **EtcdMetaStore**：etcd 事务 compare-and-swap
- **RedisMetaStore**：Lua 脚本原子检查 + 写入

## 全局锁

`MetaStore` trait 的全局锁接口用于需要跨节点排他的操作（如 Compaction）：

- `get_global_lock(LockName, ttl_secs) -> bool`
- `is_global_lock_held(LockName, ttl_secs) -> bool`
- `release_global_lock(LockName) -> bool`

支持的锁名称：
- `CleanupSessionsLock` — Session 清理
- `ChunkCompactLock(chunk_id)` — Chunk Compaction（详见 compaction doc）

全局锁使用 TTL 机制防止死锁：如果持锁进程崩溃，锁在 TTL 到期后自动释放。这与 Compaction Lock Manager 协同工作。

## Version + CAS 并发控制

`RedisMetaStore` 的 Chunk Slice List 修改使用版本号 CAS 方案（详见 `doc/architecture/redis-version-cas.md`）：

- 每个 Chunk 的 Slice 列表有一个 version key（`c{ino}_{idx}:v`）
- 任何修改操作（append/trim/replace）都必须递增 version
- 使用 Lua 脚本 `CHUNK_CAS_LUA` 做原子 CAS：仅当当前 version 匹配预期值时才执行修改
- 冲突时返回 `MetaError::ContinueRetry(VersionConflict)`，调用方退避重试

相比之前的 WATCH+MULTI+EXEC 方案，版本号 CAS：
- 不复用 WATCH 连接（每次重试不再需要新建 TCP 连接）
- 减少了网络往返次数（2 次 vs 3 次）
- 可在高冲突场景下保持稳定（不再有连接风暴）

## Rename 语义

`doc/architecture/rename_design.md` 详细记录。BrewFS 的 rename 遵循 POSIX rename(2) 语义：

- **原子性**：元数据层事务保证，（Database 使用 DB 事务，Etcd 使用 etcd 事务，Redis 使用 Lua 脚本）
- **RENAME_NOREPLACE**：目标存在时返回 EEXIST
- **RENAME_EXCHANGE**：原子交换两个文件
- **循环检测**：防止将目录 rename 到自身子目录中（基于 inode 的祖先链遍历）
- **同目录优化**：同目录 rename 走快速路径，避免重复解析父目录

### 硬链接与 Rename

rename 操作正确处理硬链接场景：
- `nlink = 1`：直接更新 `file_meta.parent` 字段
- `nlink > 1`：通过 `link_parent_meta` 表管理
- `rename_exchange` 在两个文件都有硬链接时，通过事务保证原子交换

## 权限检查

`src/meta/permission.rs`

当前权限模型为类 Unix 的 owner/group/other 三级权限（mode bits 0o777）。setuid/setgid/sticky 位被显式剥离。

权限在 VFS 层检查（open/write/create 等操作），再调用 MetaStore。MetaStore 层不重复检查，信任 VFS 授权。

## 当前限制

1. **无跨进程 cache coherence**：依赖 close-to-open + TTL 失效，无法做到实时一致性
2. **无强一致性写**：write 返回时数据可能还在内存（未 flush），进程崩溃可能丢失
3. **无分布式事务**：跨 chunk 的写入不能原子提交
4. **Compaction 期间非阻塞**：读路径可能读取正在被 compact 的 Chunk（依赖 MetaStore 的原子替换保证不读到中间态）
5. **Etcd/Redis 的 Truncate 并发**：已通过 version CAS 解决冲突风暴问题，但极端冲突下仍需多次重试
