# JuiceFS 事务引擎

> 基于 `meta/redis.go` (6249 行), `meta/base.go`

## txn() — 两阶段并发控制 (redis.go:1135)

```go
func (m *redisMeta) txn(ctx Context, txf func(tx *redis.Tx) error, keys ...string) error
```

### Phase 1 — 悲观互斥 (防惊群)

```go
h := FNV32(keys[0]) % 1024
m.txlocks[h].Lock()
defer m.txlocks[h].Unlock()
```

1024 个 mutex 按第一个 key 哈希分配。同一 inode 的所有事务 (WATCH 同一个 `i{inode}`) 串行化到同一个锁，避免并发 WATCH 冲突导致的无效重试。

**设计要点**: 这是本地进程内锁，用于减少 Redis 端冲突，不是分布式锁。跨客户端的并发仍然由 Redis WATCH 保证。

### Phase 2 — 乐观重试循环

```
for i := 0; i < maxRetry (默认 50); i++:
    if ctx.Canceled → return EINTR
    err = rdb.Watch(ctx, replaceErrno(txf), keys...)
    if err == nil → return nil
    if !shouldRetry(err, retryOnFailure) → return err
    sleep(rand(0, (i+1)²) ms)          // 二次方抖动
return lastErr
```

### shouldRetry() 错误分类 (line 1076)

```
始终重试:
  redis.TxFailedErr        ← WATCH 冲突，最常见
  ERR max number of clients
  EXECABORT, bad state

条件重试 (仅 retryOnFailure=true):
  io.EOF, io.ErrUnexpectedEOF
  timeout (dial/read/write)

集群错误 (始终重试):
  LOADING, READONLY, CLUSTERDOWN, TRYAGAIN
  MOVED, ASK (slot migration)
  ERR DISABLE/NOWRITE/NOREAD

永不重试:
  nil, context.Canceled, context.DeadlineExceeded
```

### replaceErrno() (line 1125)

```go
func replaceErrno(txf func(tx *redis.Tx) error) func(tx *redis.Tx) error {
    return func(tx *redis.Tx) error {
        err := txf(tx)
        if eno, ok := err.(syscall.Errno); ok {
            return errNo(eno)       // uintptr 类型，绕过 redis 的 transient 检测
        }
        return err
    }
}
```

将业务错误 `syscall.Errno` 转为 `errNo` 防止 Redis driver 将 `ENOENT`, `ENOSPC` 等业务错误当作瞬时 Watch 失败而自动重试。

---

## 关键事务实例

### doWrite — 写入切片 (line 3119)

文件扩展 (新写入了切片):

```lua
WATCH inodeKey(ino)

-- 1. GET i{ino} → parse attr
-- 2. 检查 TypeFile, FlagImmutable, FlagAppend
-- 3. 计算 newLength 和 delta(space)
-- 4. checkQuota() → 遍历父目录链检查配额

TxPipelined:
    RPUSH   c{ino}_{indx}  marshalSlice(pos, id, size, off, len)
    SET     i{ino}         marshal(updatedAttr)          -- new length, mtime, ctime
    INCRBY  usedSpace       delta.space                  -- 文件系统总使用空间
    RPUSH   txnLog          "WRITE(ino,indx,id,off,len)"
```

如果 `numSlices % 100 == 99` 或 `> 350` 或 `≥ 2500`，触发压缩检查。

### doMknod — 创建文件 (line 1446)

```lua
WATCH inodeKey(parent), entryKey(parent)

-- 1. GET i{parent} → parse pattr
-- 2. HGET d{parent} name → 检查重名

TxPipelined:
    SET    i{newIno}    marshal(newAttr)            -- 新 inode 属性
    SET    i{parent}    marshal(updatedPAttr)       -- 父 nlink/mtime
    HSET   d{parent}    name  packEntry(type, newIno)  -- 目录项
    INCRBY usedSpace     delta
    INCR   totalInodes
```

**WATCH 两个 key**: 保证父属性 + 目录项的一致性。如果并发创建了同名文件，`i{parent}` 或 `d{parent}` 之一会变化 → EXEC 失败 → 重试 → HGET 检测到重名 → 返回 EEXIST。

### doCompactChunk — CAS 压缩 (line 3774)

```lua
WATCH chunkKey(ino, indx)

-- 1. LRANGE chunkKey 0 N-1 (N = len(origin)/24)
-- 2. 逐字节比对 origin (CAS check)
--    → 不匹配 → return EINVAL (chunk 被修改了)

TxPipelined:
    LTRIM    c{ino}_{indx}  N  -1                 -- 删除前 N 条
    LPUSH    c{ino}_{indx}  marshalSlice(compacted) -- 插入合并结果
    LPUSH    c{ino}_{indx}  marshalSlice(skipped...) -- 略过的切片放回
    HINCRBY  sliceRefs      k{oldId_1}_{oldSize_1}  -1
    HINCRBY  sliceRefs      k{oldId_2}_{oldSize_2}  -1
    ...
    HSET     sliceRefs      k{newId}_{newSize}       0
```

双保险: 即使返回错误，如果 `sliceRefs` 中已有新 slice → 视为成功。ref < 0 的旧 slice → `deleteSlice()` 异步删除。

### doRename — 重命名 (line 2236)

```lua
WATCH inodeKey(sp), entryKey(sp), inodeKey(dp), entryKey(dp)

-- 获取源/目标属性，检查权限，增加交换逻辑
TxPipelined:
    HSET  d{dp}  newname  packEntry(type,ino)
    HDEL  d{sp}  oldname
    SET   i{sp}  updated_spattr
    SET   i{dp}  updated_dpattr
    ... (nlink/parent 调整)
```

### doUnlink — 删除 (line 1923)

```lua
WATCH inodeKey(parent), entryKey(parent), inodeKey(ino)

-- 获取父属性 + 条目 + 文件属性

TxPipelined:
    HDEL  d{parent}  name                    -- 删除目录项
    SET   i{parent}  updated_pattr           -- 更新父 mtime/nlink
    INCRBY usedSpace  -delta.space           -- 减去使用空间
    DECR  totalInodes                        -- 减少 inode 计数
    [如果 trash 启用]:
        SET  i{ino}  updated_attr (nlink=0, parent=TrashInode)
        ZADD delfiles  {ino}:{length}  expire_ts  -- 延迟删除
    [否则]:
        DEL  i{ino}                          -- 立即删除 inode
```

---

## 原子性边界

| 操作 | 原子? | 机制 | 崩溃场景 |
|------|-------|------|---------|
| 单 slice 提交 | ✅ | Redis WATCH/EXEC | — |
| 跨 chunk 写入 | ❌ | 各 chunk 独立提交 | 部分可见 |
| 单 block PUT | ✅ | S3 PUT 语义 | — |
| 数据 + 元数据 | ❌ 最终一致 | data-first, metadata-later | 孤儿 block → sliceRefs GC |
| Writeback staging | ❌ | 本地 disk → async upload | staging 丢失 = 数据丢失 |
| 压缩替换 | ✅ CAS | LTRIM+LPUSH WATCH | chunk 变化 → EINVAL → 重试 |

### 崩溃恢复机制

**数据已上传，元数据未提交** (直传模式):
- S3 上的 block 成为孤儿垃圾
- 后续 `compactChunk()` → `sliceRefs` 引用计数发现 ref < 0
- `CleanupSlices` goroutine (每 ~1h) → `DeleteObject`

**元数据已提交，数据未上传** (Writeback 模式):
- 本地 staging 文件存在但未 upload
- 重启 → `scanStaging()` 重新入队
- staging 文件丢失 → 数据丢失 (writeback trade-off)

**会话崩溃**:
- Session heartbeat 停止 → 5× Heartbeat 后过期
- 其他客户端 `CleanStaleSessions()` 清理 sustained inodes + locks

---

## Quota 配额检查 (quota.go)

```go
type Quota struct {
    MaxSpace, MaxInodes   int64
    UsedSpace, UsedInodes int64
    newSpace, newInodes   int64  // 未刷盘的增量 (原子操作)
}
```

检查链: `user quota → group quota → format-level capacity → dir chain quota`

事务中调用 `checkQuota()`:
- 如果配额超限 → 返回 `ENOSPC` / `EDQUOT`
- 配额状态通过 `flushQuotas()` goroutine (每 3s) 从 Redis 刷新

---

## Session 管理 (base.go)

```go
NewSession():
  sessCtx ← context
  refresh() goroutine (每 12s):
    ZADD allSessions sid expireTime       // heartbeat
    HGETALL sessionInfos → reload format
    check UUID, reload quotas
    CleanStaleSessions()                  // ZRangeByScore max=now

  flushStats()     goroutine (每 1s)
  flushDirStat()   goroutine (每 1s)
  flushQuotas()    goroutine (每 3s)
  cleanupDeletedFiles() goroutine (每 ~1h)
  cleanupSlices()  goroutine (每 ~1h)
  cleanupTrash()   goroutine (每 ~1h)
```

Session 过期: `expireTime = now + 5 × Heartbeat` (默认 12s → 60s).
