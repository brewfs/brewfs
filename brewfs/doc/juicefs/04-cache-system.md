# JuiceFS 缓存体系

> 5 层缓存: Redis CSC → OpenFile → Memory Pages → Disk Cache → Prefetch

---

## Layer 1: Redis Client-Side Caching (meta/redis_csc.go)

```go
type redisCache struct {
    cli          *redis.Client
    cap          int                          // LRU capacity (默认 12800)
    expiry       time.Duration                // 默认 1min
    subscription *redis.PubSub                // "__redis__:invalidate"
    inodeCache   *expirable.LRU[Ino, []byte]  // i* key 缓存
    entryCache   *expirable.LRU[string, *cachedEntry]  // d* entry 缓存
    entryTerms   *expirable.LRU[Ino, uint64]  // 每目录 term 计数
}
```

### 三层缓存策略

**inodeCache**: 缓存 `i{inode}` 的原始 Attr 字节。GET 命中时直接返回，跳过 Redis。

**entryCache**: 缓存 `d{parent}` 的目录项。每条 `cachedEntry{ino, term, Attr}`。查找时比对 `entryTerms[parent]` term: `cachedEntry.term >= entryTerms[parent]` → 有效。

**entryTerms**: 当目录被修改时 `bumpEntryTerm()` 递增 term，批量失效该目录的所有缓存条目。

### 失效机制

三种途径联动:

1. **RESP3 PUSH**: 订阅 `__redis__:invalidate` 频道 → `HandlePushNotification()` → 解析 key 类型 → remove from cache / bumpEntryTerm

2. **Process Hooks**:
   - `beforeProcess`: 拦截 GET → cache hit 直接返回; miss → 插入 sentinel 防止重复请求
   - `afterProcess`: 拦截 SET/HSET/HDEL → 驱逐对应 cache entry

3. **Reconnection**: `onInvalidateConnect()` → 全量清空三个 cache → `CLIENT TRACKING ON BCAST PREFIX {prefix}i PREFIX {prefix}d`

### 初始化

`preloadCache()`: session 创建后, 如果 `client-cache-preload > 0`, 批量预取 root 的下 `N` 个 entry 到 entryCache。

---

## Layer 2: OpenFile Cache (meta/openfile.go)

```go
type openFile struct {
    sync.RWMutex
    attr      Attr                  // 缓存属性 (免 GetAttr)
    refs      int                   // 引用计数
    lastCheck int64                 // 最后 Revalidate 时间
    first     []Slice               // chunk 0 的切片
    chunks    map[uint32][]Slice    // chunk N 的切片
}

type openfiles struct {
    sync.Mutex
    expire time.Duration            // TTL
    limit  uint64                   // 软限制 (文件数)
    files  map[Ino]*openFile
}
```

### 对象池

```go
var ofPool = sync.Pool{New: func() any { return &openFile{} }}
```

`release()` → 清空所有字段 → `ofPool.Put(of)`。`Open()` 时从 pool 获取或创建。

### 操作

| 方法 | 行为 |
|------|------|
| `Open(ino, attr)` | 从 pool 获取/创建，mtime 匹配则 KeepCache，否则 InvalidateAllChunks |
| `OpenCheck(ino, attr)` | 未过期 → 复制 attr + refs++ + 返回 true |
| `Close(ino)` | refs--，返回 refs <= 0 |
| `Check(ino, attr)` | Revalidate: 检查过期但不增加 refs |
| `Update(ino, attr)` | mtime/mtimensec 变 → InvalidateAllChunks; 否则 KeepCache |
| `ReadChunk(ino, indx)` | 返回 chunks[indx] 或 first |
| `CacheChunk(ino, indx, cs)` | 缓存切片，懒初始化 chunks map |
| `InvalidateChunk(ino, indx)` | invalidateAllChunks → 清空 chunks + first; 单 indx → 删除 |

### 淘汰

后台 goroutine:
- `limit > 0 && len(files) > limit` → 计算 `todel = len - limit`
- 每周期最多扫 1000 条
- `refs <= 0`: lastCheck > 12h → 无条件删除; 否则按 lastCheck 最老的淘汰
- sleep 自适应: `1000 * (cnt+1 - deleted*2) / (cnt+1)` ms

---

## Layer 3: Memory Pages (chunk/mem_cache.go)

```go
map[string]memItem // {atime uint32, page *Page}
```

- CacheDir="memory" 或磁盘缓存不可用时降级到此
- 2-random 淘汰 (同 disk cache)
- pending pages: `cacheStore.pages[key]` — 尚未 flush 到磁盘, 读取时优先命中
- writeback: memory mode 不支持 staging

---

## Layer 4: Disk Cache (chunk/disk_cache.go)

### 缓存文件格式

```
┌───────────────────────┐
│ Raw Data (length B)   │
├───────────────────────┤
│ Checksums (可选)      │ 每 32KB 4 字节 CRC32C
├───────────────────────┤
│ TierID (可选, 1B)     │
└───────────────────────┘
```

**Checksum 级别**: `none` (无) / `full` (仅全读) / `shrink` (裁对齐) / `extend` (补对齐)

### 多目录路由 (cacheManager)

```go
type cacheManager struct {
    consistentMap *consistenthash.Map   // murmur3, 100 virtual nodes
    storeMap      map[string]*cacheStore
    stores        []*cacheStore
}
```

- 每个缓存目录 UUID lockfile 标识 → Equal share of total cache size
- Consistent hashing: `murmur3(key) → store`
- 故障自动摘除: store down → remove from hash ring → 后台探测恢复
- Fallback: 未命中 → FNV32 旧算法重试 (兼容性)

### 异步 Flush 管线

```
cache(key, page) → pages[key] → pending chan
                                      │
                                flush() goroutine:
                                  ├─ flushPage(path.tmp, data)
                                  │    ├─ write data
                                  │    ├─ write checksums (可选)
                                  │    ├─ write tierID (可选)
                                  │    └─ rename path.tmp → path
                                  └─ keys.add(key, size, atime)
```

### 淘汰策略 (cache_eviction.go)

KeyIndex 接口:

```go
type KeyIndex interface {
    add(key, item), remove(key), get(key), peekAtime(key)
    randomIter(), evictionIter()
}
```

| 策略 | 实现 | 特点 |
|------|------|------|
| `none` | `map[key]item` | 永不淘汰, 满则丢弃新 block |
| `2-random` (默认) | map + random iter | 随机抽 2, 淘汰 atime 老的; staging 永不淘汰 |
| `lru` | map + min-heap (atime) | 严格 LRU; staging block (size<0) 不入堆 |

**淘汰目标**: 将磁盘使用降到 `capacity × 95%`, 保证 `freeRatio ≥ 0.1`.

### 健康状态机

```
Normal ──(3次 IO 错误)──→ Unstable ──(30min)──→ Down
  ^                          │
  └──(60次成功)──────────────┘
```

- **Normal**: 全并发读写, 错误 counter 每 1min 重置
- **Unstable**: 限制并发到 10, 每 500ms 探测一次; 错误率 0% + ≥ 60 次成功 → Normal
- **Down**: 所有操作返回 `errCacheDown`; cache store 从 hash ring 移除

### Staging (Writeback) 流程

```
stage(key, data, tierID)
  ├─ flushPage() → 写入 rawstaging/ 目录
  ├─ hardlink(staging → cache dir)
  └─ keys.add(key, -size, stagedBlockCooldown)  // 负 size 标记

uploadStagingFile(key, path)
  ├─ read staging file → store.upload()
  └─ bcache.uploaded() → keys.add(key, +size) → remove staging

scanDelayedStaging()
  └─ [每 min(UploadDelay, 1m)] 扫描 pendingKeys → enqueue 到期项
```

---

## Layer 5: Prefetch Engine (chunk/prefetch.go)

```go
type prefetcher struct {
    parallel int         // 并发 goroutine (Config.Prefetch)
    pending  chan string
    busy     map[string]struct{}  // 去重
    store    *cachedStore
}
```

- `loadRange()` 成功后 → `fetcher.fetch(key)` 异步预取完整 block
- `busy` map 防止同 key 重复入队
- 预取 block 通过 `store.load() → bcache.cache()` 自动缓存到磁盘

---

## 缓存查找优先级

```
read request:
  1. cacheStore.pages[key]          ← 内存 pending (L3)
  2. cacheStore.keys.get(key)       ← KeyIndex 查找 (L4, 仅 scan 后有效)
  3. openCacheFile(path)            ← 打开磁盘文件 + 校验 checksum (L4)
  4. (miss) → loadRange / group.Execute → S3

metadata request (GetAttr):
  1. openfiles.Check(ino)           ← OpenFile cache (L2)
  2. redisCache.inodeCache.Get      ← CSC inode cache (L1)
  3. (miss) → Redis GET i{inode}
```

## 缓存失效传播

```
属性变化 (Write/SetAttr/Truncate):
  redis: WATCH i{ino} → SET → CSC push → 所有客户端 L1 cache 失效
  local: openfiles.Update() → mtime 变 → InvalidateAllChunks
  local: VFS.InvalidateAttr() → modifiedAt[ino] = now

目录变化 (Create/Unlink/Rename/Mkdir/Rmdir):
  redis: HSET/DEL d{parent} → CSC push → bumpEntryTerm() → L1 entry cache 失效
  local: FUSE kernel → forget entry cache (entry_timeout)
```
