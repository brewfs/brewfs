# JuiceFS 读路径

> 基于 `vfs/vfs.go`, `vfs/reader.go`, `chunk/cached_store.go`, `chunk/disk_cache.go`

## 完整调用链

```
FUSE: VFS.Read(ino, off, buf, fh)           [vfs/vfs.go:693]
│
├─ handle.Rlock()                            等待写锁释放
├─ writer.Flush(ctx, ino)                    保证读己之所写 (reader.go:789)
└─ fileReader.Read(ctx, off, buf)            [vfs/reader.go:626]
     │
     ├─ 缓冲压力检查:                         bufferUsed > BufferSize → sleep 10ms
     │                                        bufferUsed > 2× → sleep 100ms
     ├─ cleanupRequests(block)               清理 BREAK/INVALID/30s 超时 sliceReader
     ├─ tail readahead:                      预取文件末尾 32KB
     ├─ splitRange(block)                    对齐到已有/进行中 slice 边界
     ├─ prepareRequests(ranges)              复用已有或创建新 sliceReader
     ├─ checkReadahead(block)                调整预读窗口 (翻倍/减半)
     └─ waitForIO(requests, buf)             阻塞等待所有 slice 状态 → READY
          │
          └─ [每个 slice goroutine] sliceReader.run()   [reader.go:162]
               │
               ├─ meta.Read(ino, indx, &slices)
               │    ├─ openfiles.ReadChunk()           L2 cache hit → 免 Redis
               │    └─ redisMeta.doRead()               LRange c{ino}_{indx}
               │
               └─ dataReader.Read(page, slices)         [reader.go:840]
                    │
                    └─ rSlice.ReadAt(page, off)         [cached_store.go:97]
                         │
                         ├─ ❶ bcache.load(key)          磁盘缓存查找
                         ├─ ❷ loadRange()               局部读取 (seekable + ≤25% block)
                         └─ ❸ group.Execute()           singleflight 全 block 下载
                              └─ store.load() → storage.Get() (S3)
```

## 步骤详解

### ① VFS.Read (vfs.go:693)

1. **特殊节点处理**: 如果是 `.accesslog`, `.stats`, `.control`, `.config` (ino ≥ `0x7FFFFFFF00000000`)，直接从 handle 的 in-memory buffer 读取返回。

2. **Recovery 检查**: 如果 handle 有 `O_RECOVERED` flag (crash 恢复)，重新调用 `Meta.Open()` 获取 reader。

3. **文件大小限制**: 强制 `maxFileSize = ChunkSize << 31 = 128 GiB` 硬限制。

4. **锁定**: `handle.Rlock()` — 等待 `writing == 0 && writers == 0`，写优先 (有 waiting writer 时读阻塞)。

5. **刷脏**: `v.writer.Flush(ctx, ino)` — 保证写缓冲中的数据对读可见。

6. **委托**: `h.reader.Read(ctx, off, buf)` — 进入 fileReader。

### ② handle 读写锁 (handle.go:102-149)

```go
Rlock():  while writing || writers > 0 { cond.Wait() }; readers++
Runlock(): readers--; if readers == 0 { cond.Broadcast() }
Wlock():  writers++; while readers > 0 || writing > 0 { cond.Wait() }; writers--; writing = 1
Wunlock(): writing = 0; cond.Broadcast()
```

写优先: 有 waiting writer 时新读者阻塞。保证 close/flush 不被长期运行的读饥饿。

### ③ fileReader.Read (reader.go:626)

**cleanupRequests** (line 463): 遍历 linked list，丢弃:
- 状态为 BREAK 或 INVALID 的
- 不在当前读取范围内且 30s 未访问的
- 总数超过 `maxRequests` (~160) 时丢最老的

**splitRange** (line 501): 将请求的字节范围在已有 sliceReader 的边界处拆分，产生对齐的子范围列表。

**prepareRequests** (line 561): 每个子范围:
- 如果已有 sliceReader 完全覆盖 → 增量引用计数 + 复用
- 否则 → 创建新 sliceReader → 启动 goroutine `sliceReader.run()`

**waitForIO** (line 592): 每个 sliceReader，1s timeout + 取消检查，等待 `state == READY`，然后从 page 拷贝数据到输出 buf。

### ④ sliceReader 状态机 (reader.go:35-50)

```
      NEW ──→ BUSY ──→ READY
       ↑        │        ↓
     REFRESH  BREAK → INVALID
```

- `NEW`: 初始。或从失败恢复后被 reset 为 NEW。
- `BUSY`: 正在读 meta 和 data。
- `READY`: 数据就绪可消费。
- `REFRESH`: 缓存失效，需要重新读取。
- `BREAK`: 被放弃 (无引用)，将变为 INVALID。
- `INVALID`: 已从 linked list 移除。

### ⑤ sliceReader.run (reader.go:162)

goroutine per slice:

```
1. 状态 → BUSY
2. meta.Read(ino, indx, &slices)  → 获取切片元数据 (openfile cache / Redis LRange)
3. dataReader.Read(page, slices)   → 读取数据
4. 状态 → READY, cond.Signal()
```

失败时: `m.InvalidateChunkCache()` + 指数退避重试 (最多 `maxRetries`, 默认 50).

### ⑥ rSlice.ReadAt (cached_store.go:97)

```
if offset >= length → EOF
if 跨 block 边界 → 递归各 block

❶ cache hit: bcache.load(key)
    ├─ pages map (内存 pending)
    └─ KeyIndex.get(key) → openCacheFile() → CRC32C verify

❷ partial read: seekable && offset>0 && len≤blockSize/4
    └─ store.loadRange(key, page, off) → HTTP Range GET
         └─ 成功 → prefetcher.fetch(key) 异步预取完整 block

❸ full block: group.Execute(key, fn) → singleflight 去重
    └─ store.load(key, page, cache=true)
         ├─ acquire download slot (MaxDownload 并发)
         ├─ storage.Get(key) → S3 GET
         ├─ decompress (LZ4/Zstd)
         └─ bcache.cache(key, page) → async write to disk cache
```

**缓存决策**: `shouldCache(size)` = `CacheFullBlock || size < BlockSize`

---

## 自适应预读 (Adaptive Readahead)

`fileReader` 维护 2 个 session slot (reader.go:277):

```go
type session struct {
    lastOffset, total, readahead uint64
    atime time.Time
}
```

| 条件 | 动作 |
|------|------|
| 连续读量 ≥ 当前窗口 | **翻倍** `readahead` (上限 `readAheadMax = 80% × BufferSize`) |
| 随机访问 (< 25% 窗口) 或缓冲压力 | **减半** |
| 起始 | `= blockSize` (4MB) |
| 缓冲使用 > BufferSize × 2 | **sleep 100ms 强力反压** |

`checkReadahead()` 在每个 Read 末尾调用。`readAhead()` 为预读窗口内的后续 block 创建 sliceReader (跳过已覆盖的)。

---

## Singleflight (chunk/singleflight.go)

```go
Controller.Execute(key, fn):
  if key in progress:
    wait for existing goroutine (WaitGroup) → return cached result
  else:
    run fn → cache result → broadcast to all waiters

Controller.TryPiggyback(key):
  non-blocking: join existing if in progress, else return nil
```

用于 rSlice.ReadAt 的全 block 下载: 多个并发读同一 block 时只真正下载一次。`loadRange()` 也用它复用已有的全量下载结果。
