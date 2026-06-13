# JuiceFS 写路径

> 基于 `vfs/writer.go`, `chunk/cached_store.go`, `meta/redis.go`

## 写缓冲层级

```
fileWriter (per inode)
  │
  ├─ chunkWriter (per 64MB chunk)
  │     │
  │     ├─ sliceWriter (per 连续写入段)
  │     │     │
  │     │     └─ wSlice (chunk/cached_store.go:238)
  │     │           ├─ pages[][]*Page         64KB 子页, 按 4MB block 分组
  │     │           ├─ uploaded int           已上传 watermark
  │     │           └─ errors chan error      每 block 异步结果
  │     │
  │     └─ commitThread goroutine            按 slice 创建顺序串行提交
  │
  └─ writecond / flushcond / commitcond      条件变量协调
```

## 完整调用链

```
FUSE: VFS.Write(ino, off, buf, fh)       [vfs/vfs.go:801]
│
├─ handle.Wlock()                         悲观写锁
└─ fileWriter.Write(ctx, off, buf)        [vfs/writer.go:330]
     │
     ├─ 反压: pending slices > 1000 → 阻塞; bufferUsed > BufferSize → sleep
     ├─ 等待 flush 完成 (flushwaiting > 0)
     ├─ 按 64MB chunk 边界拆分
     └─ writeChunk(chunkWriter, buf, off)
          │
          ├─ findWritableSlice(pos, size)   反向搜索可写 slice
          ├─ sliceWriter.write(buf)         写入 wSlice
          └─ [首 slice] 启动 commitThread goroutine
               │
               ▼
          wSlice.WriteAt(buf, off)         [cached_store.go:267]
          │  写入 64KB 子页 → 按 block 分组
          │  block 满 → FlushTo()
          │  slice 达 64MB → 自动 freeze
          │
          ▼ [触发上传]
          wSlice.upload(indx)               [cached_store.go:400]
          │  合并 pages → 判断 writeback → stage/put
          │
          ▼ [上传完成]
          commitThread → meta.Write()        Redis txn → RPUSH slice
```

## 上传触发条件 (多路径)

| 条件 | 动作 | 触发者 |
|------|------|--------|
| block 满 (4MB) | `FlushTo(off)` → 异步 goroutine 上传 | `sliceWriter.write()` |
| slice 达 64MB (ChunkSize) | 自动 freeze + `flushData()` | `sliceWriter.write()` |
| idle > 1s 且 age > 1s | `flushAll()` 冻结 | dataWriter goroutine (每 100ms) |
| age > 5s (flushDuration) | `flushAll()` 强制冻结 | dataWriter goroutine |
| file slices > 800 | 标记一半 chunk 刷新 | `flushAll()` |
| file slices > 1000 | **阻塞** `Write()` 等待提交 | `fileWriter.Write()` |
| Flush / Fsync / Close | 全部冻结 + 等待 commitThread | 用户显式 |

## 上传流程 (wSlice.upload, cached_store.go:400)

```
1. 合并 block 的 64KB 子页为单个 Page

2a. Writeback 模式 && blen < WritebackThresholdSize:
    ├─ bcache.stage(key, data, tierID)    写本地磁盘 staging file
    ├─ hardlink(staging → cache dir)       双链 (供读取)
    ├─ keys.add(key, -size)                负 size 标记为 staging
    ├─ UploadDelay == 0 → 立即上传 → bcache.uploaded()
    └─ UploadDelay > 0 → addDelayedStaging() → delay 到期 → pendingCh

2b. 直传 (非 writeback 或 block 太大):
    ├─ acquire currentUpload slot (MaxUpload 并发上限)
    └─ store.upload(key, block)
         ├─ compress (LZ4/Zstd)
         ├─ 重试: sync → MaxRetries+1, async → 3
         ├─ 退避: sleep(try²) 秒
         └─ PUT to S3 (超时 = PutTimeout, 默认 60s)
```

## commitThread — 顺序提交 (writer.go:186)

每个 chunk 一个串行 goroutine, 按 slice 创建顺序处理:

```go
for s in slices:
    wait s.done (100ms timeout; 超时 → freeze)
    if s.growing: wait dep.committed     // 跨 chunk 依赖
    meta.Write(ino, indx, off, slice, mtime) → redis txn
    reader.Invalidate(ino, off, size)    // reader cache 失效
    s.committed = true
```

**跨 chunk 依赖**: 如果 slice 正在扩展文件 (growing=true), 它的 `dep` = 上一个 chunk 的最后一个 slice. commitThread 会等待 `dep.committed == true` 才提交, 保证 chunk 顺序。

## 显式 Flush (fileWriter.flush, writer.go:386)

```
1. flushwaiting++ → 阻塞新 Write
2. Freeze 所有非冻结 slice → launch flushData() each
3. Wait flushcond (3s timeout per cycle)
4. Deadline = max(5min, (maxRetries+2)²/2) jitter
5. Timeout → dump goroutine stacks + return EIO
```

## 周期性 Flush (dataWriter.flushAll, writer.go:485)

每 100ms 执行:
1. 遍历所有有活跃引用的 fileWriter
2. `totalSlices() > 800` → 标记 `indx % 2 == 0` 的 chunk 刷新 (随机一半)
3. Freeze ages > `flushDuration` (5s) 的 slices
4. Freeze idle > 1s 且 started > 1s ago 的 slices

## 反压机制汇总

| 条件 | 措施 |
|------|------|
| pending slices ≥ 1000 / file | 阻塞 Write() (1ms spin) |
| bufferUsed > BufferSize | sleep 10ms |
| bufferUsed > BufferSize × 2 | sleep 100ms |
| currentUpload channel 满 | 阻塞 upload goroutine |
| currentDownload channel 满 | 阻塞 download goroutine |

## Writeback (写回) 模式

启用方式: `--writeback` + `--writeback-threshold-size` (默认 0 = all blocks).

流程:
```
Write → stage to local disk → flushPage() 
     → hardlink to cache dir → addDelayedStaging()
     → [UploadDelay timeout] → uploadStagingFile()
     → read staging → upload() → bcache.uploaded()
```

延迟上传通过:
- `UploadDelay`: 最短延迟时间
- `UploadHours`: 限制上传时间窗口 (如夜间批处理)
- `stagedBlockCooldown = CacheExpire/2`: 延后 atime 防止 upload 风暴淘汰缓存

崩溃恢复:
- `scanStaging()` 在启动时扫描 staging 目录 → 重新入队上传
- staging 文件丢失 → 数据丢失 (writeback 的 trade-off)
