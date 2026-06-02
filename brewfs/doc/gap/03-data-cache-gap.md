# 数据路径与缓存差异

## 对照对象

- BrewFS：`src/chunk/*`、`src/vfs/io/*`、`src/vfs/cache/*`、`src/cadapter/*`
- JuiceFS：`juicefs/pkg/chunk/*`、`juicefs/pkg/vfs/{reader,writer}.go`、`juicefs/pkg/object/*`、`juicefs/pkg/compress/*`

## 共同基础

两边都采用 JuiceFS 风格的数据布局：

```text
file -> chunk (默认 64MiB) -> slice -> block/object
```

BrewFS 当前默认 block 为 4MiB，并用 `SliceDesc { slice_id, chunk_id, offset, length }` 描述一次写入在 chunk 内的逻辑范围。对象 key 当前是：

```text
chunks/{slice_id}/{block_index}
```

JuiceFS 的 object key 会结合 slice id、block index、block size、hash prefix 等 format 配置组织。

## 写路径差异

### BrewFS 当前写路径

`src/vfs/io/writer.rs` 的模型是：

```text
write_at
  -> split by chunk
  -> append into SliceState(Writable)
  -> auto_flush / flush freezes slice
  -> DataUploader uploads blocks
  -> commit_chunk appends SliceDesc to meta
  -> Committed slices visible to readers
```

优点：

- 状态机清晰，利于故障注入。
- copy-on-write，不需要覆盖旧对象。
- vectored upload 避免不必要拼接。
- 全局上传 semaphore 防止对象存储连接爆炸。

风险：

- `write()`、`flush()`、`fsync()`、`close()` 的可见性与持久性边界必须严格定义。
- 后台 commit 失败如何传播给用户仍是关键。
- size 先于 data commit 被其他客户端看到时，可能读到洞或旧数据。
- truncate 与 in-flight write 的排序需要强测试。

### JuiceFS 写路径成熟点

JuiceFS `pkg/chunk/cached_store.go` 和 `pkg/vfs/writer.go` 已长期处理：

- page buffer 与 page pool
- writeback 模式
- 上传延迟、上传窗口、限速
- cache-after-write
- 对象 key 的 hash prefix 和 block size 编码
- error/backpressure/metrics

提升方向：

- 将 BrewFS 写入语义整理成明确状态表：返回给用户前至少满足什么条件。
- 增加 writeback 与非 writeback 的用户可见差异文档。
- fsync 必须等对象上传和元数据 commit 都成功，或返回错误。
- close 不能吞掉之前的异步写错误。
- 给 slice 状态机加 crash recovery 测试。

## 读路径差异

### BrewFS 当前读路径

`src/vfs/io/reader.rs` 与 `src/chunk/reader.rs` 负责：

- 按 chunk 拆分读取范围。
- 加载 slice 列表。
- 最新 slice 覆盖旧 slice。
- 未覆盖区域零填充。
- per-handle session 预测 readahead。
- reader cache 可被 writer commit 后局部失效。

`ObjectBlockStore` 还有：

- block cache
- 64KiB page cache
- 小范围 range read
- 大范围 singleflight full block read

这些设计方向合理，尤其适合随机小读与并发读合并。

### JuiceFS 读路径成熟点

JuiceFS 在 chunk 层支持：

- disk cache 状态管理
- memory cache
- prefetch
- cache eviction/check/fill
- cache hit/miss metrics
- seekable object range read
- OS cache 控制

提升方向：

- 将 BrewFS `ChunksCache`、page cache、prefetch 的配置暴露到 mount/config。
- 增加 `cache status`、`warmup`、`evict`、`check-cache` 等控制命令。
- reader cache 绑定 inode/chunk version，解决跨客户端旧数据。
- 为 range read、full block read、cache hit/miss 增加 metrics。

## 对象存储差异

### BrewFS

`ObjectBackend` 当前抽象简洁：

- `put_object`
- `put_object_vectored`
- `get_object`
- `get_object_range`
- `get_etag`
- `delete_object`

实现主要是 LocalFS 与 S3。

### JuiceFS

`ObjectStorage` 包含生产对象存储所需的完整面：

- Create bucket
- Get/Put/Copy/Delete
- Head/List/ListAll
- Multipart create/upload/copy/abort/complete/list
- Restore archived object
- Storage class、prefix、sharding、encryption wrapper
- 大量云厂商后端

提升方向：

- 将 `ObjectBackend` 分成最小读写接口和高级能力接口。
- 支持 multipart 生命周期，包括 abort/list pending uploads。
- 支持 object list，用于 GC/fsck 对账。
- 支持 storage class、restore、server-side options。
- 增加 object backend conformance test，LocalFS/S3/RustFS/MinIO 必须同测。

## 压缩与加密差异

JuiceFS 有 format 级 compression 和 object encryption。BrewFS 当前未看到完整数据面压缩/加密闭环。

提升方向：

- 在 format 中记录 compression/encryption，不允许挂载时随意改变。
- 压缩粒度建议与 object block 对齐，避免破坏 range read。
- 加密需要设计 nonce/key rotation/checksum，并明确 ETag 不再等价于明文 hash。
- 先实现 `none/zstd` 与 `none/aes-gcm`，再扩展。

## compact/gc 差异

### BrewFS 当前亮点

`src/chunk/compact` 已有：

- light compaction：删除完全被覆盖的旧 slice metadata。
- heavy compaction：读取并合并 chunk 数据，生成新 slice。
- delayed slice：元数据先删，数据延迟 GC。
- uncommitted slice：heavy compact 失败后的清理记录。
- local + global chunk lock，TTL 防止跨节点并发 compact。

这是一个很好的基础。

### 与 JuiceFS 的差距

JuiceFS 的 compact/gc 已和 CLI、status、fsck、object listing、trash/session 清理结合得更完整。BrewFS 目前更像“后台机制已存在，但产品入口和可观测性不足”。

提升方向：

- `brewfs compact <path>`：手动触发并展示进度。
- `brewfs gc --dry-run/--delete`：列出 orphan slice/object 明细。
- `brewfs fsck`：对账 meta slices 与 object blocks。
- compaction 指标：候选 chunk 数、slice 数、碎片率、重写字节、失败原因。
- compact 过程增加读写并发冲突测试和 crash 测试。

## 数据路径验收建议

- 小写放大：4KiB 随机写、fsync、覆盖写、truncate 后空间回收。
- 大文件吞吐：顺序写、顺序读、并发读写。
- 随机读缓存：page cache、block cache、singleflight 命中率。
- 故障注入：对象上传失败、元数据 commit 失败、进程 kill、对象删除失败。
- 跨客户端：writer close 后 reader reopen 可见；writer 未 fsync 时语义明确。

