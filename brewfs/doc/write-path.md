# 写路径

写路径将用户数据从 FUSE 层传输到对象存储，经过缓冲、分片、上传、元数据提交四个阶段。

源码分布：
- `src/vfs/io/writer.rs` — `FileWriter`，Slice 状态机
- `src/chunk/writer.rs` — `DataUploader`，Block 并发上传
- `src/chunk/store.rs` — `ObjectBlockStore`，对象存储写入
- `src/meta/layer.rs` — `MetaLayer`，元数据提交

## 整体流程

```
FUSE write(ino, offset, data)
  │
  ▼
VFS::write() → FileHandle::write_at(offset, data)
  │
  ├─ split_chunk_spans(layout, offset, len) → Vec<ChunkSpan>
  │
  └─ 对每个 ChunkSpan:
       │
       ├─ FileWriter::write_chunk_span(ino, chunk_index, offset_in_chunk, data)
       │    └─ SliceState 追加数据（Writable 状态）
       │
       ├─ auto_flush 定时器触发（dirty_slice_max_age_ms 到期）
       │    或手动 flush/fsync 触发
       │    或 dirty_slice_target_size 达到阈值触发
       │    └─ spawn_flush_slice(ino, chunk_index)
       │
       ├─ SliceState 状态转换:
       │    Writable → Readonly → Uploading
       │
       ├─ DataUploader::write_at_vectored(slice_id, offset_in_chunk, data)
       │    ├─ block_span_iter_slice(offset, len, layout) → Vec<BlockSpan>
       │    └─ join_all: 每个 BlockSpan 并发
       │         BlockStore::write_fresh_vectored(
       │           key = (slice_id, block_index),
       │           offset = 0,  // 相对于 Block 起始
       │           data = payload
       │         )
       │
       ├─ SliceState: Uploaded
       │
       └─ commit_chunk(ino, chunk_index)
            └─ MetaLayer::append_slice(chunk_id, SliceDesc { slice_id, chunk_id, offset, length })
            └─ SliceState: Committed → 对后续读可见
```

## FileWriter

`src/vfs/io/writer.rs`

`FileWriter` 是每个 VFS 实例的写管理器，内部维护：

- `chunks: DashMap<u64, ChunkWriter>` — 每个 Chunk 一个 `ChunkWriter`
- `write_gen: AtomicU64` — 写代数计数器，每次 write 递增，用于 reader 判断是否有新数据
- `memory: Arc<MemoryBudget>` — 全局内存预算

### ChunkWriter 和 SliceState

每个 `ChunkWriter` 包含一个 `SliceState` 链表。`SliceState` 是一个状态机：

```
Writable ──(flush触发)──→ Readonly ──(上传开始)──→ Uploading ──(上传完成)──→ Uploaded ──(提交完成)──→ Committed
```

- **Writable**：当前接收写入的 Slice，数据积累在内存缓冲区
- **Readonly**：已被冻结，等待上传。内部数据包装为 `Arc<Bytes>`，可被 reader 读取（overlay 读取：在 slice 提交前即可读到未提交数据）
- **Uploading**：正在上传到对象存储
- **Uploaded**：上传完成，等待元数据提交
- **Committed**：元数据已写入 MetaStore，对所有后续读取可见

### auto_flush 触发条件

三个条件任满足其一即触发 flush：

1. **时间到达**：当前 writable slice 的存在时间超过 `dirty_slice_max_age_ms`（默认 500ms）
2. **大小到达**：当前 writable slice 的累积数据量超过 `dirty_slice_target_size`（默认 64 MiB）
3. **手动触发**：用户调用 `fsync` / `close`，或 FileReader 读前需要 flush

### 背压机制

防止写入速度超过上传速度导致内存暴涨：

- 每个 Chunk 最多 3 个未提交 Slice（`MAX_UNFLUSHED_SLICES`），超过后新写入等待
- 单文件总 Slice 数超过 800（`MAX_SLICES_THRESHOLD`）后触发等待
- 全局内存预算（`MemoryBudget`）超过 80% 时触发 force flush

## DataUploader

`src/chunk/writer.rs`

`DataUploader` 接收一个 Slice 的完整 payload（`Bytes`），将其按 Block 边界拆分后并发上传：

```rust
pub async fn write_at_vectored(
    &self,
    slice_id: u64,
    offset: u64,    // Slice 在 Chunk 内的偏移
    payload: Bytes,
    layout: &ChunkLayout,
) -> Result<()>
```

内部流程：

1. `block_span_iter_slice(0, payload.len(), layout)` 迭代产生 `BlockSpan` 列表
2. 对每个 `BlockSpan`：提取对应的 `payload.slice(span_offset..span_offset+span_len)`
3. `join_all` 并发调用 `BlockStore::write_fresh_vectored(key, offset_in_block, data)`
4. 如果启用了压缩（Compression::Lz4 或 Zstd），在上传前压缩，并修改 key 后缀（如 `.lz4`）

### 全块写入优化

当 Slice 恰好覆盖整个 Block 时（即 `offset_in_block == 0 && len == block_size`），走 `write_fresh` 路径，直接将整个 Bytes 对象写入缓存（零拷贝），避免在 `write_fresh_vectored` 路径中的 `flat_map().collect()` 拷贝。

## BlockStore 写入

`src/chunk/store.rs` — `ObjectBlockStore`

`write_fresh_vectored(key=(slice_id, block_index), offset, data)` 路径：

1. 如果启用了 ChunksCache，将数据写入磁盘冷缓存（异步）
2. 调用 `ObjectClient::put(key, data)` 上传到对象存储
3. 写入成功后，如果满足晋升条件（访问频率达标），从冷缓存晋升到热缓存（内存）

`ObjectClient::put` 通过 cadapter 层路由到具体后端：

- **LocalFs**：`tokio::fs::write(path, data)`
- **S3**：AWS SDK `PutObject`，支持 multipart upload（通过 `part_size` 控制分片大小）

## 元数据提交

上传完成后，`commit_chunk` 将 `SliceDesc` 写入 MetaStore：

```rust
MetaLayer::append_slice(chunk_id, SliceDesc {
    slice_id,
    chunk_id,
    offset,    // Chunk 内偏移
    length,
})
```

不同后端的实现：

- **DatabaseMetaStore**：INSERT INTO `slice_meta` 并通过事务同时更新 `file_meta.size`
- **EtcdMetaStore**：通过 etcd 事务原子追加到 chunk 的 slice 列表，同时递增 version
- **RedisMetaStore**：使用 Lua CAS 脚本原子执行 `RPUSH chunk_key slice_data` + `INCR version_key`

提交完成后，`SliceState` 变为 `Committed`，`FileWriter` 调用 `reader.invalidate_chunk(chunk_id)` 通知 reader 缓存失效，读路径即可看到新数据。
