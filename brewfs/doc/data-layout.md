# 数据布局与 Chunk 系统

BrewFS 将文件数据组织为 **Chunk → Block** 两级结构，文件逻辑偏移先映射到 Chunk，再拆分为 Block 上传。元数据层（meta）独立维护文件偏移到 SliceDesc 的映射，数据路径只关心物理 IO。

源码位置：`src/chunk/`

## 核心概念

| 概念 | 默认大小 | 说明 |
|---|---|---|
| **Chunk** | 64 MiB | 文件按顺序划分的逻辑段，由 `chunk_id` 唯一标识 |
| **Block** | 4 MiB | Chunk 内的物理存储单元，对象存储的最小 IO 粒度 |
| **Slice** | 任意 | 一次写操作在某个 Chunk 内产生的连续字节区间 |
| **BlockSpan** | — | Slice 跨越单个 Block 的片段 |

一个文件从 0 开始，每 64 MiB 为一个 Chunk。一个 Chunk 内每 4 MiB 为一个 Block。写入时数据填入 Slice，Slice 按 Block 边界拆分为 BlockSpan，每个 BlockSpan 对应对象存储中的一个对象。

## ChunkLayout

`src/chunk/layout.rs` 定义了 `ChunkLayout` 和所有偏移换算：

```rust
pub struct ChunkLayout {
    pub chunk_size: u64,   // 默认 64 MiB
    pub block_size: u32,   // 默认 4 MiB
}
```

核心换算函数：

- `chunk_index_of(layout, file_offset) -> u64`：文件偏移所在的 Chunk 序号
- `within_chunk_offset(layout, file_offset) -> u64`：Chunk 内的偏移
- `chunk_offset(layout, chunk_index) -> u64`：Chunk 起始位置在文件中的偏移

文件偏移到 Chunk/Block 的映射：

```
file_offset = 70 MiB
  → chunk_index = 70 MiB / 64 MiB = 1
  → offset_in_chunk = 70 MiB % 64 MiB = 6 MiB
  → block_index = 6 MiB / 4 MiB = 1
  → offset_in_block = 6 MiB % 4 MiB = 2 MiB
```

### chunk_id 的计算

`src/vfs/mod.rs` 的 `chunk_id_for(ino, chunk_index)` 函数计算全局唯一的 chunk_id：

```rust
chunk_id = ino * 1_000_000_000 + chunk_index
```

这意味着每个 inode 最多可以拥有 10 亿个 Chunk（即约 60 PB），同时保证不同 inode 的 chunk_id 不冲突 — 因为对象存储使用 chunk_id 作为路径前缀。

`extract_ino_and_chunk_index(chunk_id)` 是逆运算，从 chunk_id 恢复 `(ino, chunk_index)`，用于 GC 扫描。

## Span\<T\>

`src/chunk/span.rs` 定义了泛型区间结构：

```rust
pub struct Span<T: SpanTag> {
    pub index: u64,   // 所在上层单元的索引
    pub offset: u64,  // 在该单元内的起始偏移
    pub len: u64,     // 区间长度
}
```

`T` 是编译期 marker trait，区分三种语义：

- `ChunkTag`：`Span<ChunkTag>` 表示文件内跨 Chunk 的一段范围
- `BlockTag`：`Span<BlockTag>` = `BlockSpan`，表示单个 Block 内的一段范围
- `PageTag`：`Span<PageTag>`，内存页粒度

`Span::split_into(&self, layout: &ChunkLayout)` 是核心方法，将粗粒度的 `Span<ChunkTag>` 切分为一组 `Span<BlockTag>`（即 `BlockSpan` 列表），是读写路径公用的基础操作。

## SliceDesc

`src/chunk/slice.rs` 定义了一次写入产生的 Slice 描述符：

```rust
pub struct SliceDesc {
    pub slice_id: u64,    // 全局自增 ID（通过 SLICE_ID_KEY 分配）
    pub chunk_id: u64,    // 所属 Chunk
    pub offset: u64,      // 在 Chunk 内的起始偏移
    pub length: u64,      // 数据长度
}
```

`slice_id` 由 `meta` 层的 `next_id(SLICE_ID_KEY)` 全局自增分配，保证跨所有 inode 的唯一性。这个唯一性至关重要 — 因为对象存储的 key 使用 `(slice_id, block_index)` 二元组。

## block_span_iter_slice

`src/chunk/slice.rs` 的导出函数，是 slice→block 映射的核心：

```rust
pub fn block_span_iter_slice(
    slice_offset: u64,  // SliceOffset — 相对于 slice 起始的偏移
    len: u64,
    layout: &ChunkLayout,
) -> impl Iterator<Item = BlockSpan>
```

给定一个 slice 内部的偏移和长度，返回一个迭代器，按 Block 边界依次产生每个 `BlockSpan`：

```
示例：layout.block_size = 4 MiB
输入：slice_offset = 3.5 MiB, len = 3 MiB

输出：
  BlockSpan { index=0, offset=3.5 MiB, len=0.5 MiB }   // Block 0 尾部
  BlockSpan { index=1, offset=0,      len=2.5 MiB }    // Block 1 头部
```

写路径使用此迭代器确定每个 Block 的上传范围；读路径使用它确定每个 Block 的读取范围。

## 对象存储键空间

Block 在对象存储中的路径格式为：

```
chunks/{slice_id}/{block_index}
```

写路径使用 `BlockStore::write_fresh_vectored(key=(slice_id, block_index), ...)` 上传，每次写入都是 COW（Copy-on-Write）— 产生新 key，不覆盖已有数据。旧数据由 GC 回收。

Block 的元信息（大小、checksum）可选存储在 `.meta` 后缀文件或对象元数据中。

## ChunkSpan

`src/chunk/util.rs` 定义了 ChunkSpan 类型别名和相关工具：

```rust
pub type ChunkSpan = Span<ChunkTag>;
```

`split_chunk_spans(layout, offset, len)` 将文件偏移范围按 Chunk 边界切割为 `Vec<ChunkSpan>`，是读写路径的第一步操作。

## BlockStore trait

`src/chunk/store.rs` 定义了 BlockStore trait：

```rust
pub trait BlockStore {
    async fn write_fresh_vectored(&self, key: BlockKey, offset: u64, data: Bytes) -> Result<()>;
    async fn read_range(&self, key: BlockKey, offset: u64, buf: &mut [u8]) -> Result<usize>;
    async fn delete_range(&self, key: BlockKey, num_blocks: usize) -> Result<()>;
    // ...
}
```

实现类型：

- `InMemoryBlockStore`：纯内存实现，用于测试
- `ObjectBlockStore<B: ObjectBackend>`：通过 ObjectBackend 访问对象存储，集成 ChunksCache 和 BandwidthLimiter

## BlockStoreConfig

控制 BlockStore 行为的配置：

```rust
pub struct BlockStoreConfig {
    pub block_size: usize,
    pub compression: Compression,
    pub range_read_threshold: usize,  // 低于此值走范围读而非全块读
    // ...
}
```

`range_read_threshold` 默认为 block_size 的 25%（即 1 MiB）。小范围读使用 HTTP Range 请求只获取需要的字节，避免全块传输。
