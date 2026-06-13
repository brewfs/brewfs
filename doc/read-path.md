# 读路径

读路径将用户的文件读取请求转换为对对象存储的 Block 级别读取，在 Slice 层面处理覆盖、空洞，在 Block 层面利用缓存减少远程 IO。

源码分布：
- `src/vfs/io/reader.rs` — `FileReader`，预读与缓存管理
- `src/chunk/reader.rs` — `DataFetcher`，Slice→Block 的定位与拼装
- `src/chunk/store.rs` — `ObjectBlockStore`，对象读取 + ChunksCache
- `src/chunk/singleflight.rs` — `SingleFlight`，并发读合并
- `src/utils/intervals.rs` — `Intervals`，区间覆盖计算

## 整体流程

```
FUSE read(ino, offset, len)
  │
  ▼
VFS::read() → FileReader::read_at(offset, len)
  │
  ├─ split_chunk_spans(layout, offset, len) → Vec<ChunkSpan>
  │
  └─ 对每个 ChunkSpan:
       │
       ├─ DataFetcher::prepare_slices(chunk_id)
       │    └─ MetaLayer::get_slices(chunk_id) → Vec<SliceDesc>
       │         (按 slice_id 升序，从 MetaClient 缓存或 MetaStore 加载)
       │
       ├─ DataFetcher::read_at(offset_in_chunk, len)
       │    │
       │    ├─ 构造 Intervals，从最新 slice 开始反向扫描:
       │    │   遍历 slices.iter().rev()
       │    │   对每个 slice:
       │    │     intervals.cut(slice.offset, slice.offset + slice.length)
       │    │     → 如果 cut 返回非空区间 → 加入 need_read 列表
       │    │   （已被覆盖的区间不会被重复读取）
       │    │
       │    ├─ 剩余未覆盖的区间 → zeros（文件空洞，填 0）
       │    │
       │    └─ 对每条 need_read: (range, slice_desc)
       │         ├─ 计算 relative_offset = range.start - slice_desc.offset
       │         ├─ block_span_iter_slice(relative_offset, range_len, layout)
       │         └─ 对每个 BlockSpan:
       │              BlockStore::read_range(
       │                key = (slice_desc.slice_id, block_span.index),
       │                offset = block_span.offset,
       │                buf = &mut output[dest_start..dest_end]
       │              )
       │
       └─ 合并结果，返回给用户
```

## FileReader

`src/vfs/io/reader.rs`

`FileReader` 负责文件级别的读取管理：

- `handles: DashMap<u64, FileReadHandle>` — 每个 inode 一个读句柄
- `read_gen: AtomicU64` — 读代数计数器

### FileReadHandle

每个 `FileReadHandle` 包含：

- `fetchers: LruCache<u64, Arc<DataFetcher>>` — 按 chunk_id 缓存的 DataFetcher（默认最多 256 个）
- `prefetch: Option<PrefetchState>` — 预读状态机

### 预读 (Read-Ahead)

顺序读检测和预取逻辑：

1. 每次 `read_at` 调用时，FileReader 检测是否连续读取：
   - 如果当前读取的 offset 紧接上次读取的末尾 → 顺序读取计数 +1
   - 如果 offset 跳跃 → 计数重置
2. 顺序读取计数超过阈值后，触发后台预取：

```
当前读: Chunk N
  → 后台并发预取: Chunk N+1, N+2 （最多 READ_SESSIONS=2 个并发）
  → 总预取窗口: DEFAULT_TOTAL_AHEAD_LIMIT = 256 MiB
```

3. 预取使用 `background_fetch` 任务：调用 `DataFetcher::prepare_slices()` 提前加载 SliceDesc 到内存，但不实际读取 Block 数据。

配置项（在 `vfs/cache/config.rs` 的 `CacheConfig`）：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `prefetch_enabled` | true | 是否启用预读 |
| `prefetch_max_bytes` | 256 MiB | 预读窗口上限 |
| `prefetch_concurrency` | 2 | 预读并发数 |

## DataFetcher

`src/chunk/reader.rs`

`DataFetcher` 是读路径的核心，负责从一个 Chunk 中定位和读取数据。

### prepare_slices

```rust
pub async fn prepare_slices(&self) -> Result<()>
```

从 MetaLayer 加载该 Chunk 的全部 `SliceDesc`，按 `slice_id` 升序排列。结果缓存在 `DataFetcher` 内部（`ArcSwap`），避免重复查询元数据。

读取时采用**最新优先**策略：从 slice_id 最大（最新写入）的 Slice 开始向前扫描。这保证了多次覆盖写入场景下，读取到的总是最新数据。

### read_at

```rust
pub async fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize>
```

使用 `Intervals` 数据结构（`src/utils/intervals.rs`）追踪已被覆盖的偏移区间。`Intervals::cut(start, end)` 方法从一个区间中"挖去"已有覆盖的部分，返回裸露的区间列表。算法流程：

1. 从后向前遍历所有 SliceDesc
2. 对每个 Slice，计算它与 `[offset, offset+len)` 的交集
3. 用 `intervals.cut()` 检查交集中尚未被更新的 Slice 覆盖的部分
4. 将未被覆盖的部分加入 `need_read` 列表
5. 遍历完后，`intervals` 中剩余的就是空洞 — 填 0
6. 对所有 `need_read` 条目并发发起 Block 读取（`FuturesUnordered`）

### 并发读取

`need_read` 中的每个条目并发执行，但单个条目内的 BlockSpan 按顺序读取（因为要填充到连续的输出缓冲区）。

Block 读取经过 `ChunksCache`（热/冷缓存）和 `SingleFlight`（并发合并），实际到对象存储的请求已被大幅削减。

## BlockStore 读取

`ObjectBlockStore::read_range(key, offset, buf)` 的路径：

1. **热缓存**（内存 `Moka Cache`）：命中则直接 `memcpy` 到输出缓冲区
2. **冷缓存**（磁盘）：命中且访问频率达到晋升阈值 → 加载到热缓存；未命中 → 第三步
3. **对象存储**：`ObjectClient::get_range(key, offset, len)` 发起 Range GET
4. 读取结果写入冷缓存（异步），供后续访问

## SingleFlight

`src/chunk/singleflight.rs`

当多个并发请求同时读取同一个 `(slice_id, block_index)` 时，只有第一个请求实际发出网络 IO，后续请求等待第一个完成并共享结果。等待使用 `tokio::sync::Notify`，不会 busy-wait。

适用场景：多个线程或进程同时读取同一文件的热点区域（如共享库、模型文件）。

## Intervals

`src/utils/intervals.rs`

一个区间集合数据结构，支持操作：

- `cut(start, end)`：从该区间集合覆盖的区域中切出一段，返回未被覆盖的区间列表
- `add(start, end)`：向集合添加一个覆盖区间
- `merge()`：合并重叠区间

在读路径中，初始化一个空的 `Intervals`，每找到一个覆盖的 Slice 就 `add` 进去。`cut` 用来计算实际需要从对象存储读取的区间。这种设计保证了重叠 Slice 场景下不会重复读取同一段数据。
