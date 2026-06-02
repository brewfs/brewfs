# Chunk 子系统设计文档

## 1. 概述

`src/chunk` 实现了 BrewFS 的数据分层存储引擎，其设计灵感来自 JuiceFS。文件数据在写入对象存储之前，按照固定粒度切分为 **Chunk → Block** 两级结构；元数据层（`meta`）单独维护文件逻辑偏移到 `SliceDesc` 的映射。读写路径均经过这一层完成数据的分割、寻址与汇聚。

---

## 2. 核心术语

| 术语 | 大小（默认） | 说明 |
|---|---|---|
| **Chunk** | 64 MiB | 文件按顺序划分的逻辑区段，由 `(ino, chunk_index)` 唯一标识 |
| **Block** | 4 MiB | Chunk 内的物理存储单元，是对象存储的最小读写粒度 |
| **Slice** | 任意长度 | 一次写操作在某个 Chunk 内产生的连续字节区间，由 `slice_id + offset + length` 描述 |
| **BlockSpan** | — | Slice 跨越单个 Block 的片段，包含 `(block_index, offset_in_block, len)` |

---

## 3. 模块结构

```
src/chunk/
├── mod.rs          # 模块入口，re-export 公共符号
├── layout.rs       # ChunkLayout：偏移计算、块索引换算
├── span.rs         # 泛型 Span<T>：ChunkTag / BlockTag / PageTag
├── slice.rs        # SliceDesc、ChunkOffset、SliceOffset、block_span_iter_slice
├── store.rs        # BlockStore trait、InMemoryBlockStore、ObjectBlockStore
├── cache.rs        # ChunksCache：双层热冷缓存 + 自适应晋升策略
├── singleflight.rs # SingleFlight：并发读合并，防止 thundering herd
├── writer.rs       # DataUploader：将 slice payload 并发写入各 Block
├── reader.rs       # DataFetcher：按 SliceDesc 读取 Block 并拼接输出
├── util.rs         # ChunkSpan 类型别名
└── compact/
    ├── mod.rs
    ├── compactor.rs  # Compactor：轻量 + 重量合并策略
    ├── worker.rs     # CompactionWorker：后台定时扫描；CompactLockManager
    └── gc.rs         # BlockStoreGC：清理孤立 Block 数据
```

---

## 4. 布局模型

`ChunkLayout` 封装了块尺寸参数，并提供全套偏移换算接口：

```
文件字节流
│
│  file_offset
├─────────────────────────────────────────────────────────────────────────
│  chunk_index = file_offset / chunk_size (64 MiB)
│  offset_in_chunk = file_offset % chunk_size
│
│    chunk_index = N
│    ┌────────────────── 64 MiB ──────────────────┐
│    │ Block 0 │ Block 1 │ ... │ Block 15 │ (4 MiB/block)
│    └─────────────────────────────────────────────┘
│         ↑
│    block_index = offset_in_chunk / block_size
│    offset_in_block = offset_in_chunk % block_size
```

`Span<T>` 是用于表示任意层级区间的泛型结构，通过编译期 marker（`ChunkTag` / `BlockTag` / `PageTag`）区分语义：

```rust
pub struct Span<T: SpanTag> {
    pub index: u64,   // 所在 chunk/block 的索引
    pub offset: u64,  // 区间在该单元内的起始偏移
    pub len: u64,     // 区间长度
}
```

`Span::split_into` 方法可将粗粒度 `Span<ChunkTag>` 拆分为若干 `Span<BlockTag>`，是读写路径的关键操作。

---

## 5. 写路径（Write Path）

### 5.1 整体流程

```
用户 write(offset, data)
       │
       ▼
vfs::io::FileWriter::write_at
  ├─ split_chunk_spans()          将文件偏移范围切分为跨 Chunk 的 ChunkSpan 列表
  └─ 对每个 ChunkSpan：
       ├─ 追加到 SliceState (Writable)
       └─ 后台 auto_flush 触发 spawn_flush_slice
              │
              ▼
         SliceState 状态机
         Writable → Readonly → Uploading → Uploaded → Committed
              │
              ▼ (Uploading)
         DataUploader::write_at_vectored
              │
              ├─ block_span_iter_slice(offset, len, layout)
              │       将 Slice 拆分为若干 BlockSpan（每个跨越一个 Block）
              │
              └─ 对每个 BlockSpan（并发执行）：
                   BlockStore::write_fresh_vectored(
                       key = (slice_id, block_index),
                       offset = offset_in_block,
                       data = 对应字节段
                   )
              │
              ▼ (Uploaded)
         commit_chunk
              └─ MetaLayer::append_slice(chunk_id, SliceDesc {
                     slice_id, chunk_id,
                     offset,    // Chunk 内偏移
                     length
                 })
              → SliceState 进入 Committed，对读端可见
```

### 5.2 对象存储键空间

Block 在对象存储中的键格式为：

```
chunks/{slice_id}/{block_index}
```

`slice_id` 由元数据层全局自增分配（`SLICE_ID_KEY`），保证唯一性。

### 5.3 写缓冲与背压

- 每个 Chunk 最多允许 `MAX_UNFLUSHED_SLICES`（= 3）个未上传的 Slice，超过后新写入等待；
- 单文件总 Slice 数超过 `MAX_SLICES_THRESHOLD`（= 800）后触发背压等待，防止元数据膨胀；
- `DataUploader::write_at_vectored` 对同一 Slice 的各 Block 并发上传（`join_all`），充分利用对象存储的并发能力。

---

## 6. 读路径（Read Path）

### 6.1 整体流程

```
用户 read(offset, len)
       │
       ▼
vfs::io::FileReader::read_at
  ├─ split_chunk_spans()      按 Chunk 边界切割读取范围
  └─ 对每个 ChunkSpan：
       ├─ DataFetcher::prepare_slices()
       │       MetaLayer::get_slices(chunk_id)
       │       → 加载该 Chunk 的全部 SliceDesc（按 slice_id 升序）
       │
       └─ DataFetcher::read_at(offset_in_chunk, len)
              │
              ├─ 使用 Intervals 结构从最新 Slice 开始反向扫描
              │   构建 need_read 列表：(range_start, range_end, SliceDesc)
              │   未被任何 Slice 覆盖的区间保持零填充（文件空洞）
              │
              └─ 对每条 need_read 条目（并发 FuturesUnordered）：
                   ├─ 计算相对于 Slice 起始的 SliceOffset
                   ├─ block_span_iter_slice(slice_offset, len, layout)
                   │       枚举覆盖 Block 列表
                   └─ 对每个 BlockSpan（顺序）：
                        BlockStore::read_range(
                            key = (slice_id, block_index),
                            offset = offset_in_block,
                            buf  = 输出缓冲区对应片段
                        )
```

### 6.2 Slice 覆盖语义

多次写入同一 Chunk 会产生多个 SliceDesc，它们在逻辑上可能相互覆盖。读取时采用**最新 Slice 优先**策略：

- 从 `slice_id` 最大（最新）的 Slice 开始向前扫描；
- 用 `Intervals` 数据结构追踪已被覆盖的偏移区间，避免重复读取；
- 仍未覆盖的区间（空洞）直接填零，无需发起 IO。

### 6.3 缓存与合并读

`ObjectBlockStore` 内置两层优化：

**双层缓存（`ChunksCache`）**
```
请求 → 热缓存（Hot, 内存，1024 项）
          命中 → 直接返回
          未命中 → 冷缓存（Cold, 元数据追踪）
                    命中且频率达阈值 → 晋升到热缓存
                    未命中 → 访问对象存储 → 写入冷缓存
```

晋升决策采用双时间窗（短窗口 10s 突发检测 + 中窗口 60s 趋势分析）的加权频率评分，并根据系统负载和命中率自适应调整阈值。

**SingleFlight 并发合并**：同一 Block 的并发读请求中，只有第一个实际发出网络请求，后续等待者共享结果，避免 thundering herd。

---

## 7. Block 寻址：`block_span_iter_slice`

这是 slice 到 block 映射的核心函数，位于 `slice.rs`。给定 `(slice_offset, slice_len, layout)` 三元组，返回一个迭代器，依次产生每个 `BlockSpan`：

```
示例：layout.block_size = 4 MiB，Slice offset=3.5 MiB，length=3 MiB

Block 0（4 MiB）: [3.5 MiB, 4 MiB)  → BlockSpan { index=0, offset=3.5*1024*1024, len=0.5 MiB }
Block 1（4 MiB）: [0,       2.5 MiB) → BlockSpan { index=1, offset=0,             len=2.5 MiB }
```

写路径用此迭代器确定每个块的上传偏移；读路径用此迭代器确定每个块的读取范围。

---

## 8. 压缩与 GC

### 8.1 触发条件

每个 Chunk 的 Slice 列表随写入增长。当满足以下条件时触发压缩：

- Slice 数量 ≥ `min_slice_count`（默认配置）
- 碎片率 ≥ `min_fragment_ratio`

碎片率定义为重叠覆盖造成的冗余字节占总 Slice 字节数的比例，使用扫描线区间合并精确计算。

### 8.2 两级压缩策略

**轻量压缩（Light Compaction）**
- 仅修改元数据，不涉及 Block 数据读写；
- 删除被更新 Slice 完全覆盖的旧 Slice 记录；
- 适用于 Slice 数量多但物理数据重叠简单的场景。

**重量压缩（Heavy Compaction）**
- 读取当前所有 Block 数据，合并写入一个新 Slice；
- 替换元数据中的 Slice 列表；
- 旧 Block 数据交由 `BlockStoreGC` 延迟清理；
- 适用于碎片率高、单凭元数据清理效果有限的场景。

### 8.3 并发控制

`CompactLockManager` 提供两级锁：

| 级别 | 实现 | 适用场景 |
|---|---|---|
| 本地锁 | `HashSet<u64>` + `RwLock` | 同进程内快速去重检查 |
| 全局锁 | `MetaStore::acquire_global_lock`（带 TTL） | 跨节点排他，崩溃后 TTL 自动过期释放 |

`CompactionWorker` 以可配置的间隔（默认 1 小时）在后台周期性扫描，每轮最多处理 `max_chunks_per_run`（默认 100）个 Chunk。

---

## 9. 数据结构速览

```rust
// 对象存储中一个 Block 的键
type BlockKey = (u64 /*slice_id*/, u32 /*block_index*/);

// Chunk 内一段连续写入的描述符，持久化到 MetaStore
pub struct SliceDesc {
    pub slice_id: u64,
    pub chunk_id: u64,
    pub offset:   u64,  // 相对 Chunk 起始的字节偏移
    pub length:   u64,
}

// Slice 跨越单个 Block 的片段
pub type BlockSpan = Span<BlockTag>;
// Span<T> { index: u64, offset: u64, len: u64 }

// 布局参数
pub struct ChunkLayout {
    pub chunk_size: u64,   // 默认 64 MiB
    pub block_size: u32,   // 默认 4 MiB
}
```

---

## 10. 设计取舍

| 取舍点 | 当前选择 | 原因 |
|---|---|---|
| Block 写模型 | COW（每次写新 key，不读旧数据） | 消除读改写，降低写延迟；旧数据由 GC 回收 |
| 多 Slice 覆盖 | 追加语义 + 读时合并 | 写路径无需锁定，支持并发写同一 Chunk |
| 空洞处理 | 零填充 | POSIX 稀疏文件语义，无额外元数据开销 |
| 缓存粒度 | Block（4 MiB） | 与对象存储 IO 单元对齐，减少放大 |
| 小范围读 | `range_read_threshold`（默认 1 MiB）以下走范围读而非全块读 | 平衡带宽放大与延迟 |

---

## 11. 性能优化建议

### 11.1 写路径优化

**增大写缓冲窗口（`WriteConfig`）**
- 当前每个 Chunk 最多积压 `MAX_UNFLUSHED_SLICES = 3` 个 Slice 才触发上传。在顺序大文件写入场景下可适当提高此值，以攒更大批次后并发上传，降低 Slice 数量和后续合并压力。
- 但注意过大会推迟数据持久化、占用更多内存缓冲。

**减少 Slice 碎片（调整压缩触发阈值）**
- 对于 Append-Only 场景（日志、WAL），每次追加都会产生新 Slice；适当降低 `min_slice_count`（当前默认 5）可更早触发轻量压缩，把碎片消灭在萌芽阶段，避免读路径需要拼接大量 Slice。
- 对于随机覆盖写场景，优先提高 `min_fragment_ratio`（当前默认 0.1），只在碎片化严重时才触发重量压缩，避免频繁数据重写。

**预分配 Block 上传并发度**
- `DataUploader::write_at_vectored` 当前将所有 Block 的 Future 收集后一次性 `join_all`，极端情况下（Slice 跨 16 个 Block）会同时发起 16 个对象存储请求。若对象存储端有并发限制，可改为分批 `join_all`（如每批 4 个），避免连接池耗尽。

### 11.2 读路径优化

**预读（Read-Ahead）**
- `FileReader` 已设计了 `background_fetch` 机制，可在用户读取前异步预取后续 Chunk 的 Slice 数据。关键参数是 `DEFAULT_TOTAL_AHEAD_LIMIT = 256 MiB` 和 `READ_SESSIONS = 2`。
- 顺序读密集型负载（流式处理、数据分析）可适当增大 `total_ahead_limit`，充分利用带宽；随机读密集型负载应关闭或缩小预读范围，避免无效数据污染缓存。

**缓存命中率调优（`ChunksCacheConfig`）**
- `hot_cache_size`（默认 1024 项 × 4 MiB ≈ 4 GiB）：根据实际可用内存调整，内存充裕时可翻倍。
- `base_promotion_threshold`（默认 10.0 次/秒）：交互型业务（频繁随机读）可降至 3–5，加快热数据晋升；批量扫描场景可升至 20+，防止一次性扫描污染热缓存。
- `short_window_weight`（默认 0.7）：突发型工作负载可调高至 0.85，使近期访问权重更高；稳定流量建议降至 0.5，让中期趋势参与决策。

**直接范围读 vs 全块读**
- `BlockStoreConfig::range_read_threshold` 默认为 block_size 的 25%（即 1 MiB）。对于以小 IO 为主的读取（如元数据访问、小文件读取），可将此阈值提高至 50%，更多情况走范围读，节省网络带宽；对于顺序大文件读取，应降低或置零，强制全块读以充分利用缓存。

**减少 Slice 扫描开销**
- `DataFetcher::read_at` 每次读取都会遍历所有 SliceDesc 做区间求差（`Intervals::cut`），Slice 越多耗时越长。核心手段是控制每个 Chunk 的 Slice 数量（见 §11.1），同时保证元数据查询有合适的索引覆盖。

### 11.3 压缩与 GC 调优

**并发压缩（`max_concurrent_tasks`）**
- 当前 `CompactionWorker` 顺序扫描 Chunk 列表，`max_concurrent_tasks`（CompactConfig 中已定义）暂未在逻辑中启用。一旦启用，建议将并发度设置为对象存储可承受连接数的 1/4，预留带宽给正常读写。

**GC 延迟时间窗口（`min_age_secs`）**
- 默认 1 小时的延迟删除窗口是为了保证读端不会读到已被元数据删除但 Block 尚未复制完的数据。若集群不存在跨节点读写竞态（单节点部署），可缩短至 5–15 分钟，加快存储空间回收。
- 多节点场景下不应低于最大可能的写入→提交延迟（通常 ≥ 30 分钟）。

**孤立 Slice 清理（`orphan_cleanup_age_secs`）**
- 默认同为 1 小时。频繁的重量压缩失败（如对象存储不稳定）会积累大量 `pending` 孤立记录；可降低此值至 10–30 分钟以加快清理，但须确保网络已恢复稳定，否则清理循环本身会持续失败。

### 11.4 对象存储适配建议

| 场景 | 建议 |
|---|---|
| 高吞吐顺序写 | 增大 Block 尺寸（如 8–16 MiB），减少请求数 |
| 低延迟随机读 | 缩小 Block 尺寸（如 1–2 MiB），减少读放大 |
| S3 兼容存储（有请求计费） | 开启全块读缓存，尽量命中 Hot Cache，减少 GET 请求 |
| 本地对象存储（MinIO 等） | 可关闭 SingleFlight 合并（并发无瓶颈），降低等待延迟 |
| 带宽受限网络 | 在写路径采用数据压缩（Zstd/LZ4）后再上传 Block，需修改 BlockStore 实现 |
