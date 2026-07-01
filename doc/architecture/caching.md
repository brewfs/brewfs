# 缓存系统

BrewFS 在多个层级实现缓存，减少元数据访问延迟和对象存储 IO 次数。从上到下分为四个缓存层：

| 缓存层 | 位置 | 粒度 | 存储 |
|---|---|---|---|
| 元数据缓存 | `meta/client/cache.rs` | inode / path | 内存 |
| VFS 读缓存 | `vfs/cache/` | page / block | 内存 |
| VFS 写缓存 | `vfs/io/writer.rs` 的 SliceState | slice | 内存 |
| ChunksCache | `chunk/cache.rs` | block (4 MiB) | 热层内存 + 冷层磁盘 |

## 1. 元数据缓存

详见 `doc/architecture/metadata.md` 的 MetaClient 部分。核心组件：

**InodeCache**：双层存储
- `Moka Cache` 管理 TTL + LRU 淘汰
- `DashMap` 提供高并发读写访问
- 目录内容用 `ChildrenState`（NotLoaded/Partial/Complete）状态追踪

**Path Cache**：
- `path_cache: Moka Cache<String, i64>` — 完整路径→inode 的 O(1) 映射
- `PathTrie` — 前缀树，支持 O(depth) 前缀失效

**缓存失效**：`inode_to_paths` 反向索引 + PathTrie 前缀删除实现级联失效

## 2. VFS 读缓存

`src/vfs/cache/`

### LRU Page Cache

`src/vfs/cache/lru_cache.rs` — 基于 LRU 的页缓存，按 page 粒度（与 block_size 对齐）缓存已读取的数据。

### ReadCache

`src/vfs/cache/read_cache.rs` — 管理文件级别的读缓存状态。`FileReader` 通过 ReadCache 判断哪些数据已在缓存中、哪些需要从对象存储获取。

### Prefetch

`src/vfs/cache/prefetch.rs` — 顺序读检测和预取逻辑。检测到连续读取模式后，后台异步预取后续 Chunk 的 SliceDesc，减少 read_at 时的元数据查询延迟。

## 3. VFS 写缓存

写缓存在 `FileWriter` 中实现，详见 `doc/architecture/write-path.md`。

- Writable Slice 在内存中累积数据
- `dirty_slice_max_age_ms`（默认 500ms）或 `dirty_slice_target_size`（默认 64 MiB）触发 flush
- 最多积压 3 个未提交 Slice（`MAX_UNFLUSHED_SLICES`）

### 内存预算

`src/vfs/memory.rs` — `MemoryBudget` 全局内存协调：

```rust
pub struct MemoryBudget {
    limit: AtomicU64,     // 内存上限
    used: AtomicU64,      // 当前使用量
}
```

Reader 和 Writer 共享同一 memory budget。使用量超过 80% 时：
- Reader 降低预读窗口
- Writer 触发 force flush

超过 100% 时，新的写操作被阻塞直到内存释放。

### Write-Back

`src/vfs/cache/write_back.rs` — Write-back 策略管理，控制脏数据的回写时机（时间触发、大小触发、手动 fsync）。

## 4. ChunksCache（双层块缓存）

`src/chunk/cache.rs`

这是最底层的 Block 级别缓存，位于 `ObjectBlockStore` 内部。

### 架构

```
读取请求 (slice_id, block_index)
  │
  ▼
热缓存 (Hot Cache)
  │ Moka Cache, 内存, 默认 1024 项 x 4 MiB ≈ 4 GiB
  │ 命中 → 直接返回
  │ 未命中 ↓
  ▼
冷缓存 (Cold Cache)
  │ 磁盘文件: {cache_root}/chunks/{slice_id}/{block_index}
  │ 内存中维护访问频率统计 (DashMap)
  │
  │ 命中且频率达标 → 晋升到热缓存 → 返回
  │ 未命中 ↓
  ▼
对象存储 (S3 / LocalFS)
  │ GET Range 请求
  │ 返回后 → 异步写入冷缓存
```

### 自适应晋升策略

冷缓存中的 Block 需要满足频率条件才能晋升到热缓存：

**双时间窗评分**：
- **短窗口**（10 秒）：检测突发访问
- **中窗口**（60 秒）：分析中期趋势

```
score = short_window_weight × short_freq + (1 - short_window_weight) × medium_freq
阈值 = base_promotion_threshold × dynamic_factor
```

`dynamic_factor` 根据系统负载和当前命中率动态调整：
- 命中率低、负载高 → 提高阈值，减少晋升（防止缓存 churn）
- 命中率高、负载低 → 降低阈值，积极缓存

### 配置

```rust
pub struct ChunksCacheConfig {
    pub hot_cache_size: usize,           // 热缓存容量（项数），默认 1024
    pub cold_cache_dir: PathBuf,         // 冷缓存目录
    pub cold_cache_size_bytes: u64,      // 冷缓存容量（字节）
    pub base_promotion_threshold: f64,   // 基础晋升阈值，默认 10.0 次/秒
    pub short_window_weight: f64,        // 短窗口权重，默认 0.7
}
```

### 冷缓存持久化

`src/chunk/cache_health.rs` — 启动时扫描冷缓存目录，恢复访问统计信息。

`src/chunk/cache_integrity.rs` — 校验冷缓存文件完整性（大小检查、checksum 验证）。

## 5. SingleFlight

`src/chunk/singleflight.rs`

同一 Block 的并发读请求合并为一次实际 IO：

```rust
pub struct SingleFlight {
    in_flight: DashMap<BlockKey, Notify>,
}
```

- 第一个到达的请求：将 `Notify` 插入 `in_flight`，执行 IO
- 后续请求：查到 key 已存在 → `notify.notified().await` 等待
- IO 完成后：第一个请求 `notify.notify_waiters()` 唤醒所有等待者，从 `in_flight` 移除 key

不涉及数据复制 — 等待者醒来后各自从 ChunksCache（此时已写入）获取数据。

## 6. 带宽限流

`src/chunk/bandwidth.rs` — `BandwidthLimiter`

对上传和下载分别限流：

```rust
pub struct BandwidthConfig {
    pub upload_limit_mibps: Option<u64>,
    pub download_limit_mibps: Option<u64>,
}
```

使用令牌桶（token bucket）算法：
- 每个 `upload_limit_mibps` MiB/s 对应每秒产生等量 tokens
- 上传/下载操作需获取足够 tokens 才能进行
- tokens 不足时，操作等待（async sleep）而非 busy-wait

## 7. 压缩传输

`src/chunk/compress.rs` — `Compression`

在 BlockStore 层透明压缩/解压：

```rust
pub enum Compression {
    None,
    Lz4,
    Zstd(i32),  // i32 = compression level
}
```

- **写路径**：`write_fresh_vectored` 上传前压缩 payload，修改对象 key 后缀（如 `.lz4` 或 `.zstd`）
- **读路径**：`read_range` 根据 key 后缀判断是否需要解压

配置方式：YAML `cache.compression: "lz4"` 或 `"zstd"`，可选 `cache.zstd_level: 3`。

预期收益：典型文本/代码数据压缩率 40-60%，即 4 MiB Block 传输量降为约 2 MiB，写吞吐提升 30-50%。

## 缓存参数速查

| 参数路径 | 默认值 | 说明 |
|---|---|---|
| `cache.read_memory_bytes` | 4 GiB | 读缓存内存上限 |
| `cache.read_ssd_bytes` | 0 | 读缓存 SSD 冷层大小 |
| `cache.write_memory_bytes` | 1 GiB | 写缓存内存上限 |
| `cache.write_ssd_bytes` | 0 | 写缓存 SSD 层大小 |
| `cache.dirty_slice_target_size` | 64 MiB | 触发 flush 的大小阈值 |
| `cache.dirty_slice_max_age_ms` | 500 | 触发 flush 的时间阈值 |
| `cache.prefetch_enabled` | true | 是否启用预读 |
| `cache.prefetch_max_bytes` | 256 MiB | 预读窗口上限 |
| `cache.prefetch_concurrency` | 2 | 预读并发数 |
| `cache.memory_budget_bytes` | 8 GiB | 全局内存预算 |
| `cache.compression` | none | 传输压缩（none/lz4/zstd） |
| `cache.bandwidth.upload_limit_mibps` | none | 上传带宽限制 |
| `cache.bandwidth.download_limit_mibps` | none | 下载带宽限制 |

## 调优要点

- **顺序大文件读写**：增大 `dirty_slice_target_size`（如 128 MiB）和 `prefetch_max_bytes`（如 512 MiB），减少 Slice 数量和元数据压力
- **随机小 IO**：减小 dirty_slice_max_age_ms（如 100ms），降低延迟；关闭 prefetch 避免无效数据污染缓存
- **内存受限**：降低 `memory_budget_bytes`，配合 `read_memory_bytes` 和 `write_memory_bytes` 控制
- **高吞吐场景**：启用 `compression: lz4`，增大 `hot_cache_size`
