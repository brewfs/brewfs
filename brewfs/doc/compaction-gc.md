# Compaction 与 GC

BrewFS 采用追加写（append-only write）+ COW（Copy-on-Write）的存储模型。每次写入产生新的 Slice，旧的 Slice 数据保留在对象存储中。随着时间推移，单个 Chunk 内会积累大量相互覆盖的 Slice，导致：

- 元数据膨胀（每个 Chunk 的 Slice 列表变长，读路径扫描开销增大）
- 存储浪费（被覆盖的旧 Block 数据无人引用但依旧占用空间）

Compaction 合并碎片化的 Slice，GC 回收无引用的 Block 数据。

源码位置：`src/chunk/compact/`

## 碎片率计算

Chunk 中所有 Slice 覆盖的区间合并后得到一个有效区间总长度 `merged_size`：

```
fragmentation = (total_slice_size - merged_size) / total_slice_size
```

`total_slice_size` 是所有 Slice 的 `length` 之和。重叠部分被重复计入，所以 `total_slice_size >= merged_size`。碎片率反映了"有多少字节是冗余的"。

```
例：Slice A [0, 100) + Slice B [50, 150)
  total = 200, merged = 150
  fragmentation = (200-150)/200 = 0.25
```

## 触发条件

```rust
if slice_count >= min_slice_count && frag_ratio >= min_fragment_ratio {
    // 触发 compaction
}
```

| 参数 | 默认值 | 说明 |
|---|---|---|
| `min_slice_count` | 5 | Slice 数量阈值 |
| `min_fragment_ratio` | 0.1 | 碎片率阈值（10%） |
| `sync_threshold` | 350 | 超过此值使用同步模式（更短的锁 TTL） |

## 两级压缩

### Light Compaction（轻量压缩）

**纯元数据操作，不涉及数据块读写。**

识别被更新的 Slice 完全覆盖的旧 Slice，直接从元数据中移除它们。判断逻辑：

1. 按 `slice_id` 降序遍历所有 Slice
2. 对每个 Slice，检查其 `[offset, offset+length)` 区间是否被更大 `slice_id` 的 Slice 集合完全覆盖
3. 如果完全覆盖 → 该 Slice 可以安全移除

不能进行"裁剪"操作（修改 Slice 的 offset/length）的原因：Block 数据的 key 是 `(slice_id, block_index)`，其中 `block_index` 基于 Slice 的原始 `offset` 计算。如果修改 offset，已有的 block_index 对应关系就会断裂，读路径将拿到错误数据。

### Heavy Compaction（重量压缩）

**读取所有 Block 数据，合并为单个新 Slice，重写所有数据。** 流程：

1. 读取当前所有 Slice 数据到内存
2. 按最新覆盖语义合并（新 Slice 覆盖旧 Slice 的重叠部分）
3. 分配新的 `slice_id`（通过 `next_id(SLICE_ID_KEY)`）
4. 通过 `record_uncommitted_slice()` 记录为 pending — 如果此步之后崩溃，GC 可以清理
5. 将合并后的数据写为新 Block 到对象存储
6. 创建覆盖整个 Chunk 的新 `SliceDesc`
7. 调用 `replace_slices_for_compact_with_version()` 原子替换元数据
8. 调用 `confirm_slice_committed()` 确认完成
9. 旧 Slice 被写入 `delayed_slice` 表，等待 GC 清理旧 Block

### Light vs Heavy 选择

Light compaction 速度极快（纯元数据修改），但只能处理"完全覆盖"场景。Heavy compaction 可以处理任意碎片化程度，但需要读写全部数据块。

Benchmark 结果：在 70% full coverage 写入场景下，Light compaction 减少 Heavy 触发次数 71.4%，总体加速 1.70x。在完全没有 full coverage 的场景下，Light compaction 的误判开销 < 1%。

策略：Compactor 优先尝试 Light compaction。如果 Light compaction 清除后的碎片率仍然超过阈值，再触发 Heavy compaction。

## Lock Manager

`src/chunk/compact/worker.rs` — `CompactLockManager`

每个 Chunk 的 compaction 使用两级锁：

| 级别 | 实现 | 用途 |
|---|---|---|
| **本地锁** | `HashSet<u64>` + `RwLock` | 同进程内快速去重，O(1) 检查 |
| **全局锁** | `MetaStore::get_global_lock(ChunkCompactLock(chunk_id), ttl)` | 跨节点排他，基于 MetaStore |

**锁的获取流程**：

1. 检查本地 HashSet，如果已有 → 跳过
2. 通过 MetaStore 尝试获取全局锁
3. 成功 → 插入本地 HashSet，开始 compaction
4. 失败（锁已被其他节点持有）→ 跳过此 Chunk

**动态 TTL**：

```rust
ttl = base_ttl + (slice_count * ttl_per_slice_ms / 1000)
ttl = clamp(ttl, min_ttl_secs, max_ttl_secs)
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `async_ttl_secs` | 10 | 异步 compaction 基础 TTL |
| `sync_ttl_secs` | 30 | 同步 compaction 基础 TTL |
| `ttl_per_slice_ms` | 50 | 每个 Slice 额外增加的时间 |
| `min_ttl_secs` | 5 | TTL 下限 |
| `max_ttl_secs` | 300 | TTL 上限 |

**TOCTOU 保护**：获取锁后重新分析 Chunk 的碎片状态。如果另一个节点已经完成 compaction（slice 数量和碎片率恢复正常），则跳过不做重复工作。

**锁释放**：正常完成 compaction 后显式释放（`ChunkLockGuard::unlock()`）。异常退出（panic/early return）时，`Drop` 实现会 spawn 一个后台任务尽力释放全局锁。最坏情况（进程崩溃）下，锁通过 TTL 过期自释放。

## CompactionWorker

`src/chunk/compact/worker.rs`

后台任务，以可配置的间隔周期性扫描：

1. **Compaction 循环**（默认间隔 1 小时）：
   - 调用 `MetaStore::list_chunk_ids(max_chunks_per_run)` 获取候选 Chunk 列表（默认 100 个）
   - 对每个 Chunk：
     - 检查 slice 数量和碎片率是否满足触发条件
     - 尝试获取两级锁（本地 + 全局）
     - TOCTOU 再检查
     - 调用 `Compactor::compact()` 执行压缩
     - 释放锁

2. 如果 `list_chunk_ids` 返回 `NotImplemented`（某些后端未实现），静默跳过本轮

启动方式：

```rust
CompactionWorker::start(worker_config, gc_config)
    → 两个独立的 tokio task:
       ├── compaction_handle   (compaction 循环)
       └── gc_handle          (GC 循环，委托给 BlockStoreGC)
```

## BlockStoreGC

`src/chunk/compact/gc.rs`

每个 GC 周期执行两个阶段：

### Phase A：Delayed Slice 清理

两阶段删除的"硬删除"阶段：

1. `MetaStore::process_delayed_slices(batch_size, min_age_secs)` 获取超过 `min_age_secs`（默认 1 小时）的 delayed 记录
2. 对每条记录，调用 `BlockStore::delete_range((slice_id, 0), num_blocks)` 删除对象存储中的 Block 数据
3. 成功 → 调用 `MetaStore::confirm_delayed_deleted(&ids)` 确认删除
4. 失败 → 不确认，留到下个 GC 周期重试（幂等操作）

**状态机**：`pending → meta_deleted → 删除`
- `process_delayed_slices` 处理 `pending` 记录时先清理 `slice_meta`，改状态为 `meta_deleted`，返回 slice 信息
- 对于已经是 `meta_deleted` 的记录，再次返回它们以支持 Block 删除重试

### Phase B：Orphan 未提交 Slice 清理

清理 Heavy Compaction 失败或崩溃遗留的孤儿数据：

1. `MetaStore::cleanup_orphan_uncommitted_slices(max_age_secs, batch_size)` 获取孤儿记录
   - `pending` 超过 `orphan_cleanup_age_secs`（默认 1 小时）的记录
   - 所有 `orphan` 状态的记录（不限制年龄 — 元数据已经没了，没有 dangling read 风险）
2. 对每条，调用 `BlockStore::delete_range()` 删除 Block 数据
3. 调用 `MetaStore::delete_uncommitted_slices(&ids)` 移除元数据记录

### GC 配置

```rust
pub struct BlockGcConfig {
    pub interval: Duration,              // 默认 1 小时
    pub min_age_secs: i64,              // 默认 1 小时
    pub batch_size: usize,             // 默认 1000
    pub block_size: u64,               // 默认 4 MiB
    pub orphan_cleanup_age_secs: i64,  // 默认 1 小时
}
```

单节点部署可缩短 `min_age_secs` 到 5-15 分钟加快空间回收；多节点场景不应低于最大写入→提交延迟。

## Compactor

`src/chunk/compact/compactor.rs`

`Compactor` 结构体封装了压缩逻辑。主要方法：

- `compact(chunk_id)` — 对一个 Chunk 执行完整的"先 light 后 heavy"压缩流程
- `light_compact(chunk_id)` — 仅执行轻量压缩
- `heavy_compact(chunk_id)` — 仅执行重量压缩

压缩结果通过 `CompactResult` 返回，包含被移除的 Slice 数量、新 Slice 信息等。
