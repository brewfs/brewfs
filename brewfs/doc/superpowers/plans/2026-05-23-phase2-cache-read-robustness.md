# BrewFS Phase 2 — 缓存鲁棒性与读可靠性

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 完成 P1-4 ~ P3-8，覆盖 insert 去重、晋升调优、读重试、tail prefetch、磁盘健康状态机。每个 task 对标 JuiceFS 对应源码。

**Prerequisites:** Phase 1 (P0) 已完成 — 页缓存升 block、写路径同步 hot insert、压缩块铺 page_cache。

**JuiceFS 源码参照:**
- `/mnt/rk8s/project/brewfs/juicefs/pkg/vfs/reader.go:162-232` — slice 读重试
- `/mnt/rk8s/project/brewfs/juicefs/pkg/vfs/reader.go:652-659` — tail prefetch
- `/mnt/rk8s/project/brewfs/juicefs/pkg/chunk/disk_cache_state.go` — 磁盘健康状态机
- `/mnt/rk8s/project/brewfs/juicefs/pkg/chunk/disk_cache.go:436-472` — insert 去重
- `/mnt/rk8s/project/brewfs/juicefs/pkg/chunk/cache_eviction.go` — 淘汰策略

---

## Task 1 (P1-4): insert_opportunistic per-key 去重

**对标 JuiceFS:** `disk_cache.go:445-456` — `cache.keys.get(k)` 检查 + `cache.pages[key]` 二次确认

**JuiceFS 模式:**
```go
// disk_cache.go:445-456
if _, ok := c.pages[key]; ok {
    return // already pending
}
if c.scanned {
    if item := c.keys.get(cacheKey); item != nil {
        return // already on disk
    }
}
p.Acquire()
c.pages[key] = p
c.pending <- pendingFile{key, p, dropCache}
```

BrewFS 当前 `insert_opportunistic` 会在 hot_cache insert 后无条件 spawn disk write。多 reader/prefetcher 并发对同 key 调用会导致重复磁盘写。

**实现:**

- [ ] **Step 1: 添加 `disk_insert_inflight: DashSet<String>` 到 ChunksCache**

在 `brewfs/src/chunk/cache.rs` ChunksCache 结构体:
```rust
use dashmap::DashSet;

pub struct ChunksCache {
    // ... existing fields ...
    disk_insert_inflight: Arc<DashSet<String>>,
}
```

- [ ] **Step 2: 在 insert_opportunistic 入口处 CAS 检查**

```rust
pub async fn insert_opportunistic(&self, key: String, data: Vec<u8>) {
    // Phase 1: hot cache — always insert (fast, in-memory)
    self.insert_hot(&key, data.clone()).await;

    // Phase 2: disk cache — dedup via inflight set
    if !self.disk_insert_inflight.insert(key.clone()) {
        return; // Another caller is already persisting this key
    }

    let cache = self.clone();
    let k = key.clone();
    let d = data;
    tokio::spawn(async move {
        // Double-check: is the key already on disk?
        if cache.is_disk_cached(&k).await {
            cache.disk_insert_inflight.remove(&k);
            return;
        }

        let result = if let Some(permit) = cache.try_disk_store_permit(&k) {
            cache.store_to_disk_with_permit(&k, &d, permit).await
        } else {
            Ok(()) // skipped under IO pressure
        };

        cache.disk_insert_inflight.remove(&k);

        if let Err(e) = result {
            tracing::debug!(key = %k, error = %e, "disk cache insert failed");
        }
    });
}
```

- [ ] **Step 3: 添加 `is_disk_cached` 方法**

```rust
pub async fn is_disk_cached(&self, key: &str) -> bool {
    self.cold_cache.contains_key(key)
    // cold_cache entry exists iff the key was previously written to disk
}
```

- [ ] **Step 4: 测试**

```rust
#[tokio::test]
async fn test_concurrent_insert_opportunistic_dedup() {
    // Spawn N concurrent insert_opportunistic with same key
    // Verify: hot_cache has 1 entry, disk bytes used <= 1 framed block
    // Verify: disk_insert_inflight is empty after all tasks complete
}
```

- [ ] **Step 5: Commit**
```bash
git commit -m "fix(cache): add per-key dedup to insert_opportunistic via DashSet

Prevent concurrent readers/prefetchers from writing the same block to
disk multiple times. Modeled after JuiceFS disk_cache.go pages-map +
KeyIndex dedup pattern."
```

---

## Task 2 (P1-5): 调优自适应晋升阈值

**对标 JuiceFS:** `cache_eviction.go` — 2-random 淘汰 + atime 比较。JuiceFS 不做频率感知晋升，但 BrewFS 的 Policy 结构可以做得更好。

**当前问题:** `base_promotion_threshold=10.0`，short window weight 偏低，导致 randread 场景下 block 需访问 10 次才晋升 hot cache。

**实现:**

- [ ] **Step 1: 降低 base threshold，提高 short window weight**

在 `brewfs/src/chunk/cache.rs` Policy::new():
```rust
const BASE_PROMOTION_THRESHOLD: f64 = 5.0;    // was 10.0
const SHORT_WINDOW_SECS: u64 = 10;             // unchanged
const SHORT_WINDOW_GRANULARITY: u64 = 1;       // unchanged
const MEDIUM_WINDOW_SECS: u64 = 60;            // unchanged
const MEDIUM_WINDOW_GRANULARITY: u64 = 5;      // unchanged

const SHORT_WEIGHT: f64 = 0.75;                // was ~0.5
const MEDIUM_WEIGHT: f64 = 0.25;               // was ~0.5
```

- [ ] **Step 2: 添加 hit-rate 反馈**

```rust
fn calculate_adaptive_threshold(&self, system_load: f64, hit_rate: f64) -> f64 {
    let mut threshold = self.base_promotion_threshold;

    // Aggressively lower threshold when hit rate is poor (cold cache / startup)
    if hit_rate < 0.3 {
        threshold *= 0.5;  // promote faster during warmup
    } else if hit_rate < 0.6 {
        threshold *= 0.75;
    }

    // Raise threshold when hot cache is full to protect working set
    let hot_utilization = self.hot_bytes.load(Relaxed) as f64 / self.max_hot_bytes as f64;
    if hot_utilization > 0.85 {
        threshold *= 1.3;
    } else if hot_utilization > 0.95 {
        threshold *= 1.6;
    }

    // System load factor — back off under memory pressure
    if system_load > 0.8 {
        threshold *= 1.2;
    }

    threshold.max(2.0) // floor: never require fewer than 2 accesses
}
```

- [ ] **Step 3: 暴露 hit_rate 指标**

```rust
// In ChunksCache::get:
pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
    // ... existing lookup logic ...
    if result.is_some() {
        self.hits.fetch_add(1, Ordering::Relaxed);
    } else {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
    result
}

pub fn hit_rate(&self) -> f64 {
    let hits = self.hits.load(Ordering::Relaxed) as f64;
    let total = hits + self.misses.load(Ordering::Relaxed) as f64;
    if total == 0.0 { return 0.0; }
    hits / total
}
```

- [ ] **Step 4: 测试 + perf 验证**
```bash
bash docker/compose-xfstests/run_redis_perf.sh --s3 --tools "fio-randread"
# 预期: BW 继续提升 (hot cache 命中率改善)
```

- [ ] **Step 5: Commit**
```bash
git commit -m "perf(cache): tune promotion thresholds and add hit-rate feedback

Lower base threshold (10->5), increase short window weight (0.5->0.75),
add adaptive scaling based on cache hit rate and hot cache utilization."
```

---

## Task 3 (P2-6): Slice 读失败指数退避重试

**对标 JuiceFS:** `vfs/reader.go:162-232` — `sliceReader.run()` 的 retry 逻辑

**JuiceFS 模式:**
```go
// reader.go:183-191, 221-231
s.tried++
if s.tried > f.r.maxRetries {
    // permanent failure
}
retry_time := time.Duration(s.tried*s.tried) * time.Millisecond  // quadratic
if retry_time > time.Second {
    retry_time = time.Second
}
time.Sleep(retry_time)
// Invalidate chunk cache before retry
m.InvalidateChunkCache(Background(), inode, indx)
```

关键要素:
1. 重试前 invalidate chunk cache (强制重新 LRANGE 切片列表)
2. 二次方退避 capped at 1s
3. 最大重试次数 `maxRetries` (默认 50, BrewFS 用 3-5 即可)

**实现:**

- [ ] **Step 1: 在 `background_fetch` 外围包 retry loop**

在 `brewfs/src/vfs/io/reader.rs:378-454`:
```rust
async fn background_fetch(
    state: Arc<ParkingMutex<SliceState>>,
    ino: u64,
    chunk_idx: u64,
    backend: Arc<Backend<B, M>>,
    config: Arc<ReadConfig>,
    memory_budget: Option<MemoryBudget>,
) {
    const MAX_RETRIES: u32 = 5;

    let range = {
        let s = state.lock();
        s.range
    };
    let fetcher = DataFetcher::new(backend.clone(), config.clone(), memory_budget.clone());

    let mut last_err = String::new();
    for attempt in 0..MAX_RETRIES {
        if fetcher.prepare_slices(ino, chunk_idx, range).await.is_err() {
            last_err = "prepare_slices failed".into();
        } else {
            match fetcher.read_at(range.0, (range.1 - range.0) as usize).await {
                Ok(data) => {
                    // Success
                    let mut s = state.lock();
                    s.page = data;
                    s.state = SliceStatus::Ready;
                    s.notify.notify_waiters();
                    return;
                }
                Err(e) => {
                    last_err = format!("{e:?}");

                    // Classify: only retry transient errors
                    if !is_transient_read_error(&e) {
                        break; // permanent error, don't retry
                    }
                }
            }
        }

        if attempt < MAX_RETRIES - 1 {
            // Quadratic backoff with cap
            let delay_ms = ((attempt + 1) * (attempt + 1) * 10).min(1000);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;

            // Invalidate chunk metadata cache before retry
            // — forces next prepare_slices to re-fetch from Redis
            backend.meta().invalidate_chunk_slices(ino, chunk_idx).await;
        }
    }

    // All retries exhausted
    let mut s = state.lock();
    s.state = SliceStatus::Invalid;
    s.err = Some(last_err);
    s.notify.notify_waiters();
}
```

- [ ] **Step 2: 添加 `is_transient_read_error` 分类**

```rust
fn is_transient_read_error(e: &anyhow::Error) -> bool {
    let msg = format!("{e:?}").to_lowercase();
    msg.contains("timeout")
        || msg.contains("connection reset")
        || msg.contains("connection refused")
        || msg.contains("temporary failure")
        || msg.contains("eagain")
        || msg.contains("broken pipe")
        || msg.contains("request canceled") // context canceled by concurrent close
}
```

- [ ] **Step 3: 添加 `invalidate_chunk_slices` 到 MetaClient trait**

如果 `MetaClient` 还没有这个方法:
```rust
pub async fn invalidate_chunk_slices(&self, ino: u64, chunk_idx: u64) {
    self.inode_cache.invalidate_slices(ino, chunk_idx).await;
}
```

- [ ] **Step 4: 测试**

Mock BlockStore: 前 2 次 `read_at` 返回 timeout error, 第 3 次成功。
```rust
#[tokio::test]
async fn test_slice_read_retry_transient() {
    // Setup mock that fails twice then succeeds
    // Verify: state is Ready (not Invalid)
    // Verify: metric shows 3 attempts
}
```

- [ ] **Step 5: Commit**
```bash
git commit -m "perf(reader): add exponential backoff retry for transient slice read failures

Modeled after JuiceFS reader.go sliceReader.run() retry logic:
- Invalidate chunk metadata cache before each retry
- Quadratic backoff capped at 1s, max 5 retries
- Only retry classified transient errors (timeout, reset, etc.)
- Permanent errors still fail immediately"
```

---

## Task 4 (P2-7): 尾部预取 (Tail Prefetch)

**对标 JuiceFS:** `reader.go:652-659` — last-block readahead

**JuiceFS 模式:**
```go
// reader.go:652-659
if block.off+uint64(block.len) > f.length-32*1024 {
    // Prefetch last block
    last := (f.length-1)/ChunkSize*ChunkSize
    // ... create sliceReader for last block
}
```

**实现:**

- [ ] **Step 1: 在 prepare_ahead_slices 末尾添加 tail prefetch**

在 `brewfs/src/vfs/io/reader.rs` `prepare_ahead_slices` 方法:
```rust
// Tail prefetch: when reading near EOF, warm the last 32KB
let file_len = self.inode.file_size();
let tail_threshold = file_len.saturating_sub(32 * 1024);
let read_end = offset + ahead;

if read_end >= tail_threshold && file_len > 32 * 1024 {
    let tail_offset = file_len.saturating_sub(32 * 1024);
    let tail_block = tail_offset / self.config.layout.block_size as u64;

    // Don't prefetch if already covered by existing slices or current readahead
    let slices = self.slices.lock();
    let already_covered = slices.iter().any(|s| {
        let s = s.lock();
        s.range.0 <= tail_offset && s.range.1 > tail_offset
    });
    drop(slices);

    if !already_covered {
        let tail_start = tail_block * self.config.layout.block_size as u64;
        let tail_len = (file_len - tail_start) as usize;
        if tail_len > 0 {
            self.submit_prefetch(
                (tail_start, tail_start + tail_len as u64),
                PrefetchPriority::Sequential,
            ).await;
        }
    }
}
```

- [ ] **Step 2: 测试**

```rust
#[tokio::test]
async fn test_tail_prefetch_on_eof_read() {
    // File: 128KB, block_size=64KB
    // Read at offset 96KB (within last 32KB of file)
    // Verify: reader slices cover EOF-32KB range
    // Verify: tail block is in prepared slices
}

#[tokio::test]
async fn test_no_tail_prefetch_on_midfile_read() {
    // File: 1MB, block_size=64KB
    // Read at offset 0
    // Verify: no tail prefetch submitted
}
```

- [ ] **Step 3: Commit**
```bash
git commit -m "perf(reader): add tail prefetch for last 32KB of file

When a read approaches the last 32KB of a file, speculatively prefetch
the tail block. Modeled after JuiceFS reader.go last-block readahead.
Avoids extra S3 round-trip for the common 'read to EOF' pattern."
```

---

## Task 5 (P3-8): 磁盘缓存 I/O 健康状态机

**对标 JuiceFS:** `chunk/disk_cache_state.go` — 完整 5 态状态机

**JuiceFS 模式 (简化版):**
```go
// disk_cache_state.go
type dcState interface {
    state() dcStateEnum
    onIOErr()    // Normal -> Unstable after 3 errors
    onIOSucc()   // Unstable -> Normal after 60 successes
    checkErr()   // wrap IO with error tracking
}

// Transitions:
// Normal -> Unstable: 3 IO errors within reset_window
// Unstable -> Normal: error_rate==0 && io_count>=60 within tick window
// Unstable -> Down:   unstable duration > 30 min
// Down:               all ops return errCacheDown
```

BrewFS 版本简化: 2 态 (Normal / Bypassed)，省略 Unstable 探测阶段。

**实现:**

- [ ] **Step 1: 创建 `brewfs/src/chunk/cache_health.rs`**

```rust
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub struct DiskHealth {
    error_count: AtomicU64,
    success_count: AtomicU64,
    bypassed: AtomicBool,
    last_error_time: AtomicU64,
    error_threshold: u64,       // 3
    recovery_threshold: u64,    // 10
    reset_window_secs: u64,     // 60
}

impl DiskHealth {
    pub fn new() -> Self {
        Self {
            error_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            bypassed: AtomicBool::new(false),
            last_error_time: AtomicU64::new(0),
            error_threshold: 3,
            recovery_threshold: 10,
            reset_window_secs: 60,
        }
    }

    pub fn is_bypassed(&self) -> bool {
        // Auto-reset error count after window
        if let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) {
            let last = self.last_error_time.load(Ordering::Relaxed);
            if last > 0 && now.as_secs() - last > self.reset_window_secs {
                self.error_count.store(0, Ordering::Relaxed);
            }
        }
        self.bypassed.load(Ordering::Relaxed)
    }

    pub fn record_error(&self) {
        if let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) {
            self.last_error_time.store(now.as_secs(), Ordering::Relaxed);
        }
        self.success_count.store(0, Ordering::Relaxed);
        let count = self.error_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= self.error_threshold {
            self.bypassed.store(true, Ordering::Relaxed);
            tracing::warn!(
                "Disk cache entering bypassed mode after {} errors",
                count
            );
        }
    }

    pub fn record_success(&self) {
        let count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= self.recovery_threshold && self.bypassed.load(Ordering::Relaxed) {
            self.bypassed.store(false, Ordering::Relaxed);
            self.error_count.store(0, Ordering::Relaxed);
            tracing::info!(
                "Disk cache recovered to normal mode after {} consecutive successes",
                count
            );
        }
    }
}
```

- [ ] **Step 2: 集成到 DiskStorage**

```rust
// In chunk/cache.rs, DiskStorage:
pub struct DiskStorage {
    // ... existing fields ...
    health: Arc<DiskHealth>,
}

impl DiskStorage {
    pub async fn store_with_health(
        &self, key: &str, data: &[u8],
    ) -> Result<(), anyhow::Error> {
        if self.health.is_bypassed() {
            return Ok(()); // Skip disk write in bypassed mode
        }

        match self.store_to_disk(key, data).await {
            Ok(()) => {
                self.health.record_success();
                Ok(())
            }
            Err(e) => {
                self.health.record_error();
                // Still return Ok to caller — disk cache failure
                // must never propagate to foreground I/O
                tracing::warn!(key = %key, error = %e, "disk cache write failed");
                Ok(())
            }
        }
    }

    pub async fn load_with_health(&self, key: &str) -> Result<Option<Vec<u8>>, anyhow::Error> {
        if self.health.is_bypassed() {
            return Ok(None); // Return miss — caller falls through to S3
        }

        match self.load_from_disk(key).await {
            Ok(Some(data)) => {
                self.health.record_success();
                Ok(Some(data))
            }
            Ok(None) => Ok(None), // Not in cache — not an error
            Err(e) => {
                self.health.record_error();
                Ok(None) // Treat as cache miss — don't propagate
            }
        }
    }
}
```

- [ ] **Step 3: 确保 disk cache 错误永不传播到前景 I/O**

审查所有 `DiskStorage` 调用点，确保:
- `store` 错误 → log + return Ok (bypass 或 swallowed)
- `load` 错误 → return None (caller falls through to S3)

- [ ] **Step 4: 测试**

```rust
#[tokio::test]
async fn test_disk_health_bypass_on_repeated_errors() {
    // Create DiskStorage pointing to a read-only directory
    // Attempt 4 writes → all fail
    // Verify: health.is_bypassed() == true
    // Verify: subsequent writes return Ok(()) (swallowed)
}

#[tokio::test]
async fn test_disk_health_recovery_after_successes() {
    // Set health to bypassed
    // Attempt 10 successful reads
    // Verify: health.is_bypassed() == false
}

#[tokio::test]
async fn test_disk_health_bypass_does_not_affect_reads() {
    // Bypassed DiskStorage
    // Show that block_cache.get() still returns Ok(None) (miss, not error)
    // Caller can still fall through to S3
}
```

- [ ] **Step 5: Commit**
```bash
git commit -m "feat(cache): add disk cache health state machine with bypass mode

Modeled after JuiceFS disk_cache_state.go:
- Normal: read/write cache with error counting
- Bypassed: skip disk after 3 errors in 60s, serve only from hot/page cache
- Recovery: 10 consecutive successes restore Normal mode
- Disk errors NEVER propagate to foreground I/O"
```

---

## Task 顺序与依赖

```
P1-4 (insert dedup) ──┐
                      ├── 可并行
P1-5 (promotion tune)─┘

P2-6 (read retry)   ── 独立，无依赖
P2-7 (tail prefetch)── 独立，无依赖

P3-8 (disk health)  ── 独立，无依赖 (但建议最后做，涉及错误语义变更)
```

推荐顺序: P1-4 → P1-5 → P2-6 → P2-7 → P3-8

---

## 验证命令

```bash
# 单元测试
cargo test -q chunk::cache::tests --lib
cargo test -q vfs::io::reader::tests --lib
cargo test -q chunk::cache_health --lib

# 性能回归
bash docker/compose-xfstests/run_redis_perf.sh --s3 --tools "fio-randread fio-seqread"

# 磁盘故障模拟 (P3-8)
# 创建只读 cache 目录，验证 bypassed 模式 + 前景读写不受影响
```

## 预期结果

| 指标 | P0 完成后 | P1/P2 完成后 | P3 完成后 |
|------|----------|-------------|----------|
| randread BW | 80+ MiB/s | 150+ MiB/s | 200+ MiB/s |
| seqread BW | 300+ MiB/s | 400+ MiB/s | 500+ MiB/s |
| cache hit rate | 60%+ | 75%+ | 80%+ |
| transient S3 error 恢复 | ❌ | ✅ retry | ✅ retry + cache bypass |
| disk failure 对前景 I/O 影响 | crash | crash | ✅ zero impact |
| insert_opportunistic 重复写 | 可能 | ✅ dedup | ✅ dedup |
