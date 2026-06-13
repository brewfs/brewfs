# BrewFS Performance Optimization Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the 8-42x read and 338x metadata performance gap vs JuiceFS by implementing block-level disk cache, inline attribute caching, and adaptive readahead improvements.

**Architecture:** Four-phase implementation. Phase 1 (P0) targets the 338x stat gap with attr cache and the 8-42x read gap with block cache + CRC32C. Phase 2 (P1) optimizes readahead and write buffering. Phase 3 (P2) adds inline compaction triggers and commit ordering. Phase 4 (P3) adds writeback staging refinements and slice ref counting.

**Tech Stack:** Rust, tokio, moka cache, Redis, S3 API, FUSE3 (asyncfuse).

**Analysis based on:** `doc/juicefs/` documentation (7 files) and thorough BrewFS source code review.

---

## Gap Summary

| # | Gap | Bench Impact | Code Location |
|---|-----|-------------|---------------|
| P0-1 | No inline attr cache → stat 338x slower | **stat 3K vs 1M ops/s** | `meta/client/cache.rs`, `meta/stores/redis/mod.rs:1151` |
| P0-2 | No block-level disk cache with integrity | **read 8-42x slower** | `chunk/cache.rs:362-381`, `chunk/store.rs:389-492` |
| P0-3 | Unused LruReadCache wastes 256MB RAM | 256MB waste | `vfs/fs/mod.rs:351-353`, `vfs/cache/lru_cache.rs:19-129` |
| P0-4 | Page cache may store compressed bytes without decompression | Data corruption risk | `chunk/store.rs:410-419` |
| P1-1 | SingleFlight only on full blocks, not pages | Concurrent reads redundant | `chunk/store.rs:389-446` |
| P1-2 | No background full-block prefetch after partial range reads | Extra network round-trips | `chunk/store.rs:399-423` |
| P1-3 | Prefetch silently drops on full queue (try_send) | Unreliable prefetch | `vfs/cache/prefetch.rs:143` |
| P1-4 | No eager block-completion flush (4MB threshold) | Write buffering delay | `vfs/io/writer.rs:1485-1526` |
| P1-5 | No tail prefetch (last 32KB of file) | Small file read latency | `vfs/io/reader.rs:693-703` |
| P2-1 | No inline compaction trigger from write path | Fragmentation accumulates 10min | `chunk/compact/compactor.rs:83-95` |
| P2-2 | No cross-chunk commit ordering | Readers see partial views | `vfs/io/writer.rs:1733-1764` |
| P2-3 | Compaction always rewrites all data (no skip-some) | Wasteful heavy compaction | `chunk/compact/compactor.rs:187-270` |
| P3-1 | No UploadDelay/UploadHours in writeback | No upload scheduling | `vfs/cache/write_back.rs` |
| P3-2 | No slice reference counting (sliceRefs) | Orphan block detection harder | (no equivalent) |
| P3-3 | Session heartbeat 60s vs 12s | Slow failure detection | `meta/client/session.rs:102` |

---

## Phase 1: P0 — Close the Biggest Gaps

### Task 1: Remove Dead LruReadCache (256MB Waste)

**Files:**
- Modify: `src/vfs/fs/mod.rs:345-355`
- Modify: `src/vfs/cache/lru_cache.rs` (keep file, mark as dead)
- Modify: `src/vfs/cache/read_cache.rs` (keep trait, mark as unused)

**Context:** `VfsState` creates `LruReadCache` with 256MB but the entire read path uses `ObjectBlockStore` which uses `ChunksCache`. The `ReadCache` trait's `get_block`/`put_block`/`remove_block` have zero callers. 256MB is wasted.

- [ ] **Step 1: Verify dead code**
```bash
cd /mnt/rk8s/project/brewfs && rg "get_block|put_block|remove_block|ReadCache|LruReadCache" src/ --no-filename
```
Expected: Only found in `vfs/cache/` and `vfs/fs/mod.rs` construction site; zero callers in actual I/O paths.

- [ ] **Step 2: Remove construction from VfsState**
Edit `src/vfs/fs/mod.rs`, remove lines ~345-355:
```rust
// REMOVED: LruReadCache was never used — entire read path goes through ObjectBlockStore
let read_cache = ReadCacheConfig {
    max_bytes: 256 * 1024 * 1024,
    ..Default::default()
};
let read_cache = LruReadCache::new(read_cache);
```
Replace the `read_cache: read_cache.clone()` field with `read_cache: NoopReadCache` or remove the field if the struct allows.

- [ ] **Step 3: Verify read path still works without LruReadCache**
```rust
// In vfs/fs/mod.rs, remove the read_cache field from VfsState if possible,
// or replace with a no-op stub that implements the ReadCache trait.
// Verify compilation:
```
```bash
cd /mnt/rk8s/project/brewfs && cargo check -p brewfs 2>&1 | head -20
```
Expected: No errors. Warning about unused `ReadCache` trait is acceptable.

- [ ] **Step 4: Add #[allow(dead_code)] to ReadCache trait and LruReadCache**
```rust
// In vfs/cache/read_cache.rs, add at top:
#[allow(dead_code)] // Kept for future block-cache integration; currently unused
pub trait ReadCache { ... }

// In vfs/cache/lru_cache.rs, add at top:
#[allow(dead_code)] // Kept for future block-cache integration; currently unused
pub struct LruReadCache { ... }
```

- [ ] **Step 5: Commit**
```bash
git add src/vfs/fs/mod.rs src/vfs/cache/
git commit -m "perf(vfs): remove unused LruReadCache allocation, free 256MB

The LruReadCache was allocated at 256MB but had zero callers — the entire
read path goes through ObjectBlockStore's ChunksCache instead. Kept the
code as dead code behind #[allow(dead_code)] for future cache integration."
```

---

### Task 2: Inline Attribute Cache — stat From 3K to 500K+ ops/s

**Files:**
- Modify: `src/meta/client/cache.rs:94-102` (InodeCache)
- Modify: `src/meta/stores/redis/mod.rs:1151-1153` (node_cache TTL)
- Modify: `src/meta/client/mod.rs:884-903` (cached_stat path)
- Modify: `src/vfs/fs/mod.rs` (FUSE attr timeout)

**Context:** RedisMetaStore's `node_cache` has 2s TTL (line 1153) which is too short. The `InodeCache` uses Moka with LRU eviction but `getattr` calls go through `cached_stat()` which checks `inode_cache.get_attr(inode)` — but this only works if the entry was populated by a prior `getattr()` or `lookup()`. After 2s, the Redis node cache expires and the next stat re-fetches from Redis. JuiceFS uses Redis CSC with per-handle attribute caching on open.

The fix: Extend node_cache TTL, add explicit attr invalidation on write, and use FUSE kernel cache timeouts properly.

- [ ] **Step 1: Extend Redis MetaStore node_cache TTL from 2s to 30s**
Edit `src/meta/stores/redis/mod.rs:1151-1153`:
```rust
// Before:
let node_cache: Cache<i64, StoredNode> = Cache::builder()
    .time_to_live(Duration::from_secs(2))
    .max_capacity(10_000)
    .build();

// After: extend TTL, add write-through invalidation
let node_cache: Cache<i64, StoredNode> = Cache::builder()
    .time_to_live(Duration::from_secs(30))  // was 2s
    .max_capacity(100_000)                   // was 10k
    .build();
```

- [ ] **Step 2: Add cache invalidation on every metadata write**
In `redis/mod.rs`, find all `SET i{inode}` sites (mknod, write, setattr, unlink, rmdir, rename) and add `self.node_cache.remove(&inode)` after the SET. Example for `do_setattr`:
```rust
// In the Lua script execution result handler for setattr:
self.node_cache.remove(&inode);  // Invalidate stale cached StoredNode
```

Search for `SET i{inode}` patterns and add invalidation:
```bash
cd /mnt/rk8s/project/brewfs && rg -n "format.*\bi\b.*\{" src/meta/stores/redis/mod.rs | head -20
```

- [ ] **Step 3: Extend FUSE kernel attribute cache timeout**
Edit `src/vfs/fs/mod.rs` — find where FUSE `attr_timeout` is set (likely in the mount options or `getattr` response). Set to 1.0s (matching JuiceFS default):
```rust
// In the getattr handler response:
reply.attr_timeout(Duration::from_secs(1));  // Was likely 0 or very short
```

- [ ] **Step 4: Hook into open() to cache attributes per handle**
Find the `open()` handler in `src/vfs/fs/mod.rs` and ensure it populates the `InodeCache`:
```rust
fn open(&self, ino: u64, flags: u32) -> Result<Opened> {
    // Populate attr cache on open — subsequent stat calls will hit cache
    let attr = self.meta.getattr(ino).await?;
    self.inode_cache.cache_attr(ino, attr);  // New method
    Ok(Opened { fh: self.next_fh(), flags })
}
```

- [ ] **Step 5: Run benchmark to verify stat improvement**
```bash
bash docker/compose-xfstests/run_redis_perf.sh --s3 --tools "metaperf"
```
Expected: stat ops/sec increases from ~3,000 to >100,000.

- [ ] **Step 6: Commit**
```bash
git add src/meta/
git commit -m "perf(meta): extend attr cache TTL to 30s, invalidate on write, cache on open

- Redis node_cache TTL: 2s → 30s, capacity: 10k → 100k
- Invalidate StoredNode cache on all SET i{inode} operations
- Extend FUSE attr_timeout to 1.0s
- Cache attributes in InodeCache on file open

Target: stat ops/sec 3K → 100K+ (35x improvement)"
```

---

### Task 3: Block-Level Disk Cache with CRC32C Verification

**Files:**
- Create: `src/chunk/cache_integrity.rs`
- Modify: `src/chunk/cache.rs:362-381` (DiskStorage::load — add CRC32C verify)
- Modify: `src/chunk/cache.rs:260-290` (DiskStorage::store — add CRC32C write)
- Modify: `src/chunk/store.rs:478-481` (async cache population)

**Context:** `ChunksCache::DiskStorage` reads cache files with no integrity check (line 362-381: `tokio::fs::read(filepath)`). JuiceFS writes CRC32C checksums per 32KB block and verifies on read. BrewFS must add this to prevent silent data corruption. Cache file format needs to change from raw data to `[data][checksums]`.

- [ ] **Step 1: Define cache file format**
In `src/chunk/cache_integrity.rs`:
```rust
use crc32c::crc32c;

/// Block size for checksum calculation (32KB, matching JuiceFS)
const CS_BLOCK: usize = 32 * 1024;

/// Compute CRC32C checksums for data, one u32 per CS_BLOCK
pub fn compute_checksums(data: &[u8]) -> Vec<u8> {
    let num_blocks = (data.len() + CS_BLOCK - 1) / CS_BLOCK;
    let mut checksums = Vec::with_capacity(num_blocks * 4);
    for i in 0..num_blocks {
        let start = i * CS_BLOCK;
        let end = std::cmp::min(start + CS_BLOCK, data.len());
        let cs = crc32c(&data[start..end]);
        checksums.extend_from_slice(&cs.to_le_bytes());
    }
    checksums
}

/// Verify CRC32C checksums for data, return true if all match
pub fn verify_checksums(data: &[u8], checksums: &[u8]) -> bool {
    let num_blocks = (data.len() + CS_BLOCK - 1) / CS_BLOCK;
    if checksums.len() != num_blocks * 4 {
        return false;
    }
    for i in 0..num_blocks {
        let start = i * CS_BLOCK;
        let end = std::cmp::min(start + CS_BLOCK, data.len());
        let expected = u32::from_le_bytes([
            checksums[i*4], checksums[i*4+1], checksums[i*4+2], checksums[i*4+3]
        ]);
        if crc32c(&data[start..end]) != expected {
            return false;
        }
    }
    true
}

/// Append checksums to data for storage
pub fn encode_cache_file(data: &[u8]) -> Vec<u8> {
    let checksums = compute_checksums(data);
    let mut result = Vec::with_capacity(data.len() + checksums.len());
    result.extend_from_slice(data);
    result.extend_from_slice(&checksums);
    result
}

/// Split cache file into (data, checksums)
pub fn decode_cache_file(raw: &[u8], data_len: usize) -> Option<(&[u8], &[u8])> {
    let checksum_len = ((data_len + CS_BLOCK - 1) / CS_BLOCK) * 4;
    if raw.len() < data_len + checksum_len {
        return None;
    }
    Some((&raw[..data_len], &raw[data_len..data_len+checksum_len]))
}
```

Add `crc32c` to `Cargo.toml`:
```toml
crc32c = "0.6"
```

- [ ] **Step 2: Update DiskStorage::store to write CRC32C**
In `src/chunk/cache.rs`, modify `DiskStorage::store()` (~line 260):
```rust
async fn store(&self, key: &BlockKey, data: &[u8]) -> Result<()> {
    let filepath = self.filepath(key);
    let encoded = cache_integrity::encode_cache_file(data);
    tokio::fs::write(&filepath, &encoded).await?;
    Ok(())
}
```

- [ ] **Step 3: Update DiskStorage::load to verify CRC32C**
In `src/chunk/cache.rs`, modify `DiskStorage::load()` (~line 362):
```rust
async fn load(&self, key: &BlockKey, expected_len: usize) -> Result<Vec<u8>> {
    let filepath = self.filepath(key);
    let raw = tokio::fs::read(&filepath).await?;
    if let Some((data, checksums)) = cache_integrity::decode_cache_file(&raw, expected_len) {
        if cache_integrity::verify_checksums(data, checksums) {
            return Ok(data.to_vec());
        }
        // Corrupted — delete and return error
        let _ = tokio::fs::remove_file(&filepath).await;
        return Err(anyhow::anyhow!("CRC32C verification failed for {}", filepath.display()));
    }
    // Legacy format (no checksums) — accept but log warning
    tracing::warn!("Cache file {} has no checksums, accepting without verification", filepath.display());
    Ok(raw)
}
```

- [ ] **Step 4: Make cache population async in read path**
In `src/chunk/store.rs:478-481`, change synchronous cache insert to spawn background:
```rust
// Before:
if self.config.cache_data {
    self.block_cache.insert(&block_key, decompressed.clone()).await;
}

// After:
if self.config.cache_data {
    let cache = self.block_cache.clone();
    let key = block_key.clone();
    let data = decompressed.clone();
    tokio::spawn(async move {
        cache.insert(&key, &data).await;
    });
}
```

- [ ] **Step 5: Add cache integrity unit test**
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_roundtrip() {
        let data = vec![0xABu8; 100_000];
        let checksums = cache_integrity::compute_checksums(&data);
        assert!(cache_integrity::verify_checksums(&data, &checksums));

        // Corrupt one byte
        let mut corrupted = data.clone();
        corrupted[50000] ^= 0xFF;
        assert!(!cache_integrity::verify_checksums(&corrupted, &checksums));
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let data = vec![0x42u8; 65536];
        let encoded = cache_integrity::encode_cache_file(&data);
        let (decoded, checksums) = cache_integrity::decode_cache_file(&encoded, data.len()).unwrap();
        assert_eq!(decoded, &data[..]);
        assert!(cache_integrity::verify_checksums(decoded, checksums));
    }
}
```

- [ ] **Step 6: Run tests**
```bash
cd /mnt/rk8s/project/brewfs && cargo test -p brewfs -- chunk::cache_integrity
```
Expected: All tests pass.

- [ ] **Step 7: Commit**
```bash
git add Cargo.toml src/chunk/
git commit -m "perf(chunk): add CRC32C verification to disk block cache

- New cache_integrity module: encode/decode/verify CRC32C checksums
- DiskStorage stores [data][checksums] format (32KB block granularity)
- Corrupted cache files are detected and deleted on load
- Legacy files without checksums are accepted with a warning
- Cache population in read path is now async (spawn background)

Prevents silent data corruption from bit rot / torn writes."
```

---

### Task 4: Fix Page Cache Compression Bug

**Files:**
- Modify: `src/chunk/store.rs:389-446` (small-range read path)

**Context:** `ObjectBlockStore::read_range()` for small reads fetches raw bytes via `get_object_range()` and stores them in page cache WITHOUT decompressing (line 410-419). If compression is enabled, the stored object on S3 is compressed, and a Range GET returns compressed bytes. The caller expects uncompressed data. Fix: either decompress after range GET, or store a flag indicating compression state.

Since the default config uses `Compression::None` in `CacheConfig`, this may not trigger in current usage. But the `BlockStoreConfig::default()` at `store.rs:181` defaults to `Compression::Lz4`, which creates inconsistency.

- [ ] **Step 1: Verify current compression state**
```bash
cd /mnt/rk8s/project/brewfs && rg "compression" src/chunk/store.rs | head -10
```
Check: is `BlockStoreConfig::default()` actually used, or does the FUSE layer override compression to `None`?

- [ ] **Step 2: If compression enabled, decompress in range-read path**
In `src/chunk/store.rs:410-419`, after `get_object_range`:
```rust
let read_len = client
    .get_object_range(&key_str, range_offset, &mut page_buf)
    .await
    .map_err(|e| anyhow::anyhow!("object store range read failed: {key_str}, {e:?}"))?;
page_buf.truncate(read_len);

// NEW: Decompress if compression is enabled
let page_bytes = if !matches!(self.config.compression, Compression::None) {
    let decompressed = crate::chunk::compress::decompress(&page_buf)?;
    Bytes::from(decompressed)
} else {
    Bytes::from(page_buf)
};
page_cache.insert(cache_key, page_bytes.clone()).await;
```

- [ ] **Step 3: Or, simply disable range-reads when compression is active**
Simpler fix — if compressed, always take the full-block path (which decompresses):
```rust
let use_range_read = !matches!(self.config.compression, Compression::None)  // NEW: no range reads when compressed
    || (range_len as u64) < range_read_threshold;
```

- [ ] **Step 4: Add test for compressed range read**
```rust
#[tokio::test]
async fn test_compressed_range_read_returns_decompressed() {
    // Setup store with Lz4 compression
    // Write known data
    // Read via range (small read path)
    // Assert data matches original (decompressed correctly)
}
```

- [ ] **Step 5: Commit**
```bash
git add src/chunk/store.rs
git commit -m "fix(chunk): decompress bytes in small-range read path when compression enabled

Range GET returns raw stored bytes. If compression is Lz4/Zstd, these bytes
are compressed and the caller expects uncompressed data. Added decompression
step after range GET when compression != None.

Alternative: simply skip range-read path when compression is active."
```

---

## Phase 2: P1 — Read Prefetch & Write Buffering

### Task 5: Add SingleFlight to Page-Level Reads

**Files:**
- Modify: `src/chunk/store.rs:147` (add page-level SingleFlight)
- Modify: `src/chunk/store.rs:389-446` (page read path)

- [ ] **Step 1: Add page_key SingleFlight to ObjectBlockStore**
```rust
// In chunk/store.rs, add to ObjectBlockStore struct:
page_flight: SingleFlight<(String, u64, usize), Bytes>,  // (key, offset, len) → data
```

- [ ] **Step 2: Wrap page fetch in page_flight.execute()**
```rust
// In read_range, page-miss path:
let page_key = (key_str.clone(), range_offset, range_len);
let page_bytes = self.page_flight
    .execute(page_key, || async {
        let mut page_buf = vec![0u8; range_len];
        let read_len = client
            .get_object_range(&key_str, range_offset, &mut page_buf)
            .await?;
        page_buf.truncate(read_len);
        Ok(Bytes::from(page_buf))
    })
    .await?;
page_cache.insert(cache_key, page_bytes.clone()).await;
```

- [ ] **Step 3: Commit**
```bash
git commit -m "perf(chunk): add SingleFlight dedup to page-level object reads"
```

---

### Task 6: Background Full-Block Prefetch After Partial Reads

**Files:**
- Modify: `src/chunk/store.rs:440-446` (after page cache insert, trigger prefetch)
- Modify: `src/vfs/cache/prefetch.rs:56-149` (ensure prefetcher can handle block keys)

- [ ] **Step 1: Trigger full-block prefetch after partial range read**
After `page_cache.insert()` in the small-range path:
```rust
// Trigger async prefetch of the remaining block
if self.config.prefetch_on_range_read {
    let prefetch_store = self.clone();
    let prefetch_key = block_key.clone();
    tokio::spawn(async move {
        let mut buf = vec![0u8; prefetch_store.config.block_size as usize];
        if let Ok(n) = prefetch_store.read_full_block(&prefetch_key, &mut buf).await {
            tracing::debug!("Prefetched full block after range read: {}", prefetch_key);
        }
    });
}
```

- [ ] **Step 2: Add config flag**
```rust
// In BlockStoreConfig:
prefetch_on_range_read: bool,  // default true
```

- [ ] **Step 3: Commit**
```bash
git commit -m "perf(chunk): trigger full-block prefetch after partial range reads"
```

---

### Task 7: Fix Prefetch — Replace try_send with Blocking Send or Backpressure

**Files:**
- Modify: `src/vfs/cache/prefetch.rs:143`

- [ ] **Step 1: Change try_send to send with bounded timeout**
```rust
// Before:
tx.try_send(task).ok();  // Silently drops

// After:
match tx.send_timeout(task, Duration::from_millis(100)).await {
    Ok(()) => {} // Success
    Err(_) => {
        metrics::prefetch_dropped.inc();
        // Signal backpressure to readahead logic
    }
}
```

- [ ] **Step 2: Expose backpressure signal to readahead**
Add an atomic counter `prefetch_queue_len` that the readahead logic checks:
```rust
// In GlobalPrefetcher:
pub fn queue_len(&self) -> usize {
    self.queue_len.load(Ordering::Relaxed)
}

// In checkReadahead (reader.rs):
if self.prefetcher.queue_len() > 900 {  // 90% full
    return;  // Stop expanding readahead window
}
```

- [ ] **Step 3: Commit**
```bash
git commit -m "fix(prefetch): replace silent try_send drop with timeout send + backpressure signal"
```

---

### Task 8: Eager Block-Completion Flush (4MB Threshold)

**Files:**
- Modify: `src/vfs/io/writer.rs:266-270` (has_idle_block)
- Modify: `src/vfs/io/writer.rs:1485-1526` (spawn_upload_task)

- [ ] **Step 1: Add per-block completion check in write_at**
After writing data to a slice, check if any individual block is complete:
```rust
// In write_at_inner, after CacheSlice::write_at / append:
let block_offset = self.offset / self.config.block_size;
if self.data.is_block_full(block_offset) {  // New method
    // Trigger upload for just this block
    self.upload_single_block(block_offset).await?;
    self.dispatched_end = (block_offset + 1) * self.config.block_size as u64;
}
```

- [ ] **Step 2: Add is_block_full to CacheSlice**
```rust
// In vfs/cache/page.rs:
pub fn is_block_full(&self, block_index: u64) -> bool {
    let block_start = (block_index as usize) * self.pages_per_block();
    let block_end = block_start + self.pages_per_block();
    self.pages[block_start..block_end].iter().all(|p| p.is_some())
}
```

- [ ] **Step 3: Run write benchmark**
```bash
bash docker/compose-xfstests/run_redis_perf.sh --s3 --tools "fio-bigwrite"
```
Expected: Write throughput stable or improved, lower memory pressure.

- [ ] **Step 4: Commit**
```bash
git commit -m "perf(writer): add eager block-completion flush at 4MB boundary

When a 4MB block fills, trigger immediate upload instead of waiting
for auto_flush (500ms+) or freeze. Matches JuiceFS FlushTo() behavior."
```

---

### Task 9: Tail Prefetch (Last 32KB of File)

**Files:**
- Modify: `src/vfs/io/reader.rs:693-703` (prepare_ahead_slices)

- [ ] **Step 1: Add tail prefetch in checkReadahead or prepare_ahead_slices**
```rust
// In prepare_ahead_slices, after normal readahead:
let file_length = self.length.load(Ordering::Acquire);
if file_length > 32 * 1024 {
    let tail_start = file_length - 32 * 1024;
    let tail_block = self.layout.block_of(tail_start)?;
    // Create a prefetch task for the last block if not already covered
    if !self.is_block_cached(tail_block) {
        self.submit_prefetch(tail_block).await;
    }
}
```

- [ ] **Step 2: Commit**
```bash
git commit -m "perf(reader): add tail prefetch for last 32KB of file"
```

---

## Phase 3: P2 — Compaction & Commit Ordering

### Task 10: Inline Compaction Trigger from Write Path

**Files:**
- Modify: `src/vfs/io/writer.rs:579-640` (try_commit)
- Modify: `src/chunk/compact/worker.rs:177-274`

- [ ] **Step 1: Check slice count after commit and trigger compaction**
In `try_commit()` or `commit_chunk()`, after successful slice append:
```rust
let num_slices = self.get_slice_count(chunk_id).await?;
if num_slices >= 2500 {
    // Force sync compaction — blocks write
    self.compactor.compact_sync(inode, chunk_id).await?;
} else if num_slices % 100 == 99 || num_slices > 350 {
    // Background async compaction
    let compactor = self.compactor.clone();
    tokio::spawn(async move {
        compactor.compact_async(inode, chunk_id).await;
    });
}
```

- [ ] **Step 2: Add compact_sync to Compactor**
```rust
pub async fn compact_sync(&self, inode: u64, chunk_id: u64) -> Result<()> {
    // Acquire lock, compact, CAS replace slices
    // Blocks caller until complete
}
```

- [ ] **Step 3: Commit**
```bash
git commit -m "perf(compact): trigger compaction from write path when slice count exceeds thresholds"
```

---

### Task 11: Cross-Chunk Commit Ordering

**Files:**
- Modify: `src/vfs/io/writer.rs:1733-1764` (commit_chunk)

- [ ] **Step 1: Track dependency on previous chunk's last slice**
```rust
// In commit_chunk, for growing slices (extending file):
if slice.is_growing {
    // Wait for previous chunk's last slice to be committed
    let prev_chunk_id = chunk_id.saturating_sub(1);  // Naive, chunk IDs may not be sequential
    if let Some(dep_slice) = self.get_last_slice(prev_chunk_id).await {
        while !dep_slice.committed.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
```

- [ ] **Step 2: Commit**
```bash
git commit -m "fix(writer): enforce cross-chunk commit ordering for growing files"
```

---

## Phase 4: P3 — Writeback Refinement

### Task 12: Writeback UploadDelay and Staging Cooldown

**Files:**
- Modify: `src/vfs/cache/write_back.rs:60-212`
- Modify: `src/vfs/cache/config.rs`

- [ ] **Step 1: Add UploadDelay config**
```rust
// In WriteBackConfig:
pub struct WriteBackConfig {
    pub enabled: bool,
    pub upload_delay: Duration,        // Minimum time before upload (default 0)
    pub upload_hours: Option<(u8, u8)>, // Optional upload time window
    pub staging_cooldown: Duration,     // Cooldown before caching staged blocks
    pub threshold_size: usize,         // Blocks > this skip staging (default = block_size)
}
```

- [ ] **Step 2: Implement delayed upload in persist path**
After `wb.persist_slice()`, instead of uploading immediately:
```rust
if let Some(delay) = upload_delay {
    let staged = StagedSlice { key, path, added: Instant::now() };
    self.pending_staging.lock().push(staged);
    // Background scanner picks up after delay
} else {
    self.upload_now(key, path).await;
}
```

- [ ] **Step 3: Add background delayed staging scanner**
```rust
async fn scan_delayed_staging(&self) {
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        let mut pending = self.pending_staging.lock();
        pending.retain(|s| {
            if s.added.elapsed() >= self.config.upload_delay
               && self.within_upload_hours() {
                self.upload_now(&s.key, &s.path);
                false  // Remove from pending
            } else {
                true  // Keep waiting
            }
        });
    }
}
```

- [ ] **Step 4: Commit**
```bash
git commit -m "feat(writeback): add UploadDelay, UploadHours, and staging cooldown"
```

---

## Test Plan

After each phase, run the full benchmark suite:

```bash
# Phase 1 verification
bash docker/compose-xfstests/run_redis_perf.sh --s3 --tools "metaperf fio-seqread fio-randread"

# Phase 2 verification
bash docker/compose-xfstests/run_redis_perf.sh --s3

# Full benchmark (all 11 tools)
bash docker/compose-xfstests/run_redis_perf.sh --s3
```

Expected progression:

| Metric | Current | After P0 | After P1 | After P2 | Target (JuiceFS) |
|--------|---------|----------|----------|----------|-----------------|
| stat ops/s | 3,061 | 100K+ | 100K+ | 100K+ | 1,035,890 |
| seqread BW | 134 MiB/s | 300 MiB/s | 500 MiB/s | 500 MiB/s | 1.1 GiB/s |
| randread BW | 29 MiB/s | 100 MiB/s | 300 MiB/s | 300 MiB/s | 1.2 GiB/s |
| randwrite BW | 55 MiB/s | 55 MiB/s | 80 MiB/s | 80 MiB/s | 184 MiB/s |
| Memory saved | — | 256MB | — | — | — |
