#[allow(dead_code)]
// Kept for future cache integration; currently bypassed by ObjectBlockStore
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use super::keys::CleanBlockKey;
use super::read_cache::{CacheCounters, CacheStats, ReadCache};

struct CacheEntry {
    data: Vec<u8>,
    generation: u64,
}

/// LRU-based in-memory read cache with configurable capacity.
///
/// Uses a generation counter for O(1) access and amortized O(n) eviction.
/// When capacity is exceeded, the least-recently-accessed entries are evicted
/// in batch to avoid per-insert eviction overhead.
pub struct LruReadCache {
    entries: Mutex<HashMap<CleanBlockKey, CacheEntry>>,
    generation: AtomicU64,
    capacity_bytes: u64,
    current_bytes: AtomicU64,
    evictions: AtomicU64,
    counters: CacheCounters,
}

impl LruReadCache {
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
            capacity_bytes,
            current_bytes: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            counters: CacheCounters::new(),
        }
    }

    fn evict_if_needed(&self) {
        let current = self.current_bytes.load(Ordering::Relaxed);
        if current <= self.capacity_bytes {
            return;
        }

        let target = self.capacity_bytes * 3 / 4;
        let mut map = self.entries.lock();

        // Collect (key, generation) pairs and sort by generation (oldest first).
        let mut candidates: Vec<(CleanBlockKey, u64, usize)> = map
            .iter()
            .map(|(k, e)| (*k, e.generation, e.data.len()))
            .collect();
        candidates.sort_unstable_by_key(|&(_, g, _)| g);

        let mut freed = 0u64;
        let mut evicted = 0u64;
        let need_to_free = current.saturating_sub(target);

        for (key, _, size) in &candidates {
            if freed >= need_to_free {
                break;
            }
            map.remove(key);
            freed += *size as u64;
            evicted += 1;
        }

        self.current_bytes.fetch_sub(freed, Ordering::Relaxed);
        self.evictions.fetch_add(evicted, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl ReadCache for LruReadCache {
    async fn get_block(&self, key: &CleanBlockKey) -> Option<Vec<u8>> {
        let mut map = self.entries.lock();
        if let Some(entry) = map.get_mut(key) {
            entry.generation = self.generation.fetch_add(1, Ordering::Relaxed);
            self.counters.record_hit();
            Some(entry.data.clone())
        } else {
            self.counters.record_miss();
            None
        }
    }

    async fn put_block(&self, key: &CleanBlockKey, data: &[u8]) -> anyhow::Result<()> {
        let access_gen = self.generation.fetch_add(1, Ordering::Relaxed);
        let size = data.len() as u64;

        {
            let mut map = self.entries.lock();
            if let Some(existing) = map.get_mut(key) {
                existing.generation = access_gen;
                return Ok(());
            }
            map.insert(
                *key,
                CacheEntry {
                    data: data.to_vec(),
                    generation: access_gen,
                },
            );
        }

        self.current_bytes.fetch_add(size, Ordering::Relaxed);
        self.evict_if_needed();
        Ok(())
    }

    async fn remove_block(&self, key: &CleanBlockKey) -> anyhow::Result<()> {
        let mut map = self.entries.lock();
        if let Some(entry) = map.remove(key) {
            self.current_bytes
                .fetch_sub(entry.data.len() as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            bytes_cached: self.current_bytes.load(Ordering::Relaxed),
        }
    }
}
