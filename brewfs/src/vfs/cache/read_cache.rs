#[allow(dead_code)]
// Kept for future cache integration; currently bypassed by ObjectBlockStore
use std::sync::atomic::{AtomicU64, Ordering};

#[allow(dead_code)]
use super::keys::CleanBlockKey;

/// Statistics for the read cache.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub bytes_cached: u64,
}

/// Trait for a global read-through block cache.
///
/// Implementations provide L1 (memory) and optionally L2 (SSD) caching for
/// committed, immutable blocks. Since committed blocks never change, no
/// invalidation is needed for overwrites — new writes create new slice IDs.
#[async_trait::async_trait]
pub trait ReadCache: Send + Sync {
    async fn get_block(&self, key: &CleanBlockKey) -> Option<Vec<u8>>;
    async fn put_block(&self, key: &CleanBlockKey, data: &[u8]) -> anyhow::Result<()>;
    async fn remove_block(&self, key: &CleanBlockKey) -> anyhow::Result<()>;
    fn stats(&self) -> CacheStats;
}

/// Atomic counters for cache hit/miss tracking.
pub(crate) struct CacheCounters {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

impl CacheCounters {
    pub fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
}
