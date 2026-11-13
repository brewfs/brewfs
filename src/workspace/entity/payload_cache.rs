//! Mount-local, byte-bounded storage for successfully authenticated data only.
//!
//! Keys identify immutable bytes, not mutable paths. Callers retain all owner,
//! session, permission and current-map checks around hits as well as misses.

use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum Key {
    Lower {
        manifest: [u8; 32],
        ino: i64,
        offset: u64,
        length: usize,
    },
    Upper {
        volume: Uuid,
        namespace: Uuid,
        slice: u64,
        size: u64,
        digest: [u8; 32],
    },
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PayloadCacheStats {
    pub capacity_bytes: u64,
    pub retained_bytes: u64,
    pub entries: usize,
    pub lower_hits: u64,
    pub lower_misses: u64,
    pub upper_hits: u64,
    pub upper_misses: u64,
    pub evictions: u64,
    pub closed: bool,
}

// Charge keys, Arc headers, LRU/hash nodes and conservative allocator overhead
// in addition to exact owned payload lengths. Active caller copies are I/O
// memory, not retained cache residency.
const ENTRY_BYTES: u64 = 512;

struct State {
    entries: LruCache<Key, Arc<[u8]>>,
    stats: PayloadCacheStats,
}

pub(crate) struct PayloadCache {
    state: Mutex<State>,
}

impl PayloadCache {
    pub(crate) fn new(capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                entries: LruCache::unbounded(),
                stats: PayloadCacheStats {
                    capacity_bytes: capacity,
                    ..Default::default()
                },
            }),
        })
    }

    pub(crate) fn get(&self, key: &Key) -> Option<Arc<[u8]>> {
        let mut state = self.state.lock();
        if state.stats.closed {
            return None;
        }
        let value = state.entries.get(key).cloned();
        match (key, value.is_some()) {
            (Key::Lower { .. }, true) => state.stats.lower_hits += 1,
            (Key::Lower { .. }, false) => state.stats.lower_misses += 1,
            (Key::Upper { .. }, true) => state.stats.upper_hits += 1,
            (Key::Upper { .. }, false) => state.stats.upper_misses += 1,
        }
        value
    }

    /// Call only after complete authentication and successful lower-owner
    /// validation. No partial body, transport error or negative result enters.
    pub(crate) fn insert(&self, key: Key, bytes: &[u8]) {
        let Some(charge) = (bytes.len() as u64).checked_add(ENTRY_BYTES) else {
            return;
        };
        let mut state = self.state.lock();
        if state.stats.closed || bytes.is_empty() || charge > state.stats.capacity_bytes {
            return;
        }
        if state.entries.get(&key).is_some() {
            return;
        }
        // Evict before copying the incoming bytes. No asynchronous eviction
        // queue can let retained entries overshoot the configured byte limit.
        while state.stats.retained_bytes > state.stats.capacity_bytes - charge {
            let (_, old) = state.entries.pop_lru().expect("accounted cache entry");
            state.stats.retained_bytes -= old.len() as u64 + ENTRY_BYTES;
            state.stats.evictions += 1;
        }
        state.entries.put(key, Arc::from(bytes));
        state.stats.retained_bytes += charge;
        state.stats.entries = state.entries.len();
    }

    pub(crate) fn close(&self) {
        let mut state = self.state.lock();
        state.stats.closed = true;
        state.entries = LruCache::unbounded();
        state.stats.retained_bytes = 0;
        state.stats.entries = 0;
    }

    pub(crate) fn stats(&self) -> PayloadCacheStats {
        self.state.lock().stats.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lower(manifest: u8, offset: u64) -> Key {
        Key::Lower {
            manifest: [manifest; 32],
            ino: 2,
            offset,
            length: 4096,
        }
    }

    #[test]
    fn payload_cache_retains_verified_bytes_and_separates_lower_identity() {
        let cache = PayloadCache::new(16 << 10);
        let bytes = vec![7; 4096];
        cache.insert(lower(1, 0), &bytes);
        let retained = cache
            .get(&lower(1, 0))
            .expect("verified data must be retained");
        assert!(retained.as_ref() == bytes);
        assert!(cache.get(&lower(2, 0)).is_none());
        assert!(cache.get(&lower(1, 4096)).is_none());
    }

    #[test]
    fn payload_cache_evicts_lru_and_closes_without_invalidating_readers() {
        let cache = PayloadCache::new(2 * (4096 + ENTRY_BYTES));
        cache.insert(lower(1, 0), &[1; 4096]);
        cache.insert(lower(2, 0), &[2; 4096]);
        let pinned = cache.get(&lower(2, 0)).unwrap();
        assert!(cache.get(&lower(1, 0)).is_some());
        cache.insert(lower(3, 0), &[3; 4096]);
        assert!(cache.get(&lower(2, 0)).is_none());
        assert_eq!(pinned[0], 2);
        assert_eq!(cache.stats().entries, 2);
        assert_eq!(cache.stats().evictions, 1);
        assert!(cache.stats().retained_bytes <= cache.stats().capacity_bytes);
        cache.close();
        cache.insert(lower(4, 0), &[4; 4096]);
        assert!(cache.get(&lower(1, 0)).is_none());
        assert_eq!(cache.stats().retained_bytes, 0);
        assert_eq!(cache.stats().entries, 0);
        assert_eq!(pinned[4095], 2);
    }

    #[test]
    fn payload_cache_zero_budget_and_oversized_values_do_not_retain() {
        for capacity in [0, 4095 + ENTRY_BYTES] {
            let cache = PayloadCache::new(capacity);
            cache.insert(lower(1, 0), &[1; 4096]);
            assert!(cache.get(&lower(1, 0)).is_none());
            assert_eq!(cache.stats().retained_bytes, 0);
        }
    }

    #[test]
    fn payload_cache_upper_identity_and_shared_lower_upper_budget() {
        let cache = PayloadCache::new(4096 + ENTRY_BYTES);
        let (volume, namespace) = (Uuid::new_v4(), Uuid::new_v4());
        let key = Key::Upper {
            volume,
            namespace,
            slice: 1,
            size: 4096,
            digest: [7; 32],
        };
        cache.insert(key.clone(), &[7; 4096]);
        for other in [
            Key::Upper {
                volume: Uuid::new_v4(),
                namespace,
                slice: 1,
                size: 4096,
                digest: [7; 32],
            },
            Key::Upper {
                volume,
                namespace: Uuid::new_v4(),
                slice: 1,
                size: 4096,
                digest: [7; 32],
            },
            Key::Upper {
                volume,
                namespace,
                slice: 2,
                size: 4096,
                digest: [7; 32],
            },
            Key::Upper {
                volume,
                namespace,
                slice: 1,
                size: 4095,
                digest: [7; 32],
            },
            Key::Upper {
                volume,
                namespace,
                slice: 1,
                size: 4096,
                digest: [8; 32],
            },
        ] {
            assert!(cache.get(&other).is_none());
        }
        assert!(cache.get(&key).is_some());
        cache.insert(lower(1, 0), &[9; 4096]);
        assert!(cache.get(&key).is_none());
        assert!(cache.get(&lower(1, 0)).is_some());
        assert_eq!(cache.stats().retained_bytes, 4096 + ENTRY_BYTES);
    }

    #[test]
    fn payload_cache_concurrent_fills_stay_within_shared_budget() {
        let cache = PayloadCache::new(4 * (4096 + ENTRY_BYTES));
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let cache = cache.clone();
                scope.spawn(move || {
                    for offset in 0..64 {
                        let key = lower(worker, offset * 4096);
                        cache.insert(key.clone(), &[worker; 4096]);
                        if let Some(bytes) = cache.get(&key) {
                            assert!(bytes.iter().all(|byte| *byte == worker));
                        }
                        let stats = cache.stats();
                        assert!(stats.retained_bytes <= stats.capacity_bytes);
                    }
                });
            }
        });
    }
}
