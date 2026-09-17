//! Shared Unix-socket cache service (PR11, P3 preview).
//!
//! This module provides the type skeleton and capability flag for the
//! same-host shared frame cache described in spec 06 §5 and §11.  The
//! actual service is **not enabled by default** and the current
//! implementation is a typed placeholder that proves the protocol shape
//! without wiring live I/O.
//!
//! Why a stub?  The P1/P2 integration (PR07–PR09) must be stable before
//! we route reads through an out-of-process cache; enabling a shared
//! service prematurely would hide correctness bugs behind an extra layer.
//! When PR11 is promoted, the items below become real:
//!
//! - Unix-domain socket handshake with namespace + capability ACL.
//! - Per-frame singleflight across processes.
//! - Encoded-frame LRU with per-tenant byte budgets.
//! - Fallback to in-process cache when the service is unavailable.
//!
//! All of the above is gated by the `shared-host-cache` feature flag,
//! which is **not** in any default set.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::native_base::wire::refs::{Hash32, ObjectId};

/// Cache key for a single immutable encoded frame.
///
/// The full key binds namespace + object identity + byte range + digest so
/// a cache hit can never cross trust boundaries (spec 06 §5).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FrameCacheKey {
    pub namespace_id: [u8; 16],
    pub object_id: ObjectId,
    pub object_full_hash: Hash32,
    pub offset: u64,
    pub stored_len: u32,
    pub stored_digest: Hash32,
}

/// Outcome of a cache lookup attempt.
#[derive(Debug)]
pub enum CacheLookupResult {
    Hit(Vec<u8>),
    Miss,
    Unavailable,
}

/// In-process fallback cache used when the shared service is absent.
///
/// This is intentionally tiny — the real capacity management lives in
/// `planner` budget accounting and in the FUSE VFS cache.  This type
/// exists only so callers can depend on a uniform interface regardless
/// of whether `shared-host-cache` is compiled in.
pub struct InProcessFrameCache {
    inner: Mutex<HashMap<FrameCacheKey, Vec<u8>>>,
    max_entries: usize,
}

impl InProcessFrameCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max_entries,
        }
    }

    pub fn lookup(&self, key: &FrameCacheKey) -> CacheLookupResult {
        let guard = self.inner.lock().unwrap();
        match guard.get(key) {
            Some(bytes) => CacheLookupResult::Hit(bytes.clone()),
            None => CacheLookupResult::Miss,
        }
    }

    pub fn insert(&self, key: FrameCacheKey, bytes: Vec<u8>) {
        let mut guard = self.inner.lock().unwrap();
        if guard.len() >= self.max_entries && !guard.contains_key(&key) {
            // Simplistic: drop a random entry when full.  This is only a
            // fallback for the no-service case; the real shared service
            // uses proper LRU with byte budgets.
            if let Some(k) = guard.keys().next().cloned() {
                guard.remove(&k);
            }
        }
        guard.insert(key, bytes);
    }
}

/// Shared cache capability probe.
///
/// Returns `None` when the service has not been compiled in or the
/// socket is not reachable.  In this preview build it always returns
/// `Unavailable` so callers fall back to `InProcessFrameCache`.
pub fn probe_shared_cache(_socket_path: &std::path::Path) -> Option<()> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u8) -> FrameCacheKey {
        FrameCacheKey {
            namespace_id: [1; 16],
            object_id: [seed; 16],
            object_full_hash: [seed; 32],
            offset: 0,
            stored_len: 64,
            stored_digest: [seed; 32],
        }
    }

    #[test]
    fn in_process_cache_hit_and_miss() {
        let cache = InProcessFrameCache::new(16);
        let k = key(1);
        assert!(matches!(cache.lookup(&k), CacheLookupResult::Miss));
        cache.insert(k.clone(), vec![42; 64]);
        match cache.lookup(&k) {
            CacheLookupResult::Hit(bytes) => assert_eq!(bytes.len(), 64),
            _ => panic!("expected hit"),
        }
    }

    #[test]
    fn in_process_cache_bounded() {
        let cache = InProcessFrameCache::new(4);
        for i in 0..8u8 {
            cache.insert(key(i), vec![i; 16]);
        }
        let guard = cache.inner.lock().unwrap();
        assert_eq!(guard.len(), 4, "cache must stay at or below max_entries");
    }

    #[test]
    fn different_namespace_keys_do_not_collide() {
        let cache = InProcessFrameCache::new(16);
        let mut k1 = key(1);
        let mut k2 = key(1);
        k2.namespace_id = [9; 16];
        cache.insert(k1.clone(), vec![1; 16]);
        assert!(
            matches!(cache.lookup(&k2), CacheLookupResult::Miss),
            "different namespace must be a miss"
        );
    }

    #[test]
    fn shared_cache_probe_is_unavailable_in_preview() {
        assert!(probe_shared_cache(std::path::Path::new("/nonexistent")).is_none());
    }
}
