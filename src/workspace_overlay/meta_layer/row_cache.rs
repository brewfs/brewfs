//! Short-lived cache of writable-layer (upper) rows for read-only paths.
//!
//! Opt-in per mount (`WorkspaceMetaLayer::with_upper_row_cache`). The
//! workspace lease is exclusive, so while it is held the mounted layer is
//! the only writer of upper rows; every local commit invalidates the keys of
//! the rows it writes, every view change invalidates the whole cache, and a
//! generation counter keeps a read that raced any invalidation from leaving
//! pre-commit rows behind. Under a proven lease the TTL bounds retention,
//! not staleness. Canceled or uncertain commits permanently disable cached
//! authority. Conditional mutations still authenticate their committed view.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use moka::sync::Cache;

use crate::workspace_overlay::catalog::PermissionSnapshot;
use crate::workspace_overlay::model::{DentryDelta, InodeDelta, XattrDelta};

const CAPACITY: u64 = 65_536;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum RowKey {
    Inode(i64),
    Dentry(i64, Vec<u8>),
    Xattr(i64, Vec<u8>),
    Permission(i64),
    Paths(i64),
    Aliases(i64),
}

#[derive(Clone)]
pub(super) enum RowValue {
    Inode(Arc<Vec<InodeDelta>>),
    Dentry(Arc<Vec<DentryDelta>>),
    Xattr(Arc<Vec<XattrDelta>>),
    Permission(Arc<PermissionSnapshot>),
    /// Complete effective paths of one inode. Only successful, complete
    /// walks are cached; quota/cycle/view errors never are.
    Paths(Arc<Vec<Vec<u8>>>),
    /// Verified effective (parent, name) aliases of one inode. Lives in the
    /// namespace cache: only commits that write dentries or delete an inode
    /// can change it, so data-only commits keep it.
    Aliases(Arc<Vec<(i64, Vec<u8>)>>),
}

/// Re-checks a lease-proven read fence: the exclusive lease is still proven
/// held and this inode's cached rows still equal the rows the read used. Any
/// local commit to the inode removes its cached rows (before and after the
/// commit), so equality means the store's rows are unchanged.
pub(crate) struct LeaseLocalFence {
    pub(super) cache: Arc<UpperRowCache>,
    pub(super) validity: Arc<crate::workspace_overlay::lifecycle::LeaseValidity>,
}

impl LeaseLocalFence {
    pub(crate) fn unchanged(
        &self,
        ino: i64,
        expected: &[crate::workspace_overlay::model::InodeDelta],
    ) -> bool {
        let same = matches!(
            self.cache.get(&RowKey::Inode(ino)),
            Some(RowValue::Inode(rows)) if rows.as_slice() == expected
        );
        same && self.validity.authoritative()
    }
}

pub(super) struct UpperRowCache {
    rows: Cache<RowKey, RowValue>,
    generation: AtomicU64,
    /// A canceled/uncertain backend write can still complete after the caller
    /// drops. No later refill may claim local authority in that mount.
    disabled: AtomicBool,
    counters: Arc<RowCacheCounters>,
}

#[derive(Debug, Default)]
pub(super) struct RowCacheCounters {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

#[derive(Debug)]
pub(super) struct RowCacheStatsExtension {
    pub counters: Arc<RowCacheCounters>,
}

impl crate::vfs::stats::FsStatsExtension for RowCacheStatsExtension {
    fn render_max_bytes(&self) -> usize {
        256
    }

    fn render_into(&self, output: &mut dyn std::fmt::Write) {
        let hits = self.counters.hits.load(Ordering::Relaxed);
        let misses = self.counters.misses.load(Ordering::Relaxed);
        let total = hits.saturating_add(misses);
        let ratio = if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        };
        let _ = writeln!(output, "brewfs_workspace_row_cache_hits_total {hits}");
        let _ = writeln!(output, "brewfs_workspace_row_cache_misses_total {misses}");
        let _ = writeln!(output, "brewfs_workspace_row_cache_hit_ratio {ratio:.6}");
    }
}

/// Retain across the complete backend commit and both invalidations. An
/// unresolved drop cannot be repaired by clearing once: the server may write
/// after that clear. Disable cached authority until this mount is replaced.
pub(super) struct CacheCommitGuard<'a> {
    upper: Option<&'a UpperRowCache>,
    namespace: Option<&'a UpperRowCache>,
    resolved: bool,
}

impl<'a> CacheCommitGuard<'a> {
    pub(super) fn new(
        upper: Option<&'a UpperRowCache>,
        namespace: Option<&'a UpperRowCache>,
    ) -> Self {
        Self {
            upper,
            namespace,
            resolved: false,
        }
    }

    /// Call only after a confirmed commit or a definite no-write rejection,
    /// and after invalidating the affected rows for the second time.
    pub(super) fn resolved(mut self) {
        self.resolved = true;
    }
}

impl Drop for CacheCommitGuard<'_> {
    fn drop(&mut self) {
        if !self.resolved {
            for cache in [self.upper, self.namespace].into_iter().flatten() {
                cache.disable();
            }
        }
    }
}

/// Captured before a backend read; the insert is dropped if any
/// invalidation happened in between.
#[derive(Clone, Copy)]
pub(super) struct RowTicket(u64);

impl UpperRowCache {
    pub(super) fn new(ttl: Duration) -> Self {
        Self::new_with_counters(ttl, Arc::new(RowCacheCounters::default()))
    }

    pub(super) fn new_with_counters(ttl: Duration, counters: Arc<RowCacheCounters>) -> Self {
        Self {
            rows: Cache::builder()
                .max_capacity(CAPACITY)
                .time_to_live(ttl)
                .build(),
            generation: AtomicU64::new(0),
            disabled: AtomicBool::new(false),
            counters,
        }
    }

    pub(super) fn ticket(&self) -> RowTicket {
        RowTicket(self.generation.load(Ordering::Acquire))
    }

    pub(super) fn get(&self, key: &RowKey) -> Option<RowValue> {
        let _stage = crate::stage_timing::Span::new(crate::stage_timing::Stage::RowCache);
        if self.disabled.load(Ordering::Acquire) {
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let value = self.rows.get(key);
        if self.disabled.load(Ordering::Acquire) {
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            None
        } else {
            if value.is_some() {
                self.counters.hits.fetch_add(1, Ordering::Relaxed);
            } else {
                self.counters.misses.fetch_add(1, Ordering::Relaxed);
            }
            value
        }
    }

    pub(super) fn insert(&self, ticket: RowTicket, key: RowKey, value: RowValue) {
        if self.disabled.load(Ordering::Acquire)
            || self.generation.load(Ordering::SeqCst) != ticket.0
        {
            return;
        }
        self.rows.insert(key.clone(), value);
        // An invalidation that ran concurrently with this insert must win.
        // Every invalidation bumps the generation before removing, so an
        // insert landing after that removal sees the bump and drops itself;
        // one landing before it is removed by the invalidation.
        if self.disabled.load(Ordering::Acquire)
            || self.generation.load(Ordering::SeqCst) != ticket.0
        {
            self.rows.invalidate(&key);
        }
    }

    fn disable(&self) {
        self.disabled.store(true, Ordering::Release);
        self.invalidate_all();
    }

    pub(super) fn invalidate_all(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.rows.invalidate_all();
    }

    /// Only these keys; unrelated cached rows stay. Still bumps the
    /// generation so any read in flight drops its insert.
    pub(super) fn invalidate_keys(&self, keys: &[RowKey]) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        for key in keys {
            self.rows.invalidate(key);
        }
    }
}

#[cfg(test)]
#[path = "row_cache_cancellation_tests.rs"]
mod cancellation_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn racing_invalidation_drops_stale_insert() {
        let cache = UpperRowCache::new(Duration::from_secs(60));
        let ticket = cache.ticket();
        cache.invalidate_all();
        cache.insert(
            ticket,
            RowKey::Inode(5),
            RowValue::Inode(Arc::new(Vec::new())),
        );
        assert!(cache.get(&RowKey::Inode(5)).is_none());
        let fresh = cache.ticket();
        cache.insert(
            fresh,
            RowKey::Inode(5),
            RowValue::Inode(Arc::new(Vec::new())),
        );
        assert!(cache.get(&RowKey::Inode(5)).is_some());
        cache.invalidate_all();
        assert!(cache.get(&RowKey::Inode(5)).is_none());
    }

    #[test]
    fn key_invalidation_keeps_unrelated_rows_and_drops_racing_insert() {
        let cache = UpperRowCache::new(Duration::from_secs(60));
        let row = || RowValue::Inode(Arc::new(Vec::new()));
        let ticket = cache.ticket();
        cache.insert(ticket, RowKey::Inode(5), row());
        cache.insert(ticket, RowKey::Inode(6), row());
        let racing = cache.ticket();
        cache.invalidate_keys(&[RowKey::Inode(5)]);
        assert!(cache.get(&RowKey::Inode(5)).is_none());
        assert!(cache.get(&RowKey::Inode(6)).is_some());
        // A read that started before the commit must not re-insert the
        // pre-commit row of any key.
        cache.insert(racing, RowKey::Inode(5), row());
        cache.insert(racing, RowKey::Inode(7), row());
        assert!(cache.get(&RowKey::Inode(5)).is_none());
        assert!(cache.get(&RowKey::Inode(7)).is_none());
    }
}
