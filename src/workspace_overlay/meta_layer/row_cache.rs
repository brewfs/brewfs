//! Short-lived cache of writable-layer (upper) rows for read-only paths.
//!
//! Opt-in per mount (`WorkspaceMetaLayer::with_upper_row_cache`). The
//! workspace lease is exclusive, so while it is held the mounted layer is
//! the only writer of upper rows; every local commit invalidates the keys of
//! the rows it writes, every view change invalidates the whole cache, and a
//! generation counter keeps a read that raced any invalidation from leaving
//! pre-commit rows behind. Under a proven lease TTL bounds reuse, while a byte
//! budget bounds retention. Canceled or uncertain commits permanently disable cached
//! authority. Conditional mutations still authenticate their committed view.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::RwLock;

use crate::workspace_overlay::catalog::PermissionSnapshot;
use crate::workspace_overlay::model::{DentryDelta, InodeDelta, XattrDelta};

const CAPACITY: u64 = 65_536;
const RECENCY_PROBES: usize = 8;
pub(crate) const DEFAULT_ROW_CACHE_BYTES: u64 = 64 << 20;

/// Shared by both row caches of a mount, including values pinned by readers.
pub(super) struct RowBudget {
    limit: u64,
    used: Arc<AtomicU64>,
    caches: Mutex<Vec<Weak<RwLock<Rows>>>>,
    #[cfg(test)]
    reclaim_steps: AtomicU64,
}

impl RowBudget {
    pub(super) fn new(limit: u64, counters: &RowCacheCounters) -> Arc<Self> {
        counters.capacity_bytes.store(limit, Ordering::Relaxed);
        Arc::new(Self {
            limit,
            used: counters.retained_bytes.clone(),
            caches: Mutex::new(Vec::new()),
            #[cfg(test)]
            reclaim_steps: AtomicU64::new(0),
        })
    }

    fn register(&self, rows: &Arc<RwLock<Rows>>) {
        let mut caches = self.caches.lock().unwrap();
        caches.retain(|rows| rows.strong_count() != 0);
        caches.push(Arc::downgrade(rows));
    }

    fn reclaim_step(&self) {
        #[cfg(test)]
        self.reclaim_steps.fetch_add(1, Ordering::Relaxed);
    }

    /// Reclaim one peer entry under admission pressure. This also releases
    /// expired rows from an idle peer, without a timer or a full-cache scan.
    /// Each successful step removes an entry; aggregate work is amortized over
    /// prior inserts. try_write prevents cross-cache lock inversion. Each
    /// removal makes at most RECENCY_PROBES second-chance checks.
    fn reclaim_peer(&self, current: &Arc<RwLock<Rows>>) -> bool {
        let mut caches = self.caches.lock().unwrap();
        caches.retain(|rows| rows.strong_count() != 0);
        for rows in caches.iter().filter_map(Weak::upgrade) {
            if Arc::ptr_eq(&rows, current) {
                continue;
            }
            self.reclaim_step();
            if let Some(mut rows) = rows.try_write() {
                let before = self.used.load(Ordering::Acquire);
                let removed = rows.evict_entry();
                rows.release_empty_index();
                if removed || self.used.load(Ordering::Acquire) < before {
                    return true;
                }
            }
        }
        false
    }

    fn admit(self: &Arc<Self>, bytes: u64) -> Option<Charge> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .ok()?;
        Some(Charge {
            budget: self.clone(),
            bytes,
        })
    }
}

struct Charge {
    budget: Arc<RowBudget>,
    bytes: u64,
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// All clones share one payload charge, including readers after eviction.
/// The data drops before its charge. No raw Arc escapes this wrapper.
pub(super) struct Retained<T>(Arc<RetainedInner<T>>);
struct RetainedInner<T> {
    value: T,
    charge: Mutex<Option<Charge>>,
}
impl<T> Retained<T> {
    pub(super) fn new(value: T) -> Self {
        Self(Arc::new(RetainedInner {
            value,
            charge: Mutex::new(None),
        }))
    }
    fn retain(&self, budget: &Arc<RowBudget>, bytes: u64) -> bool {
        let mut charge = self.0.charge.lock().unwrap();
        if let Some(charge) = charge.as_ref() {
            return Arc::ptr_eq(&charge.budget, budget);
        }
        *charge = budget.admit(bytes);
        charge.is_some()
    }
}
impl<T> Clone for Retained<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> Deref for Retained<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0.value
    }
}
impl<T> AsRef<T> for Retained<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

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
    Inode(Retained<Vec<InodeDelta>>),
    Dentry(Retained<Vec<DentryDelta>>),
    Xattr(Retained<Vec<XattrDelta>>),
    Permission(Retained<PermissionSnapshot>),
    /// Complete effective paths of one inode. Only successful, complete
    /// walks are cached; quota/cycle/view errors never are.
    Paths(Retained<Vec<Vec<u8>>>),
    /// Verified effective (parent, name) aliases of one inode. Lives in the
    /// namespace cache: only commits that write dentries or delete an inode
    /// can change it, so data-only commits keep it.
    Aliases(Retained<Vec<(i64, Vec<u8>)>>),
}

fn vector_bytes<T>(rows: &Vec<T>, extra: impl Fn(&T) -> usize) -> u64 {
    rows.iter().fold(
        rows.capacity().saturating_mul(size_of::<T>()),
        |total, row| total.saturating_add(extra(row)),
    ) as u64
}
fn inode_bytes(rows: &Vec<InodeDelta>) -> u64 {
    vector_bytes(rows, |row| {
        row.symlink_target.as_ref().map_or(0, Vec::capacity)
    })
}
fn dentry_bytes(rows: &Vec<DentryDelta>) -> u64 {
    vector_bytes(rows, |row| row.name.capacity())
}
fn xattr_bytes(rows: &Vec<XattrDelta>) -> u64 {
    vector_bytes(rows, |row| {
        row.name
            .capacity()
            .saturating_add(row.value.as_ref().map_or(0, Vec::capacity))
    })
}
impl RowValue {
    fn payload_bytes(&self) -> u64 {
        fn bytes<T>(_: &Retained<T>, heap: u64) -> u64 {
            heap.saturating_add(
                size_of::<RetainedInner<T>>() as u64 + 2 * size_of::<usize>() as u64,
            )
        }
        match self {
            Self::Inode(v) => bytes(v, inode_bytes(v)),
            Self::Dentry(v) => bytes(v, dentry_bytes(v)),
            Self::Xattr(v) => bytes(v, xattr_bytes(v)),
            Self::Permission(v) => bytes(
                v,
                inode_bytes(&v.inodes)
                    .saturating_add(xattr_bytes(&v.xattrs))
                    .saturating_add(dentry_bytes(&v.dentries)),
            ),
            Self::Paths(v) => bytes(v, vector_bytes(v, Vec::capacity)),
            Self::Aliases(v) => bytes(v, vector_bytes(v, |(_, name)| name.capacity())),
        }
    }
    fn retain(&self, budget: &Arc<RowBudget>) -> bool {
        let bytes = self.payload_bytes();
        match self {
            Self::Inode(v) => v.retain(budget, bytes),
            Self::Dentry(v) => v.retain(budget, bytes),
            Self::Xattr(v) => v.retain(budget, bytes),
            Self::Permission(v) => v.retain(budget, bytes),
            Self::Paths(v) => v.retain(budget, bytes),
            Self::Aliases(v) => v.retain(budget, bytes),
        }
    }
}

struct Entry {
    value: RowValue,
    inserted: Instant,
    referenced: AtomicBool,
    _key_charge: Charge,
}

struct Rows {
    entries: LruCache<RowKey, Entry>,
    index_charge: Option<Charge>,
    peak_entries: usize,
    #[cfg(test)]
    recency_probes: usize,
}
impl Rows {
    fn new() -> Self {
        // Do not preallocate 65,536 buckets for a tiny byte budget.
        Self {
            entries: LruCache::unbounded(),
            index_charge: None,
            peak_entries: 0,
            #[cfg(test)]
            recency_probes: 0,
        }
    }
    fn reserve_slot(&mut self, budget: &Arc<RowBudget>) -> bool {
        let count = self.entries.len() + 1;
        if count <= self.peak_entries {
            return true;
        }
        // Reserve the hash index at its high-water mark until it is freed.
        // Evicting entries does not shrink the backing table.
        let bytes = 512 + 128 * count as u64;
        let previous = self.index_charge.as_ref().map_or(0, |charge| charge.bytes);
        let Some(mut extra) = budget.admit(bytes - previous) else {
            return false;
        };
        if let Some(charge) = &mut self.index_charge {
            charge.bytes += extra.bytes;
            extra.bytes = 0;
        } else {
            self.index_charge = Some(extra);
        }
        self.peak_entries = count;
        true
    }
    fn release_empty_index(&mut self) {
        if self.entries.is_empty() {
            *self = Self::new();
        }
    }

    /// Bounded second chance, not exact LRU. Hits set a reference bit under a
    /// shared read lock; only admission moves nodes. Limit the scan even when
    /// every entry is hot, so pressure cannot turn into a full-cache sweep.
    fn evict_entry(&mut self) -> bool {
        for probe in 0..RECENCY_PROBES {
            let Some((key, entry)) = self.entries.pop_lru() else {
                return false;
            };
            #[cfg(test)]
            {
                self.recency_probes += 1;
            }
            if probe + 1 < RECENCY_PROBES && entry.referenced.swap(false, Ordering::Relaxed) {
                self.entries.put(key, entry);
            } else {
                return true;
            }
        }
        unreachable!("the last probe always evicts")
    }
}
impl Deref for Rows {
    type Target = LruCache<RowKey, Entry>;
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}
impl DerefMut for Rows {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entries
    }
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
    rows: Arc<RwLock<Rows>>,
    ttl: Duration,
    budget: Arc<RowBudget>,
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
    pub retained_bytes: Arc<AtomicU64>,
    pub capacity_bytes: AtomicU64,
}

#[derive(Debug)]
pub(super) struct RowCacheStatsExtension {
    pub counters: Arc<RowCacheCounters>,
}

impl crate::vfs::stats::FsStatsExtension for RowCacheStatsExtension {
    fn render_max_bytes(&self) -> usize {
        512
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
        let _ = writeln!(
            output,
            "brewfs_workspace_row_cache_retained_bytes {}",
            self.counters.retained_bytes.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "brewfs_workspace_row_cache_capacity_bytes {}",
            self.counters.capacity_bytes.load(Ordering::Relaxed)
        );
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
        let budget = RowBudget::new(DEFAULT_ROW_CACHE_BYTES, &counters);
        Self::with_budget(ttl, counters, budget)
    }

    pub(super) fn with_budget(
        ttl: Duration,
        counters: Arc<RowCacheCounters>,
        budget: Arc<RowBudget>,
    ) -> Self {
        let rows = Arc::new(RwLock::new(Rows::new()));
        budget.register(&rows);
        Self {
            rows,
            ttl,
            budget,
            generation: AtomicU64::new(0),
            disabled: AtomicBool::new(false),
            counters,
        }
    }

    pub(super) fn ticket(&self) -> RowTicket {
        RowTicket(self.generation.load(Ordering::Acquire))
    }

    fn evict_for_admission(&self, rows: &mut Rows) -> bool {
        // Prefer the idle peer over repeatedly churning the active cache's
        // only usable slot when that peer holds the shared budget.
        if self.budget.reclaim_peer(&self.rows) {
            return true;
        }
        self.budget.reclaim_step();
        let removed = rows.evict_entry();
        rows.release_empty_index();
        removed
    }

    pub(super) fn get(&self, key: &RowKey) -> Option<RowValue> {
        if self.disabled.load(Ordering::Acquire) {
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let (value, expired) = {
            let rows = self.rows.read();
            match rows.peek(key) {
                Some(entry) if entry.inserted.elapsed() < self.ttl => {
                    // Repeated hits, including concurrent hits on the same
                    // key, do not acquire an exclusive lock or rewrite an
                    // already-set reference bit.
                    if !entry.referenced.load(Ordering::Relaxed) {
                        entry.referenced.store(true, Ordering::Relaxed);
                    }
                    (Some(entry.value.clone()), false)
                }
                Some(_) => (None, true),
                None => (None, false),
            }
        };
        if expired {
            self.remove_expired(key);
        }
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

    fn remove_expired(&self, key: &RowKey) {
        let mut rows = self.rows.write();
        // A replacement may have won while the reader released its guard.
        // Re-check the current entry instead of deleting by the stale read.
        if rows
            .peek(key)
            .is_some_and(|entry| entry.inserted.elapsed() >= self.ttl)
        {
            rows.pop(key);
            rows.release_empty_index();
        }
    }

    pub(super) fn insert(&self, ticket: RowTicket, key: RowKey, value: RowValue) {
        if self.disabled.load(Ordering::Acquire)
            || self.generation.load(Ordering::SeqCst) != ticket.0
        {
            return;
        }
        let key_bytes = match &key {
            RowKey::Dentry(_, name) | RowKey::Xattr(_, name) => name.capacity() as u64,
            _ => 0,
        };
        // Includes the LRU node, hash-table slot and allocator bookkeeping.
        let key_bytes =
            key_bytes.saturating_add((size_of::<RowKey>() + size_of::<Entry>() + 128) as u64);
        if key_bytes > self.budget.limit {
            return;
        }
        let mut rows = self.rows.write();
        rows.pop(&key);
        if key_bytes
            .saturating_add(value.payload_bytes())
            .saturating_add(640)
            > self.budget.limit
        {
            return;
        }
        if rows.len() == CAPACITY as usize {
            rows.evict_entry();
        }
        loop {
            rows.release_empty_index();
            if !rows.reserve_slot(&self.budget) {
                if !self.evict_for_admission(&mut rows) {
                    return;
                }
                continue;
            }
            if let Some(key_charge) = self.budget.admit(key_bytes) {
                if value.retain(&self.budget) {
                    // Invalidators bump generation before taking this lock.
                    // If the bump comes later, their removal follows our put.
                    if !self.disabled.load(Ordering::Acquire)
                        && self.generation.load(Ordering::SeqCst) == ticket.0
                    {
                        rows.put(
                            key,
                            Entry {
                                value,
                                inserted: Instant::now(),
                                referenced: AtomicBool::new(false),
                                _key_charge: key_charge,
                            },
                        );
                    }
                    return;
                }
            }
            if !self.evict_for_admission(&mut rows) {
                return;
            }
        }
    }

    /// Reserve the cache's separate path copy before allocating it. The walk's
    /// Output guard belongs to the returned result and cannot cover this copy.
    pub(super) fn insert_paths(&self, ticket: RowTicket, key: RowKey, paths: &[Vec<u8>]) {
        let bytes = paths.iter().fold(
            (size_of::<RetainedInner<Vec<Vec<u8>>>>()
                + 2 * size_of::<usize>()
                + paths.len() * size_of::<Vec<u8>>()) as u64,
            |total, path| total.saturating_add(path.len() as u64),
        );
        self.insert_paths_with(ticket, key, bytes, || paths.to_vec());
    }

    pub(super) fn insert_child_paths(
        &self,
        ticket: RowTicket,
        key: RowKey,
        parents: &[Vec<u8>],
        name: &[u8],
    ) {
        let bytes = parents.iter().fold(
            (size_of::<RetainedInner<Vec<Vec<u8>>>>()
                + 2 * size_of::<usize>()
                + parents.len() * size_of::<Vec<u8>>()) as u64,
            |total, path| {
                total
                    .saturating_add(path.len() as u64)
                    .saturating_add(name.len() as u64)
                    .saturating_add(u64::from(path.last() != Some(&b'/')))
            },
        );
        self.insert_paths_with(ticket, key, bytes, || {
            parents
                .iter()
                .map(|path| {
                    let separator = usize::from(path.last() != Some(&b'/'));
                    let mut child = Vec::with_capacity(path.len() + separator + name.len());
                    child.extend_from_slice(path);
                    if separator != 0 {
                        child.push(b'/');
                    }
                    child.extend_from_slice(name);
                    child
                })
                .collect()
        });
    }

    fn insert_paths_with(
        &self,
        ticket: RowTicket,
        key: RowKey,
        bytes: u64,
        build: impl FnOnce() -> Vec<Vec<u8>>,
    ) {
        if bytes.saturating_add((size_of::<RowKey>() + size_of::<Entry>() + 128 + 640) as u64)
            > self.budget.limit
        {
            return;
        }
        let value = {
            let mut rows = self.rows.write();
            if self.disabled.load(Ordering::Acquire)
                || self.generation.load(Ordering::SeqCst) != ticket.0
            {
                return;
            }
            rows.pop(&key);
            let charge = loop {
                if let Some(charge) = self.budget.admit(bytes) {
                    break charge;
                }
                if !self.evict_for_admission(&mut rows) {
                    return;
                }
            };
            Retained(Arc::new(RetainedInner {
                value: build(),
                charge: Mutex::new(Some(charge)),
            }))
        };
        self.insert(ticket, key, RowValue::Paths(value));
    }

    fn disable(&self) {
        self.disabled.store(true, Ordering::Release);
        self.invalidate_all();
    }

    pub(super) fn invalidate_all(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        *self.rows.write() = Rows::new();
    }

    /// Only these keys; unrelated cached rows stay. Still bumps the
    /// generation so any read in flight drops its insert.
    pub(super) fn invalidate_keys(&self, keys: &[RowKey]) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        let mut rows = self.rows.write();
        for key in keys {
            rows.pop(key);
        }
        rows.release_empty_index();
    }
}

#[cfg(test)]
#[path = "row_cache_cancellation_tests.rs"]
mod cancellation_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn bounded(bytes: u64) -> UpperRowCache {
        let counters = Arc::new(RowCacheCounters::default());
        let budget = RowBudget::new(bytes, &counters);
        UpperRowCache::with_budget(Duration::from_secs(60), counters, budget)
    }

    fn xattr(bytes: usize) -> RowValue {
        RowValue::Xattr(Retained::new(vec![XattrDelta {
            layer_id: crate::workspace_overlay::ids::LayerId::new(),
            ino: 1,
            name: b"user.large".to_vec(),
            op: crate::workspace_overlay::model::ValueOp::Put,
            value: Some(vec![0; bytes]),
            sequence: 0,
        }]))
    }

    #[test]
    fn byte_limit_rejects_large_xattrs_without_flushing_small_rows() {
        let cache = bounded(8192);
        cache.insert(
            cache.ticket(),
            RowKey::Inode(1),
            RowValue::Inode(Retained::new(Vec::new())),
        );
        let before = cache.budget.used.load(Ordering::Acquire);
        for ino in 2..18 {
            cache.insert(
                cache.ticket(),
                RowKey::Xattr(ino, b"user.large".to_vec()),
                xattr(64 << 10),
            );
        }
        assert!(cache.get(&RowKey::Inode(1)).is_some());
        assert_eq!(cache.budget.used.load(Ordering::Acquire), before);
        assert_eq!(cache.rows.read().len(), 1);
    }

    #[test]
    fn readers_keep_payload_charged_after_eviction_replacement_and_close() {
        let cache = bounded(8192);
        let key = RowKey::Xattr(1, b"user.large".to_vec());
        cache.insert(cache.ticket(), key.clone(), xattr(1024));
        let pinned = cache.get(&key).unwrap();
        let pinned_bytes = pinned.payload_bytes();
        cache.insert(cache.ticket(), key.clone(), xattr(2048));
        assert!(cache.budget.used.load(Ordering::Acquire) > pinned_bytes + 2048);
        for ino in 2..20 {
            cache.insert(cache.ticket(), RowKey::Xattr(ino, Vec::new()), xattr(2048));
            assert!(cache.budget.used.load(Ordering::Acquire) <= 8192);
        }
        let used = cache.budget.used.clone();
        drop(cache);
        assert_eq!(used.load(Ordering::Acquire), pinned_bytes);
        drop(pinned);
        assert_eq!(used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn shared_payload_is_charged_once_across_both_mount_caches() {
        let upper = bounded(8192);
        let namespace = UpperRowCache::with_budget(
            Duration::from_secs(60),
            upper.counters.clone(),
            upper.budget.clone(),
        );
        let value = xattr(1024);
        let payload = value.payload_bytes();
        upper.insert(upper.ticket(), RowKey::Inode(1), value.clone());
        let one = upper.budget.used.load(Ordering::Acquire);
        namespace.insert(namespace.ticket(), RowKey::Inode(2), value.clone());
        assert_eq!(upper.budget.used.load(Ordering::Acquire), 2 * one - payload);
        upper.invalidate_all();
        namespace.invalidate_all();
        assert_eq!(upper.budget.used.load(Ordering::Acquire), payload);
        drop(value);
        assert_eq!(upper.budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn idle_expired_namespace_cannot_starve_upper_and_reclaim_work_is_linear() {
        let upper = bounded(64 << 10);
        // Zero TTL makes every namespace entry deterministically expired,
        // without sleeps or relying on the scheduler's clock granularity.
        let namespace = UpperRowCache::with_budget(
            Duration::ZERO,
            upper.counters.clone(),
            upper.budget.clone(),
        );
        for ino in 0..512 {
            namespace.insert(
                namespace.ticket(),
                RowKey::Inode(ino),
                RowValue::Inode(Retained::new(Vec::new())),
            );
        }
        let retained = namespace.rows.read().len();
        assert!(retained > 0);
        let before = upper.budget.reclaim_steps.load(Ordering::Relaxed);
        for ino in 0..512 {
            upper.insert(
                upper.ticket(),
                RowKey::Inode(ino),
                RowValue::Inode(Retained::new(Vec::new())),
            );
            assert!(upper.get(&RowKey::Inode(ino)).is_some());
            assert!(upper.budget.used.load(Ordering::Acquire) <= 64 << 10);
        }
        assert!(namespace.rows.read().is_empty());
        let steps = upper.budget.reclaim_steps.load(Ordering::Relaxed) - before;
        assert!(
            steps <= 4 * (512 + retained as u64),
            "reclaim must remove entries, not repeatedly scan retained rows: {steps}"
        );
    }

    #[test]
    fn peer_reclaim_keeps_expired_payload_charged_while_a_reader_pins_it() {
        let upper = bounded(4096);
        let namespace = UpperRowCache::with_budget(
            Duration::ZERO,
            upper.counters.clone(),
            upper.budget.clone(),
        );
        let pinned = xattr(2048);
        let bytes = pinned.payload_bytes();
        namespace.insert(
            namespace.ticket(),
            RowKey::Xattr(1, Vec::new()),
            pinned.clone(),
        );
        upper.insert(upper.ticket(), RowKey::Xattr(2, Vec::new()), xattr(1024));
        assert!(namespace.rows.read().is_empty());
        assert!(upper.get(&RowKey::Xattr(2, Vec::new())).is_none());
        assert_eq!(upper.budget.used.load(Ordering::Acquire), bytes);
        drop(pinned);
        upper.insert(upper.ticket(), RowKey::Xattr(2, Vec::new()), xattr(1024));
        assert!(upper.get(&RowKey::Xattr(2, Vec::new())).is_some());
    }

    #[test]
    fn row_cache_byte_budget_evidence() {
        for payload_bytes in [8, 64 << 10] {
            let legacy: moka::sync::Cache<RowKey, Arc<Vec<XattrDelta>>> =
                moka::sync::Cache::builder()
                    .max_capacity(CAPACITY)
                    .time_to_live(Duration::from_secs(60))
                    .build();
            let cache = bounded(512 << 10);
            for ino in 0..128 {
                let key = RowKey::Xattr(ino, b"user.large".to_vec());
                let RowValue::Xattr(rows) = xattr(payload_bytes) else {
                    unreachable!()
                };
                legacy.insert(key.clone(), Arc::new((*rows).clone()));
                cache.insert(cache.ticket(), key, RowValue::Xattr(rows));
            }
            legacy.run_pending_tasks();
            assert_eq!(legacy.entry_count(), 128);
            let legacy_payload: usize = legacy
                .iter()
                .map(|(_, rows)| {
                    rows.iter()
                        .map(|row| row.value.as_ref().unwrap().len())
                        .sum::<usize>()
                })
                .sum();
            let retained = cache.budget.used.load(Ordering::Acquire);
            let entries = cache.rows.read().len();
            assert!(retained <= 512 << 10);
            if payload_bytes == 8 {
                assert_eq!(entries, 128);
            } else {
                assert!(legacy_payload as u64 > 15 * retained);
            }
            println!(
                "row-cache-byte-evidence payload_bytes={payload_bytes} input_entries=128 legacy_payload_lower_bound_bytes={legacy_payload} legacy_entries=128 bounded_retained_bytes={retained} bounded_entries={entries} budget_bytes=524288"
            );
        }
    }

    #[test]
    fn derived_paths_are_admitted_before_the_copy_is_constructed() {
        let cache = bounded(4096);
        cache.insert_paths_with(cache.ticket(), RowKey::Paths(1), 4096, || {
            panic!("oversized path copy must not be allocated")
        });
        assert_eq!(cache.budget.used.load(Ordering::Acquire), 0);
        cache.insert_child_paths(
            cache.ticket(),
            RowKey::Paths(2),
            &[b"/parent".to_vec(), b"/".to_vec()],
            b"child",
        );
        let Some(RowValue::Paths(paths)) = cache.get(&RowKey::Paths(2)) else {
            panic!("admitted child paths must be cached")
        };
        assert_eq!(
            paths.as_slice(),
            &[b"/parent/child".to_vec(), b"/child".to_vec()]
        );
    }

    #[test]
    fn hot_key_hits_can_share_the_read_lock() {
        let cache = bounded(8192);
        let key = RowKey::Inode(1);
        cache.insert(
            cache.ticket(),
            key.clone(),
            RowValue::Inode(Retained::new(Vec::new())),
        );
        let held = cache.rows.read();
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            for _ in 0..2 {
                let send = send.clone();
                let cache = &cache;
                let key = &key;
                scope.spawn(move || send.send(cache.get(key).is_some()).unwrap());
            }
            // Keeping this guard excludes an exclusive hit path. Both hits
            // must finish before it is released, including on the same key.
            let first = receive.recv_timeout(Duration::from_secs(2));
            let second = receive.recv_timeout(Duration::from_secs(2));
            drop(held);
            assert_eq!(first, Ok(true));
            assert_eq!(second, Ok(true));
        });
    }

    #[test]
    fn expired_read_cleanup_preserves_a_fresh_replacement() {
        let cache = bounded(8192);
        let key = RowKey::Inode(1);
        let value = || RowValue::Inode(Retained::new(Vec::new()));
        cache.insert(cache.ticket(), key.clone(), value());
        cache.rows.write().get_mut(&key).unwrap().inserted = Instant::now() - 2 * cache.ttl;
        assert!(cache.rows.read().peek(&key).unwrap().inserted.elapsed() >= cache.ttl);
        // Deterministically interleave a replacement between the expired
        // read observation and its write-side cleanup.
        cache.insert(cache.ticket(), key.clone(), value());
        let retained = cache.budget.used.load(Ordering::Acquire);
        cache.remove_expired(&key);
        assert!(cache.get(&key).is_some());
        assert_eq!(cache.budget.used.load(Ordering::Acquire), retained);
    }

    #[test]
    fn second_chance_preserves_a_recent_hit_and_bounds_all_hot_scans() {
        let cache = bounded(64 << 10);
        for ino in 0..16 {
            cache.insert(
                cache.ticket(),
                RowKey::Inode(ino),
                RowValue::Inode(Retained::new(Vec::new())),
            );
        }
        assert!(cache.get(&RowKey::Inode(0)).is_some());
        {
            let mut rows = cache.rows.write();
            assert!(rows.evict_entry());
            assert!(rows.peek(&RowKey::Inode(0)).is_some());
            assert!(rows.peek(&RowKey::Inode(1)).is_none());
        }
        for ino in (0..16).filter(|ino| *ino != 1) {
            assert!(cache.get(&RowKey::Inode(ino)).is_some());
        }
        let mut rows = cache.rows.write();
        let before = rows.recency_probes;
        let count = rows.len();
        assert!(rows.evict_entry());
        assert_eq!(rows.recency_probes - before, RECENCY_PROBES);
        assert_eq!(rows.len(), count - 1);
        assert!(cache.budget.used.load(Ordering::Acquire) <= cache.budget.limit);
    }

    #[test]
    #[ignore = "diagnostic same-binary hit-path comparison; record raw timings, not a throughput acceptance test"]
    fn small_row_cache_hit_benchmark() {
        // Retain the original 128-key shape and add a single hot key. Fresh
        // caches per shape/thread count keep the 60-second TTL out of timing.
        for key_count in [1, 128] {
            for threads in [1, 8] {
                let legacy: moka::sync::Cache<RowKey, RowValue> = moka::sync::Cache::builder()
                    .max_capacity(CAPACITY)
                    .time_to_live(Duration::from_secs(60))
                    .build();
                let cache = Arc::new(bounded(512 << 10));
                for ino in 0..128 {
                    let value = RowValue::Inode(Retained::new(Vec::new()));
                    legacy.insert(RowKey::Inode(ino), value.clone());
                    cache.insert(cache.ticket(), RowKey::Inode(ino), value);
                }
                legacy.run_pending_tasks();
                let disabled = AtomicBool::new(false);
                let hits = AtomicU64::new(0);
                for repetition in 0..6 {
                    for bounded_first in [repetition % 2 == 0, repetition % 2 != 0] {
                        let barrier = std::sync::Barrier::new(threads);
                        let begin = Instant::now();
                        std::thread::scope(|scope| {
                            for _ in 0..threads {
                                scope.spawn(|| {
                                    barrier.wait();
                                    for i in 0..50_000 {
                                        let key = RowKey::Inode(i % key_count);
                                        let value = if bounded_first {
                                            cache.get(&key)
                                        } else {
                                            assert!(!disabled.load(Ordering::Acquire));
                                            let value = legacy.get(&key);
                                            assert!(!disabled.load(Ordering::Acquire));
                                            hits.fetch_add(1, Ordering::Relaxed);
                                            value
                                        };
                                        assert!(std::hint::black_box(value).is_some());
                                    }
                                });
                            }
                        });
                        println!(
                            "row-cache-hit-evidence bounded={bounded_first} threads={threads} keys={key_count} repetition={repetition} hits={} wall_ns={}",
                            threads * 50_000,
                            begin.elapsed().as_nanos()
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn key_capacity_nested_paths_and_expiration_release_their_charges() {
        let counters = Arc::new(RowCacheCounters::default());
        let budget = RowBudget::new(8192, &counters);
        let cache = UpperRowCache::with_budget(Duration::ZERO, counters, budget);
        let mut name = Vec::with_capacity(2048);
        name.push(1);
        let key = RowKey::Dentry(1, name);
        let query = key.clone();
        cache.insert_paths(cache.ticket(), key, &[vec![0; 1024], vec![1; 512]]);
        assert!(cache.budget.used.load(Ordering::Acquire) >= 2048 + 1024 + 512);
        assert!(cache.get(&query).is_none());
        assert_eq!(cache.budget.used.load(Ordering::Acquire), 0);
        cache.insert_paths(cache.ticket(), RowKey::Paths(2), &[vec![0; 8192]]);
        assert_eq!(cache.budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn racing_invalidation_drops_stale_insert() {
        let cache = UpperRowCache::new(Duration::from_secs(60));
        let ticket = cache.ticket();
        cache.invalidate_all();
        cache.insert(
            ticket,
            RowKey::Inode(5),
            RowValue::Inode(Retained::new(Vec::new())),
        );
        assert!(cache.get(&RowKey::Inode(5)).is_none());
        let fresh = cache.ticket();
        cache.insert(
            fresh,
            RowKey::Inode(5),
            RowValue::Inode(Retained::new(Vec::new())),
        );
        assert!(cache.get(&RowKey::Inode(5)).is_some());
        cache.invalidate_all();
        assert!(cache.get(&RowKey::Inode(5)).is_none());
    }

    #[test]
    fn key_invalidation_keeps_unrelated_rows_and_drops_racing_insert() {
        let cache = UpperRowCache::new(Duration::from_secs(60));
        let row = || RowValue::Inode(Retained::new(Vec::new()));
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
