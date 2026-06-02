use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

/// Cached tracking of per-inode size and allocation state.
///
/// BrewFS ensures close-to-open semantics: each `open` must see the newest
/// file state.  This struct caches the client-local view of size and provides
/// a best-effort estimate of allocated blocks for `st_blocks`.
///
/// Only `visible_size` is authoritative on the local client.  Allocated-bytes
/// tracking is an *estimate*, not a precise POSIX `st_blocks` value, because
/// the true allocated size depends on the metadata store's view of visible
/// extents after compaction, overwrites, and truncates.

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AllocatedState {
    /// No estimate available — must fall back to metadata.
    Unknown = 0,
    /// Value in `estimated_allocated_bytes` is a running estimate, not exact.
    Estimated = 1,
    /// Value was set from an authoritative source (e.g. metadata store commit
    /// result) and is exact for the current generation.
    Exact = 2,
}

pub(crate) struct Inode {
    ino: i64,

    /// Logical file size immediately visible on this client (updated on every
    /// extending write before the write returns to the caller).
    visible_size: AtomicU64,

    /// Last metadata-confirmed size.  Updated when `commit_chunk` succeeds or
    /// after truncate.
    committed_size: AtomicU64,

    /// Best-effort tracking of allocated bytes.  Updated by `commit_chunk`
    /// (incremented) and invalidated by truncate.  Use `allocated_blocks_512()`
    /// to get a value suitable for `st_blocks` when the state is `Exact`.
    estimated_allocated_bytes: AtomicU64,

    /// Whether the allocated-bytes estimate is meaningful.
    allocated_state: AtomicU8,

    /// Monotonic generation bumped on every truncate so that stale commit
    /// results from before the truncate can be detected and discarded.
    data_epoch: AtomicU64,
}

impl Inode {
    pub fn new(ino: i64, size: u64) -> Arc<Self> {
        Arc::new(Self {
            ino,
            visible_size: AtomicU64::new(size),
            committed_size: AtomicU64::new(size),
            // Start unknown — the caller may later set an exact value from
            // metadata, but we must not assume `size` equals allocated bytes.
            estimated_allocated_bytes: AtomicU64::new(0),
            allocated_state: AtomicU8::new(AllocatedState::Unknown as u8),
            data_epoch: AtomicU64::new(0),
        })
    }

    // ---- trivial accessors ----

    pub fn ino(&self) -> i64 {
        self.ino
    }

    // ---- visible size ----

    /// Current logical file size (may include uncommitted writes).
    pub fn file_size(&self) -> u64 {
        self.visible_size.load(Ordering::Acquire)
    }

    /// Set the logical size to an exact value (used by truncate).
    pub fn set_size(&self, new_size: u64) {
        self.visible_size.store(new_size, Ordering::Release);
    }

    /// Ensure the logical size is at least `min_size`, growing if necessary.
    /// This is a compare-and-swap loop so that concurrent extending writes
    /// converge to the maximum size rather than racing to a lower value.
    pub fn extend_size(&self, min_size: u64) {
        let mut cur = self.visible_size.load(Ordering::Acquire);
        while min_size > cur {
            match self.visible_size.compare_exchange_weak(
                cur,
                min_size,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(seen) => cur = seen,
            }
        }
    }

    // ---- committed size ----

    /// Last metadata-confirmed file size.
    pub fn committed_size(&self) -> u64 {
        self.committed_size.load(Ordering::Acquire)
    }

    /// Record the committed size after a successful `commit_chunk` or truncate.
    pub fn set_committed_size(&self, size: u64) {
        self.committed_size.store(size, Ordering::Release);
    }

    // ---- allocated bytes (best-effort) ----

    /// Best-effort allocated bytes.  Only use for `st_blocks` when
    /// `allocated_state()` returns `Exact`; otherwise fall back to the
    /// value carried in `FileAttr::blocks`.
    pub fn estimated_allocated_bytes(&self) -> u64 {
        self.estimated_allocated_bytes.load(Ordering::Acquire)
    }

    /// Return exact allocated-blocks value if the state is `Exact`.
    pub fn allocated_blocks_512(&self) -> Option<u64> {
        if self.allocated_state.load(Ordering::Acquire) == AllocatedState::Exact as u8 {
            Some(self.estimated_allocated_bytes().div_ceil(512))
        } else {
            None
        }
    }

    /// Record that `n` additional bytes have been committed (approximate).
    pub fn add_estimated_allocated_bytes(&self, n: u64) {
        self.estimated_allocated_bytes
            .fetch_add(n, Ordering::AcqRel);
        self.allocated_state
            .store(AllocatedState::Estimated as u8, Ordering::Release);
    }

    /// Set an exact allocated-bytes value (e.g. from a metadata commit result).
    pub fn set_exact_allocated_bytes(&self, n: u64) {
        self.estimated_allocated_bytes.store(n, Ordering::Release);
        self.allocated_state
            .store(AllocatedState::Exact as u8, Ordering::Release);
    }

    /// Mark the allocated-bytes estimate as stale (e.g. after truncate).
    pub fn invalidate_allocated_blocks(&self) {
        self.allocated_state
            .store(AllocatedState::Unknown as u8, Ordering::Release);
    }

    // ---- data epoch (truncate / setattr size change) ----

    /// Current data epoch.
    pub fn data_epoch(&self) -> u64 {
        self.data_epoch.load(Ordering::Acquire)
    }

    /// Bump the data epoch (call after truncate / setattr-size).
    /// Returns the *new* epoch value.
    pub fn bump_data_epoch(&self) -> u64 {
        self.data_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }
}
