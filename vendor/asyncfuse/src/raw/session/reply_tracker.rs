//! Readonly reply disposition. A dropped buffer is not a successful reply.
use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use super::worker::InflightGuard;
use crate::raw::reply::ReplyMemoryGuard;
use crate::raw::RequestObserver;

const PENDING: u8 = 0;
const WRITTEN: u8 = 1;
const KERNEL_FORGOT: u8 = 2;

#[derive(Clone, Copy, Debug)]
struct Failure {
    errno: Option<i32>,
    kind: io::ErrorKind,
}

#[derive(Debug)]
pub(crate) struct ReplyTracker {
    tickets: Mutex<BTreeMap<u64, Weak<ReplyOwner>>>,
    failure: Mutex<Option<Failure>>,
    inflight: Arc<AtomicUsize>,
    notify: Arc<async_notify::Notify>,
    request_observer: Option<Arc<dyn RequestObserver>>,
    // The reservation covers this Arc, its empty map, and preparation Box.
    // Each map node is covered by its Request or 1024-byte Control owner.
    _roots: Option<ReplyMemoryGuard>,
}

// event-listener 5.4.1's lazy Inner Arc contains an atomic and a mutex holding
// five pointer-sized fields. Its listener is inline in the measured future.
// Include the memory guard Arc (v3's V3OwnedPermit plus Arc header is 88 bytes).
const NOTIFY_LAZY_ALLOCATION_BOUND: usize = std::mem::size_of::<AtomicUsize>()
    + std::mem::size_of::<Mutex<[usize; 5]>>()
    + 2 * std::mem::size_of::<usize>()
    + 16;
const ADMISSION_GUARD_ALLOCATION_BOUND: usize = 96;
pub(super) const TRACKER_ALLOCATION_BOUND: usize = 304;
pub(super) const PREPARATION_OVERHEAD_BOUND: usize = 32;
const _: () = assert!(
    std::mem::size_of::<ReplyTracker>()
        + 2 * std::mem::size_of::<usize>()
        + NOTIFY_LAZY_ALLOCATION_BOUND
        + ADMISSION_GUARD_ALLOCATION_BOUND
        <= TRACKER_ALLOCATION_BOUND
);

impl ReplyTracker {
    pub(super) fn new(
        roots: Option<ReplyMemoryGuard>,
        inflight: Arc<AtomicUsize>,
        notify: Arc<async_notify::Notify>,
        request_observer: Option<Arc<dyn RequestObserver>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            tickets: Mutex::new(BTreeMap::new()),
            failure: Mutex::new(None),
            inflight,
            notify,
            request_observer,
            _roots: roots,
        })
    }

    pub(super) fn fail(&self, error: &io::Error) {
        let mut failure = self.failure.lock().unwrap_or_else(|e| e.into_inner());
        if failure.is_none() {
            *failure = Some(Failure {
                errno: error.raw_os_error(),
                kind: error.kind(),
            });
        }
        drop(failure);
        self.notify.notify();
    }

    pub(super) fn error(&self) -> Option<io::Error> {
        self.failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|failure| {
                failure
                    .errno
                    .map(io::Error::from_raw_os_error)
                    .unwrap_or_else(|| io::Error::from(failure.kind))
            })
    }

    pub(super) fn owner(
        self: &Arc<Self>,
        unique: u64,
        memory: Option<ReplyMemoryGuard>,
        inflight: InflightGuard,
    ) -> Arc<ReplyOwner> {
        let owner = Arc::new(ReplyOwner {
            unique,
            disposition: AtomicU8::new(PENDING),
            tracker: self.clone(),
            _memory: memory,
            _inflight: inflight,
        });
        let mut tickets = self.tickets.lock().unwrap_or_else(|e| e.into_inner());
        if tickets.contains_key(&unique) {
            drop(tickets);
            self.fail(&io::Error::from(io::ErrorKind::InvalidData));
            return owner;
        }
        tickets.insert(unique, Arc::downgrade(&owner));
        owner
    }

    pub(super) fn ticket(&self, unique: u64) -> Option<Arc<ReplyOwner>> {
        self.tickets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&unique)
            .and_then(Weak::upgrade)
    }

    pub(super) async fn wait_failure(&self) -> io::Error {
        loop {
            if let Some(error) = self.error() {
                return error;
            }
            // Same stored-permit Notify; failure/drain waits are sequential.
            self.notify.notified().await;
        }
    }

    pub(super) async fn drain(&self) -> io::Result<()> {
        loop {
            if let Some(error) = self.error() {
                return Err(error);
            }
            if self.inflight.load(Ordering::Acquire) == 0 {
                // Pending-owner Drop stores failure BEFORE the final count drop.
                return self.error().map_or(Ok(()), Err);
            }
            // Exactly one readonly preparation waiter uses this stored permit.
            self.notify.notified().await;
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReplyOwner {
    unique: u64,
    disposition: AtomicU8,
    // Return memory before the final inflight decrement becomes visible.
    _memory: Option<ReplyMemoryGuard>,
    _inflight: InflightGuard,
    // Keep the Roots reservation through the final Notify/count field drop.
    tracker: Arc<ReplyTracker>,
}

impl ReplyOwner {
    pub(super) fn written(&self) {
        self.disposition.store(WRITTEN, Ordering::Release);
    }
    pub(super) fn kernel_forgot(&self) {
        self.disposition.store(KERNEL_FORGOT, Ordering::Release);
    }
}

impl Drop for ReplyOwner {
    fn drop(&mut self) {
        if self.disposition.load(Ordering::Acquire) == PENDING {
            if let Some(observer) = &self.tracker.request_observer {
                // The reply owner is the final proof that a queued response
                // reached the kernel. Dropping it while pending means the
                // request was cancelled during teardown or admission failure.
                // Request observations are idempotent, so a late pump error
                // cannot create a second sample.
                observer.request_cancelled(self.unique);
            }
            self.tracker
                .fail(&io::Error::from(io::ErrorKind::BrokenPipe));
        }
        let mut tickets = self
            .tracker
            .tickets
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // A duplicate unique must not remove the original owner's weak entry.
        if tickets
            .get(&self.unique)
            .is_some_and(|ticket| std::ptr::eq(ticket.as_ptr(), self as *const ReplyOwner))
        {
            tickets.remove(&self.unique);
        }
        let empty = tickets.is_empty().then(|| std::mem::take(&mut *tickets));
        drop(tickets);
        drop(empty);
        // Field destruction happens after this Drop: failure precedes count0.
    }
}

// Conservative explicit allowances for pinned BTreeMap/channel/Bytes internals.
// These must be checked against the exact dependency versions during root review.
// Request owners additionally have the existing 8192-byte Control allowance.
pub(super) const DIRECT_OWNER_BOUND: usize =
    std::mem::size_of::<ReplyOwner>() + 2 * std::mem::size_of::<usize>()
    + 384 /* Rust 1.98.1 weak-map internal node <=288 on x86_64 */
    + 128 /* channel node */
    + 128 /* Bytes owner + owned Vec/Arc wrapper */
    + ADMISSION_GUARD_ALLOCATION_BOUND
    + 64 /* serialized 16-byte error packet and allocator allowance */
    + 32 /* transient admission charge vector */;
const _: () = assert!(DIRECT_OWNER_BOUND <= 1024);

pub(super) struct ReplyPumpLifetime {
    tracker: Option<Arc<ReplyTracker>>,
    completed: bool,
}
impl ReplyPumpLifetime {
    pub(super) fn new(tracker: Option<Arc<ReplyTracker>>) -> Self {
        Self {
            tracker,
            completed: false,
        }
    }
    pub(super) fn fail(&self, error: &io::Error) {
        if let Some(tracker) = &self.tracker {
            tracker.fail(error);
        }
    }
    pub(super) fn completed(&mut self) {
        self.completed = true;
    }
}
impl Drop for ReplyPumpLifetime {
    fn drop(&mut self) {
        if !self.completed {
            self.fail(&io::Error::from(io::ErrorKind::BrokenPipe));
        }
    }
}

/// Stack-only extraction works for both header/body and empty-header owned packets.
pub(super) fn wire_header(data: &[u8], body: Option<&[u8]>) -> io::Result<(u32, i32, u64)> {
    let body = body.unwrap_or_default();
    let mut header = [0u8; 16];
    if data
        .len()
        .checked_add(body.len())
        .map_or(true, |len| len < header.len())
    {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    let first = data.len().min(header.len());
    header[..first].copy_from_slice(&data[..first]);
    if first < header.len() {
        header[first..].copy_from_slice(&body[..16 - first]);
    }
    Ok((
        u32::from_le_bytes(header[..4].try_into().unwrap()),
        i32::from_le_bytes(header[4..8].try_into().unwrap()),
        u64::from_le_bytes(header[8..16].try_into().unwrap()),
    ))
}
