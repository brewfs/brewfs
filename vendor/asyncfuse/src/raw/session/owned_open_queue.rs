//! Fixed readonly OPEN/OPENDIR records and actual-worker-Waker queue.
use crate::raw::filesystem::Filesystem;
use crate::raw::reply::{InlineRootPermit, ReplyBytes, ReplyMemoryGuard};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use std::alloc::Layout;
use std::collections::hash_map::RandomState;
use std::future::Future;
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

// Pinned bytes 1.12.1 Shared is (*mut u8, usize, AtomicUsize). Vec->Bytes plus
// its first clone can allocate this header; admit it BEFORE either operation.
const SHARED_HEADER_BYTES: usize =
    std::mem::size_of::<(*mut u8, usize, std::sync::atomic::AtomicUsize)>();
const OPEN_PACKET_BYTES: usize = 32;

fn reserve_fixed<T>(vec: &mut Vec<T>, count: usize) -> crate::Result<()> {
    #[cfg(test)]
    allocation_failures::before_reserve()?;
    vec.try_reserve_exact(count)
        .map_err(|_| crate::Errno::from(libc::ENOMEM))?;
    #[cfg(test)]
    allocation_failures::record_allocation(vec.as_ptr() as usize);
    Ok(())
}

#[cfg(test)]
mod allocation_failures {
    std::thread_local! {
        static FAIL_AT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static ATTEMPT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        static WITNESS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    pub(super) fn set(at: usize) {
        FAIL_AT.set(at);
        ATTEMPT.set(0);
    }
    pub(super) fn witness(active: bool) {
        WITNESS.set(active);
    }
    pub(super) fn record_allocation(pointer: usize) {
        #[cfg(any(feature = "tokio-runtime", feature = "io-uring-runtime"))]
        if WITNESS.get() {
            super::tests::allocator::append_watch(pointer);
        }
        #[cfg(not(any(feature = "tokio-runtime", feature = "io-uring-runtime")))]
        let _ = pointer;
    }
    pub(super) fn record_controller(controller: &super::ControllerHandle) {
        let (_, offset) = std::alloc::Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
            .extend(std::alloc::Layout::new::<super::Controller>())
            .expect("checked controller Layout");
        record_allocation(
            std::sync::Arc::as_ptr(controller.inner.as_ref().unwrap()) as usize - offset,
        );
    }
    pub(super) fn before_reserve() -> crate::Result<()> {
        let attempt = ATTEMPT.get() + 1;
        ATTEMPT.set(attempt);
        if FAIL_AT.get() == attempt {
            Err(libc::ENOMEM.into())
        } else {
            Ok(())
        }
    }
}

#[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
pub(super) fn inject_startup_reserve_failure(at: usize) {
    allocation_failures::set(at);
}

#[derive(Debug)]
struct Record {
    unique: u64,
    box_retired: bool,
    packet: Option<Bytes>,
    request: Option<ReplyMemoryGuard>,
    roots: InlineRootPermit,
}
impl Record {
    fn can_retire(&self) -> bool {
        self.box_retired && self.packet.as_ref().map_or(true, Bytes::is_unique)
    }
    fn retire(mut self) {
        // Deallocate the real Shared header + Vec first; then the original
        // request disposition/inflight owner; the independent grant is last.
        drop(self.packet.take());
        drop(self.request.take());
        drop(self.roots);
    }
}

const NONE: usize = usize::MAX;

#[derive(Debug)]
struct Slot {
    record: Option<Record>,
    // Live slots form doubly linked hash chains. Free slots reuse `next` for
    // their lane's free list. No insertion/removal allocates or moves a record.
    next: usize,
    previous: usize,
}

#[derive(Debug, Default)]
struct RecordTable {
    slots: Vec<Slot>,
    buckets: Vec<usize>,
    free_heads: Vec<usize>,
    lane_capacity: usize,
    hash: RandomState,
}

impl RecordTable {
    fn bucket(&self, unique: u64) -> usize {
        self.hash.hash_one(unique) as usize % self.buckets.len()
    }

    fn find(&self, unique: u64) -> Option<usize> {
        if self.buckets.is_empty() {
            return None;
        }
        let mut index = self.buckets[self.bucket(unique)];
        while index != NONE {
            let slot = &self.slots[index];
            if slot.record.as_ref().expect("live hash entry").unique == unique {
                return Some(index);
            }
            index = slot.next;
        }
        None
    }

    fn insert(&mut self, lane: usize, record: Record) -> usize {
        let index = self.free_heads[lane];
        assert_ne!(index, NONE, "fixed record slot checked before reservation");
        let bucket = self.bucket(record.unique);
        self.free_heads[lane] = self.slots[index].next;
        let next = self.buckets[bucket];
        self.slots[index] = Slot {
            record: Some(record),
            next,
            previous: NONE,
        };
        if next != NONE {
            self.slots[next].previous = index;
        }
        self.buckets[bucket] = index;
        index
    }

    fn retire_if_ready(&mut self, index: usize) -> Option<Record> {
        let slot = &self.slots[index];
        if !slot.record.as_ref().is_some_and(Record::can_retire) {
            return None;
        }
        let (next, previous) = (slot.next, slot.previous);
        let record = self.slots[index].record.take().expect("live record");
        if previous == NONE {
            let bucket = self.bucket(record.unique);
            self.buckets[bucket] = next;
        } else {
            self.slots[previous].next = next;
        }
        if next != NONE {
            self.slots[next].previous = previous;
        }
        let lane = index / self.lane_capacity;
        self.slots[index].next = self.free_heads[lane];
        self.slots[index].previous = NONE;
        self.free_heads[lane] = index;
        Some(record)
    }
}

#[derive(Debug)]
struct Controller {
    records: Mutex<RecordTable>,
    base_roots: InlineRootPermit,
}

/// Arc never escapes this wrapper, and no Weak is ever constructed. Each strong
/// clone ALWAYS calls into_inner. Exactly one concurrent last Drop obtains T;
/// pinned std frees the implicit Weak/Arc allocation BEFORE returning that T.
#[derive(Debug)]
pub(crate) struct ControllerHandle {
    inner: Option<Arc<Controller>>,
}
impl ControllerHandle {
    pub(crate) fn empty() -> Self {
        Self { inner: None }
    }
    pub(crate) fn reap(&self, unique: u64) {
        let Some(controller) = &self.inner else {
            return;
        };
        // A packet Drop identifies exactly the record whose ownership changed.
        // Concurrent final-clone callbacks may find it already retired.
        let retired = {
            let mut records = controller.records.lock().unwrap_or_else(|e| e.into_inner());
            records
                .find(unique)
                .and_then(|index| records.retire_if_ready(index))
        };
        // Run every Drop/wake outside the table lock.
        if let Some(record) = retired {
            record.retire();
        }
    }
    fn has_capacity(&self, first: usize, capacity: usize) -> bool {
        let Some(controller) = &self.inner else {
            return false;
        };
        let records = controller.records.lock().unwrap_or_else(|e| e.into_inner());
        capacity != 0 && records.free_heads[first / capacity] != NONE
    }
    fn insert(
        &self,
        first: usize,
        capacity: usize,
        unique: u64,
        request: Option<ReplyMemoryGuard>,
        roots: InlineRootPermit,
    ) -> usize {
        let controller = self.inner.as_ref().expect("admitted controller");
        let mut records = controller.records.lock().unwrap_or_else(|e| e.into_inner());
        records.insert(
            first / capacity,
            Record {
                unique,
                box_retired: false,
                packet: None,
                request,
                roots,
            },
        )
    }
    fn box_retired(&self, index: usize) {
        let retired = {
            let controller = self.inner.as_ref().expect("admitted controller");
            let mut records = controller.records.lock().unwrap_or_else(|e| e.into_inner());
            records.slots[index]
                .record
                .as_mut()
                .expect("live Box record")
                .box_retired = true;
            records.retire_if_ready(index)
        };
        if let Some(record) = retired {
            record.retire();
        }
    }
    pub(crate) fn wrap_packet(&self, unique: u64, packet: Vec<u8>) -> Result<ReplyBytes, Vec<u8>> {
        let Some(controller) = &self.inner else {
            return Err(packet);
        };
        let mut records = controller.records.lock().unwrap_or_else(|e| e.into_inner());
        let Some(index) = records.find(unique) else {
            return Err(packet);
        };
        let record = records.slots[index]
            .record
            .as_mut()
            .expect("live hash entry");
        assert!(
            record.packet.is_none(),
            "one actual reply per original request"
        );
        assert!(
            packet.capacity() <= OPEN_PACKET_BYTES,
            "fixed OPEN/OPENDIR wire allocation bound"
        );
        let bytes = Bytes::from(packet);
        record.packet = Some(bytes.clone());
        Ok(ReplyBytes::tracked(bytes, self.clone(), Some(unique)))
    }
}
impl Clone for ControllerHandle {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl Drop for ControllerHandle {
    fn drop(&mut self) {
        // Take the Arc before any destructor/callback can unwind. Every strong
        // owner must reach into_inner, including Drop cleanup during unwinding.
        let Some(arc) = self.inner.take() else {
            return;
        };
        let Some(mut controller) = Arc::into_inner(arc) else {
            return;
        };
        assert!(
            controller
                .records
                .get_mut()
                .unwrap_or_else(|e| e.into_inner())
                .slots
                .iter()
                .all(|slot| slot.record.is_none()),
            "live records own a real entry or packet handle"
        );
        // No controller Arc allocation exists now. Take the grant onto stack,
        // deallocate the slots, hash buckets and free heads, then refund.
        let roots = std::mem::replace(&mut controller.base_roots, InlineRootPermit::empty());
        let records = controller
            .records
            .get_mut()
            .unwrap_or_else(|e| e.into_inner());
        drop(std::mem::take(records));
        drop(controller);
        drop(roots);
    }
}

struct OwnedOpenFuture {
    // Automatic field Drop order is essential: this complete Box Drop glue
    // runs before the retirement guard, also if the child destructor unwinds.
    future: Option<BoxFuture<'static, ()>>,
    _retirement: BoxRetirement,
}
struct BoxRetirement {
    record: usize,
    controller: ControllerHandle,
}
impl Drop for BoxRetirement {
    fn drop(&mut self) {
        self.controller.box_retired(self.record);
    }
}

pub(super) struct OpenLane {
    entries: Vec<OwnedOpenFuture>,
    cursor: usize,
    first: usize,
    capacity: usize,
    controller: ControllerHandle,
}
impl OpenLane {
    pub(super) fn has_capacity(&self) -> bool {
        self.entries.len() < self.capacity
            && self.controller.has_capacity(self.first, self.capacity)
    }
    pub(super) fn per_open_bytes(future_bytes: usize) -> crate::Result<u64> {
        future_bytes
            .checked_add(OPEN_PACKET_BYTES)
            .and_then(|n| n.checked_add(SHARED_HEADER_BYTES))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| libc::ENOMEM.into())
    }
    pub(super) fn push_admitted<F>(
        &mut self,
        future: F,
        unique: u64,
        request: Option<ReplyMemoryGuard>,
        roots: Option<InlineRootPermit>,
    ) where
        F: Future<Output = ()> + Send + 'static,
    {
        assert!(
            self.has_capacity(),
            "fixed entry and retirement record checked before Box"
        );
        let record = self.controller.insert(
            self.first,
            self.capacity,
            unique,
            request,
            roots.unwrap_or_else(InlineRootPermit::empty),
        );
        self.entries.push(OwnedOpenFuture {
            future: Some(Box::pin(future)),
            _retirement: BoxRetirement {
                record,
                controller: self.controller.clone(),
            },
        });
    }
    pub(super) fn poll_one_completion(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let len = self.entries.len();
        if len == 0 {
            return Poll::Pending;
        }
        let start = self.cursor % len;
        for offset in 0..len {
            let index = (start + offset) % len;
            if self.entries[index]
                .future
                .as_mut()
                .expect("live Box")
                .as_mut()
                .poll(cx)
                .is_ready()
            {
                drop(self.entries.swap_remove(index));
                self.cursor = if self.entries.is_empty() {
                    0
                } else {
                    index % self.entries.len()
                };
                return Poll::Ready(());
            }
        }
        self.cursor = (start + 1) % len;
        Poll::Pending
    }
}
impl Drop for OpenLane {
    fn drop(&mut self) {
        // Entry Drop marks retirement only AFTER actual Boxes deallocate.
        // This complete Vec deallocation precedes the lane handle's final Drop.
        drop(std::mem::take(&mut self.entries));
    }
}

pub(super) struct OpenLanePlan {
    lanes: Vec<Option<OpenLane>>,
    controller: ControllerHandle,
}
impl OpenLanePlan {
    pub(super) fn storage_bytes(workers: usize, capacity: usize) -> crate::Result<u64> {
        let count = workers
            .checked_mul(capacity)
            .ok_or_else(|| crate::Errno::from(libc::ENOMEM))?;
        let arc = Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
            .extend(Layout::new::<Controller>())
            .map_err(|_| crate::Errno::from(libc::ENOMEM))?
            .0
            .pad_to_align();
        let plans = Layout::array::<Option<OpenLane>>(workers)
            .map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        let slots = Layout::array::<Slot>(count).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        let buckets =
            Layout::array::<usize>(count).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        let free_heads =
            Layout::array::<usize>(workers).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        let entries = Layout::array::<OwnedOpenFuture>(capacity)
            .map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        arc.size()
            .checked_add(plans.size())
            .and_then(|n| n.checked_add(slots.size()))
            .and_then(|n| n.checked_add(buckets.size()))
            .and_then(|n| n.checked_add(free_heads.size()))
            .and_then(|n| {
                entries
                    .size()
                    .checked_mul(workers)
                    .and_then(|e| n.checked_add(e))
            })
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| libc::ENOMEM.into())
    }
    pub(super) fn prepare<FS: Filesystem>(
        fs: &FS,
        workers: usize,
        capacity: usize,
    ) -> crate::Result<Option<Self>> {
        if !fs.supports_read_cancellation() {
            return Ok(None);
        }
        // Admit every requested allocation before first controller/slot/Vec alloc.
        let bytes = Self::storage_bytes(workers, capacity)?;
        let roots = fs
            .reserve_inline_root_memory(bytes)?
            .unwrap_or_else(InlineRootPermit::empty);
        // Declare the admitted grant first: every partial allocation drops
        // before the grant on any fallible startup path.
        let count = workers * capacity;
        let mut records = RecordTable {
            lane_capacity: capacity,
            ..RecordTable::default()
        };
        reserve_fixed(&mut records.slots, count)?;
        for index in 0..count {
            records.slots.push(Slot {
                record: None,
                next: if (index + 1) % capacity == 0 {
                    NONE
                } else {
                    index + 1
                },
                previous: NONE,
            });
        }
        reserve_fixed(&mut records.buckets, count)?;
        records.buckets.resize(count, NONE);
        reserve_fixed(&mut records.free_heads, workers)?;
        records.free_heads.extend((0..workers).map(|lane| {
            if capacity == 0 {
                NONE
            } else {
                lane * capacity
            }
        }));
        let controller = ControllerHandle {
            inner: Some(Arc::new(Controller {
                records: Mutex::new(records),
                base_roots: roots,
            })),
        };
        #[cfg(test)]
        allocation_failures::record_controller(&controller);
        let mut plan = Self {
            lanes: Vec::new(),
            controller,
        };
        reserve_fixed(&mut plan.lanes, workers)?;
        for idx in 0..workers {
            let mut lane = OpenLane {
                entries: Vec::new(),
                cursor: 0,
                first: idx * capacity,
                capacity,
                controller: plan.controller.clone(),
            };
            reserve_fixed(&mut lane.entries, capacity)?;
            plan.lanes.push(Some(lane));
        }
        Ok(Some(plan))
    }
    pub(super) fn controller(&self) -> ControllerHandle {
        self.controller.clone()
    }
    pub(super) fn take(&mut self, index: usize) -> OpenLane {
        self.lanes[index].take().expect("one fixed lane per worker")
    }
}
impl Drop for OpenLanePlan {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.lanes));
    }
}

#[cfg(all(test, any(feature = "tokio-runtime", feature = "io-uring-runtime")))]
#[path = "owned_open_queue_tests.rs"]
pub(crate) mod tests;
