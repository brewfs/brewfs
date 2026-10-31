//! Real queue Context, retained Waker and real worker-handler tests.
use super::super::*;
use super::*;
use crate::raw::abi::{fuse_opcode, fuse_out_header, FUSE_OUT_HEADER_SIZE};
use crate::raw::reply::{ReplyInit, ReplyOpen};
use crate::raw::request::Request;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Wake, Waker};
use std::time::Duration;

#[derive(Debug)]
struct Charge {
    used: Arc<AtomicUsize>,
    bytes: usize,
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[derive(Debug, Default)]
pub(crate) struct ProbeFs {
    roots: Arc<AtomicUsize>,
    deny_roots: AtomicBool,
    root_charges: Mutex<Vec<usize>>,
    control: Arc<AtomicUsize>,
    control_charges: Mutex<Vec<usize>>,
    opens: AtomicUsize,
    opendirs: AtomicUsize,
    closes: AtomicUsize,
    ready: AtomicBool,
    wakers: Mutex<BTreeMap<u64, Waker>>,
}

impl ProbeFs {
    async fn wait_open(&self, unique: u64) -> crate::Result<ReplyOpen> {
        futures_util::future::poll_fn(|cx| {
            if self.ready.load(Ordering::Acquire) {
                Poll::Ready(Ok(ReplyOpen {
                    fh: unique,
                    flags: 0,
                }))
            } else {
                self.wakers
                    .lock()
                    .unwrap()
                    .insert(unique, cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
    fn release_opens(&self) {
        self.ready.store(true, Ordering::Release);
        for waker in self.wakers.lock().unwrap().values() {
            waker.wake_by_ref();
        }
    }
}

impl Filesystem for ProbeFs {
    async fn init(&self, _: Request) -> crate::Result<ReplyInit> {
        Ok(ReplyInit::default())
    }
    async fn destroy(&self, _: Request) {}
    fn supports_read_cancellation(&self) -> bool {
        true
    }
    fn reserve_input_buffer_memory(&self, bytes: u64) -> crate::Result<Option<ReplyMemoryGuard>> {
        if self.deny_roots.load(Ordering::Acquire) {
            return Err(libc::ENOMEM.into());
        }
        let bytes = usize::try_from(bytes).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        self.root_charges.lock().unwrap().push(bytes);
        self.roots.fetch_add(bytes, Ordering::AcqRel);
        Ok(Some(Arc::new(Charge {
            used: self.roots.clone(),
            bytes,
        })))
    }
    fn reserve_inline_root_memory(&self, bytes: u64) -> crate::Result<Option<InlineRootPermit>> {
        if self.deny_roots.load(Ordering::Acquire) {
            return Err(libc::ENOMEM.into());
        }
        let bytes = usize::try_from(bytes).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        self.root_charges.lock().unwrap().push(bytes);
        self.roots.fetch_add(bytes, Ordering::AcqRel);
        Ok(Some(
            InlineRootPermit::try_new(Charge {
                used: self.roots.clone(),
                bytes,
            })
            .unwrap(),
        ))
    }
    fn reserve_control_memory(&self, bytes: u64) -> crate::Result<Option<ReplyMemoryGuard>> {
        let bytes = usize::try_from(bytes).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        self.control_charges.lock().unwrap().push(bytes);
        self.control.fetch_add(bytes, Ordering::AcqRel);
        Ok(Some(Arc::new(Charge {
            used: self.control.clone(),
            bytes,
        })))
    }
    async fn open(&self, req: Request, _: crate::Inode, _: u32) -> crate::Result<ReplyOpen> {
        self.opens.fetch_add(1, Ordering::AcqRel);
        self.wait_open(req.unique).await
    }
    async fn opendir(&self, req: Request, _: crate::Inode, _: u32) -> crate::Result<ReplyOpen> {
        self.opendirs.fetch_add(1, Ordering::AcqRel);
        self.wait_open(req.unique).await
    }
    async fn release(
        &self,
        _: Request,
        _: crate::Inode,
        _: u64,
        _: u32,
        _: u64,
        _: bool,
    ) -> crate::Result<()> {
        self.closes.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
    async fn releasedir(&self, _: Request, _: crate::Inode, _: u64, _: u32) -> crate::Result<()> {
        self.closes.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
    async fn flush(&self, _: Request, _: crate::Inode, _: u64, _: u64) -> crate::Result<()> {
        self.closes.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
    #[cfg(feature = "file-lock")]
    async fn getlk(
        &self,
        _: Request,
        _: crate::Inode,
        _: u64,
        _: u64,
        _: u64,
        _: u64,
        _: u32,
        _: u32,
    ) -> crate::Result<crate::raw::reply::ReplyLock> {
        Err(libc::ENOSYS.into())
    }
    #[cfg(feature = "file-lock")]
    async fn setlk(
        &self,
        _: Request,
        _: crate::Inode,
        _: u64,
        _: u64,
        _: u64,
        _: u64,
        _: u32,
        _: u32,
        _: bool,
    ) -> crate::Result<()> {
        Err(libc::ENOSYS.into())
    }
}

#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

#[test]
fn open_lane_passes_the_actual_parent_waker_and_retained_wake_survives_removal() {
    let fs = ProbeFs::default();
    let mut plan = OpenLanePlan::prepare(&fs, 1, 2).unwrap().unwrap();
    let mut lane = plan.take(0);
    drop(plan);
    let captured = Arc::new(Mutex::new(None::<Waker>));
    let ready = Arc::new(AtomicBool::new(false));
    let captured_in = captured.clone();
    let ready_in = ready.clone();
    let future = futures_util::future::poll_fn(move |cx| {
        *captured_in.lock().unwrap() = Some(cx.waker().clone());
        if ready_in.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    });
    let (bytes, _) = (
        std::mem::size_of_val(&future),
        std::mem::align_of_val(&future),
    );
    let roots = fs
        .reserve_inline_root_memory(OpenLane::per_open_bytes(bytes).unwrap())
        .unwrap();
    lane.push_admitted(future, 1, None, roots);
    let wake_count = Arc::new(WakeCount::default());
    let parent = Waker::from(wake_count.clone());
    let mut cx = Context::from_waker(&parent);
    assert!(lane.poll_one_completion(&mut cx).is_pending());
    let retained = captured.lock().unwrap().take().unwrap();
    assert!(
        retained.will_wake(&parent),
        "no per-entry Task Waker may replace worker Context"
    );
    ready.store(true, Ordering::Release);
    retained.wake_by_ref();
    assert_eq!(wake_count.0.load(Ordering::Acquire), 1);
    assert!(lane.poll_one_completion(&mut cx).is_ready());
    drop(lane);
    // Its child Box and fixed Vec are gone; this actual Waker still only
    // addresses the parent. It cannot address a removed or moved entry.
    retained.wake_by_ref();
    assert_eq!(wake_count.0.load(Ordering::Acquire), 2);
}

#[test]
fn open_lane_plan_checked_overflow_rejects_before_reservation() {
    let fs = ProbeFs::default();
    assert!(OpenLanePlan::prepare(&fs, usize::MAX, 2).is_err());
    assert!(fs.root_charges.lock().unwrap().is_empty());
}

#[test]
fn held_packets_keep_each_lane_full_until_its_own_final_clone_drops() {
    let fs = ProbeFs::default();
    let mut plan = OpenLanePlan::prepare(&fs, 2, 1).unwrap().unwrap();
    let mut first = plan.take(0);
    let mut second = plan.take(1);
    first.push_admitted(async {}, 1, None, None);
    second.push_admitted(async {}, 2, None, None);
    let packet = first
        .controller
        .wrap_packet(1, vec![0; OPEN_PACKET_BYTES])
        .unwrap();
    let last = packet.clone();
    let other = second
        .controller
        .wrap_packet(2, vec![0; OPEN_PACKET_BYTES])
        .unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(first.poll_one_completion(&mut cx).is_ready());
    assert!(second.poll_one_completion(&mut cx).is_ready());
    assert!(!first.has_capacity());
    assert!(!second.has_capacity());
    drop(packet);
    assert!(!first.has_capacity());
    drop(last);
    assert!(first.has_capacity());
    assert!(
        !second.has_capacity(),
        "a free slot cannot migrate between workers"
    );
    // Reuse the freed slot while a different lane still holds its real packet.
    first.push_admitted(async {}, 3, None, None);
    let replacement = first
        .controller
        .wrap_packet(3, vec![3; OPEN_PACKET_BYTES])
        .unwrap();
    // A delayed callback from a concurrent old clone must not affect the reuse.
    first.controller.reap(1);
    assert!(!first.has_capacity());
    drop(replacement); // packet-before-Box retirement order
    assert!(!first.has_capacity());
    assert!(first.poll_one_completion(&mut cx).is_ready());
    assert!(first.has_capacity());
    drop(other);
    assert!(second.has_capacity());
    drop((first, second, plan));
    assert_eq!(fs.roots.load(Ordering::Acquire), 0);
}

#[test]
fn colliding_unique_index_survives_middle_head_tail_removal_and_reuse() {
    let fs = ProbeFs::default();
    let mut plan = OpenLanePlan::prepare(&fs, 1, 4).unwrap().unwrap();
    let mut lane = plan.take(0);
    let controller = plan.controller();
    let keys: Vec<_> = {
        let table = controller.inner.as_ref().unwrap().records.lock().unwrap();
        (1..)
            .filter(|unique| table.bucket(*unique) == 0)
            .take(4)
            .collect()
    };
    let mut packets = Vec::new();
    for &unique in &keys {
        lane.push_admitted(async {}, unique, None, None);
        packets.push(Some(
            controller
                .wrap_packet(unique, vec![unique as u8; OPEN_PACKET_BYTES])
                .unwrap(),
        ));
    }
    assert_eq!(
        controller.wrap_packet(u64::MAX, vec![9]).unwrap_err(),
        vec![9]
    );
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..4 {
        assert!(lane.poll_one_completion(&mut cx).is_ready());
    }
    assert!(!lane.has_capacity());
    for (step, removed) in [1, 3, 0, 2].into_iter().enumerate() {
        drop(packets[removed].take());
        let table = controller.inner.as_ref().unwrap().records.lock().unwrap();
        assert!(table.find(keys[removed]).is_none());
        for (index, packet) in packets.iter().enumerate() {
            if packet.is_some() {
                assert!(
                    table.find(keys[index]).is_some(),
                    "collision neighbor lost at step {step}"
                );
            }
        }
        drop(table);
        assert!(lane.has_capacity());
    }
    // Exercise every returned slot again and cancel without creating packets.
    for unique in 100..104 {
        lane.push_admitted(std::future::pending(), unique, None, None);
    }
    assert!(!lane.has_capacity());
    drop((lane, plan, controller));
    assert_eq!(fs.roots.load(Ordering::Acquire), 0);
}

#[test]
fn simultaneous_packet_clone_drops_retire_once_without_polling() {
    for _ in 0..32 {
        let fs = ProbeFs::default();
        let mut plan = OpenLanePlan::prepare(&fs, 1, 1).unwrap().unwrap();
        let mut lane = plan.take(0);
        let used = Arc::new(AtomicUsize::new(1));
        lane.push_admitted(
            DataFuture([0; 37]),
            1,
            Some(Arc::new(Charge {
                used: used.clone(),
                bytes: 1,
            })),
            None,
        );
        let packet = lane
            .controller
            .wrap_packet(1, vec![0; OPEN_PACKET_BYTES])
            .unwrap();
        let clone = packet.clone();
        drop(lane);
        assert_eq!(used.load(Ordering::Acquire), 1);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                drop(clone);
            });
            barrier.wait();
            drop(packet);
        });
        assert_eq!(used.load(Ordering::Acquire), 0);
        drop(plan);
        assert_eq!(fs.roots.load(Ordering::Acquire), 0);
    }
}

#[test]
fn zero_capacity_plan_has_no_available_slot() {
    let fs = ProbeFs::default();
    let mut plan = OpenLanePlan::prepare(&fs, 1, 0).unwrap().unwrap();
    let lane = plan.take(0);
    assert!(!lane.has_capacity());
    assert_eq!(
        plan.controller().wrap_packet(1, vec![1]).unwrap_err(),
        vec![1]
    );
    drop((lane, plan));
    assert_eq!(fs.roots.load(Ordering::Acquire), 0);
}

#[test]
fn request_guard_can_reenter_controller_after_slot_return() {
    #[derive(Debug)]
    struct ReentrantGuard {
        controller: ControllerHandle,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for ReentrantGuard {
        fn drop(&mut self) {
            assert!(self.controller.has_capacity(0, 1));
            self.dropped.store(true, Ordering::Release);
        }
    }
    let fs = ProbeFs::default();
    let mut plan = OpenLanePlan::prepare(&fs, 1, 1).unwrap().unwrap();
    let mut lane = plan.take(0);
    let dropped = Arc::new(AtomicBool::new(false));
    lane.push_admitted(
        std::future::pending(),
        1,
        Some(Arc::new(ReentrantGuard {
            controller: plan.controller(),
            dropped: dropped.clone(),
        })),
        None,
    );
    let packet = lane
        .controller
        .wrap_packet(1, vec![0; OPEN_PACKET_BYTES])
        .unwrap();
    drop(lane);
    assert!(!dropped.load(Ordering::Acquire));
    drop(packet);
    assert!(dropped.load(Ordering::Acquire));
    drop(plan);
    assert_eq!(fs.roots.load(Ordering::Acquire), 0);
}

fn item(session: &Session<ProbeFs>, fs: &ProbeFs, opcode: fuse_opcode, unique: u64) -> WorkItem {
    let body = if matches!(opcode, fuse_opcode::FUSE_OPEN | fuse_opcode::FUSE_OPENDIR) {
        vec![0; 8]
    } else {
        let mut data = Vec::with_capacity(24);
        data.extend_from_slice(&77u64.to_le_bytes());
        data.extend_from_slice(&0u32.to_le_bytes());
        data.extend_from_slice(&0u32.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data
    };
    let opcode = opcode as u32;
    let memory = if worker::is_close_opcode(opcode) {
        let bytes = session
            .workers
            .as_ref()
            .unwrap()
            .close_request_memory_bytes(body.len() as u64)
            .unwrap();
        fs.reserve_control_memory(bytes).unwrap()
    } else {
        Some(Arc::new(()) as ReplyMemoryGuard)
    };
    WorkItem {
        unique,
        opcode,
        in_header: InHeaderLite {
            nodeid: 1,
            uid: 0,
            gid: 0,
            pid: 0,
        },
        data: Bytes::from(body),
        _inflight_guard: Some(InflightGuard::new(
            session.inflight.clone(),
            session.inflight_notify.clone(),
        )),
        _memory_guard: memory,
        _response_registration: None,
    }
}

fn header(packet: &FuseData) -> fuse_out_header {
    let bytes = match packet {
        Either::Left(bytes) => bytes.as_slice(),
        Either::Right((head, body)) if head.is_empty() => body.as_ref(),
        Either::Right((head, _)) => head.as_slice(),
    };
    assert!(bytes.len() >= FUSE_OUT_HEADER_SIZE);
    fuse_out_header {
        len: u32::from_le_bytes(bytes[..4].try_into().unwrap()),
        error: i32::from_le_bytes(bytes[4..8].try_into().unwrap()),
        unique: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
    }
}

async fn packet(receiver: &mut UnboundedReceiver<FuseData>) -> FuseData {
    tokio::time::timeout(Duration::from_secs(1), receiver.next())
        .await
        .expect("real worker progress deadline")
        .expect("real response")
}

async fn entered(fs: &ProbeFs, count: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while fs.opens.load(Ordering::Acquire) + fs.opendirs.load(Ordering::Acquire) != count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual filesystem future was never polled");
}

#[tokio::test]
async fn actual_outer_box_roots_denial_preserves_tracked_errno_without_fs_poll() {
    let fs = Arc::new(ProbeFs::default());
    let mut session = Session::new(MountOptions::default()).with_workers(1, 2);
    let mut receiver = session.response_receivers[0].take().unwrap();
    session.ensure_workers(fs.clone()).unwrap();
    let baseline = fs.roots.load(Ordering::Acquire);
    fs.deny_roots.store(true, Ordering::Release);
    for (opcode, unique) in [
        (fuse_opcode::FUSE_OPEN, 10),
        (fuse_opcode::FUSE_OPENDIR, 11),
    ] {
        session
            .workers
            .as_ref()
            .unwrap()
            .submit(item(&session, &fs, opcode, unique));
        let reply = packet(&mut receiver).await;
        assert_eq!(header(&reply).unique, unique);
        assert_eq!(header(&reply).error, -libc::ENOMEM);
        assert_eq!(
            session.inflight.load(Ordering::Acquire),
            1,
            "real final packet still owns request"
        );
        assert_eq!(fs.roots.load(Ordering::Acquire), baseline);
        drop(reply);
        assert_eq!(session.inflight.load(Ordering::Acquire), 0);
    }
    assert_eq!(fs.opens.load(Ordering::Acquire), 0);
    assert_eq!(fs.opendirs.load(Ordering::Acquire), 0);
    session.workers.as_mut().unwrap().shutdown().await;
}

#[tokio::test]
async fn full_open_lane_keeps_flush_release_and_releasedir_progress() {
    let fs = Arc::new(ProbeFs::default());
    let mut session = Session::new(MountOptions::default()).with_workers(1, 2);
    let mut receiver = session.response_receivers[0].take().unwrap();
    session.ensure_workers(fs.clone()).unwrap();
    session
        .workers
        .as_ref()
        .unwrap()
        .submit(item(&session, &fs, fuse_opcode::FUSE_OPEN, 10));
    session
        .workers
        .as_ref()
        .unwrap()
        .submit(item(&session, &fs, fuse_opcode::FUSE_OPENDIR, 11));
    entered(&fs, 2).await;
    assert_eq!(session.inflight.load(Ordering::Acquire), 2);
    let boxed_bytes =
        OpenLane::per_open_bytes(worker::readonly_ordinary_future_layout::<ProbeFs>().0).unwrap()
            as usize;
    assert_eq!(
        fs.root_charges
            .lock()
            .unwrap()
            .iter()
            .filter(|bytes| **bytes == boxed_bytes)
            .count(),
        2
    );
    for (opcode, unique) in [
        (fuse_opcode::FUSE_FLUSH, 20),
        (fuse_opcode::FUSE_RELEASE, 21),
        (fuse_opcode::FUSE_RELEASEDIR, 22),
    ] {
        let work = item(&session, &fs, opcode, unique);
        let expected = session
            .workers
            .as_ref()
            .unwrap()
            .close_request_memory_bytes(work.data.len() as u64)
            .unwrap() as usize;
        session.workers.as_ref().unwrap().submit(work);
        let reply = packet(&mut receiver).await;
        assert_eq!(header(&reply).unique, unique);
        assert_eq!(header(&reply).error, 0);
        assert_eq!(
            *fs.control_charges.lock().unwrap().last().unwrap(),
            expected
        );
        assert_eq!(
            fs.control.load(Ordering::Acquire),
            expected,
            "close fee retains real packet"
        );
        drop(reply);
        assert_eq!(fs.control.load(Ordering::Acquire), 0);
    }
    assert_eq!(fs.closes.load(Ordering::Acquire), 3);
    session.workers.as_mut().unwrap().shutdown().await;
    assert_eq!(session.inflight.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn real_worker_open_and_opendir_complete_from_child_wake_without_new_input() {
    let fs = Arc::new(ProbeFs::default());
    let mut session = Session::new(MountOptions::default()).with_workers(1, 2);
    let mut receiver = session.response_receivers[0].take().unwrap();
    session.ensure_workers(fs.clone()).unwrap();
    for (opcode, unique) in [
        (fuse_opcode::FUSE_OPEN, 10),
        (fuse_opcode::FUSE_OPENDIR, 11),
    ] {
        session
            .workers
            .as_ref()
            .unwrap()
            .submit(item(&session, &fs, opcode, unique));
    }
    entered(&fs, 2).await;
    fs.release_opens();
    let first = packet(&mut receiver).await;
    let second = packet(&mut receiver).await;
    let mut uniques = [header(&first).unique, header(&second).unique];
    uniques.sort_unstable();
    assert_eq!(uniques, [10, 11]);
    assert_eq!(header(&first).error, 0);
    assert_eq!(header(&second).error, 0);
    assert_eq!(
        session.inflight.load(Ordering::Acquire),
        2,
        "held real packets retain original owners"
    );
    drop(first);
    drop(second);
    // Captured worker Wakers are deliberately retained through cleanup. They
    // do not keep any per-open future Box/Task allocation; worker-header memory
    // retirement is a separate OPEN boundary, never inferred from this counter.
    session.workers.as_mut().unwrap().shutdown().await;
    assert_eq!(session.inflight.load(Ordering::Acquire), 0);
    for waker in fs.wakers.lock().unwrap().values() {
        waker.wake_by_ref();
    }
}

#[path = "lifetime_allocator.rs"]
pub(crate) mod allocator;

// Independent allocation and wait progress tests.

#[derive(Debug)]
struct LargePermit {
    storage: [usize; 10],
    drops: Arc<AtomicUsize>,
}
impl Drop for LargePermit {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}
#[derive(Debug)]
#[repr(align(16))]
struct AlignedPermit(Arc<AtomicUsize>);
impl Drop for AlignedPermit {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
#[derive(Debug)]
struct MovablePermit(Arc<AtomicUsize>);
impl Drop for MovablePermit {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
#[test]
fn inline_permit_rejects_size_and_alignment_with_original_value_and_single_drop() {
    let drops = Arc::new(AtomicUsize::new(0));
    let original = match InlineRootPermit::try_new(LargePermit {
        storage: [37; 10],
        drops: drops.clone(),
    }) {
        Err(original) => original,
        Ok(_) => panic!("oversized erased payload must be rejected before writing storage"),
    };
    assert_eq!(original.storage, [37; 10]);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    drop(original);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    let original = match InlineRootPermit::try_new(AlignedPermit(drops.clone())) {
        Err(original) => original,
        Ok(_) => panic!("over-aligned erased payload must be rejected before writing storage"),
    };
    assert_eq!(drops.load(Ordering::Acquire), 1);
    drop(original);
    assert_eq!(drops.load(Ordering::Acquire), 2);
}
#[test]
fn moving_inline_permit_preserves_exactly_one_original_destructor() {
    fn transfer(permit: InlineRootPermit) -> InlineRootPermit {
        permit
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let permit = InlineRootPermit::try_new(MovablePermit(drops.clone())).unwrap();
    let moved = transfer(permit);
    let moved_again = [moved];
    assert_eq!(drops.load(Ordering::Acquire), 0);
    drop(moved_again);
    assert_eq!(drops.load(Ordering::Acquire), 1);
}

#[derive(Debug)]
struct ObservedRefund {
    used: Arc<AtomicUsize>,
    observed: Arc<AtomicUsize>,
    bytes: usize,
}
impl Drop for ObservedRefund {
    fn drop(&mut self) {
        self.observed.store(
            allocator::DEALLOCATED.load(Ordering::Acquire),
            Ordering::Release,
        );
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct DataFuture([u8; 37]);
impl Future for DataFuture {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        std::hint::black_box(&self.0);
        Poll::Pending
    }
}

#[test]
fn actual_packet_vec_and_shared_header_deallocations_precede_whole_grant_refund() {
    let _serial = allocator::SERIAL.lock().unwrap();
    let fs = ProbeFs::default();
    let mut plan = OpenLanePlan::prepare(&fs, 1, 1).unwrap().unwrap();
    let mut lane = plan.take(0);
    let baseline = fs.roots.load(Ordering::Acquire);
    let observed = Arc::new(AtomicUsize::new(0));
    let bytes = OpenLane::per_open_bytes(std::mem::size_of::<DataFuture>()).unwrap() as usize;
    fs.roots.fetch_add(bytes, Ordering::AcqRel);
    let permit = InlineRootPermit::try_new(ObservedRefund {
        used: fs.roots.clone(),
        observed: observed.clone(),
        bytes,
    })
    .unwrap();
    lane.push_admitted(DataFuture([7; 37]), 71, None, Some(permit));
    let vec = vec![7; OPEN_PACKET_BYTES];
    allocator::watch([vec.as_ptr() as usize, 0, 0, 0]);
    // This synchronous scope contains only direct Vec->Bytes and first clone:
    // pinned bytes allocates exactly one actual Shared header here.
    allocator::capture_shared(true);
    let packet = lane.controller.wrap_packet(71, vec).unwrap();
    allocator::capture_shared(false);
    assert_ne!(
        allocator::captured_shared(),
        0,
        "actual dependency header allocation must be witnessed"
    );
    let held = packet.clone();
    drop(lane); // actual nonzero future Box and actual lane Vec have deallocated
    assert_eq!(fs.roots.load(Ordering::Acquire), baseline + bytes);
    drop(packet);
    assert_eq!(fs.roots.load(Ordering::Acquire), baseline + bytes);
    drop(held);
    assert_eq!(
        observed.load(Ordering::Acquire),
        0b11,
        "actual Vec buffer AND actual dependency Shared header before grant refund"
    );
    assert_eq!(fs.roots.load(Ordering::Acquire), baseline);
    allocator::clear();
    drop(plan);
    assert_eq!(fs.roots.load(Ordering::Acquire), 0);
}

struct PanickingFuture([u8; 37]);
impl Future for PanickingFuture {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        std::hint::black_box(&self.0);
        Poll::Pending
    }
}
impl Drop for PanickingFuture {
    fn drop(&mut self) {
        panic!("controlled OPEN child destructor panic");
    }
}
#[test]
fn child_drop_unwind_still_marks_box_only_after_actual_dealloc_and_keeps_reachable_packet() {
    let _serial = allocator::SERIAL.lock().unwrap();
    let fs = ProbeFs::default();
    let mut plan = OpenLanePlan::prepare(&fs, 1, 1).unwrap().unwrap();
    let mut lane = plan.take(0);
    let baseline = fs.roots.load(Ordering::Acquire);
    let observed = Arc::new(AtomicUsize::new(0));
    let bytes = OpenLane::per_open_bytes(std::mem::size_of::<PanickingFuture>()).unwrap() as usize;
    fs.roots.fetch_add(bytes, Ordering::AcqRel);
    let permit = InlineRootPermit::try_new(ObservedRefund {
        used: fs.roots.clone(),
        observed: observed.clone(),
        bytes,
    })
    .unwrap();
    lane.push_admitted(PanickingFuture([7; 37]), 72, None, Some(permit));
    let actual_box = std::ptr::from_ref(lane.entries[0].future.as_ref().unwrap().as_ref().get_ref())
        .cast::<()>() as usize;
    let vec = vec![7; OPEN_PACKET_BYTES];
    allocator::watch([vec.as_ptr() as usize, 0, actual_box, 0]);
    allocator::capture_shared(true);
    let packet = lane.controller.wrap_packet(72, vec).unwrap();
    allocator::capture_shared(false);
    assert_ne!(allocator::captured_shared(), 0);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(lane)));
    assert!(outcome.is_err());
    assert_eq!(
        allocator::DEALLOCATED.load(Ordering::Acquire),
        0b100,
        "real Box allocation must be gone but actual packet allocations remain"
    );
    assert_eq!(fs.roots.load(Ordering::Acquire), baseline + bytes);
    drop(packet); // still reachable after child unwind; no leaked controller
    assert_eq!(observed.load(Ordering::Acquire), 0b111);
    assert_eq!(fs.roots.load(Ordering::Acquire), baseline);
    allocator::clear();
    drop(plan);
    assert_eq!(fs.roots.load(Ordering::Acquire), 0);
}

#[test]
fn final_controller_grant_refund_follows_actual_arc_slots_lanes_and_plan_dealloc() {
    let _serial = allocator::SERIAL.lock().unwrap();
    let fs = ProbeFs::default();
    let mut plan = OpenLanePlan::prepare(&fs, 1, 2).unwrap().unwrap();
    let used = fs.roots.clone();
    let observed = Arc::new(AtomicUsize::new(0));
    let charge = used.load(Ordering::Acquire);
    let controller = plan.controller.inner.as_ref().unwrap();
    let (_, offset) = Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
        .extend(Layout::new::<Controller>())
        .unwrap();
    let arc_start = Arc::as_ptr(controller) as usize - offset;
    let records = controller.records.lock().unwrap();
    let records_start = records.slots.as_ptr() as usize;
    let buckets_start = records.buckets.as_ptr() as usize;
    let free_heads_start = records.free_heads.as_ptr() as usize;
    drop(records);
    let lanes_start = plan.lanes.as_ptr() as usize;
    let mut lane = plan.take(0);
    let entries_start = lane.entries.as_ptr() as usize;
    // Replace the test provider's original inline permit while maintaining its
    // exact used count. The witness permit uses already allocated test Arcs.
    let payload = Arc::get_mut(plan.controller.inner.as_mut().unwrap());
    assert!(
        payload.is_none(),
        "lane owns an independent actual strong reference"
    );
    drop(
        lane.controller
            .inner
            .take()
            .map(|arc| ControllerHandle { inner: Some(arc) }),
    );
    let payload = Arc::get_mut(plan.controller.inner.as_mut().unwrap()).unwrap();
    let old = std::mem::replace(&mut payload.base_roots, InlineRootPermit::empty());
    drop(old);
    used.fetch_add(charge, Ordering::AcqRel);
    payload.base_roots = InlineRootPermit::try_new(ObservedRefund {
        used: used.clone(),
        observed: observed.clone(),
        bytes: charge,
    })
    .unwrap();
    lane.controller = plan.controller.clone();
    allocator::watch([arc_start, records_start, entries_start, lanes_start]);
    allocator::append_watch(buckets_start);
    allocator::append_watch(free_heads_start);
    drop(plan);
    assert_eq!(used.load(Ordering::Acquire), charge);
    drop(lane);
    assert_eq!(
        observed.load(Ordering::Acquire),
        0b111111,
        "refund must follow allocator return for controller Arc and every base Vec"
    );
    assert_eq!(used.load(Ordering::Acquire), 0);
    allocator::clear();
}

#[test]
fn simultaneous_last_controller_drops_refund_once_after_actual_arc_dealloc() {
    let _serial = allocator::SERIAL.lock().unwrap();
    let used = Arc::new(AtomicUsize::new(1));
    let observed = Arc::new(AtomicUsize::new(0));
    let handle = ControllerHandle {
        inner: Some(Arc::new(Controller {
            records: Mutex::new(RecordTable::default()),
            base_roots: InlineRootPermit::try_new(ObservedRefund {
                used: used.clone(),
                observed: observed.clone(),
                bytes: 1,
            })
            .unwrap(),
        })),
    };
    let (_, offset) = Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
        .extend(Layout::new::<Controller>())
        .unwrap();
    let start = Arc::as_ptr(handle.inner.as_ref().unwrap()) as usize - offset;
    allocator::watch([start, 0, 0, 0]);
    let other = handle.clone();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let signal = barrier.clone();
    let thread = std::thread::spawn(move || {
        signal.wait();
        drop(other);
    });
    barrier.wait();
    drop(handle);
    thread.join().unwrap();
    assert_eq!(observed.load(Ordering::Acquire), 1);
    assert_eq!(
        used.load(Ordering::Acquire),
        0,
        "exactly one final wrapper obtains T"
    );
    allocator::clear();
}

#[test]
fn injected_reserve_failures_retire_partial_base_before_returning_grant() {
    for fail_at in 1..=6 {
        let fs = ProbeFs::default();
        allocation_failures::set(fail_at);
        let result = OpenLanePlan::prepare(&fs, 2, 2);
        allocation_failures::set(0);
        assert!(
            result.is_err(),
            "slots/buckets/free heads/plans/each worker-lane reserve is fallible"
        );
        assert_eq!(
            fs.root_charges.lock().unwrap().len(),
            1,
            "admit before any new fixed allocation"
        );
        assert_eq!(
            fs.roots.load(Ordering::Acquire),
            0,
            "no leaked grant on partial startup"
        );
    }
}

#[derive(Default)]
struct FailureWitnessFs {
    used: Arc<AtomicUsize>,
    observed: Arc<AtomicUsize>,
}
impl Filesystem for FailureWitnessFs {
    async fn init(&self, _: Request) -> crate::Result<ReplyInit> {
        Ok(ReplyInit::default())
    }
    async fn destroy(&self, _: Request) {}
    fn supports_read_cancellation(&self) -> bool {
        true
    }
    fn reserve_inline_root_memory(&self, bytes: u64) -> crate::Result<Option<InlineRootPermit>> {
        let bytes = usize::try_from(bytes).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        self.used.fetch_add(bytes, Ordering::AcqRel);
        Ok(Some(
            InlineRootPermit::try_new(ObservedRefund {
                used: self.used.clone(),
                observed: self.observed.clone(),
                bytes,
            })
            .unwrap(),
        ))
    }
    #[cfg(feature = "file-lock")]
    async fn getlk(
        &self,
        _: Request,
        _: crate::Inode,
        _: u64,
        _: u64,
        _: u64,
        _: u64,
        _: u32,
        _: u32,
    ) -> crate::Result<crate::raw::reply::ReplyLock> {
        Err(libc::ENOSYS.into())
    }
    #[cfg(feature = "file-lock")]
    async fn setlk(
        &self,
        _: Request,
        _: crate::Inode,
        _: u64,
        _: u64,
        _: u64,
        _: u64,
        _: u32,
        _: u32,
        _: bool,
    ) -> crate::Result<()> {
        Err(libc::ENOSYS.into())
    }
}
#[test]
fn each_injected_reserve_failure_refunds_only_after_every_actual_partial_dealloc() {
    let _serial = allocator::SERIAL.lock().unwrap();
    for fail_at in 1..=6 {
        let fs = FailureWitnessFs::default();
        allocator::watch([0; 4]);
        allocation_failures::set(fail_at);
        allocation_failures::witness(true);
        let result = OpenLanePlan::prepare(&fs, 2, 2);
        allocation_failures::witness(false);
        allocation_failures::set(0);
        assert!(result.is_err());
        let expected = allocator::EXPECTED.load(Ordering::Acquire);
        assert_eq!(
            expected.count_ones(),
            if fail_at <= 3 {
                fail_at as u32 - 1
            } else {
                fail_at as u32
            },
            "every successful table/controller/plans/lane allocation before the injected failure"
        );
        assert_eq!(
            fs.observed.load(Ordering::Acquire),
            expected,
            "base refund must follow every actual partial allocator dealloc return"
        );
        assert_eq!(fs.used.load(Ordering::Acquire), 0);
        allocator::clear();
    }
}

#[tokio::test]
async fn stored_packet_keeps_complete_charge_and_blocks_real_prepare_until_final_clone_drop() {
    let fs = Arc::new(ProbeFs::default());
    let mut session = Session::new(MountOptions::default()).with_workers(1, 1);
    let mut receiver = session.response_receivers[0].take().unwrap();
    session.ensure_workers(fs.clone()).unwrap();
    let baseline = fs.roots.load(Ordering::Acquire);
    let whole = worker::readonly_ordinary_future_layout::<ProbeFs>().0;
    let charged = OpenLane::per_open_bytes(whole).unwrap() as usize;
    session
        .workers
        .as_ref()
        .unwrap()
        .submit(item(&session, &fs, fuse_opcode::FUSE_OPEN, 10));
    entered(&fs, 1).await;
    fs.release_opens();
    let packet = packet(&mut receiver).await;
    let Either::Right((header, body)) = packet else {
        panic!("tracked actual packet");
    };
    assert!(header.is_empty());
    let native_clone = body.clone();
    session.workers.as_mut().unwrap().shutdown().await;
    assert_eq!(fs.roots.load(Ordering::Acquire), baseline + charged);
    let (prepare, _) = session.prepare_pre_unmount(&fs).unwrap();
    let mut prepare = prepare.unwrap();
    assert!(futures_util::poll!(&mut prepare).is_pending());
    drop(body);
    assert_eq!(session.inflight.load(Ordering::Acquire), 1);
    drop(native_clone);
    assert_eq!(
        session.inflight.load(Ordering::Acquire),
        0,
        "automatic callback; no manual reap or new input"
    );
    // The packet was intentionally never written: owner failure is sticky, so
    // real preparation wakes and rejects rather than reporting false success.
    assert!(tokio::time::timeout(Duration::from_secs(1), prepare)
        .await
        .unwrap()
        .is_err());
    assert_eq!(fs.roots.load(Ordering::Acquire), baseline);
}

pub(crate) async fn actual_packet_after_real_box_retirement() -> (
    crate::raw::reply::ReplyBytes,
    Arc<AtomicUsize>,
    usize,
    Session<ProbeFs>,
) {
    let fs = Arc::new(ProbeFs::default());
    let mut session = Session::new(MountOptions::default()).with_workers(1, 1);
    let mut receiver = session.response_receivers[0].take().unwrap();
    session.ensure_workers(fs.clone()).unwrap();
    let baseline = fs.roots.load(Ordering::Acquire);
    session
        .workers
        .as_ref()
        .unwrap()
        .submit(item(&session, &fs, fuse_opcode::FUSE_OPEN, 10));
    entered(&fs, 1).await;
    fs.release_opens();
    let packet = packet(&mut receiver).await;
    session.workers.as_mut().unwrap().shutdown().await;
    let Either::Right((header, body)) = packet else {
        panic!("real tracked wire packet");
    };
    assert!(header.is_empty());
    (body, fs.roots.clone(), baseline, session)
}

#[path = "owned_open_queue_final_packet_required_tests.rs"]
mod required_final_packet;

// Compiler layouts only; no filesystem value, lane allocation or poll occurs.
#[test]
fn actual_complete_open_controller_record_and_queue_layout_receipt() {
    fn metric<T>(name: &str) {
        let layout = std::alloc::Layout::new::<T>();
        eprintln!(
            "BREWFS_ACTUAL_OPEN_TYPE_LAYOUT name={name} bytes={} align={}",
            layout.size(),
            layout.align()
        );
    }
    eprintln!(
        "BREWFS_LAYOUT_TARGET arch={} pointer_bits={}",
        std::env::consts::ARCH,
        usize::BITS
    );
    metric::<Controller>("Controller");
    metric::<ControllerHandle>("ControllerHandle");
    metric::<Record>("Record");
    metric::<Option<Record>>("Option<Record>");
    metric::<OwnedOpenFuture>("OwnedOpenFuture");
    metric::<BoxRetirement>("BoxRetirement");
    metric::<OpenLane>("OpenLane");
    metric::<Option<OpenLane>>("Option<OpenLane>");
    metric::<OpenLanePlan>("OpenLanePlan");
    metric::<InlineRootPermit>("InlineRootPermit");
    metric::<crate::raw::reply::ReplyBytes>("ReplyBytes");
    metric::<worker::WorkItem>("WorkItem");
    metric::<worker::DispatchCtx<ProbeFs>>("DispatchCtx<ProbeFs>");
    metric::<FuseData>("FuseData");
    // Same pinned std ArcInner layout formula as production admission; this
    // models the requested allocation, not an allocator's physical capacity.
    let (arc, offset) = std::alloc::Layout::new::<[AtomicUsize; 2]>()
        .extend(std::alloc::Layout::new::<Controller>())
        .unwrap();
    let arc = arc.pad_to_align();
    eprintln!("BREWFS_ACTUAL_OPEN_CONTROLLER_ARC_REQUEST bytes={} align={} value_offset={} source=pinned_std_arcinner_layout",
        arc.size(), arc.align(), offset);
    // The original per-lane capacity remains 16; configurations are printed
    // as diagnostics and do not introduce a new admission constant.
    for workers in [1usize, 2, 4] {
        let capacity = 16usize;
        let plans = std::alloc::Layout::array::<Option<OpenLane>>(workers).unwrap();
        let records = std::alloc::Layout::array::<Slot>(workers * capacity).unwrap();
        let entries = std::alloc::Layout::array::<OwnedOpenFuture>(capacity).unwrap();
        eprintln!("BREWFS_ACTUAL_OPEN_BASE_REQUEST workers={workers} capacity={capacity} controller_arc_bytes={} plan_vec_bytes={} record_vec_bytes={} each_entry_vec_bytes={} total_storage_bytes={}",
            arc.size(), plans.size(), records.size(), entries.size(),
            OpenLanePlan::storage_bytes(workers, capacity).unwrap());
    }
    eprintln!("BREWFS_ACTUAL_OPEN_PACKET_REQUEST vec_bytes={} shared_header_bytes={} header_source=pinned_bytes_1_12_1",
        OPEN_PACKET_BYTES, SHARED_HEADER_BYTES);
}
