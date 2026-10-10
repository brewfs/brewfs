//! Test transport for the existing MountHandle::unmount API.
//! It substitutes only the physical ordinary unmount syscall; the production
//! worker, handler, read lifetime, and teardown notification still run.

fn direct_control_mount(fs: Arc<OrderFs>) -> IoResult<TestMount> {
    test_mount_with_setup(fs, false, None, |session| {
        session.send_control_reply(
            Request {
                unique: 99,
                uid: 0,
                gid: 0,
                pid: 0,
            },
            libc::ENOMEM.into(),
            None,
        )
    })
}

#[tokio::test(flavor = "current_thread")]
async fn direct_control_reply_drop_is_failure_and_never_reaches_physical_unmount() {
    let fs = Arc::new(OrderFs {
        cancel_enabled: true,
        roots_capacity: 64 * 1024,
        ..OrderFs::default()
    });
    let TestMount {
        handle,
        notify,
        finished,
        inflight,
        mut replies,
        ..
    } = direct_control_mount(fs.clone()).unwrap();
    let mut unmount = Box::pin(handle.unmount());
    assert!(unmount.as_mut().now_or_never().is_none());
    let packet = replies.next().await.unwrap();
    assert_eq!(inflight.load(Ordering::Acquire), 1);
    let last = packet.clone();
    drop(packet);
    assert!(unmount.as_mut().now_or_never().is_none());
    assert_eq!(fs.ordinary_calls.load(Ordering::Acquire), 0);
    drop(last);
    let error = unmount.await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    notify.notify();
    finished.await.unwrap();
    assert_eq!(inflight.load(Ordering::Acquire), 0);
    assert_eq!(fs.ordinary_calls.load(Ordering::Acquire), 0);
    assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn direct_control_reply_real_socket_write_allows_ordinary_unmount() {
    let fs = Arc::new(OrderFs {
        cancel_enabled: true,
        roots_capacity: 64 * 1024,
        ..OrderFs::default()
    });
    let TestMount {
        handle,
        finished,
        inflight,
        mut replies,
        tracker,
        ..
    } = direct_control_mount(fs.clone()).unwrap();
    let mut unmount = Box::pin(handle.unmount());
    assert!(unmount.as_mut().now_or_never().is_none());
    let packet = replies.next().await.unwrap();
    assert_eq!(fs.ordinary_calls.load(Ordering::Acquire), 0);
    write_test_packet::<OrderFs>(packet, tracker).await.unwrap();
    unmount.await.unwrap();
    finished.await.unwrap();
    assert_eq!(inflight.load(Ordering::Acquire), 0);
    assert_eq!(fs.ordinary_calls.load(Ordering::Acquire), 1);
    assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn real_reply_socket_failure_retains_errno_and_prevents_physical_unmount() {
    let fs = Arc::new(OrderFs {
        cancel_enabled: true,
        roots_capacity: 64 * 1024,
        ..OrderFs::default()
    });
    let TestMount {
        handle,
        notify,
        finished,
        mut replies,
        tracker,
        ..
    } = direct_control_mount(fs.clone()).unwrap();
    let packet = replies.next().await.unwrap();
    let (transport, peer) = std::os::unix::net::UnixStream::pair().unwrap();
    drop(peer);
    let connection = FuseConnection::from_test_fd(transport.into()).unwrap();
    let (sender, receiver) = unbounded();
    sender.unbounded_send(packet).unwrap();
    drop(sender);
    let pump_error = Session::<OrderFs>::reply_fuse(Arc::new(connection), receiver, tracker)
        .await
        .unwrap_err();
    assert_eq!(pump_error.raw_os_error(), Some(libc::EPIPE));
    let error = handle.unmount().await.unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EPIPE));
    notify.notify();
    finished.await.unwrap();
    assert_eq!(fs.ordinary_calls.load(Ordering::Acquire), 0);
    assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
}

use super::*;
use crate::raw::reply::{ReplyData, ReplyInit};
use futures_util::FutureExt;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug, Default)]
struct OrderFs {
    cancel_enabled: bool,
    prepared_errno: Option<i32>,
    session_returned_errno: Option<i32>,
    session_panics_after_cleanup: bool,
    prepare_paused: std::sync::atomic::AtomicBool,
    prepare_waiting: AtomicUsize,
    prepare_resume: tokio::sync::Notify,
    reads: Mutex<BTreeMap<u64, oneshot::Sender<()>>>,
    entered: AtomicUsize,
    live: AtomicUsize,
    dropped: AtomicUsize,
    destroy_calls: AtomicUsize,
    ordinary_calls: AtomicUsize,
    ordinary_saw_live: AtomicUsize,
    events: Mutex<Vec<&'static str>>,
    roots_used: Arc<AtomicUsize>,
    roots_peak: AtomicUsize,
    prepare_charge: AtomicUsize,
    prepare_attempted: AtomicUsize,
    prepare_bytes_requested: AtomicUsize,
    prepare_roots_at_request: AtomicUsize,
    prepare_entry_polls: AtomicUsize,
    roots_capacity: usize,
    roots_rejected: AtomicUsize,
}

struct ReadLifetime<'a>(&'a OrderFs);
impl Drop for ReadLifetime<'_> {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::AcqRel);
        self.0.dropped.fetch_add(1, Ordering::AcqRel);
        self.0.events.lock().unwrap().push("read_dropped");
    }
}
#[derive(Debug)]
struct RootOwner {
    used: Arc<AtomicUsize>,
    bytes: usize,
}
impl Drop for RootOwner {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl Filesystem for OrderFs {
    async fn init(&self, _: Request) -> crate::Result<ReplyInit> {
        Ok(ReplyInit::default())
    }
    async fn destroy(&self, _: Request) {
        self.destroy_calls.fetch_add(1, Ordering::AcqRel);
        self.events.lock().unwrap().push("destroy_entered");
        let reads = std::mem::take(&mut *self.reads.lock().unwrap());
        for (_, sender) in reads {
            let _ = sender.send(());
        }
        while self.live.load(Ordering::Acquire) != 0 {
            tokio::task::yield_now().await;
        }
        self.events.lock().unwrap().push("destroy_drained");
    }
    async fn prepare_unmount(&self) -> crate::Result<()> {
        self.prepare_entry_polls.fetch_add(1, Ordering::AcqRel);
        let reads = std::mem::take(&mut *self.reads.lock().unwrap());
        for (_, sender) in reads {
            let _ = sender.send(());
        }
        while self.live.load(Ordering::Acquire) != 0 {
            tokio::task::yield_now().await;
        }
        self.events.lock().unwrap().push("prepare_drained");
        self.prepare_waiting.fetch_add(1, Ordering::Release);
        loop {
            let resumed = self.prepare_resume.notified();
            tokio::pin!(resumed);
            resumed.as_mut().enable();
            if !self.prepare_paused.load(Ordering::Acquire) {
                break;
            }
            resumed.await;
        }
        match self.prepared_errno {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }
    fn supports_read_cancellation(&self) -> bool {
        self.cancel_enabled
    }
    fn reserve_inline_prepare_memory(
        &self,
        bytes: u64,
    ) -> crate::Result<Option<crate::raw::reply::InlineRootPermit>> {
        let charge = usize::try_from(bytes).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        self.prepare_attempted.fetch_add(1, Ordering::AcqRel);
        self.prepare_bytes_requested
            .store(charge, Ordering::Release);
        self.prepare_roots_at_request
            .store(self.roots_used.load(Ordering::Acquire), Ordering::Release);
        let owner = self.reserve_inline_root_memory(bytes)?;
        self.prepare_charge.store(charge, Ordering::Release);
        Ok(owner)
    }
    fn reserve_inline_root_memory(
        &self,
        bytes: u64,
    ) -> crate::Result<Option<crate::raw::reply::InlineRootPermit>> {
        let bytes = usize::try_from(bytes).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        let previous = self
            .roots_used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|sum| *sum <= self.roots_capacity)
            })
            .map_err(|_| {
                self.roots_rejected.fetch_add(1, Ordering::AcqRel);
                crate::Errno::from(libc::ENOMEM)
            })?;
        self.roots_peak
            .fetch_max(previous + bytes, Ordering::AcqRel);
        Ok(Some(
            crate::raw::reply::InlineRootPermit::try_new(RootOwner {
                used: self.roots_used.clone(),
                bytes,
            })
            .unwrap(),
        ))
    }
    fn reserve_input_buffer_memory(
        &self,
        bytes: u64,
    ) -> crate::Result<Option<crate::raw::reply::ReplyMemoryGuard>> {
        let bytes = usize::try_from(bytes).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        let previous = self
            .roots_used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|sum| *sum <= self.roots_capacity)
            });
        let previous = previous.map_err(|_| {
            self.roots_rejected.fetch_add(1, Ordering::AcqRel);
            crate::Errno::from(libc::ENOMEM)
        })?;
        self.roots_peak
            .fetch_max(previous + bytes, Ordering::AcqRel);
        Ok(Some(Arc::new(RootOwner {
            used: self.roots_used.clone(),
            bytes,
        })))
    }
    async fn read(
        &self,
        req: Request,
        _: crate::Inode,
        _: u64,
        _: u64,
        _: u32,
    ) -> crate::Result<ReplyData> {
        let (sender, receiver) = oneshot::channel();
        assert!(self
            .reads
            .lock()
            .unwrap()
            .insert(req.unique, sender)
            .is_none());
        self.live.fetch_add(1, Ordering::AcqRel);
        let _lifetime = ReadLifetime(self);
        self.entered.fetch_add(1, Ordering::Release);
        let _ = receiver.await;
        Err(libc::EINTR.into())
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

fn read_item<FS: Filesystem + Send + Sync + 'static>(session: &Session<FS>) -> WorkItem {
    let mut body = vec![0; std::mem::size_of::<fuse_read_in>()];
    body[..8].copy_from_slice(&1u64.to_le_bytes());
    body[16..20].copy_from_slice(&4096u32.to_le_bytes());
    WorkItem {
        unique: 41,
        opcode: fuse_opcode::FUSE_READ as u32,
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
        _memory_guard: None,
        _response_registration: None,
    }
}

struct TestMount {
    handle: MountHandle,
    notify: Arc<async_notify::Notify>,
    finished: oneshot::Receiver<()>,
    inflight: Arc<AtomicUsize>,
    replies: UnboundedReceiver<FuseData>,
    tracker: Option<Arc<ReplyTracker>>,
}

// Bind preparation to the same factory used by the production mount paths.
fn test_mount(
    fs: Arc<OrderFs>,
    start_read: bool,
    ordinary_error: Option<i32>,
) -> IoResult<TestMount> {
    test_mount_with_setup(fs, start_read, ordinary_error, |_| Ok(()))
}

fn test_mount_with_setup(
    fs: Arc<OrderFs>,
    start_read: bool,
    ordinary_error: Option<i32>,
    setup: impl FnOnce(&mut Session<OrderFs>) -> IoResult<()>,
) -> IoResult<TestMount> {
    let mut session = Session::<OrderFs>::new(MountOptions::default()).with_workers(1, 2);
    let replies = session.response_receivers[0].take().unwrap();
    session.ensure_workers(fs.clone())?;
    setup(&mut session)?;
    if start_read {
        session
            .workers
            .as_ref()
            .unwrap()
            .submit(read_item(&session));
    }
    let inflight = session.inflight.clone();
    let notify = Arc::new(async_notify::Notify::new());
    let task_notify = notify.clone();
    let task_fs = fs.clone();
    let (finished_sender, finished) = oneshot::channel();
    let (pre_unmount, pre_unmount_memory) = session.prepare_pre_unmount(&fs)?;
    let tracker = session.readonly_reply_tracker.clone();
    let task = task::spawn(async move {
        task_notify.notified().await;
        if task_fs.supports_read_cancellation() {
            task_fs
                .destroy(Request {
                    unique: 0,
                    uid: 0,
                    gid: 0,
                    pid: 0,
                })
                .await;
        }
        if let Some(mut workers) = session.workers.take() {
            workers.shutdown().await;
        }
        task_fs.events.lock().unwrap().push("session_joined");
        let _ = finished_sender.send(());
        if task_fs.session_panics_after_cleanup {
            panic!("test actual Session panic after owned fixture cleanup");
        }
        match task_fs.session_returned_errno {
            Some(errno) => Err(IoError::from_raw_os_error(errno)),
            None => Ok(()),
        }
    });
    let operation = OrdinaryUnmountForTest {
        future: Box::pin(async move {
            fs.ordinary_calls.fetch_add(1, Ordering::AcqRel);
            fs.events.lock().unwrap().push("ordinary_unmount");
            let live = fs.live.load(Ordering::Acquire);
            fs.ordinary_saw_live.store(live, Ordering::Release);
            if live != 0 {
                return Err(IoError::from_raw_os_error(libc::EBUSY));
            }
            if let Some(error) = ordinary_error {
                return Err(IoError::from_raw_os_error(error));
            }
            Ok(())
        }),
    };
    Ok(TestMount {
        handle: MountHandle {
            inner: Some(MountHandleInner {
                task,
                mount_path: PathBuf::from("/test-transport-no-real-mount"),
                destroy_notify: notify.clone(),
                pre_unmount,
                pre_unmount_memory,
                #[cfg(feature = "unprivileged")]
                unprivileged: false,
                ordinary_unmount_for_test: Some(operation),
            }),
        },
        notify,
        finished,
        inflight,
        replies,
        tracker,
    })
}

async fn wait_read(fs: &OrderFs) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while fs.entered.load(Ordering::Acquire) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real worker did not enter the pending read");
}

// Positive preparation tests use actual socket write completion. This exercises
// the production pump, but does not substitute for Linux FUSE acceptance.
pub(super) async fn write_test_packet<FS: Filesystem + Send + Sync + 'static>(
    packet: FuseData,
    tracker: Option<Arc<ReplyTracker>>,
) -> IoResult<()> {
    let expected = match &packet {
        Either::Left(data) => reply_tracker::wire_header(data, None)?,
        Either::Right((header, body)) => reply_tracker::wire_header(header, Some(body))?,
    };
    let (transport, peer) = std::os::unix::net::UnixDatagram::pair()?;
    let connection = FuseConnection::from_test_fd(transport.into())?;
    let (sender, receiver) = unbounded();
    sender.unbounded_send(packet).unwrap();
    drop(sender);
    Session::<FS>::reply_fuse(Arc::new(connection), receiver, tracker).await?;
    peer.set_read_timeout(Some(Duration::from_secs(1)))?;
    let mut wire = [0u8; FUSE_OUT_HEADER_SIZE];
    assert_eq!(peer.recv(&mut wire)?, wire.len());
    assert_eq!(reply_tracker::wire_header(&wire, None)?, expected);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn existing_unmount_attempts_ordinary_unmount_while_read_is_live() {
    let fs = Arc::new(OrderFs {
        cancel_enabled: true,
        roots_capacity: 64 * 1024,
        ..OrderFs::default()
    });
    let TestMount {
        handle,
        finished,
        mut replies,
        tracker,
        ..
    } = test_mount(fs.clone(), true, None).unwrap();
    wait_read(&fs).await;
    // This fixture uses one real worker with two admitted OPEN slots. Its
    // independent controller/lane/record storage is live before preparation.
    let worker_base =
        usize::try_from(owned_open_queue::OpenLanePlan::storage_bytes(1, 2).unwrap()).unwrap();
    assert_eq!(
        fs.prepare_roots_at_request.load(Ordering::Acquire),
        PRE_UNMOUNT_MEMORY_BYTES as usize + worker_base,
        "original tracker Roots plus the exactly admitted worker storage"
    );
    eprintln!(
        "BREWFS_UNMOUNT_EXACT_ROOTS worker_base={worker_base} original_fixed={} prepare_charge={} peak={}",
        PRE_UNMOUNT_MEMORY_BYTES,
        fs.prepare_charge.load(Ordering::Acquire),
        fs.roots_peak.load(Ordering::Acquire)
    );
    assert_eq!(
        fs.roots_peak.load(Ordering::Acquire),
        PRE_UNMOUNT_MEMORY_BYTES as usize + worker_base + fs.prepare_charge.load(Ordering::Acquire)
    );
    let unmount = handle.unmount();
    let writer = async move {
        let packet = replies.next().await.unwrap();
        write_test_packet::<OrderFs>(packet, tracker).await.unwrap();
    };
    let (result, ()) = tokio::join!(unmount, writer);
    result.expect("pre-unmount preparation must drain the read before ordinary unmount");
    assert_eq!(fs.ordinary_saw_live.load(Ordering::Acquire), 0);
    tokio::time::timeout(Duration::from_secs(1), finished)
        .await
        .expect("test session cleanup stalled")
        .expect("test session cleanup sender dropped");
    assert_eq!(fs.destroy_calls.load(Ordering::Acquire), 1);
    assert_eq!(fs.live.load(Ordering::Acquire), 0);
    assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_unmount_error_is_preserved_after_read_preparation() {
    let fs = Arc::new(OrderFs {
        cancel_enabled: true,
        roots_capacity: 64 * 1024,
        ..OrderFs::default()
    });
    let TestMount {
        handle,
        notify,
        mut finished,
        ..
    } = test_mount(fs.clone(), false, Some(libc::EIO)).unwrap();
    let error = handle
        .unmount()
        .await
        .expect_err("physical unmount errors must remain visible");
    let destroy_at_return = fs.destroy_calls.load(Ordering::Acquire);
    let finished_at_return = matches!(finished.try_recv(), Ok(Some(())));
    if !finished_at_return {
        notify.notify();
        tokio::time::timeout(Duration::from_secs(1), finished)
            .await
            .expect("test session cleanup stalled")
            .expect("test session cleanup sender dropped");
    }
    assert_eq!(error.raw_os_error(), Some(libc::EIO));
    assert_eq!(destroy_at_return, 1);
    assert!(
        finished_at_return,
        "ordinary unmount error returned before session cleanup"
    );
    assert_eq!(fs.live.load(Ordering::Acquire), 0);
    assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn mutable_path_does_not_admit_pre_unmount_roots_charge() {
    let fs = Arc::new(OrderFs {
        cancel_enabled: false,
        roots_capacity: 64 * 1024,
        ..OrderFs::default()
    });
    let TestMount {
        handle, finished, ..
    } = test_mount(fs.clone(), false, None).unwrap();
    handle
        .unmount()
        .await
        .expect("ordinary mutable unmount should succeed");
    tokio::time::timeout(Duration::from_secs(1), finished)
        .await
        .expect("session cleanup stalled")
        .expect("test session cleanup sender dropped");
    assert_eq!(fs.roots_peak.load(Ordering::Acquire), 0);
    assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn existing_unmount_without_reads_keeps_ordinary_path() {
    let fs = Arc::new(OrderFs {
        cancel_enabled: true,
        roots_capacity: 64 * 1024,
        ..OrderFs::default()
    });
    let TestMount {
        handle, finished, ..
    } = test_mount(fs.clone(), false, None).unwrap();
    handle
        .unmount()
        .await
        .expect("ordinary unmount should succeed");
    tokio::time::timeout(Duration::from_secs(1), finished)
        .await
        .expect("session cleanup stalled")
        .expect("session cleanup sender dropped");
    assert_eq!(fs.ordinary_calls.load(Ordering::Acquire), 1);
    assert_eq!(fs.live.load(Ordering::Acquire), 0);
    assert_eq!(
        fs.events.lock().unwrap().as_slice(),
        [
            "prepare_drained",
            "ordinary_unmount",
            "destroy_entered",
            "destroy_drained",
            "session_joined"
        ]
    );
}

// External proposal: append to the existing unmount_order_tests.rs module.
// No source patch applied and no Cargo invocation performed by this auditor.
// These tests intentionally have no reply pump. Packet drop is not success.

#[tokio::test(flavor = "current_thread")]
async fn readonly_closed_response_sender_is_not_a_successful_drain() {
    let fs = Arc::new(OrderFs {
        cancel_enabled: true,
        roots_capacity: 64 * 1024,
        ..OrderFs::default()
    });
    let TestMount {
        handle,
        notify,
        finished,
        replies,
        ..
    } = test_mount(fs.clone(), true, None).unwrap();
    wait_read(&fs).await;
    drop(replies); // Real handler's send fails after preparation cancels READ.
    let result = tokio::time::timeout(Duration::from_secs(1), handle.unmount()).await;

    // Fake-session cleanup happens before RED assertions. This is not a
    // production-success unmount and is not used as a success oracle.
    notify.notify();
    tokio::time::timeout(Duration::from_secs(1), finished)
        .await
        .expect("fake session cleanup stalled")
        .expect("fake session sender dropped");
    assert_eq!(fs.live.load(Ordering::Acquire), 0);
    assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
    let error = result
        .expect("closed sender must wake terminal drain failure")
        .expect_err("closed response sender must not become successful retirement");
    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    assert_eq!(fs.ordinary_calls.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn public_typed_prepare_failure_joins_session_before_return_without_fixture_notification() {
    for prepared_errno in [libc::EPIPE, libc::EBADF] {
        for session_returned_errno in [None, Some(libc::EIO)] {
            let fs = Arc::new(OrderFs {
                cancel_enabled: true,
                prepared_errno: Some(prepared_errno),
                session_returned_errno,
                roots_capacity: 64 * 1024,
                ..OrderFs::default()
            });
            let TestMount {
                handle,
                notify,
                mut finished,
                inflight,
                replies,
                tracker,
            } = test_mount(fs.clone(), false, None).unwrap();
            // External test-owned tracker/channel references must not mask
            // ownership retirement of the real factory and Session task.
            drop(replies);
            drop(tracker);
            let result = tokio::time::timeout(Duration::from_secs(1), handle.unmount()).await;
            let finished_at_return = matches!(finished.try_recv(), Ok(Some(())));
            let destroy_at_return = fs.destroy_calls.load(Ordering::Acquire);
            let ordinary_at_return = fs.ordinary_calls.load(Ordering::Acquire);
            let roots_at_return = fs.roots_used.load(Ordering::Acquire);
            let events_at_return = fs.events.lock().unwrap().clone();

            // Only RED cleanup: record all public-return observations first.
            // GREEN must reach the above observations without test notification.
            if !finished_at_return {
                notify.notify();
                tokio::time::timeout(Duration::from_secs(1), finished)
                    .await
                    .expect("RED-only typed failure fixture cleanup stalled")
                    .expect("RED-only typed failure cleanup sender dropped");
            }

            let error = result
                .expect("failed preparation did not return after Session join")
                .expect_err("typed preparation error was discarded");
            assert_eq!(error.raw_os_error(), Some(prepared_errno));
            assert!(
                finished_at_return,
                "public failure returned before its actual Session task finished"
            );
            assert_eq!(destroy_at_return, 1);
            assert_eq!(ordinary_at_return, 0);
            assert_eq!(roots_at_return, 0);
            assert_eq!(
                events_at_return,
                [
                    "prepare_drained",
                    "destroy_entered",
                    "destroy_drained",
                    "session_joined"
                ]
            );
            assert_eq!(fs.live.load(Ordering::Acquire), 0);
            assert_eq!(fs.entered.load(Ordering::Acquire), 0);
            assert_eq!(fs.dropped.load(Ordering::Acquire), 0);
            assert_eq!(inflight.load(Ordering::Acquire), 0);
            assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn public_typed_prepare_errno_wins_when_actual_session_join_panics() {
    let fs = Arc::new(OrderFs {
        cancel_enabled: true,
        prepared_errno: Some(libc::EBADF),
        session_panics_after_cleanup: true,
        roots_capacity: 64 * 1024,
        ..OrderFs::default()
    });
    let TestMount {
        handle,
        notify,
        mut finished,
        inflight,
        replies,
        tracker,
    } = test_mount(fs.clone(), false, None).unwrap();
    drop(replies);
    drop(tracker);
    // Catch the public future too: a panic from unwrap on the real JoinError
    // must fail the oracle after safe fixture cleanup, not abort this test early.
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        std::panic::AssertUnwindSafe(handle.unmount()).catch_unwind(),
    )
    .await;
    let finished_at_return = matches!(finished.try_recv(), Ok(Some(())));
    let destroy_at_return = fs.destroy_calls.load(Ordering::Acquire);
    let ordinary_at_return = fs.ordinary_calls.load(Ordering::Acquire);
    let roots_at_return = fs.roots_used.load(Ordering::Acquire);
    if !finished_at_return {
        notify.notify();
        tokio::time::timeout(Duration::from_secs(1), finished)
            .await
            .expect("RED-only panicked Session fixture cleanup stalled")
            .expect("RED-only panicked Session cleanup sender dropped");
    }
    let error = result
        .expect("failed preparation stalled on panicked Session")
        .expect("public failure panicked while joining its actual Session")
        .expect_err("typed preparation error was discarded after Session panic");
    assert_eq!(error.raw_os_error(), Some(libc::EBADF));
    assert!(finished_at_return);
    assert_eq!(destroy_at_return, 1);
    assert_eq!(ordinary_at_return, 0);
    assert_eq!(roots_at_return, 0);
    assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
    assert_eq!(inflight.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn pending_filesystem_prepare_observes_later_socket_error_with_actual_wakeup() {
    use futures_util::task::{waker, ArcWake};
    use std::task::{Context, Poll};
    struct WakeCount(AtomicUsize);
    impl ArcWake for WakeCount {
        fn wake_by_ref(owner: &Arc<Self>) {
            owner.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let fs = Arc::new(OrderFs {
        cancel_enabled: true,
        prepare_paused: std::sync::atomic::AtomicBool::new(true),
        roots_capacity: 64 * 1024,
        ..OrderFs::default()
    });
    let TestMount {
        handle,
        notify,
        finished,
        inflight,
        mut replies,
        tracker,
    } = direct_control_mount(fs.clone()).unwrap();
    let mut unmount = Box::pin(handle.unmount());
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let task_waker = waker(wakes.clone());
    let mut context = Context::from_waker(&task_waker);
    // Drive the original production stored preparation, through its public
    // API, into a real pending FS prepare and registered task waker.
    let first_poll = unmount.as_mut().poll(&mut context);
    let entered_before_failure = fs.prepare_waiting.load(Ordering::Acquire);
    let wakes_before_failure = wakes.0.load(Ordering::SeqCst);
    let packet = replies.next().await.unwrap();
    let (transport, peer) = std::os::unix::net::UnixStream::pair().unwrap();
    drop(peer);
    let connection = FuseConnection::from_test_fd(transport.into()).unwrap();
    let (sender, receiver) = unbounded();
    sender.unbounded_send(packet).unwrap();
    drop(sender);
    // An actual production reply pump's socket write fails after FS is Pending.
    let pump_result =
        Session::<OrderFs>::reply_fuse(Arc::new(connection), receiver, tracker.clone()).await;
    let wakes_after_failure = wakes.0.load(Ordering::SeqCst);
    let result = tokio::time::timeout(Duration::from_secs(1), unmount.as_mut()).await;
    let still_paused_at_return = fs.prepare_paused.load(Ordering::Acquire);
    let ordinary_at_return = fs.ordinary_calls.load(Ordering::Acquire);

    // Drop/cancel the original public future, then clean this fake fixture
    // BEFORE assertions. This is deliberately not a production failure join.
    drop(unmount);
    fs.prepare_paused.store(false, Ordering::Release);
    fs.prepare_resume.notify_waiters();
    notify.notify();
    let cleanup = tokio::time::timeout(Duration::from_secs(1), finished).await;
    drop(replies);
    drop(tracker);
    cleanup
        .expect("fake failure-race fixture cleanup stalled")
        .expect("fake cleanup sender dropped");
    assert!(matches!(first_poll, Poll::Pending));
    assert_eq!(entered_before_failure, 1);
    assert_eq!(wakes_before_failure, 0);
    assert!(
        wakes_after_failure > 0,
        "reply failure did not wake actual pending preparation"
    );
    assert_eq!(pump_result.unwrap_err().raw_os_error(), Some(libc::EPIPE));
    assert_eq!(
        result
            .expect("FS prepare concealed the terminal reply error")
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EPIPE)
    );
    assert!(still_paused_at_return);
    assert_eq!(ordinary_at_return, 0);
    assert_eq!(fs.ordinary_calls.load(Ordering::Acquire), 0);
    assert_eq!(inflight.load(Ordering::Acquire), 0);
    assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
}

#[test]
fn actual_prepare_factory_rejects_dynamic_child_before_filesystem_poll_at_fixed_roots_capacity() {
    let fs = Arc::new(OrderFs {
        cancel_enabled: true,
        roots_capacity: PRE_UNMOUNT_MEMORY_BYTES as usize,
        ..OrderFs::default()
    });
    let mut session = Session::<OrderFs>::new(MountOptions::default());
    // The actual factory must admit its original tracker reservation first,
    // then reject the separately charged concrete FS child before first poll.
    let errno = match session.prepare_pre_unmount(&fs) {
        Err(error) => error.raw_os_error(),
        Ok((future, memory)) => {
            drop(future);
            drop(memory);
            None
        }
    };
    let tracker_present_at_return = session.readonly_reply_tracker.is_some();
    let roots_at_return = fs.roots_used.load(Ordering::Acquire);
    let peak_at_return = fs.roots_peak.load(Ordering::Acquire);
    let rejection_count = fs.roots_rejected.load(Ordering::Acquire);
    let child_admission_attempts = fs.prepare_attempted.load(Ordering::Acquire);
    let child_requested = fs.prepare_bytes_requested.load(Ordering::Acquire);
    let roots_at_child_admission = fs.prepare_roots_at_request.load(Ordering::Acquire);
    let accepted_child_charge = fs.prepare_charge.load(Ordering::Acquire);
    let fs_prepare_polls = fs.prepare_entry_polls.load(Ordering::Acquire);
    let reached_pause = fs.prepare_waiting.load(Ordering::Acquire);
    let empty_events = fs.events.lock().unwrap().is_empty();
    let empty_reads = fs.reads.lock().unwrap().is_empty();

    // There are no spawned workers or mount task in this factory-only path.
    // Release the actual Session/tracker owner before every result assertion.
    drop(session);
    assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
    assert_eq!(errno, Some(libc::ENOMEM));
    assert!(tracker_present_at_return);
    assert_eq!(roots_at_return, PRE_UNMOUNT_MEMORY_BYTES as usize);
    assert_eq!(peak_at_return, PRE_UNMOUNT_MEMORY_BYTES as usize);
    assert_eq!(rejection_count, 1);
    assert_eq!(child_admission_attempts, 1);
    assert!(child_requested > 0);
    assert_eq!(roots_at_child_admission, PRE_UNMOUNT_MEMORY_BYTES as usize);
    assert_eq!(accepted_child_charge, 0);
    assert_eq!(fs_prepare_polls, 0);
    assert_eq!(reached_pause, 0);
    assert!(empty_events);
    assert!(empty_reads);
    assert_eq!(fs.entered.load(Ordering::Acquire), 0);
    assert_eq!(fs.live.load(Ordering::Acquire), 0);
    assert_eq!(fs.dropped.load(Ordering::Acquire), 0);
    assert_eq!(fs.destroy_calls.load(Ordering::Acquire), 0);
    assert_eq!(fs.ordinary_calls.load(Ordering::Acquire), 0);
}

// The FS fixture supplies the errno only after its original read drain. The
// production prepare_pre_unmount factory and public MountHandle stay intact;
// only the existing test seam substitutes physical ordinary unmount.
#[tokio::test(flavor = "current_thread")]
async fn typed_prepare_failure_preserves_errno_and_blocks_ordinary_unmount() {
    for prepared_errno in [libc::EPIPE, libc::EBADF] {
        let fs = Arc::new(OrderFs {
            cancel_enabled: true,
            prepared_errno: Some(prepared_errno),
            roots_capacity: 64 * 1024,
            ..OrderFs::default()
        });
        // No READ is submitted: this test isolates typed FS preparation from
        // queued replies and keeps the actual ReplyTracker initially empty.
        let TestMount {
            handle,
            notify,
            finished,
            inflight,
            replies,
            tracker,
        } = test_mount(fs.clone(), false, None).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle.unmount()).await;
        let before_cleanup_events = fs.events.lock().unwrap().clone();
        let before_cleanup_destroy = fs.destroy_calls.load(Ordering::Acquire);
        let before_cleanup_ordinary = fs.ordinary_calls.load(Ordering::Acquire);
        let before_cleanup_inflight = inflight.load(Ordering::Acquire);
        let before_cleanup_tracker_error = tracker.as_ref().and_then(|tracker| tracker.error());

        // Stage A has notified and awaited the actual fixture Session task
        // before failed public return. This marker is already ready; awaiting
        // it creates no teardown notification and proves no native join.
        drop(notify);
        let cleanup = tokio::time::timeout(Duration::from_secs(1), finished).await;
        drop(replies);
        drop(tracker);
        cleanup
            .expect("fake fixture cleanup stalled after typed preparation failure")
            .expect("fake fixture cleanup sender dropped");

        let error = result
            .expect("typed preparation failure did not reach a terminal return")
            .expect_err("typed FS preparation failure was discarded by public unmount");
        assert_eq!(error.raw_os_error(), Some(prepared_errno));
        assert_eq!(
            before_cleanup_events,
            [
                "prepare_drained",
                "destroy_entered",
                "destroy_drained",
                "session_joined"
            ]
        );
        assert_eq!(before_cleanup_ordinary, 0);
        assert_eq!(fs.ordinary_calls.load(Ordering::Acquire), 0);
        assert_eq!(before_cleanup_destroy, 1);
        assert_eq!(before_cleanup_inflight, 0);
        assert!(before_cleanup_tracker_error.is_none());
        assert_eq!(fs.live.load(Ordering::Acquire), 0);
        assert_eq!(fs.entered.load(Ordering::Acquire), 0);
        assert_eq!(fs.dropped.load(Ordering::Acquire), 0);
        // This control owns an external tracker clone until the drops above.
        // The new stage-A tests drop that clone before API and assert Roots at
        // return. Native failed-unmount cleanup remains a separate gate.
        assert_eq!(fs.destroy_calls.load(Ordering::Acquire), 1);
        assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn readonly_queued_reply_dropped_without_writer_is_not_acknowledged() {
    let fs = Arc::new(OrderFs {
        cancel_enabled: true,
        roots_capacity: 64 * 1024,
        ..OrderFs::default()
    });
    let TestMount {
        handle,
        notify,
        finished,
        mut replies,
        ..
    } = test_mount(fs.clone(), true, None).unwrap();
    wait_read(&fs).await;
    fs.prepare_unmount()
        .await
        .expect("fixture preparation failed");
    let reply = tokio::time::timeout(Duration::from_secs(1), replies.next())
        .await
        .expect("real worker did not queue cancelled READ reply")
        .expect("real reply sender closed unexpectedly");
    assert_eq!(fs.live.load(Ordering::Acquire), 0);
    drop(replies);
    drop(reply); // Successfully sent to queue; never written to any transport.
    let result = tokio::time::timeout(Duration::from_secs(1), handle.unmount()).await;

    notify.notify();
    tokio::time::timeout(Duration::from_secs(1), finished)
        .await
        .expect("fake session cleanup stalled")
        .expect("fake session sender dropped");
    assert_eq!(fs.roots_used.load(Ordering::Acquire), 0);
    let error = result
        .expect("discarded queued reply must wake terminal drain failure")
        .expect_err("discarding a queued reply must not acknowledge it");
    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    assert_eq!(fs.ordinary_calls.load(Ordering::Acquire), 0);
}
