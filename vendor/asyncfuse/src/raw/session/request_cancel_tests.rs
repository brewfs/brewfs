//! The real worker and control handlers remain reachable with pending reads.
use super::*;
use crate::raw::logfs::LoggingFileSystem;
use crate::raw::reply::{ReplyData, ReplyInit};
use futures_util::FutureExt;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug, Default)]
struct ReadonlyFs {
    reads: Arc<Mutex<BTreeMap<u64, oneshot::Sender<()>>>>,
    entered: Arc<AtomicUsize>,
    live: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
    control_used: Arc<AtomicUsize>,
    control_peak: Arc<AtomicUsize>,
    control_capacity: usize,
    cancel_enabled: bool,
}

struct ReadLifetime {
    unique: u64,
    reads: Arc<Mutex<BTreeMap<u64, oneshot::Sender<()>>>>,
    live: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}
impl Drop for ReadLifetime {
    fn drop(&mut self) {
        self.reads.lock().unwrap().remove(&self.unique);
        self.live.fetch_sub(1, Ordering::AcqRel);
        self.dropped.fetch_add(1, Ordering::AcqRel);
    }
}
#[derive(Debug)]
struct MemoryOwner {
    used: Arc<AtomicUsize>,
    bytes: usize,
}
impl Drop for MemoryOwner {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl Filesystem for ReadonlyFs {
    async fn init(&self, _: Request) -> crate::Result<ReplyInit> {
        Ok(ReplyInit::default())
    }
    async fn destroy(&self, _: Request) {}
    fn supports_read_cancellation(&self) -> bool {
        self.cancel_enabled
    }
    fn reserve_control_memory(
        &self,
        bytes: u64,
    ) -> crate::Result<Option<crate::raw::reply::ReplyMemoryGuard>> {
        if self.control_capacity == 0 {
            return Ok(None);
        }
        let bytes = usize::try_from(bytes).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        let previous = self
            .control_used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|total| *total <= self.control_capacity)
            })
            .map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        self.control_peak
            .fetch_max(previous + bytes, Ordering::AcqRel);
        Ok(Some(Arc::new(MemoryOwner {
            used: self.control_used.clone(),
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
        let _lifetime = ReadLifetime {
            unique: req.unique,
            reads: self.reads.clone(),
            live: self.live.clone(),
            dropped: self.dropped.clone(),
        };
        self.entered.fetch_add(1, Ordering::Release);
        receiver.await.unwrap();
        Err(libc::EINTR.into())
    }
    async fn interrupt(&self, _: Request, unique: u64) -> crate::Result<()> {
        let sender = self
            .reads
            .lock()
            .unwrap()
            .remove(&unique)
            .ok_or(crate::Errno::from(libc::EAGAIN))?;
        sender.send(()).unwrap();
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
fn read_item<FS: Filesystem + Send + Sync + 'static>(
    session: &Session<FS>,
    unique: u64,
) -> WorkItem {
    let mut body = vec![0; std::mem::size_of::<fuse_read_in>()];
    body[..8].copy_from_slice(&1u64.to_le_bytes());
    body[16..20].copy_from_slice(&4096u32.to_le_bytes());
    WorkItem {
        unique,
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
async fn response(receiver: &mut UnboundedReceiver<FuseData>) -> fuse_out_header {
    let packet = tokio::time::timeout(Duration::from_secs(1), receiver.next())
        .await
        .expect("control/read reply was blocked")
        .unwrap();
    response_header(&packet)
}
fn response_header(packet: &FuseData) -> fuse_out_header {
    let header = match packet {
        Either::Left(data) => data.as_slice(),
        Either::Right((header, body)) if header.is_empty() => body.as_ref(),
        Either::Right((header, _)) => header.as_slice(),
    };
    assert!(header.len() >= FUSE_OUT_HEADER_SIZE);
    fuse_out_header {
        len: u32::from_le_bytes(header[..4].try_into().unwrap()),
        error: i32::from_le_bytes(header[4..8].try_into().unwrap()),
        unique: u64::from_le_bytes(header[8..16].try_into().unwrap()),
    }
}

async fn check_readonly_full_admission_and_one_worker<FS: Filesystem + Send + Sync + 'static>(
    fs: Arc<FS>,
    entered: &AtomicUsize,
    reads: &Mutex<BTreeMap<u64, oneshot::Sender<()>>>,
) {
    let mut session = Session::<FS>::new(MountOptions::default()).with_workers(1, 2);
    let mut receiver = session.response_receivers[0].take().unwrap();
    session.ensure_workers(fs.clone()).unwrap();
    let first = read_item(&session, 1);
    let second = read_item(&session, 2);
    session.workers.as_ref().unwrap().submit(first);
    session.workers.as_ref().unwrap().submit(second);
    tokio::time::timeout(Duration::from_secs(1), async {
        while entered.load(Ordering::Acquire) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second readonly request waited behind a pending first read");
    assert_eq!(session.inflight.load(Ordering::Acquire), 2);
    let (connection, _) = FuseConnection::test_control_channel();
    tokio::time::timeout(
        Duration::from_secs(1),
        session.wait_before_request_read(fs.as_ref(), &connection),
    )
    .await
    .expect("full admission hid kernel control messages")
    .unwrap();
    session
        .handle_interrupt(
            Request {
                unique: 102,
                uid: 0,
                gid: 0,
                pid: 0,
            },
            &2u64.to_le_bytes(),
            &fs,
        )
        .await
        .unwrap();
    let mut replies = vec![response(&mut receiver).await, response(&mut receiver).await];
    replies.sort_by_key(|header| header.unique);
    assert_eq!((replies[0].unique, replies[0].error), (2, -libc::EINTR));
    assert_eq!((replies[1].unique, replies[1].error), (102, 0));
    assert!(
        receiver.next().now_or_never().is_none(),
        "duplicate original read response"
    );
    assert!(
        reads.lock().unwrap().contains_key(&1),
        "interrupt targeted the wrong unique"
    );
    session
        .handle_interrupt(
            Request {
                unique: 103,
                uid: 0,
                gid: 0,
                pid: 0,
            },
            &1u64.to_le_bytes(),
            &fs,
        )
        .await
        .unwrap();
    let mut replies = vec![response(&mut receiver).await, response(&mut receiver).await];
    replies.sort_by_key(|header| header.unique);
    assert_eq!((replies[0].unique, replies[0].error), (1, -libc::EINTR));
    assert_eq!((replies[1].unique, replies[1].error), (103, 0));
    tokio::task::yield_now().await;
    assert_eq!(session.inflight.load(Ordering::Acquire), 0);
    assert!(reads.lock().unwrap().is_empty());
    assert!(receiver.next().now_or_never().is_none());
}

#[tokio::test]
async fn readonly_full_admission_and_one_worker_allow_second_read_interrupt_without_duplicate_reply(
) {
    let fs = Arc::new(ReadonlyFs {
        cancel_enabled: true,
        ..ReadonlyFs::default()
    });
    check_readonly_full_admission_and_one_worker(fs.clone(), &fs.entered, &fs.reads).await;
}

#[tokio::test]
async fn logged_readonly_full_admission_and_one_worker_keep_targeted_interrupt_reachable() {
    let readonly = ReadonlyFs {
        cancel_enabled: true,
        ..ReadonlyFs::default()
    };
    let entered = readonly.entered.clone();
    let reads = readonly.reads.clone();
    // Use the production mount wrapper, not a getter-only capability check.
    let fs = Arc::new(LoggingFileSystem::new(readonly));
    check_readonly_full_admission_and_one_worker(fs, &entered, &reads).await;
}

#[tokio::test]
async fn logged_readonly_interrupt_completes_original_read_and_releases_admission() {
    let readonly = ReadonlyFs {
        cancel_enabled: true,
        ..ReadonlyFs::default()
    };
    let entered = readonly.entered.clone();
    let reads = readonly.reads.clone();
    let fs = Arc::new(LoggingFileSystem::new(readonly));
    let mut session = Session::new(MountOptions::default()).with_workers(1, 1);
    let mut receiver = session.response_receivers[0].take().unwrap();
    session.ensure_workers(fs.clone()).unwrap();
    let read = read_item(&session, 3);
    session.workers.as_ref().unwrap().submit(read);
    tokio::time::timeout(Duration::from_secs(1), async {
        while entered.load(Ordering::Acquire) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("wrapped readonly read did not enter the actual worker");
    assert_eq!(session.inflight.load(Ordering::Acquire), 1);
    // Invoke the actual control handler while its original read is pending.
    session
        .handle_interrupt(
            Request {
                unique: 113,
                uid: 0,
                gid: 0,
                pid: 0,
            },
            &3u64.to_le_bytes(),
            &fs,
        )
        .await
        .unwrap();
    let mut replies = vec![response(&mut receiver).await, response(&mut receiver).await];
    replies.sort_by_key(|header| header.unique);
    assert_eq!((replies[0].unique, replies[0].error), (3, -libc::EINTR));
    assert_eq!((replies[1].unique, replies[1].error), (113, 0));
    tokio::task::yield_now().await;
    assert_eq!(session.inflight.load(Ordering::Acquire), 0);
    assert!(reads.lock().unwrap().is_empty());
    assert!(receiver.next().now_or_never().is_none());
}

#[tokio::test]
async fn unknown_interrupt_produces_only_its_eagain_reply_and_mutable_capacity_still_waits() {
    let fs = Arc::new(ReadonlyFs {
        cancel_enabled: true,
        ..ReadonlyFs::default()
    });
    let mut session = Session::<ReadonlyFs>::new(MountOptions::default());
    let mut receiver = session.response_receivers[0].take().unwrap();
    session
        .handle_interrupt(
            Request {
                unique: 109,
                uid: 0,
                gid: 0,
                pid: 0,
            },
            &999u64.to_le_bytes(),
            &fs,
        )
        .await
        .unwrap();
    let header = response(&mut receiver).await;
    assert_eq!((header.unique, header.error), (109, -libc::EAGAIN));
    assert!(receiver.next().now_or_never().is_none());
    let mutable = ReadonlyFs::default();
    session.max_background = 1;
    session.inflight.store(1, Ordering::Release);
    let (connection, _) = FuseConnection::test_control_channel();
    assert!(session
        .wait_before_request_read(&mutable, &connection)
        .now_or_never()
        .is_none());
    session.inflight.store(0, Ordering::Release);
}

fn owned_read_item<FS: Filesystem + Send + Sync + 'static>(
    session: &Session<FS>,
    unique: u64,
    owners: &Arc<AtomicUsize>,
) -> WorkItem {
    let mut item = read_item(session, unique);
    owners.fetch_add(1, Ordering::AcqRel);
    item._memory_guard = Some(Arc::new(MemoryOwner {
        used: owners.clone(),
        bytes: 1,
    }));
    item
}

async fn wait_entered(fs: &ReadonlyFs, count: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while fs.entered.load(Ordering::Acquire) != count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual worker did not start pending readonly requests");
}

#[tokio::test]
async fn readonly_session_drop_releases_pending_reads_uniques_and_request_owners() {
    let fs = Arc::new(ReadonlyFs {
        cancel_enabled: true,
        ..ReadonlyFs::default()
    });
    let owners = Arc::new(AtomicUsize::new(0));
    let mut session = Session::new(MountOptions::default()).with_workers(1, 2);
    let mut receiver = session.response_receivers[0].take().unwrap();
    let inflight = session.inflight.clone();
    session.ensure_workers(fs.clone()).unwrap();
    for unique in [201, 202] {
        let item = owned_read_item(&session, unique, &owners);
        session.workers.as_ref().unwrap().submit(item);
    }
    wait_entered(&fs, 2).await;
    assert_eq!(owners.load(Ordering::Acquire), 2);
    drop(session);
    tokio::time::timeout(Duration::from_secs(1), async {
        while fs.live.load(Ordering::Acquire) != 0
            || inflight.load(Ordering::Acquire) != 0
            || owners.load(Ordering::Acquire) != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping session detached pending readonly futures or their owners");
    assert_eq!(fs.dropped.load(Ordering::Acquire), 2);
    assert!(fs.reads.lock().unwrap().is_empty());
    assert!(!matches!(receiver.next().now_or_never(), Some(Some(_))));
}

#[tokio::test]
async fn readonly_drop_retires_queued_work_before_its_first_poll() {
    let fs = Arc::new(ReadonlyFs {
        cancel_enabled: true,
        ..ReadonlyFs::default()
    });
    let owners = Arc::new(AtomicUsize::new(0));
    let mut session = Session::new(MountOptions::default()).with_workers(1, 2);
    let inflight = session.inflight.clone();
    session.ensure_workers(fs.clone()).unwrap();
    for unique in [205, 206] {
        let item = owned_read_item(&session, unique, &owners);
        session.workers.as_ref().unwrap().submit(item);
    }
    // Current-thread runtime: no yield has allowed the owning worker to poll.
    drop(session);
    tokio::time::timeout(Duration::from_secs(1), async {
        while owners.load(Ordering::Acquire) != 0 || inflight.load(Ordering::Acquire) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued readonly work survived drop before first poll");
    assert_eq!(fs.entered.load(Ordering::Acquire), 0);
    assert_eq!(fs.live.load(Ordering::Acquire), 0);
    assert!(fs.reads.lock().unwrap().is_empty());
}

#[tokio::test]
async fn readonly_worker_shutdown_joins_all_pending_work_and_releases_owners() {
    let fs = Arc::new(ReadonlyFs {
        cancel_enabled: true,
        ..ReadonlyFs::default()
    });
    let owners = Arc::new(AtomicUsize::new(0));
    let mut session = Session::new(MountOptions::default()).with_workers(1, 2);
    session.ensure_workers(fs.clone()).unwrap();
    for unique in [211, 212] {
        let item = owned_read_item(&session, unique, &owners);
        session.workers.as_ref().unwrap().submit(item);
    }
    wait_entered(&fs, 2).await;
    tokio::time::timeout(
        Duration::from_secs(1),
        session.workers.as_mut().unwrap().shutdown(),
    )
    .await
    .expect("shutdown could not retire userspace readonly work");
    assert_eq!(fs.live.load(Ordering::Acquire), 0);
    assert_eq!(fs.dropped.load(Ordering::Acquire), 2);
    assert_eq!(session.inflight.load(Ordering::Acquire), 0);
    assert_eq!(owners.load(Ordering::Acquire), 0);
    assert!(fs.reads.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mutable_session_drop_preserves_pending_read_until_normal_interrupt() {
    let fs = Arc::new(ReadonlyFs::default());
    let mut session = Session::new(MountOptions::default()).with_workers(1, 1);
    let mut receiver = session.response_receivers[0].take().unwrap();
    session.ensure_workers(fs.clone()).unwrap();
    let item = read_item(&session, 221);
    session.workers.as_ref().unwrap().submit(item);
    wait_entered(&fs, 1).await;
    drop(session);
    tokio::task::yield_now().await;
    assert_eq!(fs.live.load(Ordering::Acquire), 1);
    fs.interrupt(
        Request {
            unique: 222,
            uid: 0,
            gid: 0,
            pid: 0,
        },
        221,
    )
    .await
    .unwrap();
    assert_eq!(response(&mut receiver).await.unique, 221);
    assert_eq!(fs.live.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn full_admission_rejections_keep_bounded_owners_until_slow_consumer_retires_them() {
    let fs = ReadonlyFs {
        cancel_enabled: true,
        control_capacity: 2048,
        ..ReadonlyFs::default()
    };
    let mut session = Session::new(MountOptions::default()).with_workers(1, 1);
    let mut receiver = session.response_receivers[0].take().unwrap();
    session.inflight.store(1, Ordering::Release);
    for unique in [231, 232] {
        session
            .reject_ordinary_request(
                Request {
                    unique,
                    uid: 0,
                    gid: 0,
                    pid: 0,
                },
                libc::ENOMEM.into(),
                &fs,
            )
            .await
            .unwrap();
    }
    // Intentionally keep the response consumer pending: neither header has
    // been dequeued, and ordinary admission remains full throughout.
    assert_eq!(fs.control_used.load(Ordering::Acquire), 2048);
    for unique in 233..1233 {
        let error = session
            .reject_ordinary_request(
                Request {
                    unique,
                    uid: 0,
                    gid: 0,
                    pid: 0,
                },
                libc::ENOMEM.into(),
                &fs,
            )
            .await
            .expect_err("exhausted reply budget accepted an unowned queued header");
        assert_eq!(error.raw_os_error(), Some(libc::ENOMEM));
    }
    assert_eq!(fs.control_peak.load(Ordering::Acquire), 2048);
    let first = receiver.next().await.unwrap();
    let second = receiver.next().await.unwrap();
    assert_eq!(
        (
            response_header(&first).unique,
            response_header(&first).error
        ),
        (231, -libc::ENOMEM)
    );
    assert_eq!(
        (
            response_header(&second).unique,
            response_header(&second).error
        ),
        (232, -libc::ENOMEM)
    );
    assert_eq!(fs.control_used.load(Ordering::Acquire), 2048);
    let clone = first.clone();
    drop(first);
    assert_eq!(fs.control_used.load(Ordering::Acquire), 2048);
    drop(clone);
    assert_eq!(fs.control_used.load(Ordering::Acquire), 1024);
    drop(second);
    assert_eq!(fs.control_used.load(Ordering::Acquire), 0);
    assert!(receiver.next().now_or_never().is_none());
    session.inflight.store(0, Ordering::Release);
}

#[tokio::test]
async fn readonly_interrupt_reply_owns_control_memory_until_last_consumer() {
    let readonly = ReadonlyFs {
        cancel_enabled: true,
        control_capacity: 1024,
        ..ReadonlyFs::default()
    };
    let used = readonly.control_used.clone();
    let fs = Arc::new(LoggingFileSystem::new(readonly));
    let mut session = Session::new(MountOptions::default());
    let mut receiver = session.response_receivers[0].take().unwrap();
    session
        .handle_interrupt(
            Request {
                unique: 241,
                uid: 0,
                gid: 0,
                pid: 0,
            },
            &999u64.to_le_bytes(),
            &fs,
        )
        .await
        .unwrap();
    let packet = receiver.next().await.unwrap();
    assert_eq!(
        (
            response_header(&packet).unique,
            response_header(&packet).error
        ),
        (241, -libc::EAGAIN)
    );
    assert_eq!(used.load(Ordering::Acquire), 1024);
    let clone = packet.clone();
    drop(packet);
    assert_eq!(used.load(Ordering::Acquire), 1024);
    drop(clone);
    assert_eq!(used.load(Ordering::Acquire), 0);
}
