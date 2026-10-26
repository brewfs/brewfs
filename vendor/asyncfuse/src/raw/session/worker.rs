//! Worker pool implementation for handling FUSE requests concurrently.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;

use bytes::Bytes;
use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures_channel::oneshot;
use futures_util::future::{BoxFuture, FutureExt};
use futures_util::stream::FuturesUnordered;
use futures_util::stream::{Stream, StreamExt};
use tracing::debug;

#[cfg(all(
    not(feature = "tokio-runtime"),
    not(feature = "io-uring-runtime"),
    feature = "async-io-runtime"
))]
use async_global_executor::{self as task, Task as JoinHandle};
#[cfg(any(
    all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
    feature = "io-uring-runtime"
))]
use tokio::task;
#[cfg(any(
    all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
    feature = "io-uring-runtime"
))]
use tokio::task::JoinHandle;

use crate::raw::abi::fuse_opcode;
use crate::raw::filesystem::Filesystem;
use crate::raw::FuseData;

use super::handlers::*;
use super::owned_open_queue::{OpenLane, OpenLanePlan};
use super::reply_tracker::ReplyTracker;
use super::utils::InHeaderLite;

#[derive(Debug)]
/// Represents a work item to be processed by a worker thread in the worker pool
pub(crate) struct WorkItem {
    pub(crate) unique: u64,
    pub(crate) opcode: u32,
    pub(crate) in_header: InHeaderLite,
    /// Body data (excludes fixed-size fuse_in_header) - uses Bytes for zero-copy sharing
    pub(crate) data: Bytes,
    /// Inflight guard for backpressure control.
    /// None for FORGET/BATCH_FORGET messages to prevent thread explosion during large deletions.
    pub(crate) _inflight_guard: Option<InflightGuard>,
    pub(crate) _memory_guard: Option<crate::raw::reply::ReplyMemoryGuard>,
    pub(crate) _response_registration: Option<ResponseMemoryRegistration>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WorkerJoinLedger {
    errors: Arc<Mutex<Vec<String>>>,
}

impl WorkerJoinLedger {
    #[cfg(any(
        all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
        feature = "io-uring-runtime"
    ))]
    fn record(&self, scope: &str, error: impl std::fmt::Display) {
        self.errors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(format!("{scope}: {error}"));
    }

    fn take_error(&self) -> Option<std::io::Error> {
        let mut errors = self
            .errors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let first = errors.first().cloned();
        errors.clear();
        first.map(std::io::Error::other)
    }
}

#[cfg(all(test, not(feature = "async-io-runtime"), feature = "tokio-runtime"))]
mod ordinary_worker_join_tests {
    use super::*;
    use crate::raw::reply::{ReplyAttr, ReplyInit};
    use crate::raw::request::Request;
    use crate::MountOptions;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    #[derive(Debug)]
    struct PendingFs {
        entered: AtomicUsize,
        finished: AtomicUsize,
        release: tokio::sync::Notify,
        panic_in_getattr: bool,
    }

    impl Filesystem for PendingFs {
        async fn init(&self, _: Request) -> crate::Result<ReplyInit> {
            Ok(ReplyInit::default())
        }
        async fn destroy(&self, _: Request) {}
        async fn getattr(
            &self,
            _: Request,
            _: crate::Inode,
            _: Option<u64>,
            _: u32,
        ) -> crate::Result<ReplyAttr> {
            self.entered.fetch_add(1, Ordering::Release);
            if self.panic_in_getattr {
                panic!("controlled ordinary handler panic");
            }
            self.release.notified().await;
            self.finished.fetch_add(1, Ordering::Release);
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

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_worker_reaps_10000_completed_tasks_and_keeps_pending_join() {
        const COMPLETED: usize = 10_000;
        let (sender, mut receiver) = unbounded();
        let mut owned = FuturesUnordered::<BoxFuture<'static, ()>>::new();
        let (release, wait) = oneshot::channel::<()>();
        let pending = tokio::spawn(async move { wait.await.unwrap() });
        owned.push(Box::pin(async move { pending.await.unwrap() }));
        let mut max_retained = 0;

        for unique in 1..=COMPLETED {
            let child = tokio::spawn(async {});
            while !child.is_finished() {
                tokio::task::yield_now().await;
            }
            owned.push(Box::pin(async move { child.await.unwrap() }));
            // Keep input ready: receiving more work must not starve reaping.
            sender
                .unbounded_send(WorkItem {
                    unique: unique as u64,
                    opcode: fuse_opcode::FUSE_GETATTR as u32,
                    in_header: InHeaderLite {
                        nodeid: 1,
                        uid: 0,
                        gid: 0,
                        pid: 0,
                    },
                    data: Bytes::from_static(&[0; 16]),
                    _inflight_guard: None,
                    _memory_guard: None,
                    _response_registration: None,
                })
                .unwrap();
            let item = next_ordinary_work_item(&mut receiver, &mut owned)
                .await
                .unwrap();
            assert_eq!(item.unique, unique as u64);
            max_retained = max_retained.max(owned.len());
        }

        println!(
            "completed={COMPLETED} retained={} max_retained={max_retained} pending=1",
            owned.len()
        );
        assert_eq!(max_retained, 1, "completed join bookkeeping must be reaped");
        assert_eq!(owned.len(), 1, "the unfinished join must remain owned");

        // Closing input must leave the pending join available for shutdown.
        drop(sender);
        assert!(next_ordinary_work_item(&mut receiver, &mut owned)
            .await
            .is_none());
        assert_eq!(owned.len(), 1);
        assert!(owned.next().now_or_never().is_none());
        release.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), owned.next())
                .await
                .expect("shutdown must join the pending task"),
            Some(())
        );
        assert!(owned.is_empty());
        println!("after pending task completes: retained={}", owned.len());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_worker_reaps_completed_tasks_while_input_is_idle() {
        let (sender, mut receiver) = unbounded();
        let mut owned = FuturesUnordered::<BoxFuture<'static, ()>>::new();
        let (release, wait) = oneshot::channel::<()>();
        let child = tokio::spawn(async move { wait.await.unwrap() });
        let (reaped, reaped_wait) = oneshot::channel();
        owned.push(Box::pin(async move {
            child.await.unwrap();
            reaped.send(()).unwrap();
        }));
        let mut receive = Box::pin(next_ordinary_work_item(&mut receiver, &mut owned));
        assert!(receive.as_mut().now_or_never().is_none());
        release.send(()).unwrap();
        tokio::select! {
            _ = &mut receive => panic!("the input channel is still open and empty"),
            result = tokio::time::timeout(Duration::from_secs(1), reaped_wait) => {
                result.expect("idle worker must reap the completed join").unwrap();
            }
        }
        drop(receive);
        assert!(!sender.is_closed());
        assert!(owned.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_worker_shutdown_joins_spawned_handler_before_return() {
        let fs = Arc::new(PendingFs {
            entered: AtomicUsize::new(0),
            finished: AtomicUsize::new(0),
            release: tokio::sync::Notify::new(),
            panic_in_getattr: false,
        });
        let mut session = super::super::Session::new(MountOptions::default()).with_workers(1, 2);
        session.ensure_workers(fs.clone()).unwrap();
        let workers = session.workers.as_mut().unwrap();
        workers.submit(WorkItem {
            unique: 7,
            opcode: fuse_opcode::FUSE_GETATTR as u32,
            in_header: InHeaderLite {
                nodeid: 1,
                uid: 0,
                gid: 0,
                pid: 0,
            },
            data: Bytes::from(vec![0; 16]),
            _inflight_guard: None,
            _memory_guard: None,
            _response_registration: None,
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while fs.entered.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary handler did not start");

        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut shutdown = Box::pin(workers.shutdown());
        assert!(
            matches!(shutdown.as_mut().poll(&mut cx), Poll::Pending),
            "shutdown must retain an owned join while a spawned ordinary handler is pending"
        );
        tokio::task::yield_now().await;
        assert_eq!(fs.finished.load(Ordering::Acquire), 0);
        assert!(
            matches!(shutdown.as_mut().poll(&mut cx), Poll::Pending),
            "worker join must remain pending until the handler retires"
        );

        fs.release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("ordinary worker shutdown did not join the handler");
        assert_eq!(fs.finished.load(Ordering::Acquire), 1);
        drop(session);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ordinary_worker_join_error_is_retained_as_secondary_result() {
        let fs = Arc::new(PendingFs {
            entered: AtomicUsize::new(0),
            finished: AtomicUsize::new(0),
            release: tokio::sync::Notify::new(),
            panic_in_getattr: true,
        });
        let mut session = super::super::Session::new(MountOptions::default()).with_workers(1, 2);
        session.ensure_workers(fs.clone()).unwrap();
        let workers = session.workers.as_mut().unwrap();
        workers.submit(WorkItem {
            unique: 8,
            opcode: fuse_opcode::FUSE_GETATTR as u32,
            in_header: InHeaderLite {
                nodeid: 1,
                uid: 0,
                gid: 0,
                pid: 0,
            },
            data: Bytes::from(vec![0; 16]),
            _inflight_guard: None,
            _memory_guard: None,
            _response_registration: None,
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while fs.entered.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary panic handler did not start");
        tokio::time::timeout(Duration::from_secs(1), async {
            while workers.join_ledger.errors.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary JoinError must be recorded before shutdown");
        let error = workers
            .shutdown()
            .await
            .expect("ordinary JoinError must be retained in the worker ledger");
        assert!(error.to_string().contains("ordinary handler join"));
        drop(session);
    }
}

type ResponseMemoryMap = Arc<Mutex<BTreeMap<u64, crate::raw::reply::ReplyMemoryGuard>>>;

#[derive(Debug)]
pub(crate) struct ResponseMemoryRegistration {
    unique: u64,
    map: ResponseMemoryMap,
}
impl Drop for ResponseMemoryRegistration {
    fn drop(&mut self) {
        let mut map = self.map.lock().unwrap();
        let owner = map.remove(&self.unique);
        // BTreeMap may retain an empty root allocation after its last removal.
        // Retire it before returning the last request/control memory permit.
        let empty = map.is_empty().then(|| std::mem::take(&mut *map));
        drop(map);
        drop(empty);
        drop(owner);
    }
}

struct OwnedResponseBody {
    data: crate::raw::reply::ReplyBytes,
    _guard: crate::raw::reply::ReplyMemoryGuard,
}
impl AsRef<[u8]> for OwnedResponseBody {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

pub(crate) struct ResponseSender<'a> {
    sender: &'a UnboundedSender<FuseData>,
    guard: Option<crate::raw::reply::ReplyMemoryGuard>,
    tracker: Option<&'a Arc<ReplyTracker>>,
    controller: &'a super::owned_open_queue::ControllerHandle,
    unique: u64,
}
impl ResponseSender<'_> {
    pub(crate) fn unbounded_send(
        &self,
        data: FuseData,
    ) -> Result<(), futures_channel::mpsc::TrySendError<FuseData>> {
        use futures_util::future::Either;
        let data = match data {
            Either::Left(packet) => match self.controller.wrap_packet(self.unique, packet) {
                Ok(packet) => Either::Right((Vec::new(), packet)),
                Err(packet) => match &self.guard {
                    Some(guard) => Either::Right((
                        Vec::new(),
                        crate::raw::reply::own_reply_buffer(packet, Some(guard.clone())),
                    )),
                    None => Either::Left(packet),
                },
            },
            Either::Right((header, body)) => match &self.guard {
                Some(guard) => Either::Right((
                    header,
                    Bytes::from_owner(OwnedResponseBody {
                        data: body,
                        _guard: guard.clone(),
                    })
                    .into(),
                )),
                None => Either::Right((header, body)),
            },
        };
        let result = self.sender.unbounded_send(data);
        if result.is_err() {
            if let Some(tracker) = self.tracker {
                tracker.fail(&std::io::Error::from(std::io::ErrorKind::BrokenPipe));
            }
        }
        result
    }
}

#[derive(Debug)]
/// RAII guard that tracks the number of in-flight requests
/// Increments counter on creation and decrements on drop
pub struct InflightGuard {
    inflight: Arc<AtomicUsize>,
    notify: Arc<async_notify::Notify>,
}

impl InflightGuard {
    pub fn new(inflight: Arc<AtomicUsize>, notify: Arc<async_notify::Notify>) -> Self {
        inflight.fetch_add(1, Ordering::AcqRel);
        Self { inflight, notify }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        self.notify.notify();
    }
}

#[derive(Debug)]
/// Dispatch context shared across all workers
pub(crate) struct DispatchCtx<FS: Filesystem + Send + Sync + 'static> {
    pub(crate) fs: Arc<FS>,
    pub(crate) resp: Vec<UnboundedSender<FuseData>>,
    pub(crate) notifications: UnboundedSender<crate::notify::Notification>,
    pub(crate) direct_io: bool,
    pub(crate) force_readdir_plus: bool,
    pub(crate) kernel_writeback_permissions: bool,
    pub(crate) _inflight: Arc<AtomicUsize>,
    pub(crate) _inflight_notify: Arc<async_notify::Notify>,
    pub(crate) response_memory: ResponseMemoryMap,
    pub(crate) readonly_reply_tracker: Option<Arc<ReplyTracker>>,
    pub(crate) open_controller: super::owned_open_queue::ControllerHandle,
}

impl<FS: Filesystem + Send + Sync + 'static> DispatchCtx<FS> {
    #[inline]
    pub(crate) fn resp_for(&self, unique: u64) -> ResponseSender<'_> {
        ResponseSender {
            sender: &self.resp[unique as usize % self.resp.len()],
            guard: self.response_memory.lock().unwrap().get(&unique).cloned(),
            tracker: self.readonly_reply_tracker.as_ref(),
            controller: &self.open_controller,
            unique,
        }
    }
    pub(crate) fn get_notify(&self, unique: u64) -> crate::notify::Notify {
        crate::notify::Notify::new(
            self.resp[unique as usize % self.resp.len()].clone(),
            self.notifications.clone(),
        )
    }
    pub(crate) fn register_memory(&self, item: &mut WorkItem) {
        if let Some(tracker) = &self.readonly_reply_tracker {
            if let Some(inflight) = item._inflight_guard.take() {
                item._memory_guard =
                    Some(tracker.owner(item.unique, item._memory_guard.take(), inflight));
            }
        }
        if let Some(guard) = &item._memory_guard {
            self.response_memory
                .lock()
                .unwrap()
                .insert(item.unique, guard.clone());
            item._response_registration = Some(ResponseMemoryRegistration {
                unique: item.unique,
                map: self.response_memory.clone(),
            });
        }
    }
}

#[derive(Debug)]
/// Worker pool for processing FUSE requests
pub(crate) struct Workers<FS: Filesystem + Send + Sync + 'static> {
    /// Input queues for each worker (unbounded)
    senders: Vec<UnboundedSender<WorkItem>>,
    /// Round-robin counter for load balancing
    next: AtomicUsize,
    #[allow(dead_code)]
    handles: Vec<JoinHandle<()>>,
    shutdown_senders: Vec<oneshot::Sender<()>>,
    join_ledger: WorkerJoinLedger,
    _ctx: Arc<DispatchCtx<FS>>,
}

impl<FS: Filesystem + Send + Sync + 'static> Workers<FS> {
    pub(crate) fn new(
        worker_count: usize,
        _queue_capacity: usize,
        _ctx: Arc<DispatchCtx<FS>>,
        mut open_plan: Option<OpenLanePlan>,
    ) -> crate::Result<Self> {
        let mut senders = Vec::with_capacity(worker_count);
        let mut handles = Vec::with_capacity(worker_count);
        let mut shutdown_senders = Vec::with_capacity(worker_count);
        let join_ledger = WorkerJoinLedger::default();
        for idx in 0..worker_count {
            let (tx, mut rx): (UnboundedSender<WorkItem>, UnboundedReceiver<WorkItem>) =
                unbounded();
            let (shutdown_sender, shutdown_receiver) = oneshot::channel();
            shutdown_senders.push(shutdown_sender);
            let ctx_clone = _ctx.clone();
            #[cfg(any(
                all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
                feature = "io-uring-runtime"
            ))]
            let join_ledger_clone = join_ledger.clone();
            let open_lane = open_plan.as_mut().map(|plan| plan.take(idx));
            #[cfg(all(
                not(feature = "tokio-runtime"),
                not(feature = "io-uring-runtime"),
                feature = "async-io-runtime"
            ))]
            let handle = task::spawn(async move {
                if let Some(open_lane) = open_lane {
                    process_readonly_queue(ctx_clone, idx, rx, shutdown_receiver, open_lane).await;
                    return;
                }
                let mut owned = FuturesUnordered::<BoxFuture<'static, ()>>::new();
                while let Some(item) = next_ordinary_work_item(&mut rx, &mut owned).await {
                    // Keep every spawned ordinary handler owned by the worker
                    // until its JoinHandle is Ready. Dropping the handle here
                    // would let Session shutdown return while the handler
                    // still owns request/reply state.
                    let ctx = ctx_clone.clone();
                    let child = task::spawn(async move {
                        process_work_item(&ctx, idx, item).await;
                    });
                    #[cfg(any(
                        all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
                        feature = "io-uring-runtime"
                    ))]
                    let join_ledger = join_ledger_clone.clone();
                    owned.push(Box::pin(async move {
                        #[cfg(all(
                            not(feature = "async-io-runtime"),
                            any(feature = "tokio-runtime", feature = "io-uring-runtime")
                        ))]
                        if let Err(error) = child.await {
                            join_ledger.record("ordinary handler join", error);
                        }
                        #[cfg(all(
                            feature = "async-io-runtime",
                            not(feature = "tokio-runtime"),
                            not(feature = "io-uring-runtime")
                        ))]
                        child.await;
                    }));
                }
                while owned.next().await.is_some() {}
                debug!(worker=%idx, "worker exit");
            });
            #[cfg(any(
                all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
                feature = "io-uring-runtime"
            ))]
            let handle = task::spawn(async move {
                if let Some(open_lane) = open_lane {
                    process_readonly_queue(ctx_clone, idx, rx, shutdown_receiver, open_lane).await;
                    return;
                }
                let mut owned = FuturesUnordered::<BoxFuture<'static, ()>>::new();
                while let Some(item) = next_ordinary_work_item(&mut rx, &mut owned).await {
                    let ctx = ctx_clone.clone();
                    // Preserve the mutable cache-hit inline fast path.
                    if item.opcode == fuse_opcode::FUSE_READ as u32 {
                        process_work_item(&ctx, idx, item).await;
                    } else {
                        let child = task::spawn(async move {
                            process_work_item(&ctx, idx, item).await;
                        });
                        #[cfg(any(
                            all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
                            feature = "io-uring-runtime"
                        ))]
                        let join_ledger = join_ledger_clone.clone();
                        owned.push(Box::pin(async move {
                            #[cfg(any(
                                all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
                                feature = "io-uring-runtime"
                            ))]
                            if let Err(error) = child.await {
                                join_ledger.record("ordinary handler join", error);
                            }
                            #[cfg(all(
                                feature = "async-io-runtime",
                                not(feature = "tokio-runtime"),
                                not(feature = "io-uring-runtime")
                            ))]
                            child.await;
                        }));
                    }
                }
                while owned.next().await.is_some() {}
                debug!(worker=%idx, "worker exit");
            });
            senders.push(tx);
            handles.push(handle);
        }
        Ok(Self {
            senders,
            next: AtomicUsize::new(0),
            handles,
            shutdown_senders,
            join_ledger,
            _ctx,
        })
    }

    pub(crate) fn submit(&self, mut item: WorkItem) {
        self._ctx.register_memory(&mut item);
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        if self.senders[idx].unbounded_send(item).is_err() {
            if let Some(tracker) = &self._ctx.readonly_reply_tracker {
                tracker.fail(&std::io::Error::from(std::io::ErrorKind::BrokenPipe));
            }
            tracing::warn!("failed to enqueue work item, channel closed");
        }
    }

    pub(crate) fn close_request_memory_bytes(&self, body_bytes: u64) -> crate::Result<u64> {
        // Measure the same monomorphized future that the readonly queue boxes.
        // Constructing it does not poll the FS or allocate a boxed future.
        let future = readonly_close_future(
            self._ctx.clone(),
            WorkItem {
                unique: 0,
                opcode: fuse_opcode::FUSE_RELEASE as u32,
                in_header: InHeaderLite {
                    nodeid: 0,
                    uid: 0,
                    gid: 0,
                    pid: 0,
                },
                data: Bytes::new(),
                _inflight_guard: None,
                _memory_guard: None,
                _response_registration: None,
            },
        );
        // Include two independent map nodes (handler and reply tracker), reply
        // header, owner Arc, channel/Bytes and FuturesUnordered/Box overhead.
        // This increases the per-close charge, not the configured Control pool.
        body_bytes
            .checked_add(std::mem::size_of_val(&future) as u64)
            .and_then(|bytes| bytes.checked_add(2048))
            .ok_or_else(|| libc::ENOMEM.into())
    }

    pub(crate) async fn shutdown(&mut self) -> Option<std::io::Error> {
        for sender in self.shutdown_senders.drain(..) {
            let _ = sender.send(());
        }
        self.senders.clear();
        while let Some(handle) = self.handles.last_mut() {
            #[cfg(any(
                all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
                feature = "io-uring-runtime"
            ))]
            if let Err(error) = handle.await {
                self.join_ledger.record("worker join", error);
            }
            #[cfg(all(
                feature = "async-io-runtime",
                not(feature = "tokio-runtime"),
                not(feature = "io-uring-runtime")
            ))]
            handle.await;
            // The same actual handle remains owned across a cancelled waiter.
            drop(self.handles.pop());
        }
        self.join_ledger.take_error()
    }
}

impl<FS: Filesystem + Send + Sync + 'static> Drop for Workers<FS> {
    fn drop(&mut self) {
        if self._ctx.fs.supports_read_cancellation() {
            // Drop cannot await. Wake the owning worker so its local futures
            // retire; readonly work is never detached into child tasks.
            for sender in self.shutdown_senders.drain(..) {
                let _ = sender.send(());
            }
            self.senders.clear();
        }
    }
}

async fn next_ordinary_work_item(
    receiver: &mut UnboundedReceiver<WorkItem>,
    owned: &mut FuturesUnordered<BoxFuture<'static, ()>>,
) -> Option<WorkItem> {
    futures_util::future::poll_fn(|cx| {
        // Reap ready joins before accepting more work, and register child
        // wakeups even while input is idle. Pending joins remain owned for
        // the shutdown drain.
        while let Poll::Ready(Some(())) = owned.poll_next_unpin(cx) {}
        receiver.poll_next_unpin(cx)
    })
    .await
}

// This is the exact outer future stored by the ordinary readonly queue.
// Its type includes every process_work_item handler alternative and the real
// FS specialization, not only FS::open/FS::opendir's innermost future.
fn readonly_ordinary_future<FS: Filesystem + Send + Sync + 'static>(
    ctx: Arc<DispatchCtx<FS>>,
    worker_idx: usize,
    item: WorkItem,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    async move {
        process_work_item(&ctx, worker_idx, item).await;
    }
}

pub(super) fn readonly_ordinary_future_layout<FS: Filesystem + Send + Sync + 'static>(
) -> (usize, usize) {
    fn layout<FS, F>(_: fn(Arc<DispatchCtx<FS>>, usize, WorkItem) -> F) -> (usize, usize)
    where
        FS: Filesystem + Send + Sync + 'static,
        F: std::future::Future<Output = ()>,
    {
        (std::mem::size_of::<F>(), std::mem::align_of::<F>())
    }
    // Function type inference: no ctx/WorkItem/FS value, Box, or FS poll.
    layout(readonly_ordinary_future::<FS>)
}

async fn process_readonly_queue<FS: Filesystem + Send + Sync + 'static>(
    ctx: Arc<DispatchCtx<FS>>,
    worker_idx: usize,
    mut receiver: UnboundedReceiver<WorkItem>,
    shutdown: oneshot::Receiver<()>,
    mut open_lane: OpenLane,
) {
    enum Event {
        Stop,
        Item(Option<WorkItem>),
        Complete,
    }
    let shutdown = shutdown.fuse();
    futures_util::pin_mut!(shutdown);
    let mut owned = FuturesUnordered::<BoxFuture<'static, ()>>::new();
    loop {
        let event = futures_util::future::poll_fn(|cx| {
            // Stop is always checked before input and every child poll round.
            if shutdown.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Event::Stop);
            }
            // Poll both lanes before a ready receiver can win repeatedly.
            let other_complete = !owned.is_empty()
                && matches!(Pin::new(&mut owned).poll_next(cx), Poll::Ready(Some(())));
            let open_complete = open_lane.poll_one_completion(cx).is_ready();
            if other_complete || open_complete {
                return Poll::Ready(Event::Complete);
            }
            match Pin::new(&mut receiver).poll_next(cx) {
                Poll::Ready(item) => Poll::Ready(Event::Item(item)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await;
        match event {
            Event::Stop | Event::Item(None) => break,
            Event::Complete => {}
            Event::Item(Some(item)) => {
                if is_open_opcode(item.opcode) {
                    // Type inferred from the same complete factory boxed below;
                    // no FS poll, WorkItem move, or Box has occurred at denial.
                    let (bytes, align) = readonly_ordinary_future_layout::<FS>();
                    let roots = if open_lane.has_capacity() {
                        OpenLane::per_open_bytes(bytes)
                            .and_then(|bytes| ctx.fs.reserve_inline_root_memory(bytes))
                    } else {
                        Err(libc::ENOMEM.into())
                    };
                    match roots {
                        Err(error) => {
                            let data = super::utils::reply_error_in_worker(error, item.unique)
                                .expect("serialize original tracked rejection");
                            let _ = ctx
                                .resp_for(item.unique)
                                .unbounded_send(futures_util::future::Either::Left(data));
                            drop(item);
                        }
                        Ok(roots) => {
                            let request = item._memory_guard.clone();
                            let unique = item.unique;
                            let future = readonly_ordinary_future(ctx.clone(), worker_idx, item);
                            debug_assert_eq!(std::mem::size_of_val(&future), bytes);
                            debug_assert_eq!(std::mem::align_of_val(&future), align);
                            open_lane.push_admitted(future, unique, request, roots);
                        }
                    }
                } else {
                    let ctx = ctx.clone();
                    if is_close_opcode(item.opcode) {
                        owned.push(Box::pin(readonly_close_future(ctx, item)));
                    } else {
                        owned.push(Box::pin(readonly_ordinary_future(ctx, worker_idx, item)));
                    }
                }
            }
        }
    }
    // Actual OPEN/OPENDIR Box/lane retirement precedes packet retirement.
    // Packet wrappers keep the admitted controller through exact native tails.
    drop(receiver);
    drop(open_lane);
    drop(owned);
    debug!(worker=%worker_idx, "readonly worker exit");
}

fn is_open_opcode(opcode: u32) -> bool {
    opcode == fuse_opcode::FUSE_OPEN as u32 || opcode == fuse_opcode::FUSE_OPENDIR as u32
}

pub(crate) fn is_close_opcode(opcode: u32) -> bool {
    opcode == fuse_opcode::FUSE_FLUSH as u32
        || opcode == fuse_opcode::FUSE_RELEASE as u32
        || opcode == fuse_opcode::FUSE_RELEASEDIR as u32
}

// Keep lifecycle work independent of the largest ordinary handler future.
// Its actual concrete size is admitted by close_request_memory_bytes.
fn readonly_close_future<FS: Filesystem + Send + Sync + 'static>(
    ctx: Arc<DispatchCtx<FS>>,
    item: WorkItem,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    async move {
        match fuse_opcode::try_from(item.opcode) {
            Ok(fuse_opcode::FUSE_FLUSH) => handle_flush_inline(&ctx, item).await,
            Ok(fuse_opcode::FUSE_RELEASE) => handle_release_inline(&ctx, item).await,
            Ok(fuse_opcode::FUSE_RELEASEDIR) => handle_releasedir_inline(&ctx, item).await,
            _ => unreachable!("readonly close queue received an ordinary opcode"),
        }
    }
}

/// Dispatch work item to the appropriate handler based on opcode.
/// The `item` (including `InflightGuard`) is held until the handler completes,
/// ensuring backpressure accurately reflects in-flight FS operations.
async fn process_work_item<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    worker_idx: usize,
    item: WorkItem,
) {
    let opcode_result = fuse_opcode::try_from(item.opcode);
    dispatch_to_worker! {
        match opcode_result, {
            ctx => ctx,
            worker_idx => worker_idx,
            item => item,
            FUSE_FORGET   => handle_forget_inline,
            FUSE_LOOKUP   => handle_lookup_inline,
            FUSE_GETATTR  => handle_getattr_inline,
            FUSE_OPEN     => handle_open_inline,
            FUSE_READ     => handle_read_inline,
            FUSE_WRITE    => handle_write_inline,
            FUSE_READDIR  => handle_readdir_inline,
            FUSE_SETATTR  => handle_setattr_inline,
            FUSE_READLINK => handle_readlink_inline,
            FUSE_SYMLINK  => handle_symlink_inline,
            FUSE_MKNOD    => handle_mknod_inline,
            FUSE_MKDIR    => handle_mkdir_inline,
            FUSE_UNLINK   => handle_unlink_inline,
            FUSE_RMDIR    => handle_rmdir_inline,
            FUSE_RENAME   => handle_rename_inline,
            FUSE_LINK     => handle_link_inline,
            FUSE_STATFS   => handle_statfs_inline,
            FUSE_IOCTL   => handle_ioctl_inline,
            FUSE_RELEASE  => handle_release_inline,
            FUSE_FSYNC    => handle_fsync_inline,
            FUSE_SETXATTR => handle_setxattr_inline,
            FUSE_GETXATTR => handle_getxattr_inline,
            FUSE_LISTXATTR => handle_listxattr_inline,
            FUSE_REMOVEXATTR => handle_removexattr_inline,
            FUSE_FLUSH    => handle_flush_inline,
            FUSE_OPENDIR => handle_opendir_inline,
            FUSE_RELEASEDIR => handle_releasedir_inline,
            FUSE_FSYNCDIR => handle_fsyncdir_inline,
            FUSE_ACCESS  => handle_access_inline,
            FUSE_CREATE  => handle_create_inline,
            FUSE_BMAP    => handle_bmap_inline,
            FUSE_FALLOCATE => handle_fallocate_inline,
            FUSE_READDIRPLUS => handle_readdirplus_inline,
            FUSE_RENAME2 => handle_rename2_inline,
            FUSE_LSEEK => handle_lseek_inline,
            FUSE_COPY_FILE_RANGE => handle_copy_file_range_inline,
            FUSE_POLL => handle_poll_inline,
            FUSE_DESTROY => handle_destroy_inline,
            FUSE_INTERRUPT => handle_interrupt_inline,
            FUSE_NOTIFY_REPLY => handle_notify_reply_inline,
            FUSE_BATCH_FORGET => handle_batch_forget_inline,
            _ => {
                match opcode_result {
                    #[cfg(feature = "file-lock")]
                    Ok(fuse_opcode::FUSE_GETLK) => {
                        debug!(worker=%worker_idx, unique=item.unique, "worker handling GETLK");
                        handle_getlk_inline(ctx, item).await;
                    }
                    #[cfg(feature = "file-lock")]
                    Ok(fuse_opcode::FUSE_SETLK | fuse_opcode::FUSE_SETLKW) => {
                        debug!(worker=%worker_idx, unique=item.unique, "worker handling SETLK/SETLKW");
                        let is_blocking = item.opcode == fuse_opcode::FUSE_SETLKW as u32;
                        handle_setlk_inline(ctx, item, is_blocking).await;
                    }
                    #[cfg(target_os = "macos")]
                    Ok(fuse_opcode::FUSE_SETVOLNAME) => {
                        debug!(worker=%worker_idx, unique=item.unique, "worker handling SETVOLNAME");
                        handle_setvolname_inline(ctx, item).await;
                    }
                    #[cfg(target_os = "macos")]
                    Ok(fuse_opcode::FUSE_GETXTIMES) => {
                        debug!(worker=%worker_idx, unique=item.unique, "worker handling GETXTIMES");
                        handle_getxtimes_inline(ctx, item).await;
                    }
                    #[cfg(target_os = "macos")]
                    Ok(fuse_opcode::FUSE_EXCHANGE) => {
                        debug!(worker=%worker_idx, unique=item.unique, "worker handling EXCHANGE");
                        handle_exchange_inline(ctx, item).await;
                    }
                    Ok(_) => {
                        debug!(worker=%worker_idx, unique=item.unique, opcode=item.opcode, "opcode not yet handled in worker");
                    }
                    Err(err) => {
                        debug!(worker=%worker_idx, unique=item.unique, raw=item.opcode, "unknown opcode {}", err.0);
                    }
                }
            }
        }
    }
}

/// Macro for dispatching work items to handler functions
macro_rules! dispatch_to_worker {
    (
        match $target:expr, {
            ctx => $ctx:expr,
            worker_idx => $worker_idx:expr,
            item => $item:expr,

            $( $op:ident => $handler:ident, )*

            _ => { $($other_logic:tt)* }
        }
    ) => {
        match $target {
            $(
                Ok(fuse_opcode::$op) => {
                    debug!(
                        worker = %$worker_idx,
                        unique = $item.unique,
                        "worker handling {}",
                        stringify!($op).replace("FUSE_", "")
                    );
                    $handler($ctx, $item).await;
                },
            )*
            _ => { $($other_logic)* }
        }
    };
}

pub(super) use dispatch_to_worker;

#[cfg(all(
    test,
    any(
        feature = "io-uring-runtime",
        all(not(feature = "async-io-runtime"), feature = "tokio-runtime")
    )
))]
mod shutdown_waiter_cancel_tests {
    use super::*;
    use crate::raw::reply::{InlineRootPermit, ReplyInit, ReplyMemoryGuard};
    use crate::raw::request::Request;
    use crate::MountOptions;
    use std::task::Context;
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
    struct CancelFs {
        used: Arc<AtomicUsize>,
    }
    impl CancelFs {
        fn charge(&self, bytes: u64) -> crate::Result<Charge> {
            let bytes = usize::try_from(bytes).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
            self.used.fetch_add(bytes, Ordering::AcqRel);
            Ok(Charge {
                used: self.used.clone(),
                bytes,
            })
        }
    }
    impl Filesystem for CancelFs {
        async fn init(&self, _: Request) -> crate::Result<ReplyInit> {
            Ok(ReplyInit::default())
        }
        async fn destroy(&self, _: Request) {}
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
        fn supports_read_cancellation(&self) -> bool {
            true
        }
        fn reserve_input_buffer_memory(
            &self,
            bytes: u64,
        ) -> crate::Result<Option<ReplyMemoryGuard>> {
            Ok(Some(Arc::new(self.charge(bytes)?)))
        }
        fn reserve_inline_root_memory(
            &self,
            bytes: u64,
        ) -> crate::Result<Option<InlineRootPermit>> {
            Ok(Some(
                InlineRootPermit::try_new(self.charge(bytes)?).unwrap(),
            ))
        }
    }

    // Existing production Session::ensure_workers starts the actual readonly
    // queues. No synthetic task replaces any queue or original handle.
    #[tokio::test(flavor = "current_thread")]
    async fn current_shutdown_waiter_cancel_keeps_all_actual_readonly_worker_handles_for_resumed_join(
    ) {
        let fs = Arc::new(CancelFs::default());
        let mut session = super::super::Session::new(MountOptions::default()).with_workers(2, 2);
        session.ensure_workers(fs.clone()).unwrap();
        let workers = session.workers.as_mut().unwrap();
        let original_ids = workers
            .handles
            .iter()
            .map(|handle| handle.id())
            .collect::<Vec<_>>();
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // The real queue tasks exist, with admitted lane storage, but this
        // current-thread executor has not scheduled them. No scheduler sleep.
        let mut first = Box::pin(workers.shutdown());
        let first_pending = matches!(first.as_mut().poll(&mut cx), Poll::Pending);
        drop(first);
        let retained_ids = workers
            .handles
            .iter()
            .map(|handle| handle.id())
            .collect::<Vec<_>>();
        let still_charged_before_resume = fs.used.load(Ordering::Acquire) > 0;
        let mut resumed = Box::pin(workers.shutdown());
        let resumed_pending = matches!(resumed.as_mut().poll(&mut cx), Poll::Pending);
        drop(resumed);

        tokio::time::timeout(Duration::from_secs(1), workers.shutdown())
            .await
            .expect("fixture readonly worker cleanup stalled");
        let handles_consumed_after_resume = workers.handles.is_empty();
        drop(session);
        // This final Drop/accounting drain is safe fixture cleanup on both RED
        // and GREEN; it is not a substitute for the captured same-handle facts.
        tokio::time::timeout(Duration::from_secs(1), async {
            while fs.used.load(Ordering::Acquire) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fixture readonly lane owners did not retire");

        assert!(
            first_pending && original_ids.len() == 2,
            "fixture must own two real Pending readonly worker handles"
        );
        assert!(
            still_charged_before_resume,
            "real lane/controller owner must still be charged"
        );
        assert_eq!(
            retained_ids, original_ids,
            "cancelled shutdown waiter must retain every original readonly worker JoinHandle"
        );
        assert!(
            resumed_pending,
            "resumed shutdown must actually join the original readonly workers"
        );
        assert!(
            handles_consumed_after_resume,
            "actual Ready must consume each original worker handle"
        );
    }
}
