//! FUSE session management with worker pool support.
//!
//! This module provides the core [`Session`] type for handling FUSE filesystem operations.
//! It supports both legacy single-threaded mode and modern worker pool mode for better concurrency.

#[cfg(all(
    test,
    target_os = "linux",
    feature = "tokio-runtime",
    not(feature = "io-uring-runtime"),
    not(feature = "async-io-runtime")
))]
mod close_admission_tests;
#[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
mod control_failure_tests;
mod handlers;
pub(crate) mod owned_open_queue;
mod owned_prepare;
#[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
mod reply_lifecycle_tests;
mod reply_tracker;
#[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
mod request_cancel_tests;
#[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
mod startup_preflight_tests;
#[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
mod unmount_order_tests;
mod utils;
mod worker;
#[cfg(all(
    test,
    feature = "buffer-pool",
    not(feature = "async-io-runtime"),
    feature = "tokio-runtime"
))]
mod write_buffer_admission_tests;
mod xattr_reply;

// Re-export public types
pub use worker::InflightGuard;
pub(crate) use worker::WorkItem;

// Internal types used across submodules
use reply_tracker::{ReplyPumpLifetime, ReplyTracker};
use utils::{
    apply_direct_io, is_forget_opcode, reply_error_in_place, reply_error_in_worker, spawn,
    InHeaderLite, ReadResult,
};
use worker::{DispatchCtx, Workers};

#[cfg(all(target_os = "linux", feature = "unprivileged"))]
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fmt::Debug;
use std::future::Future;
use std::io::Error as IoError;
use std::io::Result as IoResult;
use std::num::NonZeroU32;
#[allow(unused_imports)]
use std::os::fd::AsFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::ffi::OsStringExt;
#[allow(unused_imports)]
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
#[cfg(target_os = "macos")]
use std::time::Duration;

#[cfg(all(not(feature = "tokio-runtime"), feature = "async-io-runtime"))]
use async_fs::read_dir;
#[cfg(all(not(feature = "tokio-runtime"), feature = "async-io-runtime"))]
use async_global_executor::{self as task, Task as JoinHandle};
#[cfg(all(
    target_os = "linux",
    not(feature = "tokio-runtime"),
    feature = "async-io-runtime",
    feature = "unprivileged"
))]
use async_process::Command;
use bincode::Options;
use bytes::Bytes;
use futures_channel::{
    mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    oneshot,
};
use futures_util::future::Either;
#[cfg(all(not(feature = "tokio-runtime"), feature = "async-io-runtime"))]
use futures_util::future::FutureExt;
#[cfg(all(
    target_os = "macos",
    not(feature = "tokio-runtime"),
    feature = "async-io-runtime"
))]
use futures_util::select;
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use nix::mount;
#[cfg(any(target_os = "freebsd", target_os = "macos"))]
use nix::mount::MntFlags;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(all(
    target_os = "linux",
    not(feature = "async-io-runtime"),
    any(feature = "tokio-runtime", feature = "io-uring-runtime"),
    feature = "unprivileged"
))]
use tokio::process::Command;
#[cfg(any(
    all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
    feature = "io-uring-runtime"
))]
use tokio::task::JoinHandle;
#[cfg(any(
    all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
    feature = "io-uring-runtime"
))]
use tokio::{fs::read_dir, task};
use tracing::{debug, debug_span, error, instrument, warn};

#[cfg(all(target_os = "linux", feature = "unprivileged"))]
use crate::find_fusermount3;
use crate::helper::*;
use crate::notify::{Notification, Notify};
use crate::raw::abi::*;
use crate::raw::buffer_pool::AlignedBuffer;
#[cfg(all(
    feature = "buffer-pool",
    not(feature = "async-io-runtime"),
    feature = "tokio-runtime"
))]
use crate::raw::buffer_pool::BufferPool;
#[cfg(any(
    feature = "async-io-runtime",
    feature = "tokio-runtime",
    feature = "io-uring-runtime"
))]
use crate::raw::connection::FuseConnection;
use crate::raw::filesystem::Filesystem;
use crate::raw::request::Request;
use crate::raw::FuseData;
use crate::{MountOptions, SetAttr};

#[cfg(all(
    test,
    target_os = "linux",
    feature = "tokio-runtime",
    not(feature = "io-uring-runtime"),
    not(feature = "async-io-runtime")
))]
mod notification_tests;

/// Header, channel node and the synchronous readonly control operation state.
const CONTROL_REPLY_MEMORY_BYTES: u64 = 1024;
/// Conservative Roots charge for the retained pre-unmount cancellation future
/// and its Arc/Box wrapper. It is admitted before any physical mount syscall.
const PRE_UNMOUNT_MEMORY_BYTES: u64 = 512;

#[cfg(target_os = "macos")]
const MACFUSE_INIT_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(all(
    feature = "buffer-pool",
    not(feature = "async-io-runtime"),
    feature = "tokio-runtime"
))]
struct CopiedRequestBuffer {
    data: Vec<u8>,
    _memory_guard: Option<crate::raw::reply::ReplyMemoryGuard>,
}

#[cfg(all(
    feature = "buffer-pool",
    not(feature = "async-io-runtime"),
    feature = "tokio-runtime"
))]
impl AsRef<[u8]> for CopiedRequestBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(all(
    feature = "buffer-pool",
    not(feature = "async-io-runtime"),
    feature = "tokio-runtime"
))]
struct PooledRequestBuffer {
    buffer: Option<AlignedBuffer>,
    pool: Arc<BufferPool>,
    len: usize,
}

#[cfg(all(
    feature = "buffer-pool",
    not(feature = "async-io-runtime"),
    feature = "tokio-runtime"
))]
impl AsRef<[u8]> for PooledRequestBuffer {
    fn as_ref(&self) -> &[u8] {
        &self
            .buffer
            .as_ref()
            .expect("pooled request buffer already released")[..self.len]
    }
}

#[cfg(all(
    feature = "buffer-pool",
    not(feature = "async-io-runtime"),
    feature = "tokio-runtime"
))]
impl Drop for PooledRequestBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.try_release(buffer);
        }
    }
}

#[cfg(target_os = "macos")]
async fn wait_for_mount_init_ready(ready_rx: oneshot::Receiver<IoResult<()>>) -> IoResult<()> {
    #[cfg(all(not(feature = "async-io-runtime"), feature = "tokio-runtime"))]
    {
        match tokio::time::timeout(MACFUSE_INIT_TIMEOUT, ready_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(IoError::other(
                "FUSE mount task ended before init completed",
            )),
            Err(_) => Err(IoError::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "timed out after {:?} waiting for FUSE INIT",
                    MACFUSE_INIT_TIMEOUT
                ),
            )),
        }
    }

    #[cfg(all(not(feature = "tokio-runtime"), feature = "async-io-runtime"))]
    {
        let ready = FutureExt::fuse(ready_rx);
        let timeout = FutureExt::fuse(async_io::Timer::after(MACFUSE_INIT_TIMEOUT));
        let mut ready = pin!(ready);
        let mut timeout = pin!(timeout);

        select! {
            result = ready => {
                match result {
                    Ok(result) => result,
                    Err(_) => Err(IoError::other("FUSE mount task ended before init completed")),
                }
            }
            _ = timeout => {
                Err(IoError::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "timed out after {:?} waiting for FUSE INIT",
                        MACFUSE_INIT_TIMEOUT
                    ),
                ))
            }
        }
    }
}

/// A Future which returns when a file system is unmounted
///
/// when drop the [`MountHandle`], it will unmount Filesystem in background task, if user want to
/// wait unmount completely, use [`MountHandle::unmount`]
#[derive(Debug)]
pub struct MountHandle {
    inner: Option<MountHandleInner>,
}

impl MountHandle {
    pub async fn unmount(mut self) -> IoResult<()> {
        self.inner
            .take()
            .expect("unmount call twice")
            .inner_unmount()
            .await
    }
}

impl Drop for MountHandle {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            if inner.task.is_finished() {
                return;
            }

            #[cfg(all(
                not(feature = "tokio-runtime"),
                not(feature = "io-uring-runtime"),
                feature = "async-io-runtime"
            ))]
            {
                task::spawn(inner.inner_unmount()).detach();
            }

            #[cfg(any(
                all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
                feature = "io-uring-runtime"
            ))]
            {
                task::spawn(inner.inner_unmount());
            }
        }
    }
}

struct MountHandleInner {
    task: JoinHandle<IoResult<()>>,
    mount_path: PathBuf,
    destroy_notify: Arc<async_notify::Notify>,
    pre_unmount: Option<Pin<Box<dyn Future<Output = IoResult<()>> + Send + 'static>>>,
    pre_unmount_memory: Option<crate::raw::reply::ReplyMemoryGuard>,
    #[cfg(all(target_os = "linux", feature = "unprivileged"))]
    unprivileged: bool,
    #[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
    ordinary_unmount_for_test: Option<OrdinaryUnmountForTest>,
}

impl Debug for MountHandleInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MountHandleInner")
            .field("task", &self.task)
            .field("mount_path", &self.mount_path)
            .field("destroy_notify", &self.destroy_notify)
            .field("pre_unmount", &self.pre_unmount.as_ref().map(|_| "pending"))
            .field(
                "pre_unmount_memory",
                &self.pre_unmount_memory.as_ref().map(|_| "held"),
            )
            .finish()
    }
}

#[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
struct OrdinaryUnmountForTest {
    future: Pin<Box<dyn Future<Output = IoResult<()>> + Send + 'static>>,
}
#[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
impl Debug for OrdinaryUnmountForTest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrdinaryUnmountForTest")
            .finish_non_exhaustive()
    }
}

impl MountHandleInner {
    #[cfg(any(
        all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
        feature = "io-uring-runtime"
    ))]
    async fn finish_failed_physical_unmount(&mut self, error: IoError) -> IoResult<()> {
        self.destroy_notify.notify();
        let cleanup_ok = matches!((&mut self.task).await, Ok(Ok(())));
        debug!(
            target: "asyncfuse::teardown",
            event = "failed_physical_unmount_session_joined",
            cleanup_ok
        );
        Err(error)
    }

    #[cfg(all(
        not(feature = "tokio-runtime"),
        not(feature = "io-uring-runtime"),
        feature = "async-io-runtime"
    ))]
    async fn finish_failed_physical_unmount(&mut self, error: IoError) -> IoResult<()> {
        self.destroy_notify.notify();
        let cleanup_ok = matches!(
            std::panic::AssertUnwindSafe(&mut self.task)
                .catch_unwind()
                .await,
            Ok(Ok(()))
        );
        debug!(
            target: "asyncfuse::teardown",
            event = "failed_physical_unmount_session_joined",
            cleanup_ok
        );
        Err(error)
    }

    #[allow(unused_mut)]
    async fn inner_unmount(mut self) -> IoResult<()> {
        if let Some(mut prepare) = self.pre_unmount.take() {
            let prepared = prepare.as_mut().await;
            // Retire the completed admitted preparation before waking teardown.
            drop(prepare);
            if let Err(primary) = prepared {
                self.destroy_notify.notify();
                // Keep the actual owned Session handle while awaiting it. A
                // secondary task error/panic must not replace the prepare errno.
                #[cfg(any(
                    all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
                    feature = "io-uring-runtime"
                ))]
                let cleanup_ok = matches!((&mut self.task).await, Ok(Ok(())));
                #[cfg(all(
                    not(feature = "tokio-runtime"),
                    not(feature = "io-uring-runtime"),
                    feature = "async-io-runtime"
                ))]
                let cleanup_ok = matches!(
                    std::panic::AssertUnwindSafe(&mut self.task)
                        .catch_unwind()
                        .await,
                    Ok(Ok(()))
                );
                tracing::debug!(target: "asyncfuse::teardown",
                    event = "failed_prepare_session_task_joined", cleanup_ok);
                // This is Session-task completion. Native/reply actor joins
                // require their separate owned teardown implementation.
                return Err(primary);
            }
        }
        #[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
        if let Some(mut operation) = self.ordinary_unmount_for_test.take() {
            match operation.future.as_mut().await {
                Ok(()) => {
                    self.destroy_notify.notify();
                    self.task.await.unwrap()?;
                    return Ok(());
                }
                Err(error) => return self.finish_failed_physical_unmount(error).await,
            }
        }
        #[cfg(all(not(feature = "tokio-runtime"), feature = "async-io-runtime"))]
        {
            // TODO: freebsd mount is unprivileged, then unmount is unprivileged too?
            #[cfg(target_os = "freebsd")]
            {
                let mount_path = self.mount_path.clone();
                let physical = task::spawn_blocking(move || {
                    mount::unmount(&mount_path, MntFlags::MNT_SYNCHRONOUS)
                })
                .await;
                match physical {
                    Ok(()) => {
                        self.destroy_notify.notify();
                        self.task.await?;
                    }
                    Err(error) => return self.finish_failed_physical_unmount(error.into()).await,
                }
            }

            #[cfg(target_os = "macos")]
            {
                let mount_path = self.mount_path.clone();
                let physical = task::spawn_blocking(move || {
                    mount::unmount(&mount_path, MntFlags::MNT_SYNCHRONOUS)
                })
                .await;
                match physical {
                    Ok(()) => {
                        self.destroy_notify.notify();
                        self.task.await?;
                    }
                    Err(error) => return self.finish_failed_physical_unmount(error.into()).await,
                }
            }

            #[cfg(target_os = "linux")]
            {
                #[cfg(all(target_os = "linux", feature = "unprivileged"))]
                if self.unprivileged {
                    use std::io::ErrorKind;
                    let binary_path = match find_fusermount3() {
                        Ok(path) => path,
                        Err(error) => return self.finish_failed_physical_unmount(error).await,
                    };
                    let mut child = match Command::new(binary_path)
                        .args([OsStr::new("-u"), self.mount_path.as_os_str()])
                        .spawn()
                    {
                        Ok(child) => child,
                        Err(error) => return self.finish_failed_physical_unmount(error).await,
                    };
                    let status = match child.status().await {
                        Ok(status) => status,
                        Err(error) => return self.finish_failed_physical_unmount(error).await,
                    };
                    if !status.success() {
                        return self
                            .finish_failed_physical_unmount(IoError::new(
                                ErrorKind::Other,
                                "call fusermount3 -u to unmount failed",
                            ))
                            .await;
                    }

                    self.destroy_notify.notify();
                    self.task.await?;
                    return Ok(());
                }

                let mount_path = self.mount_path.clone();
                let physical = task::spawn_blocking(move || mount::umount(&mount_path)).await;
                match physical {
                    Ok(()) => {
                        self.destroy_notify.notify();
                        self.task.await?;
                    }
                    Err(error) => return self.finish_failed_physical_unmount(error.into()).await,
                }
            }
        }

        #[cfg(all(not(feature = "async-io-runtime"), feature = "tokio-runtime"))]
        {
            // TODO: freebsd mount is unprivileged, then unmount is unprivileged too?
            #[cfg(target_os = "freebsd")]
            {
                let mount_path = self.mount_path.clone();
                let physical = task::spawn_blocking(move || {
                    mount::unmount(&mount_path, MntFlags::MNT_SYNCHRONOUS)
                })
                .await;
                let physical = match physical {
                    Ok(result) => result,
                    Err(error) => {
                        return self
                            .finish_failed_physical_unmount(IoError::other(error.to_string()))
                            .await
                    }
                };
                match physical {
                    Ok(()) => {
                        self.destroy_notify.notify();
                        self.task.await.unwrap()?;
                    }
                    Err(error) => return self.finish_failed_physical_unmount(error.into()).await,
                }
            }
            #[cfg(target_os = "macos")]
            {
                let mount_path = self.mount_path.clone();
                let physical = task::spawn_blocking(move || {
                    mount::unmount(&mount_path, MntFlags::MNT_SYNCHRONOUS)
                })
                .await;
                let physical = match physical {
                    Ok(result) => result,
                    Err(error) => {
                        return self
                            .finish_failed_physical_unmount(IoError::other(error.to_string()))
                            .await
                    }
                };
                match physical {
                    Ok(()) => {
                        self.destroy_notify.notify();
                        self.task.await.unwrap()?;
                    }
                    Err(error) => return self.finish_failed_physical_unmount(error.into()).await,
                }
            }

            #[cfg(target_os = "linux")]
            {
                #[cfg(all(target_os = "linux", feature = "unprivileged"))]
                if self.unprivileged {
                    let binary_path = match find_fusermount3() {
                        Ok(path) => path,
                        Err(error) => return self.finish_failed_physical_unmount(error).await,
                    };
                    let mut child = match Command::new(binary_path)
                        .args([OsStr::new("-u"), self.mount_path.as_os_str()])
                        .spawn()
                    {
                        Ok(child) => child,
                        Err(error) => return self.finish_failed_physical_unmount(error).await,
                    };
                    let status = match child.wait().await {
                        Ok(status) => status,
                        Err(error) => return self.finish_failed_physical_unmount(error).await,
                    };
                    if !status.success() {
                        return self
                            .finish_failed_physical_unmount(IoError::other(
                                "call fusermount3 -u to unmount failed",
                            ))
                            .await;
                    }

                    self.destroy_notify.notify();
                    self.task.await.unwrap()?;
                    return Ok(());
                }

                let mount_path = self.mount_path.clone();
                let physical = task::spawn_blocking(move || mount::umount(&mount_path)).await;
                let physical = match physical {
                    Ok(result) => result,
                    Err(error) => {
                        return self
                            .finish_failed_physical_unmount(IoError::other(error.to_string()))
                            .await
                    }
                };
                match physical {
                    Ok(()) => {
                        self.destroy_notify.notify();
                        self.task.await.unwrap()?;
                    }
                    Err(error) => return self.finish_failed_physical_unmount(error.into()).await,
                }
            }
        }

        Ok(())
    }
}

impl Future for MountHandle {
    type Output = IoResult<()>;

    #[cfg(all(
        not(feature = "tokio-runtime"),
        not(feature = "io-uring-runtime"),
        feature = "async-io-runtime"
    ))]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner.as_mut().expect("inner should be Some()").task).poll(cx)
    }

    #[cfg(any(feature = "tokio-runtime", feature = "io-uring-runtime"))]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The unwrap is necessary in order to provide the same API for both runtimes, and actually
        // unwrap should not panic, when MountHandle is canceled by unmount method, user has no
        // chance to poll again
        Pin::new(&mut self.inner.as_mut().expect("inner should be Some()").task)
            .poll(cx)
            .map(Result::unwrap)
    }
}
#[cfg(any(
    feature = "async-io-runtime",
    feature = "tokio-runtime",
    feature = "io-uring-runtime"
))]
/// FUSE filesystem session with inode-based operations.
///
/// # Concurrency Model
///
/// The session supports two concurrency modes:
///
/// 1. **Legacy mode** (default, `worker_count <= 1`): Requests are processed inline with
///    async spawning for each operation. Simple but may have ordering issues.
///
/// 2. **Worker pool mode** (`worker_count > 1`): Requests are distributed to a pool of
///    worker tasks using round-robin scheduling. Provides better throughput and
///    backpressure control via `max_background`.
///
/// # Backpressure
///
/// When using worker pool mode, backpressure is applied when the number of in-flight
/// requests reaches `max_background`. FORGET/BATCH_FORGET messages are exempt from
/// this limit to prevent thread explosion during large directory deletions (matching
/// libfuse behavior).
///
/// # Example
///
/// ```ignore
/// use asyncfuse::MountOptions;
/// use asyncfuse::raw::Session;
///
/// let session = Session::new(MountOptions::default())
///     .with_workers(4, 64);  // 4 workers, max 64 in-flight
/// ```
pub struct Session<FS: Filesystem + Send + Sync + 'static> {
    fuse_connection: Option<Arc<FuseConnection>>,
    filesystem: Option<Arc<FS>>,
    response_senders: Vec<UnboundedSender<FuseData>>,
    response_receivers: Vec<Option<UnboundedReceiver<FuseData>>>,
    notification_sender: UnboundedSender<Notification>,
    notification_receiver: Option<UnboundedReceiver<Notification>>,
    mount_options: MountOptions,
    kernel_writeback_permissions: bool,
    // ---- Concurrency configuration ----
    /// Number of worker tasks to execute FUSE requests. 0 or 1 keeps legacy inline spawn behavior.
    worker_count: usize,
    /// Upper bound of in-flight (queued + running) requests before applying backpressure.
    /// FORGET/BATCH_FORGET messages are exempt to prevent thread explosion.
    max_background: usize,
    /// If true, serialize operations per (parent inode, name) / inode to preserve ordering.
    _per_inode_serial: bool,
    /// Internal worker pool (created lazily when worker_count > 1).
    workers: Option<Workers<FS>>,
    // Admitted before connection/helper/kernel startup; moved once into workers.
    readonly_open_plan: std::sync::Mutex<Option<owned_open_queue::OpenLanePlan>>,
    inflight: Arc<AtomicUsize>,
    inflight_notify: Arc<async_notify::Notify>,
    readonly_reply_tracker: Option<Arc<ReplyTracker>>,
}

fn kernel_writeback_permissions(default_permissions: bool, negotiated_flags: u32) -> bool {
    cfg!(target_os = "linux") && default_permissions && negotiated_flags & FUSE_WRITEBACK_CACHE != 0
}

#[cfg(all(test, target_os = "linux"))]
mod writeback_permission_tests {
    use super::*;

    #[test]
    fn timestamp_context_requires_kernel_permissions_and_negotiated_writeback() {
        assert!(kernel_writeback_permissions(true, FUSE_WRITEBACK_CACHE));
        assert!(!kernel_writeback_permissions(false, FUSE_WRITEBACK_CACHE));
        assert!(!kernel_writeback_permissions(true, 0));
        assert!(!kernel_writeback_permissions(false, 0));
        assert!(!kernel_writeback_permissions(true, FUSE_ASYNC_READ));
    }
}

#[cfg(any(
    feature = "async-io-runtime",
    feature = "tokio-runtime",
    feature = "io-uring-runtime"
))]
impl<FS: Filesystem + Send + Sync + 'static> Session<FS> {
    /// Compiler layout of the actual ordinary readonly worker future.
    /// This measures the same factory boxed by the queue, with no FS effects.
    #[doc(hidden)]
    pub fn readonly_ordinary_worker_future_layout() -> (usize, usize) {
        worker::readonly_ordinary_future_layout::<FS>()
    }

    /// Compiler layouts of the exact preparation factories used below.
    /// Order: complete FS/failure race child, owned holder, stored outer.
    /// Each tuple is (size, align).
    /// Uses function type inference without an FS value, allocation, or poll.
    #[doc(hidden)]
    pub fn readonly_prepare_future_layout() -> ((usize, usize), (usize, usize), (usize, usize)) {
        owned_prepare::preparation_future_layout::<FS>()
    }

    /// new a fuse filesystem session.
    pub fn new(mount_options: MountOptions) -> Self {
        let (sender, receiver) = unbounded();
        let (notification_sender, notification_receiver) = unbounded();

        Self {
            fuse_connection: None,
            filesystem: None,
            response_senders: vec![sender],
            response_receivers: vec![Some(receiver)],
            notification_sender,
            notification_receiver: Some(notification_receiver),
            mount_options,
            kernel_writeback_permissions: false,
            // default to legacy behaviour (no explicit pool)
            worker_count: 0,
            max_background: DEFAULT_MAX_BACKGROUND as usize,
            _per_inode_serial: false,
            workers: None,
            readonly_open_plan: std::sync::Mutex::new(None),
            inflight: Arc::new(AtomicUsize::new(0)),
            inflight_notify: Arc::new(async_notify::Notify::new()),
            readonly_reply_tracker: None,
        }
    }

    /// Configure worker pool parameters before mount (builder-style).
    ///
    /// # Arguments
    ///
    /// * `worker_count` - Number of worker tasks. Use 0 or 1 for legacy single-threaded behavior.
    ///   Default is 0 (legacy mode). Recommended: number of CPU cores for I/O-bound workloads.
    /// * `max_background` - Maximum number of in-flight requests before applying backpressure.
    ///   Default is 12 (same as libfuse). Increase for high-throughput scenarios.
    ///
    /// # Notes
    ///
    /// Similar to libfuse's `fuse_loop_config`, this controls the concurrency model:
    /// - FORGET/BATCH_FORGET messages are NOT counted toward `max_background` to prevent
    ///   thread explosion during large directory deletions (matching libfuse behavior).
    /// - Backpressure is applied when `inflight >= max_background`, blocking new requests
    ///   until some complete.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let session = Session::new(mount_options)
    ///     .with_workers(4, 64);  // 4 workers, up to 64 in-flight requests
    /// ```
    pub fn with_workers(mut self, worker_count: usize, max_background: usize) -> Self {
        self.worker_count = worker_count;
        self.max_background = max_background.max(1); // avoid zero
        self
    }

    fn response_sender(&self) -> &UnboundedSender<FuseData> {
        &self.response_senders[0]
    }

    fn ensure_readonly_reply_tracker(&mut self, fs: &FS) -> IoResult<()> {
        if fs.supports_read_cancellation() && self.readonly_reply_tracker.is_none() {
            // Admit BEFORE constructing tracker Arc, map, and future Box.
            let roots = fs
                .reserve_input_buffer_memory(PRE_UNMOUNT_MEMORY_BYTES)
                .map_err(IoError::from)?;
            self.readonly_reply_tracker = Some(ReplyTracker::new(
                roots,
                self.inflight.clone(),
                self.inflight_notify.clone(),
            ));
        }
        Ok(())
    }

    fn prepare_pre_unmount(
        &mut self,
        fs: &Arc<FS>,
    ) -> IoResult<(
        Option<Pin<Box<dyn Future<Output = IoResult<()>> + Send + 'static>>>,
        Option<crate::raw::reply::ReplyMemoryGuard>,
    )> {
        self.ensure_readonly_reply_tracker(fs.as_ref())?;
        let Some(tracker) = self.readonly_reply_tracker.clone() else {
            return Ok((None, None));
        };
        let child = owned_prepare::preparation_child(fs.clone(), tracker.clone());
        let child = owned_prepare::OwnedPrepare::new(fs.as_ref(), child)?;
        let prepare = owned_prepare::preparation_future(child, tracker);
        // Concrete FS futures may differ in size. Fail before Box allocation
        // if this future exceeds its part of the existing fixed Roots charge.
        if std::mem::size_of_val(&prepare)
            + reply_tracker::TRACKER_ALLOCATION_BOUND
            + reply_tracker::PREPARATION_OVERHEAD_BOUND
            > PRE_UNMOUNT_MEMORY_BYTES as usize
        {
            return Err(IoError::from_raw_os_error(libc::ENOMEM));
        }
        Ok((Some(Box::pin(prepare)), None))
    }

    fn preflight_readonly_workers(&mut self, fs: &FS) -> IoResult<()> {
        if !fs.supports_read_cancellation() || self.workers.is_some() {
            return Ok(());
        }
        // Readonly dispatch already promotes configured workers0 to worker1.
        // Do the same before any connection/helper/native/physical startup.
        self.worker_count = self.worker_count.max(1);
        let plan = self
            .readonly_open_plan
            .get_mut()
            .unwrap_or_else(|e| e.into_inner());
        if plan.is_none() {
            *plan =
                owned_open_queue::OpenLanePlan::prepare(fs, self.worker_count, self.max_background)
                    .map_err(IoError::from)?;
        }
        Ok(())
    }

    fn ensure_workers(&mut self, fs: Arc<FS>) -> IoResult<()> {
        self.ensure_readonly_reply_tracker(fs.as_ref())?;
        if self.worker_count > 0 && self.workers.is_none() {
            // Plan/controller/lane buffers are all admitted before worker spawn.
            let open_plan = match self
                .readonly_open_plan
                .get_mut()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                Some(plan) => Some(plan),
                None => owned_open_queue::OpenLanePlan::prepare(
                    fs.as_ref(),
                    self.worker_count,
                    self.max_background,
                )
                .map_err(IoError::from)?,
            };
            let open_controller = open_plan
                .as_ref()
                .map(|plan| plan.controller())
                .unwrap_or_else(owned_open_queue::ControllerHandle::empty);
            let ctx = Arc::new(DispatchCtx {
                fs,
                resp: self.response_senders.clone(),
                notifications: self.notification_sender.clone(),
                direct_io: self.mount_options.direct_io,
                force_readdir_plus: self.mount_options.force_readdir_plus,
                kernel_writeback_permissions: self.kernel_writeback_permissions,
                _inflight: self.inflight.clone(),
                _inflight_notify: self.inflight_notify.clone(),
                response_memory: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
                readonly_reply_tracker: self.readonly_reply_tracker.clone(),
                open_controller,
            });
            let workers = Workers::new(self.worker_count, self.max_background, ctx, open_plan)
                .map_err(IoError::from)?;
            self.workers = Some(workers);
            debug!(
                workers = self.worker_count,
                queue = self.max_background,
                "worker pool initialized"
            );
        }
        Ok(())
    }

    /// get a [`notify`].
    ///
    /// [`notify`]: Notify
    fn get_notify(&self) -> Notify {
        Notify::new(
            self.response_senders[0].clone(),
            self.notification_sender.clone(),
        )
    }
}

#[cfg(any(
    feature = "async-io-runtime",
    feature = "tokio-runtime",
    feature = "io-uring-runtime"
))]
impl<FS: Filesystem + Send + Sync + 'static> Session<FS> {
    async fn mount_empty_check(&self, mount_path: &Path) -> IoResult<()> {
        use std::io::ErrorKind;

        #[cfg(all(not(feature = "async-io-runtime"), feature = "tokio-runtime"))]
        if !self.mount_options.nonempty
            && matches!(read_dir(mount_path).await?.next_entry().await, Ok(Some(_)))
        {
            return Err(IoError::new(
                ErrorKind::AlreadyExists,
                "mount point is not empty",
            ));
        }

        #[cfg(all(not(feature = "tokio-runtime"), feature = "async-io-runtime"))]
        if !self.mount_options.nonempty && read_dir(mount_path).await?.next().await.is_some() {
            return Err(IoError::new(
                ErrorKind::AlreadyExists,
                "mount point is not empty",
            ));
        }

        Ok(())
    }

    /// mount the filesystem without root permission. This function will block
    /// until the filesystem is unmounted.
    // On FreeBSD, no special interface is required to mount unprivileged.
    // If vfs.usermount=1 and the user has access to the mountpoint, it will
    // just work.
    #[cfg(all(target_os = "freebsd", feature = "unprivileged"))]
    pub async fn mount_with_unprivileged<P: AsRef<Path>>(
        self,
        fs: FS,
        mount_path: P,
    ) -> IoResult<MountHandle> {
        self.mount(fs, mount_path).await
    }

    #[cfg(target_os = "macos")]
    pub async fn mount_with_unprivileged<P: AsRef<Path>>(
        mut self,
        fs: FS,
        mount_path: P,
    ) -> IoResult<MountHandle> {
        let mount_path = mount_path.as_ref();

        self.mount_empty_check(mount_path).await?;
        let fs = Arc::new(fs);
        let (pre_unmount, pre_unmount_memory) = self.prepare_pre_unmount(&fs)?;
        self.preflight_readonly_workers(fs.as_ref())?;

        #[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
        startup_preflight_tests::physical_boundary()?;
        let notify = Arc::new(async_notify::Notify::new());
        let fuse_connection = FuseConnection::new_with_unprivileged(
            self.mount_options.clone(),
            mount_path,
            notify.clone(),
        )
        .await?;

        self.fuse_connection.replace(Arc::new(fuse_connection));

        self.filesystem.replace(fs);

        let (ready_tx, ready_rx) = oneshot::channel();
        let task = task::spawn(self.inner_mount(Some(ready_tx)));
        if let Err(err) = wait_for_mount_init_ready(ready_rx).await {
            #[cfg(all(not(feature = "async-io-runtime"), feature = "tokio-runtime"))]
            task.abort();
            #[cfg(all(not(feature = "tokio-runtime"), feature = "async-io-runtime"))]
            let _ = task.cancel().await;
            return Err(err);
        }

        debug!("mount {:?} success", mount_path);

        Ok(MountHandle {
            inner: Some(MountHandleInner {
                task,
                mount_path: mount_path.to_path_buf(),
                destroy_notify: notify,
                pre_unmount,
                pre_unmount_memory,
            }),
        })
    }

    /// mount the filesystem without root permission.
    #[cfg(all(target_os = "linux", feature = "unprivileged"))]
    pub async fn mount_with_unprivileged<P: AsRef<Path>>(
        mut self,
        fs: FS,
        mount_path: P,
    ) -> IoResult<MountHandle> {
        let mount_path = mount_path.as_ref();

        self.mount_empty_check(mount_path).await?;
        let fs = Arc::new(fs);
        let (pre_unmount, pre_unmount_memory) = self.prepare_pre_unmount(&fs)?;
        self.preflight_readonly_workers(fs.as_ref())?;

        #[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
        startup_preflight_tests::physical_boundary()?;
        let notify = Arc::new(async_notify::Notify::new());
        let fuse_connection = FuseConnection::new_with_unprivileged(
            self.mount_options.clone(),
            mount_path,
            notify.clone(),
        )
        .await?;

        self.fuse_connection.replace(Arc::new(fuse_connection));

        self.filesystem.replace(fs);

        debug!("mount {:?} success", mount_path);

        Ok(MountHandle {
            inner: Some(MountHandleInner {
                task: task::spawn(self.inner_mount(None)),
                mount_path: mount_path.to_path_buf(),
                destroy_notify: notify,
                pre_unmount,
                pre_unmount_memory,
                unprivileged: true,
                #[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
                ordinary_unmount_for_test: None,
            }),
        })
    }

    /// mount the filesystem with root permission.
    #[cfg(target_os = "linux")]
    pub async fn mount<P: AsRef<Path>>(mut self, fs: FS, mount_path: P) -> IoResult<MountHandle> {
        let mount_path = mount_path.as_ref();

        self.mount_empty_check(mount_path).await?;
        let fs = Arc::new(fs);
        let (pre_unmount, pre_unmount_memory) = self.prepare_pre_unmount(&fs)?;
        self.preflight_readonly_workers(fs.as_ref())?;

        #[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
        startup_preflight_tests::physical_boundary()?;
        let notify = Arc::new(async_notify::Notify::new());
        let fuse_connection = FuseConnection::new(notify.clone())?;

        let fd = fuse_connection.as_fd().as_raw_fd();

        let options = self.mount_options.build(fd);

        let fs_name = if let Some(fs_name) = self.mount_options.fs_name.as_ref() {
            Some(fs_name.as_str())
        } else {
            Some("fuse")
        };

        debug!("mount options {:?}", options);

        if let Err(err) = mount::mount(
            fs_name,
            mount_path,
            Some("fuse"),
            self.mount_options.flags(),
            Some(options.as_os_str()),
        ) {
            error!("mount {:?} failed", mount_path);

            return Err(err.into());
        }

        self.fuse_connection.replace(Arc::new(fuse_connection));

        self.filesystem.replace(fs);

        debug!("mount {:?} success", mount_path);

        Ok(MountHandle {
            inner: Some(MountHandleInner {
                task: task::spawn(self.inner_mount(None)),
                mount_path: mount_path.to_path_buf(),
                destroy_notify: notify,
                pre_unmount,
                pre_unmount_memory,
                #[cfg(all(target_os = "linux", feature = "unprivileged"))]
                unprivileged: false,
                #[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
                ordinary_unmount_for_test: None,
            }),
        })
    }

    /// mount the filesystem
    #[cfg(target_os = "freebsd")]
    pub async fn mount<P: AsRef<Path>>(mut self, fs: FS, mount_path: P) -> IoResult<MountHandle> {
        let mount_path = mount_path.as_ref();

        self.mount_empty_check(mount_path).await?;
        let fs = Arc::new(fs);
        let (pre_unmount, pre_unmount_memory) = self.prepare_pre_unmount(&fs)?;
        self.preflight_readonly_workers(fs.as_ref())?;

        #[cfg(all(test, target_os = "linux", feature = "io-uring-runtime"))]
        startup_preflight_tests::physical_boundary()?;
        let notify = Arc::new(async_notify::Notify::new());
        let fuse_connection = FuseConnection::new(notify.clone())?;

        let fd = fuse_connection.as_fd().as_raw_fd();

        {
            let mut nmount = self.mount_options.build();
            nmount
                .str_opt_owned(c"fspath", mount_path)
                .str_opt_owned(c"fd", format!("{}", fd).as_str());
            debug!("mount options {:?}", &nmount);

            if let Err(err) = nmount.nmount(self.mount_options.flags()) {
                error!("mount {} failed: {}", mount_path.display(), err);

                return Err(std::io::Error::from(err));
            }
        }

        self.fuse_connection.replace(Arc::new(fuse_connection));

        self.filesystem.replace(fs);

        debug!("mount {:?} success", mount_path);

        Ok(MountHandle {
            inner: Some(MountHandleInner {
                task: task::spawn(self.inner_mount(None)),
                mount_path: mount_path.to_path_buf(),
                destroy_notify: notify,
                pre_unmount,
                pre_unmount_memory,
            }),
        })
    }

    #[cfg(target_os = "macos")]
    pub async fn mount<P: AsRef<Path>>(self, fs: FS, mount_path: P) -> IoResult<MountHandle> {
        self.mount_with_unprivileged(fs, mount_path).await
    }

    async fn inner_mount(
        mut self,
        ready_sender: Option<oneshot::Sender<IoResult<()>>>,
    ) -> IoResult<()> {
        let teardown_diag = std::env::var("BREWFS_FUSE_TEARDOWN_DIAG").as_deref() == Ok("1");
        let fs = self
            .filesystem
            .as_ref()
            .expect("filesystem not init")
            .clone();
        self.ensure_readonly_reply_tracker(fs.as_ref())?;
        let fuse_write_connection = self.fuse_connection.as_ref().unwrap().clone();
        let reply_count = if self.worker_count > 1 {
            self.worker_count
        } else {
            1
        };

        if reply_count > self.response_senders.len() {
            for _ in self.response_senders.len()..reply_count {
                let (tx, rx) = unbounded();
                self.response_senders.push(tx);
                self.response_receivers.push(Some(rx));
            }
        }

        let rx_vec: Vec<UnboundedReceiver<FuseData>> = self
            .response_receivers
            .iter_mut()
            .map(|r| r.take().unwrap())
            .collect();

        // Invalidation can wait for kernel locks held by a pending request.
        // Keep its writes separate from the tasks that deliver request replies.
        let notification_connection = Arc::new(fuse_write_connection.try_clone()?);
        let notification_receiver = self.notification_receiver.take().unwrap();
        #[cfg(all(
            not(feature = "tokio-runtime"),
            not(feature = "io-uring-runtime"),
            feature = "async-io-runtime"
        ))]
        task::spawn(Self::notify_fuse(
            notification_connection,
            notification_receiver,
        ))
        .detach();
        #[cfg(any(
            all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
            feature = "io-uring-runtime"
        ))]
        task::spawn(Self::notify_fuse(
            notification_connection,
            notification_receiver,
        ));

        // Spawn reply tasks independently — they live for the lifetime of the
        // runtime and stop naturally when their channels close. We intentionally
        // do NOT use select_all here: if any reply task exits early (e.g. due to
        // a transient /dev/fuse write error), the entire mount would be torn down,
        // potentially losing replies for in-flight FUSE requests and causing the
        // kernel to hang in wait_sb_inodes.
        for (i, rx) in rx_vec.into_iter().enumerate() {
            // Each reply task gets its own cloned connection to avoid deadlock:
            // with io_uring, sharing the dispatch connection's ring thread between
            // reads and writes causes the ring to block in submit_and_wait while
            // write requests queue up unsent.
            let conn = Arc::new(fuse_write_connection.try_clone()?);
            let tracker = self.readonly_reply_tracker.clone();

            #[cfg(all(
                not(feature = "tokio-runtime"),
                not(feature = "io-uring-runtime"),
                feature = "async-io-runtime"
            ))]
            task::spawn(async move {
                if let Err(e) = Self::reply_fuse(conn, rx, tracker).await {
                    tracing::error!("reply fuse task {i} exited: {e}");
                }
            })
            .detach();

            #[cfg(any(
                all(not(feature = "async-io-runtime"), feature = "tokio-runtime"),
                feature = "io-uring-runtime"
            ))]
            task::spawn(async move {
                if let Err(e) = Self::reply_fuse(conn, rx, tracker).await {
                    tracing::error!("reply fuse task {i} exited: {e}");
                }
            });
        }

        let result = self.dispatch(ready_sender).await;
        self.notification_sender.close_channel();
        if teardown_diag {
            tracing::info!(target: "asyncfuse::teardown", event="dispatch_returned",
                ok=result.is_ok(), inflight=self.inflight.load(Ordering::Acquire));
        }
        result
    }

    async fn notify_fuse(
        fuse_connection: Arc<FuseConnection>,
        mut receiver: UnboundedReceiver<Notification>,
    ) {
        while let Some(notification) = receiver.next().await {
            let (mut data, mut body) = match notification.data {
                Either::Left(data) => (crate::raw::reply::ReplyBytes::from(data), None),
                Either::Right((data, body)) => {
                    (crate::raw::reply::ReplyBytes::from(data), Some(body))
                }
            };
            let actual_len = data.len() + body.as_ref().map_or(0, |body| body.len());
            let result = loop {
                let ((ret_data, ret_body), result) =
                    fuse_connection.write_vectored(data, body).await;
                match result {
                    Ok(written) if written == actual_len => break Ok(()),
                    Ok(_) => break Err(IoError::from(std::io::ErrorKind::WriteZero)),
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                        data = ret_data;
                        body = ret_body;
                    }
                    Err(error) => break Err(error),
                }
            };
            // A canceled caller must not keep the writer alive or affect
            // subsequent notifications.
            let _ = notification.completion.send(result);
        }
    }

    async fn reply_fuse(
        fuse_connection: Arc<FuseConnection>,
        mut response_receiver: UnboundedReceiver<FuseData>,
        tracker: Option<Arc<ReplyTracker>>,
    ) -> IoResult<()> {
        let mut pump = ReplyPumpLifetime::new(tracker.clone());
        loop {
            #[cfg(all(target_os = "linux", feature = "io-uring-runtime"))]
            let response = tokio::select! {
                response = response_receiver.next() => response,
                error = fuse_connection.wait_ring_failure() => {
                    pump.fail(&error);
                    return Err(error);
                },
            };
            #[cfg(not(all(target_os = "linux", feature = "io-uring-runtime")))]
            let response = response_receiver.next().await;
            let Some(response) = response else {
                break;
            };
            let (mut data, mut extend_data) = match response {
                Either::Left(data) => (crate::raw::reply::ReplyBytes::from(data), None),
                Either::Right((data, extend_data)) => {
                    (crate::raw::reply::ReplyBytes::from(data), Some(extend_data))
                }
            };
            let extend_len = extend_data.as_ref().map(|v| v.len()).unwrap_or(0);
            let actual_len = data.len().checked_add(extend_len).ok_or_else(|| {
                let error = IoError::from(std::io::ErrorKind::InvalidData);
                pump.fail(&error);
                error
            })?;
            let ticket = if let Some(tracker) = &tracker {
                let (declared_len, _, unique) =
                    reply_tracker::wire_header(&data, extend_data.as_deref()).map_err(|error| {
                        pump.fail(&error);
                        error
                    })?;
                if declared_len as usize != actual_len {
                    let error = IoError::from(std::io::ErrorKind::InvalidData);
                    pump.fail(&error);
                    return Err(error);
                }
                if unique == 0 {
                    None // Unsolicited notification, not a request reply.
                } else {
                    Some(tracker.ticket(unique).ok_or_else(|| {
                        let error = IoError::from(std::io::ErrorKind::InvalidData);
                        pump.fail(&error);
                        error
                    })?)
                }
            } else {
                None
            };
            if data.len() >= FUSE_OUT_HEADER_SIZE {
                let actual_len = data.len() + extend_len;
                let header_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
                if header_len != actual_len {
                    let len_bytes = (actual_len as u32).to_le_bytes();
                    let mut v = data.to_vec();
                    v[0..4].copy_from_slice(&len_bytes);
                    data = crate::raw::reply::ReplyBytes::from(v);
                    warn!(
                        header_len,
                        actual_len, "adjusted fuse reply length to match payload"
                    );
                }
            }
            let reply_header = if data.len() >= FUSE_OUT_HEADER_SIZE {
                let len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                let err_code = i32::from_le_bytes([data[4], data[5], data[6], data[7]]);
                let unique = u64::from_le_bytes([
                    data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
                ]);
                Some((len, err_code, unique))
            } else {
                None
            };

            // Retry loop: keep writing this reply until it succeeds, the kernel
            // forgets the request (NotFound), or the channel is closed.
            let mut retry_delay_us: u64 = 100; // start at 100μs
            loop {
                let ((ret_data, ret_ext), result) =
                    fuse_connection.write_vectored(data, extend_data).await;
                match result {
                    Ok(written) => {
                        if tracker.is_some() && written != actual_len {
                            let error = IoError::from(std::io::ErrorKind::WriteZero);
                            pump.fail(&error);
                            return Err(error);
                        }
                        if let Some(ticket) = &ticket {
                            ticket.written();
                        }
                        break;
                    }
                    Err(err) => {
                        #[cfg(all(target_os = "linux", feature = "io-uring-runtime"))]
                        if let Some(error) = fuse_connection.ring_failure() {
                            pump.fail(&error);
                            return Err(error);
                        }
                        data = ret_data;
                        extend_data = ret_ext;
                        use std::io::ErrorKind;
                        if err.kind() == ErrorKind::NotFound {
                            if tracker.is_some() && err.raw_os_error() != Some(libc::ENOENT) {
                                pump.fail(&err);
                                return Err(err);
                            }
                            if let Some(ticket) = &ticket {
                                ticket.kernel_forgot();
                            }
                            warn!(
                                "may reply interrupted fuse request, ignore this error {}",
                                err
                            );
                            break;
                        }
                        if tracker.is_some()
                            && !matches!(err.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock)
                        {
                            // Readonly terminal failures are sticky. Mutable
                            // retries keep their existing behavior unchanged.
                            pump.fail(&err);
                            return Err(err);
                        }
                        if let Some((len, err_code, unique)) = reply_header {
                            warn!(
                                error = %err,
                                reply_len = len,
                                reply_error = err_code,
                                unique,
                                "reply fuse write error, retrying"
                            );
                        } else {
                            warn!("reply fuse write error, retrying: {}", err);
                        }
                        #[cfg(all(not(feature = "async-io-runtime"), feature = "tokio-runtime"))]
                        tokio::time::sleep(std::time::Duration::from_micros(retry_delay_us)).await;
                        #[cfg(all(not(feature = "tokio-runtime"), feature = "async-io-runtime"))]
                        async_io::Timer::after(std::time::Duration::from_micros(retry_delay_us))
                            .await;
                        // Exponential backoff capped at 10ms
                        retry_delay_us = (retry_delay_us * 2).min(10_000);
                    }
                }
            }
        }

        pump.completed();
        Ok(())
    }

    #[instrument(level = "debug", skip(self, fs), ret, err)]
    async fn init_filesystem(
        &mut self,
        fs: &FS,
        fuse_connection: &FuseConnection,
    ) -> IoResult<NonZeroU32> {
        use std::io::ErrorKind;

        let guard = fs
            .reserve_input_buffer_memory(
                (FUSE_MIN_READ_BUFFER_SIZE + FUSE_IN_HEADER_SIZE + 512) as u64,
            )
            .map_err(IoError::from)?;
        let header_buffer = vec![0; FUSE_IN_HEADER_SIZE];
        let data_buffer = AlignedBuffer::try_new_owned(FUSE_MIN_READ_BUFFER_SIZE, guard)
            .map_err(IoError::other)?;

        let (data_buffer, in_header) = match self
            .read_fuse_request(fuse_connection, header_buffer, data_buffer)
            .await
        {
            ReadResult::Fatal(error) => return Err(error),
            ReadResult::Destroy => {
                return Err(IoError::new(
                    ErrorKind::UnexpectedEof,
                    "init stage get destroy result",
                ));
            }

            ReadResult::Request {
                in_header,
                data_buffer,
                ..
            } => {
                let in_header = in_header?;
                (data_buffer, in_header)
            }
        };

        let request = Request::from(&in_header);

        let opcode = match fuse_opcode::try_from(in_header.opcode) {
            Err(err) => {
                debug!("receive unknown opcode {}", err.0);

                reply_error_in_place(libc::ENOSYS.into(), request, self.response_sender()).await;

                return Err(IoError::other(format!("receive unknown opcode {}", err.0)));
            }

            Ok(opcode) => opcode,
        };

        debug!("receive opcode {}", opcode);

        if opcode != fuse_opcode::FUSE_INIT {
            error!(?opcode, "received unexpected opcode");

            return Err(IoError::other(format!("unexpected opcode {opcode:?}")));
        }

        let data_size = in_header.len as usize - FUSE_IN_HEADER_SIZE;
        let data_ref = &data_buffer[..data_size];

        self.handle_init(request, data_ref, fuse_connection, fs)
            .await
    }

    #[instrument(level = "debug", skip(self, header_buffer, data_buffer), ret)]
    async fn read_fuse_request(
        &mut self,
        fuse_connection: &FuseConnection,
        mut header_buffer: Vec<u8>,
        mut data_buffer: AlignedBuffer,
    ) -> ReadResult {
        let res = match fuse_connection
            .read_vectored(header_buffer, data_buffer)
            .await
        {
            None => {
                #[cfg(all(target_os = "linux", feature = "io-uring-runtime"))]
                if let Some(error) = fuse_connection.ring_failure() {
                    return ReadResult::Fatal(error);
                }
                return ReadResult::Destroy;
            }

            Some(((header_buf, data_buf), res)) => {
                header_buffer = header_buf;
                data_buffer = data_buf;

                res
            }
        };
        let n = match res {
            Err(err) => {
                #[cfg(all(target_os = "linux", feature = "io-uring-runtime"))]
                if let Some(error) = fuse_connection.ring_failure() {
                    return ReadResult::Fatal(error);
                }
                if let Some(errno) = err.raw_os_error() {
                    if errno == libc::ENODEV {
                        debug!("read from /dev/fuse failed with ENODEV");

                        return ReadResult::Destroy;
                    }
                }

                error!("read from /dev/fuse failed {}", err);

                return ReadResult::Request {
                    in_header: Err(err),
                    header_buffer,
                    data_buffer,
                };
            }

            Ok(n) => n,
        };

        debug!(n, "read fuse request done");

        if n < FUSE_IN_HEADER_SIZE {
            error!(
                n,
                FUSE_IN_HEADER_SIZE, "read_vectored n is less then FUSE_IN_HEADER_SIZE"
            );

            return ReadResult::Request {
                in_header: Err(IoError::other(
                    "read_vectored n is less then FUSE_IN_HEADER_SIZE",
                )),
                header_buffer,
                data_buffer,
            };
        }

        let in_header = match get_bincode_config().deserialize::<fuse_in_header>(&header_buffer) {
            Err(err) => {
                error!("deserialize fuse_in_header failed {}", err);

                return ReadResult::Request {
                    in_header: Err(IoError::other(err)),
                    header_buffer,
                    data_buffer,
                };
            }

            Ok(in_header) => in_header,
        };

        ReadResult::Request {
            in_header: Ok(in_header),
            header_buffer,
            data_buffer,
        }
    }

    async fn dispatch(
        &mut self,
        ready_sender: Option<oneshot::Sender<IoResult<()>>>,
    ) -> IoResult<()> {
        let readonly = self
            .filesystem
            .as_ref()
            .filter(|fs| fs.supports_read_cancellation())
            .cloned();
        let result = self.dispatch_inner(ready_sender).await;
        if let Some(fs) = readonly {
            // Cancel adapter reads first, allowing their temporary close to
            // finish normally. This does not retire published kernel memory.
            fs.destroy(Request {
                unique: 0,
                uid: 0,
                gid: 0,
                pid: 0,
            })
            .await;
        }
        // Every worker owns its queue and any spawned ordinary handlers. Join
        // the original worker handles before the Session task is considered
        // complete, regardless of whether the filesystem uses readonly
        // cancellation lanes. This closes the ordinary-worker detached-task
        // hole while preserving the readonly destroy ordering above.
        let worker_join_error = if let Some(mut workers) = self.workers.take() {
            workers.shutdown().await
        } else {
            None
        };
        match (result, worker_join_error) {
            // The dispatch/prepare/unmount result is primary. Preserve it
            // while retaining a secondary worker JoinError in diagnostics.
            (Err(primary), Some(secondary)) => {
                tracing::error!(
                    target = "asyncfuse::teardown",
                    event = "secondary_worker_join_error",
                    error = %secondary
                );
                Err(primary)
            }
            (Ok(()), Some(secondary)) => Err(secondary),
            (result, None) => result,
        }
    }

    async fn dispatch_inner(
        &mut self,
        mut ready_sender: Option<oneshot::Sender<IoResult<()>>>,
    ) -> IoResult<()> {
        let teardown_diag = std::env::var("BREWFS_FUSE_TEARDOWN_DIAG").as_deref() == Ok("1");
        if teardown_diag {
            tracing::info!(target: "asyncfuse::teardown", event="dispatch_started",
                workers=self.worker_count, max_background=self.max_background);
        }
        let fuse_connection = self.fuse_connection.take().unwrap();
        let fs = self.filesystem.take().expect("filesystem not init");
        // defer worker initialization until after FUSE INIT handshake

        let max_write = match self.init_filesystem(&fs, &fuse_connection).await {
            Ok(max_write) => {
                if let Some(sender) = ready_sender.take() {
                    let _ = sender.send(Ok(()));
                }
                max_write.get() as usize
            }
            Err(err) => {
                if let Some(sender) = ready_sender.take() {
                    let return_err = if let Some(raw_os_error) = err.raw_os_error() {
                        IoError::from_raw_os_error(raw_os_error)
                    } else {
                        IoError::new(err.kind(), err.to_string())
                    };
                    let _ = sender.send(Err(err));
                    return Err(return_err);
                }
                return Err(err);
            }
        };
        let workers_active = self.worker_count > 0 || fs.supports_read_cancellation();
        if workers_active {
            self.worker_count = self.worker_count.max(1);
            self.ensure_workers(fs.clone())?;
        }
        let buffer_size = (max_write + FUSE_WRITE_IN_SIZE).max(FUSE_MIN_READ_BUFFER_SIZE);
        debug!(buffer_size, "buffer size calculated");

        // Create buffers for main loop (reused each iteration in legacy mode)
        let guard = fs
            .reserve_input_buffer_memory((buffer_size + FUSE_IN_HEADER_SIZE + 512) as u64)
            .map_err(IoError::from)?;
        let mut header_buffer = vec![0; FUSE_IN_HEADER_SIZE];
        let mut data_buffer =
            AlignedBuffer::try_new_owned(buffer_size, guard).map_err(IoError::other)?;
        #[cfg(all(
            feature = "buffer-pool",
            not(feature = "async-io-runtime"),
            feature = "tokio-runtime"
        ))]
        let request_buffer_pool = {
            let pool_capacity = self.worker_count.saturating_mul(2).clamp(16, 128);
            let pool_guard = fs
                .reserve_input_buffer_memory(
                    (4096 + pool_capacity * std::mem::size_of::<AlignedBuffer>()) as u64,
                )
                .map_err(IoError::from)?;
            let owned_fs = fs.clone();
            let allocator: crate::raw::buffer_pool::InputMemoryAllocator = Arc::new(move |bytes| {
                owned_fs
                    .reserve_input_buffer_memory(bytes + 512)
                    .map_err(IoError::from)
            });
            Arc::new(BufferPool::with_capacity_owned(
                buffer_size,
                pool_capacity,
                Some(allocator),
                pool_guard,
            ))
        };

        loop {
            if workers_active {
                let waited = !fs.supports_read_cancellation()
                    && self.inflight.load(Ordering::Acquire) >= self.max_background;
                if teardown_diag && waited {
                    tracing::info!(target: "asyncfuse::teardown", event="backpressure_enter",
                        inflight=self.inflight.load(Ordering::Acquire), max_background=self.max_background);
                }
                self.wait_before_request_read(&fs, &fuse_connection).await?;
                if teardown_diag && waited {
                    tracing::info!(target: "asyncfuse::teardown", event="backpressure_exit",
                        inflight=self.inflight.load(Ordering::Acquire));
                }
            }
            let in_header = match self
                .read_fuse_request(&fuse_connection, header_buffer, data_buffer)
                .await
            {
                ReadResult::Fatal(error) => return Err(error),
                ReadResult::Destroy => {
                    if teardown_diag {
                        tracing::info!(target: "asyncfuse::teardown", event="read_destroy",
                            inflight=self.inflight.load(Ordering::Acquire));
                    }
                    fs.destroy(Request {
                        unique: 0,
                        uid: 0,
                        gid: 0,
                        pid: 0,
                    })
                    .await;

                    return Ok(());
                }

                ReadResult::Request {
                    in_header,
                    header_buffer: header_buf,
                    data_buffer: data_buf,
                } => {
                    header_buffer = header_buf;
                    data_buffer = data_buf;

                    match in_header {
                        Err(_) => continue,

                        Ok(in_header) => in_header,
                    }
                }
            };

            let request = Request::from(&in_header);

            let opcode = match fuse_opcode::try_from(in_header.opcode) {
                Err(err) => {
                    debug!("receive unknown opcode {}", err.0);

                    self.reject_ordinary_request(request, libc::ENOSYS.into(), &fs)
                        .await?;

                    continue;
                }

                Ok(opcode) => opcode,
            };

            debug!(unique = request.unique, opcode = %opcode, "receive opcode");

            let data_size = in_header.len as usize - FUSE_IN_HEADER_SIZE;
            let data_ref = &data_buffer[..data_size];

            if self.workers.is_some() {
                match opcode {
                    fuse_opcode::FUSE_INIT => {
                        warn!("duplicated fuse init request");
                        self.handle_init(request, data_ref, &fuse_connection, &fs)
                            .await?;
                        continue;
                    }
                    fuse_opcode::FUSE_DESTROY => {
                        if teardown_diag {
                            tracing::info!(target: "asyncfuse::teardown", event="kernel_destroy",
                                inflight=self.inflight.load(Ordering::Acquire));
                        }
                        debug!("receive fuse destroy");
                        fs.destroy(request).await;
                        debug!("fuse destroyed");
                        return Ok(());
                    }
                    fuse_opcode::FUSE_FORGET => {
                        self.handle_forget(request, in_header, data_ref, &fs).await;
                        continue;
                    }
                    fuse_opcode::FUSE_INTERRUPT => {
                        self.handle_interrupt(request, data_ref, &fs).await?;
                        continue;
                    }
                    fuse_opcode::FUSE_NOTIFY_REPLY => {
                        self.handle_notify_reply(request, in_header, data_ref, &fs)
                            .await;
                        continue;
                    }
                    fuse_opcode::FUSE_BATCH_FORGET => {
                        self.handle_batch_forget(request, in_header, data_ref, &fs)
                            .await;
                        continue;
                    }
                    _ => {}
                }

                // Keep reading control opcodes when readonly requests fill
                // admission. Waiting before read would hide FUSE_INTERRUPT
                // behind the very reads that it must cancel. Reject excess
                // ordinary work explicitly instead of creating an unbounded
                // pending queue. Control messages above remain exempt.
                let readonly_close =
                    fs.supports_read_cancellation() && worker::is_close_opcode(in_header.opcode);
                if fs.supports_read_cancellation()
                    && !readonly_close
                    && self.inflight.load(Ordering::Acquire) >= self.max_background
                {
                    self.reject_ordinary_request(request, libc::ENOMEM.into(), &fs)
                        .await?;
                    continue;
                }
                let unique = request.unique;
                let opcode_raw = in_header.opcode;
                // Close operations must make progress while READ admission is
                // full. They use bounded Control ownership through their last
                // reply reference, including the actual compact handler future.
                // Exhaustion stops dispatch before any close adapter side effect.
                let memory_guard = if readonly_close {
                    let workers = self.workers.as_ref().expect("workers checked above");
                    fs.reserve_control_memory(
                        workers
                            .close_request_memory_bytes(data_ref.len() as u64)
                            .map_err(IoError::from)?,
                    )
                    .map_err(IoError::from)?
                } else {
                    match fs.reserve_request_memory(data_ref.len() as u64) {
                        Ok(guard) => guard,
                        Err(error) => {
                            self.reject_ordinary_request(request, error, &fs).await?;
                            continue;
                        }
                    }
                };
                #[cfg(not(all(
                    feature = "buffer-pool",
                    not(feature = "async-io-runtime"),
                    feature = "tokio-runtime"
                )))]
                let body_bytes = Bytes::copy_from_slice(data_ref);
                #[cfg(all(
                    feature = "buffer-pool",
                    not(feature = "async-io-runtime"),
                    feature = "tokio-runtime"
                ))]
                let body_bytes = if opcode == fuse_opcode::FUSE_WRITE {
                    let Some(body) = self
                        .acquire_pooled_write_body(
                            request,
                            &fs,
                            &request_buffer_pool,
                            &mut data_buffer,
                            data_size,
                            &memory_guard,
                        )
                        .await?
                    else {
                        continue;
                    };
                    body
                } else {
                    Bytes::copy_from_slice(data_ref)
                };

                let lite = InHeaderLite {
                    nodeid: in_header.nodeid,
                    uid: in_header.uid,
                    gid: in_header.gid,
                    pid: in_header.pid,
                };
                // FORGET messages don't count toward inflight limit to prevent thread explosion
                // during large directory deletions (see libfuse fuse_loop_mt.c)
                let inflight_guard = if is_forget_opcode(opcode_raw) {
                    None
                } else {
                    Some(InflightGuard::new(
                        self.inflight.clone(),
                        self.inflight_notify.clone(),
                    ))
                };
                let workers = self.workers.as_ref().expect("workers checked above");
                workers.submit(WorkItem {
                    unique,
                    opcode: opcode_raw,
                    in_header: lite,
                    data: body_bytes,
                    _inflight_guard: inflight_guard,
                    _memory_guard: memory_guard,
                    _response_registration: None,
                });
            } else {
                // Will concurrency in a single-threaded context cause disorder in the sequence of operations on a single file?
                match opcode {
                    fuse_opcode::FUSE_INIT => {
                        warn!("duplicated fuse init request");

                        self.handle_init(request, data_ref, &fuse_connection, &fs)
                            .await?;
                    }

                    fuse_opcode::FUSE_DESTROY => {
                        if teardown_diag {
                            tracing::info!(target: "asyncfuse::teardown", event="kernel_destroy",
                                inflight=self.inflight.load(Ordering::Acquire));
                        }
                        debug!("receive fuse destroy");

                        fs.destroy(request).await;

                        debug!("fuse destroyed");

                        return Ok(());
                    }

                    fuse_opcode::FUSE_LOOKUP => {
                        if !workers_active {
                            self.handle_lookup(request, in_header, data_ref, &fs).await;
                        }
                    }

                    fuse_opcode::FUSE_FORGET => {
                        self.handle_forget(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_GETATTR => {
                        if !workers_active {
                            self.handle_getattr(request, in_header, data_ref, &fs).await;
                        }
                    }

                    fuse_opcode::FUSE_SETATTR => {
                        self.handle_setattr(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_READLINK => {
                        self.handle_readlink(request, in_header, &fs).await;
                    }

                    fuse_opcode::FUSE_SYMLINK => {
                        self.handle_symlink(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_MKNOD => {
                        self.handle_mknod(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_MKDIR => {
                        self.handle_mkdir(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_UNLINK => {
                        self.handle_unlink(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_RMDIR => {
                        self.handle_rmdir(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_RENAME => {
                        self.handle_rename(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_LINK => {
                        self.handle_link(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_OPEN => {
                        if !workers_active {
                            self.handle_open(request, in_header, data_ref, &fs).await;
                        }
                    }

                    fuse_opcode::FUSE_READ => {
                        if !workers_active {
                            self.handle_read(request, in_header, data_ref, &fs).await;
                        }
                    }

                    fuse_opcode::FUSE_WRITE => {
                        if !workers_active {
                            self.handle_write(request, in_header, data_ref, &fs).await;
                        }
                    }

                    fuse_opcode::FUSE_STATFS => {
                        self.handle_statfs(request, in_header, &fs).await;
                    }

                    fuse_opcode::FUSE_RELEASE => {
                        self.handle_release(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_FSYNC => {
                        self.handle_fsync(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_SETXATTR => {
                        self.handle_setxattr(request, in_header, data_ref, &fs)
                            .await;
                    }

                    fuse_opcode::FUSE_GETXATTR => {
                        self.handle_getxattr(request, in_header, data_ref, &fs)
                            .await;
                    }

                    fuse_opcode::FUSE_LISTXATTR => {
                        self.handle_listxattr(request, in_header, data_ref, &fs)
                            .await;
                    }

                    fuse_opcode::FUSE_REMOVEXATTR => {
                        self.handle_removexattr(request, in_header, data_ref, &fs)
                            .await;
                    }

                    fuse_opcode::FUSE_FLUSH => {
                        self.handle_flush(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_OPENDIR => {
                        self.handle_opendir(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_READDIR => {
                        if !workers_active {
                            self.handle_readdir(request, in_header, data_ref, &fs).await;
                        }
                    }

                    fuse_opcode::FUSE_RELEASEDIR => {
                        self.handle_releasedir(request, in_header, data_ref, &fs)
                            .await;
                    }

                    fuse_opcode::FUSE_FSYNCDIR => {
                        self.handle_fsyncdir(request, in_header, data_ref, &fs)
                            .await;
                    }

                    #[cfg(feature = "file-lock")]
                    fuse_opcode::FUSE_GETLK => {
                        self.handle_getlk(request, in_header, data_ref, &fs).await;
                    }

                    #[cfg(feature = "file-lock")]
                    fuse_opcode::FUSE_SETLK | fuse_opcode::FUSE_SETLKW => {
                        self.handle_setlk(
                            request,
                            in_header,
                            data_ref,
                            opcode == fuse_opcode::FUSE_SETLKW,
                            &fs,
                        )
                        .await;
                    }

                    fuse_opcode::FUSE_ACCESS => {
                        self.handle_access(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_CREATE => {
                        self.handle_create(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_INTERRUPT => {
                        self.handle_interrupt(request, data_ref, &fs).await?;
                    }

                    fuse_opcode::FUSE_BMAP => {
                        self.handle_bmap(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_IOCTL => {
                        self.handle_ioctl(request, in_header, data_ref, &fs).await;
                    }
                    fuse_opcode::FUSE_POLL => {
                        self.handle_poll(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_NOTIFY_REPLY => {
                        self.handle_notify_reply(request, in_header, data_ref, &fs)
                            .await;
                    }

                    fuse_opcode::FUSE_BATCH_FORGET => {
                        self.handle_batch_forget(request, in_header, data_ref, &fs)
                            .await;
                    }

                    fuse_opcode::FUSE_FALLOCATE => {
                        self.handle_fallocate(request, in_header, data_ref, &fs)
                            .await;
                    }

                    fuse_opcode::FUSE_READDIRPLUS => {
                        self.handle_readdirplus(request, in_header, data_ref, &fs)
                            .await;
                    }

                    fuse_opcode::FUSE_RENAME2 => {
                        self.handle_rename2(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_LSEEK => {
                        self.handle_lseek(request, in_header, data_ref, &fs).await;
                    }

                    fuse_opcode::FUSE_COPY_FILE_RANGE => {
                        self.handle_copy_file_range(request, in_header, data_ref, &fs)
                            .await;
                    }

                    #[cfg(target_os = "macos")]
                    fuse_opcode::FUSE_SETVOLNAME => {}

                    #[cfg(target_os = "macos")]
                    fuse_opcode::FUSE_GETXTIMES => {}

                    #[cfg(target_os = "macos")]
                    fuse_opcode::FUSE_EXCHANGE => {} // fuse_opcode::CUSE_INIT => {}
                }
            }
        }
    }

    async fn wait_for_request_capacity(&self, _connection: &FuseConnection) -> IoResult<()> {
        while self.inflight.load(Ordering::Acquire) >= self.max_background {
            #[cfg(all(target_os = "linux", feature = "io-uring-runtime"))]
            tokio::select! {
                _ = self.inflight_notify.notified() => {},
                error = _connection.wait_ring_failure() => return Err(error),
            }
            #[cfg(not(all(target_os = "linux", feature = "io-uring-runtime")))]
            self.inflight_notify.notified().await;
        }
        Ok(())
    }

    async fn wait_before_request_read(&self, fs: &FS, connection: &FuseConnection) -> IoResult<()> {
        if fs.supports_read_cancellation() {
            // Full ordinary admission must not suppress the kernel control
            // stream. Excess ordinary opcodes are rejected after read.
            Ok(())
        } else {
            self.wait_for_request_capacity(connection).await
        }
    }

    #[cfg(all(
        feature = "buffer-pool",
        not(feature = "async-io-runtime"),
        feature = "tokio-runtime"
    ))]
    async fn acquire_pooled_write_body(
        &mut self,
        request: Request,
        fs: &FS,
        pool: &Arc<BufferPool>,
        data_buffer: &mut AlignedBuffer,
        data_size: usize,
        memory_guard: &Option<crate::raw::reply::ReplyMemoryGuard>,
    ) -> IoResult<Option<Bytes>> {
        match pool.acquire().await {
            Ok(next_buffer) => {
                let request_buffer = std::mem::replace(data_buffer, next_buffer);
                Ok(Some(Bytes::from_owner(PooledRequestBuffer {
                    buffer: Some(request_buffer),
                    pool: pool.clone(),
                    len: data_size,
                })))
            }
            Err(_) => {
                // Request admission already covers these actual bytes plus
                // owner/allocation overhead. Preserve the reusable input
                // buffer when another full-size Roots buffer cannot fit.
                let mut copied = Vec::new();
                if copied.try_reserve_exact(data_size).is_err() {
                    self.reject_ordinary_request(request, libc::ENOMEM.into(), fs)
                        .await?;
                    return Ok(None);
                }
                copied.extend_from_slice(&data_buffer[..data_size]);
                Ok(Some(Bytes::from_owner(CopiedRequestBuffer {
                    data: copied,
                    // Payload clones can outlive WorkItem and its reply. The
                    // actual copied bytes retain the admitted request owner.
                    _memory_guard: memory_guard.clone(),
                })))
            }
        }
    }

    async fn reject_ordinary_request(
        &mut self,
        request: Request,
        error: crate::Errno,
        fs: &FS,
    ) -> IoResult<()> {
        self.ensure_readonly_reply_tracker(fs)?;
        if fs.supports_read_cancellation() {
            // If even a small owned refusal cannot fit, stop this readonly
            // session. Continuing would turn rejection into an unbounded queue.
            let owner = fs
                .reserve_control_memory(CONTROL_REPLY_MEMORY_BYTES)
                .map_err(IoError::from)?;
            self.send_control_reply(request, error, owner)?;
        } else {
            reply_error_in_place(error, request, self.response_sender()).await;
        }
        Ok(())
    }

    fn send_control_reply(
        &self,
        request: Request,
        error: crate::Errno,
        owner: Option<crate::raw::reply::ReplyMemoryGuard>,
    ) -> IoResult<()> {
        let owner = match &self.readonly_reply_tracker {
            Some(tracker) => Some(tracker.owner(
                request.unique,
                owner,
                InflightGuard::new(self.inflight.clone(), self.inflight_notify.clone()),
            ) as crate::raw::reply::ReplyMemoryGuard),
            None => owner,
        };
        let data = reply_error_in_worker(error, request.unique).map_err(IoError::other)?;
        let packet = match owner {
            Some(owner) => Either::Right((
                Vec::new(),
                crate::raw::reply::own_reply_buffer(data, Some(owner)),
            )),
            None => Either::Left(data),
        };
        self.response_sender().unbounded_send(packet).map_err(|_| {
            let error = IoError::from(std::io::ErrorKind::BrokenPipe);
            if let Some(tracker) = &self.readonly_reply_tracker {
                tracker.fail(&error);
            }
            error
        })
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_init(
        &mut self,
        request: Request,
        data: &[u8],
        fuse_connection: &FuseConnection,
        fs: &FS,
    ) -> IoResult<NonZeroU32> {
        let init_in = match get_bincode_config().deserialize::<fuse_init_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_init_in failed {}, request unique {}",
                    err, request.unique
                );

                let init_out_header = fuse_out_header {
                    len: FUSE_OUT_HEADER_SIZE as u32,
                    error: libc::EINVAL,
                    unique: request.unique,
                };

                let init_out_header_data = get_bincode_config()
                    .serialize(&init_out_header)
                    .expect("won't happened");

                if let Err(err) = fuse_connection
                    .write_vectored(
                        crate::raw::reply::ReplyBytes::from(init_out_header_data),
                        None::<crate::raw::reply::ReplyBytes>,
                    )
                    .await
                    .1
                {
                    error!("write error init out data to /dev/fuse failed {}", err);
                }

                return Err(IoError::from_raw_os_error(libc::EINVAL));
            }

            Ok(init_in) => init_in,
        };

        debug!("fuse_init {:?}", init_in);

        let mut reply_flags = 0;

        // TODO: most of these FUSE_* flags should be controllable by the consuming crate.
        if init_in.flags & FUSE_ASYNC_READ > 0 {
            debug!("enable FUSE_ASYNC_READ");

            reply_flags |= FUSE_ASYNC_READ;
        }

        #[cfg(feature = "file-lock")]
        if init_in.flags & FUSE_POSIX_LOCKS > 0 {
            debug!("enable FUSE_POSIX_LOCKS");

            reply_flags |= FUSE_POSIX_LOCKS;
        }

        if init_in.flags & FUSE_FILE_OPS > 0 {
            debug!("enable FUSE_FILE_OPS");

            reply_flags |= FUSE_FILE_OPS;
        }

        if init_in.flags & FUSE_ATOMIC_O_TRUNC > 0 {
            debug!("enable FUSE_ATOMIC_O_TRUNC");

            reply_flags |= FUSE_ATOMIC_O_TRUNC;
        }

        if init_in.flags & FUSE_EXPORT_SUPPORT > 0 {
            debug!("enable FUSE_EXPORT_SUPPORT");

            reply_flags |= FUSE_EXPORT_SUPPORT;
        }

        if init_in.flags & FUSE_BIG_WRITES > 0 {
            debug!("enable FUSE_BIG_WRITES");

            reply_flags |= FUSE_BIG_WRITES;
        }

        if init_in.flags & FUSE_DONT_MASK > 0 && self.mount_options.dont_mask {
            debug!("enable FUSE_DONT_MASK");

            reply_flags |= FUSE_DONT_MASK;
        }

        #[cfg(not(target_os = "macos"))]
        if init_in.flags & FUSE_SPLICE_WRITE > 0 {
            debug!("enable FUSE_SPLICE_WRITE");

            reply_flags |= FUSE_SPLICE_WRITE;
        }

        #[cfg(not(target_os = "macos"))]
        if init_in.flags & FUSE_SPLICE_MOVE > 0 {
            debug!("enable FUSE_SPLICE_MOVE");

            reply_flags |= FUSE_SPLICE_MOVE;
        }

        #[cfg(not(target_os = "macos"))]
        if init_in.flags & FUSE_SPLICE_READ > 0 {
            debug!("enable FUSE_SPLICE_READ");

            reply_flags |= FUSE_SPLICE_READ;
        }

        // posix lock used, maybe we don't need bsd lock
        /*if init_in.flags&FUSE_FLOCK_LOCKS>0 {
            reply_flags |= FUSE_FLOCK_LOCKS;
        }*/

        /*if init_in.flags & FUSE_HAS_IOCTL_DIR > 0 {
            debug!("enable FUSE_HAS_IOCTL_DIR");

            reply_flags |= FUSE_HAS_IOCTL_DIR;
        }*/

        if init_in.flags & FUSE_AUTO_INVAL_DATA > 0 {
            debug!("enable FUSE_AUTO_INVAL_DATA");

            reply_flags |= FUSE_AUTO_INVAL_DATA;
        }

        if init_in.flags & FUSE_DO_READDIRPLUS > 0 || self.mount_options.force_readdir_plus {
            debug!("enable FUSE_DO_READDIRPLUS");

            reply_flags |= FUSE_DO_READDIRPLUS;
        }

        if init_in.flags & FUSE_READDIRPLUS_AUTO > 0 && !self.mount_options.force_readdir_plus {
            debug!("enable FUSE_READDIRPLUS_AUTO");

            reply_flags |= FUSE_READDIRPLUS_AUTO;
        }

        if init_in.flags & FUSE_ASYNC_DIO > 0 && self.mount_options.async_dio {
            debug!("enable FUSE_ASYNC_DIO");

            reply_flags |= FUSE_ASYNC_DIO;
        }

        if init_in.flags & FUSE_WRITEBACK_CACHE > 0 && self.mount_options.write_back {
            debug!("enable FUSE_WRITEBACK_CACHE");

            reply_flags |= FUSE_WRITEBACK_CACHE;
        }

        if init_in.flags & FUSE_NO_OPEN_SUPPORT > 0 && self.mount_options.no_open_support {
            debug!("enable FUSE_NO_OPEN_SUPPORT");

            reply_flags |= FUSE_NO_OPEN_SUPPORT;
        }

        if init_in.flags & FUSE_PARALLEL_DIROPS > 0 {
            debug!("enable FUSE_PARALLEL_DIROPS");

            reply_flags |= FUSE_PARALLEL_DIROPS;
        }

        if init_in.flags & FUSE_HANDLE_KILLPRIV > 0 && self.mount_options.handle_killpriv {
            debug!("enable FUSE_HANDLE_KILLPRIV");

            reply_flags |= FUSE_HANDLE_KILLPRIV;
        }

        if init_in.flags & FUSE_POSIX_ACL > 0 && self.mount_options.posix_acl {
            debug!("enable FUSE_POSIX_ACL");

            reply_flags |= FUSE_POSIX_ACL;
        }

        if init_in.flags & FUSE_MAX_PAGES > 0 {
            debug!("enable FUSE_MAX_PAGES");

            reply_flags |= FUSE_MAX_PAGES;
        }

        if init_in.flags & FUSE_CACHE_SYMLINKS > 0 {
            debug!("enable FUSE_CACHE_SYMLINKS");

            reply_flags |= FUSE_CACHE_SYMLINKS;
        }

        if init_in.flags & FUSE_NO_OPENDIR_SUPPORT > 0 && self.mount_options.no_open_dir_support {
            debug!("enable FUSE_NO_OPENDIR_SUPPORT");

            reply_flags |= FUSE_NO_OPENDIR_SUPPORT;
        }

        #[cfg(target_os = "macos")]
        if init_in.flags & FUSE_ALLOCATE > 0 {
            debug!("enable FUSE_ALLOCATE");

            reply_flags |= FUSE_ALLOCATE;
        }

        #[cfg(target_os = "macos")]
        if init_in.flags & FUSE_EXCHANGE_DATA > 0 {
            debug!("enable FUSE_EXCHANGE_DATA");

            reply_flags |= FUSE_EXCHANGE_DATA;
        }

        #[cfg(target_os = "macos")]
        if init_in.flags & FUSE_CASE_INSENSITIVE > 0 {
            debug!("enable FUSE_CASE_INSENSITIVE");

            reply_flags |= FUSE_CASE_INSENSITIVE;
        }

        #[cfg(target_os = "macos")]
        if init_in.flags & FUSE_VOL_RENAME > 0 {
            debug!("enable FUSE_VOL_RENAME");

            reply_flags |= FUSE_VOL_RENAME;
        }

        #[cfg(target_os = "macos")]
        if init_in.flags & FUSE_XTIMES > 0 {
            debug!("enable FUSE_XTIMES");

            reply_flags |= FUSE_XTIMES;
        }

        let init_reply = match fs.init_with_notify(request, self.get_notify()).await {
            Ok(reply) => reply,
            Err(err) => {
                let init_out_header = fuse_out_header {
                    len: FUSE_OUT_HEADER_SIZE as u32,
                    error: err.into(),
                    unique: request.unique,
                };

                let init_out_header_data = get_bincode_config()
                    .serialize(&init_out_header)
                    .expect("won't happened");

                if let Err(err) = fuse_connection
                    .write_vectored(
                        crate::raw::reply::ReplyBytes::from(init_out_header_data),
                        None::<crate::raw::reply::ReplyBytes>,
                    )
                    .await
                    .1
                {
                    error!("write error init out data to /dev/fuse failed {}", err);
                }

                return Err(err.into());
            }
        };

        // Use max_readahead from mount_options if set, otherwise use kernel's value
        let max_readahead = self
            .mount_options
            .max_readahead
            .unwrap_or(init_in.max_readahead);

        let max_write = if self.mount_options.max_write.get() < init_reply.max_write.get() {
            self.mount_options.max_write
        } else {
            init_reply.max_write
        };

        let max_background = u16::try_from(self.max_background)
            .unwrap_or(u16::MAX)
            .max(1);
        // Use max_background as the congestion threshold so the kernel never
        // throttles writeback.  With the default 3/4 ratio the kernel stops
        // sending FUSE_WRITE requests when 75% of background slots are in use,
        // which can deadlock mmap-heavy workloads (e.g. generic/013 fsstress)
        // if reply processing is slower than the kernel's dirty-page rate.
        let congestion_threshold = max_background;

        self.kernel_writeback_permissions =
            kernel_writeback_permissions(self.mount_options.default_permissions, reply_flags);

        let init_out = fuse_init_out {
            major: FUSE_KERNEL_VERSION,
            minor: FUSE_KERNEL_MINOR_VERSION,
            max_readahead,
            flags: reply_flags,
            max_background,
            congestion_threshold,
            max_write: max_write.get(),
            time_gran: DEFAULT_TIME_GRAN,
            max_pages: DEFAULT_MAX_PAGES,
            map_alignment: DEFAULT_MAP_ALIGNMENT,
            flags2: 0x1, // FUSE_HAS_MAX_READ — tells kernel max_read is valid
            max_read: 4 * 1024 * 1024,
            unused: [0; 6],
        };

        debug!("fuse init out {:?}", init_out);

        let out_header = fuse_out_header {
            len: (FUSE_OUT_HEADER_SIZE + FUSE_INIT_OUT_SIZE) as u32,
            error: 0,
            unique: request.unique,
        };

        let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_INIT_OUT_SIZE);

        get_bincode_config()
            .serialize_into(&mut data, &out_header)
            .expect("won't happened");
        get_bincode_config()
            .serialize_into(&mut data, &init_out)
            .expect("won't happened");

        if let Err(err) = fuse_connection
            .write_vectored(
                crate::raw::reply::ReplyBytes::from(data),
                None::<crate::raw::reply::ReplyBytes>,
            )
            .await
            .1
        {
            error!("write init out data to /dev/fuse failed {}", err);

            return Err(err);
        }

        debug!("fuse init done");

        Ok(max_write)
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_lookup(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let name = match get_first_null_position(data) {
            None => {
                error!("lookup body has no null, request unique {}", request.unique);

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => OsString::from_vec(data[..index].to_vec()),
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_lookup"), async move {
            debug!(
                "lookup unique {} name {:?} in parent {}",
                request.unique, name, in_header.nodeid
            );

            let data = match fs.lookup(request, in_header.nodeid, &name).await {
                Err(err) => {
                    let out_header = fuse_out_header {
                        len: FUSE_OUT_HEADER_SIZE as u32,
                        error: err.into(),
                        unique: request.unique,
                    };

                    get_bincode_config()
                        .serialize(&out_header)
                        .expect("won't happened")
                }

                Ok(entry) => {
                    let entry_out: fuse_entry_out = entry.into();

                    debug!("lookup response {:?}", entry_out);

                    let out_header = fuse_out_header {
                        len: (FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE) as u32,
                        error: 0,
                        unique: request.unique,
                    };

                    let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE);

                    get_bincode_config()
                        .serialize_into(&mut data, &out_header)
                        .expect("won't happened");
                    get_bincode_config()
                        .serialize_into(&mut data, &entry_out)
                        .expect("won't happened");

                    data
                }
            };

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    /// if Ok(true), quit the dispatch
    #[instrument(skip(self, data, fs))]
    async fn handle_forget(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let forget_in = match get_bincode_config().deserialize::<fuse_forget_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_forget_in failed {}, request unique {}",
                    err, request.unique
                );

                // no need to reply
                return;
            }

            Ok(forget_in) => forget_in,
        };

        let fs = fs.clone();

        spawn(debug_span!("fuse_forget"), async move {
            debug!(
                "forget unique {} inode {} nlookup {}",
                request.unique, in_header.nodeid, forget_in.nlookup
            );

            fs.forget(request, in_header.nodeid, forget_in.nlookup)
                .await
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_getattr(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let getattr_in = match get_bincode_config().deserialize::<fuse_getattr_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_forget_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(getattr_in) => getattr_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_getattr"), async move {
            debug!(
                "getattr unique {} inode {}",
                request.unique, in_header.nodeid
            );

            let fh = if getattr_in.getattr_flags & FUSE_GETATTR_FH > 0 {
                Some(getattr_in.fh)
            } else {
                None
            };

            let data = match fs
                .getattr(request, in_header.nodeid, fh, getattr_in.getattr_flags)
                .await
            {
                Err(err) => {
                    let out_header = fuse_out_header {
                        len: FUSE_OUT_HEADER_SIZE as u32,
                        error: err.into(),
                        unique: request.unique,
                    };

                    get_bincode_config()
                        .serialize(&out_header)
                        .expect("won't happened")
                }

                Ok(attr) => {
                    let attr_out = fuse_attr_out {
                        attr_valid: attr.ttl.as_secs(),
                        attr_valid_nsec: attr.ttl.subsec_nanos(),
                        dummy: getattr_in.dummy,
                        attr: attr.attr.into(),
                    };

                    let out_header = fuse_out_header {
                        len: (FUSE_OUT_HEADER_SIZE + FUSE_ATTR_OUT_SIZE) as u32,
                        error: 0,
                        unique: request.unique,
                    };

                    let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ATTR_OUT_SIZE);

                    get_bincode_config()
                        .serialize_into(&mut data, &out_header)
                        .expect("won't happened");
                    get_bincode_config()
                        .serialize_into(&mut data, &attr_out)
                        .expect("won't happened");

                    data
                }
            };

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_setattr(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let setattr_in = match get_bincode_config().deserialize::<fuse_setattr_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_setattr_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(setattr_in) => setattr_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        let kernel_writeback_permissions = self.kernel_writeback_permissions;
        spawn(debug_span!("fuse_setattr"), async move {
            let mut set_attr = SetAttr::from(&setattr_in);
            set_attr.kernel_writeback_permissions = kernel_writeback_permissions;

            let fh = if setattr_in.valid & FATTR_FH > 0 {
                Some(setattr_in.fh)
            } else {
                None
            };

            debug!(
                "setattr unique {} inode {} set_attr {:?}",
                request.unique, in_header.nodeid, set_attr
            );

            let data = match fs.setattr(request, in_header.nodeid, fh, set_attr).await {
                Err(err) => {
                    let out_header = fuse_out_header {
                        len: FUSE_OUT_HEADER_SIZE as u32,
                        error: err.into(),
                        unique: request.unique,
                    };

                    get_bincode_config()
                        .serialize(&out_header)
                        .expect("won't happened")
                }

                Ok(attr) => {
                    let attr_out: fuse_attr_out = attr.into();

                    let out_header = fuse_out_header {
                        len: (FUSE_OUT_HEADER_SIZE + FUSE_ATTR_OUT_SIZE) as u32,
                        error: 0,
                        unique: request.unique,
                    };

                    let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ATTR_OUT_SIZE);

                    get_bincode_config()
                        .serialize_into(&mut data, &out_header)
                        .expect("won't happened");
                    get_bincode_config()
                        .serialize_into(&mut data, &attr_out)
                        .expect("won't happened");

                    data
                }
            };

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, fs))]
    async fn handle_readlink(&mut self, request: Request, in_header: fuse_in_header, fs: &Arc<FS>) {
        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_readlink"), async move {
            debug!(
                "readlink unique {} inode {}",
                request.unique, in_header.nodeid
            );

            let data = match fs.readlink(request, in_header.nodeid).await {
                Err(err) => {
                    let out_header = fuse_out_header {
                        len: FUSE_OUT_HEADER_SIZE as u32,
                        error: err.into(),
                        unique: request.unique,
                    };

                    Either::Left(
                        get_bincode_config()
                            .serialize(&out_header)
                            .expect("won't happened"),
                    )
                }

                Ok(data) => {
                    let out_header = fuse_out_header {
                        len: (FUSE_OUT_HEADER_SIZE + data.data.len()) as u32,
                        error: 0,
                        unique: request.unique,
                    };

                    let mut data_buf = Vec::with_capacity(FUSE_OUT_HEADER_SIZE);

                    get_bincode_config()
                        .serialize_into(&mut data_buf, &out_header)
                        .expect("won't happened");

                    Either::Right((data_buf, data.data.into()))
                }
            };

            let _ = resp_sender.send(data).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_symlink(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        mut data: &[u8],
        fs: &Arc<FS>,
    ) {
        let (name, first_null_index) = match get_first_null_position(data) {
            None => {
                error!("symlink has no null, request unique {}", request.unique);

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => (OsString::from_vec(data[..index].to_vec()), index),
        };

        data = &data[first_null_index + 1..];

        let link_name = match get_first_null_position(data) {
            None => {
                error!(
                    "symlink has no second null, request unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => OsString::from_vec(data[..index].to_vec()),
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_symlink"), async move {
            debug!(
                "symlink unique {} parent {} name {:?} link {:?}",
                request.unique, in_header.nodeid, name, link_name
            );

            let data = match fs
                .symlink(request, in_header.nodeid, &name, &link_name)
                .await
            {
                Err(err) => {
                    let out_header = fuse_out_header {
                        len: FUSE_OUT_HEADER_SIZE as u32,
                        error: err.into(),
                        unique: request.unique,
                    };

                    get_bincode_config()
                        .serialize(&out_header)
                        .expect("won't happened")
                }

                Ok(entry) => {
                    let entry_out: fuse_entry_out = entry.into();

                    let out_header = fuse_out_header {
                        len: (FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE) as u32,
                        error: 0,
                        unique: request.unique,
                    };

                    let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE);

                    get_bincode_config()
                        .serialize_into(&mut data, &out_header)
                        .expect("won't happened");
                    get_bincode_config()
                        .serialize_into(&mut data, &entry_out)
                        .expect("won't happened");

                    data
                }
            };

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_mknod(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        mut data: &[u8],
        fs: &Arc<FS>,
    ) {
        let mknod_in = match get_bincode_config().deserialize::<fuse_mknod_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_mknod_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(mknod_in) => mknod_in,
        };

        data = &data[FUSE_MKNOD_IN_SIZE..];

        let name = match get_first_null_position(data) {
            None => {
                error!(
                    "fuse_mknod_in body doesn't have null, request unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => OsString::from_vec(data[..index].to_vec()),
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_mknod"), async move {
            debug!(
                "mknod unique {} parent {} name {:?} {:?}",
                request.unique, in_header.nodeid, name, mknod_in
            );

            match fs
                .mknod_with_umask(
                    request,
                    in_header.nodeid,
                    &name,
                    mknod_in.mode,
                    mknod_in._umask,
                    mknod_in.rdev,
                )
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;
                }

                Ok(entry) => {
                    let entry_out: fuse_entry_out = entry.into();

                    let out_header = fuse_out_header {
                        len: (FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE) as u32,
                        error: 0,
                        unique: request.unique,
                    };

                    let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE);

                    get_bincode_config()
                        .serialize_into(&mut data, &out_header)
                        .expect("won't happened");
                    get_bincode_config()
                        .serialize_into(&mut data, &entry_out)
                        .expect("won't happened");

                    let _ = resp_sender.send(Either::Left(data)).await;
                }
            }
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_mkdir(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        mut data: &[u8],
        fs: &Arc<FS>,
    ) {
        let mkdir_in = match get_bincode_config().deserialize::<fuse_mkdir_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_mknod_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(mkdir_in) => mkdir_in,
        };

        data = &data[FUSE_MKDIR_IN_SIZE..];

        let name = match get_first_null_position(data) {
            None => {
                error!(
                    "deserialize fuse_mknod_in doesn't have null unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => OsString::from_vec(data[..index].to_vec()),
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_mkdir"), async move {
            debug!(
                "mkdir unique {} parent {} name {:?} {:?}",
                request.unique, in_header.nodeid, name, mkdir_in
            );

            match fs
                .mkdir(
                    request,
                    in_header.nodeid,
                    &name,
                    mkdir_in.mode,
                    mkdir_in.umask,
                )
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;
                }

                Ok(entry) => {
                    let entry_out: fuse_entry_out = entry.into();

                    let out_header = fuse_out_header {
                        len: (FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE) as u32,
                        error: 0,
                        unique: request.unique,
                    };

                    let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE);

                    get_bincode_config()
                        .serialize_into(&mut data, &out_header)
                        .expect("won't happened");
                    get_bincode_config()
                        .serialize_into(&mut data, &entry_out)
                        .expect("won't happened");

                    let _ = resp_sender.send(Either::Left(data)).await;
                }
            }
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_unlink(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let name = match get_first_null_position(data) {
            None => {
                error!(
                    "unlink body doesn't have null, request unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => OsString::from_vec(data[..index].to_vec()),
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_unlink"), async move {
            debug!(
                "unlink unique {} parent {} name {:?}",
                request.unique, in_header.nodeid, name
            );

            let resp_value = if let Err(err) = fs.unlink(request, in_header.nodeid, &name).await {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_rmdir(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let name = match get_first_null_position(data) {
            None => {
                error!(
                    "rmdir body doesn't have null, request unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => OsString::from_vec(data[..index].to_vec()),
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_rmdir"), async move {
            debug!(
                "rmdir unique {} parent {} name {:?}",
                request.unique, in_header.nodeid, name
            );

            let resp_value = if let Err(err) = fs.rmdir(request, in_header.nodeid, &name).await {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_rename(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        mut data: &[u8],
        fs: &Arc<FS>,
    ) {
        let rename_in = match get_bincode_config().deserialize::<fuse_rename_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_rename_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(rename_in) => rename_in,
        };

        data = &data[FUSE_RENAME_IN_SIZE..];

        let (name, first_null_index) = match get_first_null_position(data) {
            None => {
                error!(
                    "fuse_rename_in body doesn't have null, request unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => (OsString::from_vec(data[..index].to_vec()), index),
        };

        data = &data[first_null_index + 1..];

        let new_name = match get_first_null_position(data) {
            None => {
                error!(
                    "fuse_rename_in body doesn't have null, request unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => OsString::from_vec(data[..index].to_vec()),
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_rename"), async move {
            debug!(
                "rename unique {} parent {} name {:?} new parent {} new name {:?}",
                request.unique, in_header.nodeid, name, rename_in.newdir, new_name
            );

            let resp_value = if let Err(err) = fs
                .rename(
                    request,
                    in_header.nodeid,
                    &name,
                    rename_in.newdir,
                    &new_name,
                )
                .await
            {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_link(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        mut data: &[u8],
        fs: &Arc<FS>,
    ) {
        let link_in = match get_bincode_config().deserialize::<fuse_link_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_link_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(link_in) => link_in,
        };

        data = &data[FUSE_LINK_IN_SIZE..];

        let name = match get_first_null_position(data) {
            None => {
                error!(
                    "fuse_link_in body doesn't have null, request unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => OsString::from_vec(data[..index].to_vec()),
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_link"), async move {
            debug!(
                "link unique {} inode {} new parent {} new name {:?}",
                request.unique, link_in.oldnodeid, in_header.nodeid, name
            );

            match fs
                .link(request, link_in.oldnodeid, in_header.nodeid, &name)
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;
                }

                Ok(entry) => {
                    let entry_out: fuse_entry_out = entry.into();

                    let out_header = fuse_out_header {
                        len: (FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE) as u32,
                        error: 0,
                        unique: request.unique,
                    };

                    let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE);

                    get_bincode_config()
                        .serialize_into(&mut data, &out_header)
                        .expect("won't happened");
                    get_bincode_config()
                        .serialize_into(&mut data, &entry_out)
                        .expect("won't happened");

                    let _ = resp_sender.send(Either::Left(data)).await;
                }
            }
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_open(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let open_in = match get_bincode_config().deserialize::<fuse_open_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_open_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(open_in) => open_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();
        let direct_io = self.mount_options.direct_io;

        spawn(debug_span!("fuse_open"), async move {
            debug!(
                "open unique {} inode {} flags {}",
                request.unique, in_header.nodeid, open_in.flags
            );

            let opened = match fs.open(request, in_header.nodeid, open_in.flags).await {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(opened) => opened,
            };

            let mut open_out: fuse_open_out = opened.into();
            apply_direct_io(&mut open_out.open_flags, direct_io);

            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_OPEN_OUT_SIZE) as u32,
                error: 0,
                unique: request.unique,
            };

            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_OPEN_OUT_SIZE);

            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("won't happened");
            get_bincode_config()
                .serialize_into(&mut data, &open_out)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_read(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let read_in = match get_bincode_config().deserialize::<fuse_read_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_read_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(read_in) => read_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_read"), async move {
            debug!(
                "read unique {} inode {} {:?}",
                request.unique, in_header.nodeid, read_in
            );

            let mut reply_data = match fs
                .read(
                    request,
                    in_header.nodeid,
                    read_in.fh,
                    read_in.offset,
                    read_in.size,
                )
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(reply_data) => reply_data.data,
            };

            if reply_data.len() > read_in.size as _ {
                reply_data.truncate(read_in.size as _);
            }

            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + reply_data.len()) as u32,
                error: 0,
                unique: request.unique,
            };

            let mut data_buf = Vec::with_capacity(FUSE_OUT_HEADER_SIZE);

            get_bincode_config()
                .serialize_into(&mut data_buf, &out_header)
                .expect("won't happened");

            let _ = resp_sender
                .send(Either::Right((data_buf, reply_data.into())))
                .await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_write(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        mut data: &[u8],
        fs: &Arc<FS>,
    ) {
        let write_in = match get_bincode_config().deserialize::<fuse_write_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_write_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(write_in) => write_in,
        };

        data = &data[FUSE_WRITE_IN_SIZE..];

        if write_in.size as usize != data.len() {
            error!("fuse_write_in body len is invalid");

            reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

            return;
        }

        let data = data.to_vec();

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_write"), async move {
            debug!(
                "write unique {} inode {} {:?}",
                request.unique, in_header.nodeid, write_in
            );

            // Special handling for O_DIRECT writes: ensure the buffer is 4096-byte aligned.
            // Currently we only encounter this case in xfstests.
            use std::borrow::Cow;
            const ALIGNMENT: usize = 4096;

            let mut _owned = None;

            let aligned_data: Cow<[u8]> = if data.as_ptr().align_offset(ALIGNMENT) != 0 {
                tracing::warn!("The data is not 4096 bytes aligned");
                let mut buf =
                    aligned_box::AlignedBox::<[u8]>::slice_from_default(ALIGNMENT, data.len())
                        .unwrap();
                buf.copy_from_slice(&data);
                _owned = Some(buf);
                Cow::Borrowed(_owned.as_ref().unwrap())
            } else {
                Cow::Borrowed(&data)
            };

            let reply_write = match fs
                .write(
                    request,
                    in_header.nodeid,
                    write_in.fh,
                    write_in.offset,
                    &aligned_data,
                    write_in.write_flags,
                    write_in.flags,
                )
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(reply_write) => reply_write,
            };

            let write_out: fuse_write_out = reply_write.into();

            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_WRITE_OUT_SIZE) as u32,
                error: 0,
                unique: request.unique,
            };

            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_WRITE_OUT_SIZE);

            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("won't happened");
            get_bincode_config()
                .serialize_into(&mut data, &write_out)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, fs))]
    async fn handle_statfs(&mut self, request: Request, in_header: fuse_in_header, fs: &Arc<FS>) {
        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_statfs"), async move {
            debug!(
                "statfs unique {} inode {}",
                request.unique, in_header.nodeid
            );

            let fs_stat = match fs.statfs(request, in_header.nodeid).await {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(fs_stat) => fs_stat,
            };

            let statfs_out: fuse_statfs_out = fs_stat.into();

            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_STATFS_OUT_SIZE) as u32,
                error: 0,
                unique: request.unique,
            };

            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_STATFS_OUT_SIZE);

            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("won't happened");
            get_bincode_config()
                .serialize_into(&mut data, &statfs_out)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_release(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let release_in = match get_bincode_config().deserialize::<fuse_release_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_release_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(release_in) => release_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_release"), async move {
            let flush = release_in.release_flags & FUSE_RELEASE_FLUSH > 0;

            debug!(
                "release unique {} inode {} fh {} flags {} lock_owner {} flush {}",
                request.unique,
                in_header.nodeid,
                release_in.fh,
                release_in.flags,
                release_in.lock_owner,
                flush
            );

            let resp_value = if let Err(err) = fs
                .release(
                    request,
                    in_header.nodeid,
                    release_in.fh,
                    release_in.flags,
                    release_in.lock_owner,
                    flush,
                )
                .await
            {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_fsync(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let fsync_in = match get_bincode_config().deserialize::<fuse_fsync_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_fsync_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(fsync_in) => fsync_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_fsync"), async move {
            let data_sync = fsync_in.fsync_flags & 1 > 0;

            debug!(
                "fsync unique {} inode {} fh {} data_sync {}",
                request.unique, in_header.nodeid, fsync_in.fh, data_sync
            );

            let resp_value = if let Err(err) = fs
                .fsync(request, in_header.nodeid, fsync_in.fh, data_sync)
                .await
            {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_setxattr(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        mut data: &[u8],
        fs: &Arc<FS>,
    ) {
        let setxattr_in = match get_bincode_config().deserialize::<fuse_setxattr_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_setxattr_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(setxattr_in) => setxattr_in,
        };

        data = &data[FUSE_SETXATTR_IN_SIZE..];

        let (name, first_null_index) = match get_first_null_position(data) {
            None => {
                error!(
                    "fuse_setxattr_in body has no null, request unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => (OsString::from_vec(data[..index].to_vec()), index),
        };

        data = &data[first_null_index + 1..];

        // setxattr "size" field specifies size of only "Value" part of data
        if setxattr_in.size as usize != data.len() {
            error!(
                "fuse_setxattr_in value field data length is not right, request unique {} setxattr_in.size={} data.len={}",
                request.unique,
                setxattr_in.size,
                data.len()
            );

            reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

            return;
        }

        let data = data.to_vec();

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_setxattr"), async move {
            debug!(
                "setxattr unique {} inode {}",
                request.unique, in_header.nodeid
            );

            // TODO handle os X argument
            let resp_value = if let Err(err) = fs
                .setxattr(
                    request,
                    in_header.nodeid,
                    &name,
                    &data,
                    setxattr_in.flags,
                    0,
                )
                .await
            {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_getxattr(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        mut data: &[u8],
        fs: &Arc<FS>,
    ) {
        let getxattr_in = match get_bincode_config().deserialize::<fuse_getxattr_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_getxattr_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(getxattr_in) => getxattr_in,
        };

        data = &data[FUSE_GETXATTR_IN_SIZE..];

        let name = match get_first_null_position(data) {
            None => {
                error!("fuse_getxattr_in body has no null {}", request.unique);

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => OsString::from_vec(data[..index].to_vec()),
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_getxattr"), async move {
            debug!(
                "getxattr unique {} inode {}",
                request.unique, in_header.nodeid
            );

            let xattr = match fs
                .getxattr(request, in_header.nodeid, &name, getxattr_in.size)
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(xattr) => xattr,
            };

            let data = xattr_reply::encode_xattr_reply(xattr, request.unique, getxattr_in.size);

            let _ = resp_sender.send(data).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_listxattr(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let listxattr_in = match get_bincode_config().deserialize::<fuse_getxattr_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_getxattr_in in listxattr failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(listxattr_in) => listxattr_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_listxattr"), async move {
            debug!(
                "listxattr unique {} inode {} size {}",
                request.unique, in_header.nodeid, listxattr_in.size
            );

            let xattr = match fs
                .listxattr(request, in_header.nodeid, listxattr_in.size)
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(xattr) => xattr,
            };

            let data = xattr_reply::encode_xattr_reply(xattr, request.unique, listxattr_in.size);

            let _ = resp_sender.send(data).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_removexattr(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let name = match get_first_null_position(data) {
            None => {
                error!(
                    "fuse removexattr body has no null, request unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => OsString::from_vec(data[..index].to_vec()),
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_removexattr"), async move {
            debug!(
                "removexattr unique {} inode {}",
                request.unique, in_header.nodeid
            );

            let resp_value =
                if let Err(err) = fs.removexattr(request, in_header.nodeid, &name).await {
                    err.into()
                } else {
                    0
                };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_flush(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let flush_in = match get_bincode_config().deserialize::<fuse_flush_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_flush_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(flush_in) => flush_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_flush"), async move {
            debug!(
                "flush unique {} inode {} fh {} lock_owner {}",
                request.unique, in_header.nodeid, flush_in.fh, flush_in.lock_owner
            );

            let resp_value = if let Err(err) = fs
                .flush(request, in_header.nodeid, flush_in.fh, flush_in.lock_owner)
                .await
            {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_opendir(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let open_in = match get_bincode_config().deserialize::<fuse_open_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_open_in in opendir failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(open_in) => open_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_opendir"), async move {
            debug!(
                "opendir unique {} inode {} flags {}",
                request.unique, in_header.nodeid, open_in.flags
            );

            let reply_open = match fs.opendir(request, in_header.nodeid, open_in.flags).await {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(reply_open) => reply_open,
            };

            let open_out: fuse_open_out = reply_open.into();

            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_OPEN_OUT_SIZE) as u32,
                error: 0,
                unique: request.unique,
            };

            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_OPEN_OUT_SIZE);

            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("won't happened");
            get_bincode_config()
                .serialize_into(&mut data, &open_out)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_readdir(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        if self.mount_options.force_readdir_plus {
            reply_error_in_place(libc::ENOSYS.into(), request, self.response_sender()).await;

            return;
        }

        let read_in = match get_bincode_config().deserialize::<fuse_read_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_read_in in readdir failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(read_in) => read_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_readdir"), async move {
            let reply_guard = match fs.reserve_reply_memory(u64::from(read_in.size)) {
                Ok(guard) => guard,
                Err(error) => {
                    reply_error_in_place(error, request, resp_sender).await;
                    return;
                }
            };
            debug!(
                "readdir unique {} inode {} fh {} offset {}",
                request.unique, in_header.nodeid, read_in.fh, read_in.offset
            );

            let reply_readdir = match fs
                .readdir(request, in_header.nodeid, read_in.fh, read_in.offset as i64)
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(reply_readdir) => reply_readdir,
            };

            let max_size = read_in.size as usize;

            // Pre-allocate buffer with header space at the beginning to avoid multi-allocation
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + max_size);
            // Reserve space for header (will be filled later when we know the final size)
            data.resize(FUSE_OUT_HEADER_SIZE, 0);

            let entries = reply_readdir.entries;
            let mut entries = pin!(entries);

            while let Some(entry) = entries.next().await {
                let entry = match entry {
                    Err(err) => {
                        reply_error_in_place(err, request, resp_sender).await;

                        return;
                    }

                    Ok(entry) => entry,
                };

                let name = &entry.name;

                let dir_entry_size = FUSE_DIRENT_SIZE + name.len();

                let padding_size = get_padding_size(dir_entry_size);

                // Check against max_size (entry_data portion only)
                if data.len() - FUSE_OUT_HEADER_SIZE + dir_entry_size + padding_size > max_size {
                    break;
                }

                let dir_entry = fuse_dirent {
                    ino: entry.inode,
                    off: entry.offset as u64,
                    namelen: name.len() as u32,
                    // learn from fuse-rs and golang bazil.org fuse DirentType
                    r#type: mode_from_kind_and_perm(entry.kind, 0) >> 12,
                };

                get_bincode_config()
                    .serialize_into(&mut data, &dir_entry)
                    .expect("won't happened");

                data.extend_from_slice(name.as_bytes());

                // padding
                data.resize(data.len() + padding_size, 0);
            }

            // Now fill in the header at the beginning
            let out_header = fuse_out_header {
                len: data.len() as u32,
                error: 0,
                unique: request.unique,
            };

            // Overwrite the reserved header space
            get_bincode_config()
                .serialize_into(&mut data[..FUSE_OUT_HEADER_SIZE], &out_header)
                .expect("won't happened");

            let data = crate::raw::reply::own_reply_buffer(data, reply_guard);
            let _ = resp_sender.send(Either::Right((Vec::new(), data))).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_releasedir(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let release_in = match get_bincode_config().deserialize::<fuse_release_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_release_in in releasedir failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(release_in) => release_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_releasedir"), async move {
            debug!(
                "releasedir unique {} inode {} fh {} flags {}",
                request.unique, in_header.nodeid, release_in.fh, release_in.flags
            );

            let resp_value = if let Err(err) = fs
                .releasedir(request, in_header.nodeid, release_in.fh, release_in.flags)
                .await
            {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_fsyncdir(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let fsync_in = match get_bincode_config().deserialize::<fuse_fsync_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_fsync_in in fsyncdir failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(fsync_in) => fsync_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_fsyncdir"), async move {
            let data_sync = fsync_in.fsync_flags & 1 > 0;

            debug!(
                "fsyncdir unique {} inode {} fh {} data_sync {}",
                request.unique, in_header.nodeid, fsync_in.fh, data_sync
            );

            let resp_value = if let Err(err) = fs
                .fsyncdir(request, in_header.nodeid, fsync_in.fh, data_sync)
                .await
            {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[cfg(feature = "file-lock")]
    #[instrument(skip(self, data, fs))]
    async fn handle_getlk(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let getlk_in = match get_bincode_config().deserialize::<fuse_lk_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_lk_in in getlk failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(getlk_in) => getlk_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_getlk"), async move {
            debug!(
                "getlk unique {} inode {} {:?}",
                request.unique, in_header.nodeid, getlk_in
            );

            let reply_lock = match fs
                .getlk(
                    request,
                    in_header.nodeid,
                    getlk_in.fh,
                    getlk_in.owner,
                    getlk_in.lk.start,
                    getlk_in.lk.end,
                    getlk_in.lk.r#type,
                    getlk_in.lk.pid,
                )
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(reply_lock) => reply_lock,
            };

            let getlk_out: fuse_lk_out = reply_lock.into();

            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_LK_OUT_SIZE) as u32,
                error: 0,
                unique: request.unique,
            };

            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_LK_OUT_SIZE);

            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("won't happened");
            get_bincode_config()
                .serialize_into(&mut data, &getlk_out)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[cfg(feature = "file-lock")]
    #[instrument(skip(self, data, fs))]
    async fn handle_setlk(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        block: bool,
        fs: &Arc<FS>,
    ) {
        let setlk_in = match get_bincode_config().deserialize::<fuse_lk_in>(data) {
            Err(err) => {
                let opcode = if block {
                    fuse_opcode::FUSE_SETLKW
                } else {
                    fuse_opcode::FUSE_SETLK
                };

                error!(
                    "deserialize fuse_lk_in in {:?} failed {}, request unique {}",
                    opcode, err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(setlk_in) => setlk_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_setlk"), async move {
            debug!(
                "setlk unique {} inode {} block {} {:?}",
                request.unique, in_header.nodeid, block, setlk_in
            );

            let resp = if let Err(err) = fs
                .setlk(
                    request,
                    in_header.nodeid,
                    setlk_in.fh,
                    setlk_in.owner,
                    setlk_in.lk.start,
                    setlk_in.lk.end,
                    setlk_in.lk.r#type,
                    setlk_in.lk.pid,
                    block,
                )
                .await
            {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("can't serialize into vec");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_access(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let access_in = match get_bincode_config().deserialize::<fuse_access_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_access_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(access_in) => access_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_access"), async move {
            debug!(
                "access unique {} inode {} mask {}",
                request.unique, in_header.nodeid, access_in.mask
            );

            let resp_value =
                if let Err(err) = fs.access(request, in_header.nodeid, access_in.mask).await {
                    err.into()
                } else {
                    0
                };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            debug!("access response {}", resp_value);

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_create(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        mut data: &[u8],
        fs: &Arc<FS>,
    ) {
        let create_in = match get_bincode_config().deserialize::<fuse_create_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_create_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(create_in) => create_in,
        };

        data = &data[FUSE_CREATE_IN_SIZE..];

        let name = match get_first_null_position(data) {
            None => {
                error!(
                    "fuse_create_in body has no null, request unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => OsString::from_vec(data[..index].to_vec()),
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();
        let direct_io = self.mount_options.direct_io;

        spawn(debug_span!("fuse_create"), async move {
            debug!(
                "create unique {} parent {} name {:?} mode {} flags {}",
                request.unique, in_header.nodeid, name, create_in.mode, create_in.flags
            );

            let created = match fs
                .create_with_umask(
                    request,
                    in_header.nodeid,
                    &name,
                    create_in.mode,
                    create_in._umask,
                    create_in.flags,
                )
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(created) => created,
            };

            let (entry_out, mut open_out): (fuse_entry_out, fuse_open_out) = created.into();
            apply_direct_io(&mut open_out.open_flags, direct_io);

            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE + FUSE_OPEN_OUT_SIZE) as u32,
                error: 0,
                unique: request.unique,
            };

            let mut data =
                Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE + FUSE_OPEN_OUT_SIZE);

            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("won't happened");
            get_bincode_config()
                .serialize_into(&mut data, &entry_out)
                .expect("won't happened");
            get_bincode_config()
                .serialize_into(&mut data, &open_out)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_interrupt(
        &mut self,
        request: Request,
        data: &[u8],
        fs: &Arc<FS>,
    ) -> IoResult<()> {
        let cancellable = fs.supports_read_cancellation();
        self.ensure_readonly_reply_tracker(fs.as_ref())?;
        let owner = if cancellable {
            fs.reserve_control_memory(CONTROL_REPLY_MEMORY_BYTES)
                .map_err(IoError::from)?
        } else {
            None
        };
        let interrupt_in = match get_bincode_config().deserialize::<fuse_interrupt_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_interrupt_in failed {}, request unique {}",
                    err, request.unique
                );

                if cancellable {
                    self.send_control_reply(request, libc::EINVAL.into(), owner)?;
                } else {
                    reply_error_in_place(libc::EINVAL.into(), request, self.response_sender())
                        .await;
                }

                return Ok(());
            }

            Ok(interrupt_in) => interrupt_in,
        };

        if cancellable {
            // Packed readonly interrupt is a mount-local token operation. Keep
            // it in the control reader instead of detaching unowned tasks.
            let error = match fs.interrupt(request, interrupt_in.unique).await {
                Ok(()) => 0.into(),
                Err(error) => error,
            };
            self.send_control_reply(request, error, owner)?;
            return Ok(());
        }

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_interrupt"), async move {
            debug!(
                "interrupt_in unique {} interrupt unique {}",
                request.unique, interrupt_in.unique
            );

            let resp_value = if let Err(err) = fs.interrupt(request, interrupt_in.unique).await {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
        Ok(())
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_bmap(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let bmap_in = match get_bincode_config().deserialize::<fuse_bmap_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_bmap_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(bmap_in) => bmap_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_bmap"), async move {
            debug!(
                "bmap unique {} inode {} block size {} idx {}",
                request.unique, in_header.nodeid, bmap_in.blocksize, bmap_in.block
            );

            let reply_bmap = match fs
                .bmap(request, in_header.nodeid, bmap_in.blocksize, bmap_in.block)
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(reply_bmap) => reply_bmap,
            };

            let bmap_out: fuse_bmap_out = reply_bmap.into();

            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_BMAP_OUT_SIZE) as u32,
                error: 0,
                unique: request.unique,
            };

            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_BMAP_OUT_SIZE);

            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("won't happened");
            get_bincode_config()
                .serialize_into(&mut data, &bmap_out)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_ioctl(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let ioctl_in = match get_bincode_config().deserialize::<fuse_ioctl_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_ioctl_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(ioctl_in) => ioctl_in,
        };

        let payload_end = FUSE_IOCTL_IN_SIZE.saturating_add(ioctl_in.in_size as usize);
        if data.len() < payload_end {
            reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;
            return;
        }
        let ioctl_data = data[FUSE_IOCTL_IN_SIZE..payload_end].to_vec();

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_ioctl"), async move {
            debug!(
                "ioctl unique {} inode {} fh {} flags {} cmd {} arg {} in_size {} out_size {}",
                request.unique,
                in_header.nodeid,
                ioctl_in.fh,
                ioctl_in.flags,
                ioctl_in.cmd,
                ioctl_in.arg,
                ioctl_in.in_size,
                ioctl_in.out_size
            );

            let reply_ioctl = match fs
                .ioctl(
                    request,
                    in_header.nodeid,
                    ioctl_in.fh,
                    ioctl_in.flags,
                    ioctl_in.cmd,
                    ioctl_in.arg,
                    &ioctl_data,
                    ioctl_in.out_size,
                )
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;
                    return;
                }

                Ok(reply_ioctl) => reply_ioctl,
            };

            let ioctl_out = fuse_ioctl_out {
                result: reply_ioctl.result,
                flags: reply_ioctl.flags,
                in_iovs: reply_ioctl.in_iovs,
                out_iovs: reply_ioctl.out_iovs,
            };

            let out_len = FUSE_OUT_HEADER_SIZE + FUSE_IOCTL_OUT_SIZE + reply_ioctl.data.len();
            let out_header = fuse_out_header {
                len: out_len as u32,
                error: 0,
                unique: request.unique,
            };

            let mut payload = Vec::with_capacity(out_len);
            get_bincode_config()
                .serialize_into(&mut payload, &out_header)
                .expect("won't happened");
            get_bincode_config()
                .serialize_into(&mut payload, &ioctl_out)
                .expect("won't happened");
            payload.extend_from_slice(&reply_ioctl.data);

            let _ = resp_sender.send(Either::Left(payload)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_poll(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let poll_in = match get_bincode_config().deserialize::<fuse_poll_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_poll_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(poll_in) => poll_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        let notify = self.get_notify();

        spawn(debug_span!("fuse_poll"), async move {
            debug!(
                "poll unique {} inode {} {:?}",
                request.unique, in_header.nodeid, poll_in
            );

            let kh = if poll_in.flags & FUSE_POLL_SCHEDULE_NOTIFY > 0 {
                Some(poll_in.kh)
            } else {
                None
            };

            let reply_poll = match fs
                .poll(
                    request,
                    in_header.nodeid,
                    poll_in.fh,
                    kh,
                    poll_in.flags,
                    poll_in.events,
                    &notify,
                )
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(reply_poll) => reply_poll,
            };

            let poll_out: fuse_poll_out = reply_poll.into();

            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_POLL_OUT_SIZE) as u32,
                error: 0,
                unique: request.unique,
            };

            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_POLL_OUT_SIZE);

            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("won't happened");
            get_bincode_config()
                .serialize_into(&mut data, &poll_out)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_notify_reply(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        mut data: &[u8],
        fs: &Arc<FS>,
    ) {
        let resp_sender = self.response_sender().clone();

        let notify_retrieve_in =
            match get_bincode_config().deserialize::<fuse_notify_retrieve_in>(data) {
                Err(err) => {
                    error!(
                        "deserialize fuse_notify_retrieve_in failed {}, request unique {}",
                        err, request.unique
                    );

                    // TODO need to reply or not?
                    return;
                }

                Ok(notify_retrieve_in) => notify_retrieve_in,
            };

        data = &data[FUSE_NOTIFY_RETRIEVE_IN_SIZE..];

        if data.len() < notify_retrieve_in.size as usize {
            error!(
                "fuse_notify_retrieve unique {} data size is not right",
                request.unique
            );

            // TODO need to reply or not?
            return;
        }

        let data = data[..notify_retrieve_in.size as usize].to_vec();

        let fs = fs.clone();

        spawn(debug_span!("fuse_notify_reply"), async move {
            if let Err(err) = fs
                .notify_reply(
                    request,
                    in_header.nodeid,
                    notify_retrieve_in.offset,
                    data.into(),
                )
                .await
            {
                reply_error_in_place(err, request, resp_sender).await;
            }
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_batch_forget(
        &mut self,
        request: Request,
        _in_header: fuse_in_header,
        mut data: &[u8],
        fs: &Arc<FS>,
    ) {
        let batch_forget_in = match get_bincode_config().deserialize::<fuse_batch_forget_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_batch_forget_in failed {}, request unique {}",
                    err, request.unique
                );

                // no need to reply
                return;
            }

            Ok(batch_forget_in) => batch_forget_in,
        };

        let mut forgets = vec![];

        data = &data[FUSE_BATCH_FORGET_IN_SIZE..];

        // TODO if has less data, should I return error?
        while data.len() >= FUSE_FORGET_ONE_SIZE {
            match get_bincode_config().deserialize::<fuse_forget_one>(data) {
                Err(err) => {
                    error!(
                        "deserialize fuse_batch_forget_in body fuse_forget_one failed {}, request unique {}",
                        err, request.unique
                    );

                    // no need to reply
                    return;
                }

                Ok(forget_one) => {
                    data = &data[FUSE_FORGET_ONE_SIZE..];

                    forgets.push(forget_one);
                }
            }
        }

        if forgets.len() != batch_forget_in.count as usize {
            error!(
                "fuse_forget_one count != fuse_batch_forget_in.count, request unique {}",
                request.unique
            );

            return;
        }

        let fs = fs.clone();

        spawn(debug_span!("fuse_batch_forget"), async move {
            let inodes = forgets
                .into_iter()
                .map(|forget_one| (forget_one.nodeid, forget_one._nlookup))
                .collect::<Vec<_>>();

            debug!("batch_forget unique {} inodes {:?}", request.unique, inodes);

            fs.batch_forget(request, &inodes).await
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_fallocate(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let fallocate_in = match get_bincode_config().deserialize::<fuse_fallocate_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_fallocate_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(fallocate_in) => fallocate_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_fallocate"), async move {
            debug!(
                "fallocate unique {} inode {} {:?}",
                request.unique, in_header.nodeid, fallocate_in
            );

            let resp_value = if let Err(err) = fs
                .fallocate(
                    request,
                    in_header.nodeid,
                    fallocate_in.fh,
                    fallocate_in.offset,
                    fallocate_in.length,
                    fallocate_in.mode,
                )
                .await
            {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_readdirplus(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let readdirplus_in = match get_bincode_config().deserialize::<fuse_read_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_read_in in readdirplus failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(readdirplus_in) => readdirplus_in,
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_readdirplus"), async move {
            let reply_guard = match fs.reserve_reply_memory(u64::from(readdirplus_in.size)) {
                Ok(guard) => guard,
                Err(error) => {
                    reply_error_in_place(error, request, resp_sender).await;
                    return;
                }
            };
            debug!(
                "readdirplus unique {} parent {} {:?}",
                request.unique, in_header.nodeid, readdirplus_in
            );

            let directory_plus = match fs
                .readdirplus(
                    request,
                    in_header.nodeid,
                    readdirplus_in.fh,
                    readdirplus_in.offset,
                    readdirplus_in.lock_owner,
                )
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(directory_plus) => directory_plus,
            };

            let max_size = readdirplus_in.size as usize;

            // Pre-allocate buffer with header space at the beginning to avoid multi-allocation
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + max_size);
            // Reserve space for header (will be filled later when we know the final size)
            data.resize(FUSE_OUT_HEADER_SIZE, 0);

            let entries = directory_plus.entries;
            let mut entries = pin!(entries);

            while let Some(entry) = entries.next().await {
                let entry = match entry {
                    Err(err) => {
                        reply_error_in_place(err, request, resp_sender).await;

                        return;
                    }

                    Ok(entry) => entry,
                };

                let name = &entry.name;

                let dir_entry_size = FUSE_DIRENTPLUS_SIZE + name.len();

                let padding_size = get_padding_size(dir_entry_size);

                // Check against max_size (entry_data portion only)
                if data.len() - FUSE_OUT_HEADER_SIZE + dir_entry_size + padding_size > max_size {
                    break;
                }

                let attr = entry.attr;

                let dir_entry = fuse_direntplus {
                    entry_out: fuse_entry_out {
                        nodeid: attr.ino,
                        generation: entry.generation,
                        entry_valid: entry.entry_ttl.as_secs(),
                        attr_valid: entry.attr_ttl.as_secs(),
                        entry_valid_nsec: entry.entry_ttl.subsec_nanos(),
                        attr_valid_nsec: entry.attr_ttl.subsec_nanos(),
                        attr: attr.into(),
                    },
                    dirent: fuse_dirent {
                        ino: entry.inode,
                        off: entry.offset as u64,
                        namelen: name.len() as u32,
                        // learn from fuse-rs and golang bazil.org fuse DirentType
                        r#type: mode_from_kind_and_perm(entry.kind, 0) >> 12,
                    },
                };

                get_bincode_config()
                    .serialize_into(&mut data, &dir_entry)
                    .expect("won't happened");

                data.extend_from_slice(name.as_bytes());

                // padding
                data.resize(data.len() + padding_size, 0);
            }

            // Now fill in the header at the beginning
            let out_header = fuse_out_header {
                len: data.len() as u32,
                error: 0,
                unique: request.unique,
            };

            // Overwrite the reserved header space
            get_bincode_config()
                .serialize_into(&mut data[..FUSE_OUT_HEADER_SIZE], &out_header)
                .expect("won't happened");

            let data = crate::raw::reply::own_reply_buffer(data, reply_guard);
            let _ = resp_sender.send(Either::Right((Vec::new(), data))).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_rename2(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        mut data: &[u8],
        fs: &Arc<FS>,
    ) {
        let rename2_in = match get_bincode_config().deserialize::<fuse_rename2_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_rename2_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(rename2_in) => rename2_in,
        };

        data = &data[FUSE_RENAME2_IN_SIZE..];

        let (old_name, index) = match get_first_null_position(data) {
            None => {
                error!(
                    "fuse_rename2_in body doesn't have null, request unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => (OsString::from_vec(data[..index].to_vec()), index),
        };

        data = &data[index + 1..];

        let new_name = match get_first_null_position(data) {
            None => {
                error!(
                    "fuse_rename2_in body doesn't have second null, request unique {}",
                    request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Some(index) => OsString::from_vec(data[..index].to_vec()),
        };

        let mut resp_sender = self.response_sender().clone();
        let fs = fs.clone();

        spawn(debug_span!("fuse_rename2"), async move {
            debug!(
                "rename2 unique {} parent {} name {:?} new parent {} new name {:?} flags {}",
                request.unique,
                in_header.nodeid,
                old_name,
                rename2_in.newdir,
                new_name,
                rename2_in.flags
            );

            let resp_value = if let Err(err) = fs
                .rename2(
                    request,
                    in_header.nodeid,
                    &old_name,
                    rename2_in.newdir,
                    &new_name,
                    rename2_in.flags,
                )
                .await
            {
                err.into()
            } else {
                0
            };

            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: resp_value,
                unique: request.unique,
            };

            let data = get_bincode_config()
                .serialize(&out_header)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_lseek(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let mut resp_sender = self.response_sender().clone();

        let lseek_in = match get_bincode_config().deserialize::<fuse_lseek_in>(data) {
            Err(err) => {
                error!(
                    "deserialize fuse_lseek_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(lseek_in) => lseek_in,
        };

        let fs = fs.clone();

        spawn(debug_span!("fuse_lseek"), async move {
            debug!(
                "lseek unique {} inode {} {:?}",
                request.unique, in_header.nodeid, lseek_in
            );

            let reply_lseek = match fs
                .lseek(
                    request,
                    in_header.nodeid,
                    lseek_in.fh,
                    lseek_in.offset,
                    lseek_in.whence,
                )
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(reply_lseek) => reply_lseek,
            };

            let lseek_out: fuse_lseek_out = reply_lseek.into();

            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_LSEEK_OUT_SIZE) as u32,
                error: 0,
                unique: request.unique,
            };

            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_OPEN_OUT_SIZE);

            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("won't happened");
            get_bincode_config()
                .serialize_into(&mut data, &lseek_out)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }

    #[instrument(skip(self, data, fs))]
    async fn handle_copy_file_range(
        &mut self,
        request: Request,
        in_header: fuse_in_header,
        data: &[u8],
        fs: &Arc<FS>,
    ) {
        let mut resp_sender = self.response_sender().clone();

        let copy_file_range_in = match get_bincode_config()
            .deserialize::<fuse_copy_file_range_in>(data)
        {
            Err(err) => {
                error!(
                    "deserialize fuse_copy_file_range_in failed {}, request unique {}",
                    err, request.unique
                );

                reply_error_in_place(libc::EINVAL.into(), request, self.response_sender()).await;

                return;
            }

            Ok(copy_file_range_in) => copy_file_range_in,
        };

        let fs = fs.clone();

        spawn(debug_span!("fuse_copy_file_range"), async move {
            debug!(
                "reply_copy_file_range unique {} inode {} {:?}",
                request.unique, in_header.nodeid, copy_file_range_in
            );

            let reply_copy_file_range = match fs
                .copy_file_range(
                    request,
                    in_header.nodeid,
                    copy_file_range_in.fh_in,
                    copy_file_range_in.off_in,
                    copy_file_range_in.nodeid_out,
                    copy_file_range_in.fh_out,
                    copy_file_range_in.off_out,
                    copy_file_range_in.len,
                    copy_file_range_in.flags,
                )
                .await
            {
                Err(err) => {
                    reply_error_in_place(err, request, resp_sender).await;

                    return;
                }

                Ok(reply_copy_file_range) => reply_copy_file_range,
            };

            let write_out: fuse_write_out = reply_copy_file_range.into();

            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_WRITE_OUT_SIZE) as u32,
                error: 0,
                unique: request.unique,
            };

            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_WRITE_OUT_SIZE);

            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("won't happened");
            get_bincode_config()
                .serialize_into(&mut data, &write_out)
                .expect("won't happened");

            let _ = resp_sender.send(Either::Left(data)).await;
        });
    }
}
