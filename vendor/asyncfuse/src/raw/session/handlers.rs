//! Handler functions for FUSE operations.
//!
//! Each `handle_*_inline` function processes a specific FUSE opcode in the worker pool.
//! The `WorkItem` (containing `InflightGuard`) is held until the FS operation completes,
//! ensuring backpressure is accurate.

use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::pin::pin;

use bincode::Options;
use futures_util::future::Either;
use futures_util::stream::StreamExt;
use tracing::{debug, error};

use crate::helper::*;
use crate::notify::Notify;
use crate::raw::abi::*;
use crate::raw::filesystem::Filesystem;
use crate::raw::request::Request;
use crate::{Errno, SetAttr};

use super::utils::{apply_direct_io, reply_error_in_worker};
use super::worker::{DispatchCtx, WorkItem};

#[cfg(all(test, feature = "tokio-runtime"))]
mod budget_reply_tests {
    use super::*;
    use crate::raw::reply::{DirectoryEntry, ReplyDirectory, ReplyInit, ReplyMemoryGuard};
    use std::num::NonZeroU32;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    #[derive(Debug)]
    struct Permit(Arc<AtomicUsize>);
    impl Drop for Permit {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    #[derive(Debug)]
    struct TestFs {
        owned: Arc<AtomicUsize>,
        calls: AtomicUsize,
        deny: AtomicBool,
        fail: AtomicBool,
        block: AtomicBool,
        entered: tokio::sync::Notify,
    }
    impl Filesystem for TestFs {
        async fn init(&self, _: Request) -> crate::Result<ReplyInit> {
            Ok(ReplyInit {
                max_write: NonZeroU32::new(4096).unwrap(),
            })
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
        fn reserve_reply_memory(&self, _: u64) -> crate::Result<Option<ReplyMemoryGuard>> {
            if self.deny.load(Ordering::SeqCst) {
                return Err(libc::ENOMEM.into());
            }
            self.owned.fetch_add(1, Ordering::SeqCst);
            Ok(Some(Arc::new(Permit(self.owned.clone()))))
        }
        async fn readdir<'a>(
            &'a self,
            _: Request,
            _: crate::Inode,
            _: u64,
            _: i64,
        ) -> crate::Result<
            ReplyDirectory<
                impl futures_util::Stream<Item = crate::Result<DirectoryEntry>> + Send + 'a,
            >,
        > {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.block.load(Ordering::SeqCst) {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            let entries = if self.fail.load(Ordering::SeqCst) {
                vec![Err(libc::EIO.into())]
            } else {
                vec![Ok(DirectoryEntry {
                    inode: 2,
                    kind: crate::FileType::RegularFile,
                    name: OsString::from_vec(vec![0xff, b'x']),
                    offset: 3,
                })]
            };
            Ok(ReplyDirectory {
                entries: futures_util::stream::iter(entries),
            })
        }
    }
    fn fixture() -> (
        Arc<DispatchCtx<TestFs>>,
        futures_channel::mpsc::UnboundedReceiver<crate::raw::FuseData>,
    ) {
        let fs = Arc::new(TestFs {
            owned: Arc::new(AtomicUsize::new(0)),
            calls: AtomicUsize::new(0),
            deny: AtomicBool::new(false),
            fail: AtomicBool::new(false),
            block: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
        });
        let (sender, receiver) = futures_channel::mpsc::unbounded();
        let ctx = Arc::new(DispatchCtx {
            fs,
            resp: vec![sender],
            direct_io: false,
            force_readdir_plus: false,
            kernel_writeback_permissions: false,
            _inflight: Arc::new(AtomicUsize::new(0)),
            _inflight_notify: Arc::new(async_notify::Notify::new()),
            response_memory: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            readonly_reply_tracker: None,
            open_controller: crate::raw::session::owned_open_queue::ControllerHandle::empty(),
        });
        (ctx, receiver)
    }
    fn request() -> WorkItem {
        let mut body = vec![0; std::mem::size_of::<fuse_read_in>()];
        body[..8].copy_from_slice(&1u64.to_le_bytes());
        body[16..20].copy_from_slice(&4096u32.to_le_bytes());
        WorkItem {
            unique: 1,
            opcode: fuse_opcode::FUSE_READDIR as u32,
            in_header: super::super::utils::InHeaderLite {
                nodeid: 1,
                uid: 0,
                gid: 0,
                pid: 0,
            },
            data: bytes::Bytes::from(body),
            _inflight_guard: None,
            _memory_guard: None,
            _response_registration: None,
        }
    }
    #[tokio::test]
    async fn serialized_directory_body_keeps_admission_through_slow_response_consumers() {
        let (ctx, mut receiver) = fixture();
        handle_readdir_inline(&ctx, request()).await;
        assert_eq!(ctx.fs.owned.load(Ordering::SeqCst), 1);
        let Either::Right((_, body)) = receiver.next().await.unwrap() else {
            panic!("expected successful body")
        };
        assert!(body.windows(2).any(|window| window == [0xff, b'x']));
        let consumer = body.clone();
        drop(body);
        assert_eq!(ctx.fs.owned.load(Ordering::SeqCst), 1);
        drop(consumer);
        assert_eq!(ctx.fs.owned.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn directory_admission_failure_precedes_fetch_and_stream_failure_releases_owner() {
        let (ctx, mut receiver) = fixture();
        ctx.fs.deny.store(true, Ordering::SeqCst);
        handle_readdir_inline(&ctx, request()).await;
        assert!(matches!(receiver.next().await.unwrap(), Either::Left(_)));
        assert_eq!(ctx.fs.calls.load(Ordering::SeqCst), 0);
        ctx.fs.deny.store(false, Ordering::SeqCst);
        ctx.fs.fail.store(true, Ordering::SeqCst);
        handle_readdir_inline(&ctx, request()).await;
        assert!(matches!(receiver.next().await.unwrap(), Either::Left(_)));
        assert_eq!(ctx.fs.owned.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn cancelling_directory_fetch_releases_the_real_handler_reservation() {
        let (ctx, _receiver) = fixture();
        ctx.fs.block.store(true, Ordering::SeqCst);
        let spawned = ctx.clone();
        let task = tokio::spawn(async move { handle_readdir_inline(&spawned, request()).await });
        ctx.fs.entered.notified().await;
        assert_eq!(ctx.fs.owned.load(Ordering::SeqCst), 1);
        task.abort();
        let _ = task.await;
        assert_eq!(ctx.fs.owned.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn queued_fixed_header_keeps_request_control_after_handler_and_registry_leave() {
        let (ctx, mut receiver) = fixture();
        let mut item = request();
        ctx.fs.owned.fetch_add(1, Ordering::SeqCst);
        item._memory_guard = Some(Arc::new(Permit(ctx.fs.owned.clone())));
        ctx.register_memory(&mut item);
        // Default TestFs getattr rejects. Even this fixed errno/header reply
        // must retain its request/control reservation through response send.
        handle_getattr_inline(&ctx, item).await;
        assert!(ctx.response_memory.lock().unwrap().is_empty());
        assert_eq!(ctx.fs.owned.load(Ordering::SeqCst), 1);
        let Either::Right((_, body)) = receiver.next().await.unwrap() else {
            panic!("expected owned header")
        };
        assert_eq!(body.len(), FUSE_OUT_HEADER_SIZE);
        let consumer = body.clone();
        drop(body);
        assert_eq!(ctx.fs.owned.load(Ordering::SeqCst), 1);
        drop(consumer);
        assert_eq!(ctx.fs.owned.load(Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
mod umask_tests {
    use super::super::utils::InHeaderLite;
    use super::*;
    use crate::path::InodePathBridge;
    use crate::path::PathFilesystem;
    use crate::raw::logfs::LoggingFileSystem;
    use crate::raw::object_safe_filesystem::ObjectSafeFilesystem;
    use crate::raw::reply::{ReplyCreated, ReplyEntry, ReplyInit};
    use crate::Result;
    use bytes::Bytes;
    use std::ffi::OsStr;
    use std::sync::{atomic::AtomicUsize, Arc, Mutex};

    #[derive(Clone, Debug, Default)]
    struct Recorder(Arc<Mutex<Vec<(u32, u32, u32)>>>);

    macro_rules! unsupported_file_locks {
        ($target:ty, $reply:ty) => {
            #[cfg(feature = "file-lock")]
            async fn getlk(
                &self,
                _: Request,
                _: $target,
                _: u64,
                _: u64,
                _: u64,
                _: u64,
                _: u32,
                _: u32,
            ) -> Result<$reply> {
                Err(libc::ENOSYS.into())
            }
            #[cfg(feature = "file-lock")]
            async fn setlk(
                &self,
                _: Request,
                _: $target,
                _: u64,
                _: u64,
                _: u64,
                _: u64,
                _: u32,
                _: u32,
                _: bool,
            ) -> Result<()> {
                Err(libc::ENOSYS.into())
            }
        };
    }

    impl Filesystem for Recorder {
        unsupported_file_locks!(u64, crate::raw::reply::ReplyLock);
        async fn init(&self, _: Request) -> Result<ReplyInit> {
            Ok(ReplyInit::default())
        }
        async fn destroy(&self, _: Request) {}
        async fn mknod_with_umask(
            &self,
            _: Request,
            _: u64,
            _: &OsStr,
            mode: u32,
            umask: u32,
            rdev: u32,
        ) -> Result<ReplyEntry> {
            self.0.lock().unwrap().push((mode, umask, rdev));
            Err(libc::EACCES.into())
        }
        async fn create_with_umask(
            &self,
            _: Request,
            _: u64,
            _: &OsStr,
            mode: u32,
            umask: u32,
            flags: u32,
        ) -> Result<ReplyCreated> {
            self.0.lock().unwrap().push((mode, umask, flags));
            Err(libc::EACCES.into())
        }
    }

    impl PathFilesystem for Recorder {
        unsupported_file_locks!(Option<&OsStr>, crate::path::reply::ReplyLock);
        async fn init(&self, _: Request) -> Result<crate::path::reply::ReplyInit> {
            Err(libc::ENOSYS.into())
        }
        async fn destroy(&self, _: Request) {}
        async fn mknod_with_umask(
            &self,
            _: Request,
            parent: &OsStr,
            _: &OsStr,
            mode: u32,
            umask: u32,
            rdev: u32,
        ) -> Result<crate::path::reply::ReplyEntry> {
            assert_eq!(parent, OsStr::new("/"));
            self.0.lock().unwrap().push((mode, umask, rdev));
            Err(libc::EACCES.into())
        }
        async fn create_with_umask(
            &self,
            _: Request,
            parent: &OsStr,
            _: &OsStr,
            mode: u32,
            umask: u32,
            flags: u32,
        ) -> Result<crate::path::reply::ReplyCreated> {
            assert_eq!(parent, OsStr::new("/"));
            self.0.lock().unwrap().push((mode, umask, flags));
            Err(libc::EACCES.into())
        }
    }

    #[derive(Clone, Debug, Default)]
    struct ExistingImplementation(Recorder);

    impl Filesystem for ExistingImplementation {
        unsupported_file_locks!(u64, crate::raw::reply::ReplyLock);
        async fn init(&self, _: Request) -> Result<ReplyInit> {
            Err(libc::ENOSYS.into())
        }
        async fn destroy(&self, _: Request) {}
        async fn mknod(
            &self,
            _: Request,
            _: u64,
            name: &OsStr,
            mode: u32,
            rdev: u32,
        ) -> Result<ReplyEntry> {
            assert_eq!(name.as_bytes(), b"raw-\xff");
            self.0 .0.lock().unwrap().push((mode, 0, rdev));
            Err(libc::EACCES.into())
        }
        async fn create(
            &self,
            _: Request,
            _: u64,
            name: &OsStr,
            mode: u32,
            flags: u32,
        ) -> Result<ReplyCreated> {
            assert_eq!(name.as_bytes(), b"raw-\xff");
            self.0 .0.lock().unwrap().push((mode, 0, flags));
            Err(libc::EACCES.into())
        }
    }

    impl PathFilesystem for ExistingImplementation {
        unsupported_file_locks!(Option<&OsStr>, crate::path::reply::ReplyLock);
        async fn init(&self, _: Request) -> Result<crate::path::reply::ReplyInit> {
            Err(libc::ENOSYS.into())
        }
        async fn destroy(&self, _: Request) {}
        async fn mknod(
            &self,
            _: Request,
            parent: &OsStr,
            name: &OsStr,
            mode: u32,
            rdev: u32,
        ) -> Result<crate::path::reply::ReplyEntry> {
            assert_eq!(parent, OsStr::new("/"));
            assert_eq!(name.as_bytes(), b"raw-\xff");
            self.0 .0.lock().unwrap().push((mode, 0, rdev));
            Err(libc::EACCES.into())
        }
        async fn create(
            &self,
            _: Request,
            parent: &OsStr,
            name: &OsStr,
            mode: u32,
            flags: u32,
        ) -> Result<crate::path::reply::ReplyCreated> {
            assert_eq!(parent, OsStr::new("/"));
            assert_eq!(name.as_bytes(), b"raw-\xff");
            self.0 .0.lock().unwrap().push((mode, 0, flags));
            Err(libc::EACCES.into())
        }
    }

    fn body(values: [u32; 4]) -> Bytes {
        let mut body: Vec<_> = values.into_iter().flat_map(u32::to_le_bytes).collect();
        body.extend_from_slice(b"raw-\xff\0");
        Bytes::from(body)
    }

    async fn dispatch<FS: Filesystem + Send + Sync + 'static>(fs: FS) {
        let (tx, mut rx) = futures_channel::mpsc::unbounded();
        let context = DispatchCtx {
            fs: Arc::new(fs),
            resp: vec![tx],
            direct_io: false,
            force_readdir_plus: false,
            kernel_writeback_permissions: false,
            _inflight: Arc::new(AtomicUsize::new(0)),
            _inflight_notify: Arc::new(async_notify::Notify::new()),
            response_memory: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            readonly_reply_tracker: None,
            open_controller: crate::raw::session::owned_open_queue::ControllerHandle::empty(),
        };
        let item = |opcode, unique, data| WorkItem {
            opcode,
            unique,
            data,
            _inflight_guard: None,
            _memory_guard: None,
            _response_registration: None,
            in_header: InHeaderLite {
                nodeid: 1,
                uid: 1000,
                gid: 1001,
                pid: 42,
            },
        };
        handle_mknod_inline(
            &context,
            item(
                fuse_opcode::FUSE_MKNOD as u32,
                1,
                body([0o100666, 0x1234, 0o077, 0]),
            ),
        )
        .await;
        handle_create_inline(
            &context,
            item(
                fuse_opcode::FUSE_CREATE as u32,
                2,
                body([libc::O_RDWR as u32, 0o100666, 0o027, 0]),
            ),
        )
        .await;
        for _ in 0..2 {
            let response = rx.next().await.unwrap();
            let bytes = match response {
                Either::Left(bytes) => bytes,
                Either::Right((bytes, _)) => bytes,
            };
            assert_eq!(
                i32::from_le_bytes(bytes[4..8].try_into().unwrap()),
                -libc::EACCES
            );
        }
    }

    fn expected() -> Vec<(u32, u32, u32)> {
        vec![
            (0o100666, 0o077, 0x1234),
            (0o100666, 0o027, libc::O_RDWR as u32),
        ]
    }

    #[tokio::test]
    async fn new_entry_points_forward_to_existing_raw_and_path_implementations() {
        let filesystem = ExistingImplementation::default();
        dispatch(LoggingFileSystem::new(filesystem.clone())).await;
        dispatch(InodePathBridge::new(filesystem.clone())).await;
        let forwarded: Vec<_> = expected()
            .into_iter()
            .map(|(mode, _, last)| (mode, 0, last))
            .collect();
        assert_eq!(
            *filesystem.0 .0.lock().unwrap(),
            [forwarded.clone(), forwarded].concat()
        );
    }

    #[tokio::test]
    async fn spawn_abi_preserves_umask_and_unmasked_mode() {
        use super::super::Session;
        let recorder = Recorder::default();
        let filesystem = Arc::new(LoggingFileSystem::new(recorder.clone()));
        let mut session = Session::new(crate::MountOptions::default());
        let mut replies = session.response_receivers[0].take().unwrap();
        let header = |opcode: u32, unique: u64| {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&0u32.to_le_bytes());
            bytes.extend_from_slice(&opcode.to_le_bytes());
            bytes.extend_from_slice(&unique.to_le_bytes());
            bytes.extend_from_slice(&1u64.to_le_bytes());
            for word in [1000u32, 1001, 42, 0] {
                bytes.extend_from_slice(&word.to_le_bytes());
            }
            get_bincode_config()
                .deserialize::<fuse_in_header>(&bytes)
                .unwrap()
        };
        session
            .handle_mknod(
                Request {
                    unique: 1,
                    ..Request::default()
                },
                header(fuse_opcode::FUSE_MKNOD as u32, 1),
                &body([0o100666, 0x1234, 0o077, 0]),
                &filesystem,
            )
            .await;
        session
            .handle_create(
                Request {
                    unique: 2,
                    ..Request::default()
                },
                header(fuse_opcode::FUSE_CREATE as u32, 2),
                &body([libc::O_RDWR as u32, 0o100666, 0o027, 0]),
                &filesystem,
            )
            .await;
        for _ in 0..2 {
            let response = tokio::time::timeout(std::time::Duration::from_secs(1), replies.next())
                .await
                .unwrap()
                .unwrap();
            let bytes = match response {
                Either::Left(bytes) => bytes,
                Either::Right((bytes, _)) => bytes,
            };
            assert_eq!(
                i32::from_le_bytes(bytes[4..8].try_into().unwrap()),
                -libc::EACCES
            );
        }
        assert_eq!(*recorder.0.lock().unwrap(), expected());
    }

    #[tokio::test]
    async fn worker_abi_preserves_unmasked_mode_umask_flags_and_rdev_through_logging() {
        let recorder = Recorder::default();
        dispatch(LoggingFileSystem::new(recorder.clone())).await;
        assert_eq!(*recorder.0.lock().unwrap(), expected());
    }

    #[tokio::test]
    async fn worker_abi_preserves_umask_through_inode_path_bridge() {
        let recorder = Recorder::default();
        dispatch(InodePathBridge::new(recorder.clone())).await;
        assert_eq!(*recorder.0.lock().unwrap(), expected());
    }

    #[tokio::test]
    async fn object_safe_blanket_preserves_umask() {
        let recorder = Recorder::default();
        let filesystem: &dyn ObjectSafeFilesystem = &recorder;
        assert!(filesystem
            .mknod_with_umask(
                Request::default(),
                1,
                OsStr::new("node"),
                0o100666,
                0o077,
                0x1234
            )
            .await
            .is_err());
        assert!(filesystem
            .create_with_umask(
                Request::default(),
                1,
                OsStr::new("file"),
                0o100666,
                0o027,
                libc::O_RDWR as u32
            )
            .await
            .is_err());
        assert_eq!(*recorder.0.lock().unwrap(), expected());
    }

    #[cfg(all(
        target_os = "linux",
        feature = "tokio-runtime",
        not(feature = "io-uring-runtime"),
        not(feature = "async-io-runtime")
    ))]
    #[tokio::test]
    async fn init_abi_advertises_dont_mask_only_when_requested_and_supported() {
        use super::super::Session;
        use crate::raw::connection::FuseConnection;
        use std::io::Read;
        use std::os::unix::net::UnixStream;
        for (requested, supported) in [(false, false), (false, true), (true, false), (true, true)] {
            let (transport, mut peer) = UnixStream::pair().unwrap();
            peer.set_read_timeout(Some(std::time::Duration::from_secs(1)))
                .unwrap();
            let connection = FuseConnection::from_test_fd(transport.into());
            let mut options = crate::MountOptions::default();
            options.dont_mask(requested);
            let mut session = Session::new(options);
            let flags = if supported { FUSE_DONT_MASK } else { 0 };
            let body: Vec<_> = [FUSE_KERNEL_VERSION, FUSE_KERNEL_MINOR_VERSION, 4096, flags]
                .into_iter()
                .flat_map(u32::to_le_bytes)
                .collect();
            session
                .handle_init(Request::default(), &body, &connection, &Recorder::default())
                .await
                .unwrap();
            let mut reply = vec![0; FUSE_OUT_HEADER_SIZE + FUSE_INIT_OUT_SIZE];
            peer.read_exact(&mut reply).unwrap();
            let negotiated = u32::from_le_bytes(
                reply[FUSE_OUT_HEADER_SIZE + 12..FUSE_OUT_HEADER_SIZE + 16]
                    .try_into()
                    .unwrap(),
            );
            assert_eq!(negotiated & FUSE_DONT_MASK != 0, requested && supported);
        }
    }
}

pub(super) async fn handle_lookup_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let name = match get_first_null_position(&item.data) {
        None => {
            debug!(unique = item.unique, "lookup body has no null (worker)");
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(idx) => OsString::from_vec(item.data[..idx].to_vec()),
    };
    let parent = item.in_header.nodeid;
    debug!(unique = item.unique, parent, ?name, "lookup (worker)");
    let data = match ctx.fs.lookup(Request::from(&item), parent, &name).await {
        Err(err) => reply_error_in_worker(err, item.unique).expect("serialize out_header"),
        Ok(entry) => {
            let entry_out: fuse_entry_out = entry.into();
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &entry_out)
                .expect("serialize entry");
            data
        }
    };
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_forget_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let forget_in = match get_bincode_config().deserialize::<fuse_forget_in>(&item.data) {
        Err(err) => {
            error!(
                "deserialize fuse_forget_in failed {}, request unique {}",
                err, item.unique
            );
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        nlookup = forget_in.nlookup,
        "forget (worker)"
    );

    ctx.fs
        .forget(
            Request::from(&item),
            item.in_header.nodeid,
            forget_in.nlookup,
        )
        .await;
}

pub(super) async fn handle_getattr_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let getattr_in = match get_bincode_config().deserialize::<fuse_getattr_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_getattr_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };
    let fh = if getattr_in.getattr_flags & FUSE_GETATTR_FH > 0 {
        Some(getattr_in.fh)
    } else {
        None
    };
    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        "getattr (worker)"
    );
    let data = match ctx
        .fs
        .getattr(
            Request::from(&item),
            item.in_header.nodeid,
            fh,
            getattr_in.getattr_flags,
        )
        .await
    {
        Err(err) => reply_error_in_worker(err, item.unique).expect("serialize out_header"),
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
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ATTR_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &attr_out)
                .expect("serialize attr_out");
            data
        }
    };
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_open_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let open_in = match get_bincode_config().deserialize::<fuse_open_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_open_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };
    let direct_io = ctx.direct_io;
    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        flags = open_in.flags,
        "open (worker)"
    );
    let data = match ctx
        .fs
        .open(Request::from(&item), item.in_header.nodeid, open_in.flags)
        .await
    {
        Err(err) => reply_error_in_worker(err, item.unique).expect("serialize out_header"),
        Ok(opened) => {
            let mut open_out: fuse_open_out = opened.into();
            apply_direct_io(&mut open_out.open_flags, direct_io);
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_OPEN_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_OPEN_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &open_out)
                .expect("serialize open_out");
            data
        }
    };
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_read_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let read_in = match get_bincode_config().deserialize::<fuse_read_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_read_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };
    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        size = read_in.size,
        offset = read_in.offset,
        "read (worker)"
    );
    let mut reply_data = match ctx
        .fs
        .read(
            Request::from(&item),
            item.in_header.nodeid,
            read_in.fh,
            read_in.offset,
            read_in.size,
        )
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(reply) => reply.data,
    };
    if reply_data.len() > read_in.size as usize {
        reply_data.truncate(read_in.size as usize);
    }
    let out_header = fuse_out_header {
        len: (FUSE_OUT_HEADER_SIZE + reply_data.len()) as u32,
        error: 0,
        unique: item.unique,
    };
    let mut data_buf = Vec::with_capacity(FUSE_OUT_HEADER_SIZE);
    get_bincode_config()
        .serialize_into(&mut data_buf, &out_header)
        .expect("serialize header");
    let _ = ctx
        .resp_for(item.unique)
        .unbounded_send(Either::Right((data_buf, reply_data.into())));
}

pub(super) async fn handle_write_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    if item.data.len() < FUSE_WRITE_IN_SIZE {
        // malformed
        let data =
            reply_error_in_worker(libc::EINVAL.into(), item.unique).expect("serialize out_header");
        let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        return;
    }
    let write_in =
        match get_bincode_config().deserialize::<fuse_write_in>(&item.data[..FUSE_WRITE_IN_SIZE]) {
            Err(err) => {
                debug!(
                    unique = item.unique,
                    "deserialize fuse_write_in failed {}", err
                );
                let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                    .expect("serialize out_header");
                let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
                return;
            }
            Ok(v) => v,
        };
    let payload = &item.data[FUSE_WRITE_IN_SIZE..];
    if write_in.size as usize != payload.len() {
        let data =
            reply_error_in_worker(libc::EINVAL.into(), item.unique).expect("serialize out_header");
        let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        return;
    }
    // Use Bytes::slice for zero-copy - creates a new Bytes sharing the same underlying data
    let payload_bytes = item.data.slice(FUSE_WRITE_IN_SIZE..);
    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        size = write_in.size,
        offset = write_in.offset,
        "write (worker)"
    );
    let write_out_data = match ctx
        .fs
        .write(
            Request::from(&item),
            item.in_header.nodeid,
            write_in.fh,
            write_in.offset,
            &payload_bytes,
            write_in.write_flags,
            write_in.flags,
        )
        .await
    {
        Err(err) => reply_error_in_worker(err, item.unique).expect("serialize out_header"),
        Ok(reply_write) => {
            let write_out: fuse_write_out = reply_write.into();
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_WRITE_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_WRITE_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &write_out)
                .expect("serialize write_out");
            data
        }
    };
    let _ = ctx
        .resp_for(item.unique)
        .unbounded_send(Either::Left(write_out_data));
}

pub(super) async fn handle_readdir_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    if ctx.force_readdir_plus {
        let data =
            reply_error_in_worker(libc::ENOSYS.into(), item.unique).expect("serialize out_header");
        let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        return;
    }

    let read_in = match get_bincode_config().deserialize::<fuse_read_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_read_in (readdir) failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };
    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh = read_in.fh,
        offset = read_in.offset,
        "readdir (worker)"
    );
    let reply_guard = match ctx.fs.reserve_reply_memory(u64::from(read_in.size)) {
        Ok(guard) => guard,
        Err(error) => {
            let data = reply_error_in_worker(error, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
    };
    let reply_readdir = match ctx
        .fs
        .readdir(
            Request::from(&item),
            item.in_header.nodeid,
            read_in.fh,
            read_in.offset as i64,
        )
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(r) => r,
    };
    let max_size = read_in.size as usize;
    let mut entry_data = Vec::with_capacity(max_size);
    let mut entries = pin!(reply_readdir.entries);
    while let Some(entry) = entries.next().await {
        let entry = match entry {
            Err(err) => {
                let out_header = fuse_out_header {
                    len: FUSE_OUT_HEADER_SIZE as u32,
                    error: err.into(),
                    unique: item.unique,
                };
                let data = get_bincode_config()
                    .serialize(&out_header)
                    .expect("serialize out_header");
                let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
                return;
            }
            Ok(e) => e,
        };
        let name = &entry.name;
        let dir_entry_size = FUSE_DIRENT_SIZE + name.len();
        let padding_size = get_padding_size(dir_entry_size);
        if entry_data.len() + dir_entry_size + padding_size > max_size {
            break;
        }
        let dir_entry = fuse_dirent {
            ino: entry.inode,
            off: entry.offset as u64,
            namelen: name.len() as u32,
            r#type: mode_from_kind_and_perm(entry.kind, 0) >> 12,
        };
        get_bincode_config()
            .serialize_into(&mut entry_data, &dir_entry)
            .expect("serialize dirent");
        entry_data.extend_from_slice(name.as_bytes());
        entry_data.resize(entry_data.len() + padding_size, 0);
    }
    let out_header = fuse_out_header {
        len: (FUSE_OUT_HEADER_SIZE + entry_data.len()) as u32,
        error: 0,
        unique: item.unique,
    };
    let mut data_buf = Vec::with_capacity(FUSE_OUT_HEADER_SIZE);
    get_bincode_config()
        .serialize_into(&mut data_buf, &out_header)
        .expect("serialize header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Right((
        data_buf,
        crate::raw::reply::own_reply_buffer(entry_data, reply_guard),
    )));
}

pub(super) async fn handle_setattr_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let setattr_in = match get_bincode_config().deserialize::<fuse_setattr_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_setattr_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };
    let mut set_attr = SetAttr::from(&setattr_in);
    set_attr.kernel_writeback_permissions = ctx.kernel_writeback_permissions;
    let fh = if setattr_in.valid & FATTR_FH > 0 {
        Some(setattr_in.fh)
    } else {
        None
    };
    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        "setattr (worker)"
    );
    let data = match ctx
        .fs
        .setattr(Request::from(&item), item.in_header.nodeid, fh, set_attr)
        .await
    {
        Err(err) => reply_error_in_worker(err, item.unique).expect("serialize out_header"),
        Ok(attr) => {
            let attr_out: fuse_attr_out = attr.into();
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_ATTR_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ATTR_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &attr_out)
                .expect("serialize attr_out");
            data
        }
    };
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_readlink_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        "readlink (worker)"
    );
    let data = match ctx
        .fs
        .readlink(Request::from(&item), item.in_header.nodeid)
        .await
    {
        Err(err) => {
            let out_header = fuse_out_header {
                len: FUSE_OUT_HEADER_SIZE as u32,
                error: err.into(),
                unique: item.unique,
            };
            Either::Left(
                get_bincode_config()
                    .serialize(&out_header)
                    .expect("serialize out_header"),
            )
        }
        Ok(data) => {
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + data.data.len()) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data_buf = Vec::with_capacity(FUSE_OUT_HEADER_SIZE);
            get_bincode_config()
                .serialize_into(&mut data_buf, &out_header)
                .expect("serialize header");
            Either::Right((data_buf, data.data.into()))
        }
    };
    let _ = ctx.resp_for(item.unique).unbounded_send(data);
}

pub(super) async fn handle_symlink_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let (name, first_null_index) = match get_first_null_position(&item.data) {
        None => {
            debug!(unique = item.unique, "symlink has no null (worker)");
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => (OsString::from_vec(item.data[..index].to_vec()), index),
    };
    let data = &item.data[first_null_index + 1..];
    let link_name = match get_first_null_position(data) {
        None => {
            debug!(unique = item.unique, "symlink has no second null (worker)");
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => OsString::from_vec(data[..index].to_vec()),
    };
    let parent = item.in_header.nodeid;
    debug!(
        unique = item.unique,
        parent,
        ?name,
        ?link_name,
        "symlink (worker)"
    );
    let data = match ctx
        .fs
        .symlink(Request::from(&item), parent, &name, &link_name)
        .await
    {
        Err(err) => reply_error_in_worker(err, item.unique).expect("serialize out_header"),
        Ok(entry) => {
            let entry_out: fuse_entry_out = entry.into();
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &entry_out)
                .expect("serialize entry");
            data
        }
    };
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_mknod_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let mknod_in = match get_bincode_config().deserialize::<fuse_mknod_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_mknod_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };
    let data = &item.data[FUSE_MKNOD_IN_SIZE..];
    let name = match get_first_null_position(data) {
        None => {
            debug!(
                unique = item.unique,
                "fuse_mknod_in body doesn't have null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => OsString::from_vec(data[..index].to_vec()),
    };
    debug!(
        unique = item.unique,
        parent = item.in_header.nodeid,
        ?name,
        ?mknod_in,
        "mknod (worker)"
    );
    match ctx
        .fs
        .mknod_with_umask(
            Request::from(&item),
            item.in_header.nodeid,
            &name,
            mknod_in.mode,
            mknod_in._umask,
            mknod_in.rdev,
        )
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
        Ok(entry) => {
            let entry_out: fuse_entry_out = entry.into();
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &entry_out)
                .expect("serialize entry");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
    }
}

pub(super) async fn handle_mkdir_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let mkdir_in = match get_bincode_config().deserialize::<fuse_mkdir_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_mkdir_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };
    let data = &item.data[FUSE_MKDIR_IN_SIZE..];
    let name = match get_first_null_position(data) {
        None => {
            debug!(
                unique = item.unique,
                "fuse_mkdir_in body doesn't have null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => OsString::from_vec(data[..index].to_vec()),
    };
    debug!(
        unique = item.unique,
        parent = item.in_header.nodeid,
        ?name,
        ?mkdir_in,
        "mkdir (worker)"
    );
    match ctx
        .fs
        .mkdir(
            Request::from(&item),
            item.in_header.nodeid,
            &name,
            mkdir_in.mode,
            mkdir_in.umask,
        )
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
        Ok(entry) => {
            let entry_out: fuse_entry_out = entry.into();
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &entry_out)
                .expect("serialize entry");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
    }
}

pub(super) async fn handle_unlink_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let name = match get_first_null_position(&item.data) {
        None => {
            debug!(
                unique = item.unique,
                "unlink body doesn't have null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => OsString::from_vec(item.data[..index].to_vec()),
    };
    debug!(
        unique = item.unique,
        parent = item.in_header.nodeid,
        ?name,
        "unlink (worker)"
    );
    let resp = if let Err(err) = ctx
        .fs
        .unlink(Request::from(&item), item.in_header.nodeid, &name)
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_rmdir_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let name = match get_first_null_position(&item.data) {
        None => {
            debug!(
                unique = item.unique,
                "rmdir body doesn't have null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => OsString::from_vec(item.data[..index].to_vec()),
    };
    debug!(
        unique = item.unique,
        parent = item.in_header.nodeid,
        ?name,
        "rmdir (worker)"
    );
    let resp = if let Err(err) = ctx
        .fs
        .rmdir(Request::from(&item), item.in_header.nodeid, &name)
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_rename_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let rename_in = match get_bincode_config().deserialize::<fuse_rename_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_rename_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };
    let mut data = &item.data[FUSE_RENAME_IN_SIZE..];
    let (name, first_null_index) = match get_first_null_position(data) {
        None => {
            debug!(
                unique = item.unique,
                "fuse_rename_in body doesn't have null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => (OsString::from_vec(data[..index].to_vec()), index),
    };
    data = &data[first_null_index + 1..];
    let new_name = match get_first_null_position(data) {
        None => {
            debug!(
                unique = item.unique,
                "fuse_rename_in body doesn't have second null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => OsString::from_vec(data[..index].to_vec()),
    };
    debug!(
        unique = item.unique,
        parent = item.in_header.nodeid,
        ?name,
        new_parent = rename_in.newdir,
        ?new_name,
        "rename (worker)"
    );
    let resp = if let Err(err) = ctx
        .fs
        .rename(
            Request::from(&item),
            item.in_header.nodeid,
            &name,
            rename_in.newdir,
            &new_name,
        )
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_link_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let link_in = match get_bincode_config().deserialize::<fuse_link_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_link_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };
    let data = &item.data[FUSE_LINK_IN_SIZE..];
    let name = match get_first_null_position(data) {
        None => {
            debug!(
                unique = item.unique,
                "fuse_link_in body doesn't have null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => OsString::from_vec(data[..index].to_vec()),
    };
    debug!(
        unique = item.unique,
        inode = link_in.oldnodeid,
        new_parent = item.in_header.nodeid,
        ?name,
        "link (worker)"
    );
    match ctx
        .fs
        .link(
            Request::from(&item),
            link_in.oldnodeid,
            item.in_header.nodeid,
            &name,
        )
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
        Ok(entry) => {
            let entry_out: fuse_entry_out = entry.into();
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &entry_out)
                .expect("serialize entry");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
    }
}

pub(super) async fn handle_statfs_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        "statfs (worker)"
    );
    let fs_stat = match ctx
        .fs
        .statfs(Request::from(&item), item.in_header.nodeid)
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(fs_stat) => fs_stat,
    };
    let statfs_out: fuse_statfs_out = fs_stat.into();
    let out_header = fuse_out_header {
        len: (FUSE_OUT_HEADER_SIZE + FUSE_STATFS_OUT_SIZE) as u32,
        error: 0,
        unique: item.unique,
    };
    let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_STATFS_OUT_SIZE);
    get_bincode_config()
        .serialize_into(&mut data, &out_header)
        .expect("serialize header");
    get_bincode_config()
        .serialize_into(&mut data, &statfs_out)
        .expect("serialize statfs_out");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_release_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let release_in = match get_bincode_config().deserialize::<fuse_release_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_release_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };
    let flush = release_in.release_flags & FUSE_RELEASE_FLUSH > 0;
    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh = release_in.fh,
        flags = release_in.flags,
        lock_owner = release_in.lock_owner,
        flush,
        "release (worker)"
    );
    let resp = if let Err(err) = ctx
        .fs
        .release(
            Request::from(&item),
            item.in_header.nodeid,
            release_in.fh,
            release_in.flags,
            release_in.lock_owner,
            flush,
        )
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_fsync_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let fsync_in = match get_bincode_config().deserialize::<fuse_fsync_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_fsync_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };
    let data_sync = fsync_in.fsync_flags & 1 > 0;
    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh = fsync_in.fh,
        data_sync,
        "fsync (worker)"
    );
    let resp = if let Err(err) = ctx
        .fs
        .fsync(
            Request::from(&item),
            item.in_header.nodeid,
            fsync_in.fh,
            data_sync,
        )
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_setxattr_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let setxattr_in = match get_bincode_config().deserialize::<fuse_setxattr_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_setxattr_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    let mut data = &item.data[FUSE_SETXATTR_IN_SIZE..];
    let (name, first_null_index) = match get_first_null_position(data) {
        None => {
            debug!(
                unique = item.unique,
                "fuse_setxattr_in body doesn't have null"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => (OsString::from_vec(data[..index].to_vec()), index),
    };

    data = &data[first_null_index + 1..];
    if data.len() < setxattr_in.size as usize {
        debug!(unique = item.unique, "setxattr value data too short");
        let data =
            reply_error_in_worker(libc::EINVAL.into(), item.unique).expect("serialize out_header");
        let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        return;
    }

    let value = data[..setxattr_in.size as usize].to_vec();

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        ?name,
        size = setxattr_in.size,
        flags = setxattr_in.flags,
        "setxattr (worker)"
    );
    // TODO handle os X argument
    let resp = if let Err(err) = ctx
        .fs
        .setxattr(
            Request::from(&item),
            item.in_header.nodeid,
            &name,
            &value,
            setxattr_in.flags,
            #[cfg(target_os = "macos")]
            setxattr_in.position,
            #[cfg(not(target_os = "macos"))]
            0,
        )
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_getxattr_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let getxattr_in = match get_bincode_config().deserialize::<fuse_getxattr_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_getxattr_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    let data = &item.data[FUSE_GETXATTR_IN_SIZE..];
    let name = match get_first_null_position(data) {
        None => {
            debug!(
                unique = item.unique,
                "fuse_getxattr_in body doesn't have null"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => OsString::from_vec(data[..index].to_vec()),
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        ?name,
        size = getxattr_in.size,
        "getxattr (worker)"
    );

    let reply_result = ctx
        .fs
        .getxattr(
            Request::from(&item),
            item.in_header.nodeid,
            &name,
            getxattr_in.size,
        )
        .await;

    match reply_result {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
        Ok(reply_xattr) => {
            let reply =
                super::xattr_reply::encode_xattr_reply(reply_xattr, item.unique, getxattr_in.size);
            let _ = ctx.resp_for(item.unique).unbounded_send(reply);
        }
    };
}

pub(super) async fn handle_listxattr_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let getxattr_in = match get_bincode_config().deserialize::<fuse_getxattr_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_getxattr_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        size = getxattr_in.size,
        "listxattr (worker)"
    );

    let reply_result = ctx
        .fs
        .listxattr(
            Request::from(&item),
            item.in_header.nodeid,
            getxattr_in.size,
        )
        .await;

    match reply_result {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
        Ok(reply_xattr) => {
            let reply =
                super::xattr_reply::encode_xattr_reply(reply_xattr, item.unique, getxattr_in.size);
            let _ = ctx.resp_for(item.unique).unbounded_send(reply);
        }
    };
}

pub(super) async fn handle_removexattr_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let name = match get_first_null_position(&item.data) {
        None => {
            debug!(
                unique = item.unique,
                "removexattr body doesn't have null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => OsString::from_vec(item.data[..index].to_vec()),
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        ?name,
        "removexattr (worker)"
    );

    let resp = if let Err(err) = ctx
        .fs
        .removexattr(Request::from(&item), item.in_header.nodeid, &name)
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_flush_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let flush_in = match get_bincode_config().deserialize::<fuse_flush_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_flush_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh = flush_in.fh,
        lock_owner = flush_in.lock_owner,
        "flush (worker)"
    );

    let resp = if let Err(err) = ctx
        .fs
        .flush(
            Request::from(&item),
            item.in_header.nodeid,
            flush_in.fh,
            flush_in.lock_owner,
        )
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_opendir_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let open_in = match get_bincode_config().deserialize::<fuse_open_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_open_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    let direct_io = ctx.direct_io;

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        flags = open_in.flags,
        "opendir (worker)"
    );

    let data = match ctx
        .fs
        .opendir(Request::from(&item), item.in_header.nodeid, open_in.flags)
        .await
    {
        Err(err) => reply_error_in_worker(err, item.unique).expect("serialize out_header"),
        Ok(opened) => {
            let mut open_out: fuse_open_out = opened.into();
            apply_direct_io(&mut open_out.open_flags, direct_io);
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_OPEN_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_OPEN_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &open_out)
                .expect("serialize open_out");
            data
        }
    };

    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_releasedir_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let release_in = match get_bincode_config().deserialize::<fuse_release_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_release_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh = release_in.fh,
        flags = release_in.flags,
        "releasedir (worker)"
    );

    let resp = if let Err(err) = ctx
        .fs
        .releasedir(
            Request::from(&item),
            item.in_header.nodeid,
            release_in.fh,
            release_in.flags,
        )
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_fsyncdir_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let fsync_in = match get_bincode_config().deserialize::<fuse_fsync_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_fsync_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    let data_sync = fsync_in.fsync_flags & 1 > 0;
    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh = fsync_in.fh,
        data_sync,
        "fsyncdir (worker)"
    );

    let resp = if let Err(err) = ctx
        .fs
        .fsyncdir(
            Request::from(&item),
            item.in_header.nodeid,
            fsync_in.fh,
            data_sync,
        )
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_access_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let access_in = match get_bincode_config().deserialize::<fuse_access_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_access_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        mask = access_in.mask,
        "access (worker)"
    );

    let resp = if let Err(err) = ctx
        .fs
        .access(Request::from(&item), item.in_header.nodeid, access_in.mask)
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_create_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let create_in = match get_bincode_config().deserialize::<fuse_create_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_create_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    let data = &item.data[FUSE_CREATE_IN_SIZE..];
    let name = match get_first_null_position(data) {
        None => {
            debug!(
                unique = item.unique,
                "fuse_create_in body doesn't have null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => OsString::from_vec(data[..index].to_vec()),
    };

    let direct_io = ctx.direct_io;

    debug!(
        unique = item.unique,
        parent = item.in_header.nodeid,
        ?name,
        mode = create_in.mode,
        flags = create_in.flags,
        "create (worker)"
    );

    match ctx
        .fs
        .create_with_umask(
            Request::from(&item),
            item.in_header.nodeid,
            &name,
            create_in.mode,
            create_in._umask,
            create_in.flags,
        )
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
        Ok(created) => {
            let (entry_out, mut open_out): (fuse_entry_out, fuse_open_out) = created.into();
            apply_direct_io(&mut open_out.open_flags, direct_io);
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE + FUSE_OPEN_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data =
                Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_ENTRY_OUT_SIZE + FUSE_OPEN_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &entry_out)
                .expect("serialize entry");
            get_bincode_config()
                .serialize_into(&mut data, &open_out)
                .expect("serialize open");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
    }
}

pub(super) async fn handle_bmap_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let bmap_in = match get_bincode_config().deserialize::<fuse_bmap_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_bmap_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        blocksize = bmap_in.blocksize,
        block = bmap_in.block,
        "bmap (worker)"
    );

    match ctx
        .fs
        .bmap(
            Request::from(&item),
            item.in_header.nodeid,
            bmap_in.blocksize,
            bmap_in.block,
        )
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
        Ok(bmap_reply) => {
            let bmap_out: fuse_bmap_out = bmap_reply.into();
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_BMAP_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_BMAP_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &bmap_out)
                .expect("serialize bmap_out");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
    }
}

pub(super) async fn handle_fallocate_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let fallocate_in = match get_bincode_config().deserialize::<fuse_fallocate_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_fallocate_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh = fallocate_in.fh,
        offset = fallocate_in.offset,
        length = fallocate_in.length,
        mode = fallocate_in.mode,
        "fallocate (worker)"
    );

    let resp = if let Err(err) = ctx
        .fs
        .fallocate(
            Request::from(&item),
            item.in_header.nodeid,
            fallocate_in.fh,
            fallocate_in.offset,
            fallocate_in.length,
            fallocate_in.mode,
        )
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_readdirplus_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let read_in = match get_bincode_config().deserialize::<fuse_read_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_read_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh = read_in.fh,
        offset = read_in.offset,
        "readdirplus (worker)"
    );
    let reply_guard = match ctx.fs.reserve_reply_memory(u64::from(read_in.size)) {
        Ok(guard) => guard,
        Err(error) => {
            let data = reply_error_in_worker(error, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
    };

    let reply_readdir_plus = match ctx
        .fs
        .readdirplus(
            Request::from(&item),
            item.in_header.nodeid,
            read_in.fh,
            read_in.offset,
            read_in.lock_owner,
        )
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(r) => r,
    };

    let max_size = read_in.size as usize;
    let mut entry_data = Vec::with_capacity(max_size);
    let mut entries = pin!(reply_readdir_plus.entries);

    while let Some(entry_plus) = entries.next().await {
        let entry_plus = match entry_plus {
            Err(err) => {
                let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
                let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
                return;
            }
            Ok(entry_plus) => entry_plus,
        };

        let name = &entry_plus.name;
        let dirent_plus_size = FUSE_DIRENTPLUS_SIZE + name.len();
        let padding_size = get_padding_size(dirent_plus_size);

        if entry_data.len() + dirent_plus_size + padding_size > max_size {
            break;
        }

        let entry_out = fuse_entry_out {
            nodeid: entry_plus.attr.ino,
            generation: entry_plus.generation,
            entry_valid: entry_plus.entry_ttl.as_secs(),
            attr_valid: entry_plus.attr_ttl.as_secs(),
            entry_valid_nsec: entry_plus.entry_ttl.subsec_nanos(),
            attr_valid_nsec: entry_plus.attr_ttl.subsec_nanos(),
            attr: entry_plus.attr.into(),
        };
        let dirent_plus = fuse_direntplus {
            entry_out,
            dirent: fuse_dirent {
                ino: entry_plus.inode,
                off: entry_plus.offset as u64,
                namelen: name.len() as u32,
                // learn from fuse-rs and golang bazil.org fuse DirentType
                r#type: mode_from_kind_and_perm(entry_plus.kind, 0) >> 12,
            },
        };

        get_bincode_config()
            .serialize_into(&mut entry_data, &dirent_plus)
            .expect("serialize direntplus");
        entry_data.extend_from_slice(name.as_bytes());
        entry_data.resize(entry_data.len() + padding_size, 0);
    }

    let out_header = fuse_out_header {
        len: (FUSE_OUT_HEADER_SIZE + entry_data.len()) as u32,
        error: 0,
        unique: item.unique,
    };
    let mut data_buf = Vec::with_capacity(FUSE_OUT_HEADER_SIZE);
    get_bincode_config()
        .serialize_into(&mut data_buf, &out_header)
        .expect("serialize header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Right((
        data_buf,
        crate::raw::reply::own_reply_buffer(entry_data, reply_guard),
    )));
}

pub(super) async fn handle_rename2_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let rename2_in = match get_bincode_config().deserialize::<fuse_rename2_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_rename2_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    let mut data = &item.data[FUSE_RENAME2_IN_SIZE..];
    let (name, first_null_index) = match get_first_null_position(data) {
        None => {
            debug!(
                unique = item.unique,
                "fuse_rename2_in body doesn't have null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => (OsString::from_vec(data[..index].to_vec()), index),
    };

    data = &data[first_null_index + 1..];
    let new_name = match get_first_null_position(data) {
        None => {
            debug!(
                unique = item.unique,
                "fuse_rename2_in body doesn't have second null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => OsString::from_vec(data[..index].to_vec()),
    };

    debug!(
        unique = item.unique,
        parent = item.in_header.nodeid,
        ?name,
        new_parent = rename2_in.newdir,
        ?new_name,
        flags = rename2_in.flags,
        "rename2 (worker)"
    );

    let resp = if let Err(err) = ctx
        .fs
        .rename2(
            Request::from(&item),
            item.in_header.nodeid,
            &name,
            rename2_in.newdir,
            &new_name,
            rename2_in.flags,
        )
        .await
    {
        err
    } else {
        Errno::from(0)
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_lseek_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let lseek_in = match get_bincode_config().deserialize::<fuse_lseek_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_lseek_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh = lseek_in.fh,
        offset = lseek_in.offset,
        whence = lseek_in.whence,
        "lseek (worker)"
    );

    match ctx
        .fs
        .lseek(
            Request::from(&item),
            item.in_header.nodeid,
            lseek_in.fh,
            lseek_in.offset,
            lseek_in.whence,
        )
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
        Ok(offset) => {
            let lseek_out = fuse_lseek_out {
                offset: offset.offset,
            };
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_LSEEK_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_LSEEK_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &lseek_out)
                .expect("serialize lseek_out");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
    }
}

pub(super) async fn handle_copy_file_range_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let copy_file_range_in =
        match get_bincode_config().deserialize::<fuse_copy_file_range_in>(&item.data) {
            Err(err) => {
                debug!(
                    unique = item.unique,
                    "deserialize fuse_copy_file_range_in failed {}", err
                );
                let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                    .expect("serialize out_header");
                let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
                return;
            }
            Ok(v) => v,
        };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh_in = copy_file_range_in.fh_in,
        off_in = copy_file_range_in.off_in,
        nodeid_out = copy_file_range_in.nodeid_out,
        fh_out = copy_file_range_in.fh_out,
        off_out = copy_file_range_in.off_out,
        len = copy_file_range_in.len,
        flags = copy_file_range_in.flags,
        "copy_file_range (worker)"
    );

    match ctx
        .fs
        .copy_file_range(
            Request::from(&item),
            item.in_header.nodeid,
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
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
        Ok(copied) => {
            let copy_file_range_out: fuse_write_out = copied.into();
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_WRITE_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_WRITE_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &copy_file_range_out)
                .expect("serialize copy_file_range_out");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
    }
}

#[cfg(feature = "file-lock")]
pub(super) async fn handle_getlk_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let lk_in = match get_bincode_config().deserialize::<fuse_lk_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_lk_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh = lk_in.fh,
        owner = lk_in.owner,
        "getlk (worker)"
    );

    match ctx
        .fs
        .getlk(
            Request::from(&item),
            item.in_header.nodeid,
            lk_in.fh,
            lk_in.owner,
            lk_in.lk.start,
            lk_in.lk.end,
            lk_in.lk.r#type,
            lk_in.lk.pid,
        )
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
        Ok(lock) => {
            let lk_out: fuse_lk_out = lock.into();
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_LK_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_LK_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &lk_out)
                .expect("serialize lk_out");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        }
    }
}

#[cfg(feature = "file-lock")]
pub(super) async fn handle_setlk_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
    is_blocking: bool,
) {
    let lk_in = match get_bincode_config().deserialize::<fuse_lk_in>(&item.data) {
        Err(err) => {
            let opcode = if is_blocking {
                fuse_opcode::FUSE_SETLKW
            } else {
                fuse_opcode::FUSE_SETLK
            };

            error!(
                "deserialize fuse_lk_in in {:?} failed {}, request unique {}",
                opcode, err, item.unique
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh = lk_in.fh,
        owner = lk_in.owner,
        is_blocking,
        "setlk (worker)"
    );

    let resp = if let Err(err) = ctx
        .fs
        .setlk(
            Request::from(&item),
            item.in_header.nodeid,
            lk_in.fh,
            lk_in.owner,
            lk_in.lk.start,
            lk_in.lk.end,
            lk_in.lk.r#type,
            lk_in.lk.pid,
            is_blocking,
        )
        .await
    {
        err
    } else {
        Errno::from(0)
    };

    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_poll_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let poll_in = match get_bincode_config().deserialize::<fuse_poll_in>(&item.data) {
        Err(err) => {
            error!(
                unique = item.unique,
                "deserialize fuse_poll_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        fh = poll_in.fh,
        kh = poll_in.kh,
        flags = poll_in.flags,
        "poll (worker)"
    );

    let notify = Notify::new(ctx.notification_sender(item.unique));
    let reply_poll = match ctx
        .fs
        .poll(
            Request::from(&item),
            item.in_header.nodeid,
            poll_in.fh,
            if poll_in.flags & FUSE_POLL_SCHEDULE_NOTIFY == 0 {
                None
            } else {
                Some(poll_in.kh)
            },
            poll_in.flags,
            poll_in.events,
            &notify,
        )
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(r) => r,
    };

    let poll_out: fuse_poll_out = reply_poll.into();

    let out_header = fuse_out_header {
        len: (FUSE_OUT_HEADER_SIZE + FUSE_POLL_OUT_SIZE) as u32,
        error: 0,
        unique: item.unique,
    };

    let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_POLL_OUT_SIZE);
    get_bincode_config()
        .serialize_into(&mut data, &out_header)
        .expect("serialize header");
    get_bincode_config()
        .serialize_into(&mut data, &poll_out)
        .expect("serialize poll_out");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_batch_forget_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let batch_forget_in = match get_bincode_config().deserialize::<fuse_batch_forget_in>(&item.data)
    {
        Err(err) => {
            error!(
                "deserialize fuse_batch_forget_in failed {}, request unique {}",
                err, item.unique
            );
            // batch_forget has no reply
            return;
        }
        Ok(v) => v,
    };

    let mut data = &item.data[FUSE_BATCH_FORGET_IN_SIZE..];
    let mut inodes = Vec::with_capacity(batch_forget_in.count as usize);

    for _ in 0..batch_forget_in.count {
        if data.len() < FUSE_FORGET_ONE_SIZE {
            error!(unique = item.unique, "batch_forget data too short");
            return;
        }

        let forget_one = match get_bincode_config()
            .deserialize::<fuse_forget_one>(&data[..FUSE_FORGET_ONE_SIZE])
        {
            Err(err) => {
                error!(
                    "deserialize fuse_batch_forget_in body fuse_forget_one failed {}, request unique {}",
                    err, item.unique
                );
                // no need to reply
                return;
            }
            Ok(v) => v,
        };

        inodes.push((forget_one.nodeid, forget_one._nlookup));
        data = &data[FUSE_FORGET_ONE_SIZE..];
    }

    if inodes.len() != batch_forget_in.count as usize {
        error!(
            "fuse_forget_one count != fuse_batch_forget_in.count, request unique {}",
            item.unique
        );

        return;
    }

    debug!(
        unique = item.unique,
        count = batch_forget_in.count,
        "batch_forget (worker)"
    );

    ctx.fs
        .batch_forget(
            Request {
                unique: item.unique,
                uid: item.in_header.uid,
                gid: item.in_header.gid,
                pid: item.in_header.pid,
            },
            &inodes,
        )
        .await;
    // batch_forget has no reply
}

pub(super) async fn handle_destroy_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    debug!(unique = item.unique, "destroy (worker)");
    ctx.fs.destroy(Request::from(&item)).await;
}

pub(super) async fn handle_interrupt_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let interrupt_in = match get_bincode_config().deserialize::<fuse_interrupt_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_interrupt_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    debug!(
        unique = item.unique,
        interrupted_unique = interrupt_in.unique,
        "interrupt (worker)"
    );

    let resp = match ctx
        .fs
        .interrupt(Request::from(&item), interrupt_in.unique)
        .await
    {
        Err(err) => err,
        Ok(()) => 0.into(),
    };
    let data = reply_error_in_worker(resp, item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_notify_reply_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let notify_retrieve_in =
        match get_bincode_config().deserialize::<fuse_notify_retrieve_in>(&item.data) {
            Err(err) => {
                error!(
                    "deserialize fuse_notify_retrieve_in failed {}, request unique {}",
                    err, item.unique
                );
                return;
            }
            Ok(v) => v,
        };

    let payload_start = FUSE_NOTIFY_RETRIEVE_IN_SIZE;
    let payload_end = payload_start.saturating_add(notify_retrieve_in.size as usize);
    if item.data.len() < payload_end {
        error!(
            unique = item.unique,
            size = notify_retrieve_in.size,
            available = item.data.len().saturating_sub(payload_start),
            "fuse_notify_retrieve data size is invalid"
        );
        return;
    }

    let payload = item.data.slice(payload_start..payload_end);
    if let Err(err) = ctx
        .fs
        .notify_reply(
            Request::from(&item),
            item.in_header.nodeid,
            notify_retrieve_in.offset,
            payload,
        )
        .await
    {
        let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
        let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
    }
}

#[cfg(target_os = "macos")]
pub(super) async fn handle_setvolname_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let name = match get_first_null_position(&item.data) {
        None => OsString::from_vec(item.data.to_vec()),
        Some(idx) => OsString::from_vec(item.data[..idx].to_vec()),
    };

    debug!(unique = item.unique, ?name, "setvolname (worker)");

    let resp_value = if let Err(err) = ctx.fs.setvolname(Request::from(&item), &name).await {
        err.into()
    } else {
        0
    };

    let data = reply_error_in_worker(resp_value.into(), item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

pub(super) async fn handle_ioctl_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let ioctl_in = match get_bincode_config().deserialize::<fuse_ioctl_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_ioctl_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(ioctl_in) => ioctl_in,
    };

    let payload_end = FUSE_IOCTL_IN_SIZE.saturating_add(ioctl_in.in_size as usize);
    if item.data.len() < payload_end {
        let data =
            reply_error_in_worker(libc::EINVAL.into(), item.unique).expect("serialize out_header");
        let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
        return;
    }
    let ioctl_data = &item.data[FUSE_IOCTL_IN_SIZE..payload_end];

    let request = Request::from(&item);
    let in_header = item.in_header;

    let reply_ioctl = match ctx
        .fs
        .ioctl(
            request,
            in_header.nodeid,
            ioctl_in.fh,
            ioctl_in.flags,
            ioctl_in.cmd,
            ioctl_in.arg,
            ioctl_data,
            ioctl_in.out_size,
        )
        .await
    {
        Err(err) => {
            let data = reply_error_in_worker(err, item.unique).expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
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
        unique: item.unique,
    };
    let mut data = Vec::with_capacity(out_len);
    get_bincode_config()
        .serialize_into(&mut data, &out_header)
        .expect("serialize header");
    get_bincode_config()
        .serialize_into(&mut data, &ioctl_out)
        .expect("serialize ioctl_out");
    data.extend_from_slice(&reply_ioctl.data);
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

#[cfg(target_os = "macos")]
pub(super) async fn handle_getxtimes_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    debug!(
        unique = item.unique,
        inode = item.in_header.nodeid,
        "getxtimes (worker)"
    );

    let data = match ctx
        .fs
        .getxtimes(Request::from(&item), item.in_header.nodeid)
        .await
    {
        Err(err) => reply_error_in_worker(err, item.unique).expect("serialize out_header"),
        Ok(times) => {
            let xtimes_out: fuse_getxtimes_out = times.into();
            let out_header = fuse_out_header {
                len: (FUSE_OUT_HEADER_SIZE + FUSE_GETXTIMES_OUT_SIZE) as u32,
                error: 0,
                unique: item.unique,
            };
            let mut data = Vec::with_capacity(FUSE_OUT_HEADER_SIZE + FUSE_GETXTIMES_OUT_SIZE);
            get_bincode_config()
                .serialize_into(&mut data, &out_header)
                .expect("serialize header");
            get_bincode_config()
                .serialize_into(&mut data, &xtimes_out)
                .expect("serialize getxtimes_out");
            data
        }
    };

    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}

#[cfg(target_os = "macos")]
pub(super) async fn handle_exchange_inline<FS: Filesystem + Send + Sync + 'static>(
    ctx: &DispatchCtx<FS>,
    item: WorkItem,
) {
    let exchange_in = match get_bincode_config().deserialize::<fuse_exchange_in>(&item.data) {
        Err(err) => {
            debug!(
                unique = item.unique,
                "deserialize fuse_exchange_in failed {}", err
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Ok(v) => v,
    };

    let mut data = &item.data[FUSE_EXCHANGE_IN_SIZE..];
    let (oldname, first_null_index) = match get_first_null_position(data) {
        None => {
            debug!(
                unique = item.unique,
                "fuse_exchange_in body doesn't have null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => (OsString::from_vec(data[..index].to_vec()), index),
    };

    data = &data[first_null_index + 1..];
    let newname = match get_first_null_position(data) {
        None => {
            debug!(
                unique = item.unique,
                "fuse_exchange_in body doesn't have second null (worker)"
            );
            let data = reply_error_in_worker(libc::EINVAL.into(), item.unique)
                .expect("serialize out_header");
            let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
            return;
        }
        Some(index) => OsString::from_vec(data[..index].to_vec()),
    };

    debug!(
        unique = item.unique,
        olddir = exchange_in.olddir,
        ?oldname,
        newdir = exchange_in.newdir,
        ?newname,
        options = exchange_in.options,
        "exchange (worker)"
    );

    let resp_value = if let Err(err) = ctx
        .fs
        .exchange(
            Request::from(&item),
            exchange_in.olddir,
            &oldname,
            exchange_in.newdir,
            &newname,
            exchange_in.options,
        )
        .await
    {
        err.into()
    } else {
        0
    };

    let data = reply_error_in_worker(resp_value.into(), item.unique).expect("serialize out_header");
    let _ = ctx.resp_for(item.unique).unbounded_send(Either::Left(data));
}
