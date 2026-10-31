//! Existing public startup API ordering. The test boundary refuses physical IO.
use super::*;
use crate::raw::reply::{InlineRootPermit, ReplyInit, ReplyMemoryGuard};
use std::sync::atomic::AtomicBool;

std::thread_local! {
    static ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static PHYSICAL_ENTRIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
pub(super) fn physical_boundary() -> IoResult<()> {
    if ARMED.get() {
        PHYSICAL_ENTRIES.set(PHYSICAL_ENTRIES.get() + 1);
        Err(IoError::from_raw_os_error(libc::EPERM))
    } else {
        Ok(())
    }
}
struct Boundary;
impl Boundary {
    fn arm() -> Self {
        ARMED.set(true);
        PHYSICAL_ENTRIES.set(0);
        Self
    }
}
impl Drop for Boundary {
    fn drop(&mut self) {
        ARMED.set(false);
        owned_open_queue::inject_startup_reserve_failure(0);
    }
}

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
#[derive(Default)]
struct ProbeFs {
    roots: Arc<AtomicUsize>,
    inline_attempts: AtomicUsize,
    deny_plan: AtomicBool,
    fs_polls: AtomicUsize,
}
impl Filesystem for Arc<ProbeFs> {
    async fn init(&self, _: Request) -> crate::Result<ReplyInit> {
        self.fs_polls.fetch_add(1, Ordering::AcqRel);
        Ok(ReplyInit::default())
    }
    async fn destroy(&self, _: Request) {
        self.fs_polls.fetch_add(1, Ordering::AcqRel);
    }
    fn supports_read_cancellation(&self) -> bool {
        true
    }
    fn reserve_input_buffer_memory(&self, bytes: u64) -> crate::Result<Option<ReplyMemoryGuard>> {
        let bytes = usize::try_from(bytes).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        self.roots.fetch_add(bytes, Ordering::AcqRel);
        Ok(Some(Arc::new(Charge {
            used: self.roots.clone(),
            bytes,
        })))
    }
    fn reserve_inline_root_memory(&self, bytes: u64) -> crate::Result<Option<InlineRootPermit>> {
        let attempt = self.inline_attempts.fetch_add(1, Ordering::AcqRel) + 1;
        if attempt == 2 && self.deny_plan.load(Ordering::Acquire) {
            return Err(libc::ENOMEM.into());
        }
        let bytes = usize::try_from(bytes).map_err(|_| crate::Errno::from(libc::ENOMEM))?;
        self.roots.fetch_add(bytes, Ordering::AcqRel);
        Ok(Some(
            InlineRootPermit::try_new(Charge {
                used: self.roots.clone(),
                bytes,
            })
            .unwrap(),
        ))
    }
    async fn prepare_unmount(&self) -> crate::Result<()> {
        self.fs_polls.fetch_add(1, Ordering::AcqRel);
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
struct EmptyMountDir(PathBuf);
impl EmptyMountDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        loop {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("asyncfuse-startup-{}-{id}", std::process::id()));
            match std::fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create empty test mountpoint: {error}"),
            }
        }
    }
}
impl Drop for EmptyMountDir {
    fn drop(&mut self) {
        std::fs::remove_dir(&self.0).expect("test never performs a physical mount");
    }
}

async fn public_startup_error(fs: Arc<ProbeFs>, unprivileged: bool) -> IoError {
    let directory = EmptyMountDir::new();
    // Keep default workers0: readonly startup actually routes through worker1.
    let session = Session::new(MountOptions::default());
    #[cfg(feature = "unprivileged")]
    if unprivileged {
        return match session.mount_with_unprivileged(fs, &directory.0).await {
            Err(error) => error,
            Ok(_) => panic!("test physical boundary must prevent mounting"),
        };
    }
    let _ = unprivileged;
    match session.mount(fs, &directory.0).await {
        Err(error) => error,
        Ok(_) => panic!("test physical boundary must prevent mounting"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn public_readonly_startup_plan_admission_denial_precedes_native_and_kernel_startup() {
    for unprivileged in [false, cfg!(feature = "unprivileged")] {
        let _boundary = Boundary::arm();
        let fs = Arc::new(ProbeFs::default());
        fs.deny_plan.store(true, Ordering::Release);
        let error = public_startup_error(fs.clone(), unprivileged).await;
        assert_eq!(error.raw_os_error(), Some(libc::ENOMEM));
        assert_eq!(
            PHYSICAL_ENTRIES.get(),
            0,
            "no connection/helper/native/physical startup was entered"
        );
        assert_eq!(
            fs.inline_attempts.load(Ordering::Acquire),
            2,
            "prepare then the original worker plan admission"
        );
        assert_eq!(fs.fs_polls.load(Ordering::Acquire), 0);
        assert_eq!(
            fs.roots.load(Ordering::Acquire),
            0,
            "all preflight ownership retired before API returns"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn public_readonly_startup_each_fallible_plan_reserve_precedes_physical_startup() {
    for fail_at in 1..=6 {
        let _boundary = Boundary::arm();
        let fs = Arc::new(ProbeFs::default());
        let directory = EmptyMountDir::new();
        owned_open_queue::inject_startup_reserve_failure(fail_at);
        let result = Session::new(MountOptions::default())
            .with_workers(2, 2)
            .mount(fs.clone(), &directory.0)
            .await;
        owned_open_queue::inject_startup_reserve_failure(0);
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("physical boundary must prevent mounting"),
        };
        assert_eq!(error.raw_os_error(), Some(libc::ENOMEM));
        assert_eq!(
            PHYSICAL_ENTRIES.get(),
            0,
            "records/plans/either lane failure cannot leave a kernel mount"
        );
        assert_eq!(fs.inline_attempts.load(Ordering::Acquire), 2);
        assert_eq!(fs.fs_polls.load(Ordering::Acquire), 0);
        assert_eq!(fs.roots.load(Ordering::Acquire), 0);
    }
}
