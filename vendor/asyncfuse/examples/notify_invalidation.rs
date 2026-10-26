//! Linux kernel-level invalidation probe. The mountpoint must not already exist.
//!
//! Run with a working /dev/fuse and mount privileges (or fusermount3):
//! cargo run --example notify_invalidation -- --mountpoint /tmp/asyncfuse-notify
//! Add --workers 2 to exercise the worker-pool session as well.

use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use asyncfuse::raw::{prelude::*, Filesystem, Session};
use asyncfuse::{MountOptions, Result};
use clap::Parser;
use futures_channel::oneshot;
use futures_util::Stream;

const TTL: Duration = Duration::from_secs(3600);
const NAMES: [&[u8]; 2] = [b"cached.txt", b"cached-\xff"];

#[derive(Default)]
struct Counts {
    lookup: [AtomicUsize; 2],
    getattr: [AtomicUsize; 2],
}

struct ProbeFs {
    counts: Arc<Counts>,
    ready: Mutex<Option<oneshot::Sender<Notify>>>,
}

fn attr(ino: u64) -> FileAttr {
    FileAttr {
        ino,
        size: 0,
        blocks: 0,
        atime: SystemTime::UNIX_EPOCH.into(),
        mtime: SystemTime::UNIX_EPOCH.into(),
        ctime: SystemTime::UNIX_EPOCH.into(),
        #[cfg(target_os = "macos")]
        crtime: SystemTime::UNIX_EPOCH.into(),
        kind: if ino == 1 {
            FileType::Directory
        } else {
            FileType::RegularFile
        },
        perm: if ino == 1 { 0o755 } else { 0o644 },
        nlink: if ino == 1 { 2 } else { 1 },
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        blksize: 4096,
        #[cfg(target_os = "macos")]
        flags: 0,
    }
}

impl Filesystem for ProbeFs {
    async fn init(&self, _req: Request) -> Result<ReplyInit> {
        Ok(ReplyInit::default())
    }

    async fn init_with_notify(&self, _req: Request, notify: Notify) -> Result<ReplyInit> {
        if let Some(ready) = self.ready.lock().unwrap().take() {
            let _ = ready.send(notify);
        }
        Ok(ReplyInit::default())
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> Result<ReplyEntry> {
        let index = NAMES
            .iter()
            .position(|candidate| *candidate == name.as_bytes());
        match (parent, index) {
            (1, Some(index)) => {
                self.counts.lookup[index].fetch_add(1, Ordering::SeqCst);
                Ok(ReplyEntry {
                    ttl: TTL,
                    attr: attr(index as u64 + 2),
                    generation: 1,
                })
            }
            _ => Err(libc::ENOENT.into()),
        }
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        if !(1..=3).contains(&inode) {
            return Err(libc::ENOENT.into());
        }
        if inode > 1 {
            self.counts.getattr[(inode - 2) as usize].fetch_add(1, Ordering::SeqCst);
        }
        Ok(ReplyAttr {
            ttl: TTL,
            attr: attr(inode),
        })
    }

    async fn opendir(&self, _req: Request, _inode: u64, _flags: u32) -> Result<ReplyOpen> {
        Ok(ReplyOpen { fh: 0, flags: 0 })
    }

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        _parent: u64,
        _fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<impl Stream<Item = Result<DirectoryEntry>> + Send + 'a>> {
        let entries = NAMES
            .iter()
            .enumerate()
            .filter(move |(index, _)| *index as i64 + 1 > offset)
            .map(|(index, name)| {
                Ok(DirectoryEntry {
                    inode: index as u64 + 2,
                    offset: index as i64 + 1,
                    kind: FileType::RegularFile,
                    name: OsStr::from_bytes(name).to_owned(),
                })
            });
        Ok(ReplyDirectory {
            entries: futures_util::stream::iter(entries),
        })
    }

    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        _parent: u64,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<impl Stream<Item = Result<DirectoryEntryPlus>> + Send + 'a>>
    {
        let entries = NAMES
            .iter()
            .enumerate()
            .filter(move |(index, _)| *index as u64 + 1 > offset)
            .map(|(index, name)| {
                Ok(DirectoryEntryPlus {
                    inode: index as u64 + 2,
                    generation: 1,
                    kind: FileType::RegularFile,
                    name: OsStr::from_bytes(name).to_owned(),
                    offset: index as i64 + 1,
                    attr: attr(index as u64 + 2),
                    // The first stat must issue LOOKUP and establish the long
                    // entry TTL itself, independently of directory listing.
                    entry_ttl: Duration::ZERO,
                    attr_ttl: TTL,
                })
            });
        Ok(ReplyDirectoryPlus {
            entries: futures_util::stream::iter(entries),
        })
    }

    #[cfg(feature = "file-lock")]
    async fn getlk(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _type: u32,
        _pid: u32,
    ) -> Result<ReplyLock> {
        Err(libc::ENOSYS.into())
    }

    #[cfg(feature = "file-lock")]
    async fn setlk(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _type: u32,
        _pid: u32,
        _block: bool,
    ) -> Result<()> {
        Err(libc::ENOSYS.into())
    }
}

fn check(condition: bool, message: &str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message))
    }
}

fn check_errno(result: io::Result<()>, expected: i32, operation: &str) -> io::Result<()> {
    match result {
        Err(error) if error.raw_os_error() == Some(expected) => {
            println!("{operation}: observed kernel errno {expected} ({error})");
            Ok(())
        }
        other => Err(io::Error::other(format!(
            "{operation}: expected errno {expected}, got {other:?}"
        ))),
    }
}

async fn verify(
    mountpoint: &Path,
    counts: &Counts,
    ready: oneshot::Receiver<Notify>,
) -> io::Result<Notify> {
    let notify = ready
        .await
        .map_err(|_| io::Error::other("INIT did not provide Notify"))?;
    let mut directory = tokio::fs::read_dir(mountpoint).await?;
    let mut listed = Vec::new();
    while let Some(entry) = directory.next_entry().await? {
        listed.push(entry.file_name());
    }
    drop(directory);
    check(
        NAMES
            .iter()
            .all(|name| listed.iter().any(|entry| entry.as_bytes() == *name)),
        "directory did not contain both probe files",
    )?;
    println!(
        "directory populated; entry/attribute TTL={}s",
        TTL.as_secs()
    );

    for (index, name) in NAMES.iter().enumerate() {
        let name = OsStr::from_bytes(name);
        let path = mountpoint.join(name);
        tokio::fs::metadata(&path).await?;
        let populated = counts.lookup[index].load(Ordering::SeqCst);
        for _ in 0..5 {
            tokio::fs::metadata(&path).await?;
        }
        let cached = counts.lookup[index].load(Ordering::SeqCst);
        check(
            populated > 0 && cached == populated,
            "repeated stat did not use the entry cache",
        )?;
        notify.try_invalid_entry(1, name).await?;
        tokio::fs::metadata(&path).await?;
        let invalidated = counts.lookup[index].load(Ordering::SeqCst);
        check(
            invalidated == cached + 1,
            "stat after INVAL_ENTRY did not issue exactly one new LOOKUP",
        )?;
        tokio::fs::metadata(&path).await?;
        check(
            counts.lookup[index].load(Ordering::SeqCst) == invalidated,
            "entry was not cached again after invalidation",
        )?;
        println!("INVAL_ENTRY {name:?}: LOOKUP populated={populated}, five cached stats={cached}, after acknowledged invalidation={invalidated}; PASS");

        let before = counts.getattr[index].load(Ordering::SeqCst);
        notify.try_invalid_inode(index as u64 + 2, -1, 0).await?;
        tokio::fs::metadata(&path).await?;
        let after = counts.getattr[index].load(Ordering::SeqCst);
        check(
            after == before + 1,
            "attribute invalidation did not issue GETATTR",
        )?;
        println!("INVAL_INODE {name:?}: GETATTR before={before}, after={after}; PASS");
    }

    check_errno(
        notify
            .try_invalid_entry(u64::MAX, OsStr::new("missing"))
            .await,
        libc::ENOENT,
        "INVAL_ENTRY unknown parent",
    )?;
    check_errno(
        notify.try_invalid_inode(u64::MAX, -1, 0).await,
        libc::ENOENT,
        "INVAL_INODE unknown inode",
    )?;
    Ok(notify)
}

fn check_disconnected(result: io::Result<()>, operation: &str) -> io::Result<()> {
    match result {
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
            println!("{operation} after unmount: BrokenPipe; PASS");
            Ok(())
        }
        other => Err(io::Error::other(format!(
            "{operation} after unmount: expected BrokenPipe, got {other:?}"
        ))),
    }
}

#[derive(Parser)]
struct Args {
    /// A new directory to create, mount, then remove.
    #[arg(long)]
    mountpoint: PathBuf,
    /// Request workers (zero exercises the default session).
    #[arg(long, default_value_t = 0)]
    workers: usize,
}

struct MountDirectory(PathBuf);

impl Drop for MountDirectory {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir(&self.0) {
            eprintln!("could not remove {}: {error}", self.0.display());
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();
    let args = Args::parse();
    std::fs::create_dir(&args.mountpoint)?;
    let _directory = MountDirectory(args.mountpoint.clone());
    let counts = Arc::new(Counts::default());
    let (ready_tx, ready_rx) = oneshot::channel();
    let filesystem = ProbeFs {
        counts: counts.clone(),
        ready: Mutex::new(Some(ready_tx)),
    };
    let mut options = MountOptions::default();
    options
        .uid(unsafe { libc::getuid() })
        .gid(unsafe { libc::getgid() })
        .fs_name("asyncfuse-notify-probe")
        .default_permissions(true);
    let session = Session::new(options);
    let session = if args.workers == 0 {
        session
    } else {
        session.with_workers(args.workers, 32)
    };
    #[cfg(feature = "unprivileged")]
    let handle = session
        .mount_with_unprivileged(filesystem, &args.mountpoint)
        .await?;
    #[cfg(not(feature = "unprivileged"))]
    let handle = session.mount(filesystem, &args.mountpoint).await?;
    println!(
        "mounted {}; workers={}",
        args.mountpoint.display(),
        args.workers
    );

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        verify(&args.mountpoint, &counts, ready_rx),
    )
    .await
    .unwrap_or_else(|_| {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "invalidation probe timed out",
        ))
    });
    // Unmount even when an assertion or kernel operation failed.
    let cleanup = tokio::time::timeout(Duration::from_secs(10), handle.unmount())
        .await
        .unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut, "unmount timed out")));
    if let Err(error) = &cleanup {
        eprintln!("unmount failed: {error}");
    }
    let notify = result?;
    cleanup?;
    tokio::time::timeout(Duration::from_secs(1), async {
        check_disconnected(
            notify.try_invalid_entry(1, OsStr::new("cached.txt")).await,
            "INVAL_ENTRY",
        )?;
        check_disconnected(notify.try_invalid_inode(2, -1, 0).await, "INVAL_INODE")
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "notification hung after unmount"))??;
    println!("unmounted; kernel invalidation probe PASS");
    Ok(())
}
