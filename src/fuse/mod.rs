//! FUSE adapter and request handling
//! This module provides the FUSE (Filesystem in Userspace) integration for BrewFS.
//! It implements the adapter and request handling logic required to expose the virtual filesystem
//! to the operating system via the FUSE protocol.
//!
//! Main components:
//! - `adapter`: Contains the FUSE adapter implementation.
//! - `mount`: Handles mounting the virtual filesystem using FUSE.
//! - Implementation of the `Filesystem` trait for `VFS`, enabling translation of FUSE requests
//!   into virtual filesystem operations.
//! - Helpers for attribute and file type conversion between VFS and FUSE representations.
//!
//! The module also includes platform-specific tests for mounting and basic operations,
//! and provides utilities for mapping VFS metadata to FUSE attributes.
pub(crate) mod adapter;
pub mod mount;
use crate::chunk::store::BlockStore;
use crate::control::protocol::{CONTROL_ACL_XATTR_NAME, ControlAclEntry};
use crate::meta::MetaLayer;
use crate::meta::file_lock::{FileLockQuery, FileLockRange, FileLockType};
use crate::meta::store::{MetaError, SetAttrFlags, SetAttrRequest};
use crate::posix::NAME_MAX;
use crate::vfs::error::{PathHint, VfsError};
use crate::vfs::fs::{CreateFileAtResult, FileAttr as VfsFileAttr, FileType as VfsFileType, VFS};
use asyncfuse::Errno;
use asyncfuse::Result as FuseResult;
use asyncfuse::raw::Request;
use asyncfuse::raw::flags::{FOPEN_DIRECT_IO, FOPEN_KEEP_CACHE, FUSE_WRITE_CACHE};
use asyncfuse::raw::reply::{
    DirectoryEntry, DirectoryEntryPlus, ReplyAttr, ReplyCopyFileRange, ReplyCreated, ReplyData,
    ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit, ReplyIoctl, ReplyLSeek, ReplyLock,
    ReplyOpen, ReplyStatFs, ReplyWrite, ReplyXAttr,
};
use bytes::Bytes;
use std::ffi::{OsStr, OsString};
#[cfg(target_os = "linux")]
use std::mem::size_of;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asyncfuse::raw::Filesystem;
use asyncfuse::{FileType as FuseFileType, SetAttr, Timestamp};
use futures_util::stream::{self, BoxStream};
use tracing::{debug, error, info, trace, warn};

/// Runtime-configurable kernel attribute/entry cache TTL.
/// Non-zero lets the kernel serve repeated getattr/lookup from its own cache
/// without round-tripping to userspace.  Default: 1s, matching JuiceFS' usual
/// metadata cache balance.  Override via BREWFS_CACHE_TTL_MS=0 for strict
/// multi-client coherency or targeted debugging.
fn fuse_cache_ttl() -> Duration {
    static TTL: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *TTL.get_or_init(|| {
        match std::env::var("BREWFS_CACHE_TTL_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
        {
            Some(ms) => Duration::from_millis(ms),
            None => Duration::from_secs(1),
        }
    })
}

/// A just-created regular file is commonly returned with size 0 and then
/// written, closed, reopened, and mmap'ed immediately by build tools.  Do not
/// let the kernel cache that creation attr, or the mmap may observe length 0
/// even though later writes have already updated BrewFS metadata.
fn fuse_create_cache_ttl() -> Duration {
    Duration::ZERO
}

fn fuse_direct_io_setting() -> Option<bool> {
    env_flag("BREWFS_FUSE_DIRECT_IO")
}

fn fuse_read_direct_io_enabled() -> bool {
    env_flag("BREWFS_FUSE_READ_DIRECT_IO").unwrap_or(false)
}

fn fuse_write_direct_io_enabled() -> bool {
    env_flag("BREWFS_FUSE_WRITE_DIRECT_IO").unwrap_or(false)
}

fn fuse_copy_file_range_enabled() -> bool {
    env_flag("BREWFS_FUSE_COPY_FILE_RANGE").unwrap_or(true)
}

fn env_flag(name: &str) -> Option<bool> {
    std::env::var(name).ok().and_then(|value| {
        let normalized = value.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            None
        } else {
            Some(!matches!(normalized.as_str(), "0" | "false" | "no" | "off"))
        }
    })
}

fn fuse_keep_cache_enabled() -> bool {
    match std::env::var("BREWFS_FUSE_KEEP_CACHE") {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

fn fuse_open_reply_flags(read: bool, write: bool) -> u32 {
    let mut flags = if fuse_keep_cache_enabled() {
        FOPEN_KEEP_CACHE
    } else {
        0
    };
    let read_only = read && !write;
    let write_only = write && !read;
    let direct_io = fuse_direct_io_setting().unwrap_or_else(|| {
        (write_only && fuse_write_direct_io_enabled())
            || (read_only && fuse_read_direct_io_enabled())
    });
    if (read || write) && direct_io {
        flags |= FOPEN_DIRECT_IO;
    }
    flags
}

/// Virtual inode for the `.stats` file exposed at the mount root.
/// Uses a high inode number unlikely to collide with real inodes.
const STATS_INODE: u64 = 0x7FFF_FFFF_0000_0003;
/// Linux marks execve-driven FUSE open requests with this internal flag.
/// It is not a public libc O_* bit, but it is visible in the FUSE open flags.
const FUSE_OPEN_EXEC: u32 = 0x20;
/// Name of the virtual stats file.
const STATS_FILENAME: &str = ".stats";
const STATS_FILE_SIZE: u64 = 16 * 1024;
const STATS_FILE_BLOCKS: u64 = 32;
const STATS_FILE_BLOCK_SIZE: u32 = 4096;
pub(crate) const BREWFS_FUSE_MAX_WRITE: u32 = 4 * 1024 * 1024;
#[cfg(all(test, target_os = "linux"))]
mod mount_tests {
    use super::*;
    use crate::cadapter::client::ObjectClient;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::chunk::layout::ChunkLayout;
    use crate::chunk::store::ObjectBlockStore;
    use crate::fuse::mount::{FuseConcurrencyConfig, mount_vfs_unprivileged};
    use crate::meta::factory::create_meta_store_from_url;
    use std::fs;
    use std::io::Write;
    use std::time::Duration as StdDuration;

    // Basic Linux mount smoke test controlled by BREWFS_FUSE_TEST
    #[tokio::test]
    async fn smoke_mount_and_basic_ops() {
        if std::env::var("BREWFS_FUSE_TEST").ok().as_deref() != Some("1") {
            eprintln!("skip fuse mount test: set BREWFS_FUSE_TEST=1 to enable");
            return;
        }

        let layout = ChunkLayout::default();
        let tmp_data = tempfile::tempdir().expect("tmp data");
        let client = ObjectClient::new(LocalFsBackend::new(tmp_data.path()));
        let meta = create_meta_store_from_url("sqlite::memory:")
            .await
            .expect("create meta store");
        let store = ObjectBlockStore::new_async(client)
            .await
            .expect("create block store");

        let fs = VFS::new(layout, store, meta.store().clone())
            .await
            .expect("create VFS");

        // Prepare the mount point
        let mnt = tempfile::tempdir().expect("tmp mount");
        let mnt_path = mnt.path().to_path_buf();

        // Mount in the background (until unmount)
        let handle =
            match mount_vfs_unprivileged(fs, &mnt_path, FuseConcurrencyConfig::default()).await {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("skip fuse test: mount failed: {e}");
                    return;
                }
            };

        // Give kernel/daemon a bit of time to finish INIT
        tokio::time::sleep(StdDuration::from_millis(2000)).await;

        // Basic directory/file operations
        let dir = mnt_path.join("a");
        fs::create_dir(&dir).expect("mkdir");
        let file_path = dir.join("hello.txt");
        {
            let mut f = fs::File::create(&file_path).expect("create file");
            f.write_all(b"abc").expect("write");
            f.flush().expect("flush");
        }
        let content = fs::read(&file_path).expect("read back");
        assert_eq!(content, b"abc");

        // List the directory
        let list = fs::read_dir(&dir)
            .expect("readdir")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect::<Vec<_>>();
        assert!(list.iter().any(|n| n.to_string_lossy() == "hello.txt"));

        let hard_dir = mnt_path.join("hard");
        fs::create_dir(&hard_dir).expect("mkdir hard");

        let hard_a = hard_dir.join("a.txt");
        fs::write(&hard_a, b"x").expect("write hard a");
        let hard_b = hard_dir.join("b.txt");
        fs::hard_link(&hard_a, &hard_b).expect("hardlink");

        let sub_dir = hard_dir.join("sub");
        fs::create_dir(&sub_dir).expect("mkdir sub");
        let sub_file = sub_dir.join("c.txt");
        fs::write(&sub_file, b"y").expect("write sub file");

        let sub_list = fs::read_dir(&sub_dir)
            .expect("readdir sub")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect::<Vec<_>>();
        assert!(sub_list.iter().any(|n| n.to_string_lossy() == "."));
        assert!(sub_list.iter().any(|n| n.to_string_lossy() == ".."));
        assert!(sub_list.iter().any(|n| n.to_string_lossy() == "c.txt"));

        let sub_dotdot = fs::read_link(sub_dir.join(".."));
        assert!(sub_dotdot.is_err());

        // Delete and unmount
        fs::remove_file(&hard_b).expect("unlink hard b");
        fs::remove_file(&hard_a).expect("unlink hard a");
        fs::remove_file(&sub_file).expect("unlink sub file");
        fs::remove_dir(&sub_dir).expect("rmdir sub");
        fs::remove_dir(&hard_dir).expect("rmdir hard");
        fs::remove_file(&file_path).expect("unlink");

        // Explicitly unmount and wait
        if let Err(e) = handle.unmount().await {
            eprintln!("unmount error: {e}");
        }
    }
}

impl<S, M> VFS<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    async fn unlock_owner_locks(&self, ino: u64, lock_owner: u64) {
        if !self.take_posix_lock_owner(ino as i64, lock_owner as i64) {
            return;
        }
        self.forget_fuse_lock_owner(ino as i64, lock_owner as i64);
        let _ = self
            .set_plock_ino(
                ino as i64,
                lock_owner as i64,
                false,
                FileLockType::UnLock,
                FileLockRange {
                    start: 0,
                    end: u64::MAX,
                },
                0,
            )
            .await;
    }

    async fn unlock_handle_locks(&self, ino: u64, fh: u64) {
        if fh == 0 {
            return;
        }

        for owner in self.take_fuse_lock_owners_for_handle(ino as i64, fh) {
            let _ = self
                .set_plock_ino(
                    ino as i64,
                    owner,
                    false,
                    FileLockType::UnLock,
                    FileLockRange {
                        start: 0,
                        end: u64::MAX,
                    },
                    0,
                )
                .await;
        }
    }

    #[cfg(target_os = "linux")]
    fn ioctl_ok_reply() -> ReplyIoctl {
        ReplyIoctl {
            result: 0,
            flags: 0,
            in_iovs: 0,
            out_iovs: 0,
            data: Vec::new(),
        }
    }

    #[cfg(target_os = "linux")]
    fn parse_clone_range(data: &[u8]) -> Option<FileCloneRange> {
        if data.len() < size_of::<FileCloneRange>() {
            return None;
        }

        // The kernel provides restricted ioctl payloads inline using the native
        // C layout for this architecture.
        Some(unsafe { std::ptr::read_unaligned(data.as_ptr().cast::<FileCloneRange>()) })
    }

    #[cfg(target_os = "linux")]
    async fn resolve_proc_fd_inode(&self, pid: u32, fd: i64) -> FuseResult<i64> {
        if fd < 0 {
            return Err(libc::EBADF.into());
        }

        let link = std::fs::read_link(format!("/proc/{pid}/fd/{fd}"))
            .map_err(|_| Errno::from(libc::ENOENT))?;
        let mut host_path = link.to_string_lossy().into_owned();
        if let Some(stripped) = host_path.strip_suffix(" (deleted)") {
            host_path = stripped.to_string();
        }
        if !host_path.starts_with('/') {
            return Err(libc::EXDEV.into());
        }

        let mut starts = vec![0];
        for (idx, ch) in host_path.char_indices().skip(1) {
            if ch == '/' {
                starts.push(idx);
            }
        }

        for start in starts {
            let candidate = &host_path[start..];
            if let Ok(ino) = self.lookup_path_to_ino(candidate).await {
                return Ok(ino);
            }
        }

        Err(libc::EXDEV.into())
    }

    #[cfg(target_os = "linux")]
    async fn ioctl_ficlone(&self, req: Request, dst_ino: u64, arg: u64) -> FuseResult<ReplyIoctl> {
        let src_fd = i32::try_from(arg).map_err(|_| Errno::from(libc::EINVAL))?;
        let src_ino = self
            .resolve_proc_fd_inode(req.pid, i64::from(src_fd))
            .await?;
        let src_attr = self
            .stat_ino(src_ino)
            .await
            .ok_or_else(|| Errno::from(libc::ENOENT))?;

        self.truncate_inode(dst_ino as i64, 0)
            .await
            .map_err(Into::<Errno>::into)?;
        self.copy_file_range_inodes(src_ino, 0, dst_ino as i64, 0, src_attr.size)
            .await
            .map_err(Into::<Errno>::into)?;
        self.truncate_inode(dst_ino as i64, src_attr.size)
            .await
            .map_err(Into::<Errno>::into)?;

        Ok(Self::ioctl_ok_reply())
    }

    #[cfg(target_os = "linux")]
    async fn ioctl_ficlonerange(
        &self,
        req: Request,
        dst_ino: u64,
        in_data: &[u8],
    ) -> FuseResult<ReplyIoctl> {
        let range = Self::parse_clone_range(in_data).ok_or_else(|| Errno::from(libc::EINVAL))?;
        let src_ino = self.resolve_proc_fd_inode(req.pid, range.src_fd).await?;
        self.copy_file_range_inodes(
            src_ino,
            range.src_offset,
            dst_ino as i64,
            range.dest_offset,
            range.src_length,
        )
        .await
        .map_err(Into::<Errno>::into)?;

        Ok(Self::ioctl_ok_reply())
    }

    async fn apply_new_entry_attrs(
        &self,
        ino: i64,
        uid: u32,
        gid: u32,
        mode: Option<u32>,
    ) -> Option<VfsFileAttr> {
        let sanitized_mode = mode.map(sanitize_special_mode_bits);
        if let Some(current) = self.stat_ino(ino).await {
            let mode_matches = sanitized_mode
                .map(|mode| current.mode & 0o7777 == mode)
                .unwrap_or(true);
            if current.uid == uid && current.gid == gid && mode_matches {
                return Some(current);
            }
        }

        let req = SetAttrRequest {
            uid: Some(uid),
            gid: Some(gid),
            mode: sanitized_mode,
            ..Default::default()
        };
        if attr_request_is_empty(&req) {
            return self.stat_ino(ino).await;
        }
        match self.set_attr(ino, &req, SetAttrFlags::empty()).await {
            Ok(attr) => Some(attr),
            Err(_err) => self.stat_ino(ino).await,
        }
    }

    async fn attr_for_create_result(
        &self,
        result: &CreateFileAtResult,
        uid: u32,
        gid: u32,
        mode: Option<u32>,
    ) -> Option<VfsFileAttr> {
        let sanitized_mode = mode.map(sanitize_special_mode_bits);
        if let Some(attr) = result.attr.as_ref() {
            if !result.created || result.attrs_applied {
                return Some(attr.clone());
            }

            let mode_matches = sanitized_mode
                .map(|mode| attr.mode & 0o7777 == mode)
                .unwrap_or(true);
            if attr.uid == uid && attr.gid == gid && mode_matches {
                return Some(attr.clone());
            }
        }

        if result.created && !result.attrs_applied {
            self.apply_new_entry_attrs(result.ino, uid, gid, mode).await
        } else {
            self.stat_ino(result.ino).await
        }
    }

    async fn clear_write_privilege_bits_if_needed(
        &self,
        ino: i64,
        writer_uid: u32,
    ) -> Result<(), VfsError> {
        if writer_uid == 0 {
            return Ok(());
        }

        let Some(attr) = self.stat_ino(ino).await else {
            return Err(VfsError::NotFound {
                path: PathHint::none(),
            });
        };
        if attr.kind != VfsFileType::File || attr.uid == writer_uid {
            return Ok(());
        }

        let mut flags = SetAttrFlags::empty();
        if attr.mode & 0o4000 != 0 {
            flags.insert(SetAttrFlags::CLEAR_SUID);
        }
        if attr.mode & 0o2000 != 0 {
            flags.insert(SetAttrFlags::CLEAR_SGID);
        }
        if flags.is_empty() {
            return Ok(());
        }

        self.set_attr(ino, &SetAttrRequest::default(), flags)
            .await
            .map(|_| ())
    }
}

fn fuse_lock_end_to_exclusive(end: u64) -> u64 {
    end.saturating_add(1)
}

fn exclusive_lock_end_to_fuse(end: u64) -> u64 {
    end.saturating_sub(1)
}

#[repr(C)]
#[derive(Clone, Copy)]
#[cfg(target_os = "linux")]
struct FileCloneRange {
    src_fd: i64,
    src_offset: u64,
    src_length: u64,
    dest_offset: u64,
}

#[allow(refining_impl_trait_reachable)]
impl<S, M> Filesystem for VFS<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    async fn init(&self, _req: Request) -> FuseResult<ReplyInit> {
        Ok(ReplyInit {
            max_write: NonZeroU32::new(BREWFS_FUSE_MAX_WRITE)
                .expect("BrewFS FUSE max_write must be non-zero"),
        })
    }

    async fn destroy(&self, _req: Request) {}

    // Call into VFS to resolve parent inode + name → child inode; if found, build ReplyEntry
    async fn lookup(&self, req: Request, parent: u64, name: &OsStr) -> FuseResult<ReplyEntry> {
        let name_str = name.to_string_lossy();
        debug!(
            unique = req.unique,
            parent,
            name = %name_str,
            "fuse.lookup"
        );

        // Virtual `.stats` file at mount root
        if parent as i64 == self.root_ino() && name_str == STATS_FILENAME {
            let now: Timestamp = std::time::SystemTime::now().into();
            let attr = asyncfuse::raw::reply::FileAttr {
                ino: STATS_INODE,
                size: STATS_FILE_SIZE,
                blocks: STATS_FILE_BLOCKS,
                atime: now,
                mtime: now,
                ctime: now,
                kind: FuseFileType::RegularFile,
                perm: 0o444,
                nlink: 1,
                uid: req.uid,
                gid: req.gid,
                rdev: 0,
                blksize: STATS_FILE_BLOCK_SIZE,
                #[cfg(target_os = "macos")]
                crtime: now,
                #[cfg(target_os = "macos")]
                flags: 0,
            };
            return Ok(ReplyEntry {
                ttl: Duration::from_secs(1),
                attr,
                generation: 0,
            });
        }

        validate_fuse_name(name_str.as_ref())?;

        let _timer = crate::vfs::stats::OpTimer::new(
            &self.stats().fuse_lookup_ops,
            &self.stats().fuse_lookup_lat_us,
        );

        self.ensure_access_allowed(parent as i64, req.uid, req.gid, libc::X_OK as u32)
            .await?;

        let name_str = name.to_string_lossy();
        let Some((_child_ino, vattr)) =
            self.child_attr_of(parent as i64, name_str.as_ref()).await?
        else {
            info!(parent, name = %name_str, "fuse.lookup ENOENT");
            return Err(libc::ENOENT.into());
        };
        let attr = vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr));
        // Keep generation at 0 and set TTL to 1s (tunable)
        Ok(ReplyEntry {
            ttl: fuse_cache_ttl(),
            attr,
            generation: 0,
        })
    }

    // Open file: allocate a handle for read/write operations.
    async fn open(&self, req: Request, ino: u64, flags: u32) -> FuseResult<ReplyOpen> {
        // Virtual .stats file: allow read-only open, no real file handle needed.
        if ino == STATS_INODE {
            let accmode = flags & (libc::O_ACCMODE as u32);
            if accmode != (libc::O_RDONLY as u32) {
                return Err(libc::EACCES.into());
            }
            return Ok(ReplyOpen { fh: 0, flags: 0 });
        }

        let accmode = flags & (libc::O_ACCMODE as u32);
        let read = accmode != (libc::O_WRONLY as u32);
        let write = accmode != (libc::O_RDONLY as u32);
        let append = (flags & libc::O_APPEND as u32) != 0;
        let truncate = (flags & libc::O_TRUNC as u32) != 0;
        debug!(
            ino,
            flags,
            read,
            write,
            has_append = append,
            has_trunc = truncate,
            has_creat = (flags & libc::O_CREAT as u32) != 0,
            "fuse.open"
        );
        if req.uid != 0 {
            self.ensure_inode_paths_search_allowed(ino as i64, req.uid, req.gid)
                .await?;
            self.ensure_access_allowed(ino as i64, req.uid, req.gid, open_flags_access_mask(flags))
                .await?;
        }
        if truncate {
            if !write {
                return Err(libc::EINVAL.into());
            }
            self.set_attr(
                ino as i64,
                &SetAttrRequest {
                    size: Some(0),
                    ..Default::default()
                },
                SetAttrFlags::empty(),
            )
            .await
            .map_err(Into::<Errno>::into)?;
        }
        let fh = self
            .open_fresh_ino(ino as i64, read, write, append)
            .await
            .map_err(Into::<Errno>::into)?;

        // ReplyOpen.flags carries FUSE FOPEN_* bits, not the caller's O_* flags.
        Ok(ReplyOpen {
            fh,
            flags: fuse_open_reply_flags(read, write),
        })
    }

    // Open directory: create handle for caching
    async fn opendir(&self, req: Request, ino: u64, _flags: u32) -> FuseResult<ReplyOpen> {
        debug!(ino, "fuse.opendir");
        let Some(attr) = self.stat_ino(ino as i64).await else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(attr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }
        self.ensure_access_allowed(ino as i64, req.uid, req.gid, opendir_access_mask())
            .await?;

        // Create directory handle for efficient readdir operations
        let fh = self
            .opendir(ino as i64)
            .await
            .map_err(Into::<Errno>::into)?;

        Ok(ReplyOpen { fh, flags: 0 })
    }

    // Read file: inode-based read
    async fn read(
        &self,
        req: Request,
        ino: u64,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> FuseResult<ReplyData> {
        // Virtual .stats file
        if ino == STATS_INODE {
            let content = self.stats().render();
            let bytes = content.as_bytes();
            let start = (offset as usize).min(bytes.len());
            let end = (start + size as usize).min(bytes.len());
            return Ok(ReplyData {
                data: Bytes::copy_from_slice(&bytes[start..end]),
            });
        }

        let _timer = crate::vfs::stats::OpTimer::new(
            &self.stats().fuse_read_ops,
            &self.stats().fuse_read_lat_us,
        );
        debug!(ino, fh, offset, size, "fuse.read");
        self.wait_for_prior_fuse_cached_writes(ino as i64, req.unique)
            .await;

        let data = if fh != 0 {
            match self.read(fh, offset, size as usize).await {
                Ok(data) => data,
                Err(VfsError::PermissionDenied { .. }) => {
                    // With writeback cache, the kernel can issue a read on an
                    // O_WRONLY fh to fill a partial page before writing it back.
                    let attr = self
                        .stat_ino(ino as i64)
                        .await
                        .ok_or_else(|| Errno::from(libc::ENOENT))?;
                    let tmp_fh = self
                        .open(ino as i64, attr, true, false, false)
                        .await
                        .map_err(Into::<Errno>::into)?;
                    let out = self
                        .read(tmp_fh, offset, size as usize)
                        .await
                        .map_err(Into::<Errno>::into)?;
                    let _ = self.close(tmp_fh).await;
                    out
                }
                Err(err) => return Err(err.into()),
            }
        } else {
            let attr = self
                .stat_ino(ino as i64)
                .await
                .ok_or_else(|| Errno::from(libc::ENOENT))?;
            let tmp_fh = self
                .open(ino as i64, attr, true, false, false)
                .await
                .map_err(Into::<Errno>::into)?;
            let out = self
                .read(tmp_fh, offset, size as usize)
                .await
                .map_err(Into::<Errno>::into)?;
            let _ = self.close(tmp_fh).await;
            out
        };

        self.stats()
            .fuse_read_bytes
            .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(ReplyData {
            data: Bytes::from(data),
        })
    }

    async fn readlink(&self, _req: Request, ino: u64) -> FuseResult<ReplyData> {
        debug!(ino, "fuse.readlink");
        let target = self.readlink_ino(ino as i64).await.map_err(Errno::from)?;

        // Update atime after successful readlink
        let _ = self.update_atime(ino as i64).await;

        Ok(ReplyData {
            data: Bytes::copy_from_slice(target.as_bytes()),
        })
    }

    async fn write(
        &self,
        req: Request,
        ino: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
        write_flags: u32,
        _flags: u32,
    ) -> FuseResult<ReplyWrite> {
        let _timer = crate::vfs::stats::OpTimer::new(
            &self.stats().fuse_write_ops,
            &self.stats().fuse_write_lat_us,
        );
        debug!(
            ino,
            fh,
            offset,
            size = data.len(),
            write_flags,
            "fuse.write"
        );
        let _cached_write_guard = if write_flags & FUSE_WRITE_CACHE != 0 && !data.is_empty() {
            Some(self.begin_fuse_cached_write(ino as i64, req.unique))
        } else {
            None
        };
        if fh == 0 && !data.is_empty() {
            self.ensure_access_allowed(ino as i64, req.uid, req.gid, inode_mutation_access_mask())
                .await?;
        }
        let n = if write_flags & FUSE_WRITE_CACHE != 0 {
            // Cached writes already contain the page data at the supplied
            // offset; applying O_APPEND again would duplicate the prefix.
            trace!(
                ino,
                fh,
                offset,
                len = data.len(),
                write_flags,
                "fuse.write -> write_ino (cache)"
            );
            let written = self
                .write_cached_ino(ino as i64, offset, data, req.unique)
                .await
                .map_err(Into::<Errno>::into)?;
            if fh != 0 && written > 0 {
                self.mark_handle_write_dirty(fh);
            }
            written as u32
        } else if fh != 0 {
            debug!(
                ino,
                fh,
                offset,
                len = data.len(),
                write_flags,
                "fuse.write -> write(fh)"
            );
            self.write(fh, offset, data)
                .await
                .map_err(Into::<Errno>::into)? as u32
        } else {
            debug!(
                ino,
                fh,
                offset,
                len = data.len(),
                write_flags,
                "fuse.write -> write_ino (no fh)"
            );
            self.write_ino(ino as i64, offset, data)
                .await
                .map_err(Into::<Errno>::into)? as u32
        };
        if n > 0 {
            self.clear_write_privilege_bits_if_needed(ino as i64, req.uid)
                .await
                .map_err(Into::<Errno>::into)?;
        }
        self.stats()
            .fuse_write_bytes
            .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(ReplyWrite { written: n })
    }

    // Ask VFS for inode attributes (flags ignored when fh is valid)
    async fn getattr(
        &self,
        req: Request,
        ino: u64,
        fh: Option<u64>,
        _flags: u32,
    ) -> FuseResult<ReplyAttr> {
        // Virtual .stats file
        if ino == STATS_INODE {
            let now: Timestamp = std::time::SystemTime::now().into();
            let attr = asyncfuse::raw::reply::FileAttr {
                ino: STATS_INODE,
                size: STATS_FILE_SIZE,
                blocks: STATS_FILE_BLOCKS,
                atime: now,
                mtime: now,
                ctime: now,
                kind: FuseFileType::RegularFile,
                perm: 0o444,
                nlink: 1,
                uid: req.uid,
                gid: req.gid,
                rdev: 0,
                blksize: STATS_FILE_BLOCK_SIZE,
                #[cfg(target_os = "macos")]
                crtime: now,
                #[cfg(target_os = "macos")]
                flags: 0,
            };
            return Ok(ReplyAttr {
                ttl: Duration::from_secs(1),
                attr,
            });
        }

        debug!(unique = req.unique, ino, fh = ?fh, "fuse.getattr");
        let vattr_opt = self.stat_ino(ino as i64).await;
        let vattr = if let Some(vattr) = vattr_opt {
            vattr
        } else if let Some(fh_value) = fh {
            let mut fallback_attr = self
                .handle_attr(fh_value)
                .ok_or_else(|| Errno::from(libc::ENOENT))?;
            fallback_attr.nlink = 0;
            fallback_attr
        } else if let Some(mut fallback_attr) = self.handle_attr_by_ino(ino as i64) {
            fallback_attr.nlink = 0;
            fallback_attr
        } else {
            return Err(libc::ENOENT.into());
        };

        let attr = vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr));
        Ok(ReplyAttr {
            ttl: fuse_cache_ttl(),
            attr,
        })
    }

    // Set attributes: delegate to metadata layer for mode/uid/gid/size/timestamps.
    // Userspace refines permission checks that need file-handle or utimensat semantics.
    //
    // POSIX mode changes preserve setuid/setgid/sticky; chown/write paths may
    // clear suid/sgid through explicit SetAttrFlags.
    async fn setattr(
        &self,
        req: Request,
        ino: u64,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> FuseResult<ReplyAttr> {
        debug!(unique = req.unique, ino, set_attr = ?set_attr, "fuse.setattr");
        let setattr_start = std::time::Instant::now();

        let (mut meta_req, mut meta_flags) = fuse_setattr_to_meta(&set_attr);

        // If no attributes to set, just return current attributes
        if attr_request_is_empty(&meta_req) && meta_flags.is_empty() {
            let Some(vattr) = self.stat_ino(ino as i64).await else {
                return Err(libc::ENOENT.into());
            };
            let attr = vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr));
            return Ok(ReplyAttr {
                ttl: fuse_cache_ttl(),
                attr,
            });
        }
        if fh.is_none() {
            self.ensure_inode_paths_search_allowed(ino as i64, req.uid, req.gid)
                .await?;
        }
        let write_handle_allows_truncate = fh
            .map(|fh| self.handle_allows_write_for_inode(fh, ino as i64))
            .unwrap_or(false);
        if !(setattr_is_truncate_with_optional_timestamps(&meta_req, &meta_flags)
            && write_handle_allows_truncate)
        {
            if setattr_is_timestamp_only(&meta_req, &meta_flags) {
                self.ensure_timestamp_setattr_allowed(ino as i64, req.uid, req.gid, &meta_req)
                    .await?;
            } else if setattr_is_mode_with_optional_timestamps(&meta_req, &meta_flags) {
                let requested_mode = meta_req
                    .mode
                    .expect("mode-with-optional-timestamps requests include mode");
                self.ensure_mode_setattr_allowed(ino as i64, req.uid, requested_mode)
                    .await?;
                meta_req.mode = Some(
                    self.effective_mode_setattr(
                        ino as i64,
                        req.uid,
                        req.gid,
                        req.pid,
                        requested_mode,
                    )
                    .await?,
                );
            } else if setattr_is_chown_with_optional_timestamps(&meta_req, &meta_flags) {
                let clear_suid_sgid = self
                    .ensure_chown_setattr_allowed(ino as i64, req.uid, req.gid, req.pid, &meta_req)
                    .await?;
                if clear_suid_sgid {
                    meta_flags.insert(SetAttrFlags::CLEAR_SUID | SetAttrFlags::CLEAR_SGID);
                }
            } else {
                self.ensure_access_allowed(
                    ino as i64,
                    req.uid,
                    req.gid,
                    inode_mutation_access_mask(),
                )
                .await?;
            }
        }

        // Apply the attribute changes
        let vattr = match self.set_attr(ino as i64, &meta_req, meta_flags).await {
            Ok(vattr) => {
                debug!(
                    unique = req.unique,
                    ino,
                    size = ?meta_req.size,
                    elapsed_ms = setattr_start.elapsed().as_millis() as u64,
                    "fuse.setattr complete"
                );
                vattr
            }
            Err(err) => {
                warn!(
                    unique = req.unique,
                    ino,
                    size = ?meta_req.size,
                    elapsed_ms = setattr_start.elapsed().as_millis() as u64,
                    error = %err,
                    "fuse.setattr failed"
                );
                return Err(Errno::from(err));
            }
        };

        let attr = vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr));

        // When a truncate (size change) is involved, use TTL=0 so the kernel
        // does not cache a potentially stale size.  A wrong cached size can
        // cause the kernel to call truncate_pagecache with the wrong boundary,
        // invalidating (or keeping) wrong pages in the writeback-cache path.
        let ttl = if meta_req.size.is_some() {
            Duration::ZERO
        } else {
            fuse_cache_ttl()
        };
        Ok(ReplyAttr { ttl, attr })
    }

    // Call VFS to list directory and stream DirectoryEntry items (with error/offset handling)
    async fn readdir<'a>(
        &'a self,
        _req: Request,
        ino: u64,
        fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectory<BoxStream<'a, FuseResult<DirectoryEntry>>>> {
        debug!(ino, fh, offset, "fuse.readdir");
        // Rewinddir: offset ≤ 0 means restart from the beginning.
        // Replace the cached handle with a fresh snapshot from the meta
        // layer so that entries created after opendir(3) are visible.
        if fh != 0 && offset == 0 {
            let _ = self.refresh_dir_handle(fh).await;
        }

        // Try to use handle first. FUSE directory offsets identify the next
        // entry to read: 0=start, 1=after ".", 2=after "..", and 3+=child index+1.
        let (entries, entries_offset, include_dot_entries, include_dotdot_only) = if fh != 0 {
            match offset {
                i64::MIN..=0 => (self.readdir(fh, 0), 0, true, false),
                1 => (self.readdir(fh, 0), 0, false, true),
                _ => {
                    let entries_offset = (offset as u64).saturating_sub(2);
                    (
                        self.readdir(fh, entries_offset),
                        entries_offset,
                        false,
                        false,
                    )
                }
            }
        } else {
            (None, offset.max(0) as u64, false, false)
        };

        // Fallback to stateless mode if handle not found
        let entries = if let Some(e) = entries {
            e
        } else {
            // Fallback: directly read from meta layer
            let meta_entries = self.readdir_ino(ino as i64).await;
            match meta_entries {
                Some(v) => v,
                None => {
                    if self.stat_ino(ino as i64).await.is_some() {
                        return Err(libc::ENOTDIR.into());
                    } else {
                        return Err(libc::ENOENT.into());
                    }
                }
            }
        };

        // Assemble entries including '.' and '..'; offsets reference the previous entry so start at offset+1
        let mut all: Vec<DirectoryEntry> = Vec::with_capacity(entries.len() + 2);

        // Add "." and ".." entries for handle-based reads
        if fh != 0 && include_dot_entries {
            all.push(DirectoryEntry {
                inode: ino,
                kind: FuseFileType::Directory,
                name: OsString::from("."),
                offset: 1,
            });
            let parent_ino = self
                .parent_of(ino as i64)
                .await
                .unwrap_or_else(|| self.root_ino()) as u64;
            all.push(DirectoryEntry {
                inode: parent_ino,
                kind: FuseFileType::Directory,
                name: OsString::from(".."),
                offset: 2,
            });
        } else if fh != 0 && include_dotdot_only {
            let parent_ino = self
                .parent_of(ino as i64)
                .await
                .unwrap_or_else(|| self.root_ino()) as u64;
            all.push(DirectoryEntry {
                inode: parent_ino,
                kind: FuseFileType::Directory,
                name: OsString::from(".."),
                offset: 2,
            });
        }

        // Actual child entries
        for (i, e) in entries.iter().enumerate() {
            all.push(DirectoryEntry {
                inode: e.ino as u64,
                kind: vfs_kind_to_fuse(e.kind),
                name: OsString::from(e.name.clone()),
                offset: (entries_offset + i as u64 + 3) as i64,
            });
        }

        let stream_iter = stream::iter(all.into_iter().map(Ok));
        let boxed: BoxStream<'a, FuseResult<DirectoryEntry>> = Box::pin(stream_iter);
        Ok(ReplyDirectory { entries: boxed })
    }

    // Directory read with attributes (lookup + readdir), returning DirectoryEntryPlus
    async fn readdirplus<'a>(
        &'a self,
        req: Request,
        ino: u64,
        fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> FuseResult<ReplyDirectoryPlus<BoxStream<'a, FuseResult<DirectoryEntryPlus>>>> {
        debug!(unique = req.unique, ino, fh, offset, "fuse.readdirplus");
        let ttl = fuse_cache_ttl();
        let mut all: Vec<DirectoryEntryPlus> = Vec::new();

        // Rewinddir: same logic as readdir().
        if fh != 0 && offset == 0 {
            let _ = self.refresh_dir_handle(fh).await;
        }

        // Try to use handle first with the same offset mapping as readdir().
        let (entries_from_handle, entries_offset, include_dot_entries, include_dotdot_only) =
            if fh != 0 {
                match offset {
                    0 => (self.readdir(fh, 0), 0, true, false),
                    1 => (self.readdir(fh, 0), 0, false, true),
                    _ => {
                        let entries_offset = offset.saturating_sub(2);
                        (
                            self.readdir(fh, entries_offset),
                            entries_offset,
                            false,
                            false,
                        )
                    }
                }
            } else {
                (None, offset.saturating_sub(2), false, false)
            };

        if fh != 0 && include_dot_entries {
            if let Some(attr) = self.stat_ino(ino as i64).await {
                let fattr = vfs_to_fuse_attr(&attr, &req, self.blocks_for_attr(&attr));
                all.push(DirectoryEntryPlus {
                    inode: ino,
                    generation: 0,
                    kind: FuseFileType::Directory,
                    name: OsString::from("."),
                    offset: 1,
                    attr: fattr,
                    entry_ttl: ttl,
                    attr_ttl: ttl,
                });
            } else {
                return Err(libc::ENOENT.into());
            }
            let parent_ino = self
                .parent_of(ino as i64)
                .await
                .unwrap_or_else(|| self.root_ino()) as u64;
            if let Some(pattr) = self.stat_ino(parent_ino as i64).await {
                let f = vfs_to_fuse_attr(&pattr, &req, self.blocks_for_attr(&pattr));
                all.push(DirectoryEntryPlus {
                    inode: parent_ino,
                    generation: 0,
                    kind: FuseFileType::Directory,
                    name: OsString::from(".."),
                    offset: 2,
                    attr: f,
                    entry_ttl: ttl,
                    attr_ttl: ttl,
                });
            }
        } else if fh != 0 && include_dotdot_only {
            let parent_ino = self
                .parent_of(ino as i64)
                .await
                .unwrap_or_else(|| self.root_ino()) as u64;
            if let Some(pattr) = self.stat_ino(parent_ino as i64).await {
                let f = vfs_to_fuse_attr(&pattr, &req, self.blocks_for_attr(&pattr));
                all.push(DirectoryEntryPlus {
                    inode: parent_ino,
                    generation: 0,
                    kind: FuseFileType::Directory,
                    name: OsString::from(".."),
                    offset: 2,
                    attr: f,
                    entry_ttl: ttl,
                    attr_ttl: ttl,
                });
            }
        }

        // Fallback to stateless mode if handle not found
        let entries = if let Some(e) = entries_from_handle {
            e
        } else {
            // Fallback: directly read from meta layer
            let meta_entries = self.readdir_ino(ino as i64).await;
            match meta_entries {
                Some(v) => v,
                None => {
                    if self.stat_ino(ino as i64).await.is_some() {
                        return Err(libc::ENOTDIR.into());
                    } else {
                        return Err(libc::ENOENT.into());
                    }
                }
            }
        };

        for (i, e) in entries.iter().enumerate() {
            let Some(cattr) = self.stat_ino(e.ino).await else {
                continue;
            };
            let fattr = vfs_to_fuse_attr(&cattr, &req, self.blocks_for_attr(&cattr));
            all.push(DirectoryEntryPlus {
                inode: e.ino as u64,
                generation: 0,
                kind: vfs_kind_to_fuse(e.kind),
                name: OsString::from(e.name.clone()),
                offset: (entries_offset + i as u64 + 3) as i64,
                attr: fattr,
                entry_ttl: ttl,
                attr_ttl: ttl,
            });
        }

        let stream_iter = stream::iter(all.into_iter().map(Ok));
        let boxed: BoxStream<'a, FuseResult<DirectoryEntryPlus>> = Box::pin(stream_iter);
        Ok(ReplyDirectoryPlus { entries: boxed })
    }

    // Filesystem statfs: best-effort statistics from MetaStore
    async fn statfs(&self, _req: Request, _ino: u64) -> FuseResult<ReplyStatFs> {
        let bsize: u32 = 4096;
        let frsize: u32 = 4096;
        let (blocks, bfree, bavail, files, ffree) = match self.stat_fs_cached_for_fuse().await {
            Ok(snapshot) => {
                let blocks = snapshot.total_space / frsize as u64;
                let bfree = snapshot.available_space / frsize as u64;
                let bavail = bfree;
                let files = snapshot
                    .used_inodes
                    .saturating_add(snapshot.available_inodes);
                let ffree = snapshot.available_inodes;
                (blocks, bfree, bavail, files, ffree)
            }
            Err(e) => {
                error!("statfs failed: {e}");
                (0, 0, 0, 0, u64::MAX)
            }
        };
        Ok(ReplyStatFs {
            blocks,
            bfree,
            bavail,
            files,
            ffree,
            bsize,
            namelen: NAME_MAX as u32,
            frsize,
        })
    }

    // Create a special file node (regular file, FIFO, etc.)
    // Note: Special files beyond regular/dir are not supported in this implementation
    async fn mknod(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> FuseResult<ReplyEntry> {
        debug!(
            unique = req.unique,
            parent,
            name = %name.to_string_lossy(),
            mode,
            "fuse.mknod"
        );
        let name = name.to_string_lossy();
        validate_fuse_name(name.as_ref())?;
        const S_IFMT: u32 = libc::S_IFMT as u32;
        const S_IFREG: u32 = libc::S_IFREG as u32;
        const S_IFDIR: u32 = libc::S_IFDIR as u32;
        const S_IFIFO: u32 = libc::S_IFIFO as u32;
        const S_IFSOCK: u32 = libc::S_IFSOCK as u32;
        const S_IFCHR: u32 = libc::S_IFCHR as u32;
        const S_IFBLK: u32 = libc::S_IFBLK as u32;
        let file_type = mode & S_IFMT;

        let (ino, created_attr) = match file_type {
            // Linux accepts mknod(path, 0, 0) as a regular file with mode 000.
            0 | S_IFREG => {
                self.ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid)
                    .await?;
                let result = self
                    .create_file_at_with_attrs(parent as i64, &name, true, mode, req.uid, req.gid)
                    .await
                    .map_err(Errno::from)?;
                let attr = self
                    .attr_for_create_result(&result, req.uid, req.gid, Some(mode))
                    .await;
                (result.ino, attr)
            }
            S_IFDIR => {
                self.ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid)
                    .await?;
                let ino = self
                    .mkdir_at_new(parent as i64, &name)
                    .await
                    .map_err(Errno::from)?;
                (ino, None)
            }
            S_IFIFO | S_IFSOCK | S_IFCHR | S_IFBLK => {
                self.ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid)
                    .await?;
                let kind = match file_type {
                    S_IFIFO => VfsFileType::Fifo,
                    S_IFSOCK => VfsFileType::Socket,
                    S_IFCHR => VfsFileType::CharDevice,
                    S_IFBLK => VfsFileType::BlockDevice,
                    _ => unreachable!("special file type already matched"),
                };
                let ino = self
                    .create_special_node_at(
                        parent as i64,
                        &name,
                        kind,
                        mode,
                        req.uid,
                        req.gid,
                        rdev,
                    )
                    .await
                    .map_err(Errno::from)?;
                (ino, self.stat_ino(ino).await)
            }
            _ => {
                return Err(libc::EINVAL.into());
            }
        };

        // Apply mode after normalizing to POSIX permission bits.
        let vattr = if let Some(attr) = created_attr {
            attr
        } else if let Some(attr) = self
            .apply_new_entry_attrs(ino, req.uid, req.gid, Some(mode))
            .await
        {
            attr
        } else {
            return Err(libc::ENOENT.into());
        };

        let attr = vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr));
        Ok(ReplyEntry {
            ttl: fuse_cache_ttl(),
            attr,
            generation: 0,
        })
    }

    // Create a single-level directory; return EEXIST if it already exists.
    async fn mkdir(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> FuseResult<ReplyEntry> {
        debug!(
            unique = req.unique,
            parent,
            name = %name.to_string_lossy(),
            mode,
            umask,
            "fuse.mkdir"
        );
        let name = name.to_string_lossy();
        validate_fuse_name(name.as_ref())?;
        self.ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid)
            .await?;
        let _ino = self
            .mkdir_at_new(parent as i64, &name)
            .await
            .map_err(Errno::from)?;
        // Preserve setuid/setgid/sticky, then apply the caller's umask to rwx bits.
        let masked_mode = apply_creation_umask(mode, umask);
        let Some(vattr) = self
            .apply_new_entry_attrs(_ino, req.uid, req.gid, Some(masked_mode))
            .await
        else {
            return Err(libc::ENOENT.into());
        };
        let attr = vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr));
        Ok(ReplyEntry {
            ttl: fuse_cache_ttl(),
            attr,
            generation: 0,
        })
    }

    // Create and open a file
    async fn create(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> FuseResult<ReplyCreated> {
        debug!(
            unique = req.unique,
            parent,
            name = %name.to_string_lossy(),
            mode,
            flags,
            "fuse.create"
        );
        let name = name.to_string_lossy();
        validate_fuse_name(name.as_ref())?;
        let create_new = (flags & libc::O_EXCL as u32) != 0;
        self.ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid)
            .await?;
        let create_result = match self
            .create_file_at_with_attrs(parent as i64, &name, create_new, mode, req.uid, req.gid)
            .await
        {
            Ok(result) => {
                debug!(
                    ino = result.ino,
                    name = %name,
                    flags,
                    created = result.created,
                    attrs_applied = result.attrs_applied,
                    has_append = (flags & libc::O_APPEND as u32) != 0,
                    "fuse.create ok"
                );
                result
            }
            Err(VfsError::AlreadyExists { .. }) if !create_new => {
                debug!(name = %name, "fuse.create EEXIST, falling back to lookup");
                let ino = self.child_of(parent as i64, &name).await.ok_or_else(|| {
                    debug!(name = %name, "fuse.create fallback lookup also failed");
                    Errno::from(libc::EIO)
                })?;
                CreateFileAtResult {
                    ino,
                    attr: None,
                    created: false,
                    attrs_applied: false,
                }
            }
            Err(e) => {
                debug!(name = %name, flags, error = %e, "fuse.create err");
                return Err(Errno::from(e));
            }
        };
        let Some(vattr) = self
            .attr_for_create_result(&create_result, req.uid, req.gid, Some(mode))
            .await
        else {
            return Err(libc::ENOENT.into());
        };
        let vattr = if !create_result.created && (flags & libc::O_TRUNC as u32) != 0 {
            self.set_attr(
                create_result.ino,
                &SetAttrRequest {
                    size: Some(0),
                    ..Default::default()
                },
                SetAttrFlags::empty(),
            )
            .await
            .map_err(Into::<Errno>::into)?
        } else {
            vattr
        };
        let attr = vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr));

        let accmode = flags & (libc::O_ACCMODE as u32);
        let read = accmode != (libc::O_WRONLY as u32);
        let write = accmode != (libc::O_RDONLY as u32);
        let append = (flags & libc::O_APPEND as u32) != 0;
        let fh = self
            .open_with_cached_attr(create_result.ino, vattr.clone(), read, write, append)
            .await
            .map_err(Into::<Errno>::into)?;
        Ok(ReplyCreated {
            ttl: fuse_create_cache_ttl(),
            attr,
            generation: 0,
            fh,
            flags: fuse_open_reply_flags(read, write),
        })
    }

    async fn link(
        &self,
        req: Request,
        ino: u64,
        new_parent: u64,
        new_name: &OsStr,
    ) -> FuseResult<ReplyEntry> {
        debug!(
            unique = req.unique,
            ino,
            new_parent,
            new_name = %new_name.to_string_lossy(),
            "fuse.link"
        );
        let Some(existing_attr) = self.stat_ino(ino as i64).await else {
            return Err(libc::ENOENT.into());
        };
        if matches!(existing_attr.kind, VfsFileType::Dir) {
            return Err(libc::EISDIR.into());
        }

        let Some(parent_attr) = self.stat_ino(new_parent as i64).await else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(parent_attr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }

        let new_name_str = new_name.to_string_lossy();
        validate_fuse_name(new_name_str.as_ref())?;
        self.ensure_directory_parent_namespace_mutation_allowed(new_parent, req.uid, req.gid)
            .await?;
        self.ensure_inode_paths_search_allowed(ino as i64, req.uid, req.gid)
            .await?;

        // Use the inode directly from the FUSE request; avoid roundtripping through path_of
        // which can return None if path reconstruction races with concurrent operations.
        let attr = self
            .link_by_ino(ino as i64, new_parent as i64, &new_name_str)
            .await
            .map_err(|e| match e {
                VfsError::AlreadyExists { .. } => {
                    info!(ino, new_parent, new_name = %new_name_str, "fuse.link EEXIST");
                    Errno::from(libc::EEXIST)
                }
                VfsError::NotFound { .. } => {
                    info!(ino, new_parent, new_name = %new_name_str, "fuse.link ENOENT");
                    Errno::from(libc::ENOENT)
                }
                VfsError::IsADirectory { .. } => Errno::from(libc::EISDIR),
                VfsError::NotADirectory { .. } => Errno::from(libc::ENOTDIR),
                VfsError::TooManyLinks => Errno::from(libc::EMLINK),
                VfsError::InvalidFilename => Errno::from(libc::EINVAL),
                VfsError::FilenameTooLong { .. } => Errno::from(libc::ENAMETOOLONG),
                other => {
                    info!(ino, new_parent, new_name = %new_name_str, error = %other, "fuse.link err");
                    Errno::from(libc::EIO)
                }
            })?;

        let fuse_attr = vfs_to_fuse_attr(&attr, &req, self.blocks_for_attr(&attr));
        Ok(ReplyEntry {
            ttl: fuse_cache_ttl(),
            attr: fuse_attr,
            generation: 0,
        })
    }

    async fn symlink(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        link: &OsStr,
    ) -> FuseResult<ReplyEntry> {
        debug!(
            unique = req.unique,
            parent,
            name = %name.to_string_lossy(),
            link = %link.to_string_lossy(),
            "fuse.symlink"
        );
        let name = name.to_string_lossy();
        validate_fuse_name(name.as_ref())?;

        self.ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid)
            .await?;

        if self.child_of(parent as i64, name.as_ref()).await.is_some() {
            return Err(libc::EEXIST.into());
        }

        let target = link.to_string_lossy();

        let (ino, vattr) = self
            .create_symlink_at(parent as i64, &name, target.as_ref())
            .await
            .map_err(Errno::from)?;

        let attr = self
            .apply_new_entry_attrs(ino, req.uid, req.gid, None)
            .await
            .unwrap_or(vattr);

        Ok(ReplyEntry {
            ttl: fuse_cache_ttl(),
            attr: vfs_to_fuse_attr(&attr, &req, self.blocks_for_attr(&attr)),
            generation: 0,
        })
    }

    // Remove a file
    async fn unlink(&self, req: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        debug!(parent, name = %name.to_string_lossy(), "fuse.unlink");
        let name = name.to_string_lossy();
        validate_fuse_name(name.as_ref())?;
        self.ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid)
            .await?;
        // Target must exist and be a file. Keep the resolved inode/attr so VFS
        // does not need to repeat the same lookup/stat work before unlinking.
        let Some(child) = self.child_of(parent as i64, name.as_ref()).await else {
            return Err(libc::ENOENT.into());
        };
        let Some(cattr) = self.stat_ino(child).await else {
            return Err(libc::ENOENT.into());
        };
        if matches!(cattr.kind, VfsFileType::Dir) {
            return Err(libc::EISDIR.into());
        }
        self.ensure_sticky_parent_allows_child_mutation(parent, child, req.uid)
            .await?;
        self.unlink_at_with_known_attr(parent as i64, &name, child, cattr)
            .await
            .map_err(Errno::from)
    }

    // Remove an empty directory
    async fn rmdir(&self, req: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        debug!(parent, name = %name.to_string_lossy(), "fuse.rmdir");
        let name = name.to_string_lossy();
        validate_fuse_name(name.as_ref())?;
        self.ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid)
            .await?;
        // Target must be a directory
        let Some(child) = self.child_of(parent as i64, name.as_ref()).await else {
            return Err(libc::ENOENT.into());
        };
        let Some(cattr) = self.stat_ino(child).await else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(cattr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }
        self.ensure_sticky_parent_allows_child_mutation(parent, child, req.uid)
            .await?;
        self.rmdir_at(parent as i64, &name)
            .await
            .map_err(Errno::from)
    }

    // Rename (files or directories)
    async fn rename(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> FuseResult<()> {
        debug!(
            parent,
            name = %name.to_string_lossy(),
            new_parent,
            new_name = %new_name.to_string_lossy(),
            "fuse.rename"
        );
        let name = name.to_string_lossy();
        let new_name = new_name.to_string_lossy();

        validate_fuse_name(name.as_ref())?;
        validate_fuse_name(new_name.as_ref())?;

        // POSIX rename to the same location is a no-op.
        if parent == new_parent && name == new_name {
            return Ok(());
        }

        // Ensure the source exists and keep its attributes for the later VFS
        // rename checks instead of statting it again.
        let (src_ino, src_attr) = match self.child_attr_of(parent as i64, name.as_ref()).await {
            Ok(Some(attr)) => attr,
            Ok(None) => return Err(libc::ENOENT.into()),
            Err(err) => return Err(Errno::from(err)),
        };

        // Validate the destination parent
        let Some(pattr) = self.stat_ino(new_parent as i64).await else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(pattr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }

        self.ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid)
            .await?;
        if parent != new_parent {
            self.ensure_directory_parent_namespace_mutation_allowed(new_parent, req.uid, req.gid)
                .await?;
        }
        self.ensure_sticky_parent_allows_child_mutation(parent, src_ino, req.uid)
            .await?;
        let dst_ino = self.child_of(new_parent as i64, new_name.as_ref()).await;
        if let Some(dst_ino) = dst_ino {
            self.ensure_sticky_parent_allows_child_mutation(new_parent, dst_ino, req.uid)
                .await?;
        }
        if parent != new_parent && matches!(src_attr.kind, VfsFileType::Dir) {
            self.ensure_access_allowed(src_ino, req.uid, req.gid, namespace_mutation_access_mask())
                .await?;
        }

        // Flush pending writes for the source inode before the rename so
        // that temp-file + rename patterns (e.g. object_store PutMode::Create)
        // do not race with in-flight write-back commit tasks.
        self.flush_inode(src_ino as u64).await;

        self.rename_at_with_known_attrs(
            parent as i64,
            &name,
            new_parent as i64,
            new_name.to_string(),
            src_ino,
            &src_attr,
            &pattr,
            Some(dst_ino),
        )
        .await
            .map_err(|e| {
                match e {
                    VfsError::NotFound { .. } => libc::ENOENT,
                    VfsError::AlreadyExists { .. } => libc::EEXIST,
                    VfsError::NotADirectory { .. } => libc::ENOTDIR,
                    VfsError::IsADirectory { .. } => libc::EISDIR,
                    VfsError::DirectoryNotEmpty { .. } => libc::ENOTEMPTY,
                    VfsError::PermissionDenied { .. } => libc::EACCES,
                    VfsError::CircularRename { .. } => libc::EINVAL,
                    VfsError::InvalidRenameTarget { .. } => libc::EINVAL,
                    VfsError::InvalidFilename => libc::EINVAL,
                    VfsError::FilenameTooLong { .. } => libc::ENAMETOOLONG,
                    VfsError::CrossesDevices => libc::EXDEV,
                    other => {
                        warn!(error = ?other, parent, %name, new_parent, %new_name, "unhandled VFS error during rename, mapped to EIO");
                        libc::EIO
                    }
                }
                .into()
            })
    }

    // ===== Resource release & sync: stateless implementation, return success =====
    // Close file handle
    async fn release(
        &self,
        _req: Request,
        inode: u64,
        fh: u64,
        _flags: u32,
        lock_owner: u64,
        _flush: bool,
    ) -> FuseResult<()> {
        // Virtual .stats file: no real handle to close.
        if inode == STATS_INODE {
            return Ok(());
        }
        debug!(fh, "fuse.release");
        self.unlock_owner_locks(inode, lock_owner).await;
        self.unlock_handle_locks(inode, fh).await;
        self.close(fh).await.map_err(Errno::from)?;
        Ok(())
    }

    // Flush file data to backend (called on every close of a file descriptor).
    // Must actually persist dirty data so that close() semantics are honored.
    async fn flush(&self, req: Request, inode: u64, fh: u64, lock_owner: u64) -> FuseResult<()> {
        // Virtual .stats file: nothing to flush.
        if inode == STATS_INODE {
            return Ok(());
        }
        debug!(fh, inode, "fuse.flush");
        self.wait_for_prior_fuse_cached_writes(inode as i64, req.unique)
            .await;
        self.unlock_owner_locks(inode, lock_owner).await;
        self.flush_dirty_handle_snapshot(fh)
            .await
            .map_err(Errno::from)?;
        Ok(())
    }

    // Sync file content to backend
    async fn fsync(&self, req: Request, inode: u64, fh: u64, datasync: bool) -> FuseResult<()> {
        debug!(fh, datasync, "fuse.fsync");
        self.wait_for_prior_fuse_cached_writes(inode as i64, req.unique)
            .await;
        self.fsync_snapshot(fh, datasync).await.map_err(Errno::from)
    }

    async fn fallocate(
        &self,
        req: Request,
        inode: u64,
        fh: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> FuseResult<()> {
        debug!(inode, fh, offset, length, mode, "fuse.fallocate");
        if mode != 0 {
            return Err(libc::EOPNOTSUPP.into());
        }
        if length > 0 {
            self.ensure_access_allowed(
                inode as i64,
                req.uid,
                req.gid,
                inode_mutation_access_mask(),
            )
            .await?;
            self.wait_for_prior_fuse_cached_writes(inode as i64, req.unique)
                .await;
        }
        if fh != 0 {
            self.fallocate_handle(fh, inode as i64, offset, length)
                .await
                .map_err(Errno::from)
        } else {
            self.fallocate_ino(inode as i64, offset, length)
                .await
                .map_err(Errno::from)
        }
    }

    async fn lseek(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        whence: u32,
    ) -> FuseResult<ReplyLSeek> {
        debug!(inode, offset, whence, "fuse.lseek");
        let Some(attr) = self.stat_ino(inode as i64).await else {
            return Err(libc::ENOENT.into());
        };
        if matches!(attr.kind, VfsFileType::Dir) {
            return Err(libc::EISDIR.into());
        }

        let size = attr.size as i128;
        let signed_offset = offset as i64 as i128;
        let next = match whence as i32 {
            libc::SEEK_SET => signed_offset,
            libc::SEEK_CUR => signed_offset,
            libc::SEEK_END => size + signed_offset,
            libc::SEEK_DATA => {
                if signed_offset < 0 || signed_offset >= size {
                    return Err(libc::ENXIO.into());
                }
                signed_offset
            }
            libc::SEEK_HOLE => {
                if signed_offset < 0 || signed_offset >= size {
                    return Err(libc::ENXIO.into());
                }
                size
            }
            _ => return Err(libc::EINVAL.into()),
        };

        if next < 0 || next > u64::MAX as i128 {
            return Err(libc::EINVAL.into());
        }
        Ok(ReplyLSeek {
            offset: next as u64,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn copy_file_range(
        &self,
        _req: Request,
        inode: u64,
        fh_in: u64,
        off_in: u64,
        inode_out: u64,
        fh_out: u64,
        off_out: u64,
        length: u64,
        flags: u64,
    ) -> FuseResult<ReplyCopyFileRange> {
        debug!(
            inode,
            fh_in, off_in, inode_out, fh_out, off_out, length, flags, "fuse.copy_file_range"
        );

        if flags != 0 {
            return Err(libc::EINVAL.into());
        }
        if !fuse_copy_file_range_enabled() {
            return Err(libc::ENOSYS.into());
        }

        let copied = self
            .copy_file_range(fh_in, off_in, fh_out, off_out, length)
            .await
            .map_err(Into::<Errno>::into)? as u64;

        Ok(ReplyCopyFileRange { copied })
    }

    async fn ioctl(
        &self,
        req: Request,
        inode: u64,
        _fh: u64,
        flags: u32,
        cmd: u32,
        arg: u64,
        in_data: &[u8],
        _out_size: u32,
    ) -> FuseResult<ReplyIoctl> {
        debug!(
            inode,
            flags,
            cmd,
            arg,
            in_size = in_data.len(),
            pid = req.pid,
            "fuse.ioctl"
        );

        if flags != 0 {
            return Err(libc::EOPNOTSUPP.into());
        }

        #[cfg(target_os = "linux")]
        {
            match cmd {
                x if x == libc::FICLONE as u32 => self.ioctl_ficlone(req, inode, arg).await,
                x if x == libc::FICLONERANGE as u32 => {
                    self.ioctl_ficlonerange(req, inode, in_data).await
                }
                _ => Err(libc::EOPNOTSUPP.into()),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(libc::EOPNOTSUPP.into())
        }
    }

    async fn setxattr(
        &self,
        _req: Request,
        inode: u64,
        name: &OsStr,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> FuseResult<()> {
        if position != 0 {
            return Err(libc::EINVAL.into());
        }
        if self.stat_ino(inode as i64).await.is_none() {
            return Err(libc::ENOENT.into());
        }
        let name = name.to_string_lossy();
        self.set_xattr_ino(inode as i64, &name, value, flags)
            .await
            .map_err(|e| match e {
                VfsError::AlreadyExists { .. } => Errno::from(libc::EEXIST),
                VfsError::Unsupported => Errno::from(libc::ENOSYS),
                VfsError::NotFound { .. } => Errno::from(libc::ENODATA),
                _ => Errno::from(libc::EIO),
            })
    }

    async fn getxattr(
        &self,
        _req: Request,
        inode: u64,
        name: &OsStr,
        size: u32,
    ) -> FuseResult<ReplyXAttr> {
        if self.stat_ino(inode as i64).await.is_none() {
            return Err(libc::ENOENT.into());
        }
        let name = name.to_string_lossy();
        let value = self
            .get_xattr_ino(inode as i64, &name)
            .await
            .map_err(|e| match e {
                VfsError::Unsupported => Errno::from(libc::ENOSYS),
                _ => Errno::from(libc::EIO),
            })?
            .ok_or_else(|| Errno::from(libc::ENODATA))?;
        if size == 0 {
            return Ok(ReplyXAttr::Size(value.len() as u32));
        }
        if (size as usize) < value.len() {
            return Err(libc::ERANGE.into());
        }
        Ok(ReplyXAttr::Data(Bytes::from(value)))
    }

    async fn listxattr(&self, _req: Request, inode: u64, size: u32) -> FuseResult<ReplyXAttr> {
        if self.stat_ino(inode as i64).await.is_none() {
            return Err(libc::ENOENT.into());
        }
        let names = self
            .list_xattr_ino(inode as i64)
            .await
            .map_err(|e| match e {
                VfsError::Unsupported => Errno::from(libc::ENOSYS),
                _ => Errno::from(libc::EIO),
            })?;
        let total_len: usize = names.iter().map(|n| n.len() + 1).sum();
        if size == 0 {
            return Ok(ReplyXAttr::Size(total_len as u32));
        }
        if (size as usize) < total_len {
            return Err(libc::ERANGE.into());
        }
        let mut data = Vec::with_capacity(total_len);
        for name in names {
            data.extend_from_slice(name.as_bytes());
            data.push(0);
        }
        Ok(ReplyXAttr::Data(Bytes::from(data)))
    }

    async fn removexattr(&self, _req: Request, inode: u64, name: &OsStr) -> FuseResult<()> {
        if self.stat_ino(inode as i64).await.is_none() {
            return Err(libc::ENOENT.into());
        }
        let name = name.to_string_lossy();
        self.remove_xattr_ino(inode as i64, &name)
            .await
            .map_err(|e| match e {
                VfsError::Unsupported => libc::ENOSYS.into(),
                VfsError::NotFound { .. } => libc::ENODATA.into(),
                _ => libc::EIO.into(),
            })
    }

    // Close directory handle
    async fn releasedir(&self, _req: Request, _inode: u64, fh: u64, _flags: u32) -> FuseResult<()> {
        debug!(fh, "fuse.releasedir");
        if fh == 0 {
            return Ok(()); // No handle to release
        }

        if let Err(e) = self.closedir(fh) {
            match e {
                VfsError::StaleNetworkFileHandle => {
                    // Handle not found, but that's ok - might be a stateless readdir
                    debug!("releasedir: handle {} not found (stateless mode)", fh);
                }
                _ => {
                    error!("Error releasing directory handle {}: {:?}", fh, e);
                    return Err(libc::EIO.into());
                }
            }
        }
        Ok(())
    }

    // Sync directory content to backend
    async fn fsyncdir(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _datasync: bool,
    ) -> FuseResult<()> {
        Ok(())
    }

    // Test for a POSIX file lock
    async fn getlk(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        lock_type: u32,
        _pid: u32,
    ) -> FuseResult<ReplyLock> {
        debug!(inode, lock_owner, start, end, lock_type, "fuse.getlk");
        // Convert FUSE lock type to our internal type
        let fl_type = match lock_type {
            x if x == libc::F_RDLCK as u32 => FileLockType::Read,
            x if x == libc::F_WRLCK as u32 => FileLockType::Write,
            x if x == libc::F_UNLCK as u32 => FileLockType::UnLock,
            _ => return Err(libc::EINVAL.into()),
        };

        let query = FileLockQuery {
            owner: lock_owner as i64,
            lock_type: fl_type,
            range: FileLockRange {
                start,
                end: fuse_lock_end_to_exclusive(end),
            },
        };

        match self.get_plock_ino(inode as i64, &query).await {
            Ok(info) => {
                // Convert internal lock type back to FUSE type
                let fuse_type = match info.lock_type {
                    FileLockType::Read => libc::F_RDLCK as u32,
                    FileLockType::Write => libc::F_WRLCK as u32,
                    FileLockType::UnLock => libc::F_UNLCK as u32,
                };
                Ok(ReplyLock {
                    r#type: fuse_type,
                    start: info.range.start,
                    end: exclusive_lock_end_to_fuse(info.range.end),
                    pid: info.pid,
                })
            }
            Err(e) => Err(Errno::from(e)),
        }
    }

    // Acquire, modify or release a POSIX file lock
    async fn setlk(
        &self,
        _req: Request,
        inode: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        lock_type: u32,
        pid: u32,
        block: bool,
    ) -> FuseResult<()> {
        debug!(
            inode,
            lock_owner, start, end, lock_type, pid, block, "fuse.setlk"
        );
        // Convert FUSE lock type to our internal type
        let fl_type = match lock_type {
            x if x == libc::F_RDLCK as u32 => FileLockType::Read,
            x if x == libc::F_WRLCK as u32 => FileLockType::Write,
            x if x == libc::F_UNLCK as u32 => FileLockType::UnLock,
            _ => return Err(libc::EINVAL.into()),
        };

        let range = FileLockRange {
            start,
            end: fuse_lock_end_to_exclusive(end),
        };

        // Forward block parameter to MetaStore; backend may choose to block or return conflicts
        match self
            .set_plock_ino(inode as i64, lock_owner as i64, block, fl_type, range, pid)
            .await
        {
            Ok(()) => {
                self.remember_posix_lock_owner(inode as i64, lock_owner as i64, fl_type);
                self.remember_fuse_lock_owner(inode as i64, fh, lock_owner as i64, fl_type);
                Ok(())
            }
            Err(e) => Err(Errno::from(e)),
        }
    }

    // Forget (kernel reference drop); no inode ref tracking yet, but use it to
    // release short-lived attrs kept for post-unlink kernel timestamp updates.
    async fn forget(&self, _req: Request, inode: u64, _nlookup: u64) {
        self.forget_recently_unlinked_attr(inode as i64);
    }

    // Batch forget; same cleanup as single forget.
    async fn batch_forget(&self, _req: Request, inodes: &[(u64, u64)]) {
        for (inode, _) in inodes {
            self.forget_recently_unlinked_attr(*inode as i64);
        }
    }

    // Interrupt an in-flight request (no tracking), so no-op
    async fn interrupt(&self, _req: Request, _unique: u64) -> FuseResult<()> {
        Ok(())
    }

    // Check file access permissions
    async fn access(&self, req: Request, ino: u64, mask: u32) -> FuseResult<()> {
        debug!(
            unique = req.unique,
            ino,
            mask,
            uid = req.uid,
            gid = req.gid,
            "fuse.access"
        );
        self.ensure_access_allowed(ino as i64, req.uid, req.gid, mask)
            .await
    }
}

// =============== helpers ===============
impl<S, M> VFS<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    async fn ensure_access_allowed(
        &self,
        ino: i64,
        uid: u32,
        gid: u32,
        mask: u32,
    ) -> FuseResult<()> {
        let Some(attr) = self.stat_ino(ino).await else {
            return Err(libc::ENOENT.into());
        };

        if mask == 0 {
            return Ok(());
        }

        // Root can access everything (except execute on non-executable files)
        if uid == 0 {
            // Root still needs execute permission to be set somewhere
            if (mask & libc::X_OK as u32) != 0 && (attr.mode & 0o111) == 0 {
                return Err(libc::EACCES.into());
            }
            return Ok(());
        }

        let mode = self.access_mode_for_attr(ino, &attr, uid, gid).await;

        // Check if the requested access is allowed
        // mask uses libc constants: F_OK=0, X_OK=1, W_OK=2, R_OK=4
        if (mask & libc::R_OK as u32) != 0 && (mode & 0o4) == 0 {
            return Err(libc::EACCES.into());
        }
        if (mask & libc::W_OK as u32) != 0 && (mode & 0o2) == 0 {
            return Err(libc::EACCES.into());
        }
        if (mask & libc::X_OK as u32) != 0 && (mode & 0o1) == 0 {
            return Err(libc::EACCES.into());
        }

        Ok(())
    }

    async fn ensure_inode_paths_search_allowed(
        &self,
        ino: i64,
        uid: u32,
        gid: u32,
    ) -> FuseResult<()> {
        if uid == 0 {
            return Ok(());
        }

        if ino == self.root_ino() {
            return Ok(());
        }

        let paths = self.paths_of(ino).await.map_err(Errno::from)?;
        if paths.is_empty() {
            return Err(libc::ENOENT.into());
        }

        for path in paths {
            if self.path_ancestors_search_allowed(&path, uid, gid).await? {
                return Ok(());
            }
        }

        Err(libc::EACCES.into())
    }

    async fn path_ancestors_search_allowed(
        &self,
        path: &str,
        uid: u32,
        gid: u32,
    ) -> FuseResult<bool> {
        let components: Vec<&str> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|component| !component.is_empty())
            .collect();
        if components.is_empty() {
            return Ok(true);
        }

        let mut dir = self.root_ino();
        if !self.directory_search_allowed(dir, uid, gid).await? {
            return Ok(false);
        }

        for component in components.iter().take(components.len().saturating_sub(1)) {
            let Some(next) = self.child_of(dir, component).await else {
                return Err(libc::ENOENT.into());
            };
            dir = next;
            if !self.directory_search_allowed(dir, uid, gid).await? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn directory_search_allowed(&self, ino: i64, uid: u32, gid: u32) -> FuseResult<bool> {
        match self
            .ensure_access_allowed(ino, uid, gid, libc::X_OK as u32)
            .await
        {
            Ok(()) => Ok(true),
            Err(err) if err == Errno::from(libc::EACCES) => Ok(false),
            Err(err) => Err(err),
        }
    }

    async fn ensure_mode_setattr_allowed(
        &self,
        ino: i64,
        uid: u32,
        requested_mode: u32,
    ) -> FuseResult<()> {
        let Some(attr) = self.stat_ino(ino).await else {
            return Err(libc::ENOENT.into());
        };

        if uid == 0
            || uid == attr.uid
            || mode_setattr_only_clears_suid_sgid(attr.mode, requested_mode)
        {
            Ok(())
        } else {
            Err(libc::EPERM.into())
        }
    }

    async fn effective_mode_setattr(
        &self,
        ino: i64,
        uid: u32,
        gid: u32,
        pid: u32,
        requested_mode: u32,
    ) -> FuseResult<u32> {
        let Some(attr) = self.stat_ino(ino).await else {
            return Err(libc::ENOENT.into());
        };

        if uid != 0
            && matches!(attr.kind, VfsFileType::File)
            && (requested_mode & 0o2000) != 0
            && !request_group_ids(pid, gid).contains(&attr.gid)
        {
            Ok(requested_mode & !0o2000)
        } else {
            Ok(requested_mode)
        }
    }

    async fn ensure_timestamp_setattr_allowed(
        &self,
        ino: i64,
        uid: u32,
        gid: u32,
        req: &SetAttrRequest,
    ) -> FuseResult<()> {
        let Some(attr) = self.stat_ino(ino).await else {
            return Err(libc::ENOENT.into());
        };

        if uid == 0 || uid == attr.uid {
            return Ok(());
        }

        if timestamp_request_is_ctime_only(req) {
            return Ok(());
        }

        let mode = self.access_mode_for_attr(ino, &attr, uid, gid).await;
        if timestamp_request_uses_current_time(req) {
            if (mode & 0o2) != 0 {
                Ok(())
            } else {
                Err(libc::EACCES.into())
            }
        } else {
            Err(libc::EPERM.into())
        }
    }

    async fn ensure_chown_setattr_allowed(
        &self,
        ino: i64,
        uid: u32,
        gid: u32,
        pid: u32,
        req: &SetAttrRequest,
    ) -> FuseResult<bool> {
        let Some(attr) = self.stat_ino(ino).await else {
            return Err(libc::ENOENT.into());
        };

        if req.uid.is_none() && req.gid.is_none() {
            return Ok(false);
        }

        if uid == 0 {
            return Ok(false);
        }

        if uid != attr.uid {
            return Err(libc::EPERM.into());
        }

        if let Some(new_uid) = req.uid
            && new_uid != attr.uid
        {
            return Err(libc::EPERM.into());
        }

        if let Some(new_gid) = req.gid
            && new_gid != attr.gid
            && !request_group_ids(pid, gid).contains(&new_gid)
        {
            return Err(libc::EPERM.into());
        }

        if let Some(requested_mode) = req.mode {
            let current_mode = attr.mode & 0o7777;
            let requested_mode = requested_mode & 0o7777;
            if current_mode != requested_mode
                && !mode_setattr_only_clears_suid_sgid(current_mode, requested_mode)
            {
                return Err(libc::EPERM.into());
            }
        }

        Ok(!matches!(attr.kind, VfsFileType::Dir))
    }

    async fn access_mode_for_attr(&self, ino: i64, attr: &VfsFileAttr, uid: u32, gid: u32) -> u32 {
        match self.acl_access_mode_for_inode(ino, attr, uid, gid).await {
            Some(mode) => mode,
            None => access_mode_from_bits(attr, uid, gid),
        }
    }

    async fn ensure_directory_parent_namespace_mutation_allowed(
        &self,
        parent: u64,
        uid: u32,
        gid: u32,
    ) -> FuseResult<()> {
        let Some(attr) = self.stat_ino(parent as i64).await else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(attr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }
        self.ensure_access_allowed(
            parent as i64,
            uid,
            gid,
            parent_namespace_mutation_access_mask(),
        )
        .await
    }

    async fn ensure_sticky_parent_allows_child_mutation(
        &self,
        parent: u64,
        child: i64,
        uid: u32,
    ) -> FuseResult<()> {
        if uid == 0 {
            return Ok(());
        }

        let Some(parent_attr) = self.stat_ino(parent as i64).await else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(parent_attr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }
        if (parent_attr.mode & libc::S_ISVTX as u32) == 0 {
            return Ok(());
        }

        let Some(child_attr) = self.stat_ino(child).await else {
            return Err(libc::ENOENT.into());
        };
        if uid == parent_attr.uid || uid == child_attr.uid {
            Ok(())
        } else {
            Err(libc::EPERM.into())
        }
    }

    async fn acl_access_mode_for_inode(
        &self,
        ino: i64,
        attr: &VfsFileAttr,
        uid: u32,
        gid: u32,
    ) -> Option<u32> {
        let raw = match self.get_xattr_ino(ino, CONTROL_ACL_XATTR_NAME).await {
            Ok(Some(raw)) => raw,
            Ok(None) => return None,
            Err(err) => {
                warn!(ino, error = %err, "failed to load ACL xattr for access check");
                return None;
            }
        };
        match serde_json::from_slice::<Vec<ControlAclEntry>>(&raw) {
            Ok(entries) => acl_entries_access_mode(&entries, attr, uid, gid),
            Err(err) => {
                warn!(ino, error = %err, "invalid ACL xattr for access check");
                None
            }
        }
    }
}

fn open_flags_access_mask(flags: u32) -> u32 {
    let accmode = flags & (libc::O_ACCMODE as u32);
    let mut mask = if accmode == libc::O_RDONLY as u32 && (flags & FUSE_OPEN_EXEC) != 0 {
        libc::X_OK as u32
    } else {
        match accmode {
            value if value == libc::O_WRONLY as u32 => libc::W_OK as u32,
            value if value == libc::O_RDWR as u32 => (libc::R_OK | libc::W_OK) as u32,
            _ => libc::R_OK as u32,
        }
    };
    if (flags & libc::O_TRUNC as u32) != 0 {
        mask |= libc::W_OK as u32;
    }
    mask
}

fn opendir_access_mask() -> u32 {
    libc::R_OK as u32
}

fn inode_mutation_access_mask() -> u32 {
    libc::W_OK as u32
}

fn namespace_mutation_access_mask() -> u32 {
    (libc::W_OK | libc::X_OK) as u32
}

fn parent_namespace_mutation_access_mask() -> u32 {
    namespace_mutation_access_mask()
}

fn access_mode_from_bits(attr: &VfsFileAttr, uid: u32, gid: u32) -> u32 {
    if uid == attr.uid {
        (attr.mode >> 6) & 0o7
    } else if gid == attr.gid {
        (attr.mode >> 3) & 0o7
    } else {
        attr.mode & 0o7
    }
}

fn acl_entries_access_mode(
    entries: &[ControlAclEntry],
    attr: &VfsFileAttr,
    uid: u32,
    gid: u32,
) -> Option<u32> {
    if uid == attr.uid {
        return find_acl_perm(entries, "access", "user_obj", None);
    }

    if let Some(mode) = find_acl_perm(entries, "access", "user", Some(uid)) {
        return Some(apply_acl_mask(entries, mode));
    }

    let mut group_mode = None;
    if gid == attr.gid
        && let Some(mode) = find_acl_perm(entries, "access", "group_obj", None)
    {
        group_mode = Some(mode);
    }
    if let Some(mode) = find_acl_perm(entries, "access", "group", Some(gid)) {
        group_mode = Some(group_mode.unwrap_or(0) | mode);
    }
    if let Some(mode) = group_mode {
        return Some(apply_acl_mask(entries, mode));
    }

    find_acl_perm(entries, "access", "other", None)
}

fn apply_acl_mask(entries: &[ControlAclEntry], mode: u32) -> u32 {
    match find_acl_perm(entries, "access", "mask", None) {
        Some(mask) => mode & mask,
        None => mode,
    }
}

fn find_acl_perm(
    entries: &[ControlAclEntry],
    scope: &str,
    tag: &str,
    id: Option<u32>,
) -> Option<u32> {
    entries
        .iter()
        .find(|entry| entry.scope == scope && entry.tag == tag && entry.id == id)
        .and_then(|entry| acl_perm_bits(&entry.perm))
}

fn acl_perm_bits(perm: &str) -> Option<u32> {
    if perm.len() != 3 {
        return None;
    }
    let mut bits = 0;
    for (index, ch) in perm.chars().enumerate() {
        match (index, ch) {
            (0, 'r') => bits |= 0o4,
            (1, 'w') => bits |= 0o2,
            (2, 'x') => bits |= 0o1,
            (_, '-') => {}
            _ => return None,
        }
    }
    Some(bits)
}

impl From<MetaError> for Errno {
    fn from(val: MetaError) -> Self {
        let code = match val {
            MetaError::NotFound(_) => libc::ENOENT,
            MetaError::ParentNotFound(_) => libc::ENOENT,
            MetaError::NotDirectory(_) => libc::ENOTDIR,
            MetaError::DirectoryNotEmpty(_) => libc::ENOTEMPTY,
            MetaError::AlreadyExists { .. } => libc::EEXIST,
            MetaError::LockConflict { .. } => libc::EAGAIN,
            MetaError::NotSupported(_) | MetaError::NotImplemented => libc::ENOSYS,
            MetaError::InvalidPath(_) => libc::EINVAL,
            MetaError::InvalidFilename => libc::EINVAL,
            MetaError::FilenameTooLong => libc::ENAMETOOLONG,
            _ => libc::EIO,
        };
        Errno::from(code)
    }
}

impl From<VfsError> for Errno {
    fn from(val: VfsError) -> Self {
        let code = match val {
            VfsError::NotFound { .. } => libc::ENOENT,
            VfsError::AlreadyExists { .. } => libc::EEXIST,
            VfsError::NotADirectory { .. } => libc::ENOTDIR,
            VfsError::IsADirectory { .. } => libc::EISDIR,
            VfsError::DirectoryNotEmpty { .. } => libc::ENOTEMPTY,
            VfsError::PermissionDenied { .. } => libc::EACCES,
            VfsError::ReadOnlyFilesystem { .. } => libc::EROFS,
            VfsError::ConnectionRefused => libc::ECONNREFUSED,
            VfsError::ConnectionReset => libc::ECONNRESET,
            VfsError::HostUnreachable => libc::EHOSTUNREACH,
            VfsError::NetworkUnreachable => libc::ENETUNREACH,
            VfsError::ConnectionAborted => libc::ECONNABORTED,
            VfsError::NotConnected => libc::ENOTCONN,
            VfsError::AddrInUse => libc::EADDRINUSE,
            VfsError::AddrNotAvailable => libc::EADDRNOTAVAIL,
            VfsError::NetworkDown => libc::ENETDOWN,
            VfsError::BrokenPipe => libc::EPIPE,
            VfsError::WouldBlock => libc::EAGAIN,
            VfsError::InvalidInput => libc::EINVAL,
            VfsError::InvalidData => libc::EINVAL,
            VfsError::TimedOut => libc::ETIMEDOUT,
            VfsError::WriteZero => libc::EIO,
            VfsError::StorageFull => libc::ENOSPC,
            VfsError::NotSeekable => libc::ESPIPE,
            VfsError::QuotaExceeded => libc::EDQUOT,
            VfsError::FileTooLarge => libc::EFBIG,
            VfsError::ResourceBusy => libc::EBUSY,
            VfsError::ExecutableFileBusy => libc::ETXTBSY,
            VfsError::Deadlock => libc::EDEADLK,
            VfsError::CrossesDevices => libc::EXDEV,
            VfsError::TooManyLinks => libc::EMLINK,
            VfsError::InvalidFilename => libc::EINVAL,
            VfsError::FilenameTooLong { .. } => libc::ENAMETOOLONG,
            VfsError::ArgumentListTooLong => libc::E2BIG,
            VfsError::Interrupted => libc::EINTR,
            VfsError::Unsupported => libc::ENOSYS,
            VfsError::UnexpectedEof => libc::EIO,
            VfsError::OutOfMemory => libc::ENOMEM,
            VfsError::StaleNetworkFileHandle => libc::ESTALE,
            _ => libc::EIO,
        };
        code.into()
    }
}

fn vfs_kind_to_fuse(k: VfsFileType) -> FuseFileType {
    match k {
        VfsFileType::Dir => FuseFileType::Directory,
        VfsFileType::File => FuseFileType::RegularFile,
        VfsFileType::Symlink => FuseFileType::Symlink,
        VfsFileType::Fifo => FuseFileType::NamedPipe,
        VfsFileType::Socket => FuseFileType::Socket,
        VfsFileType::CharDevice => FuseFileType::CharDevice,
        VfsFileType::BlockDevice => FuseFileType::BlockDevice,
    }
}

fn vfs_to_fuse_attr(
    v: &VfsFileAttr,
    _req: &Request,
    blocks: u64,
) -> asyncfuse::raw::reply::FileAttr {
    let perm = (v.mode & 0o7777) as u16;
    let atime = nanos_to_timestamp(v.atime);
    let mtime = nanos_to_timestamp(v.mtime);
    let ctime = nanos_to_timestamp(v.ctime);
    asyncfuse::raw::reply::FileAttr {
        ino: v.ino as u64,
        size: v.size,
        blocks,
        atime,
        mtime,
        ctime,
        #[cfg(target_os = "macos")]
        crtime: ctime,
        kind: vfs_kind_to_fuse(v.kind),
        perm,
        nlink: v.nlink,
        uid: v.uid,
        gid: v.gid,
        rdev: v.rdev,
        #[cfg(target_os = "macos")]
        flags: 0,
        blksize: 4096,
    }
}

const NANOS_PER_SEC: i64 = 1_000_000_000;

fn nanos_to_timestamp(value: i64) -> Timestamp {
    let sec = value.div_euclid(NANOS_PER_SEC);
    let nsec = value.rem_euclid(NANOS_PER_SEC) as u32;
    Timestamp::new(sec, nsec)
}

fn timestamp_to_nanos(ts: Timestamp) -> i64 {
    ts.sec
        .saturating_mul(NANOS_PER_SEC)
        .saturating_add(ts.nsec as i64)
}

fn sanitize_special_mode_bits(mode: u32) -> u32 {
    mode & 0o7777
}

fn apply_creation_umask(mode: u32, umask: u32) -> u32 {
    sanitize_special_mode_bits(mode) & !(umask & 0o777)
}

fn fuse_setattr_to_meta(set_attr: &SetAttr) -> (SetAttrRequest, SetAttrFlags) {
    let mut req = SetAttrRequest::default();
    let flags = SetAttrFlags::empty();
    if let Some(mode) = set_attr.mode {
        req.mode = Some(sanitize_special_mode_bits(mode.into()));
    }
    if let Some(uid) = set_attr.uid
        && uid != u32::MAX
    {
        req.uid = Some(uid);
    }
    if let Some(gid) = set_attr.gid
        && gid != u32::MAX
    {
        req.gid = Some(gid);
    }
    if let Some(size) = set_attr.size {
        req.size = Some(size);
    }
    if let Some(atime) = set_attr.atime {
        req.atime = Some(timestamp_to_nanos(atime));
    }
    if let Some(mtime) = set_attr.mtime {
        req.mtime = Some(timestamp_to_nanos(mtime));
    }
    if let Some(ctime) = set_attr.ctime {
        req.ctime = Some(timestamp_to_nanos(ctime));
    }
    (req, flags)
}

fn attr_request_is_empty(req: &SetAttrRequest) -> bool {
    req.mode.is_none()
        && req.uid.is_none()
        && req.gid.is_none()
        && req.size.is_none()
        && req.atime.is_none()
        && req.mtime.is_none()
        && req.ctime.is_none()
        && req.flags.is_none()
}

fn setattr_is_truncate_with_optional_timestamps(
    req: &SetAttrRequest,
    flags: &SetAttrFlags,
) -> bool {
    req.size.is_some()
        && req.mode.is_none()
        && req.uid.is_none()
        && req.gid.is_none()
        && req.flags.is_none()
        && flags.is_empty()
}

fn setattr_is_mode_with_optional_timestamps(req: &SetAttrRequest, flags: &SetAttrFlags) -> bool {
    req.mode.is_some()
        && req.uid.is_none()
        && req.gid.is_none()
        && req.size.is_none()
        && req.flags.is_none()
        && flags.is_empty()
}

fn setattr_is_chown_with_optional_timestamps(req: &SetAttrRequest, flags: &SetAttrFlags) -> bool {
    (req.uid.is_some() || req.gid.is_some())
        && req.size.is_none()
        && req.flags.is_none()
        && flags.is_empty()
}

fn mode_setattr_only_clears_suid_sgid(current_mode: u32, requested_mode: u32) -> bool {
    let current = current_mode & 0o7777;
    let requested = requested_mode & 0o7777;
    let changed = current ^ requested;
    let cleared_suid_sgid = (current & 0o6000) & !(requested & 0o6000);
    let added_suid_sgid = (requested & 0o6000) & !(current & 0o6000);

    (changed & !0o6000) == 0 && added_suid_sgid == 0 && cleared_suid_sgid != 0
}

fn setattr_is_timestamp_only(req: &SetAttrRequest, flags: &SetAttrFlags) -> bool {
    req.size.is_none()
        && req.mode.is_none()
        && req.uid.is_none()
        && req.gid.is_none()
        && req.flags.is_none()
        && (req.atime.is_some() || req.mtime.is_some() || req.ctime.is_some())
        && flags.is_empty()
}

fn timestamp_request_uses_current_time(req: &SetAttrRequest) -> bool {
    let Some(now) = current_time_nanos() else {
        return false;
    };
    let mut saw_user_timestamp = false;

    for timestamp in [req.atime, req.mtime].into_iter().flatten() {
        saw_user_timestamp = true;
        if !timestamp_is_near_now(timestamp, now) {
            return false;
        }
    }

    saw_user_timestamp
}

fn timestamp_request_is_ctime_only(req: &SetAttrRequest) -> bool {
    req.ctime.is_some() && req.atime.is_none() && req.mtime.is_none()
}

fn timestamp_is_near_now(timestamp: i64, now: i64) -> bool {
    const TIMESTAMP_NOW_TOLERANCE_NANOS: i64 = 10 * NANOS_PER_SEC;
    timestamp >= now.saturating_sub(TIMESTAMP_NOW_TOLERANCE_NANOS)
        && timestamp <= now.saturating_add(TIMESTAMP_NOW_TOLERANCE_NANOS)
}

fn request_group_ids(pid: u32, fallback_gid: u32) -> Vec<u32> {
    let mut groups = platform_process_group_ids(pid).unwrap_or_default();
    if !groups.contains(&fallback_gid) {
        groups.push(fallback_gid);
    }
    if groups.is_empty() {
        groups.push(fallback_gid);
    }
    groups
}

#[cfg(target_os = "linux")]
fn platform_process_group_ids(pid: u32) -> Option<Vec<u32>> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    parse_proc_status_groups(&status)
}

#[cfg(not(target_os = "linux"))]
fn platform_process_group_ids(_pid: u32) -> Option<Vec<u32>> {
    None
}

fn parse_proc_status_groups(status: &str) -> Option<Vec<u32>> {
    let groups = status
        .lines()
        .find_map(|line| line.strip_prefix("Groups:"))?;
    let parsed = groups
        .split_whitespace()
        .filter_map(|group| group.parse::<u32>().ok())
        .collect::<Vec<_>>();
    Some(parsed)
}

fn current_time_nanos() -> Option<i64> {
    Some(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_nanos()
            .try_into()
            .unwrap_or(i64::MAX),
    )
}

fn validate_fuse_name(name: &str) -> Result<(), Errno> {
    if name.is_empty() {
        return Err(libc::EINVAL.into());
    }
    if name.len() > NAME_MAX {
        return Err(libc::ENAMETOOLONG.into());
    }
    if name.contains('/') || name.contains('\0') {
        return Err(libc::EINVAL.into());
    }
    Ok(())
}

#[cfg(test)]
mod mode_sanitization_tests {
    use super::{
        FUSE_OPEN_EXEC, access_mode_from_bits, acl_entries_access_mode, apply_creation_umask,
        mode_setattr_only_clears_suid_sgid, namespace_mutation_access_mask, open_flags_access_mask,
        opendir_access_mask, parent_namespace_mutation_access_mask, parse_proc_status_groups,
        sanitize_special_mode_bits, validate_fuse_name, vfs_kind_to_fuse, vfs_to_fuse_attr,
    };
    use crate::control::protocol::ControlAclEntry;
    use crate::vfs::fs::{FileAttr as VfsFileAttr, FileType as VfsFileType};
    use asyncfuse::raw::Request;
    use asyncfuse::{Errno, FileType as FuseFileType};
    use std::collections::BTreeSet;

    #[test]
    fn sanitize_special_mode_bits_preserves_setuid_setgid_and_sticky() {
        assert_eq!(sanitize_special_mode_bits(0o1777), 0o1777);
        assert_eq!(sanitize_special_mode_bits(0o2755), 0o2755);
        assert_eq!(sanitize_special_mode_bits(0o4755), 0o4755);
    }

    #[test]
    fn apply_creation_umask_preserves_special_bits_and_masks_permissions() {
        assert_eq!(apply_creation_umask(0o1777, 0), 0o1777);
        assert_eq!(apply_creation_umask(0o1777, 0o022), 0o1755);
        assert_eq!(apply_creation_umask(0o4755, 0o022), 0o4755);
    }

    #[test]
    fn mode_setattr_clear_suid_sgid_exception_is_narrow() {
        assert!(mode_setattr_only_clears_suid_sgid(0o6777, 0o0777));
        assert!(mode_setattr_only_clears_suid_sgid(0o4777, 0o0777));
        assert!(mode_setattr_only_clears_suid_sgid(0o6777, 0o2777));

        assert!(!mode_setattr_only_clears_suid_sgid(0o0777, 0o4777));
        assert!(!mode_setattr_only_clears_suid_sgid(0o4777, 0o4777));
        assert!(!mode_setattr_only_clears_suid_sgid(0o1777, 0o0777));
        assert!(!mode_setattr_only_clears_suid_sgid(0o4777, 0o0755));
    }

    #[test]
    fn validate_fuse_name_returns_enametoolong_for_long_component() {
        let long_name = "x".repeat(crate::posix::NAME_MAX + 1);

        assert_eq!(
            validate_fuse_name(&long_name).unwrap_err(),
            Errno::from(libc::ENAMETOOLONG)
        );
    }

    #[test]
    fn handle_readdir_offsets_do_not_skip_children_between_pages() {
        const TOTAL_CHILDREN: usize = 4096;
        const PAGE_SIZE: usize = 50;

        let mut seen = BTreeSet::new();
        let mut offset = 0u64;

        loop {
            let batch_start = offset.saturating_sub(2);
            if batch_start as usize >= TOTAL_CHILDREN {
                break;
            }

            let batch_len = PAGE_SIZE.min(TOTAL_CHILDREN - batch_start as usize);
            for i in 0..batch_len {
                let child_index = batch_start as usize + i;
                seen.insert(child_index);
                offset = batch_start + i as u64 + 3;
            }
        }

        assert_eq!(seen.len(), TOTAL_CHILDREN);
        assert_eq!(seen.first().copied(), Some(0));
        assert_eq!(seen.last().copied(), Some(TOTAL_CHILDREN - 1));
    }

    #[test]
    fn acl_access_mode_prefers_named_user_entry_over_mode_bits() {
        let attr = test_attr(0o644, 1000, 1000);
        let entries = vec![
            acl_entry("access", "user_obj", None, "rw-"),
            acl_entry("access", "user", Some(2000), "r--"),
            acl_entry("access", "group_obj", None, "---"),
            acl_entry("access", "mask", None, "r--"),
            acl_entry("access", "other", None, "---"),
        ];

        assert_eq!(
            acl_entries_access_mode(&entries, &attr, 2000, 3000),
            Some(0o4)
        );
        assert_eq!(
            acl_entries_access_mode(&entries, &attr, 3000, 3000),
            Some(0o0)
        );
    }

    #[test]
    fn acl_access_mode_applies_mask_to_group_class() {
        let attr = test_attr(0o664, 1000, 2000);
        let entries = vec![
            acl_entry("access", "user_obj", None, "rw-"),
            acl_entry("access", "group_obj", None, "rw-"),
            acl_entry("access", "group", Some(3000), "rwx"),
            acl_entry("access", "mask", None, "r--"),
            acl_entry("access", "other", None, "---"),
        ];

        assert_eq!(
            acl_entries_access_mode(&entries, &attr, 4000, 3000),
            Some(0o4)
        );
        assert_eq!(
            acl_entries_access_mode(&entries, &attr, 4000, 2000),
            Some(0o4)
        );
    }

    #[test]
    fn access_mode_from_bits_remains_mode_fallback_without_acl() {
        let attr = test_attr(0o640, 1000, 2000);

        assert_eq!(access_mode_from_bits(&attr, 1000, 3000), 0o6);
        assert_eq!(access_mode_from_bits(&attr, 3000, 2000), 0o4);
        assert_eq!(access_mode_from_bits(&attr, 3000, 4000), 0o0);
    }

    #[test]
    fn open_flags_map_to_access_masks() {
        assert_eq!(
            open_flags_access_mask(libc::O_RDONLY as u32),
            libc::R_OK as u32
        );
        assert_eq!(
            open_flags_access_mask(libc::O_WRONLY as u32),
            libc::W_OK as u32
        );
        assert_eq!(
            open_flags_access_mask(libc::O_RDWR as u32),
            (libc::R_OK | libc::W_OK) as u32
        );
        assert_eq!(
            open_flags_access_mask((libc::O_RDONLY | libc::O_TRUNC) as u32),
            (libc::R_OK | libc::W_OK) as u32
        );
        assert_eq!(
            open_flags_access_mask((libc::O_RDONLY as u32) | FUSE_OPEN_EXEC),
            libc::X_OK as u32
        );
    }

    #[test]
    fn parse_proc_status_groups_reads_supplementary_groups() {
        let status = "Name:\tfstest\nGroups:\t65533 65534 1000\n";

        assert_eq!(
            parse_proc_status_groups(status),
            Some(vec![65533, 65534, 1000])
        );
    }

    #[test]
    fn opendir_requires_read_access() {
        assert_eq!(opendir_access_mask(), libc::R_OK as u32);
    }

    #[test]
    fn namespace_mutations_require_write_and_execute_on_parent() {
        assert_eq!(
            namespace_mutation_access_mask(),
            (libc::W_OK | libc::X_OK) as u32
        );
    }

    #[test]
    fn parent_namespace_mutations_use_namespace_mutation_mask() {
        assert_eq!(
            parent_namespace_mutation_access_mask(),
            namespace_mutation_access_mask()
        );
    }

    #[test]
    fn special_file_types_map_to_fuse_types_and_rdev() {
        assert_eq!(vfs_kind_to_fuse(VfsFileType::Fifo), FuseFileType::NamedPipe);
        assert_eq!(
            vfs_kind_to_fuse(VfsFileType::CharDevice),
            FuseFileType::CharDevice
        );
        assert_eq!(
            vfs_kind_to_fuse(VfsFileType::BlockDevice),
            FuseFileType::BlockDevice
        );
        assert_eq!(vfs_kind_to_fuse(VfsFileType::Socket), FuseFileType::Socket);

        let attr = VfsFileAttr {
            ino: 2,
            size: 0,
            blocks: 0,
            kind: VfsFileType::CharDevice,
            mode: 0o20666,
            uid: 1000,
            gid: 1000,
            atime: 0,
            mtime: 0,
            ctime: 0,
            nlink: 1,
            rdev: 0x0102,
        };

        let fuse_attr = vfs_to_fuse_attr(&attr, &Request::default(), attr.blocks);
        assert_eq!(fuse_attr.kind, FuseFileType::CharDevice);
        assert_eq!(fuse_attr.rdev, 0x0102);
    }

    fn acl_entry(scope: &str, tag: &str, id: Option<u32>, perm: &str) -> ControlAclEntry {
        ControlAclEntry {
            scope: scope.to_string(),
            tag: tag.to_string(),
            id,
            perm: perm.to_string(),
        }
    }

    fn test_attr(mode: u32, uid: u32, gid: u32) -> VfsFileAttr {
        VfsFileAttr {
            ino: 2,
            size: 0,
            blocks: 0,
            kind: VfsFileType::File,
            mode,
            uid,
            gid,
            rdev: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            nlink: 1,
        }
    }
}

#[cfg(test)]
mod fuse_init_tests {
    use super::*;
    use crate::chunk::layout::ChunkLayout;
    use crate::chunk::store::InMemoryBlockStore;
    use crate::meta::MetaLayer;
    use crate::meta::factory::create_meta_store_from_url;
    use asyncfuse::raw::Filesystem;
    use asyncfuse::raw::flags::FOPEN_DIRECT_IO;
    use std::ffi::OsStr;
    use std::sync::{Mutex as StdMutex, OnceLock};

    fn env_lock() -> &'static StdMutex<()> {
        static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| StdMutex::new(()))
    }

    async fn new_fuse_test_vfs() -> VFS<InMemoryBlockStore, impl MetaLayer> {
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        VFS::new(layout, store, meta_handle.store()).await.unwrap()
    }

    fn user_request() -> Request {
        request_with_ids(1000, 1000)
    }

    fn request_with_ids(uid: u32, gid: u32) -> Request {
        Request {
            unique: 1,
            uid,
            gid,
            pid: 42,
        }
    }

    #[tokio::test]
    async fn init_reply_advertises_large_write_requests() {
        let fs = new_fuse_test_vfs().await;

        let reply = Filesystem::init(&fs, Request::default()).await.unwrap();

        assert_eq!(reply.max_write.get(), 4 * 1024 * 1024);
    }

    #[tokio::test]
    async fn rename_requires_namespace_access_on_source_parent() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/src").await.unwrap();
        fs.mkdir_p("/dst").await.unwrap();
        fs.create_file("/src/file.txt").await.unwrap();

        let src = fs.stat("/src").await.unwrap();
        let dst = fs.stat("/dst").await.unwrap();
        fs.chmod(dst.ino, 0o777).await.unwrap();

        let err = Filesystem::rename(
            &fs,
            user_request(),
            src.ino as u64,
            OsStr::new("file.txt"),
            dst.ino as u64,
            OsStr::new("moved.txt"),
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::EACCES));
        assert!(fs.stat("/src/file.txt").await.is_ok());
        assert!(fs.stat("/dst/moved.txt").await.is_err());
    }

    #[tokio::test]
    async fn rename_requires_namespace_access_on_destination_parent() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/src").await.unwrap();
        fs.mkdir_p("/dst").await.unwrap();
        fs.create_file("/src/file.txt").await.unwrap();

        let src = fs.stat("/src").await.unwrap();
        let dst = fs.stat("/dst").await.unwrap();
        fs.chmod(src.ino, 0o777).await.unwrap();

        let err = Filesystem::rename(
            &fs,
            user_request(),
            src.ino as u64,
            OsStr::new("file.txt"),
            dst.ino as u64,
            OsStr::new("moved.txt"),
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::EACCES));
        assert!(fs.stat("/src/file.txt").await.is_ok());
        assert!(fs.stat("/dst/moved.txt").await.is_err());
    }

    #[tokio::test]
    async fn lookup_rejects_child_when_parent_lacks_search_access() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/dir").await.unwrap();
        fs.create_file("/dir/file.txt").await.unwrap();
        let dir = fs.stat("/dir").await.unwrap();
        fs.chown(dir.ino, Some(1000), Some(1000)).await.unwrap();
        fs.chmod(dir.ino, 0o644).await.unwrap();

        let err = Filesystem::lookup(&fs, user_request(), dir.ino as u64, OsStr::new("file.txt"))
            .await
            .unwrap_err();

        assert_eq!(err, Errno::from(libc::EACCES));
    }

    #[tokio::test]
    async fn open_rejects_cached_inode_when_parent_lacks_search_access() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/dir").await.unwrap();
        fs.create_file("/dir/file.txt").await.unwrap();
        let dir = fs.stat("/dir").await.unwrap();
        let file = fs.stat("/dir/file.txt").await.unwrap();
        fs.chown(dir.ino, Some(1000), Some(1000)).await.unwrap();
        fs.chown(file.ino, Some(1000), Some(1000)).await.unwrap();
        fs.chmod(dir.ino, 0o644).await.unwrap();

        let err = Filesystem::open(&fs, user_request(), file.ino as u64, libc::O_RDONLY as u32)
            .await
            .unwrap_err();

        assert_eq!(err, Errno::from(libc::EACCES));
    }

    #[tokio::test]
    async fn open_allows_root_for_cached_inode_when_parent_lacks_search_access() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/dir").await.unwrap();
        fs.create_file("/dir/file.txt").await.unwrap();
        let dir = fs.stat("/dir").await.unwrap();
        let file = fs.stat("/dir/file.txt").await.unwrap();
        fs.chmod(dir.ino, 0o000).await.unwrap();

        let reply = Filesystem::open(
            &fs,
            request_with_ids(0, 0),
            file.ino as u64,
            libc::O_RDONLY as u32,
        )
        .await
        .unwrap();

        fs.close(reply.fh).await.unwrap();
    }

    #[tokio::test]
    async fn setattr_rejects_cached_inode_when_parent_lacks_search_access() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/dir").await.unwrap();
        fs.create_file("/dir/file.txt").await.unwrap();
        let dir = fs.stat("/dir").await.unwrap();
        let file = fs.stat("/dir/file.txt").await.unwrap();
        fs.chown(dir.ino, Some(1000), Some(1000)).await.unwrap();
        fs.chown(file.ino, Some(1000), Some(1000)).await.unwrap();
        fs.chmod(dir.ino, 0o644).await.unwrap();

        let err = Filesystem::setattr(
            &fs,
            user_request(),
            file.ino as u64,
            None,
            SetAttr {
                mode: Some(0o620),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::EACCES));
    }

    #[tokio::test]
    async fn link_rejects_cached_source_inode_when_parent_lacks_search_access() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/src").await.unwrap();
        fs.mkdir_p("/dst").await.unwrap();
        fs.create_file("/src/file.txt").await.unwrap();
        let src = fs.stat("/src").await.unwrap();
        let dst = fs.stat("/dst").await.unwrap();
        let file = fs.stat("/src/file.txt").await.unwrap();
        fs.chown(src.ino, Some(1000), Some(1000)).await.unwrap();
        fs.chown(file.ino, Some(1000), Some(1000)).await.unwrap();
        fs.chmod(src.ino, 0o644).await.unwrap();
        fs.chmod(dst.ino, 0o777).await.unwrap();

        let err = Filesystem::link(
            &fs,
            user_request(),
            file.ino as u64,
            dst.ino as u64,
            OsStr::new("linked.txt"),
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::EACCES));
        assert!(fs.stat("/dst/linked.txt").await.is_err());
    }

    #[tokio::test]
    async fn rename_rejects_non_owner_from_sticky_source_parent() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/src").await.unwrap();
        fs.mkdir_p("/dst").await.unwrap();
        let src = fs.stat("/src").await.unwrap();
        let dst = fs.stat("/dst").await.unwrap();
        fs.chmod(src.ino, 0o1777).await.unwrap();
        fs.chmod(dst.ino, 0o777).await.unwrap();
        fs.create_file("/src/file.txt").await.unwrap();

        let err = Filesystem::rename(
            &fs,
            user_request(),
            src.ino as u64,
            OsStr::new("file.txt"),
            dst.ino as u64,
            OsStr::new("moved.txt"),
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::EPERM));
        assert!(fs.stat("/src/file.txt").await.is_ok());
        assert!(fs.stat("/dst/moved.txt").await.is_err());
    }

    #[tokio::test]
    async fn rename_rejects_non_owner_over_sticky_destination_child() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/src").await.unwrap();
        fs.mkdir_p("/dst").await.unwrap();
        let src = fs.stat("/src").await.unwrap();
        let dst = fs.stat("/dst").await.unwrap();
        fs.chmod(src.ino, 0o777).await.unwrap();
        fs.chmod(dst.ino, 0o1777).await.unwrap();
        fs.create_file("/src/file.txt").await.unwrap();
        let source = fs.stat("/src/file.txt").await.unwrap();
        fs.chown(source.ino, Some(1000), Some(1000)).await.unwrap();
        fs.create_file("/dst/target.txt").await.unwrap();

        let err = Filesystem::rename(
            &fs,
            user_request(),
            src.ino as u64,
            OsStr::new("file.txt"),
            dst.ino as u64,
            OsStr::new("target.txt"),
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::EPERM));
        assert!(fs.stat("/src/file.txt").await.is_ok());
        assert!(fs.stat("/dst/target.txt").await.is_ok());
    }

    #[tokio::test]
    async fn rename_rejects_cross_parent_directory_move_without_source_dir_mutation_access() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/src").await.unwrap();
        fs.mkdir_p("/dst").await.unwrap();
        fs.mkdir_p("/src/dir").await.unwrap();
        let src = fs.stat("/src").await.unwrap();
        let dst = fs.stat("/dst").await.unwrap();
        fs.chown(src.ino, Some(1000), Some(1000)).await.unwrap();
        fs.chmod(src.ino, 0o1777).await.unwrap();
        fs.chmod(dst.ino, 0o777).await.unwrap();

        let err = Filesystem::rename(
            &fs,
            user_request(),
            src.ino as u64,
            OsStr::new("dir"),
            dst.ino as u64,
            OsStr::new("dir"),
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::EACCES));
        assert!(fs.stat("/src/dir").await.is_ok());
        assert!(fs.stat("/dst/dir").await.is_err());

        Filesystem::rename(
            &fs,
            user_request(),
            src.ino as u64,
            OsStr::new("dir"),
            src.ino as u64,
            OsStr::new("renamed"),
        )
        .await
        .unwrap();

        assert!(fs.stat("/src/dir").await.is_err());
        assert!(fs.stat("/src/renamed").await.is_ok());
    }

    #[tokio::test]
    async fn setattr_size_requires_write_access_on_inode() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();

        let err = Filesystem::setattr(
            &fs,
            user_request(),
            attr.ino as u64,
            None,
            SetAttr {
                size: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::EACCES));
        assert_eq!(fs.stat("/file.txt").await.unwrap().size, 0);
    }

    #[tokio::test]
    async fn setattr_size_and_timestamps_allow_write_handle_for_mode_zero_create() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/scratch").await.unwrap();
        let scratch = fs.stat("/scratch").await.unwrap();
        fs.chmod(scratch.ino, 0o777).await.unwrap();

        let created = Filesystem::create(
            &fs,
            user_request(),
            scratch.ino as u64,
            OsStr::new("zero.txt"),
            0o000,
            (libc::O_CREAT | libc::O_RDWR) as u32,
        )
        .await
        .unwrap();

        let reply = Filesystem::setattr(
            &fs,
            user_request(),
            created.attr.ino,
            Some(created.fh),
            SetAttr {
                size: Some(0),
                mtime: Some(Timestamp::new(1, 2)),
                ctime: Some(Timestamp::new(1, 2)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.size, 0);
    }

    #[tokio::test]
    async fn create_existing_file_preserves_existing_mode() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/scratch").await.unwrap();
        let scratch = fs.stat("/scratch").await.unwrap();
        fs.chmod(scratch.ino, 0o777).await.unwrap();

        let first = Filesystem::create(
            &fs,
            user_request(),
            scratch.ino as u64,
            OsStr::new("existing.txt"),
            0o600,
            (libc::O_CREAT | libc::O_RDWR) as u32,
        )
        .await
        .unwrap();

        let second = Filesystem::create(
            &fs,
            user_request(),
            scratch.ino as u64,
            OsStr::new("existing.txt"),
            0o777,
            (libc::O_CREAT | libc::O_RDWR) as u32,
        )
        .await
        .unwrap();

        assert_eq!(second.attr.ino, first.attr.ino);
        let attr = fs.stat("/scratch/existing.txt").await.unwrap();
        assert_eq!(attr.mode & 0o7777, 0o600);
    }

    #[tokio::test]
    async fn mknod_creates_fifo_metadata() {
        let fs = new_fuse_test_vfs().await;
        let reply = Filesystem::mknod(
            &fs,
            Request::default(),
            1,
            OsStr::new("pipe"),
            libc::S_IFIFO | 0o644,
            0,
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.kind, FuseFileType::NamedPipe);
        assert_eq!(reply.attr.perm, 0o644);
        assert_eq!(reply.attr.rdev, 0);

        let attr = fs.stat("/pipe").await.unwrap();
        assert_eq!(attr.kind, VfsFileType::Fifo);
        assert_eq!(attr.rdev, 0);
    }

    #[tokio::test]
    async fn mknod_creates_char_device_metadata_with_rdev() {
        let fs = new_fuse_test_vfs().await;
        let reply = Filesystem::mknod(
            &fs,
            Request::default(),
            1,
            OsStr::new("tty"),
            libc::S_IFCHR | 0o600,
            0x0103,
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.kind, FuseFileType::CharDevice);
        assert_eq!(reply.attr.perm, 0o600);
        assert_eq!(reply.attr.rdev, 0x0103);

        let attr = fs.stat("/tty").await.unwrap();
        assert_eq!(attr.kind, VfsFileType::CharDevice);
        assert_eq!(attr.rdev, 0x0103);
    }

    #[tokio::test]
    async fn timestamp_setattr_allows_owner_without_write_bits() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chown(attr.ino, Some(1000), Some(1000)).await.unwrap();
        fs.chmod(attr.ino, 0o444).await.unwrap();

        let reply = Filesystem::setattr(
            &fs,
            user_request(),
            attr.ino as u64,
            None,
            SetAttr {
                atime: Some(Timestamp::new(1, 0)),
                mtime: Some(Timestamp::new(2, 0)),
                ctime: Some(Timestamp::new(3, 0)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.atime, Timestamp::new(1, 0));
        assert_eq!(reply.attr.mtime, Timestamp::new(2, 0));
    }

    #[tokio::test]
    async fn timestamp_setattr_rejects_non_owner_explicit_time_even_with_write_bits() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chmod(attr.ino, 0o666).await.unwrap();

        let err = Filesystem::setattr(
            &fs,
            user_request(),
            attr.ino as u64,
            None,
            SetAttr {
                atime: Some(Timestamp::new(1, 0)),
                mtime: Some(Timestamp::new(2, 0)),
                ctime: Some(Timestamp::new(3, 0)),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::EPERM));
    }

    #[tokio::test]
    async fn timestamp_setattr_allows_non_owner_current_time_with_write_bits() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chmod(attr.ino, 0o666).await.unwrap();
        let now = Timestamp::from(SystemTime::now());

        let reply = Filesystem::setattr(
            &fs,
            user_request(),
            attr.ino as u64,
            None,
            SetAttr {
                atime: Some(now),
                mtime: Some(now),
                ctime: Some(now),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.atime, now);
        assert_eq!(reply.attr.mtime, now);
    }

    #[tokio::test]
    async fn timestamp_setattr_allows_non_owner_ctime_only_chown_noop() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        let ctime = Timestamp::new(123, 456);

        let reply = Filesystem::setattr(
            &fs,
            user_request(),
            attr.ino as u64,
            None,
            SetAttr {
                ctime: Some(ctime),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.uid, 0);
        assert_eq!(reply.attr.gid, 0);
        assert_eq!(reply.attr.ctime, ctime);
    }

    #[tokio::test]
    async fn mode_setattr_allows_owner_without_write_bits() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chown(attr.ino, Some(1000), Some(1000)).await.unwrap();
        fs.chmod(attr.ino, 0o444).await.unwrap();

        let reply = Filesystem::setattr(
            &fs,
            user_request(),
            attr.ino as u64,
            None,
            SetAttr {
                mode: Some(0o600),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.perm, 0o600);
    }

    #[tokio::test]
    async fn mode_setattr_rejects_non_owner_even_with_write_bits() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chmod(attr.ino, 0o666).await.unwrap();

        let err = Filesystem::setattr(
            &fs,
            user_request(),
            attr.ino as u64,
            None,
            SetAttr {
                mode: Some(0o600),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::EPERM));
    }

    #[tokio::test]
    async fn mode_setattr_allows_non_owner_to_clear_only_suid_sgid_bits() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chmod(attr.ino, 0o6777).await.unwrap();

        let reply = Filesystem::setattr(
            &fs,
            user_request(),
            attr.ino as u64,
            None,
            SetAttr {
                mode: Some(0o777),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.perm, 0o777);
    }

    #[tokio::test]
    async fn mode_setattr_clears_sgid_for_owner_outside_file_group() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chown(attr.ino, Some(1000), Some(2000)).await.unwrap();

        let reply = Filesystem::setattr(
            &fs,
            user_request(),
            attr.ino as u64,
            None,
            SetAttr {
                mode: Some(0o2755),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.perm, 0o755);
    }

    #[tokio::test]
    async fn mode_setattr_preserves_sgid_for_owner_inside_file_group() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chown(attr.ino, Some(1000), Some(2000)).await.unwrap();

        let reply = Filesystem::setattr(
            &fs,
            request_with_ids(1000, 2000),
            attr.ino as u64,
            None,
            SetAttr {
                mode: Some(0o2755),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.perm, 0o2755);
    }

    #[tokio::test]
    async fn chown_setattr_allows_non_owner_noop_ids() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();

        let reply = Filesystem::setattr(
            &fs,
            user_request(),
            attr.ino as u64,
            None,
            SetAttr {
                uid: Some(u32::MAX),
                gid: Some(u32::MAX),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.uid, 0);
        assert_eq!(reply.attr.gid, 0);
    }

    #[tokio::test]
    async fn chown_setattr_allows_owner_group_change_and_clears_suid_sgid() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chown(attr.ino, Some(1000), Some(1000)).await.unwrap();
        fs.chmod(attr.ino, 0o6555).await.unwrap();

        let reply = Filesystem::setattr(
            &fs,
            request_with_ids(1000, 2000),
            attr.ino as u64,
            None,
            SetAttr {
                uid: Some(1000),
                gid: Some(2000),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.uid, 1000);
        assert_eq!(reply.attr.gid, 2000);
        assert_eq!(reply.attr.perm, 0o555);
    }

    #[tokio::test]
    async fn chown_setattr_allows_owner_group_change_with_kernel_clear_mode() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chown(attr.ino, Some(1000), Some(1000)).await.unwrap();
        fs.chmod(attr.ino, 0o6555).await.unwrap();

        let reply = Filesystem::setattr(
            &fs,
            request_with_ids(1000, 2000),
            attr.ino as u64,
            None,
            SetAttr {
                uid: Some(1000),
                gid: Some(2000),
                mode: Some(0o555),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(reply.attr.uid, 1000);
        assert_eq!(reply.attr.gid, 2000);
        assert_eq!(reply.attr.perm, 0o555);
    }

    #[tokio::test]
    async fn chown_setattr_rejects_non_owner_group_change() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();

        let err = Filesystem::setattr(
            &fs,
            user_request(),
            attr.ino as u64,
            None,
            SetAttr {
                gid: Some(2000),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::EPERM));
    }

    #[tokio::test]
    async fn chown_setattr_rejects_owner_group_change_to_non_member_group() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chown(attr.ino, Some(1000), Some(1000)).await.unwrap();

        let err = Filesystem::setattr(
            &fs,
            user_request(),
            attr.ino as u64,
            None,
            SetAttr {
                gid: Some(2000),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::EPERM));
    }

    #[tokio::test]
    async fn stateless_write_requires_write_access_on_inode() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();

        let err = Filesystem::write(&fs, user_request(), attr.ino as u64, 0, 0, b"x", 0, 0)
            .await
            .unwrap_err();

        assert_eq!(err, Errno::from(libc::EACCES));
        assert_eq!(fs.stat("/file.txt").await.unwrap().size, 0);
    }

    #[tokio::test]
    async fn non_owner_write_clears_suid_and_sgid_bits() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chmod(attr.ino, 0o6777).await.unwrap();

        let request = request_with_ids(65534, 65534);
        let opened = Filesystem::open(&fs, request, attr.ino as u64, libc::O_RDWR as u32)
            .await
            .unwrap();

        let reply = Filesystem::write(&fs, request, attr.ino as u64, opened.fh, 0, b"x", 0, 0)
            .await
            .unwrap();

        assert_eq!(reply.written, 1);
        assert_eq!(fs.stat("/file.txt").await.unwrap().mode & 0o7777, 0o0777);
        fs.close(opened.fh).await.unwrap();
    }

    #[tokio::test]
    async fn fallocate_requires_write_access_on_inode() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();

        let err = Filesystem::fallocate(&fs, user_request(), attr.ino as u64, 0, 0, 4096, 0)
            .await
            .unwrap_err();

        assert_eq!(err, Errno::from(libc::EACCES));
        assert_eq!(fs.stat("/file.txt").await.unwrap().size, 0);
    }

    #[tokio::test]
    async fn open_with_trunc_clears_existing_file() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.write_ino(attr.ino, 0, b"old").await.unwrap();
        fs.flush_inode(attr.ino as u64).await;
        assert_eq!(fs.stat("/file.txt").await.unwrap().size, 3);

        let reply = Filesystem::open(
            &fs,
            Request::default(),
            attr.ino as u64,
            (libc::O_WRONLY | libc::O_TRUNC) as u32,
        )
        .await
        .unwrap();

        assert_eq!(fs.stat("/file.txt").await.unwrap().size, 0);
        fs.close(reply.fh).await.unwrap();
    }

    #[tokio::test]
    async fn lseek_hole_on_empty_file_returns_enxio() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();

        let err = Filesystem::lseek(
            &fs,
            Request::default(),
            attr.ino as u64,
            0,
            0,
            libc::SEEK_HOLE as u32,
        )
        .await
        .unwrap_err();

        assert_eq!(err, Errno::from(libc::ENXIO));
    }

    #[tokio::test]
    async fn lseek_hole_on_dense_file_returns_eof() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.write_ino(attr.ino, 0, b"hello").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();

        let reply = Filesystem::lseek(
            &fs,
            Request::default(),
            attr.ino as u64,
            0,
            2,
            libc::SEEK_HOLE as u32,
        )
        .await
        .unwrap();

        assert_eq!(reply.offset, 5);
    }

    #[test]
    fn open_reply_flags_default_to_buffered_handles_for_mmap() {
        let _guard = env_lock().lock().unwrap();
        unsafe {
            std::env::remove_var("BREWFS_FUSE_DIRECT_IO");
            std::env::remove_var("BREWFS_FUSE_KEEP_CACHE");
            std::env::remove_var("BREWFS_FUSE_READ_DIRECT_IO");
            std::env::remove_var("BREWFS_FUSE_WRITE_DIRECT_IO");
        }

        assert_eq!(fuse_open_reply_flags(true, false), 0);
        assert_eq!(fuse_open_reply_flags(true, true), 0);
        assert_eq!(fuse_open_reply_flags(false, true), 0);
    }

    #[test]
    fn cache_ttl_defaults_keep_create_attrs_uncached() {
        let _guard = env_lock().lock().unwrap();
        unsafe {
            std::env::remove_var("BREWFS_CACHE_TTL_MS");
        }

        assert_eq!(fuse_cache_ttl(), Duration::from_secs(1));
        assert_eq!(fuse_create_cache_ttl(), Duration::ZERO);
    }

    #[test]
    fn open_reply_flags_can_opt_into_read_only_direct_io() {
        let _guard = env_lock().lock().unwrap();
        unsafe {
            std::env::remove_var("BREWFS_FUSE_DIRECT_IO");
            std::env::remove_var("BREWFS_FUSE_KEEP_CACHE");
            std::env::set_var("BREWFS_FUSE_READ_DIRECT_IO", "1");
            std::env::remove_var("BREWFS_FUSE_WRITE_DIRECT_IO");
        }

        assert_eq!(fuse_open_reply_flags(true, false), FOPEN_DIRECT_IO);
        assert_eq!(fuse_open_reply_flags(true, true), 0);
        assert_eq!(fuse_open_reply_flags(false, true), 0);

        unsafe {
            std::env::remove_var("BREWFS_FUSE_READ_DIRECT_IO");
        }
    }

    #[test]
    fn open_reply_flags_can_opt_into_all_handle_direct_io() {
        let _guard = env_lock().lock().unwrap();
        unsafe {
            std::env::set_var("BREWFS_FUSE_DIRECT_IO", "1");
            std::env::remove_var("BREWFS_FUSE_KEEP_CACHE");
            std::env::remove_var("BREWFS_FUSE_READ_DIRECT_IO");
            std::env::remove_var("BREWFS_FUSE_WRITE_DIRECT_IO");
        }

        assert_eq!(fuse_open_reply_flags(true, false), FOPEN_DIRECT_IO);
        assert_eq!(fuse_open_reply_flags(true, true), FOPEN_DIRECT_IO);
        assert_eq!(fuse_open_reply_flags(false, true), FOPEN_DIRECT_IO);

        unsafe {
            std::env::remove_var("BREWFS_FUSE_DIRECT_IO");
        }
    }

    #[test]
    fn open_reply_flags_direct_io_off_overrides_read_only_opt_in() {
        let _guard = env_lock().lock().unwrap();
        unsafe {
            std::env::set_var("BREWFS_FUSE_DIRECT_IO", "0");
            std::env::remove_var("BREWFS_FUSE_KEEP_CACHE");
            std::env::set_var("BREWFS_FUSE_READ_DIRECT_IO", "1");
            std::env::remove_var("BREWFS_FUSE_WRITE_DIRECT_IO");
        }

        assert_eq!(fuse_open_reply_flags(true, false), 0);
        assert_eq!(fuse_open_reply_flags(true, true), 0);
        assert_eq!(fuse_open_reply_flags(false, true), 0);

        unsafe {
            std::env::remove_var("BREWFS_FUSE_DIRECT_IO");
            std::env::remove_var("BREWFS_FUSE_READ_DIRECT_IO");
        }
    }

    #[test]
    fn open_reply_flags_ignore_empty_env_values() {
        let _guard = env_lock().lock().unwrap();
        unsafe {
            std::env::set_var("BREWFS_FUSE_DIRECT_IO", "");
            std::env::remove_var("BREWFS_FUSE_KEEP_CACHE");
            std::env::set_var("BREWFS_FUSE_READ_DIRECT_IO", "");
            std::env::set_var("BREWFS_FUSE_WRITE_DIRECT_IO", "");
        }

        assert_eq!(fuse_open_reply_flags(true, false), 0);
        assert_eq!(fuse_open_reply_flags(true, true), 0);
        assert_eq!(fuse_open_reply_flags(false, true), 0);

        unsafe {
            std::env::remove_var("BREWFS_FUSE_DIRECT_IO");
            std::env::remove_var("BREWFS_FUSE_READ_DIRECT_IO");
            std::env::remove_var("BREWFS_FUSE_WRITE_DIRECT_IO");
        }
    }

    #[test]
    fn fuse_copy_file_range_defaults_on_and_can_opt_out() {
        let _guard = env_lock().lock().unwrap();
        unsafe {
            std::env::remove_var("BREWFS_FUSE_COPY_FILE_RANGE");
        }

        assert!(fuse_copy_file_range_enabled());

        unsafe {
            std::env::set_var("BREWFS_FUSE_COPY_FILE_RANGE", "0");
        }
        assert!(!fuse_copy_file_range_enabled());

        unsafe {
            std::env::set_var("BREWFS_FUSE_COPY_FILE_RANGE", "");
        }
        assert!(fuse_copy_file_range_enabled());

        unsafe {
            std::env::remove_var("BREWFS_FUSE_COPY_FILE_RANGE");
        }
    }

    #[test]
    fn open_reply_flags_can_opt_into_write_only_direct_io() {
        let _guard = env_lock().lock().unwrap();
        unsafe {
            std::env::remove_var("BREWFS_FUSE_DIRECT_IO");
            std::env::remove_var("BREWFS_FUSE_KEEP_CACHE");
            std::env::remove_var("BREWFS_FUSE_READ_DIRECT_IO");
            std::env::set_var("BREWFS_FUSE_WRITE_DIRECT_IO", "1");
        }

        assert_eq!(fuse_open_reply_flags(true, false), 0);
        assert_eq!(fuse_open_reply_flags(true, true), 0);
        assert_eq!(fuse_open_reply_flags(false, true), FOPEN_DIRECT_IO);

        unsafe {
            std::env::remove_var("BREWFS_FUSE_WRITE_DIRECT_IO");
        }
    }

    #[test]
    fn open_reply_flags_can_opt_into_keep_cache() {
        let _guard = env_lock().lock().unwrap();
        unsafe {
            std::env::remove_var("BREWFS_FUSE_DIRECT_IO");
            std::env::set_var("BREWFS_FUSE_KEEP_CACHE", "1");
            std::env::remove_var("BREWFS_FUSE_READ_DIRECT_IO");
            std::env::remove_var("BREWFS_FUSE_WRITE_DIRECT_IO");
        }

        assert_eq!(fuse_open_reply_flags(true, false), FOPEN_KEEP_CACHE);
        assert_eq!(fuse_open_reply_flags(true, true), FOPEN_KEEP_CACHE);
        assert_eq!(fuse_open_reply_flags(false, true), FOPEN_KEEP_CACHE);

        unsafe {
            std::env::remove_var("BREWFS_FUSE_KEEP_CACHE");
        }
    }
}
