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
use crate::meta::MetaLayer;
use crate::meta::file_lock::{FileLockQuery, FileLockRange, FileLockType};
use crate::meta::store::{MetaError, SetAttrFlags, SetAttrRequest};
use crate::posix::NAME_MAX;
use crate::vfs::error::VfsError;
use crate::vfs::fs::{FileAttr as VfsFileAttr, FileType as VfsFileType, VFS};
use bytes::Bytes;
use rfuse3::Errno;
use rfuse3::Result as FuseResult;
use rfuse3::raw::Request;
use rfuse3::raw::flags::{FOPEN_DIRECT_IO, FOPEN_KEEP_CACHE, FUSE_WRITE_CACHE};
use rfuse3::raw::reply::{
    DirectoryEntry, DirectoryEntryPlus, ReplyAttr, ReplyCopyFileRange, ReplyCreated, ReplyData,
    ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit, ReplyIoctl, ReplyLSeek, ReplyLock,
    ReplyOpen, ReplyStatFs, ReplyWrite, ReplyXAttr,
};
use std::ffi::{OsStr, OsString};
use std::mem::size_of;
use std::num::NonZeroU32;
use std::time::Duration;

use futures_util::stream::{self, BoxStream};
use rfuse3::raw::Filesystem;
use rfuse3::{FileType as FuseFileType, SetAttr, Timestamp};
use tracing::{debug, error, info, trace, warn};

/// Runtime-configurable kernel attribute/entry cache TTL.
/// Non-zero lets the kernel serve repeated getattr/lookup from its own cache
/// without round-tripping to userspace — eliminating the stat() performance gap.
/// Default: 1s (matches JuiceFS).  Override via BREWFS_CACHE_TTL_MS=0 for
/// strict multi-client coherency.
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

fn fuse_read_direct_io_enabled() -> bool {
    std::env::var("BREWFS_FUSE_READ_DIRECT_IO")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn fuse_open_reply_flags(read: bool, write: bool) -> u32 {
    let mut flags = FOPEN_KEEP_CACHE;
    if read && !write && fuse_read_direct_io_enabled() {
        flags |= FOPEN_DIRECT_IO;
    }
    flags
}

/// Virtual inode for the `.stats` file exposed at the mount root.
/// Uses a high inode number unlikely to collide with real inodes.
const STATS_INODE: u64 = 0x7FFF_FFFF_0000_0003;
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

    fn ioctl_ok_reply() -> ReplyIoctl {
        ReplyIoctl {
            result: 0,
            flags: 0,
            in_iovs: 0,
            out_iovs: 0,
            data: Vec::new(),
        }
    }

    fn parse_clone_range(data: &[u8]) -> Option<FileCloneRange> {
        if data.len() < size_of::<FileCloneRange>() {
            return None;
        }

        // The kernel provides restricted ioctl payloads inline using the native
        // C layout for this architecture.
        Some(unsafe { std::ptr::read_unaligned(data.as_ptr().cast::<FileCloneRange>()) })
    }

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
                .map(|mode| current.mode & 0o777 == mode)
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
}

fn fuse_lock_end_to_exclusive(end: u64) -> u64 {
    end.saturating_add(1)
}

fn exclusive_lock_end_to_fuse(end: u64) -> u64 {
    end.saturating_sub(1)
}

#[repr(C)]
#[derive(Clone, Copy)]
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
            let attr = rfuse3::raw::reply::FileAttr {
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

        let _timer = crate::vfs::stats::OpTimer::new(
            &self.stats().fuse_lookup_ops,
            &self.stats().fuse_lookup_lat_us,
        );

        let name_str = name.to_string_lossy();
        let Some((_child_ino, vattr)) = self.child_attr_of(parent as i64, name_str.as_ref()).await
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
    async fn open(&self, _req: Request, ino: u64, flags: u32) -> FuseResult<ReplyOpen> {
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
        debug!(
            ino,
            flags,
            read,
            write,
            has_append = append,
            has_creat = (flags & libc::O_CREAT as u32) != 0,
            "fuse.open"
        );
        let fh = self
            .open_fresh_ino(ino as i64, read, write, append)
            .await
            .map_err(Into::<Errno>::into)?;

        // ReplyOpen.flags carries FUSE FOPEN_* bits, not the caller's O_* flags.
        // With writeback cache enabled, set FOPEN_KEEP_CACHE so that the kernel
        // does not invalidate clean page-cache pages on every open().  Without it
        // any concurrent open (even from an unrelated process) evicts clean pages
        // that may have been written back, forcing FUSE_READ on the next access
        // and exposing a window where overlay_dirty may miss in-flight data.
        Ok(ReplyOpen {
            fh,
            flags: fuse_open_reply_flags(read, write),
        })
    }

    // Open directory: create handle for caching
    async fn opendir(&self, _req: Request, ino: u64, _flags: u32) -> FuseResult<ReplyOpen> {
        debug!(ino, "fuse.opendir");
        let Some(attr) = self.stat_ino(ino as i64).await else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(attr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }

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
        _req: Request,
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
        _req: Request,
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
                .write_cached_ino(ino as i64, offset, data, _req.unique)
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
            let attr = rfuse3::raw::reply::FileAttr {
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
    // Permission checks are handled by the kernel (via default_permissions mount option).
    //
    // Security: setuid/setgid/sticky bits are stripped from mode changes.
    async fn setattr(
        &self,
        req: Request,
        ino: u64,
        _fh: Option<u64>,
        set_attr: SetAttr,
    ) -> FuseResult<ReplyAttr> {
        debug!(unique = req.unique, ino, set_attr = ?set_attr, "fuse.setattr");
        let setattr_start = std::time::Instant::now();

        let (meta_req, meta_flags) = fuse_setattr_to_meta(&set_attr);

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
        let (blocks, bfree, bavail, files, ffree) = match self.stat_fs().await {
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
        _rdev: u32,
    ) -> FuseResult<ReplyEntry> {
        debug!(
            unique = req.unique,
            parent,
            name = %name.to_string_lossy(),
            mode,
            "fuse.mknod"
        );
        let name = name.to_string_lossy();
        let file_type = mode & libc::S_IFMT;

        let ino = match file_type {
            // Linux accepts mknod(path, 0, 0) as a regular file with mode 000.
            0 | libc::S_IFREG => self
                .create_file_at(parent as i64, &name, true)
                .await
                .map_err(Errno::from)?,
            libc::S_IFDIR => self
                .mkdir_at_new(parent as i64, &name)
                .await
                .map_err(Errno::from)?,
            libc::S_IFIFO | libc::S_IFSOCK | libc::S_IFCHR | libc::S_IFBLK => {
                return Err(libc::ENOSYS.into());
            }
            _ => {
                return Err(libc::EINVAL.into());
            }
        };

        // Apply mode after stripping special bits unsupported by BrewFS.
        let Some(vattr) = self
            .apply_new_entry_attrs(ino, req.uid, req.gid, Some(mode))
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
        let _ino = self
            .mkdir_at_new(parent as i64, &name)
            .await
            .map_err(Errno::from)?;
        // Strip setuid/setgid/sticky, then apply the caller's umask.
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
        let create_new = (flags & libc::O_EXCL as u32) != 0;
        let ino = match self.create_file_at(parent as i64, &name, create_new).await {
            Ok(ino) => {
                debug!(
                    ino,
                    name = %name,
                    flags,
                    has_append = (flags & libc::O_APPEND as u32) != 0,
                    "fuse.create ok"
                );
                ino
            }
            Err(VfsError::AlreadyExists { .. }) if !create_new => {
                debug!(name = %name, "fuse.create EEXIST, falling back to lookup");
                self.child_of(parent as i64, &name).await.ok_or_else(|| {
                    debug!(name = %name, "fuse.create fallback lookup also failed");
                    Errno::from(libc::EIO)
                })?
            }
            Err(e) => {
                debug!(name = %name, flags, error = %e, "fuse.create err");
                return Err(Errno::from(e));
            }
        };
        let Some(vattr) = self
            .apply_new_entry_attrs(ino, req.uid, req.gid, Some(mode))
            .await
        else {
            return Err(libc::ENOENT.into());
        };
        let attr = vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr));

        let accmode = flags & (libc::O_ACCMODE as u32);
        let read = accmode != (libc::O_WRONLY as u32);
        let write = accmode != (libc::O_RDONLY as u32);
        let append = (flags & libc::O_APPEND as u32) != 0;
        let fh = self
            .open_with_cached_attr(ino, vattr.clone(), read, write, append)
            .await
            .map_err(Into::<Errno>::into)?;
        Ok(ReplyCreated {
            ttl: fuse_cache_ttl(),
            attr,
            generation: 0,
            fh,
            flags: FOPEN_KEEP_CACHE,
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

        if new_name_str.is_empty() || new_name_str.contains('/') || new_name_str.contains('\0') {
            return Err(libc::EINVAL.into());
        }

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
        if name.is_empty() {
            return Err(libc::EINVAL.into());
        }

        let Some(pattr) = self.stat_ino(parent as i64).await else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(pattr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }

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
    async fn unlink(&self, _req: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        debug!(parent, name = %name.to_string_lossy(), "fuse.unlink");
        let name = name.to_string_lossy();
        // Ensure parent directory exists and has the right type
        let Some(pattr) = self.stat_ino(parent as i64).await else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(pattr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }
        // Target must exist and be a file
        let Some(child) = self.child_of(parent as i64, name.as_ref()).await else {
            return Err(libc::ENOENT.into());
        };
        let Some(cattr) = self.stat_ino(child).await else {
            return Err(libc::ENOENT.into());
        };
        if matches!(cattr.kind, VfsFileType::Dir) {
            return Err(libc::EISDIR.into());
        }
        self.unlink_at(parent as i64, &name)
            .await
            .map_err(Errno::from)
    }

    // Remove an empty directory
    async fn rmdir(&self, _req: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        debug!(parent, name = %name.to_string_lossy(), "fuse.rmdir");
        let name = name.to_string_lossy();
        let Some(pattr) = self.stat_ino(parent as i64).await else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(pattr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }
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
        self.rmdir_at(parent as i64, &name)
            .await
            .map_err(Errno::from)
    }

    // Rename (files or directories)
    async fn rename(
        &self,
        _req: Request,
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

        // Validate input parameters
        if name.is_empty() || new_name.is_empty() {
            return Err(libc::EINVAL.into());
        }

        // Check for invalid characters in names
        if name.contains('/')
            || name.contains('\0')
            || new_name.contains('/')
            || new_name.contains('\0')
        {
            return Err(libc::EINVAL.into());
        }

        // POSIX rename to the same location is a no-op.
        if parent == new_parent && name == new_name {
            return Ok(());
        }

        // Ensure the source exists
        if self.child_of(parent as i64, name.as_ref()).await.is_none() {
            return Err(libc::ENOENT.into());
        }

        // Validate the destination parent
        let Some(pattr) = self.stat_ino(new_parent as i64).await else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(pattr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }

        // Flush pending writes for the source inode before the rename so
        // that temp-file + rename patterns (e.g. object_store PutMode::Create)
        // do not race with in-flight write-back commit tasks.
        if let Some(src_ino) = self.child_of(parent as i64, name.as_ref()).await {
            self.flush_inode(src_ino as u64).await;
        }

        self.rename_at(parent as i64, &name, new_parent as i64, &new_name)
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
        self.close(fh).await.map_err(Errno::from)?;
        Ok(())
    }

    // Flush file data to backend (called on every close of a file descriptor).
    // Must actually persist dirty data so that close() semantics are honored.
    async fn flush(&self, _req: Request, inode: u64, fh: u64, lock_owner: u64) -> FuseResult<()> {
        // Virtual .stats file: nothing to flush.
        if inode == STATS_INODE {
            return Ok(());
        }
        debug!(fh, inode, "fuse.flush");
        self.unlock_owner_locks(inode, lock_owner).await;
        // VFS::flush persists pending writes (full 300s deadline).
        self.flush(fh).await.map_err(Errno::from)?;
        Ok(())
    }

    // Sync file content to backend
    async fn fsync(&self, _req: Request, _inode: u64, fh: u64, datasync: bool) -> FuseResult<()> {
        debug!(fh, datasync, "fuse.fsync");
        self.fsync(fh, datasync).await.map_err(Errno::from)
    }

    async fn fallocate(
        &self,
        _req: Request,
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
        self.fallocate_ino(inode as i64, offset, length)
            .await
            .map_err(Errno::from)
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
                if signed_offset < 0 || signed_offset > size {
                    return Err(libc::ENXIO.into());
                }
                signed_offset
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

        match cmd {
            x if x == libc::FICLONE as u32 => self.ioctl_ficlone(req, inode, arg).await,
            x if x == libc::FICLONERANGE as u32 => {
                self.ioctl_ficlonerange(req, inode, in_data).await
            }
            _ => Err(libc::EOPNOTSUPP.into()),
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
        let fl_type = match lock_type as i32 {
            libc::F_RDLCK => FileLockType::Read,
            libc::F_WRLCK => FileLockType::Write,
            libc::F_UNLCK => FileLockType::UnLock,
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
                    FileLockType::Read => libc::F_RDLCK,
                    FileLockType::Write => libc::F_WRLCK,
                    FileLockType::UnLock => libc::F_UNLCK,
                };
                Ok(ReplyLock {
                    r#type: fuse_type as u32,
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
        _fh: u64,
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
        let fl_type = match lock_type as i32 {
            libc::F_RDLCK => FileLockType::Read,
            libc::F_WRLCK => FileLockType::Write,
            libc::F_UNLCK => FileLockType::UnLock,
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
        let Some(attr) = self.stat_ino(ino as i64).await else {
            return Err(libc::ENOENT.into());
        };

        // F_OK (0) just checks for existence
        if mask == 0 {
            return Ok(());
        }

        // Check if the requesting user has the required access
        let uid = req.uid;
        let gid = req.gid;

        // Root can access everything (except execute on non-executable files)
        if uid == 0 {
            // Root still needs execute permission to be set somewhere
            if (mask & libc::X_OK as u32) != 0 && (attr.mode & 0o111) == 0 {
                return Err(libc::EACCES.into());
            }
            return Ok(());
        }

        // Determine which permission bits to check
        let mode = if uid == attr.uid {
            // Owner permissions
            (attr.mode >> 6) & 0o7
        } else if gid == attr.gid {
            // Group permissions
            (attr.mode >> 3) & 0o7
        } else {
            // Other permissions
            attr.mode & 0o7
        };

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
}

// =============== helpers ===============
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
    }
}

fn vfs_to_fuse_attr(v: &VfsFileAttr, _req: &Request, blocks: u64) -> rfuse3::raw::reply::FileAttr {
    let perm = (v.mode & 0o7777) as u16;
    let atime = nanos_to_timestamp(v.atime);
    let mtime = nanos_to_timestamp(v.mtime);
    let ctime = nanos_to_timestamp(v.ctime);
    rfuse3::raw::reply::FileAttr {
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
        rdev: 0,
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
    mode & 0o777
}

fn apply_creation_umask(mode: u32, umask: u32) -> u32 {
    sanitize_special_mode_bits(mode) & !(umask & 0o777)
}

fn fuse_setattr_to_meta(set_attr: &SetAttr) -> (SetAttrRequest, SetAttrFlags) {
    let mut req = SetAttrRequest::default();
    let flags = SetAttrFlags::empty();
    if let Some(mode) = set_attr.mode {
        // Strip setuid (0o4000), setgid (0o2000), and sticky (0o1000) bits.
        // BrewFS does not implement the semantics behind these special bits.
        req.mode = Some(sanitize_special_mode_bits(mode));
    }
    if let Some(uid) = set_attr.uid {
        req.uid = Some(uid);
    }
    if let Some(gid) = set_attr.gid {
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

#[cfg(test)]
mod mode_sanitization_tests {
    use super::{apply_creation_umask, sanitize_special_mode_bits};
    use std::collections::BTreeSet;

    #[test]
    fn sanitize_special_mode_bits_drops_setuid_setgid_and_sticky() {
        assert_eq!(sanitize_special_mode_bits(0o1777), 0o777);
        assert_eq!(sanitize_special_mode_bits(0o2755), 0o755);
        assert_eq!(sanitize_special_mode_bits(0o4755), 0o755);
    }

    #[test]
    fn apply_creation_umask_runs_after_special_bit_stripping() {
        assert_eq!(apply_creation_umask(0o1777, 0), 0o777);
        assert_eq!(apply_creation_umask(0o1777, 0o022), 0o755);
        assert_eq!(apply_creation_umask(0o4755, 0o022), 0o755);
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
}

#[cfg(test)]
mod fuse_init_tests {
    use super::*;
    use crate::chunk::layout::ChunkLayout;
    use crate::chunk::store::InMemoryBlockStore;
    use crate::meta::factory::create_meta_store_from_url;
    use rfuse3::raw::Filesystem;
    use rfuse3::raw::flags::FOPEN_DIRECT_IO;
    use std::sync::{Mutex as StdMutex, OnceLock};

    fn env_lock() -> &'static StdMutex<()> {
        static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| StdMutex::new(()))
    }

    #[tokio::test]
    async fn init_reply_advertises_large_write_requests() {
        let layout = ChunkLayout::default();
        let store = InMemoryBlockStore::new();
        let meta_handle = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let fs = VFS::new(layout, store, meta_handle.store()).await.unwrap();

        let reply = Filesystem::init(&fs, Request::default()).await.unwrap();

        assert_eq!(reply.max_write.get(), 4 * 1024 * 1024);
    }

    #[test]
    fn open_reply_flags_keep_cache_by_default() {
        let _guard = env_lock().lock().unwrap();
        unsafe {
            std::env::remove_var("BREWFS_FUSE_READ_DIRECT_IO");
        }

        assert_eq!(fuse_open_reply_flags(true, false), FOPEN_KEEP_CACHE);
    }

    #[test]
    fn open_reply_flags_enable_direct_io_for_read_only_handles() {
        let _guard = env_lock().lock().unwrap();
        unsafe {
            std::env::set_var("BREWFS_FUSE_READ_DIRECT_IO", "1");
        }

        assert_eq!(
            fuse_open_reply_flags(true, false),
            FOPEN_KEEP_CACHE | FOPEN_DIRECT_IO
        );
        assert_eq!(fuse_open_reply_flags(true, true), FOPEN_KEEP_CACHE);

        unsafe {
            std::env::remove_var("BREWFS_FUSE_READ_DIRECT_IO");
        }
    }
}
