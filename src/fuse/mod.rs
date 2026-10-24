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
#[cfg(all(test, feature = "workspace-overlay", target_os = "linux"))]
mod cancel_tests;
#[cfg(target_os = "linux")]
pub(crate) mod clone_ioctl;
#[cfg(feature = "workspace-overlay")]
pub mod entity;
pub mod mount;
use crate::chunk::store::BlockStore;
use crate::control::protocol::{CONTROL_ACL_XATTR_NAME, ControlAclEntry};
use crate::meta::MetaLayer;
use crate::meta::file_lock::{FileLockQuery, FileLockRange, FileLockType};
use crate::meta::store::{MetaError, SetAttrFlags, SetAttrRequest};
use crate::posix::NAME_MAX;
use crate::vfs::error::{PathHint, VfsError};
use crate::vfs::fs::{CreateFileAtResult, FileAttr as VfsFileAttr, FileType as VfsFileType, VFS};
use crate::vfs::handles::RawDirEntry;
use asyncfuse::Errno;
use asyncfuse::Result as FuseResult;
use asyncfuse::notify::Notify as FuseNotify;
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
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asyncfuse::raw::Filesystem;
use asyncfuse::{FileType as FuseFileType, SetAttr, Timestamp};
use futures_util::stream::{self, BoxStream};
use tracing::{debug, error, info, trace, warn};

struct MetadataReplyOwner {
    data: Vec<u8>,
    _guard: asyncfuse::raw::reply::ReplyMemoryGuard,
}
impl AsRef<[u8]> for MetadataReplyOwner {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

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

fn os_string_from_raw_name(name: &[u8]) -> OsString {
    #[cfg(unix)]
    {
        OsString::from_vec(name.to_vec())
    }
    #[cfg(not(unix))]
    {
        OsString::from(String::from_utf8_lossy(name).into_owned())
    }
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

fn is_posix_acl_xattr(name: impl AsRef<[u8]>) -> bool {
    name.as_ref() == b"system.posix_acl_access" || name.as_ref() == b"system.posix_acl_default"
}

/// Xattr names in the `system.*` namespace are reserved for kernel POSIX ACL
/// semantics and BrewFS control-plane metadata (e.g. `system.brewfs.acl`,
/// `system.brewfs.trash`). Those are written through the meta client, never by
/// untrusted FUSE clients, so they are hidden from the FUSE xattr interface.
fn is_internal_xattr(name: impl AsRef<[u8]>) -> bool {
    name.as_ref().starts_with(b"system.")
}

/// POSIX reserves the `user.*` namespace for unprivileged extended attributes;
/// FUSE clients may write names in that namespace. Other reserved namespaces
/// are handled explicitly by the operation-specific policy below.
fn is_user_xattr_name(name: impl AsRef<[u8]>) -> bool {
    name.as_ref().starts_with(b"user.")
}

/// Linux reserves the `trusted.*` namespace for callers with
/// `CAP_SYS_ADMIN`. FUSE requests expose the caller uid but not its capability
/// set, so UID 0 is the narrowest authorization available at this layer.
/// Other reserved namespaces remain unavailable to FUSE clients.
fn is_trusted_xattr_name(name: impl AsRef<[u8]>) -> bool {
    name.as_ref().starts_with(b"trusted.")
}

fn can_access_trusted_xattr(uid: u32, name: impl AsRef<[u8]>) -> bool {
    !is_trusted_xattr_name(name) || uid == 0
}

/// Virtual inode for the `.stats` file exposed at the mount root.
/// Uses a high inode number unlikely to collide with real inodes.
const STATS_INODE: u64 = 0x7FFF_FFFF_0000_0003;
/// Linux marks execve-driven FUSE open requests with this internal flag.
/// It is not a public libc O_* bit, but it is visible in the FUSE open flags.
const FUSE_OPEN_EXEC: u32 = 0x20;
/// Name of the virtual stats file.
const STATS_FILENAME: &str = ".stats";
const STATS_FILE_BLOCK_SIZE: u32 = 4096;
pub(crate) const BREWFS_FUSE_MAX_WRITE: u32 = 4 * 1024 * 1024;
const FUSE_LOCKED_READ_ORDER_GRACE: Duration = Duration::from_millis(1);
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
    async fn fuse_read_data(
        &self,
        req: Request,
        ino: u64,
        fh: u64,
        offset: u64,
        size: u32,
        temporary: &mut Option<crate::vfs::fs::FileGuard<S, M>>,
    ) -> FuseResult<ReplyData> {
        let _timer = crate::vfs::stats::OpTimer::new(
            &self.stats().fuse_read_ops,
            &self.stats().fuse_read_lat_us,
        );
        debug!(ino, fh, offset, size, "fuse.read");
        if self.has_posix_locks_for_inode(ino as i64) {
            tokio::time::sleep(FUSE_LOCKED_READ_ORDER_GRACE).await;
        }
        self.wait_for_prior_fuse_writes(ino as i64, req.unique)
            .await;
        let data = if fh != 0 {
            match self.read_owned(fh, offset, size as usize).await {
                Ok(data) => data,
                Err(VfsError::PermissionDenied { .. }) => {
                    // Writeback page fills may arrive on an O_WRONLY handle.
                    let attr = self
                        .stat_ino(ino as i64)
                        .await
                        .ok_or(Errno::from(libc::ENOENT))?;
                    *temporary = Some(
                        self.open_guard(ino as i64, attr, true, false)
                            .await
                            .map_err(Errno::from)?,
                    );
                    self.read_owned(temporary.as_ref().unwrap().fh(), offset, size as usize)
                        .await
                        .map_err(Errno::from)?
                }
                Err(error) => {
                    #[cfg(test)]
                    eprintln!(
                        "[packed-v3-native-kernel-read-diag] stage=fuse-read inode={ino} offset={offset} size={size} error={error}"
                    );
                    return Err(error.into());
                }
            }
        } else {
            let attr = self
                .stat_ino(ino as i64)
                .await
                .ok_or(Errno::from(libc::ENOENT))?;
            *temporary = Some(
                self.open_guard(ino as i64, attr, true, false)
                    .await
                    .map_err(Errno::from)?,
            );
            self.read_owned(temporary.as_ref().unwrap().fh(), offset, size as usize)
                .await
                .map_err(Errno::from)?
        };
        self.stats()
            .fuse_read_bytes
            .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(ReplyData { data })
    }

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
        if fh == 0 || !self.has_fuse_lock_owners() {
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
    fn ioctl_fsgetxattr_reply() -> ReplyIoctl {
        // xfs_io's `stat` probes FS_IOC_FSGETXATTR before printing the
        // portable stat fields.  FUSE has no project-specific fsxattr state,
        // but returning a zeroed structure is the same neutral answer that a
        // filesystem with no flags/project quota would expose and avoids
        // leaking an unsupported-ioctl diagnostic into xfstests output.
        ReplyIoctl {
            result: 0,
            flags: 0,
            in_iovs: 0,
            out_iovs: 0,
            data: vec![0; 28], // sizeof(struct fsxattr) on Linux
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
    fn resolve_proc_fd_source(&self, pid: u32, fd: i64) -> FuseResult<clone_ioctl::SourceFile> {
        let device = self
            .fuse_mount_device()
            .ok_or_else(|| Errno::from(libc::EXDEV))?;
        clone_ioctl::source_file(pid, fd, device)
    }

    #[cfg(target_os = "linux")]
    async fn ioctl_clone(
        &self,
        req: Request,
        dst_ino: u64,
        dst_fh: u64,
        src_fd: i64,
        range: Option<(u64, u64, u64)>,
    ) -> FuseResult<ReplyIoctl> {
        let dst = self
            .pin_file_handle(dst_fh)
            .await
            .map_err(|_| Errno::from(libc::EBADF))?;
        if dst.handle.ino != dst_ino as i64 || !dst.handle.flags.write || dst.handle.flags.append {
            return Err(libc::EBADF.into());
        }
        let source = self.resolve_proc_fd_source(req.pid, src_fd)?;
        let src = self
            .pin_clone_source(source.ino, &dst)
            .map_err(|_| Errno::from(libc::EBADF))?;
        self.clone_file_handles(source, src, dst, range)
            .await
            .map_err(Errno::from)?;
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
        match self
            .set_attr_from_fuse(ino, &req, SetAttrFlags::empty())
            .await
        {
            Ok(attr) => Some(attr),
            Err(err) => {
                warn!(
                    ino,
                    error = %err,
                    error_debug = ?err,
                    "fuse.apply_new_entry_attrs_failed"
                );
                self.stat_ino(ino).await
            }
        }
    }

    fn creation_attrs_for_parent(
        parent_attr: &VfsFileAttr,
        requested_gid: u32,
        requested_mode: u32,
        is_dir: bool,
    ) -> (u32, u32) {
        let mut mode = sanitize_special_mode_bits(requested_mode);
        let gid = if parent_attr.mode & 0o2000 != 0 {
            if is_dir {
                mode |= 0o2000;
            }
            parent_attr.gid
        } else {
            requested_gid
        };
        (gid, mode)
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

        self.set_attr_from_fuse(ino, &SetAttrRequest::default(), flags)
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
    fn supports_read_cancellation(&self) -> bool {
        self.meta_layer().supports_fuse_read_cancellation()
    }
    fn reserve_input_buffer_memory(
        &self,
        bytes: u64,
    ) -> FuseResult<Option<asyncfuse::raw::reply::ReplyMemoryGuard>> {
        self.meta_layer()
            .reserve_memory(crate::meta::layer::MetadataMemoryKind::Roots, bytes)
            .map_err(|error| {
                let errno = Errno::from(VfsError::from_meta(PathHint::none(), error));
                #[cfg(test)]
                if bytes >= u64::from(BREWFS_FUSE_MAX_WRITE) {
                    eprintln!(
                        "[packed-v3-input-diag] bytes={bytes} errno={}",
                        -i32::from(errno)
                    );
                }
                errno
            })
    }
    fn reserve_inline_root_memory(
        &self,
        bytes: u64,
    ) -> FuseResult<Option<asyncfuse::raw::reply::InlineRootPermit>> {
        self.meta_layer()
            .reserve_inline_roots(bytes)
            .map_err(|error| Errno::from(VfsError::from_meta(PathHint::none(), error)))
    }
    fn reserve_inline_prepare_memory(
        &self,
        bytes: u64,
    ) -> FuseResult<Option<asyncfuse::raw::reply::InlineRootPermit>> {
        self.reserve_inline_root_memory(bytes)
    }
    fn reserve_request_memory(
        &self,
        bytes: u64,
    ) -> FuseResult<Option<asyncfuse::raw::reply::ReplyMemoryGuard>> {
        self.meta_layer()
            .reserve_memory(crate::meta::layer::MetadataMemoryKind::Request, bytes)
            .map_err(|error| {
                Errno::from(VfsError::from_meta(
                    crate::vfs::error::PathHint::none(),
                    error,
                ))
            })
    }
    fn reserve_reply_memory(
        &self,
        bytes: u64,
    ) -> FuseResult<Option<asyncfuse::raw::reply::ReplyMemoryGuard>> {
        self.meta_layer()
            .reserve_memory(crate::meta::layer::MetadataMemoryKind::Reply, bytes)
            .map_err(|error| {
                Errno::from(VfsError::from_meta(
                    crate::vfs::error::PathHint::none(),
                    error,
                ))
            })
    }
    fn reserve_control_memory(
        &self,
        bytes: u64,
    ) -> FuseResult<Option<asyncfuse::raw::reply::ReplyMemoryGuard>> {
        self.meta_layer()
            .reserve_memory(crate::meta::layer::MetadataMemoryKind::Control, bytes)
            .map_err(|error| Errno::from(VfsError::from_meta(PathHint::none(), error)))
    }
    async fn init(&self, _req: Request) -> FuseResult<ReplyInit> {
        Ok(ReplyInit {
            max_write: NonZeroU32::new(BREWFS_FUSE_MAX_WRITE)
                .expect("BrewFS FUSE max_write must be non-zero"),
        })
    }

    async fn init_with_notify(&self, _req: Request, notify: FuseNotify) -> FuseResult<ReplyInit> {
        self.set_fuse_notify(notify);
        Ok(ReplyInit {
            max_write: NonZeroU32::new(BREWFS_FUSE_MAX_WRITE)
                .expect("BrewFS FUSE max_write must be non-zero"),
        })
    }

    async fn destroy(&self, _req: Request) {
        #[cfg(target_os = "linux")]
        self.set_fuse_mount_device(None);
        if Filesystem::supports_read_cancellation(self) {
            self.shutdown_fuse_reads().await;
        }
    }

    async fn prepare_unmount(&self) -> FuseResult<()> {
        if Filesystem::supports_read_cancellation(self) {
            self.prepare_fuse_client_unmount().await?;
        }
        Ok(())
    }

    // Call into VFS to resolve parent inode + name → child inode; if found, build ReplyEntry
    async fn lookup(&self, req: Request, parent: u64, name: &OsStr) -> FuseResult<ReplyEntry> {
        let raw_name = raw_os_str_bytes(name);
        let name_str = name.to_string_lossy();
        debug!(
            unique = req.unique,
            parent,
            name = %name_str,
            "fuse.lookup"
        );

        // Preserve an actual source entry named `.stats`. Expose the virtual
        // file only when that name is absent, with the same parent search
        // check as an ordinary lookup; backend failures must not hide a file.
        if parent as i64 == self.root_ino() && raw_name == STATS_FILENAME.as_bytes() {
            self.ensure_access_allowed(parent as i64, req.uid, req.gid, req.pid, libc::X_OK as u32)
                .await?;
            if let Some((_, vattr)) = self.child_attr_of_bytes(parent as i64, raw_name).await? {
                return Ok(ReplyEntry {
                    ttl: self.permission_cache_ttl(),
                    attr: vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr)),
                    generation: 0,
                });
            }
            let snapshot = self.create_virtual_stats_snapshot(false)?;
            let now: Timestamp = std::time::SystemTime::now().into();
            let attr = asyncfuse::raw::reply::FileAttr {
                ino: STATS_INODE,
                size: snapshot.len() as u64,
                blocks: (snapshot.len() as u64).div_ceil(512),
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
                ttl: Duration::ZERO,
                attr,
                generation: 0,
            });
        }

        validate_fuse_name_bytes(raw_name)?;

        let _timer = crate::vfs::stats::OpTimer::new(
            &self.stats().fuse_lookup_ops,
            &self.stats().fuse_lookup_lat_us,
        );

        self.ensure_access_allowed(parent as i64, req.uid, req.gid, req.pid, libc::X_OK as u32)
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                eprintln!(
                    "[packed-v3-native-kernel-read-diag] stage=fuse-lookup-access unique={} parent={parent} error={_error}",
                    req.unique,
                );
            })?;

        let Some((_child_ino, vattr)) = self
            .child_attr_of_bytes(parent as i64, raw_name)
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                eprintln!(
                    "[packed-v3-native-kernel-read-diag] stage=fuse-lookup-child unique={} parent={parent} error={_error}",
                    req.unique,
                );
            })?
        else {
            info!(parent, name = %name_str, "fuse.lookup ENOENT");
            return Err(libc::ENOENT.into());
        };
        let attr = vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr));
        // Keep generation at 0 and set TTL to 1s (tunable)
        Ok(ReplyEntry {
            ttl: self.permission_cache_ttl(),
            attr,
            generation: 0,
        })
    }

    // Open file: allocate a handle for read/write operations.
    async fn open(&self, req: Request, ino: u64, flags: u32) -> FuseResult<ReplyOpen> {
        let kind = if ino == STATS_INODE {
            crate::vfs::fuse_read_cancel::ClientKind::Stats
        } else {
            crate::vfs::fuse_read_cancel::ClientKind::File
        };
        let pending = self
            .begin_fuse_client_open(ino, kind, req.unique)
            .inspect_err(|_error| {
                debug!(ino, error = ?_error, "fuse.open register failed");
                #[cfg(test)]
                eprintln!(
                    "[packed-v3-native-kernel-read-diag] stage=fuse-open-register unique={} inode={ino} flags={flags} error={_error}",
                    req.unique,
                );
            })?;
        let result = self.open_client_untracked(req, ino, flags).await;
        if let Err(error) = &result {
            debug!(ino, error = ?error, "fuse.open failed");
        }
        self.finish_client_open(pending, kind, result)
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                eprintln!(
                    "[packed-v3-native-kernel-read-diag] stage=fuse-open-final unique={} inode={ino} flags={flags} error={_error}",
                    req.unique,
                );
            })
    }

    // Open directory: create handle for caching
    async fn opendir(&self, req: Request, ino: u64, flags: u32) -> FuseResult<ReplyOpen> {
        let kind = crate::vfs::fuse_read_cancel::ClientKind::Directory;
        let pending = self.begin_fuse_client_open(ino, kind, req.unique)?;
        let result = self.opendir_client_untracked(req, ino, flags).await;
        self.finish_client_open(pending, kind, result).await
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
            let snapshot = self
                .virtual_stats_snapshot(fh)
                .ok_or(Errno::from(libc::EBADF))?;
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(snapshot.len());
            let end = start.saturating_add(size as usize).min(snapshot.len());
            return Ok(ReplyData {
                data: snapshot.slice(start..end),
            });
        }

        let mut registration = None;
        let mut temporary = None;
        let (mut result, cancelled) = if Filesystem::supports_read_cancellation(self) {
            let (registered, abort) = self.register_fuse_read(req.unique).inspect_err(|_error| {
                #[cfg(test)]
                eprintln!(
                    "[packed-v3-native-kernel-read-diag] stage=fuse-read-register unique={} inode={ino} fh={fh} offset={offset} size={size} error={_error}",
                    req.unique,
                );
            })?;
            registration = Some(registered);
            tracing::info!(target: "asyncfuse::request_cancel", event="fuse_read_started",
                unique=req.unique, ino, offset, size);
            // This entire future includes metadata, payload, decode and their
            // owned admission guards. The temporary handle stays outside it.
            let outcome = futures_util::future::Abortable::new(
                self.fuse_read_data(req, ino, fh, offset, size, &mut temporary),
                abort,
            )
            .await;
            // The Abortable and its inner future have been dropped here.
            // Keep registration through temporary close. Registry drain means
            // that normal cancellation cleanup has actually finished.
            // A result already completed in the same poll may win the race.
            // Never relabel an already successful logical read as cancelled.
            let cancelled = outcome.is_err();
            let result = if cancelled {
                Err(libc::EINTR.into())
            } else {
                outcome.unwrap()
            };
            (result, cancelled)
        } else {
            (
                self.fuse_read_data(req, ino, fh, offset, size, &mut temporary)
                    .await,
                false,
            )
        };
        if let Some(temporary) = temporary {
            // Cancellation and error both close a temporary fh before replying.
            // Dropping this outer future during close still runs FileGuard Drop.
            temporary.close().await.map_err(Errno::from)?;
        }
        if let Some(registered) = registration {
            if cancelled {
                result = Err(registered.cancellation_errno());
            }
            registered.finish();
        }
        if cancelled {
            tracing::info!(target: "asyncfuse::request_cancel", event="fuse_read_cancelled",
                unique=req.unique, future_dropped=true, temporary_handle_closed=true);
        }
        result.inspect_err(|_error| {
            #[cfg(test)]
            eprintln!(
                "[packed-v3-native-kernel-read-diag] stage=fuse-read-final unique={} inode={ino} fh={fh} offset={offset} size={size} cancelled={cancelled} error={_error}",
                req.unique,
            );
        })
    }

    async fn readlink(&self, _req: Request, ino: u64) -> FuseResult<ReplyData> {
        let reply_guard = Filesystem::reserve_reply_memory(self, 4096)?;
        debug!(ino, "fuse.readlink");
        let target = self
            .readlink_bytes_ino(ino as i64)
            .await
            .map_err(Errno::from)?;

        // Update atime after successful readlink
        let _ = self.update_atime(ino as i64).await;

        Ok(ReplyData {
            data: match reply_guard {
                Some(guard) => Bytes::from_owner(MetadataReplyOwner {
                    data: target,
                    _guard: guard,
                }),
                None => Bytes::from(target),
            },
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
        #[cfg(test)]
        let diagnostic = |stage: &str, error: Option<Errno>| {
            if data.len() != 57 {
                return;
            }
            let errno = error.map_or(0, |error| -i32::from(error));
            eprintln!(
                "[packed-v3-write-diag] stage={stage} unique={} ino={ino} fh={fh} offset={offset} len={} write_flags={write_flags} flags={_flags} errno={errno}",
                req.unique,
                data.len(),
            );
        };
        #[cfg(test)]
        diagnostic("fuse-entry", None);
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
        let _write_guard = if !data.is_empty() {
            Some(self.begin_fuse_write(ino as i64, req.unique))
        } else {
            None
        };
        #[cfg(test)]
        diagnostic("fuse-write-order-ready", None);
        if fh == 0 && !data.is_empty() {
            #[cfg(test)]
            diagnostic("fuse-stateless-access-before", None);
            self.ensure_access_allowed(
                ino as i64,
                req.uid,
                req.gid,
                req.pid,
                inode_mutation_access_mask(),
            )
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                diagnostic("fuse-stateless-access-error", Some(*_error));
            })?;
            #[cfg(test)]
            diagnostic("fuse-stateless-access-after", None);
        }
        let n = if write_flags & FUSE_WRITE_CACHE != 0 {
            #[cfg(test)]
            diagnostic("fuse-branch-cache", None);
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
                .map_err(Into::<Errno>::into)
                .inspect_err(|_error| {
                    #[cfg(test)]
                    diagnostic("fuse-cache-error", Some(*_error));
                })?;
            if fh != 0 && written > 0 {
                self.mark_handle_write_dirty(fh);
            }
            written as u32
        } else if fh != 0 {
            #[cfg(test)]
            diagnostic("fuse-branch-handle", None);
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
                .map_err(Into::<Errno>::into)
                .inspect_err(|_error| {
                    #[cfg(test)]
                    diagnostic("fuse-handle-error", Some(*_error));
                })? as u32
        } else {
            #[cfg(test)]
            diagnostic("fuse-branch-stateless", None);
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
                .map_err(Into::<Errno>::into)
                .inspect_err(|_error| {
                    #[cfg(test)]
                    diagnostic("fuse-stateless-error", Some(*_error));
                })? as u32
        };
        #[cfg(test)]
        diagnostic("fuse-data-written", None);
        if n > 0 {
            #[cfg(test)]
            diagnostic("fuse-privilege-before", None);
            self.clear_write_privilege_bits_if_needed(ino as i64, req.uid)
                .await
                .map_err(Into::<Errno>::into)
                .inspect_err(|_error| {
                    #[cfg(test)]
                    diagnostic("fuse-privilege-error", Some(*_error));
                })?;
            #[cfg(test)]
            diagnostic("fuse-privilege-after", None);
        }
        self.stats()
            .fuse_write_bytes
            .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        #[cfg(test)]
        {
            drop(_write_guard);
            diagnostic("fuse-return-ok", None);
        }
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
            let snapshot = match fh {
                Some(fh) => self
                    .virtual_stats_snapshot(fh)
                    .ok_or(Errno::from(libc::EBADF))?,
                None => self.create_virtual_stats_snapshot(false)?,
            };
            let now: Timestamp = std::time::SystemTime::now().into();
            let attr = asyncfuse::raw::reply::FileAttr {
                ino: STATS_INODE,
                size: snapshot.len() as u64,
                blocks: (snapshot.len() as u64).div_ceil(512),
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
                ttl: Duration::ZERO,
                attr,
            });
        }

        debug!(unique = req.unique, ino, fh = ?fh, "fuse.getattr");
        // Only genuine absence permits the attributes of an unlinked handle.
        // Authentication or backend failures must not become successful stats.
        let vattr_opt = self
            .stat_ino_checked(ino as i64)
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                eprintln!(
                    "[packed-v3-native-kernel-read-diag] stage=fuse-getattr unique={} inode={ino} fh={fh:?} error={_error:?}",
                    req.unique,
                );
            })
            .map_err(Errno::from)?;
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
            ttl: self.permission_cache_ttl(),
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
                ttl: self.permission_cache_ttl(),
                attr,
            });
        }
        if fh.is_none() {
            self.ensure_inode_paths_search_allowed(ino as i64, req.uid, req.gid, req.pid)
                .await?;
        }
        let write_handle_allows_truncate = fh
            .map(|fh| self.handle_allows_write_for_inode(fh, ino as i64))
            .unwrap_or(false);
        if !(setattr_is_truncate_with_optional_timestamps(&meta_req, &meta_flags)
            && write_handle_allows_truncate)
        {
            if setattr_is_timestamp_only(&meta_req, &meta_flags) {
                self.ensure_timestamp_setattr_allowed(
                    ino as i64,
                    req.uid,
                    req.gid,
                    req.pid,
                    &meta_req,
                    &meta_flags,
                )
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
                if clear_suid_sgid
                    && self.meta_layer().posix_acl_capability()
                        != crate::meta::layer::PosixAclCapability::ReadWrite
                {
                    meta_flags.insert(SetAttrFlags::CLEAR_SUID | SetAttrFlags::CLEAR_SGID);
                }
            } else {
                self.ensure_access_allowed(
                    ino as i64,
                    req.uid,
                    req.gid,
                    req.pid,
                    inode_mutation_access_mask(),
                )
                .await?;
            }
        }

        // Apply the attribute changes
        let writable_acl = self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite;
        let groups = if writable_acl && req.uid != 0 {
            verified_request_group_ids(req.pid, req.uid, req.gid)?
        } else {
            Vec::new()
        };
        let changed = if writable_acl {
            crate::meta::layer::scope_setattr_write_handle(
                ino as i64,
                write_handle_allows_truncate
                    && setattr_is_truncate_with_optional_timestamps(&meta_req, &meta_flags),
                self.set_attr_from_fuse_as(ino as i64, &meta_req, meta_flags, req.uid, &groups),
            )
            .await
        } else {
            self.set_attr_from_fuse(ino as i64, &meta_req, meta_flags)
                .await
        };
        let vattr = match changed {
            Ok(vattr) => {
                debug!(
                    unique = req.unique,
                    ino,
                    size = ?meta_req.size,
                    mode = vattr.mode,
                    uid = vattr.uid,
                    gid = vattr.gid,
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
            self.permission_cache_ttl()
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
        // A packed page has at most 256 children. Admit its independent FUSE
        // entry/name copies before allocation and retain both owners in the
        // returned stream, including when a consumer stops before EOF.
        let reply_guard = Filesystem::reserve_reply_memory(self, 256 * 1024)?;
        // Rewinddir: offset ≤ 0 means restart from the beginning.
        // Replace the cached handle with a fresh snapshot from the meta
        // layer so that entries created after opendir(3) are visible.
        if fh != 0
            && offset == 0
            && let Err(err) = self.refresh_dir_handle(fh).await
        {
            warn!(ino, fh, error = %err, "fuse.readdir handle refresh failed");
        }

        // Try to use handle first. FUSE directory offsets identify the next
        // entry to read: 0=start, 1=after ".", 2=after "..", and 3+=child index+1.
        let (entries, entries_offset, include_dot_entries, include_dotdot_only) = if fh != 0 {
            match offset {
                i64::MIN..=0 => (
                    self.readdir_page_raw_owned(fh, 0)
                        .await
                        .map_err(Errno::from)?,
                    0,
                    true,
                    false,
                ),
                1 => (
                    self.readdir_page_raw_owned(fh, 0)
                        .await
                        .map_err(Errno::from)?,
                    0,
                    false,
                    true,
                ),
                _ => {
                    let entries_offset = (offset as u64).saturating_sub(2);
                    (
                        self.readdir_page_raw_owned(fh, entries_offset)
                            .await
                            .map_err(Errno::from)?,
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
            let meta_entries: Option<Vec<crate::vfs::handles::RawDirEntry>> =
                self.readdir_ino(ino as i64).await.map(|entries| {
                    entries
                        .into_iter()
                        .map(|entry| RawDirEntry {
                            name: entry.name.into_bytes(),
                            ino: entry.ino,
                            kind: entry.kind,
                        })
                        .collect()
                });
            match meta_entries {
                Some(v) => crate::vfs::handles::OwnedDirectoryPage::from(v),
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
                name: os_string_from_raw_name(&e.name),
                offset: (entries_offset + i as u64 + 3) as i64,
            });
        }

        let guards = (entries.guard, reply_guard);
        let stream_iter = stream::iter(all.into_iter().map(move |entry| {
            let _retained = &guards;
            Ok(entry)
        }));
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
        let ttl = self.permission_cache_ttl();
        let reply_guard = Filesystem::reserve_reply_memory(self, 256 * 1024)?;
        let mut all: Vec<DirectoryEntryPlus> = Vec::new();

        // Rewinddir: same logic as readdir().
        if fh != 0 && offset == 0 {
            let _ = self.refresh_dir_handle(fh).await;
        }

        // Try to use handle first with the same offset mapping as readdir().
        let (entries_from_handle, entries_offset, include_dot_entries, include_dotdot_only) =
            if fh != 0 {
                match offset {
                    0 => (
                        self.readdir_page_raw_owned(fh, 0)
                            .await
                            .map_err(Errno::from)?,
                        0,
                        true,
                        false,
                    ),
                    1 => (
                        self.readdir_page_raw_owned(fh, 0)
                            .await
                            .map_err(Errno::from)?,
                        0,
                        false,
                        true,
                    ),
                    _ => {
                        let entries_offset = offset.saturating_sub(2);
                        (
                            self.readdir_page_raw_owned(fh, entries_offset)
                                .await
                                .map_err(Errno::from)?,
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
            let meta_entries: Option<Vec<crate::vfs::handles::RawDirEntry>> =
                self.readdir_ino(ino as i64).await.map(|entries| {
                    entries
                        .into_iter()
                        .map(|entry| RawDirEntry {
                            name: entry.name.into_bytes(),
                            ino: entry.ino,
                            kind: entry.kind,
                        })
                        .collect()
                });
            match meta_entries {
                Some(v) => crate::vfs::handles::OwnedDirectoryPage::from(v),
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
                name: os_string_from_raw_name(&e.name),
                offset: (entries_offset + i as u64 + 3) as i64,
                attr: fattr,
                entry_ttl: ttl,
                attr_ttl: ttl,
            });
        }

        let guards = (entries.guard, reply_guard);
        let stream_iter = stream::iter(all.into_iter().map(move |entry| {
            let _retained = &guards;
            Ok(entry)
        }));
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

    // Create a regular or special file node while preserving its kind and rdev.
    #[allow(clippy::unnecessary_cast)] // mode_t is narrower on some supported platforms.
    async fn mknod(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> FuseResult<ReplyEntry> {
        self.mknod_with_umask(req, parent, name, mode, 0, rdev)
            .await
    }

    async fn mknod_with_umask(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
    ) -> FuseResult<ReplyEntry> {
        let actor = if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
            && crate::meta::layer::namespace_actor().is_none()
        {
            let groups = if req.uid == 0 {
                Vec::new()
            } else {
                verified_request_group_ids(req.pid, req.uid, req.gid)?
            };
            Some(crate::meta::layer::NamespaceActor {
                uid: req.uid,
                gid: req.gid,
                groups,
            })
        } else {
            None
        };
        crate::meta::layer::scope_namespace_actor(
            actor,
            self.mknod_with_umask_with_actor(req, parent, name, mode, umask, rdev),
        )
        .await
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
        let actor = if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
            && crate::meta::layer::namespace_actor().is_none()
        {
            let groups = if req.uid == 0 {
                Vec::new()
            } else {
                verified_request_group_ids(req.pid, req.uid, req.gid)?
            };
            Some(crate::meta::layer::NamespaceActor {
                uid: req.uid,
                gid: req.gid,
                groups,
            })
        } else {
            None
        };
        crate::meta::layer::scope_namespace_actor(
            actor,
            self.mkdir_with_actor(req, parent, name, mode, umask),
        )
        .await
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
        self.create_with_umask(req, parent, name, mode, 0, flags)
            .await
    }

    async fn create_with_umask(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: u32,
    ) -> FuseResult<ReplyCreated> {
        let actor = if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
            && crate::meta::layer::namespace_actor().is_none()
        {
            let groups = if req.uid == 0 {
                Vec::new()
            } else {
                verified_request_group_ids(req.pid, req.uid, req.gid)?
            };
            Some(crate::meta::layer::NamespaceActor {
                uid: req.uid,
                gid: req.gid,
                groups,
            })
        } else {
            None
        };
        crate::meta::layer::scope_open_actor(
            actor,
            open_flags_access_mask(flags),
            self.create_with_umask_with_actor(req, parent, name, mode, umask, flags),
        )
        .await
    }

    async fn link(
        &self,
        req: Request,
        ino: u64,
        new_parent: u64,
        new_name: &OsStr,
    ) -> FuseResult<ReplyEntry> {
        let actor = if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
            && crate::meta::layer::namespace_actor().is_none()
        {
            let groups = if req.uid == 0 {
                Vec::new()
            } else {
                verified_request_group_ids(req.pid, req.uid, req.gid)?
            };
            Some(crate::meta::layer::NamespaceActor {
                uid: req.uid,
                gid: req.gid,
                groups,
            })
        } else {
            None
        };
        crate::meta::layer::scope_namespace_actor(
            actor,
            self.link_with_actor(req, ino, new_parent, new_name),
        )
        .await
    }

    async fn symlink(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        link: &OsStr,
    ) -> FuseResult<ReplyEntry> {
        let actor = if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
            && crate::meta::layer::namespace_actor().is_none()
        {
            let groups = if req.uid == 0 {
                Vec::new()
            } else {
                verified_request_group_ids(req.pid, req.uid, req.gid)?
            };
            Some(crate::meta::layer::NamespaceActor {
                uid: req.uid,
                gid: req.gid,
                groups,
            })
        } else {
            None
        };
        crate::meta::layer::scope_namespace_actor(
            actor,
            self.symlink_with_actor(req, parent, name, link),
        )
        .await
    }

    // Remove a file
    async fn unlink(&self, req: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        let actor = if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
            && crate::meta::layer::namespace_actor().is_none()
        {
            let groups = if req.uid == 0 {
                Vec::new()
            } else {
                verified_request_group_ids(req.pid, req.uid, req.gid)?
            };
            Some(crate::meta::layer::NamespaceActor {
                uid: req.uid,
                gid: req.gid,
                groups,
            })
        } else {
            None
        };
        crate::meta::layer::scope_namespace_actor(actor, self.unlink_with_actor(req, parent, name))
            .await
    }

    // Remove an empty directory
    async fn rmdir(&self, req: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        let actor = if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
            && crate::meta::layer::namespace_actor().is_none()
        {
            let groups = if req.uid == 0 {
                Vec::new()
            } else {
                verified_request_group_ids(req.pid, req.uid, req.gid)?
            };
            Some(crate::meta::layer::NamespaceActor {
                uid: req.uid,
                gid: req.gid,
                groups,
            })
        } else {
            None
        };
        crate::meta::layer::scope_namespace_actor(actor, self.rmdir_with_actor(req, parent, name))
            .await
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
        let actor = if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
            && crate::meta::layer::namespace_actor().is_none()
        {
            let groups = if req.uid == 0 {
                Vec::new()
            } else {
                verified_request_group_ids(req.pid, req.uid, req.gid)?
            };
            Some(crate::meta::layer::NamespaceActor {
                uid: req.uid,
                gid: req.gid,
                groups,
            })
        } else {
            None
        };
        crate::meta::layer::scope_namespace_actor(
            actor,
            self.rename_with_actor(req, parent, name, new_parent, new_name),
        )
        .await
    }

    // ===== Resource release & sync: stateless implementation, return success =====
    // Close file handle
    async fn release(
        &self,
        req: Request,
        inode: u64,
        fh: u64,
        _flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> FuseResult<()> {
        let kind = if inode == STATS_INODE {
            crate::vfs::fuse_read_cancel::ClientKind::Stats
        } else {
            crate::vfs::fuse_read_cancel::ClientKind::File
        };
        let client = self.begin_fuse_client_release(inode, fh, kind)?;
        if inode == STATS_INODE {
            if client.is_some() && self.virtual_stats_snapshot(fh).is_none() {
                if let Some(client) = client {
                    client.fail(Errno::from(libc::EBADF));
                }
                return Err(libc::EBADF.into());
            }
            self.release_virtual_stats(fh);
            if let Some(client) = client {
                client.finish()?;
            }
            return Ok(());
        }
        debug!(fh, "fuse.release");
        self.unlock_owner_locks(inode, lock_owner).await;
        self.unlock_handle_locks(inode, fh).await;
        if fh == 0 {
            // A stateless FUSE open has no VFS handle to release.  The
            // kernel may request the final flush here instead of sending a
            // separate FLUSH operation.
            if flush {
                self.wait_for_prior_fuse_writes(inode as i64, req.unique)
                    .await;
                self.flush_inode_required(inode)
                    .await
                    .map_err(Errno::from)?;
            }
            return Ok(());
        }
        if let Err(error) = self.close(fh).await.map_err(Errno::from) {
            if let Some(client) = client {
                client.fail(error);
            }
            return Err(error);
        }
        if let Some(client) = client {
            client.finish()?;
        }
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
        self.wait_for_prior_fuse_writes(inode as i64, req.unique)
            .await;
        self.unlock_owner_locks(inode, lock_owner).await;
        if fh == 0 {
            // asyncfuse can issue a stateless flush for a read-only open. There
            // is no per-handle dirty state to snapshot in that case, so flush
            // any inode-level writeback and avoid treating the absent handle
            // as ESTALE when the caller closes the file.
            self.flush_inode_required(inode)
                .await
                .map_err(Errno::from)?;
            return Ok(());
        }
        self.flush_dirty_handle_snapshot(fh)
            .await
            .map_err(Errno::from)?;
        Ok(())
    }

    // Sync file content to backend
    async fn fsync(&self, req: Request, inode: u64, fh: u64, datasync: bool) -> FuseResult<()> {
        debug!(fh, datasync, "fuse.fsync");
        self.wait_for_prior_fuse_writes(inode as i64, req.unique)
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
        #[cfg(feature = "workspace-overlay")]
        let workspace_hole_mode = {
            let keep_size = libc::FALLOC_FL_KEEP_SIZE as u32;
            let punch_hole = libc::FALLOC_FL_PUNCH_HOLE as u32;
            let zero_range = libc::FALLOC_FL_ZERO_RANGE as u32;
            mode == (keep_size | punch_hole)
                || mode == zero_range
                || mode == (keep_size | zero_range)
        };
        #[cfg(not(feature = "workspace-overlay"))]
        let workspace_hole_mode = false;
        if mode != 0 && !workspace_hole_mode {
            return Err(libc::EOPNOTSUPP.into());
        }
        if length > 0 {
            self.ensure_access_allowed(
                inode as i64,
                req.uid,
                req.gid,
                req.pid,
                inode_mutation_access_mask(),
            )
            .await?;
            self.wait_for_prior_fuse_writes(inode as i64, req.unique)
                .await;
        }
        #[cfg(feature = "workspace-overlay")]
        if workspace_hole_mode {
            let keep_size = mode & libc::FALLOC_FL_KEEP_SIZE as u32 != 0;
            return self
                .workspace_hole_fallocate_from_fuse(fh, inode as i64, offset, length, keep_size)
                .await
                .map_err(Errno::from);
        }
        if fh != 0 {
            // The FALLOCATE reply already updates the initiating kernel's
            // inode state. A concurrent invalidation can race buffered writes.
            self.fallocate_handle_from_fuse(fh, inode as i64, offset, length)
                .await
                .map_err(Errno::from)
        } else {
            self.fallocate_ino_from_fuse(inode as i64, offset, length)
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
        fh: u64,
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
                // FS_IOC_FSGETXATTR = _IOR('X', 31, struct fsxattr).
                // xfs_io labels this probe FS_IOC_GETXATTR in diagnostics.
                0x801c_581f => Ok(Self::ioctl_fsgetxattr_reply()),
                x if x == libc::FICLONE as u32 => {
                    let src_fd = i32::try_from(arg).map_err(|_| Errno::from(libc::EBADF))?;
                    self.ioctl_clone(req, inode, fh, i64::from(src_fd), None)
                        .await
                }
                x if x == libc::FICLONERANGE as u32 => {
                    let range = Self::parse_clone_range(in_data)
                        .ok_or_else(|| Errno::from(libc::EINVAL))?;
                    self.ioctl_clone(
                        req,
                        inode,
                        fh,
                        range.src_fd,
                        Some((range.src_offset, range.dest_offset, range.src_length)),
                    )
                    .await
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
        req: Request,
        inode: u64,
        name: &OsStr,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> FuseResult<()> {
        if position != 0 {
            return Err(libc::EINVAL.into());
        }
        if self
            .stat_ino_checked(inode as i64)
            .await
            .map_err(Errno::from)?
            .is_none()
        {
            return Err(libc::ENOENT.into());
        }
        let name = raw_os_str_bytes(name);
        // Mutable backends cannot opt in until create/chmod synchronize ACLs
        // atomically. Packed snapshots expose their frozen ACLs as read-only.
        if is_posix_acl_xattr(name) {
            match self.meta_layer().posix_acl_capability() {
                crate::meta::layer::PosixAclCapability::ReadOnly => return Err(libc::EROFS.into()),
                crate::meta::layer::PosixAclCapability::Unsupported => {
                    return Err(libc::EOPNOTSUPP.into());
                }
                crate::meta::layer::PosixAclCapability::ReadWrite => {}
            }
            if flags & !(libc::XATTR_CREATE as u32 | libc::XATTR_REPLACE as u32) != 0 {
                return Err(libc::EINVAL.into());
            }
            let groups = if req.uid == 0 {
                Vec::new()
            } else {
                verified_request_group_ids(req.pid, req.uid, req.gid)?
            };
            let name = std::str::from_utf8(name).map_err(|_| Errno::from(libc::EINVAL))?;
            return self
                .update_posix_acl_ino(inode as i64, name, Some(value), flags, req.uid, &groups)
                .await
                .map_err(posix_acl_errno);
        }
        // Control-plane xattrs such as `system.brewfs.acl` drive permission
        // decisions, so untrusted clients must never be able to write (and
        // thereby overwrite) them.
        if !is_user_xattr_name(name) && !(req.uid == 0 && is_trusted_xattr_name(name)) {
            return Err(libc::EPERM.into());
        }
        self.set_xattr_bytes_ino(inode as i64, name, value, flags)
            .await
            .map_err(|e| match e {
                VfsError::AlreadyExists { .. } => Errno::from(libc::EEXIST),
                VfsError::ReadOnlyFilesystem { .. } => Errno::from(libc::EROFS),
                VfsError::InvalidFilename | VfsError::InvalidInput => Errno::from(libc::EINVAL),
                VfsError::Unsupported => Errno::from(libc::ENOSYS),
                VfsError::NotFound { .. } => Errno::from(libc::ENODATA),
                _ => Errno::from(libc::EIO),
            })
    }

    async fn getxattr(
        &self,
        req: Request,
        inode: u64,
        name: &OsStr,
        size: u32,
    ) -> FuseResult<ReplyXAttr> {
        let reply_guard = Filesystem::reserve_reply_memory(self, u64::from(size.max(256 * 1024)))?;
        if self
            .stat_ino_checked(inode as i64)
            .await
            .map_err(Errno::from)?
            .is_none()
        {
            return Err(libc::ENOENT.into());
        }
        let name = raw_os_str_bytes(name);
        if is_posix_acl_xattr(name)
            && self.meta_layer().posix_acl_capability()
                == crate::meta::layer::PosixAclCapability::Unsupported
        {
            return Err(libc::EOPNOTSUPP.into());
        }
        if is_internal_xattr(name) && !is_posix_acl_xattr(name) {
            // Internal control-plane xattrs are hidden from FUSE clients.
            return Err(libc::ENODATA.into());
        }
        if !can_access_trusted_xattr(req.uid, name) {
            return Err(libc::EPERM.into());
        }
        let value = self
            .get_xattr_bytes_ino(inode as i64, name)
            .await
            .map_err(|e| match e {
                VfsError::Unsupported => Errno::from(libc::ENOSYS),
                VfsError::InvalidFilename | VfsError::InvalidInput => Errno::from(libc::EINVAL),
                _ => Errno::from(libc::EIO),
            })?
            .ok_or_else(|| Errno::from(libc::ENODATA))?;
        if is_posix_acl_xattr(name) {
            crate::meta::posix_acl::PosixAcl::decode(&value).map_err(|_| Errno::from(libc::EIO))?;
        }
        if size == 0 {
            return Ok(ReplyXAttr::Size(value.len() as u32));
        }
        if (size as usize) < value.len() {
            return Err(libc::ERANGE.into());
        }
        Ok(ReplyXAttr::Data(match reply_guard {
            Some(guard) => Bytes::from_owner(MetadataReplyOwner {
                data: value,
                _guard: guard,
            }),
            None => Bytes::from(value),
        }))
    }

    async fn listxattr(&self, req: Request, inode: u64, size: u32) -> FuseResult<ReplyXAttr> {
        let reply_guard = Filesystem::reserve_reply_memory(self, u64::from(size.max(256 * 1024)))?;
        if self
            .stat_ino_checked(inode as i64)
            .await
            .map_err(Errno::from)?
            .is_none()
        {
            return Err(libc::ENOENT.into());
        }
        let names = self
            .list_xattr_bytes_owned_ino(inode as i64)
            .await
            .map_err(|e| match e {
                VfsError::Unsupported => Errno::from(libc::ENOSYS),
                error => Errno::from(error),
            })?;
        let visible = |name: &&Vec<u8>| {
            // Internal control metadata stays hidden; supported Linux ACLs
            // remain visible to the kernel and xattr clients.
            (!is_internal_xattr(name)
                || (is_posix_acl_xattr(name)
                    && self.meta_layer().posix_acl_capability()
                        != crate::meta::layer::PosixAclCapability::Unsupported))
                && can_access_trusted_xattr(req.uid, name)
        };
        let total_len: usize = names
            .names
            .iter()
            .filter(visible)
            .map(|n| n.len() + 1)
            .sum();
        if total_len > 65536 {
            return Err(libc::E2BIG.into());
        }
        if size == 0 {
            return Ok(ReplyXAttr::Size(total_len as u32));
        }
        if (size as usize) < total_len {
            return Err(libc::ERANGE.into());
        }
        let mut data = Vec::with_capacity(total_len);
        for name in names.names.iter().filter(visible) {
            data.extend_from_slice(name);
            data.push(0);
        }
        let guards = (names.guard, reply_guard);
        Ok(ReplyXAttr::Data(
            if guards.0.is_some() || guards.1.is_some() {
                Bytes::from_owner(MetadataReplyOwner {
                    data,
                    _guard: std::sync::Arc::new(guards),
                })
            } else {
                Bytes::from(data)
            },
        ))
    }

    async fn removexattr(&self, req: Request, inode: u64, name: &OsStr) -> FuseResult<()> {
        if self
            .stat_ino_checked(inode as i64)
            .await
            .map_err(Errno::from)?
            .is_none()
        {
            return Err(libc::ENOENT.into());
        }
        let name = raw_os_str_bytes(name);
        if is_posix_acl_xattr(name) {
            match self.meta_layer().posix_acl_capability() {
                crate::meta::layer::PosixAclCapability::ReadOnly => return Err(libc::EROFS.into()),
                crate::meta::layer::PosixAclCapability::Unsupported => {
                    return Err(libc::EOPNOTSUPP.into());
                }
                crate::meta::layer::PosixAclCapability::ReadWrite => {}
            }
            let groups = if req.uid == 0 {
                Vec::new()
            } else {
                verified_request_group_ids(req.pid, req.uid, req.gid)?
            };
            let name = std::str::from_utf8(name).map_err(|_| Errno::from(libc::EINVAL))?;
            return self
                // Removing an xattr has replace semantics: an absent ACL must
                // return ENODATA instead of silently succeeding.
                .update_posix_acl_ino(
                    inode as i64,
                    name,
                    None,
                    libc::XATTR_REPLACE as u32,
                    req.uid,
                    &groups,
                )
                .await
                .map_err(posix_acl_errno);
        }
        if !is_user_xattr_name(name) && !(req.uid == 0 && is_trusted_xattr_name(name)) {
            return Err(libc::EPERM.into());
        }
        self.remove_xattr_bytes_ino(inode as i64, name)
            .await
            .map_err(|e| match e {
                VfsError::Unsupported => libc::ENOSYS.into(),
                VfsError::NotFound { .. } => libc::ENODATA.into(),
                VfsError::ReadOnlyFilesystem { .. } => libc::EROFS.into(),
                VfsError::InvalidFilename | VfsError::InvalidInput => libc::EINVAL.into(),
                _ => libc::EIO.into(),
            })
    }

    // Close directory handle
    async fn releasedir(&self, _req: Request, inode: u64, fh: u64, _flags: u32) -> FuseResult<()> {
        let client = self.begin_fuse_client_release(
            inode,
            fh,
            crate::vfs::fuse_read_cancel::ClientKind::Directory,
        )?;
        debug!(fh, "fuse.releasedir");
        if fh == 0 {
            return Ok(()); // No handle to release
        }

        if let Err(e) = self.closedir(fh) {
            match e {
                VfsError::StaleNetworkFileHandle => {
                    // Handle not found, but that's ok - might be a stateless readdir
                    debug!("releasedir: handle {} not found (stateless mode)", fh);
                    if let Some(client) = client {
                        client.fail(Errno::from(libc::EBADF));
                        return Err(libc::EBADF.into());
                    }
                }
                _ => {
                    error!("Error releasing directory handle {}: {:?}", fh, e);
                    let error = Errno::from(e);
                    if let Some(client) = client {
                        client.fail(error);
                    }
                    return Err(error);
                }
            }
        }
        if let Some(client) = client {
            client.finish()?;
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

    async fn interrupt(&self, req: Request, unique: u64) -> FuseResult<()> {
        if !Filesystem::supports_read_cancellation(self) {
            return Ok(());
        }
        tracing::info!(target: "asyncfuse::request_cancel", event="fuse_interrupt_received",
            unique=req.unique, target_unique=unique);
        // Unknown/finished/queued reads return EAGAIN so the kernel may retry.
        self.interrupt_fuse_read(unique)
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
        self.ensure_access_allowed(ino as i64, req.uid, req.gid, req.pid, mask)
            .await
    }
}

// =============== helpers ===============
impl<S, M> VFS<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    async fn open_client_untracked(
        &self,
        req: Request,
        ino: u64,
        flags: u32,
    ) -> FuseResult<ReplyOpen> {
        let actor = if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
        {
            let groups = if req.uid == 0 {
                Vec::new()
            } else {
                verified_request_group_ids(req.pid, req.uid, req.gid)?
            };
            Some(crate::meta::layer::NamespaceActor {
                uid: req.uid,
                gid: req.gid,
                groups,
            })
        } else {
            None
        };
        crate::meta::layer::scope_open_actor(
            actor,
            open_flags_access_mask(flags),
            self.open_with_actor(req, ino, flags),
        )
        .await
        .inspect_err(|_error| {
            #[cfg(test)]
            eprintln!(
                "[packed-v3-native-kernel-read-diag] stage=fuse-open uid={} gid={} inode={ino} flags={flags} error={_error}",
                req.uid, req.gid,
            );
        })
    }

    async fn opendir_client_untracked(
        &self,
        req: Request,
        ino: u64,
        _flags: u32,
    ) -> FuseResult<ReplyOpen> {
        debug!(ino, "fuse.opendir");
        let Some(attr) = self
            .stat_ino_checked(ino as i64)
            .await
            .map_err(Errno::from)?
        else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(attr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }
        self.ensure_access_allowed(ino as i64, req.uid, req.gid, req.pid, opendir_access_mask())
            .await?;

        // Create directory handle for efficient readdir operations
        let fh = match self.opendir(ino as i64).await {
            Ok(fh) => fh,
            Err(err) => {
                warn!(ino, error = %err, "fuse.opendir failed");
                return Err(Errno::from(err));
            }
        };

        Ok(ReplyOpen { fh, flags: 0 })
    }

    async fn finish_client_open(
        &self,
        pending: Option<crate::vfs::fuse_read_cancel::PendingClientOpen<'_>>,
        kind: crate::vfs::fuse_read_cancel::ClientKind,
        opened: FuseResult<ReplyOpen>,
    ) -> FuseResult<ReplyOpen> {
        let Some(mut pending) = pending else {
            return opened;
        };
        let reply = match opened {
            Ok(reply) => reply,
            Err(error) => {
                pending.finish_without_handle()?;
                return Err(error);
            }
        };
        let refusal = match pending.commit(reply.fh) {
            Ok(true) => return Ok(reply),
            Ok(false) => Errno::from(libc::ENODEV),
            Err(error) => error,
        };
        // Keep the original pending node/Roots charge while actual rollback
        // runs. Dropping/erroring rollback poisons the fence and retains it.
        let rollback = match kind {
            crate::vfs::fuse_read_cancel::ClientKind::File => {
                self.close(reply.fh).await.map_err(Errno::from)
            }
            crate::vfs::fuse_read_cancel::ClientKind::Directory => {
                self.closedir(reply.fh).map_err(Errno::from)
            }
            crate::vfs::fuse_read_cancel::ClientKind::Stats => {
                self.release_virtual_stats(reply.fh);
                Ok(())
            }
        };
        if let Err(error) = rollback {
            pending.fail(error);
            return Err(error);
        }
        pending.finish_rollback()?;
        Err(refusal)
    }

    async fn open_with_actor(&self, req: Request, ino: u64, flags: u32) -> FuseResult<ReplyOpen> {
        // One immutable, admitted snapshot per virtual stats handle.
        if ino == STATS_INODE {
            let accmode = flags & (libc::O_ACCMODE as u32);
            if accmode != (libc::O_RDONLY as u32) {
                return Err(libc::EACCES.into());
            }
            return Ok(ReplyOpen {
                fh: self.open_virtual_stats()?,
                flags: FOPEN_DIRECT_IO,
            });
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
            self.ensure_inode_paths_search_allowed(ino as i64, req.uid, req.gid, req.pid)
                .await?;
            self.ensure_access_allowed(
                ino as i64,
                req.uid,
                req.gid,
                req.pid,
                open_flags_access_mask(flags),
            )
            .await?;
        }
        if truncate {
            if !write {
                return Err(libc::EINVAL.into());
            }
            let request = SetAttrRequest {
                size: Some(0),
                ..Default::default()
            };
            if self.meta_layer().posix_acl_capability()
                == crate::meta::layer::PosixAclCapability::ReadWrite
            {
                let groups = crate::meta::layer::namespace_actor()
                    .map_or_else(Vec::new, |actor| actor.groups);
                self.set_attr_from_fuse_as(
                    ino as i64,
                    &request,
                    SetAttrFlags::empty(),
                    req.uid,
                    &groups,
                )
                .await
            } else {
                self.set_attr_from_fuse(ino as i64, &request, SetAttrFlags::empty())
                    .await
            }
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

    async fn ensure_access_allowed(
        &self,
        ino: i64,
        uid: u32,
        gid: u32,
        pid: u32,
        mask: u32,
    ) -> FuseResult<()> {
        // Writable-ACL layers answer existence, attributes and ACLs from one
        // atomic permission snapshot; a separate stat would be a redundant
        // round trip whose attributes the decision must not use anyway.
        if mask != 0 && self.meta_layer().permission_snapshot_answers_access() {
            let permissions = self
                .meta_layer()
                .inode_permissions(ino)
                .await
                .map_err(posix_acl_errno)?
                .ok_or_else(|| Errno::from(libc::ENOENT))?;
            let mode = self.access_mode_for_permissions(&permissions, uid, gid, pid, mask & 7)?;
            return check_requested_access(mask, mode);
        }
        let Some(attr) = self.stat_ino_checked(ino).await.map_err(Errno::from)? else {
            return Err(libc::ENOENT.into());
        };

        if mask == 0 {
            return Ok(());
        }

        // Root can access everything (except execute on non-executable files)
        if uid == 0
            && self.meta_layer().posix_acl_capability()
                == crate::meta::layer::PosixAclCapability::Unsupported
        {
            // Root still needs execute permission to be set somewhere
            if (mask & libc::X_OK as u32) != 0
                && !matches!(attr.kind, VfsFileType::Dir)
                && (attr.mode & 0o111) == 0
            {
                return Err(libc::EACCES.into());
            }
            return Ok(());
        }

        let mode = self
            .access_mode_for_attr(ino, &attr, uid, gid, pid, mask & 7)
            .await?;

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
        pid: u32,
    ) -> FuseResult<()> {
        if uid == 0 {
            return Ok(());
        }

        if ino == self.root_ino() {
            return Ok(());
        }

        let paths = self.paths_of_bytes_owned(ino).await.map_err(Errno::from)?;
        if paths.paths.is_empty() {
            return Err(libc::ENOENT.into());
        }

        // Borrow the paths so their memory and reader owner survive every
        // awaited ancestor lookup, including denied/error and early success.
        for path in &paths.paths {
            if self
                .path_ancestors_search_allowed(path, uid, gid, pid)
                .await?
            {
                return Ok(());
            }
        }

        Err(libc::EACCES.into())
    }

    async fn path_ancestors_search_allowed(
        &self,
        path: &[u8],
        uid: u32,
        gid: u32,
        pid: u32,
    ) -> FuseResult<bool> {
        let components: Vec<&[u8]> = path
            .split(|byte| *byte == b'/')
            .filter(|component| !component.is_empty())
            .collect();
        if components.is_empty() {
            return Ok(true);
        }

        let mut dir = self.root_ino();
        if !self.directory_search_allowed(dir, uid, gid, pid).await? {
            return Ok(false);
        }

        for component in components.iter().take(components.len().saturating_sub(1)) {
            // Preserve lookup errors for both UTF-8 and raw ancestor names.
            let next = match std::str::from_utf8(component) {
                Ok(name) => self
                    .child_of_checked(dir, name)
                    .await
                    .map_err(Errno::from)?,
                Err(_) => self
                    .child_attr_of_bytes(dir, component)
                    .await
                    .map_err(Errno::from)?
                    .map(|(inode, _)| inode),
            };
            let Some(next) = next else {
                return Err(libc::ENOENT.into());
            };
            dir = next;
            if !self.directory_search_allowed(dir, uid, gid, pid).await? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn directory_search_allowed(
        &self,
        ino: i64,
        uid: u32,
        gid: u32,
        pid: u32,
    ) -> FuseResult<bool> {
        match self
            .ensure_access_allowed(ino, uid, gid, pid, libc::X_OK as u32)
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
        let Some(attr) = self.stat_ino_checked(ino).await.map_err(Errno::from)? else {
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
        let Some(attr) = self.stat_ino_checked(ino).await.map_err(Errno::from)? else {
            return Err(libc::ENOENT.into());
        };

        if uid != 0
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
        pid: u32,
        req: &SetAttrRequest,
        flags: &SetAttrFlags,
    ) -> FuseResult<()> {
        let Some(attr) = self.stat_ino_checked(ino).await.map_err(Errno::from)? else {
            return Err(libc::ENOENT.into());
        };

        if uid == 0 || uid == attr.uid {
            return Ok(());
        }

        if timestamp_request_is_ctime_only(req) {
            return Ok(());
        }

        let mode = self
            .access_mode_for_attr(ino, &attr, uid, gid, pid, libc::W_OK as u32)
            .await?;
        if flags.contains(SetAttrFlags::SET_ATIME_NOW | SetAttrFlags::SET_MTIME_NOW) {
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
        let Some(attr) = self.stat_ino_checked(ino).await.map_err(Errno::from)? else {
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

    async fn mknod_with_umask_with_actor(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
    ) -> FuseResult<ReplyEntry> {
        if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
        {
            let name = name.to_str().ok_or_else(|| Errno::from(libc::EINVAL))?;
            validate_fuse_name(name)?;
            self.ensure_directory_parent_namespace_mutation_allowed(
                parent, req.uid, req.gid, req.pid,
            )
            .await?;
            let kind = match mode & libc::S_IFMT {
                0 | libc::S_IFREG => VfsFileType::File,
                libc::S_IFDIR => VfsFileType::Dir,
                libc::S_IFIFO => VfsFileType::Fifo,
                libc::S_IFSOCK => VfsFileType::Socket,
                libc::S_IFCHR => VfsFileType::CharDevice,
                libc::S_IFBLK => VfsFileType::BlockDevice,
                _ => return Err(libc::EINVAL.into()),
            };
            let created = self
                .create_node_at_with_umask(
                    parent as i64,
                    name,
                    kind,
                    mode,
                    umask,
                    req.uid,
                    req.gid,
                    rdev,
                )
                .await
                .map_err(Errno::from)?;
            let attr = created.attr.ok_or_else(|| Errno::from(libc::EIO))?;
            return Ok(ReplyEntry {
                ttl: Duration::ZERO,
                attr: vfs_to_fuse_attr(&attr, &req, self.blocks_for_attr(&attr)),
                generation: 0,
            });
        }
        debug!(
            unique = req.unique,
            parent,
            name = %name.to_string_lossy(),
            mode,
            "fuse.mknod"
        );
        let name = name.to_string_lossy();
        validate_fuse_name(name.as_ref())?;
        const S_IFMT: u32 = libc::S_IFMT;
        const S_IFREG: u32 = libc::S_IFREG;
        const S_IFDIR: u32 = libc::S_IFDIR;
        const S_IFIFO: u32 = libc::S_IFIFO;
        const S_IFSOCK: u32 = libc::S_IFSOCK;
        const S_IFCHR: u32 = libc::S_IFCHR;
        const S_IFBLK: u32 = libc::S_IFBLK;
        let file_type = mode & S_IFMT;
        if !matches!(
            file_type,
            0 | S_IFREG | S_IFDIR | S_IFIFO | S_IFSOCK | S_IFCHR | S_IFBLK
        ) {
            return Err(libc::EINVAL.into());
        }
        let parent_attr = self
            .ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid, req.pid)
            .await?;
        let (creation_gid, creation_mode) =
            Self::creation_attrs_for_parent(&parent_attr, req.gid, mode, file_type == S_IFDIR);

        let (ino, created_attr) = match file_type {
            // Linux accepts mknod(path, 0, 0) as a regular file with mode 000.
            0 | S_IFREG => {
                let result = self
                    .create_file_at_with_attrs(
                        parent as i64,
                        &name,
                        true,
                        creation_mode,
                        req.uid,
                        creation_gid,
                    )
                    .await
                    .map_err(Errno::from)?;
                let attr = self
                    .attr_for_create_result(&result, req.uid, creation_gid, Some(creation_mode))
                    .await;
                (result.ino, attr)
            }
            S_IFDIR => {
                let ino = self
                    .mkdir_at_new(parent as i64, &name)
                    .await
                    .map_err(Errno::from)?;
                (ino, None)
            }
            S_IFIFO | S_IFSOCK | S_IFCHR | S_IFBLK => {
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
                        creation_mode,
                        req.uid,
                        creation_gid,
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
            .apply_new_entry_attrs(ino, req.uid, creation_gid, Some(creation_mode))
            .await
        {
            attr
        } else {
            return Err(libc::ENOENT.into());
        };

        let attr = vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr));
        Ok(ReplyEntry {
            ttl: self.permission_cache_ttl(),
            attr,
            generation: 0,
        })
    }

    async fn mkdir_with_actor(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> FuseResult<ReplyEntry> {
        if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
        {
            let name = name.to_str().ok_or_else(|| Errno::from(libc::EINVAL))?;
            validate_fuse_name(name)?;
            self.ensure_directory_parent_namespace_mutation_allowed(
                parent, req.uid, req.gid, req.pid,
            )
            .await?;
            let created = self
                .create_node_at_with_umask(
                    parent as i64,
                    name,
                    VfsFileType::Dir,
                    mode,
                    umask,
                    req.uid,
                    req.gid,
                    0,
                )
                .await
                .map_err(Errno::from)?;
            let attr = created.attr.ok_or_else(|| Errno::from(libc::EIO))?;
            return Ok(ReplyEntry {
                ttl: Duration::ZERO,
                attr: vfs_to_fuse_attr(&attr, &req, self.blocks_for_attr(&attr)),
                generation: 0,
            });
        }
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
        let parent_attr = self
            .ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid, req.pid)
            .await?;
        // Preserve setuid/setgid/sticky, then apply the caller's umask to rwx bits.
        let masked_mode = apply_creation_umask(mode, umask);
        let (creation_gid, creation_mode) =
            Self::creation_attrs_for_parent(&parent_attr, req.gid, masked_mode, true);
        let _ino = self
            .mkdir_at_new(parent as i64, &name)
            .await
            .map_err(Errno::from)?;
        let Some(vattr) = self
            .apply_new_entry_attrs(_ino, req.uid, creation_gid, Some(creation_mode))
            .await
        else {
            return Err(libc::ENOENT.into());
        };
        let attr = vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr));
        Ok(ReplyEntry {
            ttl: self.permission_cache_ttl(),
            attr,
            generation: 0,
        })
    }

    async fn create_with_umask_with_actor(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
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
        let parent_attr = self
            .ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid, req.pid)
            .await?;
        let writable_acl = self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite;
        let (creation_gid, creation_mode) = if writable_acl {
            (req.gid, mode)
        } else {
            Self::creation_attrs_for_parent(&parent_attr, req.gid, mode, false)
        };
        let create_result = match self
            .create_file_at_with_umask(
                parent as i64,
                &name,
                create_new,
                creation_mode,
                if writable_acl { umask } else { 0 },
                req.uid,
                creation_gid,
            )
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
            .attr_for_create_result(&create_result, req.uid, creation_gid, Some(creation_mode))
            .await
        else {
            return Err(libc::ENOENT.into());
        };
        let vattr = if !create_result.created && (flags & libc::O_TRUNC as u32) != 0 {
            let request = SetAttrRequest {
                size: Some(0),
                ..Default::default()
            };
            if writable_acl {
                let groups = crate::meta::layer::namespace_actor()
                    .map_or_else(Vec::new, |actor| actor.groups);
                self.set_attr_from_fuse_as(
                    create_result.ino,
                    &request,
                    SetAttrFlags::empty(),
                    req.uid,
                    &groups,
                )
                .await
            } else {
                self.set_attr_from_fuse(create_result.ino, &request, SetAttrFlags::empty())
                    .await
            }
            .map_err(Into::<Errno>::into)?
        } else {
            vattr
        };
        let attr = vfs_to_fuse_attr(&vattr, &req, self.blocks_for_attr(&vattr));

        let accmode = flags & (libc::O_ACCMODE as u32);
        let read = accmode != (libc::O_WRONLY as u32);
        let write = accmode != (libc::O_RDONLY as u32);
        let append = (flags & libc::O_APPEND as u32) != 0;
        let fh = crate::meta::layer::scope_created_inode_open(
            create_result.ino,
            writable_acl && create_result.created,
            self.open_with_cached_attr(create_result.ino, vattr.clone(), read, write, append),
        )
        .await
        .map_err(Into::<Errno>::into)?;
        Ok(ReplyCreated {
            ttl: if writable_acl {
                self.permission_cache_ttl()
            } else {
                fuse_create_cache_ttl()
            },
            attr,
            generation: 0,
            fh,
            flags: fuse_open_reply_flags(read, write),
        })
    }

    async fn link_with_actor(
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
        self.ensure_directory_parent_namespace_mutation_allowed(
            new_parent, req.uid, req.gid, req.pid,
        )
        .await?;
        self.ensure_inode_paths_search_allowed(ino as i64, req.uid, req.gid, req.pid)
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
                VfsError::ReadOnlyFilesystem { .. } => Errno::from(libc::EROFS),
                VfsError::InvalidFilename => Errno::from(libc::EINVAL),
                VfsError::FilenameTooLong { .. } => Errno::from(libc::ENAMETOOLONG),
                other => {
                    info!(ino, new_parent, new_name = %new_name_str, error = %other, "fuse.link err");
                    Errno::from(libc::EIO)
                }
            })?;

        let fuse_attr = vfs_to_fuse_attr(&attr, &req, self.blocks_for_attr(&attr));
        Ok(ReplyEntry {
            ttl: self.permission_cache_ttl(),
            attr: fuse_attr,
            generation: 0,
        })
    }

    async fn symlink_with_actor(
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

        let parent_attr = self
            .ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid, req.pid)
            .await?;
        let (creation_gid, _) =
            Self::creation_attrs_for_parent(&parent_attr, req.gid, 0o777, false);

        if self.child_of(parent as i64, name.as_ref()).await.is_some() {
            return Err(libc::EEXIST.into());
        }

        let target = link.to_string_lossy();

        let (ino, vattr) = self
            .create_symlink_at(parent as i64, &name, target.as_ref())
            .await
            .map_err(Errno::from)?;

        let attr = if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
        {
            vattr
        } else {
            self.apply_new_entry_attrs(ino, req.uid, creation_gid, None)
                .await
                .unwrap_or(vattr)
        };

        Ok(ReplyEntry {
            ttl: self.permission_cache_ttl(),
            attr: vfs_to_fuse_attr(&attr, &req, self.blocks_for_attr(&attr)),
            generation: 0,
        })
    }

    async fn unlink_with_actor(&self, req: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        debug!(parent, name = %name.to_string_lossy(), "fuse.unlink");
        let name = name.to_string_lossy();
        validate_fuse_name(name.as_ref())?;
        self.ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid, req.pid)
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

    async fn rmdir_with_actor(&self, req: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        debug!(parent, name = %name.to_string_lossy(), "fuse.rmdir");
        let name = name.to_string_lossy();
        validate_fuse_name(name.as_ref())?;
        self.ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid, req.pid)
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

    async fn rename_with_actor(
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

        self.ensure_directory_parent_namespace_mutation_allowed(parent, req.uid, req.gid, req.pid)
            .await?;
        if parent != new_parent {
            self.ensure_directory_parent_namespace_mutation_allowed(
                new_parent, req.uid, req.gid, req.pid,
            )
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
            self.ensure_access_allowed(
                src_ino,
                req.uid,
                req.gid,
                req.pid,
                namespace_mutation_access_mask(),
            )
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
                    VfsError::ReadOnlyFilesystem { .. } => libc::EROFS,
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

    fn permission_cache_ttl(&self) -> Duration {
        if self.meta_layer().posix_acl_capability()
            == crate::meta::layer::PosixAclCapability::ReadWrite
        {
            Duration::ZERO
        } else {
            fuse_cache_ttl()
        }
    }

    /// Mode bits granted by one atomic permission snapshot (POSIX ACL,
    /// control ACL, then owner/group/other), with the root execute rule.
    fn access_mode_for_permissions(
        &self,
        permissions: &crate::meta::layer::InodePermissions,
        uid: u32,
        gid: u32,
        pid: u32,
        requested: u32,
    ) -> FuseResult<u32> {
        let attr = &permissions.attr;
        if uid == 0 {
            return Ok(
                if requested & libc::X_OK as u32 == 0
                    || attr.kind == VfsFileType::Dir
                    || attr.mode & 0o111 != 0
                {
                    requested
                } else {
                    0
                },
            );
        }
        if let Some(raw) = &permissions.access_acl {
            let acl = crate::meta::posix_acl::PosixAcl::decode(raw)
                .map_err(|_| Errno::from(libc::EIO))?;
            if acl.mode_bits() != attr.mode & 0o777 {
                return Err(libc::EIO.into());
            }
            let groups = if acl.user_access_mode(attr.uid, uid).is_some() {
                Vec::new()
            } else {
                verified_request_group_ids(pid, uid, gid)?
            };
            return Ok(
                if acl.allows_access(attr.uid, attr.gid, uid, &groups, requested) {
                    requested
                } else {
                    0
                },
            );
        }
        if let Some(raw) = &permissions.control_acl {
            let entries: Vec<ControlAclEntry> =
                serde_json::from_slice(raw).map_err(|_| Errno::from(libc::EIO))?;
            crate::control::protocol::validate_acl_entries(&entries)
                .map_err(|_| Errno::from(libc::EIO))?;
            let groups = if uid == attr.uid {
                Vec::new()
            } else {
                verified_request_group_ids(pid, uid, gid)?
            };
            if let Some(allowed) = crate::meta::posix_acl::control_acl_allows_access(
                &entries, attr.uid, attr.gid, uid, &groups, requested,
            ) {
                return Ok(if allowed { requested } else { 0 });
            }
        }
        if uid == attr.uid {
            return Ok((attr.mode >> 6) & 7);
        }
        let groups = verified_request_group_ids(pid, uid, gid)?;
        Ok(if groups.contains(&attr.gid) {
            (attr.mode >> 3) & 7
        } else {
            attr.mode & 7
        })
    }

    async fn access_mode_for_attr(
        &self,
        ino: i64,
        attr: &VfsFileAttr,
        uid: u32,
        gid: u32,
        pid: u32,
        requested: u32,
    ) -> FuseResult<u32> {
        if self.meta_layer().posix_acl_capability()
            != crate::meta::layer::PosixAclCapability::Unsupported
        {
            let permissions = self
                .meta_layer()
                .inode_permissions(ino)
                .await
                .map_err(posix_acl_errno)?
                .ok_or_else(|| Errno::from(libc::ENOENT))?;
            return self.access_mode_for_permissions(&permissions, uid, gid, pid, requested);
        }

        // This legacy control-ACL path predates the capability-aware
        // permission snapshot above. Supplementary groups still require a
        // verified request credential; an unreadable or exited process must
        // not be allowed to grant group privileges.
        let groups = verified_request_group_ids(pid, uid, gid)?;
        Ok(
            match self
                .acl_access_mode_for_inode(ino, attr, uid, &groups, requested)
                .await
            {
                Some(mode) => mode,
                None if self.meta_layer().posix_acl_capability()
                    != crate::meta::layer::PosixAclCapability::Unsupported
                    && uid != attr.uid =>
                {
                    if groups.contains(&attr.gid) {
                        (attr.mode >> 3) & 7
                    } else {
                        attr.mode & 7
                    }
                }
                None => access_mode_from_bits(attr, uid, gid),
            },
        )
    }

    async fn ensure_directory_parent_namespace_mutation_allowed(
        &self,
        parent: u64,
        uid: u32,
        gid: u32,
        pid: u32,
    ) -> FuseResult<VfsFileAttr> {
        let Some(attr) = self
            .stat_ino_checked(parent as i64)
            .await
            .map_err(Errno::from)?
        else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(attr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }
        self.ensure_access_allowed(
            parent as i64,
            uid,
            gid,
            pid,
            parent_namespace_mutation_access_mask(),
        )
        .await?;
        Ok(attr)
    }

    #[allow(clippy::unnecessary_cast)] // mode_t is narrower on some supported platforms.
    async fn ensure_sticky_parent_allows_child_mutation(
        &self,
        parent: u64,
        child: i64,
        uid: u32,
    ) -> FuseResult<()> {
        if uid == 0 {
            return Ok(());
        }

        let Some(parent_attr) = self
            .stat_ino_checked(parent as i64)
            .await
            .map_err(Errno::from)?
        else {
            return Err(libc::ENOENT.into());
        };
        if !matches!(parent_attr.kind, VfsFileType::Dir) {
            return Err(libc::ENOTDIR.into());
        }
        if (parent_attr.mode & libc::S_ISVTX as u32) == 0 {
            return Ok(());
        }

        let Some(child_attr) = self.stat_ino_checked(child).await.map_err(Errno::from)? else {
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
        groups: &[u32],
        requested: u32,
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
            Ok(entries) => crate::meta::posix_acl::control_acl_allows_access(
                &entries, attr.uid, attr.gid, uid, groups, requested,
            )
            .map(|allowed| if allowed { requested } else { 0 }),
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
    crate::meta::posix_acl::control_acl_access_mode(entries, attr.uid, attr.gid, uid, &[gid])
}

fn posix_acl_errno(error: MetaError) -> Errno {
    match error {
        MetaError::Io(error) => Errno::from(error.raw_os_error().unwrap_or(libc::EIO)),
        MetaError::NotSupported(_) | MetaError::NotImplemented => Errno::from(libc::EOPNOTSUPP),
        error => Errno::from(error),
    }
}

impl From<MetaError> for Errno {
    fn from(val: MetaError) -> Self {
        let code = match val {
            MetaError::NotFound(_) | MetaError::EntryNotFound { .. } => libc::ENOENT,
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

fn check_requested_access(mask: u32, mode: u32) -> FuseResult<()> {
    // mask uses libc constants: F_OK=0, X_OK=1, W_OK=2, R_OK=4
    if ((mask & libc::R_OK as u32) != 0 && (mode & 0o4) == 0)
        || ((mask & libc::W_OK as u32) != 0 && (mode & 0o2) == 0)
        || ((mask & libc::X_OK as u32) != 0 && (mode & 0o1) == 0)
    {
        return Err(libc::EACCES.into());
    }
    Ok(())
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

#[allow(clippy::useless_conversion)] // SetAttr mode follows platform mode_t width.
fn fuse_setattr_to_meta(set_attr: &SetAttr) -> (SetAttrRequest, SetAttrFlags) {
    let mut req = SetAttrRequest::default();
    let mut flags = SetAttrFlags::empty();
    if set_attr.atime_now {
        flags.insert(SetAttrFlags::SET_ATIME_NOW);
    }
    if set_attr.mtime_now {
        flags.insert(SetAttrFlags::SET_MTIME_NOW);
    }
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
        && (flags.bits() & !(SetAttrFlags::SET_ATIME_NOW | SetAttrFlags::SET_MTIME_NOW).bits()) == 0
}

fn setattr_is_mode_with_optional_timestamps(req: &SetAttrRequest, flags: &SetAttrFlags) -> bool {
    req.mode.is_some()
        && req.uid.is_none()
        && req.gid.is_none()
        && req.size.is_none()
        && req.flags.is_none()
        && (flags.bits() & !(SetAttrFlags::SET_ATIME_NOW | SetAttrFlags::SET_MTIME_NOW).bits()) == 0
}

fn setattr_is_chown_with_optional_timestamps(req: &SetAttrRequest, flags: &SetAttrFlags) -> bool {
    (req.uid.is_some() || req.gid.is_some())
        && req.size.is_none()
        && req.flags.is_none()
        && (flags.bits() & !(SetAttrFlags::SET_ATIME_NOW | SetAttrFlags::SET_MTIME_NOW).bits()) == 0
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
        && (req.atime.is_some()
            || req.mtime.is_some()
            || req.ctime.is_some()
            || flags.intersects(SetAttrFlags::SET_ATIME_NOW | SetAttrFlags::SET_MTIME_NOW))
        && (flags.bits() & !(SetAttrFlags::SET_ATIME_NOW | SetAttrFlags::SET_MTIME_NOW).bits()) == 0
}

fn timestamp_request_is_ctime_only(req: &SetAttrRequest) -> bool {
    req.ctime.is_some() && req.atime.is_none() && req.mtime.is_none()
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

/// Supplementary groups must belong to a complete credential pair in the request.
/// An unreadable, exited or mismatched process cannot grant group privileges.
fn verified_request_group_ids(pid: u32, uid: u32, gid: u32) -> FuseResult<Vec<u32>> {
    // Synthetic/internal requests have no process identity or supplementary groups.
    if pid == 0 {
        return Ok(vec![gid]);
    }
    #[cfg(target_os = "linux")]
    {
        use std::io::Read;
        const STATUS_LIMIT: usize = 1024 * 1024;
        let mut status = String::new();
        std::fs::File::open(format!("/proc/{pid}/status"))
            .and_then(|file| {
                file.take((STATUS_LIMIT + 1) as u64)
                    .read_to_string(&mut status)
            })
            .map_err(|_| Errno::from(libc::EACCES))?;
        if status.len() > STATUS_LIMIT {
            return Err(libc::EACCES.into());
        }
        verified_proc_status_groups(&status, uid, gid).ok_or_else(|| libc::EACCES.into())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = uid;
        Ok(vec![gid])
    }
}

fn verified_proc_status_groups(status: &str, uid: u32, gid: u32) -> Option<Vec<u32>> {
    fn fields(status: &str, prefix: &str) -> Option<Vec<u32>> {
        let mut matches = status.lines().filter_map(|line| line.strip_prefix(prefix));
        let value = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        let mut ids = Vec::new();
        for word in value.split_whitespace() {
            if ids.len() >= 65536 {
                return None;
            }
            ids.push(word.parse().ok()?);
        }
        Some(ids)
    }
    let uids = fields(status, "Uid:")?;
    let gids = fields(status, "Gid:")?;
    if uids.len() != 4 || gids.len() != 4 {
        return None;
    }
    // access() temporarily overrides subjective fsuid/fsgid with real ids.
    // /proc reports the persistent credentials; both paths use group_info.
    // Accept complete real or filesystem pairs, never cross-match identities.
    if !((uids[3] == uid && gids[3] == gid) || (uids[0] == uid && gids[0] == gid)) {
        return None;
    }
    let mut groups = fields(status, "Groups:")?;
    if !groups.contains(&gid) {
        groups.push(gid);
    }
    groups.sort_unstable();
    groups.dedup();
    Some(groups)
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

fn raw_os_str_bytes(name: &OsStr) -> &[u8] {
    #[cfg(unix)]
    {
        name.as_bytes()
    }
    #[cfg(not(unix))]
    {
        name.to_str().map_or(&[], str::as_bytes)
    }
}

fn validate_fuse_name_bytes(name: &[u8]) -> Result<(), Errno> {
    if name.is_empty() {
        return Err(libc::EINVAL.into());
    }
    if name.len() > NAME_MAX {
        return Err(libc::ENAMETOOLONG.into());
    }
    if name.contains(&b'/') || name.contains(&0) {
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
    fn writable_acl_fuse_timestamp_now_provenance_is_preserved() {
        let timestamp = asyncfuse::Timestamp::new(123, 456);
        let explicit = asyncfuse::SetAttr {
            atime: Some(timestamp),
            mtime: Some(timestamp),
            ..Default::default()
        };
        let (_, flags) = super::fuse_setattr_to_meta(&explicit);
        assert!(flags.is_empty());
        let now = asyncfuse::SetAttr {
            atime_now: true,
            mtime_now: true,
            ..explicit
        };
        let (request, flags) = super::fuse_setattr_to_meta(&now);
        assert!(flags.contains(
            crate::meta::store::SetAttrFlags::SET_ATIME_NOW
                | crate::meta::store::SetAttrFlags::SET_MTIME_NOW
        ));
        assert!(super::setattr_is_timestamp_only(&request, &flags));
    }

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
    fn verified_groups_require_exact_filesystem_identity_and_complete_fields() {
        let status = "Uid:\t0 0 0 1000\nGid:\t0 0 0 2000\nGroups:\t4000 3000 4000\n";
        assert_eq!(
            super::verified_proc_status_groups(status, 1000, 2000),
            Some(vec![2000, 3000, 4000])
        );
        for invalid in [
            status.replace("1000", "1001"),
            status.replace("2000", "2001"),
            status.replace("4000 3000 4000", "4000 invalid 3000"),
            status.replace("0 0 0 1000", "1000"),
            status.replace("Groups:", "Unknown:"),
            format!("{status}Uid:\t0 0 0 1000\n"),
            format!("{status}Groups:\t9000\n"),
        ] {
            assert!(super::verified_proc_status_groups(&invalid, 1000, 2000).is_none());
        }
        assert_eq!(
            super::verified_proc_status_groups("Uid: 1 1 1 1\nGid: 2 2 2 2\nGroups:\n", 1, 2),
            Some(vec![2])
        );
        assert!(super::verified_request_group_ids(u32::MAX, 1000, 2000).is_err());
    }

    #[test]
    fn verified_groups_accept_real_identity_for_access_but_reject_crossed_pairs() {
        let status = "Uid: 31000 0 0 0\nGid: 32000 0 0 0\nGroups: 31001 31002\n";
        assert_eq!(
            super::verified_proc_status_groups(status, 31000, 32000),
            Some(vec![31001, 31002, 32000])
        );
        assert_eq!(
            super::verified_proc_status_groups(status, 0, 0),
            Some(vec![0, 31001, 31002])
        );
        assert!(super::verified_proc_status_groups(status, 31000, 0).is_none());
        assert!(super::verified_proc_status_groups(status, 0, 32000).is_none());
        assert!(super::verified_proc_status_groups(status, 31000, 31001).is_none());
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
    use std::sync::{Arc, Mutex as StdMutex, OnceLock};

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

    #[derive(Debug)]
    struct LargeChangingStatsExtension {
        generation: Arc<std::sync::atomic::AtomicU64>,
    }
    impl crate::vfs::stats::FsStatsExtension for LargeChangingStatsExtension {
        fn render_max_bytes(&self) -> usize {
            256 * 1024
        }
        fn render_into(&self, output: &mut dyn std::fmt::Write) {
            let generation = self.generation.load(std::sync::atomic::Ordering::SeqCst);
            for row in 0..1024 {
                let _ = writeln!(
                    output,
                    "brewfs_fixture_large_metric{{row=\"{row}\"}} {generation}"
                );
            }
        }
    }

    #[tokio::test]
    async fn virtual_stats_dynamic_size_stable_multiread_and_last_reply_clone() {
        let fs = new_fuse_test_vfs().await;
        let generation = Arc::new(std::sync::atomic::AtomicU64::new(7));
        assert!(
            fs.stats()
                .set_extension(Arc::new(LargeChangingStatsExtension {
                    generation: generation.clone()
                }))
        );
        let lookup = Filesystem::lookup(
            &fs,
            Request::default(),
            fs.root_ino() as u64,
            OsStr::new(".stats"),
        )
        .await
        .unwrap();
        assert_eq!(lookup.attr.ino, STATS_INODE);
        assert!(lookup.attr.size > 16 * 1024);
        let opened = Filesystem::open(&fs, Request::default(), STATS_INODE, libc::O_RDONLY as u32)
            .await
            .unwrap();
        assert_ne!(opened.fh, 0);
        assert_eq!(opened.flags & FOPEN_DIRECT_IO, FOPEN_DIRECT_IO);
        let expected = fs.virtual_stats_snapshot(opened.fh).unwrap();
        let attr = Filesystem::getattr(&fs, Request::default(), STATS_INODE, Some(opened.fh), 0)
            .await
            .unwrap();
        assert_eq!(attr.attr.size, expected.len() as u64);
        generation.store(123456789, std::sync::atomic::Ordering::SeqCst);
        let mut result = Vec::new();
        let mut offset = 0u64;
        while offset < expected.len() as u64 {
            let reply =
                Filesystem::read(&fs, Request::default(), STATS_INODE, opened.fh, offset, 317)
                    .await
                    .unwrap();
            assert!(!reply.data.is_empty());
            offset += reply.data.len() as u64;
            result.extend_from_slice(&reply.data);
        }
        assert_eq!(result.as_slice(), expected.as_ref());
        assert!(
            Filesystem::read(&fs, Request::default(), STATS_INODE, opened.fh, offset, 317)
                .await
                .unwrap()
                .data
                .is_empty()
        );
        let replay = Filesystem::read(&fs, Request::default(), STATS_INODE, opened.fh, 12, 317)
            .await
            .unwrap();
        assert_eq!(replay.data.as_ref(), &expected[12..329]);

        let second = Filesystem::open(&fs, Request::default(), STATS_INODE, libc::O_RDONLY as u32)
            .await
            .unwrap();
        let second_attr =
            Filesystem::getattr(&fs, Request::default(), STATS_INODE, Some(second.fh), 0)
                .await
                .unwrap();
        assert_eq!(
            second_attr.attr.size,
            fs.virtual_stats_snapshot(second.fh).unwrap().len() as u64
        );
        assert_ne!(fs.virtual_stats_snapshot(second.fh).unwrap(), expected);
        let retained_reply = replay.data.clone();
        Filesystem::release(&fs, Request::default(), STATS_INODE, opened.fh, 0, 0, false)
            .await
            .unwrap();
        assert!(fs.virtual_stats_snapshot(opened.fh).is_none());
        assert_eq!(retained_reply.as_ref(), &expected[12..329]);
        assert_eq!(
            Filesystem::read(&fs, Request::default(), STATS_INODE, opened.fh, 0, 1)
                .await
                .unwrap_err(),
            Errno::from(libc::EBADF)
        );
        Filesystem::release(&fs, Request::default(), STATS_INODE, second.fh, 0, 0, false)
            .await
            .unwrap();
    }

    fn user_request() -> Request {
        request_with_ids(1000, 1000)
    }

    fn request_with_ids(uid: u32, gid: u32) -> Request {
        Request {
            unique: 1,
            uid,
            gid,
            // Unit-test requests are synthetic and intentionally use the
            // pid=0 path, which carries only the supplied primary group.
            pid: 0,
        }
    }

    #[cfg(all(feature = "workspace-overlay", target_os = "linux"))]
    async fn packed_source_test_vfs(
        source: &std::path::Path,
    ) -> (
        tempfile::TempDir,
        VFS<
            crate::workspace_overlay::packed_v3::PackedV3BlockStore<
                crate::cadapter::localfs::LocalFsBackend,
            >,
            crate::workspace_overlay::packed_v3::PackedV3ReadonlyMeta<
                crate::cadapter::localfs::LocalFsBackend,
            >,
        >,
    ) {
        use crate::cadapter::{client::ObjectClient, localfs::LocalFsBackend};
        use crate::vfs::config::VFSConfig;
        use crate::workspace_overlay::packed_v3::wire005::{
            AuthenticatedV3Snapshot, V3ProducerOptions, V3SourceConsistency, V3SourceFileLimits,
            V3SourceHardlinkPolicy, V3SourceNamespaceInventory, V3SourceNamespaceOptions,
        };
        use crate::workspace_overlay::packed_v3::{
            AccessProfile, PackedCodec, PackedV3ReadonlyMeta, SizeClassTable,
        };
        use std::sync::Arc;
        let spool = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let inventory = V3SourceNamespaceInventory::capture(
            source,
            spool.path(),
            V3SourceNamespaceOptions {
                root_inode: 1,
                consistency: V3SourceConsistency::BestEffortDetected,
                hardlink_policy: V3SourceHardlinkPolicy::VisibleLinks,
                file_limits: V3SourceFileLimits::default(),
            },
        )
        .await
        .unwrap();
        let built = inventory
            .build_snapshot(
                client.clone(),
                "source-posix".into(),
                V3ProducerOptions {
                    snapshot_id: [1; 32],
                    root_dir_key: [2; 32],
                    root_inode: 1,
                    profile: AccessProfile::RandomSmallFile,
                    size_classes: SizeClassTable::default(),
                    build_policy: Default::default(),
                    metadata_codec: PackedCodec::Raw,
                    data_codec: PackedCodec::Raw,
                },
            )
            .await
            .unwrap();
        let snapshot = AuthenticatedV3Snapshot::open(&client, &built.reference)
            .await
            .unwrap();
        let layout = ChunkLayout::default();
        let meta = Arc::new(PackedV3ReadonlyMeta::from_v3(
            client,
            snapshot,
            layout.chunk_size,
            0,
        ));
        let store = Arc::new(meta.block_store(layout.block_size).unwrap());
        let fs = VFS::from_workspace_components(VFSConfig::new(layout), store, meta).unwrap();
        (objects, fs)
    }

    #[cfg(all(feature = "workspace-overlay", target_os = "linux"))]
    fn packed_fixture_object_path(
        objects: &std::path::Path,
        magic: &[u8; 8],
    ) -> std::path::PathBuf {
        let mut matches = Vec::new();
        let mut directories = vec![objects.to_owned()];
        while let Some(directory) = directories.pop() {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    directories.push(path);
                } else if std::fs::read(&path).unwrap().starts_with(magic) {
                    matches.push(path);
                }
            }
        }
        assert_eq!(
            matches.len(),
            1,
            "small fixture must have one selected index page"
        );
        matches.pop().unwrap()
    }

    #[cfg(all(feature = "workspace-overlay", target_os = "linux"))]
    #[tokio::test]
    async fn packed_hot_inode_failures_preserve_io_errno_in_acl_xattrs_and_access() {
        use crate::meta::posix_acl::{ACCESS_XATTR, DEFAULT_XATTR};
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let source = tempfile::tempdir().unwrap();
        std::fs::set_permissions(source.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let file = source.path().join("hot-acl-file");
        std::fs::write(&file, b"hot metadata errno payload").unwrap();
        let uid = std::fs::metadata(&file).unwrap().uid() + 10000;
        let mut acl = 2u32.to_le_bytes().to_vec();
        for (tag, permissions, id) in [
            (1u16, 6u16, u32::MAX),
            (2, 4, uid),
            (4, 0, u32::MAX),
            (16, 4, u32::MAX),
            (32, 0, u32::MAX),
        ] {
            acl.extend_from_slice(&tag.to_le_bytes());
            acl.extend_from_slice(&permissions.to_le_bytes());
            acl.extend_from_slice(&id.to_le_bytes());
        }
        let c_path = std::ffi::CString::new(file.as_os_str().as_bytes()).unwrap();
        let c_name = std::ffi::CString::new(ACCESS_XATTR).unwrap();
        // SAFETY: live NUL-terminated path/name and exact ACL byte slice.
        assert_eq!(
            unsafe {
                libc::lsetxattr(
                    c_path.as_ptr(),
                    c_name.as_ptr(),
                    acl.as_ptr().cast(),
                    acl.len(),
                    0,
                )
            },
            0
        );
        std::fs::File::open(&file).unwrap().sync_all().unwrap();
        std::fs::File::open(source.path())
            .unwrap()
            .sync_all()
            .unwrap();
        let (objects, fs) = packed_source_test_vfs(source.path()).await;
        let found = Filesystem::lookup(&fs, Request::default(), 1, OsStr::new("hot-acl-file"))
            .await
            .unwrap();
        let inode = found.attr.ino;
        let request = || Request {
            uid,
            gid: 32000,
            pid: 0,
            unique: 1,
        };
        assert!(matches!(
            Filesystem::getxattr(
                &fs,
                Request::default(),
                inode,
                OsStr::from_bytes(ACCESS_XATTR),
                0
            )
            .await
            .unwrap(),
            ReplyXAttr::Size(size) if size == acl.len() as u32
        ));
        let ReplyXAttr::Data(names) = Filesystem::listxattr(&fs, Request::default(), inode, 4096)
            .await
            .unwrap()
        else {
            panic!("healthy ACL xattrs must be listed");
        };
        assert!(
            names
                .split(|byte| *byte == 0)
                .any(|name| name == ACCESS_XATTR)
        );
        Filesystem::access(&fs, request(), inode, libc::R_OK as u32)
            .await
            .unwrap();
        let opened = Filesystem::open(&fs, request(), inode, libc::O_RDONLY as u32)
            .await
            .unwrap();
        assert_eq!(
            Filesystem::getxattr(
                &fs,
                Request::default(),
                inode,
                OsStr::from_bytes(DEFAULT_XATTR),
                0
            )
            .await
            .unwrap_err(),
            Errno::from(libc::ENODATA)
        );

        // Genuine inode absence stays ENOENT with an intact authenticated index.
        let absent = i64::MAX as u64;
        assert_eq!(
            Filesystem::getxattr(
                &fs,
                Request::default(),
                absent,
                OsStr::from_bytes(ACCESS_XATTR),
                0
            )
            .await
            .unwrap_err(),
            Errno::from(libc::ENOENT)
        );
        assert_eq!(
            Filesystem::listxattr(&fs, Request::default(), absent, 0)
                .await
                .unwrap_err(),
            Errno::from(libc::ENOENT)
        );
        assert_eq!(
            Filesystem::access(&fs, request(), absent, libc::R_OK as u32)
                .await
                .unwrap_err(),
            Errno::from(libc::ENOENT)
        );

        // Target the hot inode index, leaving the cold ACL object unchanged.
        // This fixture uses cache_bytes=0, so every operation observes the fault.
        let hot = packed_fixture_object_path(objects.path(), b"BRFII005");
        let original = std::fs::read(&hot).unwrap();
        for fault in ["checksum", "missing-object", "backend-read"] {
            match fault {
                "checksum" => {
                    let mut corrupted = original.clone();
                    corrupted[4096] ^= 1;
                    std::fs::write(&hot, corrupted).unwrap();
                }
                "missing-object" => std::fs::remove_file(&hot).unwrap(),
                "backend-read" => {
                    std::fs::remove_file(&hot).unwrap();
                    std::fs::create_dir(&hot).unwrap();
                }
                _ => unreachable!(),
            }
            for handle in [None, Some(opened.fh)] {
                assert_eq!(
                    Filesystem::getattr(&fs, Request::default(), inode, handle, 0)
                        .await
                        .unwrap_err(),
                    Errno::from(libc::EIO),
                    "getattr must not use orphan attrs for a hot fault: {fault}"
                );
            }
            assert_eq!(
                Filesystem::getxattr(
                    &fs,
                    Request::default(),
                    inode,
                    OsStr::from_bytes(ACCESS_XATTR),
                    0
                )
                .await
                .unwrap_err(),
                Errno::from(libc::EIO),
                "getxattr hot fault: {fault}"
            );
            assert_eq!(
                Filesystem::listxattr(&fs, Request::default(), inode, 0)
                    .await
                    .unwrap_err(),
                Errno::from(libc::EIO),
                "listxattr hot fault: {fault}"
            );
            assert_eq!(
                Filesystem::access(&fs, request(), inode, libc::R_OK as u32)
                    .await
                    .unwrap_err(),
                Errno::from(libc::EIO),
                "access hot fault: {fault}"
            );
            // F_OK must still validate existence before the permission bypass.
            assert_eq!(
                Filesystem::access(&fs, Request::default(), inode, libc::F_OK as u32)
                    .await
                    .unwrap_err(),
                Errno::from(libc::EIO),
                "root F_OK hot fault: {fault}"
            );
            assert_eq!(
                Filesystem::setxattr(
                    &fs,
                    Request::default(),
                    inode,
                    OsStr::from_bytes(ACCESS_XATTR),
                    &acl,
                    0,
                    0
                )
                .await
                .unwrap_err(),
                Errno::from(libc::EIO),
                "setxattr hot fault: {fault}"
            );
            assert_eq!(
                Filesystem::removexattr(
                    &fs,
                    Request::default(),
                    inode,
                    OsStr::from_bytes(ACCESS_XATTR)
                )
                .await
                .unwrap_err(),
                Errno::from(libc::EIO),
                "removexattr hot fault: {fault}"
            );
            if fault == "backend-read" {
                std::fs::remove_dir(&hot).unwrap();
            }
            std::fs::write(&hot, &original).unwrap();
            Filesystem::access(&fs, request(), inode, libc::R_OK as u32)
                .await
                .unwrap();
        }
        fs.close(opened.fh).await.unwrap();
    }

    #[cfg(all(feature = "workspace-overlay", target_os = "linux"))]
    #[tokio::test]
    async fn packed_permission_ancestor_lookup_preserves_io_errno() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let source = tempfile::tempdir().unwrap();
        std::fs::set_permissions(source.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let dir = source.path().join("ancestor");
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(dir.join("child"), b"ancestor lookup payload").unwrap();
        let uid = std::fs::metadata(source.path()).unwrap().uid() + 10000;
        let (objects, fs) = packed_source_test_vfs(source.path()).await;
        assert!(
            fs.path_ancestors_search_allowed(b"/ancestor/child", uid, 32000, 0)
                .await
                .unwrap()
        );
        assert_eq!(
            fs.path_ancestors_search_allowed(b"/absent/child", uid, 32000, 0)
                .await
                .unwrap_err(),
            Errno::from(libc::ENOENT)
        );
        let group = packed_fixture_object_path(objects.path(), b"BRFGI005");
        let original = std::fs::read(&group).unwrap();
        for missing in [false, true] {
            if missing {
                std::fs::remove_file(&group).unwrap();
            } else {
                let mut corrupted = original.clone();
                corrupted[4096] ^= 1;
                std::fs::write(&group, corrupted).unwrap();
            }
            assert_eq!(
                fs.path_ancestors_search_allowed(b"/ancestor/child", uid, 32000, 0)
                    .await
                    .unwrap_err(),
                Errno::from(libc::EIO),
                "permission ancestor index fault, missing={missing}"
            );
            std::fs::write(&group, &original).unwrap();
        }
    }

    #[cfg(all(feature = "workspace-overlay", target_os = "linux"))]
    #[tokio::test]
    async fn packed_posix_acl_grants_denies_and_rejects_missing_or_corrupt_cold_objects() {
        use crate::meta::posix_acl::{ACCESS_XATTR, DEFAULT_XATTR};
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let source = tempfile::tempdir().unwrap();
        std::fs::set_permissions(source.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let file = source.path().join("acl-file");
        std::fs::write(&file, b"ACL protected payload").unwrap();
        let uid = std::fs::metadata(&file).unwrap().uid() + 10000;
        let mut acl = 2u32.to_le_bytes().to_vec();
        for (tag, permissions, id) in [
            (1u16, 6u16, u32::MAX),
            (2, 4, uid),
            (2, 0, uid + 1),
            (4, 0, u32::MAX),
            (8, 4, 31001),
            (8, 1, 31002),
            (16, 5, u32::MAX),
            (32, 4, u32::MAX),
        ] {
            acl.extend_from_slice(&tag.to_le_bytes());
            acl.extend_from_slice(&permissions.to_le_bytes());
            acl.extend_from_slice(&id.to_le_bytes());
        }
        let c_path = std::ffi::CString::new(file.as_os_str().as_bytes()).unwrap();
        let c_name = std::ffi::CString::new(ACCESS_XATTR).unwrap();
        // SAFETY: live NUL-terminated path/name and exact ACL byte slice.
        assert_eq!(
            unsafe {
                libc::lsetxattr(
                    c_path.as_ptr(),
                    c_name.as_ptr(),
                    acl.as_ptr().cast(),
                    acl.len(),
                    0,
                )
            },
            0
        );
        let (objects, fs) = packed_source_test_vfs(source.path()).await;
        let found = Filesystem::lookup(&fs, Request::default(), 1, OsStr::new("acl-file"))
            .await
            .unwrap();
        let request = |uid, gid| Request {
            uid,
            gid,
            pid: 0,
            unique: 1,
        };
        Filesystem::access(&fs, request(uid, 32000), found.attr.ino, libc::R_OK as u32)
            .await
            .unwrap();
        assert_eq!(
            Filesystem::access(
                &fs,
                request(uid + 1, 31001),
                found.attr.ino,
                libc::R_OK as u32
            )
            .await
            .unwrap_err(),
            Errno::from(libc::EACCES)
        );
        Filesystem::access(
            &fs,
            request(uid + 2, 31001),
            found.attr.ino,
            libc::R_OK as u32,
        )
        .await
        .unwrap();
        assert_eq!(
            Filesystem::access(
                &fs,
                request(uid + 2, 31001),
                found.attr.ino,
                (libc::R_OK | libc::X_OK) as u32
            )
            .await
            .unwrap_err(),
            Errno::from(libc::EACCES)
        );
        let forged = Request {
            uid: uid + 2,
            gid: 31001,
            pid: u32::MAX,
            unique: 1,
        };
        assert_eq!(
            Filesystem::access(&fs, forged, found.attr.ino, libc::R_OK as u32)
                .await
                .unwrap_err(),
            Errno::from(libc::EACCES)
        );
        for name in [ACCESS_XATTR, DEFAULT_XATTR] {
            assert_eq!(
                Filesystem::setxattr(
                    &fs,
                    Request::default(),
                    found.attr.ino,
                    OsStr::from_bytes(name),
                    &acl,
                    0,
                    0
                )
                .await
                .unwrap_err(),
                Errno::from(libc::EROFS)
            );
            assert_eq!(
                Filesystem::removexattr(
                    &fs,
                    Request::default(),
                    found.attr.ino,
                    OsStr::from_bytes(name)
                )
                .await
                .unwrap_err(),
                Errno::from(libc::EROFS)
            );
        }
        let ReplyXAttr::Data(raw) = Filesystem::getxattr(
            &fs,
            Request::default(),
            found.attr.ino,
            OsStr::from_bytes(ACCESS_XATTR),
            acl.len() as u32,
        )
        .await
        .unwrap() else {
            panic!("ACL query must return bytes");
        };
        assert_eq!(raw.as_ref(), acl);
        let ReplyXAttr::Size(size) = Filesystem::getxattr(
            &fs,
            Request::default(),
            found.attr.ino,
            OsStr::from_bytes(ACCESS_XATTR),
            0,
        )
        .await
        .unwrap() else {
            panic!("ACL size probe");
        };
        assert_eq!(size as usize, acl.len());
        assert_eq!(
            Filesystem::getxattr(
                &fs,
                Request::default(),
                found.attr.ino,
                OsStr::from_bytes(ACCESS_XATTR),
                1
            )
            .await
            .unwrap_err(),
            Errno::from(libc::ERANGE)
        );
        let mut cold = None;
        let mut directories = vec![objects.path().to_owned()];
        while let Some(directory) = directories.pop() {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    directories.push(path);
                } else if std::fs::read(&path).unwrap().starts_with(b"BRFCA005") {
                    cold = Some(path);
                }
            }
        }
        let cold = cold.expect("source ACL must publish a cold object");
        let original = std::fs::read(&cold).unwrap();
        let mut corrupted = original.clone();
        corrupted[4096] ^= 1;
        std::fs::write(&cold, &corrupted).unwrap();
        assert_eq!(
            Filesystem::access(&fs, request(uid, 32000), found.attr.ino, libc::R_OK as u32)
                .await
                .unwrap_err(),
            Errno::from(libc::EIO)
        );
        assert_eq!(
            Filesystem::getxattr(
                &fs,
                Request::default(),
                found.attr.ino,
                OsStr::from_bytes(ACCESS_XATTR),
                0
            )
            .await
            .unwrap_err(),
            Errno::from(libc::EIO)
        );
        std::fs::remove_file(&cold).unwrap();
        assert_eq!(
            Filesystem::access(&fs, request(uid, 32000), found.attr.ino, libc::R_OK as u32)
                .await
                .unwrap_err(),
            Errno::from(libc::EIO)
        );
    }

    #[cfg(all(feature = "workspace-overlay", target_os = "linux"))]
    #[tokio::test]
    async fn packed_source_stats_name_preserves_file_directory_and_virtual_fallback() {
        for directory in [false, true] {
            let source = tempfile::tempdir().unwrap();
            let path = source.path().join(".stats");
            if directory {
                std::fs::create_dir(&path).unwrap();
            } else {
                std::fs::write(&path, b"source statistics\0\xff").unwrap();
            }
            let (_objects, fs) = packed_source_test_vfs(source.path()).await;
            let found = Filesystem::lookup(&fs, request_with_ids(0, 0), 1, OsStr::new(".stats"))
                .await
                .unwrap();
            assert_ne!(found.attr.ino, STATS_INODE);
            assert_eq!(found.attr.size, std::fs::metadata(&path).unwrap().len());
            assert_eq!(
                found.attr.kind,
                if directory {
                    FuseFileType::Directory
                } else {
                    FuseFileType::RegularFile
                }
            );
            if !directory {
                let opened = Filesystem::open(&fs, request_with_ids(0, 0), found.attr.ino, 0)
                    .await
                    .unwrap();
                let data = Filesystem::read(
                    &fs,
                    request_with_ids(0, 0),
                    found.attr.ino,
                    opened.fh,
                    0,
                    64,
                )
                .await
                .unwrap();
                assert_eq!(data.data.as_ref(), b"source statistics\0\xff");
                fs.close(opened.fh).await.unwrap();
            }
        }
        let source = tempfile::tempdir().unwrap();
        let (_objects, fs) = packed_source_test_vfs(source.path()).await;
        let found = Filesystem::lookup(&fs, request_with_ids(0, 0), 1, OsStr::new(".stats"))
            .await
            .unwrap();
        assert_eq!(found.attr.ino, STATS_INODE);
    }

    #[cfg(all(feature = "workspace-overlay", target_os = "linux"))]
    #[tokio::test]
    async fn packed_source_raw_xattrs_preserve_distinct_names_sizes_and_readonly_errno() {
        use std::os::unix::ffi::OsStrExt;
        let source = tempfile::tempdir().unwrap();
        let file = source.path().join("file");
        std::fs::write(&file, b"source").unwrap();
        let path = std::ffi::CString::new(file.as_os_str().as_bytes()).unwrap();
        let names = [
            b"user.name-\xff".as_slice(),
            b"user.name-\xef\xbf\xbd".as_slice(),
        ];
        for (name, value) in names
            .into_iter()
            .zip([b"raw\0\xff".as_slice(), b"UTF8".as_slice()])
        {
            let c_name = std::ffi::CString::new(name).unwrap();
            // SAFETY: the path/name are NUL terminated; value is live.
            assert_eq!(
                unsafe {
                    libc::lsetxattr(
                        path.as_ptr(),
                        c_name.as_ptr(),
                        value.as_ptr().cast(),
                        value.len(),
                        0,
                    )
                },
                0
            );
        }
        let (_objects, fs) = packed_source_test_vfs(source.path()).await;
        let found = Filesystem::lookup(&fs, request_with_ids(0, 0), 1, OsStr::new("file"))
            .await
            .unwrap();
        for (name, expected) in names
            .into_iter()
            .zip([b"raw\0\xff".as_slice(), b"UTF8".as_slice()])
        {
            let ReplyXAttr::Data(value) = Filesystem::getxattr(
                &fs,
                Request::default(),
                found.attr.ino,
                OsStr::from_bytes(name),
                64,
            )
            .await
            .unwrap() else {
                panic!("expected xattr data")
            };
            assert_eq!(value.as_ref(), expected);
            let ReplyXAttr::Size(size) = Filesystem::getxattr(
                &fs,
                Request::default(),
                found.attr.ino,
                OsStr::from_bytes(name),
                0,
            )
            .await
            .unwrap() else {
                panic!("expected xattr size")
            };
            assert_eq!(size as usize, expected.len());
            assert_eq!(
                Filesystem::getxattr(
                    &fs,
                    Request::default(),
                    found.attr.ino,
                    OsStr::from_bytes(name),
                    1
                )
                .await
                .unwrap_err(),
                Errno::from(libc::ERANGE)
            );
            assert_eq!(
                Filesystem::setxattr(
                    &fs,
                    Request::default(),
                    found.attr.ino,
                    OsStr::from_bytes(name),
                    b"change",
                    0,
                    0
                )
                .await
                .unwrap_err(),
                Errno::from(libc::EROFS)
            );
            assert_eq!(
                Filesystem::removexattr(
                    &fs,
                    Request::default(),
                    found.attr.ino,
                    OsStr::from_bytes(name)
                )
                .await
                .unwrap_err(),
                Errno::from(libc::EROFS)
            );
        }
        let ReplyXAttr::Data(list) =
            Filesystem::listxattr(&fs, Request::default(), found.attr.ino, 1024)
                .await
                .unwrap()
        else {
            panic!("expected raw xattr list")
        };
        let listed: Vec<_> = list
            .split(|byte| *byte == 0)
            .filter(|name| !name.is_empty())
            .collect();
        let mut expected = names.to_vec();
        expected.sort();
        assert_eq!(listed, expected);
        assert!(matches!(
            fs.list_xattr_ino(found.attr.ino as i64).await.unwrap_err(),
            VfsError::InvalidFilename
        ));
        assert_eq!(
            Filesystem::getxattr(
                &fs,
                Request::default(),
                found.attr.ino,
                OsStr::new("user.absent"),
                64
            )
            .await
            .unwrap_err(),
            Errno::from(libc::ENODATA)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn string_backed_xattr_names_reject_lossy_mutation() {
        use std::os::unix::ffi::OsStrExt;
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file").await.unwrap();
        let inode = fs.stat("/file").await.unwrap().ino as u64;
        let alias = "user.name-\u{fffd}";
        fs.set_xattr_ino(inode as i64, alias, b"preserve", 0)
            .await
            .unwrap();
        let raw = OsStr::from_bytes(b"user.name-\xff");
        assert_eq!(
            Filesystem::setxattr(&fs, Request::default(), inode, raw, b"change", 0, 0)
                .await
                .unwrap_err(),
            Errno::from(libc::EINVAL)
        );
        assert_eq!(
            Filesystem::getxattr(&fs, Request::default(), inode, raw, 64)
                .await
                .unwrap_err(),
            Errno::from(libc::EINVAL)
        );
        assert_eq!(
            Filesystem::removexattr(&fs, Request::default(), inode, raw)
                .await
                .unwrap_err(),
            Errno::from(libc::EINVAL)
        );
        assert_eq!(
            fs.get_xattr_ino(inode as i64, alias).await.unwrap(),
            Some(b"preserve".to_vec())
        );
    }

    #[cfg(all(feature = "workspace-overlay", target_os = "linux"))]
    #[tokio::test]
    async fn packed_readonly_rename_returns_erofs_without_changing_namespace() {
        use crate::cadapter::{client::ObjectClient, localfs::LocalFsBackend};
        use crate::vfs::config::VFSConfig;
        use crate::workspace_overlay::packed_v3::wire005::{
            AuthenticatedV3Snapshot, V3ProducerOptions, V3SourceConsistency, V3SourceFileLimits,
            V3SourceHardlinkPolicy, V3SourceNamespaceInventory, V3SourceNamespaceOptions,
        };
        use crate::workspace_overlay::packed_v3::{
            AccessProfile, PackedCodec, PackedV3ReadonlyMeta, SizeClassTable,
        };
        use std::sync::Arc;

        let source = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("file"), b"preserve").unwrap();
        let spool = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let inventory = V3SourceNamespaceInventory::capture(
            source.path(),
            spool.path(),
            V3SourceNamespaceOptions {
                root_inode: 1,
                consistency: V3SourceConsistency::BestEffortDetected,
                hardlink_policy: V3SourceHardlinkPolicy::VisibleLinks,
                file_limits: V3SourceFileLimits::default(),
            },
        )
        .await
        .unwrap();
        let built = inventory
            .build_snapshot(
                client.clone(),
                "readonly-rename".into(),
                V3ProducerOptions {
                    snapshot_id: [1; 32],
                    root_dir_key: [2; 32],
                    root_inode: 1,
                    profile: AccessProfile::RandomSmallFile,
                    size_classes: SizeClassTable::default(),
                    build_policy: Default::default(),
                    metadata_codec: PackedCodec::Raw,
                    data_codec: PackedCodec::Raw,
                },
            )
            .await
            .unwrap();
        let snapshot = AuthenticatedV3Snapshot::open(&client, &built.reference)
            .await
            .unwrap();
        let layout = ChunkLayout::default();
        let meta = Arc::new(PackedV3ReadonlyMeta::from_v3(
            client,
            snapshot,
            layout.chunk_size,
            0,
        ));
        let store = Arc::new(meta.block_store(layout.block_size).unwrap());
        let fs = VFS::from_workspace_components(VFSConfig::new(layout), store, meta).unwrap();
        let before = Filesystem::lookup(&fs, request_with_ids(0, 0), 1, OsStr::new("file"))
            .await
            .unwrap();
        let error = Filesystem::rename(
            &fs,
            request_with_ids(0, 0),
            1,
            OsStr::new("file"),
            1,
            OsStr::new("renamed"),
        )
        .await
        .unwrap_err();
        assert_eq!(error, Errno::from(libc::EROFS));
        let error = Filesystem::link(
            &fs,
            request_with_ids(0, 0),
            before.attr.ino,
            1,
            OsStr::new("new-hardlink"),
        )
        .await
        .unwrap_err();
        assert_eq!(error, Errno::from(libc::EROFS));
        let after = Filesystem::lookup(&fs, request_with_ids(0, 0), 1, OsStr::new("file"))
            .await
            .unwrap();
        assert_eq!(before.attr.ino, after.attr.ino);
        assert_eq!(before.attr.size, after.attr.size);
        for flags in [
            libc::O_WRONLY,
            libc::O_RDWR,
            libc::O_RDONLY | libc::O_APPEND,
        ] {
            match Filesystem::open(&fs, request_with_ids(0, 0), before.attr.ino, flags as u32).await
            {
                Err(error) => assert_eq!(error, Errno::from(libc::EROFS)),
                Ok(opened) => {
                    fs.close(opened.fh).await.unwrap();
                    panic!("readonly fast open accepted write/append flags {flags}");
                }
            }
        }
        let attr = fs.stat_ino(before.attr.ino as i64).await.unwrap();
        match fs
            .open_with_cached_attr(before.attr.ino as i64, attr, true, true, false)
            .await
        {
            Err(error) => assert!(matches!(error, VfsError::ReadOnlyFilesystem { .. })),
            Ok(handle) => {
                fs.close(handle).await.unwrap();
                panic!("readonly cached-attr open accepted write");
            }
        }
        assert_eq!(
            Filesystem::lookup(&fs, request_with_ids(0, 0), 1, OsStr::new("renamed"))
                .await
                .unwrap_err(),
            Errno::from(libc::ENOENT)
        );
        assert_eq!(
            Filesystem::lookup(&fs, request_with_ids(0, 0), 1, OsStr::new("new-hardlink"))
                .await
                .unwrap_err(),
            Errno::from(libc::ENOENT)
        );
    }

    #[cfg(all(feature = "workspace-overlay", unix))]
    #[tokio::test]
    async fn packed_raw_names_open_preserves_ancestor_search_permissions() {
        use crate::cadapter::{client::ObjectClient, localfs::LocalFsBackend};
        use crate::vfs::config::VFSConfig;
        use crate::workspace_overlay::packed_v3::wire005::{
            AuthenticatedV3Snapshot, V3ProducerOptions, V3SnapshotProducer,
        };
        use crate::workspace_overlay::packed_v3::{
            AccessProfile, GroupMeta, GroupMetaEntry, PackedCodec, PackedGroupInput,
            PackedV3ReadonlyMeta, SizeClassTable,
        };
        use std::os::unix::ffi::OsStrExt;
        use std::sync::Arc;

        let objects = tempfile::tempdir().unwrap();
        let spool = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let snapshot_id = [1; 32];
        let root_key = [2; 32];
        let profile = AccessProfile::RandomSmallFile;
        let mut producer = V3SnapshotProducer::new(
            client.clone(),
            spool.path(),
            "raw-permissions".into(),
            V3ProducerOptions {
                snapshot_id,
                root_dir_key: root_key,
                root_inode: 1,
                profile,
                size_classes: SizeClassTable::default(),
                build_policy: Default::default(),
                metadata_codec: PackedCodec::Raw,
                data_codec: PackedCodec::Raw,
            },
        )
        .await
        .unwrap();
        let entry = |inode, name: &[u8], kind, mode, nlink| GroupMetaEntry {
            name: name.to_vec(),
            inode,
            kind,
            mode,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            nlink,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            size: 0,
            flags: 0,
            inline_data: Arc::from([]),
            extents: Vec::new(),
        };
        let groups = [
            (1, root_key, entry(2, b"dir-\xff", 2, 0o40700, 2)),
            (
                2,
                crate::workspace_overlay::packed_v3::directory_key(snapshot_id, 2),
                entry(3, b"file-\xfe", 1, 0o100644, 1),
            ),
        ]
        .into_iter()
        .map(|(group_id, parent_dir_key, entry)| PackedGroupInput {
            group_id,
            parent_dir_key,
            metadata: GroupMeta::new(vec![entry]).unwrap().encode().unwrap(),
            frame_ordinals: Vec::new(),
            entry_count: 1,
            file_count: u32::from(group_id == 2),
            layout_profile: profile,
        })
        .collect::<Vec<_>>();
        producer
            .add_container(1, &groups, &[], &[1, 2])
            .await
            .unwrap();
        let reference = producer.finish().await.unwrap();
        let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
            .await
            .unwrap();
        let layout = ChunkLayout::default();
        let meta = Arc::new(PackedV3ReadonlyMeta::from_v3(
            client,
            snapshot,
            layout.chunk_size,
            0,
        ));
        let store = Arc::new(meta.block_store(layout.block_size).unwrap());
        let fs = VFS::from_workspace_components(VFSConfig::new(layout), store, meta).unwrap();
        // These credentials are synthetic: no process owns this request, and
        // only the supplied primary group may participate in permission checks.
        let owner = Request {
            pid: 0,
            ..user_request()
        };
        let other = Request {
            pid: 0,
            ..request_with_ids(2000, 2000)
        };
        let dir = Filesystem::lookup(&fs, owner, 1, OsStr::from_bytes(b"dir-\xff"))
            .await
            .unwrap();
        let file = Filesystem::lookup(&fs, owner, dir.attr.ino, OsStr::from_bytes(b"file-\xfe"))
            .await
            .unwrap();
        assert_eq!(
            fs.paths_of_bytes(file.attr.ino as i64).await.unwrap(),
            vec![b"/dir-\xff/file-\xfe".to_vec()]
        );
        // String APIs retain explicit rejection instead of replacing bytes.
        assert!(fs.paths_of(file.attr.ino as i64).await.is_err());
        let opened = Filesystem::open(&fs, owner, file.attr.ino, libc::O_RDONLY as u32)
            .await
            .unwrap();
        assert!(opened.fh > 0);
        let denied = Filesystem::open(&fs, other, file.attr.ino, libc::O_RDONLY as u32)
            .await
            .unwrap_err();
        assert_eq!(denied, Errno::from(libc::EACCES));
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
    async fn creation_in_setgid_directory_inherits_group_and_directory_setgid() {
        let fs = new_fuse_test_vfs().await;
        fs.mkdir_p("/shared").await.unwrap();
        let shared = fs.stat("/shared").await.unwrap();
        fs.chown(shared.ino, Some(0), Some(2000)).await.unwrap();
        fs.chmod(shared.ino, 0o2777).await.unwrap();

        let dir = Filesystem::mkdir(
            &fs,
            user_request(),
            shared.ino as u64,
            OsStr::new("child"),
            0o755,
            0,
        )
        .await
        .unwrap();
        assert_eq!(dir.attr.gid, 2000);
        assert_eq!(dir.attr.perm, 0o2755);

        let file = Filesystem::create(
            &fs,
            user_request(),
            shared.ino as u64,
            OsStr::new("file"),
            0o640,
            (libc::O_CREAT | libc::O_EXCL | libc::O_RDWR) as u32,
        )
        .await
        .unwrap();
        assert_eq!(file.attr.gid, 2000);
        assert_eq!(file.attr.perm, 0o640);
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
    async fn writable_acl_timestamp_setattr_rejects_explicit_near_now_and_single_now() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        fs.chmod(attr.ino, 0o666).await.unwrap();
        let now = Timestamp::from(SystemTime::now());
        for request in [
            SetAttr {
                atime: Some(now),
                mtime: Some(now),
                ..Default::default()
            },
            SetAttr {
                atime: Some(now),
                atime_now: true,
                ..Default::default()
            },
        ] {
            let error = Filesystem::setattr(&fs, user_request(), attr.ino as u64, None, request)
                .await
                .unwrap_err();
            assert_eq!(error, Errno::from(libc::EPERM));
        }
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
                atime_now: true,
                mtime: Some(now),
                mtime_now: true,
                ctime: Some(now),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert!(super::timestamp_to_nanos(reply.attr.atime) >= super::timestamp_to_nanos(now));
        assert!(super::timestamp_to_nanos(reply.attr.mtime) >= super::timestamp_to_nanos(now));
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
    async fn stateless_read_flush_without_handle_succeeds() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();

        Filesystem::flush(&fs, Request::default(), attr.ino as u64, 0, 0)
            .await
            .unwrap();
        Filesystem::release(&fs, Request::default(), attr.ino as u64, 0, 0, 0, true)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn stateless_write_flush_and_release_without_handle_persists() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();

        let reply = Filesystem::write(
            &fs,
            request_with_ids(0, 0),
            attr.ino as u64,
            0,
            0,
            b"x",
            0,
            0,
        )
        .await
        .unwrap();
        assert_eq!(reply.written, 1);

        Filesystem::flush(&fs, Request::default(), attr.ino as u64, 0, 0)
            .await
            .unwrap();
        Filesystem::release(&fs, Request::default(), attr.ino as u64, 0, 0, 0, true)
            .await
            .unwrap();

        let attr = fs.stat("/file.txt").await.unwrap();
        let fh = fs.open(attr.ino, attr, true, false, false).await.unwrap();
        assert_eq!(fs.read(fh, 0, 1).await.unwrap(), b"x");
        fs.close(fh).await.unwrap();
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
    fn distinguishes_linux_posix_acl_xattrs_from_brewfs_acl_metadata() {
        assert!(is_posix_acl_xattr("system.posix_acl_access"));
        assert!(is_posix_acl_xattr("system.posix_acl_default"));
        assert!(!is_posix_acl_xattr(CONTROL_ACL_XATTR_NAME));
        assert!(!is_posix_acl_xattr("user.test"));
    }

    #[test]
    fn user_xattr_namespace_policy_rejects_reserved_namespaces() {
        assert!(is_user_xattr_name("user.test"));
        assert!(is_user_xattr_name("user."));
        assert!(!is_user_xattr_name("system.brewfs.acl"));
        assert!(!is_user_xattr_name("trusted.foo"));
        assert!(!is_user_xattr_name("security.selinux"));
        assert!(!is_user_xattr_name("userspace"));
        assert!(is_trusted_xattr_name("trusted.foo"));
        assert!(!is_trusted_xattr_name("user.foo"));

        assert!(is_internal_xattr("system.brewfs.acl"));
        assert!(is_internal_xattr("system.brewfs.trash"));
        assert!(is_internal_xattr("system.posix_acl_access"));
        assert!(!is_internal_xattr("user.test"));
        assert!(!is_internal_xattr("trusted.foo"));
    }

    #[tokio::test]
    async fn setxattr_rejects_reserved_namespaces_but_allows_user_namespace() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        let user = request_with_ids(1000, 1000);

        for name in [
            "system.brewfs.acl",
            "system.brewfs.trash",
            "security.selinux",
            "security.capability",
        ] {
            for caller in [Request::default(), user] {
                let err = Filesystem::setxattr(
                    &fs,
                    caller,
                    attr.ino as u64,
                    OsStr::new(name),
                    b"{}",
                    0,
                    0,
                )
                .await
                .unwrap_err();
                assert_eq!(err, Errno::from(libc::EPERM), "unexpected errno for {name}");
                assert_eq!(
                    Filesystem::removexattr(&fs, caller, attr.ino as u64, OsStr::new(name))
                        .await
                        .unwrap_err(),
                    Errno::from(libc::EPERM)
                );
            }
        }

        // The Linux POSIX ACL interception path is unchanged.
        let err = Filesystem::setxattr(
            &fs,
            Request::default(),
            attr.ino as u64,
            OsStr::new("system.posix_acl_access"),
            b"raw-acl",
            0,
            0,
        )
        .await
        .unwrap_err();
        assert_eq!(err, Errno::from(libc::EOPNOTSUPP));

        Filesystem::setxattr(
            &fs,
            Request::default(),
            attr.ino as u64,
            OsStr::new("user.ok"),
            b"v",
            0,
            0,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn privileged_trusted_xattrs_round_trip_and_stay_hidden_from_users() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();
        let root = Request::default();
        let user = request_with_ids(1000, 1000);

        Filesystem::setxattr(
            &fs,
            root,
            attr.ino as u64,
            OsStr::new("trusted.external"),
            b"v",
            0,
            0,
        )
        .await
        .unwrap();

        let ReplyXAttr::Data(value) = Filesystem::getxattr(
            &fs,
            root,
            attr.ino as u64,
            OsStr::new("trusted.external"),
            1,
        )
        .await
        .unwrap() else {
            panic!("privileged trusted xattr read should return data");
        };
        assert_eq!(value.as_ref(), b"v");

        let ReplyXAttr::Data(listed) = Filesystem::listxattr(&fs, root, attr.ino as u64, 4096)
            .await
            .unwrap()
        else {
            panic!("privileged trusted xattr list should return data");
        };
        assert_eq!(listed.as_ref(), b"trusted.external\0");

        assert_eq!(
            Filesystem::setxattr(
                &fs,
                user,
                attr.ino as u64,
                OsStr::new("trusted.user"),
                b"blocked",
                0,
                0,
            )
            .await
            .unwrap_err(),
            Errno::from(libc::EPERM)
        );
        assert_eq!(
            Filesystem::getxattr(
                &fs,
                user,
                attr.ino as u64,
                OsStr::new("trusted.external"),
                0,
            )
            .await
            .unwrap_err(),
            Errno::from(libc::EPERM)
        );
        let ReplyXAttr::Size(size) = Filesystem::listxattr(&fs, user, attr.ino as u64, 0)
            .await
            .unwrap()
        else {
            panic!("unprivileged xattr list size should return a size");
        };
        assert_eq!(size as usize, 0);
        assert_eq!(
            Filesystem::removexattr(&fs, user, attr.ino as u64, OsStr::new("trusted.external"))
                .await
                .unwrap_err(),
            Errno::from(libc::EPERM)
        );

        Filesystem::removexattr(&fs, root, attr.ino as u64, OsStr::new("trusted.external"))
            .await
            .unwrap();

        // The user namespace remains available through the existing path.
        Filesystem::setxattr(
            &fs,
            user,
            attr.ino as u64,
            OsStr::new("user.visible"),
            b"ok",
            0,
            0,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn internal_control_xattrs_are_hidden_from_getxattr_and_listxattr() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();

        Filesystem::setxattr(
            &fs,
            Request::default(),
            attr.ino as u64,
            OsStr::new("user.visible"),
            b"1",
            0,
            0,
        )
        .await
        .unwrap();
        // Plant control-plane metadata the way the meta client does: directly
        // through the store, bypassing the FUSE entry points.
        fs.set_xattr_ino(attr.ino as i64, "system.brewfs.acl", b"[]", 0)
            .await
            .unwrap();

        let err = Filesystem::getxattr(
            &fs,
            Request::default(),
            attr.ino as u64,
            OsStr::new("system.brewfs.acl"),
            0,
        )
        .await
        .unwrap_err();
        assert_eq!(err, Errno::from(libc::ENODATA));

        let reply = Filesystem::listxattr(&fs, Request::default(), attr.ino as u64, 0)
            .await
            .unwrap();
        let ReplyXAttr::Size(size) = reply else {
            panic!("size probe should return ReplyXAttr::Size, got {reply:?}");
        };
        assert_eq!(size as usize, "user.visible\0".len());
    }

    #[tokio::test]
    async fn removexattr_rejects_internal_control_xattrs() {
        let fs = new_fuse_test_vfs().await;
        fs.create_file("/file.txt").await.unwrap();
        let attr = fs.stat("/file.txt").await.unwrap();

        fs.set_xattr_ino(attr.ino as i64, "system.brewfs.trash", b"{}", 0)
            .await
            .unwrap();
        Filesystem::setxattr(
            &fs,
            Request::default(),
            attr.ino as u64,
            OsStr::new("user.tmp"),
            b"v",
            0,
            0,
        )
        .await
        .unwrap();

        let err = Filesystem::removexattr(
            &fs,
            Request::default(),
            attr.ino as u64,
            OsStr::new("system.brewfs.trash"),
        )
        .await
        .unwrap_err();
        assert_eq!(err, Errno::from(libc::EPERM));
        assert!(
            fs.get_xattr_ino(attr.ino as i64, "system.brewfs.trash")
                .await
                .unwrap()
                .is_some(),
            "control-plane xattr must survive a rejected remove"
        );

        Filesystem::removexattr(
            &fs,
            Request::default(),
            attr.ino as u64,
            OsStr::new("user.tmp"),
        )
        .await
        .unwrap();
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
