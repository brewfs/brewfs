//! WinFsp mount adapter for BrewFS on Windows.
//!
//! This module bridges the WinFsp native Windows filesystem API (IRP-style
//! callbacks) to the cross-platform BrewFS [`VFS`](crate::vfs::fs::VFS) layer.
//! Linux/macOS keep using the asyncfuse path in `src/fuse/`; this module is
//! only compiled on Windows when the `fuse-winfsp` feature is enabled.
//!
//! # Feature / build notes
//! - `winfsp-sys` ships a built-in import library, so the crate compiles even
//!   when WinFsp is not installed. The final binary delay-loads
//!   `winfsp-x64.dll` and fails at runtime with a clear message if WinFsp is
//!   missing.
//! - `brewfs mount` on Windows requires WinFsp 2.x (service + driver) to be
//!   installed, and the mount point is either a drive letter (`Z:`) or an
//!   empty folder (e.g. `C:\mnt\brewfs`).

use std::ffi::c_void;
use std::future::Future;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::runtime::Handle;
use tracing::{debug, info, trace, warn};

use winfsp::filesystem::{
    AsyncFileSystemContext, DirBuffer, DirInfo, DirMarker, FileInfo, FileSecurity,
    FileSystemContext, OpenFileInfo, VolumeInfo, WideNameInfo,
};
use winfsp::host::{FileSystemHost, VolumeParams};
use winfsp::{FspError, U16CStr};

use crate::chunk::store::BlockStore;
use crate::meta::MetaLayer;
use crate::meta::store::{DirEntry, FileAttr, FileType, SetAttrFlags, SetAttrRequest};
use crate::vfs::error::VfsError;
use crate::vfs::fs::VFS;

// ---------------------------------------------------------------------------
// Windows constants (documented Win32/NT values; kept local so this module has
// no hard dependency on the `windows` crate surface).
// ---------------------------------------------------------------------------

/// FILE_ATTRIBUTE_DIRECTORY
const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x0000_0010;
/// FILE_ATTRIBUTE_ARCHIVE
const FILE_ATTRIBUTE_ARCHIVE: u32 = 0x0000_0020;
/// FILE_ATTRIBUTE_REPARSE_POINT
const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
/// FILE_ATTRIBUTE_READONLY
const FILE_ATTRIBUTE_READONLY: u32 = 0x0000_0001;

/// FILE_DIRECTORY_FILE create option.
const FILE_DIRECTORY_FILE: u32 = 0x0000_0001;
/// FILE_NON_DIRECTORY_FILE create option.
const FILE_NON_DIRECTORY_FILE: u32 = 0x0000_0040;
/// FILE_OPEN_BY_FILE_ID create option (not supported by BrewFS).
const FILE_OPEN_BY_FILE_ID: u32 = 0x0000_2000;

/// FILE_READ_DATA access right.
const FILE_READ_DATA: u32 = 0x0000_0001;
/// FILE_WRITE_DATA access right.
const FILE_WRITE_DATA: u32 = 0x0000_0002;
/// FILE_APPEND_DATA access right.
const FILE_APPEND_DATA: u32 = 0x0000_0004;
/// GENERIC_READ (may be passed pre-expansion on some paths).
const GENERIC_READ: u32 = 0x8000_0000;
/// GENERIC_WRITE.
const GENERIC_WRITE: u32 = 0x4000_0000;

// NTSTATUS codes used directly because WinFSP has no Win32 error for them.
const STATUS_FILE_IS_A_DIRECTORY: i32 = 0xC000_00BAu32 as i32;
const STATUS_NOT_A_DIRECTORY: i32 = 0xC000_0103u32 as i32;
const STATUS_UNEXPECTED_IO_ERROR: i32 = 0xC000_00E9u32 as i32;

// Win32 error codes (mapped to NTSTATUS by winfsp's FspNtStatusFromWin32).
//
// NOTE: for a missing file we must return ERROR_FILE_NOT_FOUND (2) and not
// ERROR_PATH_NOT_FOUND (3). WinFsp's OpenIf/OverwriteIf fallback logic checks
// for exactly STATUS_OBJECT_NAME_NOT_FOUND before retrying the request as a
// create; returning path-not-found makes it give up and the caller never gets
// its file created.
const WIN32_FILE_NOT_FOUND: i32 = 2;
const WIN32_PATH_NOT_FOUND: i32 = 3;
const WIN32_ACCESS_DENIED: i32 = 5;
const WIN32_DIR_NOT_EMPTY: i32 = 145;
const WIN32_ALREADY_EXISTS: i32 = 183;
const WIN32_INVALID_NAME: i32 = 123;
const WIN32_FILENAME_EXCED_RANGE: i32 = 206;
const WIN32_TOO_MANY_LINKS: i32 = 192;
const WIN32_DISK_FULL: i32 = 112;
const WIN32_INVALID_PARAMETER: i32 = 87;
const WIN32_NOT_SUPPORTED: i32 = 50;

/// Difference in seconds between the Windows FILETIME epoch (1601-01-01) and
/// the Unix epoch (1970-01-01).
const UNIX_TO_FILETIME_EPOCH_SECS: i64 = 11_644_473_600;

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

/// Convert a VFS error into the WinFsp error type.
fn map_vfs_error(e: &VfsError) -> FspError {
    match e {
        VfsError::NotFound { .. } => IoError::from_raw_os_error(WIN32_FILE_NOT_FOUND).into(),
        VfsError::AlreadyExists { .. } => IoError::from_raw_os_error(WIN32_ALREADY_EXISTS).into(),
        VfsError::NotADirectory { .. } => FspError::NTSTATUS(STATUS_NOT_A_DIRECTORY),
        VfsError::IsADirectory { .. } => FspError::NTSTATUS(STATUS_FILE_IS_A_DIRECTORY),
        VfsError::DirectoryNotEmpty { .. } => {
            IoError::from_raw_os_error(WIN32_DIR_NOT_EMPTY).into()
        }
        VfsError::PermissionDenied { .. } | VfsError::ReadOnlyFilesystem { .. } => {
            IoError::from_raw_os_error(WIN32_ACCESS_DENIED).into()
        }
        VfsError::StorageFull | VfsError::QuotaExceeded => {
            IoError::from_raw_os_error(WIN32_DISK_FULL).into()
        }
        VfsError::InvalidInput => IoError::from_raw_os_error(WIN32_INVALID_PARAMETER).into(),
        VfsError::InvalidFilename => IoError::from_raw_os_error(WIN32_INVALID_NAME).into(),
        VfsError::FilenameTooLong { .. } => {
            IoError::from_raw_os_error(WIN32_FILENAME_EXCED_RANGE).into()
        }
        VfsError::TooManyLinks => IoError::from_raw_os_error(WIN32_TOO_MANY_LINKS).into(),
        VfsError::Unsupported => IoError::from_raw_os_error(WIN32_NOT_SUPPORTED).into(),
        VfsError::ResourceBusy | VfsError::ExecutableFileBusy => {
            IoError::from_raw_os_error(WIN32_ACCESS_DENIED).into()
        }
        other => {
            warn!(error = ?other, "mapping unhandled VFS error to STATUS_UNEXPECTED_IO_ERROR");
            FspError::NTSTATUS(STATUS_UNEXPECTED_IO_ERROR)
        }
    }
}

/// Run an async VFS future to completion from a WinFsp dispatcher thread.
///
/// WinFsp invokes most `FileSystemContext` callbacks synchronously on its own
/// dispatcher threads, while BrewFS' VFS is tokio-based. `Handle::block_on`
/// drives the future on the current thread using the already-running tokio
/// runtime (reactor + timers), which is safe from a non-async thread.
fn block_on<F>(rt: &Handle, fut: F) -> F::Output
where
    F: Future,
{
    rt.block_on(fut)
}

// ---------------------------------------------------------------------------
// Path and time conversions
// ---------------------------------------------------------------------------

/// Convert a WinFsp UTF-16 path (e.g. `\foo\bar`, `foo\bar` or the empty root
/// path) into a POSIX path BrewFS understands (`/foo/bar`, `/`).
fn win_path_to_posix(name: &U16CStr) -> String {
    let s = name.to_string_lossy();
    if s.is_empty() {
        return "/".to_string();
    }
    let trimmed = s.trim_start_matches('\\');
    let replaced = trimmed.replace('\\', "/");
    if replaced.starts_with('/') {
        replaced
    } else {
        format!("/{replaced}")
    }
}

/// Parent directory of a normalized POSIX path. `/` stays `/`.
fn parent_posix(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".to_string();
    }
    match trimmed.rfind('/') {
        Some(0) | None => "/".to_string(),
        Some(idx) => trimmed[..idx].to_string(),
    }
}

/// Convert a Unix timestamp (seconds) to a Windows FILETIME (100ns since 1601).
fn unix_to_filetime(secs: i64) -> u64 {
    if secs <= 0 {
        return 0;
    }
    ((secs as i128 + UNIX_TO_FILETIME_EPOCH_SECS as i128) * 10_000_000) as u64
}

/// Convert a Windows FILETIME (100ns since 1601) to a Unix timestamp (seconds).
fn filetime_to_unix(ft: u64) -> i64 {
    if ft == 0 {
        return 0;
    }
    (ft / 10_000_000) as i64 - UNIX_TO_FILETIME_EPOCH_SECS
}

/// Map BrewFS file attributes onto Windows file attributes.
fn attr_to_win_attributes(attr: &FileAttr) -> u32 {
    let mut attrs = if attr.kind == FileType::Dir {
        FILE_ATTRIBUTE_DIRECTORY
    } else {
        FILE_ATTRIBUTE_ARCHIVE
    };
    if attr.kind == FileType::Symlink {
        attrs |= FILE_ATTRIBUTE_REPARSE_POINT;
    }
    if attr.kind == FileType::File && attr.mode & 0o222 == 0 {
        attrs |= FILE_ATTRIBUTE_READONLY;
    }
    attrs
}

/// Build a WinFsp `FileInfo` from BrewFS attributes.
fn attr_to_file_info(attr: &FileAttr) -> FileInfo {
    let mut fi = FileInfo::default();
    fi.file_attributes = attr_to_win_attributes(attr);
    fi.file_size = attr.size;
    // `blocks` is in 512-byte units.
    fi.allocation_size = attr.blocks.saturating_mul(512);
    // BrewFS has no birth time; use ctime as the creation time approximation.
    fi.creation_time = unix_to_filetime(attr.ctime);
    fi.last_access_time = unix_to_filetime(attr.atime);
    fi.last_write_time = unix_to_filetime(attr.mtime);
    fi.change_time = unix_to_filetime(attr.ctime);
    fi.index_number = attr.ino as u64;
    fi.hard_links = attr.nlink;
    fi
}

/// A simple `*`/`?` wildcard matcher used for WinFsp query-directory patterns.
fn wildcard_match(pattern: &str, name: &str) -> bool {
    fn inner(p: &[char], n: &[char]) -> bool {
        match (p.first(), n.first()) {
            (None, None) => true,
            (Some('*'), _) => inner(&p[1..], n) || (!n.is_empty() && inner(p, &n[1..])),
            (Some('?'), Some(_)) => inner(&p[1..], &n[1..]),
            (Some(a), Some(b)) if a == b => inner(&p[1..], &n[1..]),
            _ => false,
        }
    }
    inner(
        &pattern.chars().collect::<Vec<_>>(),
        &name.chars().collect::<Vec<_>>(),
    )
}

// ---------------------------------------------------------------------------
// FileSystemContext / AsyncFileSystemContext implementation
// ---------------------------------------------------------------------------

/// The open-handle context WinFsp stores for each opened file/directory.
pub struct BrewFsFileContext {
    ino: i64,
    posix_path: String,
    is_dir: bool,
    /// VFS file handle (regular files only).
    fh: Option<u64>,
    /// VFS directory handle (directories only).
    dir_fh: Option<u64>,
    /// Directory enumeration buffer managed by the WinFsp FSD.
    dir_buffer: DirBuffer,
    /// Set by `set_delete`; the actual deletion happens in `cleanup`.
    delete_on_close: AtomicBool,
}

/// The WinFsp filesystem context: owns the VFS and the tokio runtime handle.
pub struct BrewFsContext<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    vfs: VFS<S, M>,
    rt: Handle,
}

impl<S, M> BrewFsContext<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    /// Resolve a WinFsp path and return its attributes.
    fn stat_posix(&self, posix: &str) -> Result<FileAttr, FspError> {
        block_on(&self.rt, self.vfs.stat(posix)).map_err(|e| map_vfs_error(&e))
    }

    /// Resolve a WinFsp path and return its attributes (async variant).
    async fn stat_posix_async(&self, posix: &str) -> Result<FileAttr, FspError> {
        self.vfs.stat(posix).await.map_err(|e| map_vfs_error(&e))
    }

    /// Open a WinFsp file/directory path and build the handle context.
    fn open_path(
        &self,
        posix: String,
        attr: FileAttr,
        read: bool,
        write: bool,
        append: bool,
    ) -> Result<BrewFsFileContext, FspError> {
        let is_dir = attr.kind == FileType::Dir;
        let (fh, dir_fh) = if is_dir {
            let dir_fh =
                block_on(&self.rt, self.vfs.opendir(attr.ino)).map_err(|e| map_vfs_error(&e))?;
            (None, Some(dir_fh))
        } else {
            let fh = block_on(
                &self.rt,
                self.vfs.open(attr.ino, attr.clone(), read, write, append),
            )
            .map_err(|e| map_vfs_error(&e))?;
            (Some(fh), None)
        };
        Ok(BrewFsFileContext {
            ino: attr.ino,
            posix_path: posix,
            is_dir,
            fh,
            dir_fh,
            dir_buffer: DirBuffer::new(),
            delete_on_close: AtomicBool::new(false),
        })
    }
}

impl<S, M> FileSystemContext for BrewFsContext<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    type FileContext = BrewFsFileContext;

    fn get_security_by_name(
        &self,
        file_name: &U16CStr,
        _security_descriptor: Option<&mut [c_void]>,
        _reparse_point_resolver: impl FnOnce(&U16CStr) -> Option<FileSecurity>,
    ) -> winfsp::Result<FileSecurity> {
        // BrewFS does not support Windows ACLs (VolumeParams.persistent_acls is
        // false). Return a zero-length security descriptor, which tells WinFsp
        // to synthesize a default descriptor for the file.
        let posix = win_path_to_posix(file_name);
        let attr = self.stat_posix(&posix)?;
        Ok(FileSecurity {
            reparse: false,
            sz_security_descriptor: 0,
            attributes: attr_to_win_attributes(&attr),
        })
    }

    fn open(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        granted_access: u32,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        if create_options & FILE_OPEN_BY_FILE_ID != 0 {
            return Err(FspError::NTSTATUS(STATUS_NOT_A_DIRECTORY));
        }
        let posix = win_path_to_posix(file_name);
        let attr = self.stat_posix(&posix)?;
        let is_dir = attr.kind == FileType::Dir;

        // Enforce directory/non-directory expectations from CreateOptions.
        if create_options & FILE_DIRECTORY_FILE != 0 && !is_dir {
            return Err(FspError::NTSTATUS(STATUS_NOT_A_DIRECTORY));
        }
        if create_options & FILE_NON_DIRECTORY_FILE != 0 && is_dir {
            return Err(FspError::NTSTATUS(STATUS_FILE_IS_A_DIRECTORY));
        }

        let (read, write, append) = access_flags(granted_access);
        let ctx = self.open_path(posix.clone(), attr.clone(), read, write, append)?;
        *file_info.as_mut() = attr_to_file_info(&attr);
        Ok(ctx)
    }

    fn create(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        granted_access: u32,
        _file_attributes: u32,
        _security_descriptor: Option<&[c_void]>,
        _allocation_size: u64,
        _extra_buffer: Option<&[u8]>,
        _extra_buffer_is_reparse_point: bool,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        if create_options & FILE_OPEN_BY_FILE_ID != 0 {
            return Err(FspError::NTSTATUS(STATUS_NOT_A_DIRECTORY));
        }
        let posix = win_path_to_posix(file_name);
        let is_dir = create_options & FILE_DIRECTORY_FILE != 0;
        if is_dir {
            block_on(&self.rt, self.vfs.mkdir_err(&posix)).map_err(|e| map_vfs_error(&e))?;
        } else {
            block_on(&self.rt, self.vfs.create_file(&posix)).map_err(|e| map_vfs_error(&e))?;
        }

        let attr = self.stat_posix(&posix)?;
        let (read, write, append) = access_flags(granted_access);
        let ctx = self.open_path(posix.clone(), attr.clone(), read, write, append)?;
        *file_info.as_mut() = attr_to_file_info(&attr);
        Ok(ctx)
    }

    fn cleanup(&self, context: &Self::FileContext, _file_name: Option<&U16CStr>, flags: u32) {
        let delete_requested = context.delete_on_close.load(Ordering::Acquire)
            || winfsp::constants::FspCleanupFlags::FspCleanupDelete.is_flagged(flags);
        if !delete_requested {
            return;
        }
        let path = context.posix_path.clone();
        let log_path = path.clone();
        let is_dir = context.is_dir;
        let vfs = &self.vfs;
        let rt = &self.rt;
        let result = block_on(rt, async move {
            if is_dir {
                // Windows shells delegate deleting a directory tree to the
                // file system (delete-on-close on the directory handle). A
                // plain rmdir would fail with DirectoryNotEmpty and the shell
                // would report a delete that never happened; recurse instead.
                vfs.remove_dir_all(&path).await
            } else {
                vfs.unlink(&path).await
            }
        });
        match result {
            Ok(()) => trace!(path = log_path, "winfsp cleanup deleted"),
            Err(e) => warn!(path = log_path, error = ?e, "winfsp cleanup delete failed"),
        }
    }

    fn close(&self, context: Self::FileContext) {
        let rt = &self.rt;
        let vfs = &self.vfs;
        if let Some(dir_fh) = context.dir_fh {
            if let Err(e) = vfs.closedir(dir_fh) {
                warn!(dir_fh, error = ?e, "winfsp close closedir failed");
            }
        }
        if let Some(fh) = context.fh {
            if let Err(e) = block_on(rt, vfs.close(fh)) {
                warn!(fh, error = ?e, "winfsp close failed");
            }
        }
    }

    fn flush(
        &self,
        context: Option<&Self::FileContext>,
        _file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let Some(ctx) = context else {
            // Whole-volume flush: nothing to do at this layer.
            return Ok(());
        };
        let Some(fh) = ctx.fh else { return Ok(()) };
        block_on(&self.rt, self.vfs.flush(fh)).map_err(|e| map_vfs_error(&e))
    }

    fn get_file_info(
        &self,
        context: &Self::FileContext,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let attr = self.stat_posix(&context.posix_path)?;
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn overwrite(
        &self,
        context: &Self::FileContext,
        _file_attributes: u32,
        _replace_file_attributes: bool,
        _allocation_size: u64,
        _extra_buffer: Option<&[u8]>,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        if context.is_dir {
            return Err(FspError::NTSTATUS(STATUS_FILE_IS_A_DIRECTORY));
        }
        block_on(&self.rt, self.vfs.truncate(&context.posix_path, 0))
            .map_err(|e| map_vfs_error(&e))?;
        let attr = self.stat_posix(&context.posix_path)?;
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn rename(
        &self,
        _context: &Self::FileContext,
        file_name: &U16CStr,
        new_file_name: &U16CStr,
        replace_if_exists: bool,
    ) -> winfsp::Result<()> {
        let old = win_path_to_posix(file_name);
        let new = win_path_to_posix(new_file_name);
        let result = if replace_if_exists {
            block_on(&self.rt, self.vfs.rename(&old, &new))
        } else {
            block_on(&self.rt, self.vfs.rename_noreplace(&old, &new))
        };
        result.map_err(|e| map_vfs_error(&e))
    }

    fn set_basic_info(
        &self,
        context: &Self::FileContext,
        _file_attributes: u32,
        _creation_time: u64,
        last_access_time: u64,
        last_write_time: u64,
        _last_change_time: u64,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        // WinFsp passes 0 for times that should not be changed.
        let mut request = SetAttrRequest::default();
        if last_access_time != 0 {
            request.atime = Some(filetime_to_unix(last_access_time));
        }
        if last_write_time != 0 {
            request.mtime = Some(filetime_to_unix(last_write_time));
        }
        block_on(
            &self.rt,
            self.vfs
                .set_attr(context.ino, &request, SetAttrFlags::empty()),
        )
        .map_err(|e| map_vfs_error(&e))?;
        let attr = self.stat_posix(&context.posix_path)?;
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn set_delete(
        &self,
        context: &Self::FileContext,
        _file_name: &U16CStr,
        delete_file: bool,
    ) -> winfsp::Result<()> {
        context
            .delete_on_close
            .store(delete_file, Ordering::Release);
        Ok(())
    }

    fn set_file_size(
        &self,
        context: &Self::FileContext,
        new_size: u64,
        _set_allocation_size: bool,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        if context.is_dir {
            return Err(FspError::NTSTATUS(STATUS_FILE_IS_A_DIRECTORY));
        }
        block_on(&self.rt, self.vfs.truncate(&context.posix_path, new_size))
            .map_err(|e| map_vfs_error(&e))?;
        let attr = self.stat_posix(&context.posix_path)?;
        *file_info = attr_to_file_info(&attr);
        Ok(())
    }

    fn get_volume_info(&self, out_volume_info: &mut VolumeInfo) -> winfsp::Result<()> {
        let snap = block_on(&self.rt, self.vfs.stat_fs()).map_err(|e| map_vfs_error(&e))?;
        out_volume_info.total_size = snap.total_space;
        out_volume_info.free_size = snap.available_space;
        out_volume_info.set_volume_label("BrewFS");
        Ok(())
    }
}

impl<S, M> AsyncFileSystemContext for BrewFsContext<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    fn spawn_task(&self, future: impl Future<Output = ()> + Send + 'static) {
        self.rt.spawn(future);
    }

    async fn read_async(
        &self,
        context: &Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> winfsp::Result<u32> {
        let fh = context
            .fh
            .ok_or_else(|| FspError::from(IoError::from_raw_os_error(WIN32_ACCESS_DENIED)))?;
        if buffer.is_empty() {
            return Ok(0);
        }
        let data = self
            .vfs
            .read(fh, offset, buffer.len())
            .await
            .map_err(|e| map_vfs_error(&e))?;
        let n = data.len().min(buffer.len());
        buffer[..n].copy_from_slice(&data[..n]);
        Ok(n as u32)
    }

    async fn write_async(
        &self,
        context: &Self::FileContext,
        buffer: &[u8],
        offset: u64,
        write_to_eof: bool,
        _constrained_io: bool,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<u32> {
        let fh = context
            .fh
            .ok_or_else(|| FspError::from(IoError::from_raw_os_error(WIN32_ACCESS_DENIED)))?;
        if buffer.is_empty() {
            return Ok(0);
        }
        // `write_to_eof` means the write is anchored at the current end of
        // file, so resolve the offset from the current attribute.
        let effective_offset = if write_to_eof {
            let attr = self.stat_posix_async(&context.posix_path).await?;
            attr.size
        } else {
            offset
        };
        let written = self
            .vfs
            .write(fh, effective_offset, buffer)
            .await
            .map_err(|e| map_vfs_error(&e))?;
        let attr = self.stat_posix_async(&context.posix_path).await?;
        *file_info = attr_to_file_info(&attr);
        Ok(written as u32)
    }

    async fn read_directory_async(
        &self,
        context: &Self::FileContext,
        pattern: Option<&U16CStr>,
        marker: DirMarker<'_>,
        buffer: &mut [u8],
    ) -> winfsp::Result<u32> {
        let dir_fh = context
            .dir_fh
            .ok_or_else(|| FspError::from(IoError::from_raw_os_error(WIN32_ACCESS_DENIED)))?;

        // Read the full directory listing (paged in 256-entry batches).
        let mut entries: Vec<DirEntry> = Vec::new();
        let mut offset = 0u64;
        loop {
            let batch = self.vfs.readdir(dir_fh, offset).unwrap_or_default();
            if batch.is_empty() {
                break;
            }
            let batch_len = batch.len() as u64;
            entries.extend(batch);
            offset += batch_len;
            if batch_len < 256 {
                break;
            }
        }

        // Build the listing with "." and ".." first (except for the root).
        let is_root = context.posix_path == "/";
        let mut listing: Vec<(String, FileAttr)> = Vec::with_capacity(entries.len() + 2);
        if !is_root {
            if let Ok(dot_attr) = self.stat_posix_async(&context.posix_path).await {
                listing.push((".".to_string(), dot_attr));
            }
            let parent = parent_posix(&context.posix_path);
            if let Ok(dotdot_attr) = self.stat_posix_async(&parent).await {
                listing.push(("..".to_string(), dotdot_attr));
            }
        }
        for entry in entries {
            let full = if context.posix_path == "/" {
                format!("/{}", entry.name)
            } else {
                format!("{}/{}", context.posix_path, entry.name)
            };
            match self.stat_posix_async(&full).await {
                Ok(attr) => listing.push((entry.name, attr)),
                Err(_) => listing.push((entry.name, attr_from_kind(entry.kind, entry.ino))),
            }
        }

        // Apply the query pattern (if any).
        let pat = pattern.map(|p| p.to_string_lossy());
        if let Some(pat) = pat.as_deref() {
            listing.retain(|(name, _)| wildcard_match(pat, name));
        }

        // Find where to resume based on the marker.
        let start = match marker.inner() {
            Some(name) => {
                let name = String::from_utf16_lossy(name);
                listing
                    .iter()
                    .position(|(n, _)| *n == name)
                    .map(|i| i + 1)
                    .unwrap_or(0)
            }
            None => 0,
        };

        // Fill the WinFsp directory buffer.
        let mut lock = context
            .dir_buffer
            .acquire(marker.is_none(), Some(buffer.len() as u32))?;
        for (name, attr) in listing.iter().skip(start) {
            let mut di = DirInfo::<255>::new();
            if let Err(e) = di.set_name(name) {
                debug!(name, error = ?e, "winfsp readdir entry name too long");
                continue;
            }
            *di.file_info_mut() = attr_to_file_info(attr);
            lock.write(&mut di)?;
        }
        drop(lock);

        Ok(context.dir_buffer.read(marker, buffer))
    }
}

/// Attribute fallback for a directory entry that could not be fully resolved.
fn attr_from_kind(kind: FileType, ino: i64) -> FileAttr {
    FileAttr {
        ino,
        size: 0,
        blocks: 0,
        kind,
        mode: 0o644,
        rdev: 0,
        uid: 0,
        gid: 0,
        atime: 0,
        mtime: 0,
        ctime: 0,
        nlink: 1,
    }
}

// ---------------------------------------------------------------------------
// WinFsp DLL discovery
// ---------------------------------------------------------------------------

/// WinFsp installs its user-mode DLL to `%ProgramFiles(x86)%\WinFsp\bin` but
/// does not add that directory to PATH. The `winfsp` crate loads
/// `winfsp-x64.dll` by bare name inside `winfsp_init`, so point this process's
/// DLL search path at the installed bin directory first via
/// `SetDllDirectoryW`. This keeps the crate compilable without WinFsp
/// installed while still loading the system WinFsp at runtime.
fn ensure_winfsp_dll_discoverable() {
    let mut candidates = Vec::new();
    if let Some(pf) = std::env::var_os("PROGRAMFILES(X86)") {
        candidates.push(PathBuf::from(pf).join("WinFsp").join("bin"));
    }
    if let Some(pf) = std::env::var_os("PROGRAMFILES") {
        candidates.push(PathBuf::from(pf).join("WinFsp").join("bin"));
    }
    let Some(bin) = candidates
        .into_iter()
        .find(|dir| dir.join("winfsp-x64.dll").exists())
    else {
        // WinFsp is not installed; winfsp_init will surface the error.
        return;
    };

    use std::os::windows::ffi::OsStrExt;
    let wide: Vec<u16> = bin.as_os_str().encode_wide().chain(Some(0)).collect();
    // SAFETY: SetDllDirectoryW only affects this process's DLL search path,
    // and `wide` is a valid nul-terminated buffer for the duration of the call.
    unsafe {
        SetDllDirectoryW(wide.as_ptr());
    }
}

/// Win32 `SetDllDirectoryW` (kernel32).
#[cfg(windows)]
unsafe extern "system" {
    #[link(name = "kernel32")]
    fn SetDllDirectoryW(lp_path_name: *const u16) -> i32;

    #[link(name = "shell32")]
    fn SHChangeNotify(
        w_event_id: i32,
        u_flags: u32,
        dw_item1: *const std::ffi::c_void,
        dw_item2: *const std::ffi::c_void,
    );
}

// ---------------------------------------------------------------------------
// Mount entry point
// ---------------------------------------------------------------------------

/// Mount a BrewFS `VFS` through WinFsp at `mount_point` (a drive letter such
/// as `Z:` or an empty folder). Blocks until Ctrl+C or a control-plane
/// shutdown signal, then unmounts.
pub async fn mount_vfs_winfsp<S, M>(
    fs: VFS<S, M>,
    mount_point: &Path,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    // Make the installed WinFsp bin directory discoverable before loading the
    // delay-loaded DLL (the installer does not add it to PATH).
    ensure_winfsp_dll_discoverable();

    // winfsp_init loads winfsp-x64.dll; fail with a clear message when WinFsp
    // is not installed.
    winfsp::winfsp_init()
        .map_err(|e| anyhow::anyhow!("WinFsp is not installed or could not be loaded: {e}"))?;

    let rt = Handle::current();
    let volume_params = build_volume_params();
    let context = BrewFsContext { vfs: fs, rt };
    let mut host = FileSystemHost::new_async(volume_params, context)
        .map_err(|e| anyhow::anyhow!("failed to create WinFsp filesystem host: {e}"))?;

    host.mount(mount_point)
        .map_err(|e| anyhow::anyhow!("failed to mount at {}: {e}", mount_point.display()))?;
    if let Err(e) = host.start() {
        host.unmount();
        return Err(anyhow::anyhow!("failed to start WinFsp dispatcher: {e}"));
    }

    info!(mount_point = %mount_point.display(), "brewfs mounted via WinFsp");
    println!("mounted at {}", mount_point.display());

    tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            signal?;
            println!("unmounting...");
        }
        changed = shutdown.changed() => {
            changed?;
            println!("unmounting via control plane...");
        }
    }

    host.stop();
    host.unmount();
    notify_drive_removed(mount_point);
    Ok(())
}

/// Tell the Windows shell that `mount_point` was just removed so Explorer
/// drops the stale drive entry immediately instead of showing a ghost drive
/// until the user refreshes.
#[cfg(windows)]
fn notify_drive_removed(mount_point: &Path) {
    const SHCNE_DRIVEREMOVED: i32 = 0x0000_0080;
    const SHCNF_PATHW: u32 = 0x0005;
    const SHCNF_FLUSH: u32 = 0x1000;

    let Some(wide) = to_wide(mount_point) else {
        return;
    };
    // SAFETY: SHChangeNotify is a shell32 broadcast; the wide string is a
    // valid NUL-terminated buffer kept alive for the duration of the call.
    unsafe {
        SHChangeNotify(
            SHCNE_DRIVEREMOVED,
            SHCNF_PATHW | SHCNF_FLUSH,
            wide.as_ptr() as *const std::ffi::c_void,
            std::ptr::null(),
        );
    }
}

/// Encode a mount point as a NUL-terminated UTF-16 string suitable for
/// shell32 APIs (drive letters like `Z:` are passed as-is).
fn to_wide(path: &Path) -> Option<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt;
    let mut wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    if wide.is_empty() {
        return None;
    }
    wide.push(0);
    Some(wide)
}

/// Build the WinFsp volume parameters for BrewFS.
fn build_volume_params() -> VolumeParams {
    let mut vp = VolumeParams::new();
    vp.sector_size(512)
        .sectors_per_allocation_unit(8)
        .max_component_length(255)
        .filesystem_name("BrewFS")
        .case_sensitive_search(true)
        .case_preserved_names(true)
        .unicode_on_disk(true)
        .persistent_acls(false)
        .reparse_points(false)
        .post_cleanup_when_modified_only(true)
        .flush_and_purge_on_cleanup(true)
        .pass_query_directory_pattern(true)
        .file_info_timeout(1000)
        .dir_info_timeout(1000);
    vp
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Decode the access flags from a WinFsp granted access mask.
fn access_flags(granted_access: u32) -> (bool, bool, bool) {
    let read = granted_access & (GENERIC_READ | FILE_READ_DATA) != 0;
    let write = granted_access & (GENERIC_WRITE | FILE_WRITE_DATA | FILE_APPEND_DATA) != 0;
    let append = granted_access & FILE_APPEND_DATA != 0;
    (read, write, append)
}

// ---------------------------------------------------------------------------
// Tests (pure helpers; no WinFsp installation required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::error::PathHint;

    /// Build a nul-terminated U16CStr from a Rust string for tests.
    fn w(s: &str) -> &'static U16CStr {
        let mut units: Vec<u16> = s.encode_utf16().collect();
        units.push(0);
        // Leak a tiny buffer: tests only, and the strings are compile-time-ish.
        let leaked: &'static mut [u16] = Box::leak(units.into_boxed_slice());
        U16CStr::from_slice(leaked).unwrap()
    }

    #[test]
    fn converts_win_paths_to_posix() {
        // Empty name is the root directory.
        assert_eq!(win_path_to_posix(w("")), "/");
        assert_eq!(win_path_to_posix(w("\\")), "/");
        assert_eq!(win_path_to_posix(w("\\foo\\bar\\baz")), "/foo/bar/baz");
        assert_eq!(win_path_to_posix(w("foo\\bar")), "/foo/bar");
    }

    #[test]
    fn computes_parent_paths() {
        assert_eq!(parent_posix("/"), "/");
        assert_eq!(parent_posix("/a"), "/");
        assert_eq!(parent_posix("/a/b/c"), "/a/b");
        assert_eq!(parent_posix("/a/b/"), "/a");
    }

    #[test]
    fn converts_between_unix_and_filetime() {
        // Unix epoch is 11644473600 seconds after the FILETIME epoch.
        assert_eq!(unix_to_filetime(0), 0);
        assert_eq!(
            unix_to_filetime(1),
            ((UNIX_TO_FILETIME_EPOCH_SECS + 1) as u64) * 10_000_000
        );
        assert_eq!(
            filetime_to_unix(unix_to_filetime(1_700_000_000)),
            1_700_000_000
        );
        assert_eq!(filetime_to_unix(0), 0);
    }

    #[test]
    fn wildcard_matching_handles_star_and_question() {
        assert!(wildcard_match("*.txt", "readme.txt"));
        assert!(!wildcard_match("*.txt", "readme.md"));
        assert!(wildcard_match("?at", "cat"));
        assert!(!wildcard_match("?at", "chat"));
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("file", "file"));
        assert!(!wildcard_match("file", "file1"));
    }

    #[test]
    fn maps_vfs_errors_to_win32_codes() {
        let e = map_vfs_error(&VfsError::NotFound {
            path: PathHint::none(),
        });
        assert!(matches!(e, FspError::WIN32(code) if code == WIN32_FILE_NOT_FOUND as u32));

        let e = map_vfs_error(&VfsError::AlreadyExists {
            path: PathHint::none(),
        });
        assert!(matches!(e, FspError::WIN32(code) if code == WIN32_ALREADY_EXISTS as u32));

        let e = map_vfs_error(&VfsError::IsADirectory {
            path: PathHint::none(),
        });
        assert!(matches!(e, FspError::NTSTATUS(code) if code == STATUS_FILE_IS_A_DIRECTORY));

        let e = map_vfs_error(&VfsError::PermissionDenied {
            path: PathHint::none(),
        });
        assert!(matches!(e, FspError::WIN32(code) if code == WIN32_ACCESS_DENIED as u32));
    }

    #[test]
    fn attribute_mapping_sets_directory_and_size() {
        let attr = FileAttr {
            ino: 42,
            size: 4096,
            blocks: 8,
            kind: FileType::File,
            mode: 0o644,
            rdev: 0,
            uid: 1000,
            gid: 1000,
            atime: 1_700_000_000,
            mtime: 1_700_000_001,
            ctime: 1_700_000_002,
            nlink: 1,
        };
        let fi = attr_to_file_info(&attr);
        assert_eq!(fi.file_size, 4096);
        assert_eq!(fi.allocation_size, 4096);
        assert_eq!(fi.index_number, 42);
        assert_eq!(fi.hard_links, 1);
        assert_eq!(fi.file_attributes & FILE_ATTRIBUTE_DIRECTORY, 0);
        assert_eq!(filetime_to_unix(fi.last_write_time), 1_700_000_001);

        let dir = FileAttr {
            kind: FileType::Dir,
            ..attr
        };
        assert_ne!(
            attr_to_file_info(&dir).file_attributes & FILE_ATTRIBUTE_DIRECTORY,
            0
        );
    }

    #[test]
    fn access_flag_decoding_covers_generic_and_specific_rights() {
        assert_eq!(access_flags(FILE_READ_DATA), (true, false, false));
        assert_eq!(access_flags(GENERIC_WRITE), (false, true, false));
        assert_eq!(
            access_flags(FILE_READ_DATA | FILE_WRITE_DATA),
            (true, true, false)
        );
        assert_eq!(access_flags(FILE_APPEND_DATA), (false, true, true));
    }
}
