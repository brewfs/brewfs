//! Windows WinFsp mount adapter for the metadata-less object filesystem.
//!
//! Bridges WinFsp IRP callbacks to [`ObjectFs`](super::ObjectFs). Writes are
//! buffered in memory and flushed as a whole-object `PutObject` on
//! close/flush — the same "cloud drive" semantics as ossfs/s3fs.

use std::ffi::c_void;
use std::future::Future;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::runtime::Handle;
use tracing::{debug, info, warn};

use winfsp::filesystem::{
    AsyncFileSystemContext, DirBuffer, DirInfo, DirMarker, FileInfo, FileSecurity,
    FileSystemContext, OpenFileInfo, VolumeInfo, WideNameInfo,
};
use winfsp::host::{FileSystemHost, VolumeParams};
use winfsp::{FspError, U16CStr};

use super::{DirEntry, ObjectFs};

const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x0000_0010;
const FILE_ATTRIBUTE_ARCHIVE: u32 = 0x0000_0020;
const FILE_ATTRIBUTE_READONLY: u32 = 0x0000_0001;
const FILE_DIRECTORY_FILE: u32 = 0x0000_0001;
const FILE_NON_DIRECTORY_FILE: u32 = 0x0000_0040;

const WIN32_FILE_NOT_FOUND: i32 = 2;
const WIN32_ACCESS_DENIED: i32 = 5;
const WIN32_NOT_SUPPORTED: i32 = 50;
const WIN32_INVALID_PARAMETER: i32 = 87;

const UNIX_TO_FILETIME_EPOCH_SECS: i64 = 11_644_473_600;

/// Convert a Unix timestamp (seconds) to Windows FILETIME (100ns since 1601).
fn unix_to_filetime(secs: i64) -> u64 {
    if secs <= 0 {
        return 0;
    }
    ((secs as i128 + UNIX_TO_FILETIME_EPOCH_SECS as i128) * 10_000_000) as u64
}

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

fn file_info_from(entry: &DirEntry, index: u64) -> FileInfo {
    let mut fi = FileInfo::default();
    fi.file_attributes = if entry.is_dir {
        FILE_ATTRIBUTE_DIRECTORY
    } else {
        FILE_ATTRIBUTE_ARCHIVE
    };
    fi.file_size = entry.size;
    fi.allocation_size = entry.size;
    fi.creation_time = unix_to_filetime(entry.mtime_secs);
    fi.last_access_time = unix_to_filetime(entry.mtime_secs);
    fi.last_write_time = unix_to_filetime(entry.mtime_secs);
    fi.change_time = unix_to_filetime(entry.mtime_secs);
    fi.index_number = index;
    fi.hard_links = 1;
    fi
}

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

/// Per-open-file state. Writes are buffered whole-file; reads go straight to
/// the object store unless the file is open for write.
pub struct OssFileContext {
    path: String,
    is_dir: bool,
    write_buf: Mutex<Option<Vec<u8>>>,
    dirty: AtomicBool,
    delete_on_close: AtomicBool,
    dir_buffer: DirBuffer,
}

impl OssFileContext {
    fn index(&self) -> u64 {
        // Stable-ish per-path index derived from the string.
        self.path.as_bytes().iter().fold(0x9E37_79B9u64, |acc, b| {
            acc.wrapping_mul(31).wrapping_add(*b as u64)
        })
    }
}

pub struct OssMountContext {
    fs: Arc<ObjectFs>,
    rt: Handle,
    mount_point: PathBuf,
}

impl OssMountContext {
    fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: Future,
    {
        self.rt.block_on(fut)
    }
}

/// Mount the object filesystem at `mount_point` via WinFsp. Blocks until
/// Ctrl+C or the process receives a termination signal.
pub async fn mount_oss_winfsp(fs: Arc<ObjectFs>, mount_point: &Path) -> anyhow::Result<()> {
    ensure_winfsp_dll_discoverable();
    winfsp::winfsp_init()
        .map_err(|e| anyhow::anyhow!("WinFsp is not installed or could not be loaded: {e}"))?;

    // Verify the bucket is reachable and the prefix lists cleanly BEFORE
    // mounting. Without this, a misconfigured endpoint (e.g. an Aliyun OSS
    // access-point URL that the SDK cannot address) mounts a volume whose
    // every operation fails with a generic I/O error.
    match fs.list("/").await {
        Ok(_) => {}
        Err(e) => {
            eprintln!(
                "ossmount: S3 连通性检查失败，拒绝挂载。请检查 endpoint/bucket/密钥配置：{e:?}"
            );
            anyhow::bail!("S3 connectivity check failed: {e:?}");
        }
    }

    let rt = Handle::current();
    let context = OssMountContext {
        fs,
        rt,
        mount_point: mount_point.to_path_buf(),
    };
    let mut host = FileSystemHost::new_async(build_volume_params(), context)
        .map_err(|e| anyhow::anyhow!("failed to create WinFsp filesystem host: {e}"))?;

    host.mount(mount_point)
        .map_err(|e| anyhow::anyhow!("failed to mount at {}: {e}", mount_point.display()))?;
    if let Err(e) = host.start() {
        host.unmount();
        return Err(anyhow::anyhow!("failed to start WinFsp dispatcher: {e}"));
    }

    info!(mount_point = %mount_point.display(), "brewfs-oss mounted via WinFsp");
    println!("mounted at {}", mount_point.display());
    write_runtime_record(mount_point);

    tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            signal?;
            println!("unmounting...");
        }
    }

    host.stop();
    host.unmount();
    remove_runtime_record();
    Ok(())
}

fn build_volume_params() -> VolumeParams {
    let mut vp = VolumeParams::new();
    vp.sector_size(512)
        .sectors_per_allocation_unit(8)
        .max_component_length(255)
        .filesystem_name("BrewFS-OSS")
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

impl FileSystemContext for OssMountContext {
    type FileContext = OssFileContext;

    fn get_security_by_name(
        &self,
        file_name: &U16CStr,
        _security_descriptor: Option<&mut [c_void]>,
        _reparse_point_resolver: impl FnOnce(&U16CStr) -> Option<FileSecurity>,
    ) -> winfsp::Result<FileSecurity> {
        let posix = win_path_to_posix(file_name);
        let entry = self
            .block_on(self.fs.stat(&posix))
            .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
        let entry = entry
            .ok_or_else(|| FspError::from(IoError::from_raw_os_error(WIN32_FILE_NOT_FOUND)))?;
        Ok(FileSecurity {
            reparse: false,
            sz_security_descriptor: 0,
            attributes: if entry.is_dir {
                FILE_ATTRIBUTE_DIRECTORY
            } else {
                FILE_ATTRIBUTE_ARCHIVE
            },
        })
    }

    fn open(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        granted_access: u32,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        let posix = win_path_to_posix(file_name);
        let entry = self
            .block_on(self.fs.stat(&posix))
            .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
        let entry = entry
            .ok_or_else(|| FspError::from(IoError::from_raw_os_error(WIN32_FILE_NOT_FOUND)))?;
        let is_dir = entry.is_dir;
        if create_options & FILE_DIRECTORY_FILE != 0 && !is_dir {
            return Err(FspError::NTSTATUS(0xC000_00BAu32 as i32)); // STATUS_FILE_IS_A_DIRECTORY
        }
        if create_options & FILE_NON_DIRECTORY_FILE != 0 && is_dir {
            return Err(FspError::NTSTATUS(0xC000_0103u32 as i32)); // STATUS_NOT_A_DIRECTORY
        }

        let write = granted_access & 0x2 != 0 || granted_access & 0x4000_0000 != 0;
        let write_buf = if is_dir {
            None
        } else if write {
            // Load existing content so the caller can modify in place.
            Some(
                self.block_on(self.fs.read_range(&posix, 0, usize::MAX))
                    .unwrap_or_default(),
            )
        } else {
            None
        };
        *file_info.as_mut() = file_info_from(&entry, file_index(&posix));
        Ok(OssFileContext {
            path: posix,
            is_dir,
            write_buf: Mutex::new(write_buf),
            dirty: AtomicBool::new(false),
            delete_on_close: AtomicBool::new(false),
            dir_buffer: DirBuffer::new(),
        })
    }

    fn create(
        &self,
        file_name: &U16CStr,
        create_options: u32,
        _granted_access: u32,
        _file_attributes: u32,
        _security_descriptor: Option<&[c_void]>,
        _allocation_size: u64,
        _extra_buffer: Option<&[u8]>,
        _extra_buffer_is_reparse_point: bool,
        file_info: &mut OpenFileInfo,
    ) -> winfsp::Result<Self::FileContext> {
        let posix = win_path_to_posix(file_name);
        let is_dir = create_options & FILE_DIRECTORY_FILE != 0;
        if is_dir {
            self.block_on(self.fs.mkdir(&posix))
                .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
        }
        let entry = DirEntry {
            name: posix.clone(),
            is_dir,
            size: 0,
            mtime_secs: 0,
        };
        let write_buf = if is_dir { None } else { Some(Vec::new()) };
        *file_info.as_mut() = file_info_from(&entry, file_index(&posix));
        Ok(OssFileContext {
            path: posix,
            is_dir,
            write_buf: Mutex::new(write_buf),
            dirty: AtomicBool::new(false),
            delete_on_close: AtomicBool::new(false),
            dir_buffer: DirBuffer::new(),
        })
    }

    fn cleanup(&self, context: &Self::FileContext, _file_name: Option<&U16CStr>, flags: u32) {
        let delete_requested = context.delete_on_close.load(Ordering::Acquire)
            || winfsp::constants::FspCleanupFlags::FspCleanupDelete.is_flagged(flags);
        if delete_requested {
            let path = context.path.clone();
            let is_dir = context.is_dir;
            let fs = Arc::clone(&self.fs);
            let result = self.block_on({
                let path = path.clone();
                async move {
                    if is_dir {
                        fs.delete_dir_recursive(&path).await
                    } else {
                        fs.delete(&path).await
                    }
                }
            });
            match result {
                Ok(()) => debug!(path = log_path(&path), "ossfs cleanup deleted"),
                Err(e) => warn!(path = log_path(&path), error = ?e, "ossfs cleanup delete failed"),
            }
            return;
        }
        if context.dirty.load(Ordering::Acquire) {
            let data = context.write_buf.lock().unwrap().clone();
            if let Some(data) = data {
                let path = context.path.clone();
                let fs = Arc::clone(&self.fs);
                if let Err(e) = self.block_on({
                    let path = path.clone();
                    async move { fs.write(&path, &data).await }
                }) {
                    warn!(path = log_path(&path), error = ?e, "ossfs cleanup flush failed");
                }
            }
        }
    }

    fn close(&self, _context: Self::FileContext) {}

    fn flush(
        &self,
        context: Option<&Self::FileContext>,
        _file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        let Some(ctx) = context else { return Ok(()) };
        if ctx.dirty.load(Ordering::Acquire) {
            let data = ctx.write_buf.lock().unwrap().clone();
            if let Some(data) = data {
                let path = ctx.path.clone();
                let fs = Arc::clone(&self.fs);
                self.block_on({
                    let path = path.clone();
                    async move { fs.write(&path, &data).await }
                })
                .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
            }
        }
        Ok(())
    }

    fn get_file_info(
        &self,
        context: &Self::FileContext,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        if let Some(buf) = context.write_buf.lock().unwrap().as_ref() {
            *file_info = file_info_from(
                &DirEntry {
                    name: context.path.clone(),
                    is_dir: context.is_dir,
                    size: buf.len() as u64,
                    mtime_secs: 0,
                },
                context.index(),
            );
            return Ok(());
        }
        let entry = self
            .block_on(self.fs.stat(&context.path))
            .map_err(|e| FspError::from(IoError::other(e.to_string())))?
            .ok_or_else(|| FspError::from(IoError::from_raw_os_error(WIN32_FILE_NOT_FOUND)))?;
        *file_info = file_info_from(&entry, context.index());
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
            return Err(FspError::NTSTATUS(0xC000_00BAu32 as i32));
        }
        if let Some(buf) = context.write_buf.lock().unwrap().as_mut() {
            buf.clear();
        }
        context.dirty.store(true, Ordering::Release);
        let entry = DirEntry {
            name: context.path.clone(),
            is_dir: false,
            size: 0,
            mtime_secs: 0,
        };
        *file_info = file_info_from(&entry, context.index());
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
        let fs = Arc::clone(&self.fs);
        self.block_on(async move { fs.rename(&old, &new).await })
            .map_err(|e| FspError::from(IoError::other(e.to_string())))?;
        let _ = replace_if_exists;
        Ok(())
    }

    fn set_basic_info(
        &self,
        context: &Self::FileContext,
        _file_attributes: u32,
        _creation_time: u64,
        _last_access_time: u64,
        _last_write_time: u64,
        _last_change_time: u64,
        file_info: &mut FileInfo,
    ) -> winfsp::Result<()> {
        // Object storage has no settable timestamps; nothing to do.
        let entry = self
            .block_on(self.fs.stat(&context.path))
            .map_err(|e| FspError::from(IoError::other(e.to_string())))?
            .unwrap_or(DirEntry {
                name: context.path.clone(),
                is_dir: context.is_dir,
                size: context
                    .write_buf
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|b| b.len() as u64)
                    .unwrap_or(0),
                mtime_secs: 0,
            });
        *file_info = file_info_from(&entry, context.index());
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
            return Err(FspError::NTSTATUS(0xC000_00BAu32 as i32));
        }
        let mut guard = context.write_buf.lock().unwrap();
        let Some(buf) = guard.as_mut() else {
            // No write handle: truncation would require a read-modify-write.
            return Err(FspError::from(IoError::from_raw_os_error(
                WIN32_NOT_SUPPORTED,
            )));
        };
        buf.resize(new_size as usize, 0);
        context.dirty.store(true, Ordering::Release);
        let entry = DirEntry {
            name: context.path.clone(),
            is_dir: false,
            size: buf.len() as u64,
            mtime_secs: 0,
        };
        *file_info = file_info_from(&entry, context.index());
        Ok(())
    }

    fn get_volume_info(&self, out_volume_info: &mut VolumeInfo) -> winfsp::Result<()> {
        out_volume_info.total_size = 1 << 50;
        out_volume_info.free_size = 1 << 50;
        out_volume_info.set_volume_label("BrewFS-OSS");
        Ok(())
    }
}

impl AsyncFileSystemContext for OssMountContext {
    fn spawn_task(&self, future: impl Future<Output = ()> + Send + 'static) {
        self.rt.spawn(future);
    }

    async fn read_async(
        &self,
        context: &Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> winfsp::Result<u32> {
        if buffer.is_empty() {
            return Ok(0);
        }
        {
            let guard = context.write_buf.lock().unwrap();
            if let Some(buf) = guard.as_ref() {
                let start = offset.min(buf.len() as u64) as usize;
                let n = (buf.len() - start).min(buffer.len());
                buffer[..n].copy_from_slice(&buf[start..start + n]);
                return Ok(n as u32);
            }
        }
        match self
            .fs
            .read_range(&context.path, offset, buffer.len())
            .await
        {
            Ok(data) => {
                let n = data.len().min(buffer.len());
                buffer[..n].copy_from_slice(&data[..n]);
                Ok(n as u32)
            }
            Err(e) => {
                eprintln!(
                    "ossfs read_range err path={} offset={} len={}: {e:?}",
                    context.path,
                    offset,
                    buffer.len()
                );
                Err(FspError::from(IoError::other(e.to_string())))
            }
        }
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
        if buffer.is_empty() {
            return Ok(0);
        }
        let new_size = {
            let mut guard = context.write_buf.lock().unwrap();
            let Some(buf) = guard.as_mut() else {
                return Err(FspError::from(IoError::from_raw_os_error(
                    WIN32_ACCESS_DENIED,
                )));
            };
            let effective = if write_to_eof {
                buf.len() as u64
            } else {
                offset
            };
            let start = effective as usize;
            if start + buffer.len() > buf.len() {
                buf.resize(start + buffer.len(), 0);
            }
            buf[start..start + buffer.len()].copy_from_slice(buffer);
            buf.len() as u64
        };
        context.dirty.store(true, Ordering::Release);
        let entry = DirEntry {
            name: context.path.clone(),
            is_dir: false,
            size: new_size,
            mtime_secs: 0,
        };
        *file_info = file_info_from(&entry, context.index());
        Ok(buffer.len() as u32)
    }

    async fn read_directory_async(
        &self,
        context: &Self::FileContext,
        pattern: Option<&U16CStr>,
        marker: DirMarker<'_>,
        buffer: &mut [u8],
    ) -> winfsp::Result<u32> {
        let entries = self.fs.list(&context.path).await.map_err(|e| {
            eprintln!("ossmount: 列目录失败 {}: {e:?}", context.path);
            FspError::from(IoError::other(e.to_string()))
        })?;

        let is_root = context.path == "/";
        let mut listing: Vec<(String, DirEntry)> = Vec::with_capacity(entries.len() + 2);
        if !is_root {
            if let Ok(Some(dot)) = self.fs.stat(&context.path).await {
                listing.push((".".to_string(), dot));
            }
            let parent = parent_posix(&context.path);
            if let Ok(Some(dotdot)) = self.fs.stat(&parent).await {
                listing.push(("..".to_string(), dotdot));
            }
        }
        for entry in entries {
            listing.push((entry.name.clone(), entry));
        }

        let pat = pattern.map(|p| p.to_string_lossy());
        if let Some(pat) = pat.as_deref() {
            listing.retain(|(name, _)| wildcard_match(pat, name));
        }

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

        let lock = context
            .dir_buffer
            .acquire(marker.is_none(), Some(buffer.len() as u32))?;
        for (name, entry) in listing.iter().skip(start) {
            let mut di = DirInfo::<255>::new();
            if let Err(e) = di.set_name(name) {
                debug!(name, error = ?e, "ossfs readdir entry name too long");
                continue;
            }
            *di.file_info_mut() = file_info_from(entry, file_index(name));
            lock.write(&mut di)?;
        }
        drop(lock);

        Ok(context.dir_buffer.read(marker, buffer))
    }
}

fn file_index(path: &str) -> u64 {
    path.as_bytes().iter().fold(0x9E37_79B9u64, |acc, b| {
        acc.wrapping_mul(31).wrapping_add(*b as u64)
    })
}

fn log_path(path: &str) -> &str {
    path
}

/// Runtime record the desktop tray app uses to list and stop `ossmount`
/// instances. Kept in `%TEMP%\brewfs-oss` so it never mixes with the BrewFS
/// control-plane registry (`%TEMP%\brewfs`).
fn runtime_record_path(pid: u32) -> PathBuf {
    std::env::temp_dir()
        .join("brewfs-oss")
        .join(format!("{pid}.json"))
}

fn write_runtime_record(mount_point: &Path) {
    let dir = std::env::temp_dir().join("brewfs-oss");
    if let Err(e) = std::fs::create_dir_all(&dir) {
        warn!(error = ?e, "ossfs failed to create runtime record dir");
        return;
    }
    let record = serde_json::json!({
        "pid": std::process::id(),
        "mount_point": mount_point.display().to_string(),
        "socket_path": "",
        "started_at": chrono::Utc::now().to_rfc3339(),
    });
    let data = serde_json::to_vec_pretty(&record).unwrap_or_default();
    if let Err(e) = std::fs::write(runtime_record_path(std::process::id()), data) {
        warn!(error = ?e, "ossfs failed to write runtime record");
    }
}

fn remove_runtime_record() {
    let _ = std::fs::remove_file(runtime_record_path(std::process::id()));
}

unsafe extern "system" {
    #[link(name = "kernel32")]
    fn SetDllDirectoryW(lp_path_name: *const u16) -> i32;
}

fn ensure_winfsp_dll_discoverable() {
    let candidates = [
        r"C:\Program Files (x86)\WinFsp\bin",
        r"C:\Program Files\WinFsp\bin",
    ];
    for dir in candidates {
        if Path::new(dir).join("winfsp-x64.dll").exists() {
            let wide: Vec<u16> = dir.encode_utf16().chain(std::iter::once(0)).collect();
            // SAFETY: SetDllDirectoryW points at a valid NUL-terminated wide
            // string kept alive for the duration of the call.
            unsafe {
                SetDllDirectoryW(wide.as_ptr());
            }
            return;
        }
    }
}
