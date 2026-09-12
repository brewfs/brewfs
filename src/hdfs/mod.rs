//! HDFS-compatible native client foundation.
//!
//! This is the native foundation for a future Hadoop `FileSystem` adapter. It
//! deliberately does not implement the HDFS NameNode/DataNode wire protocol.

#![allow(clippy::identity_op)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::cadapter::client::ObjectClient;
use crate::cadapter::localfs::LocalFsBackend;
use crate::chunk::cache::ChunksCacheConfig;
use crate::chunk::layout::ChunkLayout;
use crate::chunk::store::{BlockStoreConfig, ObjectBlockStore};
use crate::fs::{CallerIdentity, FileStat, FileSystem, FileSystemConfig, OpenFlags};
use crate::meta::factory::create_meta_store_from_url;
use crate::meta::store::{FileType, SetAttrFlags, SetAttrRequest};
use crate::meta::stores::DatabaseMetaStore;
use std::cell::RefCell;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Mutex;

pub const ABI_VERSION: u32 = (1 << 16) | 0;
const DEFAULT_CHUNK_SIZE: u64 = 64 * 1024 * 1024;
const DEFAULT_BLOCK_SIZE: u32 = 4 * 1024 * 1024;
const DEFAULT_MAX_IO: usize = 16 * 1024 * 1024;

pub const OPEN_READ: u32 = 1 << 0;
pub const OPEN_WRITE: u32 = 1 << 1;
pub const OPEN_CREATE: u32 = 1 << 2;
pub const OPEN_TRUNCATE: u32 = 1 << 3;
pub const OPEN_EXCLUSIVE: u32 = 1 << 4;
pub const OPEN_APPEND: u32 = 1 << 5;

pub const SET_MODE: u32 = 1 << 0;
pub const SET_UID: u32 = 1 << 1;
pub const SET_GID: u32 = 1 << 2;
pub const SET_SIZE: u32 = 1 << 3;
pub const SET_ATIME: u32 = 1 << 4;
pub const SET_MTIME: u32 = 1 << 5;

pub const FEATURE_XATTR: u64 = 1 << 0;
pub const FEATURE_STATFS: u64 = 1 << 1;
pub const FEATURE_APPEND: u64 = 1 << 2;

#[repr(i32)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BrewFsStatus {
    #[default]
    Ok = 0,
    End = 1,
    BufferTooSmall = 2,
    InvalidArgument = -1,
    InvalidHandle = -2,
    NotFound = -3,
    AlreadyExists = -4,
    NotADirectory = -5,
    IsADirectory = -6,
    DirectoryNotEmpty = -7,
    PermissionDenied = -8,
    ReadOnly = -9,
    NoSpace = -10,
    Unsupported = -11,
    StaleHandle = -12,
    IoError = -13,
    InternalError = -14,
    Panic = -15,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct BrewFsClientOptionsV1 {
    pub struct_size: u32,
    pub flags: u32,
    pub data_dir: *const u8,
    pub data_dir_len: usize,
    pub metadata_url: *const u8,
    pub metadata_url_len: usize,
    pub chunk_size: u64,
    pub block_size: u32,
    pub uid: u32,
    pub gid: u32,
    pub enforce_permissions: u8,
    pub reserved: [u8; 3],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct BrewFsOpenOptionsV1 {
    pub struct_size: u32,
    pub flags: u32,
    pub mode: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct BrewFsStatV1 {
    pub struct_size: u32,
    pub file_type: u32,
    pub inode: u64,
    pub size: u64,
    pub blocks: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub nlink: u32,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct BrewFsStatFsV1 {
    pub struct_size: u32,
    pub total_space: u64,
    pub available_space: u64,
    pub used_space: u64,
    pub total_inodes: u64,
    pub available_inodes: u64,
    pub used_inodes: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct BrewFsSetAttrV1 {
    pub struct_size: u32,
    pub valid_mask: u32,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub atime_ns: i64,
    pub mtime_ns: i64,
}

#[repr(C)]
pub struct BrewFsClient {
    inner: Arc<ClientInner>,
}

#[repr(C)]
pub struct BrewFsFile {
    inner: Arc<FileInner>,
}

#[repr(C)]
pub struct BrewFsDir {
    inner: Arc<DirInner>,
}

type LocalFileSystem = FileSystem<ObjectBlockStore<LocalFsBackend>, DatabaseMetaStore>;
type LocalFile = crate::fs::File<ObjectBlockStore<LocalFsBackend>, DatabaseMetaStore>;

struct ClientInner {
    runtime: Runtime,
    fs: Arc<LocalFileSystem>,
    max_io: usize,
    closed: AtomicBool,
}

struct FileInner {
    client: Arc<ClientInner>,
    file: Mutex<Option<LocalFile>>,
}

struct DirInner {
    client: Arc<ClientInner>,
    entries: Mutex<DirCursor>,
}

struct DirCursor {
    entries: Vec<crate::meta::store::DirEntry>,
    next: usize,
}

#[derive(Default)]
struct LastError {
    status: BrewFsStatus,
    message: String,
}

thread_local! {
    static LAST_ERROR: RefCell<LastError> = RefCell::new(LastError::default());
}

fn clear_error() {
    LAST_ERROR.with(|slot| {
        let mut error = slot.borrow_mut();
        error.status = BrewFsStatus::Ok;
        error.message.clear();
    });
}

fn set_error(status: BrewFsStatus, message: impl Into<String>) -> BrewFsStatus {
    LAST_ERROR.with(|slot| {
        let mut error = slot.borrow_mut();
        error.status = status;
        error.message = message.into();
    });
    status
}

fn ffi_call(f: impl FnOnce() -> Result<BrewFsStatus, BrewFsStatus>) -> BrewFsStatus {
    clear_error();
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(status)) => status,
        Ok(Err(status)) => status,
        Err(_) => set_error(BrewFsStatus::Panic, "panic caught at BrewFS C ABI boundary"),
    }
}

fn fail(status: BrewFsStatus, message: impl Into<String>) -> Result<BrewFsStatus, BrewFsStatus> {
    Err(set_error(status, message))
}

fn map_io_error(error: &io::Error) -> BrewFsStatus {
    match error.kind() {
        io::ErrorKind::NotFound => BrewFsStatus::NotFound,
        io::ErrorKind::AlreadyExists => BrewFsStatus::AlreadyExists,
        io::ErrorKind::NotADirectory => BrewFsStatus::NotADirectory,
        io::ErrorKind::IsADirectory => BrewFsStatus::IsADirectory,
        io::ErrorKind::DirectoryNotEmpty => BrewFsStatus::DirectoryNotEmpty,
        io::ErrorKind::PermissionDenied => BrewFsStatus::PermissionDenied,
        io::ErrorKind::ReadOnlyFilesystem => BrewFsStatus::ReadOnly,
        io::ErrorKind::Unsupported => BrewFsStatus::Unsupported,
        io::ErrorKind::StorageFull => BrewFsStatus::NoSpace,
        io::ErrorKind::InvalidInput => BrewFsStatus::InvalidArgument,
        _ => BrewFsStatus::IoError,
    }
}

fn io_result<T>(result: io::Result<T>) -> Result<T, BrewFsStatus> {
    result.map_err(|error| {
        let status = map_io_error(&error);
        set_error(status, error.to_string())
    })
}

fn checked_slice<'a>(ptr: *const u8, len: usize, label: &str) -> Result<&'a [u8], BrewFsStatus> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(set_error(
            BrewFsStatus::InvalidArgument,
            format!("{label} is null"),
        ));
    }
    // SAFETY: the caller keeps this read-only buffer alive for the ABI call.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

fn checked_mut_slice<'a>(
    ptr: *mut u8,
    len: usize,
    label: &str,
) -> Result<&'a mut [u8], BrewFsStatus> {
    if len == 0 {
        return Ok(&mut []);
    }
    if ptr.is_null() {
        return Err(set_error(
            BrewFsStatus::InvalidArgument,
            format!("{label} is null"),
        ));
    }
    // SAFETY: the caller owns this writable buffer for the ABI call.
    Ok(unsafe { std::slice::from_raw_parts_mut(ptr, len) })
}

fn decode_path(ptr: *const u8, len: usize) -> Result<String, BrewFsStatus> {
    let bytes = checked_slice(ptr, len, "path")?;
    if bytes.contains(&0) {
        return Err(set_error(
            BrewFsStatus::InvalidArgument,
            "path contains NUL",
        ));
    }
    let path = std::str::from_utf8(bytes)
        .map_err(|_| set_error(BrewFsStatus::InvalidArgument, "path is not UTF-8"))?;
    if path.is_empty() {
        return Err(set_error(BrewFsStatus::InvalidArgument, "path is empty"));
    }
    Ok(path.to_string())
}

fn decode_text(ptr: *const u8, len: usize, label: &str) -> Result<String, BrewFsStatus> {
    let bytes = checked_slice(ptr, len, label)?;
    let text = std::str::from_utf8(bytes).map_err(|_| {
        set_error(
            BrewFsStatus::InvalidArgument,
            format!("{label} is not UTF-8"),
        )
    })?;
    Ok(text.to_string())
}

/// Validate a caller-provided `struct_size` field.
///
/// `struct_size` must be the first field of every options/output struct so
/// that callers can be validated before the rest of the struct is touched.
/// `0` ("unset") and any value smaller than the ABI v1 size are rejected;
/// larger values are accepted for forward compatibility (only the v1 fields
/// are read or written).
fn validate_struct_size(actual: u32, required: usize, label: &str) -> Result<(), BrewFsStatus> {
    if (actual as usize) < required {
        return Err(set_error(
            BrewFsStatus::InvalidArgument,
            format!("{label}.struct_size ({actual}) is smaller than ABI v1 minimum ({required})"),
        ));
    }
    Ok(())
}

fn client_ref<'a>(ptr: *mut BrewFsClient) -> Result<&'a BrewFsClient, BrewFsStatus> {
    if ptr.is_null() {
        Err(set_error(
            BrewFsStatus::InvalidHandle,
            "client handle is null",
        ))
    } else {
        // SAFETY: the caller passes a live handle returned by client_open.
        Ok(unsafe { &*ptr })
    }
}

fn file_ref<'a>(ptr: *mut BrewFsFile) -> Result<&'a BrewFsFile, BrewFsStatus> {
    if ptr.is_null() {
        Err(set_error(
            BrewFsStatus::InvalidHandle,
            "file handle is null",
        ))
    } else {
        // SAFETY: the caller passes a live handle returned by open.
        Ok(unsafe { &*ptr })
    }
}

fn dir_ref<'a>(ptr: *mut BrewFsDir) -> Result<&'a BrewFsDir, BrewFsStatus> {
    if ptr.is_null() {
        Err(set_error(
            BrewFsStatus::InvalidHandle,
            "directory handle is null",
        ))
    } else {
        // SAFETY: the caller passes a live handle returned by readdir_open.
        Ok(unsafe { &*ptr })
    }
}

fn ensure_open(client: &ClientInner) -> Result<(), BrewFsStatus> {
    if client.closed.load(Ordering::Acquire) {
        Err(set_error(BrewFsStatus::StaleHandle, "client is closed"))
    } else {
        Ok(())
    }
}

fn file_type_code(kind: FileType) -> u32 {
    match kind {
        FileType::File => 1,
        FileType::Dir => 2,
        FileType::Symlink => 3,
        FileType::Fifo => 4,
        FileType::Socket => 5,
        FileType::CharDevice => 6,
        FileType::BlockDevice => 7,
    }
}

fn stat_from_file_stat(stat: &FileStat) -> BrewFsStatV1 {
    let attr = stat.attr();
    BrewFsStatV1 {
        struct_size: std::mem::size_of::<BrewFsStatV1>() as u32,
        file_type: file_type_code(attr.kind),
        inode: stat.inode().max(0) as u64,
        size: attr.size,
        blocks: attr.blocks,
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        nlink: attr.nlink,
        atime_ns: attr.atime,
        mtime_ns: attr.mtime,
        ctime_ns: attr.ctime,
    }
}

async fn build_local_client(
    data_dir: String,
    metadata_url: String,
    chunk_size: u64,
    block_size: u32,
    uid: u32,
    gid: u32,
    enforce_permissions: bool,
) -> io::Result<LocalFileSystem> {
    if chunk_size == 0 || block_size == 0 || chunk_size < block_size as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "chunk_size must be >= block_size and both must be non-zero",
        ));
    }
    let data_path = PathBuf::from(data_dir);
    std::fs::create_dir_all(&data_path)?;
    let meta_handle = create_meta_store_from_url(&metadata_url)
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
    let store = ObjectBlockStore::new_with_configs_async(
        ObjectClient::new(LocalFsBackend::new(&data_path)),
        ChunksCacheConfig {
            disk_storage_dir: Some(data_path.join(".brewfs-cache")),
            ..ChunksCacheConfig::default()
        },
        BlockStoreConfig::default(),
    )
    .await
    .map_err(|error| io::Error::other(error.to_string()))?;
    let config = FileSystemConfig::default()
        .with_caller(CallerIdentity::new(uid, gid, vec![gid]))
        .with_permissions(enforce_permissions);
    FileSystem::from_components(
        ChunkLayout {
            chunk_size,
            block_size,
        },
        Arc::new(store),
        meta_handle.layer(),
        config,
    )
}

fn open_flags(options: &BrewFsOpenOptionsV1) -> Result<OpenFlags, BrewFsStatus> {
    let known = OPEN_READ | OPEN_WRITE | OPEN_CREATE | OPEN_TRUNCATE | OPEN_EXCLUSIVE | OPEN_APPEND;
    if options.flags & !known != 0 {
        return Err(set_error(
            BrewFsStatus::InvalidArgument,
            "unknown open flags",
        ));
    }
    let mut flags = OpenFlags {
        read: options.flags & OPEN_READ != 0,
        write: options.flags & OPEN_WRITE != 0,
        append: options.flags & OPEN_APPEND != 0,
        create: options.flags & OPEN_CREATE != 0,
        truncate: options.flags & OPEN_TRUNCATE != 0,
        exclusive: options.flags & OPEN_EXCLUSIVE != 0,
        mode: options.mode,
    };
    if flags.append {
        flags.write = true;
    }
    if !flags.read && !flags.write && !flags.append {
        return Err(set_error(
            BrewFsStatus::InvalidArgument,
            "open requires read or write",
        ));
    }
    Ok(flags)
}

#[unsafe(no_mangle)]
pub extern "C" fn brewfs_v1_abi_version() -> u32 {
    ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn brewfs_v1_client_open(
    options: *const BrewFsClientOptionsV1,
    out_client: *mut *mut BrewFsClient,
) -> BrewFsStatus {
    ffi_call(|| {
        if out_client.is_null() {
            return fail(BrewFsStatus::InvalidArgument, "out_client is null");
        }
        // SAFETY: out_client is caller-owned writable storage.
        unsafe { *out_client = std::ptr::null_mut() };
        let options = if options.is_null() {
            BrewFsClientOptionsV1 {
                struct_size: std::mem::size_of::<BrewFsClientOptionsV1>() as u32,
                flags: 0,
                data_dir: std::ptr::null(),
                data_dir_len: 0,
                metadata_url: std::ptr::null(),
                metadata_url_len: 0,
                chunk_size: DEFAULT_CHUNK_SIZE,
                block_size: DEFAULT_BLOCK_SIZE,
                uid: 0,
                gid: 0,
                enforce_permissions: 0,
                reserved: [0; 3],
            }
        } else {
            // SAFETY: struct_size is the first field, so only these bytes are
            // read before the struct contents are validated.
            let declared = unsafe { (*options).struct_size };
            validate_struct_size(
                declared,
                std::mem::size_of::<BrewFsClientOptionsV1>(),
                "client options",
            )?;
            // SAFETY: options is non-null, validated, and borrowed only for this call.
            unsafe { *options }
        };
        let data_dir = if options.data_dir.is_null() && options.data_dir_len == 0 {
            std::env::temp_dir()
                .join("brewfs-hdfs-sdk")
                .to_string_lossy()
                .into_owned()
        } else {
            decode_text(options.data_dir, options.data_dir_len, "data_dir")?
        };
        let metadata_url = if options.metadata_url.is_null() && options.metadata_url_len == 0 {
            // Default to a persistent SQLite catalog under the data dir so
            // that multiple clients (or a restarted process) pointing at the
            // same data dir share the same metadata.
            format!("sqlite://{data_dir}/metadata.db?mode=rwc")
        } else {
            decode_text(
                options.metadata_url,
                options.metadata_url_len,
                "metadata_url",
            )?
        };
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|error| set_error(BrewFsStatus::InternalError, error.to_string()))?;
        let chunk_size = if options.chunk_size == 0 {
            DEFAULT_CHUNK_SIZE
        } else {
            options.chunk_size
        };
        let block_size = if options.block_size == 0 {
            DEFAULT_BLOCK_SIZE
        } else {
            options.block_size
        };
        let fs = runtime
            .block_on(build_local_client(
                data_dir,
                metadata_url,
                chunk_size,
                block_size,
                options.uid,
                options.gid,
                options.enforce_permissions != 0,
            ))
            .map_err(|error| set_error(BrewFsStatus::InternalError, error.to_string()))?;
        let client = Box::new(BrewFsClient {
            inner: Arc::new(ClientInner {
                runtime,
                fs: Arc::new(fs),
                max_io: DEFAULT_MAX_IO,
                closed: AtomicBool::new(false),
            }),
        });
        // SAFETY: out_client is valid caller-owned storage.
        unsafe { *out_client = Box::into_raw(client) };
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_client_close(client: *mut BrewFsClient) -> BrewFsStatus {
    ffi_call(|| {
        if client.is_null() {
            return fail(BrewFsStatus::InvalidHandle, "client handle is null");
        }
        // SAFETY: the caller passes a pointer returned by client_open exactly once.
        let client = unsafe { Box::from_raw(client) };
        client.inner.closed.store(true, Ordering::Release);
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_open(
    client: *mut BrewFsClient,
    path: *const u8,
    path_len: usize,
    options: *const BrewFsOpenOptionsV1,
    out_file: *mut *mut BrewFsFile,
) -> BrewFsStatus {
    ffi_call(|| {
        if out_file.is_null() {
            return fail(BrewFsStatus::InvalidArgument, "out_file is null");
        }
        // SAFETY: out_file is caller-owned writable storage.
        unsafe { *out_file = std::ptr::null_mut() };
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        if options.is_null() {
            return fail(BrewFsStatus::InvalidArgument, "open options is null");
        }
        let path = decode_path(path, path_len)?;
        // SAFETY: struct_size is the first field, so only these bytes are
        // read before the struct contents are validated.
        let declared = unsafe { (*options).struct_size };
        validate_struct_size(
            declared,
            std::mem::size_of::<BrewFsOpenOptionsV1>(),
            "open options",
        )?;
        // SAFETY: options is non-null and borrowed for this call.
        let options = unsafe { &*options };
        let flags = open_flags(options)?;
        let file = io_result(
            client
                .inner
                .runtime
                .block_on(client.inner.fs.open(&path, flags)),
        )?;
        let handle = Box::new(BrewFsFile {
            inner: Arc::new(FileInner {
                client: Arc::clone(&client.inner),
                file: Mutex::new(Some(file)),
            }),
        });
        // SAFETY: out_file is valid caller-owned storage.
        unsafe { *out_file = Box::into_raw(handle) };
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_file_close(file: *mut BrewFsFile) -> BrewFsStatus {
    ffi_call(|| {
        if file.is_null() {
            return fail(BrewFsStatus::InvalidHandle, "file handle is null");
        }
        // SAFETY: the caller passes a pointer returned by open exactly once.
        let file = unsafe { Box::from_raw(file) };
        let client = Arc::clone(&file.inner.client);
        let result = client.runtime.block_on(async {
            let mut guard = file.inner.file.lock().await;
            let Some(mut open_file) = guard.take() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "file is already closed",
                ));
            };
            open_file.close().await
        });
        io_result(result)?;
        Ok(BrewFsStatus::Ok)
    })
}

async fn read_open_file(
    file: &FileInner,
    buffer: &mut [u8],
    offset: Option<u64>,
) -> io::Result<usize> {
    let mut guard = file.file.lock().await;
    let open_file = guard
        .as_mut()
        .ok_or_else(|| io::Error::other("file is closed"))?;
    match offset {
        Some(offset) => open_file.read_at(buffer, offset).await,
        None => open_file.read(buffer).await,
    }
}

async fn write_open_file(file: &FileInner, data: &[u8], offset: Option<u64>) -> io::Result<usize> {
    let mut guard = file.file.lock().await;
    let open_file = guard
        .as_mut()
        .ok_or_else(|| io::Error::other("file is closed"))?;
    match offset {
        Some(offset) => open_file.write_at(data, offset).await,
        None => open_file.write(data).await,
    }
}

unsafe fn read_impl(
    file: *mut BrewFsFile,
    buffer: *mut u8,
    capacity: usize,
    offset: Option<u64>,
    out_read: *mut usize,
) -> BrewFsStatus {
    ffi_call(|| {
        if out_read.is_null() {
            return fail(BrewFsStatus::InvalidArgument, "out_read is null");
        }
        // SAFETY: out_read is caller-owned writable storage.
        unsafe { *out_read = 0 };
        let file = file_ref(file)?;
        ensure_open(&file.inner.client)?;
        if capacity > file.inner.client.max_io {
            return fail(
                BrewFsStatus::InvalidArgument,
                "read exceeds maximum I/O size",
            );
        }
        let buffer = checked_mut_slice(buffer, capacity, "read buffer")?;
        let client = Arc::clone(&file.inner.client);
        let n = io_result(
            client
                .runtime
                .block_on(read_open_file(&file.inner, buffer, offset)),
        )?;
        // SAFETY: out_read was checked above.
        unsafe { *out_read = n };
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_read(
    file: *mut BrewFsFile,
    buffer: *mut u8,
    capacity: usize,
    out_read: *mut usize,
) -> BrewFsStatus {
    unsafe { read_impl(file, buffer, capacity, None, out_read) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_pread(
    file: *mut BrewFsFile,
    offset: u64,
    buffer: *mut u8,
    capacity: usize,
    out_read: *mut usize,
) -> BrewFsStatus {
    unsafe { read_impl(file, buffer, capacity, Some(offset), out_read) }
}

unsafe fn write_impl(
    file: *mut BrewFsFile,
    data: *const u8,
    length: usize,
    offset: Option<u64>,
    out_written: *mut usize,
) -> BrewFsStatus {
    ffi_call(|| {
        if out_written.is_null() {
            return fail(BrewFsStatus::InvalidArgument, "out_written is null");
        }
        // SAFETY: out_written is caller-owned writable storage.
        unsafe { *out_written = 0 };
        let file = file_ref(file)?;
        ensure_open(&file.inner.client)?;
        if length > file.inner.client.max_io {
            return fail(
                BrewFsStatus::InvalidArgument,
                "write exceeds maximum I/O size",
            );
        }
        let data = checked_slice(data, length, "write data")?;
        let client = Arc::clone(&file.inner.client);
        let n = io_result(
            client
                .runtime
                .block_on(write_open_file(&file.inner, data, offset)),
        )?;
        // SAFETY: out_written was checked above.
        unsafe { *out_written = n };
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_write(
    file: *mut BrewFsFile,
    data: *const u8,
    length: usize,
    out_written: *mut usize,
) -> BrewFsStatus {
    unsafe { write_impl(file, data, length, None, out_written) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_pwrite(
    file: *mut BrewFsFile,
    offset: u64,
    data: *const u8,
    length: usize,
    out_written: *mut usize,
) -> BrewFsStatus {
    unsafe { write_impl(file, data, length, Some(offset), out_written) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_seek(file: *mut BrewFsFile, offset: u64) -> BrewFsStatus {
    ffi_call(|| {
        let file = file_ref(file)?;
        ensure_open(&file.inner.client)?;
        let client = Arc::clone(&file.inner.client);
        io_result(client.runtime.block_on(async {
            let mut guard = file.inner.file.lock().await;
            let open_file = guard
                .as_mut()
                .ok_or_else(|| io::Error::other("file is closed"))?;
            open_file.seek(offset);
            Ok(())
        }))?;
        Ok(BrewFsStatus::Ok)
    })
}

async fn sync_open_file(file: &FileInner, data_only: Option<bool>) -> io::Result<()> {
    let mut guard = file.file.lock().await;
    let open_file = guard
        .as_mut()
        .ok_or_else(|| io::Error::other("file is closed"))?;
    match data_only {
        None => open_file.flush().await,
        Some(data_only) => open_file.fsync(data_only).await,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_flush(file: *mut BrewFsFile) -> BrewFsStatus {
    ffi_call(|| {
        let file = file_ref(file)?;
        ensure_open(&file.inner.client)?;
        let client = Arc::clone(&file.inner.client);
        io_result(client.runtime.block_on(sync_open_file(&file.inner, None)))?;
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_fsync(file: *mut BrewFsFile, data_only: u8) -> BrewFsStatus {
    ffi_call(|| {
        let file = file_ref(file)?;
        ensure_open(&file.inner.client)?;
        let client = Arc::clone(&file.inner.client);
        io_result(
            client
                .runtime
                .block_on(sync_open_file(&file.inner, Some(data_only != 0))),
        )?;
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_stat(
    client: *mut BrewFsClient,
    path: *const u8,
    path_len: usize,
    output: *mut BrewFsStatV1,
) -> BrewFsStatus {
    ffi_call(|| {
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        if output.is_null() {
            return fail(BrewFsStatus::InvalidArgument, "stat output is null");
        }
        // SAFETY: output is caller-owned writable storage; struct_size is the
        // first field, so only these bytes are read before validation.
        let output_size = unsafe { (*output).struct_size };
        validate_struct_size(
            output_size,
            std::mem::size_of::<BrewFsStatV1>(),
            "stat output",
        )?;
        let path = decode_path(path, path_len)?;
        let stat = io_result(client.inner.runtime.block_on(client.inner.fs.stat(&path)))?;
        // SAFETY: output is non-null and caller-owned.
        unsafe { *output = stat_from_file_stat(&stat) };
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_statfs(
    client: *mut BrewFsClient,
    output: *mut BrewFsStatFsV1,
) -> BrewFsStatus {
    ffi_call(|| {
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        if output.is_null() {
            return fail(BrewFsStatus::InvalidArgument, "statfs output is null");
        }
        // SAFETY: output is caller-owned writable storage; struct_size is the
        // first field, so only these bytes are read before validation.
        let output_size = unsafe { (*output).struct_size };
        validate_struct_size(
            output_size,
            std::mem::size_of::<BrewFsStatFsV1>(),
            "statfs output",
        )?;
        let stat = io_result(client.inner.runtime.block_on(client.inner.fs.stat_fs()))?;
        // SAFETY: output is caller-owned and non-null.
        unsafe {
            *output = BrewFsStatFsV1 {
                struct_size: std::mem::size_of::<BrewFsStatFsV1>() as u32,
                total_space: stat.total_space,
                available_space: stat.avail_space,
                used_space: stat.used_space,
                total_inodes: stat.total_inodes,
                available_inodes: stat.avail_inodes,
                used_inodes: stat.used_inodes,
            }
        };
        Ok(BrewFsStatus::Ok)
    })
}

async fn remove_dir_all(fs: &LocalFileSystem, path: &str) -> io::Result<()> {
    let entries = fs.readdir(path).await?;
    for entry in entries {
        let child = if path == "/" {
            format!("/{name}", name = entry.name)
        } else {
            format!("{path}/{name}", name = entry.name)
        };
        if entry.kind == FileType::Dir {
            Box::pin(remove_dir_all(fs, &child)).await?;
        } else {
            fs.unlink(&child).await?;
        }
    }
    fs.rmdir(path).await
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_mkdir(
    client: *mut BrewFsClient,
    path: *const u8,
    path_len: usize,
) -> BrewFsStatus {
    ffi_call(|| {
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        let path = decode_path(path, path_len)?;
        io_result(client.inner.runtime.block_on(client.inner.fs.mkdir(&path)))?;
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_mkdir_p(
    client: *mut BrewFsClient,
    path: *const u8,
    path_len: usize,
) -> BrewFsStatus {
    ffi_call(|| {
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        let path = decode_path(path, path_len)?;
        io_result(
            client
                .inner
                .runtime
                .block_on(client.inner.fs.mkdir_all(&path)),
        )?;
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_delete(
    client: *mut BrewFsClient,
    path: *const u8,
    path_len: usize,
    recursive: u8,
) -> BrewFsStatus {
    ffi_call(|| {
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        let path = decode_path(path, path_len)?;
        if path == "/" {
            return fail(
                BrewFsStatus::PermissionDenied,
                "cannot delete filesystem root",
            );
        }
        let result = async {
            let stat = client.inner.fs.stat(&path).await?;
            if stat.is_dir() {
                if recursive != 0 {
                    remove_dir_all(&client.inner.fs, &path).await
                } else {
                    client.inner.fs.rmdir(&path).await
                }
            } else {
                // HDFS semantics: deleting a plain file succeeds regardless of
                // the recursive flag.
                client.inner.fs.unlink(&path).await
            }
        };
        io_result(client.inner.runtime.block_on(result))?;
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_rename(
    client: *mut BrewFsClient,
    old_path: *const u8,
    old_len: usize,
    new_path: *const u8,
    new_len: usize,
) -> BrewFsStatus {
    ffi_call(|| {
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        let old_path = decode_path(old_path, old_len)?;
        let new_path = decode_path(new_path, new_len)?;
        io_result(
            client
                .inner
                .runtime
                .block_on(client.inner.fs.rename(&old_path, &new_path)),
        )?;
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_truncate(
    client: *mut BrewFsClient,
    path: *const u8,
    path_len: usize,
    size: u64,
) -> BrewFsStatus {
    ffi_call(|| {
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        let path = decode_path(path, path_len)?;
        io_result(
            client
                .inner
                .runtime
                .block_on(client.inner.fs.truncate(&path, size)),
        )?;
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_setattr(
    client: *mut BrewFsClient,
    path: *const u8,
    path_len: usize,
    input: *const BrewFsSetAttrV1,
) -> BrewFsStatus {
    ffi_call(|| {
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        if input.is_null() {
            return fail(BrewFsStatus::InvalidArgument, "setattr input is null");
        }
        let path = decode_path(path, path_len)?;
        // SAFETY: struct_size is the first field, so only these bytes are read
        // before the struct contents are validated.
        let declared = unsafe { (*input).struct_size };
        validate_struct_size(
            declared,
            std::mem::size_of::<BrewFsSetAttrV1>(),
            "setattr input",
        )?;
        // SAFETY: input is non-null and borrowed for this call.
        let input = unsafe { &*input };
        let known = SET_MODE | SET_UID | SET_GID | SET_SIZE | SET_ATIME | SET_MTIME;
        if input.valid_mask & !known != 0 {
            return fail(BrewFsStatus::InvalidArgument, "unknown setattr fields");
        }
        let request = SetAttrRequest {
            mode: (input.valid_mask & SET_MODE != 0).then_some(input.mode),
            uid: (input.valid_mask & SET_UID != 0).then_some(input.uid),
            gid: (input.valid_mask & SET_GID != 0).then_some(input.gid),
            size: (input.valid_mask & SET_SIZE != 0).then_some(input.size),
            atime: (input.valid_mask & SET_ATIME != 0).then_some(input.atime_ns),
            mtime: (input.valid_mask & SET_MTIME != 0).then_some(input.mtime_ns),
            ..Default::default()
        };
        io_result(
            client.inner.runtime.block_on(client.inner.fs.set_attr(
                &path,
                &request,
                SetAttrFlags::empty(),
            )),
        )?;
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_readdir_open(
    client: *mut BrewFsClient,
    path: *const u8,
    path_len: usize,
    out_dir: *mut *mut BrewFsDir,
) -> BrewFsStatus {
    ffi_call(|| {
        if out_dir.is_null() {
            return fail(BrewFsStatus::InvalidArgument, "out_dir is null");
        }
        // SAFETY: out_dir is caller-owned writable storage.
        unsafe { *out_dir = std::ptr::null_mut() };
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        let path = decode_path(path, path_len)?;
        let entries = io_result(
            client
                .inner
                .runtime
                .block_on(client.inner.fs.readdir(&path)),
        )?;
        let dir = Box::new(BrewFsDir {
            inner: Arc::new(DirInner {
                client: Arc::clone(&client.inner),
                entries: Mutex::new(DirCursor { entries, next: 0 }),
            }),
        });
        // SAFETY: out_dir is valid caller-owned output storage.
        unsafe { *out_dir = Box::into_raw(dir) };
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_readdir_next(
    dir: *mut BrewFsDir,
    name: *mut u8,
    name_capacity: usize,
    required_name_length: *mut usize,
    inode: *mut u64,
    file_type: *mut u32,
    has_entry: *mut u8,
) -> BrewFsStatus {
    ffi_call(|| {
        if required_name_length.is_null()
            || inode.is_null()
            || file_type.is_null()
            || has_entry.is_null()
        {
            return fail(BrewFsStatus::InvalidArgument, "readdir output is null");
        }
        // SAFETY: output pointers are caller-owned writable storage.
        unsafe {
            *required_name_length = 0;
            *inode = 0;
            *file_type = 0;
            *has_entry = 0;
        }
        let dir = dir_ref(dir)?;
        ensure_open(&dir.inner.client)?;
        let client = Arc::clone(&dir.inner.client);
        let entry = client.runtime.block_on(async {
            let mut cursor = dir.inner.entries.lock().await;
            let Some(entry) = cursor.entries.get(cursor.next).cloned() else {
                return Ok::<Option<crate::meta::store::DirEntry>, io::Error>(None);
            };
            if name_capacity < entry.name.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("name requires {} bytes", entry.name.len()),
                ));
            }
            cursor.next += 1;
            Ok(Some(entry))
        });
        match entry {
            Ok(None) => Ok(BrewFsStatus::End),
            Ok(Some(entry)) => {
                let bytes = entry.name.as_bytes();
                // SAFETY: output pointers were checked and name capacity was validated.
                unsafe {
                    *required_name_length = bytes.len();
                    *inode = entry.ino.max(0) as u64;
                    *file_type = file_type_code(entry.kind);
                    *has_entry = 1;
                }
                let output = checked_mut_slice(name, bytes.len(), "directory name")?;
                output.copy_from_slice(bytes);
                Ok(BrewFsStatus::Ok)
            }
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                let message = error.to_string();
                let required = message
                    .strip_prefix("name requires ")
                    .and_then(|v| v.strip_suffix(" bytes"))
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(0);
                // SAFETY: required_name_length is non-null.
                unsafe { *required_name_length = required };
                Ok(BrewFsStatus::BufferTooSmall)
            }
            Err(error) => Err(set_error(map_io_error(&error), error.to_string())),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_readdir_close(dir: *mut BrewFsDir) -> BrewFsStatus {
    ffi_call(|| {
        if dir.is_null() {
            return fail(BrewFsStatus::InvalidHandle, "directory handle is null");
        }
        // SAFETY: the caller passes a pointer returned by readdir_open exactly once.
        drop(unsafe { Box::from_raw(dir) });
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_set_xattr(
    client: *mut BrewFsClient,
    path: *const u8,
    path_len: usize,
    name: *const u8,
    name_len: usize,
    value: *const u8,
    value_len: usize,
    flags: u32,
) -> BrewFsStatus {
    ffi_call(|| {
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        let path = decode_path(path, path_len)?;
        let name = decode_text(name, name_len, "xattr name")?;
        if name.is_empty() || name.contains('\0') {
            return fail(BrewFsStatus::InvalidArgument, "invalid xattr name");
        }
        let value = checked_slice(value, value_len, "xattr value")?;
        io_result(
            client
                .inner
                .runtime
                .block_on(client.inner.fs.set_xattr(&path, &name, value, flags)),
        )?;
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_get_xattr(
    client: *mut BrewFsClient,
    path: *const u8,
    path_len: usize,
    name: *const u8,
    name_len: usize,
    value: *mut u8,
    value_capacity: usize,
    required_value_length: *mut usize,
) -> BrewFsStatus {
    ffi_call(|| {
        if required_value_length.is_null() {
            return fail(
                BrewFsStatus::InvalidArgument,
                "required_value_length is null",
            );
        }
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        let path = decode_path(path, path_len)?;
        let name = decode_text(name, name_len, "xattr name")?;
        let data = io_result(
            client
                .inner
                .runtime
                .block_on(client.inner.fs.get_xattr(&path, &name)),
        )?
        .ok_or_else(|| set_error(BrewFsStatus::NotFound, "xattr not found"))?;
        // SAFETY: required_value_length is non-null.
        unsafe { *required_value_length = data.len() };
        if value_capacity < data.len() {
            return Ok(BrewFsStatus::BufferTooSmall);
        }
        let output = checked_mut_slice(value, data.len(), "xattr output")?;
        output.copy_from_slice(&data);
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_list_xattr(
    client: *mut BrewFsClient,
    path: *const u8,
    path_len: usize,
    output: *mut u8,
    output_capacity: usize,
    required_output_length: *mut usize,
) -> BrewFsStatus {
    ffi_call(|| {
        if required_output_length.is_null() {
            return fail(
                BrewFsStatus::InvalidArgument,
                "required_output_length is null",
            );
        }
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        let path = decode_path(path, path_len)?;
        let names = io_result(
            client
                .inner
                .runtime
                .block_on(client.inner.fs.list_xattr(&path)),
        )?;
        let mut encoded = Vec::new();
        for name in names {
            encoded.extend_from_slice(name.as_bytes());
            encoded.push(0);
        }
        // SAFETY: required_output_length is non-null.
        unsafe { *required_output_length = encoded.len() };
        if output_capacity < encoded.len() {
            return Ok(BrewFsStatus::BufferTooSmall);
        }
        let output = checked_mut_slice(output, encoded.len(), "xattr list output")?;
        output.copy_from_slice(&encoded);
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_remove_xattr(
    client: *mut BrewFsClient,
    path: *const u8,
    path_len: usize,
    name: *const u8,
    name_len: usize,
) -> BrewFsStatus {
    ffi_call(|| {
        let client = client_ref(client)?;
        ensure_open(&client.inner)?;
        let path = decode_path(path, path_len)?;
        let name = decode_text(name, name_len, "xattr name")?;
        io_result(
            client
                .inner
                .runtime
                .block_on(client.inner.fs.remove_xattr(&path, &name)),
        )?;
        Ok(BrewFsStatus::Ok)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn brewfs_v1_capabilities() -> u64 {
    FEATURE_XATTR | FEATURE_STATFS | FEATURE_APPEND
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn brewfs_v1_last_error(
    output: *mut u8,
    capacity: usize,
    required_length: *mut usize,
) -> BrewFsStatus {
    if required_length.is_null() {
        return set_error(BrewFsStatus::InvalidArgument, "required_length is null");
    }
    let result = catch_unwind(AssertUnwindSafe(|| {
        LAST_ERROR.with(|slot| {
            let error = slot.borrow();
            let bytes = error.message.as_bytes();
            // SAFETY: required_length is non-null and caller-owned.
            unsafe { *required_length = bytes.len() };
            if capacity < bytes.len() {
                return BrewFsStatus::BufferTooSmall;
            }
            let output = match checked_mut_slice(output, bytes.len(), "error output") {
                Ok(output) => output,
                Err(status) => return status,
            };
            output.copy_from_slice(bytes);
            error.status
        })
    }));
    match result {
        Ok(status) => status,
        Err(_) => set_error(BrewFsStatus::Panic, "panic caught at BrewFS C ABI boundary"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;
    use tempfile::tempdir;

    #[test]
    fn abi_version_has_major_one() {
        assert_eq!(brewfs_v1_abi_version() >> 16, 1);
    }

    #[test]
    fn c_abi_round_trip_local_file() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().to_string_lossy().into_owned();
        let mut options = BrewFsClientOptionsV1 {
            struct_size: std::mem::size_of::<BrewFsClientOptionsV1>() as u32,
            flags: 0,
            data_dir: root.as_ptr(),
            data_dir_len: root.len(),
            metadata_url: ptr::null(),
            metadata_url_len: 0,
            chunk_size: 64 * 1024,
            block_size: 4096,
            uid: 0,
            gid: 0,
            enforce_permissions: 0,
            reserved: [0; 3],
        };
        let mut client = ptr::null_mut();
        assert_eq!(
            brewfs_v1_client_open(&mut options, &mut client),
            BrewFsStatus::Ok
        );
        let path = b"/hello";
        let open_options = BrewFsOpenOptionsV1 {
            struct_size: std::mem::size_of::<BrewFsOpenOptionsV1>() as u32,
            flags: OPEN_WRITE | OPEN_CREATE | OPEN_TRUNCATE,
            mode: 0o644,
        };
        let mut file = ptr::null_mut();
        assert_eq!(
            unsafe { brewfs_v1_open(client, path.as_ptr(), path.len(), &open_options, &mut file) },
            BrewFsStatus::Ok
        );
        let data = b"hello hdfs sdk";
        let mut written = 0;
        assert_eq!(
            unsafe { brewfs_v1_pwrite(file, 0, data.as_ptr(), data.len(), &mut written) },
            BrewFsStatus::Ok
        );
        assert_eq!(written, data.len());
        assert_eq!(unsafe { brewfs_v1_fsync(file, 0) }, BrewFsStatus::Ok);
        assert_eq!(unsafe { brewfs_v1_file_close(file) }, BrewFsStatus::Ok);

        let ro = BrewFsOpenOptionsV1 {
            struct_size: std::mem::size_of::<BrewFsOpenOptionsV1>() as u32,
            flags: OPEN_READ,
            mode: 0,
        };
        let mut file = ptr::null_mut();
        assert_eq!(
            unsafe { brewfs_v1_open(client, path.as_ptr(), path.len(), &ro, &mut file) },
            BrewFsStatus::Ok
        );
        let mut output = [0u8; 32];
        let mut read = 0;
        assert_eq!(
            unsafe { brewfs_v1_pread(file, 0, output.as_mut_ptr(), output.len(), &mut read) },
            BrewFsStatus::Ok
        );
        assert_eq!(&output[..read], data);
        assert_eq!(unsafe { brewfs_v1_file_close(file) }, BrewFsStatus::Ok);
        assert_eq!(unsafe { brewfs_v1_client_close(client) }, BrewFsStatus::Ok);
    }

    #[test]
    fn validate_struct_size_rejects_zero_and_undersized() {
        let required = std::mem::size_of::<BrewFsOpenOptionsV1>();
        assert!(validate_struct_size(0, required, "options").is_err());
        assert!(validate_struct_size(required as u32 - 1, required, "options").is_err());
        assert!(validate_struct_size(required as u32, required, "options").is_ok());
        assert!(validate_struct_size(required as u32 + 16, required, "options").is_ok());
    }

    #[test]
    fn client_open_rejects_bad_struct_size() {
        let mut options = BrewFsClientOptionsV1 {
            struct_size: 0,
            flags: 0,
            data_dir: ptr::null(),
            data_dir_len: 0,
            metadata_url: ptr::null(),
            metadata_url_len: 0,
            chunk_size: 0,
            block_size: 0,
            uid: 0,
            gid: 0,
            enforce_permissions: 0,
            reserved: [0; 3],
        };
        let mut client = ptr::null_mut();
        assert_eq!(
            brewfs_v1_client_open(&mut options, &mut client),
            BrewFsStatus::InvalidArgument
        );
        options.struct_size = 4;
        assert_eq!(
            brewfs_v1_client_open(&mut options, &mut client),
            BrewFsStatus::InvalidArgument
        );
        assert!(client.is_null());
    }

    #[test]
    fn stat_rejects_bad_output_struct_size() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().to_string_lossy().into_owned();
        let client = open_test_client(&root);
        let path = b"/stat-guard";
        let mut output = BrewFsStatV1::default();
        output.struct_size = 0;
        assert_eq!(
            unsafe { brewfs_v1_stat(client, path.as_ptr(), path.len(), &mut output) },
            BrewFsStatus::InvalidArgument
        );
        output.struct_size = 4;
        assert_eq!(
            unsafe { brewfs_v1_stat(client, path.as_ptr(), path.len(), &mut output) },
            BrewFsStatus::InvalidArgument
        );
        let mut statfs = BrewFsStatFsV1::default();
        statfs.struct_size = 0;
        assert_eq!(
            unsafe { brewfs_v1_statfs(client, &mut statfs) },
            BrewFsStatus::InvalidArgument
        );
        assert_eq!(unsafe { brewfs_v1_client_close(client) }, BrewFsStatus::Ok);
    }

    #[test]
    fn delete_recursive_succeeds_for_plain_file() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().to_string_lossy().into_owned();
        let client = open_test_client(&root);
        let path = b"/plain-file";
        let mut file = ptr::null_mut();
        let open_options = BrewFsOpenOptionsV1 {
            struct_size: std::mem::size_of::<BrewFsOpenOptionsV1>() as u32,
            flags: OPEN_WRITE | OPEN_CREATE | OPEN_TRUNCATE,
            mode: 0o644,
        };
        assert_eq!(
            unsafe { brewfs_v1_open(client, path.as_ptr(), path.len(), &open_options, &mut file) },
            BrewFsStatus::Ok
        );
        assert_eq!(unsafe { brewfs_v1_file_close(file) }, BrewFsStatus::Ok);
        assert_eq!(
            unsafe { brewfs_v1_delete(client, path.as_ptr(), path.len(), 1) },
            BrewFsStatus::Ok
        );
        assert_eq!(unsafe { brewfs_v1_client_close(client) }, BrewFsStatus::Ok);
    }

    #[test]
    fn append_contract_starts_at_end_of_file() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().to_string_lossy().into_owned();
        let client = open_test_client(&root);
        let path = b"/appended";
        let create = BrewFsOpenOptionsV1 {
            struct_size: std::mem::size_of::<BrewFsOpenOptionsV1>() as u32,
            flags: OPEN_WRITE | OPEN_CREATE | OPEN_TRUNCATE,
            mode: 0o644,
        };
        let mut file = ptr::null_mut();
        assert_eq!(
            unsafe { brewfs_v1_open(client, path.as_ptr(), path.len(), &create, &mut file) },
            BrewFsStatus::Ok
        );
        let first = b"hello";
        let mut written = 0;
        assert_eq!(
            unsafe { brewfs_v1_pwrite(file, 0, first.as_ptr(), first.len(), &mut written) },
            BrewFsStatus::Ok
        );
        assert_eq!(unsafe { brewfs_v1_file_close(file) }, BrewFsStatus::Ok);

        // The Hadoop adapter opens with OPEN_APPEND and resumes writing at the
        // size reported by stat; verify that contract end to end.
        let append = BrewFsOpenOptionsV1 {
            struct_size: std::mem::size_of::<BrewFsOpenOptionsV1>() as u32,
            flags: OPEN_WRITE | OPEN_APPEND,
            mode: 0,
        };
        let mut file = ptr::null_mut();
        assert_eq!(
            unsafe { brewfs_v1_open(client, path.as_ptr(), path.len(), &append, &mut file) },
            BrewFsStatus::Ok
        );
        let mut stat = BrewFsStatV1::default();
        stat.struct_size = std::mem::size_of::<BrewFsStatV1>() as u32;
        assert_eq!(
            unsafe { brewfs_v1_stat(client, path.as_ptr(), path.len(), &mut stat) },
            BrewFsStatus::Ok
        );
        assert_eq!(stat.size, first.len() as u64);
        let second = b" world";
        let mut written = 0;
        assert_eq!(
            unsafe {
                brewfs_v1_pwrite(file, stat.size, second.as_ptr(), second.len(), &mut written)
            },
            BrewFsStatus::Ok
        );
        assert_eq!(unsafe { brewfs_v1_file_close(file) }, BrewFsStatus::Ok);
        let mut stat = BrewFsStatV1::default();
        stat.struct_size = std::mem::size_of::<BrewFsStatV1>() as u32;
        assert_eq!(
            unsafe { brewfs_v1_stat(client, path.as_ptr(), path.len(), &mut stat) },
            BrewFsStatus::Ok
        );
        assert_eq!(stat.size, (first.len() + second.len()) as u64);
        assert_eq!(unsafe { brewfs_v1_client_close(client) }, BrewFsStatus::Ok);
    }

    fn open_test_client(root: &str) -> *mut BrewFsClient {
        let mut options = BrewFsClientOptionsV1 {
            struct_size: std::mem::size_of::<BrewFsClientOptionsV1>() as u32,
            flags: 0,
            data_dir: root.as_ptr(),
            data_dir_len: root.len(),
            metadata_url: ptr::null(),
            metadata_url_len: 0,
            chunk_size: 64 * 1024,
            block_size: 4096,
            uid: 0,
            gid: 0,
            enforce_permissions: 0,
            reserved: [0; 3],
        };
        let mut client = ptr::null_mut();
        assert_eq!(
            brewfs_v1_client_open(&mut options, &mut client),
            BrewFsStatus::Ok
        );
        client
    }
}
