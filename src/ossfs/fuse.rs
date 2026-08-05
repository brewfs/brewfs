//! macOS / Linux FUSE mount adapter for the metadata-less object filesystem.
//!
//! Bridges the FUSE kernel protocol (via the `fuser` crate) to
//! [`ObjectFs`](super::ObjectFs). Writes are buffered in memory and flushed as
//! a whole-object `PutObject` on flush/release — the same "cloud drive"
//! semantics as the WinFsp adapter and ossfs/s3fs.
//!
//! Only compiled on non-Windows targets (macOS with macFUSE, Linux with
//! libfuse). Windows uses the WinFsp adapter in [`super::winfsp`].
#![cfg(not(windows))]

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    AccessFlags, BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, LockOwner, MountOption, OpenAccMode, OpenFlags, RenameFlags,
    ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyStatfs, ReplyWrite, ReplyXattr, Request, SessionACL, TimeOrNow, WriteFlags,
};
use tokio::runtime::Handle;
use tracing::{info, warn};

use super::{DirEntry, ObjectFs};

/// Attribute/entry cache lifetime. Object storage has no change notifications,
/// so a short TTL keeps the tree weakly consistent across machines.
const TTL: Duration = Duration::from_secs(1);
/// Root directory inode (stable).
const ROOT_INODE: u64 = 1;
/// Upper bound on the number of directories tracked for periodic kernel-cache
/// invalidation. Browsing a huge tree cannot grow this set without limit.
const MAX_TRACKED_DIRS: usize = 8192;
/// Maximum supported path component length (POSIX NAME_MAX).
const NAME_MAX: u32 = 255;

/// Stable per-path inode: FNV-1a 64-bit of the POSIX path. Deterministic so a
/// path always maps to the same inode, mirroring the WinFsp adapter's
/// index-from-path scheme. `"/"` is special-cased to `ROOT_INODE`.
fn inode_for_path(path: &str) -> u64 {
    if path == "/" {
        return ROOT_INODE;
    }
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in path.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // Keep it non-zero and distinct from the root inode.
    if hash == 0 { 2 } else { hash | 1 }
}

/// True when `mode` denotes a regular file. `libc::S_IFMT`/`libc::S_IFREG`
/// are `u16` on macOS but `u32` on Linux, so both are cast to `u32`.
#[allow(clippy::unnecessary_cast)]
fn is_regular_file_mode(mode: u32) -> bool {
    mode & libc::S_IFMT as u32 == libc::S_IFREG as u32
}

/// Join a parent path and a name into a normalized POSIX path.
fn join_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

fn epoch(secs: i64) -> SystemTime {
    if secs <= 0 {
        UNIX_EPOCH
    } else {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    }
}

/// Per-open-file state. Writes are buffered whole-file and pushed to the
/// object store on flush/release (matching the WinFsp adapter).
#[derive(Clone)]
struct OpenFile {
    path: String,
    is_dir: bool,
    /// `Some(buffer)` when the handle was opened for writing (or created);
    /// `None` for read-only handles. Reads prefer the buffer when present.
    write_buf: Option<Vec<u8>>,
    /// Whether `write_buf` holds the object's current content. Opened write
    /// handles start unloaded and fetch on the first write/truncate, so
    /// opening a file for write never downloads the whole object.
    loaded: bool,
    dirty: bool,
}

/// Resize an open handle's write buffer (truncate/extend).
fn set_size_buf(open: &mut OpenFile, new_size: u64) {
    if let Some(buf) = open.write_buf.as_mut() {
        buf.resize(new_size as usize, 0);
        open.dirty = true;
    }
}

/// FUSE filesystem bridging kernel requests to [`ObjectFs`].
pub struct OssFs {
    fs: Arc<ObjectFs>,
    /// Tokio handle used to drive the async S3 client from FUSE threads.
    rt: Handle,
    /// inode -> POSIX path (root is always `ROOT_INODE`).
    inodes: Mutex<HashMap<u64, String>>,
    /// inodes of directories that have been listed; the periodic refresh task
    /// invalidates their kernel caches so remote changes show up.
    dirs: Arc<Mutex<HashSet<u64>>>,
    /// fh -> open file state.
    files: Mutex<HashMap<u64, OpenFile>>,
    next_fh: AtomicU64,
    /// uid/gid shown in attributes (the mounting user).
    uid: u32,
    gid: u32,
}

impl OssFs {
    pub fn new(fs: Arc<ObjectFs>, rt: Handle, dirs: Arc<Mutex<HashSet<u64>>>) -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(ROOT_INODE, "/".to_string());
        dirs.lock().unwrap().insert(ROOT_INODE);
        Self {
            fs,
            rt,
            inodes: Mutex::new(inodes),
            dirs,
            files: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
        }
    }

    fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    /// Block on an async ObjectFs call from a FUSE worker thread.
    fn block_on<T>(
        &self,
        fut: impl std::future::Future<Output = anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        self.rt.block_on(fut)
    }

    fn path_of(&self, ino: INodeNo) -> Option<String> {
        if ino.0 == ROOT_INODE {
            return Some("/".to_string());
        }
        self.inodes.lock().unwrap().get(&ino.0).cloned()
    }

    fn register_inode(&self, path: &str) -> u64 {
        let ino = inode_for_path(path);
        self.inodes.lock().unwrap().insert(ino, path.to_string());
        ino
    }

    fn attr_of(&self, path: &str, entry: &DirEntry) -> FileAttr {
        let (kind, perm, nlink) = if entry.is_dir {
            (FileType::Directory, 0o755u16, 2u32)
        } else {
            (FileType::RegularFile, 0o644u16, 1u32)
        };
        let size = entry.size;
        FileAttr {
            ino: INodeNo(self.register_inode(path)),
            size,
            blocks: size.saturating_add(511) / 512,
            atime: epoch(entry.mtime_secs),
            mtime: epoch(entry.mtime_secs),
            ctime: epoch(entry.mtime_secs),
            crtime: epoch(entry.mtime_secs),
            kind,
            perm,
            nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    /// Attr for `path`, preferring an in-flight write buffer size when an open
    /// write handle exists (so fstat after write sees the new size).
    fn effective_attr(&self, path: &str, entry: &DirEntry) -> FileAttr {
        let mut entry = entry.clone();
        let buf_len = self
            .files
            .lock()
            .unwrap()
            .values()
            .find(|o| o.path == path && o.loaded)
            .and_then(|o| o.write_buf.as_ref())
            .map(|b| b.len() as u64);
        if let Some(len) = buf_len {
            entry.size = len;
        }
        self.attr_of(path, &entry)
    }

    /// Flush a dirty open file to the object store (whole-object put).
    fn flush_open(&self, open: &OpenFile) -> anyhow::Result<()> {
        if open.dirty
            && let Some(buf) = open.write_buf.as_ref()
        {
            self.block_on(self.fs.write(&open.path, buf))?;
        }
        Ok(())
    }

    /// Truncate/expand a file with no open write handle via a
    /// read-modify-write against the object store.
    fn truncate_unopened(&self, path: &str, new_size: u64) -> anyhow::Result<()> {
        let data = self
            .block_on(self.fs.read_range(path, 0, usize::MAX))
            .unwrap_or_default();
        let mut data = data;
        data.resize(new_size as usize, 0);
        self.block_on(self.fs.write(path, &data))
    }
}

impl Filesystem for OssFs {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(parent_path) = self.path_of(parent) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };
        let path = join_path(&parent_path, name);
        match self.block_on(self.fs.stat(&path)) {
            Ok(Some(entry)) => {
                let attr = self.attr_of(&path, &entry);
                reply.entry(&TTL, &attr, Generation(0));
            }
            Ok(None) => reply.error(Errno::ENOENT),
            Err(e) => {
                warn!(path = %path, error = ?e, "ossfs lookup failed");
                reply.error(Errno::EIO);
            }
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let Some(path) = self.path_of(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        match self.block_on(self.fs.stat(&path)) {
            Ok(Some(entry)) => {
                let attr = self.effective_attr(&path, &entry);
                reply.attr(&TTL, &attr);
            }
            Ok(None) => reply.error(Errno::ENOENT),
            Err(e) => {
                warn!(path = %path, error = ?e, "ossfs getattr failed");
                reply.error(Errno::EIO);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let Some(path) = self.path_of(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if let Some(new_size) = size {
            // Prefer resizing an open write handle; otherwise do a
            // read-modify-write so truncate() on an unopened file works.
            let mut handled = false;
            if let Some(fh) = fh {
                // Lazily load original content before truncating an open
                // write handle.
                let needs_load = {
                    let guard = self.files.lock().unwrap();
                    guard
                        .get(&fh.0)
                        .map(|o| o.path == path && o.write_buf.is_some() && !o.loaded)
                        .unwrap_or(false)
                };
                if needs_load {
                    let data = self
                        .block_on(self.fs.read_range(&path, 0, usize::MAX))
                        .unwrap_or_default();
                    let mut guard = self.files.lock().unwrap();
                    if let Some(open) = guard.get_mut(&fh.0) {
                        if !open.loaded
                            && let Some(buf) = open.write_buf.as_mut()
                        {
                            *buf = data;
                            open.loaded = true;
                        }
                    }
                }
                let mut guard = self.files.lock().unwrap();
                if let Some(open) = guard.get_mut(&fh.0)
                    && open.path == path
                    && open.write_buf.is_some()
                {
                    set_size_buf(open, new_size);
                    handled = true;
                }
                drop(guard);
            }
            if !handled && let Err(e) = self.truncate_unopened(&path, new_size) {
                warn!(path = %path, error = ?e, "ossfs setattr truncate failed");
                reply.error(Errno::EIO);
                return;
            }
        }
        // Object storage has no settable mode/timestamps; reply current attrs.
        match self.block_on(self.fs.stat(&path)) {
            Ok(Some(entry)) => {
                let attr = self.effective_attr(&path, &entry);
                reply.attr(&TTL, &attr);
            }
            Ok(None) => reply.error(Errno::ENOENT),
            Err(e) => {
                warn!(path = %path, error = ?e, "ossfs setattr stat failed");
                reply.error(Errno::EIO);
            }
        }
    }

    fn mknod(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        // Object storage has no device nodes/fifos/sockets; support regular
        // files only (created lazily, empty).
        if !is_regular_file_mode(mode) {
            reply.error(Errno::EPERM);
            return;
        }
        let Some(parent_path) = self.path_of(parent) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };
        let path = join_path(&parent_path, name);
        let exists = match self.block_on(self.fs.stat(&path)) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(e) => {
                warn!(path = %path, error = ?e, "ossfs mknod stat failed");
                reply.error(Errno::EIO);
                return;
            }
        };
        if !exists && let Err(e) = self.block_on(self.fs.write(&path, &[])) {
            warn!(path = %path, error = ?e, "ossfs mknod failed");
            reply.error(Errno::EIO);
            return;
        }
        let attr = self.attr_of(
            &path,
            &DirEntry {
                name: name.to_string(),
                is_dir: false,
                size: 0,
                mtime_secs: 0,
            },
        );
        reply.entry(&TTL, &attr, Generation(0));
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(parent_path) = self.path_of(parent) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };
        let path = join_path(&parent_path, name);
        if let Err(e) = self.block_on(self.fs.mkdir(&path)) {
            warn!(path = %path, error = ?e, "ossfs mkdir failed");
            reply.error(Errno::EIO);
            return;
        }
        let attr = self.attr_of(
            &path,
            &DirEntry {
                name: name.to_string(),
                is_dir: true,
                size: 0,
                mtime_secs: 0,
            },
        );
        reply.entry(&TTL, &attr, Generation(0));
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(parent_path) = self.path_of(parent) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };
        let path = join_path(&parent_path, name);
        // Refuse to unlink a directory (POSIX requires rmdir).
        match self.block_on(self.fs.stat(&path)) {
            Ok(Some(entry)) if entry.is_dir => {
                reply.error(Errno::EISDIR);
                return;
            }
            Ok(Some(_)) => {}
            Ok(None) => {
                reply.error(Errno::ENOENT);
                return;
            }
            Err(e) => {
                warn!(path = %path, error = ?e, "ossfs unlink stat failed");
                reply.error(Errno::EIO);
                return;
            }
        }
        if let Err(e) = self.block_on(self.fs.delete(&path)) {
            warn!(path = %path, error = ?e, "ossfs unlink failed");
            reply.error(Errno::EIO);
            return;
        }
        reply.ok();
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(parent_path) = self.path_of(parent) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };
        let path = join_path(&parent_path, name);
        // The object store deletes a directory tree recursively (matching the
        // WinFsp adapter's cleanup semantics), so `rm -rf` and Finder deletion
        // work even when the kernel cannot empty the dir first.
        if let Err(e) = self.block_on(self.fs.delete_dir_recursive(&path)) {
            warn!(path = %path, error = ?e, "ossfs rmdir failed");
            reply.error(Errno::EIO);
            return;
        }
        reply.ok();
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let (Some(parent_path), Some(newparent_path)) =
            (self.path_of(parent), self.path_of(newparent))
        else {
            reply.error(Errno::ENOENT);
            return;
        };
        let (Some(name), Some(newname)) = (name.to_str(), newname.to_str()) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let old = join_path(&parent_path, name);
        let new = join_path(&newparent_path, newname);
        if let Err(e) = self.block_on(self.fs.rename(&old, &new)) {
            warn!(old = %old, new = %new, error = ?e, "ossfs rename failed");
            reply.error(Errno::EIO);
            return;
        }
        // The kernel re-looks-up the new name; keep the map consistent for the
        // moved path in case it is referenced by its old inode until forget.
        self.register_inode(&new);
        reply.ok();
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let Some(path) = self.path_of(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let entry = match self.block_on(self.fs.stat(&path)) {
            Ok(Some(e)) => e,
            Ok(None) => {
                reply.error(Errno::ENOENT);
                return;
            }
            Err(e) => {
                warn!(path = %path, error = ?e, "ossfs open stat failed");
                reply.error(Errno::EIO);
                return;
            }
        };
        let write = matches!(
            flags.acc_mode(),
            OpenAccMode::O_WRONLY | OpenAccMode::O_RDWR
        );
        let write_buf = if !entry.is_dir && write {
            // Lazy: existing content is fetched on the first write/truncate
            // that needs it, so opening for write never downloads the object.
            Some(Vec::new())
        } else {
            None
        };
        let fh = self.alloc_fh();
        self.files.lock().unwrap().insert(
            fh,
            OpenFile {
                path: path.clone(),
                is_dir: entry.is_dir,
                write_buf,
                loaded: false,
                dirty: false,
            },
        );
        reply.opened(FileHandle(fh), FopenFlags::empty());
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let Some(parent_path) = self.path_of(parent) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };
        let path = join_path(&parent_path, name);
        let truncate = flags & libc::O_TRUNC != 0;
        let existing = match self.block_on(self.fs.stat(&path)) {
            Ok(e) => e,
            Err(e) => {
                warn!(path = %path, error = ?e, "ossfs create stat failed");
                reply.error(Errno::EIO);
                return;
            }
        };
        if let Some(entry) = &existing
            && entry.is_dir
        {
            reply.error(Errno::EISDIR);
            return;
        }
        let needs_existing = existing.is_some() && !truncate;
        let write_buf = Some(Vec::new());
        let attr = self.attr_of(
            &path,
            &DirEntry {
                name: name.to_string(),
                is_dir: false,
                // Existing content is kept but loaded lazily; report the real
                // size so the kernel's initial attr is not 0.
                size: if needs_existing {
                    existing.as_ref().map(|e| e.size).unwrap_or(0)
                } else {
                    0
                },
                mtime_secs: 0,
            },
        );
        let fh = self.alloc_fh();
        self.files.lock().unwrap().insert(
            fh,
            OpenFile {
                path: path.clone(),
                is_dir: false,
                write_buf,
                // New/truncated: empty buffer is authoritative. O_CREAT on an
                // existing file without O_TRUNC: original content is fetched
                // lazily on first write.
                loaded: !needs_existing,
                dirty: false,
            },
        );
        reply.created(
            &TTL,
            &attr,
            Generation(0),
            FileHandle(fh),
            FopenFlags::empty(),
        );
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let open = self.files.lock().unwrap().get(&fh.0).cloned();
        let Some(open) = open else {
            reply.error(Errno::EBADF);
            return;
        };
        if let Some(buf) = open.write_buf
            && open.loaded
        {
            let start = offset.min(buf.len() as u64) as usize;
            let n = (buf.len() - start).min(size as usize);
            reply.data(&buf[start..start + n]);
            return;
        }
        match self.block_on(self.fs.read_range(&open.path, offset, size as usize)) {
            Ok(data) => reply.data(&data),
            Err(e) => {
                warn!(path = %open.path, offset = offset, error = ?e, "ossfs read failed");
                reply.error(Errno::EIO);
            }
        }
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        // Lazily fetch the original content on the first write, without
        // holding the files lock across the S3 round trip.
        let (needs_load, path) = {
            let guard = self.files.lock().unwrap();
            match guard.get(&fh.0) {
                Some(o) => (o.write_buf.is_some() && !o.loaded, o.path.clone()),
                None => {
                    drop(guard);
                    reply.error(Errno::EBADF);
                    return;
                }
            }
        };
        if needs_load {
            let data = self
                .block_on(self.fs.read_range(&path, 0, usize::MAX))
                .unwrap_or_default();
            let mut guard = self.files.lock().unwrap();
            if let Some(o) = guard.get_mut(&fh.0) {
                // Only seed if nobody loaded meanwhile (e.g. a concurrent
                // truncate); their content wins.
                if !o.loaded
                    && let Some(buf) = o.write_buf.as_mut()
                {
                    *buf = data;
                    o.loaded = true;
                }
            }
        }
        let mut guard = self.files.lock().unwrap();
        let Some(open) = guard.get_mut(&fh.0) else {
            drop(guard);
            reply.error(Errno::EBADF);
            return;
        };
        let Some(buf) = open.write_buf.as_mut() else {
            drop(guard);
            reply.error(Errno::EACCES);
            return;
        };
        let start = offset as usize;
        if start.saturating_add(data.len()) > buf.len() {
            buf.resize(start + data.len(), 0);
        }
        buf[start..start + data.len()].copy_from_slice(data);
        open.dirty = true;
        drop(guard);
        reply.written(data.len() as u32);
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let open = self.files.lock().unwrap().get(&fh.0).cloned();
        let Some(open) = open else {
            reply.error(Errno::EBADF);
            return;
        };
        if let Err(e) = self.flush_open(&open) {
            warn!(path = %open.path, error = ?e, "ossfs flush failed");
            reply.error(Errno::EIO);
            return;
        }
        if let Some(o) = self.files.lock().unwrap().get_mut(&fh.0) {
            o.dirty = false;
        }
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let open = self.files.lock().unwrap().get(&fh.0).cloned();
        if let Some(open) = open {
            // Errors on release are not surfaced to the caller; log them.
            if let Err(e) = self.flush_open(&open) {
                warn!(path = %open.path, error = ?e, "ossfs release flush failed");
            }
            self.files.lock().unwrap().remove(&fh.0);
        }
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let open = self.files.lock().unwrap().get(&fh.0).cloned();
        let Some(open) = open else {
            reply.error(Errno::EBADF);
            return;
        };
        if let Err(e) = self.flush_open(&open) {
            warn!(path = %open.path, error = ?e, "ossfs fsync failed");
            reply.error(Errno::EIO);
            return;
        }
        if let Some(o) = self.files.lock().unwrap().get_mut(&fh.0) {
            o.dirty = false;
        }
        reply.ok();
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let Some(path) = self.path_of(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let entries = match self.block_on(self.fs.list(&path)) {
            Ok(e) => e,
            Err(e) => {
                warn!(path = %path, error = ?e, "ossfs readdir failed");
                reply.error(Errno::EIO);
                return;
            }
        };
        // Remember this directory so the periodic refresh can invalidate it.
        // Bounded: when the tracked set exceeds MAX_TRACKED_DIRS we reset to
        // just the root so a pathological tree cannot grow memory or the
        // per-tick invalidation loop without limit.
        {
            let mut dirs = self.dirs.lock().unwrap();
            dirs.insert(ino.0);
            if dirs.len() > MAX_TRACKED_DIRS {
                dirs.clear();
                dirs.insert(ROOT_INODE);
            }
        }
        // "." and ".." first (Finder expects them), then children sorted by
        // name for a stable readdir cursor.
        let mut items: Vec<(String, u64, FileType)> = Vec::with_capacity(entries.len() + 2);
        items.push((".".to_string(), ino.0, FileType::Directory));
        let parent_ino = if ino.0 == ROOT_INODE {
            ROOT_INODE
        } else {
            let parent = super::parent_path(&path);
            self.register_inode(&parent)
        };
        items.push(("..".to_string(), parent_ino, FileType::Directory));
        for entry in entries {
            let child = join_path(&path, &entry.name);
            let kind = if entry.is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            items.push((entry.name, self.register_inode(&child), kind));
        }
        items.sort_by(|a, b| a.0.cmp(&b.0));

        for (i, (name, ino, kind)) in items.iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(*ino), (i + 1) as u64, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        // Object storage has no fixed capacity; report a large synthetic pool.
        let total = 1 << 50; // 1 PiB
        reply.statfs(
            total,
            total,
            total,
            u64::MAX / 2,
            u64::MAX / 2,
            4096,
            NAME_MAX,
            4096,
        );
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        // Permission checks are best-effort on a network drive; allow all.
        reply.ok();
    }

    fn listxattr(&self, _req: &Request, _ino: INodeNo, size: u32, reply: ReplyXattr) {
        if size == 0 {
            reply.size(0);
        } else {
            reply.data(&[]);
        }
    }

    fn getxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(Errno::NO_XATTR);
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::NO_XATTR);
    }
}

/// Runtime record the desktop tray app uses to list and stop `ossmount`
/// instances. Kept in `$TMPDIR/brewfs-oss` so it never mixes with the BrewFS
/// control-plane registry (`$TMPDIR/brewfs`), matching the Windows adapter.
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

fn build_config() -> Config {
    let mut cfg = Config::default();
    cfg.mount_options = vec![MountOption::FSName("BrewFS-OSS".to_string())];
    #[cfg(target_os = "macos")]
    cfg.mount_options
        .push(MountOption::Subtype("macfuse".to_string()));
    cfg.acl = SessionACL::Owner;
    cfg.n_threads = Some(4);
    cfg
}

/// Mount an [`ObjectFs`] at `mount_point` via FUSE (macFUSE on macOS, libfuse
/// on Linux). Runs until Ctrl+C / SIGTERM / external unmount, then tears down
/// gracefully.
pub async fn mount_oss_fuse(
    fs: Arc<ObjectFs>,
    mount_point: &Path,
    refresh_secs: u64,
) -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        if !Path::new("/Library/Filesystems/macfuse.fs").exists() {
            anyhow::bail!(
                "macFUSE 未安装：请先安装 macFUSE（https://macfuse.github.io/），OSS 直挂需要它"
            );
        }
    }

    // Fail fast: verify the bucket is reachable before mounting, so we never
    // present a mount that every operation errors on.
    fs.list("/").await?;

    if !mount_point.exists() {
        std::fs::create_dir_all(mount_point).ok();
    }

    let handle = Handle::current();
    let dirs = Arc::new(Mutex::new(HashSet::new()));
    let oss_fs = OssFs::new(fs, handle, Arc::clone(&dirs));
    let session = fuser::spawn_mount2(oss_fs, mount_point, &build_config())
        .map_err(|e| anyhow::anyhow!("failed to mount at {}: {e}", mount_point.display()))?;

    info!(mount_point = %mount_point.display(), "brewfs-oss mounted via FUSE");
    println!("mounted at {}", mount_point.display());
    write_runtime_record(mount_point);

    // Periodic directory refresh: invalidate the kernel caches of every
    // directory that has been listed so changes made by other machines show
    // up without a manual refresh. The kernel re-lists lazily on the next
    // access, so this costs an S3 list only when the user actually browses.
    // macFUSE does not support kernel notifications; the errors are ignored
    // there (the 1s TTL still keeps attribute reads fresh).
    if refresh_secs > 0 {
        let notifier = session.notifier();
        let dirs = Arc::clone(&dirs);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(refresh_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // The first tick fires immediately; consume it so the first
            // refresh waits one full interval.
            interval.tick().await;
            loop {
                interval.tick().await;
                let inodes: Vec<u64> = dirs.lock().unwrap().iter().copied().collect();
                for ino in inodes {
                    let _ = notifier.inval_inode(INodeNo(ino), 0, 0);
                }
            }
        });
    }

    #[cfg(unix)]
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                break;
            }
            _ = async {
                #[cfg(unix)]
                if let Some(sig) = sigterm.as_mut() {
                    sig.recv().await;
                }
                #[cfg(not(unix))]
                std::future::pending::<()>().await;
            } => { break; }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                if session.guard.is_finished() {
                    // The session ended on its own (e.g. user ejected the
                    // volume in Finder / ran `umount`).
                    println!("filesystem session ended (unmounted externally)");
                    remove_runtime_record();
                    return Ok(());
                }
            }
        }
    }

    println!("unmounting...");
    let result = session.umount_and_join();
    remove_runtime_record();
    result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inode_for_path_is_stable_and_distinct() {
        let a = inode_for_path("/docs/report.txt");
        let b = inode_for_path("/docs/report.txt");
        assert_eq!(a, b, "same path must map to the same inode");
        assert_ne!(a, ROOT_INODE, "non-root paths must not collide with root");
        assert_ne!(a, inode_for_path("/docs/report2.txt"));
        assert_eq!(inode_for_path("/"), ROOT_INODE);
        assert_ne!(inode_for_path("/"), 0);
    }

    #[test]
    fn join_path_handles_root_and_nested() {
        assert_eq!(join_path("/", "a.txt"), "/a.txt");
        assert_eq!(join_path("/docs", "a.txt"), "/docs/a.txt");
        assert_eq!(join_path("/a/b", "c"), "/a/b/c");
    }

    #[test]
    fn is_regular_file_mode_detects_regular_files() {
        assert!(is_regular_file_mode(0o100644));
        assert!(!is_regular_file_mode(0o040755)); // directory
        assert!(!is_regular_file_mode(0o120777)); // symlink
    }

    #[test]
    fn epoch_maps_nonpositive_to_unix_epoch() {
        assert_eq!(epoch(0), UNIX_EPOCH);
        assert_eq!(epoch(-5), UNIX_EPOCH);
        assert_eq!(
            epoch(1_700_000_000),
            UNIX_EPOCH + Duration::from_secs(1_700_000_000)
        );
    }
}
