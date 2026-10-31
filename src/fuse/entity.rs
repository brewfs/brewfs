//! Dedicated, explicitly experimental current-entity FUSE adapter.
//!
//! This adapter never dispatches an entity inode through the native VFS. Files
//! use direct I/O, metadata replies have zero TTL, and kernel writeback requests
//! are rejected. Mount admission and writer renewal belong to the factory.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::num::NonZeroU32;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asyncfuse::raw::flags::{FOPEN_DIRECT_IO, FUSE_WRITE_CACHE};
use asyncfuse::raw::reply::{
    DirectoryEntry, DirectoryEntryPlus, FileAttr, ReplyAttr, ReplyCreated, ReplyData,
    ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit, ReplyLock, ReplyOpen, ReplyWrite,
    ReplyXAttr,
};
use asyncfuse::raw::{Filesystem, Request};
use asyncfuse::{Errno, FileType, Result as FuseResult, SetAttr};
use bytes::Bytes;
use futures_util::stream::{self, BoxStream};

use crate::cadapter::client::ObjectBackend;
use crate::workspace_overlay::entity::backend::EntityBackend;
use crate::workspace_overlay::entity::data::EntityData;
use crate::workspace_overlay::entity::model::{EntityInode, EntityKind};
use crate::workspace_overlay::entity::namespace::{
    CreateRequest, Credentials, EntityHandle, EntityNamespace,
};
use crate::workspace_overlay::entity::resolver::ImmutableLowerData;
use crate::workspace_overlay::entity::{EntityError, Result};

#[cfg(test)]
#[path = "entity_tests.rs"]
mod tests;

/// The experimental bootstrap uses 4 MiB chunks. A 1 MiB request stays
/// within EntityData's bounded I/O geometry even when it straddles a chunk.
pub const MAX_WRITE: u32 = 1024 * 1024;

struct OpenFile {
    handle: Arc<EntityHandle>,
    read: bool,
    write: bool,
    append: bool,
}

struct OpenDirectory {
    ino: i64,
    // Creating this handle grants enumeration for its lifetime.
    credentials: Credentials,
    snapshot: tokio::sync::Mutex<DirectorySnapshot>,
}

struct DirectorySnapshot {
    entries: Vec<(DirectoryEntry, EntityInode)>,
    started: bool,
}

struct Handles {
    closed: bool,
    next: u64,
    files: BTreeMap<u64, Arc<OpenFile>>,
    directories: BTreeMap<u64, Arc<OpenDirectory>>,
}

impl Default for Handles {
    fn default() -> Self {
        Self {
            closed: false,
            next: 1,
            files: BTreeMap::new(),
            directories: BTreeMap::new(),
        }
    }
}

impl Handles {
    fn live(&self) -> FuseResult<()> {
        if self.closed {
            Err(libc::ESTALE.into())
        } else {
            Ok(())
        }
    }

    fn reserve(&mut self) -> FuseResult<u64> {
        self.live()?;
        let handle = self.next;
        self.next = self.next.checked_add(1).ok_or(Errno::from(libc::EMFILE))?;
        Ok(handle)
    }
}

struct State<B, L, O> {
    namespace: Arc<EntityNamespace<B, L>>,
    data: Arc<EntityData<B, L, O>>,
    root: i64,
    // This mutex covers only handle bookkeeping, never metadata or object I/O.
    handles: Mutex<Handles>,
}

pub struct EntityFuse<B, L, O> {
    state: Arc<State<B, L, O>>,
}

impl<B, L, O> Clone for EntityFuse<B, L, O> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

fn errno(error: EntityError) -> Errno {
    match error {
        EntityError::Permission => libc::EACCES,
        EntityError::NotFound => libc::ENOENT,
        EntityError::Exists => libc::EEXIST,
        EntityError::IsDirectory => libc::EISDIR,
        EntityError::NotDirectory => libc::ENOTDIR,
        EntityError::NotEmpty => libc::ENOTEMPTY,
        EntityError::Fenced => libc::ESTALE,
        EntityError::Busy | EntityError::Conflict => libc::EBUSY,
        EntityError::Overflow | EntityError::Exhausted => libc::EFBIG,
        EntityError::Invalid(_) => libc::EINVAL,
        EntityError::Unsupported(_) => libc::EOPNOTSUPP,
        EntityError::Corrupt(_) | EntityError::Backend(_) => libc::EIO,
    }
    .into()
}

fn credentials(request: Request, umask: u32) -> FuseResult<Credentials> {
    Ok(Credentials {
        uid: request.uid,
        gid: request.gid,
        groups: if request.uid == 0 {
            vec![request.gid]
        } else {
            super::verified_request_group_ids(request.pid, request.uid, request.gid)?
        },
        umask,
    })
}

fn kind(value: EntityKind) -> FileType {
    match value {
        EntityKind::File => FileType::RegularFile,
        EntityKind::Directory => FileType::Directory,
        EntityKind::Symlink => FileType::Symlink,
        EntityKind::Fifo => FileType::NamedPipe,
        EntityKind::Socket => FileType::Socket,
        EntityKind::CharDevice => FileType::CharDevice,
        EntityKind::BlockDevice => FileType::BlockDevice,
    }
}

fn open_access(flags: u32) -> FuseResult<(u32, bool, bool)> {
    let (access, read, write) = match flags & libc::O_ACCMODE as u32 {
        x if x == libc::O_RDONLY as u32 => (4, true, false),
        x if x == libc::O_WRONLY as u32 => (2, false, true),
        x if x == libc::O_RDWR as u32 => (6, true, true),
        _ => return Err(libc::EINVAL.into()),
    };
    if flags & libc::O_TRUNC as u32 != 0 && !write {
        return Err(libc::EACCES.into());
    }
    Ok((access, read, write))
}

impl<B, L, O> EntityFuse<B, L, O>
where
    B: EntityBackend + 'static,
    L: ImmutableLowerData + 'static,
    O: ObjectBackend + 'static,
{
    /// Supply the namespace and its data path for this experimental mount.
    pub fn new(
        namespace: Arc<EntityNamespace<B, L>>,
        data: Arc<EntityData<B, L, O>>,
        root_ino: i64,
    ) -> Result<Self> {
        if root_ino <= 0 {
            return Err(EntityError::Invalid("invalid entity FUSE root".into()));
        }
        Ok(Self {
            state: Arc::new(State {
                namespace,
                data,
                root: root_ino,
                handles: Mutex::new(Handles::default()),
            }),
        })
    }

    /// Stop new requests and release retained opens. In-flight owned data and
    /// namespace tasks keep their own admission until they finish. The factory's
    /// orderly_close must subsequently await that session drain.
    pub fn stop_admission(&self) {
        let mut handles = self.state.handles.lock().expect("entity handles poisoned");
        handles.closed = true;
        handles.files.clear();
        handles.directories.clear();
    }

    fn live(&self) -> FuseResult<()> {
        self.state
            .handles
            .lock()
            .map_err(|_| Errno::from(libc::EIO))?
            .live()
    }

    fn reserve_handle(&self) -> FuseResult<u64> {
        self.state
            .handles
            .lock()
            .map_err(|_| Errno::from(libc::EIO))?
            .reserve()
    }

    fn ino(&self, node: u64) -> FuseResult<i64> {
        self.live()?;
        if node == 1 {
            return Ok(self.state.root);
        }
        let value = node
            .checked_sub(1)
            .and_then(|n| i64::try_from(n).ok())
            .filter(|n| *n > 0 && *n != self.state.root)
            .ok_or(Errno::from(libc::ESTALE))?;
        Ok(value)
    }

    fn node(&self, ino: i64) -> u64 {
        if ino == self.state.root {
            1
        } else {
            ino as u64 + 1
        }
    }

    fn attr(&self, inode: &EntityInode) -> FileAttr {
        FileAttr {
            ino: self.node(inode.ino),
            size: inode.size,
            blocks: inode.size.div_ceil(512),
            atime: super::nanos_to_timestamp(inode.atime_ns),
            mtime: super::nanos_to_timestamp(inode.mtime_ns),
            ctime: super::nanos_to_timestamp(inode.ctime_ns),
            #[cfg(target_os = "macos")]
            crtime: super::nanos_to_timestamp(inode.ctime_ns),
            kind: kind(inode.kind),
            perm: (inode.mode & 0o7777) as u16,
            nlink: inode.nlink,
            uid: inode.uid,
            gid: inode.gid,
            rdev: inode.rdev,
            #[cfg(target_os = "macos")]
            flags: 0,
            blksize: 4096,
        }
    }

    fn entry(&self, inode: &EntityInode) -> ReplyEntry {
        ReplyEntry {
            ttl: Duration::ZERO,
            attr: self.attr(inode),
            generation: 0,
        }
    }

    fn file(&self, node: u64, fh: u64) -> FuseResult<Arc<OpenFile>> {
        let ino = self.ino(node)?;
        let handles = self
            .state
            .handles
            .lock()
            .map_err(|_| Errno::from(libc::EIO))?;
        handles.live()?;
        handles
            .files
            .get(&fh)
            .filter(|file| file.handle.ino() == ino)
            .cloned()
            .ok_or(Errno::from(libc::EBADF))
    }

    fn directory(&self, node: u64, fh: u64) -> FuseResult<Arc<OpenDirectory>> {
        let ino = self.ino(node)?;
        let handles = self
            .state
            .handles
            .lock()
            .map_err(|_| Errno::from(libc::EIO))?;
        handles.live()?;
        handles
            .directories
            .get(&fh)
            .filter(|directory| directory.ino == ino)
            .cloned()
            .ok_or(Errno::from(libc::EBADF))
    }

    async fn directory_snapshot(
        &self,
        ino: i64,
        credentials: &Credentials,
    ) -> FuseResult<Vec<(DirectoryEntry, EntityInode)>> {
        let listed = self
            .state
            .namespace
            .readdir_snapshot(ino, credentials)
            .await
            .map_err(errno)?;
        let mut entries = Vec::with_capacity(listed.entries.len() + 2);
        entries.push((
            DirectoryEntry {
                inode: self.node(ino),
                kind: FileType::Directory,
                name: OsString::from("."),
                offset: 1,
            },
            listed.directory,
        ));
        entries.push((
            DirectoryEntry {
                inode: self.node(listed.parent.ino),
                kind: FileType::Directory,
                name: OsString::from(".."),
                offset: 2,
            },
            listed.parent,
        ));
        for (name, child) in listed.entries {
            entries.push((
                DirectoryEntry {
                    inode: self.node(child.ino),
                    kind: kind(child.kind),
                    name: OsString::from_vec(name),
                    offset: entries.len() as i64 + 1,
                },
                child,
            ));
        }
        Ok(entries)
    }

    async fn read_directory(
        &self,
        directory: &OpenDirectory,
        offset: usize,
    ) -> FuseResult<Vec<(DirectoryEntry, EntityInode)>> {
        // Serialize a rewind with reads of this handle, without holding the
        // global handle bookkeeping lock across metadata I/O. A failed refresh
        // leaves the previous snapshot intact.
        let mut snapshot = directory.snapshot.lock().await;
        if offset == 0 && snapshot.started {
            // Opening the handle granted enumeration for its lifetime, so a
            // permission change after opendir must not revoke it: keep serving
            // the last good snapshot when the refresh is refused. Structural
            // failures (the directory is gone, or the authority is fenced) still
            // surface to the caller.
            match self
                .directory_snapshot(directory.ino, &directory.credentials)
                .await
            {
                Ok(entries) => snapshot.entries = entries,
                Err(error)
                    if error == Errno::from(libc::EACCES) || error == Errno::from(libc::EPERM) => {}
                Err(error) => return Err(error),
            }
        }
        snapshot.started = true;
        Ok(snapshot.entries.iter().skip(offset).cloned().collect())
    }

    fn store_file(&self, fh: u64, file: OpenFile) -> FuseResult<()> {
        let mut handles = self
            .state
            .handles
            .lock()
            .map_err(|_| Errno::from(libc::EIO))?;
        handles.live()?;
        handles.files.insert(fh, Arc::new(file));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_file(
        &self,
        request: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: u32,
    ) -> FuseResult<ReplyCreated> {
        let parent = self.ino(parent)?;
        let (access, read, write) = open_access(flags)?;
        let fh = self.reserve_handle()?;
        let credentials = credentials(request, umask)?;
        let created = self
            .state
            .namespace
            .create_open(
                CreateRequest {
                    parent,
                    name: name.as_bytes().to_vec(),
                    kind: EntityKind::File,
                    mode: mode & 0o7777,
                    rdev: 0,
                    symlink_target: None,
                },
                credentials.clone(),
                access,
            )
            .await;
        let (inode, handle, created) = match created {
            Ok((inode, handle)) => (inode, Arc::new(handle), true),
            Err(EntityError::Exists) if flags & libc::O_EXCL as u32 == 0 => {
                let (inode, handle) = self
                    .state
                    .namespace
                    .open_with_attr(parent, name.as_bytes(), access, &credentials)
                    .await
                    .map_err(errno)?;
                let handle = Arc::new(handle);
                (inode, handle, false)
            }
            Err(error) => return Err(errno(error)),
        };
        let inode = if !created && flags & libc::O_TRUNC as u32 != 0 {
            self.state
                .data
                .truncate(handle.clone(), 0)
                .await
                .map_err(errno)?
        } else {
            inode
        };
        self.store_file(
            fh,
            OpenFile {
                handle,
                read,
                write,
                append: flags & libc::O_APPEND as u32 != 0,
            },
        )?;
        Ok(ReplyCreated {
            ttl: Duration::ZERO,
            attr: self.attr(&inode),
            generation: 0,
            fh,
            flags: FOPEN_DIRECT_IO,
        })
    }
}

#[allow(refining_impl_trait_reachable)]
impl<B, L, O> Filesystem for EntityFuse<B, L, O>
where
    B: EntityBackend + 'static,
    L: ImmutableLowerData + 'static,
    O: ObjectBackend + 'static,
{
    async fn init(&self, request: Request) -> FuseResult<ReplyInit> {
        self.live()?;
        let root = self
            .state
            .namespace
            .getattr(self.state.root, &credentials(request, 0)?)
            .await
            .map_err(errno)?;
        if root.kind != EntityKind::Directory {
            return Err(libc::ENOTDIR.into());
        }
        Ok(ReplyInit {
            max_write: NonZeroU32::new(MAX_WRITE).expect("nonzero entity max write"),
        })
    }

    async fn destroy(&self, _request: Request) {
        self.stop_admission();
    }

    async fn prepare_unmount(&self) -> FuseResult<()> {
        self.stop_admission();
        Ok(())
    }

    async fn lookup(&self, request: Request, parent: u64, name: &OsStr) -> FuseResult<ReplyEntry> {
        let parent = self.ino(parent)?;
        let inode = self
            .state
            .namespace
            .lookup(parent, name.as_bytes(), &credentials(request, 0)?)
            .await
            .map_err(errno)?;
        Ok(self.entry(&inode))
    }

    async fn getattr(
        &self,
        request: Request,
        inode: u64,
        fh: Option<u64>,
        _flags: u32,
    ) -> FuseResult<ReplyAttr> {
        let ino = self.ino(inode)?;
        let attr = if let Some(fh) = fh {
            match self.file(inode, fh) {
                Ok(file) => self
                    .state
                    .data
                    .attr(file.handle.clone())
                    .await
                    .map_err(errno)?,
                Err(_) => {
                    self.directory(inode, fh)?;
                    self.state
                        .namespace
                        .getattr(ino, &credentials(request, 0)?)
                        .await
                        .map_err(errno)?
                }
            }
        } else {
            self.state
                .namespace
                .getattr(ino, &credentials(request, 0)?)
                .await
                .map_err(errno)?
        };
        Ok(ReplyAttr {
            ttl: Duration::ZERO,
            attr: self.attr(&attr),
        })
    }

    async fn readlink(&self, request: Request, inode: u64) -> FuseResult<ReplyData> {
        let inode = self
            .state
            .namespace
            .getattr(self.ino(inode)?, &credentials(request, 0)?)
            .await
            .map_err(errno)?;
        if inode.kind != EntityKind::Symlink {
            return Err(libc::EINVAL.into());
        }
        Ok(ReplyData {
            data: Bytes::from(inode.symlink_target.ok_or(Errno::from(libc::EIO))?),
        })
    }

    async fn getxattr(
        &self,
        request: Request,
        inode: u64,
        name: &OsStr,
        size: u32,
    ) -> FuseResult<ReplyXAttr> {
        let value = self
            .state
            .namespace
            .get_xattr(self.ino(inode)?, name.as_bytes(), &credentials(request, 0)?)
            .await
            .map_err(errno)?
            .ok_or(Errno::from(libc::ENODATA))?;
        if size == 0 {
            return Ok(ReplyXAttr::Size(value.len() as u32));
        }
        if (size as usize) < value.len() {
            return Err(libc::ERANGE.into());
        }
        Ok(ReplyXAttr::Data(Bytes::from(value)))
    }

    async fn setattr(
        &self,
        request: Request,
        inode: u64,
        fh: Option<u64>,
        attr: SetAttr,
    ) -> FuseResult<ReplyAttr> {
        let ino = self.ino(inode)?;
        // Refuse a partially supported request before changing any metadata.
        let supported = SetAttr {
            size: attr.size,
            mode: attr.mode,
            lock_owner: attr.lock_owner,
            ..SetAttr::default()
        };
        if attr != supported || (attr.size.is_some() && attr.mode.is_some()) {
            return Err(libc::EOPNOTSUPP.into());
        }
        let credentials = credentials(request, 0)?;
        if let Some(size) = attr.size {
            let handle = if let Some(fh) = fh {
                let file = self.file(inode, fh)?;
                if !file.write {
                    return Err(libc::EACCES.into());
                }
                file.handle.clone()
            } else {
                let handle = self
                    .state
                    .namespace
                    .open_inode(ino, 2, &credentials)
                    .await
                    .map_err(errno)?;
                Arc::new(handle)
            };
            let inode = self
                .state
                .data
                .truncate(handle, size)
                .await
                .map_err(errno)?;
            return Ok(ReplyAttr {
                ttl: Duration::ZERO,
                attr: self.attr(&inode),
            });
        }
        if let Some(mode) = attr.mode {
            if let Some(fh) = fh
                && self.file(inode, fh).is_err()
            {
                self.directory(inode, fh)?;
            }
            self.state
                .namespace
                .chmod(ino, mode & 0o7777, credentials)
                .await
                .map_err(errno)?;
        }
        self.getattr(request, inode, fh, 0).await
    }

    async fn open(&self, request: Request, inode: u64, flags: u32) -> FuseResult<ReplyOpen> {
        let ino = self.ino(inode)?;
        let (access, read, write) = open_access(flags)?;
        let fh = self.reserve_handle()?;
        let handle = self
            .state
            .namespace
            .open_inode(ino, access, &credentials(request, 0)?)
            .await
            .map_err(errno)?;
        let handle = Arc::new(handle);
        if flags & libc::O_TRUNC as u32 != 0 {
            self.state
                .data
                .truncate(handle.clone(), 0)
                .await
                .map_err(errno)?;
        }
        self.store_file(
            fh,
            OpenFile {
                handle,
                read,
                write,
                append: flags & libc::O_APPEND as u32 != 0,
            },
        )?;
        Ok(ReplyOpen {
            fh,
            flags: FOPEN_DIRECT_IO,
        })
    }

    async fn create(
        &self,
        request: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> FuseResult<ReplyCreated> {
        self.create_file(request, parent, name, mode, 0, flags)
            .await
    }

    async fn create_with_umask(
        &self,
        request: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: u32,
    ) -> FuseResult<ReplyCreated> {
        self.create_file(request, parent, name, mode, umask, flags)
            .await
    }

    async fn read(
        &self,
        _request: Request,
        inode: u64,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> FuseResult<ReplyData> {
        let file = self.file(inode, fh)?;
        if !file.read {
            return Err(libc::EACCES.into());
        }
        Ok(ReplyData {
            data: Bytes::from(
                self.state
                    .data
                    .read(file.handle.clone(), offset, size as usize)
                    .await
                    .map_err(errno)?,
            ),
        })
    }

    async fn write(
        &self,
        _request: Request,
        inode: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
        write_flags: u32,
        _flags: u32,
    ) -> FuseResult<ReplyWrite> {
        if write_flags & FUSE_WRITE_CACHE != 0 {
            return Err(libc::EOPNOTSUPP.into());
        }
        if data.len() > MAX_WRITE as usize {
            return Err(libc::EINVAL.into());
        }
        let file = self.file(inode, fh)?;
        if !file.write {
            return Err(libc::EACCES.into());
        }
        let written = self
            .state
            .data
            .write(
                file.handle.clone(),
                offset,
                Bytes::copy_from_slice(data),
                file.append,
            )
            .await
            .map_err(errno)?;
        Ok(ReplyWrite {
            written: written.written as u32,
        })
    }

    async fn flush(&self, _request: Request, inode: u64, fh: u64, _owner: u64) -> FuseResult<()> {
        self.state
            .data
            .fsync(self.file(inode, fh)?.handle.clone())
            .await
            .map_err(errno)
    }

    async fn fsync(
        &self,
        _request: Request,
        inode: u64,
        fh: u64,
        _datasync: bool,
    ) -> FuseResult<()> {
        self.state
            .data
            .fsync(self.file(inode, fh)?.handle.clone())
            .await
            .map_err(errno)
    }

    async fn release(
        &self,
        _request: Request,
        inode: u64,
        fh: u64,
        _flags: u32,
        _owner: u64,
        flush: bool,
    ) -> FuseResult<()> {
        let file = self.file(inode, fh)?;
        let result = if flush {
            self.state
                .data
                .flush_release(file.handle.clone())
                .await
                .map_err(errno)
        } else {
            Ok(())
        };
        self.state
            .handles
            .lock()
            .map_err(|_| Errno::from(libc::EIO))?
            .files
            .remove(&fh);
        result
    }

    async fn mkdir(
        &self,
        request: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> FuseResult<ReplyEntry> {
        let inode = self
            .state
            .namespace
            .create(
                CreateRequest {
                    parent: self.ino(parent)?,
                    name: name.as_bytes().to_vec(),
                    kind: EntityKind::Directory,
                    mode: mode & 0o7777,
                    rdev: 0,
                    symlink_target: None,
                },
                credentials(request, umask)?,
            )
            .await
            .map_err(errno)?;
        Ok(self.entry(&inode))
    }

    async fn unlink(&self, request: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        self.state
            .namespace
            .unlink(
                self.ino(parent)?,
                name.as_bytes().to_vec(),
                false,
                credentials(request, 0)?,
            )
            .await
            .map_err(errno)
    }

    async fn rmdir(&self, request: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        self.state
            .namespace
            .unlink(
                self.ino(parent)?,
                name.as_bytes().to_vec(),
                true,
                credentials(request, 0)?,
            )
            .await
            .map_err(errno)
    }

    async fn access(&self, request: Request, inode: u64, mask: u32) -> FuseResult<()> {
        self.state
            .namespace
            .access(self.ino(inode)?, mask, &credentials(request, 0)?)
            .await
            .map_err(errno)
    }

    async fn opendir(&self, request: Request, inode: u64, flags: u32) -> FuseResult<ReplyOpen> {
        if flags & libc::O_ACCMODE as u32 != libc::O_RDONLY as u32 {
            return Err(libc::EACCES.into());
        }
        let ino = self.ino(inode)?;
        let fh = self.reserve_handle()?;
        let credentials = credentials(request, 0)?;
        let entries = self.directory_snapshot(ino, &credentials).await?;
        let mut handles = self
            .state
            .handles
            .lock()
            .map_err(|_| Errno::from(libc::EIO))?;
        handles.live()?;
        handles.directories.insert(
            fh,
            Arc::new(OpenDirectory {
                ino,
                credentials,
                snapshot: tokio::sync::Mutex::new(DirectorySnapshot {
                    entries,
                    started: false,
                }),
            }),
        );
        Ok(ReplyOpen { fh, flags: 0 })
    }

    async fn readdir<'a>(
        &'a self,
        _request: Request,
        inode: u64,
        fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectory<BoxStream<'a, FuseResult<DirectoryEntry>>>> {
        let directory = self.directory(inode, fh)?;
        let offset = usize::try_from(offset).map_err(|_| Errno::from(libc::EINVAL))?;
        let _admission = self
            .state
            .namespace
            .admit_directory_read()
            .await
            .map_err(errno)?;
        let entries = self
            .read_directory(&directory, offset)
            .await?
            .into_iter()
            .map(|(entry, _)| Ok(entry))
            .collect::<Vec<_>>();
        Ok(ReplyDirectory {
            entries: Box::pin(stream::iter(entries)),
        })
    }

    async fn releasedir(
        &self,
        _request: Request,
        inode: u64,
        fh: u64,
        _flags: u32,
    ) -> FuseResult<()> {
        self.directory(inode, fh)?;
        self.state
            .handles
            .lock()
            .map_err(|_| Errno::from(libc::EIO))?
            .directories
            .remove(&fh);
        Ok(())
    }

    async fn readdirplus<'a>(
        &'a self,
        _request: Request,
        inode: u64,
        fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> FuseResult<ReplyDirectoryPlus<BoxStream<'a, FuseResult<DirectoryEntryPlus>>>> {
        let directory = self.directory(inode, fh)?;
        let offset = usize::try_from(offset).map_err(|_| Errno::from(libc::EINVAL))?;
        let _admission = self
            .state
            .namespace
            .admit_directory_read()
            .await
            .map_err(errno)?;
        let mut entries = Vec::new();
        for (entry, attr) in self.read_directory(&directory, offset).await? {
            entries.push(Ok(DirectoryEntryPlus {
                inode: entry.inode,
                generation: 0,
                kind: entry.kind,
                name: entry.name,
                offset: entry.offset,
                attr: self.attr(&attr),
                entry_ttl: Duration::ZERO,
                attr_ttl: Duration::ZERO,
            }));
        }
        Ok(ReplyDirectoryPlus {
            entries: Box::pin(stream::iter(entries)),
        })
    }

    async fn fsyncdir(
        &self,
        request: Request,
        inode: u64,
        fh: u64,
        _datasync: bool,
    ) -> FuseResult<()> {
        let directory = self.directory(inode, fh)?;
        self.state
            .namespace
            .getattr(directory.ino, &credentials(request, 0)?)
            .await
            .map_err(errno)?;
        Ok(())
    }

    async fn getlk(
        &self,
        _request: Request,
        _inode: u64,
        _fh: u64,
        _owner: u64,
        _start: u64,
        _end: u64,
        _kind: u32,
        _pid: u32,
    ) -> FuseResult<ReplyLock> {
        Err(libc::EOPNOTSUPP.into())
    }

    async fn setlk(
        &self,
        _request: Request,
        _inode: u64,
        _fh: u64,
        _owner: u64,
        _start: u64,
        _end: u64,
        _kind: u32,
        _pid: u32,
        _block: bool,
    ) -> FuseResult<()> {
        Err(libc::EOPNOTSUPP.into())
    }
}

fn mount_options() -> asyncfuse::MountOptions {
    let mut options = asyncfuse::MountOptions::default();
    options.fs_name("brewfs-entity-experimental");
    options.default_permissions(false);
    options.allow_other(true);
    options.write_back(false);
    options.direct_io(true);
    options.async_dio(true);
    options.posix_acl(false);
    options.dont_mask(true);
    options.max_write(NonZeroU32::new(MAX_WRITE).expect("nonzero entity max write"));
    options.max_readahead(Some(0));
    options.custom_options(format!("max_read={MAX_WRITE}"));
    options
}

#[cfg(target_os = "linux")]
pub async fn mount_unprivileged<B, L, O>(
    filesystem: EntityFuse<B, L, O>,
    mount_point: impl AsRef<Path>,
    concurrency: super::mount::FuseConcurrencyConfig,
) -> std::io::Result<asyncfuse::raw::MountHandle>
where
    B: EntityBackend + 'static,
    L: ImmutableLowerData + 'static,
    O: ObjectBackend + 'static,
{
    #[cfg(feature = "observability")]
    let filesystem = crate::observability::fuse::ObservedFilesystem(filesystem);
    asyncfuse::raw::Session::new(mount_options())
        .with_workers(
            concurrency.worker_count.max(1),
            concurrency.max_background.max(1),
        )
        .mount_with_unprivileged(filesystem, mount_point)
        .await
}

#[cfg(target_os = "linux")]
pub async fn mount_privileged<B, L, O>(
    filesystem: EntityFuse<B, L, O>,
    mount_point: impl AsRef<Path>,
    concurrency: super::mount::FuseConcurrencyConfig,
) -> std::io::Result<asyncfuse::raw::MountHandle>
where
    B: EntityBackend + 'static,
    L: ImmutableLowerData + 'static,
    O: ObjectBackend + 'static,
{
    #[cfg(feature = "observability")]
    let filesystem = crate::observability::fuse::ObservedFilesystem(filesystem);
    asyncfuse::raw::Session::new(mount_options())
        .with_workers(
            concurrency.worker_count.max(1),
            concurrency.max_background.max(1),
        )
        .mount(filesystem, mount_point)
        .await
}
