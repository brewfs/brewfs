use async_trait::async_trait;
use std::sync::Arc;

use crate::chunk::SliceDesc;
use crate::meta::client::MetaClientMetrics;
use crate::meta::client::session::SessionInfo;
use crate::meta::file_lock::{FileLockInfo, FileLockQuery, FileLockRange, FileLockType};
use crate::meta::store::{
    AclRule, CreateEntryResult, DirEntry, FileAttr, FileType, MetaError, OpenFlags, SetAttrFlags,
    SetAttrRequest, StatFsSnapshot, chmod_request, chown_request,
};
use crate::vfs::handles::DirHandle;

pub type MetadataMemoryGuard = Arc<dyn std::fmt::Debug + Send + Sync>;

/// Raw xattr names and the admission retained through their final consumer.
pub struct OwnedXattrNames {
    pub names: Vec<Vec<u8>>,
    pub guard: Option<MetadataMemoryGuard>,
}

/// Raw inode paths and the admission/reader retained through their consumer.
/// Fields drop in declaration order: path storage is freed before its owner.
#[derive(Debug)]
pub struct OwnedPaths {
    pub paths: Vec<Vec<u8>>,
    pub guard: Option<MetadataMemoryGuard>,
}

#[derive(Clone, Copy, Debug)]
pub enum MetadataMemoryKind {
    Roots,
    Request,
    Reply,
    /// Persistent handle attributes, readers and snapshot identity. The owner
    /// remains alive independently of transient request/queue control state.
    Handle,
    /// Small request tokens and control replies, with no metadata workspace.
    Control,
}

/// High-level metadata facade used by the VFS and daemon layers.
///
/// This trait intentionally mirrors the shape of JuiceFS' `Meta` interface.
/// The goal is to expose path-friendly helpers (with caching, session
/// management, etc.) while chunk IO or maintenance workers continue to talk to
/// the raw [`MetaStore`] directly. Implementations may return
/// `MetaError::NotImplemented` for operations that have not landed yet, but the
/// signatures are provided up front to ease future parity work.
#[async_trait]
#[allow(dead_code)]
pub trait MetaLayer: Send + Sync {
    /// Opt in only when dropping a read future cannot leave mutable work.
    fn supports_fuse_read_cancellation(&self) -> bool {
        false
    }
    /// Reserve before allocating temporary state, reply copies, or a handle.
    /// The returned owner follows the actual final consumer.
    fn reserve_memory(
        &self,
        _kind: MetadataMemoryKind,
        _bytes: u64,
    ) -> Result<Option<MetadataMemoryGuard>, MetaError> {
        Ok(None)
    }
    /// Roots permits may be carried inline through actual enclosing deallocation.
    fn reserve_inline_roots(
        &self,
        _bytes: u64,
    ) -> Result<Option<asyncfuse::raw::reply::InlineRootPermit>, MetaError> {
        Ok(None)
    }
    /// Linux POSIX ACL support, distinct from control ACL/AclRule storage.
    /// Writable implementations must supply atomic mode/xattr synchronization
    /// and creation inheritance before opting into the kernel capability.
    fn posix_acl_capability(&self) -> PosixAclCapability {
        PosixAclCapability::Unsupported
    }
    /// The FUSE mount is the sole metadata writer under a live exclusive lease.
    /// Only then can Linux's cached ownership authorize a timestamp request
    /// whose original NOW/explicit distinction was lost by writeback.
    fn kernel_timestamp_permissions_authoritative(&self) -> bool {
        false
    }
    /// `inode_permissions` alone answers an access check (existence plus
    /// attributes and ACLs in one snapshot), so callers may skip the stat.
    fn permission_snapshot_answers_access(&self) -> bool {
        false
    }
    /// Optional human readable backend name.
    fn name(&self) -> &'static str {
        "meta-layer"
    }

    /// Optional metadata cache counters exposed by caching layers.
    fn metrics(&self) -> Option<Arc<MetaClientMetrics>> {
        None
    }

    /// Returns / mutates the logical root inode alias used by chroot.
    fn root_ino(&self) -> i64;

    fn chroot(&self, inode: i64);

    /// Performs backend initialization / schema checks.
    async fn initialize(&self) -> Result<(), MetaError>;

    async fn stat_fs(&self) -> Result<StatFsSnapshot, MetaError>;

    // ---------- Core path operations ----------
    async fn stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError>;

    /// Do `stat` but bypass the inode cache.
    async fn stat_fresh(&self, ino: i64) -> Result<Option<FileAttr>, MetaError>;

    /// Permission decisions must use attributes and ACLs from one metadata
    /// commit. Immutable layers can safely compose reads; writable ACL layers
    /// must override this method with a backend snapshot.
    async fn inode_permissions(&self, ino: i64) -> Result<Option<InodePermissions>, MetaError> {
        if self.posix_acl_capability() == PosixAclCapability::ReadWrite {
            return Err(MetaError::NotSupported(
                "atomic inode permission snapshot is unavailable".into(),
            ));
        }
        let Some(attr) = self.stat_fresh(ino).await? else {
            return Ok(None);
        };
        let access_acl = if self.posix_acl_capability() == PosixAclCapability::ReadOnly {
            self.get_xattr(ino, "system.posix_acl_access").await?
        } else {
            None
        };
        let control_acl = self
            .get_xattr(ino, "system.brewfs.acl")
            .await
            .ok()
            .flatten();
        Ok(Some(InodePermissions {
            attr,
            access_acl,
            control_acl,
        }))
    }

    /// POSIX ACL owner policy and the mode update share one commit version.
    /// None and an empty valid ACL mean removal. The flags-aware helper below
    /// additionally preserves Linux XATTR_CREATE/XATTR_REPLACE semantics.
    async fn update_posix_acl(
        &self,
        _ino: i64,
        _name: &str,
        _value: Option<&[u8]>,
        _uid: u32,
        _groups: &[u32],
    ) -> Result<(), MetaError> {
        Err(MetaError::NotSupported(
            "writable POSIX ACL is unavailable".into(),
        ))
    }

    /// Update a POSIX ACL xattr while preserving Linux xattr create/replace
    /// semantics. Writable backends must override this method so the
    /// existence check and ACL/mode commit share one metadata transaction.
    async fn update_posix_acl_with_flags(
        &self,
        ino: i64,
        name: &str,
        value: Option<&[u8]>,
        flags: u32,
        uid: u32,
        groups: &[u32],
    ) -> Result<(), MetaError> {
        let _ = flags;
        self.update_posix_acl(ino, name, value, uid, groups).await
    }

    async fn set_attr_as(
        &self,
        ino: i64,
        request: &SetAttrRequest,
        flags: SetAttrFlags,
        _uid: u32,
        _groups: &[u32],
    ) -> Result<FileAttr, MetaError> {
        self.set_attr(ino, request, flags).await
    }

    /// Fetch the attribute used by an open operation. Implementations may
    /// reuse an explicitly enabled open-file scoped cache for read-only opens.
    async fn stat_for_open(
        &self,
        ino: i64,
        _read: bool,
        _write: bool,
        _append: bool,
    ) -> Result<Option<FileAttr>, MetaError> {
        self.stat_fresh(ino).await
    }

    /// Record that VFS successfully opened a file handle.
    /// Fresh and cached-attribute VFS opens use this hook instead of `open`.
    /// Immutable layers must reject write/append here before a handle is
    /// allocated; checking only `open` does not protect those fast paths.
    async fn record_open(
        &self,
        _ino: i64,
        _attr: FileAttr,
        _read: bool,
        _write: bool,
        _append: bool,
    ) -> Result<(), MetaError> {
        Ok(())
    }

    /// Record that VFS released a file handle.
    async fn record_close(&self, _ino: i64) -> Result<(), MetaError> {
        Ok(())
    }

    /// Record that VFS released a handle and can provide the final handle attr.
    async fn record_close_with_attr(
        &self,
        ino: i64,
        _attr: FileAttr,
        _read: bool,
        _write: bool,
        _append: bool,
    ) -> Result<(), MetaError> {
        self.record_close(ino).await
    }

    async fn lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError>;

    async fn lookup_with_attr(
        &self,
        parent: i64,
        name: &str,
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        let Some(ino) = self.lookup(parent, name).await? else {
            return Ok(None);
        };
        let attr = self.stat(ino).await?.ok_or(MetaError::NotFound(ino))?;
        Ok(Some((ino, attr)))
    }

    /// Byte-preserving lookup capability for immutable packed snapshots.
    /// Legacy metadata backends remain string based and therefore explicitly
    /// reject names that are not valid UTF-8 instead of silently replacing
    /// bytes.
    async fn lookup_with_attr_bytes(
        &self,
        parent: i64,
        name: &[u8],
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        let name = std::str::from_utf8(name).map_err(|_| MetaError::InvalidFilename)?;
        self.lookup_with_attr(parent, name).await
    }

    async fn lookup_path(&self, path: &str) -> Result<Option<(i64, FileType)>, MetaError>;

    async fn lookup_path_with_attr(
        &self,
        path: &str,
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        let (ino, _) = match self.lookup_path(path).await? {
            Some(result) => result,
            None => return Ok(None),
        };
        let attr = self.stat(ino).await?.ok_or(MetaError::NotFound(ino))?;
        Ok(Some((ino, attr)))
    }

    async fn readdir(&self, ino: i64) -> Result<Vec<DirEntry>, MetaError>;

    async fn opendir(&self, ino: i64) -> Result<DirHandle, MetaError>;

    async fn mkdir(&self, parent: i64, name: String) -> Result<i64, MetaError>;

    async fn rmdir(&self, parent: i64, name: &str) -> Result<(), MetaError>;

    async fn create_file(&self, parent: i64, name: String) -> Result<i64, MetaError>;

    async fn create_file_with_attr(
        &self,
        parent: i64,
        name: String,
    ) -> Result<CreateEntryResult, MetaError> {
        let ino = self.create_file(parent, name).await?;
        let attr = self.stat(ino).await.ok().flatten();
        Ok(CreateEntryResult { ino, attr })
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_node(
        &self,
        parent: i64,
        name: String,
        kind: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
    ) -> Result<i64, MetaError>;

    #[allow(clippy::too_many_arguments)]
    async fn create_node_with_attr(
        &self,
        parent: i64,
        name: String,
        kind: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
    ) -> Result<CreateEntryResult, MetaError> {
        let ino = self
            .create_node(parent, name, kind, mode, uid, gid, rdev)
            .await?;
        let attr = self.stat(ino).await.ok().flatten();
        Ok(CreateEntryResult { ino, attr })
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_node_with_umask(
        &self,
        parent: i64,
        name: String,
        kind: FileType,
        mode: u32,
        umask: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
    ) -> Result<CreateEntryResult, MetaError> {
        self.create_node_with_attr(parent, name, kind, mode & !umask, uid, gid, rdev)
            .await
    }

    async fn link(&self, ino: i64, parent: i64, name: &str) -> Result<FileAttr, MetaError>;

    async fn symlink(
        &self,
        parent: i64,
        name: &str,
        target: &str,
    ) -> Result<(i64, FileAttr), MetaError>;

    async fn unlink(&self, parent: i64, name: &str) -> Result<(), MetaError>;

    async fn rename(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
    ) -> Result<(), MetaError>;

    /// Atomically rename only when the destination name is absent.
    ///
    /// Implementations must delegate to metadata storage that checks and
    /// updates the namespace within one atomic operation.
    async fn rename_noreplace(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
    ) -> Result<(), MetaError>;

    /// Rename after the caller already resolved the source and destination
    /// parent attributes. Implementations that cannot use this context can
    /// fall back to the regular rename path.
    #[allow(clippy::too_many_arguments)]
    async fn rename_with_known_attrs(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
        _src_ino: i64,
        _src_attr: FileAttr,
        _new_parent_attr: FileAttr,
        _dest_ino: Option<i64>,
        _destination_checked: bool,
    ) -> Result<(), MetaError> {
        self.rename(old_parent, old_name, new_parent, new_name)
            .await
    }

    /// Atomically exchange two files (RENAME_EXCHANGE).
    /// Both entries must exist.
    async fn rename_exchange(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: &str,
    ) -> Result<(), MetaError>;

    /// Check if a rename operation would be allowed without performing it.
    async fn can_rename(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: &str,
    ) -> Result<(), MetaError> {
        // Default implementation - just try the basic validation
        let src_ino = self
            .lookup(old_parent, old_name)
            .await?
            .ok_or(MetaError::NotFound(old_parent))?;

        // Check if destination exists and validate replacement rules
        if let Some(dest_ino) = self.lookup(new_parent, new_name).await? {
            let src_attr = self
                .stat(src_ino)
                .await?
                .ok_or(MetaError::NotFound(src_ino))?;
            let dest_attr = self
                .stat(dest_ino)
                .await?
                .ok_or(MetaError::NotFound(dest_ino))?;

            match (src_attr.kind, dest_attr.kind) {
                // Directory replacing directory - check if empty
                (FileType::Dir, FileType::Dir) => {
                    let children = self.readdir(dest_ino).await?;
                    if !children.is_empty() {
                        return Err(MetaError::DirectoryNotEmpty(dest_ino));
                    }
                }
                // Directory replacing file/symlink - not allowed
                (FileType::Dir, FileType::File) | (FileType::Dir, FileType::Symlink) => {
                    return Err(MetaError::NotDirectory(dest_ino));
                }
                // File/symlink replacing directory - not allowed
                (FileType::File, FileType::Dir) | (FileType::Symlink, FileType::Dir) => {
                    return Err(MetaError::Io(std::io::Error::from(
                        std::io::ErrorKind::IsADirectory,
                    )));
                }
                // File/symlink replacing file/symlink - allowed
                _ => {}
            }
        }

        Ok(())
    }

    /// Rename with extended flags support (similar to Linux renameat2).
    async fn rename_with_flags(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
        flags: crate::vfs::fs::RenameFlags,
    ) -> Result<(), MetaError> {
        if flags.exchange {
            // Use atomic exchange implementation
            self.rename_exchange(old_parent, old_name, new_parent, &new_name)
                .await
        } else if flags.noreplace {
            self.rename_noreplace(old_parent, old_name, new_parent, new_name)
                .await
        } else {
            // Default behavior
            self.rename(old_parent, old_name, new_parent, new_name)
                .await
        }
    }

    async fn set_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError>;

    async fn extend_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError>;

    async fn truncate(&self, ino: i64, size: u64, chunk_size: u64) -> Result<(), MetaError>;

    async fn fallocate_file(
        &self,
        ino: i64,
        mode: u8,
        offset: u64,
        size: u64,
        block_size: u64,
    ) -> Result<FileAttr, MetaError> {
        let _ = (mode, block_size);
        let end = offset
            .checked_add(size)
            .ok_or_else(|| MetaError::Internal("fallocate size overflow".into()))?;
        self.extend_file_size(ino, end).await?;
        self.stat_fresh(ino).await?.ok_or(MetaError::NotFound(ino))
    }

    async fn get_names(&self, ino: i64) -> Result<Vec<(Option<i64>, String)>, MetaError>;

    async fn get_dentries(&self, ino: i64) -> Result<Vec<(i64, String)>, MetaError>;

    async fn get_dir_parent(&self, dir_ino: i64) -> Result<Option<i64>, MetaError>;

    async fn get_paths(&self, ino: i64) -> Result<Vec<String>, MetaError>;

    /// Preserve raw namespace bytes for inode-based ancestor permission checks.
    /// String-backed stores retain their existing path behavior by default.
    async fn get_paths_bytes(&self, ino: i64) -> Result<Vec<Vec<u8>>, MetaError> {
        self.get_paths(ino)
            .await
            .map(|paths| paths.into_iter().map(String::into_bytes).collect())
    }

    /// Keep path memory and the packed generation alive across subsequent
    /// asynchronous ancestor permission checks.
    async fn get_paths_bytes_owned(&self, ino: i64) -> Result<OwnedPaths, MetaError> {
        Ok(OwnedPaths {
            paths: self.get_paths_bytes(ino).await?,
            guard: None,
        })
    }

    async fn read_symlink(&self, ino: i64) -> Result<String, MetaError>;

    async fn read_symlink_bytes(&self, ino: i64) -> Result<Vec<u8>, MetaError> {
        self.read_symlink(ino).await.map(String::into_bytes)
    }

    // ---------- Attribute + handle helpers ----------
    async fn set_attr(
        &self,
        ino: i64,
        req: &SetAttrRequest,
        flags: SetAttrFlags,
    ) -> Result<FileAttr, MetaError>;

    /// Update only the permission bits of an inode (chmod).
    ///
    /// The mode is masked to `0o7777`; setuid/setgid/sticky bits are preserved.
    /// Returns updated [`FileAttr`] or `MetaError::NotFound`.
    async fn chmod(&self, ino: i64, new_mode: u32) -> Result<FileAttr, MetaError> {
        let req = chmod_request(new_mode);
        self.set_attr(ino, &req, SetAttrFlags::empty()).await
    }

    /// Change the owner and/or group of an inode (chown).
    ///
    /// Either `uid` or `gid` may be `None` to leave that field unchanged.
    /// Returns updated [`FileAttr`] or `MetaError::NotFound`.
    async fn chown(
        &self,
        ino: i64,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Result<FileAttr, MetaError> {
        let req = chown_request(uid, gid);
        self.set_attr(ino, &req, SetAttrFlags::empty()).await
    }

    async fn open(&self, ino: i64, flags: OpenFlags) -> Result<FileAttr, MetaError>;

    async fn close(&self, ino: i64) -> Result<(), MetaError>;

    async fn write(
        &self,
        ino: i64,
        chunk_id: u64,
        slice: SliceDesc,
        new_size: u64,
    ) -> Result<(), MetaError>;

    // ---------- Metadata + ID utilities ----------
    async fn get_deleted_files(&self) -> Result<Vec<i64>, MetaError>;

    async fn remove_file_metadata(&self, ino: i64) -> Result<(), MetaError>;

    async fn get_slices(&self, chunk_id: u64) -> Result<Vec<SliceDesc>, MetaError>;

    async fn invalidate_chunk_slices(&self, _ino: i64, _chunk_index: u64) -> Result<(), MetaError> {
        Ok(())
    }

    async fn append_slice(&self, chunk_id: u64, slice: SliceDesc) -> Result<(), MetaError>;

    async fn next_id(&self, key: &str) -> Result<i64, MetaError>;

    // ---------- Session lifecycle ----------
    async fn start_session(&self, session_info: SessionInfo) -> Result<(), MetaError>;

    async fn shutdown_session(&self) -> Result<(), MetaError>;

    // ---------- File lock operations ----------
    async fn get_plock(&self, inode: i64, query: &FileLockQuery)
    -> Result<FileLockInfo, MetaError>;

    async fn set_plock(
        &self,
        inode: i64,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
        range: FileLockRange,
        pid: u32,
    ) -> Result<(), MetaError>;

    async fn get_flock(&self, inode: i64, owner: i64) -> Result<FileLockType, MetaError>;

    async fn set_flock(
        &self,
        inode: i64,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
    ) -> Result<(), MetaError>;

    // ---------- Extended attribute & ACL ----------
    async fn set_xattr(
        &self,
        inode: i64,
        name: &str,
        value: &[u8],
        flags: u32,
    ) -> Result<(), MetaError>;
    async fn get_xattr(&self, inode: i64, name: &str) -> Result<Option<Vec<u8>>, MetaError>;
    async fn list_xattr(&self, inode: i64) -> Result<Vec<String>, MetaError>;
    async fn remove_xattr(&self, inode: i64, name: &str) -> Result<(), MetaError>;

    /// Linux xattr names are bytes. String-backed stores reject names they
    /// cannot represent rather than looking up or mutating a lossy alias.
    async fn get_xattr_bytes(&self, inode: i64, name: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        let name = std::str::from_utf8(name).map_err(|_| MetaError::InvalidFilename)?;
        self.get_xattr(inode, name).await
    }
    async fn list_xattr_bytes(&self, inode: i64) -> Result<Vec<Vec<u8>>, MetaError> {
        self.list_xattr(inode)
            .await
            .map(|names| names.into_iter().map(String::into_bytes).collect())
    }
    async fn list_xattr_bytes_owned(&self, inode: i64) -> Result<OwnedXattrNames, MetaError> {
        Ok(OwnedXattrNames {
            names: self.list_xattr_bytes(inode).await?,
            guard: None,
        })
    }
    async fn set_xattr_bytes(
        &self,
        inode: i64,
        name: &[u8],
        value: &[u8],
        flags: u32,
    ) -> Result<(), MetaError> {
        let name = std::str::from_utf8(name).map_err(|_| MetaError::InvalidFilename)?;
        self.set_xattr(inode, name, value, flags).await
    }
    async fn remove_xattr_bytes(&self, inode: i64, name: &[u8]) -> Result<(), MetaError> {
        let name = std::str::from_utf8(name).map_err(|_| MetaError::InvalidFilename)?;
        self.remove_xattr(inode, name).await
    }
    async fn set_acl(&self, inode: i64, rule: AclRule) -> Result<(), MetaError>;
    async fn get_acl(
        &self,
        inode: i64,
        acl_type: u8,
        acl_id: u32,
    ) -> Result<Option<AclRule>, MetaError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PosixAclCapability {
    Unsupported,
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Debug)]
pub struct InodePermissions {
    pub attr: FileAttr,
    pub access_acl: Option<Vec<u8>>,
    pub control_acl: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub(crate) struct NamespaceActor {
    pub uid: u32,
    pub gid: u32,
    pub groups: Vec<u32>,
}

tokio::task_local! {
    static NAMESPACE_ACTOR: NamespaceActor;
}

/// Request credentials are scoped to this future, never a process-global
/// setting and never inherited by background tasks spawned from the request.
pub(crate) async fn scope_namespace_actor<F: std::future::Future>(
    actor: Option<NamespaceActor>,
    operation: F,
) -> F::Output {
    match actor {
        Some(actor) => NAMESPACE_ACTOR.scope(actor, operation).await,
        None => operation.await,
    }
}

pub(crate) fn namespace_actor() -> Option<NamespaceActor> {
    NAMESPACE_ACTOR.try_with(Clone::clone).ok()
}

tokio::task_local! {
    static OPEN_ACCESS_MASK: u32;
    static SETATTR_WRITE_HANDLE: i64;
    static CREATED_INODE_OPEN: std::cell::Cell<Option<i64>>;
}

pub(crate) async fn scope_open_actor<F: std::future::Future>(
    actor: Option<NamespaceActor>,
    access_mask: u32,
    operation: F,
) -> F::Output {
    OPEN_ACCESS_MASK
        .scope(access_mask, scope_namespace_actor(actor, operation))
        .await
}

pub(crate) fn open_access_mask() -> Option<u32> {
    OPEN_ACCESS_MASK.try_with(|mask| *mask).ok()
}

/// Only the FUSE path that verified a live writable handle may supply this
/// authority. It applies to this inode's truncate, never to chmod or chown.
pub(crate) async fn scope_setattr_write_handle<F: std::future::Future>(
    ino: i64,
    authorized: bool,
    operation: F,
) -> F::Output {
    if authorized {
        SETATTR_WRITE_HANDLE.scope(ino, operation).await
    } else {
        operation.await
    }
}

pub(crate) fn setattr_write_handle_authorizes(ino: i64) -> bool {
    SETATTR_WRITE_HANDLE
        .try_with(|authorized| *authorized == ino)
        .unwrap_or(false)
}

/// Creation authorizes its initial handle even when the requested mode is 000.
/// A fallback to an existing inode never receives this authority.
pub(crate) async fn scope_created_inode_open<F: std::future::Future>(
    ino: i64,
    created: bool,
    operation: F,
) -> F::Output {
    if created {
        CREATED_INODE_OPEN
            .scope(std::cell::Cell::new(Some(ino)), operation)
            .await
    } else {
        operation.await
    }
}

#[cfg(test)]
pub(crate) fn created_inode_open_authorizes(ino: i64) -> bool {
    CREATED_INODE_OPEN
        .try_with(|created| created.get() == Some(ino))
        .unwrap_or(false)
}

/// The current request's scoped credentials and authorities, moved out so a
/// task that executes this same request (for example a cancellation-safe
/// driver spawned to finish it) can re-enter them. The created-inode
/// authority is taken, not copied, so it stays exactly-once.
pub(crate) struct RequestScope {
    actor: Option<NamespaceActor>,
    access_mask: Option<u32>,
    setattr_write_handle: Option<i64>,
    created_inode: Option<i64>,
}

pub(crate) fn capture_request_scope() -> RequestScope {
    RequestScope {
        actor: namespace_actor(),
        access_mask: open_access_mask(),
        setattr_write_handle: SETATTR_WRITE_HANDLE.try_with(|ino| *ino).ok(),
        created_inode: CREATED_INODE_OPEN
            .try_with(|created| created.take())
            .ok()
            .flatten(),
    }
}

impl RequestScope {
    pub(crate) async fn scope<F: std::future::Future>(self, operation: F) -> F::Output {
        let Self {
            actor,
            access_mask,
            setattr_write_handle,
            created_inode,
        } = self;
        let operation = async move {
            match created_inode {
                Some(ino) => {
                    CREATED_INODE_OPEN
                        .scope(std::cell::Cell::new(Some(ino)), operation)
                        .await
                }
                None => operation.await,
            }
        };
        let operation = async move {
            match setattr_write_handle {
                Some(ino) => SETATTR_WRITE_HANDLE.scope(ino, operation).await,
                None => operation.await,
            }
        };
        let operation = async move {
            match access_mask {
                Some(mask) => OPEN_ACCESS_MASK.scope(mask, operation).await,
                None => operation.await,
            }
        };
        scope_namespace_actor(actor, operation).await
    }
}

/// Consume the creation authority exactly once for the matching inode.
pub(crate) fn take_created_inode_open_authority(ino: i64) -> bool {
    CREATED_INODE_OPEN
        .try_with(|created| {
            if created.get() == Some(ino) {
                created.set(None);
                true
            } else {
                false
            }
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod request_scope_tests {
    use super::*;

    #[tokio::test]
    async fn spawned_driver_keeps_request_scope_and_created_authority_stays_once() {
        let actor = NamespaceActor {
            uid: 1234,
            gid: 5678,
            groups: vec![9],
        };
        scope_open_actor(
            Some(actor),
            2,
            scope_setattr_write_handle(
                77,
                true,
                scope_created_inode_open(88, true, async {
                    let request = capture_request_scope();
                    // Taken, not copied: the original task no longer holds it.
                    assert!(!created_inode_open_authorizes(88));
                    tokio::spawn(request.scope(async {
                        assert_eq!(namespace_actor().unwrap().uid, 1234);
                        assert_eq!(open_access_mask(), Some(2));
                        assert!(setattr_write_handle_authorizes(77));
                        assert!(take_created_inode_open_authority(88));
                        assert!(!take_created_inode_open_authority(88));
                    }))
                    .await
                    .unwrap();
                }),
            ),
        )
        .await;
        // Outside any request, a captured scope is empty.
        tokio::spawn(capture_request_scope().scope(async {
            assert!(namespace_actor().is_none());
            assert!(open_access_mask().is_none());
            assert!(!take_created_inode_open_authority(88));
        }))
        .await
        .unwrap();
    }
}
