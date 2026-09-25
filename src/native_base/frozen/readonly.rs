//! Read-only `MetaLayer` facade for one authenticated frozen catalog.
//!
//! This type intentionally has no control-store or metadata-client field. A
//! mounted instance can therefore serve lookup/stat/readdir without Redis or
//! TiKV, while every mutating operation fails closed.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;

use crate::chunk::SliceDesc;
use crate::meta::client::MetaClientMetrics;
use crate::meta::client::session::SessionInfo;
use crate::meta::file_lock::{FileLockInfo, FileLockQuery, FileLockRange, FileLockType};
use crate::meta::layer::MetaLayer;
use crate::meta::store::{
    AclRule, DirEntry, FileAttr, FileType, MetaError, OpenFlags, SetAttrFlags, SetAttrRequest,
    StatFsSnapshot, stat_fs_snapshot_from_usage,
};
use crate::vfs::handles::{DirHandle, DirectoryPageSource, RawDirEntry};

use super::catalog::{FrozenCatalog, FrozenDirectoryEntry};
use super::decode_extent_slice_value;

const READ_ONLY_ERROR: &str = "frozen metadata snapshot is read-only";

struct FrozenDirectoryPageSource {
    catalog: Arc<dyn FrozenCatalog>,
}

#[async_trait]
impl DirectoryPageSource for FrozenDirectoryPageSource {
    async fn read_page(
        &self,
        ino: i64,
        child_offset: u64,
        max_entries: usize,
    ) -> Result<Vec<RawDirEntry>, MetaError> {
        let parent = FrozenReadonlyMeta::inode(ino)?;
        let entries = self
            .catalog
            .readdir_page(parent, child_offset, max_entries)
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))?;
        entries
            .into_iter()
            .map(|entry| {
                Ok(RawDirEntry {
                    name: entry.name,
                    ino: i64::try_from(entry.inode)
                        .map_err(|_| MetaError::Internal("packed inode exceeds i64".into()))?,
                    kind: file_type(entry.attr.kind, entry.attr.mode),
                })
            })
            .collect()
    }
}

pub struct FrozenReadonlyMeta {
    catalog: Arc<dyn FrozenCatalog>,
    root: AtomicI64,
}

impl FrozenReadonlyMeta {
    pub fn new<C>(catalog: Arc<C>, root_ino: i64) -> Self
    where
        C: FrozenCatalog + 'static,
    {
        Self {
            catalog,
            root: AtomicI64::new(root_ino),
        }
    }

    pub fn catalog(&self) -> &Arc<dyn FrozenCatalog> {
        &self.catalog
    }

    fn readonly<T>() -> Result<T, MetaError> {
        Err(MetaError::NotSupported(READ_ONLY_ERROR.into()))
    }

    fn inode(ino: i64) -> Result<u64, MetaError> {
        u64::try_from(ino).map_err(|_| MetaError::NotFound(ino))
    }

    fn attr(ino: u64, record: super::FrozenInodeRecord) -> FileAttr {
        FileAttr {
            ino: ino as i64,
            size: record.size,
            blocks: record.size.div_ceil(512),
            kind: file_type(record.kind, record.mode),
            mode: record.mode,
            rdev: record.rdev.min(u64::from(u32::MAX)) as u32,
            uid: record.uid,
            gid: record.gid,
            atime: record.atime_ns,
            mtime: record.mtime_ns,
            ctime: record.ctime_ns,
            nlink: record.nlink.min(u64::from(u32::MAX)) as u32,
        }
    }

    fn entry_name(bytes: &[u8]) -> Result<String, MetaError> {
        String::from_utf8(bytes.to_vec()).map_err(|_| MetaError::InvalidFilename)
    }

    async fn resolved_entries(&self, ino: i64) -> Result<Vec<FrozenDirectoryEntry>, MetaError> {
        let parent = Self::inode(ino)?;
        let attr = self
            .catalog
            .lookup_inode(parent)
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))?
            .ok_or(MetaError::NotFound(ino))?;
        if !file_type(attr.kind, attr.mode).is_dir() {
            return Err(MetaError::NotDirectory(ino));
        }
        self.catalog
            .readdir(parent)
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))
    }
}

fn file_type(kind: u8, mode: u32) -> FileType {
    match kind {
        1 => FileType::File,
        2 => FileType::Dir,
        3 => FileType::Symlink,
        4 => FileType::Fifo,
        5 => FileType::Socket,
        6 => FileType::CharDevice,
        7 => FileType::BlockDevice,
        _ => FileType::from_mode(mode),
    }
}

#[async_trait]
impl MetaLayer for FrozenReadonlyMeta {
    fn name(&self) -> &'static str {
        "frozen-readonly"
    }

    fn metrics(&self) -> Option<Arc<MetaClientMetrics>> {
        None
    }

    fn root_ino(&self) -> i64 {
        self.root.load(Ordering::Acquire)
    }

    fn chroot(&self, inode: i64) {
        self.root.store(inode, Ordering::Release);
    }

    async fn initialize(&self) -> Result<(), MetaError> {
        Ok(())
    }

    async fn stat_fs(&self) -> Result<StatFsSnapshot, MetaError> {
        let manifest = self.catalog.manifest();
        Ok(stat_fs_snapshot_from_usage(
            manifest.total_logical_bytes,
            manifest.file_count.saturating_add(manifest.directory_count),
        ))
    }

    async fn stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        self.stat_fresh(ino).await
    }

    async fn stat_fresh(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        let inode = Self::inode(ino)?;
        self.catalog
            .lookup_inode(inode)
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))
            .map(|record| record.map(|record| Self::attr(inode, record)))
    }

    async fn lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        let parent = Self::inode(parent)?;
        let name = name.as_bytes();
        self.catalog
            .lookup_dentry(parent, name)
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))
            .and_then(|entry| {
                entry
                    .map(|(inode, _)| {
                        i64::try_from(inode)
                            .map_err(|_| MetaError::Internal("packed inode exceeds i64".into()))
                    })
                    .transpose()
            })
    }

    async fn lookup_with_attr(
        &self,
        parent: i64,
        name: &str,
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        let parent = Self::inode(parent)?;
        let Some((inode, record)) = self
            .catalog
            .lookup_dentry(parent, name.as_bytes())
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))?
        else {
            return Ok(None);
        };
        let inode = i64::try_from(inode)
            .map_err(|_| MetaError::Internal("packed inode exceeds i64".into()))?;
        Ok(Some((inode, Self::attr(inode as u64, record))))
    }

    async fn lookup_with_attr_bytes(
        &self,
        parent: i64,
        name: &[u8],
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        let parent = Self::inode(parent)?;
        let Some((inode, record)) = self
            .catalog
            .lookup_dentry(parent, name)
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))?
        else {
            return Ok(None);
        };
        let inode = i64::try_from(inode)
            .map_err(|_| MetaError::Internal("packed inode exceeds i64".into()))?;
        Ok(Some((inode, Self::attr(inode as u64, record))))
    }

    async fn lookup_path(&self, path: &str) -> Result<Option<(i64, FileType)>, MetaError> {
        if path.is_empty() || !path.starts_with('/') {
            return Err(MetaError::InvalidPath(path.into()));
        }
        let mut inode = self.root_ino();
        for component in path.split('/').filter(|component| !component.is_empty()) {
            if component == "." {
                continue;
            }
            if component == ".." {
                let current = self
                    .catalog
                    .lookup_inode(Self::inode(inode)?)
                    .await
                    .map_err(|error| MetaError::Internal(error.to_string()))?
                    .ok_or(MetaError::NotFound(inode))?;
                inode = i64::try_from(current.parent_hint.unwrap_or(Self::inode(self.root_ino())?))
                    .map_err(|_| MetaError::Internal("packed parent inode exceeds i64".into()))?;
                continue;
            }
            let Some(next) = self.lookup(inode, component).await? else {
                return Ok(None);
            };
            inode = next;
        }
        let Some(attr) = self.stat(inode).await? else {
            return Ok(None);
        };
        Ok(Some((inode, attr.kind)))
    }

    async fn lookup_path_with_attr(
        &self,
        path: &str,
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        if path.is_empty() || !path.starts_with('/') {
            return Err(MetaError::InvalidPath(path.into()));
        }

        let root = self.root_ino();
        let mut inode = root;
        let mut record = None;
        for component in path.split('/').filter(|component| !component.is_empty()) {
            if component == "." {
                continue;
            }
            if component == ".." {
                let current = self
                    .catalog
                    .lookup_inode(Self::inode(inode)?)
                    .await
                    .map_err(|error| MetaError::Internal(error.to_string()))?
                    .ok_or(MetaError::NotFound(inode))?;
                inode = i64::try_from(current.parent_hint.unwrap_or(Self::inode(root)?))
                    .map_err(|_| MetaError::Internal("packed parent inode exceeds i64".into()))?;
                record = self
                    .catalog
                    .lookup_inode(Self::inode(inode)?)
                    .await
                    .map_err(|error| MetaError::Internal(error.to_string()))?;
                continue;
            }

            let Some((next, attr)) = self
                .catalog
                .lookup_dentry(Self::inode(inode)?, component.as_bytes())
                .await
                .map_err(|error| MetaError::Internal(error.to_string()))?
            else {
                return Ok(None);
            };
            inode = i64::try_from(next)
                .map_err(|_| MetaError::Internal("packed inode exceeds i64".into()))?;
            record = Some(attr);
        }

        let record = match record {
            Some(record) => record,
            None => self
                .catalog
                .lookup_inode(Self::inode(inode)?)
                .await
                .map_err(|error| MetaError::Internal(error.to_string()))?
                .ok_or(MetaError::NotFound(inode))?,
        };
        Ok(Some((inode, Self::attr(Self::inode(inode)?, record))))
    }

    async fn readdir(&self, ino: i64) -> Result<Vec<DirEntry>, MetaError> {
        Ok(self
            .resolved_entries(ino)
            .await?
            .into_iter()
            .map(|entry| {
                Ok(DirEntry {
                    name: Self::entry_name(&entry.name)?,
                    ino: i64::try_from(entry.inode)
                        .map_err(|_| MetaError::Internal("packed inode exceeds i64".into()))?,
                    kind: file_type(entry.attr.kind, entry.attr.mode),
                })
            })
            .collect::<Result<Vec<_>, MetaError>>()?)
    }

    async fn opendir(&self, ino: i64) -> Result<DirHandle, MetaError> {
        let parent = Self::inode(ino)?;
        let attr = self
            .catalog
            .lookup_inode(parent)
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))?
            .ok_or(MetaError::NotFound(ino))?;
        if !file_type(attr.kind, attr.mode).is_dir() {
            return Err(MetaError::NotDirectory(ino));
        }
        // Do not scan or prefetch the directory at opendir time. The page
        // source keeps the immutable catalog identity and fetches one bounded
        // range per FUSE request; eviction can therefore discard decoded
        // batches without invalidating the handle.
        Ok(DirHandle::new_paged(
            ino,
            Arc::new(FrozenDirectoryPageSource {
                catalog: Arc::clone(&self.catalog),
            }),
        ))
    }

    async fn mkdir(&self, _parent: i64, _name: String) -> Result<i64, MetaError> {
        Self::readonly()
    }

    async fn rmdir(&self, _parent: i64, _name: &str) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn create_file(&self, _parent: i64, _name: String) -> Result<i64, MetaError> {
        Self::readonly()
    }

    async fn create_node(
        &self,
        _parent: i64,
        _name: String,
        _kind: FileType,
        _mode: u32,
        _uid: u32,
        _gid: u32,
        _rdev: u32,
    ) -> Result<i64, MetaError> {
        Self::readonly()
    }

    async fn link(&self, _ino: i64, _parent: i64, _name: &str) -> Result<FileAttr, MetaError> {
        Self::readonly()
    }

    async fn symlink(
        &self,
        _parent: i64,
        _name: &str,
        _target: &str,
    ) -> Result<(i64, FileAttr), MetaError> {
        Self::readonly()
    }

    async fn unlink(&self, _parent: i64, _name: &str) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn rename(
        &self,
        _old_parent: i64,
        _old_name: &str,
        _new_parent: i64,
        _new_name: String,
    ) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn rename_noreplace(
        &self,
        _old_parent: i64,
        _old_name: &str,
        _new_parent: i64,
        _new_name: String,
    ) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn rename_exchange(
        &self,
        _old_parent: i64,
        _old_name: &str,
        _new_parent: i64,
        _new_name: &str,
    ) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn set_file_size(&self, _ino: i64, _size: u64) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn extend_file_size(&self, _ino: i64, _size: u64) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn truncate(&self, _ino: i64, _size: u64, _chunk_size: u64) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn get_names(&self, ino: i64) -> Result<Vec<(Option<i64>, String)>, MetaError> {
        let inode = Self::inode(ino)?;
        self.catalog
            .names_for_inode(inode)
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))?
            .into_iter()
            .map(|(parent, name)| {
                Ok((
                    Some(i64::try_from(parent).map_err(|_| {
                        MetaError::Internal("packed parent inode exceeds i64".into())
                    })?),
                    Self::entry_name(&name)?,
                ))
            })
            .collect()
    }

    async fn get_dentries(&self, ino: i64) -> Result<Vec<(i64, String)>, MetaError> {
        Ok(self
            .get_names(ino)
            .await?
            .into_iter()
            .filter_map(|(inode, name)| inode.map(|inode| (inode, name)))
            .collect())
    }

    async fn get_dir_parent(&self, dir_ino: i64) -> Result<Option<i64>, MetaError> {
        let inode = Self::inode(dir_ino)?;
        Ok(self
            .catalog
            .lookup_inode(inode)
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))?
            .and_then(|record| record.parent_hint.map(|parent| parent as i64)))
    }

    async fn get_paths(&self, ino: i64) -> Result<Vec<String>, MetaError> {
        let target = Self::inode(ino)?;
        let root = Self::inode(self.root_ino())?;
        let mut stack = vec![(
            target,
            Vec::<String>::new(),
            std::collections::BTreeSet::new(),
        )];
        let mut paths = Vec::new();
        while let Some((current, suffix, mut visited)) = stack.pop() {
            if !visited.insert(current) || visited.len() > 1024 {
                return Err(MetaError::TooManySymlinks);
            }
            if current == root {
                let mut components = suffix;
                components.reverse();
                paths.push(if components.is_empty() {
                    "/".to_string()
                } else {
                    format!("/{}", components.join("/"))
                });
                continue;
            }
            let names = self
                .catalog
                .names_for_inode(current)
                .await
                .map_err(|error| MetaError::Internal(error.to_string()))?;
            if names.is_empty() {
                return Err(MetaError::Internal(
                    "frozen inode has no parent dentry".into(),
                ));
            }
            for (parent, name) in names {
                let mut next_suffix = suffix.clone();
                next_suffix.push(Self::entry_name(&name)?);
                stack.push((parent, next_suffix, visited.clone()));
            }
        }
        paths.sort();
        paths.dedup();
        Ok(paths)
    }

    async fn read_symlink(&self, ino: i64) -> Result<String, MetaError> {
        let target = self
            .catalog
            .readlink(Self::inode(ino)?)
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))?
            .ok_or(MetaError::NotFound(ino))?;
        String::from_utf8(target)
            .map_err(|_| MetaError::InvalidPath("invalid symlink target".into()))
    }

    async fn set_attr(
        &self,
        _ino: i64,
        _req: &SetAttrRequest,
        _flags: SetAttrFlags,
    ) -> Result<FileAttr, MetaError> {
        Self::readonly()
    }

    async fn open(&self, ino: i64, flags: OpenFlags) -> Result<FileAttr, MetaError> {
        if flags.intersects(
            OpenFlags::WRONLY
                | OpenFlags::RDWR
                | OpenFlags::APPEND
                | OpenFlags::TRUNC
                | OpenFlags::CREATE,
        ) {
            return Self::readonly();
        }
        self.stat_fresh(ino).await?.ok_or(MetaError::NotFound(ino))
    }

    async fn close(&self, _ino: i64) -> Result<(), MetaError> {
        Ok(())
    }

    async fn write(
        &self,
        _ino: i64,
        _chunk_id: u64,
        _slice: SliceDesc,
        _new_size: u64,
    ) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn get_deleted_files(&self) -> Result<Vec<i64>, MetaError> {
        Ok(Vec::new())
    }

    async fn remove_file_metadata(&self, _ino: i64) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn get_slices(&self, chunk_id: u64) -> Result<Vec<SliceDesc>, MetaError> {
        let (ino, chunk_index) = crate::vfs::extract_ino_and_chunk_index(chunk_id);
        if ino <= 0 {
            return Err(MetaError::NotFound(ino));
        }
        let expected_chunk_id = crate::vfs::chunk_id_for(ino, chunk_index)
            .map_err(|error| MetaError::Internal(error.to_string()))?;
        if expected_chunk_id != chunk_id {
            return Err(MetaError::Internal("invalid packed chunk id".into()));
        }
        self.catalog
            .query_extents(ino as u64, chunk_index, 0, u64::MAX)
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))?
            .into_iter()
            .map(|extent| {
                let (length, slice_id, encoded_chunk_id, offset) =
                    decode_extent_slice_value(&extent.value)
                        .map_err(|error| MetaError::Internal(error.to_string()))?;
                if encoded_chunk_id != chunk_id || offset != extent.offset {
                    return Err(MetaError::Internal(
                        "packed extent key/value mismatch".into(),
                    ));
                }
                Ok(SliceDesc {
                    slice_id,
                    chunk_id,
                    offset,
                    length,
                })
            })
            .collect()
    }

    async fn append_slice(&self, _chunk_id: u64, _slice: SliceDesc) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn next_id(&self, _key: &str) -> Result<i64, MetaError> {
        Self::readonly()
    }

    async fn start_session(&self, _session_info: SessionInfo) -> Result<(), MetaError> {
        Ok(())
    }

    async fn shutdown_session(&self) -> Result<(), MetaError> {
        Ok(())
    }

    async fn get_plock(
        &self,
        _inode: i64,
        _query: &FileLockQuery,
    ) -> Result<FileLockInfo, MetaError> {
        Self::readonly()
    }

    async fn set_plock(
        &self,
        _inode: i64,
        _owner: i64,
        _block: bool,
        _lock_type: FileLockType,
        _range: FileLockRange,
        _pid: u32,
    ) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn get_flock(&self, _inode: i64, _owner: i64) -> Result<FileLockType, MetaError> {
        Self::readonly()
    }

    async fn set_flock(
        &self,
        _inode: i64,
        _owner: i64,
        _block: bool,
        _lock_type: FileLockType,
    ) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn set_xattr(
        &self,
        _inode: i64,
        _name: &str,
        _value: &[u8],
        _flags: u32,
    ) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn get_xattr(&self, _inode: i64, _name: &str) -> Result<Option<Vec<u8>>, MetaError> {
        Ok(None)
    }

    async fn list_xattr(&self, _inode: i64) -> Result<Vec<String>, MetaError> {
        Ok(Vec::new())
    }

    async fn remove_xattr(&self, _inode: i64, _name: &str) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn set_acl(&self, _inode: i64, _rule: AclRule) -> Result<(), MetaError> {
        Self::readonly()
    }

    async fn get_acl(
        &self,
        _inode: i64,
        _acl_type: u8,
        _acl_id: u32,
    ) -> Result<Option<AclRule>, MetaError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::cadapter::client::{ObjectBackend, ObjectClient};
    use crate::meta::layer::MetaLayer;
    use crate::native_base::frozen::catalog::StreamingFrozenMetadataCatalog;
    use crate::native_base::frozen::producer::{
        FrozenSnapshotInput, build_snapshot, upload_snapshot,
    };
    use crate::native_base::frozen::{FrozenRow, dentry_key, encode_extent_slice_value, inode_key};
    use crate::native_base::write::receipts::MemorySink;

    #[derive(Clone, Default)]
    struct Backend(Arc<Mutex<HashMap<String, Vec<u8>>>>);

    #[async_trait]
    impl ObjectBackend for Backend {
        async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            self.0.lock().unwrap().insert(key.into(), data.to_vec());
            Ok(())
        }

        async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(self.0.lock().unwrap().get(key).cloned())
        }

        async fn get_object_range(
            &self,
            key: &str,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<usize> {
            let Some(bytes) = self.0.lock().unwrap().get(key).cloned() else {
                return Ok(0);
            };
            let start = offset as usize;
            if start >= bytes.len() {
                return Ok(0);
            }
            let count = buf.len().min(bytes.len() - start);
            buf[..count].copy_from_slice(&bytes[start..start + count]);
            Ok(count)
        }

        async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
            Ok(String::new())
        }

        async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
            self.0.lock().unwrap().remove(key);
            Ok(())
        }
    }

    fn inode(kind: u8, mode: u32, parent_hint: Option<u64>, target: Option<&[u8]>) -> Vec<u8> {
        super::super::FrozenInodeRecord {
            kind,
            mode,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            nlink: 1,
            size: target.map_or(7, |value| value.len() as u64),
            atime_ns: 1,
            mtime_ns: 2,
            ctime_ns: 3,
            parent_hint,
            symlink_target: target.map(ToOwned::to_owned),
        }
        .encode()
    }

    #[tokio::test]
    async fn facade_reads_one_frozen_revision_and_rejects_mutations() {
        let input = FrozenSnapshotInput {
            volume_id: [41; 16],
            storage_namespace_id: [42; 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [43; 32],
            namespace_rows: vec![
                FrozenRow {
                    key: dentry_key(1, b"file"),
                    value: inode_key(2),
                },
                FrozenRow {
                    key: dentry_key(1, b"link"),
                    value: inode_key(3),
                },
                FrozenRow {
                    key: inode_key(1),
                    value: inode(2, 0o040755, None, None),
                },
                FrozenRow {
                    key: inode_key(2),
                    value: inode(1, 0o100644, Some(1), None),
                },
                FrozenRow {
                    key: inode_key(3),
                    value: inode(3, 0o120777, Some(1), Some(b"file")),
                },
            ],
            data_rows: vec![FrozenRow {
                key: crate::native_base::frozen::extent_key(2, 0, 0),
                value: encode_extent_slice_value(7, 99, crate::vfs::chunk_id_for(2, 0).unwrap(), 0),
            }],
            inventory_rows: vec![FrozenRow {
                key: b"i".to_vec(),
                value: b"object".to_vec(),
            }],
            file_count: 2,
            directory_count: 1,
            total_logical_bytes: 11,
            created_at_ns: 4,
            namespace_object_id: [44; 16],
            data_object_id: [45; 16],
            inventory_object_id: [46; 16],
            manifest_object_id: [47; 16],
        };
        let snapshot = build_snapshot(input).unwrap();
        let sink = MemorySink::default();
        upload_snapshot(&sink, &snapshot).await.unwrap();
        let backend = Backend::default();
        for (object, bytes) in &snapshot.metadata {
            backend
                .put_object(std::str::from_utf8(&object.key).unwrap(), bytes)
                .await
                .unwrap();
        }
        backend
            .put_object(
                std::str::from_utf8(&snapshot.manifest_object.key).unwrap(),
                &snapshot.manifest_bytes,
            )
            .await
            .unwrap();
        let catalog = Arc::new(
            StreamingFrozenMetadataCatalog::open_by_key(
                &ObjectClient::new(backend),
                std::str::from_utf8(&snapshot.manifest_object.key).unwrap(),
            )
            .await
            .unwrap(),
        );
        let facade = FrozenReadonlyMeta::new(catalog, 1);
        assert_eq!(facade.lookup(1, "file").await.unwrap(), Some(2));
        assert_eq!(
            facade.lookup_with_attr(1, "file").await.unwrap().unwrap().0,
            2
        );
        assert_eq!(
            facade
                .lookup_path_with_attr("/file")
                .await
                .unwrap()
                .unwrap()
                .1
                .size,
            7
        );
        assert_eq!(facade.readdir(1).await.unwrap().len(), 2);
        let handle = facade.opendir(1).await.unwrap();
        assert!(handle.is_paged());
        assert!(handle.entries.is_empty());
        let first_page = handle.get_entries_page(0, 1).await.unwrap();
        assert_eq!(first_page.len(), 1);
        let second_page = handle.get_entries_page(1, 1).await.unwrap();
        assert_eq!(second_page.len(), 1);
        assert!(handle.get_entries_page(2, 1).await.unwrap().is_empty());
        assert_eq!(
            facade.get_names(2).await.unwrap(),
            vec![(Some(1), "file".to_string())]
        );
        assert_eq!(
            facade.get_paths(2).await.unwrap(),
            vec!["/file".to_string()]
        );
        assert_eq!(facade.read_symlink(3).await.unwrap(), "file");
        assert_eq!(facade.stat(2).await.unwrap().unwrap().size, 7);
        let slices = facade
            .get_slices(crate::vfs::chunk_id_for(2, 0).unwrap())
            .await
            .unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].slice_id, 99);
        assert_eq!(slices[0].length, 7);
        assert!(matches!(
            facade.create_file(1, "new".into()).await,
            Err(MetaError::NotSupported(_))
        ));
    }
}
