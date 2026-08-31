//! `MetaLayer` implementation backed exclusively by workspace deltas.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::{Mutex, RwLock};

use crate::chunk::SliceDesc;
use crate::chunk::layout::DEFAULT_CHUNK_SIZE;
use crate::chunk::read_plan::{ReadPlanSegment, ResolvedReadPlan, WorkspaceReadPlanProvider};
use crate::meta::client::session::SessionInfo;
use crate::meta::file_lock::{
    FileLockInfo, FileLockQuery, FileLockRange, FileLockType, PlockRecord,
};
use crate::meta::layer::MetaLayer;
use crate::meta::store::{
    AclRule, CreateEntryResult, DirEntry, FileAttr, FileType, MetaError, OpenFlags, SetAttrFlags,
    SetAttrRequest, StatFsSnapshot,
};
use crate::vfs::handles::DirHandle;

use super::cache::{ReadPlanCacheKey, WorkspaceResolverCache};
use super::catalog::{
    AclMutation, AclQuery, DataMutation, DentryQuery, ExtentQuery, HeadGuard, InodeMutation,
    InodeQuery, NamespaceMutation, RecordOrphanSlice, WorkspaceStore, XattrMutation, XattrQuery,
};
use super::error::WorkspaceError;
use super::ids::LayerId;
use super::metrics::{WorkspaceMetrics, global_workspace_metrics};
use super::model::{
    AclDelta, DataExtentDelta, DentryDelta, InodeDelta, InodeState, LayerRecord, LayerState,
    ValueOp, ViewContext, XattrDelta,
};
use super::resolver::{
    ResolvedDentry, resolve_acl, resolve_dentry, resolve_directory, resolve_extents, resolve_inode,
    resolve_xattr,
};

pub struct WorkspaceMetaLayer<W> {
    store: Arc<W>,
    view: Arc<RwLock<ViewContext>>,
    layer_pair: RwLock<Option<(LayerId, u64, Vec<LayerRecord>)>>,
    root_ino: AtomicI64,
    chunk_size: u64,
    open_counts: DashMap<i64, u64>,
    resolver_cache: WorkspaceResolverCache,
    locks: Mutex<WorkspaceLocks>,
    mutation_gate: Mutex<()>,
    metrics: Arc<WorkspaceMetrics>,
}

fn validate_fixed_layer_pair(chain: &[LayerRecord]) -> Result<(), WorkspaceError> {
    let [head, base] = chain else {
        return Err(WorkspaceError::CorruptMetadata(format!(
            "workspace view must contain exactly one writable layer and one sealed base; found {} layers",
            chain.len()
        )));
    };
    if head.state != LayerState::Writable
        || head.parent_layer_id != Some(base.layer_id)
        || head.depth != 2
        || base.state != LayerState::Sealed
        || base.parent_layer_id.is_some()
        || base.depth != 1
    {
        return Err(WorkspaceError::CorruptMetadata(
            "workspace view is not a fixed sealed base plus writable overlay".into(),
        ));
    }
    Ok(())
}

#[derive(Default)]
struct WorkspaceLocks {
    plocks: BTreeMap<(i64, i64), Vec<PlockRecord>>,
    flocks: BTreeMap<(i64, i64), FileLockType>,
}

impl<W: WorkspaceStore + 'static> WorkspaceMetaLayer<W> {
    pub fn new(store: Arc<W>, view: ViewContext) -> Self {
        Self {
            store,
            view: Arc::new(RwLock::new(view)),
            layer_pair: RwLock::new(None),
            root_ino: AtomicI64::new(1),
            chunk_size: DEFAULT_CHUNK_SIZE,
            open_counts: DashMap::new(),
            resolver_cache: WorkspaceResolverCache::default(),
            locks: Mutex::new(WorkspaceLocks::default()),
            mutation_gate: Mutex::new(()),
            metrics: global_workspace_metrics(),
        }
    }

    pub fn with_chunk_size(store: Arc<W>, view: ViewContext, chunk_size: u64) -> Self {
        let mut layer = Self::new(store, view);
        layer.chunk_size = chunk_size;
        layer
    }

    pub fn store(&self) -> &Arc<W> {
        &self.store
    }

    pub async fn view_context(&self) -> ViewContext {
        self.view.read().await.clone()
    }

    pub async fn replace_view_context(&self, view: ViewContext) {
        let old_workspace = self.view.read().await.workspace_id;
        self.resolver_cache.invalidate_workspace(old_workspace);
        *self.view.write().await = view;
        *self.layer_pair.write().await = None;
    }

    async fn chain(&self) -> Result<Vec<LayerRecord>, MetaError> {
        let view = self.view.read().await.clone();
        if let Some((head, epoch, layers)) = self.layer_pair.read().await.as_ref()
            && *head == view.head_layer_id
            && *epoch == view.head_epoch
        {
            return Ok(layers.clone());
        }
        let chain = self
            .store
            .load_layer_chain(view.head_layer_id)
            .await
            .map_err(workspace_to_meta)?;
        validate_fixed_layer_pair(&chain).map_err(workspace_to_meta)?;
        self.metrics.add_resolver_steps(chain.len() as u64);
        if let Some(head) = chain.first() {
            self.metrics.set_layer_depth(u64::from(head.depth));
        }
        *self.layer_pair.write().await = Some((view.head_layer_id, view.head_epoch, chain.clone()));
        Ok(chain)
    }

    async fn guard(&self) -> HeadGuard {
        let view = self.view.read().await;
        HeadGuard {
            workspace_id: view.workspace_id,
            expected_head_layer_id: view.head_layer_id,
            expected_head_epoch: view.head_epoch,
            lease_id: view.lease_id,
            holder_generation: view.holder_generation,
        }
    }

    async fn resolve_inode_delta(&self, ino: i64) -> Result<Option<InodeDelta>, MetaError> {
        let chain = self.chain().await?;
        let rows = self
            .store
            .get_inode_deltas(InodeQuery {
                layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
                ino,
            })
            .await
            .map_err(workspace_to_meta)?;
        Ok(resolve_inode(&chain, &rows, ino)
            .map_err(workspace_to_meta)?
            .map(|resolved| resolved.inode))
    }

    async fn resolve_dentry_entry(
        &self,
        parent: i64,
        name: &[u8],
    ) -> Result<Option<ResolvedDentry>, MetaError> {
        let chain = self.chain().await?;
        let rows = self
            .store
            .get_dentry_deltas(DentryQuery {
                layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
                parent_ino: parent,
                name: Some(name.to_vec()),
            })
            .await
            .map_err(workspace_to_meta)?;
        resolve_dentry(&chain, &rows, parent, name).map_err(workspace_to_meta)
    }

    async fn mutate_inode(&self, mut inode: InodeDelta) -> Result<InodeDelta, MetaError> {
        let guard = self.guard().await;
        inode.layer_id = guard.expected_head_layer_id;
        inode.sequence = 0;
        self.store
            .apply_inode_mutation(InodeMutation { guard, inode })
            .await
            .map_err(|error| self.mutation_error(error))
    }

    async fn mutate_data(
        &self,
        mut inode: InodeDelta,
        mut extents: Vec<DataExtentDelta>,
    ) -> Result<InodeDelta, MetaError> {
        let guard = self.guard().await;
        inode.layer_id = guard.expected_head_layer_id;
        inode.sequence = 0;
        for extent in &mut extents {
            extent.layer_id = guard.expected_head_layer_id;
            extent.ino = inode.ino;
            extent.sequence = 0;
        }
        let workspace_id = guard.workspace_id;
        let ino = inode.ino;
        let private_bytes = extents
            .iter()
            .filter_map(|extent| {
                matches!(extent.kind, super::model::ExtentKind::Data { .. })
                    .then_some(extent.length)
            })
            .fold(0u64, u64::saturating_add);
        let result = self
            .store
            .apply_data_mutation(DataMutation {
                guard,
                inode,
                extents,
                chunk_size: self.chunk_size,
            })
            .await
            .map_err(|error| self.mutation_error(error))?;
        self.metrics.add_private_bytes_written(private_bytes);
        self.resolver_cache.invalidate_inode(workspace_id, ino);
        Ok(result.inode)
    }

    fn mutation_error(&self, error: WorkspaceError) -> MetaError {
        if matches!(error, WorkspaceError::Fenced) {
            self.metrics.record_fenced_write();
        }
        workspace_to_meta(error)
    }

    fn hole_extents(
        &self,
        layer_id: super::ids::LayerId,
        ino: i64,
        start: u64,
        end: u64,
    ) -> Result<Vec<DataExtentDelta>, MetaError> {
        let mut extents = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let chunk_index = cursor / self.chunk_size;
            let logical_offset = cursor % self.chunk_size;
            let length = (end - cursor).min(self.chunk_size - logical_offset);
            extents.push(DataExtentDelta::hole(
                layer_id,
                ino,
                chunk_index,
                logical_offset,
                length,
                0,
            ));
            cursor = cursor
                .checked_add(length)
                .ok_or_else(|| MetaError::Internal("truncate range overflows".into()))?;
        }
        Ok(extents)
    }

    async fn reverse_entries(&self, target: i64) -> Result<Vec<(i64, String, String)>, MetaError> {
        if target == self.root_ino() {
            return Ok(vec![(self.root_ino(), String::new(), "/".into())]);
        }
        let mut found = Vec::new();
        let mut pending = vec![(self.root_ino(), String::new())];
        let mut visited_dirs = BTreeSet::new();
        while let Some((dir, prefix)) = pending.pop() {
            if !visited_dirs.insert(dir) {
                continue;
            }
            if visited_dirs.len() > 1_000_000 {
                return Err(MetaError::Internal(
                    "workspace reverse path scan exceeded safety limit".into(),
                ));
            }
            for entry in self.readdir(dir).await? {
                let path = if prefix.is_empty() {
                    format!("/{}", entry.name)
                } else {
                    format!("{prefix}/{}", entry.name)
                };
                if entry.ino == target {
                    found.push((dir, entry.name.clone(), path.clone()));
                }
                if entry.kind == FileType::Dir {
                    pending.push((entry.ino, path));
                }
            }
        }
        found.sort_by(|left, right| left.2.cmp(&right.2));
        Ok(found)
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_entry(
        &self,
        parent: i64,
        name: String,
        kind: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
        symlink_target: Option<Vec<u8>>,
    ) -> Result<(i64, FileAttr), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        validate_name(&name)?;
        let mut parent_inode = self
            .resolve_inode_delta(parent)
            .await?
            .ok_or(MetaError::ParentNotFound(parent))?;
        if file_type_from_code(parent_inode.kind)? != FileType::Dir {
            return Err(MetaError::NotDirectory(parent));
        }
        if self
            .resolve_dentry_entry(parent, name.as_bytes())
            .await?
            .is_some()
        {
            return Err(MetaError::AlreadyExists { parent, name });
        }

        let ino = self
            .store
            .allocate_id("inode")
            .await
            .map_err(|error| self.mutation_error(error))?;
        let guard = self.guard().await;
        let now = now_ns()?;
        let inode = InodeDelta {
            layer_id: guard.expected_head_layer_id,
            ino,
            state: InodeState::Present,
            kind: file_type_code(kind),
            size: symlink_target
                .as_ref()
                .map_or(0, |target| target.len() as u64),
            mode: mode & 0o7777,
            uid,
            gid,
            rdev,
            nlink: if kind == FileType::Dir { 2 } else { 1 },
            atime_ns: now,
            mtime_ns: now,
            ctime_ns: now,
            symlink_target,
            parent_hint: Some(parent),
            data_version: 1,
            sequence: 0,
        };
        parent_inode.layer_id = guard.expected_head_layer_id;
        parent_inode.mtime_ns = now;
        parent_inode.ctime_ns = now;
        parent_inode.sequence = 0;
        if kind == FileType::Dir {
            parent_inode.nlink = parent_inode.nlink.checked_add(1).ok_or_else(|| {
                MetaError::Internal("parent directory link count overflow".into())
            })?;
        }
        self.store
            .apply_namespace_mutation(NamespaceMutation {
                guard,
                dentries: vec![DentryDelta::put(
                    inode.layer_id,
                    parent,
                    name.into_bytes(),
                    ino,
                    inode.kind,
                    0,
                )],
                inodes: vec![inode.clone(), parent_inode],
            })
            .await
            .map_err(workspace_to_meta)?;
        Ok((ino, file_attr(&inode)?))
    }

    async fn remove_entry(
        &self,
        parent: i64,
        name: &str,
        directory: bool,
    ) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        validate_name(name)?;
        let entry = self
            .resolve_dentry_entry(parent, name.as_bytes())
            .await?
            .ok_or(MetaError::NotFound(parent))?;
        let mut inode = self
            .resolve_inode_delta(entry.ino)
            .await?
            .ok_or(MetaError::NotFound(entry.ino))?;
        let kind = file_type_from_code(inode.kind)?;
        if directory && kind != FileType::Dir {
            return Err(MetaError::NotDirectory(entry.ino));
        }
        if !directory && kind == FileType::Dir {
            return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                libc::EISDIR,
            )));
        }
        if directory && !self.readdir(entry.ino).await?.is_empty() {
            return Err(MetaError::DirectoryNotEmpty(entry.ino));
        }
        let mut parent_inode = self
            .resolve_inode_delta(parent)
            .await?
            .ok_or(MetaError::ParentNotFound(parent))?;
        let guard = self.guard().await;
        let now = now_ns()?;
        parent_inode.layer_id = guard.expected_head_layer_id;
        parent_inode.mtime_ns = now;
        parent_inode.ctime_ns = now;
        parent_inode.sequence = 0;
        if directory {
            parent_inode.nlink = parent_inode.nlink.checked_sub(1).ok_or_else(|| {
                MetaError::Internal("parent directory link count underflow".into())
            })?;
        }
        inode.layer_id = guard.expected_head_layer_id;
        inode.ctime_ns = now;
        inode.sequence = 0;
        if directory {
            inode.state = InodeState::Deleted;
            inode.nlink = 0;
        } else {
            inode.nlink = inode
                .nlink
                .checked_sub(1)
                .ok_or_else(|| MetaError::Internal("inode link count underflow".into()))?;
            if inode.nlink == 0 && !self.open_counts.contains_key(&entry.ino) {
                inode.state = InodeState::Deleted;
            }
        }
        self.store
            .apply_namespace_mutation(NamespaceMutation {
                guard,
                dentries: vec![DentryDelta::whiteout(
                    parent_inode.layer_id,
                    parent,
                    name.as_bytes().to_vec(),
                    0,
                )],
                inodes: vec![inode, parent_inode],
            })
            .await
            .map_err(workspace_to_meta)?;
        Ok(())
    }
}

#[async_trait]
impl<W: WorkspaceStore + 'static> MetaLayer for WorkspaceMetaLayer<W> {
    fn name(&self) -> &'static str {
        "workspace-overlay"
    }

    fn root_ino(&self) -> i64 {
        self.root_ino.load(Ordering::Acquire)
    }

    fn chroot(&self, inode: i64) {
        self.root_ino.store(inode, Ordering::Release);
    }

    async fn initialize(&self) -> Result<(), MetaError> {
        self.store
            .capabilities()
            .validate_for_v1_mount()
            .map_err(workspace_to_meta)?;
        let header = self
            .store
            .load_volume_header()
            .await
            .map_err(workspace_to_meta)?
            .ok_or_else(|| MetaError::Internal("workspace volume marker is missing".into()))?;
        if header.volume_format != "workspace-v1" {
            return Err(MetaError::NotSupported(header.volume_format));
        }
        self.chain().await?;
        Ok(())
    }

    async fn stat_fs(&self) -> Result<StatFsSnapshot, MetaError> {
        // A correct effective-view count would require enumerating both layers
        // while blocking mutations. That O(N) scan is forbidden on the hot
        // `statfs` path and becomes unbounded for large workspaces. Report the
        // limitation explicitly until usage counters are persisted and updated
        // atomically with workspace mutations.
        Err(MetaError::NotSupported(
            "workspace statfs requires persistent usage counters".into(),
        ))
    }

    async fn stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        self.resolve_inode_delta(ino)
            .await?
            .as_ref()
            .map(file_attr)
            .transpose()
    }

    async fn stat_fresh(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        self.stat(ino).await
    }

    async fn record_open(
        &self,
        ino: i64,
        _attr: FileAttr,
        _read: bool,
        _write: bool,
        _append: bool,
    ) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        if self.resolve_inode_delta(ino).await?.is_none() {
            return Err(MetaError::NotFound(ino));
        }
        self.open_counts
            .entry(ino)
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
        Ok(())
    }

    async fn record_close(&self, ino: i64) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        let remove = if let Some(mut count) = self.open_counts.get_mut(&ino) {
            *count = count.saturating_sub(1);
            *count == 0
        } else {
            false
        };
        if remove {
            self.open_counts.remove(&ino);
            if let Some(mut inode) = self.resolve_inode_delta(ino).await?
                && inode.nlink == 0
            {
                inode.state = InodeState::Deleted;
                inode.ctime_ns = now_ns()?;
                self.mutate_inode(inode).await?;
            }
        }
        Ok(())
    }

    async fn lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        validate_name(name)?;
        Ok(self
            .resolve_dentry_entry(parent, name.as_bytes())
            .await?
            .map(|entry| entry.ino))
    }

    async fn lookup_path(&self, path: &str) -> Result<Option<(i64, FileType)>, MetaError> {
        if !path.starts_with('/') {
            return Err(MetaError::InvalidPath(path.into()));
        }
        let mut ino = self.root_ino();
        if path == "/" {
            let attr = self.stat(ino).await?.ok_or(MetaError::NotFound(ino))?;
            return Ok(Some((ino, attr.kind)));
        }
        for component in path.split('/').filter(|part| !part.is_empty()) {
            ino = match self.lookup(ino, component).await? {
                Some(ino) => ino,
                None => return Ok(None),
            };
        }
        let attr = self.stat(ino).await?.ok_or(MetaError::NotFound(ino))?;
        Ok(Some((ino, attr.kind)))
    }

    async fn readdir(&self, ino: i64) -> Result<Vec<DirEntry>, MetaError> {
        let attr = self.stat(ino).await?.ok_or(MetaError::NotFound(ino))?;
        if attr.kind != FileType::Dir {
            return Err(MetaError::NotDirectory(ino));
        }
        let chain = self.chain().await?;
        let rows = self
            .store
            .get_dentry_deltas(DentryQuery {
                layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
                parent_ino: ino,
                name: None,
            })
            .await
            .map_err(workspace_to_meta)?;
        resolve_directory(&chain, &rows, ino)
            .map_err(workspace_to_meta)?
            .into_iter()
            .map(|entry| {
                Ok(DirEntry {
                    name: String::from_utf8(entry.name).map_err(|_| MetaError::InvalidFilename)?,
                    ino: entry.ino,
                    kind: file_type_from_code(entry.entry_type)?,
                })
            })
            .collect()
    }

    async fn opendir(&self, ino: i64) -> Result<DirHandle, MetaError> {
        let attr = self.stat(ino).await?.ok_or(MetaError::NotFound(ino))?;
        if attr.kind != FileType::Dir {
            return Err(MetaError::NotDirectory(ino));
        }
        Ok(DirHandle::new(ino, self.readdir(ino).await?).with_attr(attr))
    }

    async fn mkdir(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        self.create_entry(parent, name, FileType::Dir, 0o755, 0, 0, 0, None)
            .await
            .map(|result| result.0)
    }

    async fn rmdir(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        self.remove_entry(parent, name, true).await
    }

    async fn create_file(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        self.create_entry(parent, name, FileType::File, 0o644, 0, 0, 0, None)
            .await
            .map(|result| result.0)
    }

    async fn create_file_with_attr(
        &self,
        parent: i64,
        name: String,
    ) -> Result<CreateEntryResult, MetaError> {
        let (ino, attr) = self
            .create_entry(parent, name, FileType::File, 0o644, 0, 0, 0, None)
            .await?;
        Ok(CreateEntryResult {
            ino,
            attr: Some(attr),
        })
    }

    async fn create_node(
        &self,
        parent: i64,
        name: String,
        kind: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
    ) -> Result<i64, MetaError> {
        self.create_entry(parent, name, kind, mode, uid, gid, rdev, None)
            .await
            .map(|result| result.0)
    }

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
        let (ino, attr) = self
            .create_entry(parent, name, kind, mode, uid, gid, rdev, None)
            .await?;
        Ok(CreateEntryResult {
            ino,
            attr: Some(attr),
        })
    }

    async fn link(&self, ino: i64, parent: i64, name: &str) -> Result<FileAttr, MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        validate_name(name)?;
        if self.lookup(parent, name).await?.is_some() {
            return Err(MetaError::AlreadyExists {
                parent,
                name: name.into(),
            });
        }
        let mut inode = self
            .resolve_inode_delta(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;
        if file_type_from_code(inode.kind)? == FileType::Dir {
            return Err(MetaError::NotSupported("hard links to directories".into()));
        }
        let mut parent_inode = self
            .resolve_inode_delta(parent)
            .await?
            .ok_or(MetaError::ParentNotFound(parent))?;
        if file_type_from_code(parent_inode.kind)? != FileType::Dir {
            return Err(MetaError::NotDirectory(parent));
        }
        let guard = self.guard().await;
        let now = now_ns()?;
        inode.layer_id = guard.expected_head_layer_id;
        inode.nlink = inode
            .nlink
            .checked_add(1)
            .ok_or_else(|| MetaError::Internal("inode link count overflow".into()))?;
        inode.ctime_ns = now;
        inode.sequence = 0;
        parent_inode.layer_id = guard.expected_head_layer_id;
        parent_inode.mtime_ns = now;
        parent_inode.ctime_ns = now;
        parent_inode.sequence = 0;
        self.store
            .apply_namespace_mutation(NamespaceMutation {
                guard,
                dentries: vec![DentryDelta::put(
                    inode.layer_id,
                    parent,
                    name.as_bytes().to_vec(),
                    ino,
                    inode.kind,
                    0,
                )],
                inodes: vec![inode.clone(), parent_inode],
            })
            .await
            .map_err(workspace_to_meta)?;
        file_attr(&inode)
    }

    async fn symlink(
        &self,
        parent: i64,
        name: &str,
        target: &str,
    ) -> Result<(i64, FileAttr), MetaError> {
        self.create_entry(
            parent,
            name.into(),
            FileType::Symlink,
            0o777,
            0,
            0,
            0,
            Some(target.as_bytes().to_vec()),
        )
        .await
    }

    async fn unlink(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        self.remove_entry(parent, name, false).await
    }

    async fn rename(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
    ) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        validate_name(old_name)?;
        validate_name(&new_name)?;
        if old_parent == new_parent && old_name == new_name {
            return Ok(());
        }
        let source = self
            .resolve_dentry_entry(old_parent, old_name.as_bytes())
            .await?
            .ok_or(MetaError::NotFound(old_parent))?;
        let mut source_inode = self
            .resolve_inode_delta(source.ino)
            .await?
            .ok_or(MetaError::NotFound(source.ino))?;
        let source_kind = file_type_from_code(source_inode.kind)?;
        let destination = self
            .resolve_dentry_entry(new_parent, new_name.as_bytes())
            .await?;

        if let Some(destination) = destination.as_ref()
            && destination.ino == source.ino
        {
            // POSIX rename onto another hard link to the same inode removes
            // only the old name.
            source_inode.nlink = source_inode
                .nlink
                .checked_sub(1)
                .ok_or_else(|| MetaError::Internal("inode link count underflow".into()))?;
            source_inode.ctime_ns = now_ns()?;
            let guard = self.guard().await;
            source_inode.layer_id = guard.expected_head_layer_id;
            source_inode.sequence = 0;
            self.store
                .apply_namespace_mutation(NamespaceMutation {
                    guard,
                    dentries: vec![DentryDelta::whiteout(
                        source_inode.layer_id,
                        old_parent,
                        old_name.as_bytes().to_vec(),
                        0,
                    )],
                    inodes: vec![source_inode],
                })
                .await
                .map_err(workspace_to_meta)?;
            return Ok(());
        }

        let mut destination_inode = match destination.as_ref() {
            Some(entry) => Some(
                self.resolve_inode_delta(entry.ino)
                    .await?
                    .ok_or(MetaError::NotFound(entry.ino))?,
            ),
            None => None,
        };
        if let Some(inode) = destination_inode.as_ref() {
            let destination_kind = file_type_from_code(inode.kind)?;
            match (
                source_kind == FileType::Dir,
                destination_kind == FileType::Dir,
            ) {
                (true, false) => return Err(MetaError::NotDirectory(inode.ino)),
                (false, true) => {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::EISDIR,
                    )));
                }
                (true, true) if !self.readdir(inode.ino).await?.is_empty() => {
                    return Err(MetaError::DirectoryNotEmpty(inode.ino));
                }
                _ => {}
            }
        }

        let mut old_parent_inode = self
            .resolve_inode_delta(old_parent)
            .await?
            .ok_or(MetaError::ParentNotFound(old_parent))?;
        let mut new_parent_inode = if old_parent == new_parent {
            old_parent_inode.clone()
        } else {
            self.resolve_inode_delta(new_parent)
                .await?
                .ok_or(MetaError::ParentNotFound(new_parent))?
        };
        if file_type_from_code(new_parent_inode.kind)? != FileType::Dir {
            return Err(MetaError::NotDirectory(new_parent));
        }
        let guard = self.guard().await;
        let now = now_ns()?;
        let head = guard.expected_head_layer_id;
        old_parent_inode.layer_id = head;
        old_parent_inode.mtime_ns = now;
        old_parent_inode.ctime_ns = now;
        old_parent_inode.sequence = 0;
        new_parent_inode.layer_id = head;
        new_parent_inode.mtime_ns = now;
        new_parent_inode.ctime_ns = now;
        new_parent_inode.sequence = 0;

        if source_kind == FileType::Dir && old_parent != new_parent {
            old_parent_inode.nlink = old_parent_inode
                .nlink
                .checked_sub(1)
                .ok_or_else(|| MetaError::Internal("directory nlink underflow".into()))?;
            new_parent_inode.nlink = new_parent_inode
                .nlink
                .checked_add(1)
                .ok_or_else(|| MetaError::Internal("directory nlink overflow".into()))?;
            source_inode.parent_hint = Some(new_parent);
        }
        if let Some(inode) = destination_inode.as_mut() {
            if file_type_from_code(inode.kind)? == FileType::Dir {
                new_parent_inode.nlink = new_parent_inode
                    .nlink
                    .checked_sub(1)
                    .ok_or_else(|| MetaError::Internal("directory nlink underflow".into()))?;
                inode.nlink = 0;
                inode.state = InodeState::Deleted;
            } else {
                inode.nlink = inode
                    .nlink
                    .checked_sub(1)
                    .ok_or_else(|| MetaError::Internal("inode nlink underflow".into()))?;
                if inode.nlink == 0 && !self.open_counts.contains_key(&inode.ino) {
                    inode.state = InodeState::Deleted;
                }
            }
            inode.layer_id = head;
            inode.ctime_ns = now;
            inode.sequence = 0;
        }
        source_inode.layer_id = head;
        source_inode.ctime_ns = now;
        source_inode.sequence = 0;

        let mut inodes = vec![source_inode];
        if old_parent == new_parent {
            inodes.push(new_parent_inode);
        } else {
            inodes.push(old_parent_inode);
            inodes.push(new_parent_inode);
        }
        if let Some(inode) = destination_inode {
            inodes.push(inode);
        }
        self.store
            .apply_namespace_mutation(NamespaceMutation {
                guard,
                dentries: vec![
                    DentryDelta::whiteout(head, old_parent, old_name.as_bytes().to_vec(), 0),
                    DentryDelta::put(
                        head,
                        new_parent,
                        new_name.into_bytes(),
                        source.ino,
                        source.entry_type,
                        0,
                    ),
                ],
                inodes,
            })
            .await
            .map_err(workspace_to_meta)?;
        Ok(())
    }

    async fn rename_exchange(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: &str,
    ) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        validate_name(old_name)?;
        validate_name(new_name)?;
        let left = self
            .resolve_dentry_entry(old_parent, old_name.as_bytes())
            .await?
            .ok_or(MetaError::NotFound(old_parent))?;
        let right = self
            .resolve_dentry_entry(new_parent, new_name.as_bytes())
            .await?
            .ok_or(MetaError::NotFound(new_parent))?;
        if old_parent == new_parent && old_name == new_name {
            return Ok(());
        }
        let guard = self.guard().await;
        let head = guard.expected_head_layer_id;
        let mut inodes = Vec::new();
        if old_parent != new_parent {
            let mut left_inode = self
                .resolve_inode_delta(left.ino)
                .await?
                .ok_or(MetaError::NotFound(left.ino))?;
            let mut right_inode = self
                .resolve_inode_delta(right.ino)
                .await?
                .ok_or(MetaError::NotFound(right.ino))?;
            if file_type_from_code(left_inode.kind)? == FileType::Dir {
                left_inode.parent_hint = Some(new_parent);
            }
            if file_type_from_code(right_inode.kind)? == FileType::Dir {
                right_inode.parent_hint = Some(old_parent);
            }
            left_inode.layer_id = head;
            right_inode.layer_id = head;
            left_inode.sequence = 0;
            right_inode.sequence = 0;
            inodes.extend([left_inode, right_inode]);
        }
        self.store
            .apply_namespace_mutation(NamespaceMutation {
                guard,
                dentries: vec![
                    DentryDelta::put(
                        head,
                        old_parent,
                        old_name.as_bytes().to_vec(),
                        right.ino,
                        right.entry_type,
                        0,
                    ),
                    DentryDelta::put(
                        head,
                        new_parent,
                        new_name.as_bytes().to_vec(),
                        left.ino,
                        left.entry_type,
                        0,
                    ),
                ],
                inodes,
            })
            .await
            .map_err(workspace_to_meta)?;
        Ok(())
    }

    async fn set_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError> {
        self.truncate(ino, size, self.chunk_size).await
    }

    async fn extend_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError> {
        let inode = self
            .resolve_inode_delta(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;
        if size > inode.size {
            self.set_file_size(ino, size).await?;
        }
        Ok(())
    }

    async fn truncate(&self, ino: i64, size: u64, chunk_size: u64) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        if chunk_size != self.chunk_size {
            return Err(MetaError::Internal(format!(
                "workspace chunk-size mismatch: mounted {}, requested {chunk_size}",
                self.chunk_size
            )));
        }
        let mut inode = self
            .resolve_inode_delta(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;
        if file_type_from_code(inode.kind)? != FileType::File {
            return Err(MetaError::NotSupported(
                "truncate requires a regular file".into(),
            ));
        }
        if inode.size == size {
            return Ok(());
        }
        let extents = if size < inode.size {
            self.hole_extents(inode.layer_id, ino, size, inode.size)?
        } else {
            Vec::new()
        };
        inode.size = size;
        inode.mtime_ns = now_ns()?;
        inode.ctime_ns = inode.mtime_ns;
        inode.data_version = inode.data_version.saturating_add(1);
        self.mutate_data(inode, extents).await?;
        Ok(())
    }

    async fn get_names(&self, ino: i64) -> Result<Vec<(Option<i64>, String)>, MetaError> {
        Ok(self
            .reverse_entries(ino)
            .await?
            .into_iter()
            .map(|(parent, name, _)| (Some(parent), name))
            .collect())
    }

    async fn get_dentries(&self, ino: i64) -> Result<Vec<(i64, String)>, MetaError> {
        Ok(self
            .reverse_entries(ino)
            .await?
            .into_iter()
            .map(|(parent, name, _)| (parent, name))
            .collect())
    }

    async fn get_dir_parent(&self, dir_ino: i64) -> Result<Option<i64>, MetaError> {
        Ok(self
            .resolve_inode_delta(dir_ino)
            .await?
            .and_then(|inode| inode.parent_hint))
    }

    async fn get_paths(&self, ino: i64) -> Result<Vec<String>, MetaError> {
        Ok(self
            .reverse_entries(ino)
            .await?
            .into_iter()
            .map(|(_, _, path)| path)
            .collect())
    }

    async fn read_symlink(&self, ino: i64) -> Result<String, MetaError> {
        let inode = self
            .resolve_inode_delta(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;
        if file_type_from_code(inode.kind)? != FileType::Symlink {
            return Err(MetaError::NotSupported("inode is not a symlink".into()));
        }
        String::from_utf8(
            inode
                .symlink_target
                .ok_or_else(|| MetaError::Internal("symlink inode is missing its target".into()))?,
        )
        .map_err(|_| MetaError::InvalidPath("symlink target is not UTF-8".into()))
    }

    async fn set_attr(
        &self,
        ino: i64,
        req: &SetAttrRequest,
        flags: SetAttrFlags,
    ) -> Result<FileAttr, MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        let mut inode = self
            .resolve_inode_delta(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;
        let old_size = inode.size;
        if let Some(mode) = req.mode {
            inode.mode = mode & 0o7777;
        }
        if let Some(uid) = req.uid {
            inode.uid = uid;
        }
        if let Some(gid) = req.gid {
            inode.gid = gid;
        }
        if let Some(size) = req.size {
            inode.size = size;
            if size != old_size {
                inode.data_version = inode.data_version.saturating_add(1);
            }
        }
        if let Some(atime) = req.atime {
            inode.atime_ns = atime;
        }
        if let Some(mtime) = req.mtime {
            inode.mtime_ns = mtime;
        }
        if let Some(ctime) = req.ctime {
            inode.ctime_ns = ctime;
        } else {
            inode.ctime_ns = now_ns()?;
        }
        if flags.contains(SetAttrFlags::CLEAR_SUID) {
            inode.mode &= !0o4000;
        }
        if flags.contains(SetAttrFlags::CLEAR_SGID) {
            inode.mode &= !0o2000;
        }
        let inode = if inode.size != old_size {
            let extents = if inode.size < old_size {
                self.hole_extents(inode.layer_id, ino, inode.size, old_size)?
            } else {
                Vec::new()
            };
            self.mutate_data(inode, extents).await?
        } else {
            self.mutate_inode(inode).await?
        };
        file_attr(&inode)
    }

    async fn open(&self, ino: i64, flags: OpenFlags) -> Result<FileAttr, MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        let mut inode = self
            .resolve_inode_delta(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;
        let kind = file_type_from_code(inode.kind)?;
        if kind == FileType::Symlink {
            return Err(MetaError::NotSupported(
                "opening a workspace symlink is not supported".into(),
            ));
        }
        if flags.contains(OpenFlags::TRUNC) {
            if kind != FileType::File {
                return Err(MetaError::NotSupported(
                    "truncate-on-open requires a regular file".into(),
                ));
            }
            let old_size = inode.size;
            inode.size = 0;
            inode.data_version = inode.data_version.saturating_add(1);
            inode.mtime_ns = now_ns()?;
            inode.atime_ns = now_ns()?;
            let extents = self.hole_extents(inode.layer_id, ino, 0, old_size)?;
            let inode = self.mutate_data(inode, extents).await?;
            return file_attr(&inode);
        }
        inode.atime_ns = now_ns()?;
        let inode = self.mutate_inode(inode).await?;
        file_attr(&inode)
    }

    async fn close(&self, ino: i64) -> Result<(), MetaError> {
        if self.resolve_inode_delta(ino).await?.is_some() {
            Ok(())
        } else {
            Err(MetaError::NotFound(ino))
        }
    }

    async fn write(
        &self,
        ino: i64,
        chunk_id: u64,
        slice: SliceDesc,
        new_size: u64,
    ) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        let (chunk_ino, chunk_index) = crate::vfs::extract_ino_and_chunk_index(chunk_id);
        if chunk_ino != ino || slice.chunk_id != chunk_id || slice.length == 0 {
            return Err(MetaError::InvalidPath(
                "slice does not match the workspace inode/chunk".into(),
            ));
        }
        let end = slice
            .offset
            .checked_add(slice.length)
            .ok_or_else(|| MetaError::InvalidPath("slice range overflows".into()))?;
        if end > self.chunk_size {
            return Err(MetaError::InvalidPath(
                "slice range exceeds workspace chunk size".into(),
            ));
        }
        let mut inode = self
            .resolve_inode_delta(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;
        if file_type_from_code(inode.kind)? != FileType::File {
            return Err(MetaError::NotSupported(
                "workspace data writes require a regular file".into(),
            ));
        }
        let chunk_start = chunk_index
            .checked_mul(self.chunk_size)
            .ok_or_else(|| MetaError::InvalidPath("file size overflows".into()))?;
        let slice_start = chunk_start
            .checked_add(slice.offset)
            .ok_or_else(|| MetaError::InvalidPath("slice start overflows".into()))?;
        if new_size <= slice_start {
            return Err(MetaError::InvalidPath(format!(
                "new size {new_size} does not cover slice start {slice_start}"
            )));
        }
        // The writer may persist an aligned physical slice that extends past the
        // logical EOF (for example a 64 KiB cached sub-block produced by a 4 KiB
        // mmap write).  Only publish the visible prefix.  Keeping the aligned tail
        // out of the extent graph also guarantees that a later truncate-extend
        // observes zeroes rather than resurrecting bytes beyond the old EOF.
        let visible_length = slice.length.min(new_size - slice_start);
        let now = now_ns()?;
        inode.size = inode.size.max(new_size);
        inode.mtime_ns = now;
        inode.ctime_ns = now;
        inode.data_version = inode.data_version.saturating_add(1);
        let extent = DataExtentDelta::data(
            inode.layer_id,
            ino,
            chunk_index,
            slice.offset,
            visible_length,
            slice.slice_id,
            0,
            0,
        );
        self.mutate_data(inode, vec![extent]).await?;
        Ok(())
    }

    async fn get_deleted_files(&self) -> Result<Vec<i64>, MetaError> {
        Ok(Vec::new())
    }

    async fn remove_file_metadata(&self, _ino: i64) -> Result<(), MetaError> {
        Err(MetaError::NotSupported(
            "workspace inode garbage collection is lifecycle-managed".into(),
        ))
    }

    async fn get_slices(&self, _chunk_id: u64) -> Result<Vec<SliceDesc>, MetaError> {
        Err(MetaError::NotSupported(
            "workspace extents must be consumed through a neutral read plan".into(),
        ))
    }

    async fn append_slice(&self, chunk_id: u64, slice: SliceDesc) -> Result<(), MetaError> {
        let (ino, chunk_index) = crate::vfs::extract_ino_and_chunk_index(chunk_id);
        let new_size = chunk_index
            .checked_mul(self.chunk_size)
            .and_then(|start| start.checked_add(slice.offset))
            .and_then(|start| start.checked_add(slice.length))
            .ok_or_else(|| MetaError::InvalidPath("slice file size overflows".into()))?;
        self.write(ino, chunk_id, slice, new_size).await
    }

    async fn next_id(&self, key: &str) -> Result<i64, MetaError> {
        let allocator = match key {
            crate::meta::INODE_ID_KEY => "inode",
            crate::meta::SLICE_ID_KEY => "slice",
            _ => {
                return Err(MetaError::NotSupported(format!(
                    "workspace allocator does not support key {key}"
                )));
            }
        };
        self.store
            .allocate_id(allocator)
            .await
            .map_err(workspace_to_meta)
    }

    async fn start_session(&self, _session_info: SessionInfo) -> Result<(), MetaError> {
        Ok(())
    }

    async fn shutdown_session(&self) -> Result<(), MetaError> {
        Ok(())
    }

    async fn get_plock(
        &self,
        inode: i64,
        query: &FileLockQuery,
    ) -> Result<FileLockInfo, MetaError> {
        let locks = self.locks.lock().await;
        for ((candidate_inode, owner), records) in &locks.plocks {
            if *candidate_inode == inode && *owner != query.owner {
                for record in records {
                    if record.lock_range.overlaps(&query.range)
                        && (record.lock_type == FileLockType::Write
                            || query.lock_type == FileLockType::Write)
                    {
                        return Ok(FileLockInfo {
                            lock_type: record.lock_type,
                            range: record.lock_range,
                            pid: record.pid,
                        });
                    }
                }
            }
        }
        Ok(FileLockInfo {
            lock_type: FileLockType::UnLock,
            range: FileLockRange { start: 0, end: 0 },
            pid: 0,
        })
    }

    async fn set_plock(
        &self,
        inode: i64,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
        range: FileLockRange,
        pid: u32,
    ) -> Result<(), MetaError> {
        if range.start >= range.end {
            return Err(MetaError::InvalidPath("invalid POSIX lock range".into()));
        }
        loop {
            let mut locks = self.locks.lock().await;
            let conflict = lock_type != FileLockType::UnLock
                && locks
                    .plocks
                    .iter()
                    .any(|((candidate_inode, candidate_owner), records)| {
                        *candidate_inode == inode
                            && *candidate_owner != owner
                            && PlockRecord::check_conflict(&lock_type, &range, records)
                    });
            if !conflict {
                let key = (inode, owner);
                let current = locks.plocks.remove(&key).unwrap_or_default();
                let updated = PlockRecord::update_locks(
                    current,
                    PlockRecord::new(lock_type, pid, range.start, range.end),
                );
                if !updated.is_empty() {
                    locks.plocks.insert(key, updated);
                }
                return Ok(());
            }
            drop(locks);
            if !block {
                return Err(MetaError::LockConflict {
                    inode,
                    owner,
                    range,
                });
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    async fn get_flock(&self, inode: i64, owner: i64) -> Result<FileLockType, MetaError> {
        Ok(self
            .locks
            .lock()
            .await
            .flocks
            .get(&(inode, owner))
            .copied()
            .unwrap_or(FileLockType::UnLock))
    }

    async fn set_flock(
        &self,
        inode: i64,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
    ) -> Result<(), MetaError> {
        loop {
            let mut locks = self.locks.lock().await;
            let conflict = lock_type != FileLockType::UnLock
                && locks.flocks.iter().any(
                    |((candidate_inode, candidate_owner), candidate_type)| {
                        *candidate_inode == inode
                            && *candidate_owner != owner
                            && (*candidate_type == FileLockType::Write
                                || lock_type == FileLockType::Write)
                    },
                );
            if !conflict {
                if lock_type == FileLockType::UnLock {
                    locks.flocks.remove(&(inode, owner));
                } else {
                    locks.flocks.insert((inode, owner), lock_type);
                }
                return Ok(());
            }
            drop(locks);
            if !block {
                return Err(MetaError::LockConflict {
                    inode,
                    owner,
                    range: FileLockRange {
                        start: 0,
                        end: u64::MAX,
                    },
                });
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    async fn set_xattr(
        &self,
        inode: i64,
        name: &str,
        value: &[u8],
        flags: u32,
    ) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        validate_xattr_name(name)?;
        let mut inode_delta = self
            .resolve_inode_delta(inode)
            .await?
            .ok_or(MetaError::NotFound(inode))?;
        let existing = self.get_xattr(inode, name).await?;
        let create_only = flags & libc::XATTR_CREATE as u32 != 0;
        let replace_only = flags & libc::XATTR_REPLACE as u32 != 0;
        if create_only && replace_only {
            return Err(MetaError::InvalidPath(
                "XATTR_CREATE and XATTR_REPLACE are mutually exclusive".into(),
            ));
        }
        if create_only && existing.is_some() {
            return Err(MetaError::AlreadyExists {
                parent: inode,
                name: name.into(),
            });
        }
        if replace_only && existing.is_none() {
            return Err(MetaError::NotFound(inode));
        }
        let guard = self.guard().await;
        inode_delta.layer_id = guard.expected_head_layer_id;
        inode_delta.sequence = 0;
        inode_delta.ctime_ns = now_ns()?;
        self.store
            .apply_xattr_mutation(XattrMutation {
                xattr: XattrDelta {
                    layer_id: guard.expected_head_layer_id,
                    ino: inode,
                    name: name.as_bytes().to_vec(),
                    op: ValueOp::Put,
                    value: Some(value.to_vec()),
                    sequence: 0,
                },
                inode: inode_delta,
                guard,
            })
            .await
            .map_err(workspace_to_meta)
    }

    async fn get_xattr(&self, inode: i64, name: &str) -> Result<Option<Vec<u8>>, MetaError> {
        validate_xattr_name(name)?;
        if self.resolve_inode_delta(inode).await?.is_none() {
            return Err(MetaError::NotFound(inode));
        }
        let chain = self.chain().await?;
        let rows = self
            .store
            .get_xattr_deltas(XattrQuery {
                layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
                ino: inode,
                name: Some(name.as_bytes().to_vec()),
            })
            .await
            .map_err(workspace_to_meta)?;
        Ok(resolve_xattr(&chain, &rows, inode, name.as_bytes())
            .map_err(workspace_to_meta)?
            .map(|resolved| resolved.value))
    }

    async fn list_xattr(&self, inode: i64) -> Result<Vec<String>, MetaError> {
        if self.resolve_inode_delta(inode).await?.is_none() {
            return Err(MetaError::NotFound(inode));
        }
        let chain = self.chain().await?;
        let rows = self
            .store
            .get_xattr_deltas(XattrQuery {
                layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
                ino: inode,
                name: None,
            })
            .await
            .map_err(workspace_to_meta)?;
        let mut candidates = BTreeSet::new();
        for row in &rows {
            candidates.insert(row.name.clone());
        }
        let mut names = Vec::new();
        for name in candidates {
            if resolve_xattr(&chain, &rows, inode, &name)
                .map_err(workspace_to_meta)?
                .is_some()
            {
                names.push(
                    String::from_utf8(name)
                        .map_err(|_| MetaError::Internal("non-UTF-8 xattr name".into()))?,
                );
            }
        }
        Ok(names)
    }

    async fn remove_xattr(&self, inode: i64, name: &str) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        validate_xattr_name(name)?;
        if self.get_xattr(inode, name).await?.is_none() {
            return Err(MetaError::NotFound(inode));
        }
        let mut inode_delta = self
            .resolve_inode_delta(inode)
            .await?
            .ok_or(MetaError::NotFound(inode))?;
        let guard = self.guard().await;
        inode_delta.layer_id = guard.expected_head_layer_id;
        inode_delta.sequence = 0;
        inode_delta.ctime_ns = now_ns()?;
        self.store
            .apply_xattr_mutation(XattrMutation {
                xattr: XattrDelta {
                    layer_id: guard.expected_head_layer_id,
                    ino: inode,
                    name: name.as_bytes().to_vec(),
                    op: ValueOp::Whiteout,
                    value: None,
                    sequence: 0,
                },
                inode: inode_delta,
                guard,
            })
            .await
            .map_err(workspace_to_meta)
    }

    async fn set_acl(&self, inode: i64, rule: AclRule) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        if self.resolve_inode_delta(inode).await?.is_none() {
            return Err(MetaError::NotFound(inode));
        }
        let guard = self.guard().await;
        self.store
            .apply_acl_mutation(AclMutation {
                acl: AclDelta {
                    layer_id: guard.expected_head_layer_id,
                    ino: inode,
                    acl_type: rule.acl_type,
                    acl_id: i64::from(rule.qualifier),
                    op: ValueOp::Put,
                    value: Some(rule.permissions.to_be_bytes().to_vec()),
                    sequence: 0,
                },
                guard,
            })
            .await
            .map_err(workspace_to_meta)
    }

    async fn get_acl(
        &self,
        inode: i64,
        acl_type: u8,
        acl_id: u32,
    ) -> Result<Option<AclRule>, MetaError> {
        if self.resolve_inode_delta(inode).await?.is_none() {
            return Err(MetaError::NotFound(inode));
        }
        let chain = self.chain().await?;
        let rows = self
            .store
            .get_acl_deltas(AclQuery {
                layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
                ino: inode,
                acl_type: Some(acl_type),
                acl_id: Some(i64::from(acl_id)),
            })
            .await
            .map_err(workspace_to_meta)?;
        let Some(resolved) = resolve_acl(&chain, &rows, inode, acl_type, i64::from(acl_id))
            .map_err(workspace_to_meta)?
        else {
            return Ok(None);
        };
        let bytes: [u8; 4] = resolved.value.try_into().map_err(|_| {
            MetaError::Internal("workspace ACL permissions have an invalid encoding".into())
        })?;
        Ok(Some(AclRule {
            acl_type,
            qualifier: acl_id,
            permissions: u32::from_be_bytes(bytes),
        }))
    }
}

#[async_trait]
impl<W: WorkspaceStore + 'static> WorkspaceReadPlanProvider for WorkspaceMetaLayer<W> {
    async fn read_plan(
        &self,
        ino: i64,
        chunk_index: u64,
        offset: u64,
        len: u64,
    ) -> Result<ResolvedReadPlan, MetaError> {
        let inode = self
            .resolve_inode_delta(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| MetaError::Internal("workspace read range overflows".into()))?;
        if end > self.chunk_size {
            return Err(MetaError::Internal(format!(
                "workspace read range ends at {end}, beyond chunk size {}",
                self.chunk_size
            )));
        }
        if len == 0 {
            return Ok(ResolvedReadPlan::default());
        }
        let view = self.view_context().await;
        let cache_key = ReadPlanCacheKey {
            workspace_id: view.workspace_id,
            head_epoch: view.head_epoch,
            ino,
            chunk_index,
            inode_data_version: inode.data_version,
            range_start: offset,
            range_end: end,
        };
        if let Some(plan) = self.resolver_cache.get_read_plan(&cache_key) {
            return Ok(plan);
        }
        let chain = self.chain().await?;
        let rows = self
            .store
            .get_extent_deltas(ExtentQuery {
                layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
                ino,
                chunk_index,
                range_start: offset,
                range_end: end,
            })
            .await
            .map_err(workspace_to_meta)?;
        let extents = resolve_extents(&chain, &rows, ino, chunk_index, offset..end)
            .map_err(workspace_to_meta)?;
        let segments = extents
            .into_iter()
            .map(|extent| match extent.kind {
                super::model::ExtentKind::Data {
                    slice_id,
                    slice_offset,
                } => ReadPlanSegment::Data {
                    logical_offset: extent.logical_offset,
                    length: extent.length,
                    slice_id,
                    slice_offset,
                },
                super::model::ExtentKind::Hole => ReadPlanSegment::Zero {
                    logical_offset: extent.logical_offset,
                    length: extent.length,
                },
            })
            .collect();
        let plan = ResolvedReadPlan { segments };
        self.metrics
            .add_extent_plan_segments(plan.segments.len() as u64);
        self.resolver_cache
            .insert_read_plan(cache_key, plan.clone());
        Ok(plan)
    }

    async fn range_has_data(&self, ino: i64, offset: u64, len: u64) -> Result<bool, MetaError> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| MetaError::Internal("workspace file range overflows".into()))?;
        let mut cursor = offset;
        while cursor < end {
            let chunk_index = cursor / self.chunk_size;
            let chunk_offset = cursor % self.chunk_size;
            let take = (end - cursor).min(self.chunk_size - chunk_offset);
            let plan = self.read_plan(ino, chunk_index, chunk_offset, take).await?;
            if plan
                .segments
                .iter()
                .any(|segment| matches!(segment, ReadPlanSegment::Data { .. }))
            {
                return Ok(true);
            }
            cursor = cursor
                .checked_add(take)
                .ok_or_else(|| MetaError::Internal("workspace range cursor overflows".into()))?;
        }
        Ok(false)
    }

    async fn record_orphan_slice(&self, slice_id: u64, slice_end: u64) -> Result<(), MetaError> {
        self.store
            .record_orphan_slice(RecordOrphanSlice {
                orphan_layer_id: super::ids::LayerId::new(),
                slice_id,
                slice_end,
            })
            .await
            .map_err(workspace_to_meta)
    }

    async fn apply_hole_range(
        &self,
        ino: i64,
        offset: u64,
        len: u64,
        keep_size: bool,
    ) -> Result<u64, MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        let requested_end = offset
            .checked_add(len)
            .ok_or_else(|| MetaError::InvalidPath("hole range overflows".into()))?;
        let mut inode = self
            .resolve_inode_delta(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;
        if file_type_from_code(inode.kind)? != FileType::File {
            return Err(MetaError::NotSupported(
                "hole mutation requires a regular file".into(),
            ));
        }
        let range_end = if keep_size {
            requested_end.min(inode.size)
        } else {
            requested_end
        };
        if len == 0 || offset >= range_end {
            return Ok(inode.size);
        }
        let extents = self.hole_extents(inode.layer_id, ino, offset, range_end)?;
        if !keep_size {
            inode.size = inode.size.max(requested_end);
        }
        let now = now_ns()?;
        inode.mtime_ns = now;
        inode.ctime_ns = now;
        inode.data_version = inode.data_version.saturating_add(1);
        let inode = self.mutate_data(inode, extents).await?;
        Ok(inode.size)
    }
}

fn workspace_to_meta(error: WorkspaceError) -> MetaError {
    match error {
        WorkspaceError::Io(error) => MetaError::Io(error),
        WorkspaceError::Fenced => MetaError::Io(std::io::Error::from_raw_os_error(libc::ESTALE)),
        WorkspaceError::Busy => MetaError::Io(std::io::Error::from_raw_os_error(libc::EBUSY)),
        WorkspaceError::UnsupportedCapability(capability) => {
            MetaError::NotSupported(format!("workspace capability {capability}"))
        }
        WorkspaceError::FeatureNotCompiled(feature) => {
            MetaError::NotSupported(format!("feature {feature} is not compiled"))
        }
        WorkspaceError::WorkspaceNotFound(_) | WorkspaceError::LayerNotFound(_) => {
            MetaError::NotFound(1)
        }
        WorkspaceError::LeaseNotFound(_) => {
            MetaError::Io(std::io::Error::from_raw_os_error(libc::ESTALE))
        }
        WorkspaceError::CorruptMetadata(message)
        | WorkspaceError::InvalidReadPlan(message)
        | WorkspaceError::Backend(message)
        | WorkspaceError::UnsupportedVolumeFormat(message) => MetaError::Internal(message),
        WorkspaceError::UnsupportedSchemaVersion(version) => {
            MetaError::Internal(format!("unsupported workspace schema version {version}"))
        }
        WorkspaceError::Conflict(detail) => MetaError::Internal(format!(
            "workspace conflict at {:?}: {}",
            detail.path, detail.reason
        )),
        WorkspaceError::LayerDepthLimit { depth, hard_limit } => MetaError::Internal(format!(
            "workspace layer depth {depth} exceeds {hard_limit}"
        )),
        WorkspaceError::InvalidStateTransition { from, to } => {
            MetaError::Internal(format!("invalid workspace transition {from} -> {to}"))
        }
    }
}

fn now_ns() -> Result<i64, MetaError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| MetaError::Internal(format!("system clock is before epoch: {error}")))?
        .as_nanos();
    i64::try_from(nanos).map_err(|_| MetaError::Internal("timestamp exceeds i64".into()))
}

fn file_type_code(kind: FileType) -> u8 {
    match kind {
        FileType::File => 0,
        FileType::Dir => 1,
        FileType::Symlink => 2,
        FileType::Fifo => 3,
        FileType::Socket => 4,
        FileType::CharDevice => 5,
        FileType::BlockDevice => 6,
    }
}

fn file_type_from_code(code: u8) -> Result<FileType, MetaError> {
    match code {
        0 => Ok(FileType::File),
        1 => Ok(FileType::Dir),
        2 => Ok(FileType::Symlink),
        3 => Ok(FileType::Fifo),
        4 => Ok(FileType::Socket),
        5 => Ok(FileType::CharDevice),
        6 => Ok(FileType::BlockDevice),
        _ => Err(MetaError::Internal(format!(
            "invalid workspace inode kind {code}"
        ))),
    }
}

fn file_attr(inode: &InodeDelta) -> Result<FileAttr, MetaError> {
    if inode.state != InodeState::Present {
        return Err(MetaError::NotFound(inode.ino));
    }
    Ok(FileAttr {
        ino: inode.ino,
        size: inode.size,
        blocks: inode.size.div_ceil(512),
        kind: file_type_from_code(inode.kind)?,
        mode: inode.mode,
        rdev: inode.rdev,
        uid: inode.uid,
        gid: inode.gid,
        atime: inode.atime_ns,
        mtime: inode.mtime_ns,
        ctime: inode.ctime_ns,
        nlink: inode.nlink,
    })
}

fn validate_name(name: &str) -> Result<(), MetaError> {
    if name.is_empty() || matches!(name, "." | "..") || name.as_bytes().contains(&0) {
        return Err(MetaError::InvalidFilename);
    }
    if name.as_bytes().contains(&b'/') {
        return Err(MetaError::InvalidFilename);
    }
    if name.len() > 255 {
        return Err(MetaError::FilenameTooLong);
    }
    Ok(())
}

fn validate_xattr_name(name: &str) -> Result<(), MetaError> {
    if name.is_empty() || name.as_bytes().contains(&0) {
        return Err(MetaError::InvalidPath("invalid xattr name".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
