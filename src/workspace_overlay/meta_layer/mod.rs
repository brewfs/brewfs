//! `MetaLayer` implementation backed exclusively by workspace deltas.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use dashmap::DashMap;
use futures::StreamExt;
use tokio::sync::{Mutex, RwLock};

use crate::chunk::SliceDesc;
use crate::chunk::layout::DEFAULT_CHUNK_SIZE;
use crate::chunk::read_plan::{
    PreparedUnifiedRead, ReadPlanSegment, ResolvedReadPlan, UnifiedReadRequestFence,
    WorkspaceReadPlanProvider,
};
use crate::meta::client::session::SessionInfo;
use crate::meta::file_lock::{
    FileLockInfo, FileLockQuery, FileLockRange, FileLockType, PlockRecord,
};
use crate::meta::layer::{InodePermissions, MetaLayer, PosixAclCapability};
use crate::meta::posix_acl::{ACCESS_XATTR, DEFAULT_XATTR, PosixAcl};
use crate::meta::store::{
    AclRule, CreateEntryResult, DirEntry, FileAttr, FileType, MetaError, OpenFlags, SetAttrFlags,
    SetAttrRequest, StatFsSnapshot,
};
use crate::vfs::handles::DirHandle;

use super::cache::{ReadPlanCacheKey, WorkspaceResolverCache};
use super::catalog::{
    AclQuery, DentryQuery, ExtentQuery, HeadGuard, InodeQuery, PermissionSnapshot,
    PermissionSnapshotQuery, RecordOrphanSlice, VersionedMutation, WorkspaceStore, XattrQuery,
};
use super::error::WorkspaceError;
use super::ids::LayerId;
use super::metrics::{WorkspaceMetrics, global_workspace_metrics};
use super::model::{
    AclDelta, DataExtentDelta, DentryDelta, InodeDelta, InodeState, LayerRecord, LayerState,
    ValueOp, ViewContext, XattrDelta,
};
use super::resolver::{
    Resolution, ResolvedDentry, resolve_acl_state, resolve_dentry_state, resolve_directory,
    resolve_extents, resolve_inode, resolve_inode_state, resolve_xattr, resolve_xattr_state,
};

mod packed_enumeration;
pub mod packed_lower;
mod packed_paths;
mod packed_permissions;
mod row_cache;
pub use packed_lower::{
    CatalogPackedBindingAuthority, PinnedCatalogPackedBindingAuthority,
    WorkspacePackedBindingAuthority,
};
use packed_lower::{WorkspaceLower, WorkspacePackedLower};
use packed_permissions::MutationVersion;

type PermissionInode = (
    InodeDelta,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
);

/// At most one speculative ancestor, decoded from the current mutation version.
/// Defer its error until the authoritative parent walk actually visits it.
struct PermissionReadAhead {
    ino: i64,
    permission: Result<PermissionInode, MetaError>,
}

pub struct WorkspaceMetaLayer<W> {
    store: Arc<W>,
    view: Arc<RwLock<ViewContext>>,
    layer_pair: RwLock<Option<(LayerId, u64, Vec<LayerRecord>)>>,
    root_ino: AtomicI64,
    chunk_size: u64,
    open_counts: DashMap<i64, u64>,
    resolver_cache: WorkspaceResolverCache,
    upper_rows: Option<Arc<row_cache::UpperRowCache>>,
    /// Path/alias results; invalidated only by namespace-changing commits.
    namespace_rows: Option<row_cache::UpperRowCache>,
    /// Advisory read selection only. Fresh snapshots, never these hints,
    /// determine the ancestry and permissions checked by a mutation.
    directory_hints: moka::sync::Cache<i64, i64>,
    /// When set, both caches serve rows only while the mount provably holds
    /// its exclusive lease; once that lapses they are cleared and bypassed.
    lease_validity: Option<Arc<super::lifecycle::LeaseValidity>>,
    locks: Mutex<WorkspaceLocks>,
    mutation_gate: Mutex<()>,
    /// Data writes waiting for the next group commit (see `write`).
    write_queue: std::sync::Mutex<WriteQueue>,
    metrics: Arc<WorkspaceMetrics>,
    lower: WorkspaceLower,
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

/// Cache keys whose source rows one versioned commit writes. A cached value
/// depends only on the rows of its own key: `Inode` on that inode's rows,
/// `Permission` on its inode and ACL-xattr rows, `Dentry`/`Xattr` on that
/// exact (parent|ino, name). Extent rows touch only their inode's entries.
/// ACL rows have no per-key mapping, so they clear the whole row cache.
pub(super) struct RowsWritten {
    row_keys: Option<Vec<row_cache::RowKey>>,
    /// Any dentry row or inode deletion: paths/aliases may change.
    namespace: bool,
    /// Complete affected set for a fresh create or file-only namespace
    /// mutation. Directory moves can change descendant paths and use None.
    namespace_inodes: Option<Vec<i64>>,
}

impl RowsWritten {
    pub(super) fn of(request: &VersionedMutation) -> Self {
        let namespace = !request.dentries.is_empty()
            || request
                .inodes
                .iter()
                .any(|inode| inode.state == InodeState::Deleted);
        let row_keys = request.acls.is_empty().then(|| {
            let inodes = request
                .inodes
                .iter()
                .map(|row| row.ino)
                .chain(request.extents.iter().map(|row| row.ino))
                .chain(request.xattrs.iter().map(|row| row.ino))
                .collect::<BTreeSet<_>>();
            let mut keys = Vec::with_capacity(
                inodes.len() * 2 + request.dentries.len() + request.xattrs.len(),
            );
            for ino in inodes {
                keys.push(row_cache::RowKey::Inode(ino));
                keys.push(row_cache::RowKey::Permission(ino));
            }
            for row in &request.dentries {
                keys.push(row_cache::RowKey::Dentry(row.parent_ino, row.name.clone()));
            }
            for row in &request.xattrs {
                keys.push(row_cache::RowKey::Xattr(row.ino, row.name.clone()));
            }
            keys
        });
        Self {
            row_keys,
            namespace,
            namespace_inodes: None,
        }
    }

    /// `of(request)` for a create commit whose dentry introduces `ino`.
    /// Falls back to the full namespace invalidation unless the request has
    /// exactly that shape, so a misuse can only cost performance.
    pub(super) fn of_create(request: &VersionedMutation, ino: i64) -> Self {
        let mut written = Self::of(request);
        let [dentry] = request.dentries.as_slice() else {
            return written;
        };
        let fresh = dentry.op == crate::workspace_overlay::model::DentryOp::Put
            && dentry.ino == Some(ino)
            && request.inodes.iter().any(|row| {
                row.ino == ino
                    && row.state == InodeState::Present
                    && row.parent_hint == Some(dentry.parent_ino)
            })
            && request
                .inodes
                .iter()
                .all(|row| row.state != InodeState::Deleted);
        if fresh {
            written.namespace_inodes = Some(vec![ino]);
        }
        written
    }
}

/// One `MetaLayer::write` waiting for a group commit.
struct PendingWrite {
    ino: i64,
    chunk_id: u64,
    slice: SliceDesc,
    new_size: u64,
    reply: tokio::sync::oneshot::Sender<Result<(), MetaError>>,
}

/// Concurrent data writes share one versioned commit. The mutation gate holder
/// commits one batch, then hands leadership to another waiting writer. A batch
/// is one CAS on the same head version a single write uses, so conflict detection
/// and row ordering are unchanged.
#[derive(Default)]
struct WriteQueue {
    pending: Vec<PendingWrite>,
}

/// Each member of a failed batch gets the same failure. I/O errors keep their
/// errno (the FUSE reply); every other kind keeps its variant where it carries
/// only plain data, else its message.
fn duplicate_write_error(error: &MetaError) -> MetaError {
    match error {
        MetaError::Io(io) => match io.raw_os_error() {
            Some(errno) => MetaError::Io(std::io::Error::from_raw_os_error(errno)),
            None => MetaError::Io(std::io::Error::new(io.kind(), io.to_string())),
        },
        MetaError::NotFound(ino) => MetaError::NotFound(*ino),
        MetaError::NotSupported(message) => MetaError::NotSupported(message.clone()),
        MetaError::InvalidPath(message) => MetaError::InvalidPath(message.clone()),
        MetaError::Internal(message) => MetaError::Internal(message.clone()),
        other => MetaError::Internal(other.to_string()),
    }
}

/// Bound on writes folded into one commit, far inside the packet limits.
const MAX_GROUP_WRITES: usize = 64;

/// Bound simultaneous cold snapshots (each packed snapshot admits 2 MiB).
const MAX_GROUP_INODE_READS: usize = 8;

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
            upper_rows: None,
            namespace_rows: None,
            directory_hints: moka::sync::Cache::new(4096),
            lease_validity: None,
            locks: Mutex::new(WorkspaceLocks::default()),
            mutation_gate: Mutex::new(()),
            write_queue: std::sync::Mutex::new(WriteQueue::default()),
            metrics: global_workspace_metrics(),
            lower: WorkspaceLower::Native,
        }
    }

    pub fn with_chunk_size(store: Arc<W>, view: ViewContext, chunk_size: u64) -> Self {
        let mut layer = Self::new(store, view);
        layer.chunk_size = chunk_size;
        layer
    }

    /// Serve read-only upper-row lookups from a short-lived cache. Only valid
    /// while this mount holds the exclusive workspace lease and is the sole
    /// writer; every local commit invalidates it. Frozen packed base only.
    pub fn with_upper_row_cache(mut self, ttl: std::time::Duration) -> Self {
        if !ttl.is_zero() {
            self.upper_rows = Some(Arc::new(row_cache::UpperRowCache::new(ttl)));
            self.namespace_rows = Some(row_cache::UpperRowCache::new(ttl));
        }
        self
    }

    /// Every view change. Clears namespace results too; commits use
    /// `invalidate_rows_written`.
    pub(crate) fn invalidate_upper_rows(&self) {
        if let Some(cache) = &self.upper_rows {
            cache.invalidate_all();
        }
        if let Some(cache) = &self.namespace_rows {
            cache.invalidate_all();
        }
    }

    /// One commit: the row cache drops only the entries whose source rows
    /// the commit writes (see `RowsWritten`), so unrelated directories,
    /// ancestors and ACL xattrs stay cached. Namespace changes invalidate
    /// only a known complete set of file paths/aliases; directory changes
    /// and unknown shapes clear the namespace cache, including descendants.
    pub(super) fn invalidate_rows_written(&self, written: &RowsWritten) {
        if let Some(cache) = &self.upper_rows {
            match &written.row_keys {
                Some(keys) => cache.invalidate_keys(keys),
                None => cache.invalidate_all(),
            }
        }
        if written.namespace
            && let Some(cache) = &self.namespace_rows
        {
            match &written.namespace_inodes {
                Some(inodes) => cache.invalidate_keys(
                    &inodes
                        .iter()
                        .flat_map(|ino| {
                            [
                                row_cache::RowKey::Paths(*ino),
                                row_cache::RowKey::Aliases(*ino),
                            ]
                        })
                        .collect::<Vec<_>>(),
                ),
                None => cache.invalidate_all(),
            }
        }
    }

    /// Tie the row caches to this mount's lease: they serve rows only while
    /// `validity` proves the exclusive lease is held. With this set, the TTL
    /// is only a memory/working-set bound, not the staleness bound.
    pub fn with_lease_bound_row_cache(
        mut self,
        validity: Arc<super::lifecycle::LeaseValidity>,
    ) -> Self {
        self.lease_validity = Some(validity);
        self
    }

    /// False once the lease can no longer be proven: every cached row is
    /// dropped (and stays bypassed), so no stale upper row is served.
    fn row_caches_authoritative(&self) -> bool {
        let Some(validity) = &self.lease_validity else {
            return true;
        };
        if validity.authoritative() {
            return true;
        }
        if let Some(cache) = &self.upper_rows {
            cache.invalidate_all();
        }
        if let Some(cache) = &self.namespace_rows {
            cache.invalidate_all();
        }
        false
    }

    /// Frozen read fence without a round trip: while the exclusive lease is
    /// proven held, no other writer can change upper rows and every local
    /// commit invalidates the keys it writes (before and after), so the
    /// cached rows of `ino` are exactly the store's rows. Returns the cached
    /// head/base pair and those rows, or None to use the remote fence.
    pub(super) async fn lease_proven_inode_rows(
        &self,
        ino: i64,
    ) -> Result<Option<([LayerRecord; 2], Arc<Vec<InodeDelta>>)>, MetaError> {
        if self.lease_validity.is_none() {
            return Ok(None);
        }
        let Some(cache) = self.upper_row_cache() else {
            return Ok(None);
        };
        let chain: [LayerRecord; 2] = match self.chain().await?.try_into() {
            Ok(chain) => chain,
            Err(_) => return Ok(None),
        };
        let rows = match cache.get(&row_cache::RowKey::Inode(ino)) {
            Some(row_cache::RowValue::Inode(rows)) => rows,
            _ => {
                let ticket = cache.ticket();
                let rows = Arc::new(
                    self.store
                        .get_inode_deltas(InodeQuery {
                            layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
                            ino,
                        })
                        .await
                        .map_err(workspace_to_meta)?,
                );
                cache.insert(
                    ticket,
                    row_cache::RowKey::Inode(ino),
                    row_cache::RowValue::Inode(rows.clone()),
                );
                rows
            }
        };
        // Re-check after the (possibly awaited) load: the proof must still
        // hold when the rows are used as the fence.
        if !self.row_caches_authoritative() {
            return Ok(None);
        }
        Ok(Some((chain, rows)))
    }

    /// A handle that re-checks the lease-proven local fence later (at the end
    /// of a read request) without the meta layer.
    pub(super) fn lease_local_fence(&self) -> Option<row_cache::LeaseLocalFence> {
        let validity = self.lease_validity.clone()?;
        let cache = self.upper_rows.clone()?;
        self.packed_lower()
            .is_some()
            .then_some(row_cache::LeaseLocalFence { cache, validity })
    }

    fn namespace_row_cache(&self) -> Option<&row_cache::UpperRowCache> {
        self.namespace_rows
            .as_ref()
            .filter(|_| self.packed_lower().is_some() && self.row_caches_authoritative())
    }

    fn upper_row_cache(&self) -> Option<&row_cache::UpperRowCache> {
        self.upper_rows
            .as_ref()
            .filter(|_| self.packed_lower().is_some() && self.row_caches_authoritative())
            .map(|cache| cache.as_ref())
    }

    pub fn with_read_plan_cache_max_weight(mut self, max_weight: u64) -> Self {
        self.resolver_cache = WorkspaceResolverCache::with_capacity(max_weight);
        self
    }

    /// Attach an authenticated current PM11 lower and its independent binding
    /// authority. Actual catalog binding persistence/publication must be wired
    /// separately; this constructor cannot infer it from BaseRevision.
    pub fn with_packed_v3_lower<B, S>(
        mut self,
        binding: super::catalog::PackedLowerBinding,
        lower: Arc<super::packed_v3::PackedV3ReadonlyMeta<B>>,
        authority: Arc<dyn WorkspacePackedBindingAuthority>,
        upper: Arc<S>,
        layout: crate::chunk::ChunkLayout,
    ) -> Result<Self, MetaError>
    where
        B: crate::cadapter::client::ObjectBackend + Clone + Send + Sync + 'static,
        S: crate::chunk::BlockStore + Send + Sync + 'static,
    {
        if self.chunk_size != layout.chunk_size {
            return Err(MetaError::Internal(
                "workspace/lower chunk size mismatch".into(),
            ));
        }
        self.lower = WorkspaceLower::PackedV3(WorkspacePackedLower::new(
            binding, lower, authority, upper, layout,
        )?);
        Ok(self)
    }

    /// Attach a packed lower only after loading its current persisted binding.
    /// This is the production entry point: callers cannot supply a detached
    /// manifest identity or a test-only authority in place of catalog state.
    pub async fn with_packed_v3_lower_from_store<B, S>(
        self,
        lower: Arc<super::packed_v3::PackedV3ReadonlyMeta<B>>,
        upper: Arc<S>,
        layout: crate::chunk::ChunkLayout,
    ) -> Result<Self, MetaError>
    where
        B: crate::cadapter::client::ObjectBackend + Clone + Send + Sync + 'static,
        S: crate::chunk::BlockStore + Send + Sync + 'static,
    {
        self.with_packed_v3_lower_from_store_owned(lower, upper, layout, |_| {})
            .await
    }

    pub(crate) async fn with_packed_v3_lower_from_store_owned<B, S, F>(
        self,
        lower: Arc<super::packed_v3::PackedV3ReadonlyMeta<B>>,
        upper: Arc<S>,
        layout: crate::chunk::ChunkLayout,
        retain_reader: F,
    ) -> Result<Self, MetaError>
    where
        B: crate::cadapter::client::ObjectBackend + Clone + Send + Sync + 'static,
        S: crate::chunk::BlockStore + Send + Sync + 'static,
        F: FnOnce(Arc<dyn super::packed_reader_lifecycle::PackedReaderSession>),
    {
        let guard = self.guard().await;
        let reader = self
            .store
            .clone()
            .open_packed_reader_session(
                guard.clone(),
                lower.mount_budget(),
                super::packed_reader_lifecycle::PackedReaderLeaseOptions::default(),
            )
            .await
            .map_err(workspace_to_meta)?;
        retain_reader(reader.clone());
        if !Arc::ptr_eq(&lower.mount_budget(), &reader.mount_budget()) {
            reader.shutdown().await.map_err(workspace_to_meta)?;
            return Err(workspace_to_meta(WorkspaceError::Busy));
        }
        let binding = reader.binding().clone();
        let authority = Arc::new(PinnedCatalogPackedBindingAuthority {
            store: self.store.clone(),
            reader: reader.clone(),
        });
        let result = async {
            let _startup_owner = reader.retain_request().map_err(workspace_to_meta)?;
            authority
                .validate(&guard, &binding)
                .await
                .map_err(workspace_to_meta)?;
            self.with_packed_v3_lower(binding, lower, authority, upper, layout)
        }
        .await;
        if result.is_err() {
            reader.shutdown().await.map_err(workspace_to_meta)?;
        }
        result
    }

    pub fn packed_reader_session(
        &self,
    ) -> Option<Arc<dyn super::packed_reader_lifecycle::PackedReaderSession>> {
        self.packed_lower()
            .and_then(|lower| lower.authority.reader_session())
    }

    /// Diagnostics only: render the packed lower's index cache, budget and
    /// read-observer counters in the mount's `.stats` file.
    pub fn install_packed_lower_stats(&self, stats: &crate::vfs::stats::FsStats) -> bool {
        self.packed_lower()
            .is_some_and(|lower| lower.metadata.install_stats(stats))
    }

    pub(crate) fn packed_lower(&self) -> Option<&Arc<WorkspacePackedLower>> {
        match &self.lower {
            WorkspaceLower::Native => None,
            WorkspaceLower::PackedV3(lower) => Some(lower),
        }
    }

    pub(crate) fn packed_shutdown_budget(
        &self,
    ) -> Option<Arc<super::packed_v3::wire005::V3MountBudget>> {
        self.packed_lower().map(|lower| lower.budget.clone())
    }

    // This is the real original lower transport/reader drain. Only the clean proof
    // owner defers budget closure until the clean release CAS becomes terminal.
    pub(crate) async fn shutdown_packed_runtime_for_clean_release(&self) -> Result<(), MetaError> {
        let lower = self.packed_lower().ok_or_else(|| {
            MetaError::NotSupported("packed original shutdown requires its attached lower".into())
        })?;
        let reader = lower.authority.reader_session();
        if let Some(reader) = &reader {
            reader.stop_admission();
        }
        lower.metadata.drain_packed_transport().await?;
        if let Some(reader) = reader {
            reader.shutdown().await.map_err(workspace_to_meta)?;
        }
        Ok(())
    }

    async fn packed_metadata_fence(
        &self,
    ) -> Result<
        Option<(
            HeadGuard,
            [LayerRecord; 2],
            Option<super::packed_reader_lifecycle::PackedReaderRequestOwner>,
        )>,
        MetaError,
    > {
        let Some(lower) = self.packed_lower() else {
            return Ok(None);
        };
        let _ = lower;
        // Immutable lower: the cached head/base pair is the read view. No
        // reader pin, binding authentication or remote fence per op.
        let guard = self.guard().await;
        let chain = self.chain().await?;
        let layers: [LayerRecord; 2] = chain
            .try_into()
            .map_err(|_| workspace_to_meta(WorkspaceError::Fenced))?;
        Ok(Some((guard, layers, None)))
    }

    /// Fence for multi-step reads (directory pages, xattr lists, paths). They
    /// hold a fresh head record and reader generation across awaits and
    /// re-check the head/lease between steps. The immutable lower's binding
    /// is authenticated once at attach, never per step.
    pub(super) async fn packed_enumeration_fence(
        &self,
    ) -> Result<
        Option<(
            HeadGuard,
            [LayerRecord; 2],
            Option<super::packed_reader_lifecycle::PackedReaderRequestOwner>,
        )>,
        MetaError,
    > {
        let Some(lower) = self.packed_lower() else {
            return Ok(None);
        };
        let reader_owner = lower
            .authority
            .retain_reader_request()
            .map_err(workspace_to_meta)?;
        let guard = self.guard().await;
        let chain = self.chain().await?;
        let layers = [
            self.store
                .load_layer(guard.expected_head_layer_id)
                .await
                .map_err(workspace_to_meta)?,
            chain[1].clone(),
        ];
        if layers[1].layer_id != lower.binding.base_layer_id {
            return Err(workspace_to_meta(WorkspaceError::Fenced));
        }
        validate_fixed_layer_pair(&layers).map_err(workspace_to_meta)?;
        self.store
            .validate_read_fence(guard.clone(), layers.clone())
            .await
            .map_err(workspace_to_meta)?;
        Ok(Some((guard, layers, reader_owner)))
    }

    async fn validate_packed_metadata_fence(
        &self,
        fence: Option<(
            HeadGuard,
            [LayerRecord; 2],
            Option<super::packed_reader_lifecycle::PackedReaderRequestOwner>,
        )>,
    ) -> Result<(), MetaError> {
        if let Some((guard, layers, reader_owner)) = fence {
            let lower = self
                .packed_lower()
                .ok_or_else(|| workspace_to_meta(WorkspaceError::Fenced))?;
            let _ = lower;
            // Single-step reads carry no owner and need no recheck.
            // Multi-step enumeration fences re-check the head/lease.
            if reader_owner.is_some() {
                self.store
                    .validate_read_fence(guard, layers)
                    .await
                    .map_err(workspace_to_meta)?;
            }
        }
        Ok(())
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
        self.invalidate_upper_rows();
        self.directory_hints.invalidate_all();
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

    async fn resolve_inode_delta_state(
        &self,
        ino: i64,
    ) -> Result<Resolution<InodeDelta>, MetaError> {
        let chain = self.chain().await?;
        let cache = self.upper_row_cache();
        let rows = match cache.and_then(|cache| cache.get(&row_cache::RowKey::Inode(ino))) {
            Some(row_cache::RowValue::Inode(rows)) => rows,
            _ => {
                let ticket = cache.map(row_cache::UpperRowCache::ticket);
                let rows = std::sync::Arc::new(
                    self.store
                        .get_inode_deltas(InodeQuery {
                            layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
                            ino,
                        })
                        .await
                        .map_err(workspace_to_meta)?,
                );
                if let (Some(cache), Some(ticket)) = (cache, ticket) {
                    cache.insert(
                        ticket,
                        row_cache::RowKey::Inode(ino),
                        row_cache::RowValue::Inode(rows.clone()),
                    );
                }
                rows
            }
        };
        Ok(resolve_inode_state(&chain, &rows, ino)
            .map_err(workspace_to_meta)?
            .map(|resolved| resolved.inode))
    }

    async fn resolve_inode_delta(&self, ino: i64) -> Result<Option<InodeDelta>, MetaError> {
        if self.packed_lower().is_some() {
            let snapshot = self.cached_permission_snapshot(ino).await?;
            return Ok(resolve_inode(&snapshot.layers, &snapshot.inodes, ino)
                .map_err(workspace_to_meta)?
                .map(|value| value.inode));
        }
        Ok(self.resolve_inode_delta_state(ino).await?.into_option())
    }

    fn validate_write(
        ino: i64,
        chunk_id: u64,
        slice: &SliceDesc,
        new_size: u64,
        chunk_size: u64,
    ) -> Result<(), MetaError> {
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
        if end > chunk_size {
            return Err(MetaError::InvalidPath(
                "slice range exceeds workspace chunk size".into(),
            ));
        }
        let slice_start = chunk_index
            .checked_mul(chunk_size)
            .and_then(|start| start.checked_add(slice.offset))
            .ok_or_else(|| MetaError::InvalidPath("slice start overflows".into()))?;
        if new_size <= slice_start {
            return Err(MetaError::InvalidPath(format!(
                "new size {new_size} does not cover slice start {slice_start}"
            )));
        }
        Ok(())
    }

    /// Commit one batch while owning the mutation gate. Send every reply before
    /// releasing the gate so completed writers observe their receipts before
    /// taking leadership again. The next gate waiter owns the next batch.
    async fn commit_next_write_batch(&self, _mutation_guard: tokio::sync::MutexGuard<'_, ()>) {
        let mut batch = {
            let mut queue = self.write_queue.lock().unwrap();
            if queue.pending.is_empty() {
                return;
            }
            let take = queue.pending.len().min(MAX_GROUP_WRITES);
            queue.pending.drain(..take).collect::<Vec<_>>()
        };
        let result = self.commit_write_batch(&mut batch).await;
        for member in batch {
            let _ = member.reply.send(match &result {
                Ok(()) => Ok(()),
                Err(error) => Err(duplicate_write_error(error)),
            });
        }
    }

    async fn write_target_inode(&self, ino: i64) -> Result<InodeDelta, MetaError> {
        let inode = self
            .resolve_inode_delta(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;
        if file_type_from_code(inode.kind)? != FileType::File {
            return Err(MetaError::NotSupported(
                "workspace data writes require a regular file".into(),
            ));
        }
        Ok(inode)
    }

    /// Resolve each distinct inode once per attempt, with bounded backend
    /// concurrency. Keep the results locally: cache eviction/expiry must not
    /// turn mutation construction into another round of serial snapshots.
    async fn prefetch_write_inodes(
        &self,
        batch: &[PendingWrite],
    ) -> BTreeMap<i64, Result<InodeDelta, MetaError>> {
        let inodes = batch.iter().map(|write| write.ino).collect::<BTreeSet<_>>();
        futures::stream::iter(inodes)
            .map(|ino| async move { (ino, self.write_target_inode(ino).await) })
            .buffer_unordered(MAX_GROUP_INODE_READS)
            .collect()
            .await
    }

    /// Commit every write of `batch` in one versioned mutation: one inode row
    /// per distinct inode (folded in arrival order) and every visible extent
    /// in arrival order, so a later write still wins on overlap.
    async fn commit_write_batch(&self, batch: &mut Vec<PendingWrite>) -> Result<(), MetaError> {
        for _ in 0..64 {
            // Capture the CAS version before fetching rows, and discard all
            // prefetched values on conflict before reading the next version.
            let expected_layers = self.mutation_version().await?;
            let prefetched = self.prefetch_write_inodes(batch).await;
            let mut inodes = BTreeMap::new();
            // A missing/non-file inode fails only its own members, including
            // on retry. Keep the remaining writes in their arrival order.
            for member in std::mem::take(batch) {
                match &prefetched[&member.ino] {
                    Ok(inode) => {
                        inodes.entry(member.ino).or_insert_with(|| inode.clone());
                        batch.push(member);
                    }
                    Err(error) => {
                        let _ = member.reply.send(Err(duplicate_write_error(error)));
                    }
                }
            }
            if batch.is_empty() {
                return Ok(());
            }
            let result = async {
                let mut extents = Vec::with_capacity(batch.len());
                let now = now_ns()?;
                for write in batch.iter() {
                    let inode = inodes.get_mut(&write.ino).expect("validated write inode");
                    let (_, chunk_index) = crate::vfs::extract_ino_and_chunk_index(write.chunk_id);
                    let slice_start = chunk_index * self.chunk_size + write.slice.offset;
                    // Publish only the visible prefix; see the single-write
                    // rationale (aligned tails past EOF stay out of the graph).
                    let visible_length = write.slice.length.min(write.new_size - slice_start);
                    inode.size = inode.size.max(write.new_size);
                    inode.mtime_ns = now;
                    inode.ctime_ns = now;
                    inode.data_version = inode.data_version.saturating_add(1);
                    extents.push(DataExtentDelta::data(
                        inode.layer_id,
                        write.ino,
                        chunk_index,
                        write.slice.offset,
                        visible_length,
                        write.slice.slice_id,
                        0,
                        0,
                    ));
                }
                self.mutate_data_batch(&expected_layers, inodes.into_values().collect(), extents)
                    .await
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
    }

    /// A create just committed the only dentry naming the fresh inode `ino`
    /// (it was allocated by this create, so no other alias exists), so its
    /// complete path set is exactly `parent`'s cached paths joined with
    /// `name`. Seed it so the first permission walk of the new file needs no
    /// reverse-index scan. Only when `parent`'s complete paths are cached
    /// under the lease-bound namespace cache; otherwise nothing is seeded.
    fn seed_created_paths(&self, parent: i64, name: &str, ino: i64) {
        let Some(cache) = self.namespace_row_cache() else {
            return;
        };
        let ticket = cache.ticket();
        let parent_paths = if parent == self.root_ino() {
            Arc::new(vec![b"/".to_vec()])
        } else {
            match cache.get(&row_cache::RowKey::Paths(parent)) {
                Some(row_cache::RowValue::Paths(paths)) => paths,
                _ => return,
            }
        };
        if parent_paths.is_empty() {
            return;
        }
        let paths = parent_paths
            .iter()
            .map(|path| {
                let mut child = path.clone();
                if child.last() != Some(&b'/') {
                    child.push(b'/');
                }
                child.extend_from_slice(name.as_bytes());
                child
            })
            .collect::<Vec<_>>();
        cache.insert(
            ticket,
            row_cache::RowKey::Paths(ino),
            row_cache::RowValue::Paths(Arc::new(paths)),
        );
    }

    /// One inode's permission snapshot. While the exclusive lease is proven
    /// held the lease-bound row cache answers it: this mount is the only
    /// upper writer and every commit drops the `Permission` key of each inode
    /// it writes (before and after). A mutation built on it still CASes the
    /// head version it read, so a stale entry can only fail that CAS.
    async fn cached_permission_snapshot(&self, ino: i64) -> Result<PermissionSnapshot, MetaError> {
        let lease_bound = self.lease_validity.is_some() && self.row_caches_authoritative();
        let cache = self.upper_row_cache().filter(|_| lease_bound);
        let key = row_cache::RowKey::Permission(ino);
        if let Some(row_cache::RowValue::Permission(snapshot)) =
            cache.and_then(|cache| cache.get(&key))
        {
            return Ok((*snapshot).clone());
        }
        let ticket = cache.map(row_cache::UpperRowCache::ticket);
        let snapshot = self.permission_snapshot(vec![ino], None).await?;
        if let (Some(cache), Some(ticket)) = (cache, ticket) {
            cache.insert(
                ticket,
                key,
                row_cache::RowValue::Permission(Arc::new(snapshot.clone())),
            );
        }
        Ok(snapshot)
    }

    async fn resolve_dentry_entry(
        &self,
        parent: i64,
        name: &[u8],
    ) -> Result<Option<ResolvedDentry>, MetaError> {
        let fence = self.packed_metadata_fence().await?;
        let chain = self.chain().await?;
        let cache = self.upper_row_cache();
        let key = row_cache::RowKey::Dentry(parent, name.to_vec());
        let rows = match cache.and_then(|cache| cache.get(&key)) {
            Some(row_cache::RowValue::Dentry(rows)) => rows,
            _ => {
                let ticket = cache.map(row_cache::UpperRowCache::ticket);
                let rows = std::sync::Arc::new(
                    self.store
                        .get_dentry_deltas(DentryQuery {
                            layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
                            parent_ino: parent,
                            name: Some(name.to_vec()),
                        })
                        .await
                        .map_err(workspace_to_meta)?,
                );
                if let (Some(cache), Some(ticket)) = (cache, ticket) {
                    cache.insert(ticket, key, row_cache::RowValue::Dentry(rows.clone()));
                }
                rows
            }
        };
        let result =
            match resolve_dentry_state(&chain, &rows, parent, name).map_err(workspace_to_meta)? {
                Resolution::Present(entry) => Ok(Some(entry)),
                Resolution::Masked => Ok(None),
                Resolution::Absent => {
                    let Some(lower) = self.packed_lower() else {
                        return Ok(None);
                    };
                    let entry = lower.metadata.lookup_with_attr_bytes(parent, name).await?;
                    Ok(entry.map(|(ino, attr)| ResolvedDentry {
                        layer_id: lower.binding.base_layer_id,
                        parent_ino: parent,
                        name: name.to_vec(),
                        ino,
                        entry_type: file_type_code(attr.kind),
                        sequence: 0,
                    }))
                }
            };
        if result.is_ok() {
            self.validate_packed_metadata_fence(fence).await?;
        }
        result
    }

    async fn directory_is_descendant_of(
        &self,
        mut directory: i64,
        ancestor: i64,
    ) -> Result<bool, MetaError> {
        let root = self.root_ino.load(Ordering::Acquire);
        let mut visited = BTreeSet::new();
        loop {
            if directory == ancestor {
                return Ok(true);
            }
            if directory == root {
                return Ok(false);
            }
            if !visited.insert(directory) {
                return Err(MetaError::InvalidPath(format!(
                    "directory ancestry contains a cycle at inode {directory}"
                )));
            }
            let inode = self
                .resolve_inode_delta(directory)
                .await?
                .ok_or(MetaError::NotFound(directory))?;
            if file_type_from_code(inode.kind)? != FileType::Dir {
                return Err(MetaError::NotDirectory(directory));
            }
            match inode.parent_hint {
                Some(parent) if parent != directory => directory = parent,
                _ => return Ok(false),
            }
        }
    }

    async fn permission_snapshot(
        &self,
        inodes: Vec<i64>,
        dentry: Option<(i64, Vec<u8>)>,
    ) -> Result<PermissionSnapshot, MetaError> {
        let snapshot = if let Some(lower) = self.packed_lower() {
            self.merged_packed_permission_snapshot(lower, inodes.clone(), dentry)
                .await?
        } else {
            let chain = self.chain().await?;
            self.store
                .read_permission_snapshot(PermissionSnapshotQuery {
                    layer_ids: [chain[0].layer_id, chain[1].layer_id],
                    inodes: inodes.clone(),
                    dentry,
                })
                .await
                .map_err(workspace_to_meta)?
        };
        // Retain only bounded topology hints across permission-cache eviction.
        // A stale hint may fetch an unnecessary row but cannot authorize a walk.
        for ino in inodes {
            // File data/permission reads need no topology-cache bookkeeping.
            if !snapshot
                .inodes
                .iter()
                .any(|row| row.ino == ino && row.kind == file_type_code(FileType::Dir))
            {
                continue;
            }
            if let Ok(Some(resolved)) = resolve_inode(&snapshot.layers, &snapshot.inodes, ino)
                && resolved.inode.kind == file_type_code(FileType::Dir)
                && let Some(parent) = resolved.inode.parent_hint
                && parent > 0
            {
                self.directory_hints.insert(ino, parent);
            } else {
                self.directory_hints.invalidate(&ino);
            }
        }
        Ok(snapshot)
    }

    async fn mutation_version(&self) -> Result<MutationVersion, MetaError> {
        self.retain_mutation_version().await
    }

    async fn check_namespace_actor(
        &self,
        expected: &[LayerRecord; 2],
        parent: i64,
        child: Option<i64>,
    ) -> Result<(), MetaError> {
        self.check_namespace_actor_with_parent(expected, parent, child, None)
            .await
    }

    async fn check_namespace_actor_with_parent(
        &self,
        expected: &[LayerRecord; 2],
        parent: i64,
        child: Option<i64>,
        parent_permission: Option<&PermissionInode>,
    ) -> Result<(), MetaError> {
        let Some(actor) = crate::meta::layer::namespace_actor() else {
            return Ok(());
        };
        if actor.uid == 0 {
            return Ok(());
        }
        let mut read_ahead = None;
        let loaded;
        let (directory, access, _, control) = match parent_permission {
            Some(permission) => permission,
            None => {
                loaded = self
                    .permission_inode_for_check(expected, parent, Some(&mut read_ahead))
                    .await?;
                &loaded
            }
        };
        if directory.kind != file_type_code(FileType::Dir) {
            return Err(MetaError::NotDirectory(parent));
        }
        let allowed = Self::inode_allows_actor(
            directory,
            access.as_deref(),
            control.as_deref(),
            actor.uid,
            &actor.groups,
            3,
        )?;
        if !allowed {
            return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                libc::EACCES,
            )));
        }
        // Parent search permission is part of this same conditional version,
        // including an ancestor whose ACL changed after the FUSE precheck.
        let root = self.root_ino();
        if parent != root {
            let mut ancestor = directory.parent_hint;
            let mut visited = BTreeSet::from([parent]);
            loop {
                let ino = ancestor
                    .ok_or_else(|| MetaError::Io(std::io::Error::from_raw_os_error(libc::EIO)))?;
                if !visited.insert(ino) {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(libc::EIO)));
                }
                let (inode, access, _, control) = self
                    .permission_inode_for_check(expected, ino, Some(&mut read_ahead))
                    .await?;
                if inode.kind != file_type_code(FileType::Dir) {
                    return Err(MetaError::NotDirectory(ino));
                }
                if !Self::inode_allows_actor(
                    &inode,
                    access.as_deref(),
                    control.as_deref(),
                    actor.uid,
                    &actor.groups,
                    1,
                )? {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::EACCES,
                    )));
                }
                if ino == root {
                    break;
                }
                ancestor = inode.parent_hint;
            }
        }
        if directory.mode & 0o1000 != 0
            && actor.uid != directory.uid
            && let Some(child) = child
        {
            let (inode, _, _, _) = self
                .permission_inode_for_check(expected, child, None)
                .await?;
            if actor.uid != inode.uid {
                return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                    libc::EPERM,
                )));
            }
        }
        Ok(())
    }

    fn inode_allows_actor(
        inode: &InodeDelta,
        access: Option<&[u8]>,
        control: Option<&[u8]>,
        uid: u32,
        groups: &[u32],
        requested: u32,
    ) -> Result<bool, MetaError> {
        if uid == 0 {
            return Ok(requested & 1 == 0
                || inode.kind == file_type_code(FileType::Dir)
                || inode.mode & 0o111 != 0);
        }
        if let Some(bytes) = access {
            let acl = PosixAcl::decode(bytes)
                .map_err(|_| MetaError::Io(std::io::Error::from_raw_os_error(libc::EIO)))?;
            if acl.mode_bits() != inode.mode & 0o777 {
                return Err(MetaError::Io(std::io::Error::from_raw_os_error(libc::EIO)));
            }
            return Ok(acl.allows_access(inode.uid, inode.gid, uid, groups, requested));
        }
        if let Some(bytes) = control {
            let entries: Vec<crate::control::protocol::ControlAclEntry> =
                serde_json::from_slice(bytes)
                    .map_err(|_| MetaError::Io(std::io::Error::from_raw_os_error(libc::EIO)))?;
            crate::control::protocol::validate_acl_entries(&entries)
                .map_err(|_| MetaError::Io(std::io::Error::from_raw_os_error(libc::EIO)))?;
            if let Some(allowed) = crate::meta::posix_acl::control_acl_allows_access(
                &entries, inode.uid, inode.gid, uid, groups, requested,
            ) {
                return Ok(allowed);
            }
        }
        let shift = if uid == inode.uid {
            6
        } else if groups.contains(&inode.gid) {
            3
        } else {
            0
        };
        Ok((inode.mode >> shift) & requested == requested)
    }

    fn cached_permission_for_check(
        &self,
        expected: &[LayerRecord; 2],
        ino: i64,
    ) -> Option<Arc<PermissionSnapshot>> {
        if self.lease_validity.is_some()
            && let Some(cache) = self.upper_row_cache()
            && let Some(row_cache::RowValue::Permission(snapshot)) =
                cache.get(&row_cache::RowKey::Permission(ino))
            && snapshot.layers[0].layer_id == expected[0].layer_id
            && snapshot.layers[1].layer_id == expected[1].layer_id
            && self.row_caches_authoritative()
        {
            Some(snapshot)
        } else {
            None
        }
    }

    /// For a read-only permission decision inside a conditional mutation.
    /// While the exclusive lease is proven held, the cached permission rows
    /// of `ino` are exactly the store's (every local commit invalidates the
    /// rows it writes), so they answer the decision without a round trip;
    /// the mutation's own CAS still checks its layer version. Otherwise this
    /// is `permission_inode`. Never use it for rows the commit writes back.
    async fn permission_inode_for_check(
        &self,
        expected: &[LayerRecord; 2],
        ino: i64,
        mut read_ahead: Option<&mut Option<PermissionReadAhead>>,
    ) -> Result<PermissionInode, MetaError> {
        let root = self.root_ino();
        if let Some(slot) = read_ahead.as_deref_mut() {
            if let Some(next) = slot.take_if(|next| next.ino == ino) {
                return next.permission;
            }
            // A rename may invalidate the predicted next parent. Root is the
            // one known endpoint and can be retained until the walk reaches it.
            if slot.as_ref().is_some_and(|next| next.ino != root) {
                *slot = None;
            }
        }
        if let Some(snapshot) = self.cached_permission_for_check(expected, ino) {
            return Self::decode_permission_inode(&snapshot, ino);
        }
        let mut companion = (ino != root && read_ahead.as_ref().is_some_and(|slot| slot.is_none()))
            .then(|| {
                self.directory_hints
                    .get(&ino)
                    .filter(|parent| *parent > 0 && *parent != ino)
                    .unwrap_or(root)
            })
            .filter(|parent| {
                self.cached_permission_for_check(expected, *parent)
                    .is_none()
            });
        let mut inodes = vec![ino];
        inodes.extend(companion);
        let ticket = self.upper_row_cache().map(row_cache::UpperRowCache::ticket);
        let snapshot = match self.permission_snapshot(inodes, None).await {
            // Corrupt/unavailable speculative rows must not fail an otherwise
            // valid operation. Read the required inode alone on this path.
            Err(_) if companion.is_some() => {
                companion = None;
                self.permission_snapshot(vec![ino], None).await?
            }
            result => result?,
        };
        if &snapshot.layers != expected {
            return Err(workspace_to_meta(WorkspaceError::Busy));
        }
        let permission = Self::decode_permission_inode(&snapshot, ino);
        let next = companion.map(|ino| PermissionReadAhead {
            ino,
            permission: Self::decode_permission_inode(&snapshot, ino),
        });
        if self.lease_validity.is_some()
            && let (Some(cache), Some(ticket)) = (self.upper_row_cache(), ticket)
        {
            let snapshot = Arc::new(snapshot);
            cache.insert(
                ticket,
                row_cache::RowKey::Permission(ino),
                row_cache::RowValue::Permission(snapshot.clone()),
            );
            if let Some(companion) = companion {
                cache.insert(
                    ticket,
                    row_cache::RowKey::Permission(companion),
                    row_cache::RowValue::Permission(snapshot),
                );
            }
        }
        if let Some(slot) = read_ahead
            && next.is_some()
        {
            *slot = next;
        }
        permission
    }

    async fn permission_inode(
        &self,
        expected: &[LayerRecord; 2],
        ino: i64,
    ) -> Result<
        (
            InodeDelta,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
        ),
        MetaError,
    > {
        let snapshot = self.permission_snapshot(vec![ino], None).await?;
        if &snapshot.layers != expected {
            return Err(workspace_to_meta(WorkspaceError::Busy));
        }
        Self::decode_permission_inode(&snapshot, ino)
    }

    fn decode_permission_inode(
        snapshot: &PermissionSnapshot,
        ino: i64,
    ) -> Result<PermissionInode, MetaError> {
        let inode = resolve_inode(&snapshot.layers, &snapshot.inodes, ino)
            .map_err(workspace_to_meta)?
            .ok_or(MetaError::NotFound(ino))?
            .inode;
        let value = |name: &[u8]| -> Result<Option<Vec<u8>>, MetaError> {
            Ok(resolve_xattr(&snapshot.layers, &snapshot.xattrs, ino, name)
                .map_err(workspace_to_meta)?
                .map(|resolved| resolved.value))
        };
        let access = value(ACCESS_XATTR)?;
        if let Some(bytes) = &access {
            let acl = PosixAcl::decode(bytes)
                .map_err(|_| MetaError::Io(std::io::Error::from_raw_os_error(libc::EIO)))?;
            if acl.mode_bits() != inode.mode & 0o777 {
                return Err(MetaError::Io(std::io::Error::from_raw_os_error(libc::EIO)));
            }
        }
        Ok((
            inode,
            access,
            value(DEFAULT_XATTR)?,
            value(b"system.brewfs.acl")?,
        ))
    }

    async fn mutate_inode(
        &self,
        expected: &[LayerRecord; 2],
        mut inode: InodeDelta,
        xattrs: Vec<XattrDelta>,
    ) -> Result<InodeDelta, MetaError> {
        let guard = self.guard().await;
        inode.layer_id = guard.expected_head_layer_id;
        inode.sequence = 0;
        self.commit_versioned_mutation(VersionedMutation {
            inodes: vec![inode.clone()],
            xattrs,
            ..VersionedMutation::empty(guard, expected.clone(), self.chunk_size)
        })
        .await
        .map_err(|error| self.mutation_error(error))?;
        self.resolver_cache
            .invalidate_inode(self.view.read().await.workspace_id, inode.ino);
        Ok(inode)
    }

    async fn mutate_data(
        &self,
        expected: &[LayerRecord; 2],
        mut inode: InodeDelta,
        mut extents: Vec<DataExtentDelta>,
        xattrs: Vec<XattrDelta>,
    ) -> Result<InodeDelta, MetaError> {
        let guard = self.guard().await;
        inode.layer_id = guard.expected_head_layer_id;
        inode.sequence = 0;
        for extent in &mut extents {
            extent.layer_id = guard.expected_head_layer_id;
            extent.ino = inode.ino;
            extent.sequence = 0;
        }
        let private_bytes = extents
            .iter()
            .filter_map(|extent| {
                matches!(extent.kind, super::model::ExtentKind::Data { .. })
                    .then_some(extent.length)
            })
            .fold(0u64, u64::saturating_add);
        self.commit_versioned_mutation(VersionedMutation {
            inodes: vec![inode.clone()],
            extents,
            xattrs,
            ..VersionedMutation::empty(guard.clone(), expected.clone(), self.chunk_size)
        })
        .await
        .map_err(|error| self.mutation_error(error))?;
        self.metrics.add_private_bytes_written(private_bytes);
        self.resolver_cache
            .invalidate_inode(guard.workspace_id, inode.ino);
        Ok(inode)
    }

    async fn mutate_data_batch(
        &self,
        expected: &[LayerRecord; 2],
        mut inodes: Vec<InodeDelta>,
        mut extents: Vec<DataExtentDelta>,
    ) -> Result<(), MetaError> {
        let guard = self.guard().await;
        for inode in &mut inodes {
            inode.layer_id = guard.expected_head_layer_id;
            inode.sequence = 0;
        }
        for extent in &mut extents {
            extent.layer_id = guard.expected_head_layer_id;
            extent.sequence = 0;
        }
        let private_bytes = extents
            .iter()
            .filter_map(|extent| {
                matches!(extent.kind, super::model::ExtentKind::Data { .. })
                    .then_some(extent.length)
            })
            .fold(0u64, u64::saturating_add);
        let touched = inodes.iter().map(|inode| inode.ino).collect::<Vec<_>>();
        self.commit_versioned_mutation(VersionedMutation {
            inodes,
            extents,
            ..VersionedMutation::empty(guard.clone(), expected.clone(), self.chunk_size)
        })
        .await
        .map_err(|error| self.mutation_error(error))?;
        self.metrics.add_private_bytes_written(private_bytes);
        for ino in touched {
            self.resolver_cache
                .invalidate_inode(guard.workspace_id, ino);
        }
        Ok(())
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
    async fn set_attr_checked(
        &self,
        ino: i64,
        req: &SetAttrRequest,
        flags: SetAttrFlags,
        actor: Option<(u32, &[u32])>,
    ) -> Result<FileAttr, MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        for _ in 0..64 {
            let expected_layers = self.mutation_version().await?;
            let result = async {
                let (mut inode, access, _, control) =
                    self.permission_inode(&expected_layers, ino).await?;
                let mut xattrs = Vec::new();
                let write_handle = crate::meta::layer::setattr_write_handle_authorizes(ino)
                    && req.size.is_some()
                    && req.mode.is_none()
                    && req.uid.is_none()
                    && req.gid.is_none()
                    && req.flags.is_none();
                if let Some((uid, groups)) = actor
                    && uid != 0
                {
                    if (req.mode.is_some() || req.uid.is_some() || req.gid.is_some())
                        && uid != inode.uid
                        || req.uid.is_some_and(|new_uid| new_uid != inode.uid)
                        || req.gid.is_some_and(|new_gid| {
                            new_gid != inode.gid && !groups.contains(&new_gid)
                        })
                    {
                        return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                            libc::EPERM,
                        )));
                    }
                    let allowed_write = || {
                        Self::inode_allows_actor(
                            &inode,
                            access.as_deref(),
                            control.as_deref(),
                            uid,
                            groups,
                            crate::meta::layer::open_access_mask().unwrap_or(2),
                        )
                    };
                    if req.size.is_some() && !write_handle && !allowed_write()? {
                        return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                            libc::EACCES,
                        )));
                    }
                    let user_timestamps = req.atime.is_some()
                        || req.mtime.is_some()
                        || flags
                            .intersects(SetAttrFlags::SET_ATIME_NOW | SetAttrFlags::SET_MTIME_NOW);
                    if user_timestamps
                        && req.size.is_none()
                        && req.mode.is_none()
                        && req.uid.is_none()
                        && req.gid.is_none()
                        && uid != inode.uid
                    {
                        if !req.timestamps_use_write_permission(&flags) {
                            return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                                libc::EPERM,
                            )));
                        }
                        if !Self::inode_allows_actor(
                            &inode,
                            access.as_deref(),
                            control.as_deref(),
                            uid,
                            groups,
                            2,
                        )? {
                            return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                                libc::EACCES,
                            )));
                        }
                    }
                    // ctime is kernel-derived, never a utimens user value.
                    // Linux chown(-1,-1) may update ctime for a nonowner even
                    // on a read-only inode. atime/mtime authorization is above.
                }
                let old_size = inode.size;
                if let Some(mode) = req.mode {
                    inode.mode = mode & 0o7777;
                    if let Some((uid, groups)) = actor
                        && uid != 0
                        && !groups.contains(&req.gid.unwrap_or(inode.gid))
                    {
                        inode.mode &= !0o2000;
                    }
                    if let Some(access) = access {
                        let acl = PosixAcl::decode(&access)
                            .map_err(|_| {
                                MetaError::Io(std::io::Error::from_raw_os_error(libc::EIO))
                            })?
                            .chmod(mode);
                        xattrs.push(XattrDelta {
                            layer_id: expected_layers[0].layer_id,
                            ino,
                            name: ACCESS_XATTR.to_vec(),
                            op: if acl.is_extended() {
                                ValueOp::Put
                            } else {
                                ValueOp::Whiteout
                            },
                            value: acl.is_extended().then(|| acl.encode()),
                            sequence: 0,
                        });
                    }
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
                let timestamp_now = now_ns()?;
                if flags.contains(SetAttrFlags::SET_ATIME_NOW) {
                    inode.atime_ns = timestamp_now;
                } else if let Some(atime) = req.atime {
                    inode.atime_ns = atime;
                }
                if flags.contains(SetAttrFlags::SET_MTIME_NOW) {
                    inode.mtime_ns = timestamp_now;
                } else if let Some(mtime) = req.mtime {
                    inode.mtime_ns = mtime;
                } else if req.size.is_some() {
                    // FUSE truncate/ftruncate can send only the new size.
                    // Keep explicit timestamps authoritative for combined SETATTRs.
                    inode.mtime_ns = timestamp_now;
                }
                if let Some(ctime) = req.ctime {
                    inode.ctime_ns = ctime;
                } else {
                    inode.ctime_ns = now_ns()?;
                }
                // chown clears privilege bits even for unchanged explicit IDs
                // and root. Mandatory-locking SGID (no group execute) survives.
                if (req.uid.is_some() || req.gid.is_some())
                    && inode.kind != file_type_code(FileType::Dir)
                {
                    inode.mode &= !0o4000;
                    if inode.mode & 0o010 != 0 {
                        inode.mode &= !0o2000;
                    }
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
                    self.mutate_data(&expected_layers, inode, extents, xattrs)
                        .await?
                } else {
                    self.mutate_inode(&expected_layers, inode, xattrs).await?
                };
                file_attr(&inode)
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_entry(
        &self,
        parent: i64,
        name: String,
        kind: FileType,
        mode: u32,
        umask: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
        symlink_target: Option<Vec<u8>>,
    ) -> Result<(i64, FileAttr), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        for _ in 0..64 {
            let expected_layers = self.mutation_version().await?;
            let result = async {
                validate_name(&name)?;
                // A writeback template must come from this exact mutation
                // version, never the lease-cached permission decision. Reuse
                // that fresh snapshot for authorization too.
                let parent_permission = self.permission_inode(&expected_layers, parent).await?;
                self.check_namespace_actor_with_parent(
                    &expected_layers,
                    parent,
                    None,
                    Some(&parent_permission),
                )
                .await?;
                let (mut parent_inode, _, parent_default, _) = parent_permission;
                let mut creation_mode = mode & 0o7777;
                let request_actor = crate::meta::layer::namespace_actor();
                let creation_uid = request_actor.as_ref().map_or(uid, |actor| actor.uid);
                let mut creation_gid = request_actor.as_ref().map_or(gid, |actor| actor.gid);
                let mut inherited = None;
                if kind != FileType::Symlink {
                    if let Some(bytes) = &parent_default {
                        let default = PosixAcl::decode(bytes).map_err(|_| {
                            MetaError::Io(std::io::Error::from_raw_os_error(libc::EIO))
                        })?;
                        let access = default.inherited_access(mode);
                        creation_mode = (creation_mode & !0o777) | access.mode_bits();
                        if access.is_extended() {
                            inherited = Some(access.encode());
                        }
                    } else {
                        creation_mode &= !(umask & 0o777);
                    }
                }
                if parent_inode.mode & 0o2000 != 0 {
                    creation_gid = parent_inode.gid;
                    if kind == FileType::Dir {
                        creation_mode |= 0o2000;
                    }
                }
                if let Some(actor) = &request_actor
                    && actor.uid != 0
                    && kind != FileType::Dir
                    && !actor.groups.contains(&creation_gid)
                {
                    creation_mode &= !0o2000;
                }
                if file_type_from_code(parent_inode.kind)? != FileType::Dir {
                    return Err(MetaError::NotDirectory(parent));
                }
                if self
                    .resolve_dentry_entry(parent, name.as_bytes())
                    .await?
                    .is_some()
                {
                    return Err(MetaError::AlreadyExists {
                        parent,
                        name: name.clone(),
                    });
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
                    mode: creation_mode,
                    uid: creation_uid,
                    gid: creation_gid,
                    rdev,
                    nlink: if kind == FileType::Dir { 2 } else { 1 },
                    atime_ns: now,
                    mtime_ns: now,
                    ctime_ns: now,
                    symlink_target: symlink_target.clone(),
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
                let mut xattrs = Vec::new();
                for (name, value) in [
                    (ACCESS_XATTR, inherited),
                    (
                        DEFAULT_XATTR,
                        if kind == FileType::Dir {
                            parent_default
                        } else {
                            None
                        },
                    ),
                ] {
                    if let Some(value) = value {
                        xattrs.push(XattrDelta {
                            layer_id: inode.layer_id,
                            ino,
                            name: name.to_vec(),
                            op: ValueOp::Put,
                            value: Some(value),
                            sequence: 0,
                        });
                    }
                }
                self.commit_created_mutation(
                    VersionedMutation {
                        expected_layers: expected_layers.clone(),
                        xattrs,
                        acls: Vec::new(),
                        extents: Vec::new(),
                        chunk_size: self.chunk_size,
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
                    },
                    ino,
                )
                .await
                .map_err(workspace_to_meta)?;
                self.seed_created_paths(parent, &name, ino);
                Ok((ino, file_attr(&inode)?))
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
    }

    async fn remove_entry(
        &self,
        parent: i64,
        name: &str,
        directory: bool,
    ) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        for _ in 0..64 {
            let expected_layers = self.mutation_version().await?;
            let result = async {
                validate_name(name)?;
                let entry = self
                    .resolve_dentry_entry(parent, name.as_bytes())
                    .await?
                    .ok_or(MetaError::NotFound(parent))?;
                self.check_namespace_actor(&expected_layers, parent, Some(entry.ino))
                    .await?;
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
                if directory && !self.directory_is_empty(entry.ino).await? {
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
                self.commit_namespace_mutation(
                    VersionedMutation {
                        expected_layers: expected_layers.clone(),
                        xattrs: Vec::new(),
                        acls: Vec::new(),
                        extents: Vec::new(),
                        chunk_size: self.chunk_size,
                        guard,
                        dentries: vec![DentryDelta::whiteout(
                            parent_inode.layer_id,
                            parent,
                            name.as_bytes().to_vec(),
                            0,
                        )],
                        inodes: vec![inode, parent_inode],
                    },
                    (!directory).then(|| vec![entry.ino]),
                )
                .await
                .map_err(workspace_to_meta)?;
                Ok(())
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
    }
}

#[async_trait]
impl<W: WorkspaceStore + 'static> MetaLayer for WorkspaceMetaLayer<W> {
    fn reserve_memory(
        &self,
        kind: crate::meta::layer::MetadataMemoryKind,
        bytes: u64,
    ) -> Result<Option<crate::meta::layer::MetadataMemoryGuard>, MetaError> {
        match self.packed_lower() {
            Some(lower) => lower.metadata.reserve_memory(kind, bytes),
            None => Ok(None),
        }
    }

    async fn update_posix_acl(
        &self,
        ino: i64,
        name: &str,
        value: Option<&[u8]>,
        uid: u32,
        groups: &[u32],
    ) -> Result<(), MetaError> {
        self.update_posix_acl_with_flags(ino, name, value, 0, uid, groups)
            .await
    }

    async fn update_posix_acl_with_flags(
        &self,
        ino: i64,
        name: &str,
        value: Option<&[u8]>,
        flags: u32,
        uid: u32,
        groups: &[u32],
    ) -> Result<(), MetaError> {
        if name.as_bytes() != ACCESS_XATTR && name.as_bytes() != DEFAULT_XATTR {
            return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                libc::EINVAL,
            )));
        }
        if flags & !(libc::XATTR_CREATE as u32 | libc::XATTR_REPLACE as u32) != 0
            || flags & libc::XATTR_CREATE as u32 != 0 && flags & libc::XATTR_REPLACE as u32 != 0
        {
            return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                libc::EINVAL,
            )));
        }
        let acl = match value {
            None | Some([]) => None,
            Some(bytes) if bytes == 2u32.to_le_bytes() => None,
            Some(bytes) => Some(
                PosixAcl::decode(bytes)
                    .map_err(|_| MetaError::Io(std::io::Error::from_raw_os_error(libc::EINVAL)))?,
            ),
        };
        let _local_gate = self.mutation_gate.lock().await;
        for _ in 0..64 {
            let expected = self.mutation_version().await?;
            let result = async {
                let (mut inode, access, default, _) = self.permission_inode(&expected, ino).await?;
                if inode.kind == file_type_code(FileType::Symlink) {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::EOPNOTSUPP,
                    )));
                }
                if uid != 0 && uid != inode.uid {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::EPERM,
                    )));
                }
                let existing = if name.as_bytes() == ACCESS_XATTR {
                    access.is_some()
                } else {
                    default.is_some()
                };
                if flags & libc::XATTR_CREATE as u32 != 0 && existing {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::EEXIST,
                    )));
                }
                if flags & libc::XATTR_REPLACE as u32 != 0 && !existing {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::ENODATA,
                    )));
                }
                if name.as_bytes() == DEFAULT_XATTR
                    && acl.is_some()
                    && inode.kind != file_type_code(FileType::Dir)
                {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::EACCES,
                    )));
                }
                let mut stored = acl.as_ref().map(PosixAcl::encode);
                if name.as_bytes() == ACCESS_XATTR
                    && let Some(acl) = &acl
                {
                    inode.mode = (inode.mode & !0o777) | acl.mode_bits();
                    if uid != 0 && !groups.contains(&inode.gid) {
                        inode.mode &= !0o2000;
                    }
                    if !acl.is_extended() {
                        stored = None;
                    }
                }
                inode.ctime_ns = now_ns()?;
                let xattr = XattrDelta {
                    layer_id: expected[0].layer_id,
                    ino,
                    name: name.as_bytes().to_vec(),
                    op: if stored.is_some() {
                        ValueOp::Put
                    } else {
                        ValueOp::Whiteout
                    },
                    value: stored,
                    sequence: 0,
                };
                self.mutate_inode(&expected, inode, vec![xattr])
                    .await
                    .map(|_| ())
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
    }

    fn permission_snapshot_answers_access(&self) -> bool {
        self.packed_lower().is_some()
            && self.posix_acl_capability() == PosixAclCapability::ReadWrite
    }

    fn kernel_timestamp_permissions_authoritative(&self) -> bool {
        self.lease_validity
            .as_ref()
            .is_some_and(|validity| validity.authoritative())
    }

    fn posix_acl_capability(&self) -> PosixAclCapability {
        if self.store.supports_versioned_permissions()
            && (self.packed_lower().is_none() || self.store.supports_packed_permissions())
        {
            PosixAclCapability::ReadWrite
        } else {
            PosixAclCapability::Unsupported
        }
    }

    async fn inode_permissions(&self, ino: i64) -> Result<Option<InodePermissions>, MetaError> {
        // Read-only access decision: may use the cached snapshot. Mutations
        // take their own fresh permission snapshot and CAS on its layers.
        let cache = self.upper_row_cache();
        let key = row_cache::RowKey::Permission(ino);
        let snapshot = match cache.and_then(|cache| cache.get(&key)) {
            Some(row_cache::RowValue::Permission(snapshot)) => (*snapshot).clone(),
            _ => {
                let ticket = cache.map(row_cache::UpperRowCache::ticket);
                let snapshot = self.permission_snapshot(vec![ino], None).await?;
                if let (Some(cache), Some(ticket)) = (cache, ticket) {
                    cache.insert(
                        ticket,
                        key,
                        row_cache::RowValue::Permission(std::sync::Arc::new(snapshot.clone())),
                    );
                }
                snapshot
            }
        };
        let Some(inode) =
            resolve_inode(&snapshot.layers, &snapshot.inodes, ino).map_err(workspace_to_meta)?
        else {
            return Ok(None);
        };
        let value = |name: &[u8]| -> Result<Option<Vec<u8>>, MetaError> {
            Ok(resolve_xattr(&snapshot.layers, &snapshot.xattrs, ino, name)
                .map_err(workspace_to_meta)?
                .map(|resolved| resolved.value))
        };
        let attr = file_attr(&inode.inode)?;
        let access_acl = value(ACCESS_XATTR)?;
        if let Some(bytes) = &access_acl {
            let acl = PosixAcl::decode(bytes)
                .map_err(|_| MetaError::Io(std::io::Error::from_raw_os_error(libc::EIO)))?;
            if acl.mode_bits() != attr.mode & 0o777 {
                return Err(MetaError::Io(std::io::Error::from_raw_os_error(libc::EIO)));
            }
        }
        Ok(Some(InodePermissions {
            attr,
            access_acl,
            control_acl: value(b"system.brewfs.acl")?,
        }))
    }

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
        let (ino, attr) = self
            .create_entry(parent, name, kind, mode, umask, uid, gid, rdev, None)
            .await?;
        Ok(CreateEntryResult {
            ino,
            attr: Some(attr),
        })
    }

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
        if let Some(lower) = self.packed_lower() {
            let chain = self.chain().await?;
            if lower.binding.base_layer_id != chain[1].layer_id
                || lower.metadata.root_ino() != self.root_ino()
            {
                return Err(MetaError::Internal(
                    "packed binding base/root mismatch".into(),
                ));
            }
        }
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
        let fence = self.packed_metadata_fence().await?;
        let result = match self.resolve_inode_delta_state(ino).await? {
            Resolution::Present(inode) => file_attr(&inode).map(Some),
            Resolution::Masked => Ok(None),
            Resolution::Absent => {
                let Some(lower) = self.packed_lower() else {
                    return Ok(None);
                };
                let attr = lower.metadata.stat_fresh(ino).await?;
                Ok(attr)
            }
        };
        if result.is_ok() {
            self.validate_packed_metadata_fence(fence).await?;
        }
        result
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
        let actor = crate::meta::layer::namespace_actor();
        let created = actor.is_some() && crate::meta::layer::take_created_inode_open_authority(ino);
        // A frozen-base read-only open changes no state, so it does not pay
        // an empty authenticated CAS; later write opens still do.
        // The creator's own first open of an inode its create just committed
        // (frozen mode) needs no second authenticated CAS: the create commit
        // already authenticated binding, head, lease and writer authority at
        // the same head version, and this mount is the only upper writer.
        // Any later writable open still commits one.
        let open_commits = self
            .packed_lower()
            .is_some_and(|_| (_write || _append) && !created);
        for _ in 0..64 {
            let prepared = async {
                let version = if self.packed_lower().is_some() && open_commits {
                    Some(self.mutation_version().await?)
                } else {
                    None
                };
                let snapshot = match &version {
                    // A commit follows: it CASes this exact head version.
                    Some(_) => self.permission_snapshot(vec![ino], None).await?,
                    // Decision only (no commit): the lease-bound cache.
                    None => self.cached_permission_snapshot(ino).await?,
                };
                Ok::<_, MetaError>((version, snapshot))
            }
            .await;
            let (version, snapshot) = match prepared {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                result => result?,
            };
            if let Some(version) = &version
                && **version != snapshot.layers
            {
                tokio::task::yield_now().await;
                continue;
            }
            let layers = snapshot.layers.clone();
            let (inode, access, _, control) = Self::decode_permission_inode(&snapshot, ino)?;
            if let Some(actor) = &actor
                && !created
            {
                let requested = crate::meta::layer::open_access_mask()
                    .unwrap_or(if _read { 4 } else { 0 } | if _write || _append { 2 } else { 0 });
                if !Self::inode_allows_actor(
                    &inode,
                    access.as_deref(),
                    control.as_deref(),
                    actor.uid,
                    &actor.groups,
                    requested,
                )? {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::EACCES,
                    )));
                }
            }
            if open_commits {
                match self
                    .commit_versioned_mutation(VersionedMutation::empty(
                        self.guard().await,
                        layers,
                        self.chunk_size,
                    ))
                    .await
                {
                    Err(WorkspaceError::Busy) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    result => {
                        result.map_err(workspace_to_meta)?;
                    }
                }
            }
            self.open_counts
                .entry(ino)
                .and_modify(|count| *count = count.saturating_add(1))
                .or_insert(1);
            return Ok(());
        }
        Err(workspace_to_meta(WorkspaceError::Busy))
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
        }
        // Only an unlinked inode needs the orphan-removal commit. The common
        // close of a linked file skips the mutation version entirely; the
        // loop below re-reads the inode under a fresh version before writing.
        let remove = remove
            && self
                .resolve_inode_delta(ino)
                .await?
                .is_some_and(|inode| inode.nlink == 0);
        if remove {
            for attempt in 0..64 {
                let expected_layers = self.mutation_version().await?;
                let Some(mut inode) = self.resolve_inode_delta(ino).await? else {
                    break;
                };
                if inode.nlink != 0 {
                    break;
                }
                inode.state = InodeState::Deleted;
                inode.ctime_ns = now_ns()?;
                match self.mutate_inode(&expected_layers, inode, Vec::new()).await {
                    Err(MetaError::Io(ref error))
                        if error.raw_os_error() == Some(libc::EBUSY) && attempt < 63 =>
                    {
                        tokio::task::yield_now().await;
                    }
                    result => {
                        result?;
                        break;
                    }
                }
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

    async fn lookup_with_attr(
        &self,
        parent: i64,
        name: &str,
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        self.lookup_with_attr_bytes(parent, name.as_bytes()).await
    }

    async fn lookup_with_attr_bytes(
        &self,
        parent: i64,
        name: &[u8],
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        validate_name_bytes(name)?;
        let fence = self.packed_metadata_fence().await?;
        let result = match self.resolve_dentry_entry(parent, name).await? {
            Some(entry) => {
                let attr = self
                    .stat(entry.ino)
                    .await?
                    .ok_or(MetaError::NotFound(entry.ino))?;
                Some((entry.ino, attr))
            }
            None => None,
        };
        self.validate_packed_metadata_fence(fence).await?;
        Ok(result)
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
        if self.packed_lower().is_some() {
            return Err(MetaError::NotSupported(
                "packed workspace requires bounded raw-name directory mask merge".into(),
            ));
        }
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
        if self.packed_lower().is_some() {
            return self.packed_opendir(ino).await;
        }
        let attr = self.stat(ino).await?.ok_or(MetaError::NotFound(ino))?;
        if attr.kind != FileType::Dir {
            return Err(MetaError::NotDirectory(ino));
        }
        Ok(DirHandle::new(ino, self.readdir(ino).await?).with_attr(attr))
    }

    async fn mkdir(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        self.create_entry(parent, name, FileType::Dir, 0o755, 0, 0, 0, 0, None)
            .await
            .map(|result| result.0)
    }

    async fn rmdir(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        self.remove_entry(parent, name, true).await
    }

    async fn create_file(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        self.create_entry(parent, name, FileType::File, 0o644, 0, 0, 0, 0, None)
            .await
            .map(|result| result.0)
    }

    async fn create_file_with_attr(
        &self,
        parent: i64,
        name: String,
    ) -> Result<CreateEntryResult, MetaError> {
        let (ino, attr) = self
            .create_entry(parent, name, FileType::File, 0o644, 0, 0, 0, 0, None)
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
        self.create_entry(parent, name, kind, mode, 0, uid, gid, rdev, None)
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
            .create_entry(parent, name, kind, mode, 0, uid, gid, rdev, None)
            .await?;
        Ok(CreateEntryResult {
            ino,
            attr: Some(attr),
        })
    }

    async fn link(&self, ino: i64, parent: i64, name: &str) -> Result<FileAttr, MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        for _ in 0..64 {
            let expected_layers = self.mutation_version().await?;
            let result = async {
                validate_name(name)?;
                self.check_namespace_actor(&expected_layers, parent, None)
                    .await?;
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
                self.commit_namespace_mutation(
                    VersionedMutation {
                        expected_layers: expected_layers.clone(),
                        xattrs: Vec::new(),
                        acls: Vec::new(),
                        extents: Vec::new(),
                        chunk_size: self.chunk_size,
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
                    },
                    Some(vec![ino]),
                )
                .await
                .map_err(workspace_to_meta)?;
                file_attr(&inode)
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
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
        for _ in 0..64 {
            let expected_layers = self.mutation_version().await?;
            let result = async {
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
                    // POSIX rename onto another hard link to the same inode is a no-op.
                    return Ok(());
                }

                self.check_namespace_actor(&expected_layers, old_parent, Some(source.ino))
                    .await?;
                self.check_namespace_actor(
                    &expected_layers,
                    new_parent,
                    destination.as_ref().map(|entry| entry.ino),
                )
                .await?;
                if source_kind == FileType::Dir
                    && self
                        .directory_is_descendant_of(new_parent, source.ino)
                        .await?
                {
                    return Err(MetaError::InvalidPath(
                        "cannot rename directory below itself".into(),
                    ));
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
                        (true, true) if !self.directory_is_empty(inode.ino).await? => {
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
                        new_parent_inode.nlink =
                            new_parent_inode.nlink.checked_sub(1).ok_or_else(|| {
                                MetaError::Internal("directory nlink underflow".into())
                            })?;
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

                let files = (source_kind != FileType::Dir).then(|| {
                    std::iter::once(source.ino)
                        .chain(destination.iter().map(|entry| entry.ino))
                        .collect()
                });
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
                self.commit_namespace_mutation(
                    VersionedMutation {
                        expected_layers: expected_layers.clone(),
                        xattrs: Vec::new(),
                        acls: Vec::new(),
                        extents: Vec::new(),
                        chunk_size: self.chunk_size,
                        guard,
                        dentries: vec![
                            DentryDelta::whiteout(
                                head,
                                old_parent,
                                old_name.as_bytes().to_vec(),
                                0,
                            ),
                            DentryDelta::put(
                                head,
                                new_parent,
                                new_name.as_bytes().to_vec(),
                                source.ino,
                                source.entry_type,
                                0,
                            ),
                        ],
                        inodes,
                    },
                    files,
                )
                .await
                .map_err(workspace_to_meta)?;
                Ok(())
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
    }

    async fn rename_noreplace(
        &self,
        _old_parent: i64,
        _old_name: &str,
        _new_parent: i64,
        _new_name: String,
    ) -> Result<(), MetaError> {
        // The workspace namespace uses an optimistic head guard. Its current
        // replacement rename path cannot use an already-resolved destination
        // as a no-replace precondition without reintroducing a TOCTOU window.
        // Refuse the operation until it has a dedicated guarded mutation,
        // rather than silently degrading RENAME_NOREPLACE to replacement.
        Err(MetaError::NotSupported(
            "atomic RENAME_NOREPLACE is not yet available for workspace overlays".into(),
        ))
    }

    async fn rename_exchange(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: &str,
    ) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        for _ in 0..64 {
            let expected_layers = self.mutation_version().await?;
            let result = async {
                validate_name(old_name)?;
                validate_name(new_name)?;
                let left = self
                    .resolve_dentry_entry(old_parent, old_name.as_bytes())
                    .await?
                    .ok_or_else(|| MetaError::EntryNotFound {
                        parent: old_parent,
                        name: old_name.to_owned(),
                    })?;
                let right = self
                    .resolve_dentry_entry(new_parent, new_name.as_bytes())
                    .await?
                    .ok_or_else(|| MetaError::EntryNotFound {
                        parent: new_parent,
                        name: new_name.to_owned(),
                    })?;
                self.check_namespace_actor(&expected_layers, old_parent, Some(left.ino))
                    .await?;
                self.check_namespace_actor(&expected_layers, new_parent, Some(right.ino))
                    .await?;
                if left.ino == right.ino {
                    return Ok(());
                }
                if file_type_from_code(left.entry_type)? == FileType::Dir
                    && self
                        .directory_is_descendant_of(new_parent, left.ino)
                        .await?
                {
                    return Err(MetaError::InvalidPath(format!(
                        "cannot exchange directory inode {} with an entry below it",
                        left.ino
                    )));
                }
                if file_type_from_code(right.entry_type)? == FileType::Dir
                    && self
                        .directory_is_descendant_of(old_parent, right.ino)
                        .await?
                {
                    return Err(MetaError::InvalidPath(format!(
                        "cannot exchange directory inode {} with an entry below it",
                        right.ino
                    )));
                }
                let mut left_inode = self
                    .resolve_inode_delta(left.ino)
                    .await?
                    .ok_or(MetaError::NotFound(left.ino))?;
                let mut right_inode = self
                    .resolve_inode_delta(right.ino)
                    .await?
                    .ok_or(MetaError::NotFound(right.ino))?;
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
                for parent in [&old_parent_inode, &new_parent_inode] {
                    if file_type_from_code(parent.kind)? != FileType::Dir {
                        return Err(MetaError::NotDirectory(parent.ino));
                    }
                }
                let left_is_dir = file_type_from_code(left_inode.kind)? == FileType::Dir;
                let right_is_dir = file_type_from_code(right_inode.kind)? == FileType::Dir;
                if old_parent != new_parent {
                    if left_is_dir != right_is_dir {
                        let (losing, gaining) = if left_is_dir {
                            (&mut old_parent_inode, &mut new_parent_inode)
                        } else {
                            (&mut new_parent_inode, &mut old_parent_inode)
                        };
                        losing.nlink = losing.nlink.checked_sub(1).ok_or_else(|| {
                            MetaError::Internal("directory nlink underflow".into())
                        })?;
                        gaining.nlink = gaining.nlink.checked_add(1).ok_or_else(|| {
                            MetaError::Internal("directory nlink overflow".into())
                        })?;
                    }
                    if left_is_dir {
                        left_inode.parent_hint = Some(new_parent);
                    }
                    if right_is_dir {
                        right_inode.parent_hint = Some(old_parent);
                    }
                }
                let guard = self.guard().await;
                let head = guard.expected_head_layer_id;
                let now = now_ns()?;
                old_parent_inode.mtime_ns = now;
                new_parent_inode.mtime_ns = now;
                let mut inodes = vec![left_inode, right_inode, old_parent_inode];
                if old_parent != new_parent {
                    inodes.push(new_parent_inode);
                }
                for inode in &mut inodes {
                    inode.layer_id = head;
                    inode.ctime_ns = now;
                    inode.sequence = 0;
                }
                let files = (left.entry_type != file_type_code(FileType::Dir)
                    && right.entry_type != file_type_code(FileType::Dir))
                .then(|| vec![left.ino, right.ino]);
                self.commit_namespace_mutation(
                    VersionedMutation {
                        expected_layers: expected_layers.clone(),
                        xattrs: Vec::new(),
                        acls: Vec::new(),
                        extents: Vec::new(),
                        chunk_size: self.chunk_size,
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
                    },
                    files,
                )
                .await
                .map_err(workspace_to_meta)?;
                Ok(())
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
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
        for _ in 0..64 {
            let expected_layers = self.mutation_version().await?;
            let result = async {
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
                self.mutate_data(&expected_layers, inode, extents, Vec::new())
                    .await?;
                Ok(())
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
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
        if self.packed_lower().is_some() {
            return self
                .packed_paths_bytes(ino)
                .await?
                .into_iter()
                .map(|path| String::from_utf8(path).map_err(|_| MetaError::InvalidFilename))
                .collect();
        }
        Ok(self
            .reverse_entries(ino)
            .await?
            .into_iter()
            .map(|(_, _, path)| path)
            .collect())
    }

    async fn get_paths_bytes(&self, ino: i64) -> Result<Vec<Vec<u8>>, MetaError> {
        if self.packed_lower().is_some() {
            return self.packed_paths_bytes(ino).await;
        }
        self.get_paths(ino)
            .await
            .map(|paths| paths.into_iter().map(String::into_bytes).collect())
    }

    async fn get_paths_bytes_owned(
        &self,
        ino: i64,
    ) -> Result<crate::meta::layer::OwnedPaths, MetaError> {
        if self.packed_lower().is_some() {
            return self.packed_paths_bytes_owned(ino).await;
        }
        Ok(crate::meta::layer::OwnedPaths {
            paths: self.get_paths_bytes(ino).await?,
            guard: None,
        })
    }

    async fn read_symlink(&self, ino: i64) -> Result<String, MetaError> {
        String::from_utf8(self.read_symlink_bytes(ino).await?)
            .map_err(|_| MetaError::InvalidPath("symlink target is not UTF-8".into()))
    }

    async fn read_symlink_bytes(&self, ino: i64) -> Result<Vec<u8>, MetaError> {
        let fence = self.packed_metadata_fence().await?;
        let inode = self
            .resolve_inode_delta(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;
        if file_type_from_code(inode.kind)? != FileType::Symlink {
            return Err(MetaError::NotSupported("inode is not a symlink".into()));
        }
        let target = inode
            .symlink_target
            .ok_or_else(|| MetaError::Internal("symlink inode is missing its target".into()))?;
        self.validate_packed_metadata_fence(fence).await?;
        Ok(target)
    }

    async fn set_attr(
        &self,
        ino: i64,
        req: &SetAttrRequest,
        flags: SetAttrFlags,
    ) -> Result<FileAttr, MetaError> {
        self.set_attr_checked(ino, req, flags, None).await
    }

    async fn set_attr_as(
        &self,
        ino: i64,
        req: &SetAttrRequest,
        flags: SetAttrFlags,
        uid: u32,
        groups: &[u32],
    ) -> Result<FileAttr, MetaError> {
        self.set_attr_checked(ino, req, flags, Some((uid, groups)))
            .await
    }

    async fn open(&self, ino: i64, flags: OpenFlags) -> Result<FileAttr, MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        for _ in 0..64 {
            let expected_layers = self.mutation_version().await?;
            let result = async {
                let (mut inode, access, _, control) =
                    self.permission_inode(&expected_layers, ino).await?;
                if let Some(actor) = crate::meta::layer::namespace_actor() {
                    let bits = flags.bits() & OpenFlags::RDWR.bits();
                    let requested = crate::meta::layer::open_access_mask().unwrap_or(
                        (if bits & OpenFlags::RDONLY.bits() != 0 {
                            4
                        } else {
                            0
                        }) | (if bits & OpenFlags::WRONLY.bits() != 0
                            || flags.contains(OpenFlags::TRUNC)
                        {
                            2
                        } else {
                            0
                        }),
                    );
                    if !Self::inode_allows_actor(
                        &inode,
                        access.as_deref(),
                        control.as_deref(),
                        actor.uid,
                        &actor.groups,
                        requested,
                    )? {
                        return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                            libc::EACCES,
                        )));
                    }
                }
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
                    let inode = self
                        .mutate_data(&expected_layers, inode, extents, Vec::new())
                        .await?;
                    return file_attr(&inode);
                }
                inode.atime_ns = now_ns()?;
                let inode = self
                    .mutate_inode(&expected_layers, inode, Vec::new())
                    .await?;
                file_attr(&inode)
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
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
        Self::validate_write(ino, chunk_id, &slice, new_size, self.chunk_size)?;
        let (sender, mut receiver) = tokio::sync::oneshot::channel();
        {
            let mut queue = self.write_queue.lock().unwrap();
            queue.pending.push(PendingWrite {
                ino,
                chunk_id,
                slice,
                new_size,
                reply: sender,
            });
        }
        loop {
            let guard = tokio::select! {
                biased;
                result = &mut receiver => return result.unwrap_or_else(|_| {
                    Err(MetaError::Internal("workspace group commit was dropped".into()))
                }),
                guard = self.mutation_gate.lock() => guard,
            };
            // Only gate acquisition races the receipt: once a batch is taken,
            // finish it before checking our result and handing off leadership.
            self.commit_next_write_batch(guard).await;
        }
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
        if let Some(lower) = self.packed_lower() {
            self.shutdown_packed_runtime_for_clean_release().await?;
            lower.budget.close();
        }
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
        self.set_xattr_bytes(inode, name.as_bytes(), value, flags)
            .await
    }

    async fn set_xattr_bytes(
        &self,
        inode: i64,
        name: &[u8],
        value: &[u8],
        flags: u32,
    ) -> Result<(), MetaError> {
        if let Some(name) = posix_acl_xattr_name(name) {
            if flags & !(libc::XATTR_CREATE as u32 | libc::XATTR_REPLACE as u32) != 0 {
                return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                    libc::EINVAL,
                )));
            }
            return self
                .update_posix_acl_with_flags(inode, name, Some(value), flags, 0, &[])
                .await;
        }
        let _mutation_guard = self.mutation_gate.lock().await;
        for _ in 0..64 {
            let expected_layers = self.mutation_version().await?;
            let result = async {
                validate_xattr_name_bytes(name)?;
                let mut inode_delta = self
                    .resolve_inode_delta(inode)
                    .await?
                    .ok_or(MetaError::NotFound(inode))?;
                let existing = self.get_xattr_bytes(inode, name).await?;
                if flags & !(libc::XATTR_CREATE as u32 | libc::XATTR_REPLACE as u32) != 0 {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::EINVAL,
                    )));
                }
                let is_access = name == ACCESS_XATTR;
                let is_default = name == DEFAULT_XATTR;
                let mut acl_value = Some(value.to_vec());
                if is_access || is_default {
                    if inode_delta.kind == file_type_code(FileType::Symlink) {
                        return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                            libc::EOPNOTSUPP,
                        )));
                    }
                    if is_default && inode_delta.kind != file_type_code(FileType::Dir) {
                        return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                            libc::EACCES,
                        )));
                    }
                    let acl = PosixAcl::decode(value).map_err(|_| {
                        MetaError::Io(std::io::Error::from_raw_os_error(libc::EINVAL))
                    })?;
                    if is_access {
                        inode_delta.mode = (inode_delta.mode & !0o777) | acl.mode_bits();
                        if !acl.is_extended() {
                            acl_value = None;
                        }
                    }
                }
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
                        name: String::from_utf8_lossy(name).into_owned(),
                    });
                }
                if replace_only && existing.is_none() {
                    return Err(MetaError::NotFound(inode));
                }
                let guard = self.guard().await;
                inode_delta.layer_id = guard.expected_head_layer_id;
                inode_delta.sequence = 0;
                inode_delta.ctime_ns = now_ns()?;
                self.commit_versioned_mutation(VersionedMutation {
                    xattrs: vec![XattrDelta {
                        layer_id: guard.expected_head_layer_id,
                        ino: inode,
                        name: name.to_vec(),
                        op: if acl_value.is_some() {
                            ValueOp::Put
                        } else {
                            ValueOp::Whiteout
                        },
                        value: acl_value,
                        sequence: 0,
                    }],
                    inodes: vec![inode_delta],
                    ..VersionedMutation::empty(guard, expected_layers.clone(), self.chunk_size)
                })
                .await
                .map(|_| ())
                .map_err(workspace_to_meta)
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
    }

    async fn get_xattr(&self, inode: i64, name: &str) -> Result<Option<Vec<u8>>, MetaError> {
        self.get_xattr_bytes(inode, name.as_bytes()).await
    }

    async fn get_xattr_bytes(&self, inode: i64, name: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        validate_xattr_name_bytes(name)?;
        let fence = self.packed_metadata_fence().await?;
        if self.stat(inode).await?.is_none() {
            return Err(MetaError::NotFound(inode));
        }
        let chain = self.chain().await?;
        let cache = self.upper_row_cache();
        let key = row_cache::RowKey::Xattr(inode, name.to_vec());
        let rows = match cache.and_then(|cache| cache.get(&key)) {
            Some(row_cache::RowValue::Xattr(rows)) => rows,
            _ => {
                let ticket = cache.map(row_cache::UpperRowCache::ticket);
                let rows = std::sync::Arc::new(
                    self.store
                        .get_xattr_deltas(XattrQuery {
                            layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
                            ino: inode,
                            name: Some(name.to_vec()),
                        })
                        .await
                        .map_err(workspace_to_meta)?,
                );
                if let (Some(cache), Some(ticket)) = (cache, ticket) {
                    cache.insert(ticket, key, row_cache::RowValue::Xattr(rows.clone()));
                }
                rows
            }
        };
        let result =
            match resolve_xattr_state(&chain, &rows, inode, name).map_err(workspace_to_meta)? {
                Resolution::Present(value) => Ok(Some(value.value)),
                Resolution::Masked => Ok(None),
                Resolution::Absent => {
                    let Some(lower) = self.packed_lower() else {
                        return Ok(None);
                    };
                    let value = lower.metadata.get_xattr_bytes(inode, name).await?;
                    Ok(value)
                }
            };
        if result.is_ok() {
            self.validate_packed_metadata_fence(fence).await?;
        }
        result
    }

    async fn list_xattr(&self, inode: i64) -> Result<Vec<String>, MetaError> {
        if self.packed_lower().is_some() {
            return self
                .packed_list_xattr_bytes_owned(inode)
                .await?
                .names
                .into_iter()
                .map(|name| String::from_utf8(name).map_err(|_| MetaError::InvalidFilename))
                .collect();
        }
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

    async fn list_xattr_bytes(&self, inode: i64) -> Result<Vec<Vec<u8>>, MetaError> {
        Ok(self.list_xattr_bytes_owned(inode).await?.names)
    }

    async fn list_xattr_bytes_owned(
        &self,
        inode: i64,
    ) -> Result<crate::meta::layer::OwnedXattrNames, MetaError> {
        if self.packed_lower().is_some() {
            return self.packed_list_xattr_bytes_owned(inode).await;
        }
        Ok(crate::meta::layer::OwnedXattrNames {
            names: self
                .list_xattr(inode)
                .await?
                .into_iter()
                .map(String::into_bytes)
                .collect(),
            guard: None,
        })
    }

    async fn remove_xattr(&self, inode: i64, name: &str) -> Result<(), MetaError> {
        self.remove_xattr_bytes(inode, name.as_bytes()).await
    }

    async fn remove_xattr_bytes(&self, inode: i64, name: &[u8]) -> Result<(), MetaError> {
        if let Some(name) = posix_acl_xattr_name(name) {
            return self
                .update_posix_acl_with_flags(inode, name, None, libc::XATTR_REPLACE as u32, 0, &[])
                .await;
        }
        let _mutation_guard = self.mutation_gate.lock().await;
        for _ in 0..64 {
            let expected_layers = self.mutation_version().await?;
            let result = async {
                validate_xattr_name_bytes(name)?;
                if self.get_xattr_bytes(inode, name).await?.is_none() {
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
                if inode_delta.kind == file_type_code(FileType::Symlink)
                    && (name == ACCESS_XATTR || name == DEFAULT_XATTR)
                {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::EOPNOTSUPP,
                    )));
                }
                self.commit_versioned_mutation(VersionedMutation {
                    xattrs: vec![XattrDelta {
                        layer_id: guard.expected_head_layer_id,
                        ino: inode,
                        name: name.to_vec(),
                        op: ValueOp::Whiteout,
                        value: None,
                        sequence: 0,
                    }],
                    inodes: vec![inode_delta],
                    ..VersionedMutation::empty(guard, expected_layers.clone(), self.chunk_size)
                })
                .await
                .map(|_| ())
                .map_err(workspace_to_meta)
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
    }

    async fn set_acl(&self, inode: i64, rule: AclRule) -> Result<(), MetaError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        for _ in 0..64 {
            let expected_layers = self.mutation_version().await?;
            let result = async {
                if self.resolve_inode_delta(inode).await?.is_none() {
                    return Err(MetaError::NotFound(inode));
                }
                let guard = self.guard().await;
                self.commit_versioned_mutation(VersionedMutation {
                    acls: vec![AclDelta {
                        layer_id: guard.expected_head_layer_id,
                        ino: inode,
                        acl_type: rule.acl_type,
                        acl_id: i64::from(rule.qualifier),
                        op: ValueOp::Put,
                        value: Some(rule.permissions.to_be_bytes().to_vec()),
                        sequence: 0,
                    }],
                    ..VersionedMutation::empty(guard, expected_layers.clone(), self.chunk_size)
                })
                .await
                .map(|_| ())
                .map_err(workspace_to_meta)
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
    }

    async fn get_acl(
        &self,
        inode: i64,
        acl_type: u8,
        acl_id: u32,
    ) -> Result<Option<AclRule>, MetaError> {
        let fence = self.packed_metadata_fence().await?;
        if self.stat(inode).await?.is_none() {
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
        let resolved = match resolve_acl_state(&chain, &rows, inode, acl_type, i64::from(acl_id))
            .map_err(workspace_to_meta)?
        {
            Resolution::Present(value) => value,
            Resolution::Masked => {
                self.validate_packed_metadata_fence(fence).await?;
                return Ok(None);
            }
            Resolution::Absent => {
                let Some(lower) = self.packed_lower() else {
                    return Ok(None);
                };
                let value = lower.metadata.get_acl(inode, acl_type, acl_id).await?;
                self.validate_packed_metadata_fence(fence).await?;
                return Ok(value);
            }
        };
        let bytes: [u8; 4] = resolved.value.try_into().map_err(|_| {
            MetaError::Internal("workspace ACL permissions have an invalid encoding".into())
        })?;
        self.validate_packed_metadata_fence(fence).await?;
        Ok(Some(AclRule {
            acl_type,
            qualifier: acl_id,
            permissions: u32::from_be_bytes(bytes),
        }))
    }
}

#[async_trait]
impl<W: WorkspaceStore + 'static> WorkspaceReadPlanProvider for WorkspaceMetaLayer<W> {
    fn requires_unified_read_request_fence(&self) -> bool {
        self.packed_lower().is_some()
    }

    async fn begin_unified_read_request(
        &self,
        ino: i64,
    ) -> anyhow::Result<Option<Arc<dyn UnifiedReadRequestFence>>> {
        let Some(lower) = self.packed_lower() else {
            return Ok(None);
        };
        packed_lower::begin_request(self, lower.clone(), ino)
            .await
            .map(Some)
    }

    fn supports_prepared_unified_read(&self) -> bool {
        self.packed_lower().is_some()
    }

    fn max_read_bytes(&self) -> Option<usize> {
        self.packed_lower()
            .map(|lower| lower.budget.max_read_bytes())
    }

    fn reserve_read_output(
        &self,
        length: usize,
    ) -> Result<Option<Box<dyn Send + Sync>>, MetaError> {
        self.packed_lower()
            .map(|lower| {
                lower
                    .budget
                    .output(length)
                    .map(|permit| Box::new(permit) as Box<dyn Send + Sync>)
                    .map_err(packed_lower::budget_to_meta)
            })
            .transpose()
    }

    fn begin_unified_read_operation(
        &self,
        requested: u64,
    ) -> Option<crate::cadapter::read_observer::TerminalGuard> {
        self.packed_lower()
            .and_then(|lower| lower.begin_operation(requested))
    }

    fn record_unified_read_success(&self, bytes: u64) {
        if let Some(lower) = self.packed_lower() {
            lower.metadata.record_unified_read_success(bytes);
        }
    }

    async fn prepare_unified_read(
        &self,
        ino: i64,
        chunk_index: u64,
        offset: u64,
        len: u64,
    ) -> Result<Option<PreparedUnifiedRead>, MetaError> {
        self.prepare_unified_read_observed(ino, chunk_index, offset, len, None)
            .await
    }

    async fn prepare_unified_read_observed(
        &self,
        ino: i64,
        chunk_index: u64,
        offset: u64,
        len: u64,
        delivery: Option<Arc<crate::cadapter::read_observer::OperationDelivery>>,
    ) -> Result<Option<PreparedUnifiedRead>, MetaError> {
        let Some(lower) = self.packed_lower() else {
            return Ok(None);
        };
        packed_lower::prepare(self, lower.clone(), ino, chunk_index, offset, len, delivery)
            .await
            .map(Some)
    }

    async fn read_plan(
        &self,
        ino: i64,
        chunk_index: u64,
        offset: u64,
        len: u64,
    ) -> Result<ResolvedReadPlan, MetaError> {
        if self.packed_lower().is_some() {
            return Err(MetaError::NotSupported(
                "packed workspace requires prepared unified read".into(),
            ));
        }
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
            let has_data = if self.supports_prepared_unified_read() {
                let prepared = self
                    .prepare_unified_read(ino, chunk_index, chunk_offset, take)
                    .await?
                    .ok_or_else(|| MetaError::Internal("workspace has no prepared range".into()))?;
                prepared.plan.segments.iter().any(|segment| {
                    !matches!(segment.source, crate::chunk::read_plan::ReadSource::Hole)
                })
            } else {
                self.read_plan(ino, chunk_index, chunk_offset, take)
                    .await?
                    .segments
                    .iter()
                    .any(|segment| matches!(segment, ReadPlanSegment::Data { .. }))
            };
            if has_data {
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
        for _ in 0..64 {
            let expected_layers = self.mutation_version().await?;
            let result = async {
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
                let inode = self
                    .mutate_data(&expected_layers, inode, extents, Vec::new())
                    .await?;
                Ok(inode.size)
            }
            .await;
            match result {
                Err(MetaError::Io(ref error)) if error.raw_os_error() == Some(libc::EBUSY) => {
                    tokio::task::yield_now().await;
                }
                other => return other,
            }
        }
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EBUSY,
        )))
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
        WorkspaceError::WorkspaceNotFound(_)
        | WorkspaceError::LayerNotFound(_)
        | WorkspaceError::SnapshotNotFound(_) => MetaError::NotFound(1),
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

pub(crate) fn file_attr(inode: &InodeDelta) -> Result<FileAttr, MetaError> {
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
    validate_name_bytes(name.as_bytes())
}

fn validate_name_bytes(name: &[u8]) -> Result<(), MetaError> {
    if name.is_empty() || matches!(name, b"." | b"..") || name.contains(&0) {
        return Err(MetaError::InvalidFilename);
    }
    if name.contains(&b'/') {
        return Err(MetaError::InvalidFilename);
    }
    if name.len() > 255 {
        return Err(MetaError::FilenameTooLong);
    }
    Ok(())
}

fn validate_xattr_name_bytes(name: &[u8]) -> Result<(), MetaError> {
    // Linux's xattr name copy reports ERANGE above XATTR_NAME_MAX bytes.
    if name.len() > 255 {
        return Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::ERANGE,
        )));
    }
    if name.is_empty() || name.contains(&0) {
        return Err(MetaError::InvalidPath("invalid xattr name".into()));
    }
    Ok(())
}

fn posix_acl_xattr_name(name: &[u8]) -> Option<&'static str> {
    if name == ACCESS_XATTR {
        Some("system.posix_acl_access")
    } else if name == DEFAULT_XATTR {
        Some("system.posix_acl_default")
    } else {
        None
    }
}

#[cfg(test)]
mod tests;
