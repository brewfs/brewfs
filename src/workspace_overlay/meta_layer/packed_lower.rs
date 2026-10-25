//! Authenticated PM11 lower combined with native workspace Data/Hole coverage.
//! Store binding publication and whole-request retries are separate contracts.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;

use crate::cadapter::client::ObjectBackend;
use crate::cadapter::read_observer::{OperationDelivery, TerminalGuard};
use crate::chunk::read_plan::{
    LogicalSegment, PreparedUnifiedRead, ReadGeneration, ReadPlanSegment, ReadSource,
    ReadViewChanged, ResolvedReadPlan, UnifiedReadPlan, UnifiedReadRequestFence,
    UnifiedReadSourceFetcher, WorkspaceReadPlanProvider, execute_into, prepare_overlay_plan,
};
use crate::chunk::{BlockStore, ChunkLayout};
use crate::meta::layer::MetaLayer;
use crate::meta::store::{FileType, MetaError};

use super::{WorkspaceMetaLayer, validate_fixed_layer_pair};
use crate::workspace_overlay::catalog::{
    ExtentQuery, HeadGuard, InodeQuery, MAX_UPPER_EXTENT_ROWS, PackedLowerBinding, WorkspaceStore,
};
use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::model::{ExtentKind, LayerRecord};
use crate::workspace_overlay::packed_v3::PackedV3ReadonlyMeta;
use crate::workspace_overlay::packed_v3::wire005::{
    V3BudgetPool, V3MountBudget, V3ObjectKind, V3OwnedPermit,
};
use crate::workspace_overlay::resolver::{
    Resolution, resolve_extent_coverage, resolve_inode_state,
};

// 8 x 16 MiB walk arenas = half of the default 256 MiB Metadata pool.
const MAX_CONCURRENT_PATH_WALKS: usize = 8;
// Includes fixed-width backend rows plus sentinel, resolver intermediates,
// upper segments, gaps and per-gap owner bookkeeping. Child plan clones are
// admitted separately before cloning; child owners retain their own permits.
const UPPER_PREPARATION_BYTES: u64 = 4 << 20;
// Plans kept free for in-progress reads' non-waiting recipe/driver charges
// while new upper arenas wait (same reserve as lower preparation arenas).
const UPPER_ARENA_HEADROOM_BYTES: u64 = 512 << 10;
// The request retains a fixed guard, two layer records and shared lower
// owners. Chunk preparation keeps its separate row/segment admission.
const REQUEST_FENCE_BYTES: u64 = 4096;

pub(super) fn budget_to_meta(
    error: crate::workspace_overlay::packed_v3::PackedWireError,
) -> MetaError {
    if matches!(
        error,
        crate::workspace_overlay::packed_v3::PackedWireError::LimitExceeded(_)
    ) {
        MetaError::Io(std::io::Error::from_raw_os_error(libc::ENOMEM))
    } else {
        MetaError::Internal(error.to_string())
    }
}

fn read_workspace_error(error: WorkspaceError) -> anyhow::Error {
    match error {
        WorkspaceError::Busy => ReadViewChanged.into(),
        error => error.into(),
    }
}

fn read_workspace_to_meta(error: WorkspaceError) -> MetaError {
    MetaError::Anyhow(read_workspace_error(error))
}

fn fetch_to_meta(error: anyhow::Error) -> MetaError {
    MetaError::Anyhow(error)
}

/// An authority must validate the independent binding under the supplied
/// workspace/lease guard. Tests may inject a deterministic authority, which
/// does not establish persisted publication/GC correctness.
#[async_trait]
pub trait WorkspacePackedBindingAuthority: Send + Sync {
    fn retain_reader_request(
        &self,
    ) -> Result<
        Option<crate::workspace_overlay::packed_reader_lifecycle::PackedReaderRequestOwner>,
        WorkspaceError,
    > {
        Ok(None)
    }
    fn reader_session(
        &self,
    ) -> Option<Arc<dyn crate::workspace_overlay::packed_reader_lifecycle::PackedReaderSession>>
    {
        None
    }

    async fn validate(
        &self,
        guard: &HeadGuard,
        expected: &PackedLowerBinding,
    ) -> Result<(), WorkspaceError>;
}

pub struct CatalogPackedBindingAuthority<W>(pub Arc<W>);

#[async_trait]
impl<W: WorkspaceStore + 'static> WorkspacePackedBindingAuthority
    for CatalogPackedBindingAuthority<W>
{
    async fn validate(
        &self,
        guard: &HeadGuard,
        expected: &PackedLowerBinding,
    ) -> Result<(), WorkspaceError> {
        if self
            .0
            .load_packed_lower_binding(guard.clone())
            .await?
            .as_ref()
            != Some(expected)
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
}

/// Current workspace authority plus independent persistent byte retention.
/// Retention renewal survives publication; mixed workspace reads still fence.
pub struct PinnedCatalogPackedBindingAuthority<W> {
    pub store: Arc<W>,
    pub reader: Arc<dyn crate::workspace_overlay::packed_reader_lifecycle::PackedReaderSession>,
}
#[async_trait]
impl<W: WorkspaceStore + 'static> WorkspacePackedBindingAuthority
    for PinnedCatalogPackedBindingAuthority<W>
{
    fn retain_reader_request(
        &self,
    ) -> Result<
        Option<crate::workspace_overlay::packed_reader_lifecycle::PackedReaderRequestOwner>,
        WorkspaceError,
    > {
        self.reader.retain_request().map(Some)
    }
    fn reader_session(
        &self,
    ) -> Option<Arc<dyn crate::workspace_overlay::packed_reader_lifecycle::PackedReaderSession>>
    {
        Some(self.reader.clone())
    }
    async fn validate(
        &self,
        guard: &HeadGuard,
        expected: &PackedLowerBinding,
    ) -> Result<(), WorkspaceError> {
        if self.reader.binding() != expected {
            return Err(WorkspaceError::Fenced);
        }
        self.reader.validate().await?;
        CatalogPackedBindingAuthority(self.store.clone())
            .validate(guard, expected)
            .await
    }
}

#[async_trait]
pub(crate) trait PackedLowerOperations: MetaLayer + WorkspaceReadPlanProvider {
    /// Expose the lower's packed index/budget/observer metrics in `.stats`.
    fn install_stats(&self, stats: &crate::vfs::stats::FsStats) -> bool;
    async fn drain_packed_transport(&self) -> Result<(), MetaError>;
    async fn frozen_reverse_names_page_owned(
        &self,
        inode: i64,
        after: Option<&[u8]>,
    ) -> Result<crate::workspace_overlay::packed_v3::PackedLowerReverseNames, MetaError>;
    async fn frozen_inode_metadata_owned(
        &self,
        inode: i64,
    ) -> Result<Option<crate::workspace_overlay::packed_v3::PackedLowerInodeMetadata>, MetaError>;
    async fn frozen_cold_attributes_owned(
        &self,
        inode: i64,
    ) -> Result<
        Option<
            crate::workspace_overlay::packed_v3::wire005::V3Owned<
                crate::workspace_overlay::packed_v3::wire005::V3ColdAttributes,
            >,
        >,
        MetaError,
    >;
}
#[async_trait]
impl<B: ObjectBackend + Clone + Send + Sync + 'static> PackedLowerOperations
    for PackedV3ReadonlyMeta<B>
{
    fn install_stats(&self, stats: &crate::vfs::stats::FsStats) -> bool {
        self.install_v3_stats(stats)
    }
    async fn frozen_reverse_names_page_owned(
        &self,
        inode: i64,
        after: Option<&[u8]>,
    ) -> Result<crate::workspace_overlay::packed_v3::PackedLowerReverseNames, MetaError> {
        PackedV3ReadonlyMeta::frozen_reverse_names_page_owned(self, inode, after).await
    }
    async fn frozen_inode_metadata_owned(
        &self,
        inode: i64,
    ) -> Result<Option<crate::workspace_overlay::packed_v3::PackedLowerInodeMetadata>, MetaError>
    {
        PackedV3ReadonlyMeta::frozen_inode_metadata_owned(self, inode).await
    }
    async fn drain_packed_transport(&self) -> Result<(), MetaError> {
        PackedV3ReadonlyMeta::drain_packed_transport(self).await
    }
    async fn frozen_cold_attributes_owned(
        &self,
        inode: i64,
    ) -> Result<
        Option<
            crate::workspace_overlay::packed_v3::wire005::V3Owned<
                crate::workspace_overlay::packed_v3::wire005::V3ColdAttributes,
            >,
        >,
        MetaError,
    > {
        PackedV3ReadonlyMeta::frozen_cold_attributes_owned(self, inode).await
    }
}

/// Native means native catalog layers; it is never an older packed decoder.
pub(super) enum WorkspaceLower {
    Native,
    PackedV3(Arc<WorkspacePackedLower>),
}

pub(crate) struct WorkspacePackedLower {
    pub binding: PackedLowerBinding,
    pub metadata: Arc<dyn PackedLowerOperations>,
    pub authority: Arc<dyn WorkspacePackedBindingAuthority>,
    pub upper: Arc<dyn UnifiedReadSourceFetcher>,
    pub budget: Arc<V3MountBudget>,
    /// Caps concurrent 16 MiB reverse-path walks so they cannot exhaust the
    /// Metadata pool that ordinary FUSE requests also charge.
    pub(super) path_walks: tokio::sync::Semaphore,
    /// The immutable lower's per-inode hot attributes and cold permission
    /// inputs, resolved once and kept for the life of this
    /// binding. No expiry and no re-validation: the snapshot cannot change
    /// while bound. Upper rows still shadow these on every read.
    pub(super) inode_hot_memo: moka::sync::Cache<i64, Option<Arc<LowerInodeHot>>>,
    pub(super) inode_cold_memo: moka::sync::Cache<i64, Option<Arc<LowerInodeCold>>>,
    /// The lower's authenticated reverse (parent, name) aliases of an inode, complete across pages, for the binding lifetime.
    pub(super) reverse_names: moka::sync::Cache<i64, LowerAliases>,
    memo_fills: MemoFills,
    upper_identity: usize,
    upper_layout: ChunkLayout,
}

/// A lower inode's complete authenticated reverse (parent, name) aliases.
pub(super) type LowerAliases = Arc<Vec<(i64, Vec<u8>)>>;

/// One lower inode's hot attributes, copied out of the authenticated
/// snapshot (no retained budget permit; bounded by entry count).
#[derive(Clone, Debug)]
pub(super) struct LowerInodeHot {
    pub attr: crate::meta::store::FileAttr,
    pub parent_hint: Option<i64>,
}

/// One lower inode's cold permission inputs: the symlink target and the
/// permission xattrs present in the lower, as (name, value).
#[derive(Clone, Debug)]
pub(super) struct LowerInodeCold {
    pub symlink_target: Option<Vec<u8>>,
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Byte bound for each immutable-lower memo (values plus cache overhead).
/// Eviction only costs a re-read of the immutable snapshot.
const LOWER_MEMO_BYTES: u64 = 64 << 20;
const PERMISSION_XATTRS: [&[u8]; 3] = [
    crate::meta::posix_acl::ACCESS_XATTR,
    crate::meta::posix_acl::DEFAULT_XATTR,
    b"system.brewfs.acl",
];

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MemoKind {
    Hot,
    Cold,
    Reverse,
}

type MemoFills = Mutex<BTreeMap<(MemoKind, i64), Weak<tokio::sync::Mutex<()>>>>;

impl WorkspacePackedLower {
    /// Coordinate only concurrent fills. Values (including absence) remain
    /// in the byte-bounded memos; errors and cancellation are never cached.
    async fn lock_memo_fill(
        &self,
        kind: MemoKind,
        ino: i64,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>, MetaError> {
        let lock = {
            let mut fills = self
                .memo_fills
                .lock()
                .map_err(|_| MetaError::Internal("packed memo fill registry poisoned".into()))?;
            fills.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = fills.get(&(kind, ino)).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                fills.insert((kind, ino), Arc::downgrade(&lock));
                lock
            }
        };
        Ok(lock.lock_owned().await)
    }

    /// The lower inode's hot attributes (None: no such lower inode),
    /// memoized for the binding lifetime.
    pub(super) async fn inode_hot(
        &self,
        ino: i64,
    ) -> Result<Option<Arc<LowerInodeHot>>, MetaError> {
        if let Some(found) = self.inode_hot_memo.get(&ino) {
            return Ok(found);
        }
        let _fill = self.lock_memo_fill(MemoKind::Hot, ino).await?;
        if let Some(found) = self.inode_hot_memo.get(&ino) {
            return Ok(found);
        }
        let loaded = self
            .metadata
            .frozen_inode_metadata_owned(ino)
            .await?
            .map(|hot| {
                Arc::new(LowerInodeHot {
                    attr: hot.attr.clone(),
                    parent_hint: hot.parent_hint,
                })
            });
        self.inode_hot_memo.insert(ino, loaded.clone());
        Ok(loaded)
    }

    /// The lower inode's cold permission inputs (None: no cold record),
    /// memoized for the binding lifetime.
    pub(super) async fn inode_cold(
        &self,
        ino: i64,
    ) -> Result<Option<Arc<LowerInodeCold>>, MetaError> {
        if let Some(found) = self.inode_cold_memo.get(&ino) {
            return Ok(found);
        }
        let _fill = self.lock_memo_fill(MemoKind::Cold, ino).await?;
        if let Some(found) = self.inode_cold_memo.get(&ino) {
            return Ok(found);
        }
        let loaded = self
            .metadata
            .frozen_cold_attributes_owned(ino)
            .await?
            .map(|cold| {
                Arc::new(LowerInodeCold {
                    symlink_target: cold.symlink_target.clone(),
                    xattrs: cold
                        .xattrs
                        .iter()
                        .filter(|value| PERMISSION_XATTRS.contains(&value.name.as_slice()))
                        .map(|value| (value.name.clone(), value.value.clone()))
                        .collect(),
                })
            });
        self.inode_cold_memo.insert(ino, loaded.clone());
        Ok(loaded)
    }

    /// The lower's complete reverse aliases of `ino`, memoized for the binding
    /// lifetime (only a complete walk is stored).
    pub(super) async fn reverse_aliases(
        &self,
        ino: i64,
        mut page: impl FnMut() -> Result<(), MetaError>,
    ) -> Result<LowerAliases, MetaError> {
        if let Some(found) = self.reverse_names.get(&ino) {
            return Ok(found);
        }
        let _fill = self.lock_memo_fill(MemoKind::Reverse, ino).await?;
        if let Some(found) = self.reverse_names.get(&ino) {
            return Ok(found);
        }
        let mut rows = Vec::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            page()?;
            let next = self
                .metadata
                .frozen_reverse_names_page_owned(ino, after.as_deref())
                .await?;
            if next.rows.is_empty() {
                break;
            }
            if next
                .after
                .as_ref()
                .is_none_or(|cursor| after.as_ref().is_some_and(|old| cursor <= old))
            {
                return Err(MetaError::Internal(
                    "packed reverse cursor did not advance".into(),
                ));
            }
            rows.extend(next.rows.iter().cloned());
            after = next.after.clone();
        }
        let rows = Arc::new(rows);
        self.reverse_names.insert(ino, rows.clone());
        Ok(rows)
    }
}

impl WorkspacePackedLower {
    pub(super) fn new<B, S>(
        binding: PackedLowerBinding,
        metadata: Arc<PackedV3ReadonlyMeta<B>>,
        authority: Arc<dyn WorkspacePackedBindingAuthority>,
        upper: Arc<S>,
        layout: ChunkLayout,
    ) -> Result<Arc<Self>, MetaError>
    where
        B: ObjectBackend + Clone + Send + Sync + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        if binding.binding_version == 0
            || binding.manifest.kind != V3ObjectKind::Manifest
            || &binding.manifest != metadata.manifest_reference()
            || layout.chunk_size == 0
            || layout.block_size == 0
            || layout.chunk_size != metadata.chunk_size()
        {
            return Err(MetaError::Internal(
                "packed lower binding/layout mismatch".into(),
            ));
        }
        let budget = metadata.mount_budget();
        if let Some(reader) = authority.reader_session() {
            if reader.binding() != &binding {
                return Err(MetaError::Internal(
                    "packed lower transport binding mismatch".into(),
                ));
            }
            metadata.bind_reader_session(reader)?;
        }
        Ok(Arc::new(Self {
            binding,
            metadata,
            authority,
            path_walks: tokio::sync::Semaphore::new(MAX_CONCURRENT_PATH_WALKS),
            inode_hot_memo: moka::sync::Cache::builder()
                .weigher(|_, _| 192)
                .max_capacity(LOWER_MEMO_BYTES)
                .build(),
            inode_cold_memo: moka::sync::Cache::builder()
                .weigher(|_, value: &Option<Arc<LowerInodeCold>>| {
                    let bytes = value.as_ref().map_or(0, |cold| {
                        cold.symlink_target.as_ref().map_or(0, Vec::len)
                            + cold
                                .xattrs
                                .iter()
                                .map(|(name, value)| name.len() + value.len() + 48)
                                .sum::<usize>()
                    });
                    u32::try_from(bytes).unwrap_or(u32::MAX).saturating_add(128)
                })
                .max_capacity(LOWER_MEMO_BYTES)
                .build(),
            reverse_names: moka::sync::Cache::builder()
                .weigher(|_, rows: &LowerAliases| {
                    let bytes = rows.iter().map(|(_, name)| name.len() + 40).sum::<usize>();
                    u32::try_from(bytes).unwrap_or(u32::MAX).saturating_add(96)
                })
                .max_capacity(LOWER_MEMO_BYTES)
                .build(),
            memo_fills: MemoFills::default(),
            upper_identity: Arc::as_ptr(&upper) as *const () as usize,
            upper_layout: layout,
            upper: Arc::new(NativeUpperFetcher {
                store: upper,
                layout,
            }),
            budget,
        }))
    }

    pub(super) fn begin_operation(&self, requested: u64) -> Option<TerminalGuard> {
        self.metadata.begin_unified_read_operation(requested)
    }

    pub(crate) fn frozen_upper_matches<S>(&self, upper: &Arc<S>, layout: ChunkLayout) -> bool {
        self.upper_identity == Arc::as_ptr(upper) as *const () as usize
            && self.upper_layout.chunk_size == layout.chunk_size
            && self.upper_layout.block_size == layout.block_size
    }
}

async fn capture_read_fence<W: WorkspaceStore + 'static>(
    meta: &WorkspaceMetaLayer<W>,
    lower: &WorkspacePackedLower,
) -> anyhow::Result<(
    HeadGuard,
    [LayerRecord; 2],
    Option<crate::workspace_overlay::packed_reader_lifecycle::PackedReaderRequestOwner>,
)> {
    // Immutable lower: the base record is the mount's cached one. Only
    // the writable head is read fresh, because same-epoch upper writes
    // advance its sequence; the head/lease fence runs in ensure_current.
    let guard = meta.guard().await;
    let cached: [LayerRecord; 2] = meta
        .chain()
        .await?
        .try_into()
        .map_err(|_| read_workspace_error(WorkspaceError::Fenced))?;
    let head = meta
        .store
        .load_layer(guard.expected_head_layer_id)
        .await
        .map_err(read_workspace_error)?;
    let layers = [head, cached[1].clone()];
    if layers[1].layer_id != lower.binding.base_layer_id {
        return Err(WorkspaceError::Fenced.into());
    }
    validate_fixed_layer_pair(&layers).map_err(read_workspace_error)?;
    Ok((guard, layers, None))
}

type CapturedInodeFence = (
    HeadGuard,
    [LayerRecord; 2],
    Option<crate::workspace_overlay::packed_reader_lifecycle::PackedReaderRequestOwner>,
    Vec<crate::workspace_overlay::model::InodeDelta>,
    bool,
);

/// Capture the read fence plus one inode's upper rows. A frozen base on a
/// consistent store does this in one round trip that already validates the
/// head/lease fence (`fused`); otherwise it is the separate fence sequence.
/// A frozen-base request's view, captured atomically by `begin_request`
/// (head/lease fence plus this inode's upper rows in one read). While a read
/// runs inside `with_request_view`, chunk preparation reuses it instead of
/// re-capturing, and execution skips its per-chunk fence round trips: the
/// request's single final `ensure_current` re-validates the same inode-row
/// fence after all bytes are read, so a lost lease or any same-epoch upper
/// write to this file still fails the request (and the reader retries it)
/// before bytes are returned. Work outside the scope uses the full path.
#[derive(Clone)]
pub struct RequestView {
    store_identity: usize,
    ino: i64,
    guard: HeadGuard,
    layers: [LayerRecord; 2],
    inodes: Vec<crate::workspace_overlay::model::InodeDelta>,
}

tokio::task_local! {
    static REQUEST_VIEW: RequestView;
}

/// Run a read under the request view its fence captured, if any.
pub(crate) async fn with_request_view<F: std::future::Future>(
    fence: &dyn UnifiedReadRequestFence,
    future: F,
) -> F::Output {
    match fence.request_view() {
        Some(view) => REQUEST_VIEW.scope(view, future).await,
        None => future.await,
    }
}

fn current_request_view(store_identity: usize, ino: i64) -> Option<RequestView> {
    REQUEST_VIEW
        .try_with(|view| {
            (view.store_identity == store_identity && view.ino == ino).then(|| view.clone())
        })
        .ok()
        .flatten()
}

async fn capture_inode_fence<W: WorkspaceStore + 'static>(
    meta: &WorkspaceMetaLayer<W>,
    lower: &WorkspacePackedLower,
    ino: i64,
) -> anyhow::Result<CapturedInodeFence> {
    let fused = meta.store.supports_fused_read_fence();
    if fused && let Some((layers, rows)) = meta.lease_proven_inode_rows(ino).await? {
        // Lease-proven: this mount is the only upper writer and every local
        // commit invalidates the rows it writes, so the cached head/base pair
        // and inode rows are the store's. The same rows are re-checked
        // locally (and remotely on any doubt) before bytes are returned.
        if layers[1].layer_id != lower.binding.base_layer_id {
            return Err(WorkspaceError::Fenced.into());
        }
        validate_fixed_layer_pair(&layers).map_err(read_workspace_error)?;
        return Ok((
            meta.guard().await,
            layers,
            None,
            rows.as_ref().clone(),
            true,
        ));
    }
    Ok(if fused {
        // Frozen base: one consistent round trip carries the head/lease fence,
        // the fresh writable head and this inode's rows in both layers.
        let guard = meta.guard().await;
        let cached: [LayerRecord; 2] = meta
            .chain()
            .await?
            .try_into()
            .map_err(|_| read_workspace_error(WorkspaceError::Fenced))?;
        if cached[1].layer_id != lower.binding.base_layer_id {
            return Err(WorkspaceError::Fenced.into());
        }
        let (head, inodes) = meta
            .store
            .read_fenced_inode(guard.clone(), cached[1].clone(), ino)
            .await
            .map_err(read_workspace_error)?;
        let layers = [head, cached[1].clone()];
        validate_fixed_layer_pair(&layers).map_err(read_workspace_error)?;
        (guard, layers, None, inodes, true)
    } else {
        let (guard, layers, reader_owner) = capture_read_fence(meta, lower).await?;
        // Read direct upper rows before any cached EOF can suppress preparation.
        // Only true upper absence consults the authenticated readonly lower.
        let inodes = meta
            .store
            .get_inode_deltas(InodeQuery {
                layer_ids: layers.iter().map(|layer| layer.layer_id).collect(),
                ino,
            })
            .await
            .map_err(read_workspace_error)?;
        (guard, layers, reader_owner, inodes, false)
    })
}

/// Frozen base on a consistent store: fence on this inode's upper rows
/// (every data/attr mutation rewrites them with a new data_version), so
/// writes to unrelated files do not restart this read.
#[derive(Clone)]
struct InodeFence {
    ino: i64,
    rows: Vec<crate::workspace_overlay::model::InodeDelta>,
}

/// Fence check: local when the lease-proven rows are unchanged, otherwise the
/// remote per-inode fence. The remote fence still fails closed on any change.
async fn validate_fence_or_local<W: WorkspaceStore + 'static>(
    store: &W,
    guard: &HeadGuard,
    layers: &[LayerRecord; 2],
    inode_fence: Option<&InodeFence>,
    local: Option<&super::row_cache::LeaseLocalFence>,
) -> anyhow::Result<()> {
    if let (Some(fence), Some(local)) = (inode_fence, local)
        && local.unchanged(fence.ino, &fence.rows)
    {
        return Ok(());
    }
    validate_fence(store, guard, layers, inode_fence).await
}

async fn validate_fence<W: WorkspaceStore + 'static>(
    store: &W,
    guard: &HeadGuard,
    layers: &[LayerRecord; 2],
    inode_fence: Option<&InodeFence>,
) -> anyhow::Result<()> {
    match inode_fence {
        Some(fence) => store
            .validate_inode_read_fence(
                guard.clone(),
                layers[1].clone(),
                fence.ino,
                fence.rows.clone(),
            )
            .await
            .map_err(read_workspace_error),
        None => store
            .validate_read_fence(guard.clone(), layers.clone())
            .await
            .map_err(read_workspace_error),
    }
}

struct PackedReadRequestFence<W> {
    store: Arc<W>,
    guard: HeadGuard,
    layers: [LayerRecord; 2],
    inode_fence: Option<InodeFence>,
    /// Frozen + lease-bound mounts: re-check the inode fence locally.
    local_fence: Option<super::row_cache::LeaseLocalFence>,
    request_view: Option<RequestView>,
    lower: Arc<WorkspacePackedLower>,
    file_size: u64,
    _reader_owner:
        Option<crate::workspace_overlay::packed_reader_lifecycle::PackedReaderRequestOwner>,
    _permit: V3OwnedPermit,
}

#[async_trait]
impl<W: WorkspaceStore + 'static> UnifiedReadRequestFence for PackedReadRequestFence<W> {
    fn file_size(&self) -> u64 {
        self.file_size
    }

    fn request_view(&self) -> Option<RequestView> {
        self.request_view.clone()
    }

    async fn ensure_current(&self) -> anyhow::Result<()> {
        // The immutable lower needs no binding authentication; the writable
        // head/lease fence is one MGET and still applies to every read request.
        validate_fence_or_local(
            self.store.as_ref(),
            &self.guard,
            &self.layers,
            self.inode_fence.as_ref(),
            self.local_fence.as_ref(),
        )
        .await
    }
}

pub(super) async fn begin_request<W: WorkspaceStore + 'static>(
    meta: &WorkspaceMetaLayer<W>,
    lower: Arc<WorkspacePackedLower>,
    ino: i64,
) -> anyhow::Result<Arc<dyn UnifiedReadRequestFence>> {
    let permit = lower
        .budget
        .admit(&[(V3BudgetPool::Control, REQUEST_FENCE_BYTES)])
        .map_err(budget_to_meta)?;
    let (guard, layers, reader_owner, inodes, fused) =
        capture_inode_fence(meta, &lower, ino).await?;
    let file_size =
        match resolve_inode_state(&layers, &inodes, ino).map_err(read_workspace_error)? {
            Resolution::Present(inode) => {
                if inode.inode.kind != 0 {
                    return Err(MetaError::NotSupported(
                        "read request requires a regular upper file".into(),
                    )
                    .into());
                }
                inode.inode.size
            }
            Resolution::Masked => return Err(MetaError::NotFound(ino).into()),
            Resolution::Absent => {
                let attr = lower
                    .metadata
                    .stat_fresh(ino)
                    .await?
                    .ok_or(MetaError::NotFound(ino))?;
                if attr.kind != FileType::File {
                    return Err(MetaError::NotSupported(
                        "read request requires a regular lower file".into(),
                    )
                    .into());
                }
                attr.size
            }
        };
    let inode_fence = fused.then(|| InodeFence {
        ino,
        rows: inodes.clone(),
    });
    let request_view = fused.then(|| RequestView {
        store_identity: Arc::as_ptr(&meta.store) as *const () as usize,
        ino,
        guard: guard.clone(),
        layers: layers.clone(),
        inodes: inodes.clone(),
    });
    let local_fence = if fused {
        meta.lease_local_fence()
    } else {
        None
    };
    let request = Arc::new(PackedReadRequestFence {
        store: meta.store.clone(),
        guard,
        layers,
        inode_fence,
        local_fence,
        request_view,
        lower,
        file_size,
        _reader_owner: reader_owner,
        _permit: permit,
    });
    if !fused {
        // The fused read already validated this exact fence atomically.
        request.ensure_current().await?;
    }
    Ok(request)
}

struct NativeUpperFetcher<S> {
    store: Arc<S>,
    layout: ChunkLayout,
}

#[async_trait]
impl<S: BlockStore + Send + Sync + 'static> UnifiedReadSourceFetcher for NativeUpperFetcher<S> {
    async fn read_source(&self, source: &ReadSource, output: &mut [u8]) -> anyhow::Result<()> {
        match source {
            ReadSource::Hole => output.fill(0),
            ReadSource::UpperBlock { key, block_offset } => {
                self.store.read_range(*key, *block_offset, output).await?;
            }
            ReadSource::LegacySlice {
                slice_id,
                slice_offset,
            } => {
                let plan = ResolvedReadPlan {
                    segments: vec![ReadPlanSegment::Data {
                        logical_offset: 0,
                        length: output.len() as u64,
                        slice_id: *slice_id,
                        slice_offset: *slice_offset,
                    }],
                };
                execute_into(self.store.as_ref(), self.layout, 0, &plan, output).await?;
            }
            ReadSource::PackedFrame { .. } | ReadSource::PackedInline { .. } => {
                anyhow::bail!("packed source sent to native upper fetcher");
            }
        }
        Ok(())
    }
}

/// Retains original child plans/fetchers, their readonly epoch0 and all permits.
/// Only the composed logical plan uses the outer workspace generation.
struct CompositeFetcher<W> {
    store: Arc<W>,
    guard: HeadGuard,
    layers: [LayerRecord; 2],
    inode_fence: Option<InodeFence>,
    /// Prepared inside a request view: that request's final fence covers
    /// this plan, so the executor's per-chunk checks need no round trip.
    request_fenced: bool,
    generation: ReadGeneration,
    lower: Arc<WorkspacePackedLower>,
    children: Vec<PreparedUnifiedRead>,
    _reader_owner:
        Option<crate::workspace_overlay::packed_reader_lifecycle::PackedReaderRequestOwner>,
    _permits: Vec<V3OwnedPermit>,
}

#[async_trait]
impl<W: WorkspaceStore + 'static> UnifiedReadSourceFetcher for CompositeFetcher<W> {
    async fn ensure_generation(&self, generation: ReadGeneration) -> anyhow::Result<()> {
        if generation != self.generation {
            return Err(ReadViewChanged.into());
        }
        // Head/lease fence (per inode): one round trip, unless
        // the enclosing request's final fence already covers this plan.
        if !self.request_fenced {
            validate_fence(
                self.store.as_ref(),
                &self.guard,
                &self.layers,
                self.inode_fence.as_ref(),
            )
            .await?;
        }
        for child in &self.children {
            child
                .fetcher
                .ensure_generation(child.plan.generation)
                .await?;
        }
        Ok(())
    }

    async fn read_source(&self, source: &ReadSource, output: &mut [u8]) -> anyhow::Result<()> {
        match source {
            ReadSource::Hole | ReadSource::UpperBlock { .. } | ReadSource::LegacySlice { .. } => {
                self.lower.upper.read_source(source, output).await
            }
            ReadSource::PackedFrame { .. } | ReadSource::PackedInline { .. } => {
                // Dispatch only a source in an actually retained authenticated
                // child. Never reconstruct a recipe from container offsets.
                // Overlay composition may split one lower frame at an upper
                // boundary. `shift_source` then advances only `raw_offset`,
                // so comparing the complete source value would reject a
                // valid child-owned subrange. Match the immutable frame
                // identity and validate that the requested subrange remains
                // inside the retained child segment instead.
                let child = self
                    .children
                    .iter()
                    .find(|child| {
                        child.plan.segments.iter().any(|segment| {
                            packed_source_covered_by(&segment.source, source, output.len() as u64)
                        })
                    })
                    .ok_or_else(|| {
                        anyhow::anyhow!("packed source has no retained authenticated owner")
                    })?;
                child
                    .fetcher
                    .ensure_generation(child.plan.generation)
                    .await?;
                child.fetcher.read_source(source, output).await
            }
        }
    }
}

fn packed_source_covered_by(
    owner: &ReadSource,
    requested: &ReadSource,
    requested_len: u64,
) -> bool {
    match (owner, requested) {
        (
            ReadSource::PackedFrame {
                group_id: owner_group,
                container_ordinal: owner_container,
                frame_ordinal: owner_frame,
                object_offset: owner_object,
                stored_len: owner_stored,
                raw_offset: owner_raw,
                raw_len: owner_raw_len,
                size_class: owner_class,
                codec: owner_codec,
                frame_digest: owner_digest,
            },
            ReadSource::PackedFrame {
                group_id: requested_group,
                container_ordinal: requested_container,
                frame_ordinal: requested_frame,
                object_offset: requested_object,
                stored_len: requested_stored,
                raw_len: requested_raw_len,
                size_class: requested_class,
                codec: requested_codec,
                frame_digest: requested_digest,
                raw_offset: requested_raw,
            },
        ) => {
            owner_group == requested_group
                && owner_container == requested_container
                && owner_frame == requested_frame
                && owner_object == requested_object
                && owner_stored == requested_stored
                && owner_raw_len == requested_raw_len
                && owner_class == requested_class
                && owner_codec == requested_codec
                && owner_digest == requested_digest
                && u64::from(*requested_raw) >= u64::from(*owner_raw)
                && u64::from(*requested_raw)
                    .checked_add(requested_len)
                    .is_some_and(|end| end <= u64::from(*owner_raw) + u64::from(*owner_raw_len))
        }
        (
            ReadSource::PackedInline {
                data: owner_data,
                raw_offset: owner_raw,
            },
            ReadSource::PackedInline {
                data: requested_data,
                raw_offset: requested_raw,
            },
        ) => {
            std::sync::Arc::ptr_eq(owner_data, requested_data)
                && u64::from(*requested_raw) >= u64::from(*owner_raw)
                && u64::from(*requested_raw)
                    .checked_add(requested_len)
                    .is_some_and(|end| end <= u64::from(*owner_raw) + owner_data.len() as u64)
        }
        _ => false,
    }
}

pub(super) async fn prepare<W: WorkspaceStore + 'static>(
    meta: &WorkspaceMetaLayer<W>,
    lower: Arc<WorkspacePackedLower>,
    ino: i64,
    chunk_index: u64,
    offset: u64,
    len: u64,
    delivery: Option<Arc<OperationDelivery>>,
) -> Result<PreparedUnifiedRead, MetaError> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| MetaError::Internal("workspace read range overflows".into()))?;
    if end > meta.chunk_size || len > lower.budget.max_read_bytes() as u64 {
        return Err(MetaError::Internal(
            "workspace prepared range exceeds mount limit".into(),
        ));
    }
    let base = chunk_index
        .checked_mul(meta.chunk_size)
        .ok_or_else(|| MetaError::Internal("workspace chunk base overflows".into()))?;
    let scoped = current_request_view(Arc::as_ptr(&meta.store) as *const () as usize, ino);
    let in_request = scoped.is_some();
    let (guard, layers, reader_owner, inodes, fused) = match scoped {
        Some(view) => (view.guard, view.layers, None, view.inodes, true),
        None => capture_inode_fence(meta, &lower, ino)
            .await
            .map_err(fetch_to_meta)?,
    };
    // Reserve all bounded upper preparation/bookkeeping before backend query.
    // This read holds no other Plans charge yet, so it waits for sibling
    // arenas (which shrink before lower preparation) instead of failing.
    let mut arena = lower
        .budget
        .admit_waiting_with_headroom(
            V3BudgetPool::Plans,
            UPPER_PREPARATION_BYTES,
            UPPER_ARENA_HEADROOM_BYTES,
        )
        .await
        .map_err(budget_to_meta)?;
    let mut permits = vec![
        lower
            .budget
            .admit(&[(V3BudgetPool::Control, 4096)])
            .map_err(budget_to_meta)?,
    ];
    let fence_rows = inodes.clone();
    let inode_state = resolve_inode_state(&layers, &inodes, ino).map_err(read_workspace_to_meta)?;
    let mut lower_attr = None;
    let visible_size = match inode_state {
        Resolution::Present(inode) => {
            if inode.inode.kind != 0 {
                return Err(MetaError::NotSupported(
                    "prepared read requires a regular upper file".into(),
                ));
            }
            inode.inode.size
        }
        Resolution::Masked => return Err(MetaError::NotFound(ino)),
        Resolution::Absent => {
            let attr = lower
                .metadata
                .stat_fresh(ino)
                .await?
                .ok_or(MetaError::NotFound(ino))?;
            if attr.kind != FileType::File {
                return Err(MetaError::NotSupported(
                    "prepared read requires a regular lower file".into(),
                ));
            }
            let size = attr.size;
            lower_attr = Some(attr);
            size
        }
    };
    let logical_size = visible_size.saturating_sub(base).min(meta.chunk_size);
    if end > logical_size {
        return Err(MetaError::Anyhow(
            crate::chunk::read_plan::ReadRequestBeyondView.into(),
        ));
    }
    let rows = meta
        .store
        .get_extent_deltas_bounded(
            ExtentQuery {
                layer_ids: layers.iter().map(|layer| layer.layer_id).collect(),
                ino,
                chunk_index,
                range_start: offset,
                range_end: end,
            },
            MAX_UPPER_EXTENT_ROWS,
        )
        .await
        .map_err(read_workspace_to_meta)?;
    let coverage = resolve_extent_coverage(&layers, &rows, ino, chunk_index, offset..end)
        .map_err(read_workspace_to_meta)?;
    // Backend rows are dropped here. Only resolved upper segments, their gaps
    // and per-gap bookkeeping stay live; each segment yields at most one gap.
    let retained_rows = rows.len() as u64 + 2;
    drop(rows);
    arena
        .shrink(
            V3BudgetPool::Plans,
            retained_rows
                .saturating_mul(4 * std::mem::size_of::<LogicalSegment>() as u64)
                .saturating_add(4096)
                .min(UPPER_PREPARATION_BYTES),
        )
        .map_err(budget_to_meta)?;
    permits.push(arena);
    let upper = coverage.covered.into_iter().map(|extent| LogicalSegment {
        logical_offset: extent.logical_offset,
        length: extent.length,
        source: match extent.kind {
            ExtentKind::Hole => ReadSource::Hole,
            ExtentKind::Data {
                slice_id,
                slice_offset,
            } => ReadSource::LegacySlice {
                slice_id,
                slice_offset,
            },
        },
    });
    let generation = ReadGeneration {
        workspace_head_epoch: guard.expected_head_epoch,
        // Frozen base: this inode's upper data version identifies its view;
        // unrelated upper writes do not invalidate the read.
        workspace_mutation_sequence: if fused {
            inodes
                .iter()
                .filter(|row| row.layer_id == layers[0].layer_id)
                .map(|row| row.sequence)
                .max()
                .unwrap_or(0)
        } else {
            layers[0].next_sequence
        },
        lower_snapshot: lower.binding.manifest.digest,
    };
    let preparation = prepare_overlay_plan(generation, logical_size, offset, len, upper)
        .map_err(|error| MetaError::Internal(error.to_string()))?;
    let mut children = Vec::new();
    let mut gap_plans = Vec::new();
    if !preparation.lower_gaps().is_empty() {
        // No lower metadata query whatsoever for complete upper Data/Hole
        // coverage with a present upper inode. Authenticate EOF/absence once,
        // then prepare only the exact gaps that intersect that lower inode.
        if lower_attr.is_none() {
            lower_attr = lower.metadata.stat_fresh(ino).await?;
        }
        if lower_attr
            .as_ref()
            .is_some_and(|attr| attr.kind != FileType::File)
        {
            return Err(MetaError::Internal(
                "packed lower file type disagrees with upper file".into(),
            ));
        }
        let lower_size = lower_attr.as_ref().map_or(0, |attr| {
            attr.size.saturating_sub(base).min(meta.chunk_size)
        });
        for gap in preparation.lower_gaps() {
            let mut gap_plan = UnifiedReadPlan {
                generation,
                logical_size,
                segments: Vec::new(),
            };
            let lower_end = gap.end.min(lower_size);
            if gap.start < lower_end {
                let child = lower
                    .metadata
                    .prepare_unified_read_observed(
                        ino,
                        chunk_index,
                        gap.start,
                        lower_end - gap.start,
                        delivery.clone(),
                    )
                    .await?
                    .ok_or_else(|| {
                        MetaError::Internal("packed lower supplied no prepared gap".into())
                    })?;
                if child.plan.generation != ReadGeneration::readonly(lower.binding.manifest.digest)
                    || child.plan.segments.iter().any(|segment| {
                        !matches!(
                            segment.source,
                            ReadSource::Hole
                                | ReadSource::PackedFrame { .. }
                                | ReadSource::PackedInline { .. }
                        )
                    })
                {
                    return Err(MetaError::Internal(
                        "packed child identity/source mismatch".into(),
                    ));
                }
                child
                    .plan
                    .validate(gap.start, lower_end - gap.start)
                    .map_err(|error| MetaError::Internal(error.to_string()))?;
                child
                    .fetcher
                    .ensure_generation(child.plan.generation)
                    .await
                    .map_err(fetch_to_meta)?;
                let clone_bytes = (child.plan.segments.len() as u64)
                    .checked_mul(2 * std::mem::size_of::<LogicalSegment>() as u64)
                    .ok_or_else(|| MetaError::Internal("composed plan charge overflows".into()))?;
                // Held charges here are already shrunk to their real size, so
                // waiting depends only on sibling arenas that shrink or finish.
                permits.push(
                    lower
                        .budget
                        .admit_waiting_for(V3BudgetPool::Plans, clone_bytes)
                        .await
                        .map_err(budget_to_meta)?,
                );
                gap_plan.segments = child.plan.segments.clone();
                // Keep original child generation/owners intact. The clone is
                // lifted only after authenticated binding/fence validation.
                children.push(child);
            }
            gap_plans.push(gap_plan);
        }
    }
    let plan = preparation
        .finish(gap_plans)
        .map_err(|error| MetaError::Internal(error.to_string()))?;
    let inode_fence = fused.then_some(InodeFence {
        ino,
        rows: fence_rows,
    });
    let fetcher = Arc::new(CompositeFetcher {
        store: meta.store.clone(),
        guard,
        layers,
        inode_fence,
        request_fenced: in_request,
        generation,
        lower,
        children,
        _reader_owner: reader_owner,
        _permits: permits,
    });
    if fused {
        // The fused capture validated this exact head/lease view atomically,
        // and execution re-checks it before and after the data. Only the
        // retained lower children still need their own generation check.
        for child in &fetcher.children {
            child
                .fetcher
                .ensure_generation(child.plan.generation)
                .await
                .map_err(fetch_to_meta)?;
        }
    } else {
        fetcher
            .ensure_generation(generation)
            .await
            .map_err(fetch_to_meta)?;
    }
    Ok(PreparedUnifiedRead { plan, fetcher })
}

#[cfg(test)]
mod packed_source_owner_tests {
    use std::sync::Arc;

    use super::packed_source_covered_by;
    use crate::chunk::read_plan::ReadSource;

    fn frame(raw_offset: u32, digest: [u8; 16]) -> ReadSource {
        ReadSource::PackedFrame {
            group_id: 7,
            container_ordinal: 2,
            frame_ordinal: 3,
            object_offset: 4096,
            stored_len: 512,
            raw_offset,
            raw_len: 1024,
            size_class: 1,
            codec: 0,
            frame_digest: digest,
        }
    }

    #[test]
    fn split_frame_subrange_keeps_authenticated_owner() {
        assert!(packed_source_covered_by(
            &frame(0, [9; 16]),
            &frame(512, [9; 16]),
            128
        ));
        assert!(!packed_source_covered_by(
            &frame(0, [9; 16]),
            &frame(960, [9; 16]),
            128
        ));
        assert!(!packed_source_covered_by(
            &frame(0, [9; 16]),
            &frame(512, [8; 16]),
            128
        ));
    }

    #[test]
    fn inline_owner_requires_the_same_retained_payload() {
        let payload: Arc<[u8]> = Arc::from(vec![1u8; 1024]);
        let owner = ReadSource::PackedInline {
            data: payload.clone(),
            raw_offset: 0,
        };
        let split = ReadSource::PackedInline {
            data: payload,
            raw_offset: 512,
        };
        assert!(packed_source_covered_by(&owner, &split, 128));

        let copied = ReadSource::PackedInline {
            data: Arc::from(vec![1u8; 1024]),
            raw_offset: 512,
        };
        assert!(!packed_source_covered_by(&owner, &copied, 128));
    }
}
