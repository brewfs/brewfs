//! Persistence contract for workspace metadata.

use async_trait::async_trait;
use uuid::Uuid;

use super::digest::CanonicalLayerDelta;
use super::error::WorkspaceError;
use super::ids::{JournalId, LayerId, LeaseId, SnapshotId, WorkspaceId};
use super::model::{
    AclDelta, BaseRevision, CommitResult, DataExtentDelta, DentryDelta, InodeDelta, LayerRecord,
    SealJournal, SealPhase, SealResult, SnapshotLease, SnapshotRecord, VolumeHeader,
    WorkspaceRecord, XattrDelta,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkspaceStoreCapabilities {
    pub atomic_head_switch: bool,
    pub durable_lease: bool,
    pub transactional_namespace_mutation: bool,
    pub transactional_rename: bool,
    pub watch_head_change: bool,
}

impl WorkspaceStoreCapabilities {
    pub fn validate_for_v1_mount(self) -> Result<(), WorkspaceError> {
        for (available, name) in [
            (self.atomic_head_switch, "atomic_head_switch"),
            (self.durable_lease, "durable_lease"),
            (
                self.transactional_namespace_mutation,
                "transactional_namespace_mutation",
            ),
            (self.transactional_rename, "transactional_rename"),
        ] {
            if !available {
                return Err(WorkspaceError::UnsupportedCapability(name));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadGuard {
    pub workspace_id: WorkspaceId,
    pub expected_head_layer_id: LayerId,
    pub expected_head_epoch: u64,
    pub lease_id: LeaseId,
    pub holder_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateVolumeRoot {
    pub volume_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub root_layer_id: LayerId,
    pub writable_layer_id: LayerId,
    pub owner_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcquireLease {
    pub workspace_id: WorkspaceId,
    pub lease_id: LeaseId,
    pub holder_generation: u64,
    pub ttl_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenewLease {
    pub lease_id: LeaseId,
    pub holder_generation: u64,
    pub ttl_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseLease {
    pub lease_id: LeaseId,
    pub holder_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateWorkspace {
    pub workspace_id: WorkspaceId,
    pub head_layer_id: LayerId,
    pub base_revision: BaseRevision,
    pub owner_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSnapshot {
    pub snapshot_id: SnapshotId,
    pub name: Option<String>,
    pub revision: BaseRevision,
    pub owner_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginSeal {
    pub guard: HeadGuard,
    pub journal_id: JournalId,
    pub new_head_layer_id: LayerId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvanceSeal {
    pub journal_id: JournalId,
    pub expected_phase: SealPhase,
    pub next_phase: SealPhase,
    pub pending_bytes: Option<u64>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbortSeal {
    pub journal_id: JournalId,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FastForwardCommit {
    pub source_revision: BaseRevision,
    pub source_fork_base: BaseRevision,
    pub target_workspace_id: WorkspaceId,
    pub target_expected_head_layer_id: LayerId,
    pub target_expected_head_epoch: u64,
    pub new_head_layer_id: LayerId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkDeleting {
    pub workspace_id: WorkspaceId,
    pub force_fence_lease: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SliceReference {
    pub layer_id: LayerId,
    pub slice_id: u64,
    pub slice_end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordOrphanSlice {
    pub orphan_layer_id: LayerId,
    pub slice_id: u64,
    pub slice_end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcSnapshot {
    pub root_layers: Vec<LayerId>,
    pub layers: Vec<LayerRecord>,
    pub slice_references: Vec<SliceReference>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteLayerMetadata {
    pub layer_ids: Vec<LayerId>,
    pub now_ns: i64,
    pub lease_grace_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallCompaction {
    pub workspace_id: WorkspaceId,
    pub expected_head_layer_id: LayerId,
    pub expected_head_epoch: u64,
    pub expected_parent_layer_id: LayerId,
    pub compacted_layer_id: LayerId,
    pub replacement_head_layer_id: LayerId,
    pub delta: CanonicalLayerDelta,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionResult {
    pub revision: BaseRevision,
    pub replacement_head_layer_id: LayerId,
    pub head_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DentryQuery {
    pub layer_ids: Vec<LayerId>,
    pub parent_ino: i64,
    pub name: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InodeQuery {
    pub layer_ids: Vec<LayerId>,
    pub ino: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentQuery {
    pub layer_ids: Vec<LayerId>,
    pub ino: i64,
    pub chunk_index: u64,
    pub range_start: u64,
    pub range_end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XattrQuery {
    pub layer_ids: Vec<LayerId>,
    pub ino: i64,
    pub name: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AclQuery {
    pub layer_ids: Vec<LayerId>,
    pub ino: i64,
    pub acl_type: Option<u8>,
    pub acl_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceMutation {
    pub guard: HeadGuard,
    pub dentries: Vec<DentryDelta>,
    pub inodes: Vec<InodeDelta>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InodeMutation {
    pub guard: HeadGuard,
    pub inode: InodeDelta,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendDataExtent {
    pub guard: HeadGuard,
    pub extent: DataExtentDelta,
    pub chunk_size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataMutation {
    pub guard: HeadGuard,
    pub inode: InodeDelta,
    pub extents: Vec<DataExtentDelta>,
    pub chunk_size: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataMutationResult {
    pub inode: InodeDelta,
    pub extents: Vec<DataExtentDelta>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XattrMutation {
    pub guard: HeadGuard,
    /// Full inode snapshot carrying the ctime change for this xattr operation.
    /// Both records are committed atomically by the backend.
    pub inode: InodeDelta,
    pub xattr: XattrDelta,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AclMutation {
    pub guard: HeadGuard,
    pub acl: AclDelta,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationResult {
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
}

#[async_trait]
pub trait WorkspaceStore: Send + Sync {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> WorkspaceStoreCapabilities;

    async fn initialize_workspace_schema(&self) -> Result<(), WorkspaceError>;
    async fn load_volume_header(&self) -> Result<Option<VolumeHeader>, WorkspaceError>;
    async fn load_workspace(&self, id: WorkspaceId) -> Result<WorkspaceRecord, WorkspaceError>;
    async fn load_layer(&self, id: LayerId) -> Result<LayerRecord, WorkspaceError>;
    async fn load_layer_chain(&self, head: LayerId) -> Result<Vec<LayerRecord>, WorkspaceError>;
    async fn allocate_id(&self, name: &str) -> Result<i64, WorkspaceError>;
    async fn create_volume_root(
        &self,
        request: CreateVolumeRoot,
    ) -> Result<WorkspaceRecord, WorkspaceError>;
    async fn create_workspace(
        &self,
        request: CreateWorkspace,
    ) -> Result<WorkspaceRecord, WorkspaceError>;
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceError>;
    async fn create_snapshot(
        &self,
        request: CreateSnapshot,
    ) -> Result<SnapshotRecord, WorkspaceError>;
    async fn load_snapshot(&self, id: SnapshotId) -> Result<SnapshotRecord, WorkspaceError>;
    async fn list_snapshots(&self) -> Result<Vec<SnapshotRecord>, WorkspaceError>;
    async fn delete_snapshot(&self, id: SnapshotId) -> Result<(), WorkspaceError>;

    async fn acquire_lease(&self, request: AcquireLease) -> Result<SnapshotLease, WorkspaceError>;
    async fn renew_lease(&self, request: RenewLease) -> Result<SnapshotLease, WorkspaceError>;
    async fn release_lease(&self, request: ReleaseLease) -> Result<(), WorkspaceError>;
    async fn reap_expired_leases(&self) -> Result<u64, WorkspaceError>;
    async fn list_leases(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<SnapshotLease>, WorkspaceError>;

    async fn get_dentry_deltas(
        &self,
        request: DentryQuery,
    ) -> Result<Vec<DentryDelta>, WorkspaceError>;
    async fn get_inode_deltas(
        &self,
        request: InodeQuery,
    ) -> Result<Vec<InodeDelta>, WorkspaceError>;
    async fn get_extent_deltas(
        &self,
        request: ExtentQuery,
    ) -> Result<Vec<DataExtentDelta>, WorkspaceError>;
    async fn get_xattr_deltas(
        &self,
        request: XattrQuery,
    ) -> Result<Vec<XattrDelta>, WorkspaceError>;
    async fn get_acl_deltas(&self, request: AclQuery) -> Result<Vec<AclDelta>, WorkspaceError>;

    async fn apply_namespace_mutation(
        &self,
        request: NamespaceMutation,
    ) -> Result<MutationResult, WorkspaceError>;
    async fn apply_inode_mutation(
        &self,
        request: InodeMutation,
    ) -> Result<InodeDelta, WorkspaceError>;
    async fn append_data_extent(
        &self,
        request: AppendDataExtent,
    ) -> Result<DataExtentDelta, WorkspaceError>;
    async fn apply_data_mutation(
        &self,
        request: DataMutation,
    ) -> Result<DataMutationResult, WorkspaceError>;
    async fn apply_xattr_mutation(&self, request: XattrMutation) -> Result<(), WorkspaceError>;
    async fn apply_acl_mutation(&self, request: AclMutation) -> Result<(), WorkspaceError>;

    async fn load_layer_delta(
        &self,
        layer_id: LayerId,
    ) -> Result<CanonicalLayerDelta, WorkspaceError>;
    async fn begin_seal(&self, request: BeginSeal) -> Result<SealJournal, WorkspaceError>;
    async fn advance_seal(&self, request: AdvanceSeal) -> Result<SealJournal, WorkspaceError>;
    async fn hash_seal(&self, journal_id: JournalId) -> Result<SealJournal, WorkspaceError>;
    async fn commit_seal(&self, journal_id: JournalId) -> Result<SealResult, WorkspaceError>;
    async fn abort_recoverable_seal(&self, request: AbortSeal) -> Result<(), WorkspaceError>;
    async fn load_seal_journal(&self, journal_id: JournalId)
    -> Result<SealJournal, WorkspaceError>;
    async fn list_incomplete_seal_journals(&self) -> Result<Vec<SealJournal>, WorkspaceError>;
    async fn fast_forward_commit(
        &self,
        request: FastForwardCommit,
    ) -> Result<CommitResult, WorkspaceError>;
    async fn mark_workspace_deleting(&self, request: MarkDeleting) -> Result<(), WorkspaceError>;
    async fn record_orphan_slice(&self, request: RecordOrphanSlice) -> Result<(), WorkspaceError>;
    async fn gc_snapshot(
        &self,
        now_ns: i64,
        lease_grace_ns: u64,
    ) -> Result<GcSnapshot, WorkspaceError>;
    async fn delete_layer_metadata(
        &self,
        request: DeleteLayerMetadata,
    ) -> Result<(), WorkspaceError>;
    async fn finalize_layer_metadata_deletion(
        &self,
        layer_ids: Vec<LayerId>,
    ) -> Result<(), WorkspaceError>;
    async fn install_compaction(
        &self,
        request: InstallCompaction,
    ) -> Result<CompactionResult, WorkspaceError>;
}
