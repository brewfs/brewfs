//! Persistence contract for workspace metadata.

use async_trait::async_trait;
use uuid::Uuid;

use super::digest::CanonicalLayerDelta;
use super::error::WorkspaceError;
use super::ids::{JournalId, LayerId, LeaseId, SnapshotId, WorkspaceId};
use super::model::{
    AclDelta, BaseRevision, DataExtentDelta, DentryDelta, InodeDelta, InodeState, LayerRecord,
    LayerState, SealJournal, SnapshotLease, SnapshotRecord, ValueOp, VolumeHeader, WorkspaceRecord,
    XattrDelta,
};

/// Shared by packed extent readers and writable-head publication admission.
pub(crate) const MAX_UPPER_EXTENT_ROWS: usize = 1024;

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

/// Independent, versioned packed-lower identity. This is deliberately not a
/// field of BaseRevision or any existing bincode model. Storage/publication
/// implementations must pin the complete authenticated PM11 reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedLowerBinding {
    pub binding_version: u64,
    pub base_layer_id: LayerId,
    pub manifest: super::packed_v3::wire005::V3ObjectRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateVolumeRoot {
    pub volume_format: String,
    pub schema_version: u32,
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

/// Test-only: materialize an offline workspace's resolved chain as a new flat
/// immutable lower. Production lowers come only from packed publication.
#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallTestBase {
    pub workspace_id: WorkspaceId,
    pub expected_head_layer_id: LayerId,
    pub expected_head_epoch: u64,
    pub expected_parent_layer_id: LayerId,
    pub compacted_layer_id: LayerId,
    pub replacement_head_layer_id: LayerId,
    pub delta: CanonicalLayerDelta,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestBaseResult {
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

/// One raw-name ordered page. Admission precedes transport/decoding and the
/// reservation remains with the rows until their final consumer drops them.
pub struct WorkspaceNamePage<T> {
    pub rows: Vec<T>,
    pub memory_guard: crate::meta::layer::MetadataMemoryGuard,
    /// The backend proved no row follows this page (a short page from an
    /// atomic single-range read). False never means more rows exist; callers
    /// then continue until an empty page.
    pub end: bool,
}

/// Opaque, owned completeness authority for the fixed two-layer native view.
/// Only the creating backend/budget may consume it; queries retain it through
/// their final exact authentication, including missing-alias results.
pub struct WorkspaceNativeReverseAuthority {
    pub(crate) backend_identity: usize,
    pub(crate) _backend_owner: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    pub(crate) budget_identity: usize,
    pub(crate) builds: Vec<(LayerId, Uuid)>,
    pub(crate) checks: Vec<super::stores::kv_backend::KvCheck>,
    pub(crate) _memory_guard: crate::meta::layer::MetadataMemoryGuard,
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

/// A bounded permission/creation read from one backend commit version.
/// The fixed writable-head/sealed-base pair is the only supported v3 view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionSnapshotQuery {
    pub layer_ids: [LayerId; 2],
    /// Target plus optional parent; at most two positive inode numbers.
    pub inodes: Vec<i64>,
    /// Optional prospective child identity, including its absence.
    pub dentry: Option<(i64, Vec<u8>)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionSnapshot {
    /// These exact layer versions must still match when committing a mutation.
    pub layers: [LayerRecord; 2],
    pub inodes: Vec<InodeDelta>,
    pub xattrs: Vec<XattrDelta>,
    pub dentries: Vec<DentryDelta>,
}

/// Mode, ACL bytes and a new namespace entry share a single guarded commit.
/// A changed read version returns Busy to the caller for a fresh resolve and
/// policy calculation. It cannot transparently replay stale inode/ACL input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionMutation {
    pub guard: HeadGuard,
    pub expected_layers: [LayerRecord; 2],
    pub dentries: Vec<DentryDelta>,
    pub inodes: Vec<InodeDelta>,
    pub xattrs: Vec<XattrDelta>,
}

/// Conditional commit for every operation that copies complete inode rows.
/// The caller resolves and calculates again after Busy; no stale template is
/// silently retried against a newer permission or namespace version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionedMutation {
    pub guard: HeadGuard,
    pub expected_layers: [LayerRecord; 2],
    pub dentries: Vec<DentryDelta>,
    pub inodes: Vec<InodeDelta>,
    pub xattrs: Vec<XattrDelta>,
    pub acls: Vec<AclDelta>,
    pub extents: Vec<DataExtentDelta>,
    pub chunk_size: u64,
}

impl VersionedMutation {
    pub fn empty(guard: HeadGuard, expected_layers: [LayerRecord; 2], chunk_size: u64) -> Self {
        Self {
            guard,
            expected_layers,
            dentries: Vec::new(),
            inodes: Vec::new(),
            xattrs: Vec::new(),
            acls: Vec::new(),
            extents: Vec::new(),
            chunk_size,
        }
    }

    pub fn validate(&self) -> Result<usize, WorkspaceError> {
        use crate::meta::posix_acl::{ACCESS_XATTR, DEFAULT_XATTR, PosixAcl};
        let head = self.guard.expected_head_layer_id;
        let layers = &self.expected_layers;
        crate::workspace_overlay::resolver::validate_layer_chain(head, layers)?;
        if layers[0].layer_id != head
            || layers[0].state != LayerState::Writable
            || layers[0].depth != 2
            || layers[0].parent_layer_id != Some(layers[1].layer_id)
            || layers[1].state != LayerState::Sealed
            || layers[1].depth != 1
            || layers[1].parent_layer_id.is_some()
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut inodes = std::collections::BTreeSet::new();
        for inode in &self.inodes {
            if inode.layer_id != head {
                return Err(WorkspaceError::Fenced);
            }
            if inode.ino <= 0 || !inodes.insert(inode.ino) {
                return Err(WorkspaceError::CorruptMetadata(
                    "invalid/duplicate conditional inode".into(),
                ));
            }
        }
        let mut dentries = std::collections::BTreeSet::new();
        for dentry in &self.dentries {
            if dentry.layer_id != head {
                return Err(WorkspaceError::Fenced);
            }
            dentry.validate()?;
            if !dentries.insert((dentry.parent_ino, dentry.name.clone())) {
                return Err(WorkspaceError::CorruptMetadata(
                    "duplicate conditional dentry".into(),
                ));
            }
        }
        let mut xattrs = std::collections::BTreeSet::new();
        for xattr in &self.xattrs {
            if xattr.layer_id != head {
                return Err(WorkspaceError::Fenced);
            }
            if !xattrs.insert((xattr.ino, xattr.name.clone()))
                || xattr.name.is_empty()
                || xattr.name.contains(&0)
                || !matches!(
                    (xattr.op, xattr.value.as_ref()),
                    (ValueOp::Put, Some(_)) | (ValueOp::Whiteout, None)
                )
            {
                return Err(WorkspaceError::CorruptMetadata(
                    "invalid conditional xattr".into(),
                ));
            }
            let inode = self
                .inodes
                .iter()
                .find(|inode| inode.ino == xattr.ino)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("xattr write lacks inode".into()))?;
            if xattr.name == ACCESS_XATTR || xattr.name == DEFAULT_XATTR {
                if inode.kind == 2
                    || inode.state != InodeState::Present
                    || (xattr.name == DEFAULT_XATTR && inode.kind != 1 && xattr.op == ValueOp::Put)
                {
                    return Err(WorkspaceError::CorruptMetadata(
                        "invalid POSIX ACL target kind".into(),
                    ));
                }
                if let Some(value) = &xattr.value {
                    let acl = PosixAcl::decode(value)
                        .map_err(|message| WorkspaceError::CorruptMetadata(message.into()))?;
                    if xattr.name == ACCESS_XATTR && acl.mode_bits() != inode.mode & 0o777 {
                        return Err(WorkspaceError::CorruptMetadata(
                            "mode and access ACL disagree".into(),
                        ));
                    }
                }
            }
        }
        let mut acls = std::collections::BTreeSet::new();
        for acl in &self.acls {
            if acl.layer_id != head {
                return Err(WorkspaceError::Fenced);
            }
            if acl.ino <= 0
                || !acls.insert((acl.ino, acl.acl_type, acl.acl_id))
                || !matches!(
                    (acl.op, acl.value.as_ref()),
                    (ValueOp::Put, Some(_)) | (ValueOp::Whiteout, None)
                )
            {
                return Err(WorkspaceError::CorruptMetadata(
                    "invalid conditional control ACL".into(),
                ));
            }
        }
        for extent in &self.extents {
            if extent.layer_id != head || !inodes.contains(&extent.ino) {
                return Err(WorkspaceError::Fenced);
            }
            extent.validate()?;
            if self.chunk_size == 0
                || extent
                    .logical_offset
                    .checked_add(extent.length)
                    .is_none_or(|end| end > self.chunk_size)
            {
                return Err(WorkspaceError::CorruptMetadata(
                    "conditional extent exceeds chunk".into(),
                ));
            }
        }
        [
            self.dentries.len(),
            self.inodes.len(),
            self.xattrs.len(),
            self.acls.len(),
            self.extents.len(),
        ]
        .into_iter()
        .try_fold(0usize, |count, len| {
            count.checked_add(len).ok_or_else(|| {
                WorkspaceError::CorruptMetadata("conditional mutation too large".into())
            })
        })
    }
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
    /// Called after reader/session/GC retirement by the catalog's mount owner.
    /// Individual reader sessions must not close a shared catalog transport.
    async fn shutdown_metadata_backend(&self) -> Result<(), WorkspaceError> {
        Ok(())
    }

    /// Enable only after persisted binding pins, ID reservation, merged
    /// permission/copy-up transactions and packed lifecycle/GC are implemented.
    /// Actual persistent reader lifecycle; unsupported stores fail closed.
    async fn open_packed_reader_session(
        self: std::sync::Arc<Self>,
        guard: HeadGuard,
        _budget: std::sync::Arc<super::packed_v3::wire005::V3MountBudget>,
        _options: super::packed_reader_lifecycle::PackedReaderLeaseOptions,
    ) -> Result<
        std::sync::Arc<dyn super::packed_reader_lifecycle::PackedReaderSession>,
        WorkspaceError,
    > {
        // Preserve the absent-binding ESTALE contract before reporting that
        // this catalog cannot provide persisted pins. A present binding still
        // cannot authorize an unpinned attach. KV overrides perform their own
        // exact current-binding acquisition and add no extra preflight read.
        self.load_packed_lower_binding(guard)
            .await?
            .ok_or(WorkspaceError::Fenced)?;
        Err(WorkspaceError::UnsupportedCapability(
            "persistent packed reader lifecycle",
        ))
    }

    /// Native catalogs have no packed sidecars to reap. This does not add a
    /// SQLite ledger; shared-KV stores override the actual server-time reaper.
    async fn reap_packed_reader_sessions(&self) -> Result<u64, WorkspaceError> {
        Ok(0)
    }

    fn supports_packed_workspace_mount(&self) -> bool {
        false
    }

    /// Native stores have no packed capability. A packed binding must be loaded
    /// under this head/lease guard; a missing claimed binding is an error in
    /// the caller, never a native packed-format fallback.
    async fn load_packed_lower_binding(
        &self,
        _guard: HeadGuard,
    ) -> Result<Option<PackedLowerBinding>, WorkspaceError> {
        Ok(None)
    }

    /// Initial-only foundation. Complete publication, pins and mutation
    /// capabilities remain separate gates; this must not enable packed mounts.
    async fn install_packed_lower_binding(
        &self,
        _request: super::publish::binding::InstallPackedLowerBinding,
    ) -> Result<super::publish::binding::PackedLowerBindingRecord, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "persistent packed binding",
        ))
    }

    /// Atomically publish a replacement packed-v3 PM11/PWB3 manifest against the exact
    /// current head, base and binding generation. Backends that do not expose
    /// the required transactional/CAS primitive remain fail-closed.
    async fn publish_packed_lower_binding(
        &self,
        _request: super::publish::binding::PublishPackedLowerBinding,
    ) -> Result<super::publish::binding::PackedLowerBindingRecord, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "versioned packed binding publication",
        ))
    }

    async fn load_packed_binding_record(
        &self,
        _guard: HeadGuard,
    ) -> Result<Option<super::publish::binding::PackedLowerBindingRecord>, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "persistent packed binding",
        ))
    }

    /// Immutable history is independent of the current head pointer. This is
    /// catalog inspection, not authorization to mount an old revision.
    async fn load_packed_binding_version(
        &self,
        _workspace_id: WorkspaceId,
        _version: u64,
    ) -> Result<Option<super::publish::binding::PackedLowerBindingRecord>, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "persistent packed binding",
        ))
    }

    /// Atomically compare head, lease holder/expiry and both layer versions.
    /// Busy denotes a changed sequence/version; Fenced denotes loss of authority.
    async fn validate_read_fence(
        &self,
        _guard: HeadGuard,
        _expected_layers: [LayerRecord; 2],
    ) -> Result<(), WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "workspace read fence",
        ))
    }

    fn supports_fused_read_fence(&self) -> bool {
        false
    }

    /// Take the head record this store last committed for `head`, if any.
    /// A hint: a stale record only makes the next conditional mutation fail
    /// with Busy, after which the caller re-reads the head.
    fn take_committed_head(&self, _head: LayerId) -> Option<LayerRecord> {
        None
    }

    /// Per-inode read fence for a frozen base: the head/lease guard is still
    /// held, the head still sits on `base`, and this inode's upper rows are
    /// exactly `expected`. Unrelated writes to other inodes do not fence.
    /// Busy denotes a changed inode; Fenced denotes loss of authority.
    async fn validate_inode_read_fence(
        &self,
        _guard: HeadGuard,
        _base: LayerRecord,
        _ino: i64,
        _expected: Vec<InodeDelta>,
    ) -> Result<(), WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "per-inode workspace read fence",
        ))
    }

    /// One consistent read of the hot head/lease fence, the current writable
    /// head record and one inode's rows in both layers. The returned head is
    /// fresh (its sequence fences same-epoch writes); `base` is the caller's
    /// cached sealed base, which is immutable for the binding.
    async fn read_fenced_inode(
        &self,
        _guard: HeadGuard,
        _base: LayerRecord,
        _ino: i64,
    ) -> Result<(LayerRecord, Vec<InodeDelta>), WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "fused workspace inode read fence",
        ))
    }

    /// Bound row decoding/allocation at the backend before returning it. A
    /// legacy get_extent_deltas followed by a length check is insufficient.
    async fn get_extent_deltas_bounded(
        &self,
        _request: ExtentQuery,
        _max_rows: usize,
    ) -> Result<Vec<DataExtentDelta>, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "bounded extent query",
        ))
    }

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
    /// Authority is minted only by the actual original mount shutdown.
    /// Arc ownership lets the backend retain it through a cancelled caller.
    async fn release_clean_packed_shutdown(
        self: std::sync::Arc<Self>,
        _proof: super::packed_shutdown::VerifiedCleanPackedShutdown,
    ) -> Result<(), WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "verified packed clean release",
        ))
    }

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
    async fn get_dentry_delta_page(
        &self,
        _layer: LayerId,
        _parent: i64,
        _after_name: Option<&[u8]>,
        _budget: std::sync::Arc<super::packed_v3::wire005::V3MountBudget>,
    ) -> Result<WorkspaceNamePage<DentryDelta>, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability("bounded dentry page"))
    }
    /// Global native delta page for bounded reverse alias lookup. Every page
    /// owns decoded rows; callers retain one layer fence across the whole walk.
    async fn get_layer_dentry_delta_page(
        &self,
        _layer: LayerId,
        _after: Option<(i64, &[u8])>,
        _budget: std::sync::Arc<super::packed_v3::wire005::V3MountBudget>,
    ) -> Result<WorkspaceNamePage<DentryDelta>, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "bounded global dentry page",
        ))
    }
    async fn get_native_reverse_authority(
        &self,
        _layers: &[LayerRecord],
        _budget: std::sync::Arc<super::packed_v3::wire005::V3MountBudget>,
    ) -> Result<WorkspaceNativeReverseAuthority, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "native reverse completeness authority",
        ))
    }

    async fn get_native_reverse_dentry_page(
        &self,
        _authority: &WorkspaceNativeReverseAuthority,
        _layer: LayerId,
        _ino: i64,
        _after: Option<(i64, &[u8])>,
        _budget: std::sync::Arc<super::packed_v3::wire005::V3MountBudget>,
    ) -> Result<WorkspaceNamePage<DentryDelta>, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "native reverse inode page",
        ))
    }

    async fn confirm_native_reverse_authority(
        &self,
        _authority: &WorkspaceNativeReverseAuthority,
        _budget: std::sync::Arc<super::packed_v3::wire005::V3MountBudget>,
    ) -> Result<(), WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "native reverse final authentication",
        ))
    }

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
    async fn get_xattr_delta_page(
        &self,
        _layer: LayerId,
        _ino: i64,
        _after_name: Option<&[u8]>,
        _budget: std::sync::Arc<super::packed_v3::wire005::V3MountBudget>,
    ) -> Result<WorkspaceNamePage<XattrDelta>, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability("bounded xattr page"))
    }
    async fn get_acl_deltas(&self, request: AclQuery) -> Result<Vec<AclDelta>, WorkspaceError>;

    async fn read_permission_snapshot(
        &self,
        _request: PermissionSnapshotQuery,
    ) -> Result<PermissionSnapshot, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "atomic permission snapshot",
        ))
    }

    /// One conditional native permission version and the exact immutable
    /// packed identity. Unsupported stores cannot synthesize write authority.
    async fn read_packed_permission_snapshot(
        &self,
        _guard: HeadGuard,
        _binding: PackedLowerBinding,
        _request: PermissionSnapshotQuery,
    ) -> Result<PermissionSnapshot, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "atomic packed permission snapshot",
        ))
    }

    async fn apply_permission_mutation(
        &self,
        _request: PermissionMutation,
    ) -> Result<MutationResult, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "conditional permission mutation",
        ))
    }

    async fn apply_versioned_mutation(
        &self,
        _request: VersionedMutation,
    ) -> Result<MutationResult, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "conditional metadata commit",
        ))
    }

    /// Packed metadata copy-up must check current binding, native sequence and
    /// live lease in the same CAS that installs its native delta rows.
    async fn apply_packed_versioned_mutation(
        &self,
        _request: VersionedMutation,
        _binding: PackedLowerBinding,
    ) -> Result<MutationResult, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "conditional packed metadata commit",
        ))
    }

    /// Opt in only when permission reads and all inode writes use a common
    /// commit version and backend transactions provide rollback on failure.
    fn supports_versioned_permissions(&self) -> bool {
        false
    }

    fn supports_packed_permissions(&self) -> bool {
        false
    }

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
    async fn load_seal_journal(&self, journal_id: JournalId)
    -> Result<SealJournal, WorkspaceError>;
    /// 列出指定 workspace 的所有 seal journal，包括终止状态的记录。
    async fn list_seal_journals(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<SealJournal>, WorkspaceError>;
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
    /// Durable SID publication fence, acquired before any physical DELETE.
    /// An implementation must authenticate the full reference proof and target
    /// incarnation. The default never grants legacy or packed-v3 GC authority.
    async fn reserve_gc_slice_deletion(
        &self,
        _slice_id: u64,
        _retained_slice_end: u64,
        _deleted_layers: &[LayerId],
    ) -> Result<(), WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability(
            "durable GC slice deletion reservation",
        ))
    }
    /// Reserve a bounded batch of SIDs using one catalog proof where the
    /// backend supports it. The default preserves compatibility for stores
    /// that only provide the single-SID operation.
    async fn reserve_gc_slice_deletions(
        &self,
        slices: &[(u64, u64)],
        deleted_layers: &[LayerId],
    ) -> Result<(), WorkspaceError> {
        for &(slice_id, retained_slice_end) in slices {
            self.reserve_gc_slice_deletion(slice_id, retained_slice_end, deleted_layers)
                .await?;
        }
        Ok(())
    }
    /// Authenticate a persisted reservation before a block store uses its
    /// durable completion marker to skip another extent census.
    async fn gc_slice_deletion_reservation_complete(
        &self,
        _slice_id: u64,
        _retained_slice_end: u64,
        _deleted_layers: &[LayerId],
    ) -> Result<bool, WorkspaceError> {
        Ok(false)
    }
    /// Limit one collector tick when metadata finalization uses a bounded
    /// target proof. The remaining candidates stay protected until a later
    /// tick; this keeps a slice reservation's target identities complete.
    fn gc_target_limit(&self) -> Option<usize> {
        None
    }
    async fn finalize_layer_metadata_deletion(
        &self,
        layer_ids: Vec<LayerId>,
    ) -> Result<(), WorkspaceError>;
    /// 删除超过宽限期的终止状态租约与 journal；每个 workspace 保留最近的 journal。
    async fn prune_terminal_records(
        &self,
        now_ns: i64,
        grace_ns: u64,
    ) -> Result<(), WorkspaceError>;
    /// Test fixture only; see `InstallTestBase`.
    #[cfg(test)]
    async fn install_test_base(
        &self,
        _request: InstallTestBase,
    ) -> Result<TestBaseResult, WorkspaceError> {
        Err(WorkspaceError::UnsupportedCapability("test base fixture"))
    }
}
