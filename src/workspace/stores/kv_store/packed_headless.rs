//! The public entry consumes persistent original-mount evidence and drives the
//! existing actual native source/graph/final publisher. Authority stays private.
use super::*;
#[path = "packed_admin/packed_mounted_session.rs"]
mod mounted_session;
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::chunk::{BlockStore, ChunkLayout};
use crate::meta::layer::MetaLayer;
use crate::vfs::{config::VFSConfig, fs::VFS};
use crate::workspace::meta_layer::WorkspaceMetaLayer;
use crate::workspace::model::ViewContext;
use crate::workspace::packed::PackedReadonlyMeta;
use crate::workspace::packed::snapshot::{
    AuthenticatedSnapshot, SnapshotIndexAuditLimits, SnapshotProducerOptions,
};
#[cfg(target_os = "linux")]
use crate::workspace::packed::snapshot::{FrozenNativeArtifact, NativeCaptureLimits};
#[cfg(target_os = "linux")]
use crate::workspace::stores::kv_store::packed_journal::{
    NativePublicationBuildOptions, NativePublicationPreparationFailure,
};
use crate::workspace::stores::kv_store::packed_native_freeze::NativeDeltaHashLimits;
pub use mounted_session::PackedRecoveredMountReport;
pub(in crate::workspace::stores::kv_store) use mounted_session::authenticate_mounted_recovery_writer;
pub(in crate::workspace::stores::kv_store::packed_admin) use mounted_session::{
    CleanSourceKind, CleanSourceRecord, PackedOriginalReleaseRows, decode_any_clean_source,
    decode_clean_source, encode_clean_source,
};
pub(crate) use mounted_session::{
    PackedMountGrantRequest, PackedMountedLease, PackedMountedRecoveryRequest,
    capture_mounted_recovery_owner,
};
use sha2::Digest;
use tokio_util::sync::CancellationToken;
#[path = "packed_clean_restore.rs"]
mod clean_restore;
pub(crate) use clean_restore::PackedCleanOperationOrigin;
use clean_restore::{CleanAuthorityMode, clean_operation_owner};

const PUBLISHED_CLEAN_MAGIC: &[u8; 5] = b"PHR3\x01";
const PUBLISHED_CLEAN_MAX_BYTES: usize = SOURCE_MAX_BYTES;
const CARRIER_ADMISSION_BYTES: u64 = 4 << 20;
fn published_clean_key(workspace: WorkspaceId) -> Vec<u8> {
    format!("packed/published-clean/{workspace}").into_bytes()
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishedCleanReceipt {
    snapshot: SnapshotRecord,
    binding: Vec<u8>,
    native_sealed_source_revision: BaseRevision,
    released_lease: SnapshotLease,
    closed_open: SnapshotOpenRecord,
    original_receipt: Vec<u8>,
    original_released_lease: Vec<u8>,
}
fn encode_published_clean(receipt: &PublishedCleanReceipt) -> Result<Vec<u8>, WorkspaceError> {
    let mut bytes = PUBLISHED_CLEAN_MAGIC.to_vec();
    bytes.extend(serde_json::to_vec(receipt).map_err(|_| WorkspaceError::Fenced)?);
    if bytes.len() > PUBLISHED_CLEAN_MAX_BYTES {
        return Err(WorkspaceError::Fenced);
    }
    Ok(bytes)
}
fn decode_published_clean(bytes: &[u8]) -> Result<PublishedCleanReceipt, WorkspaceError> {
    if bytes.len() > PUBLISHED_CLEAN_MAX_BYTES || !bytes.starts_with(PUBLISHED_CLEAN_MAGIC) {
        return Err(WorkspaceError::Fenced);
    }
    let value: PublishedCleanReceipt =
        serde_json::from_slice(&bytes[PUBLISHED_CLEAN_MAGIC.len()..])
            .map_err(|_| WorkspaceError::Fenced)?;
    if encode_published_clean(&value)? != bytes {
        return Err(WorkspaceError::Fenced);
    }
    Ok(value)
}
pub struct PackedPublishedViewReport {
    pub snapshot_id: SnapshotId,
    pub head_epoch: u64,
    pub binding: PackedLowerBindingRecord,
}
/// A bounded observation for reconcile routing. It grants no publication,
/// recovery, clean-release, or writable mount authority.
pub struct PackedWorkspaceViewReport {
    pub workspace: WorkspaceRecord,
    pub base_revision: BaseRevision,
    pub layer_depth: u32,
    pub leases: Vec<SnapshotLease>,
}
struct VirginPackedRead {
    control: ControlState,
    binding: PackedLowerBindingRecord,
    checks: Vec<KvCheck>,
    now: i64,
}

/// Descriptive snapshot fields, separate from lease and publication identities.
pub struct PackedHeadlessSnapshotDescription {
    pub snapshot_id: SnapshotId,
    pub snapshot_name: String,
    pub owner_id: Option<String>,
}

pub struct PackedHeadlessSnapshotRequest {
    pub snapshot_id: SnapshotId,
    pub snapshot_name: Option<String>,
    pub owner_id: Option<String>,
    pub lease_id: LeaseId,
    pub lease_ttl_ns: u64,
    pub native_journal_id: JournalId,
    pub new_head_layer_id: LayerId,
    pub temporary: std::path::PathBuf,
    temporary_owner: Option<Arc<tempfile::TempDir>>,
    pub producer: SnapshotProducerOptions,
    pub graph_limits: SnapshotIndexAuditLimits,
    pub max_rows: u64,
    pub max_logical_bytes: u64,
    pub max_data_bytes: u64,
    pub scratch_disk_bytes: u64,
    pub cancel: CancellationToken,
}
impl PackedHeadlessSnapshotRequest {
    /// A fixed small operator tier; larger experiments pass explicit limits.
    pub fn bounded_operator(
        description: PackedHeadlessSnapshotDescription,
        lease_id: LeaseId,
        native_journal_id: JournalId,
        new_head_layer_id: LayerId,
        lease_ttl_ns: u64,
        temporary: std::path::PathBuf,
    ) -> Self {
        use crate::workspace::packed::{AccessProfile, PackedCodec, SizeClassTable};
        let PackedHeadlessSnapshotDescription {
            snapshot_id,
            snapshot_name,
            owner_id,
        } = description;
        let snapshot_digest: [u8; 32] =
            sha2::Sha256::digest(snapshot_id.as_uuid().as_bytes()).into();
        let root_digest: [u8; 32] =
            sha2::Sha256::digest(new_head_layer_id.as_uuid().as_bytes()).into();
        Self {
            snapshot_id,
            snapshot_name: Some(snapshot_name),
            owner_id,
            lease_id,
            lease_ttl_ns,
            native_journal_id,
            new_head_layer_id,
            temporary,
            temporary_owner: None,
            producer: SnapshotProducerOptions {
                snapshot_id: snapshot_digest,
                root_dir_key: root_digest,
                root_inode: 1,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build_policy: crate::workspace::packed::snapshot::SnapshotBuildPolicy::default(),
                metadata_codec: PackedCodec::Raw,
                data_codec: PackedCodec::Raw,
            },
            graph_limits: SnapshotIndexAuditLimits {
                max_objects: 16_384,
                max_authenticated_bytes: 256 << 20,
                max_requested_bytes: 256 << 20,
                max_decoded_bytes: 256 << 20,
                max_frame_validation_steps: 65_536,
                max_logical_hash_bytes: 256 << 20,
                max_contexts: 16_384,
                max_visits: 65_536,
                max_leaf_records: 16_384,
                max_page_records: 16_384,
                max_disk_bytes: 64 << 20,
                sqlite_cache_bytes: 64 << 10,
                max_sql_operations: 4_000_000,
                max_sql_vm_steps: 16_000_000,
                chunk_bytes: 64 << 10,
            },
            max_rows: 16_384,
            max_logical_bytes: 256 << 20,
            max_data_bytes: 256 << 20,
            scratch_disk_bytes: 64 << 20,
            cancel: CancellationToken::new(),
        }
    }

    /// Keep an operator scratch directory alive through the detached driver,
    /// retained failure owner, and actual physical runtime teardown.
    pub fn with_temporary_owner(
        mut self,
        owner: Arc<tempfile::TempDir>,
    ) -> Result<Self, WorkspaceError> {
        if owner.path() != self.temporary {
            return Err(WorkspaceError::Fenced);
        }
        self.temporary_owner = Some(owner);
        Ok(self)
    }
}

/// The private owner retains actual source/graph/attempt evidence on failure.
/// It cannot be decoded or converted into another publication attempt.
pub struct PackedHeadlessSnapshotFailure {
    cause: Box<dyn HeldFailure>,
    committed: Option<Box<PackedSnapshotResult>>,
}
trait HeldFailure: Send {
    fn error(&self) -> &WorkspaceError;
}
impl HeldFailure for WorkspaceError {
    fn error(&self) -> &WorkspaceError {
        self
    }
}
struct OwnedFailure<T> {
    owner: T,
    error: fn(&T) -> &WorkspaceError,
}
impl<T: Send> HeldFailure for OwnedFailure<T> {
    fn error(&self) -> &WorkspaceError {
        (self.error)(&self.owner)
    }
}
impl PackedHeadlessSnapshotFailure {
    pub fn error(&self) -> &WorkspaceError {
        self.cause.error()
    }
    pub fn committed_result(&self) -> Option<&PackedSnapshotResult> {
        self.committed.as_deref()
    }
    fn held<T: Send + 'static>(owner: T, error: fn(&T) -> &WorkspaceError) -> Self {
        Self {
            cause: Box::new(OwnedFailure { owner, error }),
            committed: None,
        }
    }
}
impl From<WorkspaceError> for PackedHeadlessSnapshotFailure {
    fn from(error: WorkspaceError) -> Self {
        Self {
            cause: Box::new(error),
            committed: None,
        }
    }
}

struct ClaimAttemptOwner<B: WorkspaceKvBackend> {
    error: WorkspaceError,
    ticket: PackedCleanSourceTicket<B>,
    _checks: Vec<KvCheck>,
    _writes: Vec<KvWrite>,
}
impl<B: WorkspaceKvBackend> Drop for ClaimAttemptOwner<B> {
    fn drop(&mut self) {
        self.ticket.budget.close();
    }
}

struct HeadlessRuntimeOwner<B: WorkspaceKvBackend, S: BlockStore + Send + Sync + 'static> {
    _claim: Option<Arc<PackedCleanPublicationAuthority<B>>>,
    _store: Arc<KvWorkspaceStore<B>>,
    budget: Arc<SnapshotMountBudget>,
    _upper: Arc<S>,
    _temporary_owner: Option<Arc<tempfile::TempDir>>,
    cleanup_handle: tokio::runtime::Handle,
    metadata: std::sync::Mutex<Option<Arc<WorkspaceMetaLayer<KvWorkspaceStore<B>>>>>,
    reader: std::sync::Mutex<
        Option<Arc<dyn crate::workspace::packed_reader_lifecycle::PackedReaderSession>>,
    >,
    native: std::sync::Mutex<
        Option<Arc<super::super::packed_native_freeze::PackedNativeQuiesceFence<B>>>,
    >,
    finished: std::sync::atomic::AtomicBool,
}
impl<B: WorkspaceKvBackend, S: BlockStore + Send + Sync + 'static> Drop
    for HeadlessRuntimeOwner<B, S>
{
    fn drop(&mut self) {
        if self.finished.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let metadata = self.metadata.get_mut().unwrap().take();
        let reader = self.reader.get_mut().unwrap().take();
        let native = self.native.get_mut().unwrap().take();
        let claim = self._claim.take();
        let budget = self.budget.clone();
        let store = self._store.clone();
        let upper = self._upper.clone();
        let temporary_owner = self._temporary_owner.clone();
        // Drop never tears down a runtime underneath physical drain. It starts
        // one owning cleanup and retains source/catalog/budget until terminal.
        self.cleanup_handle.spawn(async move {
            let _native = native;
            let _claim = claim;
            let _store = store;
            let _upper = upper;
            let _temporary_owner = temporary_owner;
            if let Some(metadata) = metadata {
                if metadata
                    .shutdown_packed_runtime_for_clean_release()
                    .await
                    .is_ok()
                {
                    budget.close();
                }
            } else if let Some(reader) = reader {
                if reader.shutdown().await.is_ok() {
                    budget.close();
                }
            } else {
                budget.close();
            }
            // The actual granted lease/open and any native journal remain
            // durable recovery obligations. Failure never manufactures Released.
        });
    }
}

fn runtime_failure<B: WorkspaceKvBackend, S: BlockStore + Send + Sync + 'static>(
    error: WorkspaceError,
    runtime: &Arc<HeadlessRuntimeOwner<B, S>>,
) -> PackedHeadlessSnapshotFailure {
    PackedHeadlessSnapshotFailure::held((error, runtime.clone()), |owner| &owner.0)
}

pub(crate) struct PackedCleanPublicationAuthority<B> {
    store: Arc<KvWorkspaceStore<B>>,
    original: PackedReleasedMountReference,
    immutable: Vec<KvCheck>,
    open: SnapshotOpenRecord,
    guard: HeadGuard,
    budget: Arc<SnapshotMountBudget>,
    mode: CleanAuthorityMode,
    _owner: SnapshotOwnedPermit,
}
impl<B: WorkspaceKvBackend> PackedCleanPublicationAuthority<B> {
    pub(crate) fn belongs_to_store(&self, store: &Arc<KvWorkspaceStore<B>>) -> bool {
        Arc::ptr_eq(&self.store, store)
    }
    pub(crate) fn mount_budget(&self) -> &Arc<SnapshotMountBudget> {
        &self.budget
    }
    pub(crate) fn guard(&self) -> &HeadGuard {
        &self.guard
    }

    pub(crate) async fn authority_checks_before(
        &self,
    ) -> Result<(Vec<KvCheck>, i64), WorkspaceError> {
        let mut keys = self
            .immutable
            .iter()
            .map(|check| check.key.clone())
            .collect::<Vec<_>>();
        keys.extend([
            open_snapshot_key(self.guard.workspace_id),
            hot_lease_key(self.guard.workspace_id, self.guard.lease_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            hot_workspace_key(self.guard.workspace_id),
            hot_lease_index_key(self.guard.lease_id),
        ]);
        let (values, now) = self
            .store
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 || self.budget.state().closed {
            return Err(WorkspaceError::Fenced);
        }
        if values[..self.immutable.len()]
            .iter()
            .zip(&self.immutable)
            .any(|(actual, check)| actual != &check.expected)
        {
            return Err(WorkspaceError::Fenced);
        }
        let offset = self.immutable.len();
        let open: SnapshotOpenRecord = decode_open_value(
            values[offset].as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        let lease: SnapshotLease = decode_open_value(
            values[offset + 1]
                .as_deref()
                .ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        let workspace: WorkspaceRecord = decode_open_value(
            values[offset + 4]
                .as_deref()
                .ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        let lease_workspace: WorkspaceId = decode_open_value(
            values[offset + 5]
                .as_deref()
                .ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        if open != self.open
            || workspace.workspace_id != self.guard.workspace_id
            || workspace.active_lease != Some(self.guard.lease_id)
            || workspace.head_layer_id != self.guard.expected_head_layer_id
            || workspace.head_epoch != self.guard.expected_head_epoch
            || !matches!(
                workspace.state,
                WorkspaceState::Active | WorkspaceState::Sealing
            )
            || lease_workspace != self.guard.workspace_id
            || open.expires_at_ns <= now
            || match self.mode {
                CleanAuthorityMode::Original => {
                    open.state != SnapshotOpenState::Ready || open.recovery_required
                }
                CleanAuthorityMode::Recovery => {
                    open.state != SnapshotOpenState::Recovering || !open.recovery_required
                }
                CleanAuthorityMode::Committed => true,
            }
            || lease.lease_id != self.guard.lease_id
            || lease.workspace_id != self.guard.workspace_id
            || lease.holder_generation != self.guard.holder_generation
            || !lease.writable
            || lease.state != LeaseState::Active
            || lease.expires_at_ns <= now
        {
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[offset + 2])?;
        layer_inventory_generation(&values[offset + 3])?;
        let deadline = open.expires_at_ns.min(lease.expires_at_ns);
        let mut checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let _writer_owner = self
            .store
            .authenticate_administrative_packed_writer(self.guard.workspace_id, &mut checks)
            .await?;
        Ok((checks, deadline))
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub async fn inspect_packed_snapshot(
        &self,
        snapshot_id: SnapshotId,
    ) -> Result<SnapshotRecord, WorkspaceError> {
        let _owner = self
            .packed_reader_pin_budget
            .get()
            .ok_or(WorkspaceError::Fenced)?
            .admit(&[(SnapshotBudgetPool::Metadata, CARRIER_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let key = hot_snapshot_key(snapshot_id);
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(std::slice::from_ref(&key), source_limits(1))
            .await?;
        if values.len() != 1 || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let raw = values[0]
            .as_deref()
            .ok_or(WorkspaceError::SnapshotNotFound(snapshot_id))?;
        let snapshot: SnapshotRecord = decode_open_value(raw, SOURCE_MAX_BYTES)?;
        if snapshot.snapshot_id != snapshot_id {
            return Err(WorkspaceError::Fenced);
        }
        let (_, mut checks) = self
            .retained_packed_carrier_checks(&snapshot.revision)
            .await?;
        checks.push(KvCheck {
            key,
            expected: values[0].clone(),
        });
        self.clean_exact_cas(&checks, &[], checked_expiry(now, 30_000_000_000)?)
            .await?;
        Ok(snapshot)
    }

    async fn read_unmounted_packed_source(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<VirginPackedRead>, WorkspaceError> {
        let Some(binding) = self.inspect_packed_workspace_binding(workspace_id).await? else {
            return Ok(None);
        };
        let history = self
            .read_workspace_lease_history_checks(workspace_id, 18, source_limits(32))
            .await?;
        let mut keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(workspace_id),
            hot_layer_key(binding.head_layer_id),
            hot_layer_key(binding.base_revision.layer_id),
            packed_current_key(workspace_id),
            packed_claim_key(workspace_id),
            packed_history_key(workspace_id, binding.binding.binding_version),
            open_snapshot_key(workspace_id),
            open_snapshot_recovery_key(workspace_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
        ];
        append_workspace_history_keys(&mut keys, &history, 32)?;
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        authenticate_workspace_history_values(&keys, &values, &history)?;
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        let mut control = topology_state_from_checks(
            &keys
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>(),
        )?;
        let workspace: WorkspaceRecord = decode_open_value(required(1)?, SOURCE_MAX_BYTES)?;
        let head: LayerRecord = decode_open_value(required(2)?, SOURCE_MAX_BYTES)?;
        let base: LayerRecord = decode_open_value(required(3)?, SOURCE_MAX_BYTES)?;
        let current = decode_packed_pair(workspace_id, &values[4], &values[5], &values[6])?
            .ok_or(WorkspaceError::Fenced)?;
        if current != binding
            || control.schema_version != WORKSPACE_SCHEMA_VERSION
            || workspace.workspace_id != workspace_id
            || workspace.head_layer_id != binding.head_layer_id
            || workspace.head_epoch != binding.head_epoch
            || workspace.state != WorkspaceState::Active
            || workspace.active_lease.is_some()
            || head.layer_id != binding.head_layer_id
            || head.state != LayerState::Writable
            || head.parent_layer_id != Some(base.layer_id)
            || head.depth != 2
            || head.next_sequence != 1
            || head.owned_slice_count != 0
            || head.owned_bytes != 0
            || base.state != LayerState::Sealed
            || base.depth != 1
            || base.parent_layer_id.is_some()
            || base.sealed_version != Some(binding.base_revision.sealed_version)
            || base.root_hash != Some(binding.base_revision.root_hash)
            || values[7].is_some()
            || values[8].is_some()
            || control
                .leases
                .values()
                .any(|row| row.workspace_id == workspace_id && row.writable)
        {
            return Ok(None);
        }
        next_packed_root_generation(&values[9])?;
        layer_inventory_generation(&values[10])?;
        control.workspaces.insert(workspace_id, workspace);
        control.layers.insert(head.layer_id, head);
        control.layers.insert(base.layer_id, base);
        Ok(Some(VirginPackedRead {
            control,
            binding,
            now,
            checks: keys
                .into_iter()
                .zip(values)
                .map(|(key, expected)| KvCheck { key, expected })
                .collect(),
        }))
    }

    pub async fn inspect_unmounted_packed_source(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<PackedLowerBindingRecord>, WorkspaceError> {
        let _owner = self
            .packed_reader_pin_budget
            .get()
            .ok_or(WorkspaceError::Fenced)?
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        Ok(self
            .read_unmounted_packed_source(workspace_id)
            .await?
            .map(|read| read.binding))
    }

    pub async fn pin_clean_packed_snapshot(
        self: &Arc<Self>,
        workspace_id: WorkspaceId,
        request: CreateSnapshot,
    ) -> Result<SnapshotRecord, WorkspaceError> {
        self.require_admin_access()?;
        let owner = self
            .packed_reader_pin_budget
            .get()
            .ok_or(WorkspaceError::Fenced)?
            .admit(&[(SnapshotBudgetPool::Metadata, CARRIER_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let store = self.clone();
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            WorkspaceError::CorruptMetadata(
                "packed snapshot requires a running Tokio runtime".into(),
            )
        })?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        handle.spawn(async move {
            let _owner = owner;
            let result = async {
                let mut read = match store.read_unmounted_packed_source(workspace_id).await? {
                    Some(read) => read,
                    None => store
                        .read_clean_published_view(workspace_id)
                        .await?
                        .map(|(_, read)| read)
                        .ok_or(WorkspaceError::Fenced)?,
                };
                if request.revision != read.binding.base_revision
                    || request.snapshot_id.as_uuid().is_nil()
                {
                    return Err(WorkspaceError::Fenced);
                }
                let (carrier_binding, carrier_checks) = store
                    .retained_packed_carrier_checks(&request.revision)
                    .await?;
                // A borrowed child has its own local binding version. Fence the
                // same immutable carrier bytes, then retain the actual child
                // alias/source proof in this snapshot's final CAS.
                if carrier_binding.base_revision != read.binding.base_revision
                    || carrier_binding.highest_inode != read.binding.highest_inode
                    || carrier_binding.binding.base_layer_id != read.binding.binding.base_layer_id
                    || carrier_binding.binding.manifest != read.binding.binding.manifest
                    || (carrier_binding.workspace_id == read.binding.workspace_id
                        && carrier_binding != read.binding)
                {
                    return Err(WorkspaceError::Fenced);
                }
                let (_registry_owner, registry_checks) = store
                    .packed_registry_publication_checks(&read.binding)
                    .await?;
                for added in carrier_checks.into_iter().chain(registry_checks) {
                    if let Some(old) = read.checks.iter().find(|row| row.key == added.key) {
                        if old.expected != added.expected {
                            return Err(WorkspaceError::Busy);
                        }
                    } else {
                        read.checks.push(added);
                    }
                }
                let snapshot_key = hot_snapshot_key(request.snapshot_id);
                let name_key = request.name.as_ref().map(|name| snapshot_name_key(name));
                let mut snapshot_keys = vec![snapshot_key.clone()];
                snapshot_keys.extend(name_key.iter().cloned());
                let (values, _) = store
                    .backend
                    .get_many_consistent_with_time_bounded(
                        &snapshot_keys,
                        source_limits(snapshot_keys.len()),
                    )
                    .await?;
                if values.len() != snapshot_keys.len() {
                    return Err(WorkspaceError::Fenced);
                }
                read.checks.extend(
                    snapshot_keys
                        .iter()
                        .cloned()
                        .zip(values.iter().cloned())
                        .map(|(key, expected)| KvCheck { key, expected }),
                );
                let named_snapshot = values
                    .get(1)
                    .and_then(Option::as_deref)
                    .map(decode::<SnapshotId>)
                    .transpose()?;
                let deadline = checked_expiry(read.now, 30_000_000_000)?;
                if let Some(raw) = values[0].as_deref() {
                    let snapshot: SnapshotRecord = decode_open_value(raw, SOURCE_MAX_BYTES)?;
                    if snapshot.snapshot_id != request.snapshot_id
                        || snapshot.name != request.name
                        || snapshot.revision != request.revision
                        || snapshot.owner_id != request.owner_id
                        || (name_key.is_some() && named_snapshot != Some(request.snapshot_id))
                    {
                        return Err(WorkspaceError::Fenced);
                    }
                    store.clean_exact_cas(&read.checks, &[], deadline).await?;
                    return Ok(snapshot);
                }
                if named_snapshot.is_some() {
                    return Err(WorkspaceError::Busy);
                }
                let snapshot = SnapshotRecord {
                    snapshot_id: request.snapshot_id,
                    name: request.name,
                    revision: request.revision,
                    owner_id: request.owner_id,
                    created_at_ns: read.now,
                };
                read.control
                    .snapshots
                    .insert(snapshot.snapshot_id, snapshot.clone());
                let mut writes = vec![put(snapshot_key, &snapshot)?];
                if let Some(name_key) = name_key {
                    writes.push(put(name_key, &snapshot.snapshot_id)?);
                }
                let _holds = store
                    .prepare_native_owner_cas(&mut read.checks, &mut writes)
                    .await?;
                store
                    .clean_exact_cas(&read.checks, &writes, deadline)
                    .await?;
                Ok(snapshot)
            }
            .await;
            let _ = sender.send(result);
        });
        receiver.await.map_err(|_| WorkspaceError::Fenced)?
    }

    pub async fn inspect_packed_workspace_binding(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<PackedLowerBindingRecord>, WorkspaceError> {
        let _owner = self
            .packed_reader_pin_budget
            .get()
            .ok_or(WorkspaceError::Fenced)?
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let current = packed_current_key(workspace_id);
        let (routing, _) = self
            .backend
            .get_many_consistent_with_time_bounded(std::slice::from_ref(&current), source_limits(1))
            .await?;
        if routing.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let Some(raw) = routing[0].as_deref() else {
            return Ok(None);
        };
        let binding = PackedLowerBindingRecord::decode(raw)?;
        let keys = vec![
            current,
            packed_claim_key(workspace_id),
            packed_history_key(workspace_id, binding.binding.binding_version),
            hot_workspace_key(workspace_id),
            hot_layer_key(binding.head_layer_id),
            hot_layer_key(binding.base_revision.layer_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
        ];
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 || values[0].as_deref() != Some(raw) {
            return Err(WorkspaceError::Fenced);
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        let workspace: WorkspaceRecord = decode_open_value(required(3)?, SOURCE_MAX_BYTES)?;
        let head: LayerRecord = decode_open_value(required(4)?, SOURCE_MAX_BYTES)?;
        let base: LayerRecord = decode_open_value(required(5)?, SOURCE_MAX_BYTES)?;
        if decode_packed_pair(workspace_id, &values[0], &values[1], &values[2])?.as_ref()
            != Some(&binding)
            || workspace.workspace_id != workspace_id
            || workspace.head_layer_id != binding.head_layer_id
            || workspace.head_epoch != binding.head_epoch
            || head.layer_id != binding.head_layer_id
            || head.parent_layer_id != Some(base.layer_id)
            || head.depth != 2
            || base.depth != 1
            || base.state != LayerState::Sealed
            || base.parent_layer_id.is_some()
            || base.sealed_version != Some(binding.base_revision.sealed_version)
            || base.root_hash != Some(binding.base_revision.root_hash)
        {
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[6])?;
        layer_inventory_generation(&values[7])?;
        Ok(Some(binding))
    }

    pub async fn inspect_packed_workspace_view(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<PackedWorkspaceViewReport>, WorkspaceError> {
        let _owner = self
            .packed_reader_pin_budget
            .get()
            .ok_or(WorkspaceError::Fenced)?
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let Some(binding) = self.inspect_packed_workspace_binding(workspace_id).await? else {
            return Ok(None);
        };
        let history = self
            .read_workspace_lease_history_checks(workspace_id, 22, source_limits(32))
            .await?;
        let mut keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(workspace_id),
            hot_layer_key(binding.head_layer_id),
            hot_layer_key(binding.base_revision.layer_id),
            packed_current_key(workspace_id),
            packed_claim_key(workspace_id),
            packed_history_key(workspace_id, binding.binding.binding_version),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
        ];
        append_workspace_history_keys(&mut keys, &history, 32)?;
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        authenticate_workspace_history_values(&keys, &values, &history)?;
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        let control = topology_state_from_checks(
            &keys
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>(),
        )?;
        let workspace: WorkspaceRecord = decode_open_value(required(1)?, SOURCE_MAX_BYTES)?;
        let head: LayerRecord = decode_open_value(required(2)?, SOURCE_MAX_BYTES)?;
        let base: LayerRecord = decode_open_value(required(3)?, SOURCE_MAX_BYTES)?;
        if control.schema_version != WORKSPACE_SCHEMA_VERSION
            || decode_packed_pair(workspace_id, &values[4], &values[5], &values[6])?.as_ref()
                != Some(&binding)
            || workspace.workspace_id != workspace_id
            || workspace.head_layer_id != binding.head_layer_id
            || workspace.head_epoch != binding.head_epoch
            || workspace.state != WorkspaceState::Active
            || head.layer_id != binding.head_layer_id
            || !matches!(head.state, LayerState::Writable | LayerState::Sealing)
            || head.owner_workspace_id != Some(workspace_id)
            || head.parent_layer_id != Some(base.layer_id)
            || head.depth != 2
            || base.state != LayerState::Sealed
            || base.depth != 1
            || base.parent_layer_id.is_some()
            || base.owner_workspace_id.is_some()
            || base.sealed_version != Some(binding.base_revision.sealed_version)
            || base.root_hash != Some(binding.base_revision.root_hash)
        {
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[7])?;
        layer_inventory_generation(&values[8])?;
        let leases = control
            .leases
            .values()
            .filter(|lease| lease.workspace_id == workspace_id)
            .cloned()
            .collect::<Vec<_>>();
        let checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        self.clean_exact_cas(&checks, &[], checked_expiry(now, 30_000_000_000)?)
            .await?;
        Ok(Some(PackedWorkspaceViewReport {
            workspace,
            base_revision: binding.base_revision,
            layer_depth: head.depth,
            leases,
        }))
    }

    /// Route a removed Mount CR through the hot workspace, idle writer and PCR.
    /// The returned reference is a report; publication must admit it again.
    pub async fn inspect_original_clean_packed_mount(
        self: &Arc<Self>,
        workspace_id: WorkspaceId,
    ) -> Result<Option<PackedReleasedMountReference>, WorkspaceError> {
        let budget = self
            .packed_reader_pin_budget
            .get()
            .ok_or(WorkspaceError::Fenced)?
            .clone();
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let history = self
            .read_workspace_lease_history_checks(workspace_id, 16, source_limits(32))
            .await?;
        let mut route_keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(workspace_id),
            packed_current_key(workspace_id),
            packed_writer_key(workspace_id),
            open_snapshot_key(workspace_id),
        ];
        append_workspace_history_keys(&mut route_keys, &history, 32)?;
        let (routing, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&route_keys, source_limits(route_keys.len()))
            .await?;
        if routing.len() != route_keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        authenticate_workspace_history_values(&route_keys, &routing, &history)?;
        let required = |index: usize| routing[index].as_deref().ok_or(WorkspaceError::Fenced);
        let control = topology_state_from_checks(
            &route_keys
                .iter()
                .cloned()
                .zip(routing.iter().cloned())
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>(),
        )?;
        let workspace: WorkspaceRecord = decode_open_value(required(1)?, SOURCE_MAX_BYTES)?;
        let binding = PackedLowerBindingRecord::decode(required(2)?)?;
        let writer = PackedWriterAuthority::decode(required(3)?, workspace_id)?;
        let rows = control
            .workspaces
            .len()
            .checked_add(control.layers.len())
            .and_then(|count| count.checked_add(control.leases.len()))
            .and_then(|count| count.checked_add(control.journals.len()))
            .and_then(|count| count.checked_add(control.snapshots.len()))
            .ok_or(WorkspaceError::Fenced)?;
        if control.schema_version != WORKSPACE_SCHEMA_VERSION || rows > 256 {
            return Err(WorkspaceError::Fenced);
        }
        let catalog_workspace = control
            .workspaces
            .get(&workspace_id)
            .ok_or(WorkspaceError::WorkspaceNotFound(workspace_id))?;
        if catalog_workspace.workspace_id != workspace_id
            || workspace.workspace_id != workspace_id
            || binding.workspace_id != workspace_id
            || workspace.head_layer_id != binding.head_layer_id
            || workspace.head_epoch != binding.head_epoch
        {
            return Err(WorkspaceError::Fenced);
        }
        if workspace.state != WorkspaceState::Active
            || workspace.active_lease.is_some()
            || writer.owner.is_some()
        {
            return Ok(None);
        }
        if let Some(raw) = routing[4].as_deref() {
            let open: SnapshotOpenRecord = decode_open_value(raw, OPEN_RECORD_MAX_BYTES)?;
            validate_open_record(&open, workspace_id)?;
            if open.expires_at_ns > now
                || open.recovery_required
                || open.state != SnapshotOpenState::Ready
            {
                return Ok(None);
            }
        }
        let leases = control
            .leases
            .values()
            .filter(|lease| lease.workspace_id == workspace_id && lease.writable)
            .collect::<Vec<_>>();
        if leases.len() > 16 {
            return Err(WorkspaceError::Fenced);
        }
        let Some(latest) = leases
            .iter()
            .copied()
            .max_by_key(|lease| lease.created_at_ns)
        else {
            return Ok(None);
        };
        for (id, lease) in &control.leases {
            let touches_source = *id == latest.lease_id || lease.lease_id == latest.lease_id;
            if (lease.workspace_id == workspace_id || touches_source) && *id != lease.lease_id {
                return Err(WorkspaceError::Fenced);
            }
        }
        if latest.state != LeaseState::Released
            || leases.iter().any(|lease| {
                lease.lease_id != latest.lease_id && lease.created_at_ns >= latest.created_at_ns
            })
        {
            return Ok(None);
        }
        let guard = HeadGuard {
            workspace_id,
            expected_head_layer_id: workspace.head_layer_id,
            expected_head_epoch: workspace.head_epoch,
            lease_id: latest.lease_id,
            holder_generation: latest.holder_generation,
        };
        let key = clean_receipt_key(&guard);
        let (receipt_values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                std::slice::from_ref(&key),
                KvReadLimits {
                    max_records: 1,
                    max_key_bytes: 1024,
                    max_value_bytes: CLEAN_RECEIPT_MAX_BYTES,
                    max_total_bytes: CLEAN_RECEIPT_MAX_BYTES,
                    max_response_bytes: 8 << 10,
                    max_data_requests: 3,
                },
            )
            .await?;
        if receipt_values.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let Some(receipt_raw) = receipt_values[0].as_deref() else {
            return Ok(None);
        };
        let receipt = decode_clean_receipt(receipt_raw)?;
        if receipt.guard.to_head_guard() != guard {
            return Ok(None);
        }
        let reference = PackedReleasedMountReference {
            guard,
            mount_uid: receipt.mount_uid,
            pod_uid: receipt.pod_uid,
        };
        let mut ticket = match self
            .admit_clean_packed_source(reference.clone(), budget)
            .await
        {
            Ok(PackedCleanAdmission::Ready(ticket)) => ticket,
            Ok(PackedCleanAdmission::RequiresRecovery) | Err(WorkspaceError::Fenced) => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if !ticket
            .checks
            .iter()
            .any(|check| check.key == key && check.expected.as_deref() == Some(receipt_raw))
        {
            return Err(WorkspaceError::Busy);
        }
        // CONTROL is a bounded historical inventory. Packed install changes the
        // hot head epoch without rewriting its mirror; only the routed hot row
        // supplies the guard. Retain every scoped route in the final packet.
        for (key, expected) in route_keys.into_iter().zip(routing) {
            if let Some(check) = ticket.checks.iter().find(|check| check.key == key) {
                if check.expected != expected {
                    return Err(WorkspaceError::Busy);
                }
            } else {
                ticket.checks.push(KvCheck { key, expected });
            }
        }
        // One complete read-only CAS protects CONTROL, actual Released lease,
        // hot head/binding/history, idle PWA, open, original PCR and root facts.
        self.clean_exact_cas(&ticket.checks, &[], checked_expiry(now, 30_000_000_000)?)
            .await?;
        Ok(Some(reference))
    }

    // One mutation only. A lost reply may be confirmed by its complete exact
    // successor and a read-only CAS before the original deadline.
    pub(in crate::workspace::stores::kv_store) async fn clean_exact_cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<(), WorkspaceError> {
        self.exact_source_cas(checks, writes, deadline, 32).await
    }

    // Initial publication retains both bootstrap and carrier proofs alongside
    // entity/index and NativeHold predecessors. Its fixed envelope can exceed
    // the ordinary 32-key clean-source tier even for an empty filesystem.
    pub(in crate::workspace::stores::kv_store) async fn initial_finish_exact_cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<(), WorkspaceError> {
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        self.exact_source_cas(checks, writes, deadline, 64).await
    }

    async fn exact_source_cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
        max_keys: usize,
    ) -> Result<(), WorkspaceError> {
        let packet = self
            .prepare_topology_envelope(checks.to_vec(), writes.to_vec(), Some(deadline))
            .await?;
        let checks = packet.checks.as_slice();
        let writes = packet.writes.as_slice();
        let mut successor = checks.to_vec();
        for write in writes {
            let (key, expected) = match write {
                KvWrite::Put { key, value } => (key, Some(value.clone())),
                KvWrite::Delete { key } => (key, None),
            };
            successor
                .iter_mut()
                .find(|check| &check.key == key)
                .ok_or(WorkspaceError::Fenced)?
                .expected = expected;
        }
        let packet_bytes = |packet: &[KvCheck]| {
            packet.iter().try_fold(0usize, |sum, row| {
                sum.checked_add(row.key.len())
                    .and_then(|sum| sum.checked_add(row.expected.as_ref().map_or(0, Vec::len)))
            })
        };
        if checks.len() > max_keys
            || successor.len() > max_keys
            || packet_bytes(checks).is_none_or(|bytes| bytes > SOURCE_MAX_BYTES)
            || packet_bytes(&successor).is_none_or(|bytes| bytes > SOURCE_MAX_BYTES)
        {
            return Err(WorkspaceError::Fenced);
        }
        match self.commit_prepared_topology_packet(&packet).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(WorkspaceError::Fenced),
            Err(original) => {
                let keys = successor
                    .iter()
                    .map(|row| row.key.clone())
                    .collect::<Vec<_>>();
                match self
                    .backend
                    .get_many_consistent_with_time_bounded(
                        &keys,
                        KvReadLimits {
                            max_data_requests: keys.len().saturating_add(2).min(max_keys),
                            ..source_limits(keys.len())
                        },
                    )
                    .await
                {
                    Ok((values, now))
                        if now > 0
                            && now < deadline
                            && values.len() == successor.len()
                            && values
                                .iter()
                                .zip(&successor)
                                .all(|(value, row)| value == &row.expected) =>
                    {
                        match self
                            .backend
                            .compare_and_swap_before(&successor, &[], deadline)
                            .await
                        {
                            Ok(true) => Ok(()),
                            _ => Err(original),
                        }
                    }
                    _ => Err(original),
                }
            }
        }
    }

    async fn claim_clean_publication(
        self: &Arc<Self>,
        ticket: PackedCleanSourceTicket<B>,
        lease_id: LeaseId,
        ttl_ns: u64,
        owner_id: String,
    ) -> Result<PackedCleanPublicationAuthority<B>, PackedHeadlessSnapshotFailure> {
        if !ticket.belongs_to_store(self)
            || lease_id.as_uuid().is_nil()
            || ttl_ns == 0
            || ttl_ns > 15 * 60 * 1_000_000_000
        {
            return Err(WorkspaceError::Fenced.into());
        }
        let _claim_owner = ticket
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let keys = ticket
            .checks
            .iter()
            .map(|check| check.key.clone())
            .collect::<Vec<_>>();
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len()
            || now <= 0
            || ticket.budget.state().closed
            || values
                .iter()
                .zip(&ticket.checks)
                .any(|(row, check)| row != &check.expected)
        {
            return Err(WorkspaceError::Fenced.into());
        }
        let raw = |key: &[u8]| {
            ticket
                .checks
                .iter()
                .find(|check| check.key.as_slice() == key)
                .and_then(|check| check.expected.as_deref())
                .ok_or(WorkspaceError::Fenced)
        };
        let original = ticket.released_mount();
        let mut control = topology_state_from_checks(&ticket.checks)?;
        let released: SnapshotLease = decode_open_value(
            raw(&hot_lease_key(
                original.guard.workspace_id,
                original.guard.lease_id,
            ))?,
            SOURCE_MAX_BYTES,
        )?;
        if released.lease_id != original.guard.lease_id
            || released.workspace_id != original.guard.workspace_id
            || released.holder_generation != original.guard.holder_generation
            || released.base_revision != ticket.receipt.base_revision
            || !released.writable
            || released.state != LeaseState::Released
            || control.leases.get(&released.lease_id) != Some(&released)
        {
            return Err(WorkspaceError::Fenced.into());
        }
        // Plain InitialSource retirement updates its authoritative hot row,
        // leaving CONTROL's historical Active mirror intact. Revalidate every
        // target lease against the ticket's same-packet hot evidence before
        // hydrating that mirror; all predecessor bytes remain in the claim CAS.
        for (id, catalog) in control
            .leases
            .iter_mut()
            .filter(|(_, row)| row.workspace_id == original.guard.workspace_id)
        {
            if !clean_source_routes_other_lease(*id, catalog, released.lease_id)? {
                continue;
            }
            let hot: SnapshotLease = decode_open_value(
                raw(&hot_lease_key(original.guard.workspace_id, *id))?,
                SOURCE_MAX_BYTES,
            )?;
            if clean_source_other_lease_invalidates(catalog, Some(&hot), &released)? {
                return Err(WorkspaceError::Fenced.into());
            }
            *catalog = hot;
        }
        let generation = original
            .guard
            .holder_generation
            .checked_add(1)
            .ok_or(WorkspaceError::Fenced)?;
        let expires_at_ns = checked_expiry(now, ttl_ns)?;
        let lease = SnapshotLease {
            lease_id,
            workspace_id: original.guard.workspace_id,
            base_revision: ticket.receipt.base_revision.clone(),
            holder_generation: generation,
            writable: true,
            state: LeaseState::Active,
            expires_at_ns,
            created_at_ns: now,
            updated_at_ns: now,
        };
        let open_generation = ticket.receipt.open_owner.as_ref().map_or(Ok(1), |owner| {
            owner
                .generation
                .checked_add(1)
                .ok_or(WorkspaceError::Fenced)
        })?;
        let open = SnapshotOpenRecord {
            workspace_id: original.guard.workspace_id,
            owner_id,
            generation: open_generation,
            expires_at_ns,
            state: SnapshotOpenState::Ready,
            recovery_required: false,
        };
        validate_open_record(&open, original.guard.workspace_id)?;
        if control.leases.contains_key(&lease_id)
            || control.leases.values().any(|row| {
                row.workspace_id == original.guard.workspace_id && row.state == LeaseState::Active
            })
        {
            return Err(WorkspaceError::Busy.into());
        }
        let mut workspace = control
            .workspaces
            .get(&original.guard.workspace_id)
            .cloned()
            .ok_or(WorkspaceError::Fenced)?;
        if workspace.active_lease.is_some() {
            return Err(WorkspaceError::Busy.into());
        }
        workspace.active_lease = Some(lease_id);
        workspace.updated_at_ns = now;
        let lease_keys = vec![
            hot_lease_key(workspace.workspace_id, lease_id),
            hot_lease_index_key(lease_id),
        ];
        let (lease_values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&lease_keys, source_limits(2))
            .await?;
        if lease_values.len() != 2 || lease_values.iter().any(Option::is_some) {
            return Err(WorkspaceError::Busy.into());
        }
        control.leases.insert(lease_id, lease.clone());
        if ticket.receipt.first_admin_claim.is_some() {
            return Err(WorkspaceError::Fenced.into());
        }
        let mut claimed_receipt = ticket.receipt.clone();
        claimed_receipt.first_admin_claim = Some(FirstAdminCleanClaim {
            lease: lease.clone(),
            open_owner: open.clone(),
        });
        let claimed_receipt_bytes = encode_clean_source(&claimed_receipt)?;
        let receipt_key = ticket.receipt.key();
        let mut immutable = [
            receipt_key.clone(),
            hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
        ]
        .iter()
        .map(|key| {
            ticket
                .checks
                .iter()
                .find(|check| &check.key == key)
                .cloned()
                .ok_or(WorkspaceError::Fenced)
        })
        .collect::<Result<Vec<_>, _>>()?;
        immutable
            .iter_mut()
            .find(|check| check.key == receipt_key)
            .ok_or(WorkspaceError::Fenced)?
            .expected = Some(claimed_receipt_bytes.clone());
        let mut checks = ticket.checks.clone();
        checks.extend(
            lease_keys
                .into_iter()
                .zip(lease_values)
                .map(|(key, expected)| KvCheck { key, expected }),
        );
        let mut writes = vec![
            put(hot_workspace_key(workspace.workspace_id), &workspace)?,
            put(hot_lease_key(workspace.workspace_id, lease_id), &lease)?,
            put(hot_lease_index_key(lease_id), &workspace.workspace_id)?,
            put(open_snapshot_key(original.guard.workspace_id), &open)?,
            KvWrite::Put {
                key: receipt_key,
                value: claimed_receipt_bytes,
            },
        ];
        let _writer_owner = self.prepare_administrative_packed_writer(
            original.guard.workspace_id, crate::workspace::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::ClaimClean, &mut checks, &mut writes,
        ).await?;
        let _native_holds = self
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await?;
        let packet = self
            .prepare_topology_envelope(checks, writes, Some(expires_at_ns))
            .await?;
        let checks = packet.checks.clone();
        let writes = packet.writes.clone();
        // One actual source-claim mutation, with every predecessor ticket byte.
        // An unknown result leaves recovery work; no fresh claim is replayed.
        if let Err(error) = self.clean_exact_cas(&checks, &writes, expires_at_ns).await {
            return Err(PackedHeadlessSnapshotFailure::held(
                ClaimAttemptOwner {
                    error,
                    ticket,
                    _checks: checks,
                    _writes: writes,
                },
                |owner| &owner.error,
            ));
        }
        let guard = HeadGuard {
            lease_id,
            holder_generation: generation,
            ..original.guard.clone()
        };
        Ok(PackedCleanPublicationAuthority {
            store: self.clone(),
            original,
            immutable,
            open,
            guard,
            budget: ticket.budget,
            mode: CleanAuthorityMode::Original,
            _owner: ticket._owner,
        })
    }

    async fn finish_clean_publication(
        &self,
        claim: &PackedCleanPublicationAuthority<B>,
        result: &PackedSnapshotResult,
        name: Option<String>,
        owner_id: Option<String>,
    ) -> Result<(), WorkspaceError> {
        if !std::ptr::eq(self, claim.store.as_ref())
            || claim.budget.state().closed
            || result.packed_carrier_revision != result.binding.base_revision
            || result.binding.workspace_id != claim.guard.workspace_id
        {
            return Err(WorkspaceError::Fenced);
        }
        let target_guard = HeadGuard {
            expected_head_layer_id: result.binding.head_layer_id,
            expected_head_epoch: result.binding.head_epoch,
            ..claim.guard.clone()
        };
        let mut keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(target_guard.workspace_id),
            hot_layer_key(target_guard.expected_head_layer_id),
            hot_layer_key(result.packed_carrier_revision.layer_id),
            hot_lease_key(target_guard.workspace_id, target_guard.lease_id),
            packed_current_key(target_guard.workspace_id),
            packed_claim_key(target_guard.workspace_id),
            packed_history_key(
                target_guard.workspace_id,
                result.binding.binding.binding_version,
            ),
            open_snapshot_key(target_guard.workspace_id),
            open_snapshot_recovery_key(target_guard.workspace_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            hot_snapshot_key(result.snapshot_id),
            published_clean_key(target_guard.workspace_id),
        ];
        keys.extend(claim.immutable.iter().map(|row| row.key.clone()));
        let name_key = name.as_ref().map(|name| snapshot_name_key(name));
        let name_index = name_key.as_ref().map(|key| {
            let index = keys.len();
            keys.push(key.clone());
            index
        });
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len()
            || now <= 0
            || values[14..]
                .iter()
                .zip(&claim.immutable)
                .any(|(value, row)| value != &row.expected)
        {
            return Err(WorkspaceError::Fenced);
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        validate_current_control_raw(Some(required(0)?))?;
        let mut workspace: WorkspaceRecord = decode_open_value(required(1)?, SOURCE_MAX_BYTES)?;
        let head: LayerRecord = decode_open_value(required(2)?, SOURCE_MAX_BYTES)?;
        let base: LayerRecord = decode_open_value(required(3)?, SOURCE_MAX_BYTES)?;
        let mut lease: SnapshotLease = decode_open_value(required(4)?, SOURCE_MAX_BYTES)?;
        let current = decode_packed_pair(
            target_guard.workspace_id,
            &values[5],
            &values[6],
            &values[7],
        )?
        .ok_or(WorkspaceError::Fenced)?;
        let mut open: SnapshotOpenRecord = decode_open_value(required(8)?, OPEN_RECORD_MAX_BYTES)?;
        let recovery: Option<SnapshotRecoveryRecord> = values[9]
            .as_deref()
            .map(|bytes| decode_open_value(bytes, OPEN_RECOVERY_MAX_BYTES))
            .transpose()?;
        validate_open_record(&open, target_guard.workspace_id)?;
        let open_matches = match claim.mode {
            CleanAuthorityMode::Original | CleanAuthorityMode::Committed => open == claim.open,
            CleanAuthorityMode::Recovery => {
                open.workspace_id == claim.open.workspace_id
                    && open.owner_id == claim.open.owner_id
                    && open.generation == claim.open.generation
                    && open.expires_at_ns == claim.open.expires_at_ns
                    && open.state == SnapshotOpenState::Ready
                    && !open.recovery_required
            }
        };
        checked_hot_guard(&workspace, &head, &lease, &target_guard, now)?;
        current.validate_for_guard(&target_guard, &base)?;
        if current != result.binding
            || workspace.state != WorkspaceState::Active
            || !open_matches
            || head.next_sequence != 1
            || head.owned_slice_count != 0
            || head.owned_bytes != 0
            || open.recovery_required
            || open.expires_at_ns <= now
            || recovery
                .as_ref()
                .is_some_and(|row| row.workspace_id != target_guard.workspace_id || row.incomplete)
            || base.state != LayerState::Sealed
            || base.depth != 1
            || base.parent_layer_id.is_some()
            || base.sealed_version != Some(result.packed_carrier_revision.sealed_version)
            || base.root_hash != Some(result.packed_carrier_revision.root_hash)
            || lease.base_revision != result.packed_carrier_revision
        {
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[10])?;
        layer_inventory_generation(&values[11])?;
        if values[12].is_some() || name_index.is_some_and(|index| values[index].is_some()) {
            return Err(WorkspaceError::Busy);
        }
        let deadline = lease.expires_at_ns.min(open.expires_at_ns);
        lease.state = LeaseState::Released;
        lease.updated_at_ns = now;
        workspace.active_lease = None;
        workspace.updated_at_ns = now;
        open.expires_at_ns = now;
        let snapshot = SnapshotRecord {
            snapshot_id: result.snapshot_id,
            name,
            revision: result.packed_carrier_revision.clone(),
            owner_id,
            created_at_ns: now,
        };
        let original_value = |key: &[u8]| {
            claim
                .immutable
                .iter()
                .find(|row| row.key == key)
                .and_then(|row| row.expected.clone())
                .ok_or(WorkspaceError::Fenced)
        };
        let receipt = PublishedCleanReceipt {
            snapshot: snapshot.clone(),
            binding: result.binding.encode()?,
            native_sealed_source_revision: result.native_sealed_source_revision.clone(),
            released_lease: lease.clone(),
            closed_open: open.clone(),
            original_receipt: original_value(
                &PackedCleanOperationOrigin::parse(&claim.open.owner_id)?
                    .ok_or(WorkspaceError::Fenced)?
                    .source_key(claim.original.guard.workspace_id),
            )?,
            original_released_lease: original_value(&hot_lease_key(
                claim.original.guard.workspace_id,
                claim.original.guard.lease_id,
            ))?,
        };
        let mut checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let mut writes = vec![
            put(hot_workspace_key(workspace.workspace_id), &workspace)?,
            put(hot_lease_key(lease.workspace_id, lease.lease_id), &lease)?,
            put(open_snapshot_key(open.workspace_id), &open)?,
            put(hot_snapshot_key(snapshot.snapshot_id), &snapshot)?,
            KvWrite::Put {
                key: published_clean_key(target_guard.workspace_id),
                value: encode_published_clean(&receipt)?,
            },
        ];
        if let Some(name_key) = name_key {
            writes.push(put(name_key, &snapshot.snapshot_id)?);
        }
        let _writer_owner = self.prepare_administrative_packed_writer(
            target_guard.workspace_id, crate::workspace::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::Retire, &mut checks, &mut writes,
        ).await?;
        let _holds = self
            .prepare_clean_publication_native_owner_cas(&mut checks, &mut writes, deadline)
            .await?;
        // Actual transport/reader shutdown precedes this one atomic persisted
        // snapshot + Released lease + closed existing open owner successor.
        self.clean_exact_cas(&checks, &writes, deadline).await
    }

    /// This is a report from an actual persisted headless finish transaction.
    /// It grants no mount cutoff, VFS drain or new publication authority.
    pub async fn inspect_clean_published_view(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<PackedPublishedViewReport>, WorkspaceError> {
        let _owner = self
            .packed_reader_pin_budget
            .get()
            .ok_or(WorkspaceError::Fenced)?
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let Some((report, read)) = self.read_clean_published_view(workspace_id).await? else {
            return Ok(None);
        };
        if !self
            .backend
            .authenticate_checks_before_bounded(
                &read.checks,
                checked_expiry(read.now, 30_000_000_000)?,
                source_limits(read.checks.len()),
            )
            .await?
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(Some(report))
    }

    async fn read_clean_published_view(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<(PackedPublishedViewReport, VirginPackedRead)>, WorkspaceError> {
        let receipt_key = published_clean_key(workspace_id);
        let (routing, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                std::slice::from_ref(&receipt_key),
                source_limits(1),
            )
            .await?;
        if routing.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let Some(raw) = routing[0].as_deref() else {
            return Ok(None);
        };
        let receipt = decode_published_clean(raw)?;
        let binding = PackedLowerBindingRecord::decode(&receipt.binding)?;
        let original = decode_any_clean_source(&receipt.original_receipt)?;
        let guard = HeadGuard {
            workspace_id,
            expected_head_layer_id: binding.head_layer_id,
            expected_head_epoch: binding.head_epoch,
            lease_id: receipt.released_lease.lease_id,
            holder_generation: receipt.released_lease.holder_generation,
        };
        let history = self
            .read_workspace_lease_history_checks(workspace_id, 14, source_limits(32))
            .await?;
        let mut keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(workspace_id),
            hot_layer_key(binding.head_layer_id),
            hot_layer_key(binding.base_revision.layer_id),
            hot_lease_key(workspace_id, guard.lease_id),
            packed_current_key(workspace_id),
            packed_claim_key(workspace_id),
            packed_history_key(workspace_id, binding.binding.binding_version),
            open_snapshot_key(workspace_id),
            open_snapshot_recovery_key(workspace_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            hot_snapshot_key(receipt.snapshot.snapshot_id),
            receipt_key,
            original.key(),
            hot_lease_key(workspace_id, original.guard.lease_id),
        ];
        append_workspace_history_keys(&mut keys, &history, 32)?;
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        authenticate_workspace_history_values(&keys, &values, &history)?;
        if values[13].as_deref() != Some(raw) {
            return Ok(None);
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        let control = topology_state_from_checks(
            &keys
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>(),
        )?;
        let workspace: WorkspaceRecord = decode_open_value(required(1)?, SOURCE_MAX_BYTES)?;
        let head: LayerRecord = decode_open_value(required(2)?, SOURCE_MAX_BYTES)?;
        let base: LayerRecord = decode_open_value(required(3)?, SOURCE_MAX_BYTES)?;
        let lease: SnapshotLease = decode_open_value(required(4)?, SOURCE_MAX_BYTES)?;
        let open: SnapshotOpenRecord = decode_open_value(required(8)?, OPEN_RECORD_MAX_BYTES)?;
        let recovery: Option<SnapshotRecoveryRecord> = values[9]
            .as_deref()
            .map(|bytes| decode_open_value(bytes, OPEN_RECOVERY_MAX_BYTES))
            .transpose()?;
        let snapshot: SnapshotRecord = decode_open_value(required(12)?, SOURCE_MAX_BYTES)?;
        let actual_binding = decode_packed_pair(workspace_id, &values[5], &values[6], &values[7])?
            .ok_or(WorkspaceError::Fenced)?;
        next_packed_root_generation(&values[10])?;
        layer_inventory_generation(&values[11])?;
        if actual_binding != binding
            || workspace.workspace_id != workspace_id
            || workspace.head_layer_id != binding.head_layer_id
            || workspace.head_epoch != binding.head_epoch
            || workspace.state != WorkspaceState::Active
            || workspace.active_lease.is_some()
            || head.layer_id != binding.head_layer_id
            || head.state != LayerState::Writable
            || head.next_sequence != 1
            || head.owned_slice_count != 0
            || head.owned_bytes != 0
            || base.state != LayerState::Sealed
            || base.depth != 1
            || base.parent_layer_id.is_some()
            || lease != receipt.released_lease
            || lease.state != LeaseState::Released
            || lease.workspace_id != workspace_id
            || lease.base_revision != binding.base_revision
            || !lease.writable
            || open != receipt.closed_open
            || open.expires_at_ns > now
            || open.recovery_required
            || snapshot != receipt.snapshot
            || snapshot.revision != binding.base_revision
            || original.guard.workspace_id != workspace_id
            || values[14].as_deref() != Some(receipt.original_receipt.as_slice())
            || values[15].as_deref() != Some(receipt.original_released_lease.as_slice())
            || recovery
                .as_ref()
                .is_some_and(|row| row.workspace_id != workspace_id || row.incomplete)
            || control.leases.values().any(|other| {
                other.workspace_id == workspace_id
                    && other.writable
                    && other.lease_id != lease.lease_id
                    && (other.state == LeaseState::Active
                        || other.created_at_ns >= lease.created_at_ns
                        || other.holder_generation >= lease.holder_generation)
            })
        {
            return Ok(None);
        }
        binding.validate_for_guard(&guard, &base)?;
        let mut checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let _writer_owner = self
            .authenticate_idle_packed_writer(workspace_id, &mut checks)
            .await?;
        Ok(Some((
            PackedPublishedViewReport {
                snapshot_id: snapshot.snapshot_id,
                head_epoch: binding.head_epoch,
                binding: binding.clone(),
            },
            VirginPackedRead {
                control,
                binding,
                checks,
                now,
            },
        )))
    }

    #[cfg(target_os = "linux")]
    pub async fn publish_clean_packed_snapshot<O, S>(
        self: &Arc<Self>,
        ticket: PackedCleanSourceTicket<B>,
        client: ObjectClient<O>,
        upper: Arc<S>,
        layout: ChunkLayout,
        request: PackedHeadlessSnapshotRequest,
    ) -> Result<PackedSnapshotResult, PackedHeadlessSnapshotFailure>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        self.require_admin_access()?;
        let store = self.clone();
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            WorkspaceError::CorruptMetadata(
                "packed snapshot requires a running Tokio runtime".into(),
            )
        })?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        handle.spawn(async move {
            let result = store
                .publish_clean_packed_snapshot_owned(ticket, client, upper, layout, request)
                .await;
            let _ = sender.send(result);
        });
        receiver.await.map_err(|_| {
            PackedHeadlessSnapshotFailure::from(WorkspaceError::CorruptMetadata(
                "owned packed snapshot driver stopped".into(),
            ))
        })?
    }

    #[cfg(target_os = "linux")]
    async fn publish_clean_packed_snapshot_owned<O, S>(
        self: &Arc<Self>,
        ticket: PackedCleanSourceTicket<B>,
        client: ObjectClient<O>,
        upper: Arc<S>,
        layout: ChunkLayout,
        request: PackedHeadlessSnapshotRequest,
    ) -> Result<PackedSnapshotResult, PackedHeadlessSnapshotFailure>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        if request.snapshot_id.as_uuid().is_nil()
            || request.native_journal_id.as_uuid().is_nil()
            || request.new_head_layer_id.as_uuid().is_nil()
            || request.max_rows == 0
            || request.scratch_disk_bytes < 16 << 10
            || layout.chunk_size == 0
            || request
                .temporary_owner
                .as_ref()
                .is_some_and(|owner| owner.path() != request.temporary)
        {
            return Err(WorkspaceError::Fenced.into());
        }
        let budget = ticket.budget.clone();
        let binding = PackedLowerBindingRecord::decode(
            ticket
                .checks
                .iter()
                .find(|check| check.key == packed_current_key(ticket.receipt.guard.workspace_id))
                .and_then(|check| check.expected.as_deref())
                .ok_or(WorkspaceError::Fenced)?,
        )?;
        let expected_layer = |layer: LayerId| -> Result<LayerRecord, WorkspaceError> {
            decode_open_value(
                ticket
                    .checks
                    .iter()
                    .find(|check| check.key == hot_layer_key(layer))
                    .and_then(|check| check.expected.as_deref())
                    .ok_or(WorkspaceError::Fenced)?,
                SOURCE_MAX_BYTES,
            )
        };
        let layers = [
            expected_layer(binding.head_layer_id)?,
            expected_layer(binding.base_revision.layer_id)?,
        ];
        let owner_id = clean_operation_owner(
            &ticket.released_mount(),
            ticket.receipt.kind(),
            &request,
            layout,
        )?;
        let claim = Arc::new(
            self.claim_clean_publication(ticket, request.lease_id, request.lease_ttl_ns, owner_id)
                .await
                .inspect_err(|_failure| {
                    #[cfg(test)]
                    eprintln!("[packed-headless-diag] stage=claim-clean-publication");
                })?,
        );
        let runtime = Arc::new(HeadlessRuntimeOwner {
            _claim: Some(claim.clone()),
            _store: self.clone(),
            budget: budget.clone(),
            _upper: upper.clone(),
            _temporary_owner: request.temporary_owner.clone(),
            cleanup_handle: tokio::runtime::Handle::current(),
            metadata: std::sync::Mutex::new(None),
            reader: std::sync::Mutex::new(None),
            native: std::sync::Mutex::new(None),
            finished: std::sync::atomic::AtomicBool::new(false),
        });
        claim
            .matches_snapshot_request(&request, layout)
            .map_err(|error| runtime_failure(error, &runtime))?;
        let guard = claim.guard().clone();
        let observer = budget
            .read_observer(client.read_observer())
            .map_err(|error| {
                runtime_failure(WorkspaceError::InvalidReadPlan(error.to_string()), &runtime)
            })?;
        let client = client.with_read_observer(
            observer,
            crate::cadapter::read_observer::Engine::Packed,
            crate::cadapter::read_observer::Phase::Startup,
            crate::cadapter::read_observer::Origin::Demand,
        );
        let snapshot = AuthenticatedSnapshot::open(&client, &binding.binding.manifest)
            .await
            .map_err(|error| {
                runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?;
        let lower = Arc::new(
            PackedReadonlyMeta::from_snapshot_budget(
                client.clone(),
                snapshot,
                layout.chunk_size,
                0,
                budget.clone(),
            )
            .map_err(|error| {
                runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?,
        );
        let metadata = Arc::new(
            WorkspaceMetaLayer::with_chunk_size(
                self.clone(),
                ViewContext {
                    workspace_id: guard.workspace_id,
                    head_layer_id: guard.expected_head_layer_id,
                    head_epoch: guard.expected_head_epoch,
                    lease_id: guard.lease_id,
                    holder_generation: guard.holder_generation,
                },
                layout.chunk_size,
            )
            .with_packed_lower_from_store_owned(lower, upper.clone(), layout, |reader| {
                *runtime.reader.lock().unwrap() = Some(reader);
            })
            .await
            .map_err(|error| {
                runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?,
        );
        *runtime.metadata.lock().unwrap() = Some(metadata.clone());
        metadata.initialize().await.map_err(|error| {
            runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
        })?;
        let vfs = VFS::from_workspace_components(VFSConfig::new(layout), upper, metadata.clone())
            .map_err(|error| {
            runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
        })?;
        let local = vfs.quiesce_packed_vfs().await.map_err(|error| {
            runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
        })?;
        let native = Arc::new(
            self.clone()
                .begin_clean_packed_native_quiesce(
                    claim.clone(),
                    layers,
                    request.native_journal_id,
                    request.new_head_layer_id,
                )
                .await
                .map_err(|error| runtime_failure(error, &runtime))
                .inspect_err(|_failure| {
                    #[cfg(test)]
                    eprintln!("[packed-headless-diag] stage=begin-clean-native");
                })?,
        );
        *runtime.native.lock().unwrap() = Some(native.clone());
        let artifact = FrozenNativeArtifact::capture(
            native,
            local,
            request.temporary.clone(),
            NativeCaptureLimits {
                max_inodes: request.max_rows,
                max_names: request.max_rows,
                max_spans: request.max_rows,
                max_logical_bytes: request.max_logical_bytes,
                max_data_bytes: request.max_data_bytes,
                max_payload_disk_bytes: request.scratch_disk_bytes,
                max_sqlite_disk_bytes: request.scratch_disk_bytes,
                max_producer_spool_disk_bytes: request.scratch_disk_bytes,
                sqlite_cache_bytes: 64 << 10,
                max_sql_vm_steps: 1_000_000,
            },
            request.cancel.clone(),
        )
        .await
        .map_err(|error| {
            runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
        })?;
        let ready = self
            .prepare_frozen_native_publication(
                artifact,
                client,
                NativePublicationBuildOptions {
                    producer: request.producer,
                    temporary: request.temporary.clone(),
                    graph_scratch: request.temporary,
                    graph_limits: request.graph_limits,
                    native_hash_limits: NativeDeltaHashLimits {
                        max_native_delta_rows: request.max_rows,
                        max_canonical_bytes: request.scratch_disk_bytes,
                    },
                    chunk_size: layout.chunk_size,
                    metadata_cache_bytes: 0,
                    max_catalog_rows: request.max_rows,
                    cancel: request.cancel,
                },
            )
            .await
            .map_err(|failure| {
                PackedHeadlessSnapshotFailure::held(
                    (failure, runtime.clone()),
                    |owner| match &owner.0 {
                        NativePublicationPreparationFailure::BeforeHashed(error)
                        | NativePublicationPreparationFailure::HashedAdmission { error, .. }
                        | NativePublicationPreparationFailure::Hashed { error, .. } => error,
                        NativePublicationPreparationFailure::Promotion { failure, .. } => {
                            &failure.error
                        }
                    },
                )
            })
            .inspect_err(|_failure| {
                #[cfg(test)]
                eprintln!("[packed-headless-diag] stage=prepare-native-publication");
            })?;
        let outcome = ready
            .commit()
            .await
            .map_err(|failure| {
                PackedHeadlessSnapshotFailure::held((failure, runtime.clone()), |owner| {
                    &owner.0.error
                })
            })
            .inspect_err(|_failure| {
                #[cfg(test)]
                eprintln!("[packed-headless-diag] stage=commit-native-publication");
            })?;
        let result = PackedSnapshotResult {
            snapshot_id: request.snapshot_id,
            packed_carrier_revision: outcome.binding.base_revision.clone(),
            native_sealed_source_revision: outcome.sealed_source,
            binding: outcome.binding,
        };
        let cleanup = async {
            metadata
                .shutdown_packed_runtime_for_clean_release()
                .await
                .map_err(|error| WorkspaceError::CorruptMetadata(error.to_string()))?;
            self.finish_clean_publication(&claim, &result, request.snapshot_name, request.owner_id)
                .await
        }
        .await;
        if let Err(error) = cleanup {
            let mut failure = runtime_failure(error, &runtime);
            failure.committed = Some(Box::new(result));
            return Err(failure);
        }
        budget.close();
        runtime
            .finished
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(result)
    }
}

#[cfg(target_os = "linux")]
#[path = "packed_admin/packed_initial_snapshot.rs"]
mod initial_snapshot;
#[cfg(not(target_os = "linux"))]
#[path = "packed_admin/packed_initial_unsupported.rs"]
mod initial_snapshot;
pub(crate) use initial_snapshot::PackedInitialBootstrapAuthority;
