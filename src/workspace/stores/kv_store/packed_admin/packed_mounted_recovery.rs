//! Typed packed recovery of an expired joint mount using its original PVC.
//! A recovery owner never becomes an ordinary Ready writer through expiry.

use super::*;
#[path = "packed_clean_source.rs"]
mod clean_source;
use crate::workspace::stores::kv_store::packed_mount_writeback::{
    init_packed_mount_writeback_identity, verify_packed_mount_writeback_identity,
};
pub use clean_source::PackedRecoveredMountReport;
pub(in crate::workspace::stores::kv_store::packed_admin) use clean_source::{
    CleanSourceKind, CleanSourceRecord, decode_any_clean_source, decode_clean_source,
    encode_clean_source,
};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};

const RECOVERY_MAGIC: &[u8; 5] = b"PMR3\x01";
const RECOVERY_BYTES: usize = 8192;
// This serializes duplicate recovery drivers, not native allocator/catalog
// transactions. Holding a process-wide topology lock through replay would deadlock next_id.
static MOUNTED_RECOVERY_DRIVER_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(crate) struct PackedMountedRecoveryRequest {
    pub(crate) original: PackedReleasedMountReference,
    pub(crate) lease_id: LeaseId,
    pub(crate) recovery_pod_uid: uuid::Uuid,
    pub(crate) ttl_ns: u64,
}

pub(crate) struct PackedMountedRecoveryResult {
    pub(crate) original: PackedReleasedMountReference,
    pub(crate) released: PackedReleasedMountReference,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryReference {
    workspace_id: WorkspaceId,
    head_layer_id: LayerId,
    head_epoch: u64,
    lease_id: LeaseId,
    holder_generation: u64,
    mount_uid: uuid::Uuid,
    pod_uid: uuid::Uuid,
}

impl RecoveryReference {
    fn from_reference(reference: &PackedReleasedMountReference) -> Self {
        Self {
            workspace_id: reference.guard.workspace_id,
            head_layer_id: reference.guard.expected_head_layer_id,
            head_epoch: reference.guard.expected_head_epoch,
            lease_id: reference.guard.lease_id,
            holder_generation: reference.guard.holder_generation,
            mount_uid: reference.mount_uid,
            pod_uid: reference.pod_uid,
        }
    }

    fn reference(&self) -> PackedReleasedMountReference {
        PackedReleasedMountReference {
            guard: HeadGuard {
                workspace_id: self.workspace_id,
                expected_head_layer_id: self.head_layer_id,
                expected_head_epoch: self.head_epoch,
                lease_id: self.lease_id,
                holder_generation: self.holder_generation,
            },
            mount_uid: self.mount_uid,
            pod_uid: self.pod_uid,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::workspace::stores::kv_store::packed_admin) struct MountedRecoveryRecord {
    original: RecoveryReference,
    current: RecoveryReference,
    original_writer_incarnation: u64,
    writer_incarnation: u64,
    binding_digest: [u8; 32],
    open_owner: String,
    open_generation: u64,
    completed: bool,
    base_revision: BaseRevision,
    binding_version: u64,
    manifest_digest: [u8; 32],
    original_released_lease: Option<SnapshotLease>,
    released_lease: Option<SnapshotLease>,
    closed_open: Option<SnapshotOpenRecord>,
    head_sequence: Option<u64>,
    first_admin_claim: Option<FirstAdminCleanClaim>,
}

fn recovery_key(reference: &PackedReleasedMountReference) -> Vec<u8> {
    format!(
        "packed/mount-recovery/{}/{}",
        reference.guard.workspace_id, reference.guard.lease_id
    )
    .into_bytes()
}

fn binding_digest(binding: &PackedLowerBindingRecord) -> Result<[u8; 32], WorkspaceError> {
    Ok(sha2::Sha256::digest(binding.encode()?).into())
}

fn encode_recovery(record: &MountedRecoveryRecord) -> Result<Vec<u8>, WorkspaceError> {
    let original = &record.original;
    let current = &record.current;
    if original.workspace_id != current.workspace_id
        || original.head_layer_id != current.head_layer_id
        || original.head_epoch != current.head_epoch
        || original.mount_uid != current.mount_uid
        || original.lease_id == current.lease_id
        || original.workspace_id.as_uuid().is_nil()
        || original.lease_id.as_uuid().is_nil()
        || current.lease_id.as_uuid().is_nil()
        || original.mount_uid.is_nil()
        || original.pod_uid.is_nil()
        || current.pod_uid.is_nil()
        || original.holder_generation == 0
        || current.holder_generation <= original.holder_generation
        || record.original_writer_incarnation == 0
        || record.writer_incarnation <= record.original_writer_incarnation
        || record.binding_digest == [0; 32]
        || record.open_generation == 0
        || record.binding_version == 0
        || record.open_owner.is_empty()
        || record.open_owner.len() > OPEN_OWNER_MAX_BYTES
    {
        return Err(WorkspaceError::Fenced);
    }
    if record.first_admin_claim.is_some()
        || recovery_open_owner(
            &record.original.reference(),
            &record.binding_digest,
            record.current.lease_id,
            record.current.pod_uid,
            record.current.holder_generation,
        )? != record.open_owner
    {
        return Err(WorkspaceError::Fenced);
    }
    match (
        &record.original_released_lease,
        &record.released_lease,
        &record.closed_open,
        record.head_sequence,
    ) {
        (None, None, None, None) if !record.completed && record.first_admin_claim.is_none() => {}
        (Some(original_lease), Some(lease), Some(open), Some(_)) if record.completed => {
            if original_lease.lease_id != original.lease_id
                || original_lease.workspace_id != original.workspace_id
                || original_lease.holder_generation != original.holder_generation
                || original_lease.state != LeaseState::Released
                || !original_lease.writable
                || original_lease.base_revision != record.base_revision
                || lease.lease_id != current.lease_id
                || lease.workspace_id != current.workspace_id
                || lease.holder_generation != current.holder_generation
                || !lease.writable
                || lease.state != LeaseState::Released
                || lease.base_revision != record.base_revision
                || lease.updated_at_ns != original_lease.updated_at_ns
                || lease.updated_at_ns <= 0
                || open.workspace_id != current.workspace_id
                || open.owner_id != record.open_owner
                || open.generation != record.open_generation
                || open.expires_at_ns != lease.updated_at_ns
                || open.state != SnapshotOpenState::Ready
                || open.recovery_required
            {
                return Err(WorkspaceError::Fenced);
            }
            validate_open_record(open, current.workspace_id)?;
        }
        _ => return Err(WorkspaceError::Fenced),
    }
    let mut bytes = RECOVERY_MAGIC.to_vec();
    bytes.extend(serde_json::to_vec(record).map_err(|_| WorkspaceError::Fenced)?);
    if bytes.len() > RECOVERY_BYTES {
        return Err(WorkspaceError::Fenced);
    }
    Ok(bytes)
}

fn decode_recovery(raw: &[u8]) -> Result<MountedRecoveryRecord, WorkspaceError> {
    if raw.len() > RECOVERY_BYTES || !raw.starts_with(RECOVERY_MAGIC) {
        return Err(WorkspaceError::Fenced);
    }
    let record =
        serde_json::from_slice(&raw[RECOVERY_MAGIC.len()..]).map_err(|_| WorkspaceError::Fenced)?;
    if encode_recovery(&record)? != raw {
        return Err(WorkspaceError::Fenced);
    }
    Ok(record)
}

fn recovery_open_owner(
    original: &PackedReleasedMountReference,
    digest: &[u8; 32],
    lease: LeaseId,
    pod: uuid::Uuid,
    generation: u64,
) -> Result<String, WorkspaceError> {
    let mut hash = sha2::Sha256::new();
    hash.update(b"BrewFS-packed-mounted-PVC-recovery\0");
    hash.update(
        serde_json::to_vec(&RecoveryReference::from_reference(original))
            .map_err(|_| WorkspaceError::Fenced)?,
    );
    hash.update(digest);
    let owner = format!(
        "packed/mount-recovery/{:x}/{pod}/{lease}/{generation}",
        hash.finalize()
    );
    if owner.len() > OPEN_OWNER_MAX_BYTES {
        return Err(WorkspaceError::Fenced);
    }
    Ok(owner)
}

/// Its constructor is the actual joint recovery claim below. Public sessions
/// cannot manufacture this owner by copying a lease or Recovering open token.
pub(crate) struct PackedMountedRecoveryMutationOwner {
    backend_identity: usize,
    budget: Arc<SnapshotMountBudget>,
    guard: HeadGuard,
    binding: PackedLowerBindingRecord,
    record: MountedRecoveryRecord,
    original_epoch: u64,
    closed: AtomicBool,
    _owner: SnapshotOwnedPermit,
}

tokio::task_local! {
    static MOUNTED_RECOVERY_MUTATION: Arc<PackedMountedRecoveryMutationOwner>;
}

pub(crate) fn capture_mounted_recovery_owner() -> Option<Arc<PackedMountedRecoveryMutationOwner>> {
    MOUNTED_RECOVERY_MUTATION.try_with(Arc::clone).ok()
}

impl PackedMountedRecoveryMutationOwner {
    pub(crate) async fn scope<F: Future>(self: Arc<Self>, future: F) -> F::Output {
        MOUNTED_RECOVERY_MUTATION.scope(self, future).await
    }

    pub(crate) fn validate_dirty_epoch(&self, epoch: u64) -> Result<(), WorkspaceError> {
        if self.closed.load(Ordering::Acquire) || epoch != self.original_epoch {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
}

/// A private driver token is authenticated against the operation's complete
/// exact-check packet before its real native mutation CAS. Errors never fall
/// back to ordinary authorization. Absence of the token grants nothing.
pub(in crate::workspace::stores::kv_store) fn authenticate_mounted_recovery_writer(
    backend_identity: usize,
    budget: Option<&Arc<SnapshotMountBudget>>,
    checks: &[KvCheck],
    guard: &HeadGuard,
    lease: &SnapshotLease,
    now: i64,
) -> Result<Option<PackedWriterAuthority>, WorkspaceError> {
    let Some(owner) = capture_mounted_recovery_owner() else {
        return Ok(None);
    };
    if owner.closed.load(Ordering::Acquire)
        || owner.backend_identity != backend_identity
        || !budget.is_some_and(|budget| Arc::ptr_eq(budget, &owner.budget))
        || guard != &owner.guard
        || now <= 0
        || lease.state != LeaseState::Active
        || lease.expires_at_ns <= now
        || !lease.writable
        || lease.lease_id != guard.lease_id
        || lease.workspace_id != guard.workspace_id
        || lease.holder_generation != guard.holder_generation
        || lease.base_revision != owner.binding.base_revision
    {
        return Err(WorkspaceError::Fenced);
    }
    let raw_lease: SnapshotLease =
        scoped_required(checks, &hot_lease_key(guard.workspace_id, guard.lease_id))?;
    let workspace: WorkspaceRecord =
        scoped_required(checks, &hot_workspace_key(guard.workspace_id))?;
    let writer = PackedWriterAuthority::decode(
        scoped_raw(checks, &packed_writer_key(guard.workspace_id))?
            .ok_or(WorkspaceError::Fenced)?,
        guard.workspace_id,
    )?;
    let open: SnapshotOpenRecord = scoped_required(checks, &open_snapshot_key(guard.workspace_id))?;
    let recovery: SnapshotRecoveryRecord =
        scoped_required(checks, &open_snapshot_recovery_key(guard.workspace_id))?;
    let binding = PackedLowerBindingRecord::decode(
        scoped_raw(checks, &packed_current_key(guard.workspace_id))?
            .ok_or(WorkspaceError::Fenced)?,
    )?;
    let expected = PackedWriterOwner::Administrative {
        lease_id: guard.lease_id,
        holder_generation: guard.holder_generation,
        open_owner: owner.record.open_owner.clone(),
        open_generation: owner.record.open_generation,
        recovering: true,
    };
    if workspace.workspace_id != guard.workspace_id
        || workspace.state != WorkspaceState::Active
        || workspace.head_layer_id != guard.expected_head_layer_id
        || workspace.head_epoch != guard.expected_head_epoch
        || workspace.active_lease != Some(guard.lease_id)
        || raw_lease != *lease
        || writer.incarnation != owner.record.writer_incarnation
        || writer.owner.as_ref() != Some(&expected)
        || binding != owner.binding
        || open.workspace_id != guard.workspace_id
        || open.owner_id != owner.record.open_owner
        || open.generation != owner.record.open_generation
        || open.state != SnapshotOpenState::Recovering
        || !open.recovery_required
        || open.expires_at_ns != lease.expires_at_ns
        || recovery.workspace_id != guard.workspace_id
        || !recovery.incomplete
    {
        return Err(WorkspaceError::Fenced);
    }
    validate_open_record(&open, guard.workspace_id)?;
    Ok(Some(writer))
}

struct MountedRecoveryAuthority<B> {
    store: Arc<KvWorkspaceStore<B>>,
    reference: PackedReleasedMountReference,
    binding: PackedLowerBindingRecord,
    record: MountedRecoveryRecord,
    budget: Arc<SnapshotMountBudget>,
}

impl<B: WorkspaceKvBackend> MountedRecoveryAuthority<B> {
    fn authenticate(
        &self,
        view: &ScopedMountView,
    ) -> Result<(SnapshotLease, SnapshotOpenRecord), WorkspaceError> {
        let lease = view.requested_lease.clone().ok_or(WorkspaceError::Fenced)?;
        let open = view.open.clone().ok_or(WorkspaceError::Fenced)?;
        let guard = &self.reference.guard;
        let expected = PackedWriterOwner::Administrative {
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
            open_owner: self.record.open_owner.clone(),
            open_generation: self.record.open_generation,
            recovering: true,
        };
        let persisted = decode_recovery(
            scoped_raw(
                &view.checks,
                &recovery_key(&self.record.original.reference()),
            )?
            .ok_or(WorkspaceError::Fenced)?,
        )?;
        if persisted != self.record
            || view.binding != self.binding
            || view.writer.incarnation != self.record.writer_incarnation
            || view.writer.owner.as_ref() != Some(&expected)
            || lease.workspace_id != guard.workspace_id
            || lease.lease_id != guard.lease_id
            || lease.holder_generation != guard.holder_generation
            || !lease.writable
            || lease.state != LeaseState::Active
            || lease.expires_at_ns <= view.now
            || lease.base_revision != self.binding.base_revision
            || open.workspace_id != guard.workspace_id
            || open.owner_id != self.record.open_owner
            || open.generation != self.record.open_generation
            || open.state != SnapshotOpenState::Recovering
            || !open.recovery_required
            || open.expires_at_ns != lease.expires_at_ns
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok((lease, open))
    }

    async fn renew(&self, ttl_ns: u64) -> Result<(), WorkspaceError> {
        let _owner = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let origin = self.record.original.reference();
        let extra = [
            recovery_key(&origin),
            hot_lease_key(origin.guard.workspace_id, origin.guard.lease_id),
        ];
        let view = self
            .store
            .scoped_packed_mount_view(
                self.reference.guard.workspace_id,
                self.reference.guard.lease_id,
                true,
                &extra,
            )
            .await?;
        let (mut lease, mut open) = self.authenticate(&view)?;
        let deadline = lease.expires_at_ns;
        let expiry = checked_expiry(view.now, ttl_ns)?;
        lease.expires_at_ns = expiry;
        lease.updated_at_ns = view.now;
        open.expires_at_ns = expiry;
        let writes = vec![
            put(hot_lease_key(lease.workspace_id, lease.lease_id), &lease)?,
            put(open_snapshot_key(lease.workspace_id), &open)?,
        ];
        self.store
            .commit_packed_mount_packet(view.checks, writes, deadline.min(expiry))
            .await
    }
}

impl<B: WorkspaceKvBackend> PackedMountedLease<B> {
    pub(crate) async fn initialize_writeback_identity(
        &self,
        config: &VFSConfig,
    ) -> Result<(), WorkspaceError> {
        init_packed_mount_writeback_identity(&self.reference, &self.binding, &self.budget, config)
            .await
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(crate) async fn recover_packed_mounted_session<O, S>(
        self: &Arc<Self>,
        request: PackedMountedRecoveryRequest,
        client: ObjectClient<O>,
        upper: Arc<S>,
        config: VFSConfig,
    ) -> Result<PackedMountedRecoveryResult, WorkspaceError>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let result = store
                .recover_packed_mounted_session_owned(request, client, upper, config)
                .await;
            let _ = sender.send(result);
        });
        let received = receiver.await;
        task.await
            .map_err(|_| WorkspaceError::Backend("owned packed mounted recovery stopped".into()))?;
        received.map_err(|_| {
            WorkspaceError::Backend("owned packed mounted recovery result unavailable".into())
        })?
    }

    async fn recover_packed_mounted_session_owned<O, S>(
        self: &Arc<Self>,
        request: PackedMountedRecoveryRequest,
        client: ObjectClient<O>,
        upper: Arc<S>,
        config: VFSConfig,
    ) -> Result<PackedMountedRecoveryResult, WorkspaceError>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        if request.lease_id.as_uuid().is_nil()
            || request.recovery_pod_uid.is_nil()
            || request.lease_id == request.original.guard.lease_id
            || request.ttl_ns == 0
            || request.ttl_ns > 15 * 60 * 1_000_000_000
        {
            return Err(WorkspaceError::Fenced);
        }
        let budget = self.packed_reader_pin_budget.get().cloned().ok_or(
            WorkspaceError::UnsupportedCapability("canonical packed mounted recovery budget"),
        )?;
        let recovered = async {
            let _operation_owner = budget
                .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
                .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
            // Serialize duplicate drivers in this process. The durable PWA CAS
            // still fences separate recovery pods and all old writer generations.
            let _local_recovery = MOUNTED_RECOVERY_DRIVER_GATE.lock().await;
            let origin_key = recovery_key(&request.original);
            let extra = [
                origin_key.clone(),
                hot_lease_key(
                    request.original.guard.workspace_id,
                    request.original.guard.lease_id,
                ),
            ];
            let view = self
                .scoped_packed_mount_view(
                    request.original.guard.workspace_id,
                    request.lease_id,
                    true,
                    &extra,
                )
                .await?;
            if view.binding.head_layer_id != request.original.guard.expected_head_layer_id
                || view.binding.head_epoch != request.original.guard.expected_head_epoch
            {
                return Err(WorkspaceError::Fenced);
            }
            let digest = binding_digest(&view.binding)?;
            let prior = scoped_raw(&view.checks, &origin_key)?
                .map(decode_recovery)
                .transpose()?;
            if let Some(prior) = &prior {
                if prior.original != RecoveryReference::from_reference(&request.original)
                    || prior.binding_digest != digest
                {
                    return Err(WorkspaceError::Fenced);
                }
                if prior.completed {
                    return self
                        .confirm_completed_mounted_recovery(prior.clone(), budget.clone())
                        .await;
                }
            }
            verify_packed_mount_writeback_identity(
                &request.original,
                &view.binding,
                &budget,
                &config,
            )
            .await?;
            let open = view.open.as_ref().ok_or(WorkspaceError::Fenced)?;
            let (previous_id, previous_generation) = view
                .writer
                .owner
                .as_ref()
                .ok_or(WorkspaceError::Fenced)?
                .lease_identity();
            let previous: SnapshotLease = scoped_required(
                &view.checks,
                &hot_lease_key(request.original.guard.workspace_id, previous_id),
            )?;
            let resume = match (&view.writer.owner, &prior) {
                (
                    Some(PackedWriterOwner::Mounted {
                        lease_id,
                        holder_generation,
                        open_owner,
                        open_generation,
                    }),
                    None,
                ) => {
                    let expected_owner = mount_owner(&PackedMountGrantRequest {
                        workspace_id: request.original.guard.workspace_id,
                        lease_id: request.original.guard.lease_id,
                        holder_generation: request.original.guard.holder_generation,
                        mount_uid: request.original.mount_uid,
                        pod_uid: request.original.pod_uid,
                        ttl_ns: 1,
                    })?;
                    if *lease_id != request.original.guard.lease_id
                        || *holder_generation != request.original.guard.holder_generation
                        || *open_owner != expected_owner
                        || *open_generation != open.generation
                        || open.state != SnapshotOpenState::Ready
                        || open.recovery_required
                    {
                        return Err(WorkspaceError::Fenced);
                    }
                    false
                }
                (
                    Some(PackedWriterOwner::Administrative {
                        lease_id,
                        holder_generation,
                        open_owner,
                        open_generation,
                        recovering: true,
                    }),
                    Some(prior),
                ) => {
                    if *lease_id != prior.current.lease_id
                        || *holder_generation != prior.current.holder_generation
                        || *open_owner != prior.open_owner
                        || *open_generation != prior.open_generation
                        || prior.writer_incarnation != view.writer.incarnation
                        || open.state != SnapshotOpenState::Recovering
                        || !open.recovery_required
                    {
                        return Err(WorkspaceError::Fenced);
                    }
                    previous.state == LeaseState::Active
                        && previous.expires_at_ns > view.now
                        && *lease_id == request.lease_id
                        && prior.current.pod_uid == request.recovery_pod_uid
                }
                _ => return Err(WorkspaceError::Fenced),
            };
            let authority = if resume {
                let prior = prior.ok_or(WorkspaceError::Fenced)?;
                if !self
                    .backend
                    .authenticate_checks_before_bounded(
                        &view.checks,
                        previous.expires_at_ns,
                        source_limits(view.checks.len()),
                    )
                    .await?
                {
                    return Err(WorkspaceError::Fenced);
                }
                Arc::new(MountedRecoveryAuthority {
                    store: self.clone(),
                    reference: prior.current.reference(),
                    binding: view.binding,
                    record: prior,
                    budget: budget.clone(),
                })
            } else {
                if previous.expires_at_ns > view.now
                    || open.expires_at_ns > view.now
                    || !matches!(previous.state, LeaseState::Active | LeaseState::Expired)
                    || view.requested_lease.is_some()
                {
                    return Err(WorkspaceError::Busy);
                }
                let generation = previous_generation
                    .checked_add(1)
                    .ok_or(WorkspaceError::Fenced)?;
                if config.workspace_writer_epoch != generation {
                    return Err(WorkspaceError::Fenced);
                }
                let open_generation = open
                    .generation
                    .checked_add(1)
                    .ok_or(WorkspaceError::Fenced)?;
                let open_owner = recovery_open_owner(
                    &request.original,
                    &digest,
                    request.lease_id,
                    request.recovery_pod_uid,
                    generation,
                )?;
                let expires_at_ns = checked_expiry(view.now, request.ttl_ns)?;
                let lease = SnapshotLease {
                    lease_id: request.lease_id,
                    workspace_id: request.original.guard.workspace_id,
                    base_revision: view.binding.base_revision.clone(),
                    holder_generation: generation,
                    writable: true,
                    state: LeaseState::Active,
                    expires_at_ns,
                    created_at_ns: view.now,
                    updated_at_ns: view.now,
                };
                let current = PackedReleasedMountReference {
                    guard: HeadGuard {
                        lease_id: lease.lease_id,
                        holder_generation: generation,
                        ..request.original.guard.clone()
                    },
                    mount_uid: request.original.mount_uid,
                    pod_uid: request.recovery_pod_uid,
                };
                let next = view
                    .writer
                    .successor(Some(PackedWriterOwner::Administrative {
                        lease_id: lease.lease_id,
                        holder_generation: generation,
                        open_owner: open_owner.clone(),
                        open_generation,
                        recovering: true,
                    }))?;
                let record = MountedRecoveryRecord {
                    original: RecoveryReference::from_reference(&request.original),
                    current: RecoveryReference::from_reference(&current),
                    original_writer_incarnation: prior
                        .as_ref()
                        .map_or(view.writer.incarnation, |prior| {
                            prior.original_writer_incarnation
                        }),
                    writer_incarnation: next.incarnation,
                    binding_digest: digest,
                    open_owner: open_owner.clone(),
                    open_generation,
                    completed: false,
                    base_revision: view.binding.base_revision.clone(),
                    binding_version: view.binding.binding.binding_version,
                    manifest_digest: view.binding.binding.manifest.digest,
                    original_released_lease: None,
                    released_lease: None,
                    closed_open: None,
                    head_sequence: None,
                    first_admin_claim: None,
                };
                let mut workspace: WorkspaceRecord =
                    scoped_required(&view.checks, &hot_workspace_key(lease.workspace_id))?;
                // Authenticate the previous persisted state before changing an
                // Active lease into Expired in this takeover packet.
                if !workspace_authenticates_mount_owner(&workspace, &previous, true, view.now) {
                    return Err(WorkspaceError::Fenced);
                }
                let mut expired = previous;
                expired.state = LeaseState::Expired;
                expired.updated_at_ns = view.now;
                let open = SnapshotOpenRecord {
                    workspace_id: lease.workspace_id,
                    owner_id: open_owner,
                    generation: open_generation,
                    expires_at_ns,
                    state: SnapshotOpenState::Recovering,
                    recovery_required: true,
                };
                workspace.active_lease = Some(lease.lease_id);
                workspace.updated_at_ns = view.now;
                let writes = vec![
                    put(hot_workspace_key(lease.workspace_id), &workspace)?,
                    put(
                        hot_lease_key(expired.workspace_id, expired.lease_id),
                        &expired,
                    )?,
                    put(hot_lease_key(lease.workspace_id, lease.lease_id), &lease)?,
                    put(hot_lease_index_key(lease.lease_id), &lease.workspace_id)?,
                    put(open_snapshot_key(lease.workspace_id), &open)?,
                    put(
                        open_snapshot_recovery_key(lease.workspace_id),
                        &SnapshotRecoveryRecord {
                            workspace_id: lease.workspace_id,
                            incomplete: true,
                        },
                    )?,
                    KvWrite::Put {
                        key: packed_writer_key(lease.workspace_id),
                        value: next.encode()?,
                    },
                    KvWrite::Put {
                        key: origin_key.clone(),
                        value: encode_recovery(&record)?,
                    },
                ];
                self.commit_packed_mount_packet(view.checks, writes, expires_at_ns)
                    .await?;
                Arc::new(MountedRecoveryAuthority {
                    store: self.clone(),
                    reference: current,
                    binding: view.binding,
                    record,
                    budget: budget.clone(),
                })
            };
            if config.workspace_writer_epoch != authority.reference.guard.holder_generation {
                return Err(WorkspaceError::Fenced);
            }
            let token_owner = budget
                .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
                .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
            let token = Arc::new(PackedMountedRecoveryMutationOwner {
                backend_identity: Arc::as_ptr(&self.backend) as usize,
                budget: budget.clone(),
                guard: authority.reference.guard.clone(),
                binding: authority.binding.clone(),
                record: authority.record.clone(),
                original_epoch: request.original.guard.holder_generation,
                closed: AtomicBool::new(false),
                _owner: token_owner,
            });
            let cancel = CancellationToken::new();
            let heartbeat_cancel = cancel.clone();
            let heartbeat_authority = authority.clone();
            let ttl_ns = request.ttl_ns;
            let heartbeat = tokio::spawn(async move {
                let interval_ns = (ttl_ns / 3).max(1_000_000);
                let mut timer = tokio::time::interval(std::time::Duration::from_nanos(interval_ns));
                timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                timer.tick().await;
                loop {
                    tokio::select! {
                        _ = heartbeat_cancel.cancelled() => return Ok(()),
                        _ = timer.tick() => {
                            // Cancellation is observed only after the accepted CAS
                            // completes, so joining this task really drains renewal.
                            heartbeat_authority.renew(ttl_ns).await?;
                        }
                    }
                }
            });
            let built = token
                .clone()
                .scope(async {
                    let snapshot =
                        AuthenticatedSnapshot::open(&client, &authority.binding.binding.manifest)
                            .await
                            .map_err(|error| WorkspaceError::CorruptMetadata(error.to_string()))?;
                    let layout = config.write.layout;
                    let lower = Arc::new(
                        PackedReadonlyMeta::from_snapshot_budget(
                            client.clone(),
                            snapshot,
                            layout.chunk_size,
                            0,
                            budget.clone(),
                        )
                        .map_err(|error| WorkspaceError::CorruptMetadata(error.to_string()))?,
                    );
                    let guard = &authority.reference.guard;
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
                        .with_packed_lower_from_store(lower, upper.clone(), layout)
                        .await
                        .map_err(|error| WorkspaceError::CorruptMetadata(error.to_string()))?,
                    );
                    // Construction captures this opaque owner explicitly into the
                    // actual spawned PVC replay driver. No FUSE attachment is attempted.
                    let vfs = match VFS::from_workspace_components(config, upper, metadata.clone())
                    {
                        Ok(vfs) => vfs,
                        Err(error) => {
                            let _ = metadata.shutdown_packed_runtime_for_clean_release().await;
                            return Err(WorkspaceError::CorruptMetadata(error.to_string()));
                        }
                    };
                    Ok::<_, WorkspaceError>((metadata, vfs))
                })
                .await;
            let recovered = match built {
                Ok((metadata, vfs)) => {
                    let drain = vfs
                        .quiesce_packed_vfs()
                        .await
                        .map_err(|error| WorkspaceError::CorruptMetadata(error.to_string()));
                    cancel.cancel();
                    let joined = heartbeat.await.unwrap_or_else(|_| {
                        Err(WorkspaceError::Backend(
                            "owned packed recovery heartbeat stopped".into(),
                        ))
                    });
                    token.closed.store(true, Ordering::Release);
                    match (drain, joined) {
                        (Ok(drain), Ok(())) => {
                            self.finish_packed_mounted_recovery(&authority, metadata, drain)
                                .await
                        }
                        (Err(error), _) | (_, Err(error)) => {
                            let _ = metadata.shutdown_packed_runtime_for_clean_release().await;
                            Err(error)
                        }
                    }
                }
                Err(error) => {
                    cancel.cancel();
                    let _ = heartbeat.await;
                    token.closed.store(true, Ordering::Release);
                    Err(error)
                }
            };
            // The reply carries facts only. Revoke and drop the actual mutation
            // token and its admission before closing the ledger and delivering it.
            token.closed.store(true, Ordering::Release);
            drop(token);
            drop(authority);
            recovered
        }
        .await;
        if recovered.is_ok() {
            budget.close();
        }
        recovered
    }

    async fn finish_packed_mounted_recovery<S>(
        self: &Arc<Self>,
        authority: &Arc<MountedRecoveryAuthority<B>>,
        metadata: Arc<WorkspaceMetaLayer<KvWorkspaceStore<B>>>,
        drain: crate::vfs::fs::PackedVfsDrainFence<S, WorkspaceMetaLayer<KvWorkspaceStore<B>>>,
    ) -> Result<PackedMountedRecoveryResult, WorkspaceError>
    where
        S: BlockStore + Send + Sync + 'static,
    {
        drain
            .validate_local()
            .await
            .map_err(|_| WorkspaceError::Fenced)?;
        let (actual_metadata, _, _) = drain.frozen_source_components();
        let guard = &authority.reference.guard;
        if !Arc::ptr_eq(&metadata, &actual_metadata)
            || !Arc::ptr_eq(metadata.store(), self)
            || !metadata
                .packed_shutdown_budget()
                .is_some_and(|budget| Arc::ptr_eq(&budget, &authority.budget))
        {
            return Err(WorkspaceError::Fenced);
        }
        let context = metadata.view_context().await;
        if context.workspace_id != guard.workspace_id
            || context.head_layer_id != guard.expected_head_layer_id
            || context.head_epoch != guard.expected_head_epoch
            || context.lease_id != guard.lease_id
            || context.holder_generation != guard.holder_generation
        {
            return Err(WorkspaceError::Fenced);
        }
        // This is the actual recovery VFS's physical local drain, followed by
        // its own lower transport/reader shutdown. It is never an original
        // mount kernel-cutoff proof or an ordinary lease release.
        metadata
            .shutdown_packed_runtime_for_clean_release()
            .await
            .map_err(|_| WorkspaceError::Fenced)?;
        drain
            .validate_local()
            .await
            .map_err(|_| WorkspaceError::Fenced)?;
        let original = authority.record.original.reference();
        let source_key = CleanSourceKind::RecoveredPmr.key(guard.workspace_id, guard.lease_id);
        let index_key = clean_source::recovered_current_key(guard.workspace_id);
        let extra = [
            recovery_key(&original),
            hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
            hot_lease_index_key(original.guard.lease_id),
            source_key.clone(),
            index_key.clone(),
        ];
        let view = self
            .scoped_packed_mount_view(guard.workspace_id, guard.lease_id, true, &extra)
            .await?;
        let (mut lease, mut open) = authority.authenticate(&view)?;
        if scoped_raw(&view.checks, &source_key)?.is_some() {
            return Err(WorkspaceError::Fenced);
        }
        let deadline = lease.expires_at_ns;
        lease.state = LeaseState::Released;
        lease.updated_at_ns = view.now;
        open.state = SnapshotOpenState::Ready;
        open.recovery_required = false;
        open.expires_at_ns = view.now;
        let head: LayerRecord =
            scoped_required(&view.checks, &hot_layer_key(guard.expected_head_layer_id))?;
        let mut original_lease: SnapshotLease = scoped_required(
            &view.checks,
            &hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
        )?;
        let original_index: WorkspaceId =
            scoped_required(&view.checks, &hot_lease_index_key(original.guard.lease_id))?;
        if original_index != original.guard.workspace_id
            || original_lease.workspace_id != original.guard.workspace_id
            || original_lease.lease_id != original.guard.lease_id
            || original_lease.holder_generation != original.guard.holder_generation
            || original_lease.base_revision != authority.binding.base_revision
            || !original_lease.writable
            || !matches!(
                original_lease.state,
                LeaseState::Active | LeaseState::Expired
            )
            || original_lease.expires_at_ns > view.now
        {
            return Err(WorkspaceError::Fenced);
        }
        original_lease.state = LeaseState::Released;
        original_lease.updated_at_ns = view.now;
        let mut completed = authority.record.clone();
        completed.completed = true;
        completed.original_released_lease = Some(original_lease.clone());
        completed.released_lease = Some(lease.clone());
        completed.closed_open = Some(open.clone());
        completed.head_sequence = Some(head.next_sequence);
        let mut workspace: WorkspaceRecord =
            scoped_required(&view.checks, &hot_workspace_key(guard.workspace_id))?;
        if workspace.active_lease != Some(guard.lease_id) {
            return Err(WorkspaceError::Fenced);
        }
        workspace.active_lease = None;
        workspace.updated_at_ns = view.now;
        let idle = view.writer.successor(None)?;
        let writes = vec![
            put(hot_workspace_key(guard.workspace_id), &workspace)?,
            put(
                hot_lease_key(original_lease.workspace_id, original_lease.lease_id),
                &original_lease,
            )?,
            put(hot_lease_key(lease.workspace_id, lease.lease_id), &lease)?,
            put(open_snapshot_key(guard.workspace_id), &open)?,
            put(
                open_snapshot_recovery_key(guard.workspace_id),
                &SnapshotRecoveryRecord {
                    workspace_id: guard.workspace_id,
                    incomplete: false,
                },
            )?,
            KvWrite::Put {
                key: packed_writer_key(guard.workspace_id),
                value: idle.encode()?,
            },
            KvWrite::Put {
                key: recovery_key(&original),
                value: encode_recovery(&completed)?,
            },
            KvWrite::Put {
                key: source_key.clone(),
                value: clean_source::completed_recovery_source(&completed)?,
            },
            KvWrite::Put {
                key: index_key,
                value: source_key,
            },
        ];
        let packet = self
            .prepare_topology_packet(view.checks, writes, Some(deadline))
            .await?;
        drain
            .validate_local()
            .await
            .map_err(|_| WorkspaceError::Fenced)?;
        self.commit_prepared_packed_mount_packet(&packet).await?;
        Ok(PackedMountedRecoveryResult {
            original,
            released: authority.reference.clone(),
        })
    }

    async fn confirm_completed_mounted_recovery(
        self: &Arc<Self>,
        record: MountedRecoveryRecord,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<PackedMountedRecoveryResult, WorkspaceError> {
        let original = record.original.reference();
        let released = record.current.reference();
        let extra = [
            recovery_key(&original),
            hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
            hot_lease_index_key(original.guard.lease_id),
        ];
        let view = self
            .scoped_packed_mount_view(
                released.guard.workspace_id,
                released.guard.lease_id,
                true,
                &extra,
            )
            .await?;
        let lease = view
            .requested_lease
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?;
        let open = view.open.as_ref().ok_or(WorkspaceError::Fenced)?;
        let original_lease: SnapshotLease = scoped_required(
            &view.checks,
            &hot_lease_key(original.guard.workspace_id, original.guard.lease_id),
        )?;
        let original_index: WorkspaceId =
            scoped_required(&view.checks, &hot_lease_index_key(original.guard.lease_id))?;
        let workspace: WorkspaceRecord = scoped_required(
            &view.checks,
            &hot_workspace_key(released.guard.workspace_id),
        )?;
        let persisted = decode_recovery(
            scoped_raw(&view.checks, &recovery_key(&original))?.ok_or(WorkspaceError::Fenced)?,
        )?;
        if workspace.active_lease.is_some()
            || original_index != original.guard.workspace_id
            || persisted != record
            || !record.completed
            || view.writer.owner.is_some()
            || view.writer.incarnation
                != record
                    .writer_incarnation
                    .checked_add(1)
                    .ok_or(WorkspaceError::Fenced)?
            || binding_digest(&view.binding)? != record.binding_digest
            || lease.state != LeaseState::Released
            || lease.lease_id != released.guard.lease_id
            || lease.holder_generation != released.guard.holder_generation
            || open.owner_id != record.open_owner
            || open.generation != record.open_generation
            || open.state != SnapshotOpenState::Ready
            || open.recovery_required
            || open.expires_at_ns != lease.updated_at_ns
            || open.expires_at_ns > view.now
            || record.released_lease.as_ref() != Some(lease)
            || record.closed_open.as_ref() != Some(open)
            || record.original_released_lease.as_ref() != Some(&original_lease)
        {
            return Err(WorkspaceError::Fenced);
        }
        if !self
            .packed_reader_pin_budget
            .get()
            .is_some_and(|canonical| Arc::ptr_eq(canonical, &budget))
        {
            return Err(WorkspaceError::Fenced);
        }
        let deadline = view
            .now
            .checked_add(30 * 1_000_000_000)
            .ok_or(WorkspaceError::Fenced)?;
        if !self
            .backend
            .authenticate_checks_before_bounded(
                &view.checks,
                deadline,
                source_limits(view.checks.len()),
            )
            .await?
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(PackedMountedRecoveryResult { original, released })
    }
}
