//! Narrow packed admin admission. Publication authority stays crate-private.
//!
//! Intended placement: stores/kv_store/packed_admin.rs, with public reexport from
//! workspace. This candidate implements the persistent source admission
//! and proof-consuming clean release writer; it cannot publish a snapshot or
//! enable the mount gate.

pub use super::packed_journal::{
    PackedGcAdmin, PackedGcCursor, PackedGcPolicy, PackedGcTickReport, PackedGcTickRequest,
};
use super::packed_writer_authority::{PackedWriterAuthority, PackedWriterOwner, packed_writer_key};
use super::*;

#[path = "packed_topology_history.rs"]
mod topology_history;
use crate::workspace::packed::snapshot::{
    SnapshotBudgetPool, SnapshotMountBudget, SnapshotOwnedPermit,
};
use crate::workspace::stores::kv_backend::KvReadLimits;
pub(in crate::workspace::stores::kv_store) use topology_history::{
    append_workspace_history_keys, authenticate_workspace_history_values,
};

const CLEAN_RECEIPT_PREFIX: &[u8] = b"packed/clean-release/";
const CLEAN_RECEIPT_MAGIC: &[u8; 5] = b"PCR3\x01";
const CLEAN_RECEIPT_MAX_BYTES: usize = 4096;
const SOURCE_MAX_BYTES: usize = 48 << 10;
const SOURCE_ADMISSION_BYTES: u64 = 512 << 10;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedReleasedMountReference {
    pub guard: HeadGuard,
    pub mount_uid: uuid::Uuid,
    pub pod_uid: uuid::Uuid,
}

/// Public reports are distinct from authority. Callers cannot construct a ticket.
pub enum PackedCleanAdmission<B: WorkspaceKvBackend> {
    RequiresRecovery,
    Ready(PackedCleanSourceTicket<B>),
}

/// Retains the source's actual bounded backend read and admission. Begin native
/// quiesce must consume the complete checks exactly once; subsequent phases retain
/// the immutable receipt check alongside their successor native authority.
pub struct PackedCleanSourceTicket<B: WorkspaceKvBackend> {
    store: Arc<KvWorkspaceStore<B>>,
    receipt: Box<CleanSourceRecord>,
    checks: Vec<KvCheck>,
    budget: Arc<SnapshotMountBudget>,
    _owner: SnapshotOwnedPermit,
}

impl<B: WorkspaceKvBackend> PackedCleanSourceTicket<B> {
    pub fn released_mount(&self) -> PackedReleasedMountReference {
        PackedReleasedMountReference {
            guard: self.receipt.guard.to_head_guard(),
            mount_uid: self.receipt.mount_uid,
            pod_uid: self.receipt.pod_uid,
        }
    }

    pub(crate) fn belongs_to_store(&self, store: &Arc<KvWorkspaceStore<B>>) -> bool {
        Arc::ptr_eq(&self.store, store)
    }

    #[cfg(test)]
    pub(crate) fn retained_checks_for_test(&self) -> &[KvCheck] {
        &self.checks
    }
}

/// New independent record, never appended to existing BaseRevision/SnapshotLease.
/// Its sole writer must consume same-original-VFS drain + kernel-cutoff proof and
/// atomically persist this record with the exact Released lease transition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CleanReleaseReceipt {
    guard: CleanReleaseGuard,
    mount_uid: uuid::Uuid,
    pod_uid: uuid::Uuid,
    head_sequence: u64,
    base_revision: BaseRevision,
    binding_version: u64,
    manifest_digest: [u8; 32],
    // Existing v3 open sidecar authority, not a second owner system. A later
    // grant changes these bytes even before its first filesystem mutation.
    open_owner: Option<SnapshotOpenRecord>,
    // The private full source-claim CAS writes this fact. Before NQB issuance,
    // an exact private expiry takeover updates it to the actual successor;
    // after NQB it is immutable. Ordinary grant/open cannot create provenance.
    first_admin_claim: Option<FirstAdminCleanClaim>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FirstAdminCleanClaim {
    lease: SnapshotLease,
    // The canonical owner contains the exact logical request fingerprint and
    // original lease/mount/pod/snapshot/native journal/planned head identities.
    open_owner: SnapshotOpenRecord,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CleanReleaseGuard {
    workspace_id: WorkspaceId,
    expected_head_layer_id: LayerId,
    expected_head_epoch: u64,
    lease_id: LeaseId,
    holder_generation: u64,
}

impl From<&HeadGuard> for CleanReleaseGuard {
    fn from(guard: &HeadGuard) -> Self {
        Self {
            workspace_id: guard.workspace_id,
            expected_head_layer_id: guard.expected_head_layer_id,
            expected_head_epoch: guard.expected_head_epoch,
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
        }
    }
}

impl CleanReleaseGuard {
    fn to_head_guard(&self) -> HeadGuard {
        HeadGuard {
            workspace_id: self.workspace_id,
            expected_head_layer_id: self.expected_head_layer_id,
            expected_head_epoch: self.expected_head_epoch,
            lease_id: self.lease_id,
            holder_generation: self.holder_generation,
        }
    }
}

fn clean_receipt_key(guard: &HeadGuard) -> Vec<u8> {
    let mut key = CLEAN_RECEIPT_PREFIX.to_vec();
    key.extend_from_slice(guard.workspace_id.to_string().as_bytes());
    key.push(b'/');
    key.extend_from_slice(guard.lease_id.to_string().as_bytes());
    key
}

fn encode_clean_receipt(value: &CleanReleaseReceipt) -> Result<Vec<u8>, WorkspaceError> {
    if let Some(claim) = &value.first_admin_claim {
        validate_open_record(&claim.open_owner, value.guard.workspace_id)?;
        if claim.lease.workspace_id != value.guard.workspace_id
            || !claim.lease.writable
            || claim.lease.lease_id.as_uuid().is_nil()
            || claim.lease.lease_id == value.guard.lease_id
            || claim.lease.holder_generation <= value.guard.holder_generation
            || claim.lease.base_revision != value.base_revision
            || claim.lease.state != LeaseState::Active
            || claim.lease.created_at_ns <= 0
            || claim.lease.updated_at_ns != claim.lease.created_at_ns
            || claim.lease.expires_at_ns <= claim.lease.created_at_ns
            || claim.open_owner.expires_at_ns != claim.lease.expires_at_ns
            || claim.open_owner.state != SnapshotOpenState::Ready
            || claim.open_owner.recovery_required
            || value
                .open_owner
                .as_ref()
                .is_some_and(|original| claim.open_owner.generation <= original.generation)
            || headless::PackedCleanOperationOrigin::parse(&claim.open_owner.owner_id)?.is_none_or(
                |origin| {
                    origin.source_kind() != CleanSourceKind::OriginalPcr
                        || !origin.matches_source_reference(&PackedReleasedMountReference {
                            guard: value.guard.to_head_guard(),
                            mount_uid: value.mount_uid,
                            pod_uid: value.pod_uid,
                        })
                },
            )
        {
            return Err(WorkspaceError::Fenced);
        }
    }
    let mut bytes = CLEAN_RECEIPT_MAGIC.to_vec();
    bytes.extend(
        serde_json::to_vec(value)
            .map_err(|_| WorkspaceError::CorruptMetadata("packed clean receipt encode".into()))?,
    );
    if bytes.len() > CLEAN_RECEIPT_MAX_BYTES {
        return Err(WorkspaceError::CorruptMetadata(
            "packed clean receipt size".into(),
        ));
    }
    Ok(bytes)
}

fn decode_clean_receipt(bytes: &[u8]) -> Result<CleanReleaseReceipt, WorkspaceError> {
    if bytes.len() > CLEAN_RECEIPT_MAX_BYTES || !bytes.starts_with(CLEAN_RECEIPT_MAGIC) {
        return Err(WorkspaceError::CorruptMetadata(
            "packed clean receipt frame".into(),
        ));
    }
    let value: CleanReleaseReceipt = serde_json::from_slice(&bytes[CLEAN_RECEIPT_MAGIC.len()..])
        .map_err(|_| WorkspaceError::CorruptMetadata("packed clean receipt body".into()))?;
    if encode_clean_receipt(&value)? != bytes
        || value.binding_version == 0
        || value.mount_uid.is_nil()
        || value.pod_uid.is_nil()
    {
        return Err(WorkspaceError::CorruptMetadata(
            "packed clean receipt canonical form".into(),
        ));
    }
    if let Some(owner) = &value.open_owner {
        validate_open_record(owner, value.guard.workspace_id)?;
        if owner.recovery_required {
            return Err(WorkspaceError::Fenced);
        }
    }
    Ok(value)
}

fn source_limits(count: usize) -> KvReadLimits {
    KvReadLimits {
        max_records: count,
        max_key_bytes: 1024,
        max_value_bytes: SOURCE_MAX_BYTES,
        max_total_bytes: SOURCE_MAX_BYTES,
        max_response_bytes: 64 << 10,
        max_data_requests: count.saturating_add(2).min(32),
    }
}

// Historical lease entities remain evidence that a clean source was consumed.
// Every candidate comes from the explicitly named workspace's fenced census.
fn clean_source_routes_other_lease(
    id: LeaseId,
    row: &SnapshotLease,
    source: LeaseId,
) -> Result<bool, WorkspaceError> {
    // Validate even a row that claims to be the source before excluding it.
    if id != row.lease_id {
        return Err(WorkspaceError::Fenced);
    }
    Ok(row.lease_id != source)
}

fn clean_source_other_lease_invalidates(
    catalog: &SnapshotLease,
    hot: Option<&SnapshotLease>,
    source: &SnapshotLease,
) -> Result<bool, WorkspaceError> {
    let hot = hot.ok_or(WorkspaceError::Fenced)?;
    if hot.lease_id != catalog.lease_id
        || hot.workspace_id != catalog.workspace_id
        || hot.holder_generation != catalog.holder_generation
        || hot.base_revision != catalog.base_revision
        || hot.writable != catalog.writable
        || hot.created_at_ns != catalog.created_at_ns
        || hot.updated_at_ns < catalog.updated_at_ns
        || hot.expires_at_ns < catalog.expires_at_ns
    {
        return Err(WorkspaceError::Fenced);
    }
    if hot.state == LeaseState::Active {
        return Err(WorkspaceError::Busy);
    }
    Ok(hot.writable
        && hot.lease_id != source.lease_id
        && (hot.created_at_ns >= source.created_at_ns
            || hot.holder_generation >= source.holder_generation))
}

impl<B: WorkspaceKvBackend + 'static> KvWorkspaceStore<B> {
    /// The owned task retains the original VFS drain/cutoff and native hold
    /// admission through the actual CAS even when the receiving CLI is cancelled.
    pub(crate) async fn release_clean_packed_mount(
        self: &Arc<Self>,
        proof: crate::workspace::packed_shutdown::VerifiedCleanPackedShutdown,
    ) -> Result<(), WorkspaceError> {
        self.release_clean_packed_mount_driver(proof, None).await
    }

    pub(crate) async fn release_clean_packed_mounted_session(
        self: &Arc<Self>,
        proof: crate::workspace::packed_shutdown::VerifiedCleanPackedShutdown,
        mounted: Arc<PackedMountedLease<B>>,
    ) -> Result<(), WorkspaceError> {
        self.release_clean_packed_mount_driver(proof, Some(mounted))
            .await
    }

    async fn release_clean_packed_mount_driver(
        self: &Arc<Self>,
        proof: crate::workspace::packed_shutdown::VerifiedCleanPackedShutdown,
        mounted: Option<Arc<PackedMountedLease<B>>>,
    ) -> Result<(), WorkspaceError> {
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let result = store.release_clean_packed_mount_owned(proof, mounted).await;
            let _ = sender.send(result);
        });
        let received = receiver.await;
        task.await.map_err(|_| {
            WorkspaceError::CorruptMetadata("owned packed clean release stopped".into())
        })?;
        received.map_err(|_| {
            WorkspaceError::CorruptMetadata("owned packed clean release result unavailable".into())
        })?
    }

    async fn release_clean_packed_mount_owned(
        self: &Arc<Self>,
        proof: crate::workspace::packed_shutdown::VerifiedCleanPackedShutdown,
        mounted: Option<Arc<PackedMountedLease<B>>>,
    ) -> Result<(), WorkspaceError> {
        if let Some(authority) = &mounted {
            // The owned release retains this drain after caller cancellation.
            // Never let proof Drop close admission beneath a live renewal CAS.
            authority.close_renewals_and_drain().await?;
        }
        if !proof.belongs_to_store(self.as_ref()) {
            return Err(WorkspaceError::Fenced);
        }
        proof.validate().await?;
        // The actual original lower remains open for cleanup admission until
        // this owned task reaches terminal. Proof Drop closes that same budget.
        let budget = proof.budget();
        if !self
            .packed_reader_pin_budget
            .get()
            .is_some_and(|canonical| Arc::ptr_eq(canonical, &budget))
        {
            return Err(WorkspaceError::Fenced);
        }
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| match error {
                crate::workspace::packed::PackedWireError::LimitExceeded(message) => {
                    WorkspaceError::InvalidReadPlan(message)
                }
                other => WorkspaceError::CorruptMetadata(other.to_string()),
            })?;
        let reference = proof.reference();
        let guard = &reference.guard;
        let routing_keys = [packed_current_key(guard.workspace_id)];
        let (routed, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&routing_keys, source_limits(1))
            .await?;
        if routed.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let routed_binding =
            PackedLowerBindingRecord::decode(routed[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        let keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(guard.workspace_id),
            hot_layer_key(guard.expected_head_layer_id),
            hot_layer_key(routed_binding.base_revision.layer_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            packed_current_key(guard.workspace_id),
            packed_claim_key(guard.workspace_id),
            packed_history_key(guard.workspace_id, routed_binding.binding.binding_version),
            clean_receipt_key(guard),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            open_snapshot_key(guard.workspace_id),
            open_snapshot_recovery_key(guard.workspace_id),
            packed_writer_key(guard.workspace_id),
        ];
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 || values[5] != routed[0] {
            return Err(WorkspaceError::Fenced);
        }
        let total = keys
            .iter()
            .zip(&values)
            .try_fold(0usize, |sum, (key, value)| {
                sum.checked_add(key.len())
                    .and_then(|n| n.checked_add(value.as_ref().map_or(0, Vec::len)))
            });
        if total.is_none_or(|n| n > SOURCE_MAX_BYTES) {
            return Err(WorkspaceError::Fenced);
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        validate_current_control_raw(Some(required(0)?))?;
        let mut workspace: WorkspaceRecord = decode_open_value(required(1)?, SOURCE_MAX_BYTES)?;
        let head: LayerRecord = decode_open_value(required(2)?, SOURCE_MAX_BYTES)?;
        let base: LayerRecord = decode_open_value(required(3)?, SOURCE_MAX_BYTES)?;
        let mut lease: SnapshotLease = decode_open_value(required(4)?, SOURCE_MAX_BYTES)?;
        let original_open: Option<SnapshotOpenRecord> = values[11]
            .as_deref()
            .map(|raw| decode_open_value(raw, OPEN_RECORD_MAX_BYTES))
            .transpose()?;
        let writer = PackedWriterAuthority::decode(required(13)?, guard.workspace_id)?;
        let (open_owner, retired_writer) = if let Some(authority) = &mounted {
            let (closed, retired) = authority.original_release_successor(
                self,
                reference,
                PackedOriginalReleaseRows {
                    writer: &writer,
                    lease: &lease,
                    open: original_open.as_ref().ok_or(WorkspaceError::Fenced)?,
                },
                now,
                values[8].is_some(),
            )?;
            (Some(closed), retired)
        } else {
            // Genuine initial-source mounts retain the existing no-live-open
            // contract. A mounted joint owner cannot use this plain entry.
            if let Some(owner) = &original_open {
                validate_open_record(owner, guard.workspace_id)?;
                if owner.recovery_required {
                    return Err(WorkspaceError::Fenced);
                }
                if owner.expires_at_ns > now {
                    return Err(WorkspaceError::Busy);
                }
            }
            let retired = match &writer.owner {
                Some(PackedWriterOwner::InitialSource {
                    lease_id,
                    holder_generation,
                }) if *lease_id == guard.lease_id
                    && *holder_generation == guard.holder_generation =>
                {
                    writer.successor(None)?
                }
                None if values[8].is_some() && lease.state == LeaseState::Released => {
                    writer.clone()
                }
                _ => return Err(WorkspaceError::Fenced),
            };
            (original_open.clone(), retired)
        };
        if let Some(raw) = values[12].as_deref() {
            let recovery: SnapshotRecoveryRecord = decode_open_value(raw, OPEN_RECOVERY_MAX_BYTES)?;
            if recovery.workspace_id != guard.workspace_id || recovery.incomplete {
                return Err(WorkspaceError::Fenced);
            }
        }
        let binding = decode_packed_pair(guard.workspace_id, &values[5], &values[6], &values[7])?
            .ok_or(WorkspaceError::Fenced)?;
        binding.validate_for_guard(guard, &base)?;
        if workspace.workspace_id != guard.workspace_id
            || workspace.head_layer_id != guard.expected_head_layer_id
            || workspace.head_epoch != guard.expected_head_epoch
            || workspace.state != WorkspaceState::Active
            || head.layer_id != guard.expected_head_layer_id
            || head.state != LayerState::Writable
            || head.depth != 2
            || head.owner_workspace_id != Some(guard.workspace_id)
            || head.parent_layer_id != Some(base.layer_id)
            || base.depth != 1
            || base.parent_layer_id.is_some()
            || lease.workspace_id != guard.workspace_id
            || lease.lease_id != guard.lease_id
            || lease.holder_generation != guard.holder_generation
            || !lease.writable
            || lease.base_revision != binding.base_revision
            || (lease.state == LeaseState::Active && workspace.active_lease != Some(lease.lease_id))
            || (lease.state == LeaseState::Released && workspace.active_lease.is_some())
        {
            return Err(WorkspaceError::Fenced);
        }
        let receipt = CleanReleaseReceipt {
            guard: guard.into(),
            mount_uid: reference.mount_uid,
            pod_uid: reference.pod_uid,
            head_sequence: head.next_sequence,
            base_revision: binding.base_revision.clone(),
            binding_version: binding.binding.binding_version,
            manifest_digest: binding.binding.manifest.digest,
            open_owner,
            first_admin_claim: None,
        };
        let receipt_bytes = encode_clean_receipt(&receipt)?;
        if let Some(existing) = values[8].as_deref() {
            // A previous receipt is success only after exact atomic
            // authentication of this complete same-version source snapshot.
            if existing == receipt_bytes && lease.state == LeaseState::Released {
                let deadline = now
                    .checked_add(5 * 1_000_000_000)
                    .ok_or(WorkspaceError::Fenced)?;
                let checks = keys
                    .into_iter()
                    .zip(values)
                    .map(|(key, expected)| KvCheck { key, expected })
                    .collect::<Vec<_>>();
                proof.validate().await?;
                if !self
                    .backend
                    .authenticate_checks_before_bounded(
                        &checks,
                        deadline,
                        source_limits(checks.len()),
                    )
                    .await?
                {
                    return Err(WorkspaceError::Fenced);
                }
                proof.validate().await?;
                return Ok(());
            }
            return Err(WorkspaceError::Fenced);
        }
        if lease.state != LeaseState::Active || now >= lease.expires_at_ns {
            return Err(WorkspaceError::Fenced);
        }
        let lease_deadline = if mounted.is_some() {
            lease.expires_at_ns.min(
                original_open
                    .as_ref()
                    .ok_or(WorkspaceError::Fenced)?
                    .expires_at_ns,
            )
        } else {
            lease.expires_at_ns
        };
        lease.state = LeaseState::Released;
        lease.updated_at_ns = now;
        let released_bytes = encode(&lease)?;
        workspace.active_lease = None;
        workspace.updated_at_ns = now;
        let mut checks = keys
            .iter()
            .cloned()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let mut writes = vec![
            put(hot_workspace_key(workspace.workspace_id), &workspace)?,
            KvWrite::Put {
                key: keys[4].clone(),
                value: released_bytes.clone(),
            },
            KvWrite::Put {
                key: keys[8].clone(),
                value: receipt_bytes.clone(),
            },
        ];
        writes.push(KvWrite::Put {
            key: packed_writer_key(guard.workspace_id),
            value: retired_writer.encode()?,
        });
        if mounted.is_some() {
            writes.push(put(
                open_snapshot_key(guard.workspace_id),
                receipt.open_owner.as_ref().ok_or(WorkspaceError::Fenced)?,
            )?);
        }
        let _native_holds = self
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await?;
        let packet = self
            .prepare_topology_envelope(checks, writes, Some(lease_deadline))
            .await?;
        let checks = packet.checks.clone();
        let writes = packet.writes.clone();
        // Retain the complete attempted successor, including all unchanged
        // authorities and native-hold/root writes, for unknown-result inspection.
        let mut successor = checks.clone();
        for write in &writes {
            let (key, expected) = match write {
                KvWrite::Put { key, value } => (key, Some(value.clone())),
                KvWrite::Delete { key } => (key, None),
            };
            let checked = successor
                .iter_mut()
                .find(|check| &check.key == key)
                .ok_or(WorkspaceError::Fenced)?;
            checked.expected = expected;
        }
        let packet_bytes = |packet: &[KvCheck]| {
            packet.iter().try_fold(0usize, |sum, check| {
                sum.checked_add(check.key.len())
                    .and_then(|n| n.checked_add(check.expected.as_ref().map_or(0, Vec::len)))
            })
        };
        if successor.len() > 32
            || packet_bytes(&checks).is_none_or(|n| n > SOURCE_MAX_BYTES)
            || packet_bytes(&successor).is_none_or(|n| n > SOURCE_MAX_BYTES)
        {
            return Err(WorkspaceError::Fenced);
        }
        proof.validate().await?;
        // Exactly one mutation attempt. A false/unknown result is not replayed
        // onto a new head, holder, timestamp or source inventory.
        match self.commit_prepared_topology_packet(&packet).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(WorkspaceError::Fenced),
            Err(original) => {
                let confirmation_keys = successor
                    .iter()
                    .map(|check| check.key.clone())
                    .collect::<Vec<_>>();
                let confirmation = self
                    .backend
                    .get_many_consistent_with_time_bounded(
                        &confirmation_keys,
                        source_limits(confirmation_keys.len()),
                    )
                    .await;
                match confirmation {
                    Ok((rows, _))
                        if rows.len() == successor.len()
                            && rows
                                .iter()
                                .zip(&successor)
                                .all(|(row, check)| row == &check.expected) =>
                    {
                        if proof.validate().await.is_err() {
                            return Err(original);
                        }
                        match self
                            .backend
                            .compare_and_swap_before(&successor, &[], lease_deadline)
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

    pub async fn admit_clean_packed_source(
        self: &Arc<Self>,
        reference: PackedReleasedMountReference,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<PackedCleanAdmission<B>, WorkspaceError> {
        self.require_admin_access()?;
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| match error {
                crate::workspace::packed::PackedWireError::LimitExceeded(message) => {
                    WorkspaceError::InvalidReadPlan(message)
                }
                other => WorkspaceError::CorruptMetadata(other.to_string()),
            })?;
        let receipt_key = clean_receipt_key(&reference.guard);
        // This first read routes the second complete transaction; it grants no
        // authority and its raw bytes are checked again in that transaction.
        let routing_keys = [receipt_key.clone()];
        let (routing, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                &routing_keys,
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
        if routing.len() != 1 {
            return Err(WorkspaceError::CorruptMetadata(
                "packed clean receipt routing count".into(),
            ));
        }
        let Some(routed_bytes) = routing[0].as_deref() else {
            return Ok(PackedCleanAdmission::RequiresRecovery);
        };
        let receipt = decode_clean_receipt(routed_bytes)?;
        if receipt.guard.to_head_guard() != reference.guard
            || receipt.mount_uid != reference.mount_uid
            || receipt.pod_uid != reference.pod_uid
        {
            return Err(WorkspaceError::Fenced);
        }
        if receipt.first_admin_claim.is_some() {
            return Ok(PackedCleanAdmission::RequiresRecovery);
        }
        let guard = &reference.guard;
        // Only this explicit workspace's retained lease history is routed.
        // Its membership epoch and all exact rows join the final source packet.
        let history = self
            .read_workspace_lease_history_checks(guard.workspace_id, 18, source_limits(32))
            .await?;
        let mut keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(guard.workspace_id),
            hot_layer_key(guard.expected_head_layer_id),
            hot_layer_key(receipt.base_revision.layer_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            packed_current_key(guard.workspace_id),
            packed_claim_key(guard.workspace_id),
            packed_history_key(guard.workspace_id, receipt.binding_version),
            receipt_key,
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            open_snapshot_key(guard.workspace_id),
            open_snapshot_recovery_key(guard.workspace_id),
        ];
        let mut other_lease_rows = Vec::new();
        for old in &history {
            let index = if let Some(index) = keys.iter().position(|key| key == &old.key) {
                index
            } else {
                if keys.len() >= 32 {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "packed clean source historical lease packet exceeds 32 keys".into(),
                    ));
                }
                let index = keys.len();
                keys.push(old.key.clone());
                index
            };
            if old.key.starts_with(HOT_LEASE_PREFIX) {
                let other: SnapshotLease = decode_open_value(
                    old.expected.as_deref().ok_or(WorkspaceError::Fenced)?,
                    SOURCE_MAX_BYTES,
                )?;
                if clean_source_routes_other_lease(other.lease_id, &other, guard.lease_id)? {
                    other_lease_rows.push((other, index));
                }
            }
        }
        let limits = source_limits(keys.len());
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, limits)
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "packed clean source response".into(),
            ));
        }
        if history.iter().any(|old| {
            keys.iter()
                .position(|key| key == &old.key)
                .is_none_or(|index| values[index] != old.expected)
        }) {
            return Err(WorkspaceError::Busy);
        }
        let total = keys
            .iter()
            .zip(&values)
            .try_fold(0usize, |sum, (key, value)| {
                sum.checked_add(key.len())
                    .and_then(|n| n.checked_add(value.as_ref().map_or(0, Vec::len)))
            });
        if total.is_none_or(|n| n > SOURCE_MAX_BYTES) || values[8].as_deref() != Some(routed_bytes)
        {
            return Err(WorkspaceError::Fenced);
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        validate_current_control_raw(Some(required(0)?))?;
        let workspace: WorkspaceRecord = decode_open_value(required(1)?, SOURCE_MAX_BYTES)?;
        let head: LayerRecord = decode_open_value(required(2)?, SOURCE_MAX_BYTES)?;
        let base: LayerRecord = decode_open_value(required(3)?, SOURCE_MAX_BYTES)?;
        let lease: SnapshotLease = decode_open_value(required(4)?, SOURCE_MAX_BYTES)?;
        if lease.state != LeaseState::Released {
            return Ok(PackedCleanAdmission::RequiresRecovery);
        }
        let open_owner: Option<SnapshotOpenRecord> = values[11]
            .as_deref()
            .map(|raw| decode_open_value(raw, OPEN_RECORD_MAX_BYTES))
            .transpose()?;
        if let Some(owner) = &open_owner {
            validate_open_record(owner, guard.workspace_id)?;
            if owner.recovery_required {
                return Ok(PackedCleanAdmission::RequiresRecovery);
            }
            if owner.expires_at_ns > now {
                return Err(WorkspaceError::Busy);
            }
            if owner.expires_at_ns > lease.updated_at_ns {
                return Err(WorkspaceError::Fenced);
            }
        }
        if open_owner != receipt.open_owner {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(raw) = values[12].as_deref() {
            let recovery: SnapshotRecoveryRecord = decode_open_value(raw, OPEN_RECOVERY_MAX_BYTES)?;
            if recovery.workspace_id != guard.workspace_id {
                return Err(WorkspaceError::Fenced);
            }
            if recovery.incomplete {
                return Ok(PackedCleanAdmission::RequiresRecovery);
            }
        }
        for (catalog, index) in other_lease_rows {
            let hot = values[index]
                .as_deref()
                .map(|raw| decode_open_value::<SnapshotLease>(raw, SOURCE_MAX_BYTES))
                .transpose()?;
            if clean_source_other_lease_invalidates(&catalog, hot.as_ref(), &lease)? {
                // Actual later expired/released attempts still consume this source.
                return Ok(PackedCleanAdmission::RequiresRecovery);
            }
        }
        if workspace.workspace_id != guard.workspace_id
            || workspace.head_layer_id != guard.expected_head_layer_id
            || workspace.head_epoch != guard.expected_head_epoch
            || workspace.state != WorkspaceState::Active
            || workspace.active_lease.is_some()
            || head.layer_id != guard.expected_head_layer_id
            || head.state != LayerState::Writable
            || head.depth != 2
            || head.owner_workspace_id != Some(guard.workspace_id)
            || head.parent_layer_id != Some(base.layer_id)
            || head.next_sequence != receipt.head_sequence
            || base.state != LayerState::Sealed
            || base.depth != 1
            || base.parent_layer_id.is_some()
            || base.sealed_version != Some(receipt.base_revision.sealed_version)
            || base.root_hash != Some(receipt.base_revision.root_hash)
            || lease.workspace_id != guard.workspace_id
            || lease.lease_id != guard.lease_id
            || lease.holder_generation != guard.holder_generation
            || !lease.writable
            || lease.base_revision != receipt.base_revision
        {
            return Err(WorkspaceError::Fenced);
        }
        let binding = decode_packed_pair(guard.workspace_id, &values[5], &values[6], &values[7])?
            .ok_or(WorkspaceError::Fenced)?;
        binding.validate_for_guard(guard, &base)?;
        if binding.binding.binding_version != receipt.binding_version
            || binding.binding.manifest.digest != receipt.manifest_digest
        {
            return Err(WorkspaceError::Fenced);
        }
        let checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect();
        Ok(PackedCleanAdmission::Ready(PackedCleanSourceTicket {
            store: self.clone(),
            receipt: Box::new(CleanSourceRecord::original(receipt)),
            checks,
            budget,
            _owner: owner,
        }))
    }
}

#[path = "packed_headless.rs"]
mod headless;
pub(crate) use headless::PackedCleanPublicationAuthority;
pub use headless::PackedRecoveredMountReport;
pub(in crate::workspace::stores::kv_store) use headless::authenticate_mounted_recovery_writer;
pub(in crate::workspace::stores::kv_store::packed_admin) use headless::{
    CleanSourceKind, CleanSourceRecord, PackedOriginalReleaseRows,
};
#[cfg(target_os = "linux")]
pub use headless::{
    PackedHeadlessSnapshotDescription, PackedHeadlessSnapshotFailure, PackedHeadlessSnapshotRequest,
};
pub(crate) use headless::{
    PackedMountGrantRequest, PackedMountedLease, PackedMountedRecoveryRequest,
    capture_mounted_recovery_owner,
};
pub use headless::{PackedPublishedViewReport, PackedWorkspaceViewReport};

/// Publication result meanings must remain distinct. Only the carrier revision
/// may be stored in SnapshotRecord or accepted by the atomic packed fork path.
pub struct PackedSnapshotResult {
    pub snapshot_id: SnapshotId,
    pub packed_carrier_revision: BaseRevision,
    pub native_sealed_source_revision: BaseRevision,
    pub binding: PackedLowerBindingRecord,
}

#[cfg(test)]
#[path = "packed_admin_tests.rs"]
mod tests;

pub(crate) use headless::PackedInitialBootstrapAuthority;
