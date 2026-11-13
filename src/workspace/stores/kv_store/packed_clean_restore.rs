//! Recover an original clean packed operation through actual native APIs.
//! Existing open/lease/native recovery remains the sole current owner system.
//! Intended as a child module of packed_admin::headless, not a public proof mint.

use super::*;
use crate::workspace::stores::kv_store::packed_native_freeze::PackedNativeQuiesceFence;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};

const OWNER_PREFIX: &str = "packed-snapshot/";
const RECOVERED_OWNER_PREFIX: &str = "packed-recovered/";
const OWNER_BODY_BYTES: usize = 6 * 22 + 6 + 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CleanAuthorityMode {
    Original,
    Recovery,
    Committed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PackedCleanOperationOrigin {
    source_kind: CleanSourceKind,
    original_lease: LeaseId,
    mount_uid: uuid::Uuid,
    pod_uid: uuid::Uuid,
    snapshot_id: SnapshotId,
    native_journal_id: JournalId,
    planned_head: LayerId,
    fingerprint: [u8; 32],
}

struct CommittedCleanPublication {
    record: crate::workspace::stores::kv_store::packed_journal::PackedJournalRecord,
    origin: PackedCleanOperationOrigin,
}

fn compact_uuid(value: uuid::Uuid) -> String {
    URL_SAFE_NO_PAD.encode(value.as_bytes())
}
fn decode_uuid(raw: &str) -> Result<uuid::Uuid, WorkspaceError> {
    let mut bytes = [0; 16];
    if raw.len() != 22
        || URL_SAFE_NO_PAD
            .decode_slice(raw, &mut bytes)
            .map_err(|_| WorkspaceError::Fenced)?
            != bytes.len()
    {
        return Err(WorkspaceError::Fenced);
    }
    let value = uuid::Uuid::from_bytes(bytes);
    if value.is_nil() || compact_uuid(value) != raw {
        return Err(WorkspaceError::Fenced);
    }
    Ok(value)
}

fn logical_request_fingerprint(
    request: &PackedHeadlessSnapshotRequest,
    layout: ChunkLayout,
) -> Result<[u8; 32], WorkspaceError> {
    fn optional(hash: &mut Sha256, value: &Option<String>) -> Result<(), WorkspaceError> {
        match value {
            None => hash.update([0]),
            Some(value) => {
                if value.len() > 1024 {
                    return Err(WorkspaceError::Fenced);
                }
                hash.update([1]);
                hash.update((value.len() as u64).to_le_bytes());
                hash.update(value.as_bytes());
            }
        }
        Ok(())
    }
    let mut hash = Sha256::new();
    hash.update(b"packed-clean-logical-request\0");
    for value in [
        request.snapshot_id.as_uuid(),
        request.native_journal_id.as_uuid(),
        request.new_head_layer_id.as_uuid(),
    ] {
        hash.update(value.as_bytes());
    }
    optional(&mut hash, &request.snapshot_name)?;
    optional(&mut hash, &request.owner_id)?;
    hash.update(request.producer.snapshot_id);
    hash.update(request.producer.root_dir_key);
    for value in [
        layout.chunk_size,
        u64::from(layout.block_size),
        request.producer.root_inode,
        request.producer.size_classes.min_frame_raw_bytes,
        request.producer.size_classes.max_random_frame_raw_bytes,
        request.producer.size_classes.max_sequential_frame_raw_bytes,
    ] {
        hash.update(value.to_le_bytes());
    }
    hash.update([
        request.producer.profile as u8,
        request.producer.build_policy.frames as u8,
        u8::from(request.producer.build_policy.inline_data),
        request.producer.metadata_codec as u8,
        request.producer.data_codec as u8,
    ]);
    // Scratch paths, cancellation, validation quotas and new lease incarnation
    // are operational inputs. They do not change this logical snapshot output.
    Ok(hash.finalize().into())
}

impl PackedCleanOperationOrigin {
    pub(in crate::workspace::stores::kv_store::packed_admin) fn source_kind(
        &self,
    ) -> CleanSourceKind {
        self.source_kind
    }
    pub(in crate::workspace::stores::kv_store::packed_admin) fn source_key(
        &self,
        workspace: WorkspaceId,
    ) -> Vec<u8> {
        self.source_kind.key(workspace, self.original_lease)
    }
    pub(in crate::workspace::stores::kv_store::packed_admin) fn matches_source_reference(
        &self,
        reference: &PackedReleasedMountReference,
    ) -> bool {
        self.original_lease == reference.guard.lease_id
            && self.mount_uid == reference.mount_uid
            && self.pod_uid == reference.pod_uid
    }
    pub(crate) fn owner_id(&self) -> String {
        let prefix = match self.source_kind {
            CleanSourceKind::OriginalPcr => OWNER_PREFIX,
            CleanSourceKind::RecoveredPmr => RECOVERED_OWNER_PREFIX,
        };
        format!(
            "{prefix}{}/{}/{}/{}/{}/{}/{}",
            compact_uuid(*self.original_lease.as_uuid()),
            compact_uuid(self.mount_uid),
            compact_uuid(self.pod_uid),
            compact_uuid(*self.snapshot_id.as_uuid()),
            compact_uuid(*self.native_journal_id.as_uuid()),
            compact_uuid(*self.planned_head.as_uuid()),
            hex::encode(self.fingerprint)
        )
    }

    pub(crate) fn parse(owner: &str) -> Result<Option<Self>, WorkspaceError> {
        let (prefix, source_kind) = if owner.starts_with(OWNER_PREFIX) {
            (OWNER_PREFIX, CleanSourceKind::OriginalPcr)
        } else if owner.starts_with(RECOVERED_OWNER_PREFIX) {
            (RECOVERED_OWNER_PREFIX, CleanSourceKind::RecoveredPmr)
        } else {
            return Ok(None);
        };
        if owner.len() != prefix.len() + OWNER_BODY_BYTES {
            return Err(WorkspaceError::Fenced);
        }
        let fields = owner[prefix.len()..].split('/').collect::<Vec<_>>();
        if fields.len() != 7 {
            return Err(WorkspaceError::Fenced);
        }
        let mut fingerprint = [0; 32];
        hex::decode_to_slice(fields[6], &mut fingerprint).map_err(|_| WorkspaceError::Fenced)?;
        let origin = Self {
            source_kind,
            original_lease: LeaseId::from_uuid(decode_uuid(fields[0])?),
            mount_uid: decode_uuid(fields[1])?,
            pod_uid: decode_uuid(fields[2])?,
            snapshot_id: SnapshotId::from_uuid(decode_uuid(fields[3])?),
            native_journal_id: JournalId::from_uuid(decode_uuid(fields[4])?),
            planned_head: LayerId::from_uuid(decode_uuid(fields[5])?),
            fingerprint,
        };
        if origin.owner_id() != owner {
            return Err(WorkspaceError::Fenced);
        }
        Ok(Some(origin))
    }

    pub(crate) fn matches_request(
        &self,
        request: &PackedHeadlessSnapshotRequest,
        layout: ChunkLayout,
    ) -> Result<(), WorkspaceError> {
        if self.snapshot_id != request.snapshot_id
            || self.native_journal_id != request.native_journal_id
            || self.planned_head != request.new_head_layer_id
            || self.fingerprint != logical_request_fingerprint(request, layout)?
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
}

pub(super) fn clean_operation_owner(
    reference: &PackedReleasedMountReference,
    source_kind: CleanSourceKind,
    request: &PackedHeadlessSnapshotRequest,
    layout: ChunkLayout,
) -> Result<String, WorkspaceError> {
    let origin = PackedCleanOperationOrigin {
        source_kind,
        original_lease: reference.guard.lease_id,
        mount_uid: reference.mount_uid,
        pod_uid: reference.pod_uid,
        snapshot_id: request.snapshot_id,
        native_journal_id: request.native_journal_id,
        planned_head: request.new_head_layer_id,
        fingerprint: logical_request_fingerprint(request, layout)?,
    };
    let owner = origin.owner_id();
    let parsed = PackedCleanOperationOrigin::parse(&owner)?.ok_or(WorkspaceError::Fenced)?;
    if parsed != origin || owner.len() > OPEN_OWNER_MAX_BYTES {
        return Err(WorkspaceError::Fenced);
    }
    Ok(owner)
}

impl<B: WorkspaceKvBackend> PackedCleanPublicationAuthority<B> {
    pub(crate) fn matches_snapshot_request(
        &self,
        request: &PackedHeadlessSnapshotRequest,
        layout: ChunkLayout,
    ) -> Result<(), WorkspaceError> {
        let origin = PackedCleanOperationOrigin::parse(&self.open.owner_id)?
            .ok_or(WorkspaceError::Fenced)?;
        if origin.original_lease != self.original.guard.lease_id
            || origin.mount_uid != self.original.mount_uid
            || origin.pod_uid != self.original.pod_uid
        {
            return Err(WorkspaceError::Fenced);
        }
        origin.matches_request(request, layout)
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// The private seed consumer may keep the actual live original admin lease
    /// only after the existing canonical open becomes Recovering. That exact
    /// transition fences the old clean publisher's Original authority. The
    /// PCR private claim fact, native source identity and current open/lease
    /// are all in its following actual seed CAS; generic Q recovery keeps its
    /// original expiry requirement.
    pub(in crate::workspace::stores::kv_store) async fn retained_original_clean_seed_recovery_checks(
        self: &Arc<Self>,
        mapping: &crate::workspace::stores::kv_store::packed_native_freeze::PackedNativePlannedRotation,
        binding: &PackedLowerBindingRecord,
        head_sequence: u64,
        expected_open: &SnapshotOpenRecord,
        expected_lease: &SnapshotLease,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<Option<(Vec<KvCheck>, SnapshotOwnedPermit)>, WorkspaceError> {
        let guard = mapping.old_guard();
        let native_id = mapping.journal_id();
        let planned_head = mapping.planned_head_layer_id();
        let Some(origin) = PackedCleanOperationOrigin::parse(&expected_open.owner_id)? else {
            return Ok(None);
        };
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        if origin.native_journal_id != native_id
            || origin.planned_head != planned_head
            || budget.state().closed
        {
            return Err(WorkspaceError::Fenced);
        }
        let keys = vec![
            origin.source_key(guard.workspace_id),
            hot_lease_key(guard.workspace_id, origin.original_lease),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            open_snapshot_key(guard.workspace_id),
        ];
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        let receipt = decode_clean_source(required(0)?, origin.source_kind())?;
        let original_lease: SnapshotLease = decode_open_value(required(1)?, OPEN_RECORD_MAX_BYTES)?;
        let lease: SnapshotLease = decode_open_value(required(2)?, OPEN_RECORD_MAX_BYTES)?;
        let open: SnapshotOpenRecord = decode_open_value(required(3)?, OPEN_RECORD_MAX_BYTES)?;
        let fact = receipt
            .first_admin_claim
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?;
        let mut recovered_open = fact.open_owner.clone();
        recovered_open.state = SnapshotOpenState::Recovering;
        recovered_open.recovery_required = true;
        if receipt.guard.workspace_id != guard.workspace_id
            || receipt.guard.lease_id != origin.original_lease
            || receipt.mount_uid != origin.mount_uid
            || receipt.pod_uid != origin.pod_uid
            || receipt.guard.expected_head_layer_id != guard.expected_head_layer_id
            || receipt.guard.expected_head_epoch != guard.expected_head_epoch
            || receipt.head_sequence != head_sequence
            || receipt.base_revision != binding.base_revision
            || receipt.binding_version != binding.binding.binding_version
            || receipt.manifest_digest != binding.binding.manifest.digest
            || fact.lease != lease
            || lease != *expected_lease
            || fact.lease.lease_id != guard.lease_id
            || fact.lease.holder_generation != guard.holder_generation
            || fact.open_owner.owner_id != origin.owner_id()
            || open != *expected_open
            || open != recovered_open
            || open.owner_id != fact.open_owner.owner_id
            || open.generation != fact.open_owner.generation
            || open.state != SnapshotOpenState::Recovering
            || !open.recovery_required
            || open.expires_at_ns <= now
            || lease.state != LeaseState::Active
            || lease.expires_at_ns <= now
            || !lease.writable
            || original_lease.lease_id != origin.original_lease
            || original_lease.workspace_id != guard.workspace_id
            || original_lease.holder_generation != receipt.guard.holder_generation
            || original_lease.base_revision != receipt.base_revision
            || original_lease.state != LeaseState::Released
            || !original_lease.writable
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(Some((
            keys.into_iter()
                .zip(values)
                .map(|(key, expected)| KvCheck { key, expected })
                .collect(),
            owner,
        )))
    }

    /// Reissue only the existing PCR's private pre-seed admin claim. Generic
    /// open/grant APIs cannot create its persisted lease/open fact. An expired
    /// incarnation is replaced once with that fact in the same exact source
    /// CAS; the original clean release and source cutoff stay immutable.
    async fn restore_clean_claim_before_seed(
        self: &Arc<Self>,
        workspace_id: WorkspaceId,
        origin: &PackedCleanOperationOrigin,
        request: &PackedHeadlessSnapshotRequest,
        layout: ChunkLayout,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<
        Option<(
            Arc<PackedCleanPublicationAuthority<B>>,
            PackedLowerBindingRecord,
            [LayerRecord; 2],
        )>,
        WorkspaceError,
    > {
        origin.matches_request(request, layout)?;
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        for attempt in 0..2 {
            let seed_key =
                format!("packed/native-freeze-basis/{}", origin.native_journal_id).into_bytes();
            let receipt_key = origin.source_key(workspace_id);
            let routing = vec![
                seed_key.clone(),
                CONTROL_KEY.to_vec(),
                receipt_key.clone(),
                packed_current_key(workspace_id),
            ];
            let (routed, _) = self
                .backend
                .get_many_consistent_with_time_bounded(&routing, source_limits(routing.len()))
                .await?;
            if routed.len() != routing.len() {
                return Err(WorkspaceError::Fenced);
            }
            if routed[0].is_some() {
                return Ok(None);
            }
            validate_current_control_raw(routed[1].as_deref())?;
            let mut receipt = decode_clean_source(
                routed[2].as_deref().ok_or(WorkspaceError::Fenced)?,
                origin.source_kind(),
            )?;
            let binding = PackedLowerBindingRecord::decode(
                routed[3].as_deref().ok_or(WorkspaceError::Fenced)?,
            )?;
            let fact = receipt
                .first_admin_claim
                .as_ref()
                .ok_or(WorkspaceError::Fenced)?;
            if fact.open_owner.owner_id != origin.owner_id() {
                return Err(WorkspaceError::Fenced);
            }
            let generation = fact.lease.holder_generation;
            let lease_id = fact.lease.lease_id;
            let history = self
                .read_workspace_lease_history_checks(workspace_id, 10, source_limits(32))
                .await?;
            let mut keys = vec![
                CONTROL_KEY.to_vec(),
                hot_workspace_key(workspace_id),
                hot_layer_key(binding.head_layer_id),
                hot_layer_key(binding.base_revision.layer_id),
                hot_lease_key(workspace_id, lease_id),
                open_snapshot_key(workspace_id),
                receipt_key,
                hot_lease_key(workspace_id, origin.original_lease),
                packed_current_key(workspace_id),
                packed_claim_key(workspace_id),
                packed_history_key(workspace_id, binding.binding.binding_version),
                seed_key,
                hot_layer_key(origin.planned_head),
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                open_snapshot_recovery_key(workspace_id),
                format!("packed/native-recovery-claim/{}", origin.native_journal_id).into_bytes(),
                hot_journal_key(workspace_id, origin.native_journal_id),
                hot_lease_index_key(lease_id),
                hot_journal_index_key(origin.native_journal_id),
            ];
            append_workspace_history_keys(&mut keys, &history, 32)?;
            let (values, now) = self
                .backend
                .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
                .await?;
            if values.len() != keys.len()
                || now <= 0
                || values[0] != routed[1]
                || values[6] != routed[2]
                || values[8] != routed[3]
                || values[11].is_some()
                || values[12].is_some()
                || values[16].is_some()
                || values[17].is_some()
                || values[19].is_some()
                || budget.state().closed
            {
                return Err(WorkspaceError::Fenced);
            }
            authenticate_workspace_history_values(&keys, &values, &history)?;
            let control = topology_state_from_checks(
                &keys
                    .iter()
                    .cloned()
                    .zip(values.iter().cloned())
                    .map(|(key, expected)| KvCheck { key, expected })
                    .collect::<Vec<_>>(),
            )?;
            let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
            let workspace: WorkspaceRecord = decode_open_value(required(1)?, SOURCE_MAX_BYTES)?;
            let head: LayerRecord = decode_open_value(required(2)?, SOURCE_MAX_BYTES)?;
            let base: LayerRecord = decode_open_value(required(3)?, SOURCE_MAX_BYTES)?;
            let lease: SnapshotLease = decode_open_value(required(4)?, SOURCE_MAX_BYTES)?;
            let open: SnapshotOpenRecord = decode_open_value(required(5)?, OPEN_RECORD_MAX_BYTES)?;
            let original_lease: SnapshotLease =
                decode_open_value(required(7)?, OPEN_RECORD_MAX_BYTES)?;
            let current = decode_packed_pair(workspace_id, &values[8], &values[9], &values[10])?
                .ok_or(WorkspaceError::Fenced)?;
            let recovery: Option<SnapshotRecoveryRecord> = values[15]
                .as_deref()
                .map(|bytes| decode_open_value(bytes, OPEN_RECOVERY_MAX_BYTES))
                .transpose()?;
            let original_guard = receipt.guard.to_head_guard();
            let guard = HeadGuard {
                lease_id,
                holder_generation: generation,
                ..original_guard.clone()
            };
            let mut original_admin = lease.clone();
            if lease.state == LeaseState::Expired
                && lease.expires_at_ns <= now
                && lease.updated_at_ns >= fact.lease.updated_at_ns
                && lease.updated_at_ns <= now
            {
                original_admin.state = fact.lease.state;
                original_admin.updated_at_ns = fact.lease.updated_at_ns;
            }
            if original_admin != fact.lease || open != fact.open_owner {
                return Err(WorkspaceError::Fenced);
            }
            if lease.state == LeaseState::Active && lease.expires_at_ns > now {
                checked_hot_guard(&workspace, &head, &lease, &guard, now)?;
            } else if !matches!(lease.state, LeaseState::Active | LeaseState::Expired)
                || lease.expires_at_ns > now
                || workspace.workspace_id != workspace_id
                || workspace.head_layer_id != guard.expected_head_layer_id
                || workspace.head_epoch != guard.expected_head_epoch
                || head.layer_id != guard.expected_head_layer_id
                || head.state != LayerState::Writable
                || head.owner_workspace_id != Some(workspace_id)
                || head.parent_layer_id != Some(base.layer_id)
                || head.depth != base.depth.checked_add(1).ok_or(WorkspaceError::Fenced)?
            {
                return Err(WorkspaceError::Fenced);
            }
            validate_open_record(&open, workspace_id)?;
            current.validate_for_guard(&guard, &base)?;
            next_packed_root_generation(&values[13])?;
            layer_inventory_generation(&values[14])?;
            if current != binding
                || workspace.state != WorkspaceState::Active
                || workspace.active_lease.is_some_and(|id| id != lease_id)
                || decode_open_value::<WorkspaceId>(required(18)?, 64)? != workspace_id
                || control.schema_version != WORKSPACE_SCHEMA_VERSION
                || control.leases.get(&lease_id) != Some(&lease)
                || control.layers.get(&head.layer_id) != Some(&head)
                || control.leases.get(&original_lease.lease_id) != Some(&original_lease)
                || control.layers.get(&base.layer_id) != Some(&base)
                || control.workspaces.get(&workspace_id) != Some(&workspace)
                || head.next_sequence != receipt.head_sequence
                || lease.base_revision != receipt.base_revision
                || lease_id == origin.original_lease
                || open.owner_id != origin.owner_id()
                || open.state != SnapshotOpenState::Ready
                || open.recovery_required
                || receipt.guard.workspace_id != workspace_id
                || receipt.guard.lease_id != origin.original_lease
                || receipt.mount_uid != origin.mount_uid
                || receipt.pod_uid != origin.pod_uid
                || receipt.binding_version != binding.binding.binding_version
                || receipt.base_revision != binding.base_revision
                || receipt.manifest_digest != binding.binding.manifest.digest
                || original_lease.lease_id != origin.original_lease
                || original_lease.workspace_id != workspace_id
                || original_lease.holder_generation != original_guard.holder_generation
                || original_lease.base_revision != receipt.base_revision
                || original_lease.state != LeaseState::Released
                || !original_lease.writable
                || recovery
                    .as_ref()
                    .is_some_and(|row| row.workspace_id != workspace_id || row.incomplete)
                || control.leases.values().any(|row| {
                    row.workspace_id == workspace_id
                        && row.writable
                        && row.lease_id != lease_id
                        && (row.state == LeaseState::Active
                            || row.holder_generation >= generation
                            || row.created_at_ns >= lease.created_at_ns)
                })
            {
                return Err(WorkspaceError::Fenced);
            }
            let mut checks = keys
                .into_iter()
                .zip(values)
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>();
            let _writer_owner = self
                .authenticate_administrative_packed_writer(workspace_id, &mut checks)
                .await?;
            if lease.state != LeaseState::Active
                || lease.expires_at_ns <= now
                || open.expires_at_ns <= now
            {
                if attempt != 0
                    || lease.expires_at_ns > now
                    || request.lease_id == lease_id
                    || control.leases.contains_key(&request.lease_id)
                {
                    return Err(WorkspaceError::Fenced);
                }
                let new_key = hot_lease_key(workspace_id, request.lease_id);
                let new_index = hot_lease_index_key(request.lease_id);
                let (new_values, _) = self
                    .backend
                    .get_many_consistent_with_time_bounded(
                        &[new_key.clone(), new_index.clone()],
                        source_limits(2),
                    )
                    .await?;
                if new_values.len() != 2 || new_values.iter().any(Option::is_some) {
                    return Err(WorkspaceError::Fenced);
                }
                checks.push(KvCheck {
                    key: new_key.clone(),
                    expected: None,
                });
                checks.push(KvCheck {
                    key: new_index.clone(),
                    expected: None,
                });
                let expires_at_ns = checked_expiry(now, request.lease_ttl_ns)?;
                let successor_open = SnapshotOpenRecord {
                    generation: open
                        .generation
                        .checked_add(1)
                        .ok_or(WorkspaceError::Fenced)?,
                    expires_at_ns,
                    ..open.clone()
                };
                let successor = SnapshotLease {
                    lease_id: request.lease_id,
                    workspace_id,
                    base_revision: receipt.base_revision.clone(),
                    holder_generation: generation.checked_add(1).ok_or(WorkspaceError::Fenced)?,
                    writable: true,
                    state: LeaseState::Active,
                    expires_at_ns,
                    created_at_ns: now,
                    updated_at_ns: now,
                };
                let mut predecessor = lease.clone();
                predecessor.state = LeaseState::Expired;
                predecessor.updated_at_ns = now;
                let mut next_workspace = workspace.clone();
                next_workspace.active_lease = Some(successor.lease_id);
                next_workspace.updated_at_ns = now;
                receipt.first_admin_claim = Some(FirstAdminCleanClaim {
                    lease: successor.clone(),
                    open_owner: successor_open.clone(),
                });
                let mut writes = vec![
                    put(hot_workspace_key(workspace_id), &next_workspace)?,
                    put(
                        hot_lease_key(workspace_id, predecessor.lease_id),
                        &predecessor,
                    )?,
                    put(new_key, &successor)?,
                    put(new_index, &workspace_id)?,
                    put(open_snapshot_key(workspace_id), &successor_open)?,
                    KvWrite::Put {
                        key: checks[6].key.clone(),
                        value: encode_clean_source(&receipt)?,
                    },
                ];
                let _writer_owner = self.prepare_administrative_packed_writer(
                    workspace_id, crate::workspace::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::Update, &mut checks, &mut writes,
                ).await?;
                let _holds = self
                    .prepare_native_owner_cas(&mut checks, &mut writes)
                    .await?;
                self.clean_completion_ownership_cas(
                    &checks,
                    &writes,
                    Some(lease.expires_at_ns),
                    expires_at_ns,
                )
                .await?;
                continue;
            }
            if !self
                .backend
                .compare_and_swap_before(&checks, &[], open.expires_at_ns.min(lease.expires_at_ns))
                .await?
            {
                return Err(WorkspaceError::Fenced);
            }
            return Ok(Some((
                Arc::new(PackedCleanPublicationAuthority {
                    store: self.clone(),
                    original: PackedReleasedMountReference {
                        guard: original_guard,
                        mount_uid: receipt.mount_uid,
                        pod_uid: receipt.pod_uid,
                    },
                    immutable: [6usize, 7]
                        .into_iter()
                        .map(|index| checks[index].clone())
                        .collect(),
                    open,
                    guard,
                    budget,
                    mode: CleanAuthorityMode::Original,
                    _owner: owner,
                }),
                binding,
                [head, base],
            )));
        }
        Err(WorkspaceError::Fenced)
    }

    /// Called only after the real seed/PNB recovery has constructed its actual
    /// current source incarnation, and before that native fence is validated.
    /// The exact current open supplies the route; a name/digest alone grants no
    /// source, drain, graph, Hashed or final publication authority.
    pub(crate) async fn restore_clean_native_origin(
        self: &Arc<Self>,
        native: &PackedNativeQuiesceFence<B>,
    ) -> Result<Option<Arc<PackedCleanPublicationAuthority<B>>>, WorkspaceError> {
        if !native.is_same_store(self)
            || (!native.requires_recovery_owner() && native.recovery_basis().is_none())
        {
            return Err(WorkspaceError::Fenced);
        }
        let budget = native.mount_budget();
        if budget.state().closed {
            return Err(WorkspaceError::Fenced);
        }
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let guard = native.source_guard().clone();
        let open_key = open_snapshot_key(guard.workspace_id);
        let (routed, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                std::slice::from_ref(&open_key),
                source_limits(1),
            )
            .await?;
        if routed.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let Some(routed_bytes) = routed[0].as_deref() else {
            return Ok(None);
        };
        let routed_open: SnapshotOpenRecord =
            decode_open_value(routed_bytes, OPEN_RECORD_MAX_BYTES)?;
        let Some(origin) = PackedCleanOperationOrigin::parse(&routed_open.owner_id)? else {
            return Ok(None);
        };
        let original_ownership = !native.requires_recovery_owner();
        if original_ownership && native.recovery_basis().is_none() {
            return Err(WorkspaceError::Fenced);
        }
        let receipt_key = origin.source_key(guard.workspace_id);
        let keys = vec![
            receipt_key,
            hot_lease_key(guard.workspace_id, origin.original_lease),
            open_key,
            hot_lease_key(guard.workspace_id, guard.lease_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
        ];
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || values[2] != routed[0] || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        let receipt = decode_clean_source(required(0)?, origin.source_kind())?;
        let first_admin = receipt
            .first_admin_claim
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?;
        let original_lease: SnapshotLease = decode_open_value(required(1)?, OPEN_RECORD_MAX_BYTES)?;
        let open: SnapshotOpenRecord = decode_open_value(required(2)?, OPEN_RECORD_MAX_BYTES)?;
        let lease: SnapshotLease = decode_open_value(required(3)?, OPEN_RECORD_MAX_BYTES)?;
        validate_open_record(&open, guard.workspace_id)?;
        next_packed_root_generation(&values[4])?;
        layer_inventory_generation(&values[5])?;
        let mapping = native.mapping();
        let original_guard = receipt.guard.to_head_guard();
        let first_guard = mapping.old_guard();
        if receipt.guard.workspace_id != guard.workspace_id
            || receipt.guard.lease_id != origin.original_lease
            || receipt.mount_uid != origin.mount_uid
            || receipt.pod_uid != origin.pod_uid
            || receipt.guard.expected_head_layer_id != first_guard.expected_head_layer_id
            || receipt.guard.expected_head_epoch != first_guard.expected_head_epoch
            || first_admin.lease.lease_id != first_guard.lease_id
            || first_admin.lease.holder_generation != first_guard.holder_generation
            || first_admin.open_owner.owner_id != origin.owner_id()
            || first_guard.lease_id == origin.original_lease
            || receipt.head_sequence != mapping.old_layers()[0].next_sequence
            || origin.native_journal_id != mapping.journal_id()
            || origin.planned_head != mapping.planned_head_layer_id()
            || receipt.base_revision != native.binding().base_revision
            || receipt.binding_version != native.binding().binding.binding_version
            || receipt.manifest_digest != native.binding().binding.manifest.digest
            || original_lease.lease_id != origin.original_lease
            || original_lease.workspace_id != guard.workspace_id
            || original_lease.holder_generation != original_guard.holder_generation
            || original_lease.base_revision != receipt.base_revision
            || original_lease.state != LeaseState::Released
            || !original_lease.writable
            || (if original_ownership {
                open != first_admin.open_owner
                    || lease != first_admin.lease
                    || native.source_guard() != native.mapping().old_guard()
            } else {
                open.state != SnapshotOpenState::Recovering || !open.recovery_required
            })
            || open.expires_at_ns <= now
            || lease.lease_id != guard.lease_id
            || lease.workspace_id != guard.workspace_id
            || lease.holder_generation != guard.holder_generation
            || lease.base_revision != receipt.base_revision
            || lease.state != LeaseState::Active
            || !lease.writable
            || lease.expires_at_ns <= now
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let _writer_owner = self
            .authenticate_administrative_packed_writer(guard.workspace_id, &mut checks)
            .await?;
        let deadline = open.expires_at_ns.min(lease.expires_at_ns);
        if !self
            .backend
            .compare_and_swap_before(&checks, &[], deadline)
            .await?
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(Some(Arc::new(PackedCleanPublicationAuthority {
            store: self.clone(),
            original: PackedReleasedMountReference {
                guard: original_guard,
                mount_uid: receipt.mount_uid,
                pod_uid: receipt.pod_uid,
            },
            immutable: checks[..2].to_vec(),
            open,
            guard,
            budget,
            mode: if original_ownership {
                CleanAuthorityMode::Original
            } else {
                CleanAuthorityMode::Recovery
            },
            _owner: owner,
        })))
    }
}

#[cfg(target_os = "linux")]
impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Resume only the exact logical operation named by the existing canonical
    /// open owner. A pre-PNB seed or actual PNB basis grants current source
    /// authority; PCR, an owner string or a requested journal ID does not.
    /// Cancelling this receiver never cancels the owning recovery driver.
    pub async fn recover_clean_packed_snapshot<O, S>(
        self: &Arc<Self>,
        workspace_id: WorkspaceId,
        packed_journal_id: Option<JournalId>,
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
                "packed snapshot recovery requires a Tokio runtime".into(),
            )
        })?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        handle.spawn(async move {
            let result = store
                .recover_clean_packed_snapshot_owned(
                    workspace_id,
                    packed_journal_id,
                    client,
                    upper,
                    layout,
                    request,
                )
                .await;
            let _ = sender.send(result);
        });
        receiver.await.map_err(|_| {
            PackedHeadlessSnapshotFailure::from(WorkspaceError::CorruptMetadata(
                "owned packed snapshot recovery driver stopped".into(),
            ))
        })?
    }

    async fn recover_clean_packed_snapshot_owned<O, S>(
        self: &Arc<Self>,
        workspace_id: WorkspaceId,
        packed_journal_id: Option<JournalId>,
        client: ObjectClient<O>,
        upper: Arc<S>,
        layout: ChunkLayout,
        request: PackedHeadlessSnapshotRequest,
    ) -> Result<PackedSnapshotResult, PackedHeadlessSnapshotFailure>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        use crate::chunk::read_plan::WorkspaceReadPlanProvider;
        use crate::workspace::meta_layer::PinnedCatalogPackedBindingAuthority;
        use crate::workspace::packed_reader_lifecycle::{
            KvPackedReaderSession, PackedReaderLeaseOptions, PackedReaderSession,
        };
        use crate::workspace::stores::kv_store::packed_journal::{
            PackedJournalPhase, PackedJournalRecord,
        };
        use crate::workspace::stores::kv_store::packed_native_freeze::{
            NativePackedRecoveryClaimRequest, NativePrepareRecoveryRequest,
        };

        if workspace_id.as_uuid().is_nil()
            || request.snapshot_id.as_uuid().is_nil()
            || request.native_journal_id.as_uuid().is_nil()
            || request.new_head_layer_id.as_uuid().is_nil()
            || request.lease_id.as_uuid().is_nil()
            || packed_journal_id.is_some_and(|id| id.as_uuid().is_nil())
            || request.lease_ttl_ns == 0
            || request.lease_ttl_ns > 15 * 60 * 1_000_000_000
            || request.max_rows == 0
            || request.scratch_disk_bytes < 16 << 10
            || layout.chunk_size == 0
            || request
                .temporary_owner
                .as_ref()
                .is_some_and(|owner| owner.path() != request.temporary)
            || request.cancel.is_cancelled()
        {
            return Err(WorkspaceError::Fenced.into());
        }
        let budget = self.resolve_packed_reader_pin_budget(SnapshotMountBudget::defaults());
        if budget.state().closed {
            return Err(WorkspaceError::Fenced.into());
        }
        let observer = budget
            .read_observer(client.read_observer())
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let client = client.with_read_observer(
            observer,
            crate::cadapter::read_observer::Engine::Packed,
            crate::cadapter::read_observer::Phase::Startup,
            crate::cadapter::read_observer::Origin::Demand,
        );
        // Covers routing/decode/clone packets through the real recovery driver.
        // Native capture, source hash and graph retain their own ledger owners.
        let _routing_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let (routed, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                &[open_snapshot_key(workspace_id)],
                source_limits(1),
            )
            .await?;
        if routed.len() != 1 {
            return Err(WorkspaceError::Fenced.into());
        }
        let routed_open: SnapshotOpenRecord = decode_open_value(
            routed[0].as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        validate_open_record(&routed_open, workspace_id)?;
        let origin = PackedCleanOperationOrigin::parse(&routed_open.owner_id)?
            .ok_or(WorkspaceError::Fenced)?;
        // Reject another logical snapshot before any open/lease mutation.
        origin.matches_request(&request, layout)?;
        let owner_id = origin.owner_id();

        let actual_journal_id = self
            .read_clean_native_journal_route(
                workspace_id,
                origin.native_journal_id,
                origin.planned_head,
                &budget,
            )
            .await?;
        if packed_journal_id.is_some() && packed_journal_id != actual_journal_id {
            return Err(WorkspaceError::Fenced.into());
        }

        let routed_record = if let Some(id) = actual_journal_id {
            let key = format!("packed/journal/{id}").into_bytes();
            let (values, _) = self
                .backend
                .get_many_consistent_with_time_bounded(&[key], source_limits(1))
                .await?;
            if values.len() != 1 {
                return Err(WorkspaceError::Fenced.into());
            }
            let record =
                PackedJournalRecord::decode(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
            if record.journal_id != id
                || record.guard.workspace_id != workspace_id
                || record.phase == PackedJournalPhase::Aborted
            {
                return Err(WorkspaceError::Fenced.into());
            }
            if record.phase == PackedJournalPhase::Committed {
                return self
                    .finish_committed_clean_snapshot(
                        CommittedCleanPublication { record, origin },
                        client,
                        upper,
                        layout,
                        request,
                        budget,
                    )
                    .await;
            }
            Some(record)
        } else {
            None
        };
        let first_claim = if routed_record.is_none() {
            self.restore_clean_claim_before_seed(
                workspace_id,
                &origin,
                &request,
                layout,
                budget.clone(),
            )
            .await?
        } else {
            None
        };
        // This actual read-only basis is independent of the routing decode. It
        // verifies the original PPJ/native catalog before recovery claims.
        let basis = if let Some(record) = &routed_record {
            let basis = self
                .inspect_native_packed_recovery_basis(record, &budget)
                .await?;
            if basis.native_journal_id() != origin.native_journal_id
                || basis.planned_head_layer_id() != origin.planned_head
                || basis.old_guard().workspace_id != workspace_id
            {
                return Err(WorkspaceError::Fenced.into());
            }
            Some(basis)
        } else {
            None
        };
        let (claim, binding, first_layers, recovery) = if let Some((claim, binding, layers)) =
            first_claim
        {
            (claim, binding, Some(layers), None)
        } else {
            let live_original = if let Some(basis) = &basis {
                let keys = vec![
                    hot_lease_key(workspace_id, basis.old_guard().lease_id),
                    open_snapshot_key(workspace_id),
                ];
                let (values, now) = self
                    .backend
                    .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
                    .await?;
                if values.len() != 2 || now <= 0 {
                    return Err(WorkspaceError::Fenced.into());
                }
                let lease: SnapshotLease = decode_open_value(
                    values[0].as_deref().ok_or(WorkspaceError::Fenced)?,
                    OPEN_RECORD_MAX_BYTES,
                )?;
                let open: SnapshotOpenRecord = decode_open_value(
                    values[1].as_deref().ok_or(WorkspaceError::Fenced)?,
                    OPEN_RECORD_MAX_BYTES,
                )?;
                lease.state == LeaseState::Active
                    && lease.expires_at_ns > now
                    && lease.holder_generation == basis.old_guard().holder_generation
                    && lease.workspace_id == workspace_id
                    && lease.writable
                    && open.state == SnapshotOpenState::Ready
                    && !open.recovery_required
                    && open.owner_id == owner_id
                    && open.expires_at_ns > now
            } else {
                false
            };
            if !live_original {
                let open = self
                    .open_workspace_snapshot(
                        workspace_id,
                        owner_id.clone(),
                        std::time::Duration::from_nanos(request.lease_ttl_ns),
                    )
                    .await?;
                if open.state != SnapshotOpenState::Recovering
                    || !open.recovery_required
                    || open.owner_id != owner_id
                {
                    return Err(WorkspaceError::Fenced.into());
                }
            }
            let recovery = if let Some(basis) = basis {
                if live_original {
                    self.reissue_native_source_read(basis, budget.clone())
                        .await?
                } else {
                    let claim = self
                        .claim_native_packed_recovery(
                            basis,
                            NativePackedRecoveryClaimRequest {
                                new_lease_id: request.lease_id,
                                owner_id,
                                ttl_ns: request.lease_ttl_ns,
                            },
                            budget.clone(),
                        )
                        .await?;
                    let record = routed_record.as_ref().ok_or(WorkspaceError::Fenced)?;
                    let fresh_basis = self
                        .inspect_native_packed_recovery_basis(record, &budget)
                        .await?;
                    self.reissue_native_source_read_claimed(fresh_basis, claim, budget.clone())
                        .await?
                }
            } else {
                let key = origin.source_key(workspace_id);
                let (values, _) = self
                    .backend
                    .get_many_consistent_with_time_bounded(&[key], source_limits(1))
                    .await?;
                if values.len() != 1 {
                    return Err(WorkspaceError::Fenced.into());
                }
                let receipt = decode_clean_source(
                    values[0].as_deref().ok_or(WorkspaceError::Fenced)?,
                    origin.source_kind(),
                )?;
                let fact = receipt
                    .first_admin_claim
                    .as_ref()
                    .ok_or(WorkspaceError::Fenced)?;
                let (values, now) = self
                    .backend
                    .get_many_consistent_with_time_bounded(
                        &[hot_lease_key(workspace_id, fact.lease.lease_id)],
                        source_limits(1),
                    )
                    .await?;
                if values.len() != 1 || now <= 0 {
                    return Err(WorkspaceError::Fenced.into());
                }
                let lease: SnapshotLease = decode_open_value(
                    values[0].as_deref().ok_or(WorkspaceError::Fenced)?,
                    OPEN_RECORD_MAX_BYTES,
                )?;
                let seed_original_live = lease == fact.lease
                    && lease.state == LeaseState::Active
                    && lease.expires_at_ns > now;
                let native = self
                    .recover_packed_native_prepare(
                        NativePrepareRecoveryRequest {
                            journal_id: request.native_journal_id,
                            owner_id,
                            new_lease_id: if seed_original_live {
                                None
                            } else {
                                Some(request.lease_id)
                            },
                            ttl_ns: request.lease_ttl_ns,
                        },
                        budget.clone(),
                    )
                    .await?;
                self.reissue_native_seed_read(native, budget.clone())
                    .await?
            };
            let recovery = if recovery.basis().is_some() {
                self.quarantine_missing_native_attempt_and_reissue(
                    recovery,
                    &client,
                    request.graph_limits.max_objects,
                    &request.cancel,
                )
                .await?
            } else {
                recovery
            };
            let native = recovery.native_quiesce();
            let claim = native
                .clean_publication_origin()
                .ok_or(WorkspaceError::Fenced)?;
            if claim.mode == CleanAuthorityMode::Committed || claim.guard() != native.source_guard()
            {
                return Err(WorkspaceError::Fenced.into());
            }
            (claim, native.binding().clone(), None, Some(recovery))
        };
        let runtime = Arc::new(HeadlessRuntimeOwner {
            _claim: Some(claim.clone()),
            _store: self.clone(),
            budget: budget.clone(),
            _upper: upper.clone(),
            _temporary_owner: request.temporary_owner.clone(),
            cleanup_handle: tokio::runtime::Handle::current(),
            metadata: std::sync::Mutex::new(None),
            reader: std::sync::Mutex::new(None),
            native: std::sync::Mutex::new(
                recovery
                    .as_ref()
                    .map(|recovery| recovery.native_quiesce().clone()),
            ),
            finished: std::sync::atomic::AtomicBool::new(false),
        });
        claim
            .matches_snapshot_request(&request, layout)
            .map_err(|error| runtime_failure(error, &runtime))?;
        if !Arc::ptr_eq(claim.mount_budget(), &budget) {
            return Err(runtime_failure(WorkspaceError::Fenced, &runtime));
        }
        let lower_snapshot = AuthenticatedSnapshot::open(&client, &binding.binding.manifest)
            .await
            .map_err(|error| {
                runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?;
        let lower = Arc::new(
            PackedReadonlyMeta::from_snapshot_budget(
                client.clone(),
                lower_snapshot,
                layout.chunk_size,
                0,
                budget.clone(),
            )
            .map_err(|error| {
                runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?,
        );
        let guard = claim.guard();
        let metadata = WorkspaceMetaLayer::with_chunk_size(
            self.clone(),
            ViewContext {
                workspace_id: guard.workspace_id,
                head_layer_id: guard.expected_head_layer_id,
                head_epoch: guard.expected_head_epoch,
                lease_id: guard.lease_id,
                holder_generation: guard.holder_generation,
            },
            layout.chunk_size,
        );
        let metadata = Arc::new(if let Some(recovery) = &recovery {
            let reader: Arc<dyn PackedReaderSession> = KvPackedReaderSession::open_native_recovery(
                recovery.clone(),
                budget.clone(),
                PackedReaderLeaseOptions::default(),
            )
            .await
            .map_err(|error| runtime_failure(error, &runtime))?;
            *runtime.reader.lock().unwrap() = Some(reader.clone());
            let authority = Arc::new(PinnedCatalogPackedBindingAuthority {
                store: self.clone(),
                reader,
            });
            metadata
                .with_packed_lower(binding.binding, lower, authority, upper.clone(), layout)
                .map_err(|error| {
                    runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
                })?
        } else {
            metadata
                .with_packed_lower_from_store_owned(lower, upper.clone(), layout, |reader| {
                    *runtime.reader.lock().unwrap() = Some(reader);
                })
                .await
                .map_err(|error| {
                    runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
                })?
        });
        *runtime.metadata.lock().unwrap() = Some(metadata.clone());
        // The recovered head is Sealing. Typed frozen-source authority feeds
        // this readonly carrier; ordinary writable initialize is inapplicable.
        let provider: Arc<dyn WorkspaceReadPlanProvider> = metadata.clone();
        let vfs = VFS::from_readonly_components_with_provider(
            VFSConfig::new(layout),
            upper,
            metadata.clone(),
            provider,
        )
        .map_err(|error| {
            runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
        })?;
        let local = vfs.quiesce_packed_vfs().await.map_err(|error| {
            runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
        })?;
        let native = if let Some(recovery) = &recovery {
            recovery.native_quiesce().clone()
        } else {
            let native = Arc::new(
                self.clone()
                    .begin_clean_packed_native_quiesce(
                        claim.clone(),
                        first_layers
                            .ok_or_else(|| runtime_failure(WorkspaceError::Fenced, &runtime))?,
                        request.native_journal_id,
                        request.new_head_layer_id,
                    )
                    .await
                    .map_err(|error| runtime_failure(error, &runtime))?,
            );
            *runtime.native.lock().unwrap() = Some(native.clone());
            native
        };
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

        let ready =
            if let Some(recovery) = recovery.filter(|recovery| recovery.basis().is_some()) {
                self.resume_frozen_native_publication(
                    artifact,
                    recovery,
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
            } else {
                self.prepare_frozen_native_publication(
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
            }
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
            })?;
        let outcome = ready.commit().await.map_err(|failure| {
            PackedHeadlessSnapshotFailure::held((failure, runtime.clone()), |owner| &owner.0.error)
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

    /// Finish an actual already-committed publication. This route issues only
    /// completion authority: it cannot mint a native source or replay a commit.
    /// Actual final-source facts bind the lease incarnation even after native
    /// recovery consumed its owner pointer. Expired completion ownership can
    /// be replaced once in the same exact CAS as the persisted completion fact.
    async fn finish_committed_clean_snapshot<O, S>(
        self: &Arc<Self>,
        publication: CommittedCleanPublication,
        client: ObjectClient<O>,
        upper: Arc<S>,
        layout: ChunkLayout,
        request: PackedHeadlessSnapshotRequest,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<PackedSnapshotResult, PackedHeadlessSnapshotFailure>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        use crate::chunk::read_plan::WorkspaceReadPlanProvider;
        use crate::workspace::stores::kv_store::packed_journal::PackedJournalPhase;
        let CommittedCleanPublication { mut record, origin } = publication;
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SOURCE_ADMISSION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        origin.matches_request(&request, layout)?;
        let mut ownership_attempted = false;
        let (claim, result) = loop {
            let (native_id, planned_head, source_version, carrier) =
                record.native_completion_identities()?;
            let (source_root_hash, source_delta_digest) = record
                .final_source_hash_facts()
                .ok_or(WorkspaceError::Fenced)?;
            let target = record
                .commit_target
                .as_ref()
                .ok_or(WorkspaceError::Fenced)?
                .clone();
            if record.phase != PackedJournalPhase::Committed
                || native_id != origin.native_journal_id
                || planned_head != origin.planned_head
                || target.head_layer_id != origin.planned_head
                || target.workspace_id != record.guard.workspace_id
                || target.base_revision != carrier
            {
                return Err(WorkspaceError::Fenced.into());
            }
            let workspace_id = record.guard.workspace_id;
            let final_source_guard = record.final_source_guard().ok_or(WorkspaceError::Fenced)?;
            let guard = HeadGuard {
                expected_head_layer_id: target.head_layer_id,
                expected_head_epoch: target.head_epoch,
                ..final_source_guard.clone()
            };
            let receipt_key = origin.source_key(workspace_id);
            let history = self
                .read_workspace_lease_history_checks(workspace_id, 8, source_limits(32))
                .await?;
            let mut keys = vec![
                CONTROL_KEY.to_vec(),
                format!("packed/journal/{}", record.journal_id).into_bytes(),
                hot_workspace_key(workspace_id),
                hot_layer_key(target.head_layer_id),
                hot_layer_key(carrier.layer_id),
                hot_layer_key(record.guard.expected_head_layer_id),
                hot_lease_key(workspace_id, guard.lease_id),
                open_snapshot_key(workspace_id),
                open_snapshot_recovery_key(workspace_id),
                packed_current_key(workspace_id),
                packed_claim_key(workspace_id),
                packed_history_key(workspace_id, target.binding.binding_version),
                receipt_key,
                hot_lease_key(workspace_id, origin.original_lease),
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                format!("packed/journal-active/{}", record.journal_id).into_bytes(),
                format!("packed/native-recovery-claim/{native_id}").into_bytes(),
                hot_journal_key(workspace_id, native_id),
                hot_journal_index_key(native_id),
                hot_lease_index_key(guard.lease_id),
            ];
            append_workspace_history_keys(&mut keys, &history, 32)?;
            let (values, now) = self
                .backend
                .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
                .await?;
            if values.len() != keys.len()
                || now <= 0
                || values[1].as_deref() != Some(record.encode()?.as_slice())
                || values[16].is_some()
                || values[17].is_some()
            {
                return Err(WorkspaceError::Fenced.into());
            }
            authenticate_workspace_history_values(&keys, &values, &history)?;
            let control = topology_state_from_checks(
                &keys
                    .iter()
                    .cloned()
                    .zip(values.iter().cloned())
                    .map(|(key, expected)| KvCheck { key, expected })
                    .collect::<Vec<_>>(),
            )?;
            let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
            let workspace: WorkspaceRecord = decode_open_value(required(2)?, SOURCE_MAX_BYTES)?;
            let head: LayerRecord = decode_open_value(required(3)?, SOURCE_MAX_BYTES)?;
            let base: LayerRecord = decode_open_value(required(4)?, SOURCE_MAX_BYTES)?;
            let source: Option<LayerRecord> = values[5]
                .as_deref()
                .map(|bytes| decode_open_value(bytes, SOURCE_MAX_BYTES))
                .transpose()?;
            let lease: SnapshotLease = decode_open_value(required(6)?, SOURCE_MAX_BYTES)?;
            let open: SnapshotOpenRecord = decode_open_value(required(7)?, OPEN_RECORD_MAX_BYTES)?;
            let recovery: Option<SnapshotRecoveryRecord> = values[8]
                .as_deref()
                .map(|bytes| decode_open_value(bytes, OPEN_RECOVERY_MAX_BYTES))
                .transpose()?;
            let current = decode_packed_pair(workspace_id, &values[9], &values[10], &values[11])?
                .ok_or(WorkspaceError::Fenced)?;
            let receipt = decode_clean_source(required(12)?, origin.source_kind())?;
            let first_admin = receipt
                .first_admin_claim
                .as_ref()
                .ok_or(WorkspaceError::Fenced)?;
            let original_lease: SnapshotLease =
                decode_open_value(required(13)?, OPEN_RECORD_MAX_BYTES)?;
            let native_journal: Option<SealJournal> = values[18]
                .as_deref()
                .map(|raw| decode_open_value(raw, SOURCE_MAX_BYTES))
                .transpose()?;
            let journal_workspace: Option<WorkspaceId> = values[19]
                .as_deref()
                .map(|raw| decode_open_value(raw, 64))
                .transpose()?;
            if native_journal.is_some() != journal_workspace.is_some()
                || journal_workspace.is_some_and(|id| id != workspace_id)
                || decode_open_value::<WorkspaceId>(required(20)?, 64)? != workspace_id
            {
                return Err(WorkspaceError::Fenced.into());
            }
            if lease.state == LeaseState::Active && lease.expires_at_ns > now {
                checked_hot_guard(&workspace, &head, &lease, &guard, now)?;
            } else if !matches!(lease.state, LeaseState::Active | LeaseState::Expired)
                || lease.expires_at_ns > now
                || lease.lease_id != guard.lease_id
                || lease.holder_generation != guard.holder_generation
                || lease.workspace_id != workspace_id
                || !lease.writable
                || workspace.workspace_id != workspace_id
                || workspace.head_layer_id != guard.expected_head_layer_id
                || workspace.head_epoch != guard.expected_head_epoch
                || head.layer_id != guard.expected_head_layer_id
                || head.state != LayerState::Writable
                || head.parent_layer_id != Some(carrier.layer_id)
                || head.depth != 2
                || head.owner_workspace_id != Some(workspace_id)
            {
                return Err(WorkspaceError::Fenced.into());
            }
            validate_open_record(&open, workspace_id)?;
            current.validate_for_guard(&guard, &base)?;
            next_packed_root_generation(&values[14])?;
            layer_inventory_generation(&values[15])?;
            if control.schema_version != WORKSPACE_SCHEMA_VERSION
                || current != target
                || control.workspaces.get(&workspace_id) != Some(&workspace)
                || control.layers.get(&head.layer_id) != Some(&head)
                || control.layers.get(&base.layer_id) != Some(&base)
                || control.layers.get(&record.guard.expected_head_layer_id) != source.as_ref()
                || control.leases.get(&lease.lease_id) != Some(&lease)
                || control.leases.get(&original_lease.lease_id) != Some(&original_lease)
                || workspace.state != WorkspaceState::Active
                || workspace
                    .active_lease
                    .is_some_and(|id| id != guard.lease_id)
                || head.next_sequence != 1
                || head.owned_slice_count != 0
                || head.owned_bytes != 0
                || base.state != LayerState::Sealed
                || base.depth != 1
                || base.parent_layer_id.is_some()
                || base.sealed_version != Some(carrier.sealed_version)
                || base.root_hash != Some(carrier.root_hash)
                || source.as_ref().is_some_and(|source| {
                    source.layer_id != record.guard.expected_head_layer_id
                        || source.state != LayerState::Sealed
                        || source.sealed_version != Some(source_version)
                        || source.owner_workspace_id.is_some()
                        || source.next_sequence != receipt.head_sequence
                        || source.root_hash != Some(source_root_hash)
                        || source.delta_digest != Some(source_delta_digest)
                })
                || native_journal.as_ref().is_some_and(|journal| {
                    journal.journal_id != native_id
                        || journal.phase != SealPhase::Completed
                        || journal.workspace_id != workspace_id
                        || journal.old_head_layer_id != record.guard.expected_head_layer_id
                        || journal.expected_head_epoch != record.guard.expected_head_epoch
                        || journal.new_head_layer_id != Some(target.head_layer_id)
                        || journal.delta_digest != Some(source_delta_digest)
                        || journal.root_hash != Some(source_root_hash)
                })
                || open.owner_id != origin.owner_id()
                || open.state != SnapshotOpenState::Ready
                || open.recovery_required
                || lease.base_revision != carrier
                || recovery
                    .as_ref()
                    .is_some_and(|row| row.workspace_id != workspace_id || row.incomplete)
                || control.leases.values().any(|row| {
                    row.workspace_id == workspace_id
                        && row.writable
                        && row.lease_id != lease.lease_id
                        && (row.state == LeaseState::Active
                            || row.holder_generation >= lease.holder_generation
                            || row.created_at_ns >= lease.created_at_ns)
                })
                || receipt.guard.workspace_id != workspace_id
                || receipt.guard.lease_id != origin.original_lease
                || receipt.mount_uid != origin.mount_uid
                || receipt.pod_uid != origin.pod_uid
                || receipt.guard.expected_head_layer_id != record.guard.expected_head_layer_id
                || receipt.guard.expected_head_epoch != record.guard.expected_head_epoch
                || first_admin.lease.lease_id != record.guard.lease_id
                || first_admin.lease.holder_generation != record.guard.holder_generation
                || first_admin.open_owner.owner_id != origin.owner_id()
                || receipt.base_revision != record.expected_binding.base_revision
                || receipt.binding_version != record.expected_binding.binding.binding_version
                || receipt.manifest_digest != record.expected_binding.binding.manifest.digest
                || original_lease.lease_id != origin.original_lease
                || original_lease.workspace_id != workspace_id
                || original_lease.holder_generation != receipt.guard.holder_generation
                || original_lease.base_revision != receipt.base_revision
                || original_lease.state != LeaseState::Released
                || !original_lease.writable
            {
                return Err(WorkspaceError::Fenced.into());
            }
            let mut checks = keys
                .into_iter()
                .zip(values)
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>();
            if lease.state != LeaseState::Active
                || lease.expires_at_ns <= now
                || open.expires_at_ns <= now
            {
                if ownership_attempted {
                    return Err(WorkspaceError::Fenced.into());
                }
                ownership_attempted = true;
                let mut successor_open = open.clone();
                if open.expires_at_ns <= now {
                    successor_open.generation = open
                        .generation
                        .checked_add(1)
                        .ok_or(WorkspaceError::Fenced)?;
                }
                successor_open.expires_at_ns = checked_expiry(now, request.lease_ttl_ns)?;
                let mut writes = vec![put(open_snapshot_key(workspace_id), &successor_open)?];
                let mut next_record = record.clone();
                let not_before = if lease.state != LeaseState::Active || lease.expires_at_ns <= now
                {
                    if request.lease_id == lease.lease_id
                        || control.leases.contains_key(&request.lease_id)
                    {
                        return Err(WorkspaceError::Fenced.into());
                    }
                    let key = hot_lease_key(workspace_id, request.lease_id);
                    let index_key = hot_lease_index_key(request.lease_id);
                    let (values, _) = self
                        .backend
                        .get_many_consistent_with_time_bounded(
                            &[key.clone(), index_key.clone()],
                            source_limits(2),
                        )
                        .await?;
                    if values.len() != 2 || values.iter().any(Option::is_some) {
                        return Err(WorkspaceError::Fenced.into());
                    }
                    checks.push(KvCheck {
                        key,
                        expected: None,
                    });
                    checks.push(KvCheck {
                        key: index_key.clone(),
                        expected: None,
                    });
                    let mut predecessor = lease.clone();
                    predecessor.state = LeaseState::Expired;
                    predecessor.updated_at_ns = now;
                    let successor = SnapshotLease {
                        lease_id: request.lease_id,
                        workspace_id,
                        base_revision: carrier.clone(),
                        holder_generation: lease
                            .holder_generation
                            .checked_add(1)
                            .ok_or(WorkspaceError::Fenced)?,
                        writable: true,
                        state: LeaseState::Active,
                        expires_at_ns: successor_open.expires_at_ns,
                        created_at_ns: now,
                        updated_at_ns: now,
                    };
                    let mut next_workspace = workspace.clone();
                    next_workspace.active_lease = Some(successor.lease_id);
                    next_workspace.updated_at_ns = now;
                    let source_guard = HeadGuard {
                        lease_id: successor.lease_id,
                        holder_generation: successor.holder_generation,
                        ..final_source_guard.clone()
                    };
                    next_record = record.with_final_completion_guard(source_guard)?;
                    writes.extend([
                        put(
                            hot_lease_key(workspace_id, predecessor.lease_id),
                            &predecessor,
                        )?,
                        put(hot_lease_key(workspace_id, successor.lease_id), &successor)?,
                        put(index_key, &workspace_id)?,
                        put(hot_workspace_key(workspace_id), &next_workspace)?,
                        KvWrite::Put {
                            key: format!("packed/journal/{}", record.journal_id).into_bytes(),
                            value: next_record.encode()?,
                        },
                    ]);
                    Some(lease.expires_at_ns)
                } else {
                    None
                };
                let _writer_owner = self.prepare_administrative_packed_writer(
                    workspace_id, crate::workspace::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::Update, &mut checks, &mut writes,
                ).await?;
                let _native_holds = self
                    .prepare_native_owner_cas(&mut checks, &mut writes)
                    .await?;
                let deadline = if lease.state == LeaseState::Active && lease.expires_at_ns > now {
                    lease.expires_at_ns.min(successor_open.expires_at_ns)
                } else {
                    successor_open.expires_at_ns
                };
                self.clean_completion_ownership_cas(&checks, &writes, not_before, deadline)
                    .await?;
                record = next_record;
                continue;
            }
            let _writer_owner = self
                .authenticate_administrative_packed_writer(workspace_id, &mut checks)
                .await?;
            let deadline = open.expires_at_ns.min(lease.expires_at_ns);
            if !self
                .backend
                .compare_and_swap_before(&checks, &[], deadline)
                .await?
            {
                return Err(WorkspaceError::Fenced.into());
            }
            let claim = Arc::new(PackedCleanPublicationAuthority {
                store: self.clone(),
                original: PackedReleasedMountReference {
                    guard: receipt.guard.to_head_guard(),
                    mount_uid: receipt.mount_uid,
                    pod_uid: receipt.pod_uid,
                },
                immutable: [12usize, 13, 1, 5]
                    .into_iter()
                    .map(|index| checks[index].clone())
                    .collect(),
                open,
                guard,
                budget: budget.clone(),
                mode: CleanAuthorityMode::Committed,
                _owner: owner,
            });
            let result = PackedSnapshotResult {
                snapshot_id: request.snapshot_id,
                packed_carrier_revision: carrier,
                native_sealed_source_revision: BaseRevision {
                    layer_id: record.guard.expected_head_layer_id,
                    sealed_version: source_version,
                    root_hash: source_root_hash,
                },
                binding: target,
            };
            break (claim, result);
        };
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
        let snapshot = AuthenticatedSnapshot::open(&client, &result.binding.binding.manifest)
            .await
            .map_err(|error| {
                runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?;
        let lower = Arc::new(
            PackedReadonlyMeta::from_snapshot_budget(
                client,
                snapshot,
                layout.chunk_size,
                0,
                budget.clone(),
            )
            .map_err(|error| {
                runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?,
        );
        let guard = claim.guard();
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
        let provider: Arc<dyn WorkspaceReadPlanProvider> = metadata.clone();
        let vfs = VFS::from_readonly_components_with_provider(
            VFSConfig::new(layout),
            upper,
            metadata.clone(),
            provider,
        )
        .map_err(|error| {
            runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
        })?;
        let local = vfs.quiesce_packed_vfs().await.map_err(|error| {
            runtime_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
        })?;
        drop(local);
        drop(vfs);
        // A real current readonly runtime is retired before admin Released.
        // No source builder, graph token or native final commit is replayed.
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

    /// Submit one exact completion-owner mutation. Expiry takeover is guarded
    /// by the actual locked backend clock. Unknown replies permit only the
    /// complete exact successor plus a read-only confirmation in that window.
    async fn clean_completion_ownership_cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        not_before: Option<i64>,
        deadline: i64,
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
        let bounded = |packet: &[KvCheck]| {
            packet.iter().try_fold(0usize, |sum, row| {
                sum.checked_add(row.key.len())
                    .and_then(|sum| sum.checked_add(row.expected.as_ref().map_or(0, Vec::len)))
            })
        };
        if checks.len() > 32
            || bounded(checks).is_none_or(|bytes| bytes > SOURCE_MAX_BYTES)
            || bounded(&successor).is_none_or(|bytes| bytes > SOURCE_MAX_BYTES)
        {
            return Err(WorkspaceError::Fenced);
        }
        match self
            .backend
            .compare_and_swap_in_time_window(checks, writes, not_before, Some(deadline))
            .await
        {
            Ok(true) => Ok(()),
            Ok(false) => Err(WorkspaceError::Fenced),
            Err(original) => {
                let keys = successor
                    .iter()
                    .map(|check| check.key.clone())
                    .collect::<Vec<_>>();
                match self
                    .backend
                    .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
                    .await
                {
                    Ok((values, now))
                        if values.len() == successor.len()
                            && now > 0
                            && now < deadline
                            && not_before.is_none_or(|lower| now >= lower)
                            && values
                                .iter()
                                .zip(&successor)
                                .all(|(value, check)| value == &check.expected) =>
                    {
                        match self
                            .backend
                            .compare_and_swap_in_time_window(
                                &successor,
                                &[],
                                not_before,
                                Some(deadline),
                            )
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
}

#[cfg(test)]
mod operation_identity_tests {
    use super::*;

    fn fixture() -> (
        PackedReleasedMountReference,
        PackedHeadlessSnapshotRequest,
        ChunkLayout,
    ) {
        let reference = PackedReleasedMountReference {
            guard: HeadGuard {
                workspace_id: WorkspaceId::new(),
                expected_head_layer_id: LayerId::new(),
                expected_head_epoch: 3,
                lease_id: LeaseId::new(),
                holder_generation: 7,
            },
            mount_uid: uuid::Uuid::new_v4(),
            pod_uid: uuid::Uuid::new_v4(),
        };
        let request = PackedHeadlessSnapshotRequest::bounded_operator(
            PackedHeadlessSnapshotDescription {
                snapshot_id: SnapshotId::new(),
                snapshot_name: "operation-identity-regression".into(),
                owner_id: None,
            },
            LeaseId::new(),
            JournalId::new(),
            LayerId::new(),
            300_000_000_000,
            std::path::PathBuf::from("unused-operation-identity-scratch"),
        );
        (
            reference,
            request,
            ChunkLayout {
                chunk_size: 4096,
                block_size: 4096,
            },
        )
    }

    #[test]
    fn original_and_recovered_operation_identity_roundtrip_real_request() {
        let (reference, mut request, layout) = fixture();
        for kind in [CleanSourceKind::OriginalPcr, CleanSourceKind::RecoveredPmr] {
            let owner = clean_operation_owner(&reference, kind, &request, layout).unwrap();
            let prefix = match kind {
                CleanSourceKind::OriginalPcr => OWNER_PREFIX,
                CleanSourceKind::RecoveredPmr => RECOVERED_OWNER_PREFIX,
            };
            assert_eq!(owner.len(), prefix.len() + 202);
            assert!(owner.len() <= OPEN_OWNER_MAX_BYTES);
            let parsed = PackedCleanOperationOrigin::parse(&owner).unwrap().unwrap();
            assert_eq!(parsed.owner_id(), owner);
            assert_eq!(parsed.source_kind(), kind);
            assert!(parsed.matches_source_reference(&reference));
            parsed.matches_request(&request, layout).unwrap();
            let mut foreign_reference = reference.clone();
            foreign_reference.guard.lease_id = LeaseId::new();
            assert!(!parsed.matches_source_reference(&foreign_reference));
            request.producer.build_policy.inline_data = !request.producer.build_policy.inline_data;
            assert!(matches!(
                parsed.matches_request(&request, layout),
                Err(WorkspaceError::Fenced)
            ));
            request.producer.build_policy.inline_data = !request.producer.build_policy.inline_data;
        }
    }

    #[test]
    fn operation_identity_rejects_corrupt_fields_and_noncanonical_length() {
        let (reference, request, layout) = fixture();
        for kind in [CleanSourceKind::OriginalPcr, CleanSourceKind::RecoveredPmr] {
            let owner = clean_operation_owner(&reference, kind, &request, layout).unwrap();
            assert!(matches!(
                PackedCleanOperationOrigin::parse(&owner[..owner.len() - 1]),
                Err(WorkspaceError::Fenced)
            ));
            assert!(matches!(
                PackedCleanOperationOrigin::parse(&format!("{owner}/")),
                Err(WorkspaceError::Fenced)
            ));
            let prefix = match kind {
                CleanSourceKind::OriginalPcr => OWNER_PREFIX,
                CleanSourceKind::RecoveredPmr => RECOVERED_OWNER_PREFIX,
            };
            let mut fields = owner[prefix.len()..]
                .split('/')
                .map(str::to_owned)
                .collect::<Vec<_>>();
            fields[0] = compact_uuid(uuid::Uuid::nil());
            assert!(matches!(
                PackedCleanOperationOrigin::parse(&format!("{prefix}{}", fields.join("/"))),
                Err(WorkspaceError::Fenced)
            ));
            fields[0] = compact_uuid(*reference.guard.lease_id.as_uuid());
            fields[6].replace_range(..1, "g");
            assert!(matches!(
                PackedCleanOperationOrigin::parse(&format!("{prefix}{}", fields.join("/"))),
                Err(WorkspaceError::Fenced)
            ));
            fields[6] = "0".repeat(64);
            fields[0].replace_range(..1, "?");
            assert!(matches!(
                PackedCleanOperationOrigin::parse(&format!("{prefix}{}", fields.join("/"))),
                Err(WorkspaceError::Fenced)
            ));
        }
        assert!(
            PackedCleanOperationOrigin::parse("unrelated-owner")
                .unwrap()
                .is_none()
        );
    }
}
