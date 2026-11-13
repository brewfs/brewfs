//! Durable original Prepare/Q facts, including before a packed journal exists.
//! Missing seeds fail closed; current lease identity never substitutes for origin.

use super::super::packed_journal::{PackedJournalRecord, PackedNativeRecoveryBasisReceipt};
use super::recovery_claim::{NativeRecoveryClaimRecord as NativeSeedClaim, claim_key};
use super::*;
use uuid::Uuid;

const SEED_MAGIC: &[u8; 4] = b"NQB3";
const SEED_MAX_BYTES: usize = 16 << 10;
const SEED_FIELD_MAX_BYTES: usize = 4 << 10;
const SEED_CANONICAL_MAX_BYTES: usize = 8 << 10;
const SEED_CLAIM_MAX_BYTES: usize = 4096;
const SEED_OPERATION_BYTES: u64 = 16 << 20;
const SEED_READ_BYTES: u64 = 2 << 20;
const SEED_MAX_TTL_NS: u64 = 15 * 60 * 1_000_000_000;

/// This private record is installed only in the actual Prepare transaction.
/// Q is appended in the same transaction as the actual original Q journal.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct NativeFreezeSeed {
    schema_version: u32,
    workspace_id: WorkspaceId,
    old_head_layer_id: LayerId,
    old_head_epoch: u64,
    original_lease_id: LeaseId,
    original_holder_generation: u64,
    old_layers: [LayerRecord; 2],
    journal_id: JournalId,
    planned_head_layer_id: LayerId,
    planned_head_epoch: u64,
    binding_bytes: Vec<u8>,
    prepare_journal: SealJournal,
    quiesced_journal: Option<SealJournal>,
    canonical_quiesce: Vec<u8>,
    packed_journal_id: Option<JournalId>,
}

pub(super) fn seed_key(journal_id: JournalId) -> Vec<u8> {
    format!("packed/native-freeze-basis/{journal_id}").into_bytes()
}

fn seed_claim_key(journal_id: JournalId) -> Vec<u8> {
    claim_key(journal_id)
}

impl NativeFreezeSeed {
    fn same_original_source(&self, other: &Self) -> bool {
        let mut left = self.clone();
        let mut right = other.clone();
        left.packed_journal_id = None;
        right.packed_journal_id = None;
        left == right
    }

    pub(super) fn prepared(
        mapping: &PackedNativePlannedRotation,
        binding: &PackedLowerBindingRecord,
        journal: &SealJournal,
    ) -> Result<Self, WorkspaceError> {
        let guard = &mapping.old_guard;
        let record = Self {
            schema_version: 3,
            workspace_id: guard.workspace_id,
            old_head_layer_id: guard.expected_head_layer_id,
            old_head_epoch: guard.expected_head_epoch,
            original_lease_id: guard.lease_id,
            original_holder_generation: guard.holder_generation,
            old_layers: mapping.old_layers.clone(),
            journal_id: mapping.journal_id,
            planned_head_layer_id: mapping.planned_head_layer_id,
            planned_head_epoch: mapping.planned_head_epoch,
            binding_bytes: binding.encode()?,
            prepare_journal: journal.clone(),
            quiesced_journal: None,
            canonical_quiesce: Vec::new(),
            packed_journal_id: None,
        };
        record.validate()?;
        Ok(record)
    }

    fn mapping(&self) -> PackedNativePlannedRotation {
        PackedNativePlannedRotation {
            old_guard: HeadGuard {
                workspace_id: self.workspace_id,
                expected_head_layer_id: self.old_head_layer_id,
                expected_head_epoch: self.old_head_epoch,
                lease_id: self.original_lease_id,
                holder_generation: self.original_holder_generation,
            },
            old_layers: self.old_layers.clone(),
            journal_id: self.journal_id,
            planned_head_layer_id: self.planned_head_layer_id,
            planned_head_epoch: self.planned_head_epoch,
        }
    }

    fn binding(&self) -> Result<PackedLowerBindingRecord, WorkspaceError> {
        PackedLowerBindingRecord::decode(&self.binding_bytes)
    }

    fn frozen_head(&self) -> LayerRecord {
        let mut head = self.old_layers[0].clone();
        head.state = LayerState::Sealing;
        head
    }

    fn validate(&self) -> Result<(), WorkspaceError> {
        let mapping = self.mapping();
        if self.schema_version != 3
            || self.workspace_id.as_uuid().is_nil()
            || self.journal_id.as_uuid().is_nil()
            || self.original_lease_id.as_uuid().is_nil()
            || self.original_holder_generation == 0
            || self.old_head_layer_id != self.old_layers[0].layer_id
            || self.old_layers[0].owner_workspace_id != Some(self.workspace_id)
            || self.planned_head_layer_id.as_uuid().is_nil()
            || self
                .old_layers
                .iter()
                .any(|row| row.layer_id == self.planned_head_layer_id)
            || self.planned_head_epoch
                != self
                    .old_head_epoch
                    .checked_add(1)
                    .ok_or(WorkspaceError::Fenced)?
            || self.binding_bytes.len() > SEED_FIELD_MAX_BYTES
            || self.canonical_quiesce.len() > SEED_CANONICAL_MAX_BYTES
        {
            return Err(WorkspaceError::Fenced);
        }
        validate_permission_layers(&self.old_layers)?;
        for layer in &self.old_layers {
            if encode(layer)?.len() > SEED_FIELD_MAX_BYTES {
                return Err(native_freeze_error("seed layer exceeds schema cap"));
            }
        }
        self.binding()?
            .validate_for_guard(&mapping.old_guard, &self.old_layers[1])?;
        if self
            .packed_journal_id
            .is_some_and(|id| id.as_uuid().is_nil())
            || (self.packed_journal_id.is_some() && self.quiesced_journal.is_none())
        {
            return Err(WorkspaceError::Fenced);
        }
        let prepare = &self.prepare_journal;
        if prepare.journal_id != self.journal_id
            || prepare.workspace_id != self.workspace_id
            || prepare.old_head_layer_id != self.old_head_layer_id
            || prepare.expected_head_epoch != self.old_head_epoch
            || prepare.new_head_layer_id != Some(self.planned_head_layer_id)
            || prepare.phase != SealPhase::Prepare
            || prepare.pending_bytes != 0
            || prepare.delta_digest.is_some()
            || prepare.root_hash.is_some()
            || prepare.last_error.is_some()
            || prepare.created_at_ns <= 0
            || prepare.updated_at_ns != prepare.created_at_ns
            || encode(prepare)?.len() > SEED_FIELD_MAX_BYTES
        {
            return Err(WorkspaceError::Fenced);
        }
        match &self.quiesced_journal {
            None if self.canonical_quiesce.is_empty() => {}
            Some(journal) => {
                let mut expected = prepare.clone();
                expected.phase = SealPhase::Quiesced;
                expected.updated_at_ns = journal.updated_at_ns;
                if journal != &expected
                    || journal.updated_at_ns < prepare.updated_at_ns
                    || encode(journal)?.len() > SEED_FIELD_MAX_BYTES
                    || self.canonical_quiesce
                        != canonical_quiesce(
                            &mapping,
                            &self.binding()?,
                            &self.frozen_head(),
                            journal,
                        )?
                {
                    return Err(WorkspaceError::Fenced);
                }
            }
            _ => return Err(WorkspaceError::Fenced),
        }
        Ok(())
    }

    pub(super) fn validate_prepared_context(
        &self,
        mapping: &PackedNativePlannedRotation,
        binding: &PackedLowerBindingRecord,
        journal: &SealJournal,
    ) -> Result<(), WorkspaceError> {
        self.validate()?;
        if &self.mapping() != mapping
            || &self.binding()? != binding
            || &self.prepare_journal != journal
            || self.quiesced_journal.is_some()
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    pub(super) fn with_quiesced(&self, journal: &SealJournal) -> Result<Self, WorkspaceError> {
        if self.quiesced_journal.is_some() {
            return Err(WorkspaceError::Fenced);
        }
        let mut next = self.clone();
        next.quiesced_journal = Some(journal.clone());
        next.canonical_quiesce = canonical_quiesce(
            &next.mapping(),
            &next.binding()?,
            &next.frozen_head(),
            journal,
        )?;
        next.validate()?;
        Ok(next)
    }

    pub(super) fn origin_digest(&self) -> Result<[u8; 32], WorkspaceError> {
        let mut origin = self.clone();
        origin.quiesced_journal = None;
        origin.canonical_quiesce.clear();
        origin.packed_journal_id = None;
        Ok(Sha256::digest(encode_seed(&origin)?).into())
    }

    pub(super) fn validate_packed_basis<B: WorkspaceKvBackend>(
        &self,
        basis: &PackedNativeRecoveryBasisReceipt<B>,
    ) -> Result<(), WorkspaceError> {
        self.validate()?;
        if &self.mapping().old_guard != basis.old_guard()
            || &self.old_layers != basis.old_layers()
            || self.binding()? != *basis.binding()
            || self.journal_id != basis.native_journal_id()
            || self.packed_journal_id != Some(basis.journal_id())
            || self.planned_head_layer_id != basis.planned_head_layer_id()
            || self.planned_head_epoch != basis.planned_head_epoch()
            || self.quiesced_journal.as_ref() != Some(basis.original_quiesced_native_journal())
            || self.canonical_quiesce != basis.quiesce_receipt_bytes()
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    pub(super) fn validate_owner_basis<B: WorkspaceKvBackend>(
        &self,
        owner: &NativeSeedClaim,
        basis: &PackedNativeRecoveryBasisReceipt<B>,
    ) -> Result<(), WorkspaceError> {
        self.validate_packed_basis(basis)?;
        owner.validate_seed(self)?;
        if owner.journal_id != basis.journal_id()
            || owner.staging_id != basis.staging_incarnation()
            || owner.quiesce_digest != basis.quiesce_receipt_digest()
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
}

pub(super) fn encode_seed(seed: &NativeFreezeSeed) -> Result<Vec<u8>, WorkspaceError> {
    seed.validate()?;
    let payload = encode(seed)?;
    if payload
        .len()
        .checked_add(SEED_MAGIC.len())
        .is_none_or(|length| length > SEED_MAX_BYTES)
    {
        return Err(native_freeze_error("NQB3 record exceeds fixed schema cap"));
    }
    let mut bytes = Vec::with_capacity(SEED_MAGIC.len() + payload.len());
    bytes.extend_from_slice(SEED_MAGIC);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub(super) fn decode_seed(raw: &[u8]) -> Result<NativeFreezeSeed, WorkspaceError> {
    if raw.len() > SEED_MAX_BYTES || !raw.starts_with(SEED_MAGIC) {
        return Err(native_freeze_error("invalid NQB3 schema/size"));
    }
    let seed: NativeFreezeSeed = decode_open_value(&raw[SEED_MAGIC.len()..], SEED_MAX_BYTES)?;
    seed.validate()?;
    if encode_seed(&seed)? != raw {
        return Err(native_freeze_error("noncanonical NQB3 bytes"));
    }
    Ok(seed)
}

impl NativeSeedClaim {
    fn validate_seed(&self, seed: &NativeFreezeSeed) -> Result<(), WorkspaceError> {
        let expected_quiesce = seed
            .quiesced_journal
            .as_ref()
            .map_or([0; 32], |_| Sha256::digest(&seed.canonical_quiesce).into());
        if self.native_journal_id != seed.journal_id
            || self.workspace_id != seed.workspace_id
            || self.seed_origin_digest != seed.origin_digest()?
            || self.quiesce_digest != expected_quiesce
            || self.journal_id.as_uuid().is_nil() != self.staging_id.is_nil()
            || (seed.quiesced_journal.is_none() && !self.journal_id.as_uuid().is_nil())
            || self.lease_id.as_uuid().is_nil()
            || self.previous_lease_id.as_uuid().is_nil()
            || self.holder_generation < seed.original_holder_generation
            || (self.holder_generation == seed.original_holder_generation
                && (self.lease_id != seed.original_lease_id
                    || self.previous_lease_id != self.lease_id))
            || (self.holder_generation > seed.original_holder_generation
                && self.lease_id == self.previous_lease_id)
            || self.owner_id.trim().is_empty()
            || self.owner_id.len() > OPEN_OWNER_MAX_BYTES
            || self.open_generation == 0
            || self.created_at_ns <= 0
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    fn same_source_owner(&self, other: &Self) -> bool {
        self.native_journal_id == other.native_journal_id
            && self.workspace_id == other.workspace_id
            && self.seed_origin_digest == other.seed_origin_digest
            && self.quiesce_digest == other.quiesce_digest
            && self.lease_id == other.lease_id
            && self.holder_generation == other.holder_generation
            && self.previous_lease_id == other.previous_lease_id
            && self.owner_id == other.owner_id
            && self.open_generation == other.open_generation
            && self.created_at_ns == other.created_at_ns
            && ((self.journal_id == other.journal_id && self.staging_id == other.staging_id)
                || (other.journal_id.as_uuid().is_nil()
                    && other.staging_id.is_nil()
                    && !self.journal_id.as_uuid().is_nil()
                    && !self.staging_id.is_nil()))
    }
}

pub(crate) struct NativePrepareRecoveryRequest {
    pub(crate) journal_id: JournalId,
    pub(crate) owner_id: String,
    /// None requires the actual original/current lease to remain active.
    /// Some requests a new incarnation; a live previous lease refuses it.
    pub(crate) new_lease_id: Option<LeaseId>,
    pub(crate) ttl_ns: u64,
}

/// In-process source authority, never a drain/hash/effective-view proof.
/// Root must retain this opaque token in the real quiesce fence and merge its
/// fresh checks/deadline in source, phase and first packed-journal binding CAS.
pub(super) struct NativePrepareRecoveryAuthority<B> {
    store: Arc<KvWorkspaceStore<B>>,
    seed: NativeFreezeSeed,
    claim: Option<NativeSeedClaim>,
    guard: HeadGuard,
    owner_id: String,
    open_generation: u64,
    budget: Arc<SnapshotMountBudget>,
    _permit: SnapshotOwnedPermit,
}

/// Opaque result issued only after actual Prepare->Q or exact current-Q CAS.
/// Integration must preserve original mapping/canonical and the current guard.
pub(super) struct NativePrepareRecoveryResult<B> {
    pub(super) mapping: PackedNativePlannedRotation,
    pub(super) binding: PackedLowerBindingRecord,
    pub(super) frozen_head: LayerRecord,
    pub(super) journal: SealJournal,
    pub(super) canonical: Vec<u8>,
    pub(super) authority: Arc<NativePrepareRecoveryAuthority<B>>,
}

/// Keep this owner through the packed journal's actual terminal CAS. The
/// checks/writes atomically bind the current source owner to that incarnation.
pub(crate) struct NativeJournalOwnerHandoff {
    pub(crate) checks: Vec<KvCheck>,
    pub(crate) writes: Vec<KvWrite>,
    pub(crate) deadline_ns: i64,
    _permit: SnapshotOwnedPermit,
}

struct SeedRead {
    keys: Vec<Vec<u8>>,
    values: Vec<Option<Vec<u8>>>,
    seed: NativeFreezeSeed,
    claim: Option<NativeSeedClaim>,
    workspace: WorkspaceRecord,
    lease: SnapshotLease,
    journal: SealJournal,
    open: SnapshotOpenRecord,
    now: i64,
    packed_journal_checks: Vec<KvCheck>,
    _packed_journal_owner: Option<SnapshotOwnedPermit>,
}

fn seed_limits(records: usize) -> KvReadLimits {
    KvReadLimits {
        max_records: records,
        max_key_bytes: 1024,
        max_value_bytes: FREEZE_POINT_MAX_BYTES,
        max_total_bytes: FREEZE_POINT_MAX_BYTES,
        max_response_bytes: 64 << 10,
        // Extra slots permit only certified same-TS Get continuation. Exact
        // successor confirmation still cannot authorize another mutation.
        max_data_requests: records.saturating_add(2).min(32),
    }
}

fn exact_seed_checks(read: &SeedRead) -> Vec<KvCheck> {
    read.keys
        .iter()
        .cloned()
        .zip(read.values.iter().cloned())
        .map(|(key, expected)| KvCheck { key, expected })
        .chain(read.packed_journal_checks.iter().cloned())
        .collect()
}

fn validate_successor_bounds(
    keys: &[Vec<u8>],
    values: &[Option<Vec<u8>>],
) -> Result<(), WorkspaceError> {
    if keys.len() != values.len() {
        return Err(native_freeze_error("seed response count"));
    }
    let bytes = keys
        .iter()
        .zip(values)
        .try_fold(0usize, |sum, (key, value)| {
            let length = value.as_ref().map_or(0, Vec::len);
            if key.len() > 1024 || length > FREEZE_POINT_MAX_BYTES {
                return Err(native_freeze_error("seed fixed point bound"));
            }
            sum.checked_add(key.len())
                .and_then(|sum| sum.checked_add(length))
                .ok_or_else(|| native_freeze_error("seed aggregate overflow"))
        })?;
    if bytes > FREEZE_POINT_MAX_BYTES {
        return Err(native_freeze_error("seed aggregate exceeds fixed tier"));
    }
    Ok(())
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    async fn read_native_seed(
        &self,
        journal_id: JournalId,
        owner_id: &str,
        proposed_lease: Option<LeaseId>,
    ) -> Result<SeedRead, WorkspaceError> {
        let routing_keys = [seed_key(journal_id), seed_claim_key(journal_id)];
        let (routing, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&routing_keys, seed_limits(2))
            .await?;
        validate_successor_bounds(&routing_keys, &routing)?;
        let seed = decode_seed(routing[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if seed.journal_id != journal_id {
            return Err(WorkspaceError::Fenced);
        }
        let claim = routing[1]
            .as_deref()
            .map(|raw| decode_open_value::<NativeSeedClaim>(raw, SEED_CLAIM_MAX_BYTES))
            .transpose()?;
        if let Some(claim) = &claim {
            claim.validate_seed(&seed)?;
        }
        let mapping = seed.mapping();
        let binding = seed.binding()?;
        let lease_id = claim
            .as_ref()
            .map_or(seed.original_lease_id, |row| row.lease_id);
        let mut keys = vec![
            hot_journal_key(seed.workspace_id, journal_id),
            hot_workspace_key(seed.workspace_id),
            hot_layer_key(seed.old_head_layer_id),
            hot_layer_key(seed.old_layers[1].layer_id),
            hot_lease_key(seed.workspace_id, lease_id),
            hot_layer_key(seed.planned_head_layer_id),
            packed_current_key(seed.workspace_id),
            packed_claim_key(seed.workspace_id),
            packed_history_key(seed.workspace_id, binding.binding.binding_version),
            packed_history_key(seed.workspace_id, 1),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            open_snapshot_recovery_key(seed.workspace_id),
            routing_keys[0].clone(),
            routing_keys[1].clone(),
            open_snapshot_key(seed.workspace_id),
            hot_lease_index_key(lease_id),
            hot_journal_index_key(journal_id),
        ];
        if let Some(proposed) = proposed_lease.filter(|proposed| *proposed != lease_id) {
            keys.push(hot_lease_key(seed.workspace_id, proposed));
            keys.push(hot_lease_index_key(proposed));
        }
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, seed_limits(keys.len()))
            .await?;
        validate_successor_bounds(&keys, &values)?;
        if values[13] != routing[0] || values[14] != routing[1] {
            return Err(WorkspaceError::Busy);
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        let journal: SealJournal = decode_open_value(required(0)?, FREEZE_POINT_MAX_BYTES)?;
        let workspace: WorkspaceRecord = decode_open_value(required(1)?, FREEZE_POINT_MAX_BYTES)?;
        let head: LayerRecord = decode_open_value(required(2)?, FREEZE_POINT_MAX_BYTES)?;
        let base: LayerRecord = decode_open_value(required(3)?, FREEZE_POINT_MAX_BYTES)?;
        let lease: SnapshotLease = decode_open_value(required(4)?, FREEZE_POINT_MAX_BYTES)?;
        let current = decode_packed_pair(seed.workspace_id, &values[6], &values[7], &values[8])?
            .ok_or(WorkspaceError::Fenced)?;
        let anchor = PackedLowerBindingRecord::decode(required(9)?)?;
        let recovery: SnapshotRecoveryRecord =
            decode_open_value(required(12)?, OPEN_RECOVERY_MAX_BYTES)?;
        let open: SnapshotOpenRecord = decode_open_value(required(15)?, OPEN_RECORD_MAX_BYTES)?;
        let lease_workspace: WorkspaceId = decode_open_value(required(16)?, OPEN_RECORD_MAX_BYTES)?;
        let journal_workspace: WorkspaceId =
            decode_open_value(required(17)?, OPEN_RECORD_MAX_BYTES)?;
        let generation = claim
            .as_ref()
            .map_or(seed.original_holder_generation, |row| row.holder_generation);
        if now <= 0
            || workspace.workspace_id != seed.workspace_id
            || workspace.state != WorkspaceState::Sealing
            || workspace.active_lease.is_some_and(|id| id != lease_id)
            || (lease.state == LeaseState::Active
                && lease.expires_at_ns > now
                && workspace.active_lease != Some(lease_id))
            || lease_workspace != seed.workspace_id
            || journal_workspace != seed.workspace_id
            || workspace.head_layer_id != seed.old_head_layer_id
            || workspace.head_epoch != seed.old_head_epoch
            || head != seed.frozen_head()
            || base != seed.old_layers[1]
            || lease.lease_id != lease_id
            || lease.workspace_id != seed.workspace_id
            || lease.holder_generation != generation
            || lease.base_revision != binding.base_revision
            || !lease.writable
            || lease.created_at_ns <= 0
            || lease.updated_at_ns > now
            || values[5].is_some()
            || current != binding
            || anchor.workspace_id != seed.workspace_id
            || anchor.binding.binding_version != 1
            || recovery.workspace_id != seed.workspace_id
            || !recovery.incomplete
            || open.workspace_id != seed.workspace_id
            || open.owner_id != owner_id
            || open.generation == 0
            || open.expires_at_ns <= now
            || open.state != SnapshotOpenState::Recovering
            || !open.recovery_required
            || journal.updated_at_ns > now
            || journal.journal_id != journal_id
            || journal.workspace_id != seed.workspace_id
        {
            return Err(WorkspaceError::Fenced);
        }
        binding.validate_for_guard(&mapping.old_guard, &base)?;
        match &seed.quiesced_journal {
            None if journal == seed.prepare_journal => {}
            Some(original) => super::native_recovery::validate_recovery_phase(original, &journal)?,
            _ => return Err(WorkspaceError::Fenced),
        }
        if journal.phase == SealPhase::Hashed
            && !journal
                .delta_digest
                .zip(journal.root_hash)
                .is_some_and(|(digest, root)| {
                    base.root_hash
                        .is_some_and(|parent| root_hash(parent, digest) == root)
                })
        {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(claim) = &claim
            && (claim.created_at_ns > now || claim.created_at_ns != lease.created_at_ns)
        {
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[10])?;
        layer_inventory_generation(&values[11])?;
        Ok(SeedRead {
            keys,
            values,
            seed,
            claim,
            workspace,
            lease,
            journal,
            open,
            now,
            packed_journal_checks: Vec::new(),
            _packed_journal_owner: None,
        })
    }

    async fn commit_native_seed(
        &self,
        read: &SeedRead,
        writes: &[KvWrite],
        not_before_ns: Option<i64>,
        deadline_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        let mut checks = exact_seed_checks(read);
        let mut writes = writes.to_vec();
        // Recovery expires the actual old lease and births its successor.
        // Their native hold projections and owner epoch join this same CAS.
        let _writer_owner = self.prepare_administrative_packed_writer(
            read.seed.workspace_id, crate::workspace::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::Update, &mut checks, &mut writes,
        ).await?;
        let _native_holds = self
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await?;
        let packet = self
            .prepare_topology_envelope(checks, writes, Some(deadline_ns))
            .await?;
        let checks = packet.checks.clone();
        let writes = packet.writes.clone();
        let keys = checks
            .iter()
            .map(|check| check.key.clone())
            .collect::<Vec<_>>();
        let mut expected = checks
            .iter()
            .map(|check| check.expected.clone())
            .collect::<Vec<_>>();
        for write in &writes {
            let (key, value) = match write {
                KvWrite::Put { key, value } => (key, Some(value.clone())),
                KvWrite::Delete { key } => (key, None),
            };
            let index = keys
                .iter()
                .position(|checked| checked == key)
                .ok_or_else(|| native_freeze_error("seed write lacks exact predecessor"))?;
            expected[index] = value;
        }
        validate_successor_bounds(&keys, &expected)?;
        match self
            .backend
            .compare_and_swap_in_time_window(&checks, &writes, not_before_ns, Some(deadline_ns))
            .await
        {
            Ok(committed) => Ok(committed),
            Err(error @ WorkspaceError::Backend(_)) => {
                // No second mutation after transport uncertainty. Confirm all
                // exact submitted successors and the same locked time window.
                let confirmation = async {
                    let (actual, now) = self
                        .backend
                        .get_many_consistent_with_time_bounded(&keys, seed_limits(keys.len()))
                        .await?;
                    validate_successor_bounds(&keys, &actual)?;
                    if actual != expected
                        || now <= 0
                        || now >= deadline_ns
                        || not_before_ns.is_some_and(|lower| now < lower)
                    {
                        return Ok(false);
                    }
                    let checks = keys
                        .iter()
                        .cloned()
                        .zip(actual)
                        .map(|(key, expected)| KvCheck { key, expected })
                        .collect::<Vec<_>>();
                    self.backend
                        .compare_and_swap_in_time_window(
                            &checks,
                            &[],
                            not_before_ns,
                            Some(deadline_ns),
                        )
                        .await
                }
                .await;
                match confirmation {
                    Ok(true) => Ok(true),
                    _ => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }
}

impl<B: WorkspaceKvBackend> NativePrepareRecoveryAuthority<B> {
    pub(super) fn source_guard(&self) -> &HeadGuard {
        &self.guard
    }
    pub(super) fn mount_budget(&self) -> &Arc<SnapshotMountBudget> {
        &self.budget
    }
    pub(super) fn belongs_to_store(&self, store: &Arc<KvWorkspaceStore<B>>) -> bool {
        Arc::ptr_eq(&self.store, store)
    }

    pub(super) fn matches_original(
        &self,
        mapping: &PackedNativePlannedRotation,
        binding: &PackedLowerBindingRecord,
        journal: &SealJournal,
        canonical: &[u8],
    ) -> Result<(), WorkspaceError> {
        if &self.seed.mapping() != mapping
            || &self.seed.binding()? != binding
            || self.seed.quiesced_journal.as_ref() != Some(journal)
            || self.seed.canonical_quiesce != canonical
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    pub(super) async fn authority_checks_before(
        &self,
    ) -> Result<(Vec<KvCheck>, i64), WorkspaceError> {
        let _owner = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, SEED_READ_BYTES)])
            .map_err(native_freeze_error)?;
        if self.budget.state().closed {
            return Err(WorkspaceError::Fenced);
        }
        let read = self
            .store
            .read_native_seed(self.seed.journal_id, &self.owner_id, None)
            .await?;
        if !read.seed.same_original_source(&self.seed)
            || match (&read.claim, &self.claim) {
                (Some(actual), Some(original)) => !actual.same_source_owner(original),
                (None, None) => false,
                (Some(actual), None) => {
                    actual.lease_id != self.guard.lease_id
                        || actual.holder_generation != self.guard.holder_generation
                        || actual.previous_lease_id != self.guard.lease_id
                        || actual.owner_id != self.owner_id
                        || actual.open_generation != self.open_generation
                }
                (None, Some(_)) => true,
            }
            || read.open.generation != self.open_generation
            || read.lease.lease_id != self.guard.lease_id
            || read.lease.holder_generation != self.guard.holder_generation
            || read.lease.state != LeaseState::Active
            || read.lease.expires_at_ns <= read.now
        {
            return Err(WorkspaceError::Fenced);
        }
        let deadline = read.lease.expires_at_ns.min(read.open.expires_at_ns);
        let mut checks = exact_seed_checks(&read);
        let _writer_owner = self
            .store
            .authenticate_administrative_packed_writer(self.guard.workspace_id, &mut checks)
            .await?;
        Ok((checks, deadline))
    }

    pub(super) async fn validate(&self) -> Result<(), WorkspaceError> {
        use super::super::native_read_conflict::{
            MAX_NATIVE_READ_ATTEMPTS, NATIVE_READ_REBUILD_BYTES, normalize,
        };
        let _owner = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, SEED_READ_BYTES)])
            .map_err(native_freeze_error)?;
        // This optional owner precedes both original and fresh checks, so they
        // are dropped before the independent retained-conflict admission.
        let mut _conflict_owner = None;
        let (checks, initial_deadline) = self.authority_checks_before().await?;
        let first_checks = normalize(checks)?;
        let mut current: Option<Vec<KvCheck>> = None;
        let mut deadline = initial_deadline;
        let root = |checks: &[KvCheck]| -> Result<u64, WorkspaceError> {
            let raw = checks
                .iter()
                .find(|check| check.key.as_slice() == PACKED_ROOT_GENERATION_KEY)
                .and_then(|check| check.expected.as_deref())
                .ok_or(WorkspaceError::Fenced)?;
            decode(raw)
        };
        for attempt in 0..MAX_NATIVE_READ_ATTEMPTS {
            if self.budget.state().closed {
                return Err(WorkspaceError::Fenced);
            }
            let checks = current.as_deref().unwrap_or(&first_checks);
            // A read-only unknown reply is propagated immediately. It cannot
            // select the definite-false proof/rebuild branch below.
            if self
                .store
                .backend
                .compare_and_swap_before(checks, &[], deadline)
                .await?
            {
                return Ok(());
            }
            if attempt + 1 == MAX_NATIVE_READ_ATTEMPTS {
                return Err(WorkspaceError::Busy);
            }
            if _conflict_owner.is_none() {
                _conflict_owner = Some(
                    self.budget
                        .admit(&[(SnapshotBudgetPool::Metadata, NATIVE_READ_REBUILD_BYTES)])
                        .map_err(native_freeze_error)?,
                );
            }
            // Re-read every seed/claim/CONTROL/head/base/binding/lease/open row
            // and repeat the typed original-identity validation. A read error
            // or a semantically fenced owner exits through ? without retry.
            let (fresh, fresh_deadline) = self.authority_checks_before().await?;
            let fresh = normalize(fresh)?;
            if fresh.len() != first_checks.len()
                || first_checks.iter().any(|first| {
                    !fresh.iter().any(|actual| {
                        actual.key == first.key
                            && (first.key.as_slice() == PACKED_ROOT_GENERATION_KEY
                                || actual.expected == first.expected)
                    })
                })
            {
                return Err(WorkspaceError::Busy);
            }
            let previous_root = root(checks)?;
            let fresh_root = root(&fresh)?;
            if fresh_root < previous_root {
                return Err(WorkspaceError::Fenced);
            }
            if fresh_root == previous_root {
                // Identical complete proof plus false CAS is not a proved
                // root conflict (the first deadline may already have elapsed).
                return Err(WorkspaceError::Busy);
            }
            deadline = deadline.min(fresh_deadline);
            if deadline <= 0 || self.budget.state().closed {
                return Err(WorkspaceError::Fenced);
            }
            current = Some(fresh);
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }
}

impl<B: WorkspaceKvBackend + 'static> NativePrepareRecoveryAuthority<B> {
    pub(super) async fn prepare_packed_handoff(
        self: &Arc<Self>,
        record: &PackedJournalRecord,
    ) -> Result<NativeJournalOwnerHandoff, WorkspaceError> {
        if self.budget.state().closed
            || record.journal_id.as_uuid().is_nil()
            || record.source.staging_id.is_nil()
            || record.guard != self.seed.mapping().old_guard
            || record.expected_binding != self.seed.binding()?
            || record.source.frozen_view_token
                != <[u8; 32]>::from(Sha256::digest(&self.seed.canonical_quiesce))
            || !record.source.snapshot_backed
        {
            return Err(WorkspaceError::Fenced);
        }
        let permit = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, SEED_READ_BYTES)])
            .map_err(native_freeze_error)?;
        let authority = self.clone();
        let journal_id = record.journal_id;
        let staging_id = record.source.staging_id;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = async {
                let read = authority
                    .store
                    .read_native_seed(authority.seed.journal_id, &authority.owner_id, None)
                    .await?;
                let mut current_checks = exact_seed_checks(&read);
                let _writer_owner = authority
                    .store
                    .authenticate_administrative_packed_writer(
                        authority.guard.workspace_id,
                        &mut current_checks,
                    )
                    .await?;
                let (checks, deadline_ns) = authority.authority_checks_before().await?;
                // A second bounded snapshot must describe the identical owner,
                // lease/open, seed and typed writer; mix no generation epochs.
                if checks.len() != current_checks.len()
                    || checks.iter().zip(&current_checks).any(|(left, right)| {
                        left.key != right.key || left.expected != right.expected
                    })
                    || read.journal.phase != SealPhase::Quiesced
                {
                    return Err(WorkspaceError::Fenced);
                }
                let mut next = read.claim.clone().unwrap_or(NativeSeedClaim {
                    journal_id: JournalId::from_uuid(Uuid::nil()),
                    native_journal_id: read.seed.journal_id,
                    staging_id: Uuid::nil(),
                    workspace_id: read.seed.workspace_id,
                    seed_origin_digest: read.seed.origin_digest()?,
                    quiesce_digest: Sha256::digest(&read.seed.canonical_quiesce).into(),
                    lease_id: read.lease.lease_id,
                    holder_generation: read.lease.holder_generation,
                    previous_lease_id: read.lease.lease_id,
                    owner_id: authority.owner_id.clone(),
                    open_generation: read.open.generation,
                    created_at_ns: read.lease.created_at_ns,
                });
                if (!next.journal_id.as_uuid().is_nil() || !next.staging_id.is_nil())
                    && (next.journal_id != journal_id || next.staging_id != staging_id)
                {
                    return Err(WorkspaceError::Fenced);
                }
                next.journal_id = journal_id;
                next.staging_id = staging_id;
                next.validate_seed(&read.seed)?;
                let encoded = encode(&next)?;
                if encoded.len() > SEED_CLAIM_MAX_BYTES {
                    return Err(native_freeze_error("owner handoff exceeds fixed cap"));
                }
                if read
                    .seed
                    .packed_journal_id
                    .is_some_and(|id| id != journal_id)
                {
                    return Err(WorkspaceError::Fenced);
                }
                let mut linked_seed = read.seed.clone();
                linked_seed.packed_journal_id = Some(journal_id);
                let already_bound = read.claim.as_ref().is_some_and(|claim| {
                    claim.journal_id == journal_id && claim.staging_id == staging_id
                });
                let mut writes = if already_bound {
                    Vec::new()
                } else {
                    vec![KvWrite::Put {
                        key: seed_claim_key(read.seed.journal_id),
                        value: encoded,
                    }]
                };
                if read.seed.packed_journal_id != Some(journal_id) {
                    writes.push(KvWrite::Put {
                        key: seed_key(read.seed.journal_id),
                        value: encode_seed(&linked_seed)?,
                    });
                }
                Ok(NativeJournalOwnerHandoff {
                    checks,
                    writes,
                    deadline_ns,
                    _permit: permit,
                })
            }
            .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| native_freeze_error("native owner handoff driver stopped"))?
    }
}

impl<B: WorkspaceKvBackend + 'static> KvWorkspaceStore<B> {
    pub(super) async fn recover_native_prepare_seed(
        self: &Arc<Self>,
        request: NativePrepareRecoveryRequest,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<NativePrepareRecoveryResult<B>, WorkspaceError> {
        if request.journal_id.as_uuid().is_nil()
            || request.owner_id.trim().is_empty()
            || request.owner_id.len() > OPEN_OWNER_MAX_BYTES
            || request
                .new_lease_id
                .is_some_and(|lease| lease.as_uuid().is_nil())
            || (request.new_lease_id.is_some()
                && (request.ttl_ns == 0 || request.ttl_ns > SEED_MAX_TTL_NS))
            || budget.state().closed
        {
            return Err(WorkspaceError::Fenced);
        }
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SEED_OPERATION_BYTES)])
            .map_err(native_freeze_error)?;
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = store
                .recover_native_prepare_seed_owned(request, budget, permit)
                .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| native_freeze_error("native seed recovery driver stopped"))?
    }

    async fn recover_native_prepare_seed_owned(
        self: Arc<Self>,
        request: NativePrepareRecoveryRequest,
        budget: Arc<SnapshotMountBudget>,
        mut permit: SnapshotOwnedPermit,
    ) -> Result<NativePrepareRecoveryResult<B>, WorkspaceError> {
        for _ in 0..CAS_MAX_RETRIES {
            if budget.state().closed {
                return Err(WorkspaceError::Fenced);
            }
            let mut _clean_seed_owner = None;
            let mut _initial_seed_owner = None;
            let mut read = match self
                .read_native_seed(request.journal_id, &request.owner_id, request.new_lease_id)
                .await
            {
                Err(WorkspaceError::Busy) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                result => result?,
            };
            if read.seed.packed_journal_id.is_some()
                || read
                    .claim
                    .as_ref()
                    .is_some_and(|claim| !claim.journal_id.as_uuid().is_nil())
            {
                // Once handed to a PPJ incarnation, only its genuine durable
                // basis may reissue source reads or replace the current owner.
                return Err(WorkspaceError::Fenced);
            }
            let retained_clean_original = if request.new_lease_id.is_none()
                && read.claim.is_none()
                && matches!(read.journal.phase, SealPhase::Prepare | SealPhase::Quiesced)
            {
                let mapping = read.seed.mapping();
                let binding = read.seed.binding()?;
                if let Some((checks, owner)) = self
                    .retained_original_clean_seed_recovery_checks(
                        &mapping,
                        &binding,
                        read.seed.old_layers[0].next_sequence,
                        &read.open,
                        &read.lease,
                        &budget,
                    )
                    .await?
                {
                    _clean_seed_owner = Some(owner);
                    for check in checks {
                        if let Some(index) = read.keys.iter().position(|key| *key == check.key) {
                            if read.values[index] != check.expected {
                                return Err(WorkspaceError::Busy);
                            }
                        } else if let Some(previous) = read
                            .packed_journal_checks
                            .iter()
                            .find(|previous| previous.key == check.key)
                        {
                            if previous.expected != check.expected {
                                return Err(WorkspaceError::Busy);
                            }
                        } else {
                            read.packed_journal_checks.push(check);
                        }
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            };
            let retained_initial_original = if !retained_clean_original
                && request.new_lease_id.is_none()
                && read.claim.is_none()
                && matches!(read.journal.phase, SealPhase::Prepare | SealPhase::Quiesced)
            {
                let mapping = read.seed.mapping();
                let binding = read.seed.binding()?;
                if let Some((checks, owner)) = self
                    .retained_original_initial_seed_recovery_checks(
                        &mapping,
                        &binding,
                        read.seed.old_layers[0].next_sequence,
                        &read.open,
                        &read.lease,
                        &budget,
                    )
                    .await?
                {
                    _initial_seed_owner = Some(owner);
                    for check in checks {
                        if let Some(index) = read.keys.iter().position(|key| *key == check.key) {
                            if read.values[index] != check.expected {
                                return Err(WorkspaceError::Busy);
                            }
                        } else if let Some(previous) = read
                            .packed_journal_checks
                            .iter()
                            .find(|previous| previous.key == check.key)
                        {
                            if previous.expected != check.expected {
                                return Err(WorkspaceError::Busy);
                            }
                        } else {
                            read.packed_journal_checks.push(check);
                        }
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            };
            if read.journal.phase == SealPhase::Quiesced && read.claim.is_none() {
                // A healthy original first PNB does not bind a seed claim.
                // Q + no claim therefore needs an actual absence proof, and
                // a generic original lease must expire before replacement.
                // The narrow private admin routes prove their PCR or PBI
                // provenance and exact Recovering open in this same CAS;
                // that open already fences the old clean publisher.
                if !retained_clean_original
                    && !retained_initial_original
                    && (request.new_lease_id.is_none()
                        || !matches!(read.lease.state, LeaseState::Active | LeaseState::Expired)
                        || read.lease.expires_at_ns > read.now)
                {
                    return Err(WorkspaceError::Busy);
                }
                let (checks, owner) = self
                    .native_seed_packed_journal_absence(
                        exact_seed_checks(&read),
                        read.seed.workspace_id,
                        read.seed.journal_id,
                    )
                    .await?;
                // The absence consumer returns only additional checks;
                // keep the private PCR/source checks in the same CAS.
                read.packed_journal_checks.extend(checks);
                // Retain the complete census through the takeover CAS and
                // any exact attempted-successor confirmation after reply loss.
                read._packed_journal_owner = owner;
            }
            let same_claim = read.claim.as_ref().is_some_and(|claim| {
                Some(claim.lease_id) == request.new_lease_id
                    && claim.owner_id == request.owner_id
                    && claim.open_generation == read.open.generation
            });
            if let Some(new_id) = request.new_lease_id.filter(|_| !same_claim) {
                if new_id == read.lease.lease_id
                    || read
                        .claim
                        .as_ref()
                        .is_some_and(|claim| !claim.journal_id.as_uuid().is_nil())
                    || (read.claim.is_none()
                        && !matches!(read.journal.phase, SealPhase::Prepare | SealPhase::Quiesced))
                    || (read.lease.state == LeaseState::Active
                        && read.lease.expires_at_ns > read.now)
                    || read.keys.len() != 20
                    || read.values[18].is_some()
                    || read.values[19].is_some()
                {
                    return Err(WorkspaceError::Busy);
                }
                let lower = (read.lease.state == LeaseState::Active
                    || read._packed_journal_owner.is_some())
                .then_some(read.lease.expires_at_ns.max(1));
                let mut old = read.lease.clone();
                old.state = LeaseState::Expired;
                old.updated_at_ns = read.now;
                let holder_generation = old
                    .holder_generation
                    .checked_add(1)
                    .ok_or_else(|| native_freeze_error("seed holder generation exhausted"))?;
                let expires = checked_expiry(read.now, request.ttl_ns)?;
                let lease = SnapshotLease {
                    lease_id: new_id,
                    workspace_id: old.workspace_id,
                    base_revision: old.base_revision.clone(),
                    holder_generation,
                    writable: true,
                    state: LeaseState::Active,
                    expires_at_ns: expires,
                    created_at_ns: read.now,
                    updated_at_ns: read.now,
                };
                let claim = NativeSeedClaim {
                    journal_id: JournalId::from_uuid(Uuid::nil()),
                    native_journal_id: read.seed.journal_id,
                    staging_id: Uuid::nil(),
                    workspace_id: read.seed.workspace_id,
                    seed_origin_digest: read.seed.origin_digest()?,
                    quiesce_digest: read.seed.quiesced_journal.as_ref().map_or([0; 32], |_| {
                        Sha256::digest(&read.seed.canonical_quiesce).into()
                    }),
                    lease_id: new_id,
                    holder_generation,
                    previous_lease_id: old.lease_id,
                    owner_id: request.owner_id.clone(),
                    open_generation: read.open.generation,
                    created_at_ns: read.now,
                };
                claim.validate_seed(&read.seed)?;
                let mut workspace = read.workspace.clone();
                workspace.active_lease = Some(lease.lease_id);
                let root_generation = next_packed_root_generation(&read.values[10])?;
                let writes = [
                    put(hot_workspace_key(workspace.workspace_id), &workspace)?,
                    put(hot_lease_key(old.workspace_id, old.lease_id), &old)?,
                    put(hot_lease_key(lease.workspace_id, new_id), &lease)?,
                    put(hot_lease_index_key(new_id), &lease.workspace_id)?,
                    put(seed_claim_key(request.journal_id), &claim)?,
                    put(PACKED_ROOT_GENERATION_KEY.to_vec(), &root_generation)?,
                ];
                if !self
                    .commit_native_seed(&read, &writes, lower, expires.min(read.open.expires_at_ns))
                    .await?
                {
                    tokio::task::yield_now().await;
                }
                // Freshly read this exact installed incarnation before Q.
                continue;
            }
            if read.lease.state != LeaseState::Active || read.lease.expires_at_ns <= read.now {
                return Err(WorkspaceError::Busy);
            }
            if let Some(claim) = &read.claim
                && (claim.owner_id != request.owner_id
                    || claim.open_generation != read.open.generation
                    || request.new_lease_id != Some(claim.lease_id))
            {
                return Err(WorkspaceError::Fenced);
            }
            let deadline = read.lease.expires_at_ns.min(read.open.expires_at_ns);
            let seed = if read.journal.phase == SealPhase::Prepare {
                let mut quiesced = read.journal.clone();
                quiesced.phase = SealPhase::Quiesced;
                quiesced.updated_at_ns = read.now;
                let next = read.seed.with_quiesced(&quiesced)?;
                let mut writes = vec![
                    put(
                        hot_journal_key(quiesced.workspace_id, quiesced.journal_id),
                        &quiesced,
                    )?,
                    KvWrite::Put {
                        key: seed_key(request.journal_id),
                        value: encode_seed(&next)?,
                    },
                ];
                if let Some(claim) = &read.claim {
                    if !claim.journal_id.as_uuid().is_nil() || !claim.staging_id.is_nil() {
                        return Err(WorkspaceError::Fenced);
                    }
                    let mut quiesced_owner = claim.clone();
                    quiesced_owner.quiesce_digest = Sha256::digest(&next.canonical_quiesce).into();
                    quiesced_owner.validate_seed(&next)?;
                    writes.push(put(seed_claim_key(request.journal_id), &quiesced_owner)?);
                }
                if !self
                    .commit_native_seed(&read, &writes, None, deadline)
                    .await?
                {
                    tokio::task::yield_now().await;
                    continue;
                }
                next
            } else if read.journal.phase == SealPhase::Quiesced {
                if !self.commit_native_seed(&read, &[], None, deadline).await? {
                    tokio::task::yield_now().await;
                    continue;
                }
                read.seed.clone()
            } else {
                // DD/Hashed needs actual packed durable-basis reissuance and
                // fresh comparison/hash; this entry cannot mint that proof.
                return Err(WorkspaceError::Fenced);
            };
            permit
                .shrink(SnapshotBudgetPool::Metadata, 128 << 10)
                .map_err(native_freeze_error)?;
            let guard = HeadGuard {
                lease_id: read.lease.lease_id,
                holder_generation: read.lease.holder_generation,
                ..seed.mapping().old_guard.clone()
            };
            let authority = Arc::new(NativePrepareRecoveryAuthority {
                store: self.clone(),
                seed: seed.clone(),
                claim: read.claim.map(|mut claim| {
                    claim.quiesce_digest = Sha256::digest(&seed.canonical_quiesce).into();
                    claim
                }),
                guard,
                owner_id: request.owner_id,
                open_generation: read.open.generation,
                budget,
                _permit: permit,
            });
            authority.validate().await?;
            return Ok(NativePrepareRecoveryResult {
                mapping: seed.mapping(),
                binding: seed.binding()?,
                frozen_head: seed.frozen_head(),
                journal: seed
                    .quiesced_journal
                    .clone()
                    .ok_or(WorkspaceError::Fenced)?,
                canonical: seed.canonical_quiesce.clone(),
                authority,
            });
        }
        Err(WorkspaceError::Busy)
    }
}

#[path = "../packed_clean_recovery_source.rs"]
mod clean_recovery_source;

#[cfg(target_os = "linux")]
mod native_quarantine_route;
