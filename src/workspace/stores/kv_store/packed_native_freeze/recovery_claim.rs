//! Actual recovery ownership, independent of immutable original-Q identity.
//! A backend-clock CAS expires the previous lease and installs the successor.

use super::*;
use crate::workspace::stores::kv_store::packed_journal::PackedNativeRecoveryBasisReceipt;
use uuid::Uuid;

const CLAIM_MAX_BYTES: usize = 4096;
const CLAIM_OPERATION_BYTES: u64 = 16 << 20;
const CLAIM_READ_BYTES: u64 = 2 << 20;
const CLAIM_MAX_TTL_NS: u64 = 15 * 60 * 1_000_000_000;

pub(crate) struct NativePackedRecoveryClaimRequest {
    pub(crate) new_lease_id: LeaseId,
    pub(crate) owner_id: String,
    pub(crate) ttl_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct NativeRecoveryClaimRecord {
    pub(super) journal_id: JournalId,
    pub(super) native_journal_id: JournalId,
    pub(super) staging_id: Uuid,
    pub(super) workspace_id: WorkspaceId,
    pub(super) seed_origin_digest: [u8; 32],
    pub(super) quiesce_digest: [u8; 32],
    pub(super) lease_id: LeaseId,
    pub(super) holder_generation: u64,
    pub(super) owner_id: String,
    pub(super) open_generation: u64,
    pub(super) previous_lease_id: LeaseId,
    pub(super) created_at_ns: i64,
}

/// Only successful actual claim CAS or exact same-incarnation retry can make
/// this token. It cannot be constructed from a raw guard or lease ID.
pub(crate) struct PackedNativeRecoveryClaimFence<B> {
    store: Arc<KvWorkspaceStore<B>>,
    basis: Arc<PackedNativeRecoveryBasisReceipt<B>>,
    record: NativeRecoveryClaimRecord,
    guard: HeadGuard,
    budget: Arc<SnapshotMountBudget>,
    _permit: SnapshotOwnedPermit,
}

pub(super) fn claim_key(journal_id: JournalId) -> Vec<u8> {
    format!("packed/native-recovery-claim/{journal_id}").into_bytes()
}

impl NativeRecoveryClaimRecord {
    fn validate_for<B: WorkspaceKvBackend>(
        &self,
        basis: &PackedNativeRecoveryBasisReceipt<B>,
    ) -> Result<(), WorkspaceError> {
        if self.journal_id != basis.journal_id()
            || self.native_journal_id != basis.native_journal_id()
            || self.staging_id != basis.staging_incarnation()
            || self.workspace_id != basis.old_guard().workspace_id
            || self.seed_origin_digest == [0; 32]
            || self.quiesce_digest != basis.quiesce_receipt_digest()
            || self.lease_id.as_uuid().is_nil()
            || self.previous_lease_id.as_uuid().is_nil()
            || self.holder_generation < basis.old_guard().holder_generation
            || (self.holder_generation == basis.old_guard().holder_generation
                && (self.lease_id != basis.old_guard().lease_id
                    || self.previous_lease_id != self.lease_id))
            || (self.holder_generation > basis.old_guard().holder_generation
                && self.lease_id == self.previous_lease_id)
            || self.owner_id.trim().is_empty()
            || self.owner_id.len() > 256
            || self.open_generation == 0
            || self.created_at_ns <= 0
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
}

fn merge_basis<B: WorkspaceKvBackend>(
    checks: &mut Vec<KvCheck>,
    basis: &PackedNativeRecoveryBasisReceipt<B>,
) -> Result<(), WorkspaceError> {
    for old in basis.basis_checks() {
        // Ownership epochs advance independently. Their current exact bytes
        // are freshly read while the source entities retain their identities.
        if [PACKED_ROOT_GENERATION_KEY, LAYER_INVENTORY_GENERATION_KEY]
            .contains(&old.key.as_slice())
        {
            if !checks.iter().any(|fresh| fresh.key == old.key) {
                return Err(WorkspaceError::Fenced);
            }
            continue;
        }
        if let Some(fresh) = checks.iter().find(|fresh| fresh.key == old.key) {
            if fresh.expected != old.expected
                && !same_workspace_topology(&old.key, &old.expected, &fresh.expected)?
            {
                return Err(WorkspaceError::Fenced);
            }
        } else {
            checks.push(KvCheck {
                key: old.key.clone(),
                expected: old.expected.clone(),
            });
        }
    }
    Ok(())
}

fn same_workspace_topology(
    key: &[u8],
    before: &Option<Vec<u8>>,
    after: &Option<Vec<u8>>,
) -> Result<bool, WorkspaceError> {
    if !key.starts_with(HOT_WORKSPACE_PREFIX) {
        return Ok(false);
    }
    let (Some(before), Some(after)) = (before, after) else {
        return Ok(false);
    };
    let mut before: WorkspaceRecord = decode_open_value(before, FREEZE_POINT_MAX_BYTES)?;
    let after: WorkspaceRecord = decode_open_value(after, FREEZE_POINT_MAX_BYTES)?;
    if key != hot_workspace_key(before.workspace_id) || before.workspace_id != after.workspace_id {
        return Err(WorkspaceError::Fenced);
    }
    // The claim operation authenticates the live pointer separately. Permit
    // only that pointer to advance across the immutable source receipt.
    before.active_lease = after.active_lease;
    Ok(before == after)
}

fn merge_source_ownership<B: WorkspaceKvBackend>(
    checks: &mut Vec<KvCheck>,
    basis: &PackedNativeRecoveryBasisReceipt<B>,
) -> Result<(), WorkspaceError> {
    // The claim retains native read ownership. PPJ can legitimately advance
    // from AwaitingFullProof to Verified while that ownership remains live.
    // Initial claim still checks the complete active durable basis; every new
    // source comparison and final publisher separately checks actual PPJ.
    let guard = basis.old_guard();
    let source_keys = vec![
        hot_workspace_key(guard.workspace_id),
        hot_layer_key(guard.expected_head_layer_id),
        hot_layer_key(basis.old_layers()[1].layer_id),
        hot_layer_key(basis.planned_head_layer_id()),
        packed_current_key(guard.workspace_id),
        packed_claim_key(guard.workspace_id),
        packed_history_key(guard.workspace_id, basis.binding().binding.binding_version),
        packed_history_key(guard.workspace_id, 1),
    ];
    for key in source_keys {
        let old = basis
            .basis_checks()
            .iter()
            .find(|check| check.key == key)
            .ok_or(WorkspaceError::Fenced)?;
        if let Some(fresh) = checks.iter().find(|fresh| fresh.key == key) {
            if fresh.expected != old.expected
                && !same_workspace_topology(&key, &old.expected, &fresh.expected)?
            {
                return Err(WorkspaceError::Fenced);
            }
        } else {
            checks.push(KvCheck {
                key,
                expected: old.expected.clone(),
            });
        }
    }
    Ok(())
}

fn check_journal<B: WorkspaceKvBackend>(
    journal: &SealJournal,
    basis: &PackedNativeRecoveryBasisReceipt<B>,
    now: i64,
) -> Result<(), WorkspaceError> {
    if journal.journal_id != basis.native_journal_id()
        || journal.workspace_id != basis.old_guard().workspace_id
    {
        return Err(WorkspaceError::Fenced);
    }
    super::native_recovery::validate_recovery_phase(
        basis.original_quiesced_native_journal(),
        journal,
    )?;
    if now <= 0 || journal.updated_at_ns > now {
        return Err(WorkspaceError::Fenced);
    }
    if journal.phase == SealPhase::Hashed
        && !journal
            .delta_digest
            .zip(journal.root_hash)
            .is_some_and(|(digest, root)| {
                basis.old_layers()[1]
                    .root_hash
                    .is_some_and(|parent| root_hash(parent, digest) == root)
            })
    {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
}

impl<B: WorkspaceKvBackend> PackedNativeRecoveryClaimFence<B> {
    pub(crate) fn guard(&self) -> &HeadGuard {
        &self.guard
    }
    pub(crate) fn mount_budget(&self) -> &Arc<SnapshotMountBudget> {
        &self.budget
    }
    pub(crate) fn matches_basis(&self, basis: &PackedNativeRecoveryBasisReceipt<B>) -> bool {
        Arc::ptr_eq(&self.store, basis.store())
            && self.record.journal_id == basis.journal_id()
            && self.record.native_journal_id == basis.native_journal_id()
            && self.record.staging_id == basis.staging_incarnation()
            && self.record.quiesce_digest == basis.quiesce_receipt_digest()
            && self.basis.old_guard() == basis.old_guard()
            && self.basis.old_layers() == basis.old_layers()
            && self.basis.binding() == basis.binding()
            && self.basis.quiesce_receipt_bytes() == basis.quiesce_receipt_bytes()
    }
    pub(crate) async fn authority_checks_before(
        &self,
    ) -> Result<(Vec<KvCheck>, i64), WorkspaceError> {
        let _owner = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, CLAIM_READ_BYTES)])
            .map_err(native_freeze_error)?;
        if self.budget.state().closed {
            return Err(WorkspaceError::Fenced);
        }
        let keys = vec![
            hot_journal_key(self.guard.workspace_id, self.record.native_journal_id),
            claim_key(self.record.native_journal_id),
            hot_lease_key(self.guard.workspace_id, self.guard.lease_id),
            open_snapshot_key(self.guard.workspace_id),
            open_snapshot_recovery_key(self.guard.workspace_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            super::native_seed::seed_key(self.record.native_journal_id),
            hot_workspace_key(self.guard.workspace_id),
            hot_lease_index_key(self.guard.lease_id),
        ];
        let (values, now) = self
            .store
            .backend
            .get_many_consistent_with_time_bounded(&keys, claim_limits(keys.len()))
            .await?;
        if values.len() != keys.len() {
            return Err(native_freeze_error("short recovery claim authority read"));
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        let journal: SealJournal = decode_open_value(required(0)?, FREEZE_POINT_MAX_BYTES)?;
        check_journal(&journal, &self.basis, now)?;
        let claim: NativeRecoveryClaimRecord = decode_open_value(required(1)?, CLAIM_MAX_BYTES)?;
        claim.validate_for(&self.basis)?;
        let seed = super::native_seed::decode_seed(required(7)?)?;
        seed.validate_owner_basis(&claim, &self.basis)?;
        let lease: SnapshotLease = decode_open_value(required(2)?, FREEZE_POINT_MAX_BYTES)?;
        let open: SnapshotOpenRecord = decode_open_value(required(3)?, OPEN_RECORD_MAX_BYTES)?;
        let recovery: SnapshotRecoveryRecord =
            decode_open_value(required(4)?, OPEN_RECORD_MAX_BYTES)?;
        let workspace: WorkspaceRecord = decode_open_value(required(8)?, FREEZE_POINT_MAX_BYTES)?;
        let lease_workspace: WorkspaceId = decode_open_value(required(9)?, OPEN_RECORD_MAX_BYTES)?;
        if claim != self.record
            || workspace.workspace_id != self.guard.workspace_id
            || workspace.active_lease != Some(self.guard.lease_id)
            || lease_workspace != self.guard.workspace_id
            || lease.lease_id != self.guard.lease_id
            || lease.workspace_id != self.guard.workspace_id
            || lease.holder_generation != self.guard.holder_generation
            || lease.base_revision != self.basis.binding().base_revision
            || !lease.writable
            || lease.state != LeaseState::Active
            || lease.expires_at_ns <= now
            || lease.created_at_ns != self.record.created_at_ns
            || lease.updated_at_ns > now
            || open.workspace_id != self.guard.workspace_id
            || open.owner_id != self.record.owner_id
            || open.generation != self.record.open_generation
            || open.expires_at_ns <= now
            || open.state != SnapshotOpenState::Recovering
            || !open.recovery_required
            || recovery.workspace_id != self.guard.workspace_id
            || !recovery.incomplete
        {
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[5])?;
        layer_inventory_generation(&values[6])?;
        let deadline = lease.expires_at_ns.min(open.expires_at_ns);
        let mut checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        merge_source_ownership(&mut checks, &self.basis)?;
        let _writer_owner = self
            .store
            .authenticate_administrative_packed_writer(self.guard.workspace_id, &mut checks)
            .await?;
        Ok((checks, deadline))
    }
    pub(crate) async fn validate(&self) -> Result<(), WorkspaceError> {
        let _terminal_owner = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, CLAIM_READ_BYTES)])
            .map_err(native_freeze_error)?;
        for _ in 0..CAS_MAX_RETRIES {
            let (checks, deadline) = self.authority_checks_before().await?;
            if self
                .store
                .backend
                .compare_and_swap_before(&checks, &[], deadline)
                .await?
            {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }
}

fn claim_limits(records: usize) -> KvReadLimits {
    KvReadLimits {
        max_records: records,
        max_key_bytes: 1024,
        max_value_bytes: FREEZE_POINT_MAX_BYTES,
        max_total_bytes: FREEZE_POINT_MAX_BYTES,
        max_response_bytes: 64 << 10,
        // Preserve record/byte ownership and the 32-attempt point cap while
        // explicitly reserving at most two certified read-only continuations.
        max_data_requests: records.saturating_add(2).min(32),
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(crate) async fn claim_native_packed_recovery(
        self: &Arc<Self>,
        basis: PackedNativeRecoveryBasisReceipt<B>,
        request: NativePackedRecoveryClaimRequest,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<Arc<PackedNativeRecoveryClaimFence<B>>, WorkspaceError> {
        if !Arc::ptr_eq(self, basis.store())
            || request.new_lease_id.as_uuid().is_nil()
            || request.new_lease_id == basis.old_guard().lease_id
            || request.owner_id.trim().is_empty()
            || request.owner_id.len() > 256
            || request.ttl_ns == 0
            || request.ttl_ns > CLAIM_MAX_TTL_NS
            || budget.state().closed
        {
            return Err(WorkspaceError::Fenced);
        }
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, CLAIM_OPERATION_BYTES)])
            .map_err(native_freeze_error)?;
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = store
                .claim_native_packed_recovery_owned(Arc::new(basis), request, budget, permit)
                .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| native_freeze_error("recovery claim driver stopped"))?
    }

    async fn claim_native_packed_recovery_owned(
        self: Arc<Self>,
        basis: Arc<PackedNativeRecoveryBasisReceipt<B>>,
        request: NativePackedRecoveryClaimRequest,
        budget: Arc<SnapshotMountBudget>,
        mut permit: SnapshotOwnedPermit,
    ) -> Result<Arc<PackedNativeRecoveryClaimFence<B>>, WorkspaceError> {
        let pointer_key = claim_key(basis.native_journal_id());
        for _ in 0..CAS_MAX_RETRIES {
            let (routing, _) = self
                .backend
                .get_many_consistent_with_time_bounded(
                    std::slice::from_ref(&pointer_key),
                    claim_limits(1),
                )
                .await?;
            if routing.len() != 1 {
                return Err(native_freeze_error("short recovery claim routing read"));
            }
            let previous = routing[0]
                .as_deref()
                .map(|raw| decode_open_value::<NativeRecoveryClaimRecord>(raw, CLAIM_MAX_BYTES))
                .transpose()?;
            if let Some(previous) = &previous {
                previous.validate_for(&basis)?;
                if previous.lease_id == request.new_lease_id
                    && previous.owner_id == request.owner_id
                {
                    permit
                        .shrink(SnapshotBudgetPool::Metadata, 128 << 10)
                        .map_err(native_freeze_error)?;
                    let guard = HeadGuard {
                        lease_id: previous.lease_id,
                        holder_generation: previous.holder_generation,
                        ..basis.old_guard().clone()
                    };
                    let fence = Arc::new(PackedNativeRecoveryClaimFence {
                        store: self.clone(),
                        basis,
                        record: previous.clone(),
                        guard,
                        budget,
                        _permit: permit,
                    });
                    fence.validate().await?;
                    return Ok(fence);
                }
            }
            let previous_id = previous
                .as_ref()
                .map_or(basis.old_guard().lease_id, |record| record.lease_id);
            if previous_id == request.new_lease_id {
                return Err(WorkspaceError::Fenced);
            }
            let keys = vec![
                hot_journal_key(basis.old_guard().workspace_id, basis.native_journal_id()),
                pointer_key.clone(),
                hot_lease_key(basis.old_guard().workspace_id, previous_id),
                hot_lease_key(basis.old_guard().workspace_id, request.new_lease_id),
                open_snapshot_key(basis.old_guard().workspace_id),
                open_snapshot_recovery_key(basis.old_guard().workspace_id),
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                super::native_seed::seed_key(basis.native_journal_id()),
                hot_workspace_key(basis.old_guard().workspace_id),
                hot_lease_index_key(previous_id),
                hot_lease_index_key(request.new_lease_id),
            ];
            let (values, now) = self
                .backend
                .get_many_consistent_with_time_bounded(&keys, claim_limits(keys.len()))
                .await?;
            if values.len() != keys.len() || values[1] != routing[0] {
                tokio::task::yield_now().await;
                continue;
            }
            if values[3].is_some() || values[11].is_some() {
                return Err(WorkspaceError::Busy);
            }
            let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
            let seed = super::native_seed::decode_seed(required(8)?)?;
            seed.validate_packed_basis(&basis)?;
            if let Some(previous) = &previous {
                seed.validate_owner_basis(previous, &basis)?;
            }
            let journal: SealJournal = decode_open_value(required(0)?, FREEZE_POINT_MAX_BYTES)?;
            check_journal(&journal, &basis, now)?;
            let mut old: SnapshotLease = decode_open_value(required(2)?, FREEZE_POINT_MAX_BYTES)?;
            let open: SnapshotOpenRecord = decode_open_value(required(4)?, OPEN_RECORD_MAX_BYTES)?;
            let recovery: SnapshotRecoveryRecord =
                decode_open_value(required(5)?, OPEN_RECORD_MAX_BYTES)?;
            let mut workspace: WorkspaceRecord =
                decode_open_value(required(9)?, FREEZE_POINT_MAX_BYTES)?;
            let previous_workspace: WorkspaceId =
                decode_open_value(required(10)?, OPEN_RECORD_MAX_BYTES)?;
            let previous_generation = previous
                .as_ref()
                .map_or(basis.old_guard().holder_generation, |record| {
                    record.holder_generation
                });
            if old.lease_id != previous_id
                || workspace.workspace_id != basis.old_guard().workspace_id
                || workspace.active_lease.is_some_and(|id| id != previous_id)
                || previous_workspace != basis.old_guard().workspace_id
                || old.workspace_id != basis.old_guard().workspace_id
                || old.holder_generation != previous_generation
                || !old.writable
                || old.base_revision != basis.binding().base_revision
                || (old.state == LeaseState::Active && old.expires_at_ns > now)
                || open.workspace_id != basis.old_guard().workspace_id
                || open.owner_id != request.owner_id
                || open.generation == 0
                || open.expires_at_ns <= now
                || open.state != SnapshotOpenState::Recovering
                || !open.recovery_required
                || recovery.workspace_id != basis.old_guard().workspace_id
                || !recovery.incomplete
            {
                return Err(WorkspaceError::Busy);
            }
            let lower = (old.state == LeaseState::Active).then_some(old.expires_at_ns.max(1));
            let expires = checked_expiry(now, request.ttl_ns)?;
            let deadline = expires.min(open.expires_at_ns);
            let generation = previous_generation
                .checked_add(1)
                .ok_or_else(|| native_freeze_error("recovery holder generation exhausted"))?;
            old.state = LeaseState::Expired;
            old.updated_at_ns = now;
            let lease = SnapshotLease {
                lease_id: request.new_lease_id,
                workspace_id: old.workspace_id,
                base_revision: old.base_revision.clone(),
                holder_generation: generation,
                writable: true,
                state: LeaseState::Active,
                expires_at_ns: expires,
                created_at_ns: now,
                updated_at_ns: now,
            };
            let record = NativeRecoveryClaimRecord {
                journal_id: basis.journal_id(),
                native_journal_id: basis.native_journal_id(),
                staging_id: basis.staging_incarnation(),
                workspace_id: old.workspace_id,
                seed_origin_digest: seed.origin_digest()?,
                quiesce_digest: basis.quiesce_receipt_digest(),
                lease_id: lease.lease_id,
                holder_generation: generation,
                owner_id: request.owner_id.clone(),
                open_generation: open.generation,
                previous_lease_id: old.lease_id,
                created_at_ns: now,
            };
            record.validate_for(&basis)?;
            workspace.active_lease = Some(lease.lease_id);
            let root_generation = next_packed_root_generation(&values[6])?;
            layer_inventory_generation(&values[7])?;
            let mut checks = keys
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>();
            merge_basis(&mut checks, &basis)?;
            let mut writes = vec![
                put(hot_workspace_key(workspace.workspace_id), &workspace)?,
                put(hot_lease_key(old.workspace_id, old.lease_id), &old)?,
                put(hot_lease_key(lease.workspace_id, lease.lease_id), &lease)?,
                put(hot_lease_index_key(lease.lease_id), &lease.workspace_id)?,
                put(pointer_key.clone(), &record)?,
                put(PACKED_ROOT_GENERATION_KEY.to_vec(), &root_generation)?,
            ];
            let _writer_owner = self.prepare_administrative_packed_writer(
                basis.old_guard().workspace_id, crate::workspace::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::Update, &mut checks, &mut writes,
            ).await?;
            let _native_holds = self
                .prepare_native_owner_cas(&mut checks, &mut writes)
                .await?;
            let packet = self
                .prepare_topology_envelope(checks, writes, Some(deadline))
                .await?;
            let checks = packet.checks.clone();
            let writes = packet.writes.clone();
            let mut attempted = checks.clone();
            for write in &writes {
                let (key, expected) = match write {
                    KvWrite::Put { key, value } => (key, Some(value.clone())),
                    KvWrite::Delete { key } => (key, None),
                };
                let exact = attempted
                    .iter_mut()
                    .find(|check| check.key == *key)
                    .ok_or_else(|| native_freeze_error("claim write lacks exact predecessor"))?;
                exact.expected = expected;
            }
            let confirm_limits = KvReadLimits {
                max_records: 64,
                max_key_bytes: 1024,
                max_value_bytes: 96 << 10,
                max_total_bytes: 256 << 10,
                max_response_bytes: 128 << 10,
                max_data_requests: 64,
            };
            let confirm_bytes = attempted.iter().try_fold(0usize, |sum, check| {
                let value_len = check.expected.as_ref().map_or(0, Vec::len);
                if check.key.len() > confirm_limits.max_key_bytes
                    || value_len > confirm_limits.max_value_bytes
                {
                    return Err(native_freeze_error("claim successor exceeds fixed tier"));
                }
                sum.checked_add(check.key.len())
                    .and_then(|sum| sum.checked_add(value_len))
                    .ok_or_else(|| native_freeze_error("claim successor aggregate overflow"))
            })?;
            if attempted.len() > confirm_limits.max_records
                || confirm_bytes > confirm_limits.max_total_bytes
            {
                return Err(native_freeze_error(
                    "claim successor aggregate exceeds fixed tier",
                ));
            }
            let cas = self
                .backend
                .compare_and_swap_in_time_window(&checks, &writes, lower, Some(deadline))
                .await;
            match cas {
                Ok(false) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(true) => {}
                Err(error @ WorkspaceError::Backend(_)) => {
                    // Confirm all actual successors and unchanged authority
                    // keys, including the workspace pointer, native holds and
                    // both epochs.
                    // A matching pointer/lease trio alone cannot prove these.
                    let confirmed = async {
                        let confirm_keys = attempted
                            .iter()
                            .map(|check| check.key.clone())
                            .collect::<Vec<_>>();
                        let (actual, now) = self
                            .backend
                            .get_many_consistent_with_time_bounded(&confirm_keys, confirm_limits)
                            .await?;
                        if actual.len() != attempted.len()
                            || now <= 0
                            || now >= deadline
                            || lower.is_some_and(|minimum| now < minimum)
                            || actual
                                .iter()
                                .zip(&attempted)
                                .any(|(actual, check)| *actual != check.expected)
                        {
                            return Ok(false);
                        }
                        self.backend
                            .compare_and_swap_in_time_window(&attempted, &[], lower, Some(deadline))
                            .await
                    }
                    .await;
                    if !matches!(confirmed, Ok(true)) {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
            permit
                .shrink(SnapshotBudgetPool::Metadata, 128 << 10)
                .map_err(native_freeze_error)?;
            let guard = HeadGuard {
                lease_id: record.lease_id,
                holder_generation: record.holder_generation,
                ..basis.old_guard().clone()
            };
            let fence = Arc::new(PackedNativeRecoveryClaimFence {
                store: self.clone(),
                basis,
                record,
                guard,
                budget,
                _permit: permit,
            });
            fence.validate().await?;
            return Ok(fence);
        }
        Err(WorkspaceError::Busy)
    }
}
