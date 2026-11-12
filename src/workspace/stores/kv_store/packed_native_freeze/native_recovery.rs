//! Reissue frozen source reads from actual durable PPJ and native catalog.
//! These reads do not certify effective equality, native hash, or publication.

use super::*;
use crate::workspace::stores::kv_store::packed_journal::PackedNativeRecoveryBasisReceipt;

/// Its only constructor consumes an actual store-issued durable receipt,
/// reconstructs the original Q canonical bytes, and commits an exact timed
/// no-op CAS over durable PPJ and the current native phase plus live lease.
/// A caller cannot use arbitrary records, hashes or Sealing flags to make it.
pub(crate) struct PackedNativeRecoveryReadFence<B> {
    native: Arc<PackedNativeQuiesceFence<B>>,
}

impl<B: WorkspaceKvBackend> PackedNativeRecoveryReadFence<B> {
    pub(crate) fn native_quiesce(&self) -> &Arc<PackedNativeQuiesceFence<B>> {
        &self.native
    }
    pub(crate) fn store(&self) -> &Arc<KvWorkspaceStore<B>> {
        &self.native.store
    }
    pub(crate) fn basis(&self) -> Option<&PackedNativeRecoveryBasisReceipt<B>> {
        self.native.recovery.as_deref()
    }
    pub(crate) async fn validate(&self) -> Result<(), WorkspaceError> {
        self.native.validate().await
    }
    pub(crate) async fn authority_checks_before(
        &self,
    ) -> Result<(Vec<KvCheck>, i64), WorkspaceError> {
        self.native
            .store
            .frozen_source_authority_checks(&self.native)
            .await
    }
}

/// Check that only the supported successor fields changed after the original
/// persisted Q. Digest correctness is separately proved by actual table scans
/// before a Hashed in-process authority can be reissued.
pub(super) fn validate_recovery_phase(
    original: &SealJournal,
    current: &SealJournal,
) -> Result<(), WorkspaceError> {
    if original.phase != SealPhase::Quiesced
        || original.delta_digest.is_some()
        || original.root_hash.is_some()
        || !matches!(
            current.phase,
            SealPhase::Quiesced | SealPhase::DataDrained | SealPhase::Hashed
        )
    {
        return Err(WorkspaceError::Fenced);
    }
    if current.phase == SealPhase::Quiesced {
        return if current == original {
            Ok(())
        } else {
            Err(WorkspaceError::Fenced)
        };
    }
    if current.pending_bytes != 0
        || current.last_error.is_some()
        || current.updated_at_ns <= 0
        || current.updated_at_ns < original.updated_at_ns
        || (current.phase == SealPhase::DataDrained
            && (current.delta_digest.is_some() || current.root_hash.is_some()))
        || (current.phase == SealPhase::Hashed
            && (current.delta_digest.is_none() || current.root_hash.is_none()))
    {
        return Err(WorkspaceError::Fenced);
    }
    let mut normalized = current.clone();
    normalized.phase = original.phase;
    normalized.pending_bytes = original.pending_bytes;
    normalized.last_error = original.last_error.clone();
    normalized.updated_at_ns = original.updated_at_ns;
    normalized.delta_digest = original.delta_digest;
    normalized.root_hash = original.root_hash;
    if &normalized != original {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
}

fn merge_exact_checks(
    checks: &mut Vec<KvCheck>,
    additional: &[KvCheck],
) -> Result<(), WorkspaceError> {
    for check in additional {
        // Reader pin acquire/renew advances these concurrency epochs. Source
        // identity is unchanged. The fresh 12-key read already includes and
        // validates both generations; keep those actual current expectations
        // in the same timed CAS instead of freezing a historical epoch.
        if check.key.as_slice() == PACKED_ROOT_GENERATION_KEY
            || check.key.as_slice() == LAYER_INVENTORY_GENERATION_KEY
        {
            if !checks.iter().any(|current| current.key == check.key) {
                return Err(WorkspaceError::Fenced);
            }
            continue;
        }
        if let Some(previous) = checks.iter().find(|previous| previous.key == check.key) {
            if previous.expected != check.expected {
                return Err(WorkspaceError::Fenced);
            }
        } else {
            checks.push(KvCheck {
                key: check.key.clone(),
                expected: check.expected.clone(),
            });
        }
    }
    Ok(())
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(super) async fn frozen_source_authority_checks(
        &self,
        fence: &PackedNativeQuiesceFence<B>,
    ) -> Result<(Vec<KvCheck>, i64), WorkspaceError> {
        if !fence.belongs_to_store(self) || fence.budget.state().closed {
            return Err(WorkspaceError::Fenced);
        }
        let journal = fence
            .recovery
            .as_ref()
            .map_or(&fence.journal, |basis| basis.current_native_journal());
        let expected = (journal.phase != SealPhase::Quiesced).then_some(journal);
        let read = fence
            .read_phase_authority(expected)
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                native_authority_diagnostic("source-phase-packet", _error);
            })?;
        if read.head != fence.frozen_head || &read.journal != journal {
            #[cfg(test)]
            eprintln!(
                "[packed-native-authority-diag] stage=source-frozen-head-or-journal error=Fenced"
            );
            return Err(WorkspaceError::Fenced);
        }
        let deadline = read.authority_deadline_ns;
        let mut checks = read
            .keys
            .into_iter()
            .zip(read.values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        if let Some(basis) = &fence.recovery {
            if !Arc::ptr_eq(basis.store(), &fence.store)
                || basis.old_guard() != fence.mapping.old_guard()
                || basis.old_layers() != fence.mapping.old_layers()
                || basis.binding() != &fence.binding
                || basis.original_quiesced_native_journal() != &fence.journal
                || basis.quiesce_receipt_bytes() != fence.canonical.as_slice()
                || basis.quiesce_receipt_digest() != fence.canonical_receipt_digest()
                || read.now <= 0
                || read.journal.updated_at_ns > read.now
            {
                #[cfg(test)]
                eprintln!(
                    "[packed-native-authority-diag] stage=source-recovery-basis error=Fenced store={} guard={} layers={} binding={} journal={} canonical={} digest={} positive_clock={} journal_not_future={} journal_ahead_ns={:?}",
                    Arc::ptr_eq(basis.store(), &fence.store),
                    basis.old_guard() == fence.mapping.old_guard(),
                    basis.old_layers() == fence.mapping.old_layers(),
                    basis.binding() == &fence.binding,
                    basis.original_quiesced_native_journal() == &fence.journal,
                    basis.quiesce_receipt_bytes() == fence.canonical.as_slice(),
                    basis.quiesce_receipt_digest() == fence.canonical_receipt_digest(),
                    read.now > 0,
                    read.journal.updated_at_ns <= read.now,
                    read.journal.updated_at_ns.checked_sub(read.now),
                );
                return Err(WorkspaceError::Fenced);
            }
            validate_recovery_phase(&fence.journal, &read.journal).inspect_err(|_error| {
                #[cfg(test)]
                native_authority_diagnostic("source-recovery-phase", _error);
            })?;
            let guard = basis.old_guard();
            let keys = [
                hot_workspace_key(guard.workspace_id),
                hot_layer_key(guard.expected_head_layer_id),
                hot_layer_key(basis.old_layers()[1].layer_id),
                hot_layer_key(basis.planned_head_layer_id()),
                packed_current_key(guard.workspace_id),
                packed_claim_key(guard.workspace_id),
                packed_history_key(guard.workspace_id, basis.binding().binding.binding_version),
                packed_history_key(guard.workspace_id, 1),
            ];
            let source = keys
                .into_iter()
                .map(|key| {
                    basis
                        .basis_checks()
                        .iter()
                        .find(|check| check.key == key)
                        .cloned()
                        .ok_or(WorkspaceError::Fenced)
                })
                .collect::<Result<Vec<_>, _>>()?;
            merge_exact_checks(&mut checks, &source).inspect_err(|_error| {
                #[cfg(test)]
                native_authority_diagnostic("source-recovery-immutable-overlap", _error);
            })?;
        }
        if let Some(claim) = &fence.recovery_claim {
            let basis = fence.recovery.as_ref().ok_or(WorkspaceError::Fenced)?;
            if !claim.matches_basis(basis) || !Arc::ptr_eq(claim.mount_budget(), &fence.budget) {
                #[cfg(test)]
                eprintln!(
                    "[packed-native-authority-diag] stage=source-recovery-claim error=Fenced"
                );
                return Err(WorkspaceError::Fenced);
            }
        }
        let _writer_owner = self
            .authenticate_administrative_packed_writer(
                fence.mapping.old_guard().workspace_id,
                &mut checks,
            )
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                native_authority_diagnostic("source-admin-writer-context", _error);
            })?;
        Ok((checks, deadline))
    }
}

impl<B: WorkspaceKvBackend + 'static> KvWorkspaceStore<B> {
    /// Grant the existing typed Sealing reader only from this actual store's
    /// retained pre-PNB seed authority. No durable PNB basis is invented.
    pub(crate) async fn reissue_native_seed_read(
        self: &Arc<Self>,
        native: Arc<PackedNativeQuiesceFence<B>>,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<Arc<PackedNativeRecoveryReadFence<B>>, WorkspaceError> {
        let seed = native
            .seed_authority
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?;
        if !native.is_same_store(self)
            || !seed.belongs_to_store(self)
            || !Arc::ptr_eq(&native.budget, &budget)
            || !Arc::ptr_eq(seed.mount_budget(), &budget)
            || native.recovery.is_some()
            || native.recovery_claim.is_some()
            || budget.state().closed
        {
            return Err(WorkspaceError::Fenced);
        }
        seed.matches_original(
            &native.mapping,
            &native.binding,
            &native.journal,
            &native.canonical,
        )?;
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 1 << 20)])
            .map_err(native_freeze_error)?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _owner = permit;
            let result = async {
                if budget.state().closed {
                    return Err(WorkspaceError::Fenced);
                }
                native.validate().await?;
                Ok(Arc::new(PackedNativeRecoveryReadFence { native }))
            }
            .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| native_freeze_error("seed reader authority driver stopped"))?
    }

    pub(crate) async fn reissue_native_source_read(
        self: &Arc<Self>,
        receipt: PackedNativeRecoveryBasisReceipt<B>,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<Arc<PackedNativeRecoveryReadFence<B>>, WorkspaceError> {
        if !Arc::ptr_eq(self, receipt.store()) || budget.state().closed {
            return Err(WorkspaceError::Fenced);
        }
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, FREEZE_METADATA_BYTES)])
            .map_err(native_freeze_error)?;
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // This owner outlives caller cancellation and every actual bounded read
        // and no-op CAS. No local transport future is detached by dropping it.
        tokio::spawn(async move {
            let result = store
                .reissue_native_source_read_owned(receipt, budget, permit, None)
                .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| native_freeze_error("recovery source driver stopped"))?
    }

    pub(crate) async fn reissue_native_source_read_claimed(
        self: &Arc<Self>,
        receipt: PackedNativeRecoveryBasisReceipt<B>,
        claim: Arc<PackedNativeRecoveryClaimFence<B>>,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<Arc<PackedNativeRecoveryReadFence<B>>, WorkspaceError> {
        if !claim.matches_basis(&receipt)
            || !Arc::ptr_eq(self, receipt.store())
            || !Arc::ptr_eq(claim.mount_budget(), &budget)
            || budget.state().closed
        {
            return Err(WorkspaceError::Fenced);
        }
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, FREEZE_METADATA_BYTES)])
            .map_err(native_freeze_error)?;
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = async {
                claim.validate().await?;
                store
                    .reissue_native_source_read_owned(receipt, budget, permit, Some(claim))
                    .await
            }
            .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| native_freeze_error("claimed recovery source driver stopped"))?
    }

    async fn reissue_native_source_read_owned(
        self: Arc<Self>,
        receipt: PackedNativeRecoveryBasisReceipt<B>,
        budget: Arc<SnapshotMountBudget>,
        permit: SnapshotOwnedPermit,
        claim: Option<Arc<PackedNativeRecoveryClaimFence<B>>>,
    ) -> Result<Arc<PackedNativeRecoveryReadFence<B>>, WorkspaceError> {
        let mapping = PackedNativePlannedRotation {
            old_guard: receipt.old_guard().clone(),
            old_layers: receipt.old_layers().clone(),
            journal_id: receipt.native_journal_id(),
            planned_head_layer_id: receipt.planned_head_layer_id(),
            planned_head_epoch: receipt.planned_head_epoch(),
        };
        validate_permission_layers(&mapping.old_layers)?;
        if mapping.journal_id.as_uuid().is_nil()
            || mapping.planned_head_layer_id.as_uuid().is_nil()
            || mapping.planned_head_layer_id == mapping.old_layers[0].layer_id
            || mapping.planned_head_layer_id == mapping.old_layers[1].layer_id
            || mapping.old_layers[0].layer_id != mapping.old_guard.expected_head_layer_id
            || mapping.planned_head_epoch
                != mapping
                    .old_guard
                    .expected_head_epoch
                    .checked_add(1)
                    .ok_or(WorkspaceError::Fenced)?
        {
            return Err(WorkspaceError::Fenced);
        }
        let binding = receipt.binding().clone();
        binding.validate_for_guard(&mapping.old_guard, &mapping.old_layers[1])?;
        let journal = receipt.original_quiesced_native_journal().clone();
        validate_recovery_phase(&journal, receipt.current_native_journal())?;
        let mut frozen_head = mapping.old_layers[0].clone();
        frozen_head.state = LayerState::Sealing;
        let canonical = canonical_quiesce(&mapping, &binding, &frozen_head, &journal)?;
        if canonical != receipt.quiesce_receipt_bytes()
            || <[u8; 32]>::from(Sha256::digest(&canonical)) != receipt.quiesce_receipt_digest()
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut native = PackedNativeQuiesceFence {
            store: self.clone(),
            mapping,
            binding,
            frozen_head,
            journal,
            canonical,
            recovery: Some(Arc::new(receipt)),
            recovery_claim: claim,
            seed_authority: None,
            clean_source: None,
            bootstrap_source: None,
            budget,
            _permit: permit,
        };
        native.clean_source = self.restore_clean_native_origin(&native).await?;
        native.bootstrap_source = self.restore_initial_native_origin(&native).await?;
        let native = Arc::new(native);
        native.validate().await?;
        Ok(Arc::new(PackedNativeRecoveryReadFence { native }))
    }
}
