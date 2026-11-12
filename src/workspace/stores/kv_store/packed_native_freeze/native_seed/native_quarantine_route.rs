//! Exact source-owner routing for an abandoned physical packed attempt.
//! The caller applies these writes with old-retained/new-staging rows in one CAS.

use super::*;
use crate::workspace::stores::kv_store::packed_journal::PackedJournalPhase;
use crate::workspace::stores::kv_store::packed_native_freeze::{
    NativePackedRecoveryClaimRequest, PackedNativeRecoveryReadFence,
};

pub(crate) struct NativeQuarantineRouteSuccessor {
    pub(crate) handoff: NativeJournalOwnerHandoff,
    pub(crate) reissue_owner: Option<NativePackedRecoveryClaimRequest>,
}

impl<B: WorkspaceKvBackend + 'static> KvWorkspaceStore<B> {
    pub(crate) async fn prepare_native_quarantine_route_successor(
        self: &Arc<Self>,
        recovery: &Arc<PackedNativeRecoveryReadFence<B>>,
        next_journal: JournalId,
        next_staging: Uuid,
    ) -> Result<NativeQuarantineRouteSuccessor, WorkspaceError> {
        let basis = recovery.basis().ok_or(WorkspaceError::Fenced)?;
        let native = recovery.native_quiesce();
        let budget = native.mount_budget();
        if !Arc::ptr_eq(self, recovery.store())
            || budget.state().closed
            || basis.record().phase != PackedJournalPhase::Building
            || basis.record().commit_target.is_some()
            || next_journal.as_uuid().is_nil()
            || next_journal == basis.journal_id()
            || next_staging.is_nil()
            || next_staging == basis.staging_incarnation()
        {
            return Err(WorkspaceError::Fenced);
        }
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SEED_READ_BYTES)])
            .map_err(native_freeze_error)?;
        let (_, deadline_ns) = recovery.authority_checks_before().await?;
        let keys = vec![
            seed_key(basis.native_journal_id()),
            seed_claim_key(basis.native_journal_id()),
            open_snapshot_key(basis.old_guard().workspace_id),
        ];
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, seed_limits(keys.len()))
            .await?;
        validate_successor_bounds(&keys, &values)?;
        if values.len() != 3 || now <= 0 || deadline_ns <= now || budget.state().closed {
            return Err(WorkspaceError::Fenced);
        }
        let mut seed = decode_seed(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        seed.validate_packed_basis(basis)?;
        let open: SnapshotOpenRecord = decode_open_value(
            values[2].as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        if open.workspace_id != basis.old_guard().workspace_id
            || open.generation == 0
            || open.expires_at_ns < deadline_ns
            || open.expires_at_ns <= now
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut writes = Vec::with_capacity(2);
        let reissue_owner = if let Some(raw) = &values[1] {
            let mut claim: NativeSeedClaim = decode_open_value(raw, SEED_CLAIM_MAX_BYTES)?;
            seed.validate_owner_basis(&claim, basis)?;
            if claim.lease_id != native.source_guard().lease_id
                || claim.holder_generation != native.source_guard().holder_generation
                || claim.owner_id != open.owner_id
                || claim.open_generation != open.generation
                || claim.created_at_ns > now
                || open.state != SnapshotOpenState::Recovering
                || !open.recovery_required
            {
                return Err(WorkspaceError::Fenced);
            }
            claim.journal_id = next_journal;
            claim.staging_id = next_staging;
            claim.validate_seed(&seed)?;
            writes.push(KvWrite::Put {
                key: keys[1].clone(),
                value: encode(&claim)?,
            });
            Some(NativePackedRecoveryClaimRequest {
                new_lease_id: claim.lease_id,
                owner_id: claim.owner_id,
                ttl_ns: u64::try_from(deadline_ns - now)
                    .map_err(native_freeze_error)?
                    .min(15 * 60 * 1_000_000_000),
            })
        } else {
            if native.source_guard() != basis.old_guard()
                || open.state != SnapshotOpenState::Ready
                || open.recovery_required
            {
                return Err(WorkspaceError::Fenced);
            }
            None
        };
        seed.packed_journal_id = Some(next_journal);
        writes.push(KvWrite::Put {
            key: keys[0].clone(),
            value: encode_seed(&seed)?,
        });
        Ok(NativeQuarantineRouteSuccessor {
            handoff: NativeJournalOwnerHandoff {
                checks: keys
                    .into_iter()
                    .zip(values)
                    .map(|(key, expected)| KvCheck { key, expected })
                    .collect(),
                writes,
                deadline_ns,
                _permit: permit,
            },
            reissue_owner,
        })
    }
}
