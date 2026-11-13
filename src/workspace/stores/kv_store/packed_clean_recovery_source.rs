//! The existing NQB carries one routing link, never a new source owner.
//! This child of native_seed can decode its private original-source facts.
use super::*;

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    #[cfg(test)]
    pub(crate) fn assert_native_resume_route_packet(
        old: &PackedJournalRecord,
        next: &PackedJournalRecord,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) {
        let checked = |key: &[u8]| {
            checks
                .iter()
                .find(|row| row.key == key)
                .expect("actual route predecessor must be checked")
                .expected
                .as_deref()
        };
        let put = |key: &[u8]| {
            writes
                .iter()
                .find_map(|row| match row {
                    KvWrite::Put { key: actual, value } if actual == key => Some(value.as_slice()),
                    _ => None,
                })
                .expect("route successor must join the actual quarantine CAS")
        };
        let native_id = old.native_completion_identities().unwrap().0;
        let seed_before = decode_seed(checked(&seed_key(native_id)).unwrap()).unwrap();
        let mut seed_after = decode_seed(put(&seed_key(native_id))).unwrap();
        assert_eq!(seed_before.packed_journal_id, Some(old.journal_id));
        assert_eq!(seed_after.packed_journal_id, Some(next.journal_id));
        seed_after.packed_journal_id = seed_before.packed_journal_id;
        assert_eq!(
            seed_before, seed_after,
            "every original typed NQB source fact stays exact"
        );
        let key = seed_claim_key(native_id);
        if let Some(raw) = checked(&key) {
            let claim_before: NativeSeedClaim =
                decode_open_value(raw, SEED_CLAIM_MAX_BYTES).unwrap();
            let mut claim_after: NativeSeedClaim =
                decode_open_value(put(&key), SEED_CLAIM_MAX_BYTES).unwrap();
            assert_eq!(claim_before.journal_id, old.journal_id);
            assert_eq!(claim_before.staging_id, old.source.staging_id);
            assert_eq!(claim_after.journal_id, next.journal_id);
            assert_eq!(claim_after.staging_id, next.source.staging_id);
            claim_before.validate_seed(&seed_before).unwrap();
            claim_after.validate_seed(&seed_before).unwrap();
            claim_after.journal_id = claim_before.journal_id;
            claim_after.staging_id = claim_before.staging_id;
            assert_eq!(
                claim_before, claim_after,
                "existing typed claim changes only its physical attempt routing"
            );
        } else {
            assert!(
                writes.iter().all(|row| match row {
                    KvWrite::Put { key: actual, .. } | KvWrite::Delete { key: actual } =>
                        *actual != key,
                }),
                "original owner creates no recovery claim"
            );
        }
    }

    /// Read one actual native-journal keyed NQB. The returned ID is routing
    /// input only; the consumer must inspect the actual PPJ/native catalog.
    /// No global journal scan and no caller-generated PPJ ID are needed.
    pub(crate) async fn read_clean_native_journal_route(
        &self,
        workspace: WorkspaceId,
        native_journal: JournalId,
        planned_head: LayerId,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<Option<JournalId>, WorkspaceError> {
        if budget.state().closed {
            return Err(WorkspaceError::Fenced);
        }
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SEED_READ_BYTES)])
            .map_err(native_freeze_error)?;
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&[seed_key(native_journal)], seed_limits(1))
            .await?;
        if values.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let Some(raw) = values[0].as_deref() else {
            return Ok(None);
        };
        let seed = decode_seed(raw)?;
        if seed.workspace_id != workspace
            || seed.journal_id != native_journal
            || seed.planned_head_layer_id != planned_head
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(seed.packed_journal_id)
    }

    /// The ordinary original fence also binds the NQB route in the first PPJ
    /// transaction. It keeps its actual current lease/source owner unchanged.
    /// The existing journal writer owns these checks/writes through its CAS
    /// and exact uncertain-response confirmation, including this seed link.
    pub(crate) async fn prepare_original_native_journal_link(
        self: &Arc<Self>,
        native: &PackedNativeQuiesceFence<B>,
        record: &PackedJournalRecord,
    ) -> Result<NativeJournalOwnerHandoff, WorkspaceError> {
        if !native.is_same_store(self)
            || native.seed_authority.is_some()
            || native.recovery.is_some()
            || native.recovery_claim.is_some()
            || native.mount_budget().state().closed
        {
            return Err(WorkspaceError::Fenced);
        }
        let budget = native.mount_budget();
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, SEED_READ_BYTES)])
            .map_err(native_freeze_error)?;
        let (native_id, planned_head, _, _) = record.native_completion_identities()?;
        if native_id != native.mapping.journal_id
            || planned_head != native.mapping.planned_head_layer_id
            || record.guard != native.mapping.old_guard
            || record.expected_binding != native.binding
            || record.source.frozen_view_token != native.canonical_receipt_digest()
            || !record.source.snapshot_backed
        {
            return Err(WorkspaceError::Fenced);
        }
        let key = seed_key(native_id);
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(std::slice::from_ref(&key), seed_limits(1))
            .await?;
        if values.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let seed = decode_seed(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if seed.mapping() != native.mapping
            || seed.binding()? != native.binding
            || seed.quiesced_journal.as_ref() != Some(&native.journal)
            || seed.canonical_quiesce != native.canonical
            || seed
                .packed_journal_id
                .is_some_and(|id| id != record.journal_id)
        {
            return Err(WorkspaceError::Fenced);
        }
        let (mut checks, deadline_ns) = native.authority_checks_before().await?;
        if checks.iter().any(|check| check.key == key) {
            return Err(WorkspaceError::Fenced);
        }
        checks.push(KvCheck {
            key: key.clone(),
            expected: values[0].clone(),
        });
        let mut linked = seed.clone();
        linked.packed_journal_id = Some(record.journal_id);
        let writes = if seed.packed_journal_id == Some(record.journal_id) {
            Vec::new()
        } else {
            vec![KvWrite::Put {
                key,
                value: encode_seed(&linked)?,
            }]
        };
        Ok(NativeJournalOwnerHandoff {
            checks,
            writes,
            deadline_ns,
            _permit: permit,
        })
    }
}
