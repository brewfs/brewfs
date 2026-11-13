//! Continue only actual captured-source upload holds in the existing registry.
//! A dispatched unknown PUT is never reissued. Complete remote bytes authenticate
//! its original identity before the same exact registry completion transition.

use super::super::native_publication::NativeJournalAuthority;
use super::*;
use crate::workspace::stores::kv_store::packed_native_freeze::PackedNativeQuiesceFence;

#[cfg(test)]
impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Pure test inspection uses the production typed decoders on the packet
    /// submitted successfully to Redis/TiKV. It creates no rows or authority.
    pub(crate) fn assert_native_resume_quarantine_packet(
        old: &PackedJournalRecord,
        next: &PackedJournalRecord,
        occurrence: &PackedJournalObject,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) {
        let checked = |key: &[u8]| {
            checks
                .iter()
                .find(|row| row.key == key)
                .expect("actual predecessor must be checked")
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
                .expect("same actual CAS must write successor")
        };
        let old_root_key = registry_root_key(old.source.staging_id);
        let before = RootRow::decode(checked(&old_root_key).unwrap()).unwrap();
        let retained = RootRow::decode(put(&old_root_key)).unwrap();
        assert_eq!(before.state, RootState::Staging);
        assert_eq!(retained.state, RootState::AbortedRetained);
        assert_eq!(retained.journal_id, old.journal_id);
        assert_eq!(retained.incarnation, old.source.staging_id);
        assert_eq!(retained.members, before.members);
        assert_eq!(retained.pending_puts, before.pending_puts);
        assert!(retained.pending_puts > 0);
        let fresh = RootRow::decode(put(&registry_root_key(next.source.staging_id))).unwrap();
        assert_eq!(fresh.state, RootState::Staging);
        assert_eq!(fresh.journal_id, next.journal_id);
        assert_eq!(fresh.incarnation, next.source.staging_id);
        assert_eq!((fresh.members, fresh.pending_puts), (0, 0));
        let object_key_global = registry_object_key(&occurrence.reference);
        let member_key = registry_member_key(&occurrence.reference, old.source.staging_id);
        let object = ObjectRow::decode(checked(&object_key_global).unwrap()).unwrap();
        let member = MemberRow::decode(checked(&member_key).unwrap()).unwrap();
        assert_eq!(object.state, ObjectState::Live);
        assert!(
            object.pending_puts > 0 && member.pending_put && member.dispatched && member.retained
        );
        assert_eq!(member.reference, occurrence.reference);
        assert!(!occurrence.uploaded && !occurrence.readback_recorded);
        for key in [
            object_key_global,
            member_key,
            object_key(old.journal_id, occurrence.ordinal),
            registry_reverse_key(old.source.staging_id, occurrence.ordinal),
            identity_key(old.journal_id, &occurrence.reference.key),
            ACTIVE_COUNT_KEY.to_vec(),
        ] {
            assert!(
                writes.iter().all(|row| match row {
                    KvWrite::Put { key: actual, .. } | KvWrite::Delete { key: actual } =>
                        *actual != key,
                }),
                "old pending/inventory authority and active count stay exact"
            );
        }
        assert!(active_count(&checked(ACTIVE_COUNT_KEY).map(<[u8]>::to_vec)).unwrap() > 0);
        assert!(writes.iter().any(
            |row| matches!(row, KvWrite::Delete { key } if *key == active_key(old.journal_id))
        ));
        assert_eq!(put(&active_key(next.journal_id)), next.encode().unwrap());
        assert_eq!(put(&journal_key(next.journal_id)), next.encode().unwrap());
        let old_successor = PackedJournalRecord::decode(put(&journal_key(old.journal_id))).unwrap();
        assert_eq!(old_successor.phase, PackedJournalPhase::Aborted);
        assert_eq!(old_successor.object_count, old.object_count);
        assert_eq!(old_successor.inventory_digest, old.inventory_digest);
        Self::assert_native_resume_route_packet(old, next, checks, writes);
        for row in writes {
            let key = match row {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
            };
            assert!(
                checks.iter().any(|check| check.key == *key),
                "every successor shares this exact CAS"
            );
        }
    }
}

pub(super) enum NativeUploadContinuation {
    Absent,
    Completed(OwnedPackedJournal<PackedJournalRecord>),
    Reserved(OwnedPackedJournal<PackedJournalRecord>, PackedUploadGuard),
}

impl<B: WorkspaceKvBackend + 'static> KvWorkspaceStore<B> {
    /// This private before-PUT consumer receives a real captured native fence
    /// and the object deterministically reproduced from that exact source.
    /// Registry flags alone cannot reach its completion mutation.
    pub(in crate::workspace::stores::kv_store::packed_journal::registry) async fn resume_captured_native_upload<
        O: ObjectBackend + Clone,
    >(
        &self,
        expected: &PackedJournalRecord,
        reference: &SnapshotObjectRef,
        native: &PackedNativeQuiesceFence<B>,
        client: &ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<NativeUploadContinuation, WorkspaceError> {
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        self.configure_packed_reader_pin_budget(budget.clone())?;
        if !native.belongs_to_store(self)
            || !Arc::ptr_eq(&native.mount_budget(), budget)
            || expected.phase != PackedJournalPhase::Building
            || expected.source.frozen_view_token != native.canonical_receipt_digest()
        {
            return Err(WorkspaceError::Fenced);
        }
        let authority = NativeJournalAuthority::from_captured(native);
        let keys = vec![
            registry_object_key(reference),
            registry_member_key(reference, expected.source.staging_id),
            registry_root_key(expected.source.staging_id),
            identity_key(expected.journal_id, &reference.key),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != keys.len() {
            return Err(WorkspaceError::Fenced);
        }
        let root = RootRow::decode(values[2].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        root.check_staging(expected)?;
        if values[1].is_none() {
            if values[3].is_some() {
                return Err(WorkspaceError::Fenced);
            }
            return Ok(NativeUploadContinuation::Absent);
        }
        let object = ObjectRow::decode(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        let member = MemberRow::decode(values[1].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if object.reference != *reference
            || object.state != ObjectState::Live
            || object.memberships == 0
            || member.reference != *reference
            || member.journal_id != expected.journal_id
            || member.incarnation != expected.source.staging_id
            || member.ordinal >= expected.object_count
            || member.put_id.is_nil()
            || member.adopted
            || !member.retained
            || values[3].as_deref() != Some(member.ordinal.to_le_bytes().as_slice())
        {
            return Err(WorkspaceError::Fenced);
        }
        let occurrence = self
            .reopen_packed_object(expected, member.ordinal, budget)
            .await?;
        if occurrence.reference != *reference
            || member.pending_put == occurrence.uploaded
            || (!member.pending_put && !member.dispatched)
            || (!occurrence.uploaded && occurrence.readback_recorded)
        {
            return Err(WorkspaceError::Fenced);
        }
        let reverse_key = registry_reverse_key(expected.source.staging_id, member.ordinal);
        let reverse = self
            .packed_journal_values(std::slice::from_ref(&reverse_key))
            .await?;
        if reverse.len() != 1
            || reverse[0].as_deref()
                != Some(
                    reference
                        .encode_value()
                        .map_err(journal_budget_error)?
                        .as_slice(),
                )
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut changes = keys
            .into_iter()
            .zip(values)
            .map(|(key, value)| (key, value.clone(), value))
            .collect::<Vec<_>>();
        changes.push((reverse_key, reverse[0].clone(), reverse[0].clone()));
        changes.push((
            object_key(expected.journal_id, member.ordinal),
            Some(occurrence.encode()?),
            Some(occurrence.encode()?),
        ));
        if member.dispatched {
            // Completed and unknown-dispatched cases both authenticate the
            // actual full immutable object. Absence leaves the pending hold.
            let _remote_owner = budget
                .admit(&[(SnapshotBudgetPool::Metadata, 128 << 10)])
                .map_err(journal_budget_error)?;
            let observer = budget
                .read_observer(client.read_observer())
                .map_err(journal_budget_error)?;
            let client = client.clone().with_read_observer(
                observer,
                crate::cadapter::read_observer::Engine::Packed,
                crate::cadapter::read_observer::Phase::Startup,
                crate::cadapter::read_observer::Origin::Demand,
            );
            let class = crate::cadapter::read_observer::ReadClass::PublicationVerification;
            if client
                .typed_object_size(class, &reference.key)
                .await
                .map_err(|error| WorkspaceError::Backend(error.to_string()))?
                != Some(reference.object_len)
            {
                return Err(WorkspaceError::Fenced);
            }
            let mut digest = Sha256::new();
            let mut offset = 0;
            while offset < reference.object_len {
                if budget.state().closed {
                    return Err(WorkspaceError::Fenced);
                }
                let length = (reference.object_len - offset).min(64 << 10);
                let bytes = client
                    .typed_bounded_range(class, &reference.key, offset, length)
                    .await
                    .map_err(|error| WorkspaceError::Backend(error.to_string()))?;
                if bytes.len() as u64 != length {
                    return Err(WorkspaceError::Fenced);
                }
                digest.update(&bytes);
                offset += length;
            }
            if <[u8; 32]>::from(digest.finalize()) != reference.digest {
                return Err(WorkspaceError::Fenced);
            }
        }
        // The full old registry packet and occurrence are in the same actual
        // timed source CAS after remote authentication. A changed dispatcher,
        // object membership, source owner or root cannot reuse this result.
        self.packed_journal_write_under(Some(expected), expected, &changes, None, Some(&authority))
            .await?;
        if !member.pending_put {
            return Ok(NativeUploadContinuation::Completed(
                expected.clone().retain(owner)?,
            ));
        }
        let guard = PackedUploadGuard {
            reference: reference.clone(),
            incarnation: member.incarnation,
            journal_id: member.journal_id,
            ordinal: member.ordinal,
            put_id: member.put_id,
            _permit: budget
                .admit(&[(SnapshotBudgetPool::Metadata, 32 << 10)])
                .map_err(journal_budget_error)?,
        };
        if member.dispatched {
            // Never dispatch again. A verified actual remote object supplies
            // the narrow success fact required by the existing completion CAS.
            let next = self
                .finish_packed_upload_under(expected, guard, budget, Some(&authority))
                .await?;
            Ok(NativeUploadContinuation::Completed(next))
        } else {
            // Its actual dispatch flag is still false. The existing single-use
            // dispatch CAS must succeed before this original guard sends PUT.
            Ok(NativeUploadContinuation::Reserved(
                expected.clone().retain(owner)?,
                guard,
            ))
        }
    }
}
