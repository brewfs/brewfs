//! Never replay an unknown PUT. Retain its attempt and route a fresh one atomically.

use super::super::native_publication::{NativeJournalAuthority, append_exact_checks};
use super::*;
use crate::workspace::stores::kv_store::packed_native_freeze::PackedNativeRecoveryReadFence;

fn quarantine_read_class(
    reference: &SnapshotObjectRef,
) -> Result<crate::cadapter::read_observer::ReadClass, WorkspaceError> {
    use crate::cadapter::read_observer::ReadClass;
    Ok(match reference.kind {
        SnapshotObjectKind::GroupContainer => ReadClass::GroupMetadata,
        SnapshotObjectKind::LargeData => ReadClass::ExternalPayload,
        kind => crate::workspace::packed::snapshot::page_read_class(kind)
            .map_err(journal_budget_error)?,
    })
}

impl<B: WorkspaceKvBackend + 'static> KvWorkspaceStore<B> {
    /// The owned driver retains old source authority through the actual CAS
    /// and its exact successor confirmation even if its caller is cancelled.
    pub(crate) async fn quarantine_missing_native_attempt_and_reissue<
        O: ObjectBackend + Clone + 'static,
    >(
        self: &Arc<Self>,
        recovery: Arc<PackedNativeRecoveryReadFence<B>>,
        client: &ObjectClient<O>,
        max_objects: u64,
        cancel: &CancellationToken,
    ) -> Result<Arc<PackedNativeRecoveryReadFence<B>>, WorkspaceError> {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| journal_error("packed recovery requires a Tokio runtime"))?;
        let store = self.clone();
        let client = client.clone();
        let cancel = cancel.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        handle.spawn(async move {
            let result = store
                .quarantine_missing_native_attempt_owned(recovery, client, max_objects, cancel)
                .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| journal_error("packed quarantine driver stopped"))?
    }

    async fn quarantine_missing_native_attempt_owned<O: ObjectBackend + Clone>(
        self: Arc<Self>,
        recovery: Arc<PackedNativeRecoveryReadFence<B>>,
        client: ObjectClient<O>,
        max_objects: u64,
        cancel: CancellationToken,
    ) -> Result<Arc<PackedNativeRecoveryReadFence<B>>, WorkspaceError> {
        let basis = recovery.basis().ok_or(WorkspaceError::Fenced)?;
        let expected = basis.record();
        let budget = recovery.native_quiesce().mount_budget();
        if !Arc::ptr_eq(&self, recovery.store())
            || budget.state().closed
            || max_objects == 0
            || max_objects > MAX_OBJECTS
            || cancel.is_cancelled()
        {
            return Err(WorkspaceError::Fenced);
        }
        if expected.phase != PackedJournalPhase::Building {
            return Ok(recovery);
        }
        if expected.commit_target.is_some() || expected.object_count > max_objects {
            return Err(WorkspaceError::Fenced);
        }
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let observer = budget
            .read_observer(client.read_observer())
            .map_err(journal_budget_error)?;
        let client = client.with_read_observer(
            observer,
            crate::cadapter::read_observer::Engine::Packed,
            crate::cadapter::read_observer::Phase::Startup,
            crate::cadapter::read_observer::Origin::Demand,
        );
        let _remote_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 128 << 10)])
            .map_err(journal_budget_error)?;
        recovery.validate().await?;
        let mut inventory = inventory_start();
        let mut missing: Option<Vec<KvCheck>> = None;
        for ordinal in 0..expected.object_count {
            if cancel.is_cancelled() || budget.state().closed {
                return Err(WorkspaceError::Fenced);
            }
            let occurrence = self
                .reopen_packed_object(expected, ordinal, &budget)
                .await?;
            let reference = &occurrence.reference;
            inventory = inventory_append(inventory, ordinal, reference)?;
            let keys = vec![
                registry_object_key(reference),
                registry_member_key(reference, expected.source.staging_id),
                registry_root_key(expected.source.staging_id),
                registry_reverse_key(expected.source.staging_id, ordinal),
                identity_key(expected.journal_id, &reference.key),
                object_key(expected.journal_id, ordinal),
                journal_key(expected.journal_id),
            ];
            let values = self.packed_journal_values(&keys).await?;
            if values.len() != keys.len()
                || values[5].as_deref() != Some(occurrence.encode()?.as_slice())
                || values[6].as_deref() != Some(expected.encode()?.as_slice())
            {
                return Err(WorkspaceError::Fenced);
            }
            let object = ObjectRow::decode(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
            let member = MemberRow::decode(values[1].as_deref().ok_or(WorkspaceError::Fenced)?)?;
            let root = RootRow::decode(values[2].as_deref().ok_or(WorkspaceError::Fenced)?)?;
            root.check_staging(expected)?;
            if object.reference != *reference
                || object.state != ObjectState::Live
                || object.memberships == 0
                || member.reference != *reference
                || member.journal_id != expected.journal_id
                || member.incarnation != expected.source.staging_id
                || member.ordinal != ordinal
                || member.put_id.is_nil()
                || member.adopted
                || !member.retained
                || member.pending_put == occurrence.uploaded
                || (!member.pending_put && !member.dispatched)
                || (!occurrence.uploaded && occurrence.readback_recorded)
                || values[3].as_deref()
                    != Some(
                        reference
                            .encode_value()
                            .map_err(journal_budget_error)?
                            .as_slice(),
                    )
                || values[4].as_deref() != Some(ordinal.to_le_bytes().as_slice())
            {
                return Err(WorkspaceError::Fenced);
            }
            if member.dispatched {
                let class = quarantine_read_class(reference)?;
                let size = client
                    .typed_object_size(class, &reference.key)
                    .await
                    .map_err(|error| WorkspaceError::Backend(error.to_string()))?;
                if size.is_none() && member.pending_put {
                    if missing.is_none() {
                        missing = Some(
                            keys.into_iter()
                                .zip(values)
                                .map(|(key, expected)| KvCheck { key, expected })
                                .collect(),
                        );
                    }
                } else {
                    if size != Some(reference.object_len) {
                        return Err(WorkspaceError::Fenced);
                    }
                    let mut digest = Sha256::new();
                    let mut offset = 0;
                    while offset < reference.object_len {
                        if cancel.is_cancelled() || budget.state().closed {
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
            }
        }
        if inventory != expected.inventory_digest {
            return Err(WorkspaceError::Fenced);
        }
        let Some(missing) = missing else {
            return Ok(recovery);
        };
        // Remote absence is not terminal evidence. Leave every old inventory
        // object, global membership and pending operation intact permanently.
        let mut aborted = expected.clone();
        aborted.revision = increment(aborted.revision)?;
        aborted.phase = PackedJournalPhase::Aborted;
        aborted.abort_reason =
            "unknown dispatched PUT has no authenticated remote object; retained".into();
        let mut next = expected.clone();
        next.journal_id = JournalId::new();
        next.revision = 1;
        next.source.staging_id = Uuid::new_v4();
        next.source.staging_prefix = format!(
            "packed/staging/{}/{}",
            next.journal_id, next.source.staging_id
        );
        next.object_count = 0;
        next.inventory_digest = inventory_start();
        next.full_proof_digest = [0; 32];
        next.graph_receipt = None;
        next.abort_reason.clear();
        aborted.validate()?;
        next.validate()?;
        let route = self
            .prepare_native_quarantine_route_successor(
                &recovery,
                next.journal_id,
                next.source.staging_id,
            )
            .await?;
        let authority = NativeJournalAuthority::from_captured(recovery.native_quiesce());
        let extras = vec![
            active_key(expected.journal_id),
            ACTIVE_COUNT_KEY.to_vec(),
            JOURNAL_FEATURE_KEY.to_vec(),
            registry_root_key(expected.source.staging_id),
            journal_key(next.journal_id),
            active_key(next.journal_id),
            registry_root_key(next.source.staging_id),
        ];
        let mut first: Option<Vec<KvCheck>> = None;
        let mut first_deadline = route.handoff.deadline_ns;
        let mut previous_root = None;
        for attempt in 0..3 {
            if cancel.is_cancelled() || budget.state().closed {
                return Err(WorkspaceError::Fenced);
            }
            let mut read = self
                .native_staged_journal_authorities(expected, &extras, &authority)
                .await?;
            if read.values[10].as_deref() != Some(expected.encode()?.as_slice())
                || read.values[12] != read.values[10]
                || active_count(&read.values[13])? == 0
                || read.values[14].as_deref() != Some(b"PPJ3")
                || read.values[16..19].iter().any(Option::is_some)
            {
                return Err(WorkspaceError::Busy);
            }
            append_exact_checks(&mut read.checks, missing.clone())?;
            append_exact_checks(&mut read.checks, route.handoff.checks.clone())?;
            let old_root = recovery_root_value(expected, &aborted, false, &read.values[15])?
                .ok_or(WorkspaceError::Fenced)?;
            let mut writes = vec![
                KvWrite::Put {
                    key: journal_key(expected.journal_id),
                    value: aborted.encode()?,
                },
                KvWrite::Delete {
                    key: active_key(expected.journal_id),
                },
                KvWrite::Put {
                    key: registry_root_key(expected.source.staging_id),
                    value: old_root,
                },
                KvWrite::Put {
                    key: journal_key(next.journal_id),
                    value: next.encode()?,
                },
                KvWrite::Put {
                    key: active_key(next.journal_id),
                    value: next.encode()?,
                },
                KvWrite::Put {
                    key: registry_root_key(next.source.staging_id),
                    value: RootRow::for_journal(&next).encode()?,
                },
                put(
                    PACKED_ROOT_GENERATION_KEY.to_vec(),
                    &next_packed_root_generation(&read.values[8])?,
                )?,
            ];
            writes.extend(route.handoff.writes.iter().cloned());
            // One active attempt replaces one active attempt. The exact count
            // remains checked; native source holds see both marker successors.
            let _holds = self
                .prepare_native_owner_cas(&mut read.checks, &mut writes)
                .await?;
            first_deadline = first_deadline.min(read.authority_deadline_ns);
            let current_root = next_packed_root_generation(&read.values[8])? - 1;
            if let Some(original) = &first {
                if read.checks.len() != original.len()
                    || original.iter().any(|old| {
                        !read.checks.iter().any(|fresh| {
                            fresh.key == old.key
                                && (old.key.as_slice() == PACKED_ROOT_GENERATION_KEY
                                    || fresh.expected == old.expected)
                        })
                    })
                {
                    return Err(WorkspaceError::Busy);
                }
                if previous_root.is_none_or(|previous| current_root <= previous) {
                    return Err(WorkspaceError::Busy);
                }
            } else {
                first = Some(read.checks.clone());
            }
            previous_root = Some(current_root);
            let mut successor = read.checks.clone();
            for write in &writes {
                let (key, value) = match write {
                    KvWrite::Put { key, value } => (key, Some(value.clone())),
                    KvWrite::Delete { key } => (key, None),
                };
                successor
                    .iter_mut()
                    .find(|check| check.key == *key)
                    .ok_or(WorkspaceError::Fenced)?
                    .expected = value;
            }
            let limits = KvReadLimits {
                max_records: 64,
                max_key_bytes: 1024,
                max_data_requests: 64,
                ..journal_point_limits()
            };
            let bytes = successor.iter().try_fold(0usize, |sum, check| {
                let length = check.expected.as_ref().map_or(0, Vec::len);
                if check.key.len() > limits.max_key_bytes || length > limits.max_value_bytes {
                    return Err(WorkspaceError::Fenced);
                }
                sum.checked_add(check.key.len())
                    .and_then(|sum| sum.checked_add(length))
                    .ok_or(WorkspaceError::Fenced)
            })?;
            if successor.len() > limits.max_records || bytes > limits.max_total_bytes {
                return Err(WorkspaceError::Fenced);
            }
            match self
                .backend
                .compare_and_swap_before(&read.checks, &writes, first_deadline)
                .await
            {
                Ok(true) => break,
                Ok(false) if attempt < 2 => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(false) => return Err(WorkspaceError::Busy),
                Err(error) => {
                    // An uncertain reply permits only an exact read-only
                    // successor check. No mutation is submitted a second time.
                    let keys = successor
                        .iter()
                        .map(|check| check.key.clone())
                        .collect::<Vec<_>>();
                    let confirmation = self
                        .backend
                        .get_many_consistent_with_time_bounded(&keys, limits)
                        .await;
                    match confirmation {
                        Ok((actual, now))
                            if actual.len() == successor.len()
                                && now > 0
                                && now < first_deadline
                                && actual
                                    .iter()
                                    .zip(&successor)
                                    .all(|(value, check)| value == &check.expected) =>
                        {
                            if !self
                                .backend
                                .compare_and_swap_before(&successor, &[], first_deadline)
                                .await?
                            {
                                return Err(error);
                            }
                        }
                        _ => return Err(error),
                    }
                    break;
                }
            }
        }
        let fresh_basis = self
            .inspect_native_packed_recovery_basis(&next, &budget)
            .await?;
        if let Some(owner) = route.reissue_owner {
            let claim = self
                .claim_native_packed_recovery(fresh_basis, owner, budget.clone())
                .await?;
            let fresh_basis = self
                .inspect_native_packed_recovery_basis(&next, &budget)
                .await?;
            self.reissue_native_source_read_claimed(fresh_basis, claim, budget)
                .await
        } else {
            self.reissue_native_source_read(fresh_basis, budget).await
        }
    }
}
