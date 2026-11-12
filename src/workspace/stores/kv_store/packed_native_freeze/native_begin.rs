//! Bounded Prepare/Quiesced transactions in the actual Redis/TiKV domain.
//! The original source mapping stays fixed across unrelated generation changes.

use super::super::packed_writer_authority::{
    AdministrativeWriterTransition, PackedWriterAuthority, PackedWriterOwner, packed_writer_key,
};
use super::native_seed::{NativeFreezeSeed, decode_seed, encode_seed, seed_key};
use super::*;

struct BeginRead {
    keys: Vec<Vec<u8>>,
    values: Vec<Option<Vec<u8>>>,
    workspace: WorkspaceRecord,
    head: LayerRecord,
    lease: SnapshotLease,
    journal: Option<SealJournal>,
    seed: Option<NativeFreezeSeed>,
    now: i64,
}

fn begin_limits(count: usize) -> KvReadLimits {
    KvReadLimits {
        max_records: count,
        max_key_bytes: 1024,
        max_value_bytes: FREEZE_POINT_MAX_BYTES,
        max_total_bytes: FREEZE_POINT_MAX_BYTES,
        max_response_bytes: 64 << 10,
        // Up to two certified read-only lock continuations; never exceed the
        // 32-attempt point-read cap or change this batch's record/byte bounds.
        max_data_requests: count.saturating_add(2).min(32),
    }
}

fn begin_checks(read: &BeginRead) -> Vec<KvCheck> {
    read.keys
        .iter()
        .cloned()
        .zip(read.values.iter().cloned())
        .map(|(key, expected)| KvCheck { key, expected })
        .collect()
}

// A preexisting administrative source must still be this exact original
// lease/open. The full begin packet pins its head, binding and native journal.
fn native_begin_administrative_deadline(
    read: &BeginRead,
    checks: &[KvCheck],
) -> Result<i64, WorkspaceError> {
    let open: SnapshotOpenRecord = decode_open_value(
        read.values[14].as_deref().ok_or(WorkspaceError::Fenced)?,
        OPEN_RECORD_MAX_BYTES,
    )?;
    validate_open_record(&open, read.workspace.workspace_id)?;
    let key = packed_writer_key(read.workspace.workspace_id);
    let raw = checks
        .iter()
        .find(|check| check.key == key)
        .and_then(|check| check.expected.as_deref())
        .ok_or(WorkspaceError::Fenced)?;
    let writer = PackedWriterAuthority::decode(raw, read.workspace.workspace_id)?;
    if !matches!(writer.owner, Some(PackedWriterOwner::Administrative {
        lease_id, holder_generation, ref open_owner, open_generation, recovering: false,
    }) if lease_id == read.lease.lease_id
        && holder_generation == read.lease.holder_generation
        && open_owner == &open.owner_id
        && open_generation == open.generation)
        || open.state != SnapshotOpenState::Ready
        || open.recovery_required
        || open.expires_at_ns <= read.now
        || open.expires_at_ns < read.lease.expires_at_ns
    {
        return Err(WorkspaceError::Fenced);
    }
    Ok(read.lease.expires_at_ns.min(open.expires_at_ns))
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    async fn read_native_begin(
        &self,
        mapping: &PackedNativePlannedRotation,
        binding: &PackedLowerBindingRecord,
        predecessor: Option<&SealJournal>,
    ) -> Result<BeginRead, WorkspaceError> {
        let guard = &mapping.old_guard;
        let keys = vec![
            hot_journal_key(guard.workspace_id, mapping.journal_id),
            hot_workspace_key(guard.workspace_id),
            hot_layer_key(guard.expected_head_layer_id),
            hot_layer_key(mapping.old_layers[1].layer_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            hot_layer_key(mapping.planned_head_layer_id),
            packed_current_key(guard.workspace_id),
            packed_claim_key(guard.workspace_id),
            packed_history_key(guard.workspace_id, binding.binding.binding_version),
            packed_history_key(guard.workspace_id, 1),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            open_snapshot_recovery_key(guard.workspace_id),
            seed_key(mapping.journal_id),
            open_snapshot_key(guard.workspace_id),
            hot_journal_index_key(mapping.journal_id),
        ];
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, begin_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(native_freeze_error("invalid bounded begin response"));
        }
        begin_limits(keys.len()).validate_keys(&keys)?;
        let bytes = keys
            .iter()
            .zip(&values)
            .try_fold(0usize, |sum, (key, value)| {
                let length = value.as_ref().map_or(0, Vec::len);
                if length > FREEZE_POINT_MAX_BYTES {
                    return Err(native_freeze_error("begin value exceeds fixed tier"));
                }
                sum.checked_add(key.len())
                    .and_then(|sum| sum.checked_add(length))
                    .ok_or_else(|| native_freeze_error("begin aggregate overflow"))
            })?;
        if bytes > FREEZE_POINT_MAX_BYTES {
            return Err(native_freeze_error("begin aggregate exceeds fixed tier"));
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        let journal: Option<SealJournal> = values[0]
            .as_deref()
            .map(|raw| decode_open_value(raw, FREEZE_POINT_MAX_BYTES))
            .transpose()?;
        let journal_workspace: Option<WorkspaceId> = values[15]
            .as_deref()
            .map(|raw| decode_open_value(raw, OPEN_RECORD_MAX_BYTES))
            .transpose()?;
        let workspace: WorkspaceRecord = decode_open_value(required(1)?, FREEZE_POINT_MAX_BYTES)?;
        let head: LayerRecord = decode_open_value(required(2)?, FREEZE_POINT_MAX_BYTES)?;
        let base: LayerRecord = decode_open_value(required(3)?, FREEZE_POINT_MAX_BYTES)?;
        let lease: SnapshotLease = decode_open_value(required(4)?, FREEZE_POINT_MAX_BYTES)?;
        let current = decode_packed_pair(guard.workspace_id, &values[6], &values[7], &values[8])?
            .ok_or(WorkspaceError::Fenced)?;
        let anchor = PackedLowerBindingRecord::decode(required(9)?)?;
        let mirror: Option<SnapshotRecoveryRecord> = values[12]
            .as_deref()
            .map(|bytes| decode_open_value(bytes, OPEN_RECOVERY_MAX_BYTES))
            .transpose()?;
        let seed = values[13].as_deref().map(decode_seed).transpose()?;
        let mut expected_head = mapping.old_layers[0].clone();
        let expected_workspace_state = if predecessor.is_some() {
            expected_head.state = LayerState::Sealing;
            WorkspaceState::Sealing
        } else {
            WorkspaceState::Active
        };
        if head.schema_version != WORKSPACE_SCHEMA_VERSION
            || base.schema_version != WORKSPACE_SCHEMA_VERSION
            || workspace.workspace_id != guard.workspace_id
            || workspace.state != expected_workspace_state
            || workspace.active_lease != Some(guard.lease_id)
            || workspace.head_layer_id != guard.expected_head_layer_id
            || workspace.head_epoch != guard.expected_head_epoch
            || head.owner_workspace_id != Some(guard.workspace_id)
            || head != expected_head
            || base != mapping.old_layers[1]
            || lease.lease_id != guard.lease_id
            || lease.workspace_id != guard.workspace_id
            || lease.holder_generation != guard.holder_generation
            || lease.base_revision != binding.base_revision
            || !lease.writable
            || lease.state != LeaseState::Active
            || lease.expires_at_ns <= now
            || values[5].is_some()
            || &current != binding
            || anchor.workspace_id != guard.workspace_id
            || anchor.binding.binding_version != 1
            || mirror
                .as_ref()
                .is_some_and(|row| row.workspace_id != guard.workspace_id)
        {
            #[cfg(test)]
            eprintln!(
                "[packed-initial-composer-diag] stage=native-begin-source schema_matches={} workspace_identity={} workspace_state={} head_identity={} head_epoch={} owner_matches={} head_matches={} base_matches={} lease_identity={} lease_generation={} lease_base={} writable={} active={} lease_live={} planned_head_absent={} binding_matches={} anchor_identity={} anchor_initial={} mirror_identity={}",
                head.schema_version == WORKSPACE_SCHEMA_VERSION
                    && base.schema_version == WORKSPACE_SCHEMA_VERSION,
                workspace.workspace_id == guard.workspace_id,
                workspace.state == expected_workspace_state,
                workspace.head_layer_id == guard.expected_head_layer_id,
                workspace.head_epoch == guard.expected_head_epoch,
                head.owner_workspace_id == Some(guard.workspace_id),
                head == expected_head,
                base == mapping.old_layers[1],
                lease.lease_id == guard.lease_id && lease.workspace_id == guard.workspace_id,
                lease.holder_generation == guard.holder_generation,
                lease.base_revision == binding.base_revision,
                lease.writable,
                lease.state == LeaseState::Active,
                lease.expires_at_ns > now,
                values[5].is_none(),
                &current == binding,
                anchor.workspace_id == guard.workspace_id,
                anchor.binding.binding_version == 1,
                mirror
                    .as_ref()
                    .is_none_or(|row| row.workspace_id == guard.workspace_id)
            );
            return Err(WorkspaceError::Fenced);
        }
        binding.validate_for_guard(guard, &base)?;
        if predecessor.is_none() {
            checked_hot_guard(&workspace, &head, &lease, guard, now)?;
        }
        next_packed_root_generation(&values[10])?;
        layer_inventory_generation(&values[11])?;
        match predecessor {
            None => {
                if journal.is_some()
                    || journal_workspace.is_some()
                    || seed.is_some()
                    || mirror.as_ref().is_some_and(|row| row.incomplete)
                {
                    return Err(WorkspaceError::Fenced);
                }
            }
            Some(expected) => {
                if journal.as_ref() != Some(expected)
                    || journal_workspace != Some(guard.workspace_id)
                    || expected.phase != SealPhase::Prepare
                    || now < expected.updated_at_ns
                    || mirror.as_ref().is_none_or(|row| !row.incomplete)
                {
                    return Err(WorkspaceError::Fenced);
                }
                seed.as_ref()
                    .ok_or(WorkspaceError::Fenced)?
                    .validate_prepared_context(mapping, binding, expected)?;
            }
        }
        // The exact workspace transition Active -> Sealing excludes another
        // unfinished journal in the same workspace. Other workspaces share no
        // mutable control record with this packet.
        Ok(BeginRead {
            keys,
            values,
            workspace,
            head,
            lease,
            journal,
            seed,
            now,
        })
    }

    async fn commit_native_begin(
        &self,
        read: &BeginRead,
        writes: &[KvWrite],
        clean: Option<&super::super::packed_admin::PackedCleanPublicationAuthority<B>>,
        initial: Option<&super::super::packed_admin::PackedInitialBootstrapAuthority<B>>,
    ) -> Result<bool, WorkspaceError> {
        let mut checks = begin_checks(read);
        if clean.is_some() && initial.is_some() {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(initial) = initial {
            let (additional, deadline) =
                initial
                    .authority_checks_before()
                    .await
                    .inspect_err(|_error| {
                        #[cfg(test)]
                        native_authority_diagnostic("initial-begin-authority", _error);
                    })?;
            if deadline < read.lease.expires_at_ns {
                #[cfg(test)]
                eprintln!(
                    "[packed-initial-composer-diag] stage=native-begin-deadline deadline_covers_lease=false"
                );
                return Err(WorkspaceError::Fenced);
            }
            for added in additional {
                if let Some(old) = checks.iter().find(|check| check.key == added.key) {
                    if old.expected != added.expected {
                        return Err(WorkspaceError::Busy);
                    }
                } else {
                    checks.push(added);
                }
            }
        }
        if let Some(clean) = clean {
            let (additional, deadline) =
                clean
                    .authority_checks_before()
                    .await
                    .inspect_err(|_error| {
                        #[cfg(test)]
                        native_authority_diagnostic("native-begin-clean-authority", _error);
                    })?;
            if deadline < read.lease.expires_at_ns {
                return Err(WorkspaceError::Fenced);
            }
            for added in additional {
                if let Some(old) = checks.iter().find(|check| check.key == added.key) {
                    if old.expected != added.expected {
                        #[cfg(test)]
                        eprintln!(
                            "[packed-native-begin-diag] stage=clean-overlap key={}",
                            String::from_utf8_lossy(&added.key)
                        );
                        // Both packets were read-only. Re-enter the existing
                        // bounded begin loop with fresh source/authority reads;
                        // no mutation has been submitted or can be replayed.
                        return Ok(false);
                    }
                } else {
                    checks.push(added);
                }
            }
        }
        let mut writes = writes.to_vec();
        let mut deadline = read.lease.expires_at_ns;
        // Only the genuine first install's still-live InitialSource can enter
        // the plain original-Q route. Its exact source packet, absent open,
        // new Ready open and PWA successor all join this actual Prepare CAS.
        // Callers keep their actual VFS drain fence for subsequent capture.
        let _writer_owner = if clean.is_none() && initial.is_none() {
            if read.values[14].is_none() {
                if read.journal.is_some() {
                    return Err(WorkspaceError::Fenced);
                }
                let open = SnapshotOpenRecord {
                    workspace_id: read.workspace.workspace_id,
                    owner_id: format!("packed/native-stage/{}", read.lease.lease_id),
                    generation: 1,
                    expires_at_ns: read.lease.expires_at_ns,
                    state: SnapshotOpenState::Ready,
                    recovery_required: false,
                };
                writes.push(put(open_snapshot_key(open.workspace_id), &open)?);
                // Claiming the first administrative writer is a maintenance
                // transition: it keeps the exact catalog header in its check
                // set so a concurrent migrate cannot interleave with Prepare.
                if !checks
                    .iter()
                    .any(|check| check.key.as_slice() == CONTROL_KEY)
                {
                    let control = [CONTROL_KEY.to_vec()];
                    let (values, _) = self
                        .backend
                        .get_many_consistent_with_time_bounded(&control, begin_limits(1))
                        .await?;
                    if values.len() != 1 || values[0].is_none() {
                        return Err(WorkspaceError::Fenced);
                    }
                    checks.push(KvCheck {
                        key: CONTROL_KEY.to_vec(),
                        expected: values[0].clone(),
                    });
                }
                Some(
                    self.prepare_administrative_packed_writer(
                        read.workspace.workspace_id,
                        AdministrativeWriterTransition::ClaimInitial,
                        &mut checks,
                        &mut writes,
                    )
                    .await?,
                )
            } else {
                let owner = self
                    .authenticate_administrative_packed_writer(
                        read.workspace.workspace_id,
                        &mut checks,
                    )
                    .await?;
                deadline = native_begin_administrative_deadline(read, &checks)?;
                Some(owner)
            }
        } else {
            None
        };
        let (native_epochs_match, _native_holds) = self
            .prepare_native_owner_cas_with_epoch_status(&mut checks, &mut writes)
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                native_authority_diagnostic("native-begin-native-holds", _error);
            })?;
        if !native_epochs_match {
            return Ok(false);
        }
        let packet = self
            .prepare_topology_envelope(checks, writes, Some(deadline))
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
                .ok_or_else(|| native_freeze_error("begin write lacks exact predecessor"))?;
            expected[index] = value;
        }
        let bytes = keys
            .iter()
            .zip(&expected)
            .try_fold(0usize, |sum, (key, value)| {
                let length = value.as_ref().map_or(0, Vec::len);
                if length > FREEZE_POINT_MAX_BYTES {
                    return Err(native_freeze_error("begin successor exceeds fixed tier"));
                }
                sum.checked_add(key.len())
                    .and_then(|sum| sum.checked_add(length))
                    .ok_or_else(|| native_freeze_error("begin successor aggregate overflow"))
            })?;
        if bytes > FREEZE_POINT_MAX_BYTES {
            return Err(native_freeze_error(
                "begin successor aggregate exceeds fixed tier",
            ));
        }
        match self.commit_prepared_topology_packet(&packet).await {
            Ok(committed) => {
                #[cfg(test)]
                if !committed {
                    eprintln!("[packed-native-begin-diag] stage=begin-cas result=false");
                }
                Ok(committed)
            }
            Err(error @ WorkspaceError::Backend(_)) => {
                // An uncertain reply authorizes no second mutation. Confirm
                // the exact attempted successor and a live timed no-op only.
                let confirmed = async {
                    let (actual, now) = self
                        .backend
                        .get_many_consistent_with_time_bounded(&keys, begin_limits(keys.len()))
                        .await?;
                    if actual != expected || now <= 0 || now >= deadline {
                        return Ok(false);
                    }
                    let checks: Vec<_> = keys
                        .iter()
                        .cloned()
                        .zip(actual)
                        .map(|(key, expected)| KvCheck { key, expected })
                        .collect();
                    self.backend
                        .compare_and_swap_before(&checks, &[], deadline)
                        .await
                }
                .await;
                match confirmed {
                    Ok(true) => Ok(true),
                    _ => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn bounded_native_begin_catalog(
        &self,
        mapping: &PackedNativePlannedRotation,
        clean: Option<&super::super::packed_admin::PackedCleanPublicationAuthority<B>>,
        initial: Option<&super::super::packed_admin::PackedInitialBootstrapAuthority<B>>,
    ) -> Result<(PackedLowerBindingRecord, NativeQuiesceRead), WorkspaceError> {
        let keys = [packed_current_key(mapping.old_guard.workspace_id)];
        let (current, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, begin_limits(1))
            .await?;
        if current.len() != 1 {
            return Err(native_freeze_error("begin binding routing count"));
        }
        let binding =
            PackedLowerBindingRecord::decode(current[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        binding.validate_for_guard(&mapping.old_guard, &mapping.old_layers[1])?;
        let mut prepared = None;
        for _ in 0..CAS_MAX_RETRIES {
            let read = self
                .read_native_begin(mapping, &binding, None)
                .await
                .inspect_err(|_error| {
                    #[cfg(test)]
                    native_authority_diagnostic("initial-begin-read-prepare", _error);
                })?;
            let mut workspace = read.workspace.clone();
            workspace.state = WorkspaceState::Sealing;
            workspace.updated_at_ns = read.now;
            let mut head = read.head.clone();
            head.state = LayerState::Sealing;
            let journal = SealJournal {
                journal_id: mapping.journal_id,
                workspace_id: mapping.old_guard.workspace_id,
                old_head_layer_id: mapping.old_guard.expected_head_layer_id,
                expected_head_epoch: mapping.old_guard.expected_head_epoch,
                phase: SealPhase::Prepare,
                pending_bytes: 0,
                delta_digest: None,
                root_hash: None,
                new_head_layer_id: Some(mapping.planned_head_layer_id),
                last_error: None,
                created_at_ns: read.now,
                updated_at_ns: read.now,
            };
            let seed = NativeFreezeSeed::prepared(mapping, &binding, &journal)?;
            let writes = vec![
                put(hot_workspace_key(workspace.workspace_id), &workspace)?,
                put(hot_layer_key(head.layer_id), &head)?,
                put(
                    hot_journal_key(journal.workspace_id, journal.journal_id),
                    &journal,
                )?,
                put(
                    hot_journal_index_key(journal.journal_id),
                    &journal.workspace_id,
                )?,
                KvWrite::Put {
                    key: seed_key(mapping.journal_id),
                    value: encode_seed(&seed)?,
                },
                put(
                    open_snapshot_recovery_key(workspace.workspace_id),
                    &SnapshotRecoveryRecord {
                        workspace_id: workspace.workspace_id,
                        incomplete: true,
                    },
                )?,
            ];
            if self
                .commit_native_begin(&read, &writes, clean, initial)
                .await
                .inspect_err(|_error| {
                    #[cfg(test)]
                    native_authority_diagnostic("initial-begin-commit-prepare", _error);
                })?
            {
                prepared = Some(journal);
                break;
            }
            tokio::task::yield_now().await;
        }
        let prepared = prepared.ok_or(WorkspaceError::Busy)?;
        for _ in 0..CAS_MAX_RETRIES {
            let read = self
                .read_native_begin(mapping, &binding, Some(&prepared))
                .await
                .inspect_err(|_error| {
                    #[cfg(test)]
                    native_authority_diagnostic("initial-begin-read-quiesced", _error);
                })?;
            let mut quiesced = read.journal.clone().ok_or(WorkspaceError::Fenced)?;
            quiesced.phase = SealPhase::Quiesced;
            quiesced.updated_at_ns = read.now;
            let seed = read
                .seed
                .as_ref()
                .ok_or(WorkspaceError::Fenced)?
                .with_quiesced(&quiesced)?;
            if self
                .commit_native_begin(
                    &read,
                    &[
                        put(
                            hot_journal_key(quiesced.workspace_id, quiesced.journal_id),
                            &quiesced,
                        )?,
                        KvWrite::Put {
                            key: seed_key(mapping.journal_id),
                            value: encode_seed(&seed)?,
                        },
                    ],
                    clean,
                    initial,
                )
                .await
                .inspect_err(|_error| {
                    #[cfg(test)]
                    native_authority_diagnostic("initial-begin-commit-quiesced", _error);
                })?
            {
                let read = self
                    .read_packed_native_quiesce(mapping, &binding)
                    .await
                    .inspect_err(|_error| {
                        #[cfg(test)]
                        native_authority_diagnostic("initial-begin-final-phase", _error);
                    })?;
                return Ok((binding, read));
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }
}
