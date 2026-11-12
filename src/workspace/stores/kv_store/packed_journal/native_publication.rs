//! Actual native staging and complete publication on the shared KV substrate.
//! Durable recovery facts do not substitute for a newly captured source proof.

use super::native_rebind::PackedNativeJournalBasis;
use super::*;
use crate::workspace::stores::kv_store::packed_native_freeze::PackedNativeQuiesceFence;

fn bounded_native_value<T: serde::de::DeserializeOwned>(
    raw: &Option<Vec<u8>>,
) -> Result<T, WorkspaceError> {
    decode_open_value(raw.as_deref().ok_or(WorkspaceError::Fenced)?, RECORD_LIMIT)
}

#[cfg(target_os = "linux")]
mod complete_driver;
#[cfg(target_os = "linux")]
pub(crate) use complete_driver::{
    NativePublicationBuildOptions, NativePublicationPreparationFailure,
};

pub(super) enum NativeJournalAuthority<'a, B: WorkspaceKvBackend + 'static> {
    Quiesced(&'a PackedNativeQuiesceFence<B>),
    Recovered(&'a PackedNativeQuiesceFence<B>),
    #[cfg(target_os = "linux")]
    Hashed(&'a crate::workspace::stores::kv_store::packed_native_freeze::PackedNativePhaseFence<B>),
}
impl<'a, B: WorkspaceKvBackend + 'static> NativeJournalAuthority<'a, B> {
    pub(super) fn from_captured(fence: &'a PackedNativeQuiesceFence<B>) -> Self {
        if fence.recovery_basis().is_some() {
            Self::Recovered(fence)
        } else {
            Self::Quiesced(fence)
        }
    }

    pub(super) fn original(&self) -> &PackedNativeQuiesceFence<B> {
        match self {
            Self::Quiesced(fence) | Self::Recovered(fence) => fence,
            #[cfg(target_os = "linux")]
            Self::Hashed(fence) => fence.native_quiesce(),
        }
    }
    async fn checks(&self) -> Result<(Vec<KvCheck>, i64), WorkspaceError> {
        match self {
            Self::Quiesced(fence) => fence.authority_checks_before().await,
            Self::Recovered(fence) => fence.captured_recovery_checks_before().await,
            #[cfg(target_os = "linux")]
            Self::Hashed(fence) if fence.is_hashed() => fence.authority_checks_before().await,
            #[cfg(target_os = "linux")]
            Self::Hashed(_) => Err(WorkspaceError::Fenced),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct NativePackedPublicationBasis {
    pub(super) carrier_layer_id: LayerId,
    pub(super) carrier_sealed_version: u64,
    pub(super) source_sealed_version: u64,
    pub(super) carrier_delta_digest: [u8; 32],
    pub(super) carrier_root_hash: [u8; 32],
    pub(super) final_source_guard: Option<HeadGuard>,
    pub(super) final_source_root_hash: Option<[u8; 32]>,
    pub(super) final_source_delta_digest: Option<[u8; 32]>,
}

impl NativePackedPublicationBasis {
    pub(super) fn validate(
        &self,
        record: &PackedJournalRecord,
        native: &PackedNativeJournalBasis,
    ) -> Result<(), WorkspaceError> {
        let head: LayerRecord = decode_open_value(&record.expected_head, REFERENCE_LIMIT)?;
        let base: LayerRecord = decode_open_value(&record.expected_base, REFERENCE_LIMIT)?;
        let empty = delta_digest(&CanonicalLayerDelta::default())?;
        if (record.phase == PackedJournalPhase::Committed) != self.final_source_guard.is_some()
            || self.final_source_guard.is_some() != self.final_source_root_hash.is_some()
            || self.final_source_guard.is_some() != self.final_source_delta_digest.is_some()
            || self.final_source_guard.as_ref().is_some_and(|guard| {
                guard.workspace_id != record.guard.workspace_id
                    || guard.expected_head_layer_id != record.guard.expected_head_layer_id
                    || guard.expected_head_epoch != record.guard.expected_head_epoch
                    || guard.lease_id.as_uuid().is_nil()
                    || guard.holder_generation < record.guard.holder_generation
                    || (guard.holder_generation == record.guard.holder_generation
                        && guard.lease_id != record.guard.lease_id)
            })
        {
            return Err(WorkspaceError::Fenced);
        }
        if self.carrier_layer_id.as_uuid().is_nil()
            || self.carrier_layer_id == native.planned_head_layer_id
            || self.carrier_layer_id == head.layer_id
            || self.carrier_layer_id == base.layer_id
            || self.carrier_sealed_version == 0
            || self.source_sealed_version == 0
            || self.carrier_sealed_version == self.source_sealed_version
            || self.carrier_delta_digest != empty
            || self.carrier_root_hash != root_hash([0; 32], empty)
        {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(target) = &record.commit_target
            && (target.head_layer_id != native.planned_head_layer_id
                || target.head_epoch != native.planned_head_epoch
                || target.base_revision != self.carrier_revision()
                || target.binding.base_layer_id != self.carrier_layer_id)
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
    pub(super) fn carrier_revision(&self) -> BaseRevision {
        BaseRevision {
            layer_id: self.carrier_layer_id,
            sealed_version: self.carrier_sealed_version,
            root_hash: self.carrier_root_hash,
        }
    }
    pub(super) fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        let mut out = [b"PNP3".as_slice(), &1u64.to_le_bytes()].concat();
        out.extend_from_slice(self.carrier_layer_id.as_bytes());
        out.extend_from_slice(&self.carrier_sealed_version.to_le_bytes());
        out.extend_from_slice(&self.source_sealed_version.to_le_bytes());
        out.extend_from_slice(&self.carrier_delta_digest);
        out.extend_from_slice(&self.carrier_root_hash);
        out.push(u8::from(self.final_source_guard.is_some()));
        if let Some(guard) = &self.final_source_guard {
            out.extend_from_slice(guard.workspace_id.as_bytes());
            out.extend_from_slice(guard.expected_head_layer_id.as_bytes());
            out.extend_from_slice(&guard.expected_head_epoch.to_le_bytes());
            out.extend_from_slice(guard.lease_id.as_bytes());
            out.extend_from_slice(&guard.holder_generation.to_le_bytes());
            out.extend_from_slice(
                self.final_source_root_hash
                    .as_ref()
                    .ok_or(WorkspaceError::Fenced)?,
            );
            out.extend_from_slice(
                self.final_source_delta_digest
                    .as_ref()
                    .ok_or(WorkspaceError::Fenced)?,
            );
        }
        finish_record(out, 384)
    }
    pub(super) fn decode(raw: &[u8]) -> Result<Self, WorkspaceError> {
        let mut c = JournalCursor::checked(raw, b"PNP3", 384)?;
        let result = Self {
            carrier_layer_id: LayerId::from_uuid(Uuid::from_bytes(c.take()?)),
            carrier_sealed_version: c.u64()?,
            source_sealed_version: c.u64()?,
            carrier_delta_digest: c.take()?,
            carrier_root_hash: c.take()?,
            final_source_guard: None,
            final_source_root_hash: None,
            final_source_delta_digest: None,
        };
        let mut result = result;
        match c.take::<1>()?[0] {
            0 => {}
            1 => {
                result.final_source_guard = Some(HeadGuard {
                    workspace_id: WorkspaceId::from_uuid(Uuid::from_bytes(c.take()?)),
                    expected_head_layer_id: LayerId::from_uuid(Uuid::from_bytes(c.take()?)),
                    expected_head_epoch: c.u64()?,
                    lease_id: LeaseId::from_uuid(Uuid::from_bytes(c.take()?)),
                    holder_generation: c.u64()?,
                });
                result.final_source_root_hash = Some(c.take()?);
                result.final_source_delta_digest = Some(c.take()?);
            }
            _ => return Err(WorkspaceError::Fenced),
        };
        c.end()?;
        result.encode()?;
        Ok(result)
    }
}

/// Exact durable observations on this store, not permission to read frozen
/// source data, mint Hashed authority, mount the graph, or publish it.
/// Its only constructor below checks actual shared-KV bytes in an exact CAS.
pub(crate) struct PackedNativeRecoveryBasisReceipt<B> {
    store: Arc<KvWorkspaceStore<B>>,
    record: OwnedPackedJournal<PackedJournalRecord>,
    old_layers: [LayerRecord; 2],
    original_quiesced_journal: SealJournal,
    current_native_journal: SealJournal,
    checks: Vec<KvCheck>,
    _permit: SnapshotOwnedPermit,
}
impl<B: WorkspaceKvBackend> PackedNativeRecoveryBasisReceipt<B> {
    pub(crate) fn store(&self) -> &Arc<KvWorkspaceStore<B>> {
        &self.store
    }
    pub(crate) fn record(&self) -> &PackedJournalRecord {
        &self.record
    }
    pub(crate) fn journal_id(&self) -> JournalId {
        self.record.journal_id
    }
    pub(crate) fn revision(&self) -> u64 {
        self.record.revision
    }
    pub(crate) fn staging_incarnation(&self) -> Uuid {
        self.record.source.staging_id
    }
    pub(crate) fn old_guard(&self) -> &HeadGuard {
        &self.record.guard
    }
    pub(crate) fn old_layers(&self) -> &[LayerRecord; 2] {
        &self.old_layers
    }
    pub(crate) fn binding(&self) -> &PackedLowerBindingRecord {
        &self.record.expected_binding
    }
    fn native(&self) -> &PackedNativeJournalBasis {
        self.record
            .native_rebind
            .as_ref()
            .expect("checked native basis")
    }
    pub(crate) fn native_journal_id(&self) -> JournalId {
        self.native().native_journal_id
    }
    pub(crate) fn planned_head_layer_id(&self) -> LayerId {
        self.native().planned_head_layer_id
    }
    pub(crate) fn planned_head_epoch(&self) -> u64 {
        self.native().planned_head_epoch
    }
    pub(crate) fn original_quiesced_native_journal(&self) -> &SealJournal {
        &self.original_quiesced_journal
    }
    pub(crate) fn current_native_journal(&self) -> &SealJournal {
        &self.current_native_journal
    }
    pub(crate) fn quiesce_receipt_bytes(&self) -> &[u8] {
        &self.native().quiesce_receipt
    }
    pub(crate) fn quiesce_receipt_digest(&self) -> [u8; 32] {
        self.native().quiesce_digest
    }
    pub(crate) fn basis_checks(&self) -> &[KvCheck] {
        &self.checks
    }
}

pub(super) fn append_exact_checks(
    checks: &mut Vec<KvCheck>,
    additional: Vec<KvCheck>,
) -> Result<(), WorkspaceError> {
    for added in additional {
        if let Some(old) = checks.iter().find(|old| old.key == added.key) {
            if old.expected != added.expected {
                #[cfg(test)]
                eprintln!(
                    "packed native authority read mismatch key_class={} key_sha256={} first_len={:?} second_len={:?}",
                    if old.key.as_slice() == PACKED_ROOT_GENERATION_KEY {
                        "root-generation"
                    } else if old.key.as_slice() == LAYER_INVENTORY_GENERATION_KEY {
                        "layer-generation"
                    } else if old.key.as_slice() == CONTROL_KEY {
                        "control"
                    } else {
                        "other"
                    },
                    hex::encode(Sha256::digest(&old.key)),
                    old.expected.as_ref().map(Vec::len),
                    added.expected.as_ref().map(Vec::len),
                );
                return Err(WorkspaceError::Busy);
            }
        } else {
            checks.push(added);
        }
    }
    Ok(())
}

impl<B: WorkspaceKvBackend + 'static> KvWorkspaceStore<B> {
    pub(super) async fn native_staged_journal_authorities(
        &self,
        record: &PackedJournalRecord,
        extra_keys: &[Vec<u8>],
        authority: &NativeJournalAuthority<'_, B>,
    ) -> Result<JournalAuthorities, WorkspaceError> {
        use super::super::native_read_conflict::{
            FirstRead, MAX_NATIVE_READ_ATTEMPTS, NATIVE_READ_REBUILD_BYTES,
        };
        let budget = authority.original().mount_budget();
        let mut _owner = None;
        let mut first = FirstRead::new();
        for _ in 0..MAX_NATIVE_READ_ATTEMPTS {
            if budget.state().closed {
                return Err(WorkspaceError::Fenced);
            }
            let (mut read, root_only_conflict, root_seen) = self
                .native_staged_journal_authorities_once(record, extra_keys, authority)
                .await?;
            if read.now <= 0 || read.now >= read.authority_deadline_ns || budget.state().closed {
                return Err(WorkspaceError::Fenced);
            }
            if !root_only_conflict && _owner.is_none() {
                return Ok(read);
            }
            if _owner.is_none() {
                _owner = Some(
                    budget
                        .admit(&[(SnapshotBudgetPool::Metadata, NATIVE_READ_REBUILD_BYTES)])
                        .map_err(journal_budget_error)?,
                );
            }
            let (aligned, deadline) = first.observe(
                read.checks.clone(),
                read.authority_deadline_ns,
                root_only_conflict,
                root_seen,
            )?;
            if read.now <= 0 || read.now >= deadline || budget.state().closed {
                return Err(WorkspaceError::Fenced);
            }
            if aligned {
                read.authority_deadline_ns = deadline;
                return Ok(read);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn native_staged_journal_authorities_once(
        &self,
        record: &PackedJournalRecord,
        extra_keys: &[Vec<u8>],
        authority: &NativeJournalAuthority<'_, B>,
    ) -> Result<(JournalAuthorities, bool, u64), WorkspaceError> {
        let original = authority.original();
        let native = record
            .native_rebind
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?;
        let old_layers = [
            decode_open_value::<LayerRecord>(&record.expected_head, REFERENCE_LIMIT)?,
            decode_open_value::<LayerRecord>(&record.expected_base, REFERENCE_LIMIT)?,
        ];
        let mut historical = native.clone();
        historical.publication = None;
        if !original.belongs_to_store(self)
            || &record.guard != original.mapping().old_guard()
            || &old_layers != original.mapping().old_layers()
            || &record.expected_binding != original.binding()
            || historical != PackedNativeJournalBasis::from_fence(original)?
        {
            return Err(WorkspaceError::Fenced);
        }
        let (authority_checks, expires) = authority.checks().await?;
        // PPJ and original Q keep the immutable predecessor guard. A verified
        // recovery claim independently supplies the actual current lease.
        let source_guard = original.source_guard();
        if source_guard.workspace_id != record.guard.workspace_id
            || source_guard.expected_head_layer_id != record.guard.expected_head_layer_id
            || source_guard.expected_head_epoch != record.guard.expected_head_epoch
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut keys = vec![
            hot_workspace_key(record.guard.workspace_id),
            hot_layer_key(record.guard.expected_head_layer_id),
            hot_lease_key(source_guard.workspace_id, source_guard.lease_id),
            hot_layer_key(old_layers[1].layer_id),
            packed_current_key(record.guard.workspace_id),
            packed_claim_key(record.guard.workspace_id),
            packed_history_key(
                record.guard.workspace_id,
                record.expected_binding.binding.binding_version,
            ),
            packed_history_key(record.guard.workspace_id, 1),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            journal_key(record.journal_id),
            hot_allocator_key("inode"),
        ];
        keys.extend_from_slice(extra_keys);
        // A native publication authority may include the source handoff and
        // native hold sidecars in addition to the ordinary journal keys.  The
        // ordinary bounded point-read driver is deliberately capped at 32
        // keys; routing a complete authority packet through it made a real
        // TiKV fork fail before publication even though the packet contract
        // permits the fixed 64-key envelope.  Keep ordinary reads on their
        // smaller tier and use the dedicated packet API only when the full
        // authority crosses that boundary.  The packet API keeps one TSO and
        // one deadline while splitting the data gets into bounded windows.
        let (values, now) = if keys.len() <= journal_point_limits().max_records {
            self.backend
                .get_many_consistent_with_time_bounded(&keys, journal_point_limits())
                .await?
        } else {
            let limits = KvReadLimits {
                max_records: keys.len(),
                max_key_bytes: 1024,
                max_value_bytes: RECORD_LIMIT,
                max_total_bytes: 2 << 20,
                max_response_bytes: 2 << 20,
                max_data_requests: keys.len().saturating_add(2).min(64),
            };
            self.backend
                .get_publication_packet_consistent_with_time_bounded(&keys, limits)
                .await?
        };
        if values.len() != keys.len() {
            return Err(journal_error("short native staged authority read"));
        }
        let workspace: WorkspaceRecord = bounded_native_value(&values[0])?;
        let head: LayerRecord = bounded_native_value(&values[1])?;
        let lease: SnapshotLease = bounded_native_value(&values[2])?;
        let base: LayerRecord = bounded_native_value(&values[3])?;
        let actual = values[10]
            .as_deref()
            .map(PackedJournalRecord::decode)
            .transpose()?;
        if workspace.workspace_id != record.guard.workspace_id
            || workspace.state != WorkspaceState::Sealing
            || workspace.active_lease != Some(source_guard.lease_id)
            || workspace.head_layer_id != record.guard.expected_head_layer_id
            || workspace.head_epoch != record.guard.expected_head_epoch
            || encode(&head)? != native.frozen_head
            || base != old_layers[1]
            || lease.lease_id != source_guard.lease_id
            || lease.holder_generation != source_guard.holder_generation
            || lease.workspace_id != record.guard.workspace_id
            || lease.state != LeaseState::Active
            || !lease.writable
            || expires > lease.expires_at_ns
            || expires <= now
            || lease.expires_at_ns <= now
            || values[4].as_deref() != Some(record.expected_binding.encode()?.as_slice())
            || values[5].as_deref() != Some(PACKED_CLAIM)
            || values[6] != values[4]
            || actual.as_ref().is_some_and(|actual| {
                actual.journal_id != record.journal_id
                    || actual.source != record.source
                    || actual.guard != record.guard
                    || actual.expected_head != record.expected_head
                    || actual.expected_base != record.expected_base
                    || actual.expected_binding != record.expected_binding
                    || actual.native_rebind != record.native_rebind
            })
        {
            return Err(WorkspaceError::Fenced);
        }
        let anchor =
            PackedLowerBindingRecord::decode(values[7].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if anchor.workspace_id != record.guard.workspace_id || anchor.binding.binding_version != 1 {
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[8])?;
        layer_inventory_generation(&values[9])?;
        let mut checks = keys
            .into_iter()
            .zip(values.iter().cloned())
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let alignment = super::super::native_read_conflict::align(checks, authority_checks, false)?;
        checks = alignment.checks;
        Ok((
            JournalAuthorities {
                checks,
                values,
                authority_deadline_ns: expires,
                lease,
                workspace,
                head,
                base,
                now,
            },
            alignment.root_only_conflict,
            alignment.root_seen,
        ))
    }

    pub(crate) async fn inspect_native_packed_recovery_basis(
        self: &Arc<Self>,
        expected: &PackedJournalRecord,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<PackedNativeRecoveryBasisReceipt<B>, WorkspaceError> {
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let record_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, RECORD_LIMIT as u64)])
            .map_err(journal_budget_error)?;
        expected.validate()?;
        let native = expected
            .native_rebind
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?;
        let publication = native.publication.as_ref().ok_or(WorkspaceError::Fenced)?;
        if expected.phase.terminal() || budget.state().closed {
            return Err(WorkspaceError::Fenced);
        }
        let old_layers: [LayerRecord; 2] = [
            decode_open_value(&expected.expected_head, REFERENCE_LIMIT)?,
            decode_open_value(&expected.expected_base, REFERENCE_LIMIT)?,
        ];
        let keys = vec![
            journal_key(expected.journal_id),
            active_key(expected.journal_id),
            registry::registry_root_key(expected.source.staging_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            JOURNAL_FEATURE_KEY.to_vec(),
            ACTIVE_COUNT_KEY.to_vec(),
            hot_journal_key(expected.guard.workspace_id, native.native_journal_id),
            hot_workspace_key(expected.guard.workspace_id),
            hot_layer_key(old_layers[0].layer_id),
            hot_layer_key(old_layers[1].layer_id),
            hot_layer_key(native.planned_head_layer_id),
            hot_layer_key(publication.carrier_layer_id),
            packed_current_key(expected.guard.workspace_id),
            packed_claim_key(expected.guard.workspace_id),
            packed_history_key(
                expected.guard.workspace_id,
                expected.expected_binding.binding.binding_version,
            ),
            packed_history_key(expected.guard.workspace_id, 1),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != keys.len()
            || values[0].as_deref() != Some(expected.encode()?.as_slice())
            || values[1] != values[0]
            || values[4].as_deref() != Some(b"PPJ3")
            || active_count(&values[5])? == 0
        {
            return Err(WorkspaceError::Fenced);
        }
        registry::check_staging_root_bytes(expected, values[2].as_deref())?;
        next_packed_root_generation(&values[3])?;
        layer_inventory_generation(&values[16])?;
        let current: SealJournal = bounded_native_value(&values[6])?;
        let workspace: WorkspaceRecord = bounded_native_value(&values[7])?;
        let head: LayerRecord = bounded_native_value(&values[8])?;
        let base: LayerRecord = bounded_native_value(&values[9])?;
        if workspace.workspace_id != expected.guard.workspace_id
            || workspace.head_layer_id != old_layers[0].layer_id
            || workspace.head_epoch != expected.guard.expected_head_epoch
            || workspace.state != WorkspaceState::Sealing
            || encode(&head)? != native.frozen_head
            || base != old_layers[1]
            || values[10].is_some()
            || values[11].is_some()
            || values[12].as_deref() != Some(expected.expected_binding.encode()?.as_slice())
            || values[13].as_deref() != Some(PACKED_CLAIM)
            || values[14] != values[12]
            || current.journal_id != native.native_journal_id
            || current.workspace_id != expected.guard.workspace_id
            || current.old_head_layer_id != old_layers[0].layer_id
            || current.expected_head_epoch != expected.guard.expected_head_epoch
            || current.new_head_layer_id != Some(native.planned_head_layer_id)
            || !matches!(
                current.phase,
                SealPhase::Quiesced | SealPhase::DataDrained | SealPhase::Hashed
            )
        {
            return Err(WorkspaceError::Fenced);
        }
        let anchor =
            PackedLowerBindingRecord::decode(values[15].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if anchor.workspace_id != expected.guard.workspace_id || anchor.binding.binding_version != 1
        {
            return Err(WorkspaceError::Fenced);
        }
        let original: SealJournal =
            decode_open_value(&native.original_quiesced_journal, REFERENCE_LIMIT)?;
        let checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        if !self.backend.compare_and_swap(&checks, &[]).await? {
            return Err(WorkspaceError::Busy);
        }
        if budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        Ok(PackedNativeRecoveryBasisReceipt {
            store: self.clone(),
            record: expected.clone().retain(record_owner)?,
            old_layers,
            original_quiesced_journal: original,
            current_native_journal: current,
            checks,
            _permit: owner,
        })
    }
}
