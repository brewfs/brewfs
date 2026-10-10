//! Native catalog basis for the imported-graph sub-proof. A decoded PNB3 is a
//! recovery fact; every write still consumes the actual native quiesce fence.

use super::*;
use crate::workspace_overlay::stores::kv_store::packed_native_freeze::PackedNativeQuiesceFence;

const NATIVE_BASIS_LIMIT: usize = 5 * REFERENCE_LIMIT + 384;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PackedNativeJournalBasis {
    pub(super) native_journal_id: JournalId,
    pub(super) planned_head_layer_id: LayerId,
    pub(super) planned_head_epoch: u64,
    pub(super) frozen_head: Vec<u8>,
    pub(super) frozen_base: Vec<u8>,
    pub(super) quiesce_digest: [u8; 32],
    pub(super) original_quiesced_journal: Vec<u8>,
    pub(super) quiesce_receipt: Vec<u8>,
    pub(super) publication: Option<native_publication::NativePackedPublicationBasis>,
}
impl PackedNativeJournalBasis {
    pub(super) fn from_fence<B: WorkspaceKvBackend>(
        fence: &PackedNativeQuiesceFence<B>,
    ) -> Result<Self, WorkspaceError> {
        let [frozen_head, frozen_base] = fence.quiesced_native_basis_bytes()?;
        Ok(Self {
            native_journal_id: fence.mapping().journal_id(),
            planned_head_layer_id: fence.mapping().planned_head_layer_id(),
            planned_head_epoch: fence.mapping().planned_head_epoch(),
            frozen_head,
            frozen_base,
            quiesce_digest: fence.canonical_receipt_digest(),
            original_quiesced_journal: fence.quiesced_journal_bytes()?,
            quiesce_receipt: fence.canonical_receipt_bytes().to_vec(),
            publication: None,
        })
    }
    pub(super) fn validate(
        &self,
        record: &PackedJournalRecord,
        old_head: &LayerRecord,
        old_base: &LayerRecord,
    ) -> Result<(), WorkspaceError> {
        let frozen_head: LayerRecord = decode_open_value(&self.frozen_head, REFERENCE_LIMIT)?;
        let frozen_base: LayerRecord = decode_open_value(&self.frozen_base, REFERENCE_LIMIT)?;
        let original: SealJournal =
            decode_open_value(&self.original_quiesced_journal, REFERENCE_LIMIT)?;
        let mut expected = old_head.clone();
        expected.state = LayerState::Sealing;
        if self.native_journal_id.as_uuid().is_nil()
            || self.quiesce_digest == [0; 32]
            || self.planned_head_layer_id.as_uuid().is_nil()
            || self.planned_head_layer_id == old_head.layer_id
            || self.planned_head_layer_id == old_base.layer_id
            || self.quiesce_receipt.is_empty()
            || self.quiesce_receipt.len() > 2 * REFERENCE_LIMIT
            || <[u8; 32]>::from(Sha256::digest(&self.quiesce_receipt)) != self.quiesce_digest
            || original.journal_id != self.native_journal_id
            || original.workspace_id != record.guard.workspace_id
            || original.old_head_layer_id != old_head.layer_id
            || original.expected_head_epoch != record.guard.expected_head_epoch
            || original.new_head_layer_id != Some(self.planned_head_layer_id)
            || original.phase != SealPhase::Quiesced
            || original.delta_digest.is_some()
            || original.root_hash.is_some()
            || self.planned_head_epoch
                != record
                    .guard
                    .expected_head_epoch
                    .checked_add(1)
                    .ok_or_else(|| journal_error("native epoch overflow"))?
            || frozen_head != expected
            || frozen_base != *old_base
            || !matches!(
                record.phase,
                PackedJournalPhase::Building
                    | PackedJournalPhase::Uploading
                    | PackedJournalPhase::Readback
                    | PackedJournalPhase::AwaitingFullProof
                    | PackedJournalPhase::Verified
                    | PackedJournalPhase::Committed
                    | PackedJournalPhase::Aborted
            )
        {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(publication) = &self.publication {
            publication.validate(record, self)?;
        }
        Ok(())
    }
    pub(super) fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        let mut out = [b"PNB3".as_slice(), &1u64.to_le_bytes()].concat();
        out.extend_from_slice(self.native_journal_id.as_bytes());
        out.extend_from_slice(self.planned_head_layer_id.as_bytes());
        out.extend_from_slice(&self.planned_head_epoch.to_le_bytes());
        append_bytes(&mut out, &self.frozen_head, REFERENCE_LIMIT)?;
        append_bytes(&mut out, &self.frozen_base, REFERENCE_LIMIT)?;
        out.extend_from_slice(&self.quiesce_digest);
        append_bytes(&mut out, &self.original_quiesced_journal, REFERENCE_LIMIT)?;
        append_bytes(&mut out, &self.quiesce_receipt, 2 * REFERENCE_LIMIT)?;
        let publication = self
            .publication
            .as_ref()
            .map(native_publication::NativePackedPublicationBasis::encode)
            .transpose()?
            .unwrap_or_default();
        append_bytes(&mut out, &publication, 384)?;
        finish_record(out, NATIVE_BASIS_LIMIT)
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        let mut c = JournalCursor::checked(bytes, b"PNB3", NATIVE_BASIS_LIMIT)?;
        let result = Self {
            native_journal_id: JournalId::from_uuid(Uuid::from_bytes(c.take()?)),
            planned_head_layer_id: LayerId::from_uuid(Uuid::from_bytes(c.take()?)),
            planned_head_epoch: c.u64()?,
            frozen_head: c.bytes(REFERENCE_LIMIT)?,
            frozen_base: c.bytes(REFERENCE_LIMIT)?,
            quiesce_digest: c.take()?,
            original_quiesced_journal: c.bytes(REFERENCE_LIMIT)?,
            quiesce_receipt: c.bytes(2 * REFERENCE_LIMIT)?,
            publication: {
                let raw = c.bytes(384)?;
                if raw.is_empty() {
                    None
                } else {
                    Some(native_publication::NativePackedPublicationBasis::decode(
                        &raw,
                    )?)
                }
            },
        };
        c.end()?;
        result.encode()?;
        Ok(result)
    }
}

/// Owns the exact native authority. Durable reopen alone cannot recreate this
/// handle or claim that an independently imported graph is an effective view.
pub(crate) struct NativeReboundPackedJournal<B> {
    pub(super) record: OwnedPackedJournal<PackedJournalRecord>,
    pub(super) fence: PackedNativeQuiesceFence<B>,
}

/// Complete staged bytes and semantic graph under the real native catalog
/// fence. This is source-independent: it deliberately cannot certify that
/// these bytes equal the effective workspace view or authorize publication.
/// Only the actual bracketed graph audit below can construct this proof.
#[cfg(target_os = "linux")]
pub(crate) struct NativePackedGraphProof {
    pub(super) journal_id: JournalId,
    pub(super) audited_revision: u64,
    pub(super) staging_incarnation: Uuid,
    pub(super) inventory_digest: [u8; 32],
    pub(super) catalog_context_digest: [u8; 32],
    pub(super) native_receipt_digest: [u8; 32],
    pub(super) graph: V3IndexContextAudit,
    _permit: V3OwnedPermit,
}

#[cfg(target_os = "linux")]
impl NativePackedGraphProof {
    pub(super) fn validate_for_journal<B: WorkspaceKvBackend>(
        &self,
        expected: &PackedJournalRecord,
        fence: &PackedNativeQuiesceFence<B>,
    ) -> Result<(), WorkspaceError> {
        let target = expected
            .commit_target
            .as_ref()
            .ok_or_else(|| journal_error("native graph candidate target missing"))?;
        let (_, root_inode, highest_inode) = self.graph.publication_facts();
        if expected.phase != PackedJournalPhase::AwaitingFullProof
            || self.journal_id != expected.journal_id
            || self.audited_revision != expected.revision
            || self.staging_incarnation != expected.source.staging_id
            || self.inventory_digest != expected.inventory_digest
            || self.catalog_context_digest != audit_basis_digest(expected, expected.revision)?
            || self.native_receipt_digest != fence.canonical_receipt_digest()
            || self.graph.manifest_reference() != &target.binding.manifest
            || self.graph.counts().objects != expected.object_count
            || root_inode != 1
            || highest_inode >= i64::MAX as u64 - 1
            || target.highest_inode
                != (highest_inode as i64).max(expected.expected_binding.highest_inode)
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
}
impl<B: WorkspaceKvBackend> NativeReboundPackedJournal<B> {
    pub(crate) fn record(&self) -> &PackedJournalRecord {
        &self.record
    }
    pub(crate) fn native_fence(&self) -> &PackedNativeQuiesceFence<B> {
        &self.fence
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        OwnedPackedJournal<PackedJournalRecord>,
        PackedNativeQuiesceFence<B>,
    ) {
        (self.record, self.fence)
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Actual full graph half of native publication. A future strong factory
    /// must additionally consume the genuine effective-view comparison seal
    /// for this exact manifest and the actual native phase authority.
    #[cfg(target_os = "linux")]
    pub(crate) async fn audit_native_rebound_staged_graph<O: ObjectBackend + Clone>(
        &self,
        bound: &NativeReboundPackedJournal<B>,
        client: &ObjectClient<O>,
        budget: &Arc<V3MountBudget>,
        options: ImportedGraphAuditOptions<'_>,
    ) -> Result<NativePackedGraphProof, WorkspaceError> {
        let _operation_owner = budget
            .admit(&[(V3BudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let proof_owner = budget
            .admit(&[(V3BudgetPool::Metadata, 32 << 10)])
            .map_err(journal_budget_error)?;
        let expected = &bound.record;
        let cancel = options.cancel.clone();
        if expected.phase != PackedJournalPhase::AwaitingFullProof
            || cancel.is_cancelled()
            || budget.state().closed
        {
            return Err(WorkspaceError::Busy);
        }
        self.packed_native_journal_write(expected, expected, &bound.fence)
            .await?;
        let graph = self
            .audit_staged_packed_graph(expected, client, budget, options)
            .await?;
        if cancel.is_cancelled() || budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        self.packed_native_journal_write(expected, expected, &bound.fence)
            .await?;
        if cancel.is_cancelled() || budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        let proof = NativePackedGraphProof {
            journal_id: expected.journal_id,
            audited_revision: expected.revision,
            staging_incarnation: expected.source.staging_id,
            inventory_digest: expected.inventory_digest,
            catalog_context_digest: audit_basis_digest(expected, expected.revision)?,
            native_receipt_digest: bound.fence.canonical_receipt_digest(),
            graph,
            _permit: proof_owner,
        };
        proof.validate_for_journal(expected, &bound.fence)?;
        Ok(proof)
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn audit_native_rebound_imported_graph<O: ObjectBackend + Clone>(
        &self,
        bound: &NativeReboundPackedJournal<B>,
        source: &V3FinalSourceProof,
        client: &ObjectClient<O>,
        budget: &Arc<V3MountBudget>,
        options: ImportedGraphAuditOptions<'_>,
    ) -> Result<ImportedPackedGraphSeal, WorkspaceError> {
        self.audit_imported_graph_with_native(
            (&bound.record, source, Some(&bound.fence)),
            client,
            budget,
            options,
        )
        .await
    }

    pub(crate) async fn record_native_rebound_imported_graph(
        &self,
        mut bound: NativeReboundPackedJournal<B>,
        seal: &ImportedPackedGraphSeal,
        budget: &Arc<V3MountBudget>,
    ) -> Result<NativeReboundPackedJournal<B>, WorkspaceError> {
        let owner = budget
            .admit(&[(V3BudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let expected = &bound.record;
        if expected.phase != PackedJournalPhase::AwaitingFullProof
            || expected.graph_receipt.is_some()
            || seal.receipt.audited_revision != expected.revision
            || seal.receipt.catalog_context_digest
                != audit_basis_digest(expected, expected.revision)?
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut next = expected.next()?;
        next.graph_receipt = Some(seal.receipt.clone());
        bound.record = self
            .packed_native_journal_write(expected, &next, &bound.fence)
            .await
            .retain(owner)?;
        Ok(bound)
    }
    async fn check_native_journal_fence(
        &self,
        record: &PackedJournalRecord,
        fence: &PackedNativeQuiesceFence<B>,
    ) -> Result<(), WorkspaceError> {
        if !fence.belongs_to_store(self) {
            return Err(WorkspaceError::Fenced);
        }
        let old_layers = [
            decode(&record.expected_head)?,
            decode(&record.expected_base)?,
        ];
        fence
            .validate_context(&record.guard, &old_layers, &record.expected_binding)
            .await?;
        if record.native_rebind.as_ref() != Some(&PackedNativeJournalBasis::from_fence(fence)?) {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    pub(super) async fn packed_native_journal_write(
        &self,
        expected: &PackedJournalRecord,
        next: &PackedJournalRecord,
        fence: &PackedNativeQuiesceFence<B>,
    ) -> Result<PackedJournalRecord, WorkspaceError> {
        next.validate()?;
        if next.phase != PackedJournalPhase::AwaitingFullProof
            || next.journal_id != expected.journal_id
            || next.source != expected.source
            || next.object_count != expected.object_count
            || next.inventory_digest != expected.inventory_digest
            || next.expected_head != expected.expected_head
            || next.expected_base != expected.expected_base
            || next.guard != expected.guard
            || next.expected_binding != expected.expected_binding
            || next.commit_target != expected.commit_target
            || next.full_proof_digest != [0; 32]
            || !(next.revision == expected.revision
                || next.revision == increment_revision(expected.revision)?)
            || (next.revision == expected.revision && next != expected)
        {
            return Err(WorkspaceError::Fenced);
        }
        self.check_native_journal_fence(next, fence).await?;
        let (mut checks, expires) = fence.authority_checks_before().await?;
        let keys = [
            journal_key(expected.journal_id),
            active_key(expected.journal_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            registry::registry_root_key(expected.source.staging_id),
        ];
        let values = self.packed_journal_values(&keys).await?;
        if values.len() != keys.len() {
            return Err(journal_error("short native journal read"));
        }
        let actual = PackedJournalRecord::decode(
            values[0]
                .as_deref()
                .ok_or_else(|| journal_error("native journal missing"))?,
        )?;
        let retry = actual == *next;
        if (!retry && actual != *expected) || values[1] != values[0] {
            return Err(WorkspaceError::Busy);
        }
        registry::check_quiesced_staging_root(&actual, values[3].as_deref())?;
        for (key, expected) in keys.into_iter().zip(values.iter().cloned()) {
            if let Some(check) = checks.iter().find(|check| check.key == key) {
                if check.expected != expected {
                    return Err(WorkspaceError::Busy);
                }
            } else {
                checks.push(KvCheck { key, expected });
            }
        }
        let writes = if retry {
            Vec::new()
        } else {
            vec![
                KvWrite::Put {
                    key: journal_key(next.journal_id),
                    value: next.encode()?,
                },
                KvWrite::Put {
                    key: active_key(next.journal_id),
                    value: next.encode()?,
                },
                put(
                    PACKED_ROOT_GENERATION_KEY.to_vec(),
                    &next_packed_root_generation(&values[2])?,
                )?,
            ]
        };
        if !self
            .backend
            .compare_and_swap_before(&checks, &writes, expires)
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(next.clone())
    }

    /// Rebind after the actual caller's VFS drain and native catalog freeze.
    /// This produces only a new immutable catalog basis for a repeated imported
    /// graph audit. It does not validate the native namespace/content or publish.
    pub(crate) async fn rebind_packed_journal_after_native_freeze<S, M>(
        &self,
        expected: &PackedJournalRecord,
        fence: PackedNativeQuiesceFence<B>,
        vfs: &crate::vfs::fs::VFS<S, M>,
        drain: &crate::vfs::fs::PackedVfsDrainFence<S, M>,
        budget: &Arc<V3MountBudget>,
    ) -> Result<NativeReboundPackedJournal<B>, WorkspaceError>
    where
        S: crate::chunk::store::BlockStore + Send + Sync + 'static,
        M: crate::meta::MetaLayer + Send + Sync + 'static,
    {
        let owner = budget
            .admit(&[(V3BudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if expected.native_rebind.is_some()
            || !matches!(
                expected.phase,
                PackedJournalPhase::AwaitingFullProof | PackedJournalPhase::Verified
            )
            || !drain.is_same_vfs(vfs)
        {
            return Err(WorkspaceError::Fenced);
        }
        drain.validate_local().await.map_err(journal_error)?;
        let mut next = expected.next()?;
        next.phase = PackedJournalPhase::AwaitingFullProof;
        next.graph_receipt = None;
        next.full_proof_digest = [0; 32];
        next.native_rebind = Some(PackedNativeJournalBasis::from_fence(&fence)?);
        let record = self
            .packed_native_journal_write(expected, &next, &fence)
            .await
            .retain(owner)?;
        Ok(NativeReboundPackedJournal { record, fence })
    }
}

fn increment_revision(revision: u64) -> Result<u64, WorkspaceError> {
    revision
        .checked_add(1)
        .ok_or_else(|| journal_error("native journal revision overflow"))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::super::tests::{JournalMemoryBackend, begin, setup};
    use super::*;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::workspace_overlay::packed_v3::wire005::{
        AuthenticatedV3Snapshot, V3IndexReader, V3ProducerOptions, V3SourceConsistency,
        V3SourceFileLimits, V3SourceHardlinkPolicy, V3SourceNamespaceInventory,
        V3SourceNamespaceOptions,
    };
    use crate::workspace_overlay::packed_v3::{AccessProfile, PackedCodec, SizeClassTable};
    use crate::workspace_overlay::publish::binding::VerifiedPackedLower;

    // Exercises the actual catalog token/CAS and actual importer/graph route.
    // Local VFS driver/drain evidence is tested separately in its VFS module;
    // this test does not issue an effective-view/native publication seal.
    #[tokio::test]
    async fn native_rebind_cas_clears_old_receipt_and_reaudits_exact_postfreeze_basis() {
        let metadata = Arc::new(JournalMemoryBackend::at_time(1));
        let budget = V3MountBudget::defaults();
        let store = Arc::new(
            KvWorkspaceStore::from_arc(metadata.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        let (_old_objects, mut publish) = setup(&store).await;
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("data"), [61; 8192]).unwrap();
        std::fs::hard_link(source.path().join("data"), source.path().join("alias")).unwrap();
        std::os::unix::fs::symlink("data", source.path().join("link")).unwrap();
        let journal = begin(&store, &publish, &budget).await;
        let inventory = V3SourceNamespaceInventory::capture(
            source.path(),
            scratch.path(),
            V3SourceNamespaceOptions {
                root_inode: 1,
                consistency: V3SourceConsistency::BestEffortDetected,
                hardlink_policy: V3SourceHardlinkPolicy::VisibleLinks,
                file_limits: V3SourceFileLimits::default(),
            },
        )
        .await
        .unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let (snapshot, mut journal) = store
            .build_registered_source_snapshot(
                journal,
                inventory,
                client.clone(),
                V3ProducerOptions {
                    snapshot_id: [93; 32],
                    root_dir_key: [94; 32],
                    root_inode: 1,
                    profile: AccessProfile::RandomSmallFile,
                    size_classes: SizeClassTable::default(),
                    build_policy: crate::workspace_overlay::packed_v3::wire005::V3BuildPolicy {
                        inline_data: false,
                        p90: None,
                        ..Default::default()
                    },
                    metadata_codec: PackedCodec::Raw,
                    data_codec: PackedCodec::Raw,
                },
                budget.clone(),
            )
            .await
            .unwrap();
        let authenticated = AuthenticatedV3Snapshot::open(&client, &snapshot.reference)
            .await
            .unwrap();
        publish.lower = VerifiedPackedLower::from_authenticated_snapshot(
            &authenticated,
            &V3IndexReader::new(client.clone(), 0),
        )
        .await
        .unwrap();
        let source = snapshot.into_final_source_proof().unwrap();
        journal = store
            .freeze_packed_candidate(&journal, publish.record().unwrap(), &budget)
            .await
            .unwrap();
        journal = store
            .advance_packed_journal(&journal, &budget)
            .await
            .unwrap();
        for ordinal in 0..journal.object_count {
            journal = store
                .record_packed_object_progress(&journal, ordinal, true, &budget)
                .await
                .unwrap();
        }
        journal = store
            .advance_packed_journal(&journal, &budget)
            .await
            .unwrap();
        let old_proof = store
            .audit_imported_packed_graph(
                &journal,
                &source,
                &client,
                &budget,
                ImportedGraphAuditOptions {
                    scratch: scratch.path(),
                    limits: V3IndexAuditLimits::default(),
                    cancel: CancellationToken::new(),
                },
            )
            .await
            .unwrap();
        journal = store
            .record_imported_packed_graph(&journal, &old_proof, &budget)
            .await
            .unwrap();
        let before = metadata.rows.lock().await.clone();
        for (native_journal, planned_head) in [
            (JournalId::from_uuid(Uuid::nil()), LayerId::new()),
            (JournalId::new(), LayerId::from_uuid(Uuid::nil())),
        ] {
            assert!(matches!(
                store
                    .clone()
                    .begin_packed_native_quiesce(
                        publish.guard.clone(),
                        publish.expected_layers.clone(),
                        native_journal,
                        planned_head,
                        budget.clone(),
                    )
                    .await,
                Err(WorkspaceError::CorruptMetadata(_))
            ));
            assert_eq!(*metadata.rows.lock().await, before);
        }
        let fence = store
            .clone()
            .begin_packed_native_quiesce(
                publish.guard.clone(),
                publish.expected_layers.clone(),
                JournalId::new(),
                LayerId::new(),
                budget.clone(),
            )
            .await
            .unwrap();
        let mut next = journal.next().unwrap();
        next.graph_receipt = None;
        next.full_proof_digest = [0; 32];
        next.native_rebind = Some(PackedNativeJournalBasis::from_fence(&fence).unwrap());
        let before = metadata.rows.lock().await.clone();
        let mut wrong_id = next.clone();
        wrong_id.journal_id = JournalId::new();
        assert!(matches!(
            store
                .packed_native_journal_write(&journal, &wrong_id, &fence)
                .await,
            Err(WorkspaceError::Fenced)
        ));
        let mut nil_plan = next.clone();
        nil_plan
            .native_rebind
            .as_mut()
            .unwrap()
            .planned_head_layer_id = LayerId::from_uuid(Uuid::nil());
        assert!(matches!(
            store
                .packed_native_journal_write(&journal, &nil_plan, &fence)
                .await,
            Err(WorkspaceError::Fenced)
        ));
        let mut reused_revision = next.clone();
        reused_revision.revision = journal.revision;
        assert!(matches!(
            store
                .packed_native_journal_write(&journal, &reused_revision, &fence)
                .await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(*metadata.rows.lock().await, before);
        let owner = budget
            .admit(&[(V3BudgetPool::Metadata, OPERATION_BYTES)])
            .unwrap();
        let rebound = store
            .packed_native_journal_write(&journal, &next, &fence)
            .await
            .retain(owner)
            .unwrap();
        assert_eq!(rebound.revision, journal.revision + 1);
        assert!(rebound.graph_receipt.is_none());
        assert_eq!(rebound.expected_head, journal.expected_head);
        let actual_head: LayerRecord =
            decode(&rebound.native_rebind.as_ref().unwrap().frozen_head).unwrap();
        assert_eq!(actual_head.state, LayerState::Sealing);
        assert_eq!(actual_head.layer_id, publish.guard.expected_head_layer_id);
        let before = metadata.rows.lock().await.clone();
        assert!(
            store
                .record_imported_packed_graph(&rebound, &old_proof, &budget)
                .await
                .is_err()
        );
        assert_eq!(*metadata.rows.lock().await, before);
        let bound = NativeReboundPackedJournal {
            record: rebound,
            fence,
        };
        let native_graph = store
            .audit_native_rebound_staged_graph(
                &bound,
                &client,
                &budget,
                ImportedGraphAuditOptions {
                    scratch: scratch.path(),
                    limits: V3IndexAuditLimits::default(),
                    cancel: CancellationToken::new(),
                },
            )
            .await
            .unwrap();
        native_graph
            .validate_for_journal(&bound.record, &bound.fence)
            .unwrap();
        assert_eq!(
            native_graph.graph.manifest_reference(),
            source.manifest_reference()
        );
        assert_eq!(
            native_graph.graph.publication_facts().0,
            old_proof.receipt.physical_graph_digest
        );
        let mut newer_record = bound.record.clone();
        newer_record.revision += 1;
        assert!(matches!(
            native_graph.validate_for_journal(&newer_record, &bound.fence),
            Err(WorkspaceError::Fenced)
        ));
        let fresh = store
            .audit_native_rebound_imported_graph(
                &bound,
                &source,
                &client,
                &budget,
                ImportedGraphAuditOptions {
                    scratch: scratch.path(),
                    limits: V3IndexAuditLimits::default(),
                    cancel: CancellationToken::new(),
                },
            )
            .await
            .unwrap();
        assert_ne!(
            fresh.receipt.catalog_context_digest,
            old_proof.receipt.catalog_context_digest
        );
        assert_eq!(
            fresh.receipt.final_source_digest,
            old_proof.receipt.final_source_digest
        );
        let bound = store
            .record_native_rebound_imported_graph(bound, &fresh, &budget)
            .await
            .unwrap();
        assert!(matches!(
            native_graph.validate_for_journal(&bound.record, &bound.fence),
            Err(WorkspaceError::Fenced)
        ));
        let reopened = store
            .reopen_packed_journal(bound.record.journal_id, &bound.record.source, &budget)
            .await
            .unwrap();
        assert_eq!(reopened, bound.record);
        assert!(
            store
                .packed_journal_write(Some(&reopened), &reopened, &[], None)
                .await
                .is_err(),
            "durable PNB3 reopened an ordinary Writable authorization path"
        );
        assert_eq!(reopened.phase, PackedJournalPhase::AwaitingFullProof);
        assert!(reopened.native_rebind.is_some());
        assert_eq!(
            reopened.graph_receipt.as_ref().unwrap().audited_revision,
            fresh.receipt.audited_revision
        );
    }
}
