//! The native packed factory consumes the real frozen source and graph.
//! No digest, durable phase, manifest or allocator ID grants publication.

use super::*;
use crate::chunk::BlockStore;
use crate::workspace::packed::snapshot::{
    FrozenNativeArtifact, NativePromotionFailure, VerifiedHashedNativeView,
};
use crate::workspace::publish::binding::VerifiedPackedLower;
use crate::workspace::stores::kv_store::packed_native_freeze::{
    FrozenNativeDeltaHash, NativeDeltaHashCounts, NativeDeltaHashLimits,
};

#[cfg(test)]
mod tests;

/// This evidence owns the actual graph and the exact durable version bracketed
/// by the real native authority. Only audit_native_effective_graph constructs it.
pub(crate) struct NativeStagedGraphEvidence {
    record: OwnedPackedJournal<PackedJournalRecord>,
    graph: SnapshotIndexContextAudit,
}

pub(crate) struct NativePublicationBuildOptions {
    pub producer: crate::workspace::packed::snapshot::SnapshotProducerOptions,
    pub temporary: std::path::PathBuf,
    pub graph_scratch: std::path::PathBuf,
    pub graph_limits: SnapshotIndexAuditLimits,
    pub native_hash_limits: NativeDeltaHashLimits,
    pub chunk_size: u64,
    pub metadata_cache_bytes: u64,
    pub max_catalog_rows: u64,
    pub cancel: CancellationToken,
}

pub(crate) enum NativePublicationPreparationFailure<K, S>
where
    K: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    BeforeHashed(WorkspaceError),
    HashedAdmission {
        error: WorkspaceError,
        source: Box<VerifiedHashedNativeView<K, S>>,
        graph: Box<NativeStagedGraphEvidence>,
    },
    Promotion {
        failure: NativePromotionFailure<K, S>,
        record: Box<OwnedPackedJournal<PackedJournalRecord>>,
        graph: Box<NativeStagedGraphEvidence>,
    },
    Hashed {
        error: WorkspaceError,
        source: Box<VerifiedHashedNativeView<K, S>>,
        record: Box<OwnedPackedJournal<PackedJournalRecord>>,
        graph: Box<NativeStagedGraphEvidence>,
        attempted_successor: Option<Box<PackedJournalRecord>>,
    },
}

/// Owns every source, reader, VFS, native hash, graph and permit through the
/// final request. Its constructor consumes both independent actual proofs.
pub(crate) struct ReadyNativePackedPublication<K, S>
where
    K: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    store: Arc<KvWorkspaceStore<K>>,
    source: VerifiedHashedNativeView<K, S>,
    graph: NativeStagedGraphEvidence,
    record: OwnedPackedJournal<PackedJournalRecord>,
    lower: VerifiedPackedLower,
    seal: CompletePackedGraphSeal,
    budget: Arc<SnapshotMountBudget>,
    registry_report: Option<PackedRegistryMigrationReport>,
    _candidate_owner: Option<Arc<dyn std::any::Any + Send + Sync>>,
    publication_started: bool,
}

/// Exact attempted writes are diagnostics for committed-state confirmation;
/// no public constructor, Clone or conversion grants publication authority.
pub(crate) struct AttemptedNativePackedPublication {
    writes: Vec<KvWrite>,
    unchanged: Vec<KvCheck>,
    lease_deadline: i64,
    committed: PackedJournalRecord,
}

pub(crate) struct NativePackedPublicationFailure<K, S>
where
    K: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    pub(crate) error: WorkspaceError,
    pub(crate) publication: Box<ReadyNativePackedPublication<K, S>>,
    pub(crate) attempted_successor: Option<Box<AttemptedNativePackedPublication>>,
}

pub(crate) struct NativePackedPublicationOutcome {
    pub(crate) record: OwnedPackedJournal<PackedJournalRecord>,
    pub(crate) binding: PackedLowerBindingRecord,
    pub(crate) guard: HeadGuard,
    pub(crate) sealed_source: BaseRevision,
    pub(crate) registry_report: Option<PackedRegistryMigrationReport>,
}

impl<K, S> ReadyNativePackedPublication<K, S>
where
    K: WorkspaceKvBackend + 'static,
    S: BlockStore + Send + Sync + 'static,
{
    /// The spawned owner keeps every source/graph/native authority through the
    /// actual transport. Dropping this receiver cannot cancel the final CAS.
    pub(crate) async fn commit(
        mut self,
    ) -> Result<NativePackedPublicationOutcome, NativePackedPublicationFailure<K, S>> {
        if self.publication_started {
            return Err(NativePackedPublicationFailure {
                error: WorkspaceError::Fenced,
                publication: Box::new(self),
                attempted_successor: None,
            });
        }
        self.publication_started = true;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut attempted = None;
            let result = self.commit_owned(&mut attempted).await;
            let result = match result {
                Ok(result) => Ok(result),
                Err(error) => Err(NativePackedPublicationFailure {
                    error,
                    publication: Box::new(self),
                    attempted_successor: attempted.map(Box::new),
                }),
            };
            let _ = sender.send(result);
        });
        // The driver always sends its owned result. A task panic is an internal
        // invariant violation, not permission to replay an ambiguous commit.
        receiver
            .await
            .expect("native packed publication driver stopped")
    }

    async fn confirm_attempt(
        &self,
        attempted: &AttemptedNativePackedPublication,
    ) -> Result<NativePackedPublicationOutcome, WorkspaceError> {
        let mut expected = attempted.unchanged.clone();
        for write in &attempted.writes {
            let (key, value) = match write {
                KvWrite::Put { key, value } => (key.clone(), Some(value.clone())),
                KvWrite::Delete { key } => (key.clone(), None),
            };
            append_exact_checks(
                &mut expected,
                vec![KvCheck {
                    key,
                    expected: value,
                }],
            )?;
        }
        // Confirm the complete successor in one transaction, without point
        // reads or replaying the mutation. Keep the native proof bounds and
        // the journal key bound; the original absolute deadline still applies.
        let expected =
            crate::workspace::stores::kv_store::native_read_conflict::normalize(expected)?;
        if expected
            .iter()
            .any(|check| check.key.len() > journal_point_limits().max_key_bytes)
        {
            return Err(WorkspaceError::Fenced);
        }
        if !self
            .store
            .backend
            .compare_and_swap_before(&expected, &[], attempted.lease_deadline)
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        self.outcome(&attempted.committed)
    }

    fn outcome(
        &self,
        committed: &PackedJournalRecord,
    ) -> Result<NativePackedPublicationOutcome, WorkspaceError> {
        let binding = committed
            .commit_target
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?
            .clone();
        let phase = self.source.phase_authority();
        let basis = committed
            .native_rebind
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?;
        let publication = basis.publication.as_ref().ok_or(WorkspaceError::Fenced)?;
        Ok(NativePackedPublicationOutcome {
            record: committed.clone().retain(
                self.budget
                    .admit(&[(SnapshotBudgetPool::Metadata, RECORD_LIMIT as u64)])
                    .map_err(journal_budget_error)?,
            )?,
            guard: HeadGuard {
                expected_head_layer_id: binding.head_layer_id,
                expected_head_epoch: binding.head_epoch,
                ..phase.source_guard().clone()
            },
            binding,
            sealed_source: BaseRevision {
                layer_id: committed.guard.expected_head_layer_id,
                sealed_version: publication.source_sealed_version,
                root_hash: phase.native_delta_hash().root_hash(),
            },
            registry_report: self.registry_report,
        })
    }

    async fn commit_owned(
        &self,
        attempted: &mut Option<AttemptedNativePackedPublication>,
    ) -> Result<NativePackedPublicationOutcome, WorkspaceError> {
        let _owner = self
            .budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let phase = self.source.phase_authority();
        let authority = NativeJournalAuthority::Hashed(phase);
        Self::check_final_packet(self)?;
        for _ in 0..CAS_MAX_RETRIES {
            self.source.validate().await.map_err(journal_budget_error)?;
            let target = self
                .record
                .commit_target
                .as_ref()
                .ok_or(WorkspaceError::Fenced)?;
            let native = self
                .record
                .native_rebind
                .as_ref()
                .ok_or(WorkspaceError::Fenced)?;
            let plan = native.publication.as_ref().ok_or(WorkspaceError::Fenced)?;
            let claimed = phase.native_quiesce().requires_recovery_owner();
            let root_change = self
                .store
                .registry_transition_root(&self.record, true)
                .await?;
            let root_mapping =
                KvWorkspaceStore::<K>::registry_history_root_change(target, &root_change)?;
            let carrier_basis =
                crate::workspace::stores::kv_store::packed_carrier_basis::PackedCarrierBasis {
                    carrier_revision: plan.carrier_revision(),
                    native_sealed_source_revision: BaseRevision {
                        layer_id: self.record.guard.expected_head_layer_id,
                        sealed_version: plan.source_sealed_version,
                        root_hash: phase.native_delta_hash().root_hash(),
                    },
                    source_binding: target.clone(),
                    registry_incarnation: self.record.source.staging_id,
                };
            let carrier_bytes = carrier_basis.encode()?;
            let carrier_claim = crate::workspace::stores::kv_store::packed_carrier_basis::PackedCarrierBasis::claim(&carrier_bytes)?;
            let extras = vec![
                active_key(self.record.journal_id),
                ACTIVE_COUNT_KEY.to_vec(),
                hot_layer_key(plan.carrier_layer_id),
                hot_layer_key(native.planned_head_layer_id),
                packed_history_key(target.workspace_id, target.binding.binding_version),
                root_change.0.clone(),
                root_mapping.0.clone(),
                open_snapshot_recovery_key(target.workspace_id),
                hot_allocator_key("sealed_version"),
                hot_journal_key(target.workspace_id, native.native_journal_id),
                open_snapshot_key(target.workspace_id),
                format!("packed/native-recovery-claim/{}", native.native_journal_id).into_bytes(),
                crate::workspace::stores::kv_store::packed_carrier_basis::packed_carrier_basis_key(
                    plan.carrier_layer_id,
                ),
                crate::workspace::stores::kv_store::packed_carrier_basis::packed_carrier_claim_key(
                    plan.carrier_layer_id,
                ),
                // Carrier publication is a maintenance transition: unlike an
                // ordinary entity CAS it keeps the catalog header in its exact
                // check set, so a concurrent migrate cannot interleave between
                // packet validation and the final CAS. Appended last so every
                // earlier authority index is unchanged.
                CONTROL_KEY.to_vec(),
            ];
            let mut actual = self
                .store
                .native_staged_journal_authorities(&self.record, &extras, &authority)
                .await?;
            let (_registry_owner, registry_checks) = self
                .store
                .packed_registry_publication_checks(&self.record.expected_binding)
                .await?;
            append_exact_checks(&mut actual.checks, registry_checks)?;
            let expected_raw = self.record.encode()?;
            let active = active_count(&actual.values[13])?;
            let sealed_allocator: i64 = bounded_native_value(&actual.values[20])?;
            let recovery: SnapshotRecoveryRecord = bounded_native_value(&actual.values[19])?;
            let current_journal: SealJournal = bounded_native_value(&actual.values[21])?;
            let allocator: i64 = bounded_native_value(&actual.values[11])?;
            let open = actual.values[22]
                .as_deref()
                .map(|raw| decode_open_value::<SnapshotOpenRecord>(raw, OPEN_RECORD_MAX_BYTES))
                .transpose()?;
            if claimed != actual.values[23].is_some()
                || (claimed && open.is_none())
                || open.as_ref().is_some_and(|open| {
                    open.workspace_id != target.workspace_id
                        || open.expires_at_ns <= actual.now
                        || (claimed
                            && (open.state != SnapshotOpenState::Recovering
                                || !open.recovery_required))
                })
            {
                return Err(WorkspaceError::Fenced);
            }
            let deadline = open.as_ref().map_or(actual.authority_deadline_ns, |open| {
                actual.authority_deadline_ns.min(open.expires_at_ns)
            });
            if actual.values[10].as_deref() != Some(expected_raw.as_slice())
                || actual.values[12] != actual.values[10]
                || active == 0
                || [14usize, 15, 16, 18, 24, 25]
                    .iter()
                    .any(|index| actual.values[*index].is_some())
                || actual.values[17] != root_change.1
                || !recovery.incomplete
                || recovery.workspace_id != target.workspace_id
                || sealed_allocator <= 0
                || sealed_allocator as u64
                    <= plan.source_sealed_version.max(plan.carrier_sealed_version)
                || allocator < 2
                || &current_journal != phase.journal()
                || actual.workspace.active_lease != Some(actual.lease.lease_id)
            {
                return Err(WorkspaceError::Fenced);
            }
            let mut source_head = actual.head.clone();
            source_head.state = LayerState::Sealed;
            source_head.sealed_version = Some(plan.source_sealed_version);
            source_head.delta_digest = Some(phase.native_delta_hash().delta_digest());
            source_head.root_hash = Some(phase.native_delta_hash().root_hash());
            source_head.owner_workspace_id = None;
            source_head.sealed_at_ns = Some(actual.now);
            let carrier = LayerRecord {
                layer_id: plan.carrier_layer_id,
                parent_layer_id: None,
                state: LayerState::Sealed,
                schema_version: WORKSPACE_SCHEMA_VERSION,
                sealed_version: Some(plan.carrier_sealed_version),
                delta_digest: Some(plan.carrier_delta_digest),
                root_hash: Some(plan.carrier_root_hash),
                depth: 1,
                owner_workspace_id: None,
                next_sequence: 1,
                owned_slice_count: 0,
                owned_bytes: 0,
                created_at_ns: actual.now,
                sealed_at_ns: Some(actual.now),
            };
            let head = writable_layer(
                native.planned_head_layer_id,
                carrier.layer_id,
                2,
                target.workspace_id,
                actual.now,
            );
            let mut workspace = actual.workspace.clone();
            workspace.head_layer_id = head.layer_id;
            workspace.head_epoch = native.planned_head_epoch;
            workspace.state = WorkspaceState::Active;
            workspace.updated_at_ns = actual.now;
            let mut lease = actual.lease.clone();
            lease.base_revision = plan.carrier_revision();
            lease.updated_at_ns = actual.now;
            let mut native_journal = phase.journal().clone();
            native_journal.phase = SealPhase::Completed;
            native_journal.updated_at_ns = actual.now;
            let floor = target
                .highest_inode
                .checked_add(1)
                .ok_or_else(|| journal_error("native inode floor exhausted"))?
                .max(2);
            let next_inode = allocator.max(floor);
            let mut committed = self.record.next()?;
            committed.phase = PackedJournalPhase::Committed;
            let completion = committed
                .native_rebind
                .as_mut()
                .ok_or(WorkspaceError::Fenced)?
                .publication
                .as_mut()
                .ok_or(WorkspaceError::Fenced)?;
            completion.final_source_guard = Some(phase.source_guard().clone());
            completion.final_source_root_hash = Some(phase.native_delta_hash().root_hash());
            completion.final_source_delta_digest = Some(phase.native_delta_hash().delta_digest());
            let mut writes = vec![
                put(hot_layer_key(source_head.layer_id), &source_head)?,
                put(hot_layer_key(carrier.layer_id), &carrier)?,
                put(hot_layer_key(head.layer_id), &head)?,
                put(hot_workspace_key(workspace.workspace_id), &workspace)?,
                put(hot_lease_key(lease.workspace_id, lease.lease_id), &lease)?,
                put(hot_allocator_key("inode"), &next_inode)?,
                put(
                    hot_journal_key(native_journal.workspace_id, native_journal.journal_id),
                    &native_journal,
                )?,
                KvWrite::Delete {
                    key: extras[7].clone(),
                },
                KvWrite::Put {
                    key: packed_current_key(target.workspace_id),
                    value: target.encode()?,
                },
                KvWrite::Put {
                    key: extras[4].clone(),
                    value: target.encode()?,
                },
                KvWrite::Put {
                    key: journal_key(committed.journal_id),
                    value: committed.encode()?,
                },
                KvWrite::Delete {
                    key: active_key(committed.journal_id),
                },
                KvWrite::Put {
                    key: ACTIVE_COUNT_KEY.to_vec(),
                    value: (active - 1).to_le_bytes().to_vec(),
                },
                put(
                    PACKED_ROOT_GENERATION_KEY.to_vec(),
                    &next_packed_root_generation(&actual.values[8])?,
                )?,
                put(
                    LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                    &next_layer_inventory_generation(&actual.values[9])?,
                )?,
                KvWrite::Put {
                    key: root_change.0,
                    value: root_change.2.ok_or(WorkspaceError::Fenced)?,
                },
                KvWrite::Put {
                    key: root_mapping.0,
                    value: root_mapping.2.ok_or(WorkspaceError::Fenced)?,
                },
            ];
            // The immutable carrier locator joins the actual publication CAS.
            // It grants no independent authority; fork must still fence source
            // history, registry root, native carrier and root generations.
            writes.push(KvWrite::Put {
                key: extras[12].clone(),
                value: carrier_bytes,
            });
            writes.push(KvWrite::Put {
                key: extras[13].clone(),
                value: carrier_claim,
            });
            // The same publication transaction finishes the actual open
            // recovery owner and consumes its claim pointer. Lost replies
            // confirm these exact successors along with carrier/PWB/PPJ.
            if let Some(mut open) = open {
                open.state = SnapshotOpenState::Ready;
                open.recovery_required = false;
                writes.push(put(extras[10].clone(), &open)?);
            }
            if claimed {
                writes.push(KvWrite::Delete {
                    key: extras[11].clone(),
                });
            }
            let _writer_owner = self.store.prepare_administrative_packed_writer(
                target.workspace_id, crate::workspace::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::Update, &mut actual.checks, &mut writes,
            ).await?;
            let _native_holds = self
                .store
                .prepare_native_publication_owner_cas(
                    &self.record,
                    &mut actual.checks,
                    &mut writes,
                    deadline,
                )
                .await?;
            let _native_reverse = self
                .store
                .prepare_native_reverse_cas(&mut actual.checks, &mut writes)
                .await?;
            let packet = self
                .store
                .prepare_topology_envelope(actual.checks, writes, Some(deadline))
                .await?;
            actual.checks = packet.checks.clone();
            let writes = packet.writes.clone();
            let written_keys = writes
                .iter()
                .map(|write| match write {
                    KvWrite::Put { key, .. } | KvWrite::Delete { key } => key.as_slice(),
                })
                .collect::<std::collections::BTreeSet<_>>();
            let unchanged = actual
                .checks
                .iter()
                .filter(|check| !written_keys.contains(check.key.as_slice()))
                .cloned()
                .collect::<Vec<_>>();
            *attempted = Some(AttemptedNativePackedPublication {
                writes: writes.clone(),
                unchanged,
                lease_deadline: deadline,
                committed: committed.clone(),
            });
            match self.store.commit_prepared_topology_packet(&packet).await {
                Ok(true) => return self.outcome(&committed),
                Ok(false) => {
                    *attempted = None;
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    if let Some(attempt) = attempted.as_ref()
                        && let Ok(outcome) = self.confirm_attempt(attempt).await
                    {
                        return Ok(outcome);
                    }
                    return Err(error);
                }
            }
        }
        Err(WorkspaceError::Busy)
    }

    fn check_final_packet(&self) -> Result<(), WorkspaceError> {
        KvWorkspaceStore::<K>::check_complete_seal(&self.record, &self.seal)?;
        if self.record.phase != PackedJournalPhase::Verified
            || self.record.full_proof_digest != self.seal.proof_digest
            || self.source.candidate_manifest() != self.lower.manifest_reference()
            || self.graph.graph.manifest_reference() != self.lower.manifest_reference()
            || !self.source.phase_authority().is_hashed()
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
}

impl<K: WorkspaceKvBackend + 'static> KvWorkspaceStore<K> {
    /// Complete ordinary factory entrypoint. Each remote producer request uses
    /// registered before-PUT authority; complete graph, source comparison and
    /// native Hashed authority are consumed by the final owned packet below.
    pub(crate) async fn prepare_frozen_native_publication<O, S>(
        self: &Arc<Self>,
        artifact: FrozenNativeArtifact<K, S>,
        client: ObjectClient<O>,
        options: super::super::NativePublicationBuildOptions,
    ) -> Result<
        ReadyNativePackedPublication<K, S>,
        super::super::NativePublicationPreparationFailure<K, S>,
    >
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = store
                .prepare_frozen_native_publication_owned(artifact, client, options)
                .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .expect("native packed factory driver stopped")
    }

    async fn prepare_frozen_native_publication_owned<O, S>(
        self: &Arc<Self>,
        artifact: FrozenNativeArtifact<K, S>,
        client: ObjectClient<O>,
        options: NativePublicationBuildOptions,
    ) -> Result<ReadyNativePackedPublication<K, S>, NativePublicationPreparationFailure<K, S>>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        let budget = artifact.mount_budget();
        let early = NativePublicationPreparationFailure::BeforeHashed;
        let observer = budget
            .read_observer(client.read_observer())
            .map_err(|error| early(journal_budget_error(error)))?;
        let client = client.with_read_observer(
            observer,
            crate::cadapter::read_observer::Engine::Packed,
            crate::cadapter::read_observer::Phase::Startup,
            crate::cadapter::read_observer::Origin::Demand,
        );
        self.configure_packed_reader_pin_budget(budget.clone())
            .map_err(early)?;
        #[cfg(test)]
        eprintln!("packed native preparation stage=artifact-validate");
        artifact
            .validate()
            .await
            .map_err(|error| early(journal_budget_error(error)))?;
        if options.chunk_size == 0 || options.cancel.is_cancelled() {
            return Err(early(WorkspaceError::Busy));
        }
        #[cfg(test)]
        eprintln!("packed native preparation stage=registry-migrate");
        let registry_report: PackedRegistryMigrationReport = self
            .migrate_packed_object_registry(
                &client,
                &budget,
                PackedRegistryMigrationOptions {
                    scratch: &options.graph_scratch,
                    graph_limits: options.graph_limits,
                    max_catalog_rows: options.max_catalog_rows,
                    cancel: options.cancel.clone(),
                },
            )
            .await
            .map_err(early)?;
        #[cfg(test)]
        eprintln!("packed native preparation stage=journal-begin");
        let record = self
            .begin_native_packed_journal(&artifact)
            .await
            .map_err(early)?;
        #[cfg(test)]
        eprintln!("packed native preparation stage=candidate-produce");
        let (artifact, manifest, record) = self
            .build_registered_native_candidate(
                record,
                artifact,
                client.clone(),
                options.temporary,
                options.producer,
            )
            .await
            .map_err(early)?;
        #[cfg(test)]
        eprintln!("packed native preparation stage=candidate-freeze");
        let record = self
            .freeze_native_candidate(&record, &artifact, &manifest)
            .await
            .map_err(early)?;
        #[cfg(test)]
        eprintln!("packed native preparation stage=candidate-readback");
        let record = self
            .native_readback(record, &artifact, &client, &options.cancel)
            .await
            .map_err(early)?;
        #[cfg(test)]
        eprintln!("packed native preparation stage=graph-audit");
        let graph = self
            .audit_native_effective_graph(
                &record,
                &NativeJournalAuthority::Quiesced(artifact.native_quiesce()),
                &client,
                &budget,
                ImportedGraphAuditOptions {
                    scratch: &options.graph_scratch,
                    limits: options.graph_limits,
                    cancel: options.cancel.clone(),
                },
            )
            .await
            .map_err(early)?;
        #[cfg(test)]
        eprintln!("packed native preparation stage=candidate-open");
        let _manifest_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 512 << 10)])
            .map_err(|error| early(journal_budget_error(error)))?;
        let snapshot =
            crate::workspace::packed::snapshot::AuthenticatedSnapshot::open(&client, &manifest)
                .await
                .map_err(|error| early(journal_budget_error(error)))?;
        let candidate = Arc::new(
            crate::workspace::packed::PackedReadonlyMeta::from_snapshot_budget(
                client,
                snapshot,
                options.chunk_size,
                options.metadata_cache_bytes,
                budget.clone(),
            )
            .map_err(|error| early(journal_budget_error(error)))?,
        );
        #[cfg(test)]
        eprintln!("packed native preparation stage=candidate-compare");
        let source = artifact
            .compare_candidate(candidate.clone())
            .await
            .map_err(|error| early(journal_budget_error(error)))?;
        #[cfg(test)]
        eprintln!("packed native preparation stage=native-hash-capture");
        let native_hash = Arc::new(
            FrozenNativeDeltaHash::capture(
                source.native_quiesce().clone(),
                source.native_reader_session(),
                options.native_hash_limits,
            )
            .await
            .map_err(early)?,
        );
        #[cfg(test)]
        eprintln!("packed native preparation stage=native-hash-promote");
        let source = match source.promote_native_hashed(native_hash).await {
            Ok(Ok(source)) => source,
            Ok(Err(failure)) => {
                return Err(NativePublicationPreparationFailure::Promotion {
                    failure,
                    record: Box::new(record),
                    graph: Box::new(graph),
                });
            }
            Err(error) => return Err(early(journal_budget_error(error))),
        };
        #[cfg(test)]
        eprintln!("packed native preparation stage=publication-assemble");
        let mut ready = self
            .assemble_native_publication(source, graph, budget)
            .await?;
        ready.registry_report = Some(registry_report);
        ready._candidate_owner = Some(candidate);
        Ok(ready)
    }

    /// Reserve both native sealed versions with PPJ/active/root creation. The
    /// frozen artifact supplies actual source and catalog facts; callers never
    /// choose a publication topology, effective digest or sealed version.
    pub(crate) async fn begin_native_packed_journal<S>(
        &self,
        artifact: &FrozenNativeArtifact<K, S>,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError>
    where
        S: BlockStore + Send + Sync + 'static,
    {
        let budget = artifact.mount_budget();
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        artifact.validate().await.map_err(journal_budget_error)?;
        let native = artifact.native_quiesce();
        if !native.belongs_to_store(self) {
            return Err(WorkspaceError::Fenced);
        }
        let ids = [
            hot_allocator_key("sealed_version"),
            JOURNAL_FEATURE_KEY.to_vec(),
        ];
        let values = self.packed_journal_values(&ids).await?;
        if values.len() != ids.len() || values[1].as_deref().is_some_and(|v| v != b"PPJ3") {
            return Err(journal_error("native begin allocator/feature read"));
        }
        let first: i64 = bounded_native_value(&values[0])?;
        let after = first
            .checked_add(2)
            .filter(|v| first > 0 && *v > first)
            .ok_or_else(|| journal_error("native sealed allocator exhausted"))?;
        let empty = delta_digest(&CanonicalLayerDelta::default())?;
        let mut basis = PackedNativeJournalBasis::from_fence(native)?;
        let plan = NativePackedPublicationBasis {
            carrier_layer_id: LayerId::new(),
            source_sealed_version: first as u64,
            carrier_sealed_version: (first + 1) as u64,
            carrier_delta_digest: empty,
            carrier_root_hash: root_hash([0; 32], empty),
            final_source_guard: None,
            final_source_root_hash: None,
            final_source_delta_digest: None,
        };
        let carrier = plan.carrier_layer_id;
        basis.publication = Some(plan);
        let journal_id = JournalId::new();
        let staging_id = Uuid::new_v4();
        let digest = artifact.source_digest().map_err(journal_budget_error)?;
        let mut provenance = Sha256::new();
        provenance.update(b"BrewFS packed v3 native producer provenance\0");
        provenance.update(native.canonical_receipt_bytes());
        provenance.update(digest);
        provenance.update(journal_id.as_bytes());
        provenance.update(staging_id.as_bytes());
        let old = native.mapping().old_layers();
        let record = PackedJournalRecord {
            journal_id,
            revision: 1,
            phase: PackedJournalPhase::Building,
            guard: native.mapping().old_guard().clone(),
            source: PackedSourceView {
                snapshot_backed: true,
                effective_view_digest: digest,
                frozen_view_token: native.canonical_receipt_digest(),
                build_provenance_digest: provenance.finalize().into(),
                build_owner: format!("native-seal-{}", native.mapping().journal_id()),
                staging_id,
                staging_prefix: format!("packed/staging/{journal_id}/{staging_id}"),
            },
            expected_head: encode(&old[0])?,
            expected_base: encode(&old[1])?,
            expected_binding: native.binding().clone(),
            object_count: 0,
            inventory_digest: inventory_start(),
            commit_target: None,
            full_proof_digest: [0; 32],
            graph_receipt: None,
            native_rebind: Some(basis),
            abort_reason: String::new(),
        };
        self.packed_journal_write_under(
            None,
            &record,
            &[
                (ids[0].clone(), values[0].clone(), Some(encode(&after)?)),
                (ids[1].clone(), values[1].clone(), Some(b"PPJ3".to_vec())),
                registry::begin_root_change(&record)?,
                (hot_layer_key(carrier), None, None),
            ],
            None,
            Some(&NativeJournalAuthority::Quiesced(native)),
        )
        .await
        .retain(owner)
    }

    /// Freeze only the manifest actually made by this registered source build.
    /// Compare_candidate additionally enforces that same actual manifest later.
    async fn freeze_native_candidate<S>(
        &self,
        expected: &PackedJournalRecord,
        artifact: &FrozenNativeArtifact<K, S>,
        manifest: &SnapshotObjectRef,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError>
    where
        S: BlockStore + Send + Sync + 'static,
    {
        let budget = artifact.mount_budget();
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        artifact.validate().await.map_err(journal_budget_error)?;
        if expected.phase != PackedJournalPhase::Building
            || expected.source.effective_view_digest
                != artifact.source_digest().map_err(journal_budget_error)?
            || manifest.kind != SnapshotObjectKind::Manifest
        {
            return Err(WorkspaceError::Fenced);
        }
        let basis = expected
            .native_rebind
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?;
        let publication = basis.publication.as_ref().ok_or(WorkspaceError::Fenced)?;
        let mapping = self
            .packed_journal_value(identity_key(expected.journal_id, &manifest.key))
            .await?
            .ok_or(WorkspaceError::Fenced)?;
        let ordinal = u64::from_le_bytes(mapping.as_slice().try_into().map_err(journal_error)?);
        let object = self
            .reopen_packed_object(expected, ordinal, &budget)
            .await?;
        if object.reference != *manifest || !object.uploaded {
            return Err(WorkspaceError::Fenced);
        }
        let mut next = expected.next()?;
        next.phase = PackedJournalPhase::Uploading;
        next.commit_target = Some(PackedLowerBindingRecord {
            workspace_id: expected.guard.workspace_id,
            head_layer_id: basis.planned_head_layer_id,
            head_epoch: basis.planned_head_epoch,
            base_revision: publication.carrier_revision(),
            highest_inode: artifact
                .highest_inode()
                .max(expected.expected_binding.highest_inode),
            binding: PackedLowerBinding {
                binding_version: expected
                    .expected_binding
                    .binding
                    .binding_version
                    .checked_add(1)
                    .ok_or_else(|| journal_error("native binding version exhausted"))?,
                base_layer_id: publication.carrier_layer_id,
                manifest: manifest.clone(),
            },
        });
        self.packed_journal_write_under(
            Some(expected),
            &next,
            &[],
            None,
            Some(&NativeJournalAuthority::from_captured(
                artifact.native_quiesce(),
            )),
        )
        .await
        .retain(owner)
    }

    async fn native_readback<O, S>(
        &self,
        expected: OwnedPackedJournal<PackedJournalRecord>,
        artifact: &FrozenNativeArtifact<K, S>,
        client: &ObjectClient<O>,
        cancel: &CancellationToken,
    ) -> Result<OwnedPackedJournal<PackedJournalRecord>, WorkspaceError>
    where
        O: ObjectBackend + Clone,
        S: BlockStore + Send + Sync + 'static,
    {
        let budget = artifact.mount_budget();
        let operation = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let bytes_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 128 << 10)])
            .map_err(journal_budget_error)?;
        if !matches!(
            expected.phase,
            PackedJournalPhase::Uploading | PackedJournalPhase::Readback
        ) {
            return Err(WorkspaceError::Fenced);
        }
        let native = NativeJournalAuthority::from_captured(artifact.native_quiesce());
        let mut record = if expected.phase == PackedJournalPhase::Uploading {
            let mut next = expected.next()?;
            next.phase = PackedJournalPhase::Readback;
            self.packed_journal_write_under(Some(&expected), &next, &[], None, Some(&native))
                .await?
                .retain(
                    budget
                        .admit(&[(SnapshotBudgetPool::Metadata, RECORD_LIMIT as u64)])
                        .map_err(journal_budget_error)?,
                )?
        } else {
            expected
        };
        let observer = budget
            .read_observer(client.read_observer())
            .map_err(journal_budget_error)?;
        let client = client.clone().with_read_observer(
            observer,
            crate::cadapter::read_observer::Engine::Packed,
            crate::cadapter::read_observer::Phase::Startup,
            crate::cadapter::read_observer::Origin::Demand,
        );
        let mut buffer = vec![0u8; 64 << 10];
        let mut inventory = inventory_start();
        for ordinal in 0..record.object_count {
            if cancel.is_cancelled() || budget.state().closed {
                return Err(WorkspaceError::Busy);
            }
            let object = self.reopen_packed_object(&record, ordinal, &budget).await?;
            if !object.uploaded {
                return Err(WorkspaceError::Fenced);
            }
            let reference = &object.reference;
            reference.encode_value().map_err(journal_error)?;
            let mut hash = Sha256::new();
            let mut offset = 0;
            while offset < reference.object_len {
                if cancel.is_cancelled() || budget.state().closed {
                    return Err(WorkspaceError::Busy);
                }
                let length = (reference.object_len - offset).min(buffer.len() as u64) as usize;
                let bytes = client
                    .typed_bounded_range(
                        match reference.kind {
                            SnapshotObjectKind::GroupContainer => {
                                crate::cadapter::read_observer::ReadClass::GroupMetadata
                            }
                            SnapshotObjectKind::LargeData => {
                                crate::cadapter::read_observer::ReadClass::ExternalPayload
                            }
                            kind => crate::workspace::packed::snapshot::page_read_class(kind)
                                .map_err(journal_budget_error)?,
                        },
                        &reference.key,
                        offset,
                        length as u64,
                    )
                    .await
                    .map_err(|error| WorkspaceError::Backend(error.to_string()))?;
                let read = bytes.len();
                if read <= length {
                    buffer[..read].copy_from_slice(&bytes);
                }
                if read == 0 || read > length {
                    return Err(journal_error("native readback length mismatch"));
                }
                hash.update(&buffer[..read]);
                offset += read as u64;
            }
            if <[u8; 32]>::from(hash.finalize()) != reference.digest
                || client
                    .typed_object_size(
                        match reference.kind {
                            crate::workspace::packed::snapshot::SnapshotObjectKind::GroupContainer =>
                                crate::cadapter::read_observer::ReadClass::GroupMetadata,
                            crate::workspace::packed::snapshot::SnapshotObjectKind::LargeData =>
                                crate::cadapter::read_observer::ReadClass::ExternalPayload,
                            kind => crate::workspace::packed::snapshot::page_read_class(kind)
                                .map_err(journal_budget_error)?,
                        },
                        &reference.key,
                    )
                    .await
                    .map_err(|error| WorkspaceError::Backend(error.to_string()))?
                    != Some(reference.object_len)
            {
                return Err(journal_error(
                    "native readback full object identity mismatch",
                ));
            }
            if object.readback_recorded {
                inventory = inventory_append(inventory, ordinal, reference)?;
                continue;
            }
            let mut readback = object.value.clone();
            readback.readback_recorded = true;
            let next = record.next()?;
            let updated = self
                .packed_journal_write_under(
                    Some(&record),
                    &next,
                    &[(
                        object_key(record.journal_id, ordinal),
                        Some(object.encode()?),
                        Some(readback.encode()?),
                    )],
                    None,
                    Some(&native),
                )
                .await?;
            record = updated.retain(
                budget
                    .admit(&[(SnapshotBudgetPool::Metadata, RECORD_LIMIT as u64)])
                    .map_err(journal_budget_error)?,
            )?;
            inventory = inventory_append(inventory, ordinal, reference)?;
        }
        if inventory != record.inventory_digest {
            return Err(WorkspaceError::Fenced);
        }
        let mut next = record.next()?;
        next.phase = PackedJournalPhase::AwaitingFullProof;
        let output = self
            .packed_journal_write_under(Some(&record), &next, &[], None, Some(&native))
            .await?;
        drop(bytes_owner);
        Ok(output).retain(operation)
    }

    pub(super) async fn audit_native_effective_graph<O: ObjectBackend + Clone>(
        &self,
        expected: &PackedJournalRecord,
        native: &NativeJournalAuthority<'_, K>,
        client: &ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
        options: ImportedGraphAuditOptions<'_>,
    ) -> Result<NativeStagedGraphEvidence, WorkspaceError> {
        let operation = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let record_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, RECORD_LIMIT as u64)])
            .map_err(journal_budget_error)?;
        if !matches!(
            expected.phase,
            PackedJournalPhase::AwaitingFullProof | PackedJournalPhase::Verified
        ) || expected
            .native_rebind
            .as_ref()
            .and_then(|basis| basis.publication.as_ref())
            .is_none()
            || !expected.source.snapshot_backed
        {
            return Err(WorkspaceError::Fenced);
        }
        let cancel = options.cancel.clone();
        self.packed_journal_write_under(Some(expected), expected, &[], None, Some(native))
            .await?;
        let graph = self
            .audit_staged_packed_graph(expected, client, budget, options)
            .await?;
        self.packed_journal_write_under(Some(expected), expected, &[], None, Some(native))
            .await?;
        if cancel.is_cancelled() || budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        drop(operation);
        Ok(NativeStagedGraphEvidence {
            record: expected.clone().retain(record_owner)?,
            graph,
        })
    }

    /// A fresh recovery comparison/hash must supply a genuine Hashed token.
    /// The decoded recovery basis and original Q receipt grant no graph audit
    /// permission. The dedicated actual phase checks bracket this new audit.
    pub(crate) async fn audit_hashed_native_effective_graph<O, S>(
        &self,
        expected: &PackedJournalRecord,
        source: &VerifiedHashedNativeView<K, S>,
        client: &ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
        options: ImportedGraphAuditOptions<'_>,
    ) -> Result<NativeStagedGraphEvidence, WorkspaceError>
    where
        O: ObjectBackend + Clone,
        S: BlockStore + Send + Sync + 'static,
    {
        source.validate().await.map_err(journal_budget_error)?;
        if source.source_digest() != expected.source.effective_view_digest
            || expected
                .commit_target
                .as_ref()
                .is_none_or(|target| &target.binding.manifest != source.candidate_manifest())
        {
            return Err(WorkspaceError::Fenced);
        }
        self.audit_native_effective_graph(
            expected,
            &NativeJournalAuthority::Hashed(source.phase_authority()),
            client,
            budget,
            options,
        )
        .await
    }

    async fn assemble_native_publication_inner<S>(
        &self,
        source: &VerifiedHashedNativeView<K, S>,
        graph: &NativeStagedGraphEvidence,
        record: &mut OwnedPackedJournal<PackedJournalRecord>,
        attempted: &mut Option<PackedJournalRecord>,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<(CompletePackedGraphSeal, VerifiedPackedLower), WorkspaceError>
    where
        S: BlockStore + Send + Sync + 'static,
    {
        let _operation = budget
            .admit(&[(SnapshotBudgetPool::Metadata, OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        source.validate().await.map_err(journal_budget_error)?;
        let phase = source.phase_authority();
        let native = NativeJournalAuthority::Hashed(phase);
        let lower = VerifiedPackedLower::from_staged_graph(&graph.graph)?;
        let (physical, _, highest) = graph.graph.publication_facts();
        if !phase.is_hashed()
            || !phase.native_quiesce().belongs_to_store(self)
            || graph.record.value != record.value
            || *source.candidate_manifest() != *lower.manifest_reference()
            || source.source_digest() != record.source.effective_view_digest
            || source.highest_inode() != highest as i64
            || record.source.frozen_view_token != phase.canonical_receipt_digest()
            || record.commit_target.as_ref().is_none_or(|target| {
                target.binding.manifest != *lower.manifest_reference()
                    || target.highest_inode
                        != lower
                            .highest_inode()
                            .max(record.expected_binding.highest_inode)
            })
        {
            return Err(WorkspaceError::Fenced);
        }
        self.packed_journal_write_under(Some(record), record, &[], None, Some(&native))
            .await?;
        let receipt = if let Some(receipt) = &record.graph_receipt {
            if receipt.physical_graph_digest != physical
                || receipt.final_source_digest != source.source_digest()
                || receipt.highest_inode != highest
                || receipt.manifest != *source.candidate_manifest()
                || receipt.inventory_digest != record.inventory_digest
                || receipt.object_count != graph.graph.counts().objects
                || !receipt.snapshot_backed
                || receipt.catalog_context_digest
                    != audit_basis_digest(record, receipt.audited_revision)?
            {
                return Err(WorkspaceError::Fenced);
            }
            receipt.clone()
        } else {
            let receipt = PackedGraphReceipt {
                audited_revision: record.revision,
                staging_incarnation: record.source.staging_id,
                manifest: source.candidate_manifest().clone(),
                object_count: record.object_count,
                inventory_digest: record.inventory_digest,
                physical_graph_digest: physical,
                final_source_digest: source.source_digest(),
                catalog_context_digest: audit_basis_digest(record, record.revision)?,
                snapshot_backed: true,
                highest_inode: highest,
            };
            let mut next = record.next()?;
            next.graph_receipt = Some(receipt.clone());
            *attempted = Some(next.clone());
            let result = self
                .packed_journal_write_under(Some(record), &next, &[], None, Some(&native))
                .await?;
            *record = result.retain(
                budget
                    .admit(&[(SnapshotBudgetPool::Metadata, RECORD_LIMIT as u64)])
                    .map_err(journal_budget_error)?,
            )?;
            *attempted = None;
            receipt
        };
        let mut proof = Sha256::new();
        proof.update(b"BrewFS packed v3 complete native publication\0");
        proof.update(record.journal_id.as_bytes());
        proof.update(record.source.staging_id.as_bytes());
        proof.update(receipt.encode()?);
        proof.update(phase.canonical_receipt_bytes());
        proof.update(encode(phase.journal())?);
        proof.update(phase.native_delta_hash().delta_digest());
        proof.update(phase.native_delta_hash().root_hash());
        let counts = source.counts();
        for count in [
            counts.inodes,
            counts.names,
            counts.directories,
            counts.spans,
            counts.logical_bytes,
            counts.data_bytes,
            counts.payload_disk_bytes,
        ] {
            proof.update(count.to_le_bytes());
        }
        let native_counts: NativeDeltaHashCounts = phase.native_delta_hash().counts();
        for count in native_counts.tables {
            proof.update(count.to_le_bytes());
        }
        proof.update(native_counts.canonical_bytes.to_le_bytes());
        proof.update(
            record
                .native_rebind
                .as_ref()
                .ok_or(WorkspaceError::Fenced)?
                .encode()?,
        );
        proof.update(
            record
                .commit_target
                .as_ref()
                .ok_or(WorkspaceError::Fenced)?
                .encode()?,
        );
        let proof_digest = proof.finalize().into();
        let seal = CompletePackedGraphSeal {
            journal_id: record.journal_id,
            source: record.source.clone(),
            manifest: lower.manifest_reference().clone(),
            object_count: record.object_count,
            inventory_digest: record.inventory_digest,
            proof_digest,
            guard: record.guard.clone(),
            audited_revision: receipt.audited_revision,
            staging_incarnation: record.source.staging_id,
            graph_receipt_digest: receipt.digest()?,
        };
        Self::check_complete_seal(record, &seal)?;
        let next = if record.phase == PackedJournalPhase::Verified {
            if record.full_proof_digest != proof_digest {
                return Err(WorkspaceError::Fenced);
            }
            record.value.clone()
        } else {
            let mut next = record.next()?;
            next.phase = PackedJournalPhase::Verified;
            next.full_proof_digest = proof_digest;
            next
        };
        *attempted = Some(next.clone());
        let result = self
            .packed_journal_write_under(Some(record), &next, &[], None, Some(&native))
            .await?;
        *record = result.retain(
            budget
                .admit(&[(SnapshotBudgetPool::Metadata, RECORD_LIMIT as u64)])
                .map_err(journal_budget_error)?,
        )?;
        *attempted = None;
        Ok((seal, lower))
    }

    /// Recovery can reach this constructor only after a new actual graph audit,
    /// effective comparison, native table hash and strong Hashed reissuance.
    pub(crate) async fn assemble_native_publication<S>(
        self: &Arc<Self>,
        source: VerifiedHashedNativeView<K, S>,
        graph: NativeStagedGraphEvidence,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<ReadyNativePackedPublication<K, S>, NativePublicationPreparationFailure<K, S>>
    where
        S: BlockStore + Send + Sync + 'static,
    {
        let owner = match budget.admit(&[(SnapshotBudgetPool::Metadata, RECORD_LIMIT as u64)]) {
            Ok(owner) => owner,
            Err(error) => {
                return Err(NativePublicationPreparationFailure::HashedAdmission {
                    error: journal_budget_error(error),
                    source: Box::new(source),
                    graph: Box::new(graph),
                });
            }
        };
        let mut record = match graph.record.value.clone().retain(owner) {
            Ok(record) => record,
            Err(error) => {
                return Err(NativePublicationPreparationFailure::HashedAdmission {
                    error,
                    source: Box::new(source),
                    graph: Box::new(graph),
                });
            }
        };
        let mut attempted = None;
        let result = self
            .assemble_native_publication_inner(
                &source,
                &graph,
                &mut record,
                &mut attempted,
                &budget,
            )
            .await;
        match result {
            Ok((seal, lower)) => Ok(ReadyNativePackedPublication {
                store: self.clone(),
                source,
                graph,
                record,
                lower,
                seal,
                budget,
                registry_report: None,
                _candidate_owner: None,
                publication_started: false,
            }),
            Err(error) => Err(NativePublicationPreparationFailure::Hashed {
                error,
                source: Box::new(source),
                record: Box::new(record),
                graph: Box::new(graph),
                attempted_successor: attempted.map(Box::new),
            }),
        }
    }
}

mod native_resume;
