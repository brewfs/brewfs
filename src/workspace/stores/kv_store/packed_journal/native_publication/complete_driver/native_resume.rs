//! Real continuation of the original PPJ under freshly reissued native reads.
//! Durable target/registry facts never replace full source comparison or hash.

use super::*;
use crate::workspace::stores::kv_store::packed_native_freeze::PackedNativeRecoveryReadFence;

impl<K: WorkspaceKvBackend + 'static> KvWorkspaceStore<K> {
    /// Owns the real continuation through every PUT, readback and phase CAS.
    /// Receiver cancellation cannot cancel a dispatched operation or release
    /// its source/reader/registry holds before the owning driver returns.
    pub(crate) async fn resume_frozen_native_publication<O, S>(
        self: &Arc<Self>,
        artifact: FrozenNativeArtifact<K, S>,
        recovery: Arc<PackedNativeRecoveryReadFence<K>>,
        client: ObjectClient<O>,
        options: NativePublicationBuildOptions,
    ) -> Result<ReadyNativePackedPublication<K, S>, NativePublicationPreparationFailure<K, S>>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        let store = self.clone();
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            NativePublicationPreparationFailure::BeforeHashed(WorkspaceError::CorruptMetadata(
                "native publication recovery requires a Tokio runtime".into(),
            ))
        })?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        handle.spawn(async move {
            let result = store
                .resume_frozen_native_publication_owned(artifact, recovery, client, options)
                .await;
            let _ = sender.send(result);
        });
        receiver.await.map_err(|_| {
            NativePublicationPreparationFailure::BeforeHashed(WorkspaceError::CorruptMetadata(
                "native publication continuation driver stopped".into(),
            ))
        })?
    }

    async fn resume_frozen_native_publication_owned<O, S>(
        self: &Arc<Self>,
        mut artifact: FrozenNativeArtifact<K, S>,
        recovery: Arc<PackedNativeRecoveryReadFence<K>>,
        client: ObjectClient<O>,
        options: NativePublicationBuildOptions,
    ) -> Result<ReadyNativePackedPublication<K, S>, NativePublicationPreparationFailure<K, S>>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        let budget = artifact.mount_budget();
        let early = NativePublicationPreparationFailure::BeforeHashed;
        self.configure_packed_reader_pin_budget(budget.clone())
            .map_err(early)?;
        let observer = budget
            .read_observer(client.read_observer())
            .map_err(|error| early(journal_budget_error(error)))?;
        let client = client.with_read_observer(
            observer,
            crate::cadapter::read_observer::Engine::Packed,
            crate::cadapter::read_observer::Phase::Startup,
            crate::cadapter::read_observer::Origin::Demand,
        );
        recovery.validate().await.map_err(early)?;
        artifact
            .validate()
            .await
            .map_err(|error| early(journal_budget_error(error)))?;
        let basis = recovery
            .basis()
            .ok_or_else(|| early(WorkspaceError::Fenced))?;
        if !Arc::ptr_eq(recovery.store(), self)
            || !Arc::ptr_eq(artifact.native_quiesce(), recovery.native_quiesce())
            || !Arc::ptr_eq(&recovery.native_quiesce().mount_budget(), &budget)
            || artifact
                .source_digest()
                .map_err(|error| early(journal_budget_error(error)))?
                != basis.record().source.effective_view_digest
            || options.chunk_size == 0
            || options.cancel.is_cancelled()
        {
            return Err(early(WorkspaceError::Fenced));
        }
        let record_owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, RECORD_LIMIT as u64)])
            .map_err(|error| early(journal_budget_error(error)))?;
        let mut record = basis.record().clone().retain(record_owner).map_err(early)?;
        let rebuilt = record.phase == PackedJournalPhase::Building;
        if rebuilt {
            if record.commit_target.is_some() {
                return Err(early(WorkspaceError::Fenced));
            }
            // The original staging_id fixes deterministic physical keys. Every
            // existing object authenticates actual PRO3/PRR3/PRM3+remote bytes;
            // only an actual never-dispatched reserve may dispatch once.
            let (fresh_artifact, manifest, fresh_record) = self
                .build_registered_native_candidate(
                    record,
                    artifact,
                    client.clone(),
                    options.temporary.clone(),
                    options.producer,
                )
                .await
                .map_err(early)?;
            artifact = fresh_artifact;
            record = self
                .freeze_native_candidate(&fresh_record, &artifact, &manifest)
                .await
                .map_err(early)?;
        }
        if matches!(
            record.phase,
            PackedJournalPhase::Uploading | PackedJournalPhase::Readback
        ) {
            record = self
                .native_readback(record, &artifact, &client, &options.cancel)
                .await
                .map_err(early)?;
        }
        if !matches!(
            record.phase,
            PackedJournalPhase::AwaitingFullProof | PackedJournalPhase::Verified
        ) {
            return Err(early(WorkspaceError::Fenced));
        }
        // This complete actual graph audit independently checks every durable
        // inventory occurrence and reachable typed object. Extra old objects,
        // unresolved pending holds, mismatched registry rows or source fence
        // changes cannot be hidden by deterministic replay.
        let graph = self
            .audit_native_effective_graph(
                &record,
                &NativeJournalAuthority::from_captured(artifact.native_quiesce()),
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
        let manifest = record
            .commit_target
            .as_ref()
            .ok_or_else(|| early(WorkspaceError::Fenced))?
            .binding
            .manifest
            .clone();
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
        let source = if rebuilt {
            // Its actual producer returned and installed produced_manifest;
            // freezing that same manifest was done under the original PPJ.
            artifact.compare_candidate(candidate.clone()).await
        } else {
            artifact
                .compare_recovery_candidate(recovery, candidate.clone())
                .await
        }
        .map_err(|error| early(journal_budget_error(error)))?;
        let native_hash = Arc::new(
            FrozenNativeDeltaHash::capture(
                source.native_quiesce().clone(),
                source.native_reader_session(),
                options.native_hash_limits,
            )
            .await
            .map_err(early)?,
        );
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
        let mut ready = self
            .assemble_native_publication(source, graph, budget)
            .await?;
        ready._candidate_owner = Some(candidate);
        Ok(ready)
    }
}
