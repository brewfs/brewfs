//! The actual production before-PUT route shared by producer, index builder and
//! large-file uploads through ObjectClient. Observed read calls stay forwarded.

use super::*;
use crate::cadapter::client::ObjectByteStream;
use crate::cadapter::read_observer::{ReadContext, ReadObserver};

struct StagedBackend<O: ObjectBackend + Clone, K: WorkspaceKvBackend> {
    inner: O,
    read_client: ObjectClient<O>,
    store: Arc<KvWorkspaceStore<K>>,
    state: Arc<Mutex<OwnedPackedJournal<PackedJournalRecord>>>,
    budget: Arc<V3MountBudget>,
    native: Option<Arc<crate::workspace_overlay::stores::kv_store::packed_native_freeze::PackedNativeQuiesceFence<K>>>,
}
impl<O: ObjectBackend + Clone, K: WorkspaceKvBackend> Clone for StagedBackend<O, K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            read_client: self.read_client.clone(),
            store: self.store.clone(),
            state: self.state.clone(),
            budget: self.budget.clone(),
            native: self.native.clone(),
        }
    }
}

pub(super) fn typed_reference(key: &str, data: &[u8]) -> Result<V3ObjectRef, WorkspaceError> {
    use crate::workspace_overlay::packed_v3::wire005::V3_MAX_BODY_BYTES;
    let kind = match data.get(..8) {
        Some(b"BRFPM005") => V3ObjectKind::Manifest,
        Some(b"BRFGC005") => V3ObjectKind::GroupContainer,
        Some(b"BRFCA005") => V3ObjectKind::ColdAttributes,
        Some(b"BRFFD005") => V3ObjectKind::FrameDirectory,
        Some(b"BRFLD005") => V3ObjectKind::LargeData,
        Some(b"BRFGI005") => V3ObjectKind::GroupIndex,
        Some(b"BRFII005") => V3ObjectKind::InodeIndex,
        Some(b"BRFRI005") => V3ObjectKind::ReverseIndex,
        Some(b"BRFCI005") => V3ObjectKind::ContainerIndex,
        Some(b"BRFFI005") => V3ObjectKind::FrameIndex,
        Some(b"BRFAI005") => V3ObjectKind::ColdIndex,
        Some(b"BRFLI005") => V3ObjectKind::LargeIndex,
        Some(b"BRFSI005") => V3ObjectKind::SourceStatsIndex,
        _ => return Err(journal_error("staged PUT is not a typed packed-v3 object")),
    };
    let reference = V3ObjectRef::from_bytes(key.to_owned(), kind, data).map_err(journal_error)?;
    reference
        .verify(data, V3_MAX_BODY_BYTES)
        .map_err(journal_error)?;
    Ok(reference)
}

#[async_trait]
impl<O: ObjectBackend + Clone + 'static, K: WorkspaceKvBackend + 'static> ObjectBackend
    for StagedBackend<O, K>
{
    async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("staged packed objects require guarded create-only PUT")
    }
    async fn put_object_create_only(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        let _typed_owner = self
            .budget
            .admit(&[(V3BudgetPool::Metadata, 32 << 10)])
            .map_err(journal_budget_error)?;
        let reference = typed_reference(key, data)?;
        let mut state = self.state.lock().await;
        let native = self
            .native
            .as_deref()
            .map(super::super::native_publication::NativeJournalAuthority::from_captured);
        if !key.starts_with(&format!("{}/", state.source.staging_prefix)) {
            anyhow::bail!("staged object escaped its journal prefix");
        }
        let continuation = if let Some(source) = self.native.as_deref() {
            Some(
                self.store
                    .resume_captured_native_upload(
                        &state,
                        &reference,
                        source,
                        &self.read_client,
                        &self.budget,
                    )
                    .await?,
            )
        } else {
            None
        };
        let guard = match continuation {
            Some(super::native_upload_resume::NativeUploadContinuation::Completed(next)) => {
                *state = next;
                return Ok(());
            }
            Some(super::native_upload_resume::NativeUploadContinuation::Reserved(next, guard)) => {
                *state = next;
                guard
            }
            remaining => {
                if remaining.is_none()
                    && self
                        .store
                        .existing_completed_packed_upload_under(
                            &state,
                            &reference,
                            &self.budget,
                            native.as_ref(),
                        )
                        .await?
                {
                    return Ok(());
                }
                let (reserved, guard) = self
                    .store
                    .reserve_packed_upload_under(&state, reference, &self.budget, native.as_ref())
                    .await?;
                *state = reserved;
                guard
            }
        };
        let dispatched = self
            .store
            .dispatch_packed_upload_under(&state, &guard, &self.budget, native.as_ref())
            .await?;
        *state = dispatched;
        // On error/drop, neither root nor object pending hold is cleared. The
        // source build then fails and recovery sees a durable quarantine.
        self.inner.put_object_create_only(key, data).await?;
        let completed = self
            .store
            .finish_packed_upload_under(&state, guard, &self.budget, native.as_ref())
            .await?;
        *state = completed;
        Ok(())
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }
    async fn get_object_stream(&self, key: &str) -> anyhow::Result<Option<ObjectByteStream>> {
        self.inner.get_object_stream(key).await
    }
    async fn get_object_stream_observed(
        &self,
        key: &str,
        expected: Option<u64>,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<Option<ObjectByteStream>> {
        self.inner
            .get_object_stream_observed(key, expected, context, observer)
            .await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.inner.get_object_range(key, offset, buf).await
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        self.inner
            .get_object_range_stream(key, offset, length)
            .await
    }
    async fn get_object_range_stream_observed(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<ObjectByteStream> {
        self.inner
            .get_object_range_stream_observed(key, offset, length, context, observer)
            .await
    }
    async fn get_object_size(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size(key).await
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(key).await
    }
    async fn get_object_size_bounded_observed(
        &self,
        key: &str,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<Option<u64>> {
        self.inner
            .get_object_size_bounded_observed(key, context, observer)
            .await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
        anyhow::bail!("staged object DELETE requires an exact registry retirement guard")
    }
}

impl<K: WorkspaceKvBackend + 'static> KvWorkspaceStore<K> {
    /// A reachable production importer path. Every actual source/metadata/data
    /// PUT traverses reserve+dispatch and the actual backend before completion.
    /// It returns a private final-source certificate and durable Building
    /// inventory, leaving final native publication to its typed lifecycle.
    #[cfg(target_os = "linux")]
    pub(crate) async fn build_registered_source_snapshot<O: ObjectBackend + Clone + 'static>(
        self: &Arc<Self>,
        expected: OwnedPackedJournal<PackedJournalRecord>,
        inventory: crate::workspace_overlay::packed_v3::wire005::V3SourceNamespaceInventory,
        client: ObjectClient<O>,
        options: crate::workspace_overlay::packed_v3::wire005::V3ProducerOptions,
        budget: Arc<V3MountBudget>,
    ) -> Result<
        (
            crate::workspace_overlay::packed_v3::wire005::V3SourceNamespaceSnapshot,
            OwnedPackedJournal<PackedJournalRecord>,
        ),
        WorkspaceError,
    > {
        self.configure_packed_reader_pin_budget(budget.clone())?;
        if expected.phase != PackedJournalPhase::Building || expected.object_count != 0 {
            return Err(WorkspaceError::Busy);
        }
        let prefix = expected.source.staging_prefix.clone();
        let _state_owner = budget
            .admit(&[(V3BudgetPool::Metadata, 64 << 10)])
            .map_err(journal_budget_error)?;
        let state = Arc::new(Mutex::new(expected));
        let read_client = client.clone();
        let wrapped = client.map_backend(|inner| StagedBackend {
            inner,
            read_client,
            store: self.clone(),
            state: state.clone(),
            budget,
            native: None,
        });
        let source = inventory
            .build_snapshot(wrapped, prefix, options)
            .await
            .map_err(journal_budget_error)?;
        let state = Arc::try_unwrap(state)
            .map_err(|_| journal_error("source producer retained an unfinished upload handle"))?
            .into_inner();
        Ok((source, state))
    }

    /// Only a captured native artifact can select the Sealing producer route.
    /// The private backend sends every actual create-only request through the
    /// same global object reserve, single-use dispatch and completion CAS.
    #[cfg(target_os = "linux")]
    pub(in crate::workspace_overlay::stores::kv_store::packed_journal) async fn build_registered_native_candidate<
        O,
        S,
    >(
        self: &Arc<Self>,
        expected: OwnedPackedJournal<PackedJournalRecord>,
        artifact: crate::workspace_overlay::packed_v3::wire005::FrozenNativeArtifact<K, S>,
        client: ObjectClient<O>,
        temporary: std::path::PathBuf,
        options: crate::workspace_overlay::packed_v3::wire005::V3ProducerOptions,
    ) -> Result<
        (
            crate::workspace_overlay::packed_v3::wire005::FrozenNativeArtifact<K, S>,
            V3ObjectRef,
            OwnedPackedJournal<PackedJournalRecord>,
        ),
        WorkspaceError,
    >
    where
        O: ObjectBackend + Clone + 'static,
        S: crate::chunk::BlockStore + Send + Sync + 'static,
    {
        let budget = artifact.mount_budget();
        self.configure_packed_reader_pin_budget(budget.clone())?;
        artifact.validate().await.map_err(journal_budget_error)?;
        let native = artifact.native_quiesce().clone();
        if !native.belongs_to_store(self.as_ref())
            || expected.phase != PackedJournalPhase::Building
            || expected.source.effective_view_digest
                != artifact.source_digest().map_err(journal_budget_error)?
            || expected.source.frozen_view_token != native.canonical_receipt_digest()
            || !expected.source.snapshot_backed
            || expected
                .native_rebind
                .as_ref()
                .and_then(|basis| basis.publication.as_ref())
                .is_none()
        {
            return Err(WorkspaceError::Fenced);
        }
        let prefix = expected.source.staging_prefix.clone();
        let _state_owner = budget
            .admit(&[(V3BudgetPool::Metadata, 64 << 10)])
            .map_err(journal_budget_error)?;
        let incarnation = expected.source.staging_id;
        let state = Arc::new(Mutex::new(expected));
        let read_client = client.clone();
        let wrapped = client.map_backend(|inner| StagedBackend {
            inner,
            read_client,
            store: self.clone(),
            state: state.clone(),
            budget,
            native: Some(native),
        });
        let (artifact, manifest) = artifact
            .produce_candidate(wrapped, temporary, prefix, incarnation, options)
            .await
            .map_err(journal_budget_error)?;
        let state = Arc::try_unwrap(state)
            .map_err(|_| journal_error("native producer retained an unfinished upload handle"))?
            .into_inner();
        Ok((artifact, manifest, state))
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::tests::{JournalMemoryBackend, begin, setup};
    use super::*;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::workspace_overlay::packed_v3::wire005::{
        AuthenticatedV3Snapshot, V3IndexReader, V3ProducerOptions, V3SnapshotProducer,
        V3SourceConsistency, V3SourceFileLimits, V3SourceHardlinkPolicy,
        V3SourceNamespaceInventory, V3SourceNamespaceOptions,
    };
    use crate::workspace_overlay::packed_v3::{AccessProfile, PackedCodec, SizeClassTable};
    use crate::workspace_overlay::publish::binding::VerifiedPackedLower;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Clone)]
    struct ObservedLocalFs {
        inner: LocalFsBackend,
        metadata: Arc<JournalMemoryBackend>,
        puts: Arc<AtomicUsize>,
        deletes: Arc<AtomicUsize>,
        fail: Arc<AtomicBool>,
        delete_fail: Arc<AtomicBool>,
        delete_block: Option<Arc<tokio::sync::Notify>>,
        delete_entered: Option<Arc<tokio::sync::Notify>>,
        block: Option<Arc<tokio::sync::Notify>>,
        entered: Option<Arc<tokio::sync::Notify>>,
    }
    #[async_trait]
    impl ObjectBackend for ObservedLocalFs {
        async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
            anyhow::bail!("fixture requires create-only")
        }
        async fn put_object_create_only(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            let reference = typed_reference(key, data)?;
            let rows = self.metadata.rows.lock().await;
            let object = ObjectRow::decode(
                rows.get(&registry_object_key(&reference))
                    .expect("PUT preceded global registration"),
            )?;
            assert_eq!(object.reference, reference);
            assert_eq!(object.state, ObjectState::Live);
            assert!(object.pending_puts > 0);
            let member = rows
                .iter()
                .filter(|(k, _)| k.starts_with(REGISTRY_MEMBER_PREFIX.as_bytes()))
                .map(|(_, value)| MemberRow::decode(value).unwrap())
                .find(|member| {
                    member.reference == reference && member.pending_put && member.dispatched
                })
                .expect("PUT preceded durable single-use dispatch");
            let root = RootRow::decode(rows.get(&registry_root_key(member.incarnation)).unwrap())?;
            let journal =
                PackedJournalRecord::decode(rows.get(&journal_key(member.journal_id)).unwrap())?;
            assert_eq!(root.state, RootState::Staging);
            assert!(root.pending_puts > 0);
            root.check_staging(&journal)?;
            let occurrence = PackedJournalObject::decode(
                rows.get(&object_key(member.journal_id, member.ordinal))
                    .unwrap(),
            )?;
            assert_eq!(occurrence.reference, reference);
            assert!(!occurrence.uploaded);
            assert_eq!(
                rows.get(&registry_reverse_key(member.incarnation, member.ordinal)),
                Some(&reference.encode_value().unwrap())
            );
            assert_eq!(
                rows.get(&active_key(member.journal_id)),
                rows.get(&journal_key(member.journal_id))
            );
            drop(rows);
            self.puts.fetch_add(1, Ordering::SeqCst);
            if let Some(block) = &self.block {
                self.entered.as_ref().unwrap().notify_one();
                block.notified().await;
            }
            if self.fail.load(Ordering::SeqCst) {
                anyhow::bail!("injected unknown PUT result")
            }
            self.inner.put_object_create_only(key, data).await
        }
        async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            self.inner.get_object(key).await
        }
        async fn get_object_stream(&self, key: &str) -> anyhow::Result<Option<ObjectByteStream>> {
            self.inner.get_object_stream(key).await
        }
        async fn get_object_range(
            &self,
            key: &str,
            offset: u64,
            data: &mut [u8],
        ) -> anyhow::Result<usize> {
            self.inner.get_object_range(key, offset, data).await
        }
        async fn get_object_range_stream(
            &self,
            key: &str,
            offset: u64,
            length: u64,
        ) -> anyhow::Result<ObjectByteStream> {
            self.inner
                .get_object_range_stream(key, offset, length)
                .await
        }
        async fn get_object_size(&self, key: &str) -> anyhow::Result<Option<u64>> {
            self.inner.get_object_size(key).await
        }
        async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
            self.inner.get_object_size_bounded(key).await
        }
        async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
            self.inner.get_etag(key).await
        }
        async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
            let object_key = format!(
                "{REGISTRY_OBJECT_PREFIX}{}",
                hex::encode(Sha256::digest(key.as_bytes()))
            )
            .into_bytes();
            let rows = self.metadata.rows.lock().await;
            let object = ObjectRow::decode(
                rows.get(&object_key)
                    .expect("DELETE preceded registry reservation"),
            )?;
            assert_eq!(object.reference.key, key);
            assert_eq!(object.state, ObjectState::DeletePending);
            assert!(object.delete_dispatched);
            assert_eq!(object.memberships, 0);
            assert_eq!(object.pending_puts, 0);
            drop(rows);
            self.deletes.fetch_add(1, Ordering::SeqCst);
            if let Some(block) = &self.delete_block {
                self.delete_entered.as_ref().unwrap().notify_one();
                block.notified().await;
            }
            self.inner.delete_object(key).await?;
            if self.delete_fail.load(Ordering::SeqCst) {
                anyhow::bail!("injected committed DELETE response loss");
            }
            Ok(())
        }
    }
    fn options() -> V3ProducerOptions {
        V3ProducerOptions {
            snapshot_id: [88; 32],
            root_dir_key: [89; 32],
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
        }
    }
    async fn capture(
        source: &std::path::Path,
        scratch: &std::path::Path,
    ) -> V3SourceNamespaceInventory {
        std::fs::write(source.join("data"), [41; 8192]).unwrap();
        std::fs::hard_link(source.join("data"), source.join("alias")).unwrap();
        std::os::unix::fs::symlink("data", source.join("link")).unwrap();
        V3SourceNamespaceInventory::capture(
            source,
            scratch,
            V3SourceNamespaceOptions {
                root_inode: 1,
                consistency: V3SourceConsistency::BestEffortDetected,
                hardlink_policy: V3SourceHardlinkPolicy::VisibleLinks,
                file_limits: V3SourceFileLimits::default(),
            },
        )
        .await
        .unwrap()
    }
    fn observed(
        objects: &std::path::Path,
        metadata: Arc<JournalMemoryBackend>,
        fail: bool,
    ) -> ObservedLocalFs {
        ObservedLocalFs {
            inner: LocalFsBackend::new(objects),
            metadata,
            puts: Arc::new(AtomicUsize::new(0)),
            deletes: Arc::new(AtomicUsize::new(0)),
            fail: Arc::new(AtomicBool::new(fail)),
            delete_fail: Arc::new(AtomicBool::new(false)),
            delete_block: None,
            delete_entered: None,
            block: None,
            entered: None,
        }
    }

    #[tokio::test]
    async fn actual_registered_importer_pins_every_put_then_audits_complete_graph() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        let metadata = Arc::new(JournalMemoryBackend::default());
        let budget = V3MountBudget::defaults();
        let store = Arc::new(
            KvWorkspaceStore::from_arc(metadata.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        let (_old_objects, mut publish) = setup(&store).await;
        let journal = begin(&store, &publish, &budget).await;
        let backend = observed(objects.path(), metadata.clone(), false);
        let client = ObjectClient::new(backend.clone());
        let (snapshot, mut journal) = store
            .build_registered_source_snapshot(
                journal,
                capture(source.path(), scratch.path()).await,
                client.clone(),
                options(),
                budget.clone(),
            )
            .await
            .unwrap();
        assert!(journal.object_count > 6 && journal.object_count <= 64);
        assert_eq!(
            backend.puts.load(Ordering::SeqCst) as u64,
            journal.object_count
        );
        let root = RootRow::decode(
            metadata
                .rows
                .lock()
                .await
                .get(&registry_root_key(journal.source.staging_id))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(root.members, journal.object_count);
        assert_eq!(root.pending_puts, 0);
        let authenticated = AuthenticatedV3Snapshot::open(&client, &snapshot.reference)
            .await
            .unwrap();
        publish.lower = VerifiedPackedLower::from_authenticated_snapshot(
            &authenticated,
            &V3IndexReader::new(client.clone(), 0),
        )
        .await
        .unwrap();
        let final_source = snapshot.into_final_source_proof().unwrap();
        journal = store
            .freeze_packed_candidate(&journal, publish.record().unwrap(), &budget)
            .await
            .unwrap();
        journal = store
            .advance_packed_journal(&journal, &budget)
            .await
            .unwrap();
        for ordinal in 0..journal.object_count {
            let object = store
                .reopen_packed_object(&journal, ordinal, &budget)
                .await
                .unwrap();
            assert!(object.reference.object_len <= 4 << 20);
            let _readback_owner = budget.admit(&[(V3BudgetPool::Metadata, 8 << 20)]).unwrap();
            let bytes = backend
                .inner
                .get_object(&object.reference.key)
                .await
                .unwrap()
                .unwrap();
            object.reference.verify(&bytes, 4 << 20).unwrap();
            journal = store
                .record_packed_object_progress(&journal, ordinal, true, &budget)
                .await
                .unwrap();
        }
        journal = store
            .advance_packed_journal(&journal, &budget)
            .await
            .unwrap();
        let proof = store
            .audit_imported_packed_graph(
                &journal,
                &final_source,
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
        let receipt = store
            .record_imported_packed_graph(&journal, &proof, &budget)
            .await
            .unwrap();
        assert_eq!(receipt.phase, PackedJournalPhase::AwaitingFullProof);
        assert_eq!(
            receipt.graph_receipt.as_ref().unwrap().object_count,
            root.members
        );
    }

    #[tokio::test]
    async fn actual_importer_unknown_put_keeps_global_and_aborted_root_holds() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        let metadata = Arc::new(JournalMemoryBackend::default());
        let budget = V3MountBudget::defaults();
        let store = Arc::new(
            KvWorkspaceStore::from_arc(metadata.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        let (_old_objects, publish) = setup(&store).await;
        let journal = begin(&store, &publish, &budget).await;
        let id = journal.journal_id;
        let view = journal.source.clone();
        let backend = observed(objects.path(), metadata.clone(), true);
        assert!(
            store
                .build_registered_source_snapshot(
                    journal,
                    capture(source.path(), scratch.path()).await,
                    ObjectClient::new(backend.clone()),
                    options(),
                    budget.clone()
                )
                .await
                .is_err()
        );
        assert_eq!(backend.puts.load(Ordering::SeqCst), 1);
        let journal = store
            .reopen_packed_journal(id, &view, &budget)
            .await
            .unwrap();
        assert_eq!(journal.object_count, 1);
        let aborted = store
            .recover_packed_journal(
                &journal,
                journal.guard.clone(),
                Some("unknown PUT".into()),
                &budget,
            )
            .await
            .unwrap();
        let rows = metadata.rows.lock().await;
        let root = RootRow::decode(rows.get(&registry_root_key(view.staging_id)).unwrap()).unwrap();
        assert_eq!(root.state, RootState::AbortedRetained);
        assert_eq!(root.pending_puts, 1);
        let occurrence =
            PackedJournalObject::decode(rows.get(&object_key(id, 0)).unwrap()).unwrap();
        assert!(!occurrence.uploaded);
        let object = ObjectRow::decode(
            rows.get(&registry_object_key(&occurrence.reference))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(object.pending_puts, 1);
        assert_eq!(object.memberships, 1);
        assert_eq!(aborted.phase, PackedJournalPhase::Aborted);
    }

    #[tokio::test]
    async fn lost_dispatch_reply_never_authorizes_a_second_put() {
        let metadata = Arc::new(JournalMemoryBackend::default());
        let budget = V3MountBudget::defaults();
        let store = Arc::new(
            KvWorkspaceStore::from_arc(metadata.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        let (_old_objects, publish) = setup(&store).await;
        let journal = begin(&store, &publish, &budget).await;
        let (reserved, guard) = store
            .reserve_packed_upload(
                &journal,
                publish.lower.manifest_reference().clone(),
                &budget,
            )
            .await
            .unwrap();
        metadata.lose_reply.store(true, Ordering::SeqCst);
        assert!(
            store
                .dispatch_packed_upload(&reserved, &guard, &budget)
                .await
                .is_err()
        );
        let recovered = store
            .reopen_packed_journal(reserved.journal_id, &reserved.source, &budget)
            .await
            .unwrap();
        assert!(matches!(
            store
                .dispatch_packed_upload(&recovered, &guard, &budget)
                .await,
            Err(WorkspaceError::Fenced)
        ));
        drop(guard);
        let rows = metadata.rows.lock().await;
        let root = RootRow::decode(
            rows.get(&registry_root_key(recovered.source.staging_id))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(root.pending_puts, 1);
    }

    #[tokio::test]
    async fn cancelled_actual_importer_put_keeps_the_durable_pending_hold() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        let metadata = Arc::new(JournalMemoryBackend::default());
        let budget = V3MountBudget::defaults();
        let store = Arc::new(
            KvWorkspaceStore::from_arc(metadata.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        let (_old_objects, publish) = setup(&store).await;
        let journal = begin(&store, &publish, &budget).await;
        let id = journal.journal_id;
        let view = journal.source.clone();
        let mut backend = observed(objects.path(), metadata.clone(), false);
        let entered = Arc::new(tokio::sync::Notify::new());
        backend.entered = Some(entered.clone());
        backend.block = Some(Arc::new(tokio::sync::Notify::new()));
        let inventory = capture(source.path(), scratch.path()).await;
        let mut build = Box::pin(store.build_registered_source_snapshot(
            journal,
            inventory,
            ObjectClient::new(backend.clone()),
            options(),
            budget.clone(),
        ));
        tokio::select! {
            result = &mut build => panic!("blocked actual PUT completed: {}", result.is_ok()),
            _ = entered.notified() => {},
        }
        drop(build);
        assert_eq!(backend.puts.load(Ordering::SeqCst), 1);
        let journal = store
            .reopen_packed_journal(id, &view, &budget)
            .await
            .unwrap();
        let rows = metadata.rows.lock().await;
        let occurrence =
            PackedJournalObject::decode(rows.get(&object_key(id, 0)).unwrap()).unwrap();
        assert!(!occurrence.uploaded);
        let member = MemberRow::decode(
            rows.get(&registry_member_key(&occurrence.reference, view.staging_id))
                .unwrap(),
        )
        .unwrap();
        assert!(member.pending_put && member.dispatched && member.retained);
        let object = ObjectRow::decode(
            rows.get(&registry_object_key(&occurrence.reference))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(object.pending_puts, 1);
        let root = RootRow::decode(rows.get(&registry_root_key(view.staging_id)).unwrap()).unwrap();
        assert_eq!(root.pending_puts, 1);
        assert_eq!(journal.object_count, 1);
    }

    struct CollectorFixture {
        _objects: tempfile::TempDir,
        _source: tempfile::TempDir,
        scratch: tempfile::TempDir,
        store: Arc<KvWorkspaceStore<JournalMemoryBackend>>,
        metadata: Arc<JournalMemoryBackend>,
        client: ObjectClient<ObservedLocalFs>,
        backend: ObservedLocalFs,
        journal: OwnedPackedJournal<PackedJournalRecord>,
        nonterminal: PackedJournalRecord,
        budget: Arc<V3MountBudget>,
        old_manifest: V3ObjectRef,
    }
    async fn collector_fixture() -> CollectorFixture {
        let metadata = Arc::new(JournalMemoryBackend::default());
        let budget = V3MountBudget::defaults();
        let store = Arc::new(
            KvWorkspaceStore::from_arc(metadata.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        // Keep the actual pre-registry graph alive. The general journal setup
        // intentionally drops its initial TempDir and cannot test migration.
        let (objects, old_client, _snapshot, lower, _) =
            crate::workspace_overlay::stores::binding_tests::packed().await;
        let install =
            crate::workspace_overlay::stores::binding_tests::request(store.as_ref(), lower).await;
        let binding = store
            .install_packed_lower_binding(install.clone())
            .await
            .unwrap();
        let old_manifest = binding.binding.manifest.clone();
        let guard = HeadGuard {
            expected_head_epoch: binding.head_epoch,
            ..install.guard
        };
        let layers: [LayerRecord; 2] = store
            .load_layer_chain(guard.expected_head_layer_id)
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let publish = PublishPackedLowerBinding {
            guard,
            expected_layers: layers,
            expected_base: binding.base_revision.clone(),
            expected_binding: binding,
            lower: install.lower,
        };
        let journal = begin(&store, &publish, &budget).await;
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let backend = observed(&objects.path().join("objects"), metadata.clone(), false);
        let client = ObjectClient::new(backend.clone());
        let (snapshot, journal) = store
            .build_registered_source_snapshot(
                journal,
                capture(source.path(), scratch.path()).await,
                client.clone(),
                options(),
                budget.clone(),
            )
            .await
            .unwrap();
        drop(snapshot);
        let nonterminal = (*journal).clone();
        let journal = store
            .abort_packed_journal(&journal, "collector cancelled candidate".into(), &budget)
            .await
            .unwrap();
        // Publish must advance the manifest. Produce a second actual graph in
        // the retained old graph's object directory, then alternate the two.
        let history_producer = V3SnapshotProducer::new(
            old_client.clone(),
            scratch.path(),
            "collector-history-extra".into(),
            V3ProducerOptions {
                snapshot_id: [91; 32],
                root_dir_key: [92; 32],
                ..options()
            },
        )
        .await
        .unwrap();
        let history_manifest = history_producer.finish().await.unwrap();
        let history_snapshot = AuthenticatedV3Snapshot::open(&old_client, &history_manifest)
            .await
            .unwrap();
        let history_reader = V3IndexReader::new(old_client, 0);
        let extra_lower =
            VerifiedPackedLower::from_authenticated_snapshot(&history_snapshot, &history_reader)
                .await
                .unwrap();
        let mut current = publish.expected_binding.clone();
        for version in 0..3 {
            let guard = HeadGuard {
                expected_head_epoch: current.head_epoch,
                ..publish.guard.clone()
            };
            let layers: [LayerRecord; 2] = store
                .load_layer_chain(guard.expected_head_layer_id)
                .await
                .unwrap()
                .try_into()
                .unwrap();
            current = store
                .publish_packed_lower_binding(PublishPackedLowerBinding {
                    guard,
                    expected_layers: layers,
                    expected_base: current.base_revision.clone(),
                    expected_binding: current,
                    lower: if version % 2 == 0 {
                        extra_lower.clone()
                    } else {
                        publish.lower.clone()
                    },
                })
                .await
                .unwrap();
        }
        // Force legal short pages with further rows remaining in history.
        metadata.page_size.store(1, Ordering::SeqCst);
        CollectorFixture {
            _objects: objects,
            _source: source,
            scratch,
            store,
            metadata,
            client,
            backend,
            journal,
            nonterminal,
            budget,
            old_manifest,
        }
    }
    async fn migrate(
        fixture: &CollectorFixture,
    ) -> super::super::collector::PackedRegistryMigrationReport {
        fixture
            .store
            .migrate_packed_object_registry(
                &fixture.client,
                &fixture.budget,
                super::super::collector::PackedRegistryMigrationOptions {
                    scratch: fixture.scratch.path(),
                    graph_limits: V3IndexAuditLimits::default(),
                    max_catalog_rows: 64,
                    cancel: CancellationToken::new(),
                },
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn actual_packed_gc_aborted_route_requires_registry_then_collects_and_resumes() {
        let fixture = collector_fixture().await;
        assert!(
            fixture
                .store
                .collect_aborted_packed_journal(
                    fixture.journal.journal_id,
                    &fixture.client,
                    &fixture.budget,
                    CancellationToken::new(),
                )
                .await
                .is_err()
        );
        assert_eq!(fixture.backend.deletes.load(Ordering::SeqCst), 0);
        assert!(migrate(&fixture).await.audited_objects > 0);
        let deleted = fixture
            .store
            .collect_aborted_packed_journal(
                fixture.journal.journal_id,
                &fixture.client,
                &fixture.budget,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(deleted > 0 && deleted <= fixture.journal.object_count);
        assert_eq!(
            deleted,
            fixture.backend.deletes.load(Ordering::SeqCst) as u64
        );
        let root = RootRow::decode(
            &fixture
                .metadata
                .get(&registry_root_key(fixture.journal.source.staging_id))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!((root.state, root.members), (RootState::Retired, 0));
        let before = fixture.metadata.rows.lock().await.clone();
        assert_eq!(
            fixture
                .store
                .collect_aborted_packed_journal(
                    fixture.journal.journal_id,
                    &fixture.client,
                    &fixture.budget,
                    CancellationToken::new(),
                )
                .await
                .unwrap(),
            0
        );
        assert_eq!(*fixture.metadata.rows.lock().await, before);
        assert_eq!(
            deleted,
            fixture.backend.deletes.load(Ordering::SeqCst) as u64
        );
        // This real authenticated preexisting graph is still retained by its
        // current/history memberships after the aborted graph has disappeared.
        let retained = crate::workspace_overlay::packed_v3::wire005::audit_v3_index_contexts(
            &fixture.client,
            &fixture.old_manifest,
            fixture.scratch.path(),
            fixture.budget.clone(),
            V3IndexAuditLimits::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(retained.counts().objects > 0);
    }

    #[tokio::test]
    async fn actual_packed_gc_aborted_route_rejects_alias_nonterminal_and_nonterminal_owner() {
        for condition in 0..3 {
            let fixture = collector_fixture().await;
            migrate(&fixture).await;
            let mut requested = fixture.journal.journal_id;
            let mut rows = fixture.metadata.rows.lock().await;
            match condition {
                0 => {
                    requested = JournalId::new();
                    rows.insert(journal_key(requested), fixture.journal.encode().unwrap());
                }
                1 => {
                    // Actual producer output captured before the actual abort,
                    // with its complete receipt; the route cannot declare it terminal.
                    rows.insert(
                        journal_key(requested),
                        fixture.nonterminal.encode().unwrap(),
                    );
                }
                _ => {
                    rows.insert(active_key(requested), fixture.nonterminal.encode().unwrap());
                }
            }
            let before = rows.clone();
            drop(rows);
            assert!(matches!(
                fixture
                    .store
                    .collect_aborted_packed_journal(
                        requested,
                        &fixture.client,
                        &fixture.budget,
                        CancellationToken::new(),
                    )
                    .await,
                Err(WorkspaceError::Fenced)
            ));
            assert_eq!(*fixture.metadata.rows.lock().await, before);
            assert_eq!(fixture.backend.deletes.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn actual_packed_gc_aborted_route_budget_and_cancellation_do_not_dispatch_delete() {
        for condition in 0..4 {
            let fixture = collector_fixture().await;
            migrate(&fixture).await;
            let budget = if condition == 0 {
                V3MountBudget::defaults()
            } else {
                fixture.budget.clone()
            };
            let cancel = CancellationToken::new();
            if condition == 1 {
                budget.close();
            }
            if condition == 2 {
                cancel.cancel();
            }
            let blocked = if condition == 3 {
                let limits =
                    crate::workspace_overlay::packed_v3::wire005::V3BudgetLimits::default();
                let used = budget.state().used[V3BudgetPool::Metadata as usize];
                Some(
                    budget
                        .admit(&[(
                            V3BudgetPool::Metadata,
                            limits.bytes[V3BudgetPool::Metadata as usize] - used,
                        )])
                        .unwrap(),
                )
            } else {
                None
            };
            let before = fixture.metadata.rows.lock().await.clone();
            assert!(
                fixture
                    .store
                    .collect_aborted_packed_journal(
                        fixture.journal.journal_id,
                        &fixture.client,
                        &budget,
                        cancel,
                    )
                    .await
                    .is_err()
            );
            assert_eq!(*fixture.metadata.rows.lock().await, before);
            assert_eq!(fixture.backend.deletes.load(Ordering::SeqCst), 0);
            drop(blocked);
        }
    }

    #[tokio::test]
    async fn actual_registry_migration_retains_old_graph_and_collects_aborted_uploads() {
        let fixture = collector_fixture().await;
        assert!(
            fixture
                .store
                .collect_aborted_packed_graph(
                    &fixture.journal,
                    &fixture.client,
                    &fixture.budget,
                    CancellationToken::new()
                )
                .await
                .is_err()
        );
        assert_eq!(fixture.backend.deletes.load(Ordering::SeqCst), 0);
        let report = migrate(&fixture).await;
        assert_eq!(report.catalog_rows, 5);
        assert!(fixture.metadata.page_calls.load(Ordering::SeqCst) >= 7);
        assert!(report.audited_objects > 0);
        let deleted = fixture
            .store
            .collect_aborted_packed_graph(
                &fixture.journal,
                &fixture.client,
                &fixture.budget,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(deleted > 0 && deleted <= fixture.journal.object_count);
        assert_eq!(
            deleted,
            fixture.backend.deletes.load(Ordering::SeqCst) as u64
        );
        let rows = fixture.metadata.rows.lock().await;
        let root = RootRow::decode(
            rows.get(&registry_root_key(fixture.journal.source.staging_id))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(root.state, RootState::Retired);
        assert_eq!(root.members, 0);
        let old = ObjectRow::decode(
            rows.get(&registry_object_key(&fixture.old_manifest))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(old.state, ObjectState::Live);
        assert!(old.memberships >= 2);
        drop(rows);
        let graph = crate::workspace_overlay::packed_v3::wire005::audit_v3_index_contexts(
            &fixture.client,
            &fixture.old_manifest,
            fixture.scratch.path(),
            fixture.budget.clone(),
            V3IndexAuditLimits::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(graph.counts().objects > 0);
        drop(graph);
        assert_eq!(
            fixture
                .store
                .collect_aborted_packed_graph(
                    &fixture.journal,
                    &fixture.client,
                    &fixture.budget,
                    CancellationToken::new()
                )
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            deleted,
            fixture.backend.deletes.load(Ordering::SeqCst) as u64
        );
    }

    #[tokio::test]
    async fn actual_restart_collects_released_objects_through_the_global_index() {
        let fixture = collector_fixture().await;
        migrate(&fixture).await;
        let reference = fixture
            .store
            .release_one_aborted_member_for_test(&fixture.journal)
            .await
            .unwrap();
        assert_eq!(fixture.backend.deletes.load(Ordering::SeqCst), 0);
        let restarted = KvWorkspaceStore::from_arc(fixture.metadata.clone())
            .with_packed_reader_pin_budget(fixture.budget.clone());
        let deleted = restarted
            .collect_retiring_packed_objects(
                &fixture.client,
                &fixture.budget,
                super::super::collector::PackedObjectCollectionOptions {
                    max_objects: 256,
                    cancel: CancellationToken::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(fixture.backend.deletes.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture
                .backend
                .inner
                .get_object_size_bounded(&reference.key)
                .await
                .unwrap(),
            None
        );
        let rows = fixture.metadata.rows.lock().await;
        let object =
            ObjectRow::decode(rows.get(&registry_object_key(&reference)).unwrap()).unwrap();
        assert_eq!(object.state, ObjectState::Deleted);
        assert!(object.delete_dispatched);
        drop(rows);
        restarted
            .collect_aborted_packed_graph(
                &fixture.journal,
                &fixture.client,
                &fixture.budget,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let attempts = fixture.backend.deletes.load(Ordering::SeqCst);
        assert_eq!(
            restarted
                .collect_retiring_packed_objects(
                    &fixture.client,
                    &fixture.budget,
                    super::super::collector::PackedObjectCollectionOptions {
                        max_objects: 256,
                        cancel: CancellationToken::new(),
                    },
                )
                .await
                .unwrap(),
            0
        );
        assert_eq!(fixture.backend.deletes.load(Ordering::SeqCst), attempts);
    }

    #[tokio::test]
    async fn actual_lost_delete_dispatch_reply_never_sends_or_reissues_delete() {
        let fixture = collector_fixture().await;
        migrate(&fixture).await;
        let reference = fixture
            .store
            .release_one_aborted_member_for_test(&fixture.journal)
            .await
            .unwrap();
        // Reserve succeeds; the actual single-use dispatch commits, but its
        // response is lost before the caller receives any dispatch guard.
        fixture
            .metadata
            .lose_nth_write_reply
            .store(2, Ordering::SeqCst);
        assert!(matches!(
            fixture
                .store
                .delete_registered_packed_object(
                    &fixture.client,
                    reference.clone(),
                    &fixture.budget,
                    &CancellationToken::new(),
                )
                .await,
            Err(WorkspaceError::Backend(_))
        ));
        assert_eq!(fixture.backend.deletes.load(Ordering::SeqCst), 0);
        let rows = fixture.metadata.rows.lock().await;
        let object =
            ObjectRow::decode(rows.get(&registry_object_key(&reference)).unwrap()).unwrap();
        assert_eq!(object.state, ObjectState::DeletePending);
        assert!(object.delete_dispatched);
        let before = rows.clone();
        drop(rows);
        assert!(matches!(
            fixture
                .store
                .delete_registered_packed_object(
                    &fixture.client,
                    reference,
                    &fixture.budget,
                    &CancellationToken::new(),
                )
                .await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(
            fixture
                .store
                .collect_retiring_packed_objects(
                    &fixture.client,
                    &fixture.budget,
                    super::super::collector::PackedObjectCollectionOptions {
                        max_objects: 256,
                        cancel: CancellationToken::new(),
                    },
                )
                .await
                .unwrap(),
            0
        );
        assert_eq!(*fixture.metadata.rows.lock().await, before);
        assert_eq!(fixture.backend.deletes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn actual_unknown_delete_stays_quarantined_even_when_remote_key_is_absent() {
        let fixture = collector_fixture().await;
        migrate(&fixture).await;
        fixture.backend.delete_fail.store(true, Ordering::SeqCst);
        assert!(
            fixture
                .store
                .collect_aborted_packed_graph(
                    &fixture.journal,
                    &fixture.client,
                    &fixture.budget,
                    CancellationToken::new()
                )
                .await
                .is_err()
        );
        assert_eq!(fixture.backend.deletes.load(Ordering::SeqCst), 1);
        let rows = fixture.metadata.rows.lock().await;
        let object = rows
            .iter()
            .filter(|(key, _)| key.starts_with(REGISTRY_OBJECT_PREFIX.as_bytes()))
            .map(|(_, value)| ObjectRow::decode(value).unwrap())
            .find(|row| row.state == ObjectState::DeletePending)
            .unwrap();
        assert!(object.delete_dispatched);
        assert_eq!(object.memberships, 0);
        let before = rows.clone();
        drop(rows);
        assert_eq!(
            fixture
                .backend
                .inner
                .get_object_size_bounded(&object.reference.key)
                .await
                .unwrap(),
            None
        );
        assert!(matches!(
            fixture
                .store
                .delete_registered_packed_object(
                    &fixture.client,
                    object.reference,
                    &fixture.budget,
                    &CancellationToken::new()
                )
                .await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(*fixture.metadata.rows.lock().await, before);
        assert_eq!(fixture.backend.deletes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn actual_cancelled_delete_keeps_a_nonretryable_pending_hold() {
        let fixture = collector_fixture().await;
        migrate(&fixture).await;
        let mut backend = fixture.backend.clone();
        let entered = Arc::new(tokio::sync::Notify::new());
        backend.delete_entered = Some(entered.clone());
        backend.delete_block = Some(Arc::new(tokio::sync::Notify::new()));
        let client = ObjectClient::new(backend.clone());
        let store = fixture.store.clone();
        let budget = fixture.budget.clone();
        let expected = fixture.journal.clone();
        let cancel = CancellationToken::new();
        let run_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            store
                .collect_aborted_packed_graph(&expected, &client, &budget, run_cancel)
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        cancel.cancel();
        assert!(matches!(task.await.unwrap(), Err(WorkspaceError::Busy)));
        let rows = fixture.metadata.rows.lock().await;
        let object = rows
            .iter()
            .filter(|(key, _)| key.starts_with(REGISTRY_OBJECT_PREFIX.as_bytes()))
            .map(|(_, value)| ObjectRow::decode(value).unwrap())
            .find(|row| row.state == ObjectState::DeletePending)
            .unwrap();
        assert!(object.delete_dispatched);
        let before = rows.clone();
        drop(rows);
        assert!(matches!(
            fixture
                .store
                .delete_registered_packed_object(
                    &fixture.client,
                    object.reference,
                    &fixture.budget,
                    &CancellationToken::new()
                )
                .await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(*fixture.metadata.rows.lock().await, before);
        assert_eq!(backend.deletes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn producer_never_reactivates_retiring_or_unknown_delete_keys() {
        for state in [
            ObjectState::Retiring,
            ObjectState::DeletePending,
            ObjectState::Deleted,
        ] {
            let metadata = Arc::new(JournalMemoryBackend::default());
            let budget = V3MountBudget::defaults();
            let store = Arc::new(
                KvWorkspaceStore::from_arc(metadata.clone())
                    .with_packed_reader_pin_budget(budget.clone()),
            );
            let (_old_objects, publish) = setup(&store).await;
            let journal = begin(&store, &publish, &budget).await;
            let reference = publish.lower.manifest_reference().clone();
            let object = ObjectRow {
                reference: reference.clone(),
                revision: 7,
                state,
                memberships: 0,
                pending_puts: 0,
                delete_id: if state == ObjectState::Retiring {
                    Uuid::nil()
                } else {
                    Uuid::new_v4()
                },
                delete_dispatched: state == ObjectState::Deleted,
            };
            metadata
                .rows
                .lock()
                .await
                .insert(registry_object_key(&reference), object.encode().unwrap());
            let before = metadata.rows.lock().await.clone();
            assert!(matches!(
                store
                    .reserve_packed_upload(&journal, reference, &budget)
                    .await,
                Err(WorkspaceError::Fenced)
            ));
            assert_eq!(
                *metadata.rows.lock().await,
                before,
                "quarantined physical key was changed"
            );
        }
    }
}
