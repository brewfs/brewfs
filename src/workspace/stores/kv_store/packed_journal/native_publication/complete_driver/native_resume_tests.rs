//! Actual Redis/TiKV crash boundaries from the real native factory. Every
//! source, PUT, registry occurrence, graph and final CAS is produced normally.

use super::*;

fn object_class(reference: &SnapshotObjectRef) -> crate::cadapter::read_observer::ReadClass {
    match reference.kind {
        SnapshotObjectKind::GroupContainer => {
            crate::cadapter::read_observer::ReadClass::GroupMetadata
        }
        SnapshotObjectKind::LargeData => crate::cadapter::read_observer::ReadClass::ExternalPayload,
        kind => crate::workspace::packed::snapshot::page_read_class(kind).unwrap(),
    }
}

#[derive(Clone, Copy)]
pub(super) enum Crash {
    BuildingEmpty,
    ManifestUploaded,
    Uploading,
    Readback,
    LostPutReply,
    BeforePhysicalPut,
}

#[derive(Default)]
struct RemoteCalls {
    puts: std::sync::Mutex<Vec<String>>,
    ranges: std::sync::Mutex<Vec<(String, u64, usize)>>,
}
#[derive(Clone)]
struct ObservedObjects<O> {
    inner: O,
    calls: Arc<RemoteCalls>,
}
#[async_trait]
impl<O: ObjectBackend + Clone + 'static> ObjectBackend for ObservedObjects<O> {
    async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.calls.puts.lock().unwrap().push(key.into());
        self.inner.put_object(key, data).await
    }
    async fn put_object_create_only(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.calls.puts.lock().unwrap().push(key.into());
        self.inner.put_object_create_only(key, data).await
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        buffer: &mut [u8],
    ) -> anyhow::Result<usize> {
        let read = self.inner.get_object_range(key, offset, buffer).await?;
        self.calls
            .ranges
            .lock()
            .unwrap()
            .push((key.into(), offset, read));
        Ok(read)
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(key).await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete_object(key).await
    }
}

#[derive(Clone)]
pub(super) struct InjectedPut<O> {
    inner: O,
    first: Arc<AtomicBool>,
    before: bool,
}
impl<O> InjectedPut<O> {
    pub(super) fn new(inner: O, before: bool) -> Self {
        Self {
            inner,
            first: Arc::new(AtomicBool::new(true)),
            before,
        }
    }
}
#[async_trait]
impl<O: ObjectBackend + Clone + 'static> ObjectBackend for InjectedPut<O> {
    async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("create-only required")
    }
    async fn put_object_create_only(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        if self.first.swap(false, Ordering::SeqCst) {
            if !self.before {
                self.inner.put_object_create_only(key, data).await?;
            }
            anyhow::bail!("injected process-stop delivery at actual guarded PUT boundary")
        }
        self.inner.put_object_create_only(key, data).await
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        buffer: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.inner.get_object_range(key, offset, buffer).await
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(key).await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete_object(key).await
    }
}

async fn actual_resume<B: WorkspaceKvBackend>(
    backend: Arc<B>,
    crash: Crash,
    budget: Arc<SnapshotMountBudget>,
) {
    let f = stage_with_crash_with_budget(backend, Some(crash), budget.clone()).await;
    let store = Arc::new(KvWorkspaceStore::from_arc(f.backend.clone()));
    store
        .configure_packed_reader_pin_budget(budget.clone())
        .unwrap();
    let observer = budget.read_observer(None).unwrap();
    let calls = Arc::new(RemoteCalls::default());
    let client = f
        .client
        .clone()
        .map_backend(|inner| ObservedObjects {
            inner,
            calls: calls.clone(),
        })
        .with_read_observer(
            observer,
            crate::cadapter::read_observer::Engine::Packed,
            crate::cadapter::read_observer::Phase::Startup,
            crate::cadapter::read_observer::Origin::Demand,
        );
    let before_occurrence = if matches!(crash, Crash::LostPutReply | Crash::BeforePhysicalPut) {
        let object = store
            .reopen_packed_object(&f.record, 0, &budget)
            .await
            .unwrap();
        assert!(!object.uploaded);
        assert!(!object.readback_recorded);
        let present = client
            .typed_object_size(object_class(&object.reference), &object.reference.key)
            .await
            .unwrap();
        assert_eq!(present.is_some(), matches!(crash, Crash::LostPutReply));
        Some(object)
    } else {
        None
    };
    let basis = store
        .inspect_native_packed_recovery_basis(&f.record, &budget)
        .await
        .unwrap();
    let recovery = store
        .reissue_native_source_read(basis, budget.clone())
        .await
        .unwrap();
    let active_before = f.backend.get(ACTIVE_COUNT_KEY).await.unwrap();
    let recovery = store
        .quarantine_missing_native_attempt_and_reissue(
            recovery,
            &client,
            1000,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    let resumed_id = recovery.basis().unwrap().journal_id();
    if matches!(crash, Crash::BeforePhysicalPut) {
        assert_ne!(
            resumed_id, f.record.journal_id,
            "unknown-dispatched key must use a new physical attempt"
        );
        let old = PackedJournalRecord::decode(
            &f.backend
                .get(&journal_key(f.record.journal_id))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(old.phase, PackedJournalPhase::Aborted);
        assert_eq!(old.object_count, f.record.object_count);
        assert_eq!(old.inventory_digest, f.record.inventory_digest);
        let occurrence = store.reopen_packed_object(&old, 0, &budget).await.unwrap();
        assert_eq!(
            occurrence.encode().unwrap(),
            before_occurrence.as_ref().unwrap().encode().unwrap(),
            "quarantine must retain the exact pending occurrence"
        );
        assert_eq!(
            f.backend.get(ACTIVE_COUNT_KEY).await.unwrap(),
            active_before
        );
        let packets = f.backend.native_quarantine_packets.lock().unwrap();
        assert_eq!(
            packets.len(),
            1,
            "one actual successful quarantine transaction"
        );
        KvWorkspaceStore::<FinalDelivery<B>>::assert_native_resume_quarantine_packet(
            &f.record,
            recovery.basis().unwrap().record(),
            &occurrence,
            &packets[0].0,
            &packets[0].1,
        );
        assert_eq!(recovery.native_quiesce().source_guard(), &f.old_guard);
        assert_eq!(
            recovery.native_quiesce().canonical_receipt_digest(),
            <[u8; 32]>::from(Sha256::digest(&f.original_q))
        );
    } else {
        assert_eq!(resumed_id, f.record.journal_id);
    }
    let reader: Arc<dyn PackedReaderSession> = KvPackedReaderSession::open_native_recovery(
        recovery.clone(),
        budget.clone(),
        PackedReaderLeaseOptions::default(),
    )
    .await
    .unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &f.binding.binding.manifest)
        .await
        .unwrap();
    let lower = Arc::new(
        PackedReadonlyMeta::from_snapshot_budget(client.clone(), snapshot, 4096, 0, budget.clone())
            .unwrap(),
    );
    let guard = recovery.native_quiesce().source_guard().clone();
    let meta = Arc::new(
        WorkspaceMetaLayer::with_chunk_size(
            store.clone(),
            ViewContext {
                workspace_id: guard.workspace_id,
                head_layer_id: guard.expected_head_layer_id,
                head_epoch: guard.expected_head_epoch,
                lease_id: guard.lease_id,
                holder_generation: guard.holder_generation,
            },
            4096,
        )
        .with_packed_lower(
            f.binding.binding.clone(),
            lower,
            Arc::new(PinnedCatalogPackedBindingAuthority {
                store: store.clone(),
                reader: reader.clone(),
            }),
            f.upper.clone(),
            ChunkLayout {
                chunk_size: 4096,
                block_size: 4096,
            },
        )
        .unwrap(),
    );
    let provider: Arc<dyn WorkspaceReadPlanProvider> = meta.clone();
    let vfs = VFS::from_readonly_components_with_provider(
        VFSConfig::new(ChunkLayout {
            chunk_size: 4096,
            block_size: 4096,
        }),
        f.upper.clone(),
        meta.clone(),
        provider,
    )
    .unwrap();
    let local = vfs.quiesce_packed_vfs().await.unwrap();
    let artifact = FrozenNativeArtifact::capture(
        recovery.native_quiesce().clone(),
        local,
        f.scratch.path().to_path_buf(),
        capture_limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        artifact.source_digest().unwrap(),
        f.record.source.effective_view_digest
    );
    let ready = match store
        .resume_frozen_native_publication(
            artifact,
            recovery,
            client.clone(),
            NativePublicationBuildOptions {
                producer: producer_options(),
                temporary: f.scratch.path().to_path_buf(),
                graph_scratch: f.scratch.path().to_path_buf(),
                graph_limits: SnapshotIndexAuditLimits::default(),
                native_hash_limits: NativeDeltaHashLimits {
                    max_native_delta_rows: 1000,
                    max_canonical_bytes: 8 << 20,
                },
                chunk_size: 4096,
                metadata_cache_bytes: 0,
                max_catalog_rows: 100,
                cancel: CancellationToken::new(),
            },
        )
        .await
    {
        Ok(ready) => ready,
        Err(error) => panic!(
            "actual phase continuation failed: {}",
            preparation_error(&error)
        ),
    };
    let verified = ready.record.value.clone();
    assert_eq!(verified.journal_id, resumed_id);
    let audited_revision = verified.graph_receipt.as_ref().unwrap().audited_revision;
    let digest_before = audit_basis_digest(&verified, audited_revision).unwrap();
    let outcome = match ready.commit().await {
        Ok(value) => value,
        Err(error) => panic!("actual final failed: {}", error.error),
    };
    assert_eq!(outcome.sealed_source.root_hash, f.original_native_root);
    assert_eq!(
        outcome.record.final_source_hash_facts(),
        Some((f.original_native_root, f.original_native_digest))
    );
    assert_eq!(outcome.record.final_source_guard(), Some(&guard));
    assert_eq!(
        digest_before,
        audit_basis_digest(&outcome.record, audited_revision).unwrap(),
        "completion facts must not move the graph basis"
    );
    assert_ne!(
        outcome.binding.base_revision.root_hash, f.original_native_root,
        "native source completion facts must come from the source rather than the empty carrier"
    );
    if let Some(old) = before_occurrence {
        if matches!(crash, Crash::LostPutReply) {
            let completed = store
                .reopen_packed_object(&outcome.record, old.ordinal, &budget)
                .await
                .unwrap();
            assert_eq!(completed.reference, old.reference);
            assert!(completed.uploaded && completed.readback_recorded);
            assert!(
                calls
                    .puts
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|key| key != &old.reference.key),
                "full authentication completes the old occurrence without another physical PUT"
            );
            let mut ranges = calls
                .ranges
                .lock()
                .unwrap()
                .iter()
                .filter(|(key, _, _)| key == &old.reference.key)
                .map(|(_, offset, length)| (*offset, *length as u64))
                .collect::<Vec<_>>();
            ranges.sort_unstable();
            let mut covered = 0u64;
            for (offset, length) in ranges {
                assert!(
                    offset <= covered,
                    "remote authentication must cover the complete old object"
                );
                covered = covered.max(offset.checked_add(length).unwrap());
            }
            assert_eq!(covered, old.reference.object_len);
        } else {
            assert!(
                calls
                    .puts
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|key| key != &old.reference.key),
                "fresh attempt must not dispatch the quarantined physical key"
            );
            assert_eq!(
                client
                    .typed_object_size(object_class(&old.reference), &old.reference.key)
                    .await
                    .unwrap(),
                None,
                "new attempt must never PUT the quarantined physical key"
            );
        }
    }
    drop(outcome);
    drop(vfs);
    drop(meta);
    reader.shutdown().await.unwrap();
    drop(reader);
    drop(store);
    drop(budget);
}

async fn isolated_resume<B: WorkspaceKvBackend>(
    backend: B,
    crash: Crash,
    budget: Arc<SnapshotMountBudget>,
) {
    let backend = Arc::new(backend);
    let worker = backend.clone();
    let worker_budget = budget.clone();
    let result =
        tokio::spawn(async move { actual_resume(worker, crash, worker_budget).await }).await;
    for batch in backend.scan_prefix(b"").await.unwrap().chunks(32) {
        let checks = batch
            .iter()
            .map(|row| KvCheck {
                key: row.key.clone(),
                expected: Some(row.value.clone()),
            })
            .collect::<Vec<_>>();
        let writes = batch
            .iter()
            .map(|row| KvWrite::Delete {
                key: row.key.clone(),
            })
            .collect::<Vec<_>>();
        assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
    }
    backend.shutdown_metadata_backend().await.unwrap();
    budget.close();
    assert!(budget.state().used.iter().all(|bytes| *bytes == 0));
    if let Err(error) = result {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("resume cancelled: {error}");
    }
}
async fn redis_resume(crash: Crash) {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let budget = SnapshotMountBudget::defaults();
    isolated_resume(
        RedisWorkspaceBackend::connect(&url, &format!("packed-native-resume-{}", Uuid::new_v4()))
            .await
            .unwrap(),
        crash,
        budget,
    )
    .await;
}
async fn tikv_resume(crash: Crash) {
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    let budget = SnapshotMountBudget::defaults();
    isolated_resume(
        TiKvWorkspaceBackend::connect_with_budget(
            endpoints,
            &format!("packed-native-resume-{}", Uuid::new_v4()),
            budget.clone(),
        )
        .await
        .unwrap(),
        crash,
        budget,
    )
    .await;
}
macro_rules! selectors {
    ($redis:ident, $tikv:ident, $case:ident) => {
        #[tokio::test]
        #[ignore = "requires actual isolated Redis metadata"]
        async fn $redis() {
            redis_resume(Crash::$case).await;
        }
        #[tokio::test]
        #[ignore = "requires actual isolated TiKV metadata"]
        async fn $tikv() {
            tikv_resume(Crash::$case).await;
        }
    };
}
selectors!(
    real_redis_native_resume_building_empty,
    real_tikv_native_resume_building_empty,
    BuildingEmpty
);
selectors!(
    real_redis_native_resume_uploaded_manifest_before_target,
    real_tikv_native_resume_uploaded_manifest_before_target,
    ManifestUploaded
);
selectors!(
    real_redis_native_resume_uploading_before_readback,
    real_tikv_native_resume_uploading_before_readback,
    Uploading
);
selectors!(
    real_redis_native_resume_readback_cancelled_before_first_object,
    real_tikv_native_resume_readback_cancelled_before_first_object,
    Readback
);
selectors!(
    real_redis_native_resume_actual_put_lost_reply_full_auth_no_reput,
    real_tikv_native_resume_actual_put_lost_reply_full_auth_no_reput,
    LostPutReply
);
selectors!(
    real_redis_native_resume_dispatch_without_physical_put_quarantine_new_attempt,
    real_tikv_native_resume_dispatch_without_physical_put_quarantine_new_attempt,
    BeforePhysicalPut
);
