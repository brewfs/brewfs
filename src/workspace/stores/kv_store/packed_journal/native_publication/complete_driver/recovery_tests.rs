//! Fresh recovery over actual Redis/TiKV transactions and actual captured bytes.
//! Scheduling affects response delivery only; no fabricated authority or digest.

use super::*;
use crate::cadapter::localfs::LocalFsBackend;
use crate::workspace::ids::LeaseId;
use crate::workspace::model::LeaseState;
use crate::workspace::packed_reader_lifecycle::{KvPackedReaderSession, PackedReaderSession};
use crate::workspace::stores::kv_store::packed_native_freeze::NativePackedRecoveryClaimRequest;
use std::time::Duration;

const CLAIM_PREFIX: &[u8] = b"packed/native-recovery-claim/";
const NATIVE_BYTES: &[u8] = b"real recovered original inode payload";
const WAIT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
enum RecoveryCase {
    Normal,
    LostClaimReply,
    CancelClaimWaiter,
    LostFinalReply,
    CancelFinalWaiter,
    HeldFinalOpenExpires,
}

struct ReleaseBoundaries<B>(Arc<FinalDelivery<B>>);
impl<B> Drop for ReleaseBoundaries<B> {
    fn drop(&mut self) {
        self.0.release.notify_one();
    }
}

struct Staged<B> {
    _objects: tempfile::TempDir,
    scratch: tempfile::TempDir,
    client: ObjectClient<LocalFsBackend>,
    backend: Arc<FinalDelivery<B>>,
    old_guard: HeadGuard,
    binding: PackedLowerBindingRecord,
    record: PackedJournalRecord,
    original_q: Vec<u8>,
    original_native_digest: [u8; 32],
    original_native_root: [u8; 32],
    upper: Arc<InMemoryBlockStore>,
    inode: i64,
}

async fn stage<B: WorkspaceKvBackend>(
    backend: Arc<B>,
    budget: Arc<SnapshotMountBudget>,
) -> Staged<B> {
    stage_with_crash_with_budget(backend, None, budget).await
}

async fn stage_with_crash_with_budget<B: WorkspaceKvBackend>(
    backend: Arc<B>,
    crash: Option<native_resume_tests::Crash>,
    budget: Arc<SnapshotMountBudget>,
) -> Staged<B> {
    let (objects, client, snapshot, lower_proof, _) = packed().await;
    let scratch = tempfile::tempdir().unwrap();
    let backend = Arc::new(FinalDelivery::new(backend));
    let store = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    let install = request(store.as_ref(), lower_proof).await;
    let binding = store
        .install_packed_lower_binding(install.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: binding.head_epoch,
        ..install.guard
    };
    store
        .renew_lease(RenewLease {
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
            ttl_ns: 300_000_000_000,
        })
        .await
        .unwrap();
    let reader = store
        .clone()
        .open_packed_reader_session(
            guard.clone(),
            budget.clone(),
            PackedReaderLeaseOptions::default(),
        )
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&reader.mount_budget(), &budget));
    let lower = Arc::new(
        PackedReadonlyMeta::from_snapshot_budget(client.clone(), snapshot, 4096, 0, budget.clone())
            .unwrap(),
    );
    let upper = Arc::new(InMemoryBlockStore::new());
    let layout = ChunkLayout {
        chunk_size: 4096,
        block_size: 4096,
    };
    let authority = Arc::new(PinnedCatalogPackedBindingAuthority {
        store: store.clone(),
        reader: reader.clone(),
    });
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
            binding.binding.clone(),
            lower,
            authority,
            upper.clone(),
            layout,
        )
        .unwrap(),
    );
    meta.initialize().await.unwrap();
    let provider: Arc<dyn WorkspaceReadPlanProvider> = meta.clone();
    let vfs = VFS::from_readonly_components_with_provider(
        VFSConfig::new(layout),
        upper.clone(),
        meta.clone(),
        provider,
    )
    .unwrap();
    let inode = vfs.create_file("/real-recovery-native").await.unwrap();
    let handle = vfs
        .open(
            inode,
            vfs.stat("/real-recovery-native").await.unwrap(),
            true,
            true,
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        vfs.write(handle, 0, NATIVE_BYTES).await.unwrap(),
        NATIVE_BYTES.len()
    );
    vfs.flush(handle).await.unwrap();
    vfs.close(handle).await.unwrap();
    let local = vfs.quiesce_packed_vfs().await.unwrap();
    // The actual InitialSource and drained VFS supply this typed transition.
    // Open birth and Administrative PWA move together in the real backend CAS.
    let keys = [
        hot_lease_key(guard.workspace_id, guard.lease_id),
        open_snapshot_key(guard.workspace_id),
    ];
    let (values, now) = backend
        .get_many_consistent_with_time_bounded(
            &keys,
            crate::workspace::stores::kv_backend::KvReadLimits {
                max_records: 2,
                max_key_bytes: 256,
                max_value_bytes: 12 << 10,
                max_total_bytes: 24 << 10,
                max_response_bytes: 32 << 10,
                max_data_requests: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(values.len(), 2);
    let lease: SnapshotLease = decode_open_value(values[0].as_deref().unwrap(), 12 << 10).unwrap();
    assert!(lease.expires_at_ns > now);
    let open = SnapshotOpenRecord {
        workspace_id: guard.workspace_id,
        owner_id: format!("packed/native-stage/{}", guard.lease_id),
        generation: 1,
        expires_at_ns: lease.expires_at_ns,
        state: SnapshotOpenState::Ready,
        recovery_required: false,
    };
    let mut checks = keys
        .into_iter()
        .zip(values)
        .map(|(key, expected)| KvCheck { key, expected })
        .collect::<Vec<_>>();
    let mut writes = vec![put(open_snapshot_key(guard.workspace_id), &open).unwrap()];
    let _admin_owner = store.prepare_administrative_packed_writer(guard.workspace_id,
        crate::workspace::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::ClaimInitial,
        &mut checks, &mut writes).await.unwrap();
    store
        .clean_exact_cas(&checks, &writes, lease.expires_at_ns)
        .await
        .unwrap();
    let layers = store
        .load_layer_chain(guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let native = Arc::new(
        store
            .clone()
            .begin_packed_native_quiesce(
                guard.clone(),
                layers,
                JournalId::new(),
                LayerId::new(),
                budget.clone(),
            )
            .await
            .unwrap(),
    );
    let original_q = native.canonical_receipt_bytes().to_vec();
    let artifact = FrozenNativeArtifact::capture(
        native,
        local,
        scratch.path().to_path_buf(),
        capture_limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    if let Some(crash) = crash {
        let hash = FrozenNativeDeltaHash::capture(
            artifact.native_quiesce().clone(),
            artifact.native_reader_session(),
            NativeDeltaHashLimits {
                max_native_delta_rows: 1000,
                max_canonical_bytes: 8 << 20,
            },
        )
        .await
        .unwrap();
        let original_native_digest = hash.delta_digest();
        let original_native_root = hash.root_hash();
        drop(hash);
        store
            .migrate_packed_object_registry(
                &client,
                &budget,
                PackedRegistryMigrationOptions {
                    scratch: scratch.path(),
                    graph_limits: SnapshotIndexAuditLimits::default(),
                    max_catalog_rows: 100,
                    cancel: CancellationToken::new(),
                },
            )
            .await
            .unwrap();
        let building = store.begin_native_packed_journal(&artifact).await.unwrap();
        let journal_id = building.journal_id;
        let record = if matches!(crash, native_resume_tests::Crash::BuildingEmpty) {
            drop(artifact);
            building.value.clone()
        } else if matches!(
            crash,
            native_resume_tests::Crash::LostPutReply
                | native_resume_tests::Crash::BeforePhysicalPut
        ) {
            let client = client.clone().map_backend(|inner| {
                native_resume_tests::InjectedPut::new(
                    inner,
                    matches!(crash, native_resume_tests::Crash::BeforePhysicalPut),
                )
            });
            assert!(
                store
                    .build_registered_native_candidate(
                        building,
                        artifact,
                        client,
                        scratch.path().to_path_buf(),
                        producer_options()
                    )
                    .await
                    .is_err()
            );
            PackedJournalRecord::decode(
                &backend
                    .get(&journal_key(journal_id))
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        } else {
            let (artifact, manifest, built) = store
                .build_registered_native_candidate(
                    building,
                    artifact,
                    client.clone(),
                    scratch.path().to_path_buf(),
                    producer_options(),
                )
                .await
                .unwrap();
            if matches!(crash, native_resume_tests::Crash::ManifestUploaded) {
                drop(artifact);
                built.value.clone()
            } else {
                let uploading = store
                    .freeze_native_candidate(&built, &artifact, &manifest)
                    .await
                    .unwrap();
                if matches!(crash, native_resume_tests::Crash::Uploading) {
                    drop(artifact);
                    uploading.value.clone()
                } else {
                    let cancelled = CancellationToken::new();
                    cancelled.cancel();
                    assert!(
                        store
                            .native_readback(uploading, &artifact, &client, &cancelled)
                            .await
                            .is_err()
                    );
                    drop(artifact);
                    PackedJournalRecord::decode(
                        &backend
                            .get(&journal_key(journal_id))
                            .await
                            .unwrap()
                            .unwrap(),
                    )
                    .unwrap()
                }
            }
        };
        assert_eq!(
            record.phase,
            match crash {
                native_resume_tests::Crash::Uploading => PackedJournalPhase::Uploading,
                native_resume_tests::Crash::Readback => PackedJournalPhase::Readback,
                _ => PackedJournalPhase::Building,
            }
        );
        drop(vfs);
        drop(meta);
        reader.shutdown().await.unwrap();
        drop(reader);
        drop(store);
        drop(budget);
        return Staged {
            _objects: objects,
            scratch,
            client,
            backend,
            old_guard: guard,
            binding,
            record,
            original_q,
            original_native_digest,
            original_native_root,
            upper,
            inode,
        };
    }
    let ready = match store
        .prepare_frozen_native_publication(
            artifact,
            client.clone(),
            NativePublicationBuildOptions {
                producer: producer_options(),
                temporary: scratch.path().to_path_buf(),
                graph_scratch: scratch.path().to_path_buf(),
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
            "actual recovery staging failed: {}",
            preparation_error(&error)
        ),
    };
    let record = ready.record.value.clone();
    assert_eq!(record.phase, PackedJournalPhase::Verified);
    assert_eq!(
        ready.source.phase_authority().canonical_receipt_bytes(),
        original_q
    );
    let original_native_digest = ready
        .source
        .phase_authority()
        .native_delta_hash()
        .delta_digest();
    let original_native_root = ready
        .source
        .phase_authority()
        .native_delta_hash()
        .root_hash();
    // Destroy every old-process source/hash/graph/drain authority. The original
    // block bytes and immutable objects remain available to a fresh capture.
    drop(ready);
    drop(vfs);
    drop(meta);
    reader.shutdown().await.unwrap();
    drop(reader);
    drop(store);
    drop(budget);
    Staged {
        _objects: objects,
        scratch,
        client,
        backend,
        old_guard: guard,
        binding,
        record,
        original_q,
        original_native_digest,
        original_native_root,
        upper,
        inode,
    }
}

async fn wait_backend_expiry<B: WorkspaceKvBackend>(backend: &B, expiry: i64) {
    tokio::time::timeout(WAIT, async {
        loop {
            if backend.server_time_ns().await.unwrap() >= expiry {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual backend clock did not cross expiry");
}

fn claim_request(lease: LeaseId, owner: &str) -> NativePackedRecoveryClaimRequest {
    NativePackedRecoveryClaimRequest {
        new_lease_id: lease,
        owner_id: owner.into(),
        ttl_ns: 300_000_000_000,
    }
}

// Test-only scheduling through the real private Administrative transition.
// Retained identity checks, native holds and the changed lease/open share one CAS.
async fn shorten_administrative_expiry<B: WorkspaceKvBackend>(
    store: &KvWorkspaceStore<B>,
    guard: &HeadGuard,
    lease_ttl: Option<u64>,
    open_ttl: Option<u64>,
) -> (SnapshotLease, SnapshotOpenRecord) {
    let keys = [
        hot_lease_key(guard.workspace_id, guard.lease_id),
        open_snapshot_key(guard.workspace_id),
    ];
    let (values, now) = store
        .backend
        .get_many_consistent_with_time_bounded(
            &keys,
            crate::workspace::stores::kv_backend::KvReadLimits {
                max_records: 2,
                max_key_bytes: 256,
                max_value_bytes: 12 << 10,
                max_total_bytes: 24 << 10,
                max_response_bytes: 32 << 10,
                max_data_requests: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(values.len(), 2);
    let mut lease: SnapshotLease =
        decode_open_value(values[0].as_deref().unwrap(), 12 << 10).unwrap();
    let mut open: SnapshotOpenRecord =
        decode_open_value(values[1].as_deref().unwrap(), OPEN_RECORD_MAX_BYTES).unwrap();
    let mut deadline = lease.expires_at_ns.min(open.expires_at_ns);
    let mut checks = keys
        .into_iter()
        .zip(values)
        .map(|(key, expected)| KvCheck { key, expected })
        .collect::<Vec<_>>();
    let mut writes = Vec::new();
    if let Some(ttl) = lease_ttl {
        lease.expires_at_ns = checked_expiry(now, ttl).unwrap();
        lease.updated_at_ns = now;
        deadline = deadline.min(lease.expires_at_ns);
        writes.push(put(hot_lease_key(lease.workspace_id, lease.lease_id), &lease).unwrap());
    }
    if let Some(ttl) = open_ttl {
        open.expires_at_ns = checked_expiry(now, ttl).unwrap();
        deadline = deadline.min(open.expires_at_ns);
        writes.push(put(open_snapshot_key(guard.workspace_id), &open).unwrap());
    }
    let _writer_owner = store.prepare_administrative_packed_writer(guard.workspace_id,
        crate::workspace::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::Update,
        &mut checks, &mut writes).await.unwrap();
    let _holds = store
        .prepare_native_owner_cas(&mut checks, &mut writes)
        .await
        .unwrap();
    store
        .clean_exact_cas(&checks, &writes, deadline)
        .await
        .unwrap();
    (lease, open)
}

async fn actual_contract<B: WorkspaceKvBackend>(
    backend: Arc<B>,
    case: RecoveryCase,
    budget: Arc<SnapshotMountBudget>,
) {
    let f = stage(backend, budget.clone()).await;
    let _release_on_exit = ReleaseBoundaries(f.backend.clone());
    let store = Arc::new(
        KvWorkspaceStore::from_arc(f.backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    let owner = format!("packed/native-stage/{}", f.old_guard.lease_id);
    let new_lease = LeaseId::new();
    let open = store
        .open_workspace_snapshot(
            f.old_guard.workspace_id,
            owner.clone(),
            Duration::from_secs(300),
        )
        .await
        .unwrap();
    assert_eq!(open.state, SnapshotOpenState::Recovering);
    assert!(open.recovery_required);

    // A genuine durable basis plus actual Recovering open still cannot take a
    // live old native lease. Refusal must not submit a claim write.
    let basis = store
        .inspect_native_packed_recovery_basis(&f.record, &budget)
        .await
        .unwrap();
    assert!(matches!(
        store
            .claim_native_packed_recovery(basis, claim_request(new_lease, &owner), budget.clone())
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(f.backend.claim_calls.load(Ordering::SeqCst), 0);
    assert!(
        f.backend
            .scan_prefix(CLAIM_PREFIX)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        PackedJournalRecord::decode(
            &f.backend
                .get(&journal_key(f.record.journal_id))
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        f.record
    );

    // Expire a real production lease, then wait on the actual server clock.
    // No raw record edit, client-clock authority or injected phase is used.
    let (expired, _) =
        shorten_administrative_expiry(store.as_ref(), &f.old_guard, Some(500_000_000), None).await;
    wait_backend_expiry(f.backend.as_ref(), expired.expires_at_ns).await;
    let basis = store
        .inspect_native_packed_recovery_basis(&f.record, &budget)
        .await
        .unwrap();
    f.backend.claim_mode.store(
        match case {
            RecoveryCase::LostClaimReply => 1,
            RecoveryCase::CancelClaimWaiter => 2,
            _ => 0,
        },
        Ordering::SeqCst,
    );
    let claim = if matches!(case, RecoveryCase::CancelClaimWaiter) {
        let mut waiting = Box::pin(store.claim_native_packed_recovery(
            basis,
            claim_request(new_lease, &owner),
            budget.clone(),
        ));
        tokio::time::timeout(WAIT, async { tokio::select! { result = &mut waiting => panic!("held claim returned: {}", result.is_ok()), _ = f.backend.entered.notified() => {} } }).await.unwrap();
        let admitted = budget.state().used[SnapshotBudgetPool::Metadata as usize];
        assert!(
            admitted >= 16 << 20,
            "claim admission disappeared before response"
        );
        drop(waiting);
        assert!(
            budget.state().used[SnapshotBudgetPool::Metadata as usize] >= 16 << 20,
            "cancelled receiver dropped actual claim owner"
        );
        assert_eq!(f.backend.claim_calls.load(Ordering::SeqCst), 1);
        f.backend.release.notify_one();
        tokio::time::timeout(WAIT, f.backend.done.notified())
            .await
            .unwrap();
        f.backend.claim_mode.store(0, Ordering::SeqCst);
        let basis = store
            .inspect_native_packed_recovery_basis(&f.record, &budget)
            .await
            .unwrap();
        store
            .claim_native_packed_recovery(basis, claim_request(new_lease, &owner), budget.clone())
            .await
            .unwrap()
    } else {
        store
            .claim_native_packed_recovery(basis, claim_request(new_lease, &owner), budget.clone())
            .await
            .unwrap()
    };
    assert_eq!(
        f.backend.claim_calls.load(Ordering::SeqCst),
        1,
        "claim reply/cancellation retry repeated a write CAS"
    );
    let source_guard = claim.guard().clone();
    assert_eq!(source_guard.lease_id, new_lease);
    assert!(source_guard.holder_generation > f.old_guard.holder_generation);
    let old: SnapshotLease = decode_open_value(
        &f.backend
            .get(&hot_lease_key(
                f.old_guard.workspace_id,
                f.old_guard.lease_id,
            ))
            .await
            .unwrap()
            .unwrap(),
        48 << 10,
    )
    .unwrap();
    let current: SnapshotLease = decode_open_value(
        &f.backend
            .get(&hot_lease_key(f.old_guard.workspace_id, new_lease))
            .await
            .unwrap()
            .unwrap(),
        48 << 10,
    )
    .unwrap();
    assert_eq!(old.state, LeaseState::Expired);
    assert_eq!(current.state, LeaseState::Active);
    assert_eq!(current.holder_generation, source_guard.holder_generation);
    {
        let saved = f.backend.claim_checks.lock().unwrap();
        let (checks, lower, upper) = saved.as_ref().unwrap();
        assert_eq!(*lower, Some(expired.expires_at_ns));
        assert!(upper.unwrap() <= current.expires_at_ns);
        for key in [
            CONTROL_KEY.to_vec(),
            hot_workspace_key(f.old_guard.workspace_id),
            hot_lease_key(f.old_guard.workspace_id, f.old_guard.lease_id),
            hot_lease_key(f.old_guard.workspace_id, new_lease),
            hot_lease_index_key(f.old_guard.lease_id),
            hot_lease_index_key(new_lease),
            open_snapshot_key(f.old_guard.workspace_id),
            open_snapshot_recovery_key(f.old_guard.workspace_id),
            journal_key(f.record.journal_id),
            packed_current_key(f.old_guard.workspace_id),
        ] {
            assert!(
                checks.iter().any(|check| check.key == key),
                "actual claim omitted an exact condition"
            );
        }
    }

    // A receipt belongs to one actual store instance; a claimed source must
    // share the claim's mount ledger. These attempts fail before minting a pin.
    let other_store = Arc::new(KvWorkspaceStore::from_arc(f.backend.clone()));
    let basis = store
        .inspect_native_packed_recovery_basis(&f.record, &budget)
        .await
        .unwrap();
    assert!(matches!(
        other_store
            .reissue_native_source_read_claimed(basis, claim.clone(), budget.clone())
            .await,
        Err(WorkspaceError::Fenced)
    ));
    let basis = store
        .inspect_native_packed_recovery_basis(&f.record, &budget)
        .await
        .unwrap();
    assert!(matches!(
        store
            .reissue_native_source_read_claimed(
                basis,
                claim.clone(),
                SnapshotMountBudget::defaults()
            )
            .await,
        Err(WorkspaceError::Fenced)
    ));
    let basis = store
        .inspect_native_packed_recovery_basis(&f.record, &budget)
        .await
        .unwrap();
    let recovery = store
        .reissue_native_source_read_claimed(basis, claim.clone(), budget.clone())
        .await
        .unwrap();
    assert_eq!(
        recovery.native_quiesce().mapping().old_guard(),
        &f.old_guard
    );
    assert_eq!(recovery.native_quiesce().source_guard(), &source_guard);
    assert_eq!(
        recovery.native_quiesce().canonical_receipt_bytes(),
        f.original_q
    );
    assert!(matches!(
        KvPackedReaderSession::open_native_recovery(
            recovery.clone(),
            SnapshotMountBudget::defaults(),
            PackedReaderLeaseOptions::default()
        )
        .await,
        Err(WorkspaceError::Fenced)
    ));
    let reader: Arc<dyn PackedReaderSession> = KvPackedReaderSession::open_native_recovery(
        recovery.clone(),
        budget.clone(),
        PackedReaderLeaseOptions::default(),
    )
    .await
    .unwrap();
    reader.validate().await.unwrap();
    let lower_snapshot = AuthenticatedSnapshot::open(&f.client, &f.binding.binding.manifest)
        .await
        .unwrap();
    let lower = Arc::new(
        PackedReadonlyMeta::from_snapshot_budget(
            f.client.clone(),
            lower_snapshot,
            4096,
            0,
            budget.clone(),
        )
        .unwrap(),
    );
    let layout = ChunkLayout {
        chunk_size: 4096,
        block_size: 4096,
    };
    let authority = Arc::new(PinnedCatalogPackedBindingAuthority {
        store: store.clone(),
        reader: reader.clone(),
    });
    let meta = Arc::new(
        WorkspaceMetaLayer::with_chunk_size(
            store.clone(),
            ViewContext {
                workspace_id: source_guard.workspace_id,
                head_layer_id: source_guard.expected_head_layer_id,
                head_epoch: source_guard.expected_head_epoch,
                lease_id: source_guard.lease_id,
                holder_generation: source_guard.holder_generation,
            },
            4096,
        )
        .with_packed_lower(
            f.binding.binding.clone(),
            lower,
            authority,
            f.upper.clone(),
            layout,
        )
        .unwrap(),
    );
    // This VFS is only a newly drained frozen-source carrier. Ordinary writable
    // initialize correctly refuses a Sealing head; typed recovery supplies
    // source authority and the actual capture checks every store/guard/budget.
    let provider: Arc<dyn WorkspaceReadPlanProvider> = meta.clone();
    let vfs = VFS::from_readonly_components_with_provider(
        VFSConfig::new(layout),
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
    let target = f.record.commit_target.clone().unwrap();
    let snapshot = AuthenticatedSnapshot::open(&f.client, &target.binding.manifest)
        .await
        .unwrap();
    let candidate = Arc::new(
        PackedReadonlyMeta::from_snapshot_budget(
            f.client.clone(),
            snapshot,
            4096,
            0,
            budget.clone(),
        )
        .unwrap(),
    );
    let source = artifact
        .compare_recovery_candidate(recovery.clone(), candidate.clone())
        .await
        .unwrap();
    let hash = Arc::new(
        FrozenNativeDeltaHash::capture(
            source.native_quiesce().clone(),
            source.native_reader_session(),
            NativeDeltaHashLimits {
                max_native_delta_rows: 1000,
                max_canonical_bytes: 8 << 20,
            },
        )
        .await
        .unwrap(),
    );
    assert_eq!(hash.delta_digest(), f.original_native_digest);
    assert_eq!(hash.root_hash(), f.original_native_root);
    let source = match source.promote_native_hashed(hash).await.unwrap() {
        Ok(source) => source,
        Err(error) => panic!(
            "fresh actual recovery phase reissue failed: {}",
            error.error
        ),
    };
    assert_eq!(
        source.phase_authority().canonical_receipt_bytes(),
        f.original_q
    );
    assert_eq!(source.native_quiesce().source_guard(), &source_guard);
    let graph = store
        .audit_hashed_native_effective_graph(
            &f.record,
            &source,
            &f.client,
            &budget,
            ImportedGraphAuditOptions {
                scratch: f.scratch.path(),
                limits: SnapshotIndexAuditLimits::default(),
                cancel: CancellationToken::new(),
            },
        )
        .await
        .unwrap();
    let ready = match store
        .assemble_native_publication(source, graph, budget.clone())
        .await
    {
        Ok(ready) => ready,
        Err(error) => panic!(
            "new actual source guard factory assembly failed: {}",
            preparation_error(&error)
        ),
    };
    assert_eq!(
        ready.record.guard, f.old_guard,
        "original PPJ identity was rewritten"
    );
    let record_before_commit = ready.record.value.clone();
    let plan = ready
        .record
        .native_rebind
        .as_ref()
        .unwrap()
        .publication
        .as_ref()
        .unwrap()
        .clone();
    f.backend.mode.store(
        match case {
            RecoveryCase::LostFinalReply => Delivery::LostReply,
            RecoveryCase::CancelFinalWaiter => Delivery::CancelWaiter,
            RecoveryCase::HeldFinalOpenExpires => Delivery::HoldFinalBefore,
            _ => Delivery::Normal,
        } as u8,
        Ordering::SeqCst,
    );
    if matches!(case, RecoveryCase::HeldFinalOpenExpires) {
        let (_, open) = shorten_administrative_expiry(
            store.as_ref(),
            &source_guard,
            None,
            Some(10_000_000_000),
        )
        .await;
        let mut waiting = Box::pin(ready.commit());
        tokio::time::timeout(WAIT, async { tokio::select! { result = &mut waiting => panic!("held final returned: {}", result.is_ok()), _ = f.backend.entered.notified() => {} } }).await.unwrap();
        wait_backend_expiry(f.backend.as_ref(), open.expires_at_ns).await;
        let (checks, deadline) = f.backend.final_checks.lock().unwrap().clone().unwrap();
        assert!(deadline <= open.expires_at_ns);
        assert!(
            checks
                .iter()
                .any(|check| check.key == hot_lease_key(f.old_guard.workspace_id, new_lease))
        );
        f.backend.release.notify_one();
        assert!(
            tokio::time::timeout(WAIT, waiting).await.unwrap().is_err(),
            "held final published after its actual open owner expired"
        );
        assert_eq!(f.backend.final_calls.load(Ordering::SeqCst), 1);
        assert!(
            f.backend
                .get(&hot_layer_key(plan.carrier_layer_id))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            PackedJournalRecord::decode(
                &f.backend
                    .get(&journal_key(f.record.journal_id))
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            record_before_commit
        );
        assert_eq!(
            PackedLowerBindingRecord::decode(
                &f.backend
                    .get(&packed_current_key(f.old_guard.workspace_id))
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            f.binding
        );
        drop(vfs);
        drop(meta);
        drop(candidate);
        drop(recovery);
        drop(claim);
        reader.shutdown().await.unwrap();
        return;
    }
    let outcome = if matches!(case, RecoveryCase::CancelFinalWaiter) {
        let mut waiting = Box::pin(ready.commit());
        tokio::time::timeout(WAIT, async { tokio::select! { result = &mut waiting => panic!("held final reply returned: {}", result.is_ok()), _ = f.backend.entered.notified() => {} } }).await.unwrap();
        assert!(budget.state().used[SnapshotBudgetPool::Metadata as usize] >= OPERATION_BYTES);
        drop(waiting);
        assert!(
            budget.state().used[SnapshotBudgetPool::Metadata as usize] >= OPERATION_BYTES,
            "cancelled final receiver dropped actual source/graph owner"
        );
        f.backend.release.notify_one();
        tokio::time::timeout(WAIT, f.backend.done.notified())
            .await
            .unwrap();
        None
    } else {
        Some(match ready.commit().await {
            Ok(outcome) => outcome,
            Err(error) => panic!("recovered actual final commit failed: {}", error.error),
        })
    };
    assert_eq!(
        f.backend.final_calls.load(Ordering::SeqCst),
        1,
        "lost reply/cancellation resubmitted final CAS"
    );
    {
        let saved = f.backend.final_checks.lock().unwrap();
        let (checks, _) = saved.as_ref().unwrap();
        for key in [
            hot_lease_key(f.old_guard.workspace_id, new_lease),
            open_snapshot_key(f.old_guard.workspace_id),
            open_snapshot_recovery_key(f.old_guard.workspace_id),
            journal_key(f.record.journal_id),
        ] {
            assert!(
                checks
                    .iter()
                    .any(|check| check.key == key && check.expected.is_some()),
                "actual final CAS omitted current recovering authority"
            );
        }
        assert!(
            checks
                .iter()
                .any(|check| check.key.starts_with(CLAIM_PREFIX) && check.expected.is_some())
        );
    }
    let published_guard = HeadGuard {
        expected_head_layer_id: target.head_layer_id,
        expected_head_epoch: target.head_epoch,
        ..source_guard.clone()
    };
    if let Some(outcome) = outcome {
        assert_eq!(outcome.guard, published_guard);
        assert_eq!(outcome.binding, target);
    }
    assert_eq!(
        store
            .load_packed_binding_record(published_guard)
            .await
            .unwrap(),
        Some(target.clone())
    );
    let old: SnapshotLease = decode_open_value(
        &f.backend
            .get(&hot_lease_key(
                f.old_guard.workspace_id,
                f.old_guard.lease_id,
            ))
            .await
            .unwrap()
            .unwrap(),
        48 << 10,
    )
    .unwrap();
    let new: SnapshotLease = decode_open_value(
        &f.backend
            .get(&hot_lease_key(f.old_guard.workspace_id, new_lease))
            .await
            .unwrap()
            .unwrap(),
        48 << 10,
    )
    .unwrap();
    assert_eq!(old.state, LeaseState::Expired);
    assert_eq!(new.state, LeaseState::Active);
    assert_eq!(new.base_revision.layer_id, plan.carrier_layer_id);
    assert_eq!(new.holder_generation, source_guard.holder_generation);
    let after_open: SnapshotOpenRecord = decode_open_value(
        &f.backend
            .get(&open_snapshot_key(f.old_guard.workspace_id))
            .await
            .unwrap()
            .unwrap(),
        12 << 10,
    )
    .unwrap();
    assert_eq!(after_open.state, SnapshotOpenState::Ready);
    assert!(!after_open.recovery_required);
    assert_eq!(after_open.owner_id, owner);
    assert_eq!(after_open.generation, open.generation);
    assert_eq!(after_open.expires_at_ns, open.expires_at_ns);
    assert!(
        f.backend
            .scan_prefix(CLAIM_PREFIX)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        f.backend
            .get(&open_snapshot_recovery_key(f.old_guard.workspace_id))
            .await
            .unwrap()
            .is_none()
    );
    let committed = PackedJournalRecord::decode(
        &f.backend
            .get(&journal_key(f.record.journal_id))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(committed.phase, PackedJournalPhase::Committed);
    assert_eq!(committed.guard, f.old_guard);
    assert_eq!(
        committed.native_rebind.as_ref().unwrap().quiesce_receipt,
        f.original_q
    );
    let source_layer = store
        .load_layer(f.old_guard.expected_head_layer_id)
        .await
        .unwrap();
    assert_eq!(source_layer.delta_digest, Some(f.original_native_digest));
    assert_eq!(source_layer.root_hash, Some(f.original_native_root));
    let plan = candidate
        .prepare_unified_read(f.inode, 0, 0, NATIVE_BYTES.len() as u64)
        .await
        .unwrap()
        .unwrap();
    let mut bytes = vec![0; NATIVE_BYTES.len()];
    execute_unified_into(plan.fetcher.as_ref(), 0, &plan.plan, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, NATIVE_BYTES);
    drop(plan);
    drop(vfs);
    drop(meta);
    drop(candidate);
    drop(recovery);
    drop(claim);
    reader.shutdown().await.unwrap();
}

async fn isolated_recovery<B: WorkspaceKvBackend>(
    backend: B,
    case: RecoveryCase,
    budget: Arc<SnapshotMountBudget>,
) {
    let backend = Arc::new(backend);
    let worker = backend.clone();
    let worker_budget = budget.clone();
    let result =
        tokio::spawn(async move { actual_contract(worker, case, worker_budget).await }).await;
    let entries = backend.scan_prefix(b"").await.unwrap();
    for batch in entries.chunks(32) {
        let checks = batch
            .iter()
            .map(|entry| KvCheck {
                key: entry.key.clone(),
                expected: Some(entry.value.clone()),
            })
            .collect::<Vec<_>>();
        let writes = batch
            .iter()
            .map(|entry| KvWrite::Delete {
                key: entry.key.clone(),
            })
            .collect::<Vec<_>>();
        assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
    }
    assert!(backend.scan_prefix(b"").await.unwrap().is_empty());
    backend.shutdown_metadata_backend().await.unwrap();
    budget.close();
    assert!(budget.state().used.iter().all(|bytes| *bytes == 0));
    if let Err(error) = result {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("actual recovery task cancelled: {error}");
    }
}

async fn redis_recovery(case: RecoveryCase) {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let budget = SnapshotMountBudget::defaults();
    isolated_recovery(
        RedisWorkspaceBackend::connect(&url, &format!("g12-native-recovery-{}", Uuid::new_v4()))
            .await
            .unwrap(),
        case,
        budget,
    )
    .await;
}
async fn tikv_recovery(case: RecoveryCase) {
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    let budget = SnapshotMountBudget::defaults();
    isolated_recovery(
        TiKvWorkspaceBackend::connect_with_budget(
            endpoints,
            &format!("g12-native-recovery-{}", Uuid::new_v4()),
            budget.clone(),
        )
        .await
        .unwrap(),
        case,
        budget,
    )
    .await;
}

#[tokio::test]
#[ignore = "actual Redis UUID namespace; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_recovery_expired_lease_new_store_typed_capture_hash_factory() {
    redis_recovery(RecoveryCase::Normal).await
}
#[tokio::test]
#[ignore = "actual TiKV UUID namespace; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_recovery_expired_lease_new_store_typed_capture_hash_factory() {
    tikv_recovery(RecoveryCase::Normal).await
}
#[tokio::test]
#[ignore = "actual Redis UUID namespace; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_recovery_lost_claim_reply_exact_same_incarnation() {
    redis_recovery(RecoveryCase::LostClaimReply).await
}
#[tokio::test]
#[ignore = "actual TiKV UUID namespace; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_recovery_lost_claim_reply_exact_same_incarnation() {
    tikv_recovery(RecoveryCase::LostClaimReply).await
}
#[tokio::test]
#[ignore = "actual Redis UUID namespace; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_recovery_cancel_claim_waiter_keeps_held_actual_cas_owner() {
    redis_recovery(RecoveryCase::CancelClaimWaiter).await
}
#[tokio::test]
#[ignore = "actual TiKV UUID namespace; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_recovery_cancel_claim_waiter_keeps_held_actual_cas_owner() {
    tikv_recovery(RecoveryCase::CancelClaimWaiter).await
}
#[tokio::test]
#[ignore = "actual Redis UUID namespace; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_recovery_lost_final_reply_confirms_new_lease_open_claim_successors() {
    redis_recovery(RecoveryCase::LostFinalReply).await
}
#[tokio::test]
#[ignore = "actual TiKV UUID namespace; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_recovery_lost_final_reply_confirms_new_lease_open_claim_successors() {
    tikv_recovery(RecoveryCase::LostFinalReply).await
}
#[tokio::test]
#[ignore = "actual Redis UUID namespace; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_recovery_cancel_final_waiter_keeps_fresh_source_and_graph() {
    redis_recovery(RecoveryCase::CancelFinalWaiter).await
}
#[tokio::test]
#[ignore = "actual TiKV UUID namespace; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_recovery_cancel_final_waiter_keeps_fresh_source_and_graph() {
    tikv_recovery(RecoveryCase::CancelFinalWaiter).await
}
#[tokio::test]
#[ignore = "actual Redis UUID namespace; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_recovery_held_final_rejects_actual_open_owner_expiry() {
    redis_recovery(RecoveryCase::HeldFinalOpenExpires).await
}
#[tokio::test]
#[ignore = "actual TiKV UUID namespace; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_recovery_held_final_rejects_actual_open_owner_expiry() {
    tikv_recovery(RecoveryCase::HeldFinalOpenExpires).await
}

#[path = "native_resume_tests.rs"]
mod native_resume_tests;
