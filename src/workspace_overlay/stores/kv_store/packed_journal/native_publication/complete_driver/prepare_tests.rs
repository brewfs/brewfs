//! Actual interrupted Prepare -> typed recovered Q -> first PNB -> publication.
//! The only injected failure stops Q before submission after actual Prepare.

use super::*;
use crate::workspace_overlay::ids::LeaseId;
use crate::workspace_overlay::model::LeaseState;
use crate::workspace_overlay::packed_reader_lifecycle::{
    KvPackedReaderSession, PackedReaderSession,
};
use crate::workspace_overlay::stores::kv_store::packed_native_freeze::{
    NativePackedRecoveryClaimRequest, NativePrepareRecoveryRequest,
};
use std::time::Duration;

const CLAIM_PREFIX: &[u8] = b"packed/v3/native-recovery-claim/";
const SEED_PREFIX: &[u8] = b"packed/v3/native-freeze-basis/";
const WAIT: Duration = Duration::from_secs(30);
const PAYLOAD: &[u8] = b"actual interrupted Prepare original inode bytes";

async fn wait_expired<B: WorkspaceKvBackend>(backend: &B, expiry: i64) {
    tokio::time::timeout(WAIT, async {
        while backend.server_time_ns().await.unwrap() < expiry {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual backend lease expiry");
}

async fn prepare_contract<B: WorkspaceKvBackend>(backend: Arc<B>, expired: bool) {
    let (_objects, client, snapshot, lower_proof, _) = packed().await;
    let scratch = tempfile::tempdir().unwrap();
    let backend = Arc::new(FinalDelivery::new(backend));
    let original = Arc::new(KvWorkspaceStore::from_arc(backend.clone()));
    let install = request(original.as_ref(), lower_proof).await;
    let binding = original
        .install_packed_lower_binding(install.clone())
        .await
        .unwrap();
    let old_guard = HeadGuard {
        expected_head_epoch: binding.head_epoch,
        ..install.guard
    };
    original
        .renew_lease(RenewLease {
            lease_id: old_guard.lease_id,
            holder_generation: old_guard.holder_generation,
            ttl_ns: 300_000_000_000,
        })
        .await
        .unwrap();
    let old_reader = original
        .clone()
        .open_packed_reader_session(
            old_guard.clone(),
            V3MountBudget::defaults(),
            PackedReaderLeaseOptions::default(),
        )
        .await
        .unwrap();
    let old_budget = old_reader.mount_budget();
    let lower = Arc::new(
        PackedV3ReadonlyMeta::from_v3_budget(client.clone(), snapshot, 4096, 0, old_budget.clone())
            .unwrap(),
    );
    let upper = Arc::new(InMemoryBlockStore::new());
    let layout = ChunkLayout {
        chunk_size: 4096,
        block_size: 4096,
    };
    let old_meta = Arc::new(
        WorkspaceMetaLayer::with_chunk_size(
            original.clone(),
            ViewContext {
                workspace_id: old_guard.workspace_id,
                head_layer_id: old_guard.expected_head_layer_id,
                head_epoch: old_guard.expected_head_epoch,
                lease_id: old_guard.lease_id,
                holder_generation: old_guard.holder_generation,
            },
            4096,
        )
        .with_packed_v3_lower(
            binding.binding.clone(),
            lower,
            Arc::new(PinnedCatalogPackedBindingAuthority {
                store: original.clone(),
                reader: old_reader.clone(),
            }),
            upper.clone(),
            layout,
        )
        .unwrap(),
    );
    old_meta.initialize().await.unwrap();
    let provider: Arc<dyn WorkspaceReadPlanProvider> = old_meta.clone();
    let old_vfs = VFS::from_readonly_components_with_provider(
        VFSConfig::new(layout),
        upper.clone(),
        old_meta.clone(),
        provider,
    )
    .unwrap();
    let inode = old_vfs.create_file("/prepare-native").await.unwrap();
    let handle = old_vfs
        .open(
            inode,
            old_vfs.stat("/prepare-native").await.unwrap(),
            true,
            true,
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        old_vfs.write(handle, 0, PAYLOAD).await.unwrap(),
        PAYLOAD.len()
    );
    old_vfs.flush(handle).await.unwrap();
    old_vfs.close(handle).await.unwrap();
    let drained = old_vfs.quiesce_packed_vfs().await.unwrap();
    let old_layers: [LayerRecord; 2] = original
        .load_layer_chain(old_guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let native_journal = JournalId::new();
    let new_head = LayerId::new();
    backend.stop_original_seed_q.store(true, Ordering::SeqCst);
    assert!(
        original
            .clone()
            .begin_packed_native_quiesce(
                old_guard.clone(),
                old_layers.clone(),
                native_journal,
                new_head,
                old_budget.clone(),
            )
            .await
            .is_err()
    );
    let prepared_control = test_entity_state(backend.as_ref()).await;
    let actual_prepare = prepared_control
        .journals
        .get(&native_journal)
        .unwrap()
        .clone();
    assert_eq!(actual_prepare.phase, SealPhase::Prepare);
    assert_eq!(actual_prepare.new_head_layer_id, Some(new_head));
    let seed_key = format!("packed/v3/native-freeze-basis/{native_journal}").into_bytes();
    let prepared_seed = backend.get(&seed_key).await.unwrap().unwrap();
    assert!(prepared_seed.starts_with(b"NQB3"));
    assert!(prepared_seed.len() <= 16 << 10);
    assert!(backend.scan_prefix(CLAIM_PREFIX).await.unwrap().is_empty());
    assert!(
        backend
            .seed_prepare_checks
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .iter()
            .any(|check| check.key == seed_key && check.expected.is_none())
    );
    drop(drained);
    drop(old_vfs);
    drop(old_meta);
    old_reader.shutdown().await.unwrap();
    drop(old_reader);
    drop(old_budget);
    drop(original);

    let store = Arc::new(KvWorkspaceStore::from_arc(backend.clone()));
    let budget = V3MountBudget::defaults();
    let owner = format!("actual-prepare-owner-{}", Uuid::new_v4());
    let open = store
        .open_workspace_v3(
            old_guard.workspace_id,
            owner.clone(),
            Duration::from_secs(300),
        )
        .await
        .unwrap();
    assert_eq!(open.state, V3OpenState::Recovering);
    assert!(open.recovery_required);
    let new_lease = LeaseId::new();
    // A fresh actual Recovering owner cannot replace a live native lease.
    assert!(matches!(
        store
            .recover_packed_native_prepare(
                NativePrepareRecoveryRequest {
                    journal_id: native_journal,
                    owner_id: owner.clone(),
                    new_lease_id: Some(new_lease),
                    ttl_ns: 300_000_000_000,
                },
                budget.clone()
            )
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert!(backend.scan_prefix(CLAIM_PREFIX).await.unwrap().is_empty());
    if expired {
        let lease = store
            .renew_lease(RenewLease {
                lease_id: old_guard.lease_id,
                holder_generation: old_guard.holder_generation,
                ttl_ns: 500_000_000,
            })
            .await
            .unwrap();
        wait_expired(backend.as_ref(), lease.expires_at_ns).await;
    }
    let native = store
        .recover_packed_native_prepare(
            NativePrepareRecoveryRequest {
                journal_id: native_journal,
                owner_id: owner.clone(),
                new_lease_id: expired.then_some(new_lease),
                ttl_ns: 300_000_000_000,
            },
            budget.clone(),
        )
        .await
        .unwrap();
    let source_guard = native.source_guard().clone();
    assert_eq!(native.mapping().old_guard(), &old_guard);
    assert_eq!(native.mapping().old_layers(), &old_layers);
    assert_eq!(native.mapping().journal_id(), native_journal);
    assert_eq!(native.mapping().planned_head_layer_id(), new_head);
    assert_eq!(
        source_guard.lease_id,
        if expired {
            new_lease
        } else {
            old_guard.lease_id
        }
    );
    assert_eq!(
        source_guard.holder_generation,
        old_guard.holder_generation + u64::from(expired)
    );
    let original_q = native.canonical_receipt_bytes().to_vec();
    let q_seed = backend.get(&seed_key).await.unwrap().unwrap();
    assert_ne!(q_seed, prepared_seed);
    assert!(
        backend
            .scan_prefix(b"packed/v3/native-freeze-recovery-claim/")
            .await
            .unwrap()
            .is_empty(),
        "a second recovery cursor exists"
    );
    let reader: Arc<dyn PackedReaderSession> = KvPackedReaderSession::open_native_prepare(
        &store,
        native.clone(),
        budget.clone(),
        PackedReaderLeaseOptions::default(),
    )
    .await
    .unwrap();
    reader.validate().await.unwrap();
    let snapshot = AuthenticatedV3Snapshot::open(&client, &binding.binding.manifest)
        .await
        .unwrap();
    let lower = Arc::new(
        PackedV3ReadonlyMeta::from_v3_budget(client.clone(), snapshot, 4096, 0, budget.clone())
            .unwrap(),
    );
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
        .with_packed_v3_lower(
            binding.binding.clone(),
            lower,
            Arc::new(PinnedCatalogPackedBindingAuthority {
                store: store.clone(),
                reader: reader.clone(),
            }),
            upper.clone(),
            layout,
        )
        .unwrap(),
    );
    // Sealing recovery is a freshly drained source carrier with a genuine
    // typed pin; ordinary Writable initialization is deliberately unavailable.
    let provider: Arc<dyn WorkspaceReadPlanProvider> = meta.clone();
    let vfs = VFS::from_readonly_components_with_provider(
        VFSConfig::new(layout),
        upper.clone(),
        meta.clone(),
        provider,
    )
    .unwrap();
    let local = vfs.quiesce_packed_vfs().await.unwrap();
    let artifact = FrozenNativeArtifact::capture(
        native.clone(),
        local,
        scratch.path().to_path_buf(),
        capture_limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let ready = match store
        .prepare_frozen_native_publication(
            artifact,
            client.clone(),
            NativePublicationBuildOptions {
                producer: producer_options(),
                temporary: scratch.path().to_path_buf(),
                graph_scratch: scratch.path().to_path_buf(),
                graph_limits: V3IndexAuditLimits::default(),
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
            "actual Prepare factory failed: {}",
            preparation_error(&error)
        ),
    };
    assert_eq!(ready.record.guard, old_guard);
    assert_eq!(ready.source.phase_authority().source_guard(), &source_guard);
    assert_eq!(
        ready.source.phase_authority().canonical_receipt_bytes(),
        original_q
    );
    assert_eq!(ready.record.phase, PackedJournalPhase::Verified);
    assert_eq!(
        backend.get(&seed_key).await.unwrap().unwrap(),
        q_seed,
        "PNB mutated original Q seed"
    );
    let record = ready.record.value.clone();
    let target = ready.record.commit_target.clone().unwrap();
    let owner_key = format!("packed/v3/native-recovery-claim/{native_journal}").into_bytes();
    let owner_before = backend.get(&owner_key).await.unwrap().unwrap();
    let basis = store
        .inspect_native_packed_recovery_basis(&record, &budget)
        .await
        .unwrap();
    // A real bound PNB basis checks the actual current lease, which is active;
    // the original lease being Expired cannot authorize another owner.
    assert!(matches!(
        store
            .claim_native_packed_recovery(
                basis,
                NativePackedRecoveryClaimRequest {
                    new_lease_id: LeaseId::new(),
                    owner_id: owner.clone(),
                    ttl_ns: 300_000_000_000,
                },
                budget.clone()
            )
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(
        backend.get(&owner_key).await.unwrap().unwrap(),
        owner_before
    );
    assert!(matches!(
        store
            .recover_packed_native_prepare(
                NativePrepareRecoveryRequest {
                    journal_id: native_journal,
                    owner_id: owner.clone(),
                    new_lease_id: expired.then_some(new_lease),
                    ttl_ns: 300_000_000_000,
                },
                budget.clone()
            )
            .await,
        Err(WorkspaceError::Fenced)
    ));
    {
        let observed = backend.seed_first_pnb.lock().unwrap();
        let (checks, writes, deadline) = observed
            .as_ref()
            .expect("actual first PNB CAS not observed");
        assert!(*deadline <= open.expires_at_ns);
        for key in [
            seed_key.clone(),
            owner_key.clone(),
            hot_lease_key(source_guard.workspace_id, source_guard.lease_id),
            open_v3_key(old_guard.workspace_id),
            open_v3_recovery_key(old_guard.workspace_id),
        ] {
            assert!(
                checks.iter().any(|check| check.key == key),
                "first PNB omitted current source condition"
            );
        }
        assert!(
            writes
                .iter()
                .any(|write| matches!(write, KvWrite::Put { key, .. } if key == &owner_key)),
            "owner handoff is not in first PNB CAS"
        );
        assert!(writes.iter().any(|write| matches!(write, KvWrite::Put { key, .. } if key == &journal_key(record.journal_id))), "PPJ not in handoff CAS");
    }
    let outcome = match ready.commit().await {
        Ok(outcome) => outcome,
        Err(error) => panic!("actual Prepare final failed: {}", error.error),
    };
    let published_guard = HeadGuard {
        expected_head_layer_id: target.head_layer_id,
        expected_head_epoch: target.head_epoch,
        ..source_guard.clone()
    };
    assert_eq!(outcome.guard, published_guard);
    assert_eq!(
        store
            .load_packed_binding_record(published_guard)
            .await
            .unwrap(),
        Some(target)
    );
    let lease: SnapshotLease = decode_open_value(
        &backend
            .get(&hot_lease_key(
                source_guard.workspace_id,
                source_guard.lease_id,
            ))
            .await
            .unwrap()
            .unwrap(),
        48 << 10,
    )
    .unwrap();
    assert_eq!(lease.state, LeaseState::Active);
    if expired {
        let old: SnapshotLease = decode_open_value(
            &backend
                .get(&hot_lease_key(old_guard.workspace_id, old_guard.lease_id))
                .await
                .unwrap()
                .unwrap(),
            48 << 10,
        )
        .unwrap();
        assert_eq!(old.state, LeaseState::Expired);
    }
    let after_open: V3OpenRecord = decode_open_value(
        &backend
            .get(&open_v3_key(old_guard.workspace_id))
            .await
            .unwrap()
            .unwrap(),
        OPEN_RECORD_MAX_BYTES,
    )
    .unwrap();
    assert_eq!(after_open.state, V3OpenState::Ready);
    assert!(!after_open.recovery_required);
    drop(record);
    drop(native);
    drop(vfs);
    drop(meta);
    reader.shutdown().await.unwrap();
}

async fn isolated<B: WorkspaceKvBackend>(backend: B, expired: bool) {
    let backend = Arc::new(backend);
    let worker = backend.clone();
    let result = tokio::spawn(async move { prepare_contract(worker, expired).await }).await;
    let rows = backend.scan_prefix(b"").await.unwrap();
    for batch in rows.chunks(32) {
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
    assert!(backend.scan_prefix(b"").await.unwrap().is_empty());
    backend.shutdown_metadata_backend().await.unwrap();
    if let Err(error) = result {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("actual Prepare task cancelled: {error}");
    }
}

async fn redis(expired: bool) {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    isolated(
        RedisWorkspaceBackend::connect(&url, &format!("native-prepare-{}", Uuid::new_v4()))
            .await
            .unwrap(),
        expired,
    )
    .await;
}
async fn tikv(expired: bool) {
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    isolated(
        TiKvWorkspaceBackend::connect(endpoints, &format!("native-prepare-{}", Uuid::new_v4()))
            .await
            .unwrap(),
        expired,
    )
    .await;
}

#[tokio::test]
#[ignore = "actual Redis UUID namespace; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_prepare_original_lease_fresh_store_first_pnb_factory() {
    redis(false).await
}
#[tokio::test]
#[ignore = "actual TiKV UUID namespace; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_prepare_original_lease_fresh_store_first_pnb_factory() {
    tikv(false).await
}
#[tokio::test]
#[ignore = "actual Redis UUID namespace; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_prepare_expired_lease_unique_owner_first_pnb_factory() {
    redis(true).await
}
#[tokio::test]
#[ignore = "actual TiKV UUID namespace; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_prepare_expired_lease_unique_owner_first_pnb_factory() {
    tikv(true).await
}
