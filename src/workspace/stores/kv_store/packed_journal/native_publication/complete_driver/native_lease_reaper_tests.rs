//! Actual authenticated packed lease expiry, durable grace and hold release.
//! Each backend owns a UUID namespace; no catalog row is fabricated.

use super::*;
use crate::workspace::model::{LeaseState, WorkspaceState};
use crate::workspace::stores::kv_store::packed_journal::registry::PackedNativeLeaseReaperOptions;
use std::time::Duration;

const TTL_NS: u64 = 2_000_000_000;
const GRACE_NS: u64 = 2_000_000_000;
const WAIT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
enum Contract {
    Release,
    InterruptedPrepare,
}

fn options(lease_id: LeaseId, grace_ns: u64) -> PackedNativeLeaseReaperOptions {
    PackedNativeLeaseReaperOptions {
        lease_id,
        grace_ns,
        max_protective_rows: 1000,
        cancel: CancellationToken::new(),
    }
}

fn lease_hold_key(lease_id: LeaseId) -> Vec<u8> {
    format!("packed/native-hold/lease/{lease_id}").into_bytes()
}

fn policy_key(lease_id: LeaseId) -> Vec<u8> {
    format!("packed/native-lease-retirement/{lease_id}").into_bytes()
}

async fn wait_backend_boundary<B: WorkspaceKvBackend>(backend: &B, not_before: i64) {
    tokio::time::timeout(WAIT, async {
        while backend.server_time_ns().await.unwrap() < not_before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual backend lease/grace boundary");
}

async fn actual_lease<B: WorkspaceKvBackend>(
    backend: &B,
    workspace_id: WorkspaceId,
    lease_id: LeaseId,
) -> SnapshotLease {
    decode_open_value(
        &backend
            .get(&hot_lease_key(workspace_id, lease_id))
            .await
            .unwrap()
            .unwrap(),
        OPEN_RECORD_MAX_BYTES,
    )
    .unwrap()
}

async fn lease_contract<B: WorkspaceKvBackend>(backend: Arc<B>, contract: Contract) {
    let (objects, client, snapshot, lower_proof, payload) = packed().await;
    let backend = Arc::new(FinalDelivery::new(backend));
    let store = Arc::new(KvWorkspaceStore::from_arc(backend.clone()));
    let install = request(store.as_ref(), lower_proof).await;
    let binding = store
        .install_packed_lower_binding(install.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: binding.head_epoch,
        ..install.guard
    };
    let budget = SnapshotMountBudget::defaults();
    store
        .configure_packed_reader_pin_budget(budget.clone())
        .unwrap();
    store
        .migrate_native_packed_holds(&budget, 1000, CancellationToken::new())
        .await
        .unwrap();
    let lower = PackedReadonlyMeta::from_snapshot_budget(client, snapshot, 4096, 0, budget.clone())
        .unwrap();

    let native_journal = if matches!(contract, Contract::InterruptedPrepare) {
        // The long live lease makes Prepare independent of the later short
        // expiry fixture. Only Q submission is stopped by the existing hook.
        store
            .renew_lease(RenewLease {
                lease_id: guard.lease_id,
                holder_generation: guard.holder_generation,
                ttl_ns: 300_000_000_000,
            })
            .await
            .unwrap();
        let layers: [LayerRecord; 2] = store
            .load_layer_chain(guard.expected_head_layer_id)
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let journal_id = JournalId::new();
        let successor_head = LayerId::new();
        backend.stop_original_seed_q.store(true, Ordering::SeqCst);
        assert!(
            store
                .clone()
                .begin_packed_native_quiesce(
                    guard.clone(),
                    layers,
                    journal_id,
                    successor_head,
                    budget.clone(),
                )
                .await
                .is_err()
        );
        let control = test_entity_state(backend.as_ref()).await;
        let journal = control.journals.get(&journal_id).unwrap();
        assert_eq!(journal.phase, SealPhase::Prepare);
        assert_eq!(journal.workspace_id, guard.workspace_id);
        assert_eq!(journal.old_head_layer_id, guard.expected_head_layer_id);
        assert_eq!(journal.expected_head_epoch, guard.expected_head_epoch);
        assert_eq!(journal.new_head_layer_id, Some(successor_head));
        let seed = backend
            .get(format!("packed/native-freeze-basis/{journal_id}").as_bytes())
            .await
            .unwrap()
            .unwrap();
        assert!(seed.starts_with(b"NQB3"));
        assert!(seed.len() <= 16 << 10);
        assert!(
            backend
                .scan_prefix(JOURNAL_PREFIX)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(backend.scan_prefix(ACTIVE_PREFIX).await.unwrap().is_empty());
        let workspace: WorkspaceRecord = decode_open_value(
            &backend
                .get(&hot_workspace_key(guard.workspace_id))
                .await
                .unwrap()
                .unwrap(),
            OPEN_RECORD_MAX_BYTES,
        )
        .unwrap();
        assert_eq!(workspace.state, WorkspaceState::Sealing);
        let head: LayerRecord = decode_open_value(
            &backend
                .get(&hot_layer_key(guard.expected_head_layer_id))
                .await
                .unwrap()
                .unwrap(),
            OPEN_RECORD_MAX_BYTES,
        )
        .unwrap();
        assert_eq!(head.state, LayerState::Sealing);
        let recovery: SnapshotRecoveryRecord = decode_open_value(
            &backend
                .get(&open_snapshot_recovery_key(guard.workspace_id))
                .await
                .unwrap()
                .unwrap(),
            OPEN_RECOVERY_MAX_BYTES,
        )
        .unwrap();
        assert_eq!(recovery.workspace_id, guard.workspace_id);
        assert!(recovery.incomplete);
        assert!(
            backend
                .get(format!("packed/native-hold/journal/{journal_id}").as_bytes())
                .await
                .unwrap()
                .is_some()
        );
        Some(journal_id)
    } else {
        None
    };

    let lease = store
        .renew_lease(RenewLease {
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
            ttl_ns: TTL_NS,
        })
        .await
        .unwrap();
    let original_hold = backend
        .get(&lease_hold_key(guard.lease_id))
        .await
        .unwrap()
        .unwrap();
    let observing = store
        .reap_packed_native_lease(budget.clone(), options(guard.lease_id, GRACE_NS))
        .await
        .unwrap();
    assert!(observing.observing && !observing.reaped);
    assert_eq!(
        observing.not_before_ns,
        lease.expires_at_ns + i64::try_from(GRACE_NS).unwrap()
    );
    let observed_policy = backend
        .get(&policy_key(guard.lease_id))
        .await
        .unwrap()
        .unwrap();
    // This call runs before waiting for expiry, avoiding a narrow sampled
    // interval between the generic expiry transaction and the grace boundary.
    let early = store
        .reap_packed_native_lease(budget.clone(), options(guard.lease_id, GRACE_NS))
        .await
        .unwrap();
    assert_eq!(early, observing);
    assert!(matches!(
        store
            .reap_packed_native_lease(budget.clone(), options(guard.lease_id, GRACE_NS / 2))
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(
        backend.get(&policy_key(guard.lease_id)).await.unwrap(),
        Some(observed_policy.clone())
    );
    assert_eq!(
        backend.get(&lease_hold_key(guard.lease_id)).await.unwrap(),
        Some(original_hold.clone())
    );
    wait_backend_boundary(backend.as_ref(), lease.expires_at_ns).await;
    assert_eq!(store.reap_expired_leases().await.unwrap(), 1);
    let expired = actual_lease(backend.as_ref(), guard.workspace_id, guard.lease_id).await;
    assert_eq!(expired.state, LeaseState::Expired);
    assert_eq!(expired.base_revision, lease.base_revision);
    assert_eq!(expired.holder_generation, lease.holder_generation);
    assert_eq!(expired.expires_at_ns, lease.expires_at_ns);
    assert_eq!(
        backend.get(&lease_hold_key(guard.lease_id)).await.unwrap(),
        Some(original_hold)
    );
    wait_backend_boundary(backend.as_ref(), observing.not_before_ns).await;

    if let Some(journal_id) = native_journal {
        let keys = vec![
            hot_journal_key(guard.workspace_id, journal_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            lease_hold_key(guard.lease_id),
            format!("packed/native-hold/journal/{journal_id}").into_bytes(),
            format!("packed/native-freeze-basis/{journal_id}").into_bytes(),
            hot_workspace_key(guard.workspace_id),
            hot_layer_key(guard.expected_head_layer_id),
            open_snapshot_recovery_key(guard.workspace_id),
            packed_current_key(guard.workspace_id),
            packed_history_key(guard.workspace_id, 1),
            policy_key(guard.lease_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            CONTROL_KEY.to_vec(),
        ];
        let retained = backend.get_many_consistent(&keys).await.unwrap();
        assert!(retained.iter().all(Option::is_some));
        assert!(matches!(
            store
                .reap_packed_native_lease(budget.clone(), options(guard.lease_id, GRACE_NS))
                .await,
            Err(WorkspaceError::Busy)
        ));
        assert_eq!(backend.get_many_consistent(&keys).await.unwrap(), retained);
        let journal: SealJournal =
            decode_open_value(retained[0].as_deref().unwrap(), 48 << 10).unwrap();
        assert_eq!(journal.journal_id, journal_id);
        assert_eq!(journal.phase, SealPhase::Prepare);
        assert_eq!(
            actual_lease(backend.as_ref(), guard.workspace_id, guard.lease_id).await,
            expired
        );
        assert!(
            backend
                .scan_prefix(JOURNAL_PREFIX)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(backend.scan_prefix(ACTIVE_PREFIX).await.unwrap().is_empty());
    } else {
        let before_generation = backend.get(PACKED_ROOT_GENERATION_KEY).await.unwrap();
        let workspace_hold_key =
            format!("packed/native-hold/workspace/{}", guard.workspace_id).into_bytes();
        let workspace_hold = backend.get(&workspace_hold_key).await.unwrap();
        assert!(workspace_hold.is_some());
        let reaped = store
            .reap_packed_native_lease(budget.clone(), options(guard.lease_id, GRACE_NS))
            .await
            .unwrap();
        assert!(!reaped.observing && reaped.reaped);
        assert_eq!(reaped.not_before_ns, observing.not_before_ns);
        let released = actual_lease(backend.as_ref(), guard.workspace_id, guard.lease_id).await;
        assert_eq!(released.state, LeaseState::Released);
        assert_eq!(released.base_revision, expired.base_revision);
        assert_eq!(released.holder_generation, expired.holder_generation);
        assert_eq!(released.expires_at_ns, expired.expires_at_ns);
        assert!(
            backend
                .get(&lease_hold_key(guard.lease_id))
                .await
                .unwrap()
                .is_none()
        );
        let generation = backend.get(PACKED_ROOT_GENERATION_KEY).await.unwrap();
        assert_eq!(
            decode::<u64>(generation.as_deref().unwrap()).unwrap(),
            next_packed_root_generation(&before_generation).unwrap()
        );
        let terminal_policy = backend
            .get(&policy_key(guard.lease_id))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(terminal_policy, observed_policy);
        assert_eq!(
            store
                .reap_packed_native_lease(budget.clone(), options(guard.lease_id, GRACE_NS))
                .await
                .unwrap(),
            reaped
        );
        assert_eq!(
            actual_lease(backend.as_ref(), guard.workspace_id, guard.lease_id).await,
            released
        );
        assert_eq!(
            backend.get(PACKED_ROOT_GENERATION_KEY).await.unwrap(),
            generation
        );
        assert_eq!(
            backend.get(&policy_key(guard.lease_id)).await.unwrap(),
            Some(terminal_policy)
        );
        assert_eq!(
            backend.get(&workspace_hold_key).await.unwrap(),
            workspace_hold
        );
    }

    assert_eq!(
        backend
            .get(&packed_current_key(guard.workspace_id))
            .await
            .unwrap(),
        Some(binding.encode().unwrap())
    );
    assert_eq!(
        backend
            .get(&packed_history_key(guard.workspace_id, 1))
            .await
            .unwrap(),
        Some(binding.encode().unwrap())
    );
    assert!(
        objects
            .path()
            .join("objects")
            .join(&binding.binding.manifest.key)
            .is_file()
    );
    // The authenticated anchor still supplies its actual payload after lease
    // cleanup; this maintenance operation grants no object DELETE authority.
    let prepared = lower
        .prepare_unified_read(400, 0, 0, payload.len() as u64)
        .await
        .unwrap()
        .unwrap();
    let mut actual = vec![0; payload.len()];
    execute_unified_into(prepared.fetcher.as_ref(), 0, &prepared.plan, &mut actual)
        .await
        .unwrap();
    assert_eq!(actual, payload);
}

async fn lease_isolated<B: WorkspaceKvBackend>(backend: B, contract: Contract) {
    let backend = Arc::new(backend);
    let worker = backend.clone();
    let result = tokio::spawn(async move { lease_contract(worker, contract).await }).await;
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
        panic!("authenticated native lease reaper task cancelled: {error}");
    }
}

async fn lease_redis(contract: Contract) {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    lease_isolated(
        RedisWorkspaceBackend::connect(&url, &format!("native-lease-reaper-{}", Uuid::new_v4()))
            .await
            .unwrap(),
        contract,
    )
    .await;
}

async fn lease_tikv(contract: Contract) {
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    lease_isolated(
        TiKvWorkspaceBackend::connect(
            endpoints,
            &format!("native-lease-reaper-{}", Uuid::new_v4()),
        )
        .await
        .unwrap(),
        contract,
    )
    .await;
}

#[tokio::test]
#[ignore = "actual Redis UUID namespace; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_authenticated_native_lease_grace_releases_only_lease_hold() {
    lease_redis(Contract::Release).await;
}

#[tokio::test]
#[ignore = "actual TiKV UUID namespace; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_authenticated_native_lease_grace_releases_only_lease_hold() {
    lease_tikv(Contract::Release).await;
}

#[tokio::test]
#[ignore = "actual Redis UUID namespace; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_authenticated_native_lease_prepare_before_pnb_retains_hold() {
    lease_redis(Contract::InterruptedPrepare).await;
}

#[tokio::test]
#[ignore = "actual TiKV UUID namespace; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_authenticated_native_lease_prepare_before_pnb_retains_hold() {
    lease_tikv(Contract::InterruptedPrepare).await;
}
