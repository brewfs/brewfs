//! Actual Redis/TiKV negative admission controls. Objects in this small metadata
//! contract use the existing fixture, not RustFS; this is explicitly not K8s E2E.

use super::*;
use crate::workspace_overlay::stores::binding_tests::{packed, request};
use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
use crate::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;
use uuid::Uuid;

#[derive(Clone, Copy)]
enum Case {
    ReleasedWithoutDrainReceipt,
    ExpiredWithoutDrainReceipt,
}

async fn contract<B: WorkspaceKvBackend + 'static>(
    backend: Arc<B>,
    budget: Arc<V3MountBudget>,
    case: Case,
) {
    let (_objects, _client, _snapshot, lower, _data) = packed().await;
    let store = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    let install = request(store.as_ref(), lower).await;
    let binding = store
        .install_packed_lower_binding(install.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: binding.head_epoch,
        ..install.guard
    };
    match case {
        Case::ReleasedWithoutDrainReceipt => store
            .release_lease(ReleaseLease {
                lease_id: guard.lease_id,
                holder_generation: guard.holder_generation,
            })
            .await
            .unwrap(),
        Case::ExpiredWithoutDrainReceipt => {
            let renewed = store
                .renew_lease(RenewLease {
                    lease_id: guard.lease_id,
                    holder_generation: guard.holder_generation,
                    ttl_ns: 1,
                })
                .await
                .unwrap();
            let wait_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while backend.server_time_ns().await.unwrap() < renewed.expires_at_ns {
                assert!(tokio::time::Instant::now() < wait_deadline);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert_eq!(store.reap_expired_leases().await.unwrap(), 1);
        }
    }
    let expected_state = match case {
        Case::ReleasedWithoutDrainReceipt => LeaseState::Released,
        Case::ExpiredWithoutDrainReceipt => LeaseState::Expired,
    };
    let leases = store.list_leases(guard.workspace_id).await.unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].state, expected_state);
    assert!(!leases.iter().any(|lease| lease.state == LeaseState::Active));
    let result = store
        .admit_clean_packed_source(
            PackedReleasedMountReference {
                guard,
                mount_uid: Uuid::new_v4(),
                pod_uid: Uuid::new_v4(),
            },
            budget,
        )
        .await
        .unwrap();
    assert!(
        matches!(result, PackedCleanAdmission::RequiresRecovery),
        "absence of Active lease must never stand in for original mounted durable drain"
    );
}

async fn isolated<B: WorkspaceKvBackend + 'static>(
    backend: B,
    budget: Arc<V3MountBudget>,
    case: Case,
) {
    let backend = Arc::new(backend);
    let owned = backend.clone();
    let task_budget = budget.clone();
    let result = tokio::spawn(async move { contract(owned, task_budget, case).await }).await;
    // The complete owned namespace is tiny; bounded pages are deleted with exact
    // value checks. Cleanup completes before a test panic is resumed.
    let limits = KvReadLimits {
        max_records: 32,
        max_key_bytes: 1024,
        max_value_bytes: SOURCE_MAX_BYTES,
        max_total_bytes: SOURCE_MAX_BYTES,
        max_response_bytes: 64 << 10,
        max_data_requests: 32,
    };
    loop {
        let rows = backend
            .scan_prefix_with_byte_limits(b"", limits)
            .await
            .unwrap();
        if rows.is_empty() {
            break;
        }
        let checks = rows
            .iter()
            .map(|row| KvCheck {
                key: row.key.clone(),
                expected: Some(row.value.clone()),
            })
            .collect::<Vec<_>>();
        let writes = rows
            .iter()
            .map(|row| KvWrite::Delete {
                key: row.key.clone(),
            })
            .collect::<Vec<_>>();
        assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
    }
    backend.shutdown_metadata_backend().await.unwrap();
    budget.close();
    assert_eq!(
        budget.state().used,
        [0; 8],
        "canonical metadata owners did not drain"
    );
    if let Err(error) = result {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("owned clean-source contract was cancelled");
    }
}

async fn redis(case: Case) {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let budget = V3MountBudget::from_env().unwrap();
    isolated(
        RedisWorkspaceBackend::connect(&url, &format!("packed-clean-source-{}", Uuid::new_v4()))
            .await
            .unwrap(),
        budget,
        case,
    )
    .await;
}

async fn tikv(case: Case) {
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    let budget = V3MountBudget::from_env().unwrap();
    isolated(
        TiKvWorkspaceBackend::connect_with_budget(
            endpoints,
            &format!("packed-clean-source-{}", Uuid::new_v4()),
            budget.clone(),
        )
        .await
        .unwrap(),
        budget,
        case,
    )
    .await;
}

#[tokio::test]
#[ignore = "actual Redis UUID namespace; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_packed_clean_source_released_without_original_drain_receipt() {
    redis(Case::ReleasedWithoutDrainReceipt).await;
}

#[tokio::test]
#[ignore = "actual TiKV UUID namespace; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_packed_clean_source_released_without_original_drain_receipt() {
    tikv(Case::ReleasedWithoutDrainReceipt).await;
}

#[tokio::test]
#[ignore = "actual Redis UUID namespace; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_packed_clean_source_expired_without_original_drain_receipt() {
    redis(Case::ExpiredWithoutDrainReceipt).await;
}

#[tokio::test]
#[ignore = "actual TiKV UUID namespace; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_packed_clean_source_expired_without_original_drain_receipt() {
    tikv(Case::ExpiredWithoutDrainReceipt).await;
}

#[test]
fn original_source_authenticates_historical_hot_release_without_ignoring_live_leases() {
    let workspace = WorkspaceId::new();
    let base = BaseRevision {
        layer_id: LayerId::new(),
        sealed_version: 1,
        root_hash: [1; 32],
    };
    let catalog = SnapshotLease {
        lease_id: LeaseId::new(),
        workspace_id: workspace,
        base_revision: base.clone(),
        holder_generation: 1,
        writable: true,
        state: LeaseState::Active,
        expires_at_ns: 30,
        created_at_ns: 10,
        updated_at_ns: 10,
    };
    let source = SnapshotLease {
        lease_id: LeaseId::new(),
        holder_generation: 2,
        state: LeaseState::Released,
        created_at_ns: 20,
        updated_at_ns: 40,
        ..catalog.clone()
    };
    let mut hot = catalog.clone();
    hot.state = LeaseState::Released;
    hot.updated_at_ns = 15;
    assert!(!clean_source_other_lease_invalidates(&catalog, Some(&hot), &source).unwrap());
    assert!(matches!(
        clean_source_other_lease_invalidates(&catalog, Some(&catalog), &source),
        Err(WorkspaceError::Busy)
    ));
    assert!(matches!(
        clean_source_other_lease_invalidates(&catalog, None, &source),
        Err(WorkspaceError::Fenced)
    ));
    let mut foreign = hot.clone();
    foreign.base_revision.root_hash[0] ^= 1;
    assert!(clean_source_other_lease_invalidates(&catalog, Some(&foreign), &source).is_err());
    let mut regressed = hot.clone();
    regressed.updated_at_ns = 9;
    assert!(clean_source_other_lease_invalidates(&catalog, Some(&regressed), &source).is_err());
    let mut later = catalog.clone();
    later.holder_generation = 3;
    later.created_at_ns = 25;
    later.updated_at_ns = 25;
    let mut expired = later.clone();
    expired.state = LeaseState::Expired;
    assert!(clean_source_other_lease_invalidates(&later, Some(&expired), &source).unwrap());
}

#[test]
fn original_source_routes_reject_control_row_alias_before_source_exclusion() {
    let source = LeaseId::new();
    let row = SnapshotLease {
        lease_id: source,
        workspace_id: WorkspaceId::new(),
        base_revision: BaseRevision {
            layer_id: LayerId::new(),
            sealed_version: 1,
            root_hash: [1; 32],
        },
        holder_generation: 2,
        writable: true,
        state: LeaseState::Released,
        expires_at_ns: 30,
        created_at_ns: 20,
        updated_at_ns: 40,
    };
    assert!(!clean_source_routes_other_lease(source, &row, source).unwrap());
    assert!(
        matches!(
            clean_source_routes_other_lease(LeaseId::new(), &row, source),
            Err(WorkspaceError::Fenced)
        ),
        "malformed alias was skipped as if it were the source"
    );
    let other = SnapshotLease {
        lease_id: LeaseId::new(),
        ..row
    };
    assert!(clean_source_routes_other_lease(other.lease_id, &other, source).unwrap());
}
