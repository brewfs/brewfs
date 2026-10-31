//! Actual shared-service contracts for the Manager substrate. Manager HTTP
//! recovery tests separately exercise durable operation admission and replay.
use std::sync::Arc;

use uuid::Uuid;

use super::kv_backend::{KvWrite, WorkspaceKvBackend};
use super::kv_store::KvWorkspaceStore;
use super::manager_guard::{MANAGER_CONTROLLER_KEY, ManagerGuard};
use super::redis::RedisWorkspaceBackend;
use super::tikv::TiKvWorkspaceBackend;
use crate::workspace_overlay::catalog::{CreateVolumeRoot, WorkspaceStore};
use crate::workspace_overlay::ids::{LayerId, WorkspaceId};

async fn contract(first: Arc<dyn WorkspaceKvBackend>, peer: Arc<dyn WorkspaceKvBackend>) {
    let foreign = KvWrite::Put {
        key: b"foreign/orphan".to_vec(),
        value: b"retain".to_vec(),
    };
    assert!(
        peer.compare_and_swap(&[], std::slice::from_ref(&foreign))
            .await
            .unwrap()
    );
    let journal = Uuid::new_v4();
    assert!(
        ManagerGuard::acquire(first.clone(), journal, Uuid::new_v4(), 60_000_000_000, true)
            .await
            .is_err()
    );
    assert!(peer.get(MANAGER_CONTROLLER_KEY).await.unwrap().is_none());
    assert_eq!(
        peer.get(b"foreign/orphan").await.unwrap(),
        Some(b"retain".to_vec())
    );
    assert!(
        peer.compare_and_swap(
            &[],
            &[KvWrite::Delete {
                key: b"foreign/orphan".to_vec()
            }]
        )
        .await
        .unwrap()
    );

    let guard = ManagerGuard::acquire(first.clone(), journal, Uuid::new_v4(), 60_000_000_000, true)
        .await
        .unwrap();
    assert!(
        ManagerGuard::acquire(
            peer.clone(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            60_000_000_000,
            false
        )
        .await
        .is_err()
    );
    assert!(
        ManagerGuard::acquire(peer.clone(), journal, Uuid::new_v4(), 60_000_000_000, false)
            .await
            .is_err()
    );
    let store = KvWorkspaceStore::new(guard.guarded_backend());
    let request = CreateVolumeRoot {
        volume_format: "workspace-v1".into(),
        schema_version: 1,
        volume_id: Uuid::new_v4(),
        workspace_id: WorkspaceId::new(),
        root_layer_id: LayerId::new(),
        writable_layer_id: LayerId::new(),
        owner_id: Some("manager-contract".into()),
        upper_bucket: None,
    };
    // A non-Manager pre-init writer cannot hide an orphan behind the owner row.
    assert!(peer.compare_and_swap(&[], &[foreign]).await.unwrap());
    assert!(
        store
            .create_volume_root_in_empty_namespace(request.clone())
            .await
            .is_err()
    );
    assert!(peer.get(b"control").await.unwrap().is_none());
    assert!(
        peer.compare_and_swap(
            &[],
            &[KvWrite::Delete {
                key: b"foreign/orphan".to_vec()
            }]
        )
        .await
        .unwrap()
    );
    let root = store
        .create_volume_root_in_empty_namespace(request.clone())
        .await
        .unwrap();
    assert_eq!(root.workspace_id, request.workspace_id);
    assert_eq!(
        store.load_volume_header().await.unwrap().unwrap().volume_id,
        request.volume_id
    );
    assert!(
        store
            .create_volume_root_in_empty_namespace(request)
            .await
            .is_err()
    );
    guard.renew().await.unwrap();
    guard.release().await.unwrap();
    let reopened =
        ManagerGuard::acquire_existing(peer.clone(), journal, Uuid::new_v4(), 60_000_000_000)
            .await
            .unwrap();
    assert!(
        guard
            .guarded_backend()
            .compare_and_swap(
                &[],
                &[KvWrite::Put {
                    key: b"stale-effect".to_vec(),
                    value: vec![1]
                }]
            )
            .await
            .is_err()
    );
    assert!(peer.get(b"stale-effect").await.unwrap().is_none());
    assert_eq!(
        KvWorkspaceStore::new(reopened.guarded_backend())
            .load_workspace(root.workspace_id)
            .await
            .unwrap(),
        root
    );
    reopened.release().await.unwrap();
    assert!(
        ManagerGuard::acquire(
            peer.clone(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            60_000_000_000,
            false
        )
        .await
        .is_err()
    );

    // Only the unique UUID test namespace is removed, after both sessions stop.
    let writes = peer
        .scan_prefix(b"")
        .await
        .unwrap()
        .into_iter()
        .map(|row| KvWrite::Delete { key: row.key })
        .collect::<Vec<_>>();
    assert!(peer.compare_and_swap(&[], &writes).await.unwrap());
    first.shutdown_metadata_backend().await.unwrap();
    peer.shutdown_metadata_backend().await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_REDIS_URL; real Manager ownership and strict root bootstrap"]
async fn real_redis_manager_ownership_and_empty_root_contract() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
    let namespace = format!("manager-guard-{}", Uuid::new_v4().simple());
    let first = RedisWorkspaceBackend::connect_read_only(&url, &namespace)
        .await
        .unwrap();
    let peer = RedisWorkspaceBackend::connect_read_only(&url, &namespace)
        .await
        .unwrap();
    contract(Arc::new(first), Arc::new(peer)).await;
    let mut wire = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("DEL")
        .arg(format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/__index/keys"))
        .arg(format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/__index/ready"))
        .query_async::<()>(&mut wire)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_TIKV_PD_ENDPOINTS; real Manager ownership and strict root bootstrap"]
async fn real_tikv_manager_ownership_and_empty_root_contract() {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let namespace = format!("manager-guard-{}", Uuid::new_v4().simple());
    let first = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
        .await
        .unwrap();
    let peer = TiKvWorkspaceBackend::connect(endpoints, &namespace)
        .await
        .unwrap();
    contract(Arc::new(first), Arc::new(peer)).await;
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_REDIS_URL; raw unindexed foreign keys"]
async fn real_redis_manager_bootstrap_rejects_unindexed_rows_without_mutation() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
    let namespace = format!("manager-unindexed-{}", Uuid::new_v4().simple());
    let raw = Arc::new(
        RedisWorkspaceBackend::connect_read_only(&url, &namespace)
            .await
            .unwrap(),
    );
    let mut wire = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let key = format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/packed/v3/history/foreign");
    redis::cmd("SET")
        .arg(&key)
        .arg("untouched")
        .query_async::<()>(&mut wire)
        .await
        .unwrap();
    assert!(raw.has_raw_prefix(b"packed/v3/history/").await.unwrap());
    assert!(raw.validate_manager_catalog_index().await.is_err());
    assert!(
        ManagerGuard::acquire(
            raw.clone(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            60_000_000_000,
            true
        )
        .await
        .is_err()
    );
    assert!(raw.get(MANAGER_CONTROLLER_KEY).await.unwrap().is_none());
    let value: String = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut wire)
        .await
        .unwrap();
    assert_eq!(value, "untouched");
    redis::cmd("DEL")
        .arg(&key)
        .query_async::<()>(&mut wire)
        .await
        .unwrap();
}

async fn native_locator_blocks_bootstrap(
    raw: Arc<dyn WorkspaceKvBackend>,
    guard: Option<Arc<ManagerGuard>>,
) {
    if let Some(guard) = guard {
        let store = KvWorkspaceStore::new(guard.guarded_backend());
        assert!(
            store
                .create_volume_root_in_empty_namespace(CreateVolumeRoot {
                    volume_format: "workspace-v1".into(),
                    schema_version: 1,
                    volume_id: Uuid::new_v4(),
                    workspace_id: WorkspaceId::new(),
                    root_layer_id: LayerId::new(),
                    writable_layer_id: LayerId::new(),
                    owner_id: None,
                    upper_bucket: None,
                })
                .await
                .is_err()
        );
        guard.release().await.unwrap();
    } else {
        assert!(
            ManagerGuard::acquire(
                raw.clone(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                60_000_000_000,
                true
            )
            .await
            .is_err()
        );
        assert!(raw.get(MANAGER_CONTROLLER_KEY).await.unwrap().is_none());
    }
    assert!(raw.get(b"control").await.unwrap().is_none());
    assert!(raw.scan_prefix(b"workspace/").await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_REDIS_URL; native locator inserted after preflight"]
async fn real_redis_manager_bootstrap_rechecks_native_locator_atomically() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
    let mut wire = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    for preclaimed in [false, true] {
        let namespace = format!("manager-native-race-{}", Uuid::new_v4().simple());
        let raw = Arc::new(
            RedisWorkspaceBackend::connect_read_only(&url, &namespace)
                .await
                .unwrap(),
        );
        let guard = if preclaimed {
            Some(
                ManagerGuard::acquire(
                    raw.clone(),
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    60_000_000_000,
                    true,
                )
                .await
                .unwrap(),
            )
        } else {
            None
        };
        assert!(raw.load_manager_native_locator().await.unwrap().is_none());
        let locator = format!("nb2/header/{namespace}");
        redis::cmd("SET")
            .arg(&locator)
            .arg("foreign-native-header")
            .query_async::<()>(&mut wire)
            .await
            .unwrap();
        native_locator_blocks_bootstrap(raw.clone(), guard).await;
        assert_eq!(
            raw.load_manager_native_locator().await.unwrap(),
            Some(b"foreign-native-header".to_vec())
        );
        redis::cmd("DEL")
            .arg(&locator)
            .arg(format!(
                "{{brewfs-ws-v1}}:{namespace}:ws:v1/manager/controller"
            ))
            .arg(format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/__index/keys"))
            .arg(format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/__index/ready"))
            .query_async::<()>(&mut wire)
            .await
            .unwrap();
        raw.shutdown_metadata_backend().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_TIKV_PD_ENDPOINTS; native locator inserted after preflight"]
async fn real_tikv_manager_bootstrap_rechecks_native_locator_atomically() {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let wire = tikv_client::TransactionClient::new(endpoints.clone())
        .await
        .unwrap();
    // Both sides of the absolute locator's byte order exercise the lock plan.
    for (preclaimed, prefix) in [(false, "a"), (true, "z")] {
        let namespace = format!("{prefix}-manager-native-race-{}", Uuid::new_v4().simple());
        let raw = Arc::new(
            TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
                .await
                .unwrap(),
        );
        let guard = if preclaimed {
            Some(
                ManagerGuard::acquire(
                    raw.clone(),
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                    60_000_000_000,
                    true,
                )
                .await
                .unwrap(),
            )
        } else {
            None
        };
        assert!(raw.load_manager_native_locator().await.unwrap().is_none());
        let locator = format!("nb2/header/{namespace}").into_bytes();
        let mut transaction = wire.begin_pessimistic().await.unwrap();
        transaction
            .put(locator.clone(), b"foreign-native-header".to_vec())
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        native_locator_blocks_bootstrap(raw.clone(), guard).await;
        assert_eq!(
            raw.load_manager_native_locator().await.unwrap(),
            Some(b"foreign-native-header".to_vec())
        );
        let mut transaction = wire.begin_pessimistic().await.unwrap();
        transaction.delete(locator).await.unwrap();
        transaction
            .delete(format!("{namespace}/ws:v1/manager/controller").into_bytes())
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        raw.shutdown_metadata_backend().await.unwrap();
    }
}
