//! Shared real backend gate for the v3 binding CAS substrate.
use super::kv_backend::{KvCheck, KvWrite, WorkspaceKvBackend};
use super::redis::RedisWorkspaceBackend;
use super::tikv::TiKvWorkspaceBackend;
use crate::workspace_overlay::error::WorkspaceError;

async fn contract<B: WorkspaceKvBackend>(backend: B) {
    let key = b"g10c-clock-cas".to_vec();
    let checks = [KvCheck {
        key: key.clone(),
        expected: None,
    }];
    let writes = [KvWrite::Put {
        key: key.clone(),
        value: b"nonzero-new-value".to_vec(),
    }];
    let now = backend.server_time_ns().await.unwrap();
    assert!(matches!(
        backend
            .compare_and_swap_before(&checks, &writes, now - 1)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(backend.get(&key).await.unwrap(), None);
    assert!(
        backend
            .compare_and_swap_before(&checks, &writes, now.checked_add(30_000_000_000).unwrap())
            .await
            .unwrap()
    );
    assert!(
        !backend
            .compare_and_swap_before(&checks, &[], now.checked_add(30_000_000_000).unwrap())
            .await
            .unwrap()
    );
    let (values, observed) = backend
        .get_many_consistent_with_time(std::slice::from_ref(&key))
        .await
        .unwrap();
    assert_eq!(values, [Some(b"nonzero-new-value".to_vec())]);
    assert!(observed >= now);
    let exact = [KvCheck {
        key: key.clone(),
        expected: values[0].clone(),
    }];
    assert!(
        backend
            .compare_and_swap(&exact, &[KvWrite::Delete { key }])
            .await
            .unwrap()
    );
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_clock_cas_rejects_expired_deadline_without_writes() {
    let backend = RedisWorkspaceBackend::connect(
        &std::env::var("BREWFS_TEST_REDIS_URL").unwrap(),
        &format!("g10c-clock-{}", uuid::Uuid::new_v4().simple()),
    )
    .await
    .unwrap();
    contract(backend).await;
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_clock_cas_rejects_expired_deadline_without_writes() {
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    let backend = TiKvWorkspaceBackend::connect(
        endpoints,
        &format!("g10c-clock-{}", uuid::Uuid::new_v4().simple()),
    )
    .await
    .unwrap();
    contract(backend).await;
}
