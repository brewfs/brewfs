//! Real backend sidecar contracts. These do not exercise mount or seal recovery.

use std::time::Duration;

use super::*;
use crate::workspace_overlay::stores::kv_store::{V3OpenState, V3OpenToken};

fn only_open_winner(
    first: Result<V3OpenToken, WorkspaceError>,
    second: Result<V3OpenToken, WorkspaceError>,
) -> V3OpenToken {
    match (first, second) {
        (Ok(token), Err(WorkspaceError::Busy)) | (Err(WorkspaceError::Busy), Ok(token)) => token,
        results => panic!("exactly one independent owner must win: {results:?}"),
    }
}

async fn replace_binding_values<B: WorkspaceKvBackend>(
    backend: &B,
    keys: &[Vec<u8>],
    expected: &[Option<Vec<u8>>],
    replacement: &[Option<Vec<u8>>],
) {
    assert_eq!(keys.len(), expected.len());
    assert_eq!(keys.len(), replacement.len());
    let checks: Vec<_> = keys
        .iter()
        .zip(expected)
        .map(|(key, value)| KvCheck {
            key: key.clone(),
            expected: value.clone(),
        })
        .collect();
    let writes: Vec<_> = keys
        .iter()
        .zip(replacement)
        .map(|(key, value)| match value {
            Some(value) => KvWrite::Put {
                key: key.clone(),
                value: value.clone(),
            },
            None => KvWrite::Delete { key: key.clone() },
        })
        .collect();
    assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
}

async fn assert_stale_token<B: WorkspaceKvBackend>(
    store: &KvWorkspaceStore<B>,
    backend: &B,
    token: &V3OpenToken,
    sidecar_key: &[u8],
) {
    let before = backend.get(sidecar_key).await.unwrap();
    for operation in 0..3 {
        let result = match operation {
            0 => store.mark_workspace_v3_ready(token).await.map(|_| ()),
            1 => store
                .renew_workspace_v3(token, Duration::from_secs(30))
                .await
                .map(|_| ()),
            _ => store.close_workspace_v3(token).await,
        };
        assert!(
            matches!(result, Err(WorkspaceError::Fenced)),
            "stale sidecar operation {operation}: {result:?}"
        );
        assert_eq!(backend.get(sidecar_key).await.unwrap(), before);
    }
}

async fn real_open_contract<B: WorkspaceKvBackend + Clone>(first: B, second: B) {
    // The callers establish two connections to the same isolated namespace.
    let a = KvWorkspaceStore::new(first.clone());
    let b = KvWorkspaceStore::new(second.clone());
    let (_objects, _client, _snapshot, proof, _payload) = packed().await;
    let request = request(&a, proof).await;
    let binding = a
        .install_packed_lower_binding(request.clone())
        .await
        .unwrap();
    let workspace_id = request.guard.workspace_id;
    let workspace = a.load_workspace(workspace_id).await.unwrap();
    assert_eq!(b.load_workspace(workspace_id).await.unwrap(), workspace);
    let sidecar_key = format!("open/v3/{workspace_id}").into_bytes();
    assert_eq!(first.get(&sidecar_key).await.unwrap(), None);

    let before = first.server_time_ns().await.unwrap();
    let (left, right) = tokio::join!(
        a.open_workspace_v3(workspace_id, "real-owner-a", Duration::from_secs(30)),
        b.open_workspace_v3(workspace_id, "real-owner-b", Duration::from_secs(30)),
    );
    let winner = only_open_winner(left, right);
    let after = second.server_time_ns().await.unwrap();
    assert!(after >= before);
    assert!(winner.expires_at_ns > after);
    assert_eq!(winner.generation, 1);
    assert_eq!(winner.state, V3OpenState::Ready);
    assert!(!winner.recovery_required);

    let reopened = b
        .open_workspace_v3(workspace_id, &winner.owner_id, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(reopened.generation, winner.generation);
    assert_eq!(reopened.owner_id, winner.owner_id);
    let ready = a.mark_workspace_v3_ready(&reopened).await.unwrap();
    assert_eq!(ready.state, V3OpenState::Ready);
    assert!(!ready.recovery_required);

    let binding_keys = [
        format!("packed/v3/claim/{workspace_id}").into_bytes(),
        format!("packed/v3/current/{workspace_id}").into_bytes(),
        format!(
            "packed/v3/history/{workspace_id}/{:016x}",
            binding.binding.binding_version
        )
        .into_bytes(),
    ];
    let original = first.get_many_consistent(&binding_keys).await.unwrap();
    assert_eq!(original[0], Some(b"PWC3".to_vec()));
    assert_eq!(original[1], Some(binding.encode().unwrap()));
    assert_eq!(original[2], original[1]);
    for (name, replacements) in [
        ("missing claim", vec![(0, None)]),
        ("bad claim", vec![(0, Some(b"bad-claim".to_vec()))]),
        ("missing current", vec![(1, None)]),
        ("bad current", vec![(1, Some(b"bad-current".to_vec()))]),
        ("missing history", vec![(2, None)]),
        ("bad history", vec![(2, Some(b"bad-history".to_vec()))]),
        ("orphan first history", vec![(0, None), (1, None)]),
    ] {
        let mut corrupt = original.clone();
        for (index, value) in replacements {
            corrupt[index] = value;
        }
        replace_binding_values(&first, &binding_keys, &original, &corrupt).await;
        let sidecar_before = second.get(&sidecar_key).await.unwrap();
        let open = a
            .open_workspace_v3(workspace_id, &ready.owner_id, Duration::from_secs(30))
            .await;
        assert!(
            matches!(open, Err(WorkspaceError::CorruptMetadata(_))),
            "{name} was accepted by open: {open:?}"
        );
        assert_eq!(second.get(&sidecar_key).await.unwrap(), sidecar_before);
        let mark_ready = b.mark_workspace_v3_ready(&ready).await;
        assert!(
            matches!(mark_ready, Err(WorkspaceError::CorruptMetadata(_))),
            "{name} was accepted by ready: {mark_ready:?}"
        );
        assert_eq!(second.get(&sidecar_key).await.unwrap(), sidecar_before);
        assert_eq!(
            second.get_many_consistent(&binding_keys).await.unwrap(),
            corrupt
        );
        replace_binding_values(&second, &binding_keys, &corrupt, &original).await;
    }

    let renewed = b
        .renew_workspace_v3(&ready, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(renewed.generation, ready.generation);
    assert_eq!(renewed.owner_id, ready.owner_id);
    assert_eq!(renewed.state, V3OpenState::Ready);
    assert!(renewed.expires_at_ns > first.server_time_ns().await.unwrap());
    // Client time only bounds the test; expiry is proved by the backend clock.
    tokio::time::timeout(Duration::from_secs(15), async {
        while second.server_time_ns().await.unwrap() < renewed.expires_at_ns {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("backend clock did not reach the actual sidecar deadline");
    assert_stale_token(&a, &first, &renewed, &sidecar_key).await;

    let (left, right) = tokio::join!(
        a.open_workspace_v3(workspace_id, "real-takeover-a", Duration::from_secs(30)),
        b.open_workspace_v3(workspace_id, "real-takeover-b", Duration::from_secs(30)),
    );
    let takeover = only_open_winner(left, right);
    assert_eq!(takeover.generation, renewed.generation + 1);
    assert_eq!(takeover.state, V3OpenState::Ready);
    assert_stale_token(&b, &second, &ready, &sidecar_key).await;
    b.close_workspace_v3(&takeover).await.unwrap();
    assert_stale_token(&a, &first, &takeover, &sidecar_key).await;
    let final_owner = a
        .open_workspace_v3(workspace_id, "real-final", Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(final_owner.generation, takeover.generation + 1);
    b.close_workspace_v3(&final_owner).await.unwrap();
    a.release_lease(ReleaseLease {
        lease_id: request.guard.lease_id,
        holder_generation: request.guard.holder_generation,
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL"]
async fn public_real_redis_v3_open_contract() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
    let namespace = format!("v3-open-{}", Uuid::new_v4().simple());
    let first = RedisWorkspaceBackend::connect(&url, &namespace)
        .await
        .unwrap();
    let second = RedisWorkspaceBackend::connect(&url, &namespace)
        .await
        .unwrap();
    real_open_contract(first, second).await;
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn public_real_tikv_v3_open_contract() {
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    let namespace = format!("v3-open-{}", Uuid::new_v4().simple());
    let first = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
        .await
        .unwrap();
    let second = TiKvWorkspaceBackend::connect(endpoints, &namespace)
        .await
        .unwrap();
    real_open_contract(first, second).await;
}
