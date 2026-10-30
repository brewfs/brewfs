use super::tests::{Backend, Objects};
use super::*;

fn layer(number: u128) -> LayerRecord {
    LayerRecord {
        layer_id: LayerId::from_uuid(uuid::Uuid::from_u128(number)),
        parent_layer_id: None,
        state: LayerState::Deleting,
        schema_version: WORKSPACE_SCHEMA_VERSION,
        sealed_version: None,
        delta_digest: None,
        root_hash: None,
        depth: 1,
        owner_workspace_id: None,
        next_sequence: 1,
        owned_slice_count: 0,
        owned_bytes: 0,
        created_at_ns: 1,
        sealed_at_ns: None,
    }
}
fn policy() -> PackedGcPolicy {
    PackedGcPolicy {
        lease_ttl_seconds: 30,
        grace_seconds: 60,
        max_scans: 1,
        max_operations: 1,
        max_protective_rows: 64,
    }
}
fn scope(backend: Arc<Backend>, objects: Objects) -> Arc<dyn PackedGcAdmin> {
    let budget = V3MountBudget::defaults();
    let store =
        Arc::new(KvWorkspaceStore::from_arc(backend).with_packed_reader_pin_budget(budget.clone()));
    Arc::new(Handle {
        runtime: Arc::new(Runtime {
            store,
            client: ObjectClient::new(objects),
            budget,
            layout: ChunkLayout {
                chunk_size: 4 << 20,
                block_size: 1 << 20,
            },
            serial: tokio::sync::Mutex::new(()),
            closed: AtomicBool::new(false),
        }),
    })
}
async fn tick(
    backend: Arc<Backend>,
    objects: Objects,
    cursor: PackedGcCursor,
) -> PackedGcTickReport {
    // Each operator slice constructs a fresh admin owner. Only the strict
    // encoded route survives; neither a previous permit nor proof is reused.
    let admin = scope(backend, objects);
    let report = admin
        .tick(PackedGcTickRequest {
            policy: policy(),
            cursor,
            cancel: CancellationToken::new(),
        })
        .await
        .unwrap();
    admin.shutdown().await.unwrap();
    report
}

#[tokio::test]
async fn native_failed_proof_preserves_deep_layer_cursor_across_fresh_scopes() {
    let backend = Arc::new(Backend::default());
    let objects = Objects::default();
    let mut keys = Vec::new();
    for number in 1..=3 {
        let row = layer(number);
        let key = hot_layer_key(row.layer_id);
        backend
            .rows
            .lock()
            .await
            .insert(key.clone(), encode(&row).unwrap());
        keys.push(key);
    }
    // This backend intentionally cannot authenticate a complete deletion
    // basis. Failure must dispatch no object DELETE and must not pin routing
    // forever to the first layer when a later catalog owner starts.
    let mut cursor = PackedGcCursor {
        tier: Tier::Native,
        after: None,
    };
    for key in keys {
        let report = tick(backend.clone(), objects.clone(), cursor).await;
        assert_eq!(report.scanned, 1);
        assert_eq!(report.attempted, 1);
        assert_eq!(report.deferred, 1);
        assert_eq!(report.native_deleted_layers, 0);
        assert_eq!(report.deleted_objects, 0);
        assert!(matches!(report.next_cursor.tier, Tier::Native));
        assert_eq!(report.next_cursor.after, Some(key));
        cursor = PackedGcCursor::decode(&report.next_cursor.encode().unwrap()).unwrap();
    }
    let report = tick(backend, objects.clone(), cursor).await;
    assert!(matches!(report.next_cursor.tier, Tier::History));
    assert_eq!(report.scanned, 1);
    assert_eq!(report.attempted, 0);
    assert_eq!(objects.deletes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn workspace_keyset_skips_active_rows_and_retains_deleting_route() {
    let backend = Arc::new(Backend::default());
    let objects = Objects::default();
    let mut expected = Vec::new();
    for (number, state) in [(1, WorkspaceState::Active), (2, WorkspaceState::Deleting)] {
        let row = WorkspaceRecord {
            upper_bucket: None,
            workspace_id: WorkspaceId::from_uuid(uuid::Uuid::from_u128(number)),
            head_layer_id: LayerId::new(),
            head_epoch: 1,
            fork_base: None,
            active_lease: None,
            owner_id: None,
            state,
            created_at_ns: 1,
            updated_at_ns: 2,
        };
        let key = hot_workspace_key(row.workspace_id);
        backend
            .rows
            .lock()
            .await
            .insert(key.clone(), encode(&row).unwrap());
        expected.push(key);
    }
    let first = tick(
        backend.clone(),
        objects.clone(),
        PackedGcCursor {
            tier: Tier::NativeWorkspace,
            after: None,
        },
    )
    .await;
    assert_eq!(first.attempted, 0);
    assert_eq!(first.next_cursor.after, Some(expected[0].clone()));
    let second = tick(
        backend.clone(),
        objects.clone(),
        PackedGcCursor::decode(&first.next_cursor.encode().unwrap()).unwrap(),
    )
    .await;
    assert_eq!(second.attempted, 1);
    assert_eq!(second.deferred, 1);
    assert_eq!(second.next_cursor.after, Some(expected[1].clone()));
    let end = tick(backend, objects.clone(), second.next_cursor).await;
    assert!(matches!(end.next_cursor.tier, Tier::Native));
    assert_eq!(objects.deletes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn native_route_requires_exact_canonical_layer_identity() {
    let backend = Arc::new(Backend::default());
    let objects = Objects::default();
    let wrong_key = hot_layer_key(layer(1).layer_id);
    backend
        .rows
        .lock()
        .await
        .insert(wrong_key.clone(), encode(&layer(2)).unwrap());
    backend
        .rows
        .lock()
        .await
        .insert(hot_layer_key(layer(3).layer_id), encode(&layer(3)).unwrap());
    let refused = tick(
        backend.clone(),
        objects.clone(),
        PackedGcCursor {
            tier: Tier::Native,
            after: None,
        },
    )
    .await;
    assert_eq!(refused.attempted, 0);
    assert_eq!(refused.deferred, 1);
    assert_eq!(refused.native_deleted_layers, 0);
    assert_eq!(refused.next_cursor.after, Some(wrong_key));
    let later = tick(
        backend,
        objects.clone(),
        PackedGcCursor::decode(&refused.next_cursor.encode().unwrap()).unwrap(),
    )
    .await;
    assert_eq!(later.attempted, 1);
    assert_eq!(later.deferred, 1);
    assert_eq!(
        later.next_cursor.after,
        Some(hot_layer_key(layer(3).layer_id))
    );
    assert_eq!(objects.deletes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn final_empty_native_page_ends_cycle_even_with_unused_quota() {
    let backend = Arc::new(Backend::default());
    let objects = Objects::default();
    let admin = scope(backend, objects);
    let mut quota = policy();
    quota.max_scans = 64;
    quota.max_operations = 16;
    let report = admin
        .tick(PackedGcTickRequest {
            policy: quota,
            cursor: PackedGcCursor {
                tier: Tier::Native,
                after: None,
            },
            cancel: CancellationToken::new(),
        })
        .await
        .unwrap();
    assert_eq!(report.scanned, 1);
    assert_eq!(report.attempted, 0);
    assert!(matches!(report.next_cursor.tier, Tier::History));
    assert!(report.next_cursor.after.is_none());
    admin.shutdown().await.unwrap();
}

#[test]
fn workspace_and_layer_cursor_namespaces_cannot_be_interchanged() {
    let key = hot_workspace_key(WorkspaceId::new());
    assert!(
        PackedGcCursor {
            tier: Tier::Native,
            after: Some(key.clone())
        }
        .encode()
        .is_err()
    );
    assert!(
        PackedGcCursor {
            tier: Tier::NativeWorkspace,
            after: Some(key)
        }
        .encode()
        .is_ok()
    );
    let key = hot_layer_key(LayerId::new());
    assert!(
        PackedGcCursor {
            tier: Tier::NativeWorkspace,
            after: Some(key.clone())
        }
        .encode()
        .is_err()
    );
    assert!(
        PackedGcCursor {
            tier: Tier::Native,
            after: Some(key)
        }
        .encode()
        .is_ok()
    );
}
