//! Actual catalog reservations and GuardedBlocks share the production collector.
use super::super::tests::Objects;
use super::*;
use crate::workspace_overlay::catalog::{CreateVolumeRoot, RecordOrphanSlice, WorkspaceStore};
use crate::workspace_overlay::stores::kv_store::native_reverse;
use crate::workspace_overlay::stores::kv_store::native_slice_deletion::slice_deletion_key;
use crate::workspace_overlay::stores::kv_store::packed_journal::tests::JournalMemoryBackend;
use uuid::Uuid;

const SLICE: u64 = 9001;
const BLOCK_BYTES: u32 = 1 << 20;
const END: u64 = 65 * BLOCK_BYTES as u64;

fn layer(id: u128) -> LayerId {
    LayerId::from_uuid(Uuid::from_u128(id))
}

async fn catalog() -> (
    Arc<JournalMemoryBackend>,
    Arc<KvWorkspaceStore<JournalMemoryBackend>>,
    Arc<V3MountBudget>,
) {
    let backend = Arc::new(JournalMemoryBackend::at_time(100_000_000_000));
    let budget = V3MountBudget::defaults();
    let store = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    store.initialize_workspace_schema().await.unwrap();
    store
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: VOLUME_FORMAT.into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::from_u128(9002),
            workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(9003)),
            root_layer_id: layer(9004),
            writable_layer_id: layer(9005),
            owner_id: None,
        })
        .await
        .unwrap();
    orphan(&store, layer(9006), SLICE).await;
    (backend, store, budget)
}

async fn orphan(store: &KvWorkspaceStore<JournalMemoryBackend>, target: LayerId, sid: u64) {
    store
        .record_orphan_slice(RecordOrphanSlice {
            orphan_layer_id: target,
            slice_id: sid,
            slice_end: END,
        })
        .await
        .unwrap();
}

async fn tick(
    store: Arc<KvWorkspaceStore<JournalMemoryBackend>>,
    budget: Arc<V3MountBudget>,
    objects: Objects,
) -> Result<u64, WorkspaceError> {
    collect_one(
        store,
        ObjectClient::new(objects),
        budget,
        ChunkLayout {
            chunk_size: 4 << 20,
            block_size: BLOCK_BYTES,
        },
        PackedGcPolicy {
            lease_ttl_seconds: 30,
            grace_seconds: 0,
            max_scans: 1,
            max_operations: 1,
            max_protective_rows: 64,
        },
        CancellationToken::new(),
        layer(9006),
    )
    .await
}

async fn three_ticks(change_catalog: bool) {
    let (backend, store, budget) = catalog().await;
    let objects = Objects::default();
    let baseline = budget.state().used;
    assert!(
        tick(store.clone(), budget.clone(), objects.clone())
            .await
            .is_err()
    );
    assert_eq!(objects.deletes.load(Ordering::SeqCst), 64);
    let reservation = backend
        .get(&slice_deletion_key(SLICE))
        .await
        .unwrap()
        .unwrap();
    let target_state = backend
        .get(&native_reverse::state_key(layer(9006)))
        .await
        .unwrap()
        .unwrap();
    if change_catalog {
        orphan(&store, layer(9010), 9011).await;
    }
    assert!(
        tick(store.clone(), budget.clone(), objects.clone())
            .await
            .is_err()
    );
    assert_eq!(objects.deletes.load(Ordering::SeqCst), 128);
    assert_eq!(
        backend
            .get(&slice_deletion_key(SLICE))
            .await
            .unwrap()
            .unwrap(),
        reservation
    );
    assert_eq!(
        backend
            .get(&native_reverse::state_key(layer(9006)))
            .await
            .unwrap()
            .unwrap(),
        target_state
    );
    if change_catalog {
        store
            .finalize_layer_metadata_deletion(vec![layer(9010)])
            .await
            .unwrap();
    }
    assert_eq!(
        tick(store.clone(), budget.clone(), objects.clone())
            .await
            .unwrap(),
        1
    );
    assert_eq!(objects.deletes.load(Ordering::SeqCst), 130);
    assert!(
        backend
            .get(&hot_layer_key(layer(9006)))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .get(&native_reverse::state_key(layer(9006)))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        backend
            .get(&slice_deletion_key(SLICE))
            .await
            .unwrap()
            .unwrap(),
        reservation
    );
    let progress: RangeProgress = decode_open_value(
        &backend
            .get(&range_progress_key((SLICE, 0), 65))
            .await
            .unwrap()
            .unwrap(),
        512,
    )
    .unwrap();
    assert_eq!(progress.next, 65);
    assert_eq!(budget.state().used, baseline);
}

#[tokio::test]
async fn native_slice_actual_catalog_sixty_five_blocks_resume_with_fixed_quota() {
    three_ticks(false).await;
}

#[tokio::test]
async fn native_slice_actual_catalog_unrelated_birth_and_death_preserve_resume() {
    three_ticks(true).await;
}

#[tokio::test]
async fn native_slice_actual_catalog_exact_target_aba_rejects_old_reservation() {
    let (backend, store, budget) = catalog().await;
    let objects = Objects::default();
    assert!(
        tick(store.clone(), budget.clone(), objects.clone())
            .await
            .is_err()
    );
    let hot = backend
        .get(&hot_layer_key(layer(9006)))
        .await
        .unwrap()
        .unwrap();
    let state = backend
        .get(&native_reverse::state_key(layer(9006)))
        .await
        .unwrap()
        .unwrap();
    store
        .finalize_layer_metadata_deletion(vec![layer(9006)])
        .await
        .unwrap();
    // The public birth at the same backend time reproduces identical hot bytes.
    // A different SID remains legal; the original SID is permanently fenced.
    orphan(&store, layer(9006), 9012).await;
    assert_eq!(
        backend
            .get(&hot_layer_key(layer(9006)))
            .await
            .unwrap()
            .unwrap(),
        hot
    );
    let replacement = backend
        .get(&native_reverse::state_key(layer(9006)))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        native_reverse::gc_incarnation(&state, layer(9006)).unwrap(),
        native_reverse::gc_incarnation(&replacement, layer(9006)).unwrap()
    );
    assert!(matches!(
        store
            .reserve_gc_slice_deletion(SLICE, END, &[layer(9006)])
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(objects.deletes.load(Ordering::SeqCst), 64);
    assert!(
        backend
            .get(&block_delete_key((SLICE, 32)))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn native_slice_absent_identity_requires_explicit_admin_and_never_changes_existing_build() {
    let (backend, store, budget) = catalog().await;
    let key = native_reverse::state_key(layer(9006));
    backend.rows.lock().await.remove(&key);
    assert!(matches!(
        store
            .reserve_gc_slice_deletion(SLICE, END, &[layer(9006)])
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert!(backend.get(&key).await.unwrap().is_none());
    let runtime = KvWorkspaceStore::from_arc(backend.clone())
        .with_packed_reader_pin_budget(budget.clone())
        .into_runtime();
    assert!(matches!(
        runtime
            .initialize_native_reverse_deleting_identity(layer(9006), budget.clone())
            .await,
        Err(WorkspaceError::UnsupportedCapability(_))
    ));
    store
        .initialize_native_reverse_deleting_identity(layer(9006), budget.clone())
        .await
        .unwrap();
    let first = backend.get(&key).await.unwrap().unwrap();
    native_reverse::gc_incarnation(&first, layer(9006)).unwrap();
    store
        .reserve_gc_slice_deletion(SLICE, END, &[layer(9006)])
        .await
        .unwrap();
    store
        .initialize_native_reverse_deleting_identity(layer(9006), budget.clone())
        .await
        .unwrap();
    assert_eq!(backend.get(&key).await.unwrap().unwrap(), first);
    assert!(matches!(
        store
            .start_native_reverse_index(layer(9006), budget.clone())
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert!(matches!(
        store
            .advance_native_reverse_index(layer(9006), budget)
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(backend.get(&key).await.unwrap().unwrap(), first);
}

#[tokio::test]
async fn native_slice_admin_unknown_reply_preserves_one_build_and_bad_header_cannot_initialize() {
    let (backend, store, budget) = catalog().await;
    let key = native_reverse::state_key(layer(9006));
    backend.rows.lock().await.remove(&key);
    backend.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        store
            .initialize_native_reverse_deleting_identity(layer(9006), budget.clone())
            .await,
        Err(WorkspaceError::Backend(_))
    ));
    let first = backend.get(&key).await.unwrap().unwrap();
    store
        .initialize_native_reverse_deleting_identity(layer(9006), budget.clone())
        .await
        .unwrap();
    assert_eq!(backend.get(&key).await.unwrap().unwrap(), first);
    let mut rows = backend.rows.lock().await;
    rows.remove(&key);
    rows.remove(VOLUME_HEADER_KEY);
    drop(rows);
    assert!(matches!(
        store
            .initialize_native_reverse_deleting_identity(layer(9006), budget)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert!(backend.get(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn native_slice_reserved_identity_corruption_and_noncanonical_reservation_fail_closed() {
    let (backend, store, _) = catalog().await;
    store
        .reserve_gc_slice_deletion(SLICE, END, &[layer(9006)])
        .await
        .unwrap();
    let key = slice_deletion_key(SLICE);
    let original = backend.get(&key).await.unwrap().unwrap();
    let state_key = native_reverse::state_key(layer(9006));
    let state = backend.get(&state_key).await.unwrap().unwrap();
    backend
        .rows
        .lock()
        .await
        .get_mut(&state_key)
        .unwrap()
        .push(0);
    assert!(
        store
            .reserve_gc_slice_deletion(SLICE, END, &[layer(9006)])
            .await
            .is_err()
    );
    backend.rows.lock().await.insert(state_key, state);
    backend.rows.lock().await.get_mut(&key).unwrap().push(0);
    assert!(
        store
            .reserve_gc_slice_deletion(SLICE, END, &[layer(9006)])
            .await
            .is_err()
    );
    backend.rows.lock().await.insert(key, original);
    store
        .reserve_gc_slice_deletion(SLICE, END, &[layer(9006)])
        .await
        .unwrap();
}

#[tokio::test]
async fn native_slice_bounded_schema_allows_only_reserved_family_to_reach_forty_eight_kib() {
    let (backend, _store, budget) = catalog().await;
    let baseline = budget.state().used;
    let _tick_owner = budget
        .admit(&[(V3BudgetPool::Metadata, 128 << 20)])
        .unwrap();
    let _reservation_owner = budget.admit(&[(V3BudgetPool::Metadata, 32 << 20)]).unwrap();
    let bounded = Bounded {
        backend,
        cancel: CancellationToken::new(),
        budget: budget.clone(),
        calls: AtomicUsize::new(0),
        rows: AtomicUsize::new(0),
        bytes: AtomicUsize::new(0),
    };
    let key = slice_deletion_key(9999);
    let raw = vec![1; 48 << 10];
    assert!(
        bounded
            .compare_and_swap(
                &[KvCheck {
                    key: key.clone(),
                    expected: None
                }],
                &[KvWrite::Put {
                    key: key.clone(),
                    value: raw.clone()
                }]
            )
            .await
            .unwrap()
    );
    let values = bounded
        .get_many_consistent_with_time_bounded(
            std::slice::from_ref(&key),
            Bounded::<JournalMemoryBackend>::limits(1, 48 << 10),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(values, vec![Some(raw)]);
    assert_eq!(
        Bounded::<JournalMemoryBackend>::value_bytes(b"packed/v3/native-delete-progress/x"),
        4 << 10
    );
    let other = b"packed/v3/native-delete-progress/oversized".to_vec();
    assert!(matches!(
        bounded
            .compare_and_swap(
                &[],
                &[KvWrite::Put {
                    key: other,
                    value: vec![1; 4097]
                }]
            )
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert!(matches!(
        bounded
            .compare_and_swap(
                &[],
                &[KvWrite::Put {
                    key,
                    value: vec![1; (48 << 10) + 1]
                }]
            )
            .await,
        Err(WorkspaceError::Busy)
    ));
    drop(_reservation_owner);
    drop(_tick_owner);
    assert_eq!(budget.state().used, baseline);
}
