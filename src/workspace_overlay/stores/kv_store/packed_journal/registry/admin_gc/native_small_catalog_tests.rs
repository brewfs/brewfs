//! Exercise the real native collector with a substrate that refuses the same
//! unsupported value plans as TiKV. No unbounded GET/SCAN fallback is available.
use super::super::tests::Objects;
use super::*;
use crate::workspace_overlay::catalog::{CreateVolumeRoot, RecordOrphanSlice, WorkspaceStore};
use crate::workspace_overlay::model::DataExtentDelta;
use crate::workspace_overlay::stores::kv_store::packed_journal::tests::JournalMemoryBackend;
use tokio::sync::Mutex;

struct CappedCatalog {
    inner: Arc<JournalMemoryBackend>,
    reads: Mutex<Vec<(Vec<Vec<u8>>, KvReadLimits)>>,
    writes: AtomicUsize,
    read_rows: AtomicUsize,
}

fn decoder_bytes(limits: KvReadLimits) -> Result<usize, WorkspaceError> {
    limits.validate()?;
    let bytes = match limits.max_value_bytes {
        0..=4096 => 8 << 10,
        4097..=12288 => 16 << 10,
        12289..=49152 => 64 << 10,
        49153..=98304 => 128 << 10,
        _ => {
            return Err(WorkspaceError::UnsupportedCapability(
                "fixture TiKV value schema exceeds 96 KiB",
            ));
        }
    };
    if limits.max_response_bytes < bytes {
        return Err(WorkspaceError::InvalidReadPlan(
            "fixture decoder exceeds caller envelope".into(),
        ));
    }
    Ok(bytes)
}

#[async_trait]
impl WorkspaceKvBackend for CappedCatalog {
    fn name(&self) -> &'static str {
        "tikv-capped-native-catalog-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    async fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        panic!("unbounded native GET")
    }
    async fn scan_prefix(&self, _: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        panic!("unbounded native SCAN")
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        decoder_bytes(limits)?;
        self.reads.lock().await.push((keys.to_vec(), limits));
        let result = self
            .inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await?;
        self.read_rows.fetch_add(keys.len(), Ordering::SeqCst);
        Ok(result)
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        mut limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.max_response_bytes = decoder_bytes(limits)?;
        let rows = self
            .inner
            .scan_prefix_with_byte_limits(prefix, limits)
            .await?;
        self.read_rows.fetch_add(rows.len(), Ordering::SeqCst);
        Ok(rows)
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        mut limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.max_response_bytes = decoder_bytes(limits)?;
        let rows = self
            .inner
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await?;
        self.read_rows.fetch_add(rows.len(), Ordering::SeqCst);
        Ok(rows)
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        if !writes.is_empty() {
            self.writes.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.compare_and_swap(checks, writes).await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: i64,
    ) -> Result<bool, WorkspaceError> {
        if !writes.is_empty() {
            self.writes.fetch_add(1, Ordering::SeqCst);
        }
        self.inner
            .compare_and_swap_before(checks, writes, before)
            .await
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        before: i64,
        limits: KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        decoder_bytes(limits)?;
        self.inner
            .authenticate_checks_before_bounded(checks, before, limits)
            .await
    }
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
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

async fn catalog() -> (
    Arc<CappedCatalog>,
    Arc<KvWorkspaceStore<CappedCatalog>>,
    LayerId,
) {
    let inner = Arc::new(JournalMemoryBackend::at_time(100_000_000_000));
    let setup = KvWorkspaceStore::from_arc(inner.clone());
    setup.initialize_workspace_schema().await.unwrap();
    setup
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: uuid::Uuid::from_u128(801),
            workspace_id: WorkspaceId::from_uuid(uuid::Uuid::from_u128(802)),
            root_layer_id: LayerId::from_uuid(uuid::Uuid::from_u128(803)),
            writable_layer_id: LayerId::from_uuid(uuid::Uuid::from_u128(804)),
            owner_id: None,
        })
        .await
        .unwrap();
    let target = LayerId::from_uuid(uuid::Uuid::from_u128(805));
    setup
        .record_orphan_slice(RecordOrphanSlice {
            orphan_layer_id: target,
            slice_id: 806,
            slice_end: 2,
        })
        .await
        .unwrap();
    // Simulate an old orphan while retaining the real collector's 60s grace.
    let mut rows = inner.rows.lock().await;
    let mut control = test_topology_from_rows(&rows);
    let target_row = control.layers.get_mut(&target).unwrap();
    target_row.created_at_ns = 1;
    let target_row = target_row.clone();
    test_write_topology_rows(&mut rows, &control);
    rows.insert(hot_layer_key(target), encode(&target_row).unwrap());
    drop(rows);
    let backend = Arc::new(CappedCatalog {
        inner,
        reads: Mutex::new(Vec::new()),
        writes: AtomicUsize::new(0),
        read_rows: AtomicUsize::new(0),
    });
    let store = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone())
            .with_packed_reader_pin_budget(V3MountBudget::defaults()),
    );
    (backend, store, target)
}

async fn many_sid_catalog() -> (
    Arc<CappedCatalog>,
    Arc<KvWorkspaceStore<CappedCatalog>>,
    LayerId,
) {
    let (backend, store, target) = catalog().await;
    let mut rows = backend.inner.rows.lock().await;
    for index in 1..128u64 {
        let extent = DataExtentDelta::data(target, index as i64 + 1, 0, 0, 1, 10_000 + index, 0, 1);
        rows.insert(
            crate::workspace_overlay::stores::kv_store::extent_key(&extent),
            encode(&extent).unwrap(),
        );
    }
    drop(rows);
    (backend, store, target)
}

#[tokio::test]
async fn small_catalog_native_collection_uses_supported_decoders_and_removes_only_target() {
    let (backend, store, target) = catalog().await;
    let objects = Objects::default();
    let count = collect_one(
        store,
        ObjectClient::new(objects.clone()),
        V3MountBudget::defaults(),
        ChunkLayout {
            chunk_size: 4 << 20,
            block_size: 1 << 20,
        },
        policy(),
        CancellationToken::new(),
        target,
    )
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        objects.deletes.load(Ordering::SeqCst),
        2,
        "one physical block's versioned and legacy objects"
    );
    let rows = backend.inner.rows.lock().await;
    let control = test_topology_from_rows(&rows);
    assert!(!control.layers.contains_key(&target));
    assert!(!rows.contains_key(&hot_layer_key(target)));
    assert!(
        control
            .layers
            .contains_key(&LayerId::from_uuid(uuid::Uuid::from_u128(803)))
    );
    drop(rows);
    let plans = backend.reads.lock().await;
    assert!(plans.iter().any(
        |(keys, limits)| keys.iter().any(|key| key.as_slice() == CONTROL_KEY)
            && limits.max_value_bytes > 0
            && limits.max_value_bytes <= OPEN_RECORD_MAX_BYTES
    ));
    assert!(
        plans
            .iter()
            .all(|(_, limits)| limits.max_value_bytes <= 96 << 10
                && limits.max_response_bytes <= 128 << 10)
    );
}

#[tokio::test]
async fn native_collection_eventually_removes_layer_with_many_sids_across_bounded_ticks() {
    let (backend, store, target) = many_sid_catalog().await;
    let objects = Objects::default();
    let mut completed = false;
    for _ in 0..16 {
        backend.read_rows.store(0, Ordering::SeqCst);
        let result = collect_one(
            store.clone(),
            ObjectClient::new(objects.clone()),
            V3MountBudget::defaults(),
            ChunkLayout {
                chunk_size: 4 << 20,
                block_size: 1 << 20,
            },
            policy(),
            CancellationToken::new(),
            target,
        )
        .await;
        assert!(
            backend.read_rows.load(Ordering::SeqCst) <= MAX_ROWS + 64,
            "one collector tick exceeded its bounded metadata-row budget"
        );
        if result.is_ok() {
            completed = true;
            break;
        }
    }
    assert!(
        completed,
        "bounded retries must eventually finish native GC"
    );
    assert_eq!(objects.deletes.load(Ordering::SeqCst), 256);
    let rows = backend.inner.rows.lock().await;
    assert!(!rows.contains_key(&hot_layer_key(target)));
}

#[tokio::test]
async fn missing_or_uninitialized_control_refuses_before_mutation_or_physical_delete() {
    for uninitialized in [false, true] {
        let (backend, store, target) = catalog().await;
        // The selected orphan and valid header sidecar remain discoverable.
        // Missing or bootstrap CONTROL cannot authorize GC from hot mirrors.
        let mut rows = backend.inner.rows.lock().await;
        if uninitialized {
            let KvWrite::Put { key, value } = put_control(&ControlHeader {
                schema_version: WORKSPACE_SCHEMA_VERSION,
                header: None,
                catalog_format: CATALOG_FORMAT,
            })
            .unwrap() else {
                unreachable!()
            };
            rows.insert(key, value);
        } else {
            assert!(rows.remove(CONTROL_KEY).is_some());
        }
        let before = rows.clone();
        drop(rows);
        let objects = Objects::default();
        let result = collect_one(
            store,
            ObjectClient::new(objects.clone()),
            V3MountBudget::defaults(),
            ChunkLayout {
                chunk_size: 4 << 20,
                block_size: 1 << 20,
            },
            policy(),
            CancellationToken::new(),
            target,
        )
        .await;
        assert!(matches!(result, Err(WorkspaceError::Fenced)));
        assert_eq!(objects.deletes.load(Ordering::SeqCst), 0);
        assert_eq!(backend.writes.load(Ordering::SeqCst), 0);
        assert_eq!(*backend.inner.rows.lock().await, before);
    }
}

#[tokio::test]
async fn oversized_workspace_entity_refuses_before_mutation_or_physical_delete() {
    let (backend, store, target) = catalog().await;
    let mut rows = backend.inner.rows.lock().await;
    let control = test_topology_from_rows(&rows);
    let mut workspace = control.workspaces.values().next().unwrap().clone();
    workspace.owner_id = Some("x".repeat(CONTROL_VALUE_BYTES));
    let oversized = encode(&workspace).unwrap();
    assert!(oversized.len() > CONTROL_VALUE_BYTES);
    assert!(
        decode::<WorkspaceRecord>(&oversized).is_ok(),
        "valid topology must fail specifically on its size bound"
    );
    rows.insert(hot_workspace_key(workspace.workspace_id), oversized);
    drop(rows);
    let before = backend.inner.rows.lock().await.clone();
    let objects = Objects::default();
    assert!(
        collect_one(
            store,
            ObjectClient::new(objects.clone()),
            V3MountBudget::defaults(),
            ChunkLayout {
                chunk_size: 4 << 20,
                block_size: 1 << 20
            },
            policy(),
            CancellationToken::new(),
            target
        )
        .await
        .is_err()
    );
    assert_eq!(objects.deletes.load(Ordering::SeqCst), 0);
    assert_eq!(backend.writes.load(Ordering::SeqCst), 0);
    assert_eq!(*backend.inner.rows.lock().await, before);
}

#[tokio::test]
async fn mixed_control_batch_does_not_expand_hot_record_schema_or_allow_oversized_cas() {
    let (backend, _, target) = catalog().await;
    let bounded = Bounded {
        backend: backend.clone(),
        cancel: CancellationToken::new(),
        budget: V3MountBudget::defaults(),
        calls: AtomicUsize::new(0),
        rows: AtomicUsize::new(0),
        bytes: AtomicUsize::new(0),
    };
    let key = hot_layer_key(target);
    backend
        .inner
        .rows
        .lock()
        .await
        .insert(key.clone(), vec![b'x'; (12 << 10) + 1]);
    assert!(
        bounded
            .get_many_consistent(&[CONTROL_KEY.to_vec(), key])
            .await
            .is_err(),
        "the mixed batch's larger decoder cannot broaden the hot-layer semantic cap"
    );
    assert!(
        bounded
            .compare_and_swap(
                &[],
                &[KvWrite::Put {
                    key: CONTROL_KEY.to_vec(),
                    value: vec![b'x'; CONTROL_VALUE_BYTES + 1]
                }]
            )
            .await
            .is_err()
    );
    assert_eq!(backend.writes.load(Ordering::SeqCst), 0);
}
