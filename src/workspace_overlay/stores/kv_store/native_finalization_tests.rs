use super::*;
use crate::workspace_overlay::catalog::WorkspaceStore;
use crate::workspace_overlay::packed_v3::wire005::V3MountBudget;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct Backend {
    rows: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    short_pages: AtomicBool,
    cancelled: AtomicBool,
    lose_metadata_reply: AtomicBool,
    cancel_after_metadata: AtomicBool,
    metadata_deleted: AtomicUsize,
    page_calls: AtomicUsize,
    mutate_before_metadata_cas: Mutex<Vec<KvWrite>>,
    packets: Mutex<Vec<(Vec<KvCheck>, Vec<KvWrite>)>>,
}

fn metadata_key(key: &[u8]) -> bool {
    key.starts_with(b"delta/") || key.starts_with(b"packed/v3/native-reverse/rows/")
}

#[async_trait]
impl WorkspaceKvBackend for Backend {
    fn name(&self) -> &'static str {
        "native-paged-finalization-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    fn native_gc_metadata_page_quota(&self) -> Option<usize> {
        Some(32)
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        Ok(self.rows.lock().await.get(key).cloned())
    }

    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        let rows = self.rows.lock().await;
        Ok(keys.iter().map(|key| rows.get(key).cloned()).collect())
    }

    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        limits.validate_keys(keys)?;
        let values = self.get_many_consistent(keys).await?;
        let mut total = 0usize;
        for (key, value) in keys.iter().zip(&values) {
            let size = value.as_ref().map_or(0, Vec::len);
            total += key.len() + size;
            if size > limits.max_value_bytes || total > limits.max_total_bytes {
                return Err(WorkspaceError::Busy);
            }
        }
        Ok((values, 10))
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        assert!(!metadata_key(prefix), "metadata families require pages");
        let rows = self.rows.lock().await;
        Ok(rows
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect())
    }

    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate_scan_page(prefix, after)?;
        self.page_calls.fetch_add(1, Ordering::SeqCst);
        let count = if self.short_pages.load(Ordering::SeqCst) {
            1
        } else {
            limits.max_records
        };
        let rows = self.rows.lock().await;
        let mut total = 0usize;
        let mut page = Vec::new();
        for (key, value) in rows
            .iter()
            .filter(|(key, _)| {
                key.starts_with(prefix) && after.is_none_or(|after| key.as_slice() > after)
            })
            .take(count)
        {
            total += key.len() + value.len();
            if key.len() > limits.max_key_bytes
                || value.len() > limits.max_value_bytes
                || total > limits.max_total_bytes
            {
                return Err(WorkspaceError::Busy);
            }
            page.push(KvEntry {
                key: key.clone(),
                value: value.clone(),
            });
        }
        Ok(page)
    }

    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Err(WorkspaceError::Busy);
        }
        assert!(checks.len() + writes.len() <= 256);
        let bytes = checks
            .iter()
            .map(|check| check.key.len() + check.expected.as_ref().map_or(0, Vec::len))
            .sum::<usize>()
            + writes
                .iter()
                .map(|write| match write {
                    KvWrite::Put { key, value } => key.len() + value.len(),
                    KvWrite::Delete { key } => key.len(),
                })
                .sum::<usize>();
        assert!(bytes <= 256 << 10);
        let mut rows = self.rows.lock().await;
        let deleted = writes
            .iter()
            .filter(|write| matches!(write, KvWrite::Delete { key } if metadata_key(key)))
            .count();
        if deleted > 0 {
            for write in self.mutate_before_metadata_cas.lock().await.drain(..) {
                match write {
                    KvWrite::Put { key, value } => {
                        rows.insert(key, value);
                    }
                    KvWrite::Delete { key } => {
                        rows.remove(&key);
                    }
                }
            }
        }
        if checks
            .iter()
            .any(|check| rows.get(&check.key) != check.expected.as_ref())
        {
            return Ok(false);
        }
        self.packets
            .lock()
            .await
            .push((checks.to_vec(), writes.to_vec()));
        for write in writes {
            match write {
                KvWrite::Put { key, value } => {
                    rows.insert(key.clone(), value.clone());
                }
                KvWrite::Delete { key } => {
                    rows.remove(key);
                }
            }
        }
        self.metadata_deleted.fetch_add(deleted, Ordering::SeqCst);
        if deleted > 0 && self.cancel_after_metadata.swap(false, Ordering::SeqCst) {
            self.cancelled.store(true, Ordering::SeqCst);
        }
        if deleted > 0 && self.lose_metadata_reply.swap(false, Ordering::SeqCst) {
            return Err(WorkspaceError::Backend(
                "committed metadata reply lost".into(),
            ));
        }
        Ok(true)
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(10)
    }
}

fn store(backend: Arc<Backend>) -> KvWorkspaceStore<Backend> {
    KvWorkspaceStore::from_arc(backend).with_packed_reader_pin_budget(V3MountBudget::defaults())
}

async fn catalog(count: usize) -> (Arc<Backend>, LayerId) {
    let backend = Arc::new(Backend::default());
    let target = LayerId::from_uuid(Uuid::from_u128(501));
    let header = VolumeHeader {
        schema_version: WORKSPACE_SCHEMA_VERSION,
        volume_format: VOLUME_FORMAT.into(),
        volume_id: Uuid::from_u128(502),
        created_at_ns: 1,
    };
    let layer = LayerRecord {
        layer_id: target,
        parent_layer_id: None,
        state: LayerState::Sealed,
        schema_version: WORKSPACE_SCHEMA_VERSION,
        sealed_version: Some(1),
        delta_digest: None,
        root_hash: Some([1; 32]),
        depth: 1,
        owner_workspace_id: None,
        next_sequence: 1,
        owned_slice_count: 0,
        owned_bytes: 0,
        created_at_ns: 1,
        sealed_at_ns: Some(2),
    };
    let mut control = ControlState {
        header: Some(header.clone()),
        ..Default::default()
    };
    control.layers.insert(target, layer.clone());
    {
        let mut rows = backend.rows.lock().await;
        test_write_topology_rows(&mut rows, &control);
        rows.insert(VOLUME_HEADER_KEY.to_vec(), encode(&header).unwrap());
        rows.insert(
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            encode(&1u64).unwrap(),
        );
        rows.insert(hot_layer_key(target), encode(&layer).unwrap());
    }
    let setup = store(backend.clone());
    let budget = setup.packed_reader_pin_budget.get().unwrap().clone();
    setup
        .start_native_reverse_index(target, budget.clone())
        .await
        .unwrap();
    assert!(
        setup
            .advance_native_reverse_index(target, budget)
            .await
            .unwrap()
    );
    let mut rows = backend.rows.lock().await;
    let mut layer = layer;
    layer.state = LayerState::Deleting;
    control.layers.insert(target, layer.clone());
    test_write_topology_rows(&mut rows, &control);
    rows.insert(hot_layer_key(target), encode(&layer).unwrap());
    for number in 0..count {
        // Whiteouts still exercise primary deletion without adding an alias.
        let row = DentryDelta::whiteout(
            target,
            1,
            format!("entry-{number:04}").into_bytes(),
            number as u64 + 1,
        );
        rows.insert(dentry_key(&row), encode(&row).unwrap());
    }
    for family in 1..FAMILIES {
        let mut key = family_prefix(target, family);
        key.extend_from_slice(b"test-row");
        rows.insert(key, b"opaque-deletion-only-test-row".to_vec());
    }
    drop(rows);
    backend.packets.lock().await.clear();
    (backend, target)
}

async fn finish(backend: &Arc<Backend>, target: LayerId) {
    for _ in 0..100 {
        match store(backend.clone())
            .finalize_layer_metadata_deletion(vec![target])
            .await
        {
            Ok(()) => return,
            Err(WorkspaceError::Busy) => {}
            other => panic!("finalization failed: {other:?}"),
        }
    }
    panic!("durable finalization made no terminal progress");
}

#[tokio::test]
async fn native_metadata_pages_survive_restart_and_keep_topology_until_empty() {
    let (backend, target) = catalog(70).await;
    assert!(matches!(
        store(backend.clone())
            .finalize_layer_metadata_deletion(vec![target])
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(backend.metadata_deleted.load(Ordering::SeqCst), 32);
    {
        let rows = backend.rows.lock().await;
        assert!(rows.contains_key(&state_key(&[target])));
        assert!(rows.contains_key(&hot_layer_key(target)));
        assert!(rows.contains_key(&native_reverse::state_key(target)));
        let control = test_topology_from_rows(&rows);
        assert_eq!(control.layers[&target].state, LayerState::Deleting);
    }
    finish(&backend, target).await;
    let rows = backend.rows.lock().await;
    assert!(!rows.contains_key(&hot_layer_key(target)));
    assert!(!rows.contains_key(&native_reverse::state_key(target)));
    assert!(!rows.contains_key(&state_key(&[target])));
    assert!(!rows.keys().any(|key| metadata_key(key)));
    assert_eq!(backend.metadata_deleted.load(Ordering::SeqCst), 76);
    drop(rows);
    let packets = backend.packets.lock().await;
    let (checks, writes) = packets.last().unwrap();
    for key in [
        state_key(&[target]),
        native_reverse::state_key(target),
        hot_layer_key(target),
    ] {
        assert!(checks.iter().any(|check| check.key == key));
        assert!(
            writes
                .iter()
                .any(|write| matches!(write, KvWrite::Delete { key: written } if *written == key))
        );
    }
}

#[tokio::test]
async fn native_metadata_short_pages_require_real_empty_pages() {
    let (backend, target) = catalog(3).await;
    backend.short_pages.store(true, Ordering::SeqCst);
    finish(&backend, target).await;
    assert_eq!(backend.metadata_deleted.load(Ordering::SeqCst), 9);
    assert!(backend.page_calls.load(Ordering::SeqCst) >= 20);
}

#[tokio::test]
async fn native_metadata_unknown_reply_and_cancel_resume_committed_progress() {
    for cancel in [false, true] {
        let (backend, target) = catalog(45).await;
        backend.lose_metadata_reply.store(!cancel, Ordering::SeqCst);
        backend
            .cancel_after_metadata
            .store(cancel, Ordering::SeqCst);
        assert!(
            store(backend.clone())
                .finalize_layer_metadata_deletion(vec![target])
                .await
                .is_err()
        );
        assert_eq!(backend.metadata_deleted.load(Ordering::SeqCst), 8);
        assert!(
            backend
                .rows
                .lock()
                .await
                .contains_key(&hot_layer_key(target))
        );
        backend.cancelled.store(false, Ordering::SeqCst);
        finish(&backend, target).await;
        assert_eq!(backend.metadata_deleted.load(Ordering::SeqCst), 51);
    }
}

#[tokio::test]
async fn native_metadata_retained_volume_target_and_carrier_cannot_refresh() {
    for fault in 1..4 {
        let (backend, target) = catalog(45).await;
        assert!(matches!(
            store(backend.clone())
                .finalize_native_metadata_pages(vec![target], 1)
                .await,
            Err(WorkspaceError::Busy)
        ));
        let mut rows = backend.rows.lock().await;
        match fault {
            0 => {
                rows.insert(
                    LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                    encode(&2u64).unwrap(),
                );
            }
            1 => {
                let mut header: VolumeHeader = decode(&rows[VOLUME_HEADER_KEY]).unwrap();
                header.volume_id = Uuid::from_u128(503);
                rows.insert(VOLUME_HEADER_KEY.to_vec(), encode(&header).unwrap());
            }
            2 => {
                let mut layer: LayerRecord = decode(&rows[&hot_layer_key(target)]).unwrap();
                layer.created_at_ns += 1;
                rows.insert(hot_layer_key(target), encode(&layer).unwrap());
            }
            _ => {
                rows.insert(
                    format!("packed/v3/sealed-carrier/{target}").into_bytes(),
                    b"corrupt-carrier".to_vec(),
                );
            }
        }
        let before = rows.clone();
        drop(rows);
        assert!(
            store(backend.clone())
                .finalize_layer_metadata_deletion(vec![target])
                .await
                .is_err()
        );
        assert_eq!(*backend.rows.lock().await, before);
    }
}

#[tokio::test]
async fn native_metadata_cursor_cannot_replace_terminal_empty_proof() {
    let (backend, target) = catalog(2).await;
    assert!(matches!(
        store(backend.clone())
            .finalize_native_metadata_pages(vec![target], 1)
            .await,
        Err(WorkspaceError::Busy)
    ));
    let key = state_key(&[target]);
    let mut rows = backend.rows.lock().await;
    let mut basis: Basis = decode(&rows[&key]).unwrap();
    basis.family = FAMILIES;
    rows.insert(key.clone(), encode(&basis).unwrap());
    drop(rows);
    assert!(matches!(
        store(backend.clone())
            .finalize_layer_metadata_deletion(vec![target])
            .await,
        Err(WorkspaceError::Busy)
    ));
    let rows = backend.rows.lock().await;
    let basis: Basis = decode(&rows[&key]).unwrap();
    assert_eq!(basis.family, 0);
    assert!(rows.contains_key(&hot_layer_key(target)));
    drop(rows);
    finish(&backend, target).await;
}

#[tokio::test]
async fn native_metadata_missing_target_with_rows_and_corrupt_cursor_are_fenced() {
    for corrupt in [false, true] {
        let (backend, target) = catalog(2).await;
        if corrupt {
            assert!(
                store(backend.clone())
                    .finalize_native_metadata_pages(vec![target], 1)
                    .await
                    .is_err()
            );
            let key = state_key(&[target]);
            let mut rows = backend.rows.lock().await;
            let mut basis: Basis = decode(&rows[&key]).unwrap();
            basis.family = FAMILIES + 1;
            rows.insert(key, encode(&basis).unwrap());
        } else {
            let mut rows = backend.rows.lock().await;
            rows.remove(&hot_layer_key(target));
            rows.remove(&native_reverse::state_key(target));
            let mut control = test_topology_from_rows(&rows);
            control.layers.remove(&target);
            test_write_topology_rows(&mut rows, &control);
        }
        let before = backend.rows.lock().await.clone();
        assert!(matches!(
            store(backend.clone())
                .finalize_layer_metadata_deletion(vec![target])
                .await,
            Err(WorkspaceError::Fenced)
        ));
        // Missing-target rejection may durably admit an empty cursor but never
        // removes metadata or topology; corrupt state performs no mutation.
        let after = backend.rows.lock().await;
        for (key, value) in before
            .iter()
            .filter(|(key, _)| !key.starts_with(STATE_PREFIX))
        {
            assert_eq!(after.get(key), Some(value));
        }
    }
}

#[tokio::test]
async fn native_metadata_unrelated_inventory_birth_and_death_do_not_stall_resume() {
    let (backend, target) = catalog(45).await;
    assert!(matches!(
        store(backend.clone())
            .finalize_native_metadata_pages(vec![target], 1)
            .await,
        Err(WorkspaceError::Busy)
    ));
    let key = state_key(&[target]);
    let original: Basis = decode(&backend.rows.lock().await[&key]).unwrap();
    let unrelated = LayerId::from_uuid(Uuid::from_u128(701));
    {
        let mut rows = backend.rows.lock().await;
        let mut control = test_topology_from_rows(&rows);
        let mut layer = control.layers[&target].clone();
        layer.layer_id = unrelated;
        layer.state = LayerState::Sealed;
        control.layers.insert(unrelated, layer.clone());
        rows.insert(hot_layer_key(unrelated), encode(&layer).unwrap());
        test_write_topology_rows(&mut rows, &control);
        rows.insert(
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            encode(&2u64).unwrap(),
        );
    }
    assert!(matches!(
        store(backend.clone())
            .finalize_layer_metadata_deletion(vec![target])
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(backend.metadata_deleted.load(Ordering::SeqCst), 33);
    {
        let mut rows = backend.rows.lock().await;
        let saved: Basis = decode(&rows[&key]).unwrap();
        assert_eq!(saved.inventory, original.inventory);
        assert_eq!(saved.incarnations, original.incarnations);
        let mut control = test_topology_from_rows(&rows);
        control.layers.remove(&unrelated);
        rows.remove(&hot_layer_key(unrelated));
        test_write_topology_rows(&mut rows, &control);
        rows.insert(
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            encode(&3u64).unwrap(),
        );
    }
    finish(&backend, target).await;
    assert_eq!(backend.metadata_deleted.load(Ordering::SeqCst), 51);
}

#[tokio::test]
async fn native_metadata_identical_target_bytes_with_new_reverse_incarnation_are_rejected() {
    let (backend, target) = catalog(3).await;
    assert!(matches!(
        store(backend.clone())
            .finalize_native_metadata_pages(vec![target], 1)
            .await,
        Err(WorkspaceError::Busy)
    ));
    let raw = backend.rows.lock().await[&hot_layer_key(target)].clone();
    {
        let mut rows = backend.rows.lock().await;
        let mut layer: LayerRecord = decode(&raw).unwrap();
        layer.state = LayerState::Sealed;
        rows.insert(hot_layer_key(target), encode(&layer).unwrap());
        rows.insert(
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            encode(&3u64).unwrap(),
        );
    }
    let replacement = store(backend.clone());
    replacement
        .start_native_reverse_index(
            target,
            replacement.packed_reader_pin_budget.get().unwrap().clone(),
        )
        .await
        .unwrap();
    backend.rows.lock().await.insert(hot_layer_key(target), raw);
    let before = backend.rows.lock().await.clone();
    assert!(matches!(
        store(backend.clone())
            .finalize_layer_metadata_deletion(vec![target])
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(*backend.rows.lock().await, before);
    assert_eq!(backend.metadata_deleted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_metadata_legacy_deleting_identity_is_explicit_and_survives_resume() {
    let (backend, target) = catalog(3).await;
    backend
        .rows
        .lock()
        .await
        .remove(&native_reverse::state_key(target));
    assert!(matches!(
        store(backend.clone())
            .finalize_layer_metadata_deletion(vec![target])
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(backend.metadata_deleted.load(Ordering::SeqCst), 0);
    assert!(
        !backend
            .rows
            .lock()
            .await
            .contains_key(&native_reverse::state_key(target))
    );
    let maintenance = store(backend.clone());
    maintenance
        .initialize_native_reverse_deleting_identity(
            target,
            maintenance.packed_reader_pin_budget.get().unwrap().clone(),
        )
        .await
        .unwrap();
    finish(&backend, target).await;
    assert_eq!(backend.metadata_deleted.load(Ordering::SeqCst), 9);
}

#[test]
fn native_metadata_authority_keys_are_private_and_canonical() {
    let target = LayerId::from_uuid(Uuid::from_u128(901));
    assert!(is_state_key(&state_key(&[target])));
    assert!(is_reverse_state_key(&native_reverse::state_key(target)));
    assert!(!is_state_key(b"packed/v3/native-finalization/x"));
    assert!(!is_reverse_state_key(
        b"packed/v3/native-reverse/state/not-a-layer"
    ));
    assert!(!is_reverse_state_key(
        &[native_reverse::state_key(target), b"/extra".to_vec()].concat()
    ));
}

#[tokio::test]
async fn native_metadata_actual_page_cas_rejects_authority_races() {
    for fault in 0..3 {
        let (backend, target) = catalog(2).await;
        let rows = backend.rows.lock().await;
        let write = match fault {
            0 => KvWrite::Put {
                key: LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                value: encode(&2u64).unwrap(),
            },
            1 => {
                let mut header: VolumeHeader = decode(&rows[VOLUME_HEADER_KEY]).unwrap();
                header.volume_id = Uuid::from_u128(999);
                KvWrite::Put {
                    key: VOLUME_HEADER_KEY.to_vec(),
                    value: encode(&header).unwrap(),
                }
            }
            _ => {
                let mut layer: LayerRecord = decode(&rows[&hot_layer_key(target)]).unwrap();
                layer.created_at_ns += 1;
                KvWrite::Put {
                    key: hot_layer_key(target),
                    value: encode(&layer).unwrap(),
                }
            }
        };
        drop(rows);
        backend.mutate_before_metadata_cas.lock().await.push(write);
        assert!(matches!(
            store(backend.clone())
                .finalize_layer_metadata_deletion(vec![target])
                .await,
            Err(WorkspaceError::Busy)
        ));
        assert_eq!(backend.metadata_deleted.load(Ordering::SeqCst), 0);
        assert!(
            backend
                .rows
                .lock()
                .await
                .contains_key(&hot_layer_key(target))
        );
    }
}
