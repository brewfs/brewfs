use super::*;
use crate::workspace::stores::binding_tests::{packed, request};
use std::sync::atomic::{AtomicI64, Ordering};

type CasPause = (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);

#[derive(Clone)]
struct FreezeBackend {
    rows: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    now: Arc<AtomicI64>,
    locked_clock: Arc<Mutex<Option<i64>>>,
    cas_pause: Arc<Mutex<Option<CasPause>>>,
}
impl Default for FreezeBackend {
    fn default() -> Self {
        Self {
            rows: Default::default(),
            now: Arc::new(AtomicI64::new(1_000_000_000)),
            locked_clock: Default::default(),
            cas_pause: Default::default(),
        }
    }
}
impl FreezeBackend {
    async fn cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        self.cas_with_authentication_limits(checks, writes, before, None)
            .await
    }

    async fn cas_with_authentication_limits(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: Option<i64>,
        authentication_limits: Option<crate::workspace::stores::kv_backend::KvReadLimits>,
    ) -> Result<bool, WorkspaceError> {
        let pause = self.cas_pause.lock().await.take();
        if let Some((started, resume)) = pause {
            started.notify_one();
            resume.notified().await;
        }
        let mut rows = self.rows.lock().await;
        if let Some(limits) = authentication_limits {
            let mut authentication_total = 0usize;
            for check in checks {
                let value_bytes = rows.get(&check.key).map_or(0, Vec::len);
                authentication_total = authentication_total
                    .checked_add(check.key.len())
                    .and_then(|bytes| bytes.checked_add(value_bytes))
                    .ok_or_else(|| {
                        WorkspaceError::InvalidReadPlan(
                            "fixture authentication byte count overflow".into(),
                        )
                    })?;
                let response_bytes = check
                    .key
                    .len()
                    .checked_add(value_bytes)
                    .and_then(|bytes| bytes.checked_add(16))
                    .ok_or_else(|| {
                        WorkspaceError::InvalidReadPlan(
                            "fixture authentication response byte count overflow".into(),
                        )
                    })?;
                if value_bytes > limits.max_value_bytes
                    || authentication_total > limits.max_total_bytes
                    || response_bytes > limits.max_response_bytes
                {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "fixture authentication snapshot exceeds byte limits".into(),
                    ));
                }
            }
        }
        if checks
            .iter()
            .any(|check| rows.get(&check.key) != check.expected.as_ref())
        {
            return Ok(false);
        }
        if let Some(clock) = self.locked_clock.lock().await.take() {
            self.now.store(clock, Ordering::SeqCst);
        }
        if before.is_some_and(|deadline| self.now.load(Ordering::SeqCst) >= deadline) {
            return Err(WorkspaceError::Fenced);
        }
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
        Ok(true)
    }
}
#[async_trait]
impl WorkspaceKvBackend for FreezeBackend {
    fn name(&self) -> &'static str {
        "native-catalog-freeze-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
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
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let rows = self.rows.lock().await;
        Ok((
            keys.iter().map(|key| rows.get(key).cloned()).collect(),
            self.now.load(Ordering::SeqCst),
        ))
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        limits.validate_keys(keys)?;
        let rows = self.rows.lock().await;
        let total = keys.iter().try_fold(0usize, |total, key| {
            let length = rows.get(key).map_or(0, Vec::len);
            if length > limits.max_value_bytes {
                return None;
            }
            total.checked_add(key.len())?.checked_add(length)
        });
        if total.is_none_or(|total| total > limits.max_total_bytes) {
            return Err(WorkspaceError::InvalidReadPlan(
                "freeze fixture bounded values".into(),
            ));
        }
        Ok((
            keys.iter().map(|key| rows.get(key).cloned()).collect(),
            self.now.load(Ordering::SeqCst),
        ))
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        Ok(self
            .rows
            .lock()
            .await
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect())
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None).await
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate_scan_page(prefix, after)?;
        let rows = self.rows.lock().await;
        let selected = || {
            rows.range(prefix.to_vec()..)
                .take_while(|(key, _)| key.starts_with(prefix))
                .filter(|(key, _)| after.is_none_or(|after| key.as_slice() > after))
                .take(limits.max_records.min(limits.max_data_requests))
        };
        let mut total = 0usize;
        let mut response = 16usize;
        for (key, value) in selected() {
            total = total
                .checked_add(key.len())
                .and_then(|sum| sum.checked_add(value.len()))
                .ok_or(WorkspaceError::Busy)?;
            response = response
                .checked_add(32)
                .and_then(|sum| sum.checked_add(key.len()))
                .and_then(|sum| sum.checked_add(value.len()))
                .ok_or(WorkspaceError::Busy)?;
            if key.len() > limits.max_key_bytes
                || value.len() > limits.max_value_bytes
                || total > limits.max_total_bytes
                || response > limits.max_response_bytes
            {
                return Err(WorkspaceError::InvalidReadPlan(
                    "freeze fixture keyset page exceeds limits before cloning".into(),
                ));
            }
        }
        Ok(selected()
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect())
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, Some(deadline)).await
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        expires_at_ns: i64,
        limits: crate::workspace::stores::kv_backend::KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        crate::workspace::stores::kv_backend::validate_bounded_authentication_checks(
            checks, limits,
        )?;
        crate::workspace::stores::kv_backend::validate_cas_time_window(None, Some(expires_at_ns))?;
        self.cas_with_authentication_limits(checks, &[], Some(expires_at_ns), Some(limits))
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(self.now.load(Ordering::SeqCst))
    }
}

async fn fixture() -> (
    Arc<KvWorkspaceStore<FreezeBackend>>,
    FreezeBackend,
    HeadGuard,
    [LayerRecord; 2],
    Arc<SnapshotMountBudget>,
) {
    let backend = FreezeBackend::default();
    let budget = SnapshotMountBudget::defaults();
    let store = Arc::new(
        KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    let (_directory, _client, _snapshot, lower, _) = packed().await;
    let initial = request(store.as_ref(), lower).await;
    let binding = store
        .install_packed_lower_binding(initial.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: binding.head_epoch,
        ..initial.guard
    };
    let layers = store
        .load_layer_chain(guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    (store, backend, guard, layers, budget)
}

#[tokio::test]
async fn packed_native_quiesce_stops_real_mutations_and_preserves_exact_old_new_mapping() {
    let (store, backend, guard, layers, budget) = fixture().await;
    let new_head = LayerId::new();
    let journal = JournalId::new();
    let fence = store
        .clone()
        .begin_packed_native_quiesce(guard.clone(), layers.clone(), journal, new_head, budget)
        .await
        .unwrap();
    fence
        .validate_context(&guard, &layers, fence.binding())
        .await
        .unwrap();
    assert_eq!(fence.mapping().old_layers(), &layers);
    assert_eq!(fence.mapping().planned_head_layer_id(), new_head);
    assert_eq!(
        fence.mapping().planned_head_epoch(),
        guard.expected_head_epoch + 1
    );
    assert_eq!(
        fence.mapping().native_sealed_source_layer_id(),
        layers[0].layer_id
    );
    assert_eq!(fence.mapping().journal_id(), journal);
    assert_ne!(fence.canonical_receipt_digest(), [0; 32]);
    assert!(
        fence
            .canonical_receipt_bytes()
            .starts_with(b"BrewFS-packed-native-catalog-quiesce\0")
    );
    let before = backend.rows.lock().await.clone();
    assert!(matches!(
        store
            .apply_namespace_mutation(NamespaceMutation {
                guard: guard.clone(),
                dentries: vec![DentryDelta::put(
                    layers[0].layer_id,
                    1,
                    b"late".to_vec(),
                    2,
                    0,
                    0
                )],
                inodes: Vec::new(),
            })
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(*backend.rows.lock().await, before);
    assert!(matches!(
        store
            .validate_read_fence(guard.clone(), layers.clone())
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert!(matches!(
        store.load_packed_binding_record(guard).await,
        Err(WorkspaceError::Fenced)
    ));
    drop(fence);
    assert_eq!(
        store.load_seal_journal(journal).await.unwrap().phase,
        SealPhase::Quiesced
    );
    assert_eq!(
        store.load_layer(layers[0].layer_id).await.unwrap().state,
        LayerState::Sealing
    );
}

#[tokio::test]
async fn packed_native_quiesce_rejects_context_mixing_and_sequence_drift() {
    let (store, backend, guard, layers, budget) = fixture().await;
    let fence = store
        .clone()
        .begin_packed_native_quiesce(
            guard.clone(),
            layers.clone(),
            JournalId::new(),
            LayerId::new(),
            budget,
        )
        .await
        .unwrap();
    let mut wrong_guard = guard;
    wrong_guard.expected_head_epoch += 1;
    assert!(matches!(
        fence
            .validate_context(&wrong_guard, &layers, fence.binding())
            .await,
        Err(WorkspaceError::Fenced)
    ));
    let mut changed = layers[0].clone();
    changed.state = LayerState::Sealing;
    changed.next_sequence += 1;
    backend
        .rows
        .lock()
        .await
        .insert(hot_layer_key(changed.layer_id), encode(&changed).unwrap());
    assert!(matches!(
        fence.validate().await,
        Err(WorkspaceError::Fenced)
    ));
}

#[tokio::test]
async fn packed_native_quiesce_lease_expiry_is_checked_at_locked_backend_validation() {
    let (store, backend, guard, layers, budget) = fixture().await;
    let fence = store
        .clone()
        .begin_packed_native_quiesce(
            guard.clone(),
            layers,
            JournalId::new(),
            LayerId::new(),
            budget,
        )
        .await
        .unwrap();
    let lease: SnapshotLease = decode(
        &backend
            .get(&hot_lease_key(guard.workspace_id, guard.lease_id))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    *backend.locked_clock.lock().await = Some(lease.expires_at_ns);
    assert!(matches!(
        fence.validate().await,
        Err(WorkspaceError::Fenced)
    ));
}

#[tokio::test]
async fn packed_native_begin_rejects_oversized_control_before_any_catalog_change() {
    let (store, backend, guard, layers, budget) = fixture().await;
    {
        let mut rows = backend.rows.lock().await;
        let mut encoded = rows.get(CONTROL_KEY).unwrap().clone();
        encoded.resize(FREEZE_POINT_MAX_BYTES + 1, 0);
        assert!(encoded.len() > FREEZE_POINT_MAX_BYTES);
        rows.insert(CONTROL_KEY.to_vec(), encoded);
    }
    let before = backend.rows.lock().await.clone();
    let result = store
        .begin_packed_native_quiesce(guard, layers, JournalId::new(), LayerId::new(), budget)
        .await;
    assert!(result.is_err());
    assert_eq!(
        *backend.rows.lock().await,
        before,
        "oversized CONTROL advanced the workspace or journal before rejecting it"
    );
}

#[tokio::test]
async fn packed_native_begin_rejects_existing_planned_head_before_any_catalog_change() {
    let (store, backend, guard, layers, budget) = fixture().await;
    let planned = LayerId::new();
    let mut occupied = layers[0].clone();
    occupied.layer_id = planned;
    backend
        .rows
        .lock()
        .await
        .insert(hot_layer_key(planned), encode(&occupied).unwrap());
    let before = backend.rows.lock().await.clone();
    let result = store
        .begin_packed_native_quiesce(guard, layers, JournalId::new(), planned, budget)
        .await;
    assert!(result.is_err());
    assert_eq!(
        *backend.rows.lock().await,
        before,
        "an occupied planned head left a partial native seal"
    );
}

#[tokio::test]
async fn packed_native_begin_cancelled_waiter_retains_owner_until_quiesced() {
    let (store, backend, guard, layers, budget) = fixture().await;
    let journal = JournalId::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *backend.cas_pause.lock().await = Some((started.clone(), resume.clone()));
    let task_budget = budget.clone();
    let caller = tokio::spawn(async move {
        store
            .begin_packed_native_quiesce(guard, layers, journal, LayerId::new(), task_budget)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    assert!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize] >= FREEZE_METADATA_BYTES,
        "cancelled waiter released an in-flight begin owner"
    );
    resume.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let control = test_topology_from_rows(&*backend.rows.lock().await);
            if control
                .journals
                .get(&journal)
                .is_some_and(|row| row.phase == SealPhase::Quiesced)
                && budget.state().used[SnapshotBudgetPool::Metadata as usize] == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[path = "seed_readonly_conflict_tests.rs"]
mod seed_readonly_conflict_tests;
