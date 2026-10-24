//! Retention leases tested through actual public producer/binding fixtures.
//! These contracts do not certify graph publication or an old merged view.

#[path = "cancelled_lower_transport_tests.rs"]
mod cancelled_lower_transport_tests;

#[path = "native_effective_export_tests.rs"]
mod native_effective_export_tests;

mod entity_directory_page_tests;
mod entity_lower_tests;

use super::*;
use crate::workspace_overlay::stores::binding_tests::{packed, packed_with_snapshot_id, request};
use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
use crate::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

type NotificationPair = (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
type SharedTestBarrier = Arc<Mutex<Option<NotificationPair>>>;

#[derive(Clone)]
struct PinMemoryBackend {
    rows: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    now: Arc<AtomicI64>,
    lose_reply: Arc<AtomicBool>,
    validation_clock: Arc<Mutex<Option<i64>>>,
    cas_barrier: SharedTestBarrier,
    reply_barrier: SharedTestBarrier,
    page_prefixes: Arc<Mutex<Vec<Vec<u8>>>>,
    page_size: Arc<AtomicI64>,
    page_barrier: SharedTestBarrier,
    /// Models Redis' single-range pages. A forced `page_size` truncation is
    /// then a lie, so only enable this while `page_size` is unset.
    short_page_is_end: Arc<AtomicBool>,
    /// Consistent timed reads (head/lease and per-inode read fences).
    timed_reads: Arc<AtomicI64>,
    /// Consistent untimed multi-key reads (permission snapshots and rows).
    consistent_reads: Arc<AtomicI64>,
    /// Packed mutation view reads: bounded reads of the workspace record
    /// together with at least ten more view keys.
    view_reads: Arc<AtomicI64>,
    /// All bounded reads, used to prove a bound immutable memo hit is local.
    bounded_reads: Arc<AtomicI64>,
}
impl Default for PinMemoryBackend {
    fn default() -> Self {
        Self {
            rows: Default::default(),
            // Genuine backend timestamps are positive; explicit clock faults
            // still override this value at the actual locked CAS boundary.
            now: Arc::new(AtomicI64::new(1)),
            lose_reply: Default::default(),
            validation_clock: Default::default(),
            cas_barrier: Default::default(),
            reply_barrier: Default::default(),
            page_prefixes: Default::default(),
            page_size: Default::default(),
            page_barrier: Default::default(),
            short_page_is_end: Default::default(),
            timed_reads: Default::default(),
            consistent_reads: Default::default(),
            view_reads: Default::default(),
            bounded_reads: Default::default(),
        }
    }
}
impl PinMemoryBackend {
    async fn cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        self.cas_with_authentication_limits(checks, writes, lower, upper, None)
            .await
    }

    async fn cas_with_authentication_limits(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
        authentication_limits: Option<crate::workspace_overlay::stores::kv_backend::KvReadLimits>,
    ) -> Result<bool, WorkspaceError> {
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
        let barrier = self.cas_barrier.lock().await.take();
        if let Some((entered, resume)) = barrier {
            entered.notify_waiters();
            resume.notified().await;
        }
        if let Some(now) = self.validation_clock.lock().await.take() {
            self.now.store(now, Ordering::SeqCst);
        }
        let now = self.now.load(Ordering::SeqCst);
        if lower.is_some_and(|lower| now < lower) {
            return Err(WorkspaceError::Busy);
        }
        if upper.is_some_and(|upper| now >= upper) {
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
        drop(rows);
        // Model an already committed server mutation whose response has not
        // reached its caller. Catalog inspection is free to proceed, so only
        // retaining the heartbeat join can prevent an early release.
        if !writes.is_empty() {
            let barrier = self.reply_barrier.lock().await.take();
            if let Some((entered, resume)) = barrier {
                entered.notify_waiters();
                resume.notified().await;
            }
        }
        if !writes.is_empty() && self.lose_reply.swap(false, Ordering::SeqCst) {
            return Err(WorkspaceError::Backend(
                "injected committed pin response loss".into(),
            ));
        }
        Ok(true)
    }
}
#[async_trait]
impl WorkspaceKvBackend for PinMemoryBackend {
    fn name(&self) -> &'static str {
        "packed-reader-memory-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    fn short_scan_page_is_end(&self) -> bool {
        self.short_page_is_end.load(Ordering::SeqCst)
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        Ok(self.rows.lock().await.get(key).cloned())
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.consistent_reads.fetch_add(1, Ordering::SeqCst);
        let rows = self.rows.lock().await;
        Ok(keys.iter().map(|key| rows.get(key).cloned()).collect())
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.timed_reads.fetch_add(1, Ordering::SeqCst);
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
        self.bounded_reads.fetch_add(1, Ordering::SeqCst);
        limits.validate_keys(keys)?;
        if keys.len() >= 11 && keys[0].starts_with(b"ws/") {
            self.view_reads.fetch_add(1, Ordering::SeqCst);
        }
        let rows = self.rows.lock().await;
        let mut total = 0usize;
        for key in keys {
            let bytes = rows.get(key).map_or(0, Vec::len);
            total += key.len() + bytes;
            if bytes > limits.max_value_bytes || total > limits.max_total_bytes {
                return Err(pin_error("test point materialization exceeds byte limit"));
            }
        }
        Ok((
            keys.iter().map(|key| rows.get(key).cloned()).collect(),
            self.now.load(Ordering::SeqCst),
        ))
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.scan_prefix_bounded(prefix, usize::MAX).await
    }
    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        if limit == 0 {
            return Err(WorkspaceError::InvalidReadPlan("zero scan limit".into()));
        }
        Ok(self
            .rows
            .lock()
            .await
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .take(limit)
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
        self.cas(checks, writes, None, None).await
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate_scan_page(prefix, after)?;
        self.page_prefixes.lock().await.push(prefix.to_vec());
        let configured = self.page_size.load(Ordering::SeqCst);
        let cap = if configured > 0 {
            limits.max_records.min(configured as usize)
        } else {
            limits.max_records
        };
        let rows = self.rows.lock().await;
        let selected = || {
            rows.iter()
                .filter(|(key, _)| {
                    key.starts_with(prefix) && after.is_none_or(|after| key.as_slice() > after)
                })
                .take(cap)
        };
        let mut logical_bytes = 0usize;
        // The in-memory test transport has a fixed count header and two
        // length-prefixed byte strings per row. Check borrowed stored bytes
        // BEFORE cloning any response, just as its fixed envelope requires.
        let mut response_bytes = 8usize;
        for (key, value) in selected() {
            logical_bytes = logical_bytes
                .checked_add(key.len())
                .and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or_else(|| pin_error("test page logical byte overflow"))?;
            response_bytes = response_bytes
                .checked_add(16)
                .and_then(|bytes| bytes.checked_add(key.len()))
                .and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or_else(|| pin_error("test page response byte overflow"))?;
            if key.len() > limits.max_key_bytes
                || value.len() > limits.max_value_bytes
                || logical_bytes > limits.max_total_bytes
                || response_bytes > limits.max_response_bytes
            {
                return Err(pin_error("test page exceeds limit before materialization"));
            }
        }
        let page = selected()
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        drop(rows);
        // Pause only this page; other reads must be able to take the now-empty
        // hook while the simulated remote reply remains outstanding.
        let barrier = self.page_barrier.lock().await.take();
        if let Some((entered, resume)) = barrier {
            entered.notify_one();
            resume.notified().await;
        }
        Ok(page)
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate()?;
        let rows = self.rows.lock().await;
        let selected = || {
            rows.iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .take(limits.max_records)
        };
        let mut total = 0usize;
        for (key, value) in selected() {
            total += key.len() + value.len();
            if key.len() > limits.max_key_bytes
                || value.len() > limits.max_value_bytes
                || total > limits.max_total_bytes
            {
                return Err(pin_error("test scan materialization exceeds byte limit"));
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
        expiry: i64,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None, Some(expiry)).await
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        if lower.is_none() && upper.is_none() || lower.zip(upper).is_some_and(|(a, b)| a >= b) {
            return Err(WorkspaceError::InvalidReadPlan(
                "invalid time window".into(),
            ));
        }
        self.cas(checks, writes, lower, upper).await
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        expires_at_ns: i64,
        limits: crate::workspace_overlay::stores::kv_backend::KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        crate::workspace_overlay::stores::kv_backend::validate_bounded_authentication_checks(
            checks, limits,
        )?;
        crate::workspace_overlay::stores::kv_backend::validate_cas_time_window(
            None,
            Some(expires_at_ns),
        )?;
        self.cas_with_authentication_limits(checks, &[], None, Some(expires_at_ns), Some(limits))
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(self.now.load(Ordering::SeqCst))
    }
}

async fn setup<B: WorkspaceKvBackend>(store: &KvWorkspaceStore<B>) -> AcquirePackedReaderPin {
    let (_directory, _client, _snapshot, lower, _) = packed().await;
    let initial = request(store, lower).await;
    let binding = store
        .install_packed_lower_binding(initial.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: binding.head_epoch,
        ..initial.guard
    };
    let expected_layers = store
        .load_layer_chain(guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    AcquirePackedReaderPin {
        slot: 0,
        expected_slot_generation: 0,
        pin_id: Uuid::new_v4(),
        owner_id: "pin-contract-reader".into(),
        holder_generation: 1,
        ttl_ns: 1_000_000_000,
        gc_grace_ns: 500_000_000,
        guard,
        expected_layers,
        expected_binding: binding,
    }
}
fn memory_store(backend: PinMemoryBackend) -> KvWorkspaceStore<PinMemoryBackend> {
    KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(V3MountBudget::defaults())
}

#[tokio::test]
async fn packed_reader_corrupt_feature_and_scan_bytes_fail_closed_without_permit_leaks() {
    let backend = PinMemoryBackend::default();
    let budget = V3MountBudget::defaults();
    let store =
        KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    backend
        .rows
        .lock()
        .await
        .insert(FEATURE_KEY.to_vec(), vec![0; 2 << 20]);
    assert!(store.reap_expired_packed_readers().await.is_err());
    assert!(store.packed_reader_pin_roots().await.is_err());
    assert_eq!(budget.state().used[V3BudgetPool::Metadata as usize], 0);
    {
        let mut rows = backend.rows.lock().await;
        rows.insert(FEATURE_KEY.to_vec(), FEATURE.to_vec());
        rows.insert(COUNT_KEY.to_vec(), 1u64.to_le_bytes().to_vec());
        rows.insert(pin_key(0), vec![0; 2 << 20]);
        rows.insert(active_key(0), vec![0; 2 << 20]);
    }
    assert!(store.list_packed_reader_pin_slots().await.is_err());
    assert!(store.packed_reader_pin_roots().await.is_err());
    assert_eq!(budget.state().used[V3BudgetPool::Metadata as usize], 0);
    assert_eq!(
        backend.rows.lock().await.get(&pin_key(0)).unwrap().len(),
        2 << 20
    );
}

#[tokio::test]
async fn packed_reader_feature_probe_rejects_closed_admission_before_entering_backend() {
    let backend = PinMemoryBackend::default();
    let budget = V3MountBudget::defaults();
    let store =
        KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    // Every backend read blocks on this mutex. A read attempted before local
    // admission would time out instead of returning the closed-budget error.
    let _blocked_rows = backend.rows.lock().await;
    budget.close();
    let roots =
        tokio::time::timeout(Duration::from_millis(100), store.packed_reader_pin_roots()).await;
    assert!(matches!(roots, Ok(Err(WorkspaceError::InvalidReadPlan(_)))));
    let reaper = tokio::time::timeout(
        Duration::from_millis(100),
        store.reap_expired_packed_readers(),
    )
    .await;
    assert!(matches!(
        reaper,
        Ok(Err(WorkspaceError::InvalidReadPlan(_)))
    ));
    assert_eq!(budget.state().used[V3BudgetPool::Metadata as usize], 0);
}

#[tokio::test]
async fn packed_reader_reaper_requires_explicit_shared_budget_when_pins_exist() {
    let backend = PinMemoryBackend::default();
    let store = memory_store(backend.clone());
    let acquire = setup(&store).await;
    let pin = store.acquire_packed_reader_pin(&acquire).await.unwrap();
    backend
        .now
        .store(pin.expires_at_ns + pin.gc_grace_ns as i64, Ordering::SeqCst);
    let before = backend.rows.lock().await.clone();
    let peer = KvWorkspaceStore::new(backend.clone());
    assert!(matches!(
        peer.reap_packed_reader_sessions().await,
        Err(WorkspaceError::UnsupportedCapability(_))
    ));
    assert_eq!(*backend.rows.lock().await, before);
    assert!(peer.packed_reader_pin_budget.get().is_none());
    let canonical = store.packed_reader_pin_budget.get().unwrap().clone();
    peer.configure_packed_reader_pin_budget(canonical).unwrap();
    assert_eq!(peer.reap_packed_reader_sessions().await.unwrap(), 1);
    assert!(
        peer.packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .is_empty()
    );
}

#[tokio::test]
async fn packed_reader_pin_survives_publication_but_stale_new_acquisition_is_fenced() {
    let backend = PinMemoryBackend::default();
    let store = memory_store(backend.clone());
    let acquire = setup(&store).await;
    let pin = store.acquire_packed_reader_pin(&acquire).await.unwrap();
    let (_directory, _client, _snapshot, lower, _) = packed_with_snapshot_id([71; 32]).await;
    let publication = PublishPackedLowerBinding {
        guard: acquire.guard.clone(),
        expected_layers: acquire.expected_layers.clone(),
        expected_base: acquire.expected_binding.base_revision.clone(),
        expected_binding: acquire.expected_binding.clone(),
        lower,
    };
    let current = store
        .publish_packed_lower_binding(publication)
        .await
        .unwrap();
    assert_ne!(current.binding.manifest, pin.binding.binding.manifest);
    let peer = memory_store(backend.clone());
    let renewed = peer
        .renew_packed_reader_pin(&pin, Uuid::new_v4(), 2_000_000_000)
        .await
        .unwrap();
    assert_eq!(renewed.binding, acquire.expected_binding);
    assert_eq!(
        peer.validate_packed_reader_pin(&pin)
            .await
            .unwrap()
            .revision,
        renewed.revision
    );
    let stale_acquire = AcquirePackedReaderPin {
        slot: 1,
        pin_id: Uuid::new_v4(),
        ..acquire
    };
    assert!(matches!(
        peer.acquire_packed_reader_pin(&stale_acquire).await,
        Err(WorkspaceError::Fenced)
    ));
    let roots = peer.packed_reader_pin_roots().await.unwrap();
    assert_eq!(roots.bindings, vec![renewed.binding.clone()]);
    assert!(
        roots
            .native_roots
            .contains(&renewed.binding.base_revision.layer_id)
    );
    assert!(roots.native_roots.contains(&renewed.binding.head_layer_id));
}

#[tokio::test]
async fn packed_reader_expiry_and_grace_are_checked_at_locked_validation() {
    let backend = PinMemoryBackend::default();
    let store = memory_store(backend.clone());
    let mut acquire = setup(&store).await;
    acquire.ttl_ns = 100;
    acquire.gc_grace_ns = 50;
    let pin = store.acquire_packed_reader_pin(&acquire).await.unwrap();
    let unchanged = backend.rows.lock().await.clone();
    backend.now.store(pin.expires_at_ns - 1, Ordering::SeqCst);
    *backend.validation_clock.lock().await = Some(pin.expires_at_ns);
    assert!(matches!(
        store
            .renew_packed_reader_pin(&pin, Uuid::new_v4(), 100)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(*backend.rows.lock().await, unchanged);
    assert!(matches!(
        store.validate_packed_reader_pin(&pin).await,
        Err(WorkspaceError::Fenced)
    ));
    let cutoff = pin.expires_at_ns + 50;
    backend.now.store(cutoff - 1, Ordering::SeqCst);
    assert!(matches!(
        store.reap_packed_reader_pin(&pin, Uuid::new_v4()).await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(*backend.rows.lock().await, unchanged);
    assert_eq!(
        store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .len(),
        1
    );
    backend.now.store(cutoff + 1, Ordering::SeqCst);
    *backend.validation_clock.lock().await = Some(cutoff - 1);
    assert!(matches!(
        store.reap_packed_reader_pin(&pin, Uuid::new_v4()).await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(*backend.rows.lock().await, unchanged);
    *backend.validation_clock.lock().await = Some(cutoff);
    let reaped = store
        .reap_packed_reader_pin(&pin, Uuid::new_v4())
        .await
        .unwrap();
    assert_eq!(reaped.state, PackedReaderPinState::Reaped);
    assert!(
        store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .is_empty()
    );
    assert!(matches!(
        store
            .renew_packed_reader_pin(&pin, Uuid::new_v4(), 100)
            .await,
        Err(WorkspaceError::Fenced)
    ));
}

#[tokio::test]
async fn packed_reader_first_insert_fences_old_gc_and_pin_root_protects_native_deletion() {
    let backend = PinMemoryBackend::default();
    let store = memory_store(backend.clone());
    let acquire = setup(&store).await;
    let old_roots = store.packed_reader_pin_roots().await.unwrap();
    assert!(old_roots.bindings.is_empty());
    let pin = store.acquire_packed_reader_pin(&acquire).await.unwrap();
    assert!(
        !backend
            .compare_and_swap(&old_roots.checks, &[])
            .await
            .unwrap()
    );
    store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: pin.binding.workspace_id,
            force_fence_lease: true,
        })
        .await
        .unwrap();
    // Isolate the durable pin from independent PWB3 history/current roots.
    // Test-only corruption removes binding rows; no public history retirement
    // or full graph authorization is claimed by this fixture.
    {
        let mut rows = backend.rows.lock().await;
        rows.remove(&packed_current_key(pin.binding.workspace_id));
        rows.remove(&packed_history_key(pin.binding.workspace_id, 1));
        let generation =
            next_packed_root_generation(&rows.get(PACKED_ROOT_GENERATION_KEY).cloned()).unwrap();
        rows.insert(
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            encode(&generation).unwrap(),
        );
    }
    let snapshot = store.gc_snapshot(10_000_000_000, 0).await.unwrap();
    assert!(snapshot.root_layers.contains(&pin.binding.head_layer_id));
    assert!(
        snapshot
            .root_layers
            .contains(&pin.binding.base_revision.layer_id)
    );
    assert!(matches!(
        store
            .delete_layer_metadata(DeleteLayerMetadata {
                layer_ids: vec![
                    pin.binding.head_layer_id,
                    pin.binding.base_revision.layer_id
                ],
                now_ns: 10_000_000_000,
                lease_grace_ns: 0,
            })
            .await,
        Err(WorkspaceError::Busy)
    ));
}

#[tokio::test]
async fn packed_reader_slot_reuse_prevents_aba_and_bounds_terminal_storage() {
    let backend = PinMemoryBackend::default();
    let store = memory_store(backend.clone());
    let mut acquire = setup(&store).await;
    let first = store.acquire_packed_reader_pin(&acquire).await.unwrap();
    let old = (*first).clone();
    let mut terminal = store
        .release_packed_reader_pin(&first, Uuid::new_v4())
        .await
        .unwrap();
    for _ in 0..64 {
        acquire.expected_slot_generation = terminal.slot_generation;
        acquire.pin_id = Uuid::new_v4();
        let pin = store.acquire_packed_reader_pin(&acquire).await.unwrap();
        assert!(pin.slot_generation > terminal.slot_generation);
        terminal = store
            .release_packed_reader_pin(&pin, Uuid::new_v4())
            .await
            .unwrap();
    }
    assert!(matches!(
        store.validate_packed_reader_pin(&old).await,
        Err(WorkspaceError::Fenced)
    ));
    assert!(matches!(
        store.release_packed_reader_pin(&old, Uuid::new_v4()).await,
        Err(WorkspaceError::Fenced)
    ));
    let stale_request = AcquirePackedReaderPin {
        expected_slot_generation: 0,
        pin_id: old.pin_id,
        ..acquire
    };
    assert!(matches!(
        store.acquire_packed_reader_pin(&stale_request).await,
        Err(WorkspaceError::Busy)
    ));
    let listed = store.list_packed_reader_pin_slots().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].slot_generation, terminal.slot_generation);
    assert_eq!(
        backend
            .rows
            .lock()
            .await
            .keys()
            .filter(|key| key.starts_with(PIN_PREFIX))
            .count(),
        1
    );
    assert_eq!(
        backend
            .rows
            .lock()
            .await
            .keys()
            .filter(|key| key.starts_with(ACTIVE_PREFIX))
            .count(),
        0
    );
}

#[tokio::test]
async fn packed_reader_lost_responses_retry_without_advancing_generation_or_count() {
    let backend = PinMemoryBackend::default();
    let budget = V3MountBudget::defaults();
    let store =
        KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    let acquire = setup(&store).await;
    backend.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.acquire_packed_reader_pin(&acquire).await,
        Err(WorkspaceError::Backend(_))
    ));
    let after_acquire = backend.rows.lock().await.clone();
    let pin = store.acquire_packed_reader_pin(&acquire).await.unwrap();
    assert_eq!(*backend.rows.lock().await, after_acquire);
    assert_eq!(
        budget.state().used[V3BudgetPool::Metadata as usize],
        RECORD_BYTES as u64
    );
    let operation = Uuid::new_v4();
    backend.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        store
            .renew_packed_reader_pin(&pin, operation, 2_000_000_000)
            .await,
        Err(WorkspaceError::Backend(_))
    ));
    let after_renew = backend.rows.lock().await.clone();
    let renewed = store
        .renew_packed_reader_pin(&pin, operation, 2_000_000_000)
        .await
        .unwrap();
    assert_eq!(*backend.rows.lock().await, after_renew);
    let operation = Uuid::new_v4();
    backend.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.release_packed_reader_pin(&renewed, operation).await,
        Err(WorkspaceError::Backend(_))
    ));
    let after_release = backend.rows.lock().await.clone();
    let released = store
        .release_packed_reader_pin(&renewed, operation)
        .await
        .unwrap();
    assert_eq!(*backend.rows.lock().await, after_release);
    drop(pin);
    drop(renewed);
    drop(released);
    assert_eq!(budget.state().used[V3BudgetPool::Metadata as usize], 0);
}

#[tokio::test]
async fn packed_reader_reap_lost_response_cannot_reap_a_reused_slot() {
    let backend = PinMemoryBackend::default();
    let store = memory_store(backend.clone());
    let mut acquire = setup(&store).await;
    acquire.ttl_ns = 100;
    acquire.gc_grace_ns = 50;
    let pin = store.acquire_packed_reader_pin(&acquire).await.unwrap();
    backend.now.store(pin.expires_at_ns + 50, Ordering::SeqCst);
    let operation = Uuid::new_v4();
    backend.lose_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.reap_packed_reader_pin(&pin, operation).await,
        Err(WorkspaceError::Backend(_))
    ));
    let after_reap = backend.rows.lock().await.clone();
    let reaped = store.reap_packed_reader_pin(&pin, operation).await.unwrap();
    assert_eq!(*backend.rows.lock().await, after_reap);
    acquire.expected_slot_generation = reaped.slot_generation;
    acquire.pin_id = Uuid::new_v4();
    let winner = store.acquire_packed_reader_pin(&acquire).await.unwrap();
    let after_reuse = backend.rows.lock().await.clone();
    assert!(matches!(
        store.reap_packed_reader_pin(&pin, operation).await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(*backend.rows.lock().await, after_reuse);
    assert_eq!(
        store
            .validate_packed_reader_pin(&winner)
            .await
            .unwrap()
            .slot_generation,
        winner.slot_generation
    );
}

#[tokio::test]
async fn packed_reader_full_slot_capacity_rejects_new_grant_before_write() {
    let backend = PinMemoryBackend::default();
    let store = memory_store(backend.clone());
    let mut acquire = setup(&store).await;
    for slot in 0..PACKED_READER_SLOT_COUNT {
        acquire.slot = slot as u16;
        acquire.pin_id = Uuid::new_v4();
        store.acquire_packed_reader_pin(&acquire).await.unwrap();
    }
    let full = backend.rows.lock().await.clone();
    acquire.slot = 0;
    acquire.pin_id = Uuid::new_v4();
    assert!(matches!(
        store.acquire_packed_reader_pin(&acquire).await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(*backend.rows.lock().await, full);
    acquire.slot = PACKED_READER_SLOT_COUNT as u16;
    assert!(matches!(
        store.acquire_packed_reader_pin(&acquire).await,
        Err(WorkspaceError::InvalidReadPlan(_))
    ));
    assert_eq!(*backend.rows.lock().await, full);
    assert_eq!(
        store.list_packed_reader_pin_slots().await.unwrap().len(),
        PACKED_READER_SLOT_COUNT
    );
}

#[tokio::test]
async fn packed_reader_lifecycle_binding_open_shutdown_joins_and_drains_owners() {
    use crate::workspace_overlay::packed_reader_lifecycle::PackedReaderLeaseOptions;
    let backend = PinMemoryBackend::default();
    let store = Arc::new(memory_store(backend.clone()));
    let acquire = setup(&store).await;
    let session = store
        .clone()
        .open_packed_reader_session(
            acquire.guard,
            V3MountBudget::defaults(),
            PackedReaderLeaseOptions {
                ttl_ns: 100_000_000,
                gc_grace_ns: 50_000_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(session.binding(), &acquire.expected_binding.binding);
    let owner = session.retain_request().unwrap();
    session.validate().await.unwrap();
    let closing = session.clone();
    let close = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match session.retain_request() {
                Err(WorkspaceError::Fenced) => break,
                Ok(extra) => drop(extra),
                Err(error) => panic!("unexpected admission error: {error}"),
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !close.is_finished(),
        "shutdown released an owned generation before drain"
    );
    assert_eq!(
        store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .len(),
        1
    );
    drop(owner);
    close.await.unwrap().unwrap();
    assert!(
        store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .is_empty()
    );
    let finished = backend.rows.lock().await.clone();
    session.shutdown().await.unwrap();
    assert_eq!(*backend.rows.lock().await, finished);
}

#[tokio::test]
async fn packed_reader_lifecycle_heartbeat_renews_an_old_binding_after_publication() {
    use crate::workspace_overlay::packed_reader_lifecycle::PackedReaderLeaseOptions;
    let backend = PinMemoryBackend::default();
    let store = Arc::new(memory_store(backend.clone()));
    let acquire = setup(&store).await;
    let session = store
        .clone()
        .open_packed_reader_session(
            acquire.guard.clone(),
            V3MountBudget::defaults(),
            PackedReaderLeaseOptions {
                ttl_ns: 30_000_000,
                gc_grace_ns: 10_000_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (_directory, _client, _snapshot, lower, _) = packed_with_snapshot_id([81; 32]).await;
    let current = store
        .publish_packed_lower_binding(PublishPackedLowerBinding {
            guard: acquire.guard,
            expected_layers: acquire.expected_layers,
            expected_base: acquire.expected_binding.base_revision.clone(),
            expected_binding: acquire.expected_binding.clone(),
            lower,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let slots = store.list_packed_reader_pin_slots().await.unwrap();
            if slots[0].revision > 1 {
                assert_eq!(slots[0].binding, acquire.expected_binding);
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    session.validate().await.unwrap();
    assert_ne!(session.binding().manifest, current.binding.manifest);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_reader_cancelled_shutdown_retains_the_heartbeat_join_obligation() {
    use crate::workspace_overlay::packed_reader_lifecycle::PackedReaderLeaseOptions;
    let backend = PinMemoryBackend::default();
    let store = Arc::new(memory_store(backend.clone()));
    let acquire = setup(&store).await;
    let session = store
        .clone()
        .open_packed_reader_session(
            acquire.guard,
            V3MountBudget::defaults(),
            PackedReaderLeaseOptions {
                ttl_ns: 30_000_000,
                gc_grace_ns: 10_000_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let wait = entered.notified();
    tokio::pin!(wait);
    wait.as_mut().enable();
    *backend.reply_barrier.lock().await = Some((entered.clone(), resume.clone()));
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap();
    let closing = session.clone();
    let first_close = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while session.retain_request().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first_close.abort();
    assert!(first_close.await.unwrap_err().is_cancelled());
    let mut second_close = Box::pin(session.shutdown());
    // Renewal has committed and all catalog locks are free. A close that lost
    // the handle could inspect/release immediately; the actual join must wait
    // for the blocked response. Direct polling reaches that precise boundary.
    assert!(futures::poll!(second_close.as_mut()).is_pending());
    assert_eq!(
        store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .len(),
        1
    );
    resume.notify_one();
    tokio::time::timeout(Duration::from_secs(1), second_close)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .is_empty()
    );
}

#[tokio::test]
async fn packed_reader_abandoned_session_has_no_task_session_cycle_and_is_reapable() {
    use crate::workspace_overlay::packed_reader_lifecycle::{
        KvPackedReaderSession, PackedReaderLeaseOptions,
    };
    let backend = PinMemoryBackend::default();
    let store = Arc::new(memory_store(backend.clone()));
    let acquire = setup(&store).await;
    let session = KvPackedReaderSession::open(
        store.clone(),
        acquire.guard,
        V3MountBudget::defaults(),
        PackedReaderLeaseOptions {
            ttl_ns: 30_000_000,
            gc_grace_ns: 10_000_000,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let weak = Arc::downgrade(&session);
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let wait = entered.notified();
    tokio::pin!(wait);
    wait.as_mut().enable();
    *backend.cas_barrier.lock().await = Some((entered.clone(), resume.clone()));
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap();
    drop(session);
    assert!(
        weak.upgrade().is_none(),
        "heartbeat retained session across a blocked network CAS"
    );
    resume.notify_one();
    backend.now.store(100_000_000, Ordering::SeqCst);
    assert_eq!(store.reap_packed_reader_sessions().await.unwrap(), 1);
    assert!(
        store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .is_empty()
    );
}

#[tokio::test]
async fn packed_reader_delivery_validation_rechecks_local_stop_after_backend_success() {
    use crate::workspace_overlay::packed_reader_lifecycle::PackedReaderLeaseOptions;
    let backend = PinMemoryBackend::default();
    let store = Arc::new(memory_store(backend.clone()));
    let acquire = setup(&store).await;
    let session = store
        .clone()
        .open_packed_reader_session(
            acquire.guard,
            V3MountBudget::defaults(),
            PackedReaderLeaseOptions::default(),
        )
        .await
        .unwrap();
    let owner = session.retain_request().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let wait = entered.notified();
    tokio::pin!(wait);
    wait.as_mut().enable();
    *backend.cas_barrier.lock().await = Some((entered.clone(), resume.clone()));
    let validating = session.clone();
    let validate = tokio::spawn(async move { validating.validate().await });
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap();
    let closing = session.clone();
    let close = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match session.retain_request() {
                Err(WorkspaceError::Fenced) => break,
                Ok(extra) => drop(extra),
                Err(error) => panic!("unexpected request admission error: {error}"),
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    resume.notify_one();
    assert!(matches!(
        validate.await.unwrap(),
        Err(WorkspaceError::Fenced)
    ));
    assert!(!close.is_finished());
    drop(owner);
    close.await.unwrap().unwrap();
}

async fn real_contract<B: WorkspaceKvBackend>(backend: Arc<B>) {
    let store = KvWorkspaceStore::from_arc(backend.clone())
        .with_packed_reader_pin_budget(V3MountBudget::defaults());
    let mut acquire = setup(&store).await;
    acquire.ttl_ns = 1_000_000_000;
    acquire.gc_grace_ns = 0;
    let pin = store.acquire_packed_reader_pin(&acquire).await.unwrap();
    let peer = KvWorkspaceStore::from_arc(backend.clone())
        .with_packed_reader_pin_budget(V3MountBudget::defaults());
    let reopened = peer.validate_packed_reader_pin(&pin).await.unwrap();
    assert_eq!(*reopened, *pin);
    assert_eq!(peer.list_packed_reader_pin_slots().await.unwrap().len(), 1);
    assert_eq!(
        peer.packed_reader_pin_roots().await.unwrap().bindings[0],
        acquire.expected_binding
    );
    let renewed = peer
        .renew_packed_reader_pin(&pin, Uuid::new_v4(), 1_000_000_000)
        .await
        .unwrap();
    assert!(matches!(
        peer.reap_packed_reader_pin(&renewed, Uuid::new_v4()).await,
        Err(WorkspaceError::Busy)
    ));
    // Only this contract owns this fresh UUID namespace. Bounded server-time
    // polling supplies no reaper authority; the actual temporal CAS does.
    let mut reached = false;
    for _ in 0..30 {
        if backend.server_time_ns().await.unwrap() >= renewed.expires_at_ns {
            reached = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(reached, "test backend clock did not reach reader expiry");
    let reaped = peer
        .reap_packed_reader_pin(&renewed, Uuid::new_v4())
        .await
        .unwrap();
    assert_eq!(reaped.state, PackedReaderPinState::Reaped);
    assert!(
        peer.packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .is_empty()
    );
    assert!(matches!(
        peer.validate_packed_reader_pin(&renewed).await,
        Err(WorkspaceError::Fenced)
    ));
    acquire.expected_slot_generation = reaped.slot_generation;
    acquire.pin_id = Uuid::new_v4();
    let winner = peer.acquire_packed_reader_pin(&acquire).await.unwrap();
    assert!(matches!(
        peer.release_packed_reader_pin(&renewed, Uuid::new_v4())
            .await,
        Err(WorkspaceError::Fenced)
    ));
    peer.release_packed_reader_pin(&winner, Uuid::new_v4())
        .await
        .unwrap();
}
async fn isolated_contract<B: WorkspaceKvBackend>(backend: B) {
    let backend = Arc::new(backend);
    let worker = backend.clone();
    let outcome = tokio::spawn(async move { real_contract(worker).await }).await;
    let entries = backend
        .scan_prefix(b"")
        .await
        .expect("owned namespace cleanup scan");
    let checks: Vec<_> = entries
        .iter()
        .map(|entry| KvCheck {
            key: entry.key.clone(),
            expected: Some(entry.value.clone()),
        })
        .collect();
    let writes: Vec<_> = entries
        .into_iter()
        .map(|entry| KvWrite::Delete { key: entry.key })
        .collect();
    assert!(
        backend
            .compare_and_swap(&checks, &writes)
            .await
            .expect("owned namespace cleanup CAS")
    );
    match outcome {
        Ok(()) => {}
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("reader pin contract cancelled: {error}"),
    }
}
#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL; owns one UUID namespace"]
async fn real_redis_packed_reader_pin_reopen_and_crashed_reader_expiry() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let namespace = format!("g12-reader-pins-{}", Uuid::new_v4());
    isolated_contract(
        RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap(),
    )
    .await;
}
#[tokio::test]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS; owns one UUID namespace"]
async fn real_tikv_packed_reader_pin_reopen_and_crashed_reader_expiry() {
    let endpoints: Vec<String> = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .expect("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .split(',')
        .map(str::to_owned)
        .collect();
    let namespace = format!("g12-reader-pins-{}", Uuid::new_v4());
    isolated_contract(
        TiKvWorkspaceBackend::connect(endpoints, &namespace)
            .await
            .unwrap(),
    )
    .await;
}
