//! Native lease retirement behavior under an actual backend validation clock.
//! Typed catalog/PPJ rows test ownership, not authenticated packed graph proof.

use super::*;
use crate::workspace_overlay::stores::kv_store::packed_journal::tests::JournalMemoryBackend;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Default)]
struct LeaseMemoryBackend {
    memory: JournalMemoryBackend,
    now: AtomicI64,
    lose_reply: AtomicBool,
    release_attempts: AtomicUsize,
    release_commits: AtomicUsize,
    force_release_clock: AtomicI64,
    short_point_read: AtomicBool,
    fail_page: AtomicBool,
    before_release: Mutex<Option<Vec<KvWrite>>>,
    after_lost_reply: Mutex<Option<Vec<KvWrite>>>,
    cancel_after_page: Mutex<Option<CancellationToken>>,
    pause_next_write: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Semaphore>)>>,
}

#[async_trait]
impl WorkspaceKvBackend for LeaseMemoryBackend {
    fn name(&self) -> &'static str {
        "native-lease-retirement-memory-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        self.memory.get(key).await
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.memory.get_many_consistent(keys).await
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        Ok((
            self.memory.get_many_consistent(keys).await?,
            self.now.load(Ordering::SeqCst),
        ))
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let (mut rows, _) = self
            .memory
            .get_many_consistent_with_time_bounded(keys, limits)
            .await?;
        if self.short_point_read.swap(false, Ordering::SeqCst) {
            rows.pop();
        }
        Ok((rows, self.now.load(Ordering::SeqCst)))
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.memory.scan_prefix(prefix).await
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.memory
            .scan_prefix_with_byte_limits(prefix, limits)
            .await
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        if self.fail_page.swap(false, Ordering::SeqCst) {
            return Err(WorkspaceError::Backend(
                "injected incomplete protective hold page".into(),
            ));
        }
        let page = self
            .memory
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await?;
        if let Some(cancel) = self.cancel_after_page.lock().await.take() {
            cancel.cancel();
        }
        Ok(page)
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.compare_and_swap_in_time_window(checks, writes, None, None)
            .await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        expires_at_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        self.compare_and_swap_in_time_window(checks, writes, None, Some(expires_at_ns))
            .await
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        let releasing = writes.iter().any(|write| match write {
            KvWrite::Put { key, value } if key.starts_with(HOT_LEASE_PREFIX) => {
                decode::<SnapshotLease>(value)
                    .is_ok_and(|lease| lease.state == LeaseState::Released)
            }
            _ => false,
        });
        if releasing {
            self.release_attempts.fetch_add(1, Ordering::SeqCst);
            let forced = self.force_release_clock.load(Ordering::SeqCst);
            if forced > 0 {
                self.now.store(forced, Ordering::SeqCst);
            }
        }
        if !writes.is_empty()
            && let Some((entered, gate)) = self.pause_next_write.lock().await.take()
        {
            entered.notify_one();
            gate.acquire().await.unwrap().forget();
        }
        let mut rows = self.memory.rows.lock().await;
        if releasing && let Some(changes) = self.before_release.lock().await.take() {
            for change in changes {
                match change {
                    KvWrite::Put { key, value } => {
                        rows.insert(key, value);
                    }
                    KvWrite::Delete { key } => {
                        rows.remove(&key);
                    }
                }
            }
        }
        let now = self.now.load(Ordering::SeqCst);
        if lower.is_some_and(|bound| now < bound)
            || upper.is_some_and(|bound| now >= bound)
            || checks
                .iter()
                .any(|check| rows.get(&check.key) != check.expected.as_ref())
        {
            return Ok(false);
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
        if releasing {
            // The atomic successor must remove its hold and advance the epoch.
            for write in writes {
                if let KvWrite::Put { key, value } = write
                    && key.starts_with(HOT_LEASE_PREFIX)
                {
                    let lease: SnapshotLease = decode(value)?;
                    if lease.state == LeaseState::Released {
                        let old = checks
                            .iter()
                            .find(|check| check.key == *key)
                            .and_then(|check| check.expected.as_deref())
                            .map(decode::<SnapshotLease>)
                            .transpose()?
                            .ok_or(WorkspaceError::Fenced)?;
                        assert!(!rows.contains_key(&NativeHold::lease(&old).unwrap().key()));
                        let epoch = checks
                            .iter()
                            .find(|check| check.key == PACKED_ROOT_GENERATION_KEY)
                            .and_then(|check| check.expected.as_deref())
                            .map(decode::<u64>)
                            .transpose()?
                            .unwrap();
                        assert_eq!(
                            decode::<u64>(rows.get(PACKED_ROOT_GENERATION_KEY).unwrap())?,
                            epoch + 1
                        );
                    }
                }
            }
            self.release_commits.fetch_add(1, Ordering::SeqCst);
        }
        drop(rows);
        if !writes.is_empty() && self.lose_reply.swap(false, Ordering::SeqCst) {
            if let Some(changes) = self.after_lost_reply.lock().await.take() {
                let mut rows = self.memory.rows.lock().await;
                for change in changes {
                    match change {
                        KvWrite::Put { key, value } => {
                            rows.insert(key, value);
                        }
                        KvWrite::Delete { key } => {
                            rows.remove(&key);
                        }
                    }
                }
            }
            return Err(WorkspaceError::Backend(
                "injected committed lease retirement reply loss".into(),
            ));
        }
        Ok(true)
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
        let rows = self.memory.rows.lock().await;
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
        if checks
            .iter()
            .any(|check| rows.get(&check.key) != check.expected.as_ref())
        {
            return Ok(false);
        }
        if self.now.load(Ordering::SeqCst) >= expires_at_ns {
            return Err(WorkspaceError::Fenced);
        }
        Ok(true)
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(self.now.load(Ordering::SeqCst))
    }
}

#[derive(serde::Serialize)]
struct TestGate {
    run: Uuid,
    active: bool,
}

fn options(lease_id: LeaseId) -> PackedNativeLeaseReaperOptions {
    PackedNativeLeaseReaperOptions {
        lease_id,
        grace_ns: 100,
        max_protective_rows: 32,
        cancel: CancellationToken::new(),
    }
}

struct Fixture {
    backend: Arc<LeaseMemoryBackend>,
    store: Arc<KvWorkspaceStore<LeaseMemoryBackend>>,
    budget: Arc<V3MountBudget>,
    workspace: WorkspaceRecord,
    head: LayerRecord,
    base: LayerRecord,
    lease: SnapshotLease,
}

impl Fixture {
    async fn new() -> Self {
        let backend = Arc::new(LeaseMemoryBackend::default());
        backend.now.store(900, Ordering::SeqCst);
        backend.memory.page_size.store(1, Ordering::SeqCst);
        let store = Arc::new(KvWorkspaceStore::from_arc(backend.clone()));
        let budget = V3MountBudget::defaults();
        store
            .configure_packed_reader_pin_budget(budget.clone())
            .unwrap();
        let base = LayerRecord {
            layer_id: LayerId::new(),
            parent_layer_id: None,
            state: LayerState::Sealed,
            schema_version: WORKSPACE_SCHEMA_VERSION,
            sealed_version: Some(1),
            delta_digest: Some([41; 32]),
            root_hash: Some([42; 32]),
            depth: 1,
            owner_workspace_id: None,
            next_sequence: 1,
            owned_slice_count: 0,
            owned_bytes: 0,
            created_at_ns: 1,
            sealed_at_ns: Some(2),
        };
        let revision = revision_from_layer(&base).unwrap();
        let mut workspace = WorkspaceRecord {
            upper_bucket: None,
            workspace_id: WorkspaceId::new(),
            head_layer_id: LayerId::new(),
            head_epoch: 1,
            fork_base: Some(revision.clone()),
            active_lease: None,
            owner_id: Some("native-lease-reaper-behavior".into()),
            state: WorkspaceState::Active,
            created_at_ns: 3,
            updated_at_ns: 3,
        };
        let head = writable_layer(
            workspace.head_layer_id,
            base.layer_id,
            2,
            workspace.workspace_id,
            3,
        );
        let lease = SnapshotLease {
            lease_id: LeaseId::new(),
            workspace_id: workspace.workspace_id,
            base_revision: revision,
            holder_generation: 1,
            writable: true,
            state: LeaseState::Active,
            expires_at_ns: 1000,
            created_at_ns: 3,
            updated_at_ns: 3,
        };
        workspace.active_lease = Some(lease.lease_id);
        let mut control = ControlState::default();
        control
            .workspaces
            .insert(workspace.workspace_id, workspace.clone());
        for layer in [&base, &head] {
            control.layers.insert(layer.layer_id, layer.clone());
        }
        control.leases.insert(lease.lease_id, lease.clone());
        let workspace_hold = NativeHold::workspace(&workspace).unwrap();
        let lease_hold = NativeHold::lease(&lease).unwrap();
        backend.memory.rows.lock().await.extend([
            (
                hot_lease_index_key(lease.lease_id),
                encode(&workspace.workspace_id).unwrap(),
            ),
            (
                hot_workspace_key(workspace.workspace_id),
                encode(&workspace).unwrap(),
            ),
            (hot_layer_key(base.layer_id), encode(&base).unwrap()),
            (hot_layer_key(head.layer_id), encode(&head).unwrap()),
            (
                hot_lease_key(lease.workspace_id, lease.lease_id),
                encode(&lease).unwrap(),
            ),
            (workspace_hold.key(), workspace_hold.encode().unwrap()),
            (lease_hold.key(), lease_hold.encode().unwrap()),
            (
                HOLD_FEATURE.to_vec(),
                encode(&TestGate {
                    run: Uuid::new_v4(),
                    active: true,
                })
                .unwrap(),
            ),
            (PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&1u64).unwrap()),
            (
                LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                encode(&1u64).unwrap(),
            ),
        ]);
        test_write_topology_rows(&mut *backend.memory.rows.lock().await, &control);
        Self {
            backend,
            store,
            budget,
            workspace,
            head,
            base,
            lease,
        }
    }

    async fn run(&self) -> Result<PackedNativeLeaseReaperReport, WorkspaceError> {
        self.store
            .reap_packed_native_lease(self.budget.clone(), options(self.lease.lease_id))
            .await
    }

    async fn actual_lease(&self) -> SnapshotLease {
        decode(
            &self
                .backend
                .get(&hot_lease_key(self.lease.workspace_id, self.lease.lease_id))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap()
    }

    async fn assert_retained(&self) {
        let lease = self.actual_lease().await;
        assert_ne!(lease.state, LeaseState::Released);
        let hold = NativeHold::lease(&lease).unwrap();
        assert_eq!(
            self.backend.get(&hold.key()).await.unwrap(),
            Some(hold.encode().unwrap())
        );
        assert_eq!(self.backend.release_commits.load(Ordering::SeqCst), 0);
        assert_eq!(self.budget.state().used[V3BudgetPool::Metadata as usize], 0);
    }

    async fn observe_then_expire(&self) {
        let report = self.run().await.unwrap();
        assert!(report.observing && !report.reaped);
        assert_eq!(report.not_before_ns, 1100);
        self.backend.now.store(1100, Ordering::SeqCst);
    }

    async fn install_journal(&self, workspace: WorkspaceId, phase: SealPhase) -> SealJournal {
        let mut rows = self.backend.memory.rows.lock().await;
        let mut control = test_topology_from_rows(&rows);
        let (head, epoch) = if workspace == self.workspace.workspace_id {
            (self.head.layer_id, self.workspace.head_epoch)
        } else {
            let mut owner = self.workspace.clone();
            owner.workspace_id = workspace;
            owner.head_layer_id = LayerId::new();
            let head = writable_layer(owner.head_layer_id, self.base.layer_id, 2, workspace, 3);
            let hold = NativeHold::workspace(&owner).unwrap();
            rows.insert(hot_workspace_key(workspace), encode(&owner).unwrap());
            rows.insert(hot_layer_key(head.layer_id), encode(&head).unwrap());
            rows.insert(hold.key(), hold.encode().unwrap());
            control.workspaces.insert(workspace, owner.clone());
            control.layers.insert(head.layer_id, head);
            (owner.head_layer_id, owner.head_epoch)
        };
        let journal = SealJournal {
            journal_id: JournalId::new(),
            workspace_id: workspace,
            old_head_layer_id: head,
            expected_head_epoch: epoch,
            phase,
            pending_bytes: 0,
            delta_digest: None,
            root_hash: None,
            new_head_layer_id: None,
            last_error: None,
            created_at_ns: 3,
            updated_at_ns: 3,
        };
        control.journals.insert(journal.journal_id, journal.clone());
        test_write_topology_rows(&mut rows, &control);
        if let Some(hold) = NativeHold::journal(&journal) {
            rows.insert(hold.key(), hold.encode().unwrap());
        }
        let epoch: u64 = decode(rows.get(PACKED_ROOT_GENERATION_KEY).unwrap()).unwrap();
        rows.insert(
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            encode(&(epoch + 1)).unwrap(),
        );
        journal
    }
}

fn is_protected(error: WorkspaceError) -> bool {
    matches!(error, WorkspaceError::Busy | WorkspaceError::Fenced)
}

#[tokio::test]
async fn native_lease_reaper_backend_expiry_and_grace_boundary_are_atomic() {
    let fixture = Fixture::new().await;
    fixture.observe_then_expire().await;
    fixture.backend.now.store(1000, Ordering::SeqCst);
    assert!(fixture.run().await.unwrap().observing);
    fixture.backend.now.store(1099, Ordering::SeqCst);
    assert!(fixture.run().await.unwrap().observing);
    fixture.assert_retained().await;
    fixture.backend.now.store(1100, Ordering::SeqCst);
    let report = fixture.run().await.unwrap();
    assert!(report.reaped && !report.observing);
    assert_eq!(report.not_before_ns, 1100);
    assert_eq!(fixture.actual_lease().await.state, LeaseState::Released);
    assert!(
        fixture
            .backend
            .get(&NativeHold::lease(&fixture.lease).unwrap().key())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fixture.backend.release_commits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_lease_reaper_first_observation_never_skips_durable_grace_policy() {
    let fixture = Fixture::new().await;
    fixture.backend.now.store(5000, Ordering::SeqCst);
    let report = fixture.run().await.unwrap();
    assert!(report.observing && !report.reaped);
    assert!(
        fixture
            .backend
            .get(&policy_key(fixture.lease.lease_id))
            .await
            .unwrap()
            .is_some()
    );
    fixture.assert_retained().await;
    assert!(fixture.run().await.unwrap().reaped);
}

#[tokio::test]
async fn native_lease_reaper_sampled_time_cannot_override_final_backend_clock() {
    let fixture = Fixture::new().await;
    fixture.observe_then_expire().await;
    fixture
        .backend
        .force_release_clock
        .store(1099, Ordering::SeqCst);
    let result = fixture.run().await;
    assert!(result.is_err() || result.is_ok_and(|report| !report.reaped));
    assert!(fixture.backend.release_attempts.load(Ordering::SeqCst) > 0);
    fixture.assert_retained().await;
    fixture
        .backend
        .force_release_clock
        .store(0, Ordering::SeqCst);
    fixture.backend.now.store(1100, Ordering::SeqCst);
    assert!(fixture.run().await.unwrap().reaped);
}

#[tokio::test]
async fn native_lease_reaper_durable_grace_cannot_be_shortened_or_replaced() {
    let fixture = Fixture::new().await;
    fixture.observe_then_expire().await;
    let before = fixture.backend.memory.rows.lock().await.clone();
    for grace in [1, 101] {
        let mut changed = options(fixture.lease.lease_id);
        changed.grace_ns = grace;
        assert!(matches!(
            fixture
                .store
                .reap_packed_native_lease(fixture.budget.clone(), changed)
                .await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(*fixture.backend.memory.rows.lock().await, before);
    }
    fixture.assert_retained().await;
}

#[tokio::test]
async fn native_lease_reaper_actual_renewal_extends_observation_without_changing_grace() {
    let fixture = Fixture::new().await;
    fixture.run().await.unwrap();
    fixture.backend.now.store(950, Ordering::SeqCst);
    let renewed = fixture
        .store
        .renew_lease(RenewLease {
            lease_id: fixture.lease.lease_id,
            holder_generation: fixture.lease.holder_generation,
            ttl_ns: 250,
        })
        .await
        .unwrap();
    assert_eq!(renewed.expires_at_ns, 1200);
    let report = fixture.run().await.unwrap();
    assert!(report.observing && !report.reaped);
    assert_eq!(report.not_before_ns, 1300);
    fixture.backend.now.store(1299, Ordering::SeqCst);
    assert!(fixture.run().await.unwrap().observing);
    fixture.assert_retained().await;
    fixture.backend.now.store(1300, Ordering::SeqCst);
    assert!(fixture.run().await.unwrap().reaped);
}

#[tokio::test]
async fn native_lease_reaper_actual_short_renewal_cannot_lower_observed_expiry_watermark() {
    let fixture = Fixture::new().await;
    fixture.run().await.unwrap();
    fixture.backend.now.store(950, Ordering::SeqCst);
    let renewed = fixture
        .store
        .renew_lease(RenewLease {
            lease_id: fixture.lease.lease_id,
            holder_generation: fixture.lease.holder_generation,
            ttl_ns: 20,
        })
        .await
        .unwrap();
    assert_eq!(renewed.expires_at_ns, 970);
    assert_eq!(fixture.run().await.unwrap().not_before_ns, 1100);
    fixture.backend.now.store(1099, Ordering::SeqCst);
    assert!(fixture.run().await.unwrap().observing);
    fixture.assert_retained().await;
    fixture.backend.now.store(1100, Ordering::SeqCst);
    assert!(fixture.run().await.unwrap().reaped);
}

#[tokio::test]
async fn native_lease_reaper_holder_replacement_and_sidecar_corruption_are_not_zero() {
    for mutation in 0..4 {
        let fixture = Fixture::new().await;
        fixture.observe_then_expire().await;
        let mut lease = fixture.lease.clone();
        let mut rows = fixture.backend.memory.rows.lock().await;
        match mutation {
            0 => {
                lease.holder_generation += 1;
                rows.insert(
                    hot_lease_key(lease.workspace_id, lease.lease_id),
                    encode(&lease).unwrap(),
                );
                let hold = NativeHold::lease(&lease).unwrap();
                rows.insert(hold.key(), hold.encode().unwrap());
            }
            1 => {
                lease.created_at_ns += 1;
                rows.insert(
                    hot_lease_key(lease.workspace_id, lease.lease_id),
                    encode(&lease).unwrap(),
                );
            }
            2 => {
                let mut sidecar = lease.clone();
                sidecar.base_revision.root_hash[0] ^= 1;
                let hold = NativeHold::lease(&sidecar).unwrap();
                rows.insert(hold.key(), hold.encode().unwrap());
            }
            3 => {
                let hold = NativeHold::lease(&lease).unwrap();
                rows.remove(&hold.key());
            }
            _ => unreachable!(),
        }
        let before = rows.clone();
        drop(rows);
        assert!(fixture.run().await.is_err());
        assert_eq!(*fixture.backend.memory.rows.lock().await, before);
        assert_ne!(fixture.actual_lease().await.state, LeaseState::Released);
        assert_eq!(fixture.backend.release_commits.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn native_lease_reaper_quiescing_sealing_and_incomplete_recovery_keep_hold() {
    for mode in 0..5 {
        let fixture = Fixture::new().await;
        fixture.observe_then_expire().await;
        let mut rows = fixture.backend.memory.rows.lock().await;
        match mode {
            0..=2 => {
                let mut workspace = fixture.workspace.clone();
                workspace.state = [
                    WorkspaceState::Quiescing,
                    WorkspaceState::Sealing,
                    WorkspaceState::Error,
                ][mode];
                rows.insert(
                    hot_workspace_key(workspace.workspace_id),
                    encode(&workspace).unwrap(),
                );
                let hold = NativeHold::workspace(&workspace).unwrap();
                rows.insert(hold.key(), hold.encode().unwrap());
            }
            3 => {
                let mut head = fixture.head.clone();
                head.state = LayerState::Sealing;
                rows.insert(hot_layer_key(head.layer_id), encode(&head).unwrap());
            }
            4 => {
                rows.insert(
                    open_v3_recovery_key(fixture.workspace.workspace_id),
                    encode(&V3RecoveryRecord {
                        workspace_id: fixture.workspace.workspace_id,
                        incomplete: true,
                    })
                    .unwrap(),
                );
            }
            _ => unreachable!(),
        }
        drop(rows);
        assert!(is_protected(fixture.run().await.unwrap_err()));
        fixture.assert_retained().await;
        let hold = NativeHold::workspace(&fixture.workspace).unwrap();
        let mut rows = fixture.backend.memory.rows.lock().await;
        rows.insert(
            hot_workspace_key(fixture.workspace.workspace_id),
            encode(&fixture.workspace).unwrap(),
        );
        rows.insert(
            hot_layer_key(fixture.head.layer_id),
            encode(&fixture.head).unwrap(),
        );
        rows.insert(hold.key(), hold.encode().unwrap());
        rows.insert(
            open_v3_recovery_key(fixture.workspace.workspace_id),
            encode(&V3RecoveryRecord {
                workspace_id: fixture.workspace.workspace_id,
                incomplete: false,
            })
            .unwrap(),
        );
        drop(rows);
        assert!(fixture.run().await.unwrap().reaped);
    }
}

#[tokio::test]
async fn native_lease_reaper_native_prepare_before_pnb_and_all_nonterminal_phases_protect() {
    for phase in [
        SealPhase::Prepare,
        SealPhase::Quiesced,
        SealPhase::DataDrained,
        SealPhase::Hashed,
        SealPhase::HeadSwitched,
    ] {
        let fixture = Fixture::new().await;
        fixture.observe_then_expire().await;
        let journal = fixture
            .install_journal(fixture.workspace.workspace_id, phase)
            .await;
        assert!(
            fixture
                .backend
                .get(JOURNAL_FEATURE_KEY)
                .await
                .unwrap()
                .is_none()
        );
        assert!(is_protected(fixture.run().await.unwrap_err()));
        fixture.assert_retained().await;
        assert!(
            fixture
                .backend
                .get(&NativeHold::journal(&journal).unwrap().key())
                .await
                .unwrap()
                .is_some()
        );
    }
    for phase in [SealPhase::Completed, SealPhase::Aborted] {
        let fixture = Fixture::new().await;
        fixture.observe_then_expire().await;
        fixture
            .install_journal(fixture.workspace.workspace_id, phase)
            .await;
        assert!(fixture.run().await.unwrap().reaped);
    }
}

async fn install_active_ppj(fixture: &Fixture) -> PackedJournalRecord {
    let reference = V3ObjectRef::from_bytes(
        "packed-v3/lease-tests/manifest".into(),
        V3ObjectKind::Manifest,
        &crate::workspace_overlay::packed_v3::wire005::encode_v3_object(
            V3ObjectKind::Manifest,
            b"x",
            1,
        )
        .unwrap(),
    )
    .unwrap();
    let guard = HeadGuard {
        workspace_id: fixture.workspace.workspace_id,
        expected_head_layer_id: fixture.head.layer_id,
        expected_head_epoch: fixture.workspace.head_epoch,
        lease_id: fixture.lease.lease_id,
        holder_generation: fixture.lease.holder_generation,
    };
    let binding = PackedLowerBindingRecord {
        workspace_id: guard.workspace_id,
        head_layer_id: guard.expected_head_layer_id,
        head_epoch: guard.expected_head_epoch,
        base_revision: fixture.lease.base_revision.clone(),
        highest_inode: 2,
        binding: PackedLowerBinding {
            binding_version: 1,
            base_layer_id: fixture.base.layer_id,
            manifest: reference,
        },
    };
    let record = PackedJournalRecord {
        journal_id: JournalId::new(),
        revision: 1,
        phase: PackedJournalPhase::Building,
        guard,
        source: PackedSourceView {
            snapshot_backed: false,
            effective_view_digest: [11; 32],
            frozen_view_token: [12; 32],
            build_provenance_digest: [13; 32],
            build_owner: "lease-retirement-typed-ppj".into(),
            staging_id: Uuid::new_v4(),
            staging_prefix: "lease-retirement-typed-ppj/build".into(),
        },
        expected_head: encode(&fixture.head).unwrap(),
        expected_base: encode(&fixture.base).unwrap(),
        expected_binding: binding,
        object_count: 0,
        inventory_digest: inventory_start(),
        commit_target: None,
        full_proof_digest: [0; 32],
        graph_receipt: None,
        native_rebind: None,
        abort_reason: String::new(),
    };
    let raw = record.encode().unwrap();
    fixture.backend.memory.rows.lock().await.extend([
        (JOURNAL_FEATURE_KEY.to_vec(), b"PPJ3".to_vec()),
        (ACTIVE_COUNT_KEY.to_vec(), 1u64.to_le_bytes().to_vec()),
        (journal_key(record.journal_id), raw.clone()),
        (active_key(record.journal_id), raw),
        (PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&3u64).unwrap()),
    ]);
    record
}

#[tokio::test]
async fn native_lease_reaper_actual_active_ppj_protects_same_workspace_without_native_journal() {
    let fixture = Fixture::new().await;
    fixture.observe_then_expire().await;
    let record = install_active_ppj(&fixture).await;
    assert!(is_protected(fixture.run().await.unwrap_err()));
    fixture.assert_retained().await;
    let mut terminal = record;
    terminal.revision += 1;
    terminal.phase = PackedJournalPhase::Aborted;
    terminal.abort_reason = "typed lease retirement fixture abort".into();
    let mut rows = fixture.backend.memory.rows.lock().await;
    rows.insert(journal_key(terminal.journal_id), terminal.encode().unwrap());
    rows.remove(&active_key(terminal.journal_id));
    rows.insert(ACTIVE_COUNT_KEY.to_vec(), 0u64.to_le_bytes().to_vec());
    rows.insert(PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&4u64).unwrap());
    drop(rows);
    assert!(fixture.run().await.unwrap().reaped);
}

#[tokio::test]
async fn native_lease_reaper_missing_journal_sentinels_cannot_hide_actual_active_ppj() {
    for retain_active_mirror in [true, false] {
        let fixture = Fixture::new().await;
        fixture.observe_then_expire().await;
        let record = install_active_ppj(&fixture).await;
        let mut rows = fixture.backend.memory.rows.lock().await;
        rows.remove(JOURNAL_FEATURE_KEY);
        rows.remove(ACTIVE_COUNT_KEY);
        assert_eq!(
            rows.get(&journal_key(record.journal_id)),
            Some(&record.encode().unwrap())
        );
        if retain_active_mirror {
            assert_eq!(
                rows.get(&active_key(record.journal_id)),
                Some(&record.encode().unwrap())
            );
        } else {
            rows.remove(&active_key(record.journal_id));
        }
        let before = rows.clone();
        drop(rows);
        assert!(matches!(fixture.run().await, Err(WorkspaceError::Fenced)));
        assert_eq!(*fixture.backend.memory.rows.lock().await, before);
        fixture.assert_retained().await;
        assert_eq!(fixture.backend.release_attempts.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn native_lease_reaper_logical_expiry_is_not_native_hold_retirement() {
    let fixture = Fixture::new().await;
    fixture.backend.now.store(1000, Ordering::SeqCst);
    assert_eq!(fixture.store.reap_expired_leases().await.unwrap(), 1);
    assert_eq!(fixture.actual_lease().await.state, LeaseState::Expired);
    fixture.assert_retained().await;
    assert!(fixture.run().await.unwrap().observing);
    fixture.backend.now.store(1099, Ordering::SeqCst);
    assert!(fixture.run().await.unwrap().observing);
    fixture.assert_retained().await;
    fixture.backend.now.store(1100, Ordering::SeqCst);
    assert!(fixture.run().await.unwrap().reaped);
}

#[tokio::test]
async fn native_lease_reaper_page_quota_backend_failure_and_cancel_never_mean_empty() {
    for mode in 0..3 {
        let fixture = Fixture::new().await;
        fixture.observe_then_expire().await;
        fixture
            .install_journal(WorkspaceId::new(), SealPhase::Prepare)
            .await;
        fixture
            .install_journal(WorkspaceId::new(), SealPhase::Prepare)
            .await;
        let mut operation = options(fixture.lease.lease_id);
        match mode {
            0 => operation.max_protective_rows = 1,
            1 => fixture.backend.fail_page.store(true, Ordering::SeqCst),
            2 => *fixture.backend.cancel_after_page.lock().await = Some(operation.cancel.clone()),
            _ => unreachable!(),
        }
        assert!(
            fixture
                .store
                .reap_packed_native_lease(fixture.budget.clone(), operation)
                .await
                .is_err()
        );
        fixture.assert_retained().await;
        assert!(fixture.run().await.unwrap().reaped);
        assert!(fixture.backend.memory.page_calls.load(Ordering::SeqCst) >= 3);
    }
}

#[tokio::test]
async fn native_lease_reaper_cancelled_or_zero_quota_operation_does_not_create_policy() {
    for cancelled in [false, true] {
        let fixture = Fixture::new().await;
        let mut operation = options(fixture.lease.lease_id);
        if cancelled {
            operation.cancel.cancel();
        } else {
            operation.max_protective_rows = 0;
        }
        let before = fixture.backend.memory.rows.lock().await.clone();
        assert!(
            fixture
                .store
                .reap_packed_native_lease(fixture.budget.clone(), operation)
                .await
                .is_err()
        );
        assert_eq!(*fixture.backend.memory.rows.lock().await, before);
        fixture.assert_retained().await;
    }
}

#[tokio::test]
async fn native_lease_reaper_truncated_bounded_read_does_not_consume_observed_hold() {
    let fixture = Fixture::new().await;
    fixture.observe_then_expire().await;
    fixture
        .backend
        .short_point_read
        .store(true, Ordering::SeqCst);
    let before = fixture.backend.memory.rows.lock().await.clone();
    assert!(fixture.run().await.is_err());
    assert_eq!(*fixture.backend.memory.rows.lock().await, before);
    fixture.assert_retained().await;
}

#[tokio::test]
async fn native_lease_reaper_missing_gate_epoch_recovery_or_actual_base_is_not_zero() {
    for mode in 0..6 {
        let fixture = Fixture::new().await;
        fixture.observe_then_expire().await;
        let mut rows = fixture.backend.memory.rows.lock().await;
        match mode {
            0 => {
                rows.remove(HOLD_FEATURE);
            }
            1 => {
                rows.insert(
                    HOLD_FEATURE.to_vec(),
                    encode(&TestGate {
                        run: Uuid::new_v4(),
                        active: false,
                    })
                    .unwrap(),
                );
            }
            2 => {
                rows.insert(
                    LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                    encode(&0u64).unwrap(),
                );
            }
            3 => {
                rows.insert(
                    open_v3_recovery_key(fixture.workspace.workspace_id),
                    encode(&V3RecoveryRecord {
                        workspace_id: WorkspaceId::new(),
                        incomplete: false,
                    })
                    .unwrap(),
                );
            }
            4 => {
                let mut base = fixture.base.clone();
                base.root_hash.as_mut().unwrap()[0] ^= 1;
                rows.insert(hot_layer_key(base.layer_id), encode(&base).unwrap());
            }
            5 => {
                rows.insert(
                    hot_layer_key(fixture.head.layer_id),
                    vec![0; OPEN_RECORD_MAX_BYTES + 1],
                );
            }
            _ => unreachable!(),
        }
        let before = rows.clone();
        drop(rows);
        assert!(fixture.run().await.is_err());
        assert_eq!(*fixture.backend.memory.rows.lock().await, before);
        fixture.assert_retained().await;
    }
}

#[tokio::test]
async fn native_lease_reaper_concurrent_owner_or_gate_change_fences_final_cas() {
    for mode in 0..5 {
        let fixture = Fixture::new().await;
        fixture.observe_then_expire().await;
        let changes = match mode {
            0 => {
                let mut lease = fixture.lease.clone();
                lease.expires_at_ns = 1200;
                lease.updated_at_ns = 950;
                let hold = NativeHold::lease(&lease).unwrap();
                vec![
                    put(hot_lease_key(lease.workspace_id, lease.lease_id), &lease).unwrap(),
                    KvWrite::Put {
                        key: hold.key(),
                        value: hold.encode().unwrap(),
                    },
                    put(PACKED_ROOT_GENERATION_KEY.to_vec(), &3u64).unwrap(),
                ]
            }
            1 => {
                let journal = SealJournal {
                    journal_id: JournalId::new(),
                    workspace_id: fixture.workspace.workspace_id,
                    old_head_layer_id: fixture.head.layer_id,
                    expected_head_epoch: fixture.workspace.head_epoch,
                    phase: SealPhase::Prepare,
                    pending_bytes: 0,
                    delta_digest: None,
                    root_hash: None,
                    new_head_layer_id: None,
                    last_error: None,
                    created_at_ns: 3,
                    updated_at_ns: 3,
                };
                let hold = NativeHold::journal(&journal).unwrap();
                vec![
                    put(
                        hot_journal_key(journal.workspace_id, journal.journal_id),
                        &journal,
                    )
                    .unwrap(),
                    put(journal_index_key(journal.journal_id), &journal.workspace_id).unwrap(),
                    KvWrite::Put {
                        key: hold.key(),
                        value: hold.encode().unwrap(),
                    },
                    put(PACKED_ROOT_GENERATION_KEY.to_vec(), &3u64).unwrap(),
                ]
            }
            2 => {
                let mut lease = fixture.lease.clone();
                lease.holder_generation += 1;
                let hold = NativeHold::lease(&lease).unwrap();
                vec![
                    put(hot_lease_key(lease.workspace_id, lease.lease_id), &lease).unwrap(),
                    KvWrite::Put {
                        key: hold.key(),
                        value: hold.encode().unwrap(),
                    },
                    put(PACKED_ROOT_GENERATION_KEY.to_vec(), &3u64).unwrap(),
                ]
            }
            3 => vec![
                put(
                    open_v3_recovery_key(fixture.workspace.workspace_id),
                    &V3RecoveryRecord {
                        workspace_id: fixture.workspace.workspace_id,
                        incomplete: true,
                    },
                )
                .unwrap(),
            ],
            4 => vec![
                put(
                    HOLD_FEATURE.to_vec(),
                    &TestGate {
                        run: Uuid::new_v4(),
                        active: true,
                    },
                )
                .unwrap(),
            ],
            _ => unreachable!(),
        };
        *fixture.backend.before_release.lock().await = Some(changes);
        assert!(is_protected(fixture.run().await.unwrap_err()));
        assert_eq!(fixture.backend.release_attempts.load(Ordering::SeqCst), 1);
        fixture.assert_retained().await;
        if mode == 1 || mode == 2 || mode == 3 {
            assert!(is_protected(fixture.run().await.unwrap_err()));
        }
    }
}

#[tokio::test]
async fn native_lease_reaper_committed_lost_reply_is_confirmed_without_second_release() {
    let fixture = Fixture::new().await;
    fixture.observe_then_expire().await;
    fixture.backend.lose_reply.store(true, Ordering::SeqCst);
    assert!(fixture.run().await.unwrap().reaped);
    assert_eq!(fixture.actual_lease().await.state, LeaseState::Released);
    assert_eq!(fixture.backend.release_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.backend.release_commits.load(Ordering::SeqCst), 1);
    assert!(
        fixture
            .backend
            .get(&NativeHold::lease(&fixture.lease).unwrap().key())
            .await
            .unwrap()
            .is_none()
    );
    assert!(fixture.run().await.unwrap().reaped);
    assert_eq!(fixture.backend.release_attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_lease_reaper_lost_reply_requires_complete_successor_epoch() {
    let fixture = Fixture::new().await;
    fixture.observe_then_expire().await;
    *fixture.backend.after_lost_reply.lock().await = Some(vec![
        put(PACKED_ROOT_GENERATION_KEY.to_vec(), &4u64).unwrap(),
    ]);
    fixture.backend.lose_reply.store(true, Ordering::SeqCst);
    assert!(fixture.run().await.is_err());
    assert_eq!(fixture.backend.release_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.backend.release_commits.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.actual_lease().await.state, LeaseState::Released);
    assert!(
        fixture
            .backend
            .get(&NativeHold::lease(&fixture.lease).unwrap().key())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture.budget.state().used[V3BudgetPool::Metadata as usize],
        0
    );
}

#[tokio::test]
async fn native_lease_reaper_dropped_waiter_retains_driver_admission_until_backend_finishes() {
    let fixture = Fixture::new().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *fixture.backend.pause_next_write.lock().await = Some((entered.clone(), gate.clone()));
    let store = fixture.store.clone();
    let budget = fixture.budget.clone();
    let lease_id = fixture.lease.lease_id;
    let waiter = tokio::spawn(async move {
        store
            .reap_packed_native_lease(budget, options(lease_id))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    waiter.abort();
    let _ = waiter.await;
    assert!(fixture.budget.state().used[V3BudgetPool::Metadata as usize] > 0);
    assert!(
        fixture
            .backend
            .get(&policy_key(lease_id))
            .await
            .unwrap()
            .is_none()
    );
    assert_ne!(fixture.actual_lease().await.state, LeaseState::Released);
    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if fixture
                .backend
                .get(&policy_key(lease_id))
                .await
                .unwrap()
                .is_some()
                && fixture.budget.state().used[V3BudgetPool::Metadata as usize] == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fixture.assert_retained().await;
}

// Additional packed-v3 regressions. The complete lease-reaper01 prefix stays
// byte-for-byte unchanged; these observers borrow its existing typed fixture.

const MAIN_CENSUS_MIN_METADATA: usize = (32 + 16) << 20;

type MainCensusPause = (Arc<tokio::sync::Notify>, Arc<tokio::sync::Semaphore>);

struct LeaseCensusObserver {
    inner: Arc<LeaseMemoryBackend>,
    budget: Arc<V3MountBudget>,
    main_pages: AtomicUsize,
    main_empty_pages: AtomicUsize,
    release_calls: AtomicUsize,
    confirmation_reads: AtomicUsize,
    uncertain_release: AtomicBool,
    fail_main_page: AtomicUsize,
    cancel_after_main_page: Mutex<Option<CancellationToken>>,
    pause_main_page: Mutex<Option<MainCensusPause>>,
    pause_release: Mutex<Option<MainCensusPause>>,
    pause_confirmation: Mutex<Option<MainCensusPause>>,
}

impl LeaseCensusObserver {
    fn new(fixture: &Fixture) -> Self {
        Self {
            inner: fixture.backend.clone(),
            budget: fixture.budget.clone(),
            main_pages: AtomicUsize::new(0),
            main_empty_pages: AtomicUsize::new(0),
            release_calls: AtomicUsize::new(0),
            confirmation_reads: AtomicUsize::new(0),
            uncertain_release: AtomicBool::new(false),
            fail_main_page: AtomicUsize::new(0),
            cancel_after_main_page: Mutex::new(None),
            pause_main_page: Mutex::new(None),
            pause_release: Mutex::new(None),
            pause_confirmation: Mutex::new(None),
        }
    }

    fn assert_main_owner(&self, operation: &str) {
        let admitted = self.budget.state().used[V3BudgetPool::Metadata as usize] as usize;
        assert!(
            admitted >= MAIN_CENSUS_MIN_METADATA,
            "main census owner disappeared during {operation}: {admitted} bytes"
        );
    }
}

#[async_trait]
impl WorkspaceKvBackend for LeaseCensusObserver {
    fn name(&self) -> &'static str {
        "native-lease-main-census-observer-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        self.inner.get(key).await
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.inner.get_many_consistent(keys).await
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.inner.get_many_consistent_with_time(keys).await
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        if self.uncertain_release.load(Ordering::SeqCst) {
            self.assert_main_owner("complete uncertain successor confirmation");
            self.confirmation_reads.fetch_add(1, Ordering::SeqCst);
            if let Some((entered, release)) = self.pause_confirmation.lock().await.take() {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            }
        }
        self.inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner.scan_prefix(prefix).await
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner
            .scan_prefix_with_byte_limits(prefix, limits)
            .await
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        if prefix == JOURNAL_PREFIX {
            // Metadata must be owned before the backend begins materializing
            // even an empty page, and remain owned across that backend await.
            self.assert_main_owner("main namespace page admission");
            assert!(limits.max_records <= 4);
            assert!(limits.max_value_bytes <= 48 << 10);
            assert!(limits.max_response_bytes <= 256 << 10);
            assert_eq!(limits.max_data_requests, 64);
            let page = self.main_pages.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some((entered, release)) = self.pause_main_page.lock().await.take() {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            }
            if self.fail_main_page.load(Ordering::SeqCst) == page {
                return Err(WorkspaceError::Backend(
                    "injected partially visited MAIN namespace failure".into(),
                ));
            }
        }
        let page = self
            .inner
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await?;
        if prefix == JOURNAL_PREFIX {
            self.assert_main_owner("main namespace page completion");
            if page.is_empty() {
                self.main_empty_pages.fetch_add(1, Ordering::SeqCst);
            }
            if let Some(cancel) = self.cancel_after_main_page.lock().await.take() {
                cancel.cancel();
            }
        }
        Ok(page)
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.compare_and_swap_in_time_window(checks, writes, None, None)
            .await
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        let releasing = writes.iter().any(|write| match write {
            KvWrite::Put { key, value } if key.starts_with(HOT_LEASE_PREFIX) => {
                decode::<SnapshotLease>(value)
                    .is_ok_and(|lease| lease.state == LeaseState::Released)
            }
            _ => false,
        });
        if releasing {
            self.assert_main_owner("final atomic lease/hold release");
            self.release_calls.fetch_add(1, Ordering::SeqCst);
            if let Some((entered, release)) = self.pause_release.lock().await.take() {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            }
        } else if self.uncertain_release.load(Ordering::SeqCst) {
            self.assert_main_owner("uncertain successor final timed no-op");
        }
        let result = self
            .inner
            .compare_and_swap_in_time_window(checks, writes, lower, upper)
            .await;
        if releasing && matches!(result, Err(WorkspaceError::Backend(_))) {
            self.uncertain_release.store(true, Ordering::SeqCst);
        }
        result
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
        self.inner
            .authenticate_checks_before_bounded(checks, expires_at_ns, limits)
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
    }
}

fn observed_store(
    fixture: &Fixture,
) -> (
    Arc<LeaseCensusObserver>,
    Arc<KvWorkspaceStore<LeaseCensusObserver>>,
) {
    let backend = Arc::new(LeaseCensusObserver::new(fixture));
    let store = Arc::new(KvWorkspaceStore::from_arc(backend.clone()));
    store
        .configure_packed_reader_pin_budget(fixture.budget.clone())
        .unwrap();
    (backend, store)
}

async fn terminal_main_rows(fixture: &Fixture, count: usize) {
    let original = install_active_ppj(fixture).await;
    let mut rows = fixture.backend.memory.rows.lock().await;
    rows.remove(&active_key(original.journal_id));
    rows.remove(&journal_key(original.journal_id));
    rows.insert(ACTIVE_COUNT_KEY.to_vec(), 0u64.to_le_bytes().to_vec());
    for _ in 0..count {
        let mut terminal = original.clone();
        terminal.journal_id = JournalId::new();
        terminal.revision += 1;
        terminal.phase = PackedJournalPhase::Aborted;
        terminal.abort_reason = "typed terminal MAIN census fixture".into();
        rows.insert(journal_key(terminal.journal_id), terminal.encode().unwrap());
    }
}

async fn main_owner_idle(fixture: &Fixture) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.budget.state().used[V3BudgetPool::Metadata as usize] != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_lease_reaper_present_sentinels_cannot_hide_nonterminal_main_only() {
    let fixture = Fixture::new().await;
    fixture.observe_then_expire().await;
    let record = install_active_ppj(&fixture).await;
    let mut rows = fixture.backend.memory.rows.lock().await;
    rows.remove(&active_key(record.journal_id));
    rows.insert(ACTIVE_COUNT_KEY.to_vec(), 0u64.to_le_bytes().to_vec());
    let before = rows.clone();
    drop(rows);
    assert!(fixture.run().await.is_err());
    assert_eq!(*fixture.backend.memory.rows.lock().await, before);
    fixture.assert_retained().await;
    assert_eq!(fixture.backend.release_attempts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn native_lease_reaper_foreign_nonterminal_main_requires_exact_active_pair() {
    let fixture = Fixture::new().await;
    fixture.observe_then_expire().await;
    let original = install_active_ppj(&fixture).await;
    let mut foreign = original.clone();
    foreign.journal_id = JournalId::new();
    foreign.guard.workspace_id = WorkspaceId::new();
    foreign.guard.lease_id = LeaseId::new();
    foreign.guard.expected_head_layer_id = LayerId::new();
    foreign.expected_binding.workspace_id = foreign.guard.workspace_id;
    foreign.expected_binding.head_layer_id = foreign.guard.expected_head_layer_id;
    let mut head: LayerRecord = decode(&foreign.expected_head).unwrap();
    head.layer_id = foreign.guard.expected_head_layer_id;
    head.owner_workspace_id = Some(foreign.guard.workspace_id);
    foreign.expected_head = encode(&head).unwrap();
    let raw = foreign.encode().unwrap();
    let mut rows = fixture.backend.memory.rows.lock().await;
    rows.remove(&active_key(original.journal_id));
    rows.remove(&journal_key(original.journal_id));
    rows.insert(ACTIVE_COUNT_KEY.to_vec(), 0u64.to_le_bytes().to_vec());
    rows.insert(journal_key(foreign.journal_id), raw);
    let before = rows.clone();
    drop(rows);
    assert!(fixture.run().await.is_err());
    assert_eq!(*fixture.backend.memory.rows.lock().await, before);
    fixture.assert_retained().await;
}

#[tokio::test]
async fn native_lease_reaper_terminal_main_short_pages_require_actual_empty_completion() {
    for terminal_history in [false, true] {
        let fixture = Fixture::new().await;
        fixture.observe_then_expire().await;
        if terminal_history {
            terminal_main_rows(&fixture, 5).await;
        } else {
            assert!(
                fixture
                    .backend
                    .get(JOURNAL_FEATURE_KEY)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                fixture
                    .backend
                    .get(ACTIVE_COUNT_KEY)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        // Isolate the census return owner from the later native-owner CAS
        // permit, including the absent-feature scanner's 1 MiB probe owner.
        let operation_owner = fixture
            .budget
            .admit(&[(V3BudgetPool::Metadata, 32 << 20)])
            .unwrap();
        let mut read = fixture
            .store
            .read_packed_native_lease_reaper(fixture.lease.lease_id)
            .await
            .unwrap();
        let census_owner = fixture
            .store
            .packed_native_lease_no_protective_journal(
                &mut read,
                &options(fixture.lease.lease_id),
                &fixture.budget,
            )
            .await
            .unwrap();
        assert!(census_owner.is_some());
        assert!(
            fixture.budget.state().used[V3BudgetPool::Metadata as usize]
                >= MAIN_CENSUS_MIN_METADATA as u64
        );
        drop(census_owner);
        assert_eq!(
            fixture.budget.state().used[V3BudgetPool::Metadata as usize],
            32 << 20
        );
        drop(read);
        drop(operation_owner);
        let (backend, store) = observed_store(&fixture);
        let report = store
            .reap_packed_native_lease(fixture.budget.clone(), options(fixture.lease.lease_id))
            .await
            .unwrap();
        assert!(report.reaped);
        // JournalMemoryBackend returns one row at a time. Five short nonempty
        // pages must continue; only their sixth actual page is empty.
        if terminal_history {
            assert_eq!(backend.main_pages.load(Ordering::SeqCst), 6);
        } else {
            assert_eq!(backend.main_pages.load(Ordering::SeqCst), 1);
        }
        assert_eq!(backend.main_empty_pages.load(Ordering::SeqCst), 1);
        assert_eq!(backend.release_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.actual_lease().await.state, LeaseState::Released);
        main_owner_idle(&fixture).await;
    }
}

#[tokio::test]
async fn native_lease_reaper_partial_main_quota_failure_and_cancel_preserve_policy_and_hold() {
    for fault in 0..3 {
        let fixture = Fixture::new().await;
        fixture.observe_then_expire().await;
        terminal_main_rows(&fixture, 2).await;
        let (backend, store) = observed_store(&fixture);
        let mut operation = options(fixture.lease.lease_id);
        match fault {
            0 => operation.max_protective_rows = 1,
            1 => backend.fail_main_page.store(2, Ordering::SeqCst),
            2 => *backend.cancel_after_main_page.lock().await = Some(operation.cancel.clone()),
            _ => unreachable!(),
        }
        let before = fixture.backend.memory.rows.lock().await.clone();
        assert!(
            store
                .reap_packed_native_lease(fixture.budget.clone(), operation)
                .await
                .is_err()
        );
        assert_eq!(*fixture.backend.memory.rows.lock().await, before);
        fixture.assert_retained().await;
        assert_eq!(backend.release_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn native_lease_reaper_dropped_main_census_waiter_keeps_row_owner_until_backend_finishes() {
    let fixture = Fixture::new().await;
    fixture.observe_then_expire().await;
    terminal_main_rows(&fixture, 1).await;
    let (backend, store) = observed_store(&fixture);
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    *backend.pause_main_page.lock().await = Some((entered.clone(), release.clone()));
    let budget = fixture.budget.clone();
    let lease_id = fixture.lease.lease_id;
    let waiter = tokio::spawn(async move {
        store
            .reap_packed_native_lease(budget, options(lease_id))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    waiter.abort();
    let _ = waiter.await;
    backend.assert_main_owner("dropped waiter with MAIN read in flight");
    assert_ne!(fixture.actual_lease().await.state, LeaseState::Released);
    release.add_permits(1);
    main_owner_idle(&fixture).await;
    assert_eq!(fixture.actual_lease().await.state, LeaseState::Released);
    assert_eq!(backend.main_empty_pages.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.backend.release_commits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_lease_reaper_main_owner_survives_final_cas_and_unknown_complete_confirmation() {
    for lost_reply in [false, true] {
        let fixture = Fixture::new().await;
        fixture.observe_then_expire().await;
        terminal_main_rows(&fixture, 1).await;
        let (backend, store) = observed_store(&fixture);
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        *backend.pause_release.lock().await = Some((entered.clone(), release.clone()));
        let confirm_entered = Arc::new(tokio::sync::Notify::new());
        let confirm_release = Arc::new(tokio::sync::Semaphore::new(0));
        if lost_reply {
            fixture.backend.lose_reply.store(true, Ordering::SeqCst);
            *backend.pause_confirmation.lock().await =
                Some((confirm_entered.clone(), confirm_release.clone()));
        }
        let budget = fixture.budget.clone();
        let lease_id = fixture.lease.lease_id;
        let waiter = tokio::spawn(async move {
            store
                .reap_packed_native_lease(budget, options(lease_id))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        backend.assert_main_owner("paused final release CAS");
        assert_ne!(fixture.actual_lease().await.state, LeaseState::Released);
        release.add_permits(1);
        if lost_reply {
            tokio::time::timeout(Duration::from_secs(2), confirm_entered.notified())
                .await
                .unwrap();
            assert_eq!(fixture.actual_lease().await.state, LeaseState::Released);
            waiter.abort();
            let _ = waiter.await;
            backend.assert_main_owner("dropped waiter with uncertain confirmation in flight");
            confirm_release.add_permits(1);
            main_owner_idle(&fixture).await;
            assert!(backend.confirmation_reads.load(Ordering::SeqCst) > 0);
        } else {
            assert!(waiter.await.unwrap().unwrap().reaped);
            main_owner_idle(&fixture).await;
        }
        assert_eq!(fixture.actual_lease().await.state, LeaseState::Released);
        assert_eq!(fixture.backend.release_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.backend.release_commits.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn native_lease_reaper_main_census_fits_shared_64m_budget_with_existing_owner() {
    use crate::workspace_overlay::packed_v3::wire005::V3BudgetLimits;

    for present in [false, true] {
        let mut fixture = Fixture::new().await;
        let mut limits = V3BudgetLimits::default();
        limits.bytes[V3BudgetPool::Metadata as usize] = 64 << 20;
        fixture.budget = V3MountBudget::new(limits).unwrap();
        fixture.store = Arc::new(KvWorkspaceStore::from_arc(fixture.backend.clone()));
        fixture
            .store
            .configure_packed_reader_pin_budget(fixture.budget.clone())
            .unwrap();
        fixture.observe_then_expire().await;
        if present {
            terminal_main_rows(&fixture, 1).await;
        }
        let before = fixture.backend.memory.rows.lock().await.clone();
        let existing_owner = fixture
            .budget
            .admit(&[(V3BudgetPool::Metadata, 8 << 20)])
            .unwrap();
        let operation_owner = fixture
            .budget
            .admit(&[(V3BudgetPool::Metadata, 32 << 20)])
            .unwrap();
        let mut read = fixture
            .store
            .read_packed_native_lease_reaper(fixture.lease.lease_id)
            .await
            .unwrap();
        let census_owner = fixture
            .store
            .packed_native_lease_no_protective_journal(
                &mut read,
                &options(fixture.lease.lease_id),
                &fixture.budget,
            )
            .await
            .unwrap();
        assert!(census_owner.is_some());
        assert_eq!(
            fixture.budget.state().used[V3BudgetPool::Metadata as usize],
            56 << 20,
        );
        assert!(fixture.budget.state().peak[V3BudgetPool::Metadata as usize] <= 57 << 20);
        drop(census_owner);
        assert_eq!(
            fixture.budget.state().used[V3BudgetPool::Metadata as usize],
            40 << 20,
        );
        assert_eq!(*fixture.backend.memory.rows.lock().await, before);
        drop(read);
        drop(operation_owner);
        drop(existing_owner);
        fixture.assert_retained().await;
    }
}
