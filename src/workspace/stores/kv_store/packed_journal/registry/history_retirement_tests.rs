//! Behavioral contracts for grace, complete confirmation and durable pickup.
//! Fixture rows exercise the driver; they do not certify a packed graph.
use super::*;
use crate::cadapter::localfs::LocalFsBackend;
use crate::workspace::stores::kv_store::packed_journal::tests::JournalMemoryBackend;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

#[derive(Default)]
struct HistoryMemoryBackend {
    memory: JournalMemoryBackend,
    now: AtomicI64,
    lose_reply: AtomicBool,
    write_attempts: AtomicUsize,
    largest_point_batch: AtomicUsize,
    bounded_reads: AtomicUsize,
    unbounded_reads: AtomicUsize,
    cancel_after_queue: Mutex<Option<CancellationToken>>,
    pause_next_write: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Semaphore>)>>,
}

#[async_trait]
impl WorkspaceKvBackend for HistoryMemoryBackend {
    fn name(&self) -> &'static str {
        "history-retirement-memory-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    async fn authenticate_gc_admin(&self) -> Result<(), WorkspaceError> {
        self.memory.authenticate_gc_admin().await
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        self.unbounded_reads.fetch_add(1, Ordering::SeqCst);
        self.memory.get(key).await
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.memory.get_many_consistent(keys).await
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.bounded_reads.fetch_add(1, Ordering::SeqCst);
        self.largest_point_batch
            .fetch_max(keys.len(), Ordering::SeqCst);
        let (rows, _) = self
            .memory
            .get_many_consistent_with_time_bounded(keys, limits)
            .await?;
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
        self.memory
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await
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
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        self.compare_and_swap_in_time_window(checks, writes, None, Some(deadline))
            .await
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        if !writes.is_empty() {
            let pause = self.pause_next_write.lock().await.take();
            if let Some((entered, gate)) = pause {
                entered.notify_one();
                gate.acquire().await.unwrap().forget();
            }
        }
        let mut rows = self.memory.rows.lock().await;
        let now = self.now.load(Ordering::SeqCst);
        if lower.is_some_and(|lower| now < lower)
            || upper.is_some_and(|upper| now >= upper)
            || checks
                .iter()
                .any(|check| rows.get(&check.key) != check.expected.as_ref())
        {
            return Ok(false);
        }
        if !writes.is_empty() {
            self.write_attempts.fetch_add(1, Ordering::SeqCst);
        }
        let mut queued = false;
        for write in writes {
            match write {
                KvWrite::Put { key, value } => {
                    queued |= key.starts_with(b"packed/registry/history-delete-queue/");
                    rows.insert(key.clone(), value.clone());
                }
                KvWrite::Delete { key } => {
                    rows.remove(key);
                }
            }
        }
        drop(rows);
        if queued && let Some(cancel) = self.cancel_after_queue.lock().await.take() {
            cancel.cancel();
        }
        if !writes.is_empty() && self.lose_reply.swap(false, Ordering::SeqCst) {
            return Err(WorkspaceError::Backend(
                "injected historical committed reply loss".into(),
            ));
        }
        Ok(true)
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
struct TestNativeGate {
    run: Uuid,
    active: bool,
}
fn native_gate() -> Vec<u8> {
    encode(&TestNativeGate {
        run: Uuid::new_v4(),
        active: true,
    })
    .unwrap()
}
// This fixture models a completed migration of both unfinished journal heads.
// An Active legacy gate alone deliberately cannot authorize a zero-owner census.
const TEST_NATIVE_JOURNAL_HEADS_FEATURE: &[u8] = b"packed/native-hold-journal-heads";
fn registry_gate() -> Vec<u8> {
    let mut raw = row_header(b"PRG3");
    raw.extend_from_slice(&1u64.to_le_bytes());
    raw.extend_from_slice(Uuid::new_v4().as_bytes());
    raw.push(1);
    raw.extend_from_slice(&1u64.to_le_bytes());
    raw.extend_from_slice(&1u64.to_le_bytes());
    finish_record(raw, 128).unwrap()
}
fn options(incarnation: Uuid) -> PackedHistoryRetirementOptions {
    PackedHistoryRetirementOptions {
        incarnation,
        grace_ns: 100,
        max_native_holds: 32,
        max_current_bindings: 32,
        max_work: u64::MAX,
        cancel: CancellationToken::new(),
    }
}
struct Fixture {
    backend: Arc<HistoryMemoryBackend>,
    store: Arc<KvWorkspaceStore<HistoryMemoryBackend>>,
    budget: Arc<SnapshotMountBudget>,
    root: RootRow,
    reference: SnapshotObjectRef,
    objects: tempfile::TempDir,
    client: ObjectClient<LocalFsBackend>,
}
impl Fixture {
    async fn new() -> Self {
        let backend = Arc::new(HistoryMemoryBackend::default());
        backend.now.store(1000, Ordering::SeqCst);
        backend.memory.page_size.store(1, Ordering::SeqCst);
        let store = Arc::new(KvWorkspaceStore::from_arc(backend.clone()));
        let budget = SnapshotMountBudget::defaults();
        let kind = crate::workspace::packed::snapshot::SnapshotObjectKind::Manifest;
        let object_bytes =
            crate::workspace::packed::snapshot::encode_snapshot_object(kind, b"x", 1).unwrap();
        let reference = SnapshotObjectRef::from_bytes(
            "packed/history-tests/manifest".into(),
            kind,
            &object_bytes,
        )
        .unwrap();
        let base = BaseRevision {
            layer_id: LayerId::new(),
            sealed_version: 1,
            root_hash: [23; 32],
        };
        let binding = PackedLowerBindingRecord {
            workspace_id: WorkspaceId::new(),
            head_layer_id: LayerId::new(),
            head_epoch: 1,
            highest_inode: 400,
            binding: PackedLowerBinding {
                binding_version: 2,
                base_layer_id: base.layer_id,
                manifest: reference.clone(),
            },
            base_revision: base,
        };
        let root = RootRow {
            journal_id: JournalId::new(),
            incarnation: Uuid::new_v4(),
            revision: 1,
            state: RootState::BindingHistory,
            members: 1,
            pending_puts: 0,
            binding: Some(binding.clone()),
        };
        let object = ObjectRow {
            reference: reference.clone(),
            revision: 1,
            state: ObjectState::Live,
            memberships: 1,
            pending_puts: 0,
            delete_id: Uuid::nil(),
            delete_dispatched: false,
        };
        let member = MemberRow {
            reference: reference.clone(),
            journal_id: root.journal_id,
            incarnation: root.incarnation,
            ordinal: 0,
            put_id: Uuid::nil(),
            adopted: true,
            pending_put: false,
            dispatched: false,
            retained: true,
        };
        let completed_native_gate = native_gate();
        backend.memory.rows.lock().await.extend([
            (GATE_KEY.to_vec(), registry_gate()),
            (HOLD_FEATURE.to_vec(), completed_native_gate.clone()),
            (
                TEST_NATIVE_JOURNAL_HEADS_FEATURE.to_vec(),
                completed_native_gate,
            ),
            (PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&1u64).unwrap()),
            (
                LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                encode(&1u64).unwrap(),
            ),
            (registry_root_key(root.incarnation), root.encode().unwrap()),
            (registry_history_root_key(&binding), root.encode().unwrap()),
            (
                packed_history_key(binding.workspace_id, 2),
                binding.encode().unwrap(),
            ),
            (registry_object_key(&reference), object.encode().unwrap()),
            (
                registry_member_key(&reference, root.incarnation),
                member.encode().unwrap(),
            ),
            (
                registry_reverse_key(root.incarnation, 0),
                reference.encode_value().unwrap(),
            ),
        ]);
        let objects = tempfile::tempdir().unwrap();
        let path = objects.path().join(&reference.key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, object_bytes).unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        Self {
            backend,
            store,
            budget,
            root,
            reference,
            objects,
            client,
        }
    }
    async fn run(&self) -> Result<PackedHistoryRetirementReport, WorkspaceError> {
        self.store
            .retire_packed_binding_history(
                self.client.clone(),
                self.budget.clone(),
                options(self.root.incarnation),
            )
            .await
    }
    async fn root(&self) -> RootRow {
        RootRow::decode(
            &self
                .backend
                .get(&registry_root_key(self.root.incarnation))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap()
    }
    async fn make_committed_fixture(&self) {
        self.run().await.unwrap();
        let read = self
            .store
            .history_retirement_read(self.root.incarnation)
            .await
            .unwrap();
        let mut root = read.root;
        root.state = RootState::Retiring;
        root.revision += 1;
        let mut observation = read.observation.unwrap();
        observation.phase = ObservationPhase::Committed;
        let binding = root.binding.as_ref().unwrap();
        self.backend.memory.rows.lock().await.extend([
            (registry_root_key(root.incarnation), root.encode().unwrap()),
            (registry_history_root_key(binding), root.encode().unwrap()),
            (
                observation_key(root.incarnation),
                observation.encode().unwrap(),
            ),
            (PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&3u64).unwrap()),
        ]);
        self.backend
            .memory
            .rows
            .lock()
            .await
            .remove(&packed_history_key(binding.workspace_id, 2));
        self.backend.now.store(1100, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn packed_history_retirement_two_idle_histories_eventually_retire() {
    let fixture = Fixture::new().await;
    let mut second = fixture.root.clone();
    second.incarnation = Uuid::new_v4();
    second.journal_id = JournalId::new();
    let binding = second.binding.as_mut().unwrap();
    binding.workspace_id = WorkspaceId::new();
    binding.head_layer_id = LayerId::new();
    binding.base_revision.layer_id = LayerId::new();
    binding.binding.base_layer_id = binding.base_revision.layer_id;
    let member = MemberRow {
        reference: fixture.reference.clone(),
        journal_id: second.journal_id,
        incarnation: second.incarnation,
        ordinal: 0,
        put_id: Uuid::nil(),
        adopted: true,
        pending_put: false,
        dispatched: false,
        retained: true,
    };
    {
        let mut rows = fixture.backend.memory.rows.lock().await;
        let key = registry_object_key(&fixture.reference);
        let mut object = ObjectRow::decode(&rows[&key]).unwrap();
        object.memberships = 2;
        rows.extend([
            (key, object.encode().unwrap()),
            (
                registry_root_key(second.incarnation),
                second.encode().unwrap(),
            ),
            (
                registry_history_root_key(second.binding.as_ref().unwrap()),
                second.encode().unwrap(),
            ),
            (
                packed_history_key(second.binding.as_ref().unwrap().workspace_id, 2),
                second.binding.as_ref().unwrap().encode().unwrap(),
            ),
            (
                registry_member_key(&fixture.reference, second.incarnation),
                member.encode().unwrap(),
            ),
            (
                registry_reverse_key(second.incarnation, 0),
                fixture.reference.encode_value().unwrap(),
            ),
        ]);
    }
    // Independent histories in one volume share the same generation and clock.
    // Actual retirement may restart the other grace once; observing must not
    // keep both histories alive indefinitely under the same ordered GC cursor.
    let incarnations = [fixture.root.incarnation, second.incarnation];
    let mut released = 0;
    let mut deleted = 0;
    for cycle in 0..=4 {
        fixture
            .backend
            .now
            .store(1000 + cycle * 100, Ordering::SeqCst);
        for (index, incarnation) in incarnations.iter().enumerate() {
            let report = fixture
                .store
                .retire_packed_binding_history(
                    fixture.client.clone(),
                    fixture.budget.clone(),
                    options(*incarnation),
                )
                .await
                .unwrap();
            eprintln!("cycle={cycle} history={index} report={report:?}");
            if cycle == 0 {
                assert!(report.observing && !report.retired);
                assert_eq!(report.not_before_ns, 1100);
            }
            released += report.released_members;
            deleted += report.deleted_objects;
        }
    }
    for incarnation in incarnations {
        let root = RootRow::decode(
            &fixture
                .backend
                .get(&registry_root_key(incarnation))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            (root.state, root.members),
            (RootState::Retired, 0),
            "idle histories must finish grace despite each other's observations"
        );
    }
    assert_eq!((released, deleted), (2, 1));
    assert!(!fixture.objects.path().join(&fixture.reference.key).exists());
}

#[tokio::test]
async fn packed_history_retirement_generation_change_restarts_grace() {
    for key in [PACKED_ROOT_GENERATION_KEY, LAYER_INVENTORY_GENERATION_KEY] {
        let fixture = Fixture::new().await;
        assert_eq!(fixture.run().await.unwrap().not_before_ns, 1100);
        fixture.backend.now.store(1100, Ordering::SeqCst);
        {
            let mut rows = fixture.backend.memory.rows.lock().await;
            let generation: u64 = decode(&rows[key]).unwrap();
            rows.insert(key.to_vec(), encode(&(generation + 1)).unwrap());
        }
        let restarted = fixture.run().await.unwrap();
        assert!(restarted.observing && !restarted.retired);
        assert_eq!(restarted.not_before_ns, 1200);
        assert_eq!(fixture.root().await.members, 1);
        fixture.backend.now.store(1199, Ordering::SeqCst);
        assert!(fixture.run().await.unwrap().observing);
        assert!(fixture.objects.path().join(&fixture.reference.key).exists());
        fixture.backend.now.store(1200, Ordering::SeqCst);
        assert!(fixture.run().await.unwrap().retired);
    }
}

#[tokio::test]
async fn packed_history_retirement_generation_race_rejects_observation_and_retirement() {
    for already_observed in [false, true] {
        let fixture = Fixture::new().await;
        if already_observed {
            fixture.run().await.unwrap();
            fixture.backend.now.store(1100, Ordering::SeqCst);
        }
        let entered = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        *fixture.backend.pause_next_write.lock().await = Some((entered.clone(), gate.clone()));
        let store = fixture.store.clone();
        let budget = fixture.budget.clone();
        let client = fixture.client.clone();
        let incarnation = fixture.root.incarnation;
        let waiter = tokio::spawn(async move {
            store
                .retire_packed_binding_history(client, budget, options(incarnation))
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let before = {
            let mut rows = fixture.backend.memory.rows.lock().await;
            let generation: u64 = decode(&rows[PACKED_ROOT_GENERATION_KEY]).unwrap();
            rows.insert(
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                encode(&(generation + 1)).unwrap(),
            );
            rows.clone()
        };
        gate.add_permits(1);
        assert!(matches!(waiter.await.unwrap(), Err(WorkspaceError::Busy)));
        assert_eq!(*fixture.backend.memory.rows.lock().await, before);
        assert_eq!(fixture.root().await.state, RootState::BindingHistory);
        assert!(fixture.objects.path().join(&fixture.reference.key).exists());
    }
}

#[tokio::test]
async fn packed_history_retirement_grace_then_release_delete_and_tombstone() {
    let fixture = Fixture::new().await;
    let observing = fixture.run().await.unwrap();
    assert!(observing.observing && !observing.retired);
    assert_eq!(observing.not_before_ns, 1100);
    fixture.backend.now.store(1099, Ordering::SeqCst);
    assert!(fixture.run().await.unwrap().observing);
    assert_eq!(fixture.root().await.members, 1);
    assert!(fixture.objects.path().join(&fixture.reference.key).exists());
    fixture.backend.now.store(1100, Ordering::SeqCst);
    let report = fixture.run().await.unwrap();
    assert!(report.retired && !report.observing);
    assert_eq!(
        (
            report.released_members,
            report.deleted_objects,
            report.quarantined_objects
        ),
        (1, 1, 0)
    );
    let root = fixture.root().await;
    assert_eq!((root.state, root.members), (RootState::Retired, 0));
    let rows = fixture.backend.memory.rows.lock().await;
    assert_eq!(
        rows.get(&registry_root_key(root.incarnation)),
        rows.get(&registry_history_root_key(root.binding.as_ref().unwrap()))
    );
    assert!(!rows.contains_key(&packed_history_key(
        root.binding.as_ref().unwrap().workspace_id,
        2
    )));
    assert!(!rows.contains_key(&delete_queue_key(root.incarnation, 0)));
    assert!(!fixture.objects.path().join(&fixture.reference.key).exists());
}

#[tokio::test]
async fn packed_history_retirement_gate_replacement_restarts_grace_without_epoch_bump() {
    for key in [GATE_KEY, HOLD_FEATURE] {
        let fixture = Fixture::new().await;
        fixture.run().await.unwrap();
        let epoch = fixture
            .backend
            .get(PACKED_ROOT_GENERATION_KEY)
            .await
            .unwrap();
        fixture.backend.now.store(1100, Ordering::SeqCst);
        {
            let next = if key == GATE_KEY {
                registry_gate()
            } else {
                native_gate()
            };
            let mut rows = fixture.backend.memory.rows.lock().await;
            rows.insert(key.to_vec(), next.clone());
            if key == HOLD_FEATURE {
                rows.insert(TEST_NATIVE_JOURNAL_HEADS_FEATURE.to_vec(), next);
            }
        }
        assert_eq!(
            fixture
                .backend
                .get(PACKED_ROOT_GENERATION_KEY)
                .await
                .unwrap(),
            epoch
        );
        let report = fixture.run().await.unwrap();
        assert!(report.observing);
        assert_eq!(report.not_before_ns, 1200);
        assert_eq!(fixture.root().await.members, 1);
    }
}

#[tokio::test]
async fn packed_history_retirement_policy_change_and_anchor_one_are_fenced() {
    let fixture = Fixture::new().await;
    fixture.run().await.unwrap();
    let before = fixture.backend.memory.rows.lock().await.clone();
    let mut changed = options(fixture.root.incarnation);
    changed.grace_ns = 1;
    assert!(matches!(
        fixture
            .store
            .retire_packed_binding_history(fixture.client.clone(), fixture.budget.clone(), changed)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(*fixture.backend.memory.rows.lock().await, before);
    let mut root = fixture.root.clone();
    root.binding.as_mut().unwrap().binding.binding_version = 1;
    fixture
        .backend
        .memory
        .rows
        .lock()
        .await
        .insert(registry_root_key(root.incarnation), root.encode().unwrap());
    assert!(matches!(fixture.run().await, Err(WorkspaceError::Fenced)));
    assert!(fixture.objects.path().join(&fixture.reference.key).exists());
}

#[tokio::test]
async fn packed_history_retirement_cancel_after_release_preserves_delete_pickup() {
    let fixture = Fixture::new().await;
    fixture.run().await.unwrap();
    fixture.backend.now.store(1100, Ordering::SeqCst);
    let operation = options(fixture.root.incarnation);
    *fixture.backend.cancel_after_queue.lock().await = Some(operation.cancel.clone());
    assert!(matches!(
        fixture
            .store
            .retire_packed_binding_history(
                fixture.client.clone(),
                fixture.budget.clone(),
                operation
            )
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(
        (fixture.root().await.state, fixture.root().await.members),
        (RootState::Retiring, 0)
    );
    assert!(
        fixture
            .backend
            .get(&delete_queue_key(fixture.root.incarnation, 0))
            .await
            .unwrap()
            .is_some()
    );
    assert!(fixture.objects.path().join(&fixture.reference.key).exists());
    let resumed = fixture.run().await.unwrap();
    assert!(resumed.retired);
    assert_eq!((resumed.released_members, resumed.deleted_objects), (0, 1));
    assert!(!fixture.objects.path().join(&fixture.reference.key).exists());
}

#[tokio::test]
async fn packed_history_retirement_resume_rejects_target_current_and_orphan_claim() {
    for target_current in [true, false] {
        let fixture = Fixture::new().await;
        fixture.make_committed_fixture().await;
        let binding = fixture.root.binding.as_ref().unwrap();
        let mut rows = fixture.backend.memory.rows.lock().await;
        rows.insert(
            packed_claim_key(binding.workspace_id),
            PACKED_CLAIM.to_vec(),
        );
        if target_current {
            rows.insert(
                packed_current_key(binding.workspace_id),
                binding.encode().unwrap(),
            );
        }
        drop(rows);
        assert!(matches!(fixture.run().await, Err(WorkspaceError::Fenced)));
        assert_eq!(fixture.root().await.members, 1);
        assert!(fixture.objects.path().join(&fixture.reference.key).exists());
    }
}

#[tokio::test]
async fn packed_history_retirement_resume_gate_change_and_stale_deadline_cannot_delete() {
    let fixture = Fixture::new().await;
    fixture.make_committed_fixture().await;
    let context = HistoryDeleteContext {
        incarnation: fixture.root.incarnation,
        ordinal: 0,
        lower: 1099,
    };
    assert!(matches!(
        fixture
            .store
            .history_retirement_delete_checks(context, &fixture.reference)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    fixture
        .backend
        .memory
        .rows
        .lock()
        .await
        .insert(HOLD_FEATURE.to_vec(), native_gate());
    assert!(matches!(fixture.run().await, Err(WorkspaceError::Fenced)));
    assert_eq!(fixture.root().await.members, 1);
}

#[tokio::test]
async fn packed_history_retirement_lost_reply_confirms_complete_batched_successor_once() {
    let backend = Arc::new(HistoryMemoryBackend::default());
    backend.now.store(2000, Ordering::SeqCst);
    let store = KvWorkspaceStore::from_arc(backend.clone());
    let checks = (0..6)
        .map(|ordinal| KvCheck {
            key: format!("history-confirm/{ordinal}").into_bytes(),
            expected: Some(vec![1; RECORD_LIMIT]),
        })
        .collect::<Vec<_>>();
    backend.memory.rows.lock().await.extend(
        checks
            .iter()
            .map(|check| (check.key.clone(), check.expected.clone().unwrap())),
    );
    let writes = [KvWrite::Put {
        key: checks[0].key.clone(),
        value: vec![2; RECORD_LIMIT],
    }];
    backend.lose_reply.store(true, Ordering::SeqCst);
    store
        .history_clock_cas(&checks, &writes, Some(2000))
        .await
        .unwrap();
    assert_eq!(backend.write_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(backend.largest_point_batch.load(Ordering::SeqCst), 2);
    assert_eq!(
        backend.get(&checks[0].key).await.unwrap(),
        Some(vec![2; RECORD_LIMIT])
    );
}

#[tokio::test]
async fn packed_history_retirement_unknown_delete_stays_quarantined_on_resume() {
    let fixture = Fixture::new().await;
    fixture.make_committed_fixture().await;
    let path = fixture.objects.path().join(&fixture.reference.key);
    // A directory makes the real LocalFs physical DELETE fail after dispatch.
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(matches!(
        fixture.run().await,
        Err(WorkspaceError::Backend(_))
    ));
    let key = registry_object_key(&fixture.reference);
    let pending = ObjectRow::decode(&fixture.backend.get(&key).await.unwrap().unwrap()).unwrap();
    assert_eq!(pending.state, ObjectState::DeletePending);
    assert!(pending.delete_dispatched && !pending.delete_id.is_nil());
    assert!(
        fixture
            .backend
            .get(&delete_queue_key(fixture.root.incarnation, 0))
            .await
            .unwrap()
            .is_some()
    );
    let resumed = fixture.run().await.unwrap();
    assert!(resumed.retired);
    assert_eq!(
        (resumed.deleted_objects, resumed.quarantined_objects),
        (0, 1)
    );
    assert_eq!(
        fixture.backend.get(&key).await.unwrap().unwrap(),
        pending.encode().unwrap()
    );
    assert!(path.is_dir());
}

#[tokio::test]
async fn packed_history_retirement_other_root_membership_prevents_delete() {
    let fixture = Fixture::new().await;
    let key = registry_object_key(&fixture.reference);
    let mut object = ObjectRow::decode(&fixture.backend.get(&key).await.unwrap().unwrap()).unwrap();
    object.memberships = 2;
    let mut other_root = fixture.root.clone();
    other_root.incarnation = Uuid::new_v4();
    other_root.journal_id = JournalId::new();
    let member = MemberRow {
        reference: fixture.reference.clone(),
        journal_id: other_root.journal_id,
        incarnation: other_root.incarnation,
        ordinal: 0,
        put_id: Uuid::nil(),
        adopted: true,
        pending_put: false,
        dispatched: false,
        retained: true,
    };
    fixture.backend.memory.rows.lock().await.extend([
        (key.clone(), object.encode().unwrap()),
        (
            registry_root_key(other_root.incarnation),
            other_root.encode().unwrap(),
        ),
        (
            registry_member_key(&fixture.reference, other_root.incarnation),
            member.encode().unwrap(),
        ),
        (
            registry_reverse_key(other_root.incarnation, 0),
            fixture.reference.encode_value().unwrap(),
        ),
    ]);
    fixture.run().await.unwrap();
    fixture.backend.now.store(1100, Ordering::SeqCst);
    let report = fixture.run().await.unwrap();
    assert!(report.retired);
    assert_eq!((report.released_members, report.deleted_objects), (1, 0));
    let object = ObjectRow::decode(&fixture.backend.get(&key).await.unwrap().unwrap()).unwrap();
    assert_eq!((object.state, object.memberships), (ObjectState::Live, 1));
    assert!(fixture.objects.path().join(&fixture.reference.key).exists());
}

#[tokio::test]
async fn packed_history_retirement_dropped_waiter_keeps_owned_driver_and_permit() {
    let fixture = Fixture::new().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *fixture.backend.pause_next_write.lock().await = Some((entered.clone(), gate.clone()));
    let store = fixture.store.clone();
    let budget = fixture.budget.clone();
    let client = fixture.client.clone();
    let incarnation = fixture.root.incarnation;
    let waiter = tokio::spawn(async move {
        store
            .retire_packed_binding_history(client, budget, options(incarnation))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    waiter.abort();
    let _ = waiter.await;
    assert!(
        fixture.budget.state().used[SnapshotBudgetPool::Metadata as usize]
            >= HISTORY_OPERATION_BYTES
    );
    gate.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if fixture
                .backend
                .get(&observation_key(incarnation))
                .await
                .unwrap()
                .is_some()
                && fixture.budget.state().used[SnapshotBudgetPool::Metadata as usize] == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fixture.root().await.state, RootState::BindingHistory);
    assert!(fixture.objects.path().join(&fixture.reference.key).exists());
}

#[path = "initial_history_retirement_tests.rs"]
mod initial_history;

#[path = "admin_gc_history_tests.rs"]
mod admin_gc_history_tests;
