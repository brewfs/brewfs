//! Actual interrupted Prepare -> recovered opaque seed -> timed read-only CAS.
//! No test constructs a source authority or depends on the new retry helper.

use super::*;
use std::sync::atomic::AtomicBool;

const WAIT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    UnchangedFalse,
    RootOnce,
    RootChurn,
    LeaseExtension,
    OpenExtension,
    UnknownReply,
    FreshReadError,
    LockedFirstDeadline,
}

struct Submission {
    checks: Vec<KvCheck>,
    deadline: i64,
}

struct Schedule {
    fault: Fault,
    seed_key: Vec<u8>,
    lease_key: Vec<u8>,
    open_key: Vec<u8>,
    submissions: Vec<Submission>,
    full_reads: usize,
    read_errors: usize,
    mutation_calls: usize,
    after_fault: Option<BTreeMap<Vec<u8>, Vec<u8>>>,
}

#[derive(Clone)]
struct SeedBackend {
    inner: FreezeBackend,
    stop_original_q: Arc<AtomicBool>,
    schedule: Arc<Mutex<Option<Schedule>>>,
}

impl SeedBackend {
    fn new(inner: FreezeBackend) -> Self {
        Self {
            inner,
            stop_original_q: Arc::new(AtomicBool::new(true)),
            schedule: Default::default(),
        }
    }

    async fn bounded_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        limits.validate_scan_page(prefix, after)?;
        let rows = self.inner.rows.lock().await;
        let lower = after.map_or_else(|| Included(prefix.to_vec()), |key| Excluded(key.to_vec()));
        let selected = || {
            rows.range((lower.clone(), Unbounded))
                .take_while(|(key, _)| key.starts_with(prefix))
                .take(limits.max_records)
        };
        let mut total = 0usize;
        let mut response = 0usize;
        // Admit each stored key/value before cloning any result. The range is
        // bounded at iteration; an unbounded scan is never truncated afterward.
        for (key, value) in selected() {
            if key.len() > limits.max_key_bytes || value.len() > limits.max_value_bytes {
                return Err(WorkspaceError::InvalidReadPlan(
                    "seed fixture scan row bound".into(),
                ));
            }
            let row_bytes = key.len().checked_add(value.len()).ok_or_else(|| {
                WorkspaceError::InvalidReadPlan("seed fixture scan byte overflow".into())
            })?;
            total = total.checked_add(row_bytes).ok_or_else(|| {
                WorkspaceError::InvalidReadPlan("seed fixture scan total overflow".into())
            })?;
            response = response
                .checked_add(row_bytes)
                .and_then(|bytes| bytes.checked_add(32))
                .ok_or_else(|| {
                    WorkspaceError::InvalidReadPlan("seed fixture scan response overflow".into())
                })?;
            if total > limits.max_total_bytes || response > limits.max_response_bytes {
                return Err(WorkspaceError::InvalidReadPlan(
                    "seed fixture scan byte bound".into(),
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

    async fn cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        // Stop exactly the original Q submission, after real Prepare/NQB3.
        // Neither a prepared nor quiesced seed/journal is forged in the test.
        if self.stop_original_q.load(Ordering::SeqCst)
            && writes.iter().any(|write| {
                matches!(write, KvWrite::Put { key, .. }
                    if key.starts_with(b"packed/native-freeze-basis/"))
            })
        {
            for write in writes {
                if let KvWrite::Put { key, value } = write
                    && key.starts_with(HOT_JOURNAL_PREFIX)
                {
                    let journal: SealJournal = decode(value)?;
                    if journal.phase == SealPhase::Quiesced
                        && self.stop_original_q.swap(false, Ordering::SeqCst)
                    {
                        return Err(WorkspaceError::Backend(
                            "test interrupted original Q before submission".into(),
                        ));
                    }
                }
            }
        }

        let mut schedule = self.schedule.lock().await;
        let Some(schedule) = schedule.as_mut() else {
            return self.inner.cas(checks, writes, before).await;
        };
        if !writes.is_empty() {
            schedule.mutation_calls += 1;
        }
        let target = writes.is_empty()
            && checks.iter().any(|row| row.key == schedule.seed_key)
            && checks.iter().any(|row| row.key == schedule.open_key)
            && checks.iter().any(|row| row.key == schedule.lease_key)
            && checks
                .iter()
                .any(|row| row.key.starts_with(HOT_JOURNAL_PREFIX))
            && checks
                .iter()
                .any(|row| row.key.as_slice() == PACKED_ROOT_GENERATION_KEY);
        if !target {
            return self.inner.cas(checks, writes, before).await;
        }
        let deadline = before.expect("actual seed validation must use timed CAS");
        schedule.submissions.push(Submission {
            checks: checks.to_vec(),
            deadline,
        });
        let ordinal = schedule.submissions.len();
        {
            let mut rows = self.inner.rows.lock().await;
            assert!(
                checks
                    .iter()
                    .all(|check| rows.get(&check.key) == check.expected.as_ref()),
                "scheduled no-op was not based on an actual complete catalog snapshot"
            );
            assert!(self.inner.now.load(Ordering::SeqCst) < deadline);
            if ordinal == 1 && schedule.fault == Fault::UnknownReply {
                schedule.after_fault = Some(rows.clone());
                return Err(WorkspaceError::Backend(
                    "test unknown native seed read-only CAS reply".into(),
                ));
            }
            if ordinal == 1 && schedule.fault == Fault::UnchangedFalse {
                // Fault injection supplies false without any observable fresh
                // conflict proof. It cannot authorize a second submission.
                schedule.after_fault = Some(rows.clone());
                return Ok(false);
            }
            if ordinal == 1 || schedule.fault == Fault::RootChurn {
                let old_root = rows.get(PACKED_ROOT_GENERATION_KEY).unwrap().clone();
                let next = next_packed_root_generation(&Some(old_root))?;
                rows.insert(PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&next)?);
                match schedule.fault {
                    Fault::LeaseExtension => {
                        let mut lease: SnapshotLease =
                            decode(rows.get(&schedule.lease_key).unwrap())?;
                        lease.expires_at_ns += 1;
                        rows.insert(schedule.lease_key.clone(), encode(&lease)?);
                    }
                    Fault::OpenExtension => {
                        let mut open: SnapshotOpenRecord =
                            decode(rows.get(&schedule.open_key).unwrap())?;
                        open.expires_at_ns += 1;
                        rows.insert(schedule.open_key.clone(), encode(&open)?);
                    }
                    _ => {}
                }
                schedule.after_fault = Some(rows.clone());
            }
            if ordinal == 2 && schedule.fault == Fault::LockedFirstDeadline {
                // Move the backend clock at the locked CAS, after the fresh
                // complete read. Expiry is checked by the actual fixture CAS.
                self.inner
                    .now
                    .store(schedule.submissions[0].deadline, Ordering::SeqCst);
            }
        }
        // Root changes yield a real exact-check mismatch in the backend CAS.
        // All successful and deadline-expired submissions use its normal path.
        self.inner.cas(checks, writes, before).await
    }
}

#[async_trait]
impl WorkspaceKvBackend for SeedBackend {
    fn name(&self) -> &'static str {
        "packed-native-seed-readonly-test"
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
        {
            let mut schedule = self.schedule.lock().await;
            if let Some(schedule) = schedule.as_mut()
                && keys.iter().any(|key| key.starts_with(HOT_JOURNAL_PREFIX))
                && keys.iter().any(|key| key == &schedule.seed_key)
                && keys.iter().any(|key| key == &schedule.open_key)
            {
                schedule.full_reads += 1;
                if schedule.fault == Fault::FreshReadError && !schedule.submissions.is_empty() {
                    schedule.read_errors += 1;
                    return Err(WorkspaceError::Backend(
                        "test fresh native seed bounded read error".into(),
                    ));
                }
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
        self.bounded_page(prefix, None, limits).await
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.bounded_page(prefix, after, limits).await
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None).await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, Some(deadline)).await
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        not_before_ns: Option<i64>,
        before_ns: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        crate::workspace::stores::kv_backend::validate_cas_time_window(not_before_ns, before_ns)?;
        if not_before_ns.is_some_and(|lower| self.inner.now.load(Ordering::SeqCst) < lower) {
            return Err(WorkspaceError::Busy);
        }
        self.cas(checks, writes, before_ns).await
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
        self.inner
            .authenticate_checks_before_bounded(checks, expires_at_ns, limits)
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
    }
}

struct Source {
    backend: SeedBackend,
    fence: Arc<PackedNativeQuiesceFence<SeedBackend>>,
    before: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Source {
    async fn new(fault: Fault) -> Self {
        let (old_store, inner, guard, layers, budget) = fixture().await;
        drop(old_store);
        let backend = SeedBackend::new(inner);
        let store = Arc::new(
            KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
        );
        let journal = JournalId::new();
        let planned_head = LayerId::new();
        let interrupted = store
            .clone()
            .begin_packed_native_quiesce(
                guard.clone(),
                layers.clone(),
                journal,
                planned_head,
                budget.clone(),
            )
            .await;
        assert!(matches!(interrupted, Err(WorkspaceError::Backend(_))));
        assert!(!backend.stop_original_q.load(Ordering::SeqCst));
        let seed_key = native_seed::seed_key(journal);
        let seed_before = backend.get(&seed_key).await.unwrap().unwrap();
        assert!(seed_before.starts_with(b"NQB3"));
        let control = test_topology_from_rows(&*backend.inner.rows.lock().await);
        assert_eq!(
            control.journals.get(&journal).unwrap().phase,
            SealPhase::Prepare
        );
        assert_eq!(
            control.journals.get(&journal).unwrap().new_head_layer_id,
            Some(planned_head)
        );
        assert!(
            backend
                .scan_prefix(b"packed/native-recovery-claim/")
                .await
                .unwrap()
                .is_empty()
        );
        let prepared_open: SnapshotOpenRecord = decode_open_value(
            &backend
                .get(&open_snapshot_key(guard.workspace_id))
                .await
                .unwrap()
                .unwrap(),
            OPEN_RECORD_MAX_BYTES,
        )
        .unwrap();
        let owner = prepared_open.owner_id;
        let open = store
            .open_workspace_snapshot(
                guard.workspace_id,
                &owner,
                std::time::Duration::from_secs(300),
            )
            .await
            .unwrap();
        assert_eq!(open.state, SnapshotOpenState::Recovering);
        assert!(open.recovery_required);
        let fence = store
            .recover_packed_native_prepare(
                NativePrepareRecoveryRequest {
                    journal_id: journal,
                    owner_id: owner,
                    new_lease_id: None,
                    ttl_ns: 300_000_000_000,
                },
                budget,
            )
            .await
            .unwrap();
        assert!(fence.seed_authority.is_some());
        assert_eq!(fence.source_guard(), &guard);
        assert_eq!(fence.mapping().old_guard(), &guard);
        assert_eq!(fence.mapping().old_layers(), &layers);
        assert_eq!(fence.mapping().planned_head_layer_id(), planned_head);
        let control = test_topology_from_rows(&*backend.inner.rows.lock().await);
        assert_eq!(
            control.journals.get(&journal).unwrap().phase,
            SealPhase::Quiesced
        );
        let seed_after = backend.get(&seed_key).await.unwrap().unwrap();
        assert!(seed_after.starts_with(b"NQB3"));
        assert_ne!(seed_before, seed_after);
        let before = backend.inner.rows.lock().await.clone();
        *backend.schedule.lock().await = Some(Schedule {
            fault,
            seed_key,
            lease_key: hot_lease_key(guard.workspace_id, guard.lease_id),
            open_key: open_snapshot_key(guard.workspace_id),
            submissions: Vec::new(),
            full_reads: 0,
            read_errors: 0,
            mutation_calls: 0,
            after_fault: None,
        });
        Self {
            backend,
            fence,
            before,
        }
    }

    async fn validate(&self) -> Result<(), WorkspaceError> {
        tokio::time::timeout(WAIT, self.fence.seed_authority.as_ref().unwrap().validate())
            .await
            .expect("seed validation exceeded small deterministic test deadline")
    }

    async fn assert_calls(&self, noops: usize, full_reads: usize, read_errors: usize) {
        let schedule = self.backend.schedule.lock().await;
        let schedule = schedule.as_ref().unwrap();
        assert_eq!(schedule.submissions.len(), noops);
        assert_eq!(schedule.full_reads, full_reads);
        assert_eq!(schedule.read_errors, read_errors);
        assert_eq!(
            schedule.mutation_calls, 0,
            "seed validation submitted a mutation"
        );
        let first = &schedule.submissions[0];
        let first_rows: BTreeMap<_, _> = first
            .checks
            .iter()
            .map(|row| (row.key.clone(), row.expected.clone()))
            .collect();
        let journal_key = hot_journal_key(
            self.fence.source_guard().workspace_id,
            self.fence.mapping().journal_id(),
        );
        assert_eq!(
            first_rows.get(&journal_key),
            Some(&self.before.get(&journal_key).cloned())
        );
        assert!(
            first
                .checks
                .iter()
                .all(|row| { self.before.get(&row.key) == row.expected.as_ref() })
        );
        let lease: SnapshotLease = decode(self.before.get(&schedule.lease_key).unwrap()).unwrap();
        let open: SnapshotOpenRecord =
            decode(self.before.get(&schedule.open_key).unwrap()).unwrap();
        assert_eq!(first.deadline, lease.expires_at_ns.min(open.expires_at_ns));
        for submitted in &schedule.submissions {
            assert_eq!(
                submitted.deadline, first.deadline,
                "first deadline was extended"
            );
            let current: BTreeMap<_, _> = submitted
                .checks
                .iter()
                .map(|row| (row.key.clone(), row.expected.clone()))
                .collect();
            assert_eq!(current.len(), first_rows.len());
            assert!(
                first_rows.iter().all(|(key, value)| {
                    key.as_slice() == PACKED_ROOT_GENERATION_KEY || current.get(key) == Some(value)
                }),
                "a later submission adopted non-root authority changes"
            );
        }
        let expected = schedule.after_fault.as_ref().unwrap();
        assert_eq!(expected.len(), self.before.len());
        let root_changes = if schedule.fault == Fault::RootChurn {
            noops as u64
        } else if matches!(schedule.fault, Fault::UnchangedFalse | Fault::UnknownReply) {
            0
        } else {
            1
        };
        let old_root: u64 = decode(self.before.get(PACKED_ROOT_GENERATION_KEY).unwrap()).unwrap();
        let new_root: u64 = decode(expected.get(PACKED_ROOT_GENERATION_KEY).unwrap()).unwrap();
        assert_eq!(new_root, old_root + root_changes);
        assert!(
            self.before.iter().all(|(key, value)| {
                key.as_slice() == PACKED_ROOT_GENERATION_KEY
                    || (schedule.fault == Fault::LeaseExtension && key == &schedule.lease_key)
                    || (schedule.fault == Fault::OpenExtension && key == &schedule.open_key)
                    || expected.get(key) == Some(value)
            }),
            "the scheduled event altered unrelated durable metadata"
        );
        if schedule.fault == Fault::LeaseExtension {
            let mut changed: SnapshotLease =
                decode(expected.get(&schedule.lease_key).unwrap()).unwrap();
            assert_eq!(changed.expires_at_ns, lease.expires_at_ns + 1);
            changed.expires_at_ns = lease.expires_at_ns;
            assert_eq!(changed, lease);
        }
        if schedule.fault == Fault::OpenExtension {
            let mut changed: SnapshotOpenRecord =
                decode(expected.get(&schedule.open_key).unwrap()).unwrap();
            assert_eq!(changed.expires_at_ns, open.expires_at_ns + 1);
            changed.expires_at_ns = open.expires_at_ns;
            assert_eq!(changed, open);
        }
        assert_eq!(
            &*self.backend.inner.rows.lock().await,
            expected,
            "read-only validation changed persistent catalog rows"
        );
    }
}

#[tokio::test]
async fn seed_readonly_unchanged_false_has_no_root_retry_authority() {
    let source = Source::new(Fault::UnchangedFalse).await;
    assert!(matches!(source.validate().await, Err(WorkspaceError::Busy)));
    source.assert_calls(1, 2, 0).await;
}

#[tokio::test]
async fn seed_readonly_root_once_rebuilds_full_proof_without_extending_first_deadline() {
    let source = Source::new(Fault::RootOnce).await;
    source.validate().await.unwrap();
    source.assert_calls(2, 2, 0).await;
}

#[tokio::test]
async fn seed_readonly_root_churn_stops_after_three_noop_submissions() {
    let source = Source::new(Fault::RootChurn).await;
    assert!(matches!(source.validate().await, Err(WorkspaceError::Busy)));
    source.assert_calls(3, 3, 0).await;
}

#[tokio::test]
async fn seed_readonly_root_plus_valid_lease_expiry_extension_is_not_root_only() {
    let source = Source::new(Fault::LeaseExtension).await;
    assert!(matches!(source.validate().await, Err(WorkspaceError::Busy)));
    source.assert_calls(1, 2, 0).await;
}

#[tokio::test]
async fn seed_readonly_root_plus_valid_open_expiry_extension_is_not_root_only() {
    let source = Source::new(Fault::OpenExtension).await;
    assert!(matches!(source.validate().await, Err(WorkspaceError::Busy)));
    source.assert_calls(1, 2, 0).await;
}

#[tokio::test]
async fn seed_readonly_unknown_reply_propagates_without_another_submission() {
    let source = Source::new(Fault::UnknownReply).await;
    assert!(matches!(source.validate().await,
        Err(WorkspaceError::Backend(message)) if message.contains("unknown native seed")));
    source.assert_calls(1, 1, 0).await;
}

#[tokio::test]
async fn seed_readonly_fresh_bounded_read_error_propagates_without_another_submission() {
    let source = Source::new(Fault::FreshReadError).await;
    assert!(matches!(source.validate().await,
        Err(WorkspaceError::Backend(message)) if message.contains("fresh native seed")));
    source.assert_calls(1, 2, 1).await;
}

#[tokio::test]
async fn seed_readonly_first_deadline_is_enforced_at_actual_locked_cas() {
    let source = Source::new(Fault::LockedFirstDeadline).await;
    assert!(matches!(
        source.validate().await,
        Err(WorkspaceError::Fenced)
    ));
    source.assert_calls(2, 2, 0).await;
}
