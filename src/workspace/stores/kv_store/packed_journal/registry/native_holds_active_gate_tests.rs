//! Repeated migration authenticates the actual four-key active gate. Initial
//! migration requires a complete bounded entity census before activation.
use super::*;
use crate::workspace::stores::kv_store::packed_journal::tests::JournalMemoryBackend;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum Fault {
    None,
    RootEpoch,
    Cancel,
    Close,
    ShortBasis,
    RaceCancel,
    RaceClose,
    CensusUnavailable,
}

struct ActiveGateBackend {
    inner: Arc<JournalMemoryBackend>,
    budget: Arc<SnapshotMountBudget>,
    cancel: CancellationToken,
    fault: Fault,
    reads: AtomicUsize,
    control_reads: AtomicUsize,
    auths: AtomicUsize,
    census_auths: AtomicUsize,
    census_scans: AtomicUsize,
    gate_installs: AtomicUsize,
}

#[async_trait::async_trait]
impl WorkspaceKvBackend for ActiveGateBackend {
    fn name(&self) -> &'static str {
        "active-native-gate-test"
    }

    async fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        panic!("active migration must not use unbounded GET")
    }

    async fn scan_prefix(&self, _: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        panic!("migration must not issue an unbounded namespace scan")
    }

    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate_scan_page(prefix, after)?;
        assert!(
            matches!(self.fault, Fault::CensusUnavailable),
            "an active gate must not scan entity namespaces"
        );
        assert_eq!(prefix, HOT_WORKSPACE_PREFIX);
        assert!(after.is_none());
        self.census_scans.fetch_add(1, Ordering::SeqCst);
        Err(WorkspaceError::UnsupportedCapability(
            "fixture cannot complete entity census",
        ))
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
    }

    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let ordinal = self.reads.fetch_add(1, Ordering::SeqCst);
        if keys.iter().any(|key| key.as_slice() == CONTROL_KEY) {
            self.control_reads.fetch_add(1, Ordering::SeqCst);
            return Err(WorkspaceError::UnsupportedCapability(
                "fixture forbids CONTROL reads",
            ));
        }
        assert_eq!(
            keys,
            [
                HOLD_FEATURE.to_vec(),
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                HOLD_JOURNAL_HEADS_FEATURE.to_vec(),
            ]
        );
        assert!(limits.max_value_bytes <= 4096);
        let mut result = self
            .inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await?;
        if matches!(self.fault, Fault::ShortBasis) {
            result.0.pop();
        } else if ordinal == 0 && matches!(self.fault, Fault::RaceCancel | Fault::RaceClose) {
            // The first read is an actual inactive snapshot. A different
            // worker completes migration before the fallback's second read.
            let mut gate = NativeHoldGate::decode(result.0[0].as_deref().unwrap())?;
            assert!(!gate.active);
            gate.active = true;
            let raw = encode(&gate)?;
            let mut rows = self.inner.rows.lock().await;
            rows.insert(HOLD_FEATURE.to_vec(), raw.clone());
            rows.insert(HOLD_JOURNAL_HEADS_FEATURE.to_vec(), raw);
        }
        Ok(result)
    }

    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        assert_eq!(checks.len(), 4);
        assert!(checks.iter().all(|check| check.key != CONTROL_KEY));
        if !writes.is_empty() {
            assert!(matches!(self.fault, Fault::CensusUnavailable));
            assert_eq!(writes.len(), 1);
            let KvWrite::Put { key, value } = &writes[0] else {
                panic!("initial migration must install its inactive gate");
            };
            assert_eq!(key.as_slice(), HOLD_FEATURE);
            assert!(!NativeHoldGate::decode(value)?.active);
            self.gate_installs.fetch_add(1, Ordering::SeqCst);
            return self.inner.compare_and_swap(checks, writes).await;
        }
        let active = checks
            .iter()
            .find(|check| check.key == HOLD_FEATURE)
            .and_then(|check| check.expected.as_deref())
            .map(NativeHoldGate::decode)
            .transpose()?
            .is_some_and(|gate| gate.active);
        if !active {
            assert!(matches!(self.fault, Fault::CensusUnavailable));
            self.census_auths.fetch_add(1, Ordering::SeqCst);
            return self.inner.compare_and_swap(checks, writes).await;
        }
        self.auths.fetch_add(1, Ordering::SeqCst);
        if matches!(self.fault, Fault::RootEpoch) {
            self.inner
                .rows
                .lock()
                .await
                .insert(PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&2u64).unwrap());
        }
        let matched = self.inner.compare_and_swap(checks, writes).await?;
        match self.fault {
            Fault::Cancel | Fault::RaceCancel => self.cancel.cancel(),
            Fault::Close | Fault::RaceClose => self.budget.close(),
            Fault::None | Fault::RootEpoch | Fault::ShortBasis | Fault::CensusUnavailable => {}
        }
        Ok(matched)
    }
}

async fn backend(active: bool, fault: Fault) -> Arc<ActiveGateBackend> {
    let inner = Arc::new(JournalMemoryBackend::default());
    let mut control = ControlState::default();
    if !matches!(fault, Fault::RaceCancel | Fault::RaceClose) {
        control.allocators.insert("x".repeat(200_000), 1);
    }
    let catalog_bytes = encode(&control).unwrap();
    assert_eq!(
        catalog_bytes.len() > 96 << 10,
        !matches!(fault, Fault::RaceCancel | Fault::RaceClose)
    );
    let mut rows = inner.rows.lock().await;
    test_write_topology_rows(&mut rows, &control);
    rows.insert(
        HOLD_FEATURE.to_vec(),
        encode(&NativeHoldGate {
            run: Uuid::new_v4(),
            active: active && !matches!(fault, Fault::RaceCancel | Fault::RaceClose),
        })
        .unwrap(),
    );
    let gate = rows.get(HOLD_FEATURE).unwrap().clone();
    rows.insert(HOLD_JOURNAL_HEADS_FEATURE.to_vec(), gate);
    rows.insert(PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&1u64).unwrap());
    rows.insert(
        LAYER_INVENTORY_GENERATION_KEY.to_vec(),
        encode(&1u64).unwrap(),
    );
    drop(rows);
    Arc::new(ActiveGateBackend {
        inner,
        budget: SnapshotMountBudget::defaults(),
        cancel: CancellationToken::new(),
        fault,
        reads: AtomicUsize::new(0),
        control_reads: AtomicUsize::new(0),
        auths: AtomicUsize::new(0),
        census_auths: AtomicUsize::new(0),
        census_scans: AtomicUsize::new(0),
        gate_installs: AtomicUsize::new(0),
    })
}

#[tokio::test]
async fn active_native_hold_migration_authenticates_without_large_control() {
    let backend = backend(true, Fault::None).await;
    let before = backend.inner.rows.lock().await.clone();
    let store = KvWorkspaceStore::from_arc(backend.clone());
    assert_eq!(
        store
            .migrate_native_packed_holds(&backend.budget, 1, backend.cancel.clone())
            .await
            .unwrap(),
        0
    );
    assert_eq!(backend.reads.load(Ordering::SeqCst), 1);
    assert_eq!(backend.control_reads.load(Ordering::SeqCst), 0);
    assert_eq!(backend.auths.load(Ordering::SeqCst), 1);
    assert_eq!(backend.census_scans.load(Ordering::SeqCst), 0);
    assert_eq!(backend.gate_installs.load(Ordering::SeqCst), 0);
    assert_eq!(backend.budget.state().used, [0; 8]);
    assert_eq!(*backend.inner.rows.lock().await, before);
}

#[tokio::test]
async fn active_native_hold_migration_rejects_authentication_drift_and_shutdown() {
    for fault in [
        Fault::RootEpoch,
        Fault::Cancel,
        Fault::Close,
        Fault::RaceCancel,
        Fault::RaceClose,
    ] {
        let backend = backend(true, fault).await;
        let store = KvWorkspaceStore::from_arc(backend.clone());
        assert!(matches!(
            store
                .migrate_native_packed_holds(&backend.budget, 1, backend.cancel.clone())
                .await,
            Err(WorkspaceError::Busy)
        ));
        assert_eq!(backend.control_reads.load(Ordering::SeqCst), 0);
        assert_eq!(
            backend.reads.load(Ordering::SeqCst),
            1 + usize::from(matches!(fault, Fault::RaceCancel | Fault::RaceClose))
        );
        assert_eq!(backend.auths.load(Ordering::SeqCst), 1);
        assert_eq!(backend.census_scans.load(Ordering::SeqCst), 0);
        assert_eq!(backend.gate_installs.load(Ordering::SeqCst), 0);
        assert_eq!(backend.budget.state().used, [0; 8]);
    }
}

#[tokio::test]
async fn active_native_hold_migration_rejects_cancelled_or_closed_before_read() {
    for closed in [false, true] {
        let backend = backend(true, Fault::None).await;
        if closed {
            backend.budget.close();
        } else {
            backend.cancel.cancel();
        }
        let store = KvWorkspaceStore::from_arc(backend.clone());
        assert!(
            store
                .migrate_native_packed_holds(&backend.budget, 1, backend.cancel.clone())
                .await
                .is_err()
        );
        assert_eq!(backend.reads.load(Ordering::SeqCst), 0);
        assert_eq!(backend.auths.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn missing_or_inactive_native_hold_gate_requires_complete_entity_census() {
    for missing in [false, true] {
        let backend = backend(false, Fault::CensusUnavailable).await;
        if missing {
            backend.inner.rows.lock().await.remove(HOLD_FEATURE);
        }
        let store = KvWorkspaceStore::from_arc(backend.clone());
        assert!(matches!(
            store
                .migrate_native_packed_holds(&backend.budget, 1, backend.cancel.clone())
                .await,
            Err(WorkspaceError::UnsupportedCapability(
                "fixture cannot complete entity census"
            ))
        ));
        assert_eq!(backend.control_reads.load(Ordering::SeqCst), 0);
        assert_eq!(backend.auths.load(Ordering::SeqCst), 0);
        assert_eq!(backend.census_auths.load(Ordering::SeqCst), 1);
        assert_eq!(backend.census_scans.load(Ordering::SeqCst), 1);
        assert_eq!(backend.gate_installs.load(Ordering::SeqCst), 1);
        let rows = backend.inner.rows.lock().await;
        assert!(
            !NativeHoldGate::decode(rows.get(HOLD_FEATURE).unwrap())
                .unwrap()
                .active
        );
        assert!(!rows.keys().any(|key| key.starts_with(HOLD_PREFIX)));
        assert_eq!(backend.budget.state().used, [0; 8]);
    }
}

#[tokio::test]
async fn invalid_active_native_hold_basis_never_authenticates_or_reads_control() {
    for fault in [Fault::ShortBasis, Fault::None] {
        let backend = backend(true, fault).await;
        if matches!(fault, Fault::None) {
            backend
                .inner
                .rows
                .lock()
                .await
                .insert(HOLD_FEATURE.to_vec(), b"corrupt".to_vec());
        }
        let store = KvWorkspaceStore::from_arc(backend.clone());
        assert!(
            store
                .migrate_native_packed_holds(&backend.budget, 1, backend.cancel.clone())
                .await
                .is_err()
        );
        assert_eq!(backend.control_reads.load(Ordering::SeqCst), 0);
        assert_eq!(backend.auths.load(Ordering::SeqCst), 0);
    }
}
