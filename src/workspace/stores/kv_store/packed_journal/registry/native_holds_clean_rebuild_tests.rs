//! Actual native hold preparations with independently advancing root epochs.
use super::*;
use crate::workspace::stores::kv_store::packed_journal::tests::JournalMemoryBackend;
use crate::workspace::stores::kv_store::packed_writer_authority::{
    PackedWriterAuthority, PackedWriterOwner, packed_writer_key,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

enum Fault {
    RootOnce,
    RootAlways,
    EpochOverlap,
    QuotaAfterFirstFullScan,
    NoProgressFalse,
    ReadOnlyError,
    NonRoot(Vec<u8>),
    Expire,
    Close,
    CloseWithoutRootMove,
    MutationFalse,
    MutationError,
    MutationAppliedReplyLost,
    MutationAppliedReplyLostThenExpire,
    MutationAppliedReplyLostThenChangePwa,
    HoldMismatch,
    HoldMismatchHotAdvanced,
}

struct BoundedReadTrace {
    keys: Vec<Vec<u8>>,
    limits: KvReadLimits,
    metadata_bytes: u64,
}

struct FinalCasTrace {
    checks: Vec<KvCheck>,
    writes: Vec<KvWrite>,
    deadline: i64,
    metadata_bytes: u64,
}

struct MovingRootBackend {
    inner: Arc<JournalMemoryBackend>,
    budget: Arc<SnapshotMountBudget>,
    fault: Fault,
    scans: AtomicUsize,
    reads: Mutex<Vec<Vec<Vec<u8>>>>,
    empty_cas: AtomicUsize,
    mutations: AtomicUsize,
    expired: AtomicBool,
    epoch_reads: AtomicUsize,
    read_plans: Mutex<Vec<BoundedReadTrace>>,
    final_packets: Mutex<Vec<FinalCasTrace>>,
}

#[async_trait::async_trait]
impl WorkspaceKvBackend for MovingRootBackend {
    fn name(&self) -> &'static str {
        "clean-finish-moving-root-test"
    }

    async fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        panic!("clean finish must not issue unbounded GET")
    }

    async fn scan_prefix(&self, _: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        panic!("clean finish must not materialize a namespace")
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
    }

    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.read_plans.lock().await.push(BoundedReadTrace {
            keys: keys.to_vec(),
            limits,
            metadata_bytes: self.budget.state().used[SnapshotBudgetPool::Metadata as usize],
        });
        if keys.len() == 3
            && keys[0].as_slice() == HOLD_FEATURE
            && matches!(self.fault, Fault::EpochOverlap)
            && self.epoch_reads.fetch_add(1, Ordering::SeqCst) == 0
        {
            self.inner
                .rows
                .lock()
                .await
                .insert(PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&2u64).unwrap());
        }
        if keys.len() == 1
            && keys[0].starts_with(HOLD_PREFIX)
            && matches!(
                self.fault,
                Fault::HoldMismatch | Fault::HoldMismatchHotAdvanced
            )
        {
            // Simulate an independently changed derived hold after the
            // packet's source and epoch reads. The source advance variant
            // models a lease heartbeat committing both rows together.
            let mut rows = self.inner.rows.lock().await;
            let raw = rows.get(&keys[0]).cloned().expect("fixture native hold");
            let mut hold = NativeHold::decode(&raw).unwrap();
            if let NativeHold::Lease { generation, .. } = &mut hold {
                *generation += 1;
            } else {
                panic!("fixture native hold must be a lease");
            }
            rows.insert(keys[0].clone(), hold.encode().unwrap());
            if matches!(self.fault, Fault::HoldMismatchHotAdvanced) {
                let source = rows
                    .iter()
                    .find(|(key, _)| key.starts_with(HOT_LEASE_PREFIX))
                    .map(|(key, raw)| (key.clone(), raw.clone()))
                    .expect("fixture hot lease");
                let mut lease: SnapshotLease = decode(&source.1).unwrap();
                lease.holder_generation += 1;
                lease.updated_at_ns += 1;
                rows.insert(source.0, encode(&lease).unwrap());
            }
        }
        if keys.iter().any(|key| key.as_slice() == CONTROL_KEY) {
            self.reads.lock().await.push(keys.to_vec());
        }
        let (values, now) = self
            .inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await?;
        Ok((
            values,
            if self.expired.load(Ordering::SeqCst) {
                100
            } else {
                now
            },
        ))
    }

    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        assert_eq!(prefix, REGISTRY_ROOT_PREFIX.as_bytes());
        let scan = self.scans.fetch_add(1, Ordering::SeqCst);
        if matches!(self.fault, Fault::CloseWithoutRootMove) {
            self.budget.close();
        }
        if (scan == 0
            && !matches!(
                self.fault,
                Fault::EpochOverlap | Fault::QuotaAfterFirstFullScan | Fault::CloseWithoutRootMove
            ))
            || matches!(self.fault, Fault::RootAlways)
            || (scan == 128 && matches!(self.fault, Fault::QuotaAfterFirstFullScan))
        {
            // Model a different retained reader's genuine root renewal after
            // the successful pre-page census CAS, before its post-page CAS.
            let mut rows = self.inner.rows.lock().await;
            let root: u64 = decode(rows.get(PACKED_ROOT_GENERATION_KEY).unwrap()).unwrap();
            rows.insert(
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                encode(&(root + 1)).unwrap(),
            );
            if let Fault::NonRoot(key) = &self.fault {
                rows.insert(key.clone(), b"independent non-root successor".to_vec());
            }
            if matches!(self.fault, Fault::Expire) {
                self.expired.store(true, Ordering::SeqCst);
            }
            if matches!(self.fault, Fault::Close) {
                self.budget.close();
            }
        }
        self.inner
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await
    }

    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        assert!(writes.is_empty(), "preparation must never mutate");
        let call = self.empty_cas.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            if matches!(self.fault, Fault::NoProgressFalse) {
                return Ok(false);
            }
            if matches!(self.fault, Fault::ReadOnlyError) {
                return Err(WorkspaceError::Backend("unknown census reply".into()));
            }
        }
        self.inner.compare_and_swap(checks, writes).await
    }

    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        assert_eq!(deadline, 100, "original deadline must remain fixed");
        self.final_packets.lock().await.push(FinalCasTrace {
            checks: checks.to_vec(),
            writes: writes.to_vec(),
            deadline,
            metadata_bytes: self.budget.state().used[SnapshotBudgetPool::Metadata as usize],
        });
        if writes.is_empty() {
            if matches!(self.fault, Fault::MutationAppliedReplyLostThenChangePwa)
                && self.mutations.load(Ordering::SeqCst) != 0
            {
                // The unknown read already observed every correct successor.
                // A later owner change must still fail the final read-only CAS.
                let pwa = checks
                    .iter()
                    .find(|row| row.key.starts_with(b"packed/writer/"))
                    .unwrap();
                self.inner
                    .rows
                    .lock()
                    .await
                    .insert(pwa.key.clone(), b"later writer incarnation".to_vec());
            }
            return self
                .inner
                .compare_and_swap_before(checks, writes, deadline)
                .await;
        }
        self.mutations.fetch_add(1, Ordering::SeqCst);
        if matches!(self.fault, Fault::MutationFalse) {
            return Ok(false);
        }
        if matches!(self.fault, Fault::MutationError) {
            return Err(WorkspaceError::Backend(
                "unknown final mutation reply".into(),
            ));
        }
        let result = self
            .inner
            .compare_and_swap_before(checks, writes, deadline)
            .await?;
        if result
            && matches!(
                self.fault,
                Fault::MutationAppliedReplyLost
                    | Fault::MutationAppliedReplyLostThenExpire
                    | Fault::MutationAppliedReplyLostThenChangePwa
            )
        {
            if matches!(self.fault, Fault::MutationAppliedReplyLostThenExpire) {
                self.expired.store(true, Ordering::SeqCst);
            }
            return Err(WorkspaceError::Backend("lost applied final reply".into()));
        }
        Ok(result)
    }
}

struct Fixture {
    backend: Arc<MovingRootBackend>,
    store: KvWorkspaceStore<MovingRootBackend>,
    checks: Vec<KvCheck>,
    writes: Vec<KvWrite>,
    lease_hold: Vec<u8>,
    snapshot_hold: Vec<u8>,
    pwa: Vec<u8>,
}

async fn fixture(fault: Fault) -> Fixture {
    let workspace_id = WorkspaceId::new();
    let revision = BaseRevision {
        layer_id: LayerId::new(),
        sealed_version: 1,
        root_hash: [7; 32],
    };
    let mut lease = SnapshotLease {
        lease_id: LeaseId::new(),
        workspace_id,
        base_revision: revision.clone(),
        holder_generation: 1,
        writable: true,
        state: LeaseState::Active,
        expires_at_ns: 100,
        created_at_ns: 1,
        updated_at_ns: 1,
    };
    let snapshot = SnapshotRecord {
        snapshot_id: SnapshotId::new(),
        name: None,
        revision,
        owner_id: None,
        created_at_ns: 1,
    };
    let mut control = ControlState::default();
    control.leases.insert(lease.lease_id, lease.clone());
    let layer = LayerRecord {
        layer_id: snapshot.revision.layer_id,
        parent_layer_id: None,
        state: LayerState::Sealed,
        schema_version: WORKSPACE_SCHEMA_VERSION,
        sealed_version: Some(snapshot.revision.sealed_version),
        delta_digest: Some([8; 32]),
        root_hash: Some(snapshot.revision.root_hash),
        depth: 0,
        owner_workspace_id: None,
        next_sequence: 1,
        owned_slice_count: 0,
        owned_bytes: 0,
        created_at_ns: 1,
        sealed_at_ns: Some(1),
    };
    control.layers.insert(layer.layer_id, layer.clone());
    let lease_hold = NativeHold::lease(&lease).unwrap();
    let snapshot_hold = NativeHold::snapshot(&snapshot);
    let open = SnapshotOpenRecord {
        workspace_id,
        owner_id: "clean-rebuild-test".into(),
        generation: 1,
        expires_at_ns: 100,
        state: SnapshotOpenState::Ready,
        recovery_required: false,
    };
    let pwa = packed_writer_key(workspace_id);
    let writer = PackedWriterAuthority {
        workspace_id,
        incarnation: 1,
        owner: Some(PackedWriterOwner::Administrative {
            lease_id: lease.lease_id,
            holder_generation: 1,
            open_owner: open.owner_id.clone(),
            open_generation: 1,
            recovering: false,
        }),
    };
    let inner = Arc::new(JournalMemoryBackend::default());
    {
        let mut rows = inner.rows.lock().await;
        test_write_topology_rows(&mut rows, &control);
        rows.insert(hot_layer_key(layer.layer_id), encode(&layer).unwrap());
        rows.insert(
            hot_lease_key(lease.workspace_id, lease.lease_id),
            encode(&lease).unwrap(),
        );
        rows.insert(pwa.clone(), writer.encode().unwrap());
        rows.insert(open_snapshot_key(workspace_id), encode(&open).unwrap());
        rows.insert(
            HOLD_FEATURE.to_vec(),
            encode(&NativeHoldGate {
                run: Uuid::new_v4(),
                active: true,
            })
            .unwrap(),
        );
        let gate = rows.get(HOLD_FEATURE).unwrap().clone();
        rows.insert(HOLD_JOURNAL_HEADS_FEATURE.to_vec(), gate);
        rows.insert(lease_hold.key(), lease_hold.encode().unwrap());
        rows.insert(PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&1u64).unwrap());
        rows.insert(
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            encode(&1u64).unwrap(),
        );
    }
    let keys = vec![
        CONTROL_KEY.to_vec(),
        hot_lease_key(lease.workspace_id, lease.lease_id),
        hot_snapshot_key(snapshot.snapshot_id),
        pwa.clone(),
        open_snapshot_key(workspace_id),
        PACKED_ROOT_GENERATION_KEY.to_vec(),
        LAYER_INVENTORY_GENERATION_KEY.to_vec(),
    ];
    let values = inner.get_many_consistent(&keys).await.unwrap();
    let checks = keys
        .into_iter()
        .zip(values)
        .map(|(key, expected)| KvCheck { key, expected })
        .collect();
    lease.state = LeaseState::Released;
    lease.updated_at_ns = 1;
    control.leases.insert(lease.lease_id, lease.clone());
    control
        .snapshots
        .insert(snapshot.snapshot_id, snapshot.clone());
    let mut closed_open = open;
    closed_open.expires_at_ns = 1;
    let writes = vec![
        KvWrite::Put {
            key: hot_lease_key(lease.workspace_id, lease.lease_id),
            value: encode(&lease).unwrap(),
        },
        KvWrite::Put {
            key: hot_snapshot_key(snapshot.snapshot_id),
            value: encode(&snapshot).unwrap(),
        },
        KvWrite::Put {
            key: open_snapshot_key(workspace_id),
            value: encode(&closed_open).unwrap(),
        },
        KvWrite::Put {
            key: pwa.clone(),
            value: PackedWriterAuthority {
                owner: None,
                incarnation: 2,
                ..writer
            }
            .encode()
            .unwrap(),
        },
    ];
    let budget = SnapshotMountBudget::defaults();
    let backend = Arc::new(MovingRootBackend {
        inner,
        budget: budget.clone(),
        fault,
        scans: AtomicUsize::new(0),
        reads: Mutex::new(Vec::new()),
        empty_cas: AtomicUsize::new(0),
        mutations: AtomicUsize::new(0),
        expired: AtomicBool::new(false),
        epoch_reads: AtomicUsize::new(0),
        read_plans: Mutex::new(Vec::new()),
        final_packets: Mutex::new(Vec::new()),
    });
    let store = KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget);
    Fixture {
        backend,
        store,
        checks,
        writes,
        lease_hold: lease_hold.key(),
        snapshot_hold: snapshot_hold.key(),
        pwa,
    }
}

#[tokio::test]
async fn clean_finish_rebuilds_actual_hold_census_only_on_independent_root_advance() {
    let mut fixture = fixture(Fault::RootOnce).await;
    let _owner = fixture
        .store
        .prepare_clean_publication_native_owner_cas(&mut fixture.checks, &mut fixture.writes, 100)
        .await
        .unwrap();
    assert_eq!(fixture.backend.scans.load(Ordering::SeqCst), 2);
    let reads = fixture.backend.reads.lock().await;
    assert_eq!(reads.len(), 2);
    assert_eq!(reads[0], reads[1]);
    for key in [
        &fixture.lease_hold,
        &fixture.snapshot_hold,
        &fixture.pwa,
        &HOLD_FEATURE.to_vec(),
    ] {
        assert!(reads[0].contains(key));
    }
    drop(reads);
    fixture
        .store
        .clean_exact_cas(&fixture.checks, &fixture.writes, 100)
        .await
        .unwrap();
    assert_eq!(fixture.backend.mutations.load(Ordering::SeqCst), 1);
    let rows = fixture.backend.inner.rows.lock().await;
    assert!(!rows.contains_key(&fixture.lease_hold));
    assert!(rows.contains_key(&fixture.snapshot_hold));
    assert_eq!(
        decode::<u64>(rows.get(PACKED_ROOT_GENERATION_KEY).unwrap()).unwrap(),
        3
    );
}

#[tokio::test]
async fn clean_finish_rebuild_rejects_any_planned_delete_before_backend_requests() {
    let mut fixture = fixture(Fault::RootOnce).await;
    fixture.writes.push(KvWrite::Delete {
        key: b"physical-delete-is-not-publication".to_vec(),
    });
    assert!(matches!(
        fixture
            .store
            .prepare_clean_publication_native_owner_cas(
                &mut fixture.checks,
                &mut fixture.writes,
                100,
            )
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(fixture.backend.reads.lock().await.len(), 0);
    assert_eq!(fixture.backend.empty_cas.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.backend.scans.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.backend.mutations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn clean_finish_rebuild_rejects_every_changed_non_root_predecessor() {
    // Both real sidecar predecessors and all original authority facts are fixed.
    for select in 0..7 {
        let mut fixture = fixture(Fault::RootOnce).await;
        let key = match select {
            0 => fixture.lease_hold.clone(),
            1 => fixture.snapshot_hold.clone(),
            2 => fixture.pwa.clone(),
            3 => HOLD_FEATURE.to_vec(),
            4 => LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            5 => CONTROL_KEY.to_vec(),
            _ => fixture
                .checks
                .iter()
                .find(|row| row.key.starts_with(HOT_LEASE_PREFIX))
                .unwrap()
                .key
                .clone(),
        };
        // The store owns an Arc, so choose the fault while constructing the
        // replacement wrapper around the same genuine fixture rows below.
        let backend = Arc::new(MovingRootBackend {
            inner: fixture.backend.inner.clone(),
            budget: fixture.backend.budget.clone(),
            fault: Fault::NonRoot(key),
            scans: AtomicUsize::new(0),
            reads: Mutex::new(Vec::new()),
            empty_cas: AtomicUsize::new(0),
            mutations: AtomicUsize::new(0),
            expired: AtomicBool::new(false),
            epoch_reads: AtomicUsize::new(0),
            read_plans: Mutex::new(Vec::new()),
            final_packets: Mutex::new(Vec::new()),
        });
        let store = KvWorkspaceStore::from_arc(backend.clone())
            .with_packed_reader_pin_budget(backend.budget.clone());
        assert!(
            store
                .prepare_clean_publication_native_owner_cas(
                    &mut fixture.checks,
                    &mut fixture.writes,
                    100
                )
                .await
                .is_err()
        );
        assert_eq!(backend.scans.load(Ordering::SeqCst), 1);
        assert_eq!(backend.mutations.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn clean_finish_rebuild_is_bounded_and_requires_strict_progress() {
    for fault in [
        Fault::RootAlways,
        Fault::NoProgressFalse,
        Fault::ReadOnlyError,
        Fault::Expire,
        Fault::Close,
        Fault::CloseWithoutRootMove,
    ] {
        let mut fixture = fixture(fault).await;
        assert!(
            fixture
                .store
                .prepare_clean_publication_native_owner_cas(
                    &mut fixture.checks,
                    &mut fixture.writes,
                    100
                )
                .await
                .is_err()
        );
        assert_eq!(fixture.backend.mutations.load(Ordering::SeqCst), 0);
        assert!(fixture.backend.scans.load(Ordering::SeqCst) <= MAX_NATIVE_READ_ATTEMPTS);
        if matches!(fixture.backend.fault, Fault::RootAlways) {
            assert_eq!(
                fixture.backend.scans.load(Ordering::SeqCst),
                MAX_NATIVE_READ_ATTEMPTS
            );
        }
        if matches!(fixture.backend.fault, Fault::ReadOnlyError) {
            assert_eq!(fixture.backend.empty_cas.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.backend.reads.lock().await.len(), 1);
        }
    }
}

#[tokio::test]
async fn clean_finish_never_retries_false_or_unknown_final_mutation() {
    for fault in [
        Fault::MutationFalse,
        Fault::MutationError,
        Fault::MutationAppliedReplyLost,
    ] {
        let mut fixture = fixture(fault).await;
        let _owner = fixture
            .store
            .prepare_clean_publication_native_owner_cas(
                &mut fixture.checks,
                &mut fixture.writes,
                100,
            )
            .await
            .unwrap();
        let result = fixture
            .store
            .clean_exact_cas(&fixture.checks, &fixture.writes, 100)
            .await;
        assert_eq!(
            result.is_ok(),
            matches!(fixture.backend.fault, Fault::MutationAppliedReplyLost)
        );
        assert_eq!(fixture.backend.mutations.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.backend.scans.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn native_owner_hold_mismatch_rebuilds_when_hot_source_advanced() {
    let mut fixture = fixture(Fault::HoldMismatchHotAdvanced).await;
    let original_writes = fixture.writes.clone();
    let (epochs_match, owner) = fixture
        .store
        .prepare_native_owner_cas_with_epoch_status(&mut fixture.checks, &mut fixture.writes)
        .await
        .unwrap();
    assert!(!epochs_match);
    assert!(owner.is_none());
    assert_eq!(fixture.writes, original_writes);
    let source = fixture
        .checks
        .iter()
        .find(|row| row.key.starts_with(HOT_LEASE_PREFIX))
        .unwrap();
    assert_ne!(
        fixture.backend.inner.rows.lock().await.get(&source.key),
        source.expected.as_ref()
    );
}

#[tokio::test]
async fn native_owner_hold_mismatch_stays_fenced_when_hot_source_is_unchanged() {
    let mut fixture = fixture(Fault::HoldMismatch).await;
    let original_writes = fixture.writes.clone();
    assert!(matches!(
        fixture
            .store
            .prepare_native_owner_cas(&mut fixture.checks, &mut fixture.writes)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(fixture.writes, original_writes);
    let source = fixture
        .checks
        .iter()
        .find(|row| row.key.starts_with(HOT_LEASE_PREFIX))
        .unwrap();
    assert_eq!(
        fixture.backend.inner.rows.lock().await.get(&source.key),
        source.expected.as_ref()
    );
}

#[tokio::test]
async fn native_publication_rebuilds_known_epoch_overlap_and_replaces_root_increment() {
    // Reproduce the real TiKV failure: the owner epoch point read sees a root
    // successor after the full publication authority read, before any census.
    let mut fixture = fixture(Fault::EpochOverlap).await;
    fixture.writes.push(KvWrite::Put {
        key: PACKED_ROOT_GENERATION_KEY.to_vec(),
        value: encode(&2u64).unwrap(),
    });
    let original_writes = fixture.writes.clone();
    let _owner = fixture
        .store
        .prepare_publication_owner_read_only_rebuild(
            &mut fixture.checks,
            &mut fixture.writes,
            100,
            true,
        )
        .await
        .unwrap();
    assert_eq!(fixture.backend.reads.lock().await.len(), 2);
    assert_eq!(fixture.backend.scans.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.backend.mutations.load(Ordering::SeqCst), 0);
    for old in &original_writes {
        if !matches!(old, KvWrite::Put { key, .. } if key.as_slice() == PACKED_ROOT_GENERATION_KEY)
        {
            assert!(
                fixture.writes.contains(old),
                "all other planned successor bytes stay fixed"
            );
        }
    }
    assert!(
        fixture
            .checks
            .iter()
            .any(|row| row.key.as_slice() == PACKED_ROOT_GENERATION_KEY
                && row.expected == Some(encode(&2u64).unwrap()))
    );
    assert!(fixture.writes.iter().any(|write| matches!(write, KvWrite::Put { key, value } if key.as_slice() == PACKED_ROOT_GENERATION_KEY && *value == encode(&3u64).unwrap())));
    fixture
        .store
        .clean_exact_cas(&fixture.checks, &fixture.writes, 100)
        .await
        .unwrap();
    assert_eq!(fixture.backend.mutations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_publication_rebuild_rejects_unproven_root_successor() {
    for invalid in [1u64, 3u64] {
        let mut fixture = fixture(Fault::EpochOverlap).await;
        fixture.writes.push(KvWrite::Put {
            key: PACKED_ROOT_GENERATION_KEY.to_vec(),
            value: encode(&invalid).unwrap(),
        });
        assert!(
            fixture
                .store
                .prepare_publication_owner_read_only_rebuild(
                    &mut fixture.checks,
                    &mut fixture.writes,
                    100,
                    true,
                )
                .await
                .is_err()
        );
        assert_eq!(fixture.backend.reads.lock().await.len(), 0);
        assert_eq!(fixture.backend.mutations.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn native_publication_rebuild_shares_census_quota_across_discarded_attempts() {
    let mut fixture = fixture(Fault::QuotaAfterFirstFullScan).await;
    {
        let mut rows = fixture.backend.inner.rows.lock().await;
        for _ in 0..MAX_HOLD_BIRTH_ROOT_VISITS / 2 + 1 {
            let root = RootRow {
                journal_id: JournalId::new(),
                incarnation: Uuid::new_v4(),
                revision: 1,
                state: RootState::Retired,
                members: 0,
                pending_puts: 0,
                binding: None,
            };
            rows.insert(registry_root_key(root.incarnation), root.encode().unwrap());
        }
    }
    fixture.writes.push(KvWrite::Put {
        key: PACKED_ROOT_GENERATION_KEY.to_vec(),
        value: encode(&2u64).unwrap(),
    });
    assert!(matches!(
        fixture
            .store
            .prepare_publication_owner_read_only_rebuild(
                &mut fixture.checks,
                &mut fixture.writes,
                100,
                true,
            )
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(fixture.backend.reads.lock().await.len(), 2);
    assert_eq!(fixture.backend.scans.load(Ordering::SeqCst), 257);
    assert_eq!(fixture.backend.mutations.load(Ordering::SeqCst), 0);
}

async fn initial_finish_fixture(fault: Fault, count: usize) -> Fixture {
    let mut fixture = fixture(fault).await;
    let _owner = fixture
        .store
        .prepare_clean_publication_native_owner_cas(&mut fixture.checks, &mut fixture.writes, 100)
        .await
        .unwrap();
    // Keep the real prepared NativeHold/PWA packet; pad only unchanged,
    // explicitly absent facts to exercise the initial finish's fixed tier.
    let packet = fixture
        .store
        .prepare_topology_envelope(fixture.checks.clone(), fixture.writes.clone(), Some(100))
        .await
        .unwrap();
    fixture.checks = packet.checks.clone();
    fixture.writes = packet.writes.clone();
    assert!(fixture.checks.len() < count);
    while fixture.checks.len() < count {
        fixture.checks.push(KvCheck {
            key: format!("test/initial-finish-extra/{:02}", fixture.checks.len()).into_bytes(),
            expected: None,
        });
    }
    fixture.backend.reads.lock().await.clear();
    fixture.backend.read_plans.lock().await.clear();
    fixture.backend.final_packets.lock().await.clear();
    fixture
}

fn complete_successor(packet: &FinalCasTrace) -> Vec<KvCheck> {
    let mut successor = packet.checks.clone();
    for write in &packet.writes {
        let (key, expected) = match write {
            KvWrite::Put { key, value } => (key, Some(value.clone())),
            KvWrite::Delete { key } => (key, None),
        };
        successor
            .iter_mut()
            .find(|row| &row.key == key)
            .unwrap()
            .expected = expected;
    }
    successor
}

#[tokio::test]
async fn initial_finish_tier_confirms_complete_35_and_64_key_packets_after_lost_reply() {
    for count in [35, 64] {
        let fixture = initial_finish_fixture(Fault::MutationAppliedReplyLost, count).await;
        fixture
            .store
            .initial_finish_exact_cas(
                &fixture.checks,
                &fixture.writes,
                100,
                &fixture.backend.budget,
            )
            .await
            .unwrap();
        assert_eq!(fixture.backend.mutations.load(Ordering::SeqCst), 1);
        let packets = fixture.backend.final_packets.lock().await;
        assert_eq!(packets.len(), 2, "one mutation and one exact confirmation");
        assert_eq!(packets[0].checks.len(), count);
        assert_eq!(packets[0].writes, fixture.writes);
        for key in [&fixture.lease_hold, &fixture.snapshot_hold, &fixture.pwa] {
            assert!(packets[0].checks.iter().any(|row| &row.key == key));
            assert!(packets[0].writes.iter().any(|write| match write {
                KvWrite::Put { key: written, .. } | KvWrite::Delete { key: written } =>
                    written.as_slice() == key.as_slice(),
            }));
        }
        let successor = complete_successor(&packets[0]);
        assert_eq!(packets[1].checks, successor);
        assert!(packets[1].writes.is_empty());
        for packet in packets.iter() {
            assert_eq!(packet.deadline, 100);
            assert!(packet.metadata_bytes >= 512 << 10);
        }
        let reads = fixture.backend.read_plans.lock().await;
        assert_eq!(reads.len(), 1, "only the complete unknown-result read");
        assert_eq!(
            reads[0].keys,
            successor
                .iter()
                .map(|row| row.key.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(reads[0].limits.max_records, count);
        assert_eq!(reads[0].limits.max_value_bytes, 48 << 10);
        assert_eq!(reads[0].limits.max_total_bytes, 48 << 10);
        assert_eq!(reads[0].limits.max_response_bytes, 64 << 10);
        assert_eq!(reads[0].limits.max_data_requests, (count + 2).min(64));
        assert!(reads[0].metadata_bytes >= 512 << 10);
        let rows = fixture.backend.inner.rows.lock().await;
        for check in &successor {
            assert_eq!(rows.get(&check.key), check.expected.as_ref());
        }
        assert!(!rows.contains_key(&fixture.lease_hold));
        assert!(rows.contains_key(&fixture.snapshot_hold));
        let lease: SnapshotLease = decode_open_value(
            fixture
                .checks
                .iter()
                .find(|row| row.key.starts_with(HOT_LEASE_PREFIX))
                .unwrap()
                .expected
                .as_deref()
                .unwrap(),
            12 << 10,
        )
        .unwrap();
        assert!(
            PackedWriterAuthority::decode(rows.get(&fixture.pwa).unwrap(), lease.workspace_id)
                .unwrap()
                .owner
                .is_none()
        );
        assert_eq!(
            fixture.backend.budget.state().used[SnapshotBudgetPool::Metadata as usize],
            0
        );
    }
}

#[tokio::test]
async fn ordinary_clean_tier_rejects_the_same_35_key_packet_without_mutation() {
    let fixture = initial_finish_fixture(Fault::MutationAppliedReplyLost, 35).await;
    let before = fixture.backend.inner.rows.lock().await.clone();
    assert!(matches!(
        fixture
            .store
            .clean_exact_cas(&fixture.checks, &fixture.writes, 100)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(fixture.backend.mutations.load(Ordering::SeqCst), 0);
    assert!(fixture.backend.final_packets.lock().await.is_empty());
    assert_eq!(*fixture.backend.inner.rows.lock().await, before);
}

#[tokio::test]
async fn initial_finish_tier_rejects_key_and_both_byte_overflows_without_mutation() {
    for overflow in 0..3 {
        let mut fixture = initial_finish_fixture(
            Fault::MutationAppliedReplyLost,
            if overflow == 0 { 65 } else { 35 },
        )
        .await;
        if overflow != 0 {
            let check = fixture.checks.last_mut().unwrap();
            let bytes = vec![0; (48 << 10) + 1];
            if overflow == 1 {
                check.expected = Some(bytes.clone());
                fixture
                    .backend
                    .inner
                    .rows
                    .lock()
                    .await
                    .insert(check.key.clone(), bytes);
            } else {
                fixture.writes.push(KvWrite::Put {
                    key: check.key.clone(),
                    value: bytes,
                });
            }
        }
        let before = fixture.backend.inner.rows.lock().await.clone();
        assert!(matches!(
            fixture
                .store
                .initial_finish_exact_cas(
                    &fixture.checks,
                    &fixture.writes,
                    100,
                    &fixture.backend.budget,
                )
                .await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(fixture.backend.mutations.load(Ordering::SeqCst), 0);
        assert!(fixture.backend.final_packets.lock().await.is_empty());
        assert_eq!(*fixture.backend.inner.rows.lock().await, before);
        assert_eq!(
            fixture.backend.budget.state().used[SnapshotBudgetPool::Metadata as usize],
            0
        );
    }
}

#[tokio::test]
async fn initial_finish_tier_rejects_expiry_and_a_later_writer_after_applied_reply_loss() {
    for fault in [
        Fault::MutationAppliedReplyLostThenExpire,
        Fault::MutationAppliedReplyLostThenChangePwa,
    ] {
        let fixture = initial_finish_fixture(fault, 35).await;
        let result = fixture
            .store
            .initial_finish_exact_cas(
                &fixture.checks,
                &fixture.writes,
                100,
                &fixture.backend.budget,
            )
            .await;
        assert!(matches!(result, Err(WorkspaceError::Backend(_))));
        assert_eq!(fixture.backend.mutations.load(Ordering::SeqCst), 1);
        let packets = fixture.backend.final_packets.lock().await;
        let successor = complete_successor(&packets[0]);
        assert_eq!(fixture.backend.read_plans.lock().await.len(), 1);
        if matches!(
            fixture.backend.fault,
            Fault::MutationAppliedReplyLostThenExpire
        ) {
            assert_eq!(packets.len(), 1, "expiry forbids even the confirmation CAS");
        } else {
            assert_eq!(packets.len(), 2);
            assert_eq!(packets[1].checks, successor);
            assert!(packets[1].writes.is_empty());
            assert_eq!(packets[1].deadline, 100);
            let pwa = successor.iter().find(|row| row.key == fixture.pwa).unwrap();
            assert_ne!(
                fixture.backend.inner.rows.lock().await.get(&fixture.pwa),
                pwa.expected.as_ref()
            );
        }
    }
}
