use super::*;
use crate::chunk::{BlockStore, IncompleteBlockRead};
use crate::meta::MetaLayer;
use crate::workspace_overlay::catalog::{DeleteLayerMetadata, MarkDeleting, ReleaseLease};
use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
use crate::workspace_overlay::stores::g13_gc_contract_tests::{
    BOUNDARIES, GRACE_NS, SHARED_BYTES, SHARED_SLICE, assert_child_reads_shared, assert_delete,
    assert_mark, build_shared_base, collector,
};
use tokio::sync::Semaphore;

const START_NS: i64 = 1_000_000_000;
const TTL_NS: u64 = 100;
const WATCHDOG: Duration = Duration::from_secs(10);

async fn grace_fixture(
    reaped: bool,
    released: bool,
) -> (
    Arc<KvWorkspaceStore<MemoryBackend>>,
    Arc<KvWorkspaceStore<MemoryBackend>>,
    SnapshotLease,
) {
    let backend = MemoryBackend::default();
    backend.clock.store(START_NS, Ordering::SeqCst);
    // Distinct store instances have independent topology mutexes and share
    // only the real backend records/CAS authority.
    let writer = Arc::new(budgeted_kv_store(backend.clone()));
    let peer = Arc::new(budgeted_kv_store(backend.clone()));
    writer.initialize_workspace_schema().await.unwrap();
    let workspace = writer
        .create_volume_root(create_request(91_000))
        .await
        .unwrap();
    let lease = writer
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(id(91_005)),
            holder_generation: 1,
            ttl_ns: TTL_NS,
        })
        .await
        .unwrap();
    if released {
        writer
            .release_lease(ReleaseLease {
                lease_id: lease.lease_id,
                holder_generation: lease.holder_generation,
            })
            .await
            .unwrap();
    }
    // Expiry is inclusive for the production backend clock. Exercise both
    // public transitions: reap before deletion, or expire in the deletion CAS.
    backend.clock.store(lease.expires_at_ns, Ordering::SeqCst);
    if reaped {
        assert_eq!(peer.reap_expired_leases().await.unwrap(), 1);
    }
    writer
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: workspace.workspace_id,
            force_fence_lease: false,
        })
        .await
        .unwrap();
    let deleting = peer.load_workspace(workspace.workspace_id).await.unwrap();
    assert_eq!(deleting.state, WorkspaceState::Deleting);
    assert_eq!(deleting.active_lease, None);
    let mut actual = peer.list_leases(workspace.workspace_id).await.unwrap();
    assert_eq!(actual.len(), 1);
    let actual = actual.pop().unwrap();
    assert_eq!(actual.expires_at_ns, lease.expires_at_ns);
    assert_eq!(
        actual.state,
        if released {
            LeaseState::Released
        } else {
            LeaseState::Expired
        }
    );
    (writer, peer, actual)
}

#[tokio::test]
async fn g13a_kv_mark_before_and_after_reap_has_identical_grace_boundaries() {
    let (_writer, peer, lease) = grace_fixture(false, false).await;
    for elapsed in BOUNDARIES {
        assert_mark(peer.as_ref(), &lease, elapsed).await;
    }
    // Workspace deletion already expired the lease; a later reap is a no-op
    // and cannot shorten its recovery-root grace period.
    assert_eq!(peer.reap_expired_leases().await.unwrap(), 0);
    let actual = peer.list_leases(lease.workspace_id).await.unwrap();
    assert_eq!(actual.len(), 1);
    let reaped = &actual[0];
    assert_eq!(reaped.state, LeaseState::Expired);
    assert_eq!(reaped.lease_id, lease.lease_id);
    assert_eq!(reaped.base_revision, lease.base_revision);
    assert_eq!(reaped.expires_at_ns, lease.expires_at_ns);
    for elapsed in BOUNDARIES {
        assert_mark(peer.as_ref(), reaped, elapsed).await;
    }
}

#[tokio::test]
async fn g13a_kv_delete_revalidation_preserves_expired_base_until_grace() {
    for reaped in [false, true] {
        for elapsed in BOUNDARIES {
            let (writer, peer, lease) = grace_fixture(reaped, false).await;
            assert_delete(writer.as_ref(), peer.as_ref(), &lease, elapsed).await;
        }
    }
}

#[tokio::test]
async fn g13a_kv_released_lease_uses_release_time_for_gc_grace() {
    // KV catalogs conservatively retain Released roots too, using release
    // time rather than the later expiry. Cover both mark and destructive
    // revalidation at the exact boundary, including the old pre-expiry case.
    for elapsed in BOUNDARIES.into_iter().chain([TTL_NS as i64 - 1]) {
        let (writer, peer, lease) = grace_fixture(false, true).await;
        assert!(lease.updated_at_ns < lease.expires_at_ns);
        let now = lease.updated_at_ns.checked_add(elapsed).unwrap();
        let base = lease.base_revision.layer_id;
        let protected = elapsed < GRACE_NS as i64;
        assert_eq!(
            peer.gc_snapshot(now, GRACE_NS)
                .await
                .unwrap()
                .root_layers
                .contains(&base),
            protected,
            "released lease mark at elapsed={elapsed}",
        );
        let before = peer.backend.records.lock().await.clone();
        let result = writer
            .delete_layer_metadata(DeleteLayerMetadata {
                layer_ids: vec![base],
                now_ns: now,
                lease_grace_ns: GRACE_NS,
            })
            .await;
        if protected {
            assert!(
                matches!(result, Err(WorkspaceError::Busy)),
                "released lease deletion at elapsed={elapsed}: {result:?}",
            );
            assert_eq!(*peer.backend.records.lock().await, before);
        } else {
            result.unwrap();
            assert_eq!(
                peer.load_layer(base).await.unwrap().state,
                LayerState::Deleting
            );
            writer.finalize_layer_metadata_deletion(vec![base]).await.unwrap();
            assert!(matches!(
                peer.load_layer(base).await,
                Err(WorkspaceError::LayerNotFound(found)) if found == base,
            ));
        }
    }
}

#[derive(Clone)]
struct PausePoint {
    entered: Arc<Semaphore>,
    resume: Arc<Semaphore>,
}

impl PausePoint {
    fn new() -> Self {
        Self {
            entered: Arc::new(Semaphore::new(0)),
            resume: Arc::new(Semaphore::new(0)),
        }
    }

    async fn pause(&self) {
        self.entered.add_permits(1);
        self.resume.acquire().await.unwrap().forget();
    }

    async fn wait_entered(&self) {
        tokio::time::timeout(WATCHDOG, self.entered.acquire())
            .await
            .expect("scheduled production boundary was not reached")
            .unwrap()
            .forget();
    }

    fn continue_once(&self) {
        self.resume.add_permits(1);
    }
}

struct ScanPause {
    remaining: usize,
    prefix: Vec<u8>,
    pause: PausePoint,
}

struct ForkPause {
    workspace_key: Vec<u8>,
    pause: PausePoint,
}

struct PointReadPause {
    keys: Vec<Vec<u8>>,
    pause: PausePoint,
}

/// A scheduling adapter only: every returned row comes from MemoryBackend,
/// and all checks, writes and backend-time predicates reach its original CAS.
#[derive(Clone, Default)]
struct ScheduledBackend {
    inner: MemoryBackend,
    scan_pause: Arc<Mutex<Option<ScanPause>>>,
    fork_pause: Arc<Mutex<Option<ForkPause>>>,
    point_read_pause: Arc<Mutex<Option<PointReadPause>>>,
}

impl ScheduledBackend {
    async fn pause_after_workspace_scan(&self, occurrence: usize) -> PausePoint {
        self.pause_after_prefix_scan(HOT_WORKSPACE_PREFIX, occurrence)
            .await
    }

    async fn pause_after_prefix_scan(&self, prefix: &[u8], occurrence: usize) -> PausePoint {
        assert!(occurrence > 0);
        let pause = PausePoint::new();
        let mut schedule = self.scan_pause.lock().await;
        assert!(schedule.is_none());
        *schedule = Some(ScanPause {
            remaining: occurrence,
            prefix: prefix.to_vec(),
            pause: pause.clone(),
        });
        pause
    }

    async fn pause_before_fork_cas(&self, workspace_id: WorkspaceId) -> PausePoint {
        let pause = PausePoint::new();
        let mut schedule = self.fork_pause.lock().await;
        assert!(schedule.is_none());
        *schedule = Some(ForkPause {
            workspace_key: hot_workspace_key(workspace_id),
            pause: pause.clone(),
        });
        pause
    }

    async fn pause_after_point_read(&self, keys: &[&[u8]]) -> PausePoint {
        let pause = PausePoint::new();
        let mut schedule = self.point_read_pause.lock().await;
        assert!(schedule.is_none());
        *schedule = Some(PointReadPause {
            keys: keys.iter().map(|key| key.to_vec()).collect(),
            pause: pause.clone(),
        });
        pause
    }

    async fn after_point_read(&self, keys: &[Vec<u8>]) {
        let pause = {
            let mut schedule = self.point_read_pause.lock().await;
            if schedule.as_ref().is_some_and(|gate| gate.keys == keys) {
                schedule.take().map(|gate| gate.pause)
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.pause().await;
        }
    }

    async fn after_scan(&self, prefix: &[u8]) {
        let pause = {
            let mut schedule = self.scan_pause.lock().await;
            if let Some(gate) = schedule.as_mut() {
                if prefix != gate.prefix {
                    return;
                }
                gate.remaining -= 1;
                if gate.remaining == 0 {
                    schedule.take().map(|gate| gate.pause)
                } else {
                    None
                }
            } else {
                None
            }
        };
        // Both scheduling and backend-record locks are released before wait.
        if let Some(pause) = pause {
            pause.pause().await;
        }
    }

    async fn before_cas(&self, checks: &[KvCheck], writes: &[KvWrite]) {
        let pause = {
            let mut schedule = self.fork_pause.lock().await;
            let matches = schedule.as_ref().is_some_and(|gate| {
                checks
                    .iter()
                    .any(|check| check.key == gate.workspace_key && check.expected.is_none())
                    && writes.iter().any(|write| {
                        matches!(write,
                        KvWrite::Put { key, .. } if *key == gate.workspace_key)
                    })
            });
            if matches {
                schedule.take().map(|gate| gate.pause)
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.pause().await;
        }
    }
}

#[async_trait]
impl WorkspaceKvBackend for ScheduledBackend {
    fn supports_consistent_reads(&self) -> bool {
        self.inner.supports_consistent_reads()
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        self.inner.get(key).await
    }

    async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.inner.get_many(keys).await
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
        limits: crate::workspace_overlay::stores::kv_backend::KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let rows = self
            .inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await?;
        self.after_point_read(keys).await;
        Ok(rows)
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: crate::workspace_overlay::stores::kv_backend::KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        let rows = self
            .inner
            .scan_prefix_with_byte_limits(prefix, limits)
            .await?;
        self.after_scan(prefix).await;
        Ok(rows)
    }

    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: crate::workspace_overlay::stores::kv_backend::KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        let rows = self
            .inner
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await?;
        self.after_scan(prefix).await;
        Ok(rows)
    }

    async fn get_many_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.inner.get_many_with_time(keys).await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        let rows = self.inner.scan_prefix(prefix).await?;
        self.after_scan(prefix).await;
        Ok(rows)
    }

    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        let rows = self.inner.scan_prefix_bounded(prefix, limit).await?;
        self.after_scan(prefix).await;
        Ok(rows)
    }

    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.before_cas(checks, writes).await;
        self.inner.compare_and_swap(checks, writes).await
    }

    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        expires_at_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        self.before_cas(checks, writes).await;
        self.inner
            .compare_and_swap_before(checks, writes, expires_at_ns)
            .await
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
        self.before_cas(checks, &[]).await;
        self.inner
            .authenticate_checks_before_bounded(checks, expires_at_ns, limits)
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
    }
}

fn fork_request(base: BaseRevision, offset: u128) -> CreateWorkspace {
    CreateWorkspace {
        base_revision: base,
        workspace_id: WorkspaceId::from_uuid(id(offset + 1)),
        head_layer_id: LayerId::from_uuid(id(offset + 2)),
        owner_id: Some("g13-child".into()),
    }
}

#[tokio::test]
async fn g13b_kv_fork_after_real_revalidation_root_scan_keeps_shared_bytes() {
    let backend = ScheduledBackend::default();
    backend.inner.clock.store(START_NS, Ordering::SeqCst);
    let gc_store = Arc::new(budgeted_kv_store(backend.clone()));
    let peer = Arc::new(budgeted_kv_store(backend.clone()));
    let (base, ino, blocks) = build_shared_base(gc_store.clone(), create_request(92_000)).await;
    let before = peer.load_layer(base.layer_id).await.unwrap();
    // Scan 1 is actual gc_snapshot roots. Scan 2 is actual destructive
    // revalidation hydration. Pause after MemoryBackend obtained its rows,
    // before those old rows are consumed; no GcSnapshot is fabricated.
    let pause = backend.pause_after_workspace_scan(2).await;
    let gc = collector(gc_store.clone(), blocks.clone(), 0);
    let deleting = tokio::spawn(async move { gc.run_at(i64::MAX / 2).await });
    pause.wait_entered().await;
    let child = peer
        .create_workspace(fork_request(base.clone(), 93_000))
        .await
        .unwrap();
    pause.continue_once();
    let result = tokio::time::timeout(WATCHDOG, deleting)
        .await
        .expect("GC failed to finish after resuming real root scan")
        .unwrap();
    assert!(
        matches!(result, Err(WorkspaceError::Busy)),
        "new child root must invalidate old destructive scan: {result:?}"
    );
    assert_eq!(peer.load_layer(base.layer_id).await.unwrap(), before);
    assert_child_reads_shared(
        peer.clone(),
        child.workspace_id,
        &base,
        ino,
        blocks.as_ref(),
    )
    .await;

    // A new genuine mark/sweep sees the child and can clean unrelated layers.
    let report = collector(gc_store, blocks.clone(), 0)
        .run_at(i64::MAX / 2)
        .await
        .unwrap();
    assert!(!report.deleted_layers.contains(&base.layer_id));
    assert!(!report.deleted_layers.contains(&child.head_layer_id));
    assert!(!report.deleted_slices.contains(&SHARED_SLICE));
    assert_child_reads_shared(peer, child.workspace_id, &base, ino, blocks.as_ref()).await;
}

#[tokio::test]
async fn g13b_kv_delete_wins_before_fork_cas_rejects_child_without_orphan_rows() {
    // Exercise both irreversible metadata mark and completed real collection.
    for finalize in [false, true] {
        let backend = ScheduledBackend::default();
        backend.inner.clock.store(START_NS, Ordering::SeqCst);
        let forking_store = Arc::new(budgeted_kv_store(backend.clone()));
        let gc_store = Arc::new(budgeted_kv_store(backend.clone()));
        let (base, _ino, blocks) =
            build_shared_base(gc_store.clone(), create_request(94_000)).await;
        let request = fork_request(base.clone(), 95_000);
        let child_id = request.workspace_id;
        let child_head = request.head_layer_id;
        let pause = backend.pause_before_fork_cas(child_id).await;
        let forking = tokio::spawn(async move { forking_store.create_workspace(request).await });
        pause.wait_entered().await;
        if finalize {
            let report = collector(gc_store.clone(), blocks.clone(), 0)
                .run_at(i64::MAX / 2)
                .await
                .unwrap();
            assert!(report.deleted_layers.contains(&base.layer_id));
            assert!(report.deleted_slices.contains(&SHARED_SLICE));
            assert!(report.orphan_bytes >= SHARED_BYTES.len() as u64);
            assert!(matches!(gc_store.load_layer(base.layer_id).await,
                Err(WorkspaceError::LayerNotFound(found)) if found == base.layer_id));
            let mut bytes = [9; SHARED_BYTES.len()];
            let error = blocks
                .read_range((SHARED_SLICE, 0), 0, &mut bytes)
                .await
                .unwrap_err();
            assert!(error.downcast_ref::<IncompleteBlockRead>().is_some());
            assert_eq!(bytes, [9; SHARED_BYTES.len()]);
        } else {
            gc_store
                .delete_layer_metadata(DeleteLayerMetadata {
                    layer_ids: vec![base.layer_id],
                    now_ns: i64::MAX / 2,
                    lease_grace_ns: 0,
                })
                .await
                .unwrap();
            assert_eq!(
                gc_store.load_layer(base.layer_id).await.unwrap().state,
                LayerState::Deleting
            );
        }
        pause.continue_once();
        let result = tokio::time::timeout(WATCHDOG, forking)
            .await
            .expect("fork failed to finish after resuming actual CAS")
            .unwrap();
        assert!(
            matches!(result, Err(WorkspaceError::LayerNotFound(found)) if found == base.layer_id),
            "fork must reread and reject a deleted/nonsealed base: {result:?}"
        );
        assert!(matches!(gc_store.load_workspace(child_id).await,
            Err(WorkspaceError::WorkspaceNotFound(found)) if found == child_id));
        assert!(matches!(gc_store.load_layer(child_head).await,
            Err(WorkspaceError::LayerNotFound(found)) if found == child_head));
        assert!(
            backend
                .get(&hot_workspace_key(child_id))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            backend
                .get(&hot_layer_key(child_head))
                .await
                .unwrap()
                .is_none()
        );
    }
}

// Positive lifecycle controls. These exercise production public metadata/GC
// APIs and the original MemoryBackend CAS; no root/layer rows are injected.
#[tokio::test]
async fn g13c_kv_deleted_fork_releases_cached_roots_and_shared_bytes_for_real_gc() {
    let backend = MemoryBackend::default();
    backend.clock.store(START_NS, Ordering::SeqCst);
    let writer = Arc::new(budgeted_kv_store(backend.clone()));
    let peer = Arc::new(budgeted_kv_store(backend.clone()));
    let (base, ino, blocks) = build_shared_base(writer.clone(), create_request(96_000)).await;
    let child = peer
        .create_workspace(fork_request(base.clone(), 97_000))
        .await
        .unwrap();
    assert_child_reads_shared(
        peer.clone(),
        child.workspace_id,
        &base,
        ino,
        blocks.as_ref(),
    )
    .await;
    let retained = collector(writer.clone(), blocks.clone(), 0)
        .run_at(i64::MAX / 2)
        .await
        .unwrap();
    assert!(!retained.deleted_layers.contains(&base.layer_id));
    assert!(!retained.deleted_layers.contains(&child.head_layer_id));
    assert!(!retained.deleted_slices.contains(&SHARED_SLICE));
    assert_child_reads_shared(
        peer.clone(),
        child.workspace_id,
        &base,
        ino,
        blocks.as_ref(),
    )
    .await;

    peer.mark_workspace_deleting(MarkDeleting {
        workspace_id: child.workspace_id,
        force_fence_lease: false,
    })
    .await
    .unwrap();
    assert_eq!(
        writer
            .load_workspace(child.workspace_id)
            .await
            .unwrap()
            .state,
        WorkspaceState::Deleting
    );
    assert_eq!(
        writer.load_layer(child.head_layer_id).await.unwrap().state,
        LayerState::Deleting
    );
    // A fresh store must observe persisted state, not a shared per-instance cache.
    let fresh = Arc::new(budgeted_kv_store(backend.clone()));
    let before = fresh.gc_snapshot(i64::MAX / 2, 0).await.unwrap();
    assert!(
        before.root_layers.is_empty(),
        "deleted fork must not retain a cached Active GC root: {:?}",
        before.root_layers
    );
    let report = collector(fresh, blocks.clone(), 0)
        .run_at(i64::MAX / 2)
        .await
        .unwrap();
    assert!(report.deleted_layers.contains(&base.layer_id));
    assert!(report.deleted_layers.contains(&child.head_layer_id));
    assert_eq!(report.deleted_slices, vec![SHARED_SLICE]);
    assert_eq!(report.orphan_bytes, SHARED_BYTES.len() as u64);
    for layer in [base.layer_id, child.head_layer_id] {
        assert!(matches!(writer.load_layer(layer).await,
            Err(WorkspaceError::LayerNotFound(found)) if found == layer));
        assert!(backend.get(&hot_layer_key(layer)).await.unwrap().is_none());
    }
    let mut bytes = [9; SHARED_BYTES.len()];
    let error = blocks
        .read_range((SHARED_SLICE, 0), 0, &mut bytes)
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<IncompleteBlockRead>().is_some());
    assert_eq!(bytes, [9; SHARED_BYTES.len()]);
    let after = peer.gc_snapshot(i64::MAX / 2, 0).await.unwrap();
    assert!(after.root_layers.is_empty());
    assert!(
        after.layers.is_empty(),
        "finalized hot deletion must not resurrect cached layers"
    );
    assert!(after.slice_references.is_empty());
    let retry = collector(peer, blocks, 0)
        .run_at(i64::MAX / 2)
        .await
        .unwrap();
    assert!(retry.deleted_layers.is_empty());
    assert!(retry.deleted_slices.is_empty());
    assert_eq!(retry.orphan_bytes, 0);
}

#[tokio::test]
async fn g13d_kv_packed_binding_history_keeps_layer_roots_after_workspace_delete() {
    let backend = MemoryBackend::default();
    backend.clock.store(START_NS, Ordering::SeqCst);
    let store = budgeted_kv_store(backend);
    store.initialize_workspace_schema().await.unwrap();
    let mut workspace = store
        .create_volume_root(create_request(100_000))
        .await
        .unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(id(100_005)),
            holder_generation: 1,
            ttl_ns: TTL_NS,
        })
        .await
        .unwrap();
    store
        .release_lease(ReleaseLease {
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        })
        .await
        .unwrap();

    // This catalog fixture persists the same PWB3 current/history records used
    // by the v3 open path. It intentionally leaves the workspace lifecycle
    // free to enter Deleting, so GC must retain binding-target layers itself.
    let binding = super::install_open_binding_fixture(&store, &mut workspace, 1).await;
    store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: workspace.workspace_id,
            force_fence_lease: false,
        })
        .await
        .unwrap();

    let snapshot = store.gc_snapshot(i64::MAX / 2, 0).await.unwrap();
    assert!(
        snapshot
            .root_layers
            .contains(&binding.base_revision.layer_id),
        "PWB3 base layer must remain a GC root while binding history is retained"
    );
    assert!(
        snapshot.root_layers.contains(&binding.head_layer_id),
        "PWB3 writable head must remain a GC root while binding history is retained"
    );

    // The destructive path must repeat the same root check instead of
    // trusting a caller-provided candidate list.
    for layer_id in [binding.base_revision.layer_id, binding.head_layer_id] {
        let error = store
            .delete_layer_metadata(DeleteLayerMetadata {
                layer_ids: vec![layer_id],
                now_ns: i64::MAX / 2,
                lease_grace_ns: 0,
            })
            .await
            .unwrap_err();
        assert!(
            matches!(error, WorkspaceError::Busy),
            "PWB3 binding root {layer_id} must reject destructive revalidation: {error:?}"
        );
        assert_eq!(
            store.load_layer(layer_id).await.unwrap().state,
            if layer_id == binding.head_layer_id {
                LayerState::Deleting
            } else {
                LayerState::Sealed
            }
        );
    }
    let head = store.load_layer(binding.head_layer_id).await.unwrap();
    assert!(matches!(
        store
            .finalize_layer_metadata_deletion(vec![binding.head_layer_id])
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(store.load_layer(binding.head_layer_id).await.unwrap(), head);
}

#[tokio::test]
async fn g13d_kv_first_binding_after_root_scan_fences_mark_and_finalize() {
    for finalize in [false, true] {
        let backend = ScheduledBackend::default();
        backend.inner.clock.store(START_NS, Ordering::SeqCst);
        let gc_store = Arc::new(budgeted_kv_store(backend.clone()));
        let peer = Arc::new(budgeted_kv_store(backend.clone()));
        let (_objects, _client, _snapshot, proof, _payload) =
            crate::workspace_overlay::stores::binding_tests::packed().await;
        let request =
            crate::workspace_overlay::stores::binding_tests::request(peer.as_ref(), proof).await;
        let base_id = request.expected_base.layer_id;
        let head_id = request.guard.expected_head_layer_id;
        // All packed-root scans and their final epoch probe return the same
        // rootless predecessor. Complete a genuine first install and workspace
        // deletion before topology hydration, so the final CAS must fence the
        // first binding rather than reject inconsistent pre-scan epochs.
        let pause = backend
            .pause_after_point_read(&[
                PACKED_ROOT_GENERATION_KEY,
                b"packed/v3/journal-feature",
                b"packed/v3/journal-active-count",
            ])
            .await;
        let mut deleting = tokio::spawn(async move {
            if finalize {
                gc_store
                    .finalize_layer_metadata_deletion(vec![head_id])
                    .await
            } else {
                gc_store
                    .delete_layer_metadata(DeleteLayerMetadata {
                        layer_ids: vec![base_id],
                        now_ns: i64::MAX / 2,
                        lease_grace_ns: 0,
                    })
                    .await
            }
        });
        let early_result = tokio::select! {
            () = pause.wait_entered() => None,
            result = &mut deleting => {
                let result = result.unwrap();
                assert!(finalize, "mark lane must exercise the packed-root scan");
                assert!(matches!(&result, Err(WorkspaceError::Busy)),
                    "Writable finalization must reject before scanning: {result:?}");
                backend.point_read_pause.lock().await.take();
                Some(result)
            }
        };
        let binding = peer
            .install_packed_lower_binding(request.clone())
            .await
            .unwrap();
        peer.release_lease(ReleaseLease {
            lease_id: request.guard.lease_id,
            holder_generation: request.guard.holder_generation,
        })
        .await
        .unwrap();
        peer.mark_workspace_deleting(MarkDeleting {
            workspace_id: request.guard.workspace_id,
            force_fence_lease: false,
        })
        .await
        .unwrap();
        let before_base = peer.load_layer(base_id).await.unwrap();
        let before_head = peer.load_layer(head_id).await.unwrap();
        let attempts = backend.inner.cas_checks.lock().await.len();
        let scanned = early_result.is_none();
        let result = match early_result {
            Some(result) => result,
            None => {
                pause.continue_once();
                tokio::time::timeout(WATCHDOG, deleting)
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        assert!(
            matches!(result, Err(WorkspaceError::Busy)),
            "finalize={finalize}: {result:?}"
        );
        assert_eq!(peer.load_layer(base_id).await.unwrap(), before_base);
        assert_eq!(peer.load_layer(head_id).await.unwrap(), before_head);
        assert_eq!(
            peer.load_packed_binding_version(request.guard.workspace_id, 1)
                .await
                .unwrap(),
            Some(binding)
        );
        // The mark lane must reach this CAS. A correct finalize pre-scan
        // rejection proves only its candidate-state guard, not the root fence.
        if scanned {
            let checks = backend.inner.cas_checks.lock().await;
            assert!(
                checks[attempts..]
                    .iter()
                    .any(|keys| keys.contains(&PACKED_ROOT_GENERATION_KEY.to_vec()))
            );
        }
    }
}

#[tokio::test]
async fn g13d_kv_packed_root_change_at_delete_cas_is_revalidated() {
    let backend = MemoryBackend::default();
    backend.clock.store(START_NS, Ordering::SeqCst);
    let store = Arc::new(budgeted_kv_store(backend.clone()));
    store.initialize_workspace_schema().await.unwrap();

    let mut workspace = store
        .create_volume_root(create_request(101_000))
        .await
        .unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(id(101_005)),
            holder_generation: 1,
            ttl_ns: TTL_NS,
        })
        .await
        .unwrap();
    store
        .release_lease(ReleaseLease {
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        })
        .await
        .unwrap();
    let binding = super::install_open_binding_fixture(&store, &mut workspace, 1).await;
    store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: workspace.workspace_id,
            force_fence_lease: false,
        })
        .await
        .unwrap();

    let child = store
        .create_workspace(fork_request(binding.base_revision.clone(), 102_000))
        .await
        .unwrap();
    let child_lease = store
        .acquire_lease(AcquireLease {
            workspace_id: child.workspace_id,
            lease_id: LeaseId::from_uuid(id(102_005)),
            holder_generation: 1,
            ttl_ns: TTL_NS,
        })
        .await
        .unwrap();
    store
        .release_lease(ReleaseLease {
            lease_id: child_lease.lease_id,
            holder_generation: child_lease.holder_generation,
        })
        .await
        .unwrap();
    let sealed = crate::workspace_overlay::test_base_fixture::install_test_base(&store, child.workspace_id).await;
    store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: child.workspace_id,
            force_fence_lease: false,
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .load_layer(sealed.revision.layer_id)
            .await
            .unwrap()
            .state,
        LayerState::Sealed
    );

    // Inject a binding-root change immediately before the first destructive
    // CAS. The old exact check must fail; the retry then sees the new root and
    // rejects the candidate before changing its layer state.
    let mut changed = binding.clone();
    changed.binding.binding_version = 2;
    changed.head_layer_id = sealed.revision.layer_id;
    let bytes = changed.encode().unwrap();
    backend.mutate_on_cas.lock().await.extend([
        KvWrite::Put {
            key: packed_current_key(workspace.workspace_id),
            value: bytes.clone(),
        },
        KvWrite::Put {
            key: packed_history_key(workspace.workspace_id, 2),
            value: bytes,
        },
    ]);
    let error = store
        .delete_layer_metadata(DeleteLayerMetadata {
            layer_ids: vec![sealed.revision.layer_id],
            now_ns: i64::MAX / 2,
            lease_grace_ns: 0,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(error, WorkspaceError::Busy),
        "new PWB3 root must invalidate the stale delete CAS: {error:?}"
    );
    assert_eq!(
        store
            .load_layer(sealed.revision.layer_id)
            .await
            .unwrap()
            .state,
        LayerState::Sealed
    );
}

#[tokio::test]
async fn g13c_kv_sealed_then_deleted_fork_releases_all_cached_history_for_real_gc() {
    let backend = MemoryBackend::default();
    backend.clock.store(START_NS, Ordering::SeqCst);
    let writer = Arc::new(budgeted_kv_store(backend.clone()));
    let peer = Arc::new(budgeted_kv_store(backend.clone()));
    let (base, ino, blocks) = build_shared_base(writer.clone(), create_request(98_000)).await;
    let child = peer
        .create_workspace(fork_request(base.clone(), 99_000))
        .await
        .unwrap();
    let lease = peer
        .acquire_lease(AcquireLease {
            workspace_id: child.workspace_id,
            lease_id: LeaseId::new(),
            holder_generation: 11,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    peer.release_lease(ReleaseLease {
        lease_id: lease.lease_id,
        holder_generation: lease.holder_generation,
    })
    .await
    .unwrap();
    let sealed = crate::workspace_overlay::test_base_fixture::install_test_base(&peer, child.workspace_id).await;
    assert_child_reads_shared(
        peer.clone(),
        child.workspace_id,
        &sealed.revision,
        ino,
        blocks.as_ref(),
    )
    .await;
    let active = writer.load_workspace(child.workspace_id).await.unwrap();
    assert_eq!(active.state, WorkspaceState::Active);
    assert_eq!(active.head_layer_id, sealed.replacement_head_layer_id);
    assert_eq!(active.head_epoch, sealed.head_epoch);
    assert_ne!(active.head_layer_id, child.head_layer_id);
    peer.mark_workspace_deleting(MarkDeleting {
        workspace_id: child.workspace_id,
        force_fence_lease: false,
    })
    .await
    .unwrap();
    let fresh = Arc::new(budgeted_kv_store(backend.clone()));
    let before = fresh.gc_snapshot(i64::MAX / 2, 0).await.unwrap();
    assert!(
        before.root_layers.is_empty(),
        "completed seals plus deleted fork must release every cached root"
    );
    let mut expected_layers = before
        .layers
        .iter()
        .map(|layer| layer.layer_id)
        .collect::<Vec<_>>();
    expected_layers.sort();
    assert!(expected_layers.contains(&sealed.revision.layer_id));
    assert!(expected_layers.contains(&sealed.replacement_head_layer_id));
    let report = collector(fresh, blocks.clone(), 0)
        .run_at(i64::MAX / 2)
        .await
        .unwrap();
    let mut deleted_layers = report.deleted_layers.clone();
    deleted_layers.sort();
    assert_eq!(
        deleted_layers, expected_layers,
        "GC must reclaim all obsolete seal/compaction metadata after the last root is deleted"
    );
    assert_eq!(report.deleted_slices, vec![SHARED_SLICE]);
    assert_eq!(report.orphan_bytes, SHARED_BYTES.len() as u64);
    for layer in expected_layers {
        assert!(matches!(writer.load_layer(layer).await,
            Err(WorkspaceError::LayerNotFound(found)) if found == layer));
        assert!(backend.get(&hot_layer_key(layer)).await.unwrap().is_none());
    }
    let mut bytes = [9; SHARED_BYTES.len()];
    let error = blocks
        .read_range((SHARED_SLICE, 0), 0, &mut bytes)
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<IncompleteBlockRead>().is_some());
    assert_eq!(bytes, [9; SHARED_BYTES.len()]);
    let after = peer.gc_snapshot(i64::MAX / 2, 0).await.unwrap();
    assert!(after.root_layers.is_empty());
    assert!(
        after.layers.is_empty(),
        "seal history cannot reappear from an old ControlState cache"
    );
    assert!(after.slice_references.is_empty());
    let retry = collector(peer, blocks, 0)
        .run_at(i64::MAX / 2)
        .await
        .unwrap();
    assert!(retry.deleted_layers.is_empty());
    assert!(retry.deleted_slices.is_empty());
    assert_eq!(retry.orphan_bytes, 0);
}

#[tokio::test]
async fn g13e_kv_finalize_rejects_unstaged_native_mutation_and_then_cleans_staged_rows() {
    let backend = ScheduledBackend::default();
    backend.inner.clock.store(START_NS, Ordering::SeqCst);
    let finalizer = Arc::new(budgeted_kv_store(backend.clone()));
    let peer = Arc::new(budgeted_kv_store(backend.clone()));
    peer.initialize_workspace_schema().await.unwrap();
    let workspace = peer
        .create_volume_root(create_request(107_000))
        .await
        .unwrap();
    let lease = peer
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(id(107_005)),
            holder_generation: 1,
            ttl_ns: TTL_NS,
        })
        .await
        .unwrap();
    let head = workspace.head_layer_id;
    let view = ViewContext {
        workspace_id: workspace.workspace_id,
        head_layer_id: head,
        head_epoch: workspace.head_epoch,
        lease_id: lease.lease_id,
        holder_generation: lease.holder_generation,
    };
    let pause = backend
        .pause_after_prefix_scan(&extent_layer_prefix(head), 1)
        .await;
    let mut finalizing =
        tokio::spawn(async move { finalizer.finalize_layer_metadata_deletion(vec![head]).await });
    // A correct pre-scan guard returns Busy before this scan. Accept that
    // outcome, and cancel the unused schedule before the cleanup control.
    let early_result = tokio::select! {
        () = pause.wait_entered() => None,
        result = &mut finalizing => {
            let result = result.unwrap();
            assert!(matches!(&result, Err(WorkspaceError::Busy)),
                "Writable candidate must reject finalization before scanning: {result:?}");
            backend.scan_pause.lock().await.take();
            Some(result)
        }
    };
    let metadata = WorkspaceMetaLayer::new(peer.clone(), view);
    let inode = metadata
        .create_file(1, "after-finalize-scan".into())
        .await
        .unwrap();
    let delta = peer.load_layer_delta(head).await.unwrap();
    assert!(
        delta
            .dentries
            .iter()
            .any(|row| row.name == b"after-finalize-scan")
    );
    assert!(delta.inodes.iter().any(|row| row.ino == inode));
    peer.mark_workspace_deleting(MarkDeleting {
        workspace_id: workspace.workspace_id,
        force_fence_lease: true,
    })
    .await
    .unwrap();
    let deleting = peer.load_layer(head).await.unwrap();
    assert_eq!(deleting.state, LayerState::Deleting);
    let result = match early_result {
        Some(result) => result,
        None => {
            pause.continue_once();
            tokio::time::timeout(WATCHDOG, finalizing)
                .await
                .unwrap()
                .unwrap()
        }
    };
    assert!(
        matches!(result, Err(WorkspaceError::Busy)),
        "finalize accepted rows scanned before the last public mutation: {result:?}"
    );
    assert_eq!(peer.load_layer(head).await.unwrap(), deleting);
    assert_eq!(peer.load_layer_delta(head).await.unwrap(), delta);

    // The same public rows must be fully removed after a real GC stage.
    peer.delete_layer_metadata(DeleteLayerMetadata {
        layer_ids: vec![head],
        now_ns: i64::MAX / 2,
        lease_grace_ns: 0,
    })
    .await
    .unwrap();
    peer.finalize_layer_metadata_deletion(vec![head])
        .await
        .unwrap();
    assert!(matches!(peer.load_layer(head).await,
        Err(WorkspaceError::LayerNotFound(found)) if found == head));
    assert!(backend.get(&hot_layer_key(head)).await.unwrap().is_none());
    for prefix in [
        dentry_layer_prefix(head),
        inode_layer_prefix(head),
        xattr_layer_prefix(head),
        acl_layer_prefix(head),
        extent_layer_prefix(head),
    ] {
        assert!(
            backend.scan_prefix(&prefix).await.unwrap().is_empty(),
            "finalized layer left rows in {prefix:?}"
        );
    }
    peer.finalize_layer_metadata_deletion(vec![head])
        .await
        .unwrap();
    peer.finalize_layer_metadata_deletion(Vec::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn g13e_kv_finalize_prescan_authority_change_preserves_public_orphan() {
    for initially_absent in [false, true] {
        let backend = ScheduledBackend::default();
        backend.inner.clock.store(START_NS, Ordering::SeqCst);
        let finalizer = Arc::new(budgeted_kv_store(backend.clone()));
        let peer = Arc::new(budgeted_kv_store(backend.clone()));
        peer.initialize_workspace_schema().await.unwrap();
        let layer = LayerId::from_uuid(id(108_000));
        if !initially_absent {
            peer.record_orphan_slice(RecordOrphanSlice {
                orphan_layer_id: layer,
                slice_id: 108_001,
                slice_end: 6,
            })
            .await
            .unwrap();
        }
        let pause = backend
            .pause_after_prefix_scan(&extent_layer_prefix(layer), 1)
            .await;
        let mut finalizing = tokio::spawn(async move {
            finalizer
                .finalize_layer_metadata_deletion(vec![layer])
                .await
        });
        let early_result = tokio::select! {
            () = pause.wait_entered() => None,
            result = &mut finalizing => {
                let result = result.unwrap();
                // An absent candidate may finish its no-op before a later
                // creation. A conservative Busy is also safe in either lane.
                assert!(matches!(&result, Err(WorkspaceError::Busy))
                    || (initially_absent && result.is_ok()),
                    "unexpected pre-scan result: absent={initially_absent}: {result:?}");
                backend.scan_pause.lock().await.take();
                Some(result)
            }
        };
        if !initially_absent {
            peer.finalize_layer_metadata_deletion(vec![layer])
                .await
                .unwrap();
        }
        // Both transitions use production APIs. A newly-created Deleting
        // layer is a new authority, even when its UUID matches the old target.
        peer.record_orphan_slice(RecordOrphanSlice {
            orphan_layer_id: layer,
            slice_id: 108_002,
            slice_end: 7,
        })
        .await
        .unwrap();
        let replacement = peer.load_layer(layer).await.unwrap();
        let delta = peer.load_layer_delta(layer).await.unwrap();
        assert_eq!(replacement.state, LayerState::Deleting);
        assert_eq!(replacement.owned_bytes, 7);
        assert_eq!(delta.extents.len(), 1);
        let result = match early_result {
            Some(result) => result,
            None => {
                pause.continue_once();
                let result = tokio::time::timeout(WATCHDOG, finalizing)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    matches!(&result, Err(WorkspaceError::Busy)),
                    "scanned authority changed: absent={initially_absent}: {result:?}"
                );
                result
            }
        };
        assert!(result.is_ok() || matches!(&result, Err(WorkspaceError::Busy)));
        assert_eq!(peer.load_layer(layer).await.unwrap(), replacement);
        assert_eq!(peer.load_layer_delta(layer).await.unwrap(), delta);
        peer.delete_layer_metadata(DeleteLayerMetadata {
            layer_ids: vec![layer],
            now_ns: i64::MAX / 2,
            lease_grace_ns: 0,
        })
        .await
        .unwrap();
        peer.finalize_layer_metadata_deletion(vec![layer])
            .await
            .unwrap();
        assert!(
            backend
                .scan_prefix(&extent_layer_prefix(layer))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(backend.get(&hot_layer_key(layer)).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn g13e_kv_finalize_byte_aba_preserves_recreated_public_orphan() {
    let backend = ScheduledBackend::default();
    // Equal backend timestamps and owned bytes reproduce the complete hot
    // LayerRecord, while the replacement extent refers to a different slice.
    backend.inner.clock.store(START_NS, Ordering::SeqCst);
    let finalizer = Arc::new(budgeted_kv_store(backend.clone()));
    let peer = Arc::new(budgeted_kv_store(backend.clone()));
    peer.initialize_workspace_schema().await.unwrap();
    let layer = LayerId::from_uuid(id(109_000));
    peer.record_orphan_slice(RecordOrphanSlice {
        orphan_layer_id: layer,
        slice_id: 109_001,
        slice_end: 6,
    })
    .await
    .unwrap();
    let original = peer.load_layer(layer).await.unwrap();
    let original_raw = backend.get(&hot_layer_key(layer)).await.unwrap();
    let original_delta = peer.load_layer_delta(layer).await.unwrap();
    assert!(original_raw.is_some());
    let pause = backend
        .pause_after_prefix_scan(&extent_layer_prefix(layer), 1)
        .await;
    let finalizing = tokio::spawn(async move {
        finalizer
            .finalize_layer_metadata_deletion(vec![layer])
            .await
    });
    pause.wait_entered().await;
    peer.finalize_layer_metadata_deletion(vec![layer])
        .await
        .unwrap();
    assert!(backend.get(&hot_layer_key(layer)).await.unwrap().is_none());
    peer.record_orphan_slice(RecordOrphanSlice {
        orphan_layer_id: layer,
        slice_id: 109_002,
        slice_end: 6,
    })
    .await
    .unwrap();
    let replacement = peer.load_layer(layer).await.unwrap();
    let replacement_raw = backend.get(&hot_layer_key(layer)).await.unwrap();
    let replacement_delta = peer.load_layer_delta(layer).await.unwrap();
    assert_eq!(replacement, original, "fixture must reproduce LayerRecord");
    assert_eq!(
        replacement_raw, original_raw,
        "fixture must reproduce every authority byte"
    );
    assert_ne!(replacement_delta, original_delta);
    assert_eq!(replacement_delta.extents.len(), 1);
    pause.continue_once();
    let result = tokio::time::timeout(WATCHDOG, finalizing)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, Err(WorkspaceError::Busy)),
        "finalize accepted a byte-identical recreated layer: {result:?}"
    );
    assert_eq!(
        backend.get(&hot_layer_key(layer)).await.unwrap(),
        original_raw
    );
    assert_eq!(peer.load_layer(layer).await.unwrap(), replacement);
    assert_eq!(
        peer.load_layer_delta(layer).await.unwrap(),
        replacement_delta
    );
    peer.delete_layer_metadata(DeleteLayerMetadata {
        layer_ids: vec![layer],
        now_ns: i64::MAX / 2,
        lease_grace_ns: 0,
    })
    .await
    .unwrap();
    peer.finalize_layer_metadata_deletion(vec![layer])
        .await
        .unwrap();
    assert!(backend.get(&hot_layer_key(layer)).await.unwrap().is_none());
    assert!(
        backend
            .scan_prefix(&extent_layer_prefix(layer))
            .await
            .unwrap()
            .is_empty()
    );
}
