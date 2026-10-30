// Candidate inclusion: kv_store.rs::tests::g13_real_backend_candidates.
// Every metadata read/write/CAS uses a real Redis or TiKV backend. Scheduling
// pauses occur only after real scan results or before the real CAS invocation.
use super::*;
use crate::workspace_overlay::catalog::{DeleteLayerMetadata, MarkDeleting, ReleaseLease};
use crate::workspace_overlay::stores::g13_gc_contract_tests::{
    SHARED_SLICE, assert_child_reads_shared, build_shared_base, collector,
};
use std::future::Future;
use tokio::sync::Semaphore;
use tokio::task::{AbortHandle, JoinHandle};

const BOUNDARY_TIMEOUT: Duration = Duration::from_secs(30);
const CASE_TIMEOUT: Duration = Duration::from_secs(180);
const GC_NOW: i64 = i64::MAX / 2;

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
        tokio::time::timeout(BOUNDARY_TIMEOUT, self.resume.acquire())
            .await
            .expect("real-backend boundary was never resumed")
            .unwrap()
            .forget();
    }

    async fn wait_entered(&self) {
        tokio::time::timeout(BOUNDARY_TIMEOUT, self.entered.acquire())
            .await
            .expect("production real-backend boundary was not reached")
            .unwrap()
            .forget();
    }

    fn release_on_drop(&self) -> ResumeOnDrop {
        ResumeOnDrop(self.clone())
    }

    fn resume(&self) {
        self.resume.add_permits(1);
    }
}

struct ResumeOnDrop(PausePoint);
impl Drop for ResumeOnDrop {
    fn drop(&mut self) {
        self.0.resume();
    }
}

struct JobDone(Arc<Semaphore>);
impl Drop for JobDone {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

// Await actual task destruction before namespace cleanup, including panic and
// timeout paths. The completion guard is captured before the first task poll.
type JobEntries = Vec<(AbortHandle, Arc<Semaphore>)>;

#[derive(Clone, Default)]
struct Jobs(Arc<std::sync::Mutex<JobEntries>>);

impl Jobs {
    fn spawn<T: Send + 'static>(
        &self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Flight<T> {
        let done = Arc::new(Semaphore::new(0));
        let completion = JobDone(done.clone());
        let handle = tokio::spawn(async move {
            let _completion = completion;
            future.await
        });
        self.0.lock().unwrap().push((handle.abort_handle(), done));
        Flight(handle)
    }

    async fn stop_and_wait(&self) {
        let jobs = std::mem::take(&mut *self.0.lock().unwrap());
        for (abort, _) in &jobs {
            abort.abort();
        }
        for (_, done) in jobs {
            tokio::time::timeout(BOUNDARY_TIMEOUT, done.acquire())
                .await
                .expect("real-backend task was not retired before cleanup")
                .unwrap()
                .forget();
        }
    }
}

struct Flight<T>(JoinHandle<T>);
impl<T> Drop for Flight<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Flight<T> {
    async fn finish(&mut self) -> T {
        tokio::time::timeout(BOUNDARY_TIMEOUT, &mut self.0)
            .await
            .expect("resumed real-backend operation did not finish")
            .unwrap()
    }
}

struct ScanPause {
    prefix: Vec<u8>,
    remaining: usize,
    pause: PausePoint,
}

struct ForkPause {
    workspace_key: Vec<u8>,
    pause: PausePoint,
}

#[derive(Clone)]
struct ScheduledRealBackend<B> {
    inner: B,
    scan_pause: Arc<Mutex<Option<ScanPause>>>,
    fork_pause: Arc<Mutex<Option<ForkPause>>>,
    attempted_checks: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
    conflicting_checks: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
}

impl<B: WorkspaceKvBackend + Clone> ScheduledRealBackend<B> {
    fn new(inner: B) -> Self {
        Self {
            inner,
            scan_pause: Arc::new(Mutex::new(None)),
            fork_pause: Arc::new(Mutex::new(None)),
            attempted_checks: Arc::new(Mutex::new(Vec::new())),
            conflicting_checks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn pause_after_scan(&self, prefix: &[u8], occurrence: usize) -> PausePoint {
        assert!(occurrence > 0);
        let pause = PausePoint::new();
        let mut schedule = self.scan_pause.lock().await;
        assert!(schedule.is_none());
        *schedule = Some(ScanPause {
            prefix: prefix.to_vec(),
            remaining: occurrence,
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

    async fn after_scan(&self, prefix: &[u8]) {
        let pause = {
            let mut schedule = self.scan_pause.lock().await;
            if let Some(gate) = schedule.as_mut() {
                if gate.prefix != prefix {
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
        if let Some(pause) = pause {
            pause.pause().await;
        }
    }

    async fn before_cas(&self, checks: &[KvCheck], writes: &[KvWrite]) {
        self.attempted_checks
            .lock()
            .await
            .push(checks.iter().map(|check| check.key.clone()).collect());
        let pause = {
            let mut schedule = self.fork_pause.lock().await;
            let matched = schedule.as_ref().is_some_and(|gate| {
                checks
                    .iter()
                    .any(|check| check.key == gate.workspace_key && check.expected.is_none())
                    && writes.iter().any(|write| {
                        matches!(write, KvWrite::Put { key, .. } if *key == gate.workspace_key)
                    })
            });
            if matched {
                schedule.take().map(|gate| gate.pause)
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause.pause().await;
        }
    }

    async fn record_result(&self, checks: &[KvCheck], result: &Result<bool, WorkspaceError>) {
        if matches!(result, Ok(false)) {
            self.conflicting_checks
                .lock()
                .await
                .push(checks.iter().map(|check| check.key.clone()).collect());
        }
    }
}

#[async_trait]
impl<B: WorkspaceKvBackend + Clone> WorkspaceKvBackend for ScheduledRealBackend<B> {
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
        self.inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await
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
        max_records: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        let rows = self.inner.scan_prefix_bounded(prefix, max_records).await?;
        self.after_scan(prefix).await;
        Ok(rows)
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.before_cas(checks, writes).await;
        let result = self.inner.compare_and_swap(checks, writes).await;
        self.record_result(checks, &result).await;
        result
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        expires_at_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        self.before_cas(checks, writes).await;
        let result = self
            .inner
            .compare_and_swap_before(checks, writes, expires_at_ns)
            .await;
        self.record_result(checks, &result).await;
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
        self.before_cas(checks, &[]).await;
        let result = self
            .inner
            .authenticate_checks_before_bounded(checks, expires_at_ns, limits)
            .await;
        self.record_result(checks, &result).await;
        result
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
    }
}

fn fresh_root_request() -> CreateVolumeRoot {
    CreateVolumeRoot {
        upper_bucket: None,
        volume_format: "workspace-v1".into(),
        schema_version: WORKSPACE_SCHEMA_VERSION,
        volume_id: Uuid::new_v4(),
        workspace_id: WorkspaceId::new(),
        root_layer_id: LayerId::new(),
        writable_layer_id: LayerId::new(),
        owner_id: Some("g13-real-backend".into()),
    }
}

fn fresh_fork_request(base_revision: BaseRevision) -> CreateWorkspace {
    CreateWorkspace {
        base_revision,
        workspace_id: WorkspaceId::new(),
        head_layer_id: LayerId::new(),
        owner_id: Some("g13-real-backend-child".into()),
        upper_bucket: None,
    }
}

#[derive(Clone, Copy, Debug)]
enum Case {
    FirstPackedBinding,
    PackedHistory,
    NativeForkAfterScan,
    NativeDeleteBeforeForkCas,
    MissingOrphanCreation,
    OrphanRecreation,
    NativeRecreation,
}

const CASES: [Case; 7] = [
    Case::FirstPackedBinding,
    Case::PackedHistory,
    Case::NativeForkAfterScan,
    Case::NativeDeleteBeforeForkCas,
    Case::MissingOrphanCreation,
    Case::OrphanRecreation,
    Case::NativeRecreation,
];

async fn packed_case<B: WorkspaceKvBackend + Clone>(
    backend: ScheduledRealBackend<B>,
    peer_backend: B,
    jobs: Jobs,
    race: bool,
) {
    let gc_store = Arc::new(budgeted_kv_store(backend.clone()));
    let peer = Arc::new(budgeted_kv_store(peer_backend));
    let (_objects, _client, _snapshot, proof, _payload) =
        crate::workspace_overlay::stores::binding_tests::packed().await;
    let request =
        crate::workspace_overlay::stores::binding_tests::request(peer.as_ref(), proof).await;
    let base = request.expected_base.layer_id;
    let head = request.guard.expected_head_layer_id;
    let pause = if race {
        Some(backend.pause_after_scan(b"packed/v3/current/", 1).await)
    } else {
        None
    };
    let resume = pause.as_ref().map(PausePoint::release_on_drop);
    let mut deletion = pause.as_ref().map(|_| {
        let gc_store = gc_store.clone();
        jobs.spawn(async move {
            gc_store
                .delete_layer_metadata(DeleteLayerMetadata {
                    layer_ids: vec![base],
                    now_ns: GC_NOW,
                    lease_grace_ns: 0,
                })
                .await
        })
    });
    if let Some(pause) = &pause {
        pause.wait_entered().await;
        assert!(
            peer.load_packed_binding_version(request.guard.workspace_id, 1)
                .await
                .unwrap()
                .is_none()
        );
    }
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
    let before_base = peer.load_layer(base).await.unwrap();
    let before_head = peer.load_layer(head).await.unwrap();
    if let Some(deletion) = deletion.as_mut() {
        drop(resume);
        assert!(matches!(deletion.finish().await, Err(WorkspaceError::Busy)));
        assert!(
            backend
                .conflicting_checks
                .lock()
                .await
                .iter()
                .any(|keys| { keys.contains(&PACKED_ROOT_GENERATION_KEY.to_vec()) }),
            "the actual backend must reject the stale packed-root generation CAS"
        );
    }
    if !race {
        // Isolate retained history from current: install above authenticated
        // both records through the public API. This test-only exact deletion
        // removes current, preserves the original history bytes, and advances
        // the production root fence atomically through independent backend B.
        let keys = vec![
            packed_current_key(request.guard.workspace_id),
            packed_history_key(request.guard.workspace_id, 1),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
        ];
        let values = peer.backend.get_many_consistent(&keys).await.unwrap();
        assert_eq!(values.len(), 3);
        assert!(values[0].is_some());
        assert_eq!(values[0], values[1]);
        let next = next_packed_root_generation(&values[2]).unwrap();
        let checks = keys
            .iter()
            .cloned()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let writes = [
            KvWrite::Delete {
                key: keys[0].clone(),
            },
            put(PACKED_ROOT_GENERATION_KEY.to_vec(), &next).unwrap(),
        ];
        assert!(
            peer.backend
                .compare_and_swap(&checks, &writes)
                .await
                .unwrap()
        );
        assert!(peer.backend.get(&keys[0]).await.unwrap().is_none());
    }
    let snapshot = peer.gc_snapshot(GC_NOW, 0).await.unwrap();
    for layer in [base, head] {
        assert!(snapshot.root_layers.contains(&layer));
        assert!(matches!(
            gc_store
                .delete_layer_metadata(DeleteLayerMetadata {
                    layer_ids: vec![layer],
                    now_ns: GC_NOW,
                    lease_grace_ns: 0,
                })
                .await,
            Err(WorkspaceError::Busy)
        ));
    }
    // This is a Deleting candidate. It exercises genuine finalizer root
    // revalidation, unlike an early rejection of a still-Writable candidate.
    assert_eq!(before_head.state, LayerState::Deleting);
    assert!(matches!(
        gc_store.finalize_layer_metadata_deletion(vec![head]).await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(peer.load_layer(base).await.unwrap(), before_base);
    assert_eq!(peer.load_layer(head).await.unwrap(), before_head);
    assert_eq!(
        peer.load_packed_binding_version(request.guard.workspace_id, 1)
            .await
            .unwrap(),
        Some(binding)
    );
}

async fn native_fork_case<B: WorkspaceKvBackend + Clone>(
    backend: ScheduledRealBackend<B>,
    peer_backend: B,
    jobs: Jobs,
    delete_first: bool,
) {
    let scheduled_store = Arc::new(budgeted_kv_store(backend.clone()));
    let peer = Arc::new(budgeted_kv_store(peer_backend));
    // Real native metadata/CAS; payload bytes alone use the existing bounded
    // InMemoryBlockStore helper. No metadata or authority row is injected.
    let (base, inode, blocks) = build_shared_base(peer.clone(), fresh_root_request()).await;
    if delete_first {
        let request = fresh_fork_request(base.clone());
        let child = request.workspace_id;
        let child_head = request.head_layer_id;
        let pause = backend.pause_before_fork_cas(child).await;
        let resume = pause.release_on_drop();
        let mut forking =
            jobs.spawn(async move { scheduled_store.create_workspace(request).await });
        pause.wait_entered().await;
        peer.delete_layer_metadata(DeleteLayerMetadata {
            layer_ids: vec![base.layer_id],
            now_ns: GC_NOW,
            lease_grace_ns: 0,
        })
        .await
        .unwrap();
        let deleting = peer.load_layer(base.layer_id).await.unwrap();
        assert_eq!(deleting.state, LayerState::Deleting);
        drop(resume);
        assert!(
            matches!(forking.finish().await, Err(WorkspaceError::LayerNotFound(found)) if found == base.layer_id)
        );
        assert!(
            matches!(peer.load_workspace(child).await, Err(WorkspaceError::WorkspaceNotFound(found)) if found == child)
        );
        assert!(
            matches!(peer.load_layer(child_head).await, Err(WorkspaceError::LayerNotFound(found)) if found == child_head)
        );
        assert!(
            backend
                .inner
                .get(&hot_workspace_key(child))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            backend
                .inner
                .get(&hot_layer_key(child_head))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(peer.load_layer(base.layer_id).await.unwrap(), deleting);
    } else {
        let before = peer.load_layer(base.layer_id).await.unwrap();
        // One genuine destructive revalidation scan (no synthetic GcSnapshot).
        let pause = backend.pause_after_scan(HOT_WORKSPACE_PREFIX, 1).await;
        let resume = pause.release_on_drop();
        let layer = base.layer_id;
        let mut deletion = jobs.spawn(async move {
            scheduled_store
                .delete_layer_metadata(DeleteLayerMetadata {
                    layer_ids: vec![layer],
                    now_ns: GC_NOW,
                    lease_grace_ns: 0,
                })
                .await
        });
        pause.wait_entered().await;
        let child = peer
            .create_workspace(fresh_fork_request(base.clone()))
            .await
            .unwrap();
        drop(resume);
        assert!(matches!(deletion.finish().await, Err(WorkspaceError::Busy)));
        assert_eq!(peer.load_layer(base.layer_id).await.unwrap(), before);
        assert_child_reads_shared(
            peer.clone(),
            child.workspace_id,
            &base,
            inode,
            blocks.as_ref(),
        )
        .await;
        let report = collector(peer.clone(), blocks.clone(), 0)
            .run_at(GC_NOW)
            .await
            .unwrap();
        assert!(!report.deleted_layers.contains(&base.layer_id));
        assert!(!report.deleted_layers.contains(&child.head_layer_id));
        assert!(!report.deleted_slices.contains(&SHARED_SLICE));
        assert_child_reads_shared(peer, child.workspace_id, &base, inode, blocks.as_ref()).await;
    }
}

async fn recreation_case<B: WorkspaceKvBackend + Clone>(
    backend: ScheduledRealBackend<B>,
    peer_backend: B,
    jobs: Jobs,
    case: Case,
) {
    let finalizer = Arc::new(budgeted_kv_store(backend.clone()));
    let peer = Arc::new(budgeted_kv_store(peer_backend));
    peer.initialize_workspace_schema().await.unwrap();
    let layer = if matches!(case, Case::NativeRecreation) {
        let (base, _, _blocks) = build_shared_base(peer.clone(), fresh_root_request()).await;
        peer.delete_layer_metadata(DeleteLayerMetadata {
            layer_ids: vec![base.layer_id],
            now_ns: GC_NOW,
            lease_grace_ns: 0,
        })
        .await
        .unwrap();
        assert!(
            !peer
                .load_layer_delta(base.layer_id)
                .await
                .unwrap()
                .extents
                .is_empty()
        );
        base.layer_id
    } else {
        // Orphan registration does not initialize a volume. The real peer's
        // bounded native finalizer also authenticates that volume authority.
        peer.create_volume_root(fresh_root_request()).await.unwrap();
        let layer = LayerId::new();
        if matches!(case, Case::OrphanRecreation) {
            peer.record_orphan_slice(RecordOrphanSlice {
                orphan_layer_id: layer,
                slice_id: 200_001,
                slice_end: 6,
            })
            .await
            .unwrap();
        }
        layer
    };
    assert!(
        peer.load_volume_header().await.unwrap().is_some(),
        "recreation fixture must provide initialized volume authority"
    );
    let generation_before = backend
        .inner
        .get(LAYER_INVENTORY_GENERATION_KEY)
        .await
        .unwrap();
    let pause = backend
        .pause_after_scan(&extent_layer_prefix(layer), 1)
        .await;
    let resume = pause.release_on_drop();
    let mut deletion = jobs.spawn(async move {
        finalizer
            .finalize_layer_metadata_deletion(vec![layer])
            .await
    });
    pause.wait_entered().await;
    if !matches!(case, Case::MissingOrphanCreation) {
        peer.finalize_layer_metadata_deletion(vec![layer])
            .await
            .unwrap();
    }
    assert!(
        backend
            .inner
            .get(&hot_layer_key(layer))
            .await
            .unwrap()
            .is_none()
    );
    peer.record_orphan_slice(RecordOrphanSlice {
        orphan_layer_id: layer,
        slice_id: 200_002,
        slice_end: 7,
    })
    .await
    .unwrap();
    let replacement = peer.load_layer(layer).await.unwrap();
    let raw = backend.inner.get(&hot_layer_key(layer)).await.unwrap();
    let delta = peer.load_layer_delta(layer).await.unwrap();
    assert_eq!(replacement.state, LayerState::Deleting);
    assert_eq!(replacement.owned_bytes, 7);
    assert_eq!(delta.extents.len(), 1);
    let generation_after = backend
        .inner
        .get(LAYER_INVENTORY_GENERATION_KEY)
        .await
        .unwrap();
    assert_ne!(
        generation_before, generation_after,
        "public deletion/creation must change the live inventory authority"
    );
    // Server timestamps are real. This proves changed inventory/recreation;
    // it deliberately does not claim a byte-identical LayerRecord ABA.
    drop(resume);
    assert!(
        matches!(deletion.finish().await, Err(WorkspaceError::Busy)),
        "{case:?}"
    );
    assert_eq!(backend.inner.get(&hot_layer_key(layer)).await.unwrap(), raw);
    assert_eq!(peer.load_layer(layer).await.unwrap(), replacement);
    assert_eq!(peer.load_layer_delta(layer).await.unwrap(), delta);
    // Positive control: fresh public staging/finalization removes replacement.
    peer.delete_layer_metadata(DeleteLayerMetadata {
        layer_ids: vec![layer],
        now_ns: GC_NOW,
        lease_grace_ns: 0,
    })
    .await
    .unwrap();
    peer.finalize_layer_metadata_deletion(vec![layer])
        .await
        .unwrap();
    assert!(
        backend
            .inner
            .get(&hot_layer_key(layer))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        backend
            .inner
            .scan_prefix(&extent_layer_prefix(layer))
            .await
            .unwrap()
            .is_empty()
    );
}

async fn run_case<B: WorkspaceKvBackend + Clone>(a: B, b: B, jobs: Jobs, case: Case) {
    let scheduled = ScheduledRealBackend::new(a);
    match case {
        Case::FirstPackedBinding => packed_case(scheduled, b, jobs, true).await,
        Case::PackedHistory => packed_case(scheduled, b, jobs, false).await,
        Case::NativeForkAfterScan => native_fork_case(scheduled, b, jobs, false).await,
        Case::NativeDeleteBeforeForkCas => native_fork_case(scheduled, b, jobs, true).await,
        _ => recreation_case(scheduled, b, jobs, case).await,
    }
}

async fn cleanup_logical_namespace<B: WorkspaceKvBackend>(backend: &B) {
    // Backend scoping maps only this case's fresh UUID namespace. No global
    // Redis FLUSH, TiKV range-delete, compose teardown, or foreign key scan.
    for _ in 0..64 {
        let rows = backend
            .scan_prefix_bounded(b"", 128)
            .await
            .expect("namespace cleanup scan failed");
        if rows.is_empty() {
            return;
        }
        let checks: Vec<_> = rows
            .iter()
            .map(|row| KvCheck {
                key: row.key.clone(),
                expected: Some(row.value.clone()),
            })
            .collect();
        let writes: Vec<_> = rows
            .into_iter()
            .map(|row| KvWrite::Delete { key: row.key })
            .collect();
        assert!(
            backend
                .compare_and_swap(&checks, &writes)
                .await
                .expect("namespace cleanup CAS failed"),
            "case tasks still modified namespace during cleanup"
        );
    }
    panic!("bounded namespace cleanup exhausted its test-only key budget");
}

async fn run_and_cleanup<B: WorkspaceKvBackend + Clone>(
    a: B,
    b: B,
    case: Case,
) -> (Result<(), tokio::task::JoinError>, bool) {
    let jobs = Jobs::default();
    let case_jobs = jobs.clone();
    let case_a = a.clone();
    let mut task = tokio::spawn(async move { run_case(case_a, b, case_jobs, case).await });
    let (result, timed_out) = match tokio::time::timeout(CASE_TIMEOUT, &mut task).await {
        Ok(result) => (result, false),
        Err(_) => {
            task.abort();
            (task.await, true)
        }
    };
    jobs.stop_and_wait().await;
    cleanup_logical_namespace(&a).await;
    (result, timed_out)
}

async fn cleanup_redis_index(url: &str, namespace: &str) {
    // Redis owns two internal index keys not returned by logical prefix reads.
    // Delete exactly these keys after all indexed user keys and tasks are gone.
    let prefix = format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/");
    let keys = [
        format!("{prefix}__index/keys"),
        format!("{prefix}__index/ready"),
    ];
    let client = redis::Client::open(url).expect("isolated Redis cleanup client failed");
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("isolated Redis cleanup connection failed");
    redis::cmd("DEL")
        .arg(&keys)
        .query_async::<usize>(&mut connection)
        .await
        .expect("isolated Redis index cleanup failed");
    let remaining: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
        .arg(&keys)
        .query_async(&mut connection)
        .await
        .expect("isolated Redis index cleanup read failed");
    assert!(remaining.iter().all(Option::is_none));
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL; uses fresh per-case UUID namespaces"]
async fn g13_real_redis_destructive_revalidation_contract() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL is required");
    for case in CASES {
        let namespace = format!("g13real-{}", Uuid::new_v4().simple());
        // Independent connections and independent KvWorkspaceStore mutexes.
        let a = RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .expect("real Redis connection A failed");
        let b = RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .expect("real Redis connection B failed");
        let (result, timed_out) = run_and_cleanup(a, b, case).await;
        cleanup_redis_index(&url, &namespace).await;
        assert!(
            !timed_out && result.is_ok(),
            "{case:?}: real Redis contract task failed: {result:?}, timed_out={timed_out}"
        );
    }
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS; uses fresh per-case UUID namespaces"]
async fn g13_real_tikv_destructive_revalidation_contract() {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .expect("BREWFS_TEST_TIKV_PD_ENDPOINTS is required")
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    for case in CASES {
        let namespace = format!("g13real-{}", Uuid::new_v4().simple());
        let a = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
            .await
            .expect("real TiKV connection A failed");
        let b = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
            .await
            .expect("real TiKV connection B failed");
        let (result, timed_out) = run_and_cleanup(a, b, case).await;
        assert!(
            !timed_out && result.is_ok(),
            "{case:?}: real TiKV contract task failed: {result:?}, timed_out={timed_out}"
        );
    }
}
