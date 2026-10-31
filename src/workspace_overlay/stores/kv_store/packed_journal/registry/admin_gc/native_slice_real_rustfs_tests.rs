//! Two real metadata clients and production collector against an owned RustFS.
//! Scheduling changes when a genuine final CAS runs; it never replaces it.
use super::*;
use crate::cadapter::s3::{S3Backend, S3Config};
use crate::workspace_overlay::catalog::{
    AcquireLease, AppendDataExtent, CreateVolumeRoot, HeadGuard, RecordOrphanSlice, WorkspaceStore,
};
use crate::workspace_overlay::ids::LeaseId;
use crate::workspace_overlay::model::{DataExtentDelta, LayerState};
use crate::workspace_overlay::stores::kv_backend::{KvEntry, KvReadLimits};
use crate::workspace_overlay::stores::kv_store::native_reverse;
use crate::workspace_overlay::stores::kv_store::native_slice_deletion::slice_deletion_key;
use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
use crate::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{AbortHandle, JoinHandle};
use uuid::Uuid;

const SLICE: u64 = 22001;
const BLOCK_BYTES: u32 = 4096;
const BOUNDARY_TIMEOUT: Duration = Duration::from_secs(30);
const CASE_TIMEOUT: Duration = Duration::from_secs(180);

fn object_ok<T>(result: anyhow::Result<T>) -> T {
    // An SDK failure can carry signed request headers in its source chain.
    // Test logs retain only this fixed diagnostic, never Debug on that chain.
    result.unwrap_or_else(|_| panic!("actual RustFS fixture operation failed"))
}

#[derive(Clone, Copy, Debug)]
enum Case {
    PublicationWins,
    ReservationWins,
    SixtyFiveBlockRecovery,
}

impl Case {
    fn label(self) -> &'static str {
        match self {
            Self::PublicationWins => "publication-wins",
            Self::ReservationWins => "reservation-wins-unknown-delete",
            Self::SixtyFiveBlockRecovery => "sixty-five-block-recovery",
        }
    }

    fn blocks(self) -> u32 {
        match self {
            Self::SixtyFiveBlockRecovery => 65,
            _ => 1,
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
        tokio::time::timeout(BOUNDARY_TIMEOUT, self.resume.acquire())
            .await
            .expect("actual extent CAS was not resumed")
            .unwrap()
            .forget();
    }

    async fn wait_entered(&self) {
        tokio::time::timeout(BOUNDARY_TIMEOUT, self.entered.acquire())
            .await
            .expect("public extent append did not reach its actual final CAS")
            .unwrap()
            .forget();
    }

    fn resume(&self) {
        self.resume.add_permits(1);
    }
}

struct JobDone(Arc<Semaphore>);
impl Drop for JobDone {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

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
        let task = tokio::spawn(async move {
            let _completion = completion;
            future.await
        });
        self.0.lock().unwrap().push((task.abort_handle(), done));
        Flight(task)
    }

    async fn stop_and_wait(&self) -> anyhow::Result<()> {
        let jobs = std::mem::take(&mut *self.0.lock().unwrap());
        for (abort, _) in &jobs {
            abort.abort();
        }
        for (_, done) in jobs {
            tokio::time::timeout(BOUNDARY_TIMEOUT, done.acquire())
                .await??
                .forget();
        }
        Ok(())
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
            .expect("actual resumed extent append timed out")
            .unwrap()
    }
}

#[derive(Clone)]
struct Scheduled<B> {
    inner: B,
    observe_extents: bool,
    pause: Arc<Mutex<Option<PausePoint>>>,
    reservation_pause: Arc<Mutex<Option<PausePoint>>>,
    attempts: Arc<AtomicUsize>,
    conflicts: Arc<AtomicUsize>,
    commits: Arc<AtomicUsize>,
    reservation_conflicts: Arc<AtomicUsize>,
}

impl<B: WorkspaceKvBackend + Clone> Scheduled<B> {
    fn new(inner: B, observe_extents: bool) -> Self {
        Self {
            inner,
            observe_extents,
            pause: Arc::new(Mutex::new(None)),
            reservation_pause: Arc::new(Mutex::new(None)),
            attempts: Arc::new(AtomicUsize::new(0)),
            conflicts: Arc::new(AtomicUsize::new(0)),
            commits: Arc::new(AtomicUsize::new(0)),
            reservation_conflicts: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn pause_next_append(&self) -> PausePoint {
        let pause = PausePoint::new();
        assert!(self.pause.lock().await.replace(pause.clone()).is_none());
        pause
    }

    async fn pause_next_reservation(&self) -> PausePoint {
        let pause = PausePoint::new();
        assert!(
            self.reservation_pause
                .lock()
                .await
                .replace(pause.clone())
                .is_none()
        );
        pause
    }

    fn is_reservation(writes: &[KvWrite]) -> bool {
        writes.iter().any(
            |write| matches!(write, KvWrite::Put { key, .. } if *key == slice_deletion_key(SLICE)),
        )
    }

    async fn before_cas(&self, checks: &[KvCheck], writes: &[KvWrite]) -> bool {
        if Self::is_reservation(writes) {
            assert!(checks.iter().any(|check| {
                check
                    .key
                    .starts_with(b"packed/v3/native-extent-generation/")
            }));
            let pause = self.reservation_pause.lock().await.take();
            if let Some(pause) = pause {
                pause.pause().await;
            }
        }
        let extent = self.observe_extents && writes.iter().any(|write| {
            matches!(write, KvWrite::Put { key, .. } if key.starts_with(b"delta/extent/"))
        });
        if extent {
            // Authenticate the production packet at the scheduling boundary.
            assert!(checks.iter().any(|check| {
                check.key == slice_deletion_key(SLICE) && check.expected.is_none()
            }));
            assert!(checks.iter().any(|check| {
                check
                    .key
                    .starts_with(b"packed/v3/native-extent-generation/")
            }));
            self.attempts.fetch_add(1, Ordering::SeqCst);
            let pause = self.pause.lock().await.take();
            if let Some(pause) = pause {
                pause.pause().await;
            }
        }
        extent
    }

    fn record_result(
        &self,
        extent: bool,
        writes: &[KvWrite],
        result: &Result<bool, WorkspaceError>,
    ) {
        if Self::is_reservation(writes) && matches!(result, Ok(false)) {
            self.reservation_conflicts.fetch_add(1, Ordering::SeqCst);
        }
        if extent {
            match result {
                Ok(true) => {
                    self.commits.fetch_add(1, Ordering::SeqCst);
                }
                Ok(false) => {
                    self.conflicts.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {}
            }
        }
    }
}

#[async_trait]
impl<B: WorkspaceKvBackend + Clone> WorkspaceKvBackend for Scheduled<B> {
    fn name(&self) -> &'static str {
        self.inner.name()
    }
    fn supports_consistent_reads(&self) -> bool {
        self.inner.supports_consistent_reads()
    }
    fn native_gc_metadata_page_quota(&self) -> Option<usize> {
        self.inner.native_gc_metadata_page_quota()
    }
    async fn authenticate_gc_admin(&self) -> Result<(), WorkspaceError> {
        self.inner.authenticate_gc_admin().await
    }
    async fn shutdown_metadata_backend(&self) -> Result<(), WorkspaceError> {
        self.inner.shutdown_metadata_backend().await
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
    async fn get_many_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.inner.get_many_with_time(keys).await
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
        self.inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await
    }
    async fn get_publication_packet_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.inner
            .get_publication_packet_consistent_with_time_bounded(keys, limits)
            .await
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner.scan_prefix(prefix).await
    }
    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        count: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner.scan_prefix_bounded(prefix, count).await
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
        self.inner
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        before: i64,
        limits: KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        self.inner
            .authenticate_checks_before_bounded(checks, before, limits)
            .await
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        let extent = self.before_cas(checks, writes).await;
        let result = self.inner.compare_and_swap(checks, writes).await;
        self.record_result(extent, writes, &result);
        result
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        before: i64,
    ) -> Result<bool, WorkspaceError> {
        let extent = self.before_cas(checks, writes).await;
        let result = self
            .inner
            .compare_and_swap_before(checks, writes, before)
            .await;
        self.record_result(extent, writes, &result);
        result
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        let extent = self.before_cas(checks, writes).await;
        let result = self
            .inner
            .compare_and_swap_in_time_window(checks, writes, lower, upper)
            .await;
        self.record_result(extent, writes, &result);
        result
    }
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
    }
}

#[derive(Clone)]
struct ActualObjects {
    inner: S3Backend,
    prefix: String,
    deletes: Arc<Mutex<Vec<String>>>,
    successful_deletes: Arc<AtomicUsize>,
    lose_delete_reply: Arc<AtomicBool>,
}

impl ActualObjects {
    fn key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }
}

#[async_trait]
impl ObjectBackend for ActualObjects {
    fn forbids_mutation_replay(&self) -> bool {
        self.inner.forbids_mutation_replay()
    }
    async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object(&self.key(key), data).await
    }
    async fn put_object_create_only(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.inner
            .put_object_create_only(&self.key(key), data)
            .await
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(&self.key(key)).await
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(&self.key(key)).await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        bytes: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.inner
            .get_object_range(&self.key(key), offset, bytes)
            .await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(&self.key(key)).await
    }
    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        // Counts are actual delegated adapter calls, not a claim about HTTP attempts.
        self.deletes.lock().await.push(key.to_owned());
        self.inner.delete_object(&self.key(key)).await?;
        self.successful_deletes.fetch_add(1, Ordering::SeqCst);
        if self.lose_delete_reply.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected unknown reply after an actual successful RustFS DELETE");
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Objects {
    runtime: ActualObjects,
    admin: ActualObjects,
}

impl Objects {
    async fn connect() -> Self {
        let required =
            |name| std::env::var(name).expect("explicit owned RustFS configuration required");
        assert_eq!(required("BREWFS_TEST_ORIGINAL_OBJECT_BACKEND"), "rustfs");
        let endpoint = required("BREWFS_TEST_ORIGINAL_RUSTFS_ENDPOINT");
        let port = endpoint
            .strip_prefix("http://127.0.0.1:")
            .and_then(|port| port.parse::<u16>().ok())
            .expect("owned RustFS endpoint must be HTTP IPv4 loopback");
        assert_ne!(port, 0);
        let owner = required("BREWFS_TEST_ORIGINAL_RUSTFS_OWNER");
        assert_eq!(Uuid::parse_str(&owner).unwrap().simple().to_string(), owner);
        let bucket = required("BREWFS_TEST_ORIGINAL_RUSTFS_BUCKET");
        assert_eq!(bucket, format!("brewfs-v3-lifecycle-{owner}"));
        let config = S3Config {
            bucket,
            endpoint: Some(endpoint),
            region: Some("us-east-1".into()),
            force_path_style: true,
            max_concurrency: 2,
            max_retries: 1,
            ..Default::default()
        };
        let runtime_key = required("BREWFS_TEST_ORIGINAL_RUSTFS_ACCESS_KEY");
        let runtime_secret = required("BREWFS_TEST_ORIGINAL_RUSTFS_SECRET_KEY");
        let admin_key = required("BREWFS_TEST_SID_RUSTFS_ADMIN_ACCESS_KEY");
        let admin_secret = required("BREWFS_TEST_SID_RUSTFS_ADMIN_SECRET_KEY");
        assert!(
            runtime_key != admin_key,
            "runtime and GC admin IAM identities must differ"
        );
        assert!(
            runtime_secret != admin_secret,
            "runtime and GC admin secrets must differ"
        );
        let runtime = object_ok(
            S3Backend::with_static_credentials(config.clone(), runtime_key, runtime_secret).await,
        );
        // This constructor disables SDK retries as well as adapter mutation replay.
        let admin =
            object_ok(S3Backend::with_gc_static_credentials(config, admin_key, admin_secret).await);
        assert!(admin.forbids_mutation_replay());
        let prefix = format!("sid-real/{}/", Uuid::new_v4().simple());
        let wrap = |inner| ActualObjects {
            inner,
            prefix: prefix.clone(),
            deletes: Arc::new(Mutex::new(Vec::new())),
            successful_deletes: Arc::new(AtomicUsize::new(0)),
            lose_delete_reply: Arc::new(AtomicBool::new(false)),
        };
        Self {
            runtime: wrap(runtime),
            admin: wrap(admin),
        }
    }

    async fn iam_controls(&self) {
        object_ok(
            self.runtime
                .put_object("iam-runtime-deny", b"owned-runtime-sentinel")
                .await,
        );
        let denied = self.runtime.delete_object("iam-runtime-deny").await;
        let denied = denied.expect_err("runtime DELETE must be denied by actual RustFS IAM");
        let service = denied
            .downcast_ref::<SdkError<DeleteObjectError>>()
            .and_then(|error| match error {
                SdkError::ServiceError(service) => Some(service),
                _ => None,
            })
            .expect("runtime DELETE must fail with structured S3 service error");
        assert_eq!(service.raw().status().as_u16(), 403);
        assert!(service.err().meta().code() == Some("AccessDenied"));
        assert_eq!(
            object_ok(self.runtime.get_object("iam-runtime-deny").await).unwrap(),
            b"owned-runtime-sentinel"
        );
        let denied = self
            .admin
            .put_object("iam-admin-put-deny", b"must-not-exist")
            .await;
        let denied = denied.expect_err("admin PUT must be denied by actual RustFS IAM");
        let service = denied
            .downcast_ref::<SdkError<PutObjectError>>()
            .and_then(|error| match error {
                SdkError::ServiceError(service) => Some(service),
                _ => None,
            })
            .expect("admin PUT must fail with structured S3 service error");
        assert_eq!(service.raw().status().as_u16(), 403);
        assert!(service.err().meta().code() == Some("AccessDenied"));
        assert!(
            object_ok(
                self.runtime
                    .get_object_size_bounded("iam-admin-put-deny")
                    .await
            )
            .is_none()
        );
        assert!(self.admin.deletes.lock().await.is_empty());
    }

    async fn block_store(&self) -> (ObjectBlockStore<ActualObjects>, tempfile::TempDir) {
        let scratch = tempfile::tempdir().unwrap();
        let blocks = object_ok(
            ObjectBlockStore::new_with_configs_async(
                ObjectClient::new(self.runtime.clone()),
                ChunksCacheConfig::with_budgets(0, 0, scratch.path().join("chunks")),
                BlockStoreConfig {
                    block_size: BLOCK_BYTES as usize,
                    page_cache_capacity: 0,
                    range_background_prefetch: false,
                    populate_write_cache_after_upload: false,
                    persist_write_cache_after_upload: false,
                    ..Default::default()
                },
            )
            .await,
        );
        (blocks, scratch)
    }

    async fn seed(&self, count: u32) {
        let (writer, _scratch) = self.block_store().await;
        for block in 0..count {
            let payload = vec![17 + block as u8; BLOCK_BYTES as usize];
            assert_eq!(
                object_ok(writer.write_fresh_range((SLICE, block), 0, &payload).await),
                u64::from(BLOCK_BYTES)
            );
            assert!(
                object_ok(
                    self.runtime
                        .get_object_size_bounded(&format!("chunks-v2/{SLICE}/{block}"))
                        .await
                )
                .is_some()
            );
        }
    }

    async fn assert_present(&self) {
        // A new block store has no write cache, format cache, or page-cache evidence.
        let (reader, _scratch) = self.block_store().await;
        let mut bytes = vec![0; BLOCK_BYTES as usize];
        object_ok(reader.read_range((SLICE, 0), 0, &mut bytes).await);
        assert_eq!(bytes, vec![17; BLOCK_BYTES as usize]);
    }

    async fn assert_absent(&self, count: u32) {
        for block in 0..count {
            for family in ["chunks-v2", "chunks"] {
                assert!(
                    object_ok(
                        self.runtime
                            .get_object_size_bounded(&format!("{family}/{SLICE}/{block}"))
                            .await
                    )
                    .is_none(),
                    "fresh actual RustFS HEAD must show deleted key absent"
                );
            }
        }
    }

    async fn assert_deletes(&self, expected: usize) {
        let keys = self.admin.deletes.lock().await;
        assert_eq!(keys.len(), expected);
        assert_eq!(
            self.admin.successful_deletes.load(Ordering::SeqCst),
            expected
        );
        assert_eq!(
            keys.iter().collect::<std::collections::BTreeSet<_>>().len(),
            expected,
            "no physical adapter DELETE may be replayed"
        );
    }

    async fn cleanup(&self, count: u32) -> anyhow::Result<()> {
        // Cleanup is outside the observed collector. Only exact fixture keys in
        // this fresh UUID prefix are named; the runner also removes owned tmpfs.
        let mut keys = vec![
            "iam-runtime-deny".to_owned(),
            "iam-admin-put-deny".to_owned(),
        ];
        for block in 0..count {
            keys.push(format!("chunks-v2/{SLICE}/{block}"));
            keys.push(format!("chunks/{SLICE}/{block}"));
        }
        for key in keys {
            let key = self.admin.key(&key);
            if self
                .admin
                .inner
                .get_object_size_bounded(&key)
                .await?
                .is_some()
            {
                self.admin.inner.delete_object(&key).await?;
            }
            anyhow::ensure!(
                self.admin
                    .inner
                    .get_object_size_bounded(&key)
                    .await?
                    .is_none(),
                "owned object cleanup absence proof failed"
            );
        }
        Ok(())
    }
}

async fn orphan<B: WorkspaceKvBackend>(
    store: &KvWorkspaceStore<B>,
    layer: LayerId,
    sid: u64,
    end: u64,
) {
    store
        .record_orphan_slice(RecordOrphanSlice {
            orphan_layer_id: layer,
            slice_id: sid,
            slice_end: end,
        })
        .await
        .unwrap();
}

async fn tick<B: WorkspaceKvBackend>(
    store: Arc<KvWorkspaceStore<B>>,
    budget: Arc<V3MountBudget>,
    objects: &Objects,
    target: LayerId,
) -> Result<u64, WorkspaceError> {
    collect_one(
        store,
        ObjectClient::new(objects.admin.clone()),
        budget,
        ChunkLayout {
            chunk_size: 4 << 20,
            block_size: BLOCK_BYTES,
        },
        PackedGcPolicy {
            lease_ttl_seconds: 30,
            grace_seconds: 0,
            max_scans: 1,
            max_operations: 1,
            max_protective_rows: 64,
        },
        CancellationToken::new(),
        target,
    )
    .await
}

async fn warm_metadata_clients<B: WorkspaceKvBackend>(backend: &B, budget: &Arc<V3MountBudget>) {
    // TiKV retains a separately owned SDK client for each bounded wire class.
    // Warm both independent backends before measuring transient case owners.
    // These are genuine bounded absence reads, with no mutation or scheduler CAS.
    let key = b"packed/v3/native-sid-test-client-warmup".to_vec();
    for (value_bytes, response_bytes) in [
        (4 << 10, 8 << 10),
        (12 << 10, 16 << 10),
        (48 << 10, 64 << 10),
        (96 << 10, 128 << 10),
    ] {
        let _read_owner = budget
            .admit(&[(
                crate::workspace_overlay::packed_v3::wire005::V3BudgetPool::Metadata,
                2 << 20,
            )])
            .unwrap();
        let (values, now) = backend
            .get_many_consistent_with_time_bounded(
                std::slice::from_ref(&key),
                KvReadLimits {
                    max_records: 1,
                    max_key_bytes: 256,
                    max_value_bytes: value_bytes,
                    max_total_bytes: value_bytes + 256,
                    max_response_bytes: response_bytes,
                    max_data_requests: 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(values, vec![None]);
        assert!(now > 0);
    }
}

async fn run_case<B: WorkspaceKvBackend + Clone>(
    a: B,
    b: B,
    budget: Arc<V3MountBudget>,
    objects: Objects,
    jobs: Jobs,
    case: Case,
) {
    let collector_scheduled = Scheduled::new(a, false);
    let backend = Arc::new(collector_scheduled.clone());
    let store = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    let scheduled = Scheduled::new(b, true);
    let publisher = Arc::new(
        KvWorkspaceStore::new(scheduled.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    store.initialize_workspace_schema().await.unwrap();
    let request = CreateVolumeRoot {
        upper_bucket: None,
        volume_format: VOLUME_FORMAT.into(),
        schema_version: WORKSPACE_SCHEMA_VERSION,
        volume_id: Uuid::new_v4(),
        workspace_id: WorkspaceId::new(),
        root_layer_id: LayerId::new(),
        writable_layer_id: LayerId::new(),
        owner_id: None,
    };
    store.create_volume_root(request.clone()).await.unwrap();
    let workspace = store.load_workspace(request.workspace_id).await.unwrap();
    let target = LayerId::new();
    let end = u64::from(case.blocks()) * u64::from(BLOCK_BYTES);
    orphan(&store, target, SLICE, end).await;
    objects.iam_controls().await;
    objects.seed(case.blocks()).await;
    // Keep prewarming inside the cleanup-managed task, including its failures.
    warm_metadata_clients(backend.as_ref(), &budget).await;
    warm_metadata_clients(&scheduled, &budget).await;
    if backend.name() == "workspace-tikv" {
        // Two main clients plus all four bounded clients per independent backend.
        assert_eq!(
            budget.state().used,
            [10 * (64 << 10), 10 * (4 << 20), 0, 0, 0, 0, 0, 0],
            "both TiKV backends must retain exactly ten SDK resident owners"
        );
    }
    let baseline = budget.state().used;
    match case {
        Case::PublicationWins | Case::ReservationWins => {
            let lease_id = LeaseId::new();
            store
                .acquire_lease(AcquireLease {
                    workspace_id: request.workspace_id,
                    lease_id,
                    holder_generation: 1,
                    ttl_ns: 300_000_000_000,
                })
                .await
                .unwrap();
            let append = AppendDataExtent {
                guard: HeadGuard {
                    workspace_id: request.workspace_id,
                    expected_head_layer_id: workspace.head_layer_id,
                    expected_head_epoch: workspace.head_epoch,
                    lease_id,
                    holder_generation: 1,
                },
                extent: DataExtentDelta::data(
                    workspace.head_layer_id,
                    2,
                    0,
                    0,
                    u64::from(BLOCK_BYTES),
                    SLICE,
                    0,
                    1,
                ),
                chunk_size: 4 << 20,
            };
            if matches!(case, Case::PublicationWins) {
                // Prepare the complete old deletion proof before publication.
                // The real reservation CAS must then reject that old basis.
                let pause = collector_scheduled.pause_next_reservation().await;
                let reserve_store = store.clone();
                let mut reservation = jobs.spawn(async move {
                    reserve_store
                        .reserve_gc_slice_deletion(SLICE, end, &[target])
                        .await
                });
                pause.wait_entered().await;
                publisher.append_data_extent(append).await.unwrap();
                assert_eq!(scheduled.commits.load(Ordering::SeqCst), 1);
                pause.resume();
                assert!(matches!(
                    reservation.finish().await,
                    Err(WorkspaceError::Busy)
                ));
                assert_eq!(
                    collector_scheduled
                        .reservation_conflicts
                        .load(Ordering::SeqCst),
                    1
                );
                assert_eq!(
                    tick(store.clone(), budget.clone(), &objects, target)
                        .await
                        .unwrap(),
                    1
                );
                objects.assert_deletes(0).await;
                objects.assert_present().await;
                assert!(
                    backend
                        .get(&slice_deletion_key(SLICE))
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(backend.get(&hot_layer_key(target)).await.unwrap().is_none());
            } else {
                let before = store.load_layer(workspace.head_layer_id).await.unwrap();
                let pause = scheduled.pause_next_append().await;
                let mut task =
                    jobs.spawn(async move { publisher.append_data_extent(append).await });
                pause.wait_entered().await;
                store
                    .reserve_gc_slice_deletion(SLICE, end, &[target])
                    .await
                    .unwrap();
                let reservation = backend
                    .get(&slice_deletion_key(SLICE))
                    .await
                    .unwrap()
                    .unwrap();
                pause.resume();
                assert!(matches!(task.finish().await, Err(WorkspaceError::Busy)));
                assert_eq!(scheduled.attempts.load(Ordering::SeqCst), 1);
                assert_eq!(scheduled.conflicts.load(Ordering::SeqCst), 1);
                assert_eq!(scheduled.commits.load(Ordering::SeqCst), 0);
                assert_eq!(
                    store.load_layer(workspace.head_layer_id).await.unwrap(),
                    before
                );
                assert!(
                    backend
                        .scan_prefix(&extent_layer_prefix(workspace.head_layer_id))
                        .await
                        .unwrap()
                        .is_empty()
                );
                objects
                    .admin
                    .lose_delete_reply
                    .store(true, Ordering::SeqCst);
                assert!(
                    tick(store.clone(), budget.clone(), &objects, target)
                        .await
                        .is_err()
                );
                objects.assert_deletes(1).await;
                objects.assert_absent(1).await;
                let dispatched = backend
                    .get(&block_delete_key((SLICE, 0)))
                    .await
                    .unwrap()
                    .unwrap();
                let row: BlockDelete = decode_open_value(&dispatched, 512).unwrap();
                assert!(row.state == BlockDeleteState::Dispatched);
                assert!(
                    tick(store.clone(), budget.clone(), &objects, target)
                        .await
                        .is_err()
                );
                objects.assert_deletes(1).await;
                assert_eq!(
                    backend
                        .get(&block_delete_key((SLICE, 0)))
                        .await
                        .unwrap()
                        .unwrap(),
                    dispatched
                );
                assert_eq!(
                    backend
                        .get(&slice_deletion_key(SLICE))
                        .await
                        .unwrap()
                        .unwrap(),
                    reservation
                );
                assert!(
                    backend
                        .get(&range_progress_key((SLICE, 0), 1))
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(backend.get(&hot_layer_key(target)).await.unwrap().is_some());
                assert_eq!(
                    store.load_layer(target).await.unwrap().state,
                    LayerState::Deleting
                );
            }
        }
        Case::SixtyFiveBlockRecovery => {
            assert!(
                tick(store.clone(), budget.clone(), &objects, target)
                    .await
                    .is_err()
            );
            objects.assert_deletes(64).await;
            objects.assert_absent(32).await;
            assert!(
                object_ok(
                    objects
                        .runtime
                        .get_object_size_bounded(&format!("chunks-v2/{SLICE}/32"))
                        .await
                )
                .is_some()
            );
            let reservation = backend
                .get(&slice_deletion_key(SLICE))
                .await
                .unwrap()
                .unwrap();
            let identity = backend
                .get(&native_reverse::state_key(target))
                .await
                .unwrap()
                .unwrap();
            let before_birth: u64 = decode(
                &backend
                    .get(LAYER_INVENTORY_GENERATION_KEY)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            let unrelated = LayerId::new();
            orphan(&store, unrelated, SLICE + 1, u64::from(BLOCK_BYTES)).await;
            let after_birth: u64 = decode(
                &backend
                    .get(LAYER_INVENTORY_GENERATION_KEY)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert!(
                after_birth > before_birth,
                "unrelated birth must advance actual inventory"
            );
            assert!(
                backend
                    .get(&hot_layer_key(unrelated))
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(
                tick(store.clone(), budget.clone(), &objects, target)
                    .await
                    .is_err()
            );
            objects.assert_deletes(128).await;
            objects.assert_absent(64).await;
            assert!(
                object_ok(
                    objects
                        .runtime
                        .get_object_size_bounded(&format!("chunks-v2/{SLICE}/64"))
                        .await
                )
                .is_some()
            );
            assert_eq!(
                backend
                    .get(&slice_deletion_key(SLICE))
                    .await
                    .unwrap()
                    .unwrap(),
                reservation
            );
            assert_eq!(
                backend
                    .get(&native_reverse::state_key(target))
                    .await
                    .unwrap()
                    .unwrap(),
                identity
            );
            store
                .finalize_layer_metadata_deletion(vec![unrelated])
                .await
                .unwrap();
            let after_death: u64 = decode(
                &backend
                    .get(LAYER_INVENTORY_GENERATION_KEY)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert!(
                after_death > after_birth,
                "unrelated death must advance actual inventory"
            );
            assert!(
                backend
                    .get(&hot_layer_key(unrelated))
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                backend
                    .get(&slice_deletion_key(SLICE))
                    .await
                    .unwrap()
                    .unwrap(),
                reservation
            );
            assert_eq!(
                backend
                    .get(&native_reverse::state_key(target))
                    .await
                    .unwrap()
                    .unwrap(),
                identity
            );
            assert_eq!(
                tick(store.clone(), budget.clone(), &objects, target)
                    .await
                    .unwrap(),
                1
            );
            objects.assert_deletes(130).await;
            objects.assert_absent(65).await;
            assert!(backend.get(&hot_layer_key(target)).await.unwrap().is_none());
            assert!(
                backend
                    .get(&native_reverse::state_key(target))
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                backend
                    .get(&slice_deletion_key(SLICE))
                    .await
                    .unwrap()
                    .unwrap(),
                reservation
            );
            let progress: RangeProgress = decode_open_value(
                &backend
                    .get(&range_progress_key((SLICE, 0), 65))
                    .await
                    .unwrap()
                    .unwrap(),
                512,
            )
            .unwrap();
            assert_eq!(progress.next, 65);
        }
    }
    assert_eq!(budget.state().used, baseline);
    eprintln!(
        "actual-sid-rustfs metadata_backend={} object_backend=rustfs case={} iam_controls=passed delegated_deletes={}",
        backend.name(),
        case.label(),
        objects.admin.deletes.lock().await.len()
    );
    let keys = objects.admin.deletes.lock().await;
    eprintln!(
        "actual-sid-rustfs-evidence {}",
        serde_json::json!({
            "metadata_backend": backend.name(), "object_backend": "rustfs", "case": case.label(),
            "object_prefix": objects.admin.prefix, "delegated_delete_keys": *keys,
            "delegated_deletes": keys.len(), "successful_deletes": objects.admin.successful_deletes.load(Ordering::SeqCst),
            "runtime_delete_status": 403, "runtime_delete_code": "AccessDenied",
            "admin_put_status": 403, "admin_put_code": "AccessDenied",
            "unknown_delete_quarantined": matches!(case, Case::ReservationWins),
        })
    );
}

async fn cleanup_namespace<B: WorkspaceKvBackend>(backend: &B) -> anyhow::Result<()> {
    for _ in 0..64 {
        let rows = backend.scan_prefix_bounded(b"", 128).await?;
        if rows.is_empty() {
            return Ok(());
        }
        let checks = rows
            .iter()
            .map(|row| KvCheck {
                key: row.key.clone(),
                expected: Some(row.value.clone()),
            })
            .collect::<Vec<_>>();
        let writes = rows
            .into_iter()
            .map(|row| KvWrite::Delete { key: row.key })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            backend.compare_and_swap(&checks, &writes).await?,
            "case tasks modified namespace during exact cleanup"
        );
    }
    anyhow::bail!("fresh metadata namespace cleanup exceeded fixed key budget")
}

async fn run_and_cleanup<B: WorkspaceKvBackend + Clone>(
    a: B,
    b: B,
    budget: Arc<V3MountBudget>,
    objects: Objects,
    case: Case,
) -> Option<bool> {
    let jobs = Jobs::default();
    let case_jobs = jobs.clone();
    let case_a = a.clone();
    let case_b = b.clone();
    let case_budget = budget.clone();
    let case_objects = objects.clone();
    let mut task = tokio::spawn(async move {
        run_case(case_a, case_b, case_budget, case_objects, case_jobs, case).await
    });
    let (result, timed_out) = match tokio::time::timeout(CASE_TIMEOUT, &mut task).await {
        Ok(result) => (result, false),
        Err(_) => {
            task.abort();
            (task.await, true)
        }
    };
    // No detached publisher may race namespace/object cleanup, even after panic.
    let stopped = jobs.stop_and_wait().await;
    if stopped.is_err() {
        // No cleanup is authorized while a task may still be live.
        let _ = a.shutdown_metadata_backend().await;
        let _ = b.shutdown_metadata_backend().await;
        return None;
    }
    let metadata_cleanup = cleanup_namespace(&a).await;
    let object_cleanup = objects.cleanup(case.blocks()).await;
    let shutdown_a = a.shutdown_metadata_backend().await;
    let shutdown_b = b.shutdown_metadata_backend().await;
    assert_eq!(
        budget.state().used,
        [0; 8],
        "all SID operation and SDK resident owners must drain after both backend shutdowns"
    );
    Some(
        !timed_out
            && result.is_ok()
            && metadata_cleanup.is_ok()
            && object_cleanup.is_ok()
            && shutdown_a.is_ok()
            && shutdown_b.is_ok(),
    )
}

async fn cleanup_redis_index(url: &str, namespace: &str) {
    let prefix = format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/");
    let keys = [
        format!("{prefix}__index/keys"),
        format!("{prefix}__index/ready"),
    ];
    let client = redis::Client::open(url).expect("owned Redis cleanup client");
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("owned Redis cleanup connection");
    redis::cmd("DEL")
        .arg(&keys)
        .query_async::<usize>(&mut connection)
        .await
        .expect("exact owned Redis index cleanup");
    let remaining: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
        .arg(&keys)
        .query_async(&mut connection)
        .await
        .expect("exact Redis index absence read");
    assert!(remaining.iter().all(Option::is_none));
}

async fn run_redis(case: Case) {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("owned Redis URL required");
    let namespace = format!("sid-real-{}", Uuid::new_v4().simple());
    let budget = V3MountBudget::defaults();
    let objects = Objects::connect().await;
    let a = RedisWorkspaceBackend::connect(&url, &namespace)
        .await
        .expect("actual Redis client A");
    let b = RedisWorkspaceBackend::connect(&url, &namespace)
        .await
        .expect("actual Redis client B");
    let passed = run_and_cleanup(a, b, budget, objects, case).await;
    if passed.is_some() {
        cleanup_redis_index(&url, &namespace).await;
    }
    assert!(
        passed == Some(true),
        "actual Redis/RustFS SID case {case:?} or owned cleanup failed"
    );
}

async fn run_tikv(case: Case) {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .expect("owned TiKV PD endpoints required")
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let namespace = format!("sid-real-{}", Uuid::new_v4().simple());
    let budget = V3MountBudget::defaults();
    let objects = Objects::connect().await;
    let a =
        TiKvWorkspaceBackend::connect_with_budget(endpoints.clone(), &namespace, budget.clone())
            .await
            .expect("actual TiKV client A");
    let b = TiKvWorkspaceBackend::connect_with_budget(endpoints, &namespace, budget.clone())
        .await
        .expect("actual TiKV client B");
    assert!(
        run_and_cleanup(a, b, budget, objects, case).await == Some(true),
        "actual TiKV/RustFS SID case {case:?} or owned cleanup failed"
    );
}

#[tokio::test]
#[ignore = "owned runner only: actual Redis plus runtime/admin RustFS IAM credentials"]
async fn real_redis_rustfs_sid_publication_wins() {
    run_redis(Case::PublicationWins).await;
}

#[tokio::test]
#[ignore = "owned runner only: actual TiKV plus runtime/admin RustFS IAM credentials"]
async fn real_tikv_rustfs_sid_publication_wins() {
    run_tikv(Case::PublicationWins).await;
}

#[tokio::test]
#[ignore = "owned runner only: actual Redis plus runtime/admin RustFS IAM credentials"]
async fn real_redis_rustfs_sid_reservation_wins() {
    run_redis(Case::ReservationWins).await;
}

#[tokio::test]
#[ignore = "owned runner only: actual TiKV plus runtime/admin RustFS IAM credentials"]
async fn real_tikv_rustfs_sid_reservation_wins() {
    run_tikv(Case::ReservationWins).await;
}

#[tokio::test]
#[ignore = "owned runner only: actual Redis plus runtime/admin RustFS IAM credentials"]
async fn real_redis_rustfs_sid_sixty_five_block_recovery() {
    run_redis(Case::SixtyFiveBlockRecovery).await;
}

#[tokio::test]
#[ignore = "owned runner only: actual TiKV plus runtime/admin RustFS IAM credentials"]
async fn real_tikv_rustfs_sid_sixty_five_block_recovery() {
    run_tikv(Case::SixtyFiveBlockRecovery).await;
}
