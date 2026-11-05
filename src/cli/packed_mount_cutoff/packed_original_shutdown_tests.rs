//! Actual original mounted VFS, Redis/TiKV metadata and selected object bytes.
//! Intended placement: cli/packed_mount_cutoff/packed_original_shutdown_tests.rs.
//! These ignored tests require real Linux FUSE. They do not simulate a cutoff,
//! mint drain/status authorities or claim K8s end-to-end coverage. LocalFS is
//! the default; the owned RustFS runner explicitly selects the real S3 adapter.

#[path = "packed_headless_route_tests.rs"]
mod packed_headless_route_tests;
#[path = "packed_headless_snapshot_tests.rs"]
mod packed_headless_snapshot_tests;

use super::*;
use crate::cadapter::client::{ObjectBackend, ObjectByteStream, ObjectClient};
use crate::cadapter::localfs::LocalFsBackend;
use crate::cadapter::s3::{S3Backend, S3Config};
use crate::chunk::ChunkLayout;
use crate::chunk::cache::ChunksCacheConfig;
use crate::chunk::compress::Compression;
use crate::chunk::store::{BlockStoreConfig, ObjectBlockStore};
use crate::fuse::mount::{FuseConcurrencyConfig, mount_vfs_privileged, mount_vfs_unprivileged};
use crate::meta::layer::MetaLayer;
use crate::vfs::config::VFSConfig;
use crate::vfs::fs::VFS;
use crate::workspace_overlay::catalog::{ReleaseLease, WorkspaceStore};
use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::ids::{JournalId, LayerId, LeaseId, SnapshotId, WorkspaceId};
use crate::workspace_overlay::meta_layer::{
    PinnedCatalogPackedBindingAuthority, WorkspaceMetaLayer,
};
use crate::workspace_overlay::model::{
    LayerRecord, LeaseState, SnapshotLease, ViewContext, VolumeHeader, WorkspaceRecord,
};
use crate::workspace_overlay::packed_reader_lifecycle::PackedReaderLeaseOptions;
use crate::workspace_overlay::packed_shutdown::VerifiedCleanPackedShutdown;
use crate::workspace_overlay::packed_v3::PackedV3ReadonlyMeta;
use crate::workspace_overlay::packed_v3::wire005::{
    AuthenticatedV3Snapshot, V3BudgetPool, V3IndexReader,
};
use crate::workspace_overlay::publish::binding::VerifiedPackedLower;
use crate::workspace_overlay::stores::binding_tests::{packed, request};
use crate::workspace_overlay::stores::kv_backend::{
    KvCheck, KvEntry, KvReadLimits, KvWrite, WorkspaceKvBackend,
};
use crate::workspace_overlay::stores::kv_store::packed_admin::{
    PackedCleanAdmission, PackedMountGrantRequest, PackedReleasedMountReference,
};
use crate::workspace_overlay::stores::kv_store::packed_reader_pins::PackedReaderPinState;
use crate::workspace_overlay::stores::kv_store::{KvWorkspaceStore, V3OpenState};
use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
use crate::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify, Semaphore};
use uuid::Uuid;

const RECEIPT_PREFIX: &[u8] = b"packed-v3/clean-release/";
const ROOT_GENERATION: &[u8] = b"packed/v3/root-generation";
type Connection<B> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<B, WorkspaceError>> + Send>>;
const WAIT: Duration = Duration::from_secs(30);
const PAYLOAD: &[u8] = b"actual original mounted packed-v3 LocalFS durable payload";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Case {
    Positive,
    ReleaseErrorBefore,
    ReleaseLostReply,
    CancelReleaseWaiter,
    CancelOriginalShutdownWaiter,
    OriginalReaderShutdownError,
    LaterActualLeaseGrant,
    LaterActualOpenGrant,
    LostReplyThenActualGrant,
    HeadlessSnapshot,
    HeadlessRouteContracts,
}

#[derive(Clone)]
struct ReleasePacket {
    checks: Vec<KvCheck>,
    writes: Vec<KvWrite>,
    deadline: i64,
}

struct DeliveryBackend<B> {
    inner: Arc<B>,
    budget: Arc<V3MountBudget>,
    mode: AtomicU8,
    attempts: AtomicUsize,
    commits: AtomicUsize,
    confirmation_reads: AtomicUsize,
    confirmation_cas: AtomicUsize,
    packet: Mutex<Option<ReleasePacket>>,
    later_grant: Mutex<Option<SnapshotLease>>,
    entered: Notify,
    release: Semaphore,
    terminal: AtomicBool,
    terminal_notify: Notify,
}

impl<B: WorkspaceKvBackend> DeliveryBackend<B> {
    fn new(inner: Arc<B>, budget: Arc<V3MountBudget>) -> Self {
        Self {
            inner,
            budget,
            mode: AtomicU8::new(0),
            attempts: AtomicUsize::new(0),
            commits: AtomicUsize::new(0),
            confirmation_reads: AtomicUsize::new(0),
            confirmation_cas: AtomicUsize::new(0),
            packet: Mutex::new(None),
            later_grant: Mutex::new(None),
            entered: Notify::new(),
            release: Semaphore::new(0),
            terminal: AtomicBool::new(false),
            terminal_notify: Notify::new(),
        }
    }

    fn is_release(writes: &[KvWrite]) -> bool {
        writes.iter().any(|write| {
            matches!(write, KvWrite::Put { key, value }
            if key.starts_with(RECEIPT_PREFIX) && value.starts_with(b"PCR3\x01"))
        })
    }

    fn released_lease(writes: &[KvWrite]) -> SnapshotLease {
        let leases = writes
            .iter()
            .filter_map(|write| match write {
                KvWrite::Put { key, value } if key.starts_with(b"lease/") => {
                    let lease: SnapshotLease = decode_envelope(value);
                    assert_eq!(*key, lease_key(lease.workspace_id, lease.lease_id));
                    Some(lease)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            leases.len(),
            1,
            "actual release must write one scoped lease"
        );
        assert_eq!(leases[0].state, LeaseState::Released);
        leases.into_iter().next().unwrap()
    }

    async fn exact_successor(&self) -> Vec<KvCheck> {
        let packet = self.packet.lock().await;
        let packet = packet.as_ref().expect("actual clean release packet");
        let mut checks = packet.checks.clone();
        for write in &packet.writes {
            let (key, expected) = match write {
                KvWrite::Put { key, value } => (key.clone(), Some(value.clone())),
                KvWrite::Delete { key } => (key.clone(), None),
            };
            if let Some(check) = checks.iter_mut().find(|check| check.key == key) {
                check.expected = expected;
            } else {
                checks.push(KvCheck { key, expected });
            }
        }
        checks
    }
}

#[async_trait]
impl<B: WorkspaceKvBackend> WorkspaceKvBackend for DeliveryBackend<B> {
    fn supports_consistent_reads(&self) -> bool {
        self.inner.supports_consistent_reads()
    }
    fn name(&self) -> &'static str {
        self.inner.name()
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
        if self.commits.load(Ordering::SeqCst) == 1
            && keys.iter().any(|key| key.starts_with(RECEIPT_PREFIX))
        {
            self.confirmation_reads.fetch_add(1, Ordering::SeqCst);
            let successor = self.exact_successor().await;
            assert_eq!(
                keys.len(),
                successor.len(),
                "unknown confirmation omitted actual packet conditions"
            );
            for check in successor {
                assert!(keys.contains(&check.key));
            }
        }
        self.inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await
    }
    async fn get_many_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.inner.get_many_with_time(keys).await
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner.scan_prefix(prefix).await
    }
    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        cap: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner.scan_prefix_bounded(prefix, cap).await
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
        if self.commits.load(Ordering::SeqCst) == 1
            && checks
                .iter()
                .any(|check| check.key.starts_with(RECEIPT_PREFIX))
        {
            let successor = self.exact_successor().await;
            assert_eq!(checks.len(), successor.len());
            for check in &successor {
                assert!(checks.contains(check));
            }
            assert_eq!(
                expires_at_ns,
                self.packet.lock().await.as_ref().unwrap().deadline
            );
            self.confirmation_cas.fetch_add(1, Ordering::SeqCst);
        }
        self.inner
            .authenticate_checks_before_bounded(checks, expires_at_ns, limits)
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        if self.mode.load(Ordering::SeqCst) == 5 && writes.iter().any(|write|
            matches!(write, KvWrite::Delete { key } if key.starts_with(b"packed/v3/reader-active/"))) {
            return Err(WorkspaceError::Backend("actual reader release failed before submission".into()));
        }
        self.inner.compare_and_swap(checks, writes).await
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        self.inner
            .compare_and_swap_in_time_window(checks, writes, lower, upper)
            .await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        if writes.is_empty()
            && self.commits.load(Ordering::SeqCst) == 1
            && checks
                .iter()
                .any(|check| check.key.starts_with(RECEIPT_PREFIX))
        {
            let successor = self.exact_successor().await;
            assert_eq!(checks.len(), successor.len());
            for check in &successor {
                assert!(checks.contains(check));
            }
            assert_eq!(
                deadline,
                self.packet.lock().await.as_ref().unwrap().deadline
            );
            self.confirmation_cas.fetch_add(1, Ordering::SeqCst);
        }
        if !Self::is_release(writes) {
            return self
                .inner
                .compare_and_swap_before(checks, writes, deadline)
                .await;
        }
        let released = Self::released_lease(writes);
        let workspace = workspace_key(released.workspace_id);
        let prior_workspace: WorkspaceRecord = decode_envelope(
            checks
                .iter()
                .find(|check| check.key == workspace)
                .and_then(|check| check.expected.as_deref())
                .expect("actual release checked the workspace pointer"),
        );
        assert_eq!(prior_workspace.active_lease, Some(released.lease_id));
        let successor_workspace: WorkspaceRecord = decode_envelope(
            writes
                .iter()
                .find_map(|write| match write {
                    KvWrite::Put { key, value } if *key == workspace => Some(value.as_slice()),
                    _ => None,
                })
                .expect("actual release cleared the workspace pointer in the same CAS"),
        );
        let mut expected_workspace = prior_workspace;
        expected_workspace.active_lease = None;
        expected_workspace.updated_at_ns = released.updated_at_ns;
        assert_eq!(successor_workspace, expected_workspace);
        assert!(checks.iter().any(|check| check.key == b"control"));
        assert!(
            checks
                .iter()
                .any(|check| check.key == b"packed/v3/topology-generation")
        );
        for write in writes {
            let key = match write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
            };
            assert_eq!(
                checks.iter().filter(|check| check.key == *key).count(),
                1,
                "actual release write lacks one exact predecessor condition"
            );
        }
        self.attempts.fetch_add(1, Ordering::SeqCst);
        *self.packet.lock().await = Some(ReleasePacket {
            checks: checks.to_vec(),
            writes: writes.to_vec(),
            deadline,
        });
        let mode = self.mode.load(Ordering::SeqCst);
        if mode == 1 {
            self.terminal.store(true, Ordering::SeqCst);
            self.terminal_notify.notify_one();
            return Err(WorkspaceError::Backend(
                "actual clean release rejected before submission".into(),
            ));
        }
        if mode == 3 {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        let result = self
            .inner
            .compare_and_swap_before(checks, writes, deadline)
            .await?;
        if result {
            self.commits.fetch_add(1, Ordering::SeqCst);
        }
        if result && mode == 4 {
            // A real production grant interleaves after commit and before the
            // unknown response. It changes the workspace pointer, lease/index,
            // native hold and topology/root authority, without rewriting CONTROL.
            let admin = Arc::new(
                KvWorkspaceStore::from_arc(self.inner.clone())
                    .with_packed_reader_pin_budget(self.budget.clone()),
            );
            let next = admin
                .grant_packed_mounted_session(PackedMountGrantRequest {
                    workspace_id: released.workspace_id,
                    lease_id: LeaseId::new(),
                    holder_generation: released.holder_generation + 1,
                    mount_uid: Uuid::new_v4(),
                    pod_uid: Uuid::new_v4(),
                    ttl_ns: 300_000_000_000,
                })
                .await
                .unwrap();
            *self.later_grant.lock().await = Some(next.lease());
        }
        self.terminal.store(true, Ordering::SeqCst);
        self.terminal_notify.notify_one();
        if result && (mode == 2 || mode == 4) {
            return Err(WorkspaceError::Backend(
                "actual committed clean release reply lost".into(),
            ));
        }
        Ok(result)
    }
}

#[derive(Default)]
pub(crate) struct ObjectProbe {
    puts: AtomicUsize,
    keys: Mutex<std::collections::BTreeSet<String>>,
    hold_next_range: AtomicBool,
    entered: Notify,
    resume: Notify,
    terminal: AtomicBool,
}

#[derive(Clone)]
pub(crate) struct ActualObjects {
    inner: Arc<dyn ObjectBackend>,
    prefix: String,
    probe: Arc<ObjectProbe>,
}

impl ActualObjects {
    fn key(&self, key: &str) -> String {
        assert!(!key.is_empty() && key.len() <= 1024 && !key.starts_with('/'));
        assert!(key.split('/').all(|part| part != ".." && part != "."));
        format!("{}{key}", self.prefix)
    }

    async fn record(&self, key: &str) {
        let mut keys = self.probe.keys.lock().await;
        assert!(
            keys.contains(key) || keys.len() < 4096,
            "small fixture key bound"
        );
        keys.insert(key.to_owned());
        self.probe.puts.fetch_add(1, Ordering::SeqCst);
    }
}

async fn selected_objects(
    root: &std::path::Path,
    probe: Arc<ObjectProbe>,
) -> ObjectClient<ActualObjects> {
    select_objects(root, probe, None).await
}

pub(crate) async fn selected_cli_objects(
    root: &std::path::Path,
    probe: Arc<ObjectProbe>,
    case: &str,
) -> ObjectClient<ActualObjects> {
    assert!(matches!(case, "rc" | "tc" | "rk" | "tk" | "rp" | "tp"));
    select_objects(root, probe, Some(case)).await
}

async fn select_objects(
    root: &std::path::Path,
    probe: Arc<ObjectProbe>,
    cli_case: Option<&str>,
) -> ObjectClient<ActualObjects> {
    let mode = match std::env::var("BREWFS_TEST_ORIGINAL_OBJECT_BACKEND") {
        Ok(mode) => mode,
        Err(std::env::VarError::NotPresent) => "localfs".into(),
        Err(_) => panic!("invalid original-chain object backend selection"),
    };
    let (inner, prefix): (Arc<dyn ObjectBackend>, String) = match mode.as_str() {
        "localfs" => (Arc::new(LocalFsBackend::new(root)), String::new()),
        "rustfs" => {
            let required = |name| std::env::var(name).expect("explicit owned RustFS configuration");
            let endpoint = required("BREWFS_TEST_ORIGINAL_RUSTFS_ENDPOINT");
            let port = endpoint
                .strip_prefix("http://127.0.0.1:")
                .and_then(|port| port.parse::<u16>().ok())
                .expect("owned RustFS endpoint must be HTTP IPv4 loopback and a valid port");
            assert_ne!(port, 0);
            let owner = required("BREWFS_TEST_ORIGINAL_RUSTFS_OWNER");
            let owner_id = Uuid::parse_str(&owner).expect("owned RustFS run UUID");
            assert_eq!(owner_id.simple().to_string(), owner);
            let bucket = required("BREWFS_TEST_ORIGINAL_RUSTFS_BUCKET");
            let expected_bucket = match cli_case {
                None => format!("brewfs-v3-lifecycle-{owner}"),
                Some(case) => format!("brewfs-v3-lifecycle-{owner}-cli-{case}"),
            };
            assert_eq!(bucket, expected_bucket);
            let backend = S3Backend::with_static_credentials(
                S3Config {
                    bucket,
                    endpoint: Some(endpoint),
                    region: Some("us-east-1".into()),
                    force_path_style: true,
                    max_concurrency: 2,
                    max_retries: 1,
                    ..Default::default()
                },
                required("BREWFS_TEST_ORIGINAL_RUSTFS_ACCESS_KEY"),
                required("BREWFS_TEST_ORIGINAL_RUSTFS_SECRET_KEY"),
            )
            .await
            .expect("explicit owned RustFS static runtime credentials");
            (
                Arc::new(backend),
                if cli_case.is_some() {
                    String::new()
                } else {
                    format!("original-chain/{}/", Uuid::new_v4().simple())
                },
            )
        }
        _ => panic!("unsupported original-chain object backend"),
    };
    eprintln!("actual-original-chain object_backend={mode}");
    let client = ObjectClient::new(ActualObjects {
        inner,
        prefix,
        probe: probe.clone(),
    });
    // LocalFS is only a fixture producer. On RustFS every immutable fixture
    // object is uploaded before its manifest and lower proof are reauthenticated
    // through the selected client. All subsequent object I/O uses that client.
    let mut pending = vec![root.to_path_buf()];
    let mut total = 0u64;
    let mut count = 0usize;
    let mut visits = 0usize;
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            visits += 1;
            assert!(visits <= 8192, "small fixture inode visit bound");
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                pending.push(entry.path());
                assert!(pending.len() <= 4096);
                continue;
            }
            assert!(kind.is_file(), "unexpected fixture inode kind");
            let length = entry.metadata().unwrap().len();
            assert!(length <= 1 << 20, "small fixture single object bound");
            total = total.checked_add(length).unwrap();
            count += 1;
            assert!(
                total <= 8 << 20 && count <= 4096,
                "small fixture upload bound"
            );
            let path = entry.path();
            let key = path.strip_prefix(root).unwrap().to_str().unwrap();
            if mode == "rustfs" {
                client
                    .put_object_create_only(key, &std::fs::read(&path).unwrap())
                    .await
                    .unwrap();
            } else {
                probe.keys.lock().await.insert(key.to_owned());
            }
        }
    }
    assert!(count > 0);
    // The original VFS's positive PUT assertion must exclude fixture uploads.
    probe.puts.store(0, Ordering::SeqCst);
    client
}

struct ReleaseBoundaries<B> {
    delivery: Arc<DeliveryBackend<B>>,
    objects: Arc<ObjectProbe>,
}
impl<B> Drop for ReleaseBoundaries<B> {
    fn drop(&mut self) {
        self.delivery.release.add_permits(1);
        self.objects.resume.notify_one();
    }
}

#[async_trait]
impl ObjectBackend for ActualObjects {
    fn forbids_mutation_replay(&self) -> bool {
        self.inner.forbids_mutation_replay()
    }
    async fn put_object(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object(&self.key(key), bytes).await?;
        self.record(key).await;
        Ok(())
    }
    async fn put_object_vectored(
        &self,
        key: &str,
        chunks: Vec<bytes::Bytes>,
    ) -> anyhow::Result<()> {
        self.inner
            .put_object_vectored(&self.key(key), chunks)
            .await?;
        self.record(key).await;
        Ok(())
    }
    async fn put_object_create_only(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner
            .put_object_create_only(&self.key(key), bytes)
            .await?;
        self.record(key).await;
        Ok(())
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(&self.key(key)).await
    }
    async fn get_object_stream(&self, key: &str) -> anyhow::Result<Option<ObjectByteStream>> {
        self.inner.get_object_stream(&self.key(key)).await
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
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        if self.probe.hold_next_range.swap(false, Ordering::SeqCst) {
            self.probe.entered.notify_one();
            self.probe.resume.notified().await;
            let result = self
                .inner
                .get_object_range_stream(&self.key(key), offset, length)
                .await;
            self.probe.terminal.store(true, Ordering::SeqCst);
            return result;
        }
        self.inner
            .get_object_range_stream(&self.key(key), offset, length)
            .await
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(&self.key(key)).await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(&self.key(key)).await
    }
    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete_object(&self.key(key)).await
    }
}

fn decode_envelope<T: serde::de::DeserializeOwned>(raw: &[u8]) -> T {
    bincode::deserialize(
        raw.strip_prefix(b"BWSKV001")
            .expect("actual workspace envelope"),
    )
    .unwrap()
}

// Current small CONTROL header; all directory and ownership facts are entities.
// Observations cannot construct a source or drain authority.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct ControlSnapshot {
    schema_version: u32,
    header: Option<VolumeHeader>,
    catalog_format: u32,
}

fn decode_control(raw: &[u8]) -> ControlSnapshot {
    bincode::deserialize(
        raw.strip_prefix(b"BWSCT002")
            .expect("actual CONTROL header"),
    )
    .unwrap()
}

fn workspace_key(id: WorkspaceId) -> Vec<u8> {
    format!("ws/{id}").into_bytes()
}

fn lease_key(workspace: WorkspaceId, id: LeaseId) -> Vec<u8> {
    format!("lease/{workspace}/{id}").into_bytes()
}

fn lease_index_key(id: LeaseId) -> Vec<u8> {
    format!("lease-id/{id}").into_bytes()
}
fn receipt_key(reference: &PackedReleasedMountReference) -> Vec<u8> {
    format!(
        "packed-v3/clean-release/{}/{}",
        reference.guard.workspace_id, reference.guard.lease_id
    )
    .into_bytes()
}

fn point_limits(cap: usize) -> KvReadLimits {
    KvReadLimits {
        max_records: cap,
        max_key_bytes: 1024,
        max_value_bytes: 48 << 10,
        max_total_bytes: 48 << 10,
        max_response_bytes: 64 << 10,
        max_data_requests: cap + 2,
    }
}

async fn wait_closed(budget: &Arc<V3MountBudget>) {
    tokio::time::timeout(WAIT, async {
        while !budget.state().closed {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned original budget reached terminal closure");
}

async fn assert_pin_state<B: WorkspaceKvBackend>(
    backend: Arc<B>,
    reference: &PackedReleasedMountReference,
    expected: PackedReaderPinState,
    budget: &Arc<V3MountBudget>,
) {
    // Observation after terminal closure is a fresh admin read ledger; it does
    // not reopen the original mounted source's cleanup budget.
    let observer =
        KvWorkspaceStore::from_arc(backend).with_packed_reader_pin_budget(budget.clone());
    let slots = observer.list_packed_reader_pin_slots().await.unwrap();
    assert_eq!(slots.len(), 1);
    assert_eq!(slots[0].acquired_guard, reference.guard);
    assert_eq!(slots[0].state, expected);
}

async fn inspect_release<B: WorkspaceKvBackend>(
    backend: &B,
    reference: &PackedReleasedMountReference,
    original: &SnapshotLease,
    expect_receipt: bool,
    later_grant: Option<&SnapshotLease>,
) {
    let mut keys = vec![
        b"control".to_vec(),
        workspace_key(reference.guard.workspace_id),
        lease_key(reference.guard.workspace_id, reference.guard.lease_id),
        lease_index_key(reference.guard.lease_id),
        receipt_key(reference),
    ];
    if let Some(next) = later_grant {
        assert!(expect_receipt);
        assert_eq!(next.workspace_id, reference.guard.workspace_id);
        assert_ne!(next.lease_id, original.lease_id);
        keys.push(lease_key(next.workspace_id, next.lease_id));
        keys.push(lease_index_key(next.lease_id));
    }
    let (rows, now) = backend
        .get_many_consistent_with_time_bounded(&keys, point_limits(keys.len()))
        .await
        .unwrap();
    assert_eq!(rows.len(), keys.len());
    let control = decode_control(rows[0].as_ref().unwrap());
    let workspace: WorkspaceRecord = decode_envelope(rows[1].as_ref().unwrap());
    let lease: SnapshotLease = decode_envelope(rows[2].as_ref().unwrap());
    let routed_workspace: WorkspaceId = decode_envelope(rows[3].as_ref().unwrap());
    assert_eq!(control.schema_version, 1);
    assert_eq!(control.catalog_format, 2);
    assert!(control.header.is_some());
    assert_eq!(workspace.workspace_id, reference.guard.workspace_id);
    assert_eq!(
        workspace.head_layer_id,
        reference.guard.expected_head_layer_id
    );
    assert_eq!(workspace.head_epoch, reference.guard.expected_head_epoch);
    assert_eq!(routed_workspace, reference.guard.workspace_id);
    assert_eq!(lease.lease_id, reference.guard.lease_id);
    assert_eq!(lease.workspace_id, reference.guard.workspace_id);
    assert_eq!(lease.holder_generation, reference.guard.holder_generation);
    assert!(original.expires_at_ns <= lease.expires_at_ns);
    assert!(original.updated_at_ns <= lease.updated_at_ns && lease.updated_at_ns <= now);
    let mut expected = original.clone();
    expected.state = lease.state;
    expected.updated_at_ns = lease.updated_at_ns;
    assert_eq!(
        lease, expected,
        "release changed the renewed lease identity or expiry"
    );
    assert_eq!(
        lease.state,
        if expect_receipt {
            LeaseState::Released
        } else {
            LeaseState::Active
        }
    );
    assert_eq!(
        workspace.active_lease,
        if let Some(next) = later_grant {
            let actual: SnapshotLease = decode_envelope(rows[5].as_ref().unwrap());
            let route: WorkspaceId = decode_envelope(rows[6].as_ref().unwrap());
            assert_eq!(actual, *next);
            assert_eq!(actual.state, LeaseState::Active);
            assert_eq!(actual.holder_generation, original.holder_generation + 1);
            assert_eq!(route, workspace.workspace_id);
            Some(next.lease_id)
        } else if expect_receipt {
            None
        } else {
            Some(lease.lease_id)
        }
    );
    if expect_receipt {
        assert!(rows[4].as_ref().unwrap().starts_with(b"PCR3\x01"));
    } else {
        assert!(rows[4].is_none());
    }
}

async fn actual_original_chain<B, F>(connect: Arc<F>, case: Case)
where
    B: WorkspaceKvBackend,
    F: Fn(Arc<V3MountBudget>) -> Connection<B> + Send + Sync + 'static,
{
    let mount_budget = V3MountBudget::defaults();
    let original_backend = Arc::new(connect(mount_budget.clone()).await.unwrap());
    let observation_budget = V3MountBudget::defaults();
    let bare = Arc::new(connect(observation_budget.clone()).await.unwrap());
    let (_objects, _fixture_client, fixture_snapshot, _fixture_proof, lower_payload) =
        packed().await;
    let objects = Arc::new(ObjectProbe::default());
    let actual_client = selected_objects(&_objects.path().join("objects"), objects.clone()).await;
    let snapshot =
        AuthenticatedV3Snapshot::open(&actual_client, fixture_snapshot.manifest_reference())
            .await
            .unwrap();
    let lower_proof = VerifiedPackedLower::from_authenticated_snapshot(
        &snapshot,
        &V3IndexReader::new(actual_client.clone(), 0),
    )
    .await
    .unwrap();
    let original_snapshot = snapshot.clone();
    let expected_lower_payload = lower_payload.clone();
    let cache = tempfile::tempdir().unwrap();
    let mount = tempfile::tempdir().unwrap();
    let delivery = Arc::new(DeliveryBackend::new(
        original_backend.clone(),
        mount_budget.clone(),
    ));
    let store = Arc::new(
        KvWorkspaceStore::from_arc(delivery.clone())
            .with_packed_reader_pin_budget(mount_budget.clone()),
    );
    let install = request(store.as_ref(), lower_proof).await;
    let binding = store
        .install_packed_lower_binding(install.clone())
        .await
        .unwrap();
    // A genuine InitialSource retires through the production same-CAS
    // lease/PWA path before a real joint mounted grant can be issued.
    store
        .release_lease(ReleaseLease {
            lease_id: install.guard.lease_id,
            holder_generation: install.guard.holder_generation,
        })
        .await
        .unwrap();
    let mounted = store
        .grant_packed_mounted_session(PackedMountGrantRequest {
            workspace_id: binding.workspace_id,
            lease_id: LeaseId::new(),
            holder_generation: install.guard.holder_generation + 1,
            mount_uid: Uuid::new_v4(),
            pod_uid: Uuid::new_v4(),
            ttl_ns: 300_000_000_000,
        })
        .await
        .unwrap();
    let reference = mounted.reference();
    let guard = reference.guard.clone();
    // Release must preserve the real renewed entity's identity and expiry.
    mounted.renew(300_000_000_000).await.unwrap();
    let original_lease: SnapshotLease = decode_envelope(
        &bare
            .get(&lease_key(guard.workspace_id, guard.lease_id))
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(original_lease.state, LeaseState::Active);
    assert!(original_lease.expires_at_ns >= mounted.lease().expires_at_ns);
    let reader = store
        .clone()
        .open_packed_reader_session(
            guard.clone(),
            mount_budget.clone(),
            PackedReaderLeaseOptions {
                ttl_ns: 900_000_000_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let budget = reader.mount_budget();
    let _release_boundaries = ReleaseBoundaries {
        delivery: delivery.clone(),
        objects: objects.clone(),
    };
    let layout = ChunkLayout {
        chunk_size: 4096,
        block_size: 4096,
    };
    let upper = Arc::new(
        ObjectBlockStore::new_with_configs_async(
            actual_client.clone(),
            ChunksCacheConfig::with_budgets(1 << 20, 1 << 20, cache.path().join("chunk-cache")),
            BlockStoreConfig {
                block_size: 4096,
                compression: Compression::None,
                populate_write_cache_after_upload: false,
                range_background_prefetch: false,
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    let lower = Arc::new(
        PackedV3ReadonlyMeta::from_v3_budget(
            actual_client.clone(),
            snapshot,
            4096,
            0,
            budget.clone(),
        )
        .unwrap(),
    );
    let meta = Arc::new(
        WorkspaceMetaLayer::with_chunk_size(
            store.clone(),
            ViewContext {
                workspace_id: guard.workspace_id,
                head_layer_id: guard.expected_head_layer_id,
                head_epoch: guard.expected_head_epoch,
                lease_id: guard.lease_id,
                holder_generation: guard.holder_generation,
            },
            4096,
        )
        .with_packed_v3_lower(
            binding.binding.clone(),
            lower,
            Arc::new(PinnedCatalogPackedBindingAuthority {
                store: store.clone(),
                reader: reader.clone(),
            }),
            upper.clone(),
            layout,
        )
        .unwrap(),
    );
    meta.initialize().await.unwrap();
    // The genuine fixture root belongs to uid 0. Give this disposable upper
    // root write permission through the actual metadata path so the kernel
    // operation also works when unprivileged FUSE is owned by a non-root uid.
    meta.chmod(1, 0o777).await.unwrap();
    let vfs = VFS::from_workspace_components(
        VFSConfig::new(layout)
            .workspace_writeback_root(cache.path().join("writeback"))
            .workspace_writer_epoch(guard.holder_generation),
        upper.clone(),
        meta.clone(),
    )
    .unwrap();
    let path = prepare(mount.path(), &budget).unwrap();
    let concurrency = FuseConcurrencyConfig {
        worker_count: 2,
        max_background: 8,
    };
    let handle = if std::env::var("BREWFS_TEST_PRIVILEGED_FUSE").ok().as_deref() == Some("1") {
        mount_vfs_privileged(vfs.clone(), &path, concurrency)
            .await
            .unwrap()
    } else {
        mount_vfs_unprivileged(vfs.clone(), &path, concurrency)
            .await
            .unwrap()
    };
    let identity = match OwnedPackedMountIdentity::capture(&path, vfs.clone(), budget.clone()) {
        Ok(identity) => identity,
        Err(error) => {
            handle
                .unmount()
                .await
                .expect("join actual mount after capture failure");
            panic!("original kernel identity capture failed: {error}");
        }
    };
    assert_eq!(reference.guard, guard);
    let kernel_path = path.clone();
    let diagnostic_meta = meta.clone();
    let diagnostic_runtime = tokio::runtime::Handle::current();
    let kernel_operations = tokio::task::spawn_blocking(move || {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(kernel_path.join("original-clean-native"))
            .unwrap();
        file.write_all(PAYLOAD).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let native_bytes = std::fs::read(kernel_path.join("original-clean-native")).unwrap_or_else(
            |kernel_error| {
                // Failure-only observation keeps the real kernel read as the
                // contract and reveals the underlying preparation/transport error.
                use crate::chunk::read_plan::{WorkspaceReadPlanProvider, execute_unified_into};
                let diagnostic = diagnostic_runtime.block_on(async {
                    let inode = match diagnostic_meta.lookup(1, "original-clean-native").await {
                        Ok(Some(inode)) => inode,
                        Ok(None) => return "native metadata lookup returned no inode".to_string(),
                        Err(error) => return format!("native metadata lookup failed: {error:?}"),
                    };
                    if let Err(error) = diagnostic_meta.stat_fresh(inode).await {
                        return format!("native metadata stat failed for {inode}: {error:?}");
                    }
                    let prepared = match diagnostic_meta
                        .prepare_unified_read(inode, 0, 0, PAYLOAD.len() as u64)
                        .await
                    {
                        Ok(Some(prepared)) => prepared,
                        Ok(None) => return "native preparation returned no plan".to_string(),
                        Err(error) => return format!("native preparation failed: {error:?}"),
                    };
                    let mut bytes = vec![0; PAYLOAD.len()];
                    match execute_unified_into(
                        prepared.fetcher.as_ref(),
                        0,
                        &prepared.plan,
                        &mut bytes,
                    )
                    .await
                    {
                        Ok(()) => format!(
                            "direct native plan executed; payload_matches={}",
                            bytes == PAYLOAD
                        ),
                        Err(error) => format!("native plan execution failed: {error:#}"),
                    }
                });
                panic!(
                    "actual just-written native kernel read failed: {kernel_error}; {diagnostic}"
                );
            },
        );
        assert_eq!(native_bytes, PAYLOAD);
        assert_eq!(
            std::fs::metadata(kernel_path.join("nonzero"))
                .unwrap()
                .len(),
            4096
        );
        assert_eq!(
            std::fs::read(kernel_path.join("nonzero")).unwrap(),
            lower_payload
        );
    })
    .await;
    handle.unmount().await.unwrap();
    if let Err(error) = kernel_operations {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("actual kernel original operations cancelled: {error}");
    }
    let cutoff = identity.after_worker_join().unwrap();
    cutoff.validate().unwrap();
    mounted.close_renewals_and_drain().await.unwrap();
    let drain = vfs.quiesce_packed_vfs().await.unwrap();
    assert!(
        objects.puts.load(Ordering::SeqCst) > 0,
        "original VFS issued no actual selected-backend data PUT"
    );
    assert!(!budget.state().closed);
    assert!(Arc::ptr_eq(
        &meta.packed_shutdown_budget().unwrap(),
        &budget
    ));

    if case == Case::OriginalReaderShutdownError {
        delivery.mode.store(5, Ordering::SeqCst);
        let failed = VerifiedCleanPackedShutdown::from_original_shutdown(
            reference.clone(),
            drain,
            cutoff,
            &store,
        )
        .await;
        assert!(
            failed.is_err(),
            "actual reader release error minted clean proof"
        );
        drop(failed);
        assert!(
            !budget.state().closed,
            "failed runtime shutdown closed retry/retention budget"
        );
        assert_eq!(delivery.attempts.load(Ordering::SeqCst), 0);
        inspect_release(bare.as_ref(), &reference, &original_lease, false, None).await;
        assert_pin_state(
            bare.clone(),
            &reference,
            PackedReaderPinState::Active,
            &observation_budget,
        )
        .await;
        delivery.mode.store(0, Ordering::SeqCst);
        meta.shutdown_session().await.unwrap();
        wait_closed(&budget).await;
        assert_pin_state(
            bare.clone(),
            &reference,
            PackedReaderPinState::Released,
            &observation_budget,
        )
        .await;
        inspect_release(bare.as_ref(), &reference, &original_lease, false, None).await;
    } else if case == Case::CancelOriginalShutdownWaiter {
        objects.hold_next_range.store(true, Ordering::SeqCst);
        let owned_meta = meta.clone();
        let read = tokio::spawn(async move { owned_meta.stat_fresh(400).await });
        tokio::time::timeout(WAIT, objects.entered.notified())
            .await
            .unwrap();
        read.abort();
        assert!(read.await.unwrap_err().is_cancelled());
        let owned_store = store.clone();
        let owned_reference = reference.clone();
        let constructor = tokio::spawn(async move {
            VerifiedCleanPackedShutdown::from_original_shutdown(
                owned_reference,
                drain,
                cutoff,
                &owned_store,
            )
            .await
        });
        tokio::time::timeout(WAIT, async {
            loop {
                if reader.retain_request().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!constructor.is_finished());
        assert!(!budget.state().closed);
        assert_eq!(delivery.attempts.load(Ordering::SeqCst), 0);
        assert_pin_state(
            bare.clone(),
            &reference,
            PackedReaderPinState::Active,
            &observation_budget,
        )
        .await;
        constructor.abort();
        match constructor.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("cancelled constructor receiver unexpectedly returned"),
        }
        objects.resume.notify_one();
        wait_closed(&budget).await;
        assert!(objects.terminal.load(Ordering::SeqCst));
        inspect_release(bare.as_ref(), &reference, &original_lease, false, None).await;
        assert_pin_state(
            bare.clone(),
            &reference,
            PackedReaderPinState::Released,
            &observation_budget,
        )
        .await;
    } else {
        let proof = VerifiedCleanPackedShutdown::from_original_shutdown(
            reference.clone(),
            drain,
            cutoff,
            &store,
        )
        .await
        .unwrap();
        assert!(
            !budget.state().closed,
            "proof mint closed cleanup admission before release"
        );
        assert!(Arc::ptr_eq(&proof.budget(), &budget));
        assert!(reader.retain_request().is_err());
        assert_pin_state(
            bare.clone(),
            &reference,
            PackedReaderPinState::Released,
            &observation_budget,
        )
        .await;
        let mode = match case {
            Case::ReleaseErrorBefore => 1,
            Case::ReleaseLostReply => 2,
            Case::CancelReleaseWaiter => 3,
            Case::LostReplyThenActualGrant => 4,
            _ => 0,
        };
        delivery.mode.store(mode, Ordering::SeqCst);
        if case == Case::CancelReleaseWaiter {
            let owned = mounted.clone();
            let release = tokio::spawn(async move { owned.release_original(proof).await });
            tokio::time::timeout(WAIT, delivery.entered.notified())
                .await
                .unwrap();
            assert!(!budget.state().closed);
            assert!(!delivery.terminal.load(Ordering::SeqCst));
            assert!(
                budget.state().used[V3BudgetPool::Metadata as usize] >= 16 << 20,
                "owned writer lost native hold admission before actual transport"
            );
            inspect_release(bare.as_ref(), &reference, &original_lease, false, None).await;
            release.abort();
            assert!(release.await.unwrap_err().is_cancelled());
            assert!(!budget.state().closed);
            delivery.release.add_permits(1);
            wait_closed(&budget).await;
            assert!(
                delivery.terminal.load(Ordering::SeqCst),
                "original budget closed before release transport terminal"
            );
        } else {
            let result = mounted.release_original(proof).await;
            match case {
                Case::ReleaseErrorBefore => assert!(matches!(result,
                    Err(WorkspaceError::Backend(ref message)) if message == "actual clean release rejected before submission")),
                Case::LostReplyThenActualGrant => assert!(matches!(result,
                    Err(WorkspaceError::Backend(ref message)) if message == "actual committed clean release reply lost")),
                _ => result.unwrap(),
            }
            wait_closed(&budget).await;
        }
        assert_eq!(
            delivery.attempts.load(Ordering::SeqCst),
            1,
            "unknown/cancel replayed release mutation"
        );
        let committed = case != Case::ReleaseErrorBefore;
        assert_eq!(
            delivery.commits.load(Ordering::SeqCst),
            usize::from(committed)
        );
        let later_grant = delivery.later_grant.lock().await.clone();
        assert_eq!(
            later_grant.is_some(),
            case == Case::LostReplyThenActualGrant
        );
        inspect_release(
            bare.as_ref(),
            &reference,
            &original_lease,
            committed,
            later_grant.as_ref(),
        )
        .await;
        if case == Case::ReleaseLostReply {
            assert_eq!(delivery.confirmation_reads.load(Ordering::SeqCst), 1);
            assert_eq!(delivery.confirmation_cas.load(Ordering::SeqCst), 1);
        }
        if case == Case::LostReplyThenActualGrant {
            assert_eq!(delivery.confirmation_reads.load(Ordering::SeqCst), 1);
            assert_eq!(
                delivery.confirmation_cas.load(Ordering::SeqCst),
                0,
                "changed successor must not execute confirmation CAS"
            );
        }
        if committed {
            // A new admin operation has its own open ledger. Cleanup above was
            // on the original lower's same ledger through actual transport.
            let admin_budget = observation_budget.clone();
            let admin = Arc::new(
                KvWorkspaceStore::from_arc(bare.clone())
                    .with_packed_reader_pin_budget(admin_budget.clone()),
            );
            let head_before = admin
                .load_layer(guard.expected_head_layer_id)
                .await
                .unwrap();
            let source = admin
                .admit_clean_packed_source(reference.clone(), admin_budget.clone())
                .await;
            if case == Case::LostReplyThenActualGrant {
                assert!(matches!(source, Err(WorkspaceError::Busy)));
            } else {
                let ticket = match source.unwrap() {
                    PackedCleanAdmission::Ready(ticket) => ticket,
                    PackedCleanAdmission::RequiresRecovery => {
                        panic!("actual clean original source not admitted")
                    }
                };
                assert_eq!(ticket.released_mount(), reference);
                let old_checks = ticket.retained_checks_for_test().to_vec();
                assert!(
                    old_checks.iter().any(|check| {
                        check.key == lease_key(install.guard.workspace_id, install.guard.lease_id)
                            && check.expected.as_deref().is_some_and(|raw| {
                                decode_envelope::<SnapshotLease>(raw).state == LeaseState::Released
                            })
                    }),
                    "source omitted the actual scoped InitialSource release"
                );
                assert!(old_checks.iter().any(|check| check.key == b"control"));
                assert!(old_checks.iter().any(|check| check.key == ROOT_GENERATION));
                assert!(old_checks.iter().any(|check| {
                    check.key == workspace_key(guard.workspace_id)
                        && check.expected.as_deref().is_some_and(|raw| {
                            decode_envelope::<WorkspaceRecord>(raw)
                                .active_lease
                                .is_none()
                        })
                }));
                assert!(
                    old_checks
                        .iter()
                        .any(|check| check.key == b"packed/v3/topology-generation")
                );
                assert!(
                    old_checks
                        .iter()
                        .any(|check| check.key == lease_key(guard.workspace_id, guard.lease_id))
                );
                assert!(bare.compare_and_swap(&old_checks, &[]).await.unwrap());
                if case == Case::LaterActualLeaseGrant {
                    let next = admin
                        .grant_packed_mounted_session(PackedMountGrantRequest {
                            workspace_id: guard.workspace_id,
                            lease_id: LeaseId::new(),
                            holder_generation: guard.holder_generation + 1,
                            mount_uid: Uuid::new_v4(),
                            pod_uid: Uuid::new_v4(),
                            ttl_ns: 300_000_000_000,
                        })
                        .await
                        .unwrap();
                    let new = next.lease();
                    assert_eq!(new.state, LeaseState::Active);
                    assert_eq!(
                        admin
                            .load_layer(guard.expected_head_layer_id)
                            .await
                            .unwrap(),
                        head_before,
                        "actual grant unexpectedly changed source inode sequence"
                    );
                    assert!(matches!(
                        admin
                            .admit_clean_packed_source(reference.clone(), admin_budget.clone())
                            .await,
                        Err(WorkspaceError::Busy)
                    ));
                    assert!(
                        !bare.compare_and_swap(&old_checks, &[]).await.unwrap(),
                        "old source ticket survived actual new writer grant"
                    );
                    next.renew(1_000_000_000).await.unwrap();
                    let renewed: SnapshotLease = decode_envelope(
                        &bare
                            .get(&lease_key(new.workspace_id, new.lease_id))
                            .await
                            .unwrap()
                            .unwrap(),
                    );
                    let retained_keys = vec![
                        lease_key(new.workspace_id, new.lease_id),
                        format!("packed/v3/native-hold/lease/{}", new.lease_id).into_bytes(),
                        format!("packed-v3/writer/{}", guard.workspace_id).into_bytes(),
                        format!("open/v3/{}", guard.workspace_id).into_bytes(),
                        lease_key(guard.workspace_id, guard.lease_id),
                        lease_key(install.guard.workspace_id, install.guard.lease_id),
                        lease_index_key(new.lease_id),
                        lease_index_key(guard.lease_id),
                        lease_index_key(install.guard.lease_id),
                    ];
                    let retained_limits = KvReadLimits {
                        max_records: retained_keys.len(),
                        max_key_bytes: 1024,
                        max_value_bytes: 48 << 10,
                        max_total_bytes: 48 << 10,
                        max_response_bytes: 64 << 10,
                        max_data_requests: retained_keys.len() + 2,
                    };
                    let (before_expiry, _) = bare
                        .get_many_consistent_with_time_bounded(&retained_keys, retained_limits)
                        .await
                        .unwrap();
                    assert_eq!(before_expiry.len(), retained_keys.len());
                    assert!(before_expiry.iter().all(Option::is_some));
                    assert_eq!(
                        decode_envelope::<SnapshotLease>(before_expiry[0].as_ref().unwrap()),
                        renewed
                    );
                    for index in [4, 5] {
                        assert_eq!(
                            decode_envelope::<SnapshotLease>(
                                before_expiry[index].as_ref().unwrap()
                            )
                            .state,
                            LeaseState::Released,
                            "the actual original and InitialSource leases are already released"
                        );
                    }
                    for index in [6, 7, 8] {
                        assert_eq!(
                            decode_envelope::<WorkspaceId>(before_expiry[index].as_ref().unwrap()),
                            guard.workspace_id,
                            "retained lease index routed outside the original workspace"
                        );
                    }
                    let before_workspace = admin.load_workspace(guard.workspace_id).await.unwrap();
                    assert_eq!(before_workspace.active_lease, Some(new.lease_id));
                    tokio::time::timeout(WAIT, async {
                        while bare.server_time_ns().await.unwrap() < renewed.expires_at_ns {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    assert_eq!(
                        admin.reap_expired_leases().await.unwrap(),
                        1,
                        "only the actual later mounted lease enters logical expiry"
                    );
                    let (after_expiry, now) = bare
                        .get_many_consistent_with_time_bounded(&retained_keys, retained_limits)
                        .await
                        .unwrap();
                    assert_eq!(after_expiry.len(), retained_keys.len());
                    let expired: SnapshotLease = decode_envelope(after_expiry[0].as_ref().unwrap());
                    assert_eq!(expired.state, LeaseState::Expired);
                    assert!(expired.updated_at_ns >= renewed.expires_at_ns);
                    assert!(expired.updated_at_ns <= now);
                    let mut expected_expired = renewed.clone();
                    expected_expired.state = LeaseState::Expired;
                    expected_expired.updated_at_ns = expired.updated_at_ns;
                    assert_eq!(expired, expected_expired);
                    let mut expected_workspace = before_workspace;
                    expected_workspace.active_lease = None;
                    expected_workspace.updated_at_ns = expired.updated_at_ns;
                    assert_eq!(
                        admin.load_workspace(guard.workspace_id).await.unwrap(),
                        expected_workspace,
                        "logical expiry must clear only its own active lease pointer"
                    );
                    assert_eq!(
                        &after_expiry[1..],
                        &before_expiry[1..],
                        "logical expiry must retain the exact mounted native hold/PWA/open, both released predecessors and their indexes"
                    );
                    assert_eq!(admin.reap_expired_leases().await.unwrap(), 0);
                    let puts_before_stale_admission = objects.puts.load(Ordering::SeqCst);
                    assert!(
                        matches!(
                            admin
                                .admit_clean_packed_source(reference.clone(), admin_budget.clone())
                                .await,
                            Err(WorkspaceError::Fenced)
                        ),
                        "expiry of a later joint owner must not revive the original PCR reference"
                    );
                    // The later owner has no original mounted-drain PCR of its
                    // own. Its reference requires recovery; the older PCR is
                    // separately and explicitly fenced by the retained open.
                    assert!(matches!(
                        admin
                            .admit_clean_packed_source(next.reference(), admin_budget.clone())
                            .await
                            .unwrap(),
                        PackedCleanAdmission::RequiresRecovery
                    ));
                    let (after_rejected_admission, _) = bare
                        .get_many_consistent_with_time_bounded(&retained_keys, retained_limits)
                        .await
                        .unwrap();
                    assert_eq!(
                        after_rejected_admission, after_expiry,
                        "stale and recovery-only admission must retain all joint owner rows"
                    );
                    assert_eq!(
                        admin
                            .load_layer(guard.expected_head_layer_id)
                            .await
                            .unwrap(),
                        head_before
                    );
                    assert_eq!(
                        objects.puts.load(Ordering::SeqCst),
                        puts_before_stale_admission,
                        "rejected source admission must issue no object PUT"
                    );
                    assert!(!bare.compare_and_swap(&old_checks, &[]).await.unwrap());
                }
                if case == Case::LaterActualOpenGrant {
                    let token = admin
                        .open_workspace_v3(
                            guard.workspace_id,
                            format!("actual-next-open-{}", Uuid::new_v4()),
                            Duration::from_secs(300),
                        )
                        .await
                        .unwrap();
                    assert_eq!(token.state, V3OpenState::Ready);
                    assert_eq!(
                        admin
                            .load_layer(guard.expected_head_layer_id)
                            .await
                            .unwrap(),
                        head_before
                    );
                    assert!(matches!(
                        admin
                            .admit_clean_packed_source(reference.clone(), admin_budget.clone())
                            .await,
                        Err(WorkspaceError::Busy)
                    ));
                    assert!(
                        !bare.compare_and_swap(&old_checks, &[]).await.unwrap(),
                        "old source ticket survived actual open owner grant"
                    );
                    admin.close_workspace_v3(&token).await.unwrap();
                    assert!(
                        admin
                            .admit_clean_packed_source(reference.clone(), admin_budget.clone())
                            .await
                            .is_err(),
                        "closed later open owner revived old PCR"
                    );
                    assert!(!bare.compare_and_swap(&old_checks, &[]).await.unwrap());
                }
                if case == Case::HeadlessSnapshot {
                    let object_keys = {
                        let keys = objects.keys.lock().await;
                        keys.iter().cloned().collect()
                    };
                    packed_headless_snapshot_tests::consume(
                        connect.clone(),
                        bare.clone(),
                        admin,
                        ticket,
                        packed_headless_snapshot_tests::OriginalSnapshotFixture {
                            client: actual_client.clone(),
                            upper: upper.clone(),
                            layout,
                            object_keys,
                            original: original_snapshot,
                            expected_lower: expected_lower_payload,
                        },
                        reference.clone(),
                    )
                    .await;
                } else if case == Case::HeadlessRouteContracts {
                    drop(ticket);
                    packed_headless_route_tests::contract(
                        bare.clone(),
                        admin_budget.clone(),
                        reference.clone(),
                    )
                    .await;
                }
            }
        } else {
            let admin_budget = observation_budget.clone();
            let admin = Arc::new(
                KvWorkspaceStore::from_arc(bare.clone())
                    .with_packed_reader_pin_budget(admin_budget.clone()),
            );
            assert!(matches!(
                admin
                    .admit_clean_packed_source(reference.clone(), admin_budget)
                    .await
                    .unwrap(),
                PackedCleanAdmission::RequiresRecovery
            ));
        }
    }
    drop(reader);
    drop(vfs);
    drop(meta);
    drop(upper);
    drop(store);
    drop(mounted);
    original_backend.shutdown_metadata_backend().await.unwrap();
    bare.shutdown_metadata_backend().await.unwrap();
    observation_budget.close();
    assert!(
        observation_budget
            .state()
            .used
            .iter()
            .all(|bytes| *bytes == 0)
    );
    tokio::time::timeout(WAIT, async {
        while budget.state().used.iter().any(|bytes| *bytes != 0) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original mount owners released after terminal shutdown");
}

async fn isolated<B, F>(connect: F, case: Case)
where
    B: WorkspaceKvBackend,
    F: Fn(Arc<V3MountBudget>) -> Connection<B> + Send + Sync + 'static,
{
    let connect = Arc::new(connect);
    let owned = connect.clone();
    let outcome = tokio::spawn(async move { actual_original_chain(owned, case).await }).await;
    let cleanup_budget = V3MountBudget::defaults();
    let backend = connect(cleanup_budget.clone()).await.unwrap();
    loop {
        let rows = backend
            .scan_prefix_with_byte_limits(b"", point_limits(16))
            .await
            .unwrap();
        if rows.is_empty() {
            break;
        }
        let checks = rows
            .iter()
            .map(|row| KvCheck {
                key: row.key.clone(),
                expected: Some(row.value.clone()),
            })
            .collect::<Vec<_>>();
        let writes = rows
            .iter()
            .map(|row| KvWrite::Delete {
                key: row.key.clone(),
            })
            .collect::<Vec<_>>();
        assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
    }
    backend.shutdown_metadata_backend().await.unwrap();
    cleanup_budget.close();
    assert!(cleanup_budget.state().used.iter().all(|bytes| *bytes == 0));
    if let Err(error) = outcome {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("actual original shutdown contract cancelled: {error}");
    }
}

async fn redis(case: Case) {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let namespace = format!("original-shutdown-{}", Uuid::new_v4());
    isolated(
        move |_budget| {
            let url = url.clone();
            let namespace = namespace.clone();
            Box::pin(async move { RedisWorkspaceBackend::connect(&url, &namespace).await })
                as Connection<RedisWorkspaceBackend>
        },
        case,
    )
    .await;
}

async fn tikv(case: Case) {
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    let namespace = format!("original-shutdown-{}", Uuid::new_v4());
    isolated(
        move |budget| {
            let endpoints = endpoints.clone();
            let namespace = namespace.clone();
            Box::pin(async move {
                TiKvWorkspaceBackend::connect_with_budget(endpoints, &namespace, budget).await
            }) as Connection<TiKvWorkspaceBackend>
        },
        case,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, Redis UUID namespace, LocalFS objects"]
async fn real_redis_original_packed_shutdown_clean_receipt_same_budget() {
    redis(Case::Positive).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, TiKV UUID namespace, LocalFS objects"]
async fn real_tikv_original_packed_shutdown_clean_receipt_same_budget() {
    tikv(Case::Positive).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, Redis UUID namespace, LocalFS objects"]
async fn real_redis_original_packed_shutdown_release_error_never_writes_receipt() {
    redis(Case::ReleaseErrorBefore).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, TiKV UUID namespace, LocalFS objects"]
async fn real_tikv_original_packed_shutdown_release_error_never_writes_receipt() {
    tikv(Case::ReleaseErrorBefore).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, Redis UUID namespace, LocalFS objects"]
async fn real_redis_original_packed_shutdown_unknown_reply_exact_full_packet_without_replay() {
    redis(Case::ReleaseLostReply).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, TiKV UUID namespace, LocalFS objects"]
async fn real_tikv_original_packed_shutdown_unknown_reply_exact_full_packet_without_replay() {
    tikv(Case::ReleaseLostReply).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, Redis UUID namespace, LocalFS objects"]
async fn real_redis_original_packed_shutdown_cancel_release_keeps_owned_budget_until_cas() {
    redis(Case::CancelReleaseWaiter).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, TiKV UUID namespace, LocalFS objects"]
async fn real_tikv_original_packed_shutdown_cancel_release_keeps_owned_budget_until_cas() {
    tikv(Case::CancelReleaseWaiter).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, Redis UUID namespace, LocalFS objects"]
async fn real_redis_original_packed_shutdown_cancel_constructor_waits_actual_lower_transport() {
    redis(Case::CancelOriginalShutdownWaiter).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, TiKV UUID namespace, LocalFS objects"]
async fn real_tikv_original_packed_shutdown_cancel_constructor_waits_actual_lower_transport() {
    tikv(Case::CancelOriginalShutdownWaiter).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, Redis UUID namespace, LocalFS objects"]
async fn real_redis_original_packed_shutdown_reader_error_retains_pin_and_cleanup_budget() {
    redis(Case::OriginalReaderShutdownError).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, TiKV UUID namespace, LocalFS objects"]
async fn real_tikv_original_packed_shutdown_reader_error_retains_pin_and_cleanup_budget() {
    tikv(Case::OriginalReaderShutdownError).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, Redis UUID namespace, LocalFS objects"]
async fn real_redis_original_packed_shutdown_actual_later_lease_grant_fences_ticket_without_mutation()
 {
    redis(Case::LaterActualLeaseGrant).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, TiKV UUID namespace, LocalFS objects"]
async fn real_tikv_original_packed_shutdown_actual_later_lease_grant_fences_ticket_without_mutation()
 {
    tikv(Case::LaterActualLeaseGrant).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, Redis UUID namespace, LocalFS objects"]
async fn real_redis_original_packed_shutdown_actual_later_open_grant_fences_ticket_without_mutation()
 {
    redis(Case::LaterActualOpenGrant).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, TiKV UUID namespace, LocalFS objects"]
async fn real_tikv_original_packed_shutdown_actual_later_open_grant_fences_ticket_without_mutation()
{
    tikv(Case::LaterActualOpenGrant).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, Redis UUID namespace, LocalFS objects"]
async fn real_redis_original_packed_shutdown_unknown_then_actual_grant_rejects_stale_confirmation()
{
    redis(Case::LostReplyThenActualGrant).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, TiKV UUID namespace, LocalFS objects"]
async fn real_tikv_original_packed_shutdown_unknown_then_actual_grant_rejects_stale_confirmation() {
    tikv(Case::LostReplyThenActualGrant).await;
}
