//! Complete first packed-v3 composer on actual Redis/TiKV and LocalFS.
//! The public consumer creates all authority through its real VFS/native chain.

use super::*;
use crate::cadapter::client::ObjectByteStream;
use crate::cadapter::localfs::LocalFsBackend;
use crate::cadapter::read_observer::{Engine, Origin, Phase, ReadContext, ReadObserver};
use crate::chunk::cache::ChunksCacheConfig;
use crate::chunk::compress::Compression;
use crate::chunk::store::{BlockStoreConfig, ObjectBlockStore};
use crate::meta::store::{FileAttr, FileType};
use crate::workspace_overlay::catalog::{CreateWorkspace, WorkspaceStore};
use crate::workspace_overlay::packed_v3::wire005::V3RootAttributes;
use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
use crate::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;
use async_trait::async_trait;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const SMALL_BYTES: u64 = 8 << 20;
type Connection<B> = Pin<Box<dyn Future<Output = Result<B, WorkspaceError>> + Send>>;

#[derive(Clone)]
struct ActualLocalFs {
    inner: LocalFsBackend,
    puts: Arc<AtomicU64>,
}

#[async_trait]
impl ObjectBackend for ActualLocalFs {
    async fn put_object(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put_object(key, bytes).await
    }
    async fn put_object_vectored(
        &self,
        key: &str,
        chunks: Vec<bytes::Bytes>,
    ) -> anyhow::Result<()> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put_object_vectored(key, chunks).await
    }
    async fn put_object_create_only(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put_object_create_only(key, bytes).await
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }
    async fn get_object_stream(&self, key: &str) -> anyhow::Result<Option<ObjectByteStream>> {
        self.inner.get_object_stream(key).await
    }
    async fn get_object_stream_observed(
        &self,
        key: &str,
        expected: Option<u64>,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<Option<ObjectByteStream>> {
        self.inner
            .get_object_stream_observed(key, expected, context, observer)
            .await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        bytes: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.inner.get_object_range(key, offset, bytes).await
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        self.inner
            .get_object_range_stream(key, offset, length)
            .await
    }
    async fn get_object_range_stream_observed(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<ObjectByteStream> {
        self.inner
            .get_object_range_stream_observed(key, offset, length, context, observer)
            .await
    }
    async fn get_object_size(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size(key).await
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(key).await
    }
    async fn get_object_size_bounded_observed(
        &self,
        key: &str,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<Option<u64>> {
        self.inner
            .get_object_size_bounded_observed(key, context, observer)
            .await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
        panic!("initial composer, idempotent reopen and root read cannot delete objects")
    }
}

fn client(
    objects: &Path,
    puts: Arc<AtomicU64>,
    budget: &Arc<V3MountBudget>,
) -> ObjectClient<ActualLocalFs> {
    ObjectClient::new(ActualLocalFs {
        inner: LocalFsBackend::new(objects),
        puts,
    })
    .with_read_observer(
        budget.read_observer(None).unwrap(),
        Engine::PackedV3,
        Phase::Startup,
        Origin::Demand,
    )
}

async fn upper(
    client: ObjectClient<ActualLocalFs>,
    cache: &Path,
) -> Arc<ObjectBlockStore<ActualLocalFs>> {
    Arc::new(
        ObjectBlockStore::new_with_configs_async(
            client,
            ChunksCacheConfig::with_budgets(1 << 20, 1 << 20, cache.to_path_buf()),
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
    )
}

fn request(
    snapshot_id: SnapshotId,
    journal_id: JournalId,
    next_head: LayerId,
    lease_id: LeaseId,
    scratch: Arc<tempfile::TempDir>,
) -> PackedHeadlessSnapshotRequest {
    let mut request = PackedHeadlessSnapshotRequest::bounded_operator(
        PackedHeadlessSnapshotDescription {
            snapshot_id,
            snapshot_name: format!("actual-initial-packed-v3-{snapshot_id}"),
            owner_id: None,
        },
        lease_id,
        journal_id,
        next_head,
        300_000_000_000,
        scratch.path().to_path_buf(),
    )
    .with_temporary_owner(scratch)
    .unwrap();
    request.max_rows = 512;
    request.max_logical_bytes = SMALL_BYTES;
    request.max_data_bytes = SMALL_BYTES;
    request.scratch_disk_bytes = SMALL_BYTES;
    request.graph_limits = V3IndexAuditLimits {
        max_objects: 4096,
        max_authenticated_bytes: 16 << 20,
        max_requested_bytes: 16 << 20,
        max_decoded_bytes: 16 << 20,
        max_frame_validation_steps: 4096,
        max_logical_hash_bytes: SMALL_BYTES,
        max_contexts: 4096,
        max_visits: 16_384,
        max_leaf_records: 4096,
        max_page_records: 4096,
        max_disk_bytes: SMALL_BYTES,
        sqlite_cache_bytes: 64 << 10,
        max_sql_operations: 1_000_000,
        max_sql_vm_steps: 4_000_000,
        chunk_bytes: 4096,
    };
    request
}

async fn retired(budget: &V3MountBudget) {
    for _ in 0..100 {
        if budget.state().used.iter().all(|bytes| *bytes == 0) {
            assert!(budget.state().closed);
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        budget.state().used,
        [0; 8],
        "actual operation owners did not drain"
    );
}

fn assert_root(actual: &FileAttr, expected: &V3RootAttributes) {
    assert_eq!(actual.kind, FileType::Dir);
    assert_eq!(actual.ino, expected.inode as i64);
    assert_eq!(
        (actual.size, actual.blocks),
        (expected.size, expected.blocks)
    );
    assert_eq!(
        (actual.mode, actual.uid, actual.gid, actual.nlink),
        (expected.mode, expected.uid, expected.gid, expected.nlink)
    );
    assert_eq!(
        (actual.atime, actual.mtime, actual.ctime),
        (expected.atime_ns, expected.mtime_ns, expected.ctime_ns)
    );
}

async fn contract<B, F>(connect: Arc<F>, objects: Arc<tempfile::TempDir>)
where
    B: WorkspaceKvBackend,
    F: Fn(Arc<V3MountBudget>) -> Connection<B> + Send + Sync + 'static,
{
    let puts = Arc::new(AtomicU64::new(0));
    let scratch = Arc::new(tempfile::tempdir().unwrap());
    let cache = tempfile::tempdir().unwrap();
    let volume = CreateVolumeRoot {
        upper_bucket: None,
        volume_format: "workspace-v1".into(),
        schema_version: 1,
        volume_id: uuid::Uuid::new_v4(),
        workspace_id: WorkspaceId::new(),
        root_layer_id: LayerId::new(),
        writable_layer_id: LayerId::new(),
        owner_id: None,
    };
    let snapshot_id = SnapshotId::new();
    let journal_id = JournalId::new();
    let next_head = LayerId::new();
    let original_lease = LeaseId::new();
    let layout = ChunkLayout {
        chunk_size: 4096,
        block_size: 4096,
    };

    // First installation, original VFS drain, native factory, final carrier CAS
    // and Released+Snapshot finish all run through this one public consumer.
    let initial_budget = V3MountBudget::defaults();
    let initial_backend = Arc::new(connect(initial_budget.clone()).await.unwrap());
    let initial = Arc::new(
        KvWorkspaceStore::from_arc(initial_backend.clone())
            .with_packed_reader_pin_budget(initial_budget.clone()),
    );
    let initial_client = client(objects.path(), puts.clone(), &initial_budget);
    let initial_upper = upper(initial_client.clone(), &cache.path().join("first")).await;
    let result = match initial
        .bootstrap_initial_packed_snapshot(
            volume.clone(),
            initial_client,
            initial_upper.clone(),
            layout,
            request(
                snapshot_id,
                journal_id,
                next_head,
                original_lease,
                scratch.clone(),
            ),
        )
        .await
    {
        Ok(result) => result,
        Err(failure) => panic!(
            "actual initial packed-v3 composer failed: {} (committed={})",
            failure.error(),
            failure.committed_result().is_some()
        ),
    };
    assert_eq!(result.snapshot_id, snapshot_id);
    assert_eq!(result.packed_carrier_revision, result.binding.base_revision);
    assert_ne!(
        result.packed_carrier_revision,
        result.native_sealed_source_revision
    );
    assert_eq!(result.binding.head_layer_id, next_head);
    assert_eq!(result.binding.binding.binding_version, 2);
    assert_eq!(result.binding.highest_inode, 1);
    assert!(puts.load(Ordering::SeqCst) > 1);
    drop(initial_upper);
    drop(initial);
    initial_backend.shutdown_metadata_backend().await.unwrap();
    drop(initial_backend);
    retired(&initial_budget).await;

    // Reopen is a separate owned operation. In particular TiKV reconnects its
    // real transports with this same new canonical ledger rather than borrowing
    // the closed first-operation transport or opening an unobserved client.
    let before_reopen = puts.load(Ordering::SeqCst);
    let reopen_budget = V3MountBudget::defaults();
    let reopen_backend = Arc::new(connect(reopen_budget.clone()).await.unwrap());
    let reopened = Arc::new(
        KvWorkspaceStore::from_arc(reopen_backend.clone())
            .with_packed_reader_pin_budget(reopen_budget.clone()),
    );
    let reopen_client = client(objects.path(), puts.clone(), &reopen_budget);
    let reopen_upper = upper(reopen_client.clone(), &cache.path().join("reopen")).await;
    let repeated = match reopened
        .bootstrap_initial_packed_snapshot(
            volume.clone(),
            reopen_client,
            reopen_upper.clone(),
            layout,
            request(
                snapshot_id,
                journal_id,
                next_head,
                original_lease,
                scratch.clone(),
            ),
        )
        .await
    {
        Ok(result) => result,
        Err(failure) => panic!(
            "actual same-request initial reopen failed: {} (committed={})",
            failure.error(),
            failure.committed_result().is_some()
        ),
    };
    assert_eq!(repeated.snapshot_id, result.snapshot_id);
    assert_eq!(repeated.binding, result.binding);
    assert_eq!(
        repeated.packed_carrier_revision,
        result.packed_carrier_revision
    );
    assert_eq!(
        repeated.native_sealed_source_revision,
        result.native_sealed_source_revision
    );
    assert_eq!(
        puts.load(Ordering::SeqCst),
        before_reopen,
        "same logical request must inspect its actual finish without another PUT"
    );
    drop(reopen_upper);
    drop(reopened);
    reopen_backend.shutdown_metadata_backend().await.unwrap();
    drop(reopen_backend);
    retired(&reopen_budget).await;

    let read_budget = V3MountBudget::defaults();
    let read_backend = Arc::new(connect(read_budget.clone()).await.unwrap());
    let store = Arc::new(
        KvWorkspaceStore::from_arc(read_backend.clone())
            .with_packed_reader_pin_budget(read_budget.clone()),
    );
    let owner = read_budget
        .admit(&[(V3BudgetPool::Metadata, 1 << 20)])
        .unwrap();
    let persisted = store.load_snapshot(snapshot_id).await.unwrap();
    assert_eq!(persisted.revision, result.packed_carrier_revision);
    assert_eq!(
        store
            .load_workspace(volume.workspace_id)
            .await
            .unwrap()
            .head_layer_id,
        next_head
    );
    let leases = store.list_leases(volume.workspace_id).await.unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].lease_id, original_lease);
    assert_eq!(leases[0].state, LeaseState::Released);
    let published = store
        .inspect_initial_packed_snapshot(volume.workspace_id)
        .await
        .unwrap()
        .expect("actual atomic initial snapshot receipt");
    assert_eq!(published.binding, result.binding);
    assert_eq!(
        store
            .inspect_packed_carrier_revision(&persisted.revision)
            .await
            .unwrap(),
        result.binding
    );

    // Derive expected attributes from the genuine root row written by this
    // consumer, including actual backend-time timestamps. No root fixture,
    // catalog record, binding, fence or publication authority is synthesized.
    let (root_rows, _) = read_backend
        .get_many_consistent_with_time_bounded(
            &[inode_identity_key(volume.root_layer_id, 1)],
            source_limits(1),
        )
        .await
        .unwrap();
    assert_eq!(root_rows.len(), 1);
    let root: InodeDelta =
        decode_open_value(root_rows[0].as_deref().unwrap(), SOURCE_MAX_BYTES).unwrap();
    assert_eq!(root.layer_id, volume.root_layer_id);
    assert_eq!(root.state, InodeState::Present);
    let expected_root = V3RootAttributes {
        inode: 1,
        size: root.size,
        blocks: 0,
        mode: 0o040000 | root.mode,
        uid: root.uid,
        gid: root.gid,
        nlink: root.nlink,
        atime_ns: root.atime_ns,
        mtime_ns: root.mtime_ns,
        ctime_ns: root.ctime_ns,
    };
    let read_client = client(objects.path(), puts.clone(), &read_budget);
    let snapshot = AuthenticatedV3Snapshot::open(&read_client, &result.binding.binding.manifest)
        .await
        .unwrap();
    assert_eq!(
        snapshot.manifest().source.as_ref().unwrap().root,
        expected_root
    );

    // Mandatory carrier birth writes the real private alias. Its leased reader
    // then authenticates the child's durable binding through production pins.
    let child = store
        .create_workspace_from_packed_carrier(CreateWorkspace {
            upper_bucket: None,
            workspace_id: WorkspaceId::new(),
            head_layer_id: LayerId::new(),
            base_revision: persisted.revision.clone(),
            owner_id: None,
        })
        .await
        .unwrap();
    assert_eq!(child.fork_base.as_ref(), Some(&persisted.revision));
    let session = crate::workspace_overlay::lifecycle::WorkspaceMountSession::acquire_for_mount(
        store.clone(),
        child.workspace_id,
        1,
        Duration::from_secs(300),
        Duration::from_secs(60),
        false,
        read_budget.clone(),
    )
    .await
    .unwrap();
    let lease = session.lease.clone();
    let guard = HeadGuard {
        workspace_id: child.workspace_id,
        expected_head_layer_id: child.head_layer_id,
        expected_head_epoch: child.head_epoch,
        lease_id: lease.lease_id,
        holder_generation: lease.holder_generation,
    };
    let child_binding = store
        .load_packed_lower_binding(guard)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child_binding.base_layer_id, persisted.revision.layer_id);
    assert_eq!(child_binding.manifest, result.binding.binding.manifest);
    let read_upper = upper(read_client.clone(), &cache.path().join("fork-read")).await;
    let lower = Arc::new(
        PackedV3ReadonlyMeta::from_v3_budget(
            read_client,
            snapshot,
            layout.chunk_size,
            0,
            read_budget.clone(),
        )
        .unwrap(),
    );
    let metadata = WorkspaceMetaLayer::with_chunk_size(
        store.clone(),
        ViewContext {
            workspace_id: child.workspace_id,
            head_layer_id: child.head_layer_id,
            head_epoch: child.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        },
        layout.chunk_size,
    )
    .with_packed_v3_lower_from_store(lower, read_upper.clone(), layout)
    .await
    .unwrap();
    assert_root(
        &metadata.stat_fresh(1).await.unwrap().unwrap(),
        &expected_root,
    );
    metadata
        .shutdown_packed_runtime_for_clean_release()
        .await
        .unwrap();
    session.release().await.unwrap();
    assert_eq!(
        puts.load(Ordering::SeqCst),
        before_reopen,
        "carrier birth and actual authenticated root read cannot issue PUTs"
    );
    drop(metadata);
    drop(read_upper);
    drop(owner);
    drop(store);
    read_backend.shutdown_metadata_backend().await.unwrap();
    drop(read_backend);
    read_budget.close();
    retired(&read_budget).await;
    eprintln!(
        "packed-v3 complete initial composer + actual VFS/native factory + carrier Snapshot + same-request reopen + mandatory fork/pinned root read passed"
    );
}

async fn clear_owned_namespace<B: WorkspaceKvBackend>(backend: &B) {
    let limits = KvReadLimits {
        max_records: 32,
        max_key_bytes: 1024,
        max_value_bytes: 64 << 10,
        max_total_bytes: 2 << 20,
        max_response_bytes: 2 << 20,
        max_data_requests: 32,
    };
    for _ in 0..512 {
        let entries = backend
            .scan_prefix_page_with_byte_limits(b"", None, limits)
            .await
            .unwrap();
        if entries.is_empty() {
            return;
        }
        let checks = entries
            .iter()
            .map(|entry| KvCheck {
                key: entry.key.clone(),
                expected: Some(entry.value.clone()),
            })
            .collect::<Vec<_>>();
        let writes = entries
            .iter()
            .map(|entry| KvWrite::Delete {
                key: entry.key.clone(),
            })
            .collect::<Vec<_>>();
        assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
    }
    panic!("owned small initial-composer namespace cleanup exceeded its page bound");
}

async fn isolated<B, F>(connect: F)
where
    B: WorkspaceKvBackend,
    F: Fn(Arc<V3MountBudget>) -> Connection<B> + Send + Sync + 'static,
{
    let connect = Arc::new(connect);
    let worker_connect = connect.clone();
    let objects = Arc::new(tempfile::tempdir().unwrap());
    let worker_objects = objects.clone();
    let result = tokio::spawn(async move { contract(worker_connect, worker_objects).await }).await;
    // Fixture cleanup is a fresh scope even after a test panic; it touches only
    // this test's UUID namespace and never borrows a closed operation budget.
    let cleanup_budget = V3MountBudget::defaults();
    let cleanup = connect(cleanup_budget.clone()).await.unwrap();
    clear_owned_namespace(&cleanup).await;
    cleanup.shutdown_metadata_backend().await.unwrap();
    drop(cleanup);
    cleanup_budget.close();
    retired(&cleanup_budget).await;
    match result {
        Ok(()) => {}
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("initial composer test cancelled: {error}"),
    }
}

async fn initial_root_reverse_birth<B, F>(connect: F)
where
    B: WorkspaceKvBackend,
    F: Fn(Arc<V3MountBudget>) -> Connection<B> + Send + Sync + 'static,
{
    let connect = Arc::new(connect);
    let worker_connect = connect.clone();
    let result = tokio::spawn(async move {
        let budget = V3MountBudget::defaults();
        let backend = Arc::new(worker_connect(budget.clone()).await.unwrap());
        let store = KvWorkspaceStore::from_arc(backend.clone())
            .with_packed_reader_pin_budget(budget.clone());
        let volume = CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: uuid::Uuid::new_v4(),
            workspace_id: WorkspaceId::new(),
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: None,
        };
        // Exercise the genuine direct birth before any publication can replace
        // the head/base. The ordinary create_volume_root route cannot cover it.
        store.ensure_initial_volume_root(&volume).await.unwrap();
        let layers = store
            .load_layer_chain(volume.writable_layer_id)
            .await
            .unwrap();
        let proof = store
            .get_native_reverse_authority(&layers, budget.clone())
            .await
            .unwrap();
        for layer in &layers {
            let page = store
                .get_native_reverse_dentry_page(&proof, layer.layer_id, 400, None, budget.clone())
                .await
                .unwrap();
            assert!(page.rows.is_empty());
        }
        // A retry authenticates the same build incarnation rather than
        // inventing a fresh completeness claim for an existing layer.
        store.ensure_initial_volume_root(&volume).await.unwrap();
        store
            .confirm_native_reverse_authority(&proof, budget.clone())
            .await
            .unwrap();
        drop(proof);
        drop(store);
        backend.shutdown_metadata_backend().await.unwrap();
        drop(backend);
        budget.close();
        retired(&budget).await;
    })
    .await;
    let cleanup_budget = V3MountBudget::defaults();
    let cleanup = connect(cleanup_budget.clone()).await.unwrap();
    clear_owned_namespace(&cleanup).await;
    cleanup.shutdown_metadata_backend().await.unwrap();
    drop(cleanup);
    cleanup_budget.close();
    retired(&cleanup_budget).await;
    match result {
        Ok(()) => {}
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("initial reverse birth test cancelled: {error}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires BREWFS_TEST_REDIS_URL; actual first native root CAS and UUID namespace"]
async fn real_redis_initial_root_birth_has_complete_native_reverse_authority() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let namespace = format!("packed-v3-initial-reverse-birth-{}", uuid::Uuid::new_v4());
    initial_root_reverse_birth(move |_budget| {
        let url = url.clone();
        let namespace = namespace.clone();
        Box::pin(async move { RedisWorkspaceBackend::connect(&url, &namespace).await })
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS; actual first native root CAS and UUID namespace"]
async fn real_tikv_initial_root_birth_has_complete_native_reverse_authority() {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .expect("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let namespace = format!("packed-v3-initial-reverse-birth-{}", uuid::Uuid::new_v4());
    initial_root_reverse_birth(move |budget| {
        let endpoints = endpoints.clone();
        let namespace = namespace.clone();
        Box::pin(async move {
            TiKvWorkspaceBackend::connect_with_budget(endpoints, &namespace, budget).await
        })
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires BREWFS_TEST_REDIS_URL; actual full composer, LocalFS and UUID namespace"]
async fn real_redis_initial_composer_snapshot_reopen_carrier_fork_and_actual_root_read() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let namespace = format!("packed-v3-initial-composer-{}", uuid::Uuid::new_v4());
    isolated(move |_budget| {
        let url = url.clone();
        let namespace = namespace.clone();
        Box::pin(async move { RedisWorkspaceBackend::connect(&url, &namespace).await })
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS; actual full composer, LocalFS and UUID namespace"]
async fn real_tikv_initial_composer_snapshot_reopen_carrier_fork_and_actual_root_read() {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .expect("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let namespace = format!("packed-v3-initial-composer-{}", uuid::Uuid::new_v4());
    isolated(move |budget| {
        let endpoints = endpoints.clone();
        let namespace = namespace.clone();
        Box::pin(async move {
            TiKvWorkspaceBackend::connect_with_budget(endpoints, &namespace, budget).await
        })
    })
    .await;
}
