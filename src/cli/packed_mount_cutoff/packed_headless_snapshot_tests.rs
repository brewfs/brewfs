//! Actual original mounted chain to the public packed-v3 snapshot consumer.
//! This is a child of packed_original_shutdown_tests, sharing its real fixture.
//! No source fence, graph completion, ready publisher or carrier is fabricated.

use super::*;
use crate::chunk::read_plan::{WorkspaceReadPlanProvider, execute_unified_into};
use crate::workspace_overlay::packed_v3::wire005::{
    AuthenticatedV3Snapshot, V3BuildPolicy, V3IndexAuditLimits, V3ProducerOptions,
};
use crate::workspace_overlay::packed_v3::{AccessProfile, PackedCodec, SizeClassTable};
use crate::workspace_overlay::publish::binding::PackedLowerBindingRecord;
use crate::workspace_overlay::stores::kv_store::packed_admin::{
    PackedCleanSourceTicket, PackedHeadlessSnapshotDescription, PackedHeadlessSnapshotRequest,
};
use futures_util::StreamExt;

const SCRATCH_BYTES: u64 = 8 << 20;

async fn read_original_object(client: &ObjectClient<ActualObjects>, key: &str) -> Vec<u8> {
    let length = client
        .typed_object_size(crate::cadapter::read_observer::ReadClass::LogicalRead, key)
        .await
        .unwrap()
        .unwrap();
    assert!(length <= 1 << 20, "small fixture single object bound");
    let mut stream = client
        .backend_object_stream(key, Some(length), None)
        .await
        .unwrap()
        .unwrap();
    let mut bytes = Vec::with_capacity(usize::try_from(length).unwrap());
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert!(bytes.len().checked_add(chunk.len()).unwrap() <= length as usize);
        bytes.extend_from_slice(&chunk);
    }
    assert_eq!(bytes.len() as u64, length);
    bytes
}

async fn original_object_bytes(
    client: &ObjectClient<ActualObjects>,
    keys: &[String],
) -> BTreeMap<String, Vec<u8>> {
    let mut original = BTreeMap::new();
    let mut total = 0u64;
    assert!(keys.len() <= 4096);
    for key in keys {
        let bytes = read_original_object(client, key).await;
        total = total.checked_add(bytes.len() as u64).unwrap();
        assert!(
            total <= SCRATCH_BYTES,
            "small fixture object inventory exceeded bound"
        );
        assert!(original.insert(key.clone(), bytes).is_none());
    }
    assert!(!original.is_empty());
    original
}

async fn read_snapshot_file(
    metadata: &PackedV3ReadonlyMeta<ActualObjects>,
    name: &str,
    expected: &[u8],
) {
    let inode = metadata.lookup(1, name).await.unwrap().unwrap();
    let attributes = metadata.stat_fresh(inode).await.unwrap().unwrap();
    assert_eq!(attributes.size, expected.len() as u64);
    let _output = metadata
        .reserve_read_output(expected.len())
        .unwrap()
        .expect("actual packed-v3 output ownership");
    let prepared = metadata
        .prepare_unified_read(inode, 0, 0, expected.len() as u64)
        .await
        .unwrap()
        .unwrap();
    let mut bytes = vec![0; expected.len()];
    execute_unified_into(prepared.fetcher.as_ref(), 0, &prepared.plan, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, expected);
}

pub(super) struct OriginalSnapshotFixture {
    pub(super) client: ObjectClient<ActualObjects>,
    pub(super) upper: Arc<ObjectBlockStore<ActualObjects>>,
    pub(super) layout: ChunkLayout,
    pub(super) object_keys: Vec<String>,
    pub(super) original: AuthenticatedV3Snapshot,
    pub(super) expected_lower: Vec<u8>,
}

pub(super) async fn consume<B, F>(
    connect: Arc<F>,
    bare: Arc<B>,
    admin: Arc<KvWorkspaceStore<B>>,
    ticket: PackedCleanSourceTicket<B>,
    fixture: OriginalSnapshotFixture,
    released: PackedReleasedMountReference,
) where
    B: WorkspaceKvBackend,
    F: Fn(Arc<V3MountBudget>) -> Connection<B> + Send + Sync + 'static,
{
    let OriginalSnapshotFixture {
        client,
        upper,
        layout,
        object_keys,
        original,
        expected_lower,
    } = fixture;
    assert_eq!(ticket.released_mount(), released);
    assert_eq!(
        admin
            .inspect_original_clean_packed_mount(released.guard.workspace_id)
            .await
            .unwrap(),
        Some(released.clone()),
        "actual PCR remains discoverable after the Mount CR is removed"
    );
    let original_objects = original_object_bytes(&client, &object_keys).await;
    let original_reference = original.manifest_reference().clone();
    let scratch = tempfile::tempdir().unwrap();
    let snapshot_id = SnapshotId::new();
    let journal_id = JournalId::new();
    let next_head = LayerId::new();
    let mut request = PackedHeadlessSnapshotRequest::bounded_operator(
        PackedHeadlessSnapshotDescription {
            snapshot_id,
            snapshot_name: format!("actual-original-packed-v3-{snapshot_id}"),
            owner_id: None,
        },
        LeaseId::new(),
        journal_id,
        next_head,
        300_000_000_000,
        scratch.path().to_path_buf(),
    );
    request.producer = V3ProducerOptions {
        snapshot_id: [41; 32],
        root_dir_key: [43; 32],
        root_inode: 1,
        profile: AccessProfile::RandomSmallFile,
        size_classes: SizeClassTable::default(),
        build_policy: V3BuildPolicy {
            inline_data: false,
            p90: None,
            ..Default::default()
        },
        metadata_codec: PackedCodec::Raw,
        data_codec: PackedCodec::Raw,
    };
    request.graph_limits = V3IndexAuditLimits {
        max_objects: 4096,
        max_authenticated_bytes: 16 << 20,
        max_requested_bytes: 16 << 20,
        max_decoded_bytes: 16 << 20,
        max_frame_validation_steps: 4096,
        max_logical_hash_bytes: SCRATCH_BYTES,
        max_contexts: 4096,
        max_visits: 16_384,
        max_leaf_records: 4096,
        max_page_records: 4096,
        max_disk_bytes: SCRATCH_BYTES,
        sqlite_cache_bytes: 64 << 10,
        max_sql_operations: 1_000_000,
        max_sql_vm_steps: 4_000_000,
        chunk_bytes: 4096,
    };
    request.max_rows = 512;
    request.max_logical_bytes = SCRATCH_BYTES;
    request.max_data_bytes = SCRATCH_BYTES;
    request.scratch_disk_bytes = SCRATCH_BYTES;
    let result = match admin
        .publish_clean_packed_snapshot(ticket, client.clone(), upper, layout, request)
        .await
    {
        Ok(result) => result,
        Err(failure) => panic!(
            "actual original-to-packed-v3 publisher failed: {} (committed={})",
            failure.error(),
            failure.committed_result().is_some()
        ),
    };
    assert_eq!(result.snapshot_id, snapshot_id);
    assert_eq!(result.packed_carrier_revision, result.binding.base_revision);
    assert_ne!(
        result.packed_carrier_revision, result.native_sealed_source_revision,
        "final snapshot substituted its historical native audit source for the packed carrier"
    );

    // Publication cleanup closes its own ledger. Subsequent inspection/read is
    // a fresh operation with a fresh ledger, including actual transport/output.
    bare.shutdown_metadata_backend().await.unwrap();
    let read_budget = V3MountBudget::defaults();
    let bare = Arc::new(connect(read_budget.clone()).await.unwrap());
    let observer = Arc::new(
        KvWorkspaceStore::from_arc(bare.clone()).with_packed_reader_pin_budget(read_budget.clone()),
    );
    let persisted = observer.load_snapshot(snapshot_id).await.unwrap();
    assert_eq!(persisted.revision, result.packed_carrier_revision);
    assert_ne!(persisted.revision, result.native_sealed_source_revision);
    assert_eq!(
        observer
            .load_workspace(released.guard.workspace_id)
            .await
            .unwrap()
            .head_layer_id,
        next_head
    );
    assert!(
        observer
            .verify_original_packed_mount_for_cleanup(released.clone())
            .await
            .unwrap(),
        "original PCR cleanup must survive real source consumption and head advance"
    );
    let clean = observer
        .inspect_clean_published_view(released.guard.workspace_id)
        .await
        .unwrap()
        .expect("actual same-CAS headless finish receipt");
    assert_eq!(clean.snapshot_id, snapshot_id);
    assert_eq!(clean.binding, result.binding);
    assert_eq!(clean.head_epoch, result.binding.head_epoch);
    let repeated = observer
        .pin_clean_packed_snapshot(
            released.guard.workspace_id,
            crate::workspace_overlay::catalog::CreateSnapshot {
                snapshot_id: SnapshotId::new(),
                name: None,
                revision: result.packed_carrier_revision.clone(),
                owner_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(repeated.revision, result.packed_carrier_revision);
    let fork_id = crate::workspace_overlay::ids::WorkspaceId::new();
    observer
        .create_workspace_from_packed_carrier(crate::workspace_overlay::catalog::CreateWorkspace {
            workspace_id: fork_id,
            head_layer_id: LayerId::new(),
            base_revision: result.packed_carrier_revision.clone(),
            owner_id: None,
        })
        .await
        .unwrap();
    let untouched = observer
        .inspect_unmounted_packed_source(fork_id)
        .await
        .unwrap()
        .expect("actual empty fork with no writable lease history");
    assert_eq!(untouched.base_revision, result.packed_carrier_revision);
    let first_fork_snapshot = observer
        .pin_clean_packed_snapshot(
            fork_id,
            crate::workspace_overlay::catalog::CreateSnapshot {
                snapshot_id: SnapshotId::new(),
                name: None,
                revision: result.packed_carrier_revision.clone(),
                owner_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(first_fork_snapshot.revision, result.packed_carrier_revision);
    let current_key = format!("packed/v3/current/{}", released.guard.workspace_id).into_bytes();
    let (current_rows, _) = bare
        .get_many_consistent_with_time_bounded(&[current_key], point_limits(1))
        .await
        .unwrap();
    let current = PackedLowerBindingRecord::decode(current_rows[0].as_ref().unwrap()).unwrap();
    assert_eq!(current, result.binding);

    let authenticated = AuthenticatedV3Snapshot::open(&client, &result.binding.binding.manifest)
        .await
        .unwrap();
    assert_ne!(authenticated.manifest_reference(), &original_reference);
    let metadata = PackedV3ReadonlyMeta::from_v3_budget(
        client.clone(),
        authenticated,
        layout.chunk_size,
        0,
        read_budget.clone(),
    )
    .unwrap();
    read_snapshot_file(&metadata, "original-clean-native", PAYLOAD).await;
    read_snapshot_file(&metadata, "nonzero", &expected_lower).await;
    metadata.drain_packed_transport().await.unwrap();
    drop(metadata);

    // The original lower and upper object bytes remain immutable through the
    // actual registry/journal/head+binding publication and SnapshotRecord write.
    for (key, expected) in original_objects {
        assert_eq!(
            read_original_object(&client, &key).await,
            expected,
            "actual publication changed or removed an original object"
        );
    }
    let reopened_original = AuthenticatedV3Snapshot::open(&client, &original_reference)
        .await
        .unwrap();
    assert_eq!(reopened_original.manifest_reference(), &original_reference);
    let original_metadata = PackedV3ReadonlyMeta::from_v3_budget(
        client,
        reopened_original,
        layout.chunk_size,
        0,
        read_budget.clone(),
    )
    .unwrap();
    read_snapshot_file(&original_metadata, "nonzero", &expected_lower).await;
    original_metadata.drain_packed_transport().await.unwrap();
    drop(original_metadata);
    let subsequent = crate::workspace_overlay::lifecycle::WorkspaceMountSession::acquire_for_mount(
        observer.clone(),
        released.guard.workspace_id,
        released.guard.holder_generation.checked_add(2).unwrap(),
        Duration::from_secs(300),
        Duration::from_secs(60),
        false,
        read_budget.clone(),
    )
    .await
    .unwrap();
    assert!(
        observer
            .inspect_clean_published_view(released.guard.workspace_id)
            .await
            .unwrap()
            .is_none(),
        "a later actual grant invalidates published-clean status before its first mutation"
    );
    assert!(
        observer
            .inspect_original_clean_packed_mount(released.guard.workspace_id)
            .await
            .unwrap()
            .is_none()
    );
    // The lifecycle owns the real never-attached boundary; the fixture
    // cannot construct its abort token or use a plain lease-only release.
    subsequent.release().await.unwrap();
    assert!(
        observer
            .inspect_clean_published_view(released.guard.workspace_id)
            .await
            .unwrap()
            .is_none(),
        "plain release cannot restore the earlier headless clean proof"
    );
    assert!(
        observer
            .inspect_original_clean_packed_mount(released.guard.workspace_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        observer
            .verify_original_packed_mount_for_cleanup(released.clone())
            .await
            .unwrap(),
        "exact original cleanup must survive a subsequent actual writer generation"
    );
    drop(observer);
    bare.shutdown_metadata_backend().await.unwrap();
    read_budget.close();
    assert!(read_budget.state().used.iter().all(|bytes| *bytes == 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, Redis UUID namespace, original shutdown and actual packed-v3 publisher"]
async fn real_redis_original_packed_shutdown_to_headless_snapshot_persists_carrier_and_reads_both_sources()
 {
    redis(Case::HeadlessSnapshot).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, TiKV UUID namespace, original shutdown and actual packed-v3 publisher"]
async fn real_tikv_original_packed_shutdown_to_headless_snapshot_persists_carrier_and_reads_both_sources()
 {
    tikv(Case::HeadlessSnapshot).await;
}
