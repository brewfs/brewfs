//! Real authenticated packed bytes plus collector-fenced durable ownership.

use super::*;
use crate::cadapter::client::ObjectClient;
use crate::cadapter::s3::S3Backend;
use crate::workspace_overlay::entity::EntityError;
use crate::workspace_overlay::entity::data::EntityData;
use crate::workspace_overlay::entity::keys::EntityKeys;
use crate::workspace_overlay::entity::lifecycle::{DeletedLowerOwnerProof, Lifecycle};
use crate::workspace_overlay::entity::memory::MemoryEntityBackend;
use crate::workspace_overlay::entity::model::{ChunkMap, EntityDentry, EntityKind, LowerBinding};
use crate::workspace_overlay::entity::namespace::{Credentials, EntityNamespace};
use crate::workspace_overlay::entity::packed_lower::PackedLower;
use crate::workspace_overlay::entity::resolver::{ImmutableLower, ImmutableLowerData};
use crate::workspace_overlay::entity::test_objects;
use crate::workspace_overlay::packed_v3::wire005::{
    AuthenticatedV3Snapshot, V3IndexReader, V3ProducerOptions, V3SnapshotProducer,
};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, GroupMeta, GroupMetaEntry, GroupMetaExtent, PackedCodec, PackedFrameInput,
    PackedGroupInput, SizeClass, SizeClassTable,
};
use crate::workspace_overlay::publish::binding::VerifiedPackedLower;

struct Fixture<B> {
    _directory: tempfile::TempDir,
    client: ObjectClient<S3Backend>,
    objects: S3Backend,
    snapshot: AuthenticatedV3Snapshot,
    store: Arc<KvWorkspaceStore<B>>,
    source: PackedReaderPin,
    binding: LowerBinding,
    payload: Vec<u8>,
}

async fn fixture<B: WorkspaceKvBackend>(backend: Arc<B>) -> Fixture<B> {
    let objects = test_objects::backend().await;
    let client = ObjectClient::new(objects.clone());
    // Local temporary storage is only the producer's bounded metadata spool;
    // every packed object and upper payload goes through the real S3 adapter.
    let directory = tempfile::tempdir().unwrap();
    let payload: Vec<u8> = (0..4096).map(|i| (i % 251 + 1) as u8).collect();
    let group = PackedGroupInput {
        group_id: 1,
        parent_dir_key: [7; 32],
        metadata: GroupMeta::new(vec![GroupMetaEntry {
            name: b"nonzero".to_vec(),
            inode: 400,
            kind: 1,
            mode: 0o100644,
            uid: 1,
            gid: 2,
            rdev: 0,
            nlink: 1,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            size: payload.len() as u64,
            flags: 0,
            inline_data: Arc::from([]),
            extents: vec![GroupMetaExtent {
                file_offset: 0,
                logical_len: payload.len() as u32,
                frame_ordinal: 0,
                raw_offset: 0,
                raw_len: payload.len() as u32,
            }],
        }])
        .unwrap()
        .encode()
        .unwrap(),
        frame_ordinals: vec![0],
        entry_count: 1,
        file_count: 1,
        layout_profile: AccessProfile::RandomSmallFile,
    };
    let mut producer = V3SnapshotProducer::new(
        client.clone(),
        directory.path(),
        format!("entity-packed-test/{}", Uuid::new_v4()),
        V3ProducerOptions {
            snapshot_id: [9; 32],
            root_dir_key: [7; 32],
            root_inode: 1,
            profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            build_policy: Default::default(),
            metadata_codec: PackedCodec::Raw,
            data_codec: PackedCodec::Raw,
        },
    )
    .await
    .unwrap();
    producer
        .add_container(
            1,
            &[group],
            &[PackedFrameInput {
                raw: payload.clone(),
                size_class: SizeClass::Tiny,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 0,
            }],
            &[1],
        )
        .await
        .unwrap();
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
        .await
        .unwrap();
    let reader = V3IndexReader::new(client.clone(), 0);
    let verified = VerifiedPackedLower::from_authenticated_snapshot(&snapshot, &reader)
        .await
        .unwrap();
    let store = Arc::new(
        KvWorkspaceStore::from_arc(backend)
            .with_packed_reader_pin_budget(V3MountBudget::defaults()),
    );
    let initial = request(store.as_ref(), verified).await;
    let binding_record = store
        .install_packed_lower_binding(initial.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: binding_record.head_epoch,
        ..initial.guard
    };
    let layers = store
        .load_layer_chain(guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let acquired = store
        .acquire_packed_reader_pin(&AcquirePackedReaderPin {
            slot: 0,
            expected_slot_generation: 0,
            pin_id: Uuid::new_v4(),
            owner_id: "entity-adapter-test".into(),
            holder_generation: 1,
            ttl_ns: 10_000_000_000,
            gc_grace_ns: 0,
            guard,
            expected_layers: layers,
            expected_binding: binding_record.clone(),
        })
        .await
        .unwrap();
    let source = (*acquired).clone();
    let binding = LowerBinding {
        manifest_key: snapshot.manifest_reference().key.as_bytes().to_vec(),
        manifest_digest: snapshot.manifest_reference().digest,
        root_ino: 1,
        max_ino: binding_record.highest_inode,
    };
    Fixture {
        _directory: directory,
        client,
        objects,
        snapshot,
        store,
        source,
        binding,
        payload,
    }
}

async fn deletion_proof(
    volume: Uuid,
    namespace: Uuid,
    binding: &LowerBinding,
) -> DeletedLowerOwnerProof {
    let lifecycle = Lifecycle::new(
        Arc::new(MemoryEntityBackend::new(1)),
        EntityKeys::new(volume, namespace),
    );
    lifecycle.provision(binding).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), 1_000_000).await.unwrap();
    let maintenance = lifecycle.enter_maintenance(&session).await.unwrap();
    assert!(matches!(
        lifecycle.deleted_lower_owner_proof(&maintenance).await,
        Err(EntityError::Busy)
    ));
    lifecycle.begin_discard(&maintenance).await.unwrap();
    lifecycle.discard_empty(&maintenance).await.unwrap();
    lifecycle.discard_empty(&maintenance).await.unwrap();
    lifecycle
        .deleted_lower_owner_proof(&maintenance)
        .await
        .unwrap()
}

/// Exercise the actual packed lower through copy-up and durable upper data,
/// then obtain release authority from that same namespace's completed discard.
async fn packed_copy_up_contract<B: WorkspaceKvBackend>(
    lower: Arc<PackedLower<S3Backend, B>>,
    objects: S3Backend,
    volume: Uuid,
    namespace_id: Uuid,
    payload: &[u8],
) -> DeletedLowerOwnerProof {
    let backend = Arc::new(MemoryEntityBackend::new(1));
    let keys = EntityKeys::new(volume, namespace_id);
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    lifecycle.provision(lower.binding()).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), 1_000_000).await.unwrap();
    let namespace = EntityNamespace::new(backend, lower.clone(), keys, session.clone());
    let credentials = Credentials {
        uid: 0,
        gid: 0,
        groups: vec![0],
        umask: 0o022,
    };
    let handle = Arc::new(
        namespace
            .open(1, b"nonzero", 6, &credentials)
            .await
            .unwrap(),
    );
    let data = EntityData::new(namespace, Arc::new(objects), 1024)
        .await
        .unwrap();
    assert_eq!(
        data.read(handle.clone(), 0, payload.len()).await.unwrap(),
        payload
    );
    let replacement = bytes::Bytes::from_static(b"cross-boundary replacement");
    let written = data
        .write(handle.clone(), 1000, replacement.clone(), false)
        .await
        .unwrap();
    assert_eq!(written.written, replacement.len());
    let mut expected = payload.to_vec();
    expected[1000..1000 + replacement.len()].copy_from_slice(&replacement);
    assert_eq!(
        data.read(handle.clone(), 0, expected.len()).await.unwrap(),
        expected
    );
    data.fsync(handle.clone()).await.unwrap();
    assert_eq!(
        lower.read_range(400, 0, payload.len()).await.unwrap(),
        payload
    );

    data.truncate(handle.clone(), 1011).await.unwrap();
    data.truncate(handle.clone(), payload.len() as u64)
        .await
        .unwrap();
    expected[1011..].fill(0);
    assert_eq!(
        data.read(handle.clone(), 0, expected.len()).await.unwrap(),
        expected
    );
    data.fsync(handle.clone()).await.unwrap();
    drop(handle);
    let maintenance = lifecycle.enter_maintenance(&session).await.unwrap();
    let mut completed = false;
    for _ in 0..32 {
        if data.discard_step(&maintenance).await.unwrap() {
            completed = true;
            break;
        }
    }
    assert!(completed, "bounded copy-up fixture discard did not finish");
    assert!(data.discard_step(&maintenance).await.unwrap());
    lifecycle
        .deleted_lower_owner_proof(&maintenance)
        .await
        .unwrap()
}

async fn adapter_contract<B: WorkspaceKvBackend>(fixture: &Fixture<B>) {
    let volume = Uuid::new_v4();
    let namespace = Uuid::new_v4();
    let old_roots = fixture.store.packed_reader_pin_roots().await.unwrap();
    let lower = PackedLower::open(
        fixture.store.clone(),
        fixture.client.clone(),
        volume,
        namespace,
        &fixture.source,
        fixture.binding.clone(),
    )
    .await
    .unwrap();
    assert!(
        !fixture
            .store
            .backend
            .compare_and_swap(&old_roots.checks, &[])
            .await
            .unwrap()
    );
    assert_eq!(
        lower.inode(1).await.unwrap().unwrap().kind,
        EntityKind::Directory
    );
    let inode = lower.inode(400).await.unwrap().unwrap();
    assert_eq!(
        (inode.uid, inode.gid, inode.mode, inode.size),
        (1, 2, 0o644, fixture.payload.len() as u64)
    );
    assert_eq!(
        lower.dentry(1, b"nonzero").await.unwrap(),
        Some(EntityDentry::Present {
            ino: 400,
            kind: EntityKind::File
        })
    );
    assert!(lower.dentry(1, b"absent").await.unwrap().is_none());
    assert_eq!(lower.readdir(1).await.unwrap().len(), 1);
    assert_eq!(
        lower.read_range(400, 7, 33).await.unwrap(),
        fixture.payload[7..40]
    );
    assert!(
        lower
            .read_range(400, fixture.payload.len() as u64, 1)
            .await
            .is_err()
    );
    assert_eq!(
        lower.chunk(400, 0, 8192).await.unwrap(),
        Some(
            ChunkMap::inherited(8192, &fixture.binding, 400, 0, fixture.payload.len() as u64)
                .unwrap()
        )
    );

    fixture
        .store
        .release_packed_reader_pin(&fixture.source, Uuid::new_v4())
        .await
        .unwrap();
    let roots = fixture.store.packed_reader_pin_roots().await.unwrap();
    assert_eq!(roots.bindings, vec![fixture.source.binding.clone()]);
    assert!(
        roots
            .native_roots
            .contains(&fixture.source.binding.base_revision.layer_id)
    );
    drop(lower);
    // Simulate loss of all process-local store state, including its configured
    // pin budget. The original reader lease has already been released.
    let reopened_store = Arc::new(KvWorkspaceStore::from_arc(fixture.store.backend.clone()));
    let lower = Arc::new(
        PackedLower::reopen(
            reopened_store,
            fixture.client.clone(),
            volume,
            namespace,
            fixture.binding.clone(),
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        lower
            .read_range(400, 0, fixture.payload.len())
            .await
            .unwrap(),
        fixture.payload
    );
    let foreign = deletion_proof(Uuid::new_v4(), namespace, &fixture.binding).await;
    assert!(matches!(
        fixture
            .store
            .release_packed_entity_lower(lower.owner(), &foreign)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    let proof = packed_copy_up_contract(
        lower.clone(),
        fixture.objects.clone(),
        volume,
        namespace,
        &fixture.payload,
    )
    .await;
    fixture
        .store
        .release_packed_entity_lower(lower.owner(), &proof)
        .await
        .unwrap();
    fixture
        .store
        .release_packed_entity_lower(lower.owner(), &proof)
        .await
        .unwrap();
    assert!(
        fixture
            .store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .is_empty()
    );
    assert!(matches!(
        lower.read_range(400, 0, 1).await,
        Err(EntityError::Fenced)
    ));
    assert!(
        PackedLower::reopen(
            fixture.store.clone(),
            fixture.client.clone(),
            volume,
            namespace,
            fixture.binding.clone()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_* and AWS credentials"]
async fn packed_entity_lower_real_bytes_ownership_restart_and_terminal_release() {
    adapter_contract(&fixture(Arc::new(PinMemoryBackend::default())).await).await;
}

#[tokio::test]
#[ignore = "requires isolated real RustFS; run tools/testing/run_entity_data_tests.sh"]
async fn packed_entity_lower_commit_before_upload_reads_and_drains_real_objects() {
    use crate::workspace_overlay::entity::writeback::WritebackConfig;
    let fixture = fixture(Arc::new(PinMemoryBackend::default())).await;
    let volume = Uuid::new_v4();
    let namespace_id = Uuid::new_v4();
    let keys = EntityKeys::new(volume, namespace_id);
    let lower = Arc::new(
        PackedLower::open(
            fixture.store.clone(),
            fixture.client.clone(),
            volume,
            namespace_id,
            &fixture.source,
            fixture.binding.clone(),
        )
        .await
        .unwrap(),
    );
    let backend = Arc::new(MemoryEntityBackend::new(1));
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    lifecycle.provision(lower.binding()).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), 1_000_000).await.unwrap();
    lower.bind_session(&session, &keys, 1024).await.unwrap();
    let namespace = EntityNamespace::new(backend, lower, keys, session.clone());
    let spool = tempfile::tempdir().unwrap();
    let data = EntityData::new_with_writeback(
        namespace.clone(),
        Arc::new(fixture.objects.clone()),
        1024,
        Some(WritebackConfig {
            directory: spool.path().into(),
            capacity_bytes: 1 << 20,
            min_free_bytes: 0,
        }),
    )
    .await
    .unwrap();
    let credentials = Credentials {
        uid: 0,
        gid: 0,
        groups: vec![],
        umask: 0,
    };
    let handle = Arc::new(
        namespace
            .open(1, b"nonzero", 6, &credentials)
            .await
            .unwrap(),
    );
    let patch = bytes::Bytes::from_static(b"entity asynchronous packed upper");
    data.write(handle.clone(), 1000, patch.clone(), false)
        .await
        .unwrap();
    let mut expected = fixture.payload;
    expected[1000..1000 + patch.len()].copy_from_slice(&patch);
    assert_eq!(
        data.read(handle.clone(), 0, expected.len()).await.unwrap(),
        expected
    );
    data.fsync(handle.clone()).await.unwrap();
    drop(handle);
    data.drain_remote(std::time::Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(data.writeback_status().await.unwrap().pending_objects, 0);
    lifecycle.enter_maintenance(&session).await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_* and AWS credentials"]
async fn packed_entity_lower_bound_memos_are_local_coalesced_and_session_fenced() {
    use crate::cadapter::read_observer::{Engine, Origin, Phase, ReadObserver};

    let backend = Arc::new(PinMemoryBackend::default());
    let fixture = fixture(backend.clone()).await;
    let observer = Arc::new(ReadObserver::default());
    let client = ObjectClient::new(fixture.objects.clone()).with_read_observer(
        observer.clone(),
        Engine::PackedV3,
        Phase::Runtime,
        Origin::Demand,
    );
    let volume = Uuid::new_v4();
    let namespace = Uuid::new_v4();
    let mut lower = Arc::new(
        PackedLower::open(
            fixture.store.clone(),
            client,
            volume,
            namespace,
            &fixture.source,
            fixture.binding.clone(),
        )
        .await
        .unwrap(),
    );
    Arc::get_mut(&mut lower)
        .unwrap()
        .set_memo_capacity_for_test(8192);
    let keys = EntityKeys::new(volume, namespace);
    let other_lifecycle = Lifecycle::new(Arc::new(MemoryEntityBackend::new(1)), keys.clone());
    let mut wrong_binding = fixture.binding.clone();
    wrong_binding.manifest_digest[0] ^= 1;
    other_lifecycle.provision(&wrong_binding).await.unwrap();
    let other_session = other_lifecycle
        .acquire(Uuid::new_v4(), 1_000_000)
        .await
        .unwrap();
    assert!(matches!(
        lower.bind_session(&other_session, &keys, 8192).await,
        Err(EntityError::Fenced)
    ));

    let lifecycle = Lifecycle::new(Arc::new(MemoryEntityBackend::new(1)), keys.clone());
    lifecycle.provision(&fixture.binding).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), 1_000_000).await.unwrap();
    lower.bind_session(&session, &keys, 8192).await.unwrap();
    assert!(matches!(
        lower.bind_session(&session, &keys, 8192).await,
        Err(EntityError::Fenced)
    ));
    let reads_before = backend.bounded_reads.load(Ordering::SeqCst);
    let object_rows_before = observer.snapshot().rows;
    for _ in 0..16 {
        assert_eq!(lower.inode(400).await.unwrap().unwrap().size, 4096);
        assert!(lower.dentry(1, b"nonzero").await.unwrap().is_some());
        assert!(lower.dentry(1, b"cached-absent").await.unwrap().is_none());
        assert!(
            lower
                .xattr(400, b"system.posix_acl_access")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            lower
                .xattr(400, b"system.posix_acl_default")
                .await
                .unwrap()
                .is_none()
        );
        assert!(lower.chunk(400, 0, 8192).await.unwrap().is_some());
        assert_eq!(lower.readdir(1).await.unwrap().len(), 1);
    }
    assert_eq!(backend.bounded_reads.load(Ordering::SeqCst), reads_before);
    assert_eq!(observer.snapshot().rows, object_rows_before);

    // A cold entity-facing inode memo has one elected async load, even when
    // many callers arrive concurrently. Its bytes still come from real RustFS.
    lower.clear_memos_for_test().await;
    let fills = lower.memo_fill_count_for_test();
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let lower = lower.clone();
        tasks.push(tokio::spawn(async move { lower.inode(400).await.unwrap() }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap().unwrap().ino, 400);
    }
    assert_eq!(lower.memo_fill_count_for_test() - fills, 1);
    let fills = lower.memo_fill_count_for_test();
    for _ in 0..2 {
        assert!(matches!(
            lower.dentry(400, b"not-a-directory").await,
            Err(EntityError::NotDirectory)
        ));
    }
    assert_eq!(lower.memo_fill_count_for_test() - fills, 2);
    lower.readdir(1).await.unwrap();
    for index in 0..128 {
        let name = format!("missing-{index:03}-{}", "n".repeat(200));
        assert!(lower.dentry(1, name.as_bytes()).await.unwrap().is_none());
    }
    assert!(lower.retained_memo_bytes_for_test().await <= 8192);
    assert_eq!(lower.inode(400).await.unwrap().unwrap().size, 4096);
    lifecycle.enter_maintenance(&session).await.unwrap();
    assert!(matches!(lower.inode(400).await, Err(EntityError::Fenced)));
    assert!(matches!(
        lower.read_range(400, 0, 1).await,
        Err(EntityError::Fenced)
    ));
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_* and AWS credentials"]
async fn packed_entity_lower_rejects_descriptor_and_manifest_tampering() {
    let fixture = fixture(Arc::new(PinMemoryBackend::default())).await;
    let volume = Uuid::new_v4();
    for changed in 0..4 {
        let mut binding = fixture.binding.clone();
        match changed {
            0 => binding.max_ino += 1,
            1 => binding.root_ino += 1,
            2 => binding.manifest_digest[0] ^= 1,
            _ => binding.manifest_key.push(b'x'),
        }
        let namespace = Uuid::new_v4();
        assert!(
            PackedLower::open(
                fixture.store.clone(),
                fixture.client.clone(),
                volume,
                namespace,
                &fixture.source,
                binding
            )
            .await
            .is_err()
        );
        assert!(
            fixture
                .store
                .load_packed_entity_lower(volume, namespace)
                .await
                .is_err()
        );
    }
    let reference = fixture.snapshot.manifest_reference();
    let mut bytes = fixture
        .client
        .get_object(&reference.key)
        .await
        .unwrap()
        .unwrap();
    bytes[crate::workspace_overlay::packed_v3::wire005::V3_HEADER_LEN + 10] ^= 1;
    fixture
        .client
        .put_object(&reference.key, &bytes)
        .await
        .unwrap();
    assert!(
        PackedLower::open(
            fixture.store.clone(),
            fixture.client.clone(),
            volume,
            Uuid::new_v4(),
            &fixture.source,
            fixture.binding.clone()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_* and AWS credentials"]
async fn packed_entity_lower_payload_tampering_is_error_never_zero_fallback() {
    let fixture = fixture(Arc::new(PinMemoryBackend::default())).await;
    let lower = PackedLower::open(
        fixture.store.clone(),
        fixture.client.clone(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        &fixture.source,
        fixture.binding.clone(),
    )
    .await
    .unwrap();
    let reader = V3IndexReader::new(fixture.client.clone(), 0);
    let container = fixture.snapshot.container_ref(&reader, 0).await.unwrap();
    let mut bytes = fixture
        .client
        .get_object(&container.key)
        .await
        .unwrap()
        .unwrap();
    let start = bytes
        .windows(fixture.payload.len())
        .position(|window| window == fixture.payload)
        .unwrap();
    bytes[start + 100] ^= 1;
    fixture
        .client
        .put_object(&container.key, &bytes)
        .await
        .unwrap();
    let result = lower.read_range(400, 0, fixture.payload.len()).await;
    // open() authenticated only metadata; this reader has not demanded any
    // payload before the overwrite. The first full read covers the mutated
    // byte and reaches the wire005 coordinator's frame authentication.
    // read_inode_range currently wraps that typed checksum error as Backend.
    // Require this precise failure, not any transport error or zero fallback.
    assert!(
        matches!(
            &result,
            Err(EntityError::Backend(message) | EntityError::Corrupt(message))
                if message.contains("packed v3 hash mismatch for wire005 coalesced frame")
        ),
        "tampered packed payload must fail authentication: {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_* and AWS credentials"]
async fn packed_entity_lower_promotion_expiry_and_lost_replies_are_fenced() {
    let backend = PinMemoryBackend::default();
    let fixture = fixture(Arc::new(backend.clone())).await;
    let volume = Uuid::new_v4();
    let namespace = Uuid::new_v4();
    let original_now = backend.now.load(Ordering::SeqCst);
    *backend.validation_clock.lock().await = Some(fixture.source.expires_at_ns);
    assert!(matches!(
        fixture
            .store
            .retain_packed_entity_lower(volume, namespace, &fixture.source)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert!(
        fixture
            .store
            .load_packed_entity_lower(volume, namespace)
            .await
            .is_err()
    );
    *backend.validation_clock.lock().await = None;
    // The one-shot CAS clock injection persists into `now`; restore this
    // fixture's time before the independent lost-response scenario.
    backend.now.store(original_now, Ordering::SeqCst);
    backend.lose_reply.store(true, Ordering::SeqCst);
    let result = fixture
        .store
        .retain_packed_entity_lower(volume, namespace, &fixture.source)
        .await;
    assert!(
        matches!(&result, Err(WorkspaceError::Backend(_))),
        "promotion must surface the injected lost reply: {result:?}"
    );
    let owner = fixture
        .store
        .retain_packed_entity_lower(volume, namespace, &fixture.source)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .len(),
        2
    );
    let proof = deletion_proof(volume, namespace, &fixture.binding).await;
    backend.lose_reply.store(true, Ordering::SeqCst);
    let result = fixture
        .store
        .release_packed_entity_lower(&owner, &proof)
        .await;
    assert!(
        matches!(&result, Err(WorkspaceError::Backend(_))),
        "release must surface the injected lost reply: {result:?}"
    );
    fixture
        .store
        .release_packed_entity_lower(&owner, &proof)
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .retain_packed_entity_lower(volume, namespace, &fixture.source)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(
        fixture
            .store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .len(),
        1
    );
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_* and AWS credentials"]
async fn packed_entity_lower_retains_native_roots_after_original_reader_released() {
    let backend = PinMemoryBackend::default();
    let fixture = fixture(Arc::new(backend.clone())).await;
    fixture
        .store
        .retain_packed_entity_lower(Uuid::new_v4(), Uuid::new_v4(), &fixture.source)
        .await
        .unwrap();
    fixture
        .store
        .release_packed_reader_pin(&fixture.source, Uuid::new_v4())
        .await
        .unwrap();
    fixture
        .store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: fixture.source.binding.workspace_id,
            force_fence_lease: true,
        })
        .await
        .unwrap();
    // Isolate the new durable owner from independent publication roots. This
    // test-only deletion does not claim to exercise physical graph retirement.
    {
        let mut rows = backend.rows.lock().await;
        rows.remove(&packed_current_key(fixture.source.binding.workspace_id));
        rows.remove(&packed_history_key(fixture.source.binding.workspace_id, 1));
        let generation =
            next_packed_root_generation(&rows.get(PACKED_ROOT_GENERATION_KEY).cloned()).unwrap();
        rows.insert(
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            encode(&generation).unwrap(),
        );
    }
    let snapshot = fixture.store.gc_snapshot(100_000_000_000, 0).await.unwrap();
    assert!(
        snapshot
            .root_layers
            .contains(&fixture.source.binding.base_revision.layer_id)
    );
    assert!(
        snapshot
            .root_layers
            .contains(&fixture.source.binding.head_layer_id)
    );
    assert!(matches!(
        fixture
            .store
            .delete_layer_metadata(DeleteLayerMetadata {
                layer_ids: vec![
                    fixture.source.binding.base_revision.layer_id,
                    fixture.source.binding.head_layer_id
                ],
                now_ns: 100_000_000_000,
                lease_grace_ns: 0,
            })
            .await,
        Err(WorkspaceError::Busy)
    ));
}

#[tokio::test]
#[ignore = "requires isolated Redis and RustFS via BREWFS_TEST_REDIS_URL, BREWFS_PACKED_GRAPH_TEST_S3_* and AWS credentials"]
async fn real_redis_packed_entity_lower_owner_and_authenticated_reads() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let backend = Arc::new(
        RedisWorkspaceBackend::connect(&url, &format!("entity-lower-{}", Uuid::new_v4()))
            .await
            .unwrap(),
    );
    let worker = backend.clone();
    let outcome = tokio::spawn(async move {
        adapter_contract(&fixture(worker).await).await;
    })
    .await;
    let entries = backend.scan_prefix(b"").await.unwrap();
    let checks: Vec<_> = entries
        .iter()
        .map(|entry| KvCheck {
            key: entry.key.clone(),
            expected: Some(entry.value.clone()),
        })
        .collect();
    let writes: Vec<_> = entries
        .into_iter()
        .map(|entry| KvWrite::Delete { key: entry.key })
        .collect();
    assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
    if let Err(error) = outcome {
        std::panic::resume_unwind(error.into_panic());
    }
}
