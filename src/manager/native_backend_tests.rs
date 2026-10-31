//! Actual shared-control Manager children must retain nonempty P1 namespace
//! and data. Object bytes use the ordinary in-memory object sink; catalog and
//! native publication transactions run against the selected real service.

use super::backend_tests::{Remote, close, configure, register, settled};
use super::*;
use crate::chunk::BlockStore;
use crate::native_base::runtime::{
    FROZEN_METADATA_FEATURE, NativeRuntimeCapabilities, NativeVolumeHeader,
    admin_test_support::{
        LOWER_BYTES, LOWER_SLICE, NativeP1RuntimeFixture, Seed, seed_catalog_lower,
    },
    initialize_volume, p1_storage_namespace_id,
};
use crate::native_base::write::{
    keys::Keys,
    receipts::MemorySink,
    store::{ControlStore, Txn},
};
use crate::workspace_overlay::{
    catalog::{CreateSnapshot, CreateWorkspace, MarkDeleting, ReleaseLease},
    ids::SnapshotId,
    stores::{
        kv_backend::WorkspaceKvBackend, kv_store::KvWorkspaceStore,
        manager_guard::MANAGER_CONTROLLER_KEY, redis::RedisWorkspaceBackend,
        tikv::TiKvWorkspaceBackend,
    },
};
use futures::FutureExt;

async fn rejected_registration(manager: &Manager, volume: Uuid, key: &str, code: &str) {
    match manager
        .create_volume(key, register(volume, "workspace-native-v2"))
        .await
    {
        Err(error) => assert_eq!(error.code, code, "registration case {key}: {error:?}"),
        Ok(operation) => {
            let rejected = settled(manager, operation.id).await;
            assert_eq!(
                rejected.state,
                OperationState::Failed,
                "registration case {key}: {rejected:?}"
            );
            assert_eq!(
                rejected.error.as_ref().unwrap().code,
                code,
                "registration case {key}: {rejected:?}"
            );
        }
    }
    assert!(manager.list_volumes().await.unwrap().is_empty());
}

async fn assert_contract<B: WorkspaceKvBackend>(
    manager: &Manager,
    remote: Remote,
    catalog: Arc<KvWorkspaceStore<B>>,
    fixture: &NativeP1RuntimeFixture,
    seed: &Seed,
) {
    let header = &fixture.header;
    let volume = Uuid::from_bytes(header.volume_id);
    let keys = Keys::new(&header.volume_id);
    let locator = Keys::volume_header(&fixture.namespace);
    // Each rejection is before a controller claim or volume binding. Restore
    // exactly the owned header between cases; no source data changes.
    for (label, changed, code) in [
        (
            "wrong-volume",
            NativeVolumeHeader {
                volume_id: *Uuid::new_v4().as_bytes(),
                ..header.clone()
            },
            "conflict",
        ),
        (
            "wrong-namespace",
            NativeVolumeHeader {
                storage_namespace_id: [31; 16],
                ..header.clone()
            },
            "conflict",
        ),
        (
            "frozen",
            NativeVolumeHeader {
                required_features: FROZEN_METADATA_FEATURE,
                ..header.clone()
            },
            "unsupported",
        ),
    ] {
        fixture
            .control
            .run(Txn::new().put(locator.clone(), changed.encode()))
            .await
            .unwrap();
        rejected_registration(manager, volume, label, code).await;
        fixture
            .control
            .run(Txn::new().put(locator.clone(), header.encode()))
            .await
            .unwrap();
    }
    fixture
        .control
        .run(Txn::new().put(locator.clone(), b"malformed-native-header".to_vec()))
        .await
        .unwrap();
    rejected_registration(manager, volume, "malformed", "conflict").await;
    fixture
        .control
        .run(Txn::new().put(locator, header.encode()))
        .await
        .unwrap();
    let published = keys.fork_base(seed.root_workspace.as_bytes());
    fixture
        .control
        .run(Txn::new().put(published.clone(), vec![1]))
        .await
        .unwrap();
    rejected_registration(manager, volume, "published-lower", "unsupported").await;
    fixture
        .control
        .run(Txn::new().delete(published))
        .await
        .unwrap();
    let bucket_child = catalog
        .create_workspace(CreateWorkspace {
            workspace_id: WorkspaceId::new(),
            head_layer_id: LayerId::new(),
            base_revision: seed.revision.clone(),
            owner_id: None,
            upper_bucket: Some("native-unsupported-upper-bucket".into()),
        })
        .await
        .unwrap();
    rejected_registration(manager, volume, "upper-bucket", "unsupported").await;
    // Logical removal only; no object bucket or shared lower is touched.
    catalog
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: bucket_child.workspace_id,
            force_fence_lease: false,
        })
        .await
        .unwrap();
    let raw = remote.raw(&fixture.namespace).await;
    assert!(raw.get(MANAGER_CONTROLLER_KEY).await.unwrap().is_none());
    raw.shutdown_metadata_backend().await.unwrap();

    let registered = manager
        .create_volume("register-nonempty", register(volume, "workspace-native-v2"))
        .await
        .unwrap();
    assert_eq!(
        settled(manager, registered.id).await.state,
        OperationState::Succeeded
    );
    let revision = manager
        .inspect_revision(volume, seed.revision.layer_id)
        .await
        .unwrap();
    let Revision::Native(native) = &revision else {
        panic!("native revision lost its explicit identity")
    };
    assert_eq!(native.catalog_revision, seed.revision);
    assert_eq!(native.storage_namespace_id, header.storage_namespace_id);
    let fork_request = ForkRequest {
        revision,
        count: 2,
        owner: Some("real-native-child".into()),
    };
    let accepted = manager
        .fork(volume, "two-native-children", fork_request.clone())
        .await
        .unwrap();
    let completed = settled(manager, accepted.id).await;
    assert_eq!(completed.state, OperationState::Succeeded, "{completed:?}");
    assert!(
        completed
            .items
            .iter()
            .all(|item| item.metadata_ready && item.remote_ready.is_none())
    );
    let ids = completed
        .items
        .iter()
        .map(|item| item.workspace_id.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    assert_eq!(
        manager
            .fork(volume, "two-native-children", fork_request)
            .await
            .unwrap()
            .id,
        accepted.id
    );
    for id in &ids {
        let child = manager.inspect_workspace(volume, *id).await.unwrap();
        assert_eq!(child.base_revision.catalog_revision(), &seed.revision);
        assert!(child.record.upper_bucket.is_none());
        assert_eq!(child.native.as_ref().unwrap()["domain"]["state"], "active");
    }

    // Registration is not permanent admission for a changed native context.
    // Revalidate before the catalog marker, leaving Active intact on refusal.
    for (key, value, operation_key, code) in [
        (
            keys.fork_base(ids[0].as_bytes()),
            vec![1],
            "delete-published-context",
            "unsupported",
        ),
        (
            keys.writer_lease(ids[0].as_bytes()),
            b"native-writer-present".to_vec(),
            "delete-native-writer",
            "conflict",
        ),
    ] {
        fixture
            .control
            .run(
                Txn::new()
                    .check_absent(key.clone())
                    .put(key.clone(), value.clone()),
            )
            .await
            .unwrap();
        let operation = manager
            .delete_workspace(volume, ids[0], operation_key)
            .await
            .unwrap();
        let rejected = settled(manager, operation.id).await;
        assert_eq!(rejected.state, OperationState::Failed, "{rejected:?}");
        assert_eq!(rejected.error.unwrap().code, code);
        assert_eq!(
            catalog.load_workspace(ids[0]).await.unwrap().state,
            WorkspaceState::Active
        );
        assert!(
            fixture
                .control
                .get(&keys.workspace_deleted(ids[0].as_bytes()))
                .await
                .unwrap()
                .is_none()
        );
        fixture
            .control
            .run(Txn::new().check_bytes(key.clone(), value).delete(key))
            .await
            .unwrap();
    }

    let (left, left_lease) = fixture.open(catalog.clone(), ids[0], 71).await;
    let (right, right_lease) = fixture.open(catalog.clone(), ids[1], 72).await;
    let inode = seed.inode as u64;
    assert_eq!(
        left.read(inode, 0, LOWER_BYTES.len()).await.unwrap(),
        LOWER_BYTES
    );
    assert_eq!(
        right.read(inode, 0, LOWER_BYTES.len()).await.unwrap(),
        LOWER_BYTES
    );
    left.write(inode, 0, b"CHILD").await.unwrap();
    left.fsync(inode).await.unwrap();
    let mut expected = LOWER_BYTES.to_vec();
    expected[..5].copy_from_slice(b"CHILD");
    assert_eq!(left.read(inode, 0, expected.len()).await.unwrap(), expected);
    assert_eq!(
        right.read(inode, 0, LOWER_BYTES.len()).await.unwrap(),
        LOWER_BYTES
    );
    catalog
        .release_lease(ReleaseLease {
            lease_id: left_lease.lease_id,
            holder_generation: left_lease.holder_generation,
        })
        .await
        .unwrap();
    let (reopened, reopened_lease) = fixture.open(catalog.clone(), ids[0], 73).await;
    assert_eq!(
        reopened.read(inode, 0, expected.len()).await.unwrap(),
        expected,
        "independent runtime must read committed child bytes"
    );
    let leased_delete = manager
        .delete_workspace(volume, ids[0], "delete-child")
        .await
        .unwrap();
    assert_eq!(
        settled(manager, leased_delete.id).await.state,
        OperationState::Failed
    );
    assert!(
        fixture
            .control
            .get(&keys.workspace_deleted(ids[0].as_bytes()))
            .await
            .unwrap()
            .is_none()
    );
    catalog
        .create_snapshot(CreateSnapshot {
            snapshot_id: SnapshotId::new(),
            name: None,
            revision: seed.revision.clone(),
            owner_id: None,
        })
        .await
        .unwrap();
    catalog
        .release_lease(ReleaseLease {
            lease_id: reopened_lease.lease_id,
            holder_generation: reopened_lease.holder_generation,
        })
        .await
        .unwrap();
    let retry = manager
        .delete_workspace(volume, ids[0], "delete-child")
        .await
        .unwrap();
    assert_eq!(retry.id, leased_delete.id);
    let deleted = settled(manager, retry.id).await;
    assert_eq!(deleted.state, OperationState::Succeeded, "{deleted:?}");
    assert!(deleted.items[0].deletion_marked);
    assert!(!deleted.items[0].physical_reclaimed);
    let view = manager.inspect_workspace(volume, ids[0]).await.unwrap();
    assert_eq!(
        view.native.as_ref().unwrap()["domain"]["state"],
        "quarantined"
    );
    reopened.write(inode, 0, b"STALE").await.unwrap();
    assert!(reopened.fsync(inode).await.is_err());
    assert_eq!(
        right.read(inode, 0, LOWER_BYTES.len()).await.unwrap(),
        LOWER_BYTES
    );
    admin::verify_revision(catalog.as_ref(), &seed.revision)
        .await
        .unwrap();
    let mut original = vec![0; LOWER_BYTES.len()];
    seed.blocks
        .read_range((LOWER_SLICE, 0), 0, &mut original)
        .await
        .unwrap();
    assert_eq!(original, LOWER_BYTES);
    catalog
        .release_lease(ReleaseLease {
            lease_id: right_lease.lease_id,
            holder_generation: right_lease.holder_generation,
        })
        .await
        .unwrap();
}

async fn qualification<B: WorkspaceKvBackend>(
    remote: Remote,
    namespace: String,
    catalog: Arc<KvWorkspaceStore<B>>,
    control: Arc<dyn ControlStore>,
) {
    let volume = Uuid::new_v4();
    let seed = seed_catalog_lower(catalog.clone(), volume).await;
    let header = NativeVolumeHeader::p1(
        *volume.as_bytes(),
        p1_storage_namespace_id(&namespace, &volume),
    );
    initialize_volume(
        control.as_ref(),
        &namespace,
        &header,
        NativeRuntimeCapabilities::compiled(),
    )
    .await
    .unwrap();
    let fixture = NativeP1RuntimeFixture {
        control,
        header,
        namespace: namespace.clone(),
        blocks: seed.blocks.clone(),
        sink: Arc::new(MemorySink::default()),
    };
    let dir = tempfile::tempdir().unwrap();
    configure(dir.path(), remote.config(&namespace));
    let manager = Manager::open(dir.path()).await.unwrap();
    let outcome = std::panic::AssertUnwindSafe(assert_contract(
        &manager,
        remote,
        catalog.clone(),
        &fixture,
        &seed,
    ))
    .catch_unwind()
    .await;
    close(manager).await;
    catalog.shutdown_metadata_backend().await.unwrap();
    remote.cleanup(&namespace, Some(volume)).await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_REDIS_URL; real Manager native P1 namespace/data qualification"]
async fn real_redis_manager_native_p1_nonempty_fork_runtime() {
    let namespace = format!("manager-native-runtime-{}", Uuid::new_v4().simple());
    let url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
    let catalog = Arc::new(KvWorkspaceStore::new(
        // Seed through the production CLI's administrator connection, which
        // establishes the Redis index before Manager's read-only admission.
        RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap(),
    ));
    let control = Arc::new(
        crate::native_base::write::redis::RedisControlStore::connect(&url)
            .await
            .unwrap(),
    );
    qualification(Remote::Redis, namespace, catalog, control).await;
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_TIKV_PD_ENDPOINTS; real Manager native P1 namespace/data qualification"]
async fn real_tikv_manager_native_p1_nonempty_fork_runtime() {
    let namespace = format!("manager-native-runtime-{}", Uuid::new_v4().simple());
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let catalog = Arc::new(KvWorkspaceStore::new(
        TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
            .await
            .unwrap(),
    ));
    let control = Arc::new(
        crate::native_base::write::tikv::TiKvControlStore::connect(&endpoints)
            .await
            .unwrap(),
    );
    qualification(Remote::TiKv, namespace, catalog, control).await;
}
