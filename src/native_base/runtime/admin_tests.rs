use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use uuid::Uuid;

use super::{
    NativeDataRuntime, NativeP1Workspace, NativeRuntimeCapabilities, NativeVolumeHeader,
    ZeroBaseDataSource, ensure_p1_control, initialize_volume, inspect_p1_control, mark_p1_deleting,
    p1_storage_namespace_id, validate_p1_volume,
};
use crate::chunk::BlockStore;
use crate::native_base::write::keys::Keys;
use crate::native_base::write::memory::MemoryControlStore;
use crate::native_base::write::overlay::{OverlayParams, WriteOverlay};
use crate::native_base::write::receipts::MemorySink;
use crate::native_base::write::records::HeadState;
use crate::native_base::write::store::{ControlStore, StoreError, Txn};
use crate::workspace_overlay::admin::{
    ensure_workspace, inspect_workspace, mark_workspace_deleting, verify_revision,
};
use crate::workspace_overlay::catalog::{
    CreateSnapshot, CreateWorkspace, ReleaseLease, WorkspaceStore,
};
use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::ids::{LayerId, SnapshotId, WorkspaceId};
use crate::workspace_overlay::model::VolumeHeader;
use crate::workspace_overlay::stores::database::SqliteWorkspaceStore;

// The durable marker is an admission fence, not a claim that uploads drained.
fn deletion_key(volume_id: &[u8; 16], workspace_id: &[u8; 16]) -> Vec<u8> {
    let mut key = Keys::volume_prefix(volume_id);
    key.extend_from_slice(b"deleted/");
    key.extend_from_slice(workspace_id);
    key
}

#[tokio::test]
async fn deletion_fences_a_native_writer_that_already_initialized() {
    let control = Arc::new(MemoryControlStore::default());
    let params = OverlayParams {
        volume_id: [1; 16],
        workspace_id: [2; 16],
        domain_id: [3; 16],
        writer_generation: 1,
        block_size: 64,
    };
    let runtime = NativeDataRuntime::new(
        Arc::new(WriteOverlay::new(
            control.clone(),
            Arc::new(MemorySink::default()),
            params,
        )),
        Arc::new(ZeroBaseDataSource),
    );
    runtime.initialize([4; 16], 0).await.unwrap();
    runtime.write(7, 0, b"before deletion").await.unwrap();
    runtime.fsync(7).await.unwrap();
    control
        .run(Txn::new().put(deletion_key(&[1; 16], &[2; 16]), vec![1]))
        .await
        .unwrap();
    runtime.write(7, 0, b"after deletion!").await.unwrap();
    assert!(runtime.fsync(7).await.is_err(), "deleted writer published");
}

#[tokio::test]
async fn deletion_fences_lazy_native_head_initialization() {
    let control = Arc::new(MemoryControlStore::default());
    control
        .run(Txn::new().put(deletion_key(&[1; 16], &[2; 16]), vec![1]))
        .await
        .unwrap();
    let runtime = NativeDataRuntime::new(
        Arc::new(WriteOverlay::new(
            control.clone(),
            Arc::new(MemorySink::default()),
            OverlayParams {
                volume_id: [1; 16],
                workspace_id: [2; 16],
                domain_id: [3; 16],
                writer_generation: 1,
                block_size: 64,
            },
        )),
        Arc::new(ZeroBaseDataSource),
    );
    assert!(runtime.initialize([4; 16], 0).await.is_err());
    assert!(
        control
            .get(&Keys::new(&[1; 16]).head(&[2; 16]))
            .await
            .unwrap()
            .is_none()
    );
}

const NAMESPACE: &str = "native-p1-admin-test";

fn request() -> NativeP1Workspace {
    NativeP1Workspace {
        workspace_id: WorkspaceId::new(),
        domain_id: Uuid::new_v4(),
        owner_id: Uuid::new_v4(),
        initial_epoch: 0,
        initial_generation: 1,
    }
}

async fn control_fixture() -> (Arc<MemoryControlStore>, NativeVolumeHeader) {
    let control = Arc::new(MemoryControlStore::default());
    let volume_id = Uuid::new_v4();
    let header = NativeVolumeHeader::p1(
        *volume_id.as_bytes(),
        p1_storage_namespace_id(NAMESPACE, &volume_id),
    );
    initialize_volume(
        control.as_ref(),
        NAMESPACE,
        &header,
        NativeRuntimeCapabilities::compiled(),
    )
    .await
    .unwrap();
    (control, header)
}

#[tokio::test]
async fn exact_native_p1_identity_replays_but_owner_domain_and_progress_do_not() {
    let (control, header) = control_fixture().await;
    let request = request();
    let first = ensure_p1_control(control.as_ref(), &header, NAMESPACE, &request)
        .await
        .unwrap();
    assert_eq!(
        ensure_p1_control(control.as_ref(), &header, NAMESPACE, &request)
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        first.head.as_ref().unwrap().head_id,
        request.workspace_id.into_uuid()
    );
    assert_eq!(
        first.domain.as_ref().unwrap().namespace_id,
        request.workspace_id.into_uuid()
    );
    for changed in [
        NativeP1Workspace {
            owner_id: Uuid::new_v4(),
            ..request.clone()
        },
        NativeP1Workspace {
            domain_id: Uuid::new_v4(),
            ..request.clone()
        },
        NativeP1Workspace {
            initial_generation: 2,
            ..request.clone()
        },
    ] {
        assert!(matches!(
            ensure_p1_control(control.as_ref(), &header, NAMESPACE, &changed).await,
            Err(WorkspaceError::Conflict(_))
        ));
    }
    let keys = Keys::new(&header.volume_id);
    let key = keys.head(request.workspace_id.as_bytes());
    let mut head = HeadState::decode(&control.get(&key).await.unwrap().unwrap()).unwrap();
    head.head.commit_seq += 1;
    control
        .run(Txn::new().put(key, head.encode()))
        .await
        .unwrap();
    assert!(matches!(
        ensure_p1_control(control.as_ref(), &header, NAMESPACE, &request).await,
        Err(WorkspaceError::Conflict(_))
    ));
    let actual = inspect_p1_control(control.as_ref(), &header, NAMESPACE, request.workspace_id)
        .await
        .unwrap();
    assert_eq!(actual.head.unwrap().commit_seq, 1);
}

#[tokio::test]
async fn native_p1_rejects_header_aliases_frozen_and_published_lowers() {
    let (control, header) = control_fixture().await;
    let catalog = VolumeHeader {
        volume_format: "workspace-v1".into(),
        schema_version: 1,
        volume_id: Uuid::from_bytes(header.volume_id),
        created_at_ns: 0,
    };
    validate_p1_volume(&header, NAMESPACE, &catalog).unwrap();
    assert!(validate_p1_volume(&header, "other-namespace", &catalog).is_err());
    let mut wrong_catalog = catalog.clone();
    wrong_catalog.volume_format = "workspace-entity-v1".into();
    assert!(validate_p1_volume(&header, NAMESPACE, &wrong_catalog).is_err());
    let mut frozen = header.clone();
    frozen.required_features = super::FROZEN_METADATA_FEATURE;
    assert!(validate_p1_volume(&frozen, NAMESPACE, &catalog).is_err());
    let request = request();
    let keys = Keys::new(&header.volume_id);
    control
        .run(Txn::new().put(keys.fork_base(request.workspace_id.as_bytes()), vec![1]))
        .await
        .unwrap();
    assert!(matches!(
        ensure_p1_control(control.as_ref(), &header, NAMESPACE, &request).await,
        Err(WorkspaceError::UnsupportedCapability(_))
    ));
    assert!(
        control
            .get(&keys.head(request.workspace_id.as_bytes()))
            .await
            .unwrap()
            .is_none()
    );
}

struct LostReply {
    inner: Arc<MemoryControlStore>,
    lose_once: AtomicBool,
}

#[async_trait]
impl ControlStore for LostReply {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.get(key).await
    }
    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        self.inner.scan(prefix).await
    }
    async fn run(&self, txn: Txn) -> Result<(), StoreError> {
        let writes = !txn.writes.is_empty();
        self.inner.run(txn).await?;
        if writes && self.lose_once.swap(false, Ordering::SeqCst) {
            Err(StoreError::Backend(
                "reply lost after durable commit".into(),
            ))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn native_p1_lost_creation_and_delete_replies_reconcile_exactly() {
    let (control, header) = control_fixture().await;
    let lost = LostReply {
        inner: control.clone(),
        lose_once: AtomicBool::new(true),
    };
    let request = request();
    ensure_p1_control(&lost, &header, NAMESPACE, &request)
        .await
        .unwrap();
    assert_eq!(
        control
            .scan(&Keys::volume_prefix(&header.volume_id))
            .await
            .unwrap()
            .len(),
        2
    );
    lost.lose_once.store(true, Ordering::SeqCst);
    mark_p1_deleting(&lost, &header, NAMESPACE, request.workspace_id)
        .await
        .unwrap();
    mark_p1_deleting(&lost, &header, NAMESPACE, request.workspace_id)
        .await
        .unwrap();
    assert!(
        inspect_p1_control(&lost, &header, NAMESPACE, request.workspace_id)
            .await
            .unwrap()
            .deleting
    );
    assert!(matches!(
        ensure_p1_control(&lost, &header, NAMESPACE, &request).await,
        Err(WorkspaceError::Fenced)
    ));
    let view = inspect_p1_control(&lost, &header, NAMESPACE, request.workspace_id)
        .await
        .unwrap();
    assert_eq!(view.domain.unwrap().state, "quarantined");
}

#[tokio::test]
async fn deletion_of_uninitialized_native_child_installs_a_quarantined_sentinel() {
    let (control, header) = control_fixture().await;
    let workspace_id = WorkspaceId::new();
    mark_p1_deleting(control.as_ref(), &header, NAMESPACE, workspace_id)
        .await
        .unwrap();
    let first = inspect_p1_control(control.as_ref(), &header, NAMESPACE, workspace_id)
        .await
        .unwrap();
    assert!(first.deleting);
    assert_eq!(
        first.head.as_ref().unwrap().head_id,
        workspace_id.into_uuid()
    );
    assert_eq!(first.domain.as_ref().unwrap().state, "quarantined");
    let keys = Keys::new(&header.volume_id);
    let domain = crate::native_base::write::domain::decode_domain(
        &control
            .get(&keys.domain(first.domain.as_ref().unwrap().domain_id.as_bytes()))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(domain.accepted_ticket_end.is_none());
    assert!(domain.close_ref.is_none());
    mark_p1_deleting(control.as_ref(), &header, NAMESPACE, workspace_id)
        .await
        .unwrap();
    assert_eq!(
        inspect_p1_control(control.as_ref(), &header, NAMESPACE, workspace_id)
            .await
            .unwrap(),
        first
    );
}

struct InsertBeforeWrite {
    inner: Arc<MemoryControlStore>,
    target: Vec<u8>,
    injected_key: Vec<u8>,
    injected_value: Vec<u8>,
    armed: AtomicBool,
}

#[async_trait]
impl ControlStore for InsertBeforeWrite {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.get(key).await
    }
    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        self.inner.scan(prefix).await
    }
    async fn run(&self, txn: Txn) -> Result<(), StoreError> {
        if txn.writes.iter().any(|(key, _)| key == &self.target)
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.inner
                .run(Txn::new().put(self.injected_key.clone(), self.injected_value.clone()))
                .await?;
        }
        self.inner.run(txn).await
    }
}

#[tokio::test]
async fn a_native_writer_lease_racing_delete_blocks_the_atomic_marker() {
    let (control, header) = control_fixture().await;
    let request = request();
    ensure_p1_control(control.as_ref(), &header, NAMESPACE, &request)
        .await
        .unwrap();
    let keys = Keys::new(&header.volume_id);
    let racing = InsertBeforeWrite {
        inner: control.clone(),
        target: keys.workspace_deleted(request.workspace_id.as_bytes()),
        injected_key: keys.writer_lease(request.workspace_id.as_bytes()),
        injected_value: b"active-writer".to_vec(),
        armed: AtomicBool::new(true),
    };
    assert!(matches!(
        mark_p1_deleting(&racing, &header, NAMESPACE, request.workspace_id).await,
        Err(WorkspaceError::Busy)
    ));
    assert!(
        control
            .get(&keys.workspace_deleted(request.workspace_id.as_bytes()))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn deletion_wins_atomic_head_initialization_and_publication_races() {
    for initialize in [true, false] {
        let control = Arc::new(MemoryControlStore::default());
        let keys = Keys::new(&[1; 16]);
        let racing = Arc::new(InsertBeforeWrite {
            inner: control.clone(),
            target: keys.head(&[2; 16]),
            injected_key: keys.workspace_deleted(&[2; 16]),
            injected_value: vec![1],
            armed: AtomicBool::new(initialize),
        });
        let params = OverlayParams {
            volume_id: [1; 16],
            workspace_id: [2; 16],
            domain_id: [3; 16],
            writer_generation: 1,
            block_size: 64,
        };
        let sink = Arc::new(MemorySink::default());
        let runtime = NativeDataRuntime::new(
            Arc::new(WriteOverlay::new(
                racing.clone(),
                sink.clone(),
                params.clone(),
            )),
            Arc::new(ZeroBaseDataSource),
        );
        if initialize {
            assert!(runtime.initialize([4; 16], 0).await.is_err());
            assert!(control.get(&keys.head(&[2; 16])).await.unwrap().is_none());
        } else {
            runtime.initialize([4; 16], 0).await.unwrap();
            runtime.write(7, 0, b"before").await.unwrap();
            runtime.fsync(7).await.unwrap();
            let before = control.get(&keys.head(&[2; 16])).await.unwrap();
            racing.armed.store(true, Ordering::SeqCst);
            runtime.write(7, 0, b"AFTER!").await.unwrap();
            assert!(runtime.fsync(7).await.is_err());
            assert_eq!(control.get(&keys.head(&[2; 16])).await.unwrap(), before);
            let reader = NativeDataRuntime::new(
                Arc::new(WriteOverlay::new(control, sink, params)),
                Arc::new(ZeroBaseDataSource),
            );
            assert_eq!(reader.read(7, 0, 6).await.unwrap(), b"before");
        }
    }
}

/// Uses actual catalog metadata, immutable extents and WorkspaceBaseDataSource;
/// no in-memory byte-array BaseDataSource stands in for a nonempty lower.
#[tokio::test]
async fn native_p1_children_read_nonempty_catalog_lower_and_isolate_writes() {
    let (control, header) = control_fixture().await;
    let catalog = Arc::new(
        SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    );
    let seed = super::admin_test_support::seed_catalog_lower(
        catalog.clone(),
        Uuid::from_bytes(header.volume_id),
    )
    .await;
    let base = seed.revision;
    let ino = seed.inode;
    let bytes = super::admin_test_support::LOWER_BYTES;
    let blocks = seed.blocks;
    let fixture = super::admin_test_support::NativeP1RuntimeFixture {
        control: control.clone(),
        header: header.clone(),
        namespace: NAMESPACE.into(),
        blocks: blocks.clone(),
        sink: Arc::new(MemorySink::default()),
    };
    let mut children = Vec::new();
    for generation in [7, 8] {
        let request = request();
        ensure_p1_control(control.as_ref(), &header, NAMESPACE, &request)
            .await
            .unwrap();
        ensure_workspace(
            catalog.as_ref(),
            CreateWorkspace {
                workspace_id: request.workspace_id,
                head_layer_id: LayerId::new(),
                base_revision: base.clone(),
                owner_id: Some("native-child".into()),
                upper_bucket: None,
            },
        )
        .await
        .unwrap();
        let (runtime, lease) = fixture
            .open(catalog.clone(), request.workspace_id, generation)
            .await;
        assert_eq!(
            runtime.read(ino as u64, 0, bytes.len()).await.unwrap(),
            bytes
        );
        children.push((request, lease, runtime));
    }
    children[0].2.write(ino as u64, 0, b"CHILD").await.unwrap();
    children[0].2.fsync(ino as u64).await.unwrap();
    let mut changed = bytes.to_vec();
    changed[..5].copy_from_slice(b"CHILD");
    assert_eq!(
        children[0]
            .2
            .read(ino as u64, 0, bytes.len())
            .await
            .unwrap(),
        changed
    );
    assert_eq!(
        children[1]
            .2
            .read(ino as u64, 0, bytes.len())
            .await
            .unwrap(),
        bytes
    );
    assert!(matches!(
        mark_workspace_deleting(catalog.as_ref(), children[0].0.workspace_id, false).await,
        Err(WorkspaceError::Busy)
    ));
    catalog
        .create_snapshot(CreateSnapshot {
            snapshot_id: SnapshotId::new(),
            name: None,
            revision: base.clone(),
            owner_id: None,
        })
        .await
        .unwrap();
    let (left, lease, _) = &children[0];
    catalog
        .release_lease(ReleaseLease {
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        })
        .await
        .unwrap();
    mark_workspace_deleting(catalog.as_ref(), left.workspace_id, false)
        .await
        .unwrap();
    mark_p1_deleting(control.as_ref(), &header, NAMESPACE, left.workspace_id)
        .await
        .unwrap();
    assert!(
        inspect_p1_control(control.as_ref(), &header, NAMESPACE, left.workspace_id)
            .await
            .unwrap()
            .deleting
    );
    assert_eq!(
        inspect_workspace(catalog.as_ref(), left.workspace_id)
            .await
            .unwrap()
            .base_revision,
        base
    );
    verify_revision(catalog.as_ref(), &base).await.unwrap();
    assert_eq!(
        children[1]
            .2
            .read(ino as u64, 0, bytes.len())
            .await
            .unwrap(),
        bytes
    );
    let mut original = vec![0; bytes.len()];
    blocks.read_range((77, 0), 0, &mut original).await.unwrap();
    assert_eq!(original, bytes);
}
