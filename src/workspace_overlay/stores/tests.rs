use std::sync::Arc;

use tokio::sync::Barrier;
use uuid::Uuid;

use crate::workspace_overlay::catalog::{
    AcquireLease, AppendDataExtent, CreateVolumeRoot, CreateWorkspace, DentryQuery, ExtentQuery,
    HeadGuard, NamespaceMutation, RecordOrphanSlice, ReleaseLease, RenewLease, WorkspaceStore,
    XattrMutation, XattrQuery,
};
use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::ids::{LayerId, LeaseId, WorkspaceId};
use crate::workspace_overlay::model::{
    BaseRevision, DataExtentDelta, DentryDelta, ExtentKind, LayerState, ValueOp, XattrDelta,
};

use super::database::{SqliteWorkspaceStore, StoreFailpoint};

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn create_request() -> CreateVolumeRoot {
    CreateVolumeRoot {
        volume_id: id(1),
        workspace_id: WorkspaceId::from_uuid(id(2)),
        root_layer_id: LayerId::from_uuid(id(3)),
        writable_layer_id: LayerId::from_uuid(id(4)),
        owner_id: Some("test-owner".into()),
    }
}

async fn initialized_store() -> SqliteWorkspaceStore {
    let store = SqliteWorkspaceStore::connect("sqlite::memory:")
        .await
        .unwrap();
    store.initialize_workspace_schema().await.unwrap();
    store
}

#[tokio::test]
async fn schema_initialization_is_idempotent_and_does_not_create_a_marker() {
    let store = initialized_store().await;
    store.initialize_workspace_schema().await.unwrap();
    let names = store.schema_table_names().await.unwrap();
    assert_eq!(names.len(), 12);
    assert!(names.iter().all(|name| name.starts_with("ws_v1_")));
    assert_eq!(store.load_volume_header().await.unwrap(), None);
}

#[tokio::test]
async fn volume_root_creation_is_atomic_and_builds_a_valid_two_layer_chain() {
    let store = initialized_store().await;
    let request = create_request();
    let workspace = store.create_volume_root(request.clone()).await.unwrap();
    assert_eq!(workspace.workspace_id, request.workspace_id);
    assert_eq!(workspace.head_layer_id, request.writable_layer_id);

    let header = store.load_volume_header().await.unwrap().unwrap();
    assert_eq!(header.volume_id, request.volume_id);
    assert_eq!(header.volume_format, "workspace-v1");
    assert_eq!(header.schema_version, 1);

    let chain = store
        .load_layer_chain(request.writable_layer_id)
        .await
        .unwrap();
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[0].state, LayerState::Writable);
    assert_eq!(chain[1].state, LayerState::Sealed);
    assert_eq!(chain[1].sealed_version, Some(1));
    assert!(chain[1].delta_digest.is_some());
    assert!(chain[1].root_hash.is_some());
}

#[tokio::test]
async fn head_guard_fences_stale_epoch_and_generation_without_partial_writes() {
    let store = initialized_store().await;
    let request = create_request();
    let workspace = store.create_volume_root(request.clone()).await.unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(id(5)),
            holder_generation: 7,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap();
    let guard = HeadGuard {
        workspace_id: workspace.workspace_id,
        expected_head_layer_id: workspace.head_layer_id,
        expected_head_epoch: workspace.head_epoch,
        lease_id: lease.lease_id,
        holder_generation: lease.holder_generation,
    };

    store
        .apply_namespace_mutation(NamespaceMutation {
            guard: guard.clone(),
            dentries: vec![DentryDelta::put(
                workspace.head_layer_id,
                1,
                b"ok".to_vec(),
                2,
                1,
                0,
            )],
            inodes: Vec::new(),
        })
        .await
        .unwrap();

    let mut stale = guard;
    stale.expected_head_epoch += 1;
    let error = store
        .apply_namespace_mutation(NamespaceMutation {
            guard: stale,
            dentries: vec![DentryDelta::put(
                workspace.head_layer_id,
                1,
                b"fenced".to_vec(),
                3,
                1,
                0,
            )],
            inodes: Vec::new(),
        })
        .await
        .unwrap_err();
    assert!(matches!(error, WorkspaceError::Fenced));

    let rows = store
        .get_dentry_deltas(DentryQuery {
            layer_ids: vec![workspace.head_layer_id],
            parent_ino: 1,
            name: None,
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, b"ok");
    assert_eq!(rows[0].sequence, 1);
}

#[tokio::test]
async fn injected_failure_before_commit_rolls_back_the_entire_namespace_mutation() {
    let store = initialized_store().await;
    let request = create_request();
    let workspace = store.create_volume_root(request).await.unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(id(6)),
            holder_generation: 1,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap();
    store.set_failpoint(StoreFailpoint::BeforeCommit);

    let result = store
        .apply_namespace_mutation(NamespaceMutation {
            guard: HeadGuard {
                workspace_id: workspace.workspace_id,
                expected_head_layer_id: workspace.head_layer_id,
                expected_head_epoch: workspace.head_epoch,
                lease_id: lease.lease_id,
                holder_generation: lease.holder_generation,
            },
            dentries: vec![DentryDelta::put(
                workspace.head_layer_id,
                1,
                b"rolled-back".to_vec(),
                2,
                1,
                0,
            )],
            inodes: Vec::new(),
        })
        .await;
    assert!(result.is_err());
    store.set_failpoint(StoreFailpoint::Disabled);

    let rows = store
        .get_dentry_deltas(DentryQuery {
            layer_ids: vec![workspace.head_layer_id],
            parent_ino: 1,
            name: Some(b"rolled-back".to_vec()),
        })
        .await
        .unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn lease_singleton_renewal_and_release_are_generation_fenced() {
    let store = initialized_store().await;
    let workspace = store.create_volume_root(create_request()).await.unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(id(7)),
            holder_generation: 4,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap();
    let duplicate = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(id(8)),
            holder_generation: 5,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap_err();
    assert!(matches!(duplicate, WorkspaceError::Busy));

    let stale = store
        .renew_lease(RenewLease {
            lease_id: lease.lease_id,
            holder_generation: 99,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap_err();
    assert!(matches!(stale, WorkspaceError::Fenced));
    let renewed = store
        .renew_lease(RenewLease {
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    assert!(renewed.expires_at_ns > lease.expires_at_ns);

    store
        .release_lease(ReleaseLease {
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        })
        .await
        .unwrap();
    store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(id(8)),
            holder_generation: 5,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn expired_lease_is_reaped_before_a_new_generation_mounts() {
    let store = initialized_store().await;
    let workspace = store.create_volume_root(create_request()).await.unwrap();
    let expired = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(id(70)),
            holder_generation: 1,
            ttl_ns: 1,
        })
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;

    let replacement = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(id(71)),
            holder_generation: 2,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap();
    assert_eq!(replacement.holder_generation, 2);
    assert!(matches!(
        store
            .renew_lease(RenewLease {
                lease_id: expired.lease_id,
                holder_generation: expired.holder_generation,
                ttl_ns: 30_000_000_000,
            })
            .await,
        Err(WorkspaceError::Fenced)
    ));
    let leases = store.list_leases(workspace.workspace_id).await.unwrap();
    assert!(leases.iter().any(|lease| {
        lease.lease_id == expired.lease_id
            && lease.state == crate::workspace_overlay::model::LeaseState::Expired
    }));
}

#[tokio::test]
async fn uploaded_orphan_is_persisted_as_an_unreachable_gc_layer() {
    let store = initialized_store().await;
    store.create_volume_root(create_request()).await.unwrap();
    let orphan_layer_id = LayerId::from_uuid(id(72));
    store
        .record_orphan_slice(RecordOrphanSlice {
            orphan_layer_id,
            slice_id: 9001,
            slice_end: 8193,
        })
        .await
        .unwrap();

    let layer = store.load_layer(orphan_layer_id).await.unwrap();
    assert_eq!(layer.state, LayerState::Deleting);
    assert_eq!(layer.parent_layer_id, None);
    assert_eq!(layer.owned_slice_count, 1);
    assert_eq!(layer.owned_bytes, 8193);
    let snapshot = store.gc_snapshot(i64::MAX, 0).await.unwrap();
    assert!(!snapshot.root_layers.contains(&orphan_layer_id));
    assert!(snapshot.slice_references.iter().any(|reference| {
        reference.layer_id == orphan_layer_id
            && reference.slice_id == 9001
            && reference.slice_end == 8193
    }));
}

#[tokio::test]
async fn guarded_extent_and_xattr_mutations_round_trip_with_store_sequences() {
    let store = initialized_store().await;
    let workspace = store.create_volume_root(create_request()).await.unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(id(9)),
            holder_generation: 1,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap();
    let guard = HeadGuard {
        workspace_id: workspace.workspace_id,
        expected_head_layer_id: workspace.head_layer_id,
        expected_head_epoch: workspace.head_epoch,
        lease_id: lease.lease_id,
        holder_generation: lease.holder_generation,
    };

    let written = store
        .append_data_extent(AppendDataExtent {
            guard: guard.clone(),
            extent: DataExtentDelta::data(workspace.head_layer_id, 2, 0, 4096, 8192, 100, 32, 0),
            chunk_size: 64 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(written.sequence, 1);
    let extents = store
        .get_extent_deltas(ExtentQuery {
            layer_ids: vec![workspace.head_layer_id],
            ino: 2,
            chunk_index: 0,
            range_start: 0,
            range_end: 16 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(extents, vec![written]);
    assert_eq!(
        extents[0].kind,
        ExtentKind::Data {
            slice_id: 100,
            slice_offset: 32
        }
    );

    store
        .apply_xattr_mutation(XattrMutation {
            guard,
            xattr: XattrDelta {
                layer_id: workspace.head_layer_id,
                ino: 2,
                name: b"user.agent".to_vec(),
                op: ValueOp::Put,
                value: Some(b"private".to_vec()),
                sequence: 0,
            },
        })
        .await
        .unwrap();
    let xattrs = store
        .get_xattr_deltas(XattrQuery {
            layer_ids: vec![workspace.head_layer_id],
            ino: 2,
            name: Some(b"user.agent".to_vec()),
        })
        .await
        .unwrap();
    assert_eq!(xattrs.len(), 1);
    assert_eq!(xattrs[0].sequence, 2);
    assert_eq!(xattrs[0].value.as_deref(), Some(b"private".as_slice()));
}

#[tokio::test]
async fn workspace_allocators_are_monotonic_and_reject_unknown_names() {
    let store = initialized_store().await;
    store.create_volume_root(create_request()).await.unwrap();
    assert_eq!(store.allocate_id("inode").await.unwrap(), 2);
    assert_eq!(store.allocate_id("inode").await.unwrap(), 3);
    assert_eq!(store.allocate_id("slice").await.unwrap(), 1);
    assert_eq!(store.allocate_id("sealed_version").await.unwrap(), 2);
    assert!(store.allocate_id("unknown").await.is_err());
}

#[tokio::test]
async fn independent_store_instances_serialize_concurrent_workspace_writers() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("workspace-catalog.sqlite");
    let url = format!("sqlite://{}", database.display());
    let store_a = Arc::new(SqliteWorkspaceStore::connect(&url).await.unwrap());
    store_a.initialize_workspace_schema().await.unwrap();
    assert_eq!(store_a.journal_mode().await.unwrap(), "wal");
    let first = store_a.create_volume_root(create_request()).await.unwrap();
    let root = store_a
        .load_layer_chain(first.head_layer_id)
        .await
        .unwrap()
        .into_iter()
        .find(|layer| layer.parent_layer_id.is_none())
        .unwrap();
    let base_revision = BaseRevision {
        layer_id: root.layer_id,
        sealed_version: root.sealed_version.unwrap(),
        root_hash: root.root_hash.unwrap(),
    };

    // A second connection has its own in-process gate, just like a separate
    // BrewFS mount process. SQLite therefore has to coordinate these writers.
    let store_b = Arc::new(SqliteWorkspaceStore::connect(&url).await.unwrap());
    assert_eq!(store_b.journal_mode().await.unwrap(), "wal");
    let second = store_b
        .create_workspace(CreateWorkspace {
            workspace_id: WorkspaceId::from_uuid(id(80)),
            head_layer_id: LayerId::from_uuid(id(81)),
            base_revision,
            owner_id: Some("second-writer".into()),
        })
        .await
        .unwrap();
    let first_lease = store_a
        .acquire_lease(AcquireLease {
            workspace_id: first.workspace_id,
            lease_id: LeaseId::from_uuid(id(82)),
            holder_generation: 1,
            ttl_ns: 120_000_000_000,
        })
        .await
        .unwrap();
    let second_lease = store_b
        .acquire_lease(AcquireLease {
            workspace_id: second.workspace_id,
            lease_id: LeaseId::from_uuid(id(83)),
            holder_generation: 1,
            ttl_ns: 120_000_000_000,
        })
        .await
        .unwrap();
    let first_guard = HeadGuard {
        workspace_id: first.workspace_id,
        expected_head_layer_id: first.head_layer_id,
        expected_head_epoch: first.head_epoch,
        lease_id: first_lease.lease_id,
        holder_generation: first_lease.holder_generation,
    };
    let second_guard = HeadGuard {
        workspace_id: second.workspace_id,
        expected_head_layer_id: second.head_layer_id,
        expected_head_epoch: second.head_epoch,
        lease_id: second_lease.lease_id,
        holder_generation: second_lease.holder_generation,
    };

    const MUTATIONS: u64 = 64;
    let barrier = Arc::new(Barrier::new(2));
    let writer_a = {
        let store = Arc::clone(&store_a);
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            for index in 0..MUTATIONS {
                store
                    .apply_namespace_mutation(NamespaceMutation {
                        guard: first_guard.clone(),
                        dentries: vec![DentryDelta::put(
                            first.head_layer_id,
                            1,
                            format!("writer-a-{index}").into_bytes(),
                            1_000 + index as i64,
                            1,
                            0,
                        )],
                        inodes: Vec::new(),
                    })
                    .await?;
            }
            Ok::<(), WorkspaceError>(())
        })
    };
    let writer_b = {
        let store = Arc::clone(&store_b);
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            for index in 0..MUTATIONS {
                store
                    .apply_namespace_mutation(NamespaceMutation {
                        guard: second_guard.clone(),
                        dentries: vec![DentryDelta::put(
                            second.head_layer_id,
                            1,
                            format!("writer-b-{index}").into_bytes(),
                            2_000 + index as i64,
                            1,
                            0,
                        )],
                        inodes: Vec::new(),
                    })
                    .await?;
            }
            Ok::<(), WorkspaceError>(())
        })
    };
    writer_a.await.unwrap().unwrap();
    writer_b.await.unwrap().unwrap();

    let first_rows = store_a
        .get_dentry_deltas(DentryQuery {
            layer_ids: vec![first.head_layer_id],
            parent_ino: 1,
            name: None,
        })
        .await
        .unwrap();
    let second_rows = store_b
        .get_dentry_deltas(DentryQuery {
            layer_ids: vec![second.head_layer_id],
            parent_ino: 1,
            name: None,
        })
        .await
        .unwrap();
    assert_eq!(first_rows.len(), MUTATIONS as usize);
    assert_eq!(second_rows.len(), MUTATIONS as usize);
}
