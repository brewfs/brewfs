#![cfg(feature = "workspace-overlay")]

use std::sync::Arc;

use brewfs::meta::MetaLayer;
use brewfs::meta::store::{SetAttrFlags, SetAttrRequest};
use brewfs::workspace_overlay::catalog::{AcquireLease, CreateVolumeRoot, WorkspaceStore};
use brewfs::workspace_overlay::ids::{LayerId, LeaseId, WorkspaceId};
use brewfs::workspace_overlay::meta_layer::WorkspaceMetaLayer;
use brewfs::workspace_overlay::model::{ViewContext, WORKSPACE_SCHEMA_VERSION};
use brewfs::workspace_overlay::stores::database::SqliteWorkspaceStore;
use uuid::Uuid;

async fn fixture() -> (WorkspaceMetaLayer<SqliteWorkspaceStore>, i64) {
    let store = Arc::new(
        SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    );
    store.initialize_workspace_schema().await.unwrap();
    let workspace_id = WorkspaceId::from_uuid(Uuid::new_v4());
    let workspace = store
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::new_v4(),
            workspace_id,
            root_layer_id: LayerId::from_uuid(Uuid::new_v4()),
            writable_layer_id: LayerId::from_uuid(Uuid::new_v4()),
            owner_id: None,
        })
        .await
        .unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id,
            lease_id: LeaseId::from_uuid(Uuid::new_v4()),
            holder_generation: 1,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    let meta = WorkspaceMetaLayer::new(
        store,
        ViewContext {
            workspace_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        },
    );
    let ino = meta
        .create_file(meta.root_ino(), "timestamps".into())
        .await
        .unwrap();
    (meta, ino)
}

// Exercise the atomic SETATTR used by both mounted truncate and ftruncate.
// Seed old timestamps so advancing the clock never requires a sleep.
async fn assert_truncate_updates_timestamps(size: u64) {
    let (meta, ino) = fixture().await;
    let before = meta
        .set_attr(
            ino,
            &SetAttrRequest {
                size: Some(64),
                atime: Some(11),
                mtime: Some(22),
                ctime: Some(33),
                ..Default::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();
    let after = meta
        .set_attr_as(
            ino,
            &SetAttrRequest {
                size: Some(size),
                ..Default::default()
            },
            SetAttrFlags::empty(),
            0,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(after.size, size);
    assert!(
        after.mtime > before.mtime,
        "truncate to {size} must advance mtime"
    );
    assert!(
        after.ctime > before.ctime,
        "truncate to {size} must advance ctime"
    );
    assert_eq!(after.atime, before.atime);
    let persisted = meta.stat(ino).await.unwrap().unwrap();
    assert_eq!(
        (persisted.mtime, persisted.ctime),
        (after.mtime, after.ctime)
    );
}

#[tokio::test]
async fn truncate_updates_timestamps_when_extending() {
    assert_truncate_updates_timestamps(123).await;
}

#[tokio::test]
async fn truncate_updates_timestamps_when_shrinking() {
    assert_truncate_updates_timestamps(0).await;
}

#[tokio::test]
async fn truncate_updates_timestamps_with_unchanged_size() {
    assert_truncate_updates_timestamps(64).await;
}

#[tokio::test]
async fn truncate_preserves_explicit_times_and_other_setattrs_preserve_mtime() {
    let (meta, ino) = fixture().await;
    let explicit = meta
        .set_attr(
            ino,
            &SetAttrRequest {
                size: Some(123),
                atime: Some(11),
                mtime: Some(22),
                ctime: Some(33),
                ..Default::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();
    assert_eq!(
        (explicit.atime, explicit.mtime, explicit.ctime),
        (11, 22, 33)
    );
    let chmod = meta.chmod(ino, 0o600).await.unwrap();
    assert_eq!(chmod.mtime, explicit.mtime);
    let now = meta
        .set_attr(
            ino,
            &SetAttrRequest {
                size: Some(0),
                mtime: Some(1),
                ..Default::default()
            },
            SetAttrFlags::SET_MTIME_NOW,
        )
        .await
        .unwrap();
    assert!(now.mtime > explicit.mtime);
}
