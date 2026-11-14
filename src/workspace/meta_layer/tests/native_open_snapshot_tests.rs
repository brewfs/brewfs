//! Real, independent SQLite instances; scheduling is explicit, not a sleep.

use super::*;
use crate::meta::store::OpenFlags;
use crate::workspace::stores::database::StoreFailpoint;

struct Fixture {
    _directory: tempfile::TempDir,
    writer: WorkspaceMetaLayer<SqliteWorkspaceStore>,
    observer: WorkspaceMetaLayer<SqliteWorkspaceStore>,
    ino: i64,
}

async fn fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("native-open.sqlite").display()
    );
    let first = Arc::new(SqliteWorkspaceStore::connect(&url).await.unwrap());
    first.initialize_workspace_schema().await.unwrap();
    let workspace_id = WorkspaceId::new();
    let workspace = first
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace".into(),
            schema_version: crate::workspace::model::WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::new_v4(),
            workspace_id,
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: Some("native-open-single-snapshot".into()),
        })
        .await
        .unwrap();
    let lease = first
        .acquire_lease(AcquireLease {
            workspace_id,
            lease_id: LeaseId::new(),
            holder_generation: 1,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    let view = ViewContext {
        workspace_id,
        head_layer_id: workspace.head_layer_id,
        head_epoch: workspace.head_epoch,
        lease_id: lease.lease_id,
        holder_generation: lease.holder_generation,
    };
    let writer = WorkspaceMetaLayer::new(first, view.clone());
    let observer = WorkspaceMetaLayer::new(
        Arc::new(SqliteWorkspaceStore::connect(&url).await.unwrap()),
        view,
    );
    let ino = writer.create_file(1, "shared-policy".into()).await.unwrap();
    writer
        .set_xattr(ino, "system.posix_acl_access", &named_acl(0o640), 0)
        .await
        .unwrap();
    Fixture {
        _directory: directory,
        writer,
        observer,
        ino,
    }
}

#[tokio::test]
async fn native_open_uses_one_target_permission_snapshot_without_root_query() {
    let f = fixture().await;
    assert!(f.observer.packed_lower().is_none());
    let before = f.observer.store().permission_snapshot_read_counts();
    let attr = f.observer.open(f.ino, OpenFlags::RDONLY).await.unwrap();
    let after = f.observer.store().permission_snapshot_read_counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 0));
    assert_eq!(attr.mode & 0o777, 0o640);
    let permission = f.observer.inode_permissions(f.ino).await.unwrap().unwrap();
    assert_eq!(permission.attr.mode & 0o777, 0o640);
    assert_eq!(permission.access_acl, Some(named_acl(0o640)));
}

#[tokio::test]
async fn native_open_snapshot_rejects_intervening_chmod_then_refreshes_atime() {
    let f = fixture().await;
    let (stale_version, (mut stale_inode, access, _, _)) =
        f.observer.retain_native_open_mutation(f.ino).await.unwrap();
    assert_eq!(stale_inode.mode & 0o777, 0o640);
    assert_eq!(access, Some(named_acl(0o640)));
    let requested_atime = stale_inode.atime_ns.checked_add(1_000_000_000).unwrap();
    stale_inode.atime_ns = requested_atime;

    // The other SQLite instance commits after the exact target snapshot and
    // before the attempted atime write. No stale row may overwrite its policy.
    f.writer
        .set_attr(
            f.ino,
            &SetAttrRequest {
                mode: Some(0o604),
                ..Default::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();
    let committed = f.observer.inode_permissions(f.ino).await.unwrap().unwrap();
    assert_eq!(committed.attr.mode & 0o777, 0o604);
    assert_eq!(committed.access_acl, Some(named_acl(0o604)));
    assert_ne!(committed.attr.atime, requested_atime);
    let error = f
        .observer
        .mutate_inode(&stale_version, stale_inode, Vec::new())
        .await
        .unwrap_err();
    assert!(matches!(error, MetaError::Io(error) if error.raw_os_error() == Some(libc::EBUSY)));
    let unchanged = f.observer.inode_permissions(f.ino).await.unwrap().unwrap();
    assert_eq!(unchanged.attr.mode, committed.attr.mode);
    assert_eq!(unchanged.access_acl, committed.access_acl);
    assert_eq!(unchanged.attr.atime, committed.attr.atime);

    let (fresh_version, (mut fresh_inode, access, _, _)) =
        f.observer.retain_native_open_mutation(f.ino).await.unwrap();
    assert_ne!(
        fresh_version[0].next_sequence,
        stale_version[0].next_sequence
    );
    assert_eq!(access, Some(named_acl(0o604)));
    fresh_inode.atime_ns = requested_atime;
    f.observer
        .mutate_inode(&fresh_version, fresh_inode, Vec::new())
        .await
        .unwrap();
    let final_permission = f.observer.inode_permissions(f.ino).await.unwrap().unwrap();
    assert_eq!(final_permission.attr.mode, committed.attr.mode);
    assert_eq!(final_permission.access_acl, Some(named_acl(0o604)));
    assert_eq!(final_permission.attr.atime, requested_atime);
}

#[tokio::test]
async fn native_open_retries_target_snapshot_busy_once() {
    let f = fixture().await;
    let before = f.observer.store().permission_snapshot_read_counts();
    f.observer
        .store()
        .set_failpoint(StoreFailpoint::PermissionReadBusyOnce);
    let attr = f.observer.open(f.ino, OpenFlags::RDONLY).await.unwrap();
    let after = f.observer.store().permission_snapshot_read_counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (2, 0));
    assert_eq!(attr.mode & 0o777, 0o640);
    let permissions = f.observer.inode_permissions(f.ino).await.unwrap().unwrap();
    assert_eq!(permissions.attr.mode & 0o777, 0o640);
    assert_eq!(permissions.access_acl, Some(named_acl(0o640)));
}

#[tokio::test]
async fn native_open_target_snapshot_busy_exhausts_original_limit_without_write() {
    let f = fixture().await;
    let before = f.observer.inode_permissions(f.ino).await.unwrap().unwrap();
    let counts = f.observer.store().permission_snapshot_read_counts();
    f.observer
        .store()
        .set_failpoint(StoreFailpoint::PermissionReadAlwaysBusy);
    let error = f.observer.open(f.ino, OpenFlags::RDONLY).await.unwrap_err();
    assert!(matches!(error, MetaError::Io(error) if error.raw_os_error() == Some(libc::EBUSY)));
    let after_counts = f.observer.store().permission_snapshot_read_counts();
    assert_eq!(
        (after_counts.0 - counts.0, after_counts.1 - counts.1),
        (64, 0)
    );
    f.observer.store().set_failpoint(StoreFailpoint::Disabled);
    let after = f.observer.inode_permissions(f.ino).await.unwrap().unwrap();
    assert_eq!(after.attr.atime, before.attr.atime);
    assert_eq!(after.attr.mode, before.attr.mode);
    assert_eq!(after.access_acl, before.access_acl);
}

#[tokio::test]
async fn native_open_propagates_target_snapshot_non_busy_without_retry() {
    let f = fixture().await;
    let before = f.observer.inode_permissions(f.ino).await.unwrap().unwrap();
    let counts = f.observer.store().permission_snapshot_read_counts();
    f.observer
        .store()
        .set_failpoint(StoreFailpoint::PermissionReadBackendError);
    let error = f.observer.open(f.ino, OpenFlags::RDONLY).await.unwrap_err();
    assert!(
        matches!(error, MetaError::Internal(message) if message == "injected permission snapshot backend error")
    );
    let after_counts = f.observer.store().permission_snapshot_read_counts();
    assert_eq!(
        (after_counts.0 - counts.0, after_counts.1 - counts.1),
        (1, 0)
    );
    f.observer.store().set_failpoint(StoreFailpoint::Disabled);
    let after = f.observer.inode_permissions(f.ino).await.unwrap().unwrap();
    assert_eq!(after.attr.atime, before.attr.atime);
    assert_eq!(after.attr.mode, before.attr.mode);
    assert_eq!(after.access_acl, before.access_acl);
}
