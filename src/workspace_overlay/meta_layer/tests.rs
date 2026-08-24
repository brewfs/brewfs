use std::sync::Arc;

use uuid::Uuid;

use crate::chunk::SliceDesc;
use crate::chunk::read_plan::{ReadPlanSegment, WorkspaceReadPlanProvider};
use crate::meta::MetaLayer;
use crate::meta::file_lock::{FileLockQuery, FileLockRange, FileLockType};
use crate::meta::store::{FileType, SetAttrFlags, SetAttrRequest};
use crate::workspace_overlay::catalog::{AcquireLease, CreateVolumeRoot, WorkspaceStore};
use crate::workspace_overlay::ids::{LayerId, LeaseId, WorkspaceId};
use crate::workspace_overlay::lifecycle::{NoopDurableRemoteBarrier, WorkspaceLifecycle};
use crate::workspace_overlay::model::ViewContext;
use crate::workspace_overlay::stores::database::SqliteWorkspaceStore;

use super::WorkspaceMetaLayer;

async fn test_meta() -> WorkspaceMetaLayer<SqliteWorkspaceStore> {
    let store = Arc::new(
        SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    );
    store.initialize_workspace_schema().await.unwrap();
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(100));
    let workspace = store
        .create_volume_root(CreateVolumeRoot {
            volume_id: Uuid::from_u128(101),
            workspace_id,
            root_layer_id: LayerId::from_uuid(Uuid::from_u128(102)),
            writable_layer_id: LayerId::from_uuid(Uuid::from_u128(103)),
            owner_id: Some("meta-test".into()),
        })
        .await
        .unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id,
            lease_id: LeaseId::from_uuid(Uuid::from_u128(104)),
            holder_generation: 1,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    WorkspaceMetaLayer::new(
        store,
        ViewContext {
            workspace_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        },
    )
}

#[tokio::test]
async fn create_unlink_recreate_and_rmdir_follow_effective_view() {
    let meta = test_meta().await;
    let root = meta.root_ino();
    let before = meta.stat(root).await.unwrap().unwrap();
    assert_eq!(before.kind, FileType::Dir);
    assert_eq!(before.nlink, 2);

    let work = meta.mkdir(root, "work".into()).await.unwrap();
    assert_eq!(meta.stat(root).await.unwrap().unwrap().nlink, 3);
    let first = meta.create_file(work, "result".into()).await.unwrap();
    assert_eq!(meta.lookup(work, "result").await.unwrap(), Some(first));
    let entries = meta.readdir(work).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "result");

    let not_empty = meta.rmdir(root, "work").await.unwrap_err();
    assert!(matches!(
        not_empty,
        crate::meta::store::MetaError::DirectoryNotEmpty(ino) if ino == work
    ));
    meta.unlink(work, "result").await.unwrap();
    assert_eq!(meta.lookup(work, "result").await.unwrap(), None);
    let second = meta.create_file(work, "result".into()).await.unwrap();
    assert_ne!(first, second);
    meta.unlink(work, "result").await.unwrap();
    meta.rmdir(root, "work").await.unwrap();
    assert_eq!(meta.lookup(root, "work").await.unwrap(), None);
    assert_eq!(meta.stat(root).await.unwrap().unwrap().nlink, 2);
}

#[tokio::test]
async fn rename_exchange_and_hardlink_update_dentries_and_nlink_atomically() {
    let meta = test_meta().await;
    let root = meta.root_ino();
    let left = meta.mkdir(root, "left".into()).await.unwrap();
    let right = meta.mkdir(root, "right".into()).await.unwrap();
    let source = meta.create_file(left, "source".into()).await.unwrap();
    let other = meta.create_file(right, "other".into()).await.unwrap();

    let linked = meta.link(source, right, "linked").await.unwrap();
    assert_eq!(linked.nlink, 2);
    meta.rename(left, "source", right, "moved".into())
        .await
        .unwrap();
    assert_eq!(meta.lookup(left, "source").await.unwrap(), None);
    assert_eq!(meta.lookup(right, "linked").await.unwrap(), Some(source));
    assert_eq!(meta.lookup(right, "moved").await.unwrap(), Some(source));

    meta.rename_exchange(right, "moved", right, "other")
        .await
        .unwrap();
    assert_eq!(meta.lookup(right, "moved").await.unwrap(), Some(other));
    assert_eq!(meta.lookup(right, "other").await.unwrap(), Some(source));
    meta.unlink(right, "linked").await.unwrap();
    assert_eq!(meta.stat(source).await.unwrap().unwrap().nlink, 1);
}

#[tokio::test]
async fn symlink_setattr_and_xattr_mutations_round_trip() {
    let meta = test_meta().await;
    let root = meta.root_ino();
    let file = meta.create_file(root, "file".into()).await.unwrap();
    let (link, attr) = meta.symlink(root, "link", "file").await.unwrap();
    assert_eq!(attr.kind, FileType::Symlink);
    assert_eq!(meta.read_symlink(link).await.unwrap(), "file");

    let updated = meta
        .set_attr(
            file,
            &SetAttrRequest {
                mode: Some(0o640),
                uid: Some(2000),
                gid: Some(3000),
                size: None,
                atime: None,
                mtime: None,
                ctime: None,
                flags: None,
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();
    assert_eq!(
        (updated.mode, updated.uid, updated.gid),
        (0o640, 2000, 3000)
    );

    meta.set_xattr(file, "user.agent", b"private", 0)
        .await
        .unwrap();
    assert_eq!(
        meta.get_xattr(file, "user.agent").await.unwrap(),
        Some(b"private".to_vec())
    );
    assert_eq!(meta.list_xattr(file).await.unwrap(), vec!["user.agent"]);
    meta.remove_xattr(file, "user.agent").await.unwrap();
    assert_eq!(meta.get_xattr(file, "user.agent").await.unwrap(), None);
}

#[tokio::test]
async fn setattr_timestamps_round_trip_as_nanoseconds() {
    let meta = test_meta().await;
    let file = meta
        .create_file(meta.root_ino(), "timestamps".into())
        .await
        .unwrap();
    let atime = 1_725_000_000_123_456_789;
    let mtime = 1_725_000_001_234_567_890;
    let ctime = 1_725_000_002_345_678_901;

    let updated = meta
        .set_attr(
            file,
            &SetAttrRequest {
                atime: Some(atime),
                mtime: Some(mtime),
                ctime: Some(ctime),
                ..SetAttrRequest::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();

    assert_eq!(updated.atime, atime);
    assert_eq!(updated.mtime, mtime);
    assert_eq!(updated.ctime, ctime);
    let stat = meta.stat(file).await.unwrap().unwrap();
    assert_eq!(stat.atime, atime);
    assert_eq!(stat.mtime, mtime);
    assert_eq!(stat.ctime, ctime);
}

#[tokio::test]
async fn writes_build_read_plans_and_truncate_holes_prevent_data_revival() {
    let meta = test_meta().await;
    let file = meta
        .create_file(meta.root_ino(), "data".into())
        .await
        .unwrap();
    let chunk_id = crate::vfs::chunk_id_for(file, 0).unwrap();
    meta.write(
        file,
        chunk_id,
        SliceDesc {
            slice_id: 10,
            chunk_id,
            offset: 0,
            length: 8,
        },
        8,
    )
    .await
    .unwrap();
    meta.write(
        file,
        chunk_id,
        SliceDesc {
            slice_id: 11,
            chunk_id,
            offset: 2,
            length: 2,
        },
        8,
    )
    .await
    .unwrap();

    let plan = meta.read_plan(file, 0, 0, 8).await.unwrap();
    assert_eq!(
        plan.segments,
        vec![
            ReadPlanSegment::Data {
                logical_offset: 0,
                length: 2,
                slice_id: 10,
                slice_offset: 0,
            },
            ReadPlanSegment::Data {
                logical_offset: 2,
                length: 2,
                slice_id: 11,
                slice_offset: 0,
            },
            ReadPlanSegment::Data {
                logical_offset: 4,
                length: 4,
                slice_id: 10,
                slice_offset: 4,
            },
        ]
    );

    meta.truncate(file, 3, crate::chunk::DEFAULT_CHUNK_SIZE)
        .await
        .unwrap();
    meta.extend_file_size(file, 8).await.unwrap();
    let plan = meta.read_plan(file, 0, 0, 8).await.unwrap();
    assert_eq!(
        plan.segments,
        vec![
            ReadPlanSegment::Data {
                logical_offset: 0,
                length: 2,
                slice_id: 10,
                slice_offset: 0,
            },
            ReadPlanSegment::Data {
                logical_offset: 2,
                length: 1,
                slice_id: 11,
                slice_offset: 0,
            },
            ReadPlanSegment::Zero {
                logical_offset: 3,
                length: 5,
            },
        ]
    );
    assert!(meta.range_has_data(file, 0, 3).await.unwrap());
    assert!(!meta.range_has_data(file, 3, 5).await.unwrap());
}

#[tokio::test]
async fn aligned_physical_slice_is_clipped_to_logical_eof() {
    let meta = test_meta().await;
    let file = meta
        .create_file(meta.root_ino(), "aligned-tail".into())
        .await
        .unwrap();
    let chunk_id = crate::vfs::chunk_id_for(file, 0).unwrap();

    meta.write(
        file,
        chunk_id,
        SliceDesc {
            slice_id: 20,
            chunk_id,
            offset: 0,
            length: 64 * 1024,
        },
        4 * 1024,
    )
    .await
    .unwrap();

    assert_eq!(meta.stat(file).await.unwrap().unwrap().size, 4 * 1024);
    assert_eq!(
        meta.read_plan(file, 0, 0, 4 * 1024).await.unwrap().segments,
        vec![ReadPlanSegment::Data {
            logical_offset: 0,
            length: 4 * 1024,
            slice_id: 20,
            slice_offset: 0,
        }]
    );

    meta.extend_file_size(file, 8 * 1024).await.unwrap();
    assert_eq!(
        meta.read_plan(file, 0, 0, 8 * 1024).await.unwrap().segments,
        vec![
            ReadPlanSegment::Data {
                logical_offset: 0,
                length: 4 * 1024,
                slice_id: 20,
                slice_offset: 0,
            },
            ReadPlanSegment::Zero {
                logical_offset: 4 * 1024,
                length: 4 * 1024,
            },
        ]
    );
}

#[tokio::test]
async fn punch_hole_and_zero_range_replace_data_without_lower_byte_revival() {
    let meta = test_meta().await;
    let file = meta
        .create_file(meta.root_ino(), "hole-data".into())
        .await
        .unwrap();
    let chunk_id = crate::vfs::chunk_id_for(file, 0).unwrap();
    meta.write(
        file,
        chunk_id,
        SliceDesc {
            slice_id: 12,
            chunk_id,
            offset: 0,
            length: 8,
        },
        8,
    )
    .await
    .unwrap();

    assert_eq!(meta.apply_hole_range(file, 2, 3, true).await.unwrap(), 8);
    assert_eq!(
        meta.read_plan(file, 0, 0, 8).await.unwrap().segments,
        vec![
            ReadPlanSegment::Data {
                logical_offset: 0,
                length: 2,
                slice_id: 12,
                slice_offset: 0,
            },
            ReadPlanSegment::Zero {
                logical_offset: 2,
                length: 3,
            },
            ReadPlanSegment::Data {
                logical_offset: 5,
                length: 3,
                slice_id: 12,
                slice_offset: 5,
            },
        ]
    );
    assert_eq!(meta.apply_hole_range(file, 7, 4, false).await.unwrap(), 11);
    assert_eq!(meta.stat(file).await.unwrap().unwrap().size, 11);
    assert_eq!(
        meta.read_plan(file, 0, 7, 4).await.unwrap().segments,
        vec![ReadPlanSegment::Zero {
            logical_offset: 7,
            length: 4,
        }]
    );
}

#[tokio::test]
async fn locks_conflict_within_a_workspace_but_not_across_workspace_instances() {
    let meta = test_meta().await;
    let other_workspace = test_meta().await;
    let file = meta
        .create_file(meta.root_ino(), "locked".into())
        .await
        .unwrap();
    let range = FileLockRange { start: 0, end: 10 };

    meta.set_plock(file, 1, false, FileLockType::Write, range, 100)
        .await
        .unwrap();
    let conflict = meta
        .get_plock(
            file,
            &FileLockQuery {
                owner: 2,
                lock_type: FileLockType::Read,
                range,
            },
        )
        .await
        .unwrap();
    assert_eq!(conflict.lock_type, FileLockType::Write);
    assert!(
        meta.set_plock(file, 2, false, FileLockType::Read, range, 200)
            .await
            .is_err()
    );
    other_workspace
        .set_plock(file, 2, false, FileLockType::Write, range, 200)
        .await
        .unwrap();

    meta.set_flock(file, 1, false, FileLockType::Write)
        .await
        .unwrap();
    assert!(
        meta.set_flock(file, 2, false, FileLockType::Read)
            .await
            .is_err()
    );
    other_workspace
        .set_flock(file, 2, false, FileLockType::Write)
        .await
        .unwrap();
}

#[tokio::test]
async fn open_unlink_keeps_inode_alive_until_last_close() {
    let meta = test_meta().await;
    let root = meta.root_ino();
    let file = meta.create_file(root, "open".into()).await.unwrap();
    let attr = meta.stat(file).await.unwrap().unwrap();
    meta.record_open(file, attr, true, true, false)
        .await
        .unwrap();
    meta.unlink(root, "open").await.unwrap();
    assert_eq!(meta.lookup(root, "open").await.unwrap(), None);
    assert_eq!(meta.stat(file).await.unwrap().unwrap().nlink, 0);
    meta.record_close(file).await.unwrap();
    assert!(meta.stat(file).await.unwrap().is_none());
}

#[tokio::test]
async fn concurrent_directory_creates_do_not_lose_parent_updates() {
    let meta = Arc::new(test_meta().await);
    let root = meta.root_ino();
    let mut tasks = Vec::new();
    for index in 0..32 {
        let meta = meta.clone();
        tasks.push(tokio::spawn(async move {
            meta.mkdir(root, format!("dir-{index}")).await.unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(meta.readdir(root).await.unwrap().len(), 32);
    assert_eq!(meta.stat(root).await.unwrap().unwrap().nlink, 34);
}

#[tokio::test]
async fn sibling_workspaces_share_the_base_but_isolate_namespace_and_data_changes() {
    let base = test_meta().await;
    let store = base.store().clone();
    let base_view = base.view_context().await;
    let root = base.root_ino();
    let file = base.create_file(root, "shared".into()).await.unwrap();
    let chunk_id = crate::vfs::chunk_id_for(file, 0).unwrap();
    base.write(
        file,
        chunk_id,
        SliceDesc {
            chunk_id,
            slice_id: 501,
            offset: 0,
            length: 8,
        },
        8,
    )
    .await
    .unwrap();
    let revision = WorkspaceLifecycle::new(store.clone())
        .seal(&base_view, &NoopDurableRemoteBarrier)
        .await
        .unwrap()
        .revision;
    let children = WorkspaceLifecycle::new(store.clone())
        .fork_revision(revision, 2, Some("agent".into()))
        .await
        .unwrap();

    let mut views = Vec::new();
    for (index, child) in children.iter().enumerate() {
        let lease = store
            .acquire_lease(AcquireLease {
                workspace_id: child.workspace_id,
                lease_id: LeaseId::new(),
                holder_generation: index as u64 + 10,
                ttl_ns: 60_000_000_000,
            })
            .await
            .unwrap();
        views.push(ViewContext {
            workspace_id: child.workspace_id,
            head_layer_id: child.head_layer_id,
            head_epoch: child.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        });
    }
    let left = WorkspaceMetaLayer::new(store.clone(), views[0].clone());
    let right = WorkspaceMetaLayer::new(store, views[1].clone());
    assert_eq!(left.lookup(root, "shared").await.unwrap(), Some(file));
    assert_eq!(right.lookup(root, "shared").await.unwrap(), Some(file));

    left.create_file(root, "left-only".into()).await.unwrap();
    right.create_file(root, "right-only".into()).await.unwrap();
    left.write(
        file,
        chunk_id,
        SliceDesc {
            chunk_id,
            slice_id: 502,
            offset: 2,
            length: 2,
        },
        8,
    )
    .await
    .unwrap();
    right
        .write(
            file,
            chunk_id,
            SliceDesc {
                chunk_id,
                slice_id: 503,
                offset: 4,
                length: 2,
            },
            8,
        )
        .await
        .unwrap();

    assert_eq!(left.lookup(root, "right-only").await.unwrap(), None);
    assert_eq!(right.lookup(root, "left-only").await.unwrap(), None);
    assert!(
        left.read_plan(file, 0, 0, 8)
            .await
            .unwrap()
            .segments
            .iter()
            .any(|segment| matches!(segment, ReadPlanSegment::Data { slice_id: 502, .. }))
    );
    assert!(
        !left
            .read_plan(file, 0, 0, 8)
            .await
            .unwrap()
            .segments
            .iter()
            .any(|segment| matches!(segment, ReadPlanSegment::Data { slice_id: 503, .. }))
    );
    assert!(
        right
            .read_plan(file, 0, 0, 8)
            .await
            .unwrap()
            .segments
            .iter()
            .any(|segment| matches!(segment, ReadPlanSegment::Data { slice_id: 503, .. }))
    );
}
