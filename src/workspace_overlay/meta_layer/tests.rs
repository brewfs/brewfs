use std::sync::Arc;

use uuid::Uuid;

use crate::chunk::SliceDesc;
use crate::chunk::read_plan::{ReadPlanSegment, WorkspaceReadPlanProvider};
use crate::meta::MetaLayer;
use crate::meta::file_lock::{FileLockQuery, FileLockRange, FileLockType};
use crate::meta::store::{FileType, MetaError, SetAttrFlags, SetAttrRequest};
use crate::workspace_overlay::catalog::{AcquireLease, CreateVolumeRoot, WorkspaceStore};
use crate::workspace_overlay::ids::{LayerId, LeaseId, WorkspaceId};
use crate::workspace_overlay::lifecycle::WorkspaceLifecycle;
use crate::workspace_overlay::model::ViewContext;
use crate::workspace_overlay::stores::database::SqliteWorkspaceStore;

use super::WorkspaceMetaLayer;

#[path = "tests/existing_api_contract_tests.rs"]
mod existing_api_contract_tests;

#[path = "tests/packed_lower_tests.rs"]
mod packed_lower_tests;

#[path = "tests/packed_open_preparation_tests.rs"]
mod packed_open_preparation_tests;

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
            volume_format: "workspace-v1".into(),
            schema_version: crate::workspace_overlay::model::WORKSPACE_SCHEMA_VERSION,
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
async fn rename_exchange_cross_parent_directory_file_updates_nlink() {
    let meta = test_meta().await;
    let root = meta.root_ino();
    let a = meta.mkdir(root, "A".into()).await.unwrap();
    let b = meta.mkdir(root, "B".into()).await.unwrap();
    let d = meta.mkdir(a, "d".into()).await.unwrap();
    let f = meta.create_file(b, "f".into()).await.unwrap();
    let before_a = meta.stat(a).await.unwrap().unwrap();
    let before_b = meta.stat(b).await.unwrap().unwrap();
    assert_eq!((before_a.nlink, before_b.nlink), (3, 2));

    meta.rename_exchange(a, "d", b, "f").await.unwrap();

    assert_eq!(meta.lookup(a, "d").await.unwrap(), Some(f));
    assert_eq!(meta.lookup(b, "f").await.unwrap(), Some(d));
    let after_a = meta.stat(a).await.unwrap().unwrap();
    let after_b = meta.stat(b).await.unwrap().unwrap();
    eprintln!(
        "parent nlink before: ({}, {}), after: ({}, {})",
        before_a.nlink, before_b.nlink, after_a.nlink, after_b.nlink
    );
    eprintln!(
        "parent mtime/ctime unchanged: A={}, B={}",
        (before_a.mtime, before_a.ctime) == (after_a.mtime, after_a.ctime),
        (before_b.mtime, before_b.ctime) == (after_b.mtime, after_b.ctime)
    );
    assert_eq!((after_a.nlink, after_b.nlink), (2, 3));
}

#[tokio::test]
async fn rename_exchange_cross_parent_updates_metadata_for_each_entry_kind() {
    for (left_dir, right_dir) in [(true, false), (false, true), (true, true), (false, false)] {
        assert_exchange_metadata(left_dir, right_dir, false).await;
    }
}

#[tokio::test]
async fn rename_exchange_same_parent_updates_timestamps_without_changing_nlink() {
    for (left_dir, right_dir) in [(true, false), (false, true), (true, true), (false, false)] {
        assert_exchange_metadata(left_dir, right_dir, true).await;
    }
}

async fn assert_exchange_metadata(left_dir: bool, right_dir: bool, same_parent: bool) {
    let meta = test_meta().await;
    let a = meta.mkdir(meta.root_ino(), "A".into()).await.unwrap();
    let b = if same_parent {
        a
    } else {
        meta.mkdir(meta.root_ino(), "B".into()).await.unwrap()
    };
    let left = if left_dir {
        meta.mkdir(a, "left".into()).await.unwrap()
    } else {
        meta.create_file(a, "left".into()).await.unwrap()
    };
    let right = if right_dir {
        meta.mkdir(b, "right".into()).await.unwrap()
    } else {
        meta.create_file(b, "right".into()).await.unwrap()
    };
    // Fixed old timestamps make the assertions independent of clock resolution.
    for ino in [a, b, left, right] {
        meta.set_attr(
            ino,
            &SetAttrRequest {
                atime: Some(100),
                mtime: Some(200),
                ctime: Some(300),
                ..SetAttrRequest::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();
    }

    meta.rename_exchange(a, "left", b, "right").await.unwrap();

    let reopened = WorkspaceMetaLayer::new(meta.store().clone(), meta.view_context().await);
    for reader in [&meta, &reopened] {
        assert_eq!(reader.lookup(a, "left").await.unwrap(), Some(right));
        assert_eq!(reader.lookup(b, "right").await.unwrap(), Some(left));
        let after_a = reader.stat(a).await.unwrap().unwrap();
        let after_b = reader.stat(b).await.unwrap().unwrap();
        let expected_a = 2 + u32::from(right_dir) + u32::from(same_parent && left_dir);
        let expected_b = 2 + u32::from(left_dir) + u32::from(same_parent && right_dir);
        assert_eq!((after_a.nlink, after_b.nlink), (expected_a, expected_b));
        for parent in [&after_a, &after_b] {
            assert!(parent.mtime > 300);
            assert_eq!(parent.ctime, parent.mtime);
            assert_eq!(parent.atime, 100);
        }
        for (ino, is_dir, parent) in [(left, left_dir, b), (right, right_dir, a)] {
            let inode = reader.resolve_inode_delta(ino).await.unwrap().unwrap();
            assert_eq!(inode.ctime_ns, after_a.ctime);
            assert_eq!(inode.mtime_ns, 200);
            assert_eq!(inode.atime_ns, 100);
            assert_eq!(inode.nlink, if is_dir { 2 } else { 1 });
            if is_dir {
                assert_eq!(inode.parent_hint, Some(parent));
            }
        }
    }
}

#[tokio::test]
async fn rename_exchange_same_inode_is_a_noop() {
    let meta = test_meta().await;
    let root = meta.root_ino();
    let other = meta.mkdir(root, "other".into()).await.unwrap();
    let file = meta.create_file(root, "file".into()).await.unwrap();
    meta.link(file, root, "alias").await.unwrap();
    meta.link(file, other, "alias").await.unwrap();
    let mut before = Vec::new();
    for ino in [root, other, file] {
        before.push(meta.resolve_inode_delta(ino).await.unwrap().unwrap());
    }
    for (parent, name) in [(root, "file"), (root, "alias"), (other, "alias")] {
        meta.rename_exchange(root, "file", parent, name)
            .await
            .unwrap();
        assert_eq!(meta.lookup(root, "file").await.unwrap(), Some(file));
        assert_eq!(meta.lookup(parent, name).await.unwrap(), Some(file));
        for inode in &before {
            assert_eq!(
                meta.resolve_inode_delta(inode.ino).await.unwrap().as_ref(),
                Some(inode)
            );
        }
    }
}

#[tokio::test]
async fn rename_onto_same_inode_keeps_both_hard_links() {
    let meta = test_meta().await;
    let root = meta.root_ino();
    let file = meta.create_file(root, "file".into()).await.unwrap();
    meta.link(file, root, "alias").await.unwrap();

    meta.rename(root, "file", root, "alias".into())
        .await
        .unwrap();

    assert_eq!(meta.lookup(root, "file").await.unwrap(), Some(file));
    assert_eq!(meta.lookup(root, "alias").await.unwrap(), Some(file));
    assert_eq!(meta.stat(file).await.unwrap().unwrap().nlink, 2);
}

#[tokio::test]
async fn rename_exchange_rejects_descendants() {
    let meta = test_meta().await;
    let root = meta.root_ino();
    let ancestor = meta.mkdir(root, "ancestor".into()).await.unwrap();
    let intermediate = meta.mkdir(ancestor, "intermediate".into()).await.unwrap();
    let descendant = meta.mkdir(intermediate, "descendant".into()).await.unwrap();
    let file = meta.create_file(ancestor, "file".into()).await.unwrap();

    let error = meta
        .rename_exchange(root, "ancestor", intermediate, "descendant")
        .await
        .unwrap_err();
    assert!(matches!(error, MetaError::InvalidPath(_)));
    assert_eq!(meta.lookup(root, "ancestor").await.unwrap(), Some(ancestor));
    assert_eq!(
        meta.lookup(intermediate, "descendant").await.unwrap(),
        Some(descendant)
    );

    let error = meta
        .rename_exchange(intermediate, "descendant", root, "ancestor")
        .await
        .unwrap_err();
    assert!(matches!(error, MetaError::InvalidPath(_)));
    assert_eq!(meta.lookup(root, "ancestor").await.unwrap(), Some(ancestor));
    assert_eq!(
        meta.lookup(intermediate, "descendant").await.unwrap(),
        Some(descendant)
    );

    let error = meta
        .rename_exchange(root, "ancestor", ancestor, "file")
        .await
        .unwrap_err();
    assert!(matches!(error, MetaError::InvalidPath(_)));
    assert_eq!(meta.lookup(root, "ancestor").await.unwrap(), Some(ancestor));
    assert_eq!(meta.lookup(ancestor, "file").await.unwrap(), Some(file));

    let error = meta
        .rename_exchange(root, "missing", ancestor, "file")
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MetaError::EntryNotFound { parent, name }
            if parent == root && name == "missing"
    ));
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
async fn concurrent_open_before_final_close_keeps_unlinked_inode_alive() {
    let meta = Arc::new(test_meta().await);
    let root = meta.root_ino();
    let file = meta.create_file(root, "open-race".into()).await.unwrap();
    let attr = meta.stat(file).await.unwrap().unwrap();
    meta.record_open(file, attr.clone(), true, false, false)
        .await
        .unwrap();
    meta.unlink(root, "open-race").await.unwrap();

    let gate = meta.mutation_gate.lock().await;
    let opening_meta = meta.clone();
    let mut opening = tokio::spawn(async move {
        opening_meta
            .record_open(file, attr, true, false, false)
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut opening)
            .await
            .is_err(),
        "the open must be queued behind the held mutation gate"
    );

    let closing_meta = meta.clone();
    let mut closing = tokio::spawn(async move { closing_meta.record_close(file).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut closing)
            .await
            .is_err(),
        "the close must be queued behind the open"
    );

    drop(gate);
    opening.await.unwrap().unwrap();
    closing.await.unwrap().unwrap();
    assert!(meta.stat(file).await.unwrap().is_some());
    meta.record_close(file).await.unwrap();
    assert!(meta.stat(file).await.unwrap().is_none());
}

#[tokio::test]
async fn final_close_before_concurrent_open_rejects_stale_inode() {
    let meta = Arc::new(test_meta().await);
    let root = meta.root_ino();
    let file = meta
        .create_file(root, "close-wins-race".into())
        .await
        .unwrap();
    let attr = meta.stat(file).await.unwrap().unwrap();
    meta.record_open(file, attr.clone(), true, false, false)
        .await
        .unwrap();
    meta.unlink(root, "close-wins-race").await.unwrap();

    let gate = meta.mutation_gate.lock().await;
    let closing_meta = meta.clone();
    let mut closing = tokio::spawn(async move { closing_meta.record_close(file).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut closing)
            .await
            .is_err(),
        "the close must be queued first"
    );

    let opening_meta = meta.clone();
    let mut opening = tokio::spawn(async move {
        opening_meta
            .record_open(file, attr, true, false, false)
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut opening)
            .await
            .is_err(),
        "the open must be queued behind the close"
    );

    drop(gate);
    closing.await.unwrap().unwrap();
    assert!(matches!(
        opening.await.unwrap(),
        Err(crate::meta::store::MetaError::NotFound(ino)) if ino == file
    ));
    assert!(!meta.open_counts.contains_key(&file));
    assert!(meta.stat(file).await.unwrap().is_none());
}

#[tokio::test]
async fn stale_open_paths_do_not_allocate_a_handle_or_increment_open_count() {
    let meta = Arc::new(test_meta().await);
    let root = meta.root_ino();
    let file = meta
        .create_file(root, "stale-vfs-open".into())
        .await
        .unwrap();
    let stale_attr = meta.stat(file).await.unwrap().unwrap();
    meta.unlink(root, "stale-vfs-open").await.unwrap();

    let layout = crate::chunk::layout::ChunkLayout::default();
    let fs = crate::vfs::fs::VFS::from_workspace_components(
        crate::vfs::config::VFSConfig::new(layout),
        Arc::new(crate::chunk::store::InMemoryBlockStore::new()),
        meta.clone(),
    )
    .unwrap();
    assert!(matches!(
        fs.open_with_cached_attr(file, stale_attr.clone(), true, false, false)
            .await,
        Err(crate::vfs::error::VfsError::NotFound { .. })
    ));
    assert!(matches!(
        fs.open(file, stale_attr.clone(), true, false, false).await,
        Err(crate::vfs::error::VfsError::NotFound { .. })
    ));
    assert!(matches!(
        fs.open_guard(file, stale_attr, true, false).await,
        Err(crate::vfs::error::VfsError::NotFound { .. })
    ));
    assert!(
        !meta.open_counts.contains_key(&file),
        "rejected opens must not increment the metadata open count"
    );
    assert!(
        fs.handles_for(file).is_empty(),
        "rejected opens must not leak an allocated VFS handle"
    );
}

#[tokio::test]
async fn statfs_rejects_unbounded_layer_scans_until_usage_counters_exist() {
    let meta = test_meta().await;
    assert!(matches!(
        meta.stat_fs().await,
        Err(crate::meta::store::MetaError::NotSupported(message))
            if message == "workspace statfs requires persistent usage counters"
    ));
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
    let revision = crate::workspace_overlay::test_base_fixture::install_test_base(
        &store,
        base_view.workspace_id,
    )
    .await
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

fn linux_acl(rows: &[(u16, u16, u32)]) -> Vec<u8> {
    let mut value = 2u32.to_le_bytes().to_vec();
    for (tag, permission, id) in rows {
        value.extend_from_slice(&tag.to_le_bytes());
        value.extend_from_slice(&permission.to_le_bytes());
        value.extend_from_slice(&id.to_le_bytes());
    }
    value
}

fn named_acl(mode: u32) -> Vec<u8> {
    linux_acl(&[
        (1, ((mode >> 6) & 7) as u16, u32::MAX),
        (2, 7, 1234),
        (4, 7, u32::MAX),
        (16, ((mode >> 3) & 7) as u16, u32::MAX),
        (32, (mode & 7) as u16, u32::MAX),
    ])
}

#[tokio::test]
async fn writable_acl_set_and_chmod_keep_mode_and_named_entries_together() {
    let meta = test_meta().await;
    let ino = meta.create_file(1, "acl-file".into()).await.unwrap();
    meta.set_xattr(ino, "system.posix_acl_access", &named_acl(0o640), 0)
        .await
        .unwrap();
    assert_eq!(meta.stat(ino).await.unwrap().unwrap().mode, 0o640);
    meta.set_attr(
        ino,
        &SetAttrRequest {
            mode: Some(0o751),
            ..Default::default()
        },
        SetAttrFlags::empty(),
    )
    .await
    .unwrap();
    let raw = meta
        .get_xattr(ino, "system.posix_acl_access")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw, named_acl(0o751));
    meta.remove_xattr(ino, "system.posix_acl_access")
        .await
        .unwrap();
    assert_eq!(meta.stat(ino).await.unwrap().unwrap().mode, 0o751);
    assert_eq!(
        meta.get_xattr(ino, "system.posix_acl_access")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn writable_acl_xattr_flags_are_checked_atomically() {
    let meta = test_meta().await;
    let ino = meta.create_file(1, "acl-flags".into()).await.unwrap();
    let first = named_acl(0o640);
    let replacement = named_acl(0o750);

    meta.set_xattr(
        ino,
        "system.posix_acl_access",
        &first,
        libc::XATTR_CREATE as u32,
    )
    .await
    .unwrap();
    let error = meta
        .set_xattr(
            ino,
            "system.posix_acl_access",
            &replacement,
            libc::XATTR_CREATE as u32,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, MetaError::Io(error) if error.raw_os_error() == Some(libc::EEXIST)));
    assert_eq!(
        meta.get_xattr(ino, "system.posix_acl_access")
            .await
            .unwrap(),
        Some(first.clone())
    );

    meta.set_xattr(
        ino,
        "system.posix_acl_access",
        &replacement,
        libc::XATTR_REPLACE as u32,
    )
    .await
    .unwrap();
    assert_eq!(
        meta.get_xattr(ino, "system.posix_acl_access")
            .await
            .unwrap(),
        Some(replacement.clone())
    );
    meta.remove_xattr(ino, "system.posix_acl_access")
        .await
        .unwrap();
    let error = meta
        .remove_xattr(ino, "system.posix_acl_access")
        .await
        .unwrap_err();
    assert!(matches!(error, MetaError::Io(error) if error.raw_os_error() == Some(libc::ENODATA)));

    let error = meta
        .set_xattr(
            ino,
            "system.posix_acl_access",
            &first,
            libc::XATTR_REPLACE as u32,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, MetaError::Io(error) if error.raw_os_error() == Some(libc::ENODATA)));
    let error = meta
        .set_xattr(
            ino,
            "system.posix_acl_access",
            &first,
            (libc::XATTR_CREATE | libc::XATTR_REPLACE) as u32,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, MetaError::Io(error) if error.raw_os_error() == Some(libc::EINVAL)));

    let dir = meta.mkdir(1, "acl-default-flags".into()).await.unwrap();
    meta.set_xattr(
        dir,
        "system.posix_acl_default",
        &first,
        libc::XATTR_CREATE as u32,
    )
    .await
    .unwrap();
    let error = meta
        .set_xattr(
            dir,
            "system.posix_acl_default",
            &replacement,
            libc::XATTR_CREATE as u32,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, MetaError::Io(error) if error.raw_os_error() == Some(libc::EEXIST)));
}

#[tokio::test]
async fn writable_acl_parent_default_is_inherited_and_directory_default_is_preserved() {
    let meta = test_meta().await;
    let directory = meta.mkdir(1, "acl-parent".into()).await.unwrap();
    let default = named_acl(0o770);
    meta.set_xattr(directory, "system.posix_acl_default", &default, 0)
        .await
        .unwrap();
    let child = meta
        .create_node_with_attr(
            directory,
            "child".into(),
            FileType::File,
            0o660,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    assert_eq!(
        meta.get_xattr(child, "system.posix_acl_access")
            .await
            .unwrap(),
        Some(named_acl(0o660))
    );
    let nested = meta
        .create_node_with_attr(
            directory,
            "nested".into(),
            FileType::Dir,
            0o750,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    assert_eq!(
        meta.get_xattr(nested, "system.posix_acl_default")
            .await
            .unwrap(),
        Some(default)
    );
    assert_eq!(
        meta.get_xattr(nested, "system.posix_acl_access")
            .await
            .unwrap(),
        Some(named_acl(0o750))
    );
}

#[tokio::test]
async fn writable_acl_base_form_uses_mode_and_invalid_targets_are_rejected() {
    let meta = test_meta().await;
    let ino = meta.create_file(1, "basic-acl".into()).await.unwrap();
    let basic = linux_acl(&[(1, 7, u32::MAX), (4, 5, u32::MAX), (32, 0, u32::MAX)]);
    meta.set_xattr(ino, "system.posix_acl_access", &basic, 0)
        .await
        .unwrap();
    assert_eq!(meta.stat(ino).await.unwrap().unwrap().mode, 0o750);
    assert_eq!(
        meta.get_xattr(ino, "system.posix_acl_access")
            .await
            .unwrap(),
        None
    );
    for (name, value, expected) in [
        ("system.posix_acl_access", vec![1, 2, 3], libc::EINVAL),
        ("system.posix_acl_default", named_acl(0o770), libc::EACCES),
    ] {
        let error = meta.set_xattr(ino, name, &value, 0).await.unwrap_err();
        assert!(matches!(error, MetaError::Io(error) if error.raw_os_error() == Some(expected)));
    }
    let link = meta.symlink(1, "acl-link", "basic-acl").await.unwrap().0;
    let error = meta
        .set_xattr(link, "system.posix_acl_access", &named_acl(0o640), 0)
        .await
        .unwrap_err();
    assert!(
        matches!(error, MetaError::Io(error) if error.raw_os_error() == Some(libc::EOPNOTSUPP))
    );
}

#[tokio::test]
async fn writable_acl_precommit_fault_keeps_original_inode_and_acl() {
    use crate::workspace_overlay::stores::database::StoreFailpoint;
    let meta = test_meta().await;
    let ino = meta.create_file(1, "acl-fault".into()).await.unwrap();
    let before = meta.stat(ino).await.unwrap().unwrap();
    meta.store().set_failpoint(StoreFailpoint::BeforeCommit);
    assert!(
        meta.set_xattr(ino, "system.posix_acl_access", &named_acl(0o640), 0)
            .await
            .is_err()
    );
    meta.store().set_failpoint(StoreFailpoint::Disabled);
    let after = meta.stat(ino).await.unwrap().unwrap();
    assert_eq!((after.mode, after.ctime), (before.mode, before.ctime));
    assert_eq!(
        meta.get_xattr(ino, "system.posix_acl_access")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn writable_acl_raw_umask_and_setgid_inheritance_match_parent_version() {
    let meta = test_meta().await;
    let plain = meta
        .create_node_with_umask(
            1,
            "plain".into(),
            FileType::File,
            0o666,
            0o077,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    assert_eq!(meta.stat(plain).await.unwrap().unwrap().mode, 0o600);
    let parent = meta
        .create_node_with_umask(1, "sgid".into(), FileType::Dir, 0o2770, 0, 1000, 4444, 0)
        .await
        .unwrap()
        .ino;
    meta.update_posix_acl(
        parent,
        "system.posix_acl_default",
        Some(&named_acl(0o770)),
        1000,
        &[4444],
    )
    .await
    .unwrap();
    let child = meta
        .create_node_with_umask(
            parent,
            "inherited".into(),
            FileType::File,
            0o666,
            0o077,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    let permissions = meta.inode_permissions(child).await.unwrap().unwrap();
    assert_eq!((permissions.attr.mode, permissions.attr.gid), (0o660, 4444));
    assert_eq!(permissions.access_acl, Some(named_acl(0o660)));
    let directory = meta
        .create_node_with_umask(
            parent,
            "nested-sgid".into(),
            FileType::Dir,
            0o777,
            0o077,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    assert_eq!(
        (
            meta.stat(directory).await.unwrap().unwrap().mode,
            meta.stat(directory).await.unwrap().unwrap().gid
        ),
        (0o2770, 4444)
    );
    assert_eq!(
        meta.get_xattr(directory, "system.posix_acl_default")
            .await
            .unwrap(),
        Some(named_acl(0o770))
    );
    let link = meta
        .symlink(parent, "symbolic", "inherited")
        .await
        .unwrap()
        .0;
    assert_eq!(
        meta.get_xattr(link, "system.posix_acl_access")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        meta.get_xattr(link, "system.posix_acl_default")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn writable_acl_owner_policy_empty_remove_and_setgid_rules_are_atomic() {
    let meta = test_meta().await;
    let ino = meta
        .create_node_with_umask(1, "owned".into(), FileType::File, 0o2640, 0, 1000, 2000, 0)
        .await
        .unwrap()
        .ino;
    let denied = meta
        .update_posix_acl(
            ino,
            "system.posix_acl_access",
            Some(&named_acl(0o640)),
            1234,
            &[2000],
        )
        .await
        .unwrap_err();
    assert!(matches!(denied, MetaError::Io(error) if error.raw_os_error() == Some(libc::EPERM)));
    assert_eq!(meta.stat(ino).await.unwrap().unwrap().mode, 0o2640);
    meta.update_posix_acl(
        ino,
        "system.posix_acl_access",
        Some(&named_acl(0o640)),
        1000,
        &[3000],
    )
    .await
    .unwrap();
    assert_eq!(meta.stat(ino).await.unwrap().unwrap().mode, 0o640);
    meta.update_posix_acl(
        ino,
        "system.posix_acl_access",
        Some(&2u32.to_le_bytes()),
        1000,
        &[3000],
    )
    .await
    .unwrap();
    assert_eq!(
        meta.get_xattr(ino, "system.posix_acl_access")
            .await
            .unwrap(),
        None
    );
    meta.update_posix_acl(ino, "system.posix_acl_access", None, 1000, &[3000])
        .await
        .unwrap();
    meta.update_posix_acl(ino, "system.posix_acl_default", Some(&[]), 1000, &[3000])
        .await
        .unwrap();
    assert_eq!(meta.stat(ino).await.unwrap().unwrap().mode, 0o640);
    let denied = meta
        .set_attr_as(
            ino,
            &SetAttrRequest {
                mode: Some(0o777),
                ..Default::default()
            },
            SetAttrFlags::empty(),
            1234,
            &[2000],
        )
        .await
        .unwrap_err();
    assert!(matches!(denied, MetaError::Io(error) if error.raw_os_error() == Some(libc::EPERM)));
}

#[tokio::test]
async fn writable_acl_combined_chmod_truncate_fault_preserves_whole_version() {
    use crate::workspace_overlay::stores::database::StoreFailpoint;
    let meta = test_meta().await;
    let ino = meta.create_file(1, "combined-acl".into()).await.unwrap();
    meta.set_xattr(ino, "system.posix_acl_access", &named_acl(0o640), 0)
        .await
        .unwrap();
    meta.truncate(ino, 8192, crate::chunk::layout::DEFAULT_CHUNK_SIZE)
        .await
        .unwrap();
    let before = meta.permission_snapshot(vec![ino], None).await.unwrap();
    meta.store().set_failpoint(StoreFailpoint::BeforeCommit);
    assert!(
        meta.set_attr(
            ino,
            &SetAttrRequest {
                mode: Some(0o751),
                size: Some(4096),
                ..Default::default()
            },
            SetAttrFlags::empty()
        )
        .await
        .is_err()
    );
    meta.store().set_failpoint(StoreFailpoint::Disabled);
    assert_eq!(
        meta.permission_snapshot(vec![ino], None).await.unwrap(),
        before
    );
    meta.set_attr(
        ino,
        &SetAttrRequest {
            mode: Some(0o751),
            size: Some(4096),
            ..Default::default()
        },
        SetAttrFlags::empty(),
    )
    .await
    .unwrap();
    let after = meta.inode_permissions(ino).await.unwrap().unwrap();
    assert_eq!((after.attr.mode, after.attr.size), (0o751, 4096));
    assert_eq!(after.access_acl, Some(named_acl(0o751)));
}

#[tokio::test]
async fn writable_acl_independent_sqlite_instances_keep_mode_acl_during_atime_writes() {
    use crate::meta::store::OpenFlags;
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("permissions.sqlite").display()
    );
    let first = Arc::new(SqliteWorkspaceStore::connect(&url).await.unwrap());
    first.initialize_workspace_schema().await.unwrap();
    let workspace_id = WorkspaceId::new();
    let workspace = first
        .create_volume_root(CreateVolumeRoot {
            volume_format: "workspace-v1".into(),
            schema_version: crate::workspace_overlay::model::WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::new_v4(),
            workspace_id,
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: Some("independent-permissions".into()),
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
    let other_store = Arc::new(SqliteWorkspaceStore::connect(&url).await.unwrap());
    let observer = WorkspaceMetaLayer::new(other_store, view);
    let ino = writer.create_file(1, "shared-policy".into()).await.unwrap();
    writer
        .set_xattr(ino, "system.posix_acl_access", &named_acl(0o640), 0)
        .await
        .unwrap();
    let update = async {
        for mode in [0o604, 0o640].into_iter().cycle().take(64) {
            writer
                .set_attr(
                    ino,
                    &SetAttrRequest {
                        mode: Some(mode),
                        ..Default::default()
                    },
                    SetAttrFlags::empty(),
                )
                .await
                .unwrap();
        }
    };
    let read_and_touch = async {
        for _ in 0..128 {
            observer.open(ino, OpenFlags::RDONLY).await.unwrap();
            let permissions = observer.inode_permissions(ino).await.unwrap().unwrap();
            let raw = permissions.access_acl.unwrap();
            assert_eq!(
                crate::meta::posix_acl::PosixAcl::decode(&raw)
                    .unwrap()
                    .mode_bits(),
                permissions.attr.mode & 0o777
            );
        }
    };
    tokio::join!(update, read_and_touch);
    assert_eq!(
        observer
            .inode_permissions(ino)
            .await
            .unwrap()
            .unwrap()
            .access_acl,
        Some(named_acl(0o640))
    );
}

#[tokio::test]
async fn writable_acl_namespace_actor_checks_named_denial_sticky_and_scope_isolation() {
    use crate::meta::layer::{NamespaceActor, scope_namespace_actor};
    let meta = test_meta().await;
    let parent = meta
        .create_node_with_umask(
            1,
            "policy-parent".into(),
            FileType::Dir,
            0o777,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    let denied_acl = linux_acl(&[
        (1, 7, u32::MAX),
        (2, 0, 1234),
        (4, 7, u32::MAX),
        (16, 7, u32::MAX),
        (32, 7, u32::MAX),
    ]);
    meta.set_xattr(parent, "system.posix_acl_access", &denied_acl, 0)
        .await
        .unwrap();
    let actor = NamespaceActor {
        uid: 1234,
        gid: 3000,
        groups: vec![3000],
    };
    let denied = scope_namespace_actor(
        Some(actor.clone()),
        meta.create_file(parent, "must-not-appear".into()),
    )
    .await
    .unwrap_err();
    assert!(matches!(denied, MetaError::Io(error) if error.raw_os_error() == Some(libc::EACCES)));
    assert_eq!(meta.lookup(parent, "must-not-appear").await.unwrap(), None);
    assert!(crate::meta::layer::namespace_actor().is_none());
    meta.remove_xattr(parent, "system.posix_acl_access")
        .await
        .unwrap();
    meta.set_attr(
        parent,
        &SetAttrRequest {
            mode: Some(0o1777),
            ..Default::default()
        },
        SetAttrFlags::empty(),
    )
    .await
    .unwrap();
    let owned = meta
        .create_node_with_umask(
            parent,
            "owned-by-other".into(),
            FileType::File,
            0o644,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    let denied = scope_namespace_actor(Some(actor.clone()), meta.unlink(parent, "owned-by-other"))
        .await
        .unwrap_err();
    assert!(matches!(denied, MetaError::Io(error) if error.raw_os_error() == Some(libc::EPERM)));
    assert_eq!(
        meta.lookup(parent, "owned-by-other").await.unwrap(),
        Some(owned)
    );
    let created = scope_namespace_actor(
        Some(actor.clone()),
        meta.symlink(parent, "actor-symlink", "owned-by-other"),
    )
    .await
    .unwrap()
    .0;
    assert_eq!(
        (
            meta.stat(created).await.unwrap().unwrap().uid,
            meta.stat(created).await.unwrap().unwrap().gid
        ),
        (1234, 3000)
    );
    let parallel = async {
        let scoped = scope_namespace_actor(Some(actor), async {
            tokio::task::yield_now().await;
            assert_eq!(crate::meta::layer::namespace_actor().unwrap().uid, 1234);
        });
        let unscoped = async {
            tokio::task::yield_now().await;
            assert!(crate::meta::layer::namespace_actor().is_none());
        };
        tokio::join!(scoped, unscoped);
    };
    parallel.await;
    assert!(crate::meta::layer::namespace_actor().is_none());
}

#[tokio::test]
async fn writable_acl_namespace_actor_checks_changed_ancestor_permission() {
    use crate::meta::layer::{NamespaceActor, scope_namespace_actor};
    let meta = test_meta().await;
    let ancestor = meta
        .create_node_with_umask(
            1,
            "search-ancestor".into(),
            FileType::Dir,
            0o777,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    let parent = meta
        .create_node_with_umask(
            ancestor,
            "writable-child".into(),
            FileType::Dir,
            0o777,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    let actor = NamespaceActor {
        uid: 1234,
        gid: 3000,
        groups: vec![3000],
    };
    scope_namespace_actor(
        Some(actor.clone()),
        meta.create_file(parent, "initially-allowed".into()),
    )
    .await
    .unwrap();
    meta.set_attr(
        ancestor,
        &SetAttrRequest {
            mode: Some(0o700),
            ..Default::default()
        },
        SetAttrFlags::empty(),
    )
    .await
    .unwrap();
    let denied = scope_namespace_actor(
        Some(actor),
        meta.create_file(parent, "denied-after-revoke".into()),
    )
    .await
    .unwrap_err();
    assert!(matches!(denied, MetaError::Io(error) if error.raw_os_error() == Some(libc::EACCES)));
    assert_eq!(
        meta.lookup(parent, "denied-after-revoke").await.unwrap(),
        None
    );
}

fn control_acl_json(scope: &str, named_permission: Option<&str>) -> Vec<u8> {
    use crate::control::protocol::ControlAclEntry;
    let entry = |tag: &str, id, perm: &str| ControlAclEntry {
        scope: scope.into(),
        tag: tag.into(),
        id,
        perm: perm.into(),
    };
    let mut rows = vec![
        entry("user_obj", None, "rwx"),
        entry("group_obj", None, "---"),
        entry("other", None, "---"),
    ];
    if let Some(permission) = named_permission {
        rows.push(entry("user", Some(1234), permission));
        rows.push(entry("mask", None, "rwx"));
    }
    serde_json::to_vec(&rows).unwrap()
}

fn policy_actor() -> crate::meta::layer::NamespaceActor {
    crate::meta::layer::NamespaceActor {
        uid: 1234,
        gid: 3000,
        groups: vec![3000],
    }
}

#[tokio::test]
async fn writable_acl_control_default_only_falls_back_to_mode_for_namespace_and_size() {
    use crate::meta::layer::scope_namespace_actor;
    let meta = test_meta().await;
    let ancestor = meta
        .create_node_with_umask(
            1,
            "control-ancestor".into(),
            FileType::Dir,
            0o777,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    let parent = meta
        .create_node_with_umask(
            ancestor,
            "control-parent".into(),
            FileType::Dir,
            0o777,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    for ino in [ancestor, parent] {
        meta.set_xattr(
            ino,
            "system.brewfs.acl",
            &control_acl_json("default", None),
            0,
        )
        .await
        .unwrap();
    }
    let ino = scope_namespace_actor(
        Some(policy_actor()),
        meta.create_file(parent, "mode-fallback".into()),
    )
    .await
    .unwrap();
    meta.set_xattr(
        ino,
        "system.brewfs.acl",
        &control_acl_json("default", None),
        0,
    )
    .await
    .unwrap();
    meta.set_attr_as(
        ino,
        &SetAttrRequest {
            size: Some(17),
            ..Default::default()
        },
        SetAttrFlags::empty(),
        1234,
        &[3000],
    )
    .await
    .unwrap();
    assert_eq!(meta.stat(ino).await.unwrap().unwrap().size, 17);
}

#[tokio::test]
async fn writable_acl_control_denial_and_grant_govern_truncate_same_version() {
    let meta = test_meta().await;
    let ino = meta
        .create_node_with_umask(
            1,
            "control-size".into(),
            FileType::File,
            0o666,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    meta.set_xattr(
        ino,
        "system.brewfs.acl",
        &control_acl_json("access", Some("---")),
        0,
    )
    .await
    .unwrap();
    let before = meta.permission_snapshot(vec![ino], None).await.unwrap();
    let denied = meta
        .set_attr_as(
            ino,
            &SetAttrRequest {
                size: Some(5),
                ..Default::default()
            },
            SetAttrFlags::empty(),
            1234,
            &[3000],
        )
        .await
        .unwrap_err();
    assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EACCES)));
    assert_eq!(
        meta.permission_snapshot(vec![ino], None).await.unwrap(),
        before
    );
    meta.set_attr(
        ino,
        &SetAttrRequest {
            mode: Some(0),
            ..Default::default()
        },
        SetAttrFlags::empty(),
    )
    .await
    .unwrap();
    meta.set_xattr(
        ino,
        "system.brewfs.acl",
        &control_acl_json("access", Some("-w-")),
        0,
    )
    .await
    .unwrap();
    meta.set_attr_as(
        ino,
        &SetAttrRequest {
            size: Some(5),
            ..Default::default()
        },
        SetAttrFlags::empty(),
        1234,
        &[3000],
    )
    .await
    .unwrap();
    assert_eq!(meta.stat(ino).await.unwrap().unwrap().size, 5);
}

#[tokio::test]
async fn timestamp_setattr_group_writeback_current_time() {
    let meta = test_meta().await;
    let ino = meta
        .create_node_with_umask(1, "group-time".into(), FileType::File, 0o660, 0, 99, 99, 0)
        .await
        .unwrap()
        .ino;
    for mode in [0o660, 0o1660, 0o2660, 0o4660, 0o7660] {
        meta.set_attr(
            ino,
            &SetAttrRequest {
                mode: Some(mode),
                ..Default::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();
        let changed = meta
            .set_attr_as(
                ino,
                &SetAttrRequest {
                    atime: Some(123),
                    mtime: Some(456),
                    ctime: Some(456),
                    ..Default::default()
                },
                SetAttrFlags::SET_ATIME_NOW | SetAttrFlags::KERNEL_WRITEBACK_TIMES,
                100,
                &[99],
            )
            .await
            .unwrap();
        assert_eq!(changed.mtime, 456);
        assert_eq!((changed.uid, changed.gid, changed.mode), (99, 99, mode));
    }
}

#[tokio::test]
async fn writeback_timestamp_authority_requires_live_exclusive_lease() {
    use crate::workspace_overlay::lifecycle::LeaseValidity;
    use std::time::{Duration, Instant};

    let meta = test_meta().await;
    assert!(!meta.kernel_timestamp_permissions_authoritative());
    let validity = Arc::new(LeaseValidity::new(Instant::now(), Duration::from_secs(60)));
    let meta = meta.with_lease_bound_row_cache(validity.clone());
    assert!(meta.kernel_timestamp_permissions_authoritative());
    validity.revoke();
    assert!(!meta.kernel_timestamp_permissions_authoritative());

    let expired = Arc::new(LeaseValidity::new(Instant::now(), Duration::ZERO));
    let meta = test_meta().await.with_lease_bound_row_cache(expired);
    assert!(!meta.kernel_timestamp_permissions_authoritative());
}

#[tokio::test]
async fn writeback_timestamp_rechecks_current_groups_mode_and_acl() {
    let meta = test_meta().await;
    let ino = meta
        .create_node_with_umask(
            1,
            "checked-time".into(),
            FileType::File,
            0o660,
            0,
            99,
            99,
            0,
        )
        .await
        .unwrap()
        .ino;
    let request = SetAttrRequest {
        atime: Some(123),
        mtime: Some(456),
        ctime: Some(789),
        ..Default::default()
    };
    let flags = || SetAttrFlags::SET_ATIME_NOW | SetAttrFlags::KERNEL_WRITEBACK_TIMES;
    // Membership in a supplementary group grants exactly the same access.
    meta.set_attr_as(ino, &request, flags(), 100, &[100, 99])
        .await
        .unwrap();
    let before = meta.permission_snapshot(vec![ino], None).await.unwrap();
    for (groups, checked_flags, errno) in [
        (vec![100], flags(), libc::EACCES),
        (vec![99], SetAttrFlags::SET_ATIME_NOW, libc::EPERM),
    ] {
        let error = meta
            .set_attr_as(ino, &request, checked_flags, 100, &groups)
            .await
            .unwrap_err();
        assert!(matches!(error, MetaError::Io(e) if e.raw_os_error() == Some(errno)));
        assert_eq!(
            meta.permission_snapshot(vec![ino], None).await.unwrap(),
            before
        );
    }
    meta.set_attr(
        ino,
        &SetAttrRequest {
            mode: Some(0o600),
            ..Default::default()
        },
        SetAttrFlags::empty(),
    )
    .await
    .unwrap();
    let before = meta.permission_snapshot(vec![ino], None).await.unwrap();
    let error = meta
        .set_attr_as(ino, &request, flags(), 100, &[99])
        .await
        .unwrap_err();
    assert!(matches!(error, MetaError::Io(e) if e.raw_os_error() == Some(libc::EACCES)));
    assert_eq!(
        meta.permission_snapshot(vec![ino], None).await.unwrap(),
        before
    );

    meta.set_attr(
        ino,
        &SetAttrRequest {
            mode: Some(0o666),
            ..Default::default()
        },
        SetAttrFlags::empty(),
    )
    .await
    .unwrap();
    meta.set_xattr(
        ino,
        "system.brewfs.acl",
        &control_acl_json("access", Some("---")),
        0,
    )
    .await
    .unwrap();
    let before = meta.permission_snapshot(vec![ino], None).await.unwrap();
    let error = meta
        .set_attr_as(ino, &request, flags(), 1234, &[99])
        .await
        .unwrap_err();
    assert!(matches!(error, MetaError::Io(e) if e.raw_os_error() == Some(libc::EACCES)));
    assert_eq!(
        meta.permission_snapshot(vec![ino], None).await.unwrap(),
        before
    );
    meta.set_attr_as(ino, &request, flags(), 0, &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn writable_acl_explicit_near_now_and_single_now_require_owner_both_now_uses_write_acl() {
    let meta = test_meta().await;
    let ino = meta
        .create_node_with_umask(
            1,
            "timestamps".into(),
            FileType::File,
            0o666,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    let now = super::now_ns().unwrap();
    for (request, flags) in [
        (
            SetAttrRequest {
                atime: Some(now),
                mtime: Some(now),
                ..Default::default()
            },
            SetAttrFlags::empty(),
        ),
        (
            SetAttrRequest {
                atime: Some(now),
                ..Default::default()
            },
            SetAttrFlags::SET_ATIME_NOW,
        ),
    ] {
        let before = meta.permission_snapshot(vec![ino], None).await.unwrap();
        let denied = meta
            .set_attr_as(ino, &request, flags, 1234, &[3000])
            .await
            .unwrap_err();
        assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EPERM)));
        assert_eq!(
            meta.permission_snapshot(vec![ino], None).await.unwrap(),
            before
        );
    }
    let both_now = || SetAttrFlags::SET_ATIME_NOW | SetAttrFlags::SET_MTIME_NOW;
    let changed = meta
        .set_attr_as(ino, &SetAttrRequest::default(), both_now(), 1234, &[3000])
        .await
        .unwrap();
    assert_eq!(changed.atime, changed.mtime);
    assert!(changed.atime >= now);
    meta.set_xattr(
        ino,
        "system.brewfs.acl",
        &control_acl_json("access", Some("---")),
        0,
    )
    .await
    .unwrap();
    let denied = meta
        .set_attr_as(ino, &SetAttrRequest::default(), both_now(), 1234, &[3000])
        .await
        .unwrap_err();
    assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EACCES)));
    meta.set_attr_as(
        ino,
        &SetAttrRequest {
            atime: Some(42),
            ..Default::default()
        },
        SetAttrFlags::empty(),
        1000,
        &[2000],
    )
    .await
    .unwrap();
    assert_eq!(meta.stat(ino).await.unwrap().unwrap().atime, 42);
}

#[tokio::test]
async fn writable_acl_nonowner_unchanged_chown_rejected_and_root_killpriv_matches_linux() {
    let meta = test_meta().await;
    let ino = meta
        .create_node_with_umask(
            1,
            "chown-policy".into(),
            FileType::File,
            0o6755,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    for request in [
        SetAttrRequest {
            uid: Some(1000),
            ..Default::default()
        },
        SetAttrRequest {
            gid: Some(2000),
            ..Default::default()
        },
    ] {
        let before = meta.permission_snapshot(vec![ino], None).await.unwrap();
        let denied = meta
            .set_attr_as(ino, &request, SetAttrFlags::empty(), 1234, &[3000])
            .await
            .unwrap_err();
        assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EPERM)));
        assert_eq!(
            meta.permission_snapshot(vec![ino], None).await.unwrap(),
            before
        );
    }
    let changed = meta
        .set_attr_as(
            ino,
            &SetAttrRequest {
                uid: Some(1000),
                gid: Some(2000),
                ..Default::default()
            },
            SetAttrFlags::empty(),
            0,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(changed.mode, 0o755);
    meta.set_attr(
        ino,
        &SetAttrRequest {
            mode: Some(0o6644),
            ..Default::default()
        },
        SetAttrFlags::empty(),
    )
    .await
    .unwrap();
    let changed = meta
        .set_attr_as(
            ino,
            &SetAttrRequest {
                gid: Some(2000),
                ..Default::default()
            },
            SetAttrFlags::empty(),
            0,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(changed.mode, 0o2644);
    let dir = meta
        .create_node_with_umask(
            1,
            "chown-dir".into(),
            FileType::Dir,
            0o6755,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    let changed = meta
        .set_attr_as(
            dir,
            &SetAttrRequest {
                gid: Some(2000),
                ..Default::default()
            },
            SetAttrFlags::empty(),
            0,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(changed.mode, 0o6755);
}

#[tokio::test]
async fn writable_acl_foreign_group_special_bits_match_linux_create_and_chmod() {
    use crate::meta::layer::scope_namespace_actor;
    let meta = test_meta().await;
    let parent = meta
        .create_node_with_umask(
            1,
            "special-parent".into(),
            FileType::Dir,
            0o2777,
            0,
            1000,
            4001,
            0,
        )
        .await
        .unwrap()
        .ino;
    for (index, kind) in [
        FileType::File,
        FileType::Dir,
        FileType::Fifo,
        FileType::Socket,
    ]
    .into_iter()
    .enumerate()
    {
        let created = scope_namespace_actor(
            Some(policy_actor()),
            meta.create_node_with_umask(
                parent,
                format!("kind-{index}"),
                kind,
                0o2777,
                0,
                1234,
                3000,
                0,
            ),
        )
        .await
        .unwrap();
        let created_attr = created.attr.expect("created inode attributes");
        assert_eq!(created_attr.gid, 4001);
        assert_eq!(
            created_attr.mode,
            if kind == FileType::Dir { 0o2777 } else { 0o777 }
        );
        let changed = meta
            .set_attr_as(
                created.ino,
                &SetAttrRequest {
                    mode: Some(0o2777),
                    ..Default::default()
                },
                SetAttrFlags::empty(),
                1234,
                &[3000],
            )
            .await
            .unwrap();
        assert_eq!(changed.mode, 0o777);
    }
}

#[tokio::test]
async fn writable_acl_ancestor_corruption_cannot_skip_search_authorization() {
    use crate::meta::layer::scope_namespace_actor;
    let meta = test_meta().await;
    let parent = meta
        .create_node_with_umask(
            1,
            "bad-ancestry".into(),
            FileType::Dir,
            0o777,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    for hint in [Some(parent), None] {
        let expected = meta.mutation_version().await.unwrap();
        let mut inode = meta.resolve_inode_delta(parent).await.unwrap().unwrap();
        inode.parent_hint = hint;
        meta.mutate_inode(&expected, inode, Vec::new())
            .await
            .unwrap();
        let denied = scope_namespace_actor(
            Some(policy_actor()),
            meta.create_file(parent, "must-stay-absent".into()),
        )
        .await
        .unwrap_err();
        assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EIO)));
        assert_eq!(meta.lookup(parent, "must-stay-absent").await.unwrap(), None);
    }
    let expected = meta.mutation_version().await.unwrap();
    let mut inode = meta.resolve_inode_delta(parent).await.unwrap().unwrap();
    inode.parent_hint = Some(1);
    meta.mutate_inode(&expected, inode, Vec::new())
        .await
        .unwrap();
    scope_namespace_actor(
        Some(policy_actor()),
        meta.create_file(parent, "valid-ancestry".into()),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn writable_acl_write_handle_authority_is_inode_bound_and_cannot_authorize_chmod() {
    use crate::meta::layer::scope_setattr_write_handle;
    let meta = test_meta().await;
    let first = meta
        .create_node_with_umask(1, "opened".into(), FileType::File, 0o400, 0, 1000, 2000, 0)
        .await
        .unwrap()
        .ino;
    let other = meta
        .create_node_with_umask(
            1,
            "not-opened".into(),
            FileType::File,
            0o400,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    let request = SetAttrRequest {
        size: Some(17),
        ..Default::default()
    };
    let changed = scope_setattr_write_handle(
        first,
        true,
        meta.set_attr_as(first, &request, SetAttrFlags::empty(), 1234, &[3000]),
    )
    .await
    .unwrap();
    assert_eq!(changed.size, 17);
    let denied = scope_setattr_write_handle(
        first,
        true,
        meta.set_attr_as(other, &request, SetAttrFlags::empty(), 1234, &[3000]),
    )
    .await
    .unwrap_err();
    assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EACCES)));
    let denied = scope_setattr_write_handle(
        first,
        true,
        meta.set_attr_as(
            first,
            &SetAttrRequest {
                mode: Some(0o777),
                ..request
            },
            SetAttrFlags::empty(),
            1234,
            &[3000],
        ),
    )
    .await
    .unwrap_err();
    assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EPERM)));
    assert!(!crate::meta::layer::setattr_write_handle_authorizes(first));
}

#[tokio::test]
async fn writable_acl_independent_store_revoke_after_precheck_denies_open_trunc_and_fast_open() {
    use crate::meta::layer::scope_namespace_actor;
    use crate::meta::store::OpenFlags;
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("revocation.sqlite").display()
    );
    let first = Arc::new(SqliteWorkspaceStore::connect(&url).await.unwrap());
    first.initialize_workspace_schema().await.unwrap();
    let workspace_id = WorkspaceId::new();
    let workspace = first
        .create_volume_root(CreateVolumeRoot {
            volume_format: "workspace-v1".into(),
            schema_version: crate::workspace_overlay::model::WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::new_v4(),
            workspace_id,
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: Some("open-revoke".into()),
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
    let opener = WorkspaceMetaLayer::new(
        Arc::new(SqliteWorkspaceStore::connect(&url).await.unwrap()),
        view,
    );
    let ino = writer
        .create_node_with_umask(1, "revoke".into(), FileType::File, 0o666, 0, 1000, 2000, 0)
        .await
        .unwrap()
        .ino;
    writer
        .truncate(ino, 31, crate::chunk::layout::DEFAULT_CHUNK_SIZE)
        .await
        .unwrap();
    let old = opener.inode_permissions(ino).await.unwrap().unwrap();
    assert_eq!(old.attr.mode & 6, 6);
    writer
        .set_xattr(
            ino,
            "system.brewfs.acl",
            &control_acl_json("access", Some("---")),
            0,
        )
        .await
        .unwrap();
    let before = writer.permission_snapshot(vec![ino], None).await.unwrap();
    let denied = scope_namespace_actor(
        Some(policy_actor()),
        opener.open(ino, OpenFlags::WRONLY | OpenFlags::TRUNC),
    )
    .await
    .unwrap_err();
    assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EACCES)));
    assert_eq!(
        writer.permission_snapshot(vec![ino], None).await.unwrap(),
        before
    );
    let denied = scope_namespace_actor(
        Some(policy_actor()),
        opener.record_open(ino, old.attr, true, true, false),
    )
    .await
    .unwrap_err();
    assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EACCES)));
    assert!(!opener.open_counts.contains_key(&ino));
    let denied = crate::meta::layer::scope_open_actor(
        Some(policy_actor()),
        6,
        opener.set_attr_as(
            ino,
            &SetAttrRequest {
                size: Some(0),
                ..Default::default()
            },
            SetAttrFlags::empty(),
            1234,
            &[3000],
        ),
    )
    .await
    .unwrap_err();
    assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EACCES)));
    assert_eq!(
        writer.permission_snapshot(vec![ino], None).await.unwrap(),
        before
    );
    // Creation/open request flags require the full access mask at the commit,
    // so write-only ACL permission cannot satisfy an O_RDWR truncate.
    writer
        .set_xattr(
            ino,
            "system.brewfs.acl",
            &control_acl_json("access", Some("-w-")),
            0,
        )
        .await
        .unwrap();
    let expected = writer.permission_snapshot(vec![ino], None).await.unwrap();
    let denied = crate::meta::layer::scope_open_actor(
        Some(policy_actor()),
        6,
        opener.set_attr_as(
            ino,
            &SetAttrRequest {
                size: Some(0),
                ..Default::default()
            },
            SetAttrFlags::empty(),
            1234,
            &[3000],
        ),
    )
    .await
    .unwrap_err();
    assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EACCES)));
    assert_eq!(
        writer.permission_snapshot(vec![ino], None).await.unwrap(),
        expected
    );
    let mut stale = opener.resolve_inode_delta(ino).await.unwrap().unwrap();
    stale.size = 100;
    writer
        .set_attr(
            ino,
            &SetAttrRequest {
                mode: Some(0o600),
                ..Default::default()
            },
            SetAttrFlags::empty(),
        )
        .await
        .unwrap();
    let current = writer.permission_snapshot(vec![ino], None).await.unwrap();
    let denied = crate::meta::layer::scope_created_inode_open(
        ino,
        true,
        opener.mutate_inode(&expected.layers, stale, Vec::new()),
    )
    .await
    .unwrap_err();
    assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EBUSY)));
    assert_eq!(
        writer.permission_snapshot(vec![ino], None).await.unwrap(),
        current
    );
    assert_eq!(writer.stat(ino).await.unwrap().unwrap().size, 31);
}

#[tokio::test]
async fn writable_acl_kernel_ctime_only_chown_noop_matches_linux_without_write_permission() {
    let meta = test_meta().await;
    let ino = meta
        .create_node_with_umask(
            1,
            "ctime-noop".into(),
            FileType::File,
            0o444,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    let changed = meta
        .set_attr_as(
            ino,
            &SetAttrRequest {
                ctime: Some(42),
                ..Default::default()
            },
            SetAttrFlags::empty(),
            1234,
            &[3000],
        )
        .await
        .unwrap();
    assert_eq!(changed.ctime, 42);
    assert_eq!(
        (changed.uid, changed.gid, changed.mode),
        (1000, 2000, 0o444)
    );
}

#[tokio::test]
async fn writable_acl_created_mode_zero_first_open_is_single_use_and_inode_bound() {
    use crate::meta::layer::{scope_created_inode_open, scope_namespace_actor};
    let meta = test_meta().await;
    let parent = meta
        .create_node_with_umask(
            1,
            "mode-zero-parent".into(),
            FileType::Dir,
            0o777,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap()
        .ino;
    let created = scope_namespace_actor(
        Some(policy_actor()),
        meta.create_node_with_umask(
            parent,
            "mode-zero".into(),
            FileType::File,
            0,
            0,
            1234,
            3000,
            0,
        ),
    )
    .await
    .unwrap();
    let other = meta
        .create_node_with_umask(
            parent,
            "other-mode-zero".into(),
            FileType::File,
            0,
            0,
            1000,
            2000,
            0,
        )
        .await
        .unwrap();
    let created_attr = created.attr.expect("created inode attributes");
    let other_attr = other.attr.expect("other created inode attributes");
    scope_namespace_actor(
        Some(policy_actor()),
        scope_created_inode_open(created.ino, true, async {
            let denied = meta
                .record_open(other.ino, other_attr, false, true, false)
                .await
                .unwrap_err();
            assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EACCES)));
            assert!(crate::meta::layer::created_inode_open_authorizes(
                created.ino
            ));
            meta.record_open(created.ino, created_attr.clone(), false, true, false)
                .await
                .unwrap();
            assert!(!crate::meta::layer::created_inode_open_authorizes(
                created.ino
            ));
            let denied = meta
                .record_open(created.ino, created_attr.clone(), false, true, false)
                .await
                .unwrap_err();
            assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EACCES)));
            let denied = meta
                .set_attr_as(
                    created.ino,
                    &SetAttrRequest {
                        size: Some(17),
                        ..Default::default()
                    },
                    SetAttrFlags::empty(),
                    1234,
                    &[3000],
                )
                .await
                .unwrap_err();
            assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EACCES)));
        }),
    )
    .await;
    meta.record_close(created.ino).await.unwrap();
    let denied = scope_namespace_actor(
        Some(policy_actor()),
        meta.record_open(created.ino, created_attr, false, true, false),
    )
    .await
    .unwrap_err();
    assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::EACCES)));
    assert!(!crate::meta::layer::created_inode_open_authorizes(
        created.ino
    ));
}

#[tokio::test]
async fn writable_acl_created_and_handle_authority_cannot_bypass_lease_fencing() {
    use crate::meta::layer::{scope_created_inode_open, scope_setattr_write_handle};
    use crate::workspace_overlay::catalog::ReleaseLease;
    let meta = test_meta().await;
    let created = meta
        .create_node_with_umask(
            1,
            "lease-mode-zero".into(),
            FileType::File,
            0,
            0,
            1234,
            3000,
            0,
        )
        .await
        .unwrap();
    let before = meta
        .permission_snapshot(vec![created.ino], None)
        .await
        .unwrap();
    let guard = meta.guard().await;
    meta.store()
        .release_lease(ReleaseLease {
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
        })
        .await
        .unwrap();
    let request = SetAttrRequest {
        size: Some(7),
        ..Default::default()
    };
    let denied = scope_created_inode_open(
        created.ino,
        true,
        scope_setattr_write_handle(
            created.ino,
            true,
            meta.set_attr_as(created.ino, &request, SetAttrFlags::empty(), 1234, &[3000]),
        ),
    )
    .await
    .unwrap_err();
    assert!(matches!(denied,MetaError::Io(e) if e.raw_os_error()==Some(libc::ESTALE)));
    assert_eq!(
        meta.permission_snapshot(vec![created.ino], None)
            .await
            .unwrap(),
        before
    );
}
