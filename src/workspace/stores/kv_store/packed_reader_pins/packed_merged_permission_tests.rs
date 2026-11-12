//! Real VFS paths over authenticated lower attributes and actual conditional KV.

use super::*;
use crate::meta::posix_acl::PosixAcl;
use crate::workspace::catalog::{ExtentQuery, PermissionSnapshotQuery};
use crate::workspace::packed::snapshot::{
    SnapshotColdAttributes, SnapshotIndexReader, SnapshotProducer, SnapshotXattr,
};
use crate::workspace::packed::{
    GroupMeta, GroupMetaEntry, GroupMetaExtent, PackedFrameInput, PackedGroupInput, SizeClass,
};
use crate::workspace::publish::binding::VerifiedPackedLower;

#[tokio::test]
async fn packed_workspace_fuse_listxattr_bytes_retain_reader_and_both_output_charges() {
    use crate::workspace::packed::snapshot::SnapshotBudgetPool;
    use asyncfuse::raw::reply::ReplyXAttr;
    use asyncfuse::raw::{Filesystem, Request};
    let f = cold_fixture().await;
    let output_pool = SnapshotBudgetPool::Output as usize;
    let baseline = f.budget.state().used[output_pool];
    let ReplyXAttr::Data(data) = Filesystem::listxattr(&f.vfs, Request::default(), 400, 4096)
        .await
        .unwrap()
    else {
        panic!("nonzero listxattr buffer must return data");
    };
    assert_eq!(data.as_ref(), b"system.posix_acl_access\0user.boundary\0");
    let retained =
        baseline + (2 << 20) + (256 << 10) + SnapshotMountBudget::REPLY_ALLOCATION_ALLOWANCE_BYTES;
    assert_eq!(f.budget.state().used[output_pool], retained);
    let last_consumer = data.clone();
    drop(data);
    let reader = f.reader.clone();
    let mut shutdown = tokio::spawn(async move { reader.shutdown().await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(f.budget.state().used[output_pool], retained);
    drop(last_consumer);
    assert_eq!(f.budget.state().used[output_pool], baseline);
    tokio::time::timeout(std::time::Duration::from_secs(2), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn packed_workspace_listxattr_merges_raw_upper_and_masks_lower_with_owned_output() {
    use crate::workspace::packed::snapshot::SnapshotBudgetPool;
    let f = cold_fixture().await;
    f.meta.remove_xattr(400, "user.boundary").await.unwrap();
    f.meta
        .set_xattr(400, "user.added", b"upper", 0)
        .await
        .unwrap();
    f.meta
        .set_xattr_bytes(400, b"user.raw-\xff", b"raw", 0)
        .await
        .unwrap();
    let output_pool = SnapshotBudgetPool::Output as usize;
    let baseline = f.budget.state().used[output_pool];
    let names = f.meta.list_xattr_bytes_owned(400).await.unwrap();
    assert_eq!(
        names.names,
        vec![
            b"system.posix_acl_access".to_vec(),
            b"user.added".to_vec(),
            b"user.raw-\xff".to_vec()
        ]
    );
    assert_eq!(f.budget.state().used[output_pool], baseline + (2 << 20));
    drop(names);
    assert_eq!(f.budget.state().used[output_pool], baseline);
    // The immutable adapter must retain the same final-consumer reservation.
    let lower = f.meta.packed_lower().unwrap();
    let names = lower.metadata.list_xattr_bytes_owned(400).await.unwrap();
    assert!(names.names.iter().any(|name| name == b"user.boundary"));
    assert_eq!(f.budget.state().used[output_pool], baseline + (2 << 20));
    drop(names);
    assert_eq!(f.budget.state().used[output_pool], baseline);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_workspace_listxattr_cancel_releases_admission_before_backend_read() {
    use crate::workspace::packed::snapshot::SnapshotBudgetPool;
    use futures::FutureExt;
    let f = cold_fixture().await;
    let rows = f.backend.rows.lock().await;
    let baseline = f.budget.state().used;
    let mut pending = Box::pin(f.meta.list_xattr_bytes_owned(400));
    assert!(pending.as_mut().now_or_never().is_none());
    assert_eq!(
        f.budget.state().used[SnapshotBudgetPool::Output as usize],
        baseline[SnapshotBudgetPool::Output as usize] + (2 << 20)
    );
    drop(pending);
    assert_eq!(f.budget.state().used, baseline);
    drop(rows);
    f.reader.shutdown().await.unwrap();
}

fn access_acl() -> Vec<u8> {
    acl_bytes(6, 4, 4)
}
fn default_acl() -> Vec<u8> {
    acl_bytes(7, 5, 5)
}
fn acl_bytes(owner: u16, group: u16, mask: u16) -> Vec<u8> {
    let mut bytes = 2u32.to_le_bytes().to_vec();
    for (tag, permissions, id) in [
        (1u16, owner, u32::MAX),
        (2, 4, 1234),
        (4, group, u32::MAX),
        (16, mask, u32::MAX),
        (32, 0, u32::MAX),
    ] {
        bytes.extend_from_slice(&tag.to_le_bytes());
        bytes.extend_from_slice(&permissions.to_le_bytes());
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    PosixAcl::decode(&bytes).unwrap();
    bytes
}

async fn cold_fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(directory.path().join("objects")));
    let payload: Vec<u8> = (0..4096).map(|index| (index % 251 + 1) as u8).collect();
    let file = GroupMetaEntry {
        name: b"nonzero".to_vec(),
        inode: 400,
        kind: 1,
        mode: 0o100640,
        uid: 1,
        gid: 2,
        rdev: 0,
        nlink: 1,
        atime_ns: 11,
        mtime_ns: 12,
        ctime_ns: 13,
        size: payload.len() as u64,
        flags: 0,
        inline_data: Arc::from([]),
        extents: vec![GroupMetaExtent {
            file_offset: 0,
            logical_len: 4096,
            frame_ordinal: 0,
            raw_offset: 0,
            raw_len: 4096,
        }],
    };
    let symlink = GroupMetaEntry {
        name: b"packed-symlink".to_vec(),
        inode: 401,
        kind: 3,
        mode: 0o120777,
        uid: 1,
        gid: 2,
        rdev: 0,
        nlink: 1,
        atime_ns: 21,
        mtime_ns: 22,
        ctime_ns: 23,
        size: 4096,
        flags: 0,
        inline_data: Arc::from([]),
        extents: Vec::new(),
    };
    let group = PackedGroupInput {
        group_id: 1,
        parent_dir_key: [7; 32],
        metadata: GroupMeta::new(vec![file, symlink])
            .unwrap()
            .encode()
            .unwrap(),
        frame_ordinals: vec![0],
        entry_count: 2,
        file_count: 1,
        layout_profile: AccessProfile::RandomSmallFile,
    };
    let mut producer = SnapshotProducer::new(
        client.clone(),
        directory.path(),
        "merged-permissions".into(),
        SnapshotProducerOptions {
            snapshot_id: [0x41; 32],
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
    for cold in [
        SnapshotColdAttributes {
            inode: 1,
            symlink_target: None,
            xattrs: vec![SnapshotXattr {
                name: b"system.posix_acl_default".to_vec(),
                value: default_acl(),
            }],
            acl: Vec::new(),
        },
        SnapshotColdAttributes {
            inode: 400,
            symlink_target: None,
            xattrs: vec![
                SnapshotXattr {
                    name: b"system.posix_acl_access".to_vec(),
                    value: access_acl(),
                },
                SnapshotXattr {
                    name: b"user.boundary".to_vec(),
                    value: vec![0x5a; 65536],
                },
            ],
            acl: Vec::new(),
        },
        SnapshotColdAttributes {
            inode: 401,
            symlink_target: Some(vec![b'x'; 4096]),
            xattrs: Vec::new(),
            acl: Vec::new(),
        },
    ] {
        producer.add_cold_attributes(&cold).await.unwrap();
    }
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    let reader = SnapshotIndexReader::new(client.clone(), 0);
    let proof = VerifiedPackedLower::from_authenticated_snapshot(&snapshot, &reader)
        .await
        .unwrap();
    fixture_from_packed(false, (directory, client, snapshot, proof, payload)).await
}

#[tokio::test]
async fn packed_copy_up_vfs_link_preserves_original_id_cold_acl_and_lower_data_without_extent_copy()
{
    let f = cold_fixture().await;
    let original = f.vfs.stat("/nonzero").await.unwrap();
    let permission = f.meta.inode_permissions(400).await.unwrap().unwrap();
    assert_eq!(permission.access_acl, Some(access_acl()));
    let alias = f.vfs.link("/nonzero", "/alias").await.unwrap();
    assert_eq!(
        (
            alias.ino,
            alias.nlink,
            alias.uid,
            alias.gid,
            alias.mode,
            alias.size
        ),
        (
            400,
            2,
            original.uid,
            original.gid,
            original.mode,
            original.size
        )
    );
    let extents = f
        .store
        .get_extent_deltas(ExtentQuery {
            layer_ids: vec![f.guard.expected_head_layer_id],
            ino: 400,
            chunk_index: 0,
            range_start: 0,
            range_end: CHUNK,
        })
        .await
        .unwrap();
    assert!(
        extents.is_empty(),
        "metadata-only copy-up copied lower file extents"
    );
    let fh = f.vfs.open(400, alias, true, false, false).await.unwrap();
    assert_eq!(f.vfs.read(fh, 0, f.payload.len()).await.unwrap(), f.payload);
    f.vfs.close(fh).await.unwrap();
    assert_eq!(
        f.vfs
            .get_xattr_bytes_ino(400, b"user.boundary")
            .await
            .unwrap(),
        Some(vec![0x5a; 65536])
    );
    f.vfs
        .remove_xattr_ino(400, "system.posix_acl_access")
        .await
        .unwrap();
    assert!(
        f.meta
            .inode_permissions(400)
            .await
            .unwrap()
            .unwrap()
            .access_acl
            .is_none(),
        "upper whiteout revived lower access ACL"
    );
    let symlink = f
        .vfs
        .link("/packed-symlink", "/symlink-alias")
        .await
        .unwrap();
    assert_eq!((symlink.ino, symlink.nlink, symlink.size), (401, 2, 4096));
    assert_eq!(
        f.vfs.readlink("/symlink-alias").await.unwrap().as_bytes(),
        vec![b'x'; 4096]
    );
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_copy_up_vfs_create_inherits_lower_default_acl_and_chmod_keeps_access_mode_exact() {
    let f = cold_fixture().await;
    let child = f.vfs.create_file("/inherited").await.unwrap();
    assert!(child > 401);
    let inherited = f
        .vfs
        .get_xattr_ino(child, "system.posix_acl_access")
        .await
        .unwrap()
        .unwrap();
    let attr = f.vfs.stat("/inherited").await.unwrap();
    let acl = PosixAcl::decode(&inherited).unwrap();
    assert_eq!(acl.mode_bits(), attr.mode & 0o777);
    assert_eq!(acl.user_access_mode(attr.uid, 1234), Some(4));
    let changed = f.vfs.chmod(child, 0o600).await.unwrap();
    let after = f
        .vfs
        .get_xattr_ino(child, "system.posix_acl_access")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        PosixAcl::decode(&after).unwrap().mode_bits(),
        changed.mode & 0o777
    );
    assert_eq!(changed.mode & 0o777, 0o600);
    assert_eq!(
        f.vfs
            .get_xattr_ino(1, "system.posix_acl_default")
            .await
            .unwrap(),
        Some(default_acl())
    );
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_copy_up_vfs_deleted_inode_never_falls_back_to_immutable_lower() {
    let f = cold_fixture().await;
    f.vfs.unlink("/nonzero").await.unwrap();
    assert!(f.meta.stat(400).await.unwrap().is_none());
    assert!(f.meta.inode_permissions(400).await.unwrap().is_none());
    assert!(f.vfs.stat("/nonzero").await.is_err());
    assert!(f.vfs.link("/nonzero", "/resurrected").await.is_err());
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_copy_up_stale_head_and_changed_pwb_reject_templates_before_any_write() {
    let f = cold_fixture().await;
    f.vfs.link("/nonzero", "/alias").await.unwrap();
    let binding = f
        .store
        .load_packed_binding_record(f.guard.clone())
        .await
        .unwrap()
        .unwrap()
        .binding;
    let query = PermissionSnapshotQuery {
        layer_ids: [f.guard.expected_head_layer_id, binding.base_layer_id],
        inodes: vec![400],
        dentry: None,
    };
    let snapshot = f
        .store
        .read_packed_permission_snapshot(f.guard.clone(), binding.clone(), query.clone())
        .await
        .unwrap();
    let inode = snapshot
        .inodes
        .iter()
        .find(|row| row.layer_id == f.guard.expected_head_layer_id && row.ino == 400)
        .unwrap()
        .clone();
    let stale = VersionedMutation {
        inodes: vec![inode],
        ..VersionedMutation::empty(f.guard.clone(), snapshot.layers, CHUNK)
    };
    f.vfs.chmod(400, 0o600).await.unwrap();
    let before = f.backend.rows.lock().await.clone();
    assert!(matches!(
        f.store
            .apply_packed_versioned_mutation(stale, binding.clone())
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(*f.backend.rows.lock().await, before);
    let fresh = f
        .store
        .read_packed_permission_snapshot(f.guard.clone(), binding.clone(), query)
        .await
        .unwrap();
    let inode = fresh
        .inodes
        .iter()
        .find(|row| row.layer_id == f.guard.expected_head_layer_id && row.ino == 400)
        .unwrap()
        .clone();
    let request = VersionedMutation {
        inodes: vec![inode],
        ..VersionedMutation::empty(f.guard.clone(), fresh.layers, CHUNK)
    };
    // Test-only corruption of current PWB; this is a refusal contract, not a
    // substitute for actual typed publication or recovery authorization.
    f.backend
        .rows
        .lock()
        .await
        .remove(&packed_claim_key(f.guard.workspace_id));
    let unchanged = f.backend.rows.lock().await.clone();
    assert!(
        f.store
            .apply_packed_versioned_mutation(request, binding)
            .await
            .is_err()
    );
    assert_eq!(*f.backend.rows.lock().await, unchanged);
    f.reader.shutdown().await.unwrap();
}
