//! Raw POSIX names on the production Redis/TiKV catalog and real packed reader.

#[path = "packed_paths_tests.rs"]
mod packed_paths_tests;

use super::*;
use crate::meta::posix_acl::PosixAcl;
use crate::workspace::packed::snapshot::{
    SnapshotColdAttributes, SnapshotIndexReader, SnapshotProducer, SnapshotXattr,
};
use crate::workspace::packed::{
    GroupMeta, GroupMetaEntry, GroupMetaExtent, PackedFrameInput, PackedGroupInput, SizeClass,
};
use crate::workspace::publish::binding::VerifiedPackedLower;

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

async fn raw_fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(directory.path().join("objects")));
    let payload: Vec<u8> = (0..4096).map(|index| (index % 251 + 1) as u8).collect();
    let file = GroupMetaEntry {
        name: b"nonzero-\xff".to_vec(),
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
                    name: b"user.raw-\xff".to_vec(),
                    value: vec![0x5a; 65536],
                },
            ],
            acl: Vec::new(),
        },
        SnapshotColdAttributes {
            inode: 401,
            symlink_target: Some(vec![0xff; 4096]),
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

async fn raw_mutation(f: &Fixture) -> VersionedMutation {
    VersionedMutation::empty(
        f.guard.clone(),
        f.store
            .load_layer_chain(f.guard.expected_head_layer_id)
            .await
            .unwrap()
            .try_into()
            .unwrap(),
        CHUNK,
    )
}

#[tokio::test]
async fn raw_bytes_packed_lookup_preserves_invalid_utf8_and_rejects_invalid_components() {
    let f = raw_fixture().await;
    let (ino, attr) = f
        .meta
        .lookup_with_attr_bytes(1, b"nonzero-\xff")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((ino, attr.ino, attr.size), (400, 400, 4096));
    assert_eq!(
        f.vfs
            .child_attr_of_bytes(1, b"nonzero-\xff")
            .await
            .unwrap()
            .unwrap()
            .0,
        400
    );
    assert!(
        f.meta
            .lookup_with_attr_bytes(1, b"nonzero-\xef\xbf\xbd")
            .await
            .unwrap()
            .is_none()
    );
    for name in [b"".as_slice(), b".", b"..", b"a/b", b"a\0b"] {
        assert!(matches!(
            f.meta.lookup_with_attr_bytes(1, name).await,
            Err(crate::meta::store::MetaError::InvalidFilename)
        ));
    }
    assert!(matches!(
        f.meta.lookup_with_attr_bytes(1, &[b'x'; 256]).await,
        Err(crate::meta::store::MetaError::FilenameTooLong)
    ));
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn raw_bytes_upper_alias_and_whiteout_override_authenticated_lower() {
    let f = raw_fixture().await;
    f.meta
        .set_xattr_bytes(400, b"user.copy-up-\xfe", b"value", 0)
        .await
        .unwrap();
    super::add_raw_alias(&f).await;
    assert_eq!(
        f.meta
            .lookup_with_attr_bytes(1, RAW_ALIAS)
            .await
            .unwrap()
            .unwrap()
            .0,
        400
    );
    let mut mutation = raw_mutation(&f).await;
    mutation.dentries.push(DentryDelta::whiteout(
        f.guard.expected_head_layer_id,
        1,
        b"nonzero-\xff".to_vec(),
        0,
    ));
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    assert!(
        f.meta
            .lookup_with_attr_bytes(1, b"nonzero-\xff")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.meta
            .lookup_with_attr_bytes(1, RAW_ALIAS)
            .await
            .unwrap()
            .unwrap()
            .0,
        400
    );
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn raw_bytes_xattr_mutation_preserves_keys_flags_acl_and_lower_mask() {
    let f = raw_fixture().await;
    let name = b"user.raw-\xff";
    assert_eq!(
        f.vfs.get_xattr_bytes_ino(400, name).await.unwrap(),
        Some(vec![0x5a; 65536])
    );
    assert!(
        f.meta
            .get_xattr_bytes(400, b"user.raw-\xef\xbf\xbd")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        f.meta
            .set_xattr_bytes(400, name, b"wrong", libc::XATTR_CREATE as u32)
            .await
            .is_err()
    );
    f.vfs
        .set_xattr_bytes_ino(400, name, b"replacement", libc::XATTR_REPLACE as u32)
        .await
        .unwrap();
    assert_eq!(
        f.meta.get_xattr_bytes(400, name).await.unwrap(),
        Some(b"replacement".to_vec())
    );
    let permissions = f.meta.inode_permissions(400).await.unwrap().unwrap();
    assert_eq!(permissions.access_acl, Some(access_acl()));
    assert_eq!(
        (
            permissions.attr.uid,
            permissions.attr.gid,
            permissions.attr.mode & 0o777
        ),
        (1, 2, 0o640)
    );
    f.vfs.remove_xattr_bytes_ino(400, name).await.unwrap();
    assert!(f.meta.get_xattr_bytes(400, name).await.unwrap().is_none());
    assert!(
        f.meta
            .set_xattr_bytes(400, name, b"wrong", libc::XATTR_REPLACE as u32)
            .await
            .is_err()
    );
    f.meta
        .set_xattr_bytes(400, name, b"created", libc::XATTR_CREATE as u32)
        .await
        .unwrap();
    assert_eq!(
        f.meta.get_xattr_bytes(400, name).await.unwrap(),
        Some(b"created".to_vec())
    );
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn raw_bytes_xattr_invalid_name_and_flag_fail_before_metadata_mutation() {
    let f = raw_fixture().await;
    let before = f.backend.rows.lock().await.clone();
    let overlong = [b'x'; 256];
    for operation in [
        f.meta.get_xattr_bytes(400, &overlong).await.map(|_| ()),
        f.meta.set_xattr_bytes(400, &overlong, b"value", 0).await,
        f.meta.remove_xattr_bytes(400, &overlong).await,
    ] {
        assert!(
            matches!(operation, Err(crate::meta::store::MetaError::Io(error)) if error.raw_os_error() == Some(libc::ERANGE))
        );
    }
    for name in [b"".as_slice(), b"user.a\0b"] {
        assert!(f.meta.get_xattr_bytes(400, name).await.is_err());
        assert!(
            f.meta
                .set_xattr_bytes(400, name, b"value", 0)
                .await
                .is_err()
        );
        assert!(f.meta.remove_xattr_bytes(400, name).await.is_err());
    }
    for flags in [u32::MAX, (libc::XATTR_CREATE | libc::XATTR_REPLACE) as u32] {
        assert!(
            f.meta
                .set_xattr_bytes(400, b"user.new-\xfe", b"value", flags)
                .await
                .is_err()
        );
    }
    assert_eq!(*f.backend.rows.lock().await, before);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn raw_bytes_symlink_target_survives_lower_and_metadata_copy_up() {
    let f = raw_fixture().await;
    assert_eq!(
        f.meta.read_symlink_bytes(401).await.unwrap(),
        vec![0xff; 4096]
    );
    assert_eq!(
        f.vfs.readlink_bytes_ino(401).await.unwrap(),
        vec![0xff; 4096]
    );
    assert!(f.meta.read_symlink(401).await.is_err());
    f.meta
        .set_xattr_bytes(401, b"user.symlink-\xfe", b"copy-up", 0)
        .await
        .unwrap();
    assert_eq!(
        f.meta.read_symlink_bytes(401).await.unwrap(),
        vec![0xff; 4096]
    );
    let mut inode = f
        .store
        .load_layer_delta(f.guard.expected_head_layer_id)
        .await
        .unwrap()
        .inodes
        .into_iter()
        .find(|row| row.ino == 401)
        .unwrap();
    inode.symlink_target = Some(b"upper-\xfe".to_vec());
    inode.size = 7;
    inode.sequence = 0;
    let mut mutation = raw_mutation(&f).await;
    mutation.inodes.push(inode);
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    assert_eq!(f.meta.read_symlink_bytes(401).await.unwrap(), b"upper-\xfe");
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn raw_bytes_deleted_inode_and_stopped_reader_do_not_fall_back_or_write() {
    let f = raw_fixture().await;
    f.meta
        .set_xattr_bytes(401, b"user.copy-up", b"yes", 0)
        .await
        .unwrap();
    let mut inode = f
        .store
        .load_layer_delta(f.guard.expected_head_layer_id)
        .await
        .unwrap()
        .inodes
        .into_iter()
        .find(|row| row.ino == 401)
        .unwrap();
    inode.state = InodeState::Deleted;
    inode.nlink = 0;
    inode.sequence = 0;
    let mut mutation = raw_mutation(&f).await;
    mutation.inodes.push(inode);
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    assert!(f.meta.read_symlink_bytes(401).await.is_err());
    assert!(f.meta.get_xattr_bytes(401, b"user.copy-up").await.is_err());
    f.reader.shutdown().await.unwrap();
    let before = f.backend.rows.lock().await.clone();
    assert!(
        f.meta
            .lookup_with_attr_bytes(1, b"nonzero-\xff")
            .await
            .is_err()
    );
    assert!(f.meta.get_xattr_bytes(400, b"user.raw-\xff").await.is_err());
    assert!(f.meta.read_symlink_bytes(401).await.is_err());
    assert!(
        f.meta
            .set_xattr_bytes(400, b"user.raw-\xff", b"stale", 0)
            .await
            .is_err()
    );
    assert!(
        f.meta
            .remove_xattr_bytes(400, b"user.raw-\xff")
            .await
            .is_err()
    );
    assert_eq!(*f.backend.rows.lock().await, before);
}
