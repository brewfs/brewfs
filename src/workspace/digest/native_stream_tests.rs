use super::*;
use crate::workspace::model::{DentryOp, InodeState, ValueOp};

fn layer() -> LayerId {
    LayerId::from_uuid(uuid::Uuid::from_u128(1))
}

fn inode(ino: i64, state: InodeState) -> InodeDelta {
    InodeDelta {
        layer_id: layer(),
        ino,
        state,
        kind: 1,
        size: 91,
        mode: 0o100644,
        uid: 19,
        gid: 23,
        rdev: 0,
        nlink: 2,
        atime_ns: i64::MIN,
        mtime_ns: -1,
        ctime_ns: i64::MAX,
        symlink_target: Some(vec![0, 255]),
        parent_hint: Some(-17),
        data_version: u64::MAX,
        sequence: 7,
    }
}

fn stream(mut delta: CanonicalLayerDelta) -> Result<([u8; 32], u64), WorkspaceError> {
    let mut hash = NativeDeltaHasher::new(layer(), 1 << 30, 96 << 10)?;
    macro_rules! table {
        ($ty:ty, $field:ident) => {
            delta.$field.sort_by_key(CanonicalNativeRow::canonical_key);
            hash.begin_table::<$ty>(delta.$field.len() as u64)?;
            for row in &delta.$field {
                hash.row(row)?;
            }
        };
    }
    table!(DentryDelta, dentries);
    table!(InodeDelta, inodes);
    table!(XattrDelta, xattrs);
    table!(AclDelta, acls);
    table!(DataExtentDelta, extents);
    hash.finish()
}

#[test]
fn empty_stream_matches_native_canonical_delta_and_root() {
    let delta = CanonicalLayerDelta::default();
    let (actual, bytes) = stream(delta.clone()).unwrap();
    assert_eq!(actual, super::super::delta_digest(&delta).unwrap());
    assert_eq!(bytes, 57);
    assert_eq!(
        super::super::root_hash([23; 32], actual),
        super::super::root_hash([23; 32], super::super::delta_digest(&delta).unwrap())
    );
}

#[test]
fn complete_delta_matches_existing_encoder_with_negative_ids_raw_names_and_tombstones() {
    let mut delta = CanonicalLayerDelta::default();
    for (sequence, ino) in [i64::MAX, -1, 0, i64::MIN, 1].into_iter().enumerate() {
        delta.inodes.push(inode(
            ino,
            if sequence % 2 == 0 {
                InodeState::Deleted
            } else {
                InodeState::Present
            },
        ));
        for name in [vec![255, 0], vec![0, 255], vec![0], vec![255]] {
            delta.dentries.push(DentryDelta::whiteout(
                layer(),
                ino,
                name.clone(),
                sequence as u64,
            ));
            delta.xattrs.push(XattrDelta {
                layer_id: layer(),
                ino,
                name,
                op: ValueOp::Whiteout,
                value: None,
                sequence: sequence as u64,
            });
        }
        for acl_id in [i64::MAX, -1, 0, i64::MIN] {
            delta.acls.push(AclDelta {
                layer_id: layer(),
                ino,
                acl_type: 1,
                acl_id,
                op: ValueOp::Put,
                value: Some(vec![0, 255, 7]),
                sequence: sequence as u64,
            });
        }
        delta.extents.push(DataExtentDelta::data(
            layer(),
            ino,
            u64::MAX,
            13,
            2,
            29,
            5,
            3,
        ));
        delta
            .extents
            .push(DataExtentDelta::hole(layer(), ino, u64::MAX, 0, 13, 1));
    }
    delta.dentries.push(DentryDelta::put(
        layer(),
        0,
        b"unreachable".to_vec(),
        313,
        1,
        123,
    ));
    delta.xattrs.push(XattrDelta {
        layer_id: layer(),
        ino: 313,
        name: b"user.boundary".to_vec(),
        op: ValueOp::Put,
        value: Some(vec![255; 65536]),
        sequence: 999,
    });
    let canonical = canonical_delta_bytes(&delta).unwrap();
    let (actual, bytes) = stream(delta.clone()).unwrap();
    assert_eq!(actual, *blake3::hash(&canonical).as_bytes());
    assert_eq!(bytes, canonical.len() as u64);
    delta.inodes.retain(|row| row.state != InodeState::Deleted);
    assert_ne!(
        actual,
        stream(delta).unwrap().0,
        "Deleted records belong to native delta"
    );
}

#[test]
fn signed_sort_keys_preserve_i64_order_and_acl_id_order() {
    let values = [i64::MIN, -2, -1, 0, 1, i64::MAX];
    let inode_keys: Vec<_> = values
        .iter()
        .map(|value| inode(*value, InodeState::Present).canonical_key())
        .collect();
    assert!(inode_keys.windows(2).all(|pair| pair[0] < pair[1]));
    let acl_keys: Vec<_> = values
        .iter()
        .map(|value| {
            AclDelta {
                layer_id: layer(),
                ino: -9,
                acl_type: 0,
                acl_id: *value,
                op: ValueOp::Whiteout,
                value: None,
                sequence: 0,
            }
            .canonical_key()
        })
        .collect();
    assert!(acl_keys.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn table_count_duplicate_layer_and_invalid_payload_fail_closed() {
    let mut hash = NativeDeltaHasher::new(layer(), 1 << 20, 96 << 10).unwrap();
    hash.begin_table::<DentryDelta>(2).unwrap();
    let row = DentryDelta::whiteout(layer(), 1, b"a".to_vec(), 0);
    hash.row(&row).unwrap();
    assert!(hash.row(&row).is_err());
    assert!(hash.begin_table::<InodeDelta>(0).is_err());
    assert!(hash.finish().is_err());
    let mut bad = row.clone();
    bad.op = DentryOp::Put;
    assert!(bad.canonical_body(96 << 10).is_err());
    bad = row;
    bad.layer_id = LayerId::from_uuid(uuid::Uuid::from_u128(2));
    let mut hash = NativeDeltaHasher::new(layer(), 1 << 20, 96 << 10).unwrap();
    hash.begin_table::<DentryDelta>(1).unwrap();
    assert!(hash.row(&bad).is_err());
}

#[test]
fn quota_cannot_produce_digest_from_partial_or_oversized_stream() {
    let mut hash = NativeDeltaHasher::new(layer(), 57, 96 << 10).unwrap();
    hash.begin_table::<DentryDelta>(1).unwrap();
    assert!(
        hash.row(&DentryDelta::whiteout(layer(), 1, b"a".to_vec(), 0))
            .is_err()
    );
    assert!(hash.finish().is_err());
    let huge = XattrDelta {
        layer_id: layer(),
        ino: 1,
        name: b"user.huge".to_vec(),
        op: ValueOp::Put,
        value: Some(vec![0; 96 << 10]),
        sequence: 1,
    };
    assert!(huge.canonical_body(96 << 10).is_err());
}
