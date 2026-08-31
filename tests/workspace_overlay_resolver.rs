#![cfg(feature = "workspace-overlay")]

use std::ops::Range;

use brewfs::workspace_overlay::digest::{CanonicalLayerDelta, canonical_delta_bytes, delta_digest};
use brewfs::workspace_overlay::ids::{LayerId, WorkspaceId};
use brewfs::workspace_overlay::model::{
    DataExtentDelta, DentryDelta, ExtentKind, InodeDelta, InodeState, LayerRecord, LayerState,
    ValueOp, XattrDelta,
};
use brewfs::workspace_overlay::resolver::{
    resolve_dentry, resolve_directory, resolve_extents, resolve_inode, resolve_xattr,
    validate_layer_chain,
};
use uuid::Uuid;

fn layer_id(value: u128) -> LayerId {
    LayerId::from_uuid(Uuid::from_u128(value))
}

fn workspace_id(value: u128) -> WorkspaceId {
    WorkspaceId::from_uuid(Uuid::from_u128(value))
}

fn layer(id: u128, parent: Option<u128>, state: LayerState, depth: u32) -> LayerRecord {
    LayerRecord {
        layer_id: layer_id(id),
        parent_layer_id: parent.map(layer_id),
        state,
        schema_version: 1,
        sealed_version: (state == LayerState::Sealed).then_some(id as u64),
        delta_digest: (state == LayerState::Sealed).then_some([id as u8; 32]),
        root_hash: (state == LayerState::Sealed).then_some([(id + 1) as u8; 32]),
        depth,
        owner_workspace_id: (state == LayerState::Writable).then_some(workspace_id(99)),
        next_sequence: 1,
        owned_slice_count: 0,
        owned_bytes: 0,
        created_at_ns: 0,
        sealed_at_ns: (state == LayerState::Sealed).then_some(0),
    }
}

fn chain() -> Vec<LayerRecord> {
    vec![
        layer(2, Some(1), LayerState::Writable, 2),
        layer(1, None, LayerState::Sealed, 1),
    ]
}

#[test]
fn layer_chain_rejects_cycles_depth_mismatch_and_unsealed_parents() {
    validate_layer_chain(layer_id(2), &chain()).unwrap();

    let mut cyclic = chain();
    cyclic[1].parent_layer_id = Some(layer_id(2));
    assert!(validate_layer_chain(layer_id(2), &cyclic).is_err());

    let mut wrong_depth = chain();
    wrong_depth[0].depth = 3;
    assert!(validate_layer_chain(layer_id(2), &wrong_depth).is_err());

    let mut writable_parent = chain();
    writable_parent[1].state = LayerState::Writable;
    assert!(validate_layer_chain(layer_id(2), &writable_parent).is_err());
}

#[test]
fn dentry_whiteout_hides_lower_entry_and_recreate_wins() {
    let layers = chain();
    let lower = DentryDelta::put(layer_id(1), 1, b"tool".to_vec(), 10, 1, 1);
    let whiteout = DentryDelta::whiteout(layer_id(2), 1, b"tool".to_vec(), 2);

    assert_eq!(
        resolve_dentry(&layers, &[lower.clone(), whiteout.clone()], 1, b"tool").unwrap(),
        None
    );

    let recreated = DentryDelta::put(layer_id(2), 1, b"tool".to_vec(), 11, 1, 3);
    let resolved = resolve_dentry(&layers, &[lower, whiteout, recreated], 1, b"tool")
        .unwrap()
        .unwrap();
    assert_eq!(resolved.ino, 11);
    assert_eq!(resolved.layer_id, layer_id(2));
}

#[test]
fn readdir_is_newest_wins_filters_whiteouts_and_sorts_raw_names() {
    let layers = chain();
    let deltas = vec![
        DentryDelta::put(layer_id(1), 1, b"z".to_vec(), 20, 1, 1),
        DentryDelta::put(layer_id(1), 1, b"a".to_vec(), 21, 1, 2),
        DentryDelta::whiteout(layer_id(2), 1, b"z".to_vec(), 3),
        DentryDelta::put(layer_id(2), 1, b"m".to_vec(), 22, 1, 4),
    ];

    let entries = resolve_directory(&layers, &deltas, 1).unwrap();
    let names: Vec<&[u8]> = entries.iter().map(|entry| entry.name.as_slice()).collect();
    assert_eq!(names, vec![b"a".as_slice(), b"m".as_slice()]);
}

#[test]
fn extent_resolution_handles_data_hole_data_and_clipped_slice_offsets() {
    let layers = chain();
    let extents = vec![
        DataExtentDelta::data(layer_id(1), 7, 0, 0, 100, 1000, 10, 1),
        DataExtentDelta::hole(layer_id(2), 7, 0, 20, 60, 1),
        DataExtentDelta::data(layer_id(2), 7, 0, 40, 20, 2000, 5, 2),
    ];

    let plan = resolve_extents(&layers, &extents, 7, 0, Range { start: 0, end: 100 }).unwrap();
    assert_eq!(plan.len(), 5);
    assert_eq!(plan[0].logical_offset, 0);
    assert_eq!(plan[0].length, 20);
    assert_eq!(
        plan[0].kind,
        ExtentKind::Data {
            slice_id: 1000,
            slice_offset: 10
        }
    );
    assert_eq!(plan[1].kind, ExtentKind::Hole);
    assert_eq!((plan[1].logical_offset, plan[1].length), (20, 20));
    assert_eq!(
        plan[2].kind,
        ExtentKind::Data {
            slice_id: 2000,
            slice_offset: 5
        }
    );
    assert_eq!((plan[2].logical_offset, plan[2].length), (40, 20));
    assert_eq!(plan[3].kind, ExtentKind::Hole);
    assert_eq!((plan[3].logical_offset, plan[3].length), (60, 20));
    assert_eq!(
        plan[4].kind,
        ExtentKind::Data {
            slice_id: 1000,
            slice_offset: 90
        }
    );
    assert_eq!((plan[4].logical_offset, plan[4].length), (80, 20));
}

#[test]
fn uncovered_ranges_are_zero_and_invalid_extents_fail_closed() {
    let layers = chain();
    let plan = resolve_extents(&layers, &[], 7, 0, 4..12).unwrap();
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].kind, ExtentKind::Hole);
    assert_eq!((plan[0].logical_offset, plan[0].length), (4, 8));

    let zero_length = DataExtentDelta::data(layer_id(2), 7, 0, 0, 0, 1, 0, 1);
    assert!(resolve_extents(&layers, &[zero_length], 7, 0, 0..8).is_err());

    let overflow = DataExtentDelta::data(layer_id(2), 7, 0, u64::MAX, 2, 1, 0, 1);
    assert!(resolve_extents(&layers, &[overflow], 7, 0, 0..8).is_err());
}

#[test]
fn empty_delta_canonical_bytes_are_a_stable_golden_fixture() {
    let empty = CanonicalLayerDelta::default();
    let bytes = canonical_delta_bytes(&empty).unwrap();
    let mut expected = b"BWSDELTA".to_vec();
    expected.extend_from_slice(&1_u32.to_be_bytes());
    for table in 1_u8..=5 {
        expected.push(table);
        expected.extend_from_slice(&0_u64.to_be_bytes());
    }
    assert_eq!(bytes, expected);
    assert_eq!(
        hex::encode(delta_digest(&empty).unwrap()),
        "a5b54177cc3b11d6d7dde476bf74b5c0f4bdff165e3fc08252ba851e684f3c1b"
    );
}

#[test]
fn canonical_encoding_is_independent_of_input_row_order() {
    let a = DentryDelta::put(layer_id(1), 1, b"a".to_vec(), 2, 1, 1);
    let b = DentryDelta::whiteout(layer_id(1), 1, b"b".to_vec(), 2);
    let left = CanonicalLayerDelta {
        dentries: vec![b.clone(), a.clone()],
        ..CanonicalLayerDelta::default()
    };
    let right = CanonicalLayerDelta {
        dentries: vec![a, b],
        ..CanonicalLayerDelta::default()
    };

    assert_eq!(
        canonical_delta_bytes(&left).unwrap(),
        canonical_delta_bytes(&right).unwrap()
    );
}

#[test]
fn inode_delete_and_xattr_whiteout_stop_lower_lookup() {
    let layers = chain();
    let mut lower_inode = inode(layer_id(1), 42, InodeState::Present, 1);
    lower_inode.size = 123;
    let deleted_inode = inode(layer_id(2), 42, InodeState::Deleted, 2);
    assert_eq!(
        resolve_inode(&layers, &[lower_inode, deleted_inode], 42).unwrap(),
        None
    );

    let lower_xattr = XattrDelta {
        layer_id: layer_id(1),
        ino: 42,
        name: b"user.agent".to_vec(),
        op: ValueOp::Put,
        value: Some(b"shared".to_vec()),
        sequence: 1,
    };
    let whiteout = XattrDelta {
        layer_id: layer_id(2),
        ino: 42,
        name: b"user.agent".to_vec(),
        op: ValueOp::Whiteout,
        value: None,
        sequence: 2,
    };
    assert_eq!(
        resolve_xattr(&layers, &[lower_xattr, whiteout], 42, b"user.agent").unwrap(),
        None
    );
}

#[test]
fn randomized_extent_operations_match_a_byte_array_oracle() {
    const WIDTH: usize = 64;
    for seed in 1_u64..=128 {
        let layers = chain();
        let mut extents = vec![DataExtentDelta::data(
            layer_id(1),
            7,
            0,
            0,
            WIDTH as u64,
            1,
            0,
            1,
        )];
        let mut oracle = [1_u8; WIDTH];
        let mut random = seed;
        for sequence in 1_u64..=32 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let start = (random as usize) % WIDTH;
            random = random.rotate_left(17);
            let length = 1 + (random as usize) % (WIDTH - start);
            if random & 1 == 0 {
                extents.push(DataExtentDelta::hole(
                    layer_id(2),
                    7,
                    0,
                    start as u64,
                    length as u64,
                    sequence,
                ));
                oracle[start..start + length].fill(0);
            } else {
                let byte = (sequence as u8).wrapping_add(1);
                extents.push(DataExtentDelta::data(
                    layer_id(2),
                    7,
                    0,
                    start as u64,
                    length as u64,
                    byte as u64,
                    0,
                    sequence,
                ));
                oracle[start..start + length].fill(byte);
            }
        }

        let plan = resolve_extents(&layers, &extents, 7, 0, 0..WIDTH as u64).unwrap();
        let mut actual = [0_u8; WIDTH];
        for extent in plan {
            let range =
                extent.logical_offset as usize..(extent.logical_offset + extent.length) as usize;
            if let ExtentKind::Data { slice_id, .. } = extent.kind {
                actual[range].fill(slice_id as u8);
            }
        }
        assert_eq!(actual, oracle, "seed {seed}");
    }
}

fn inode(layer_id: LayerId, ino: i64, state: InodeState, sequence: u64) -> InodeDelta {
    InodeDelta {
        layer_id,
        ino,
        state,
        kind: 1,
        size: 0,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        rdev: 0,
        nlink: 1,
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        symlink_target: None,
        parent_hint: Some(1),
        data_version: 1,
        sequence,
    }
}
