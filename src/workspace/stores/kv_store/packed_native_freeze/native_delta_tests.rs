use super::*;

#[test]
fn native_full_prefix_keys_match_canonical_order_for_every_signed_component() {
    let layer = LayerId::from_uuid(uuid::Uuid::from_u128(1));
    let values = [i64::MIN, -2, -1, 0, 1, i64::MAX];
    let mut dentries = Vec::new();
    let mut xattrs = Vec::new();
    let mut acls = Vec::new();
    let mut extents = Vec::new();
    for ino in values {
        for name in [
            vec![0],
            vec![0, 0],
            vec![0, 255],
            vec![1],
            vec![255],
            vec![255, 0],
        ] {
            dentries.push(DentryDelta::whiteout(layer, ino, name.clone(), 1));
            xattrs.push(XattrDelta {
                layer_id: layer,
                ino,
                name,
                op: ValueOp::Whiteout,
                value: None,
                sequence: 1,
            });
        }
        for acl_type in [0, 1, 255] {
            for acl_id in values {
                acls.push(AclDelta {
                    layer_id: layer,
                    ino,
                    acl_type,
                    acl_id,
                    op: ValueOp::Whiteout,
                    value: None,
                    sequence: 1,
                });
            }
        }
        for chunk in [0, 1, u64::MAX] {
            for sequence in [0, 1, u64::MAX] {
                extents.push(DataExtentDelta::hole(layer, ino, chunk, 0, 1, sequence));
            }
        }
    }
    fn compare<R: FrozenDeltaRow + Clone>(mut rows: Vec<R>) {
        rows.reverse();
        let mut expected = rows.clone();
        rows.sort_by_key(FrozenDeltaRow::physical_key);
        expected.sort_by_key(CanonicalNativeRow::canonical_key);
        assert_eq!(
            rows.iter()
                .map(CanonicalNativeRow::canonical_key)
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(CanonicalNativeRow::canonical_key)
                .collect::<Vec<_>>()
        );
        assert!(
            rows.windows(2)
                .all(|pair| pair[0].canonical_key() < pair[1].canonical_key())
        );
    }
    compare(dentries);
    compare(xattrs);
    compare(acls);
    compare(extents);
    assert!(
        values
            .windows(2)
            .all(|pair| inode_identity_key(layer, pair[0]) < inode_identity_key(layer, pair[1]))
    );
}
