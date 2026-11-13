//! SHA-correct, real-producer namespace relations. The public index audit
//! remains a partial proof; these tests never construct Install/Publish authority.
#![cfg(target_os = "linux")]

use super::*;
use crate::workspace::packed::snapshot::{
    CapturedSourceLayout, SnapshotGroupRef, SnapshotInodeLocation,
};
use crate::workspace::packed::{GroupMeta, directory_key};

fn namespace_input(name: &[u8], inode: u64, kind: u8, nlink: u32) -> PackedFileInput {
    PackedFileInput {
        name: name.to_vec(),
        inode,
        kind,
        mode: if kind == 2 { 0o040755 } else { 0o100644 },
        uid: 0,
        gid: 0,
        rdev: 0,
        nlink,
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        flags: 0,
        data: if kind == 1 { vec![41; 128] } else { vec![] },
    }
}

fn locator(record: &SnapshotIndexRecord) -> SnapshotInodeLocation {
    let SnapshotIndexValue::Leaf(value) = &record.value else {
        panic!("producer locator leaf expected")
    };
    SnapshotInodeLocation::decode_value(value).unwrap()
}

fn encode_locator(
    record: &mut SnapshotIndexRecord,
    location: &SnapshotInodeLocation,
    reverse: bool,
) {
    record.value = SnapshotIndexValue::Leaf(location.encode_value().unwrap());
    let key = if reverse {
        location.reverse_key()
    } else {
        location.hot.inode.to_be_bytes().to_vec()
    };
    record.first_key = key.clone();
    record.last_key = key;
}

async fn namespace_page(fixture: &Fixture, kind: SnapshotRootKind) -> SnapshotIndexPage {
    let result = page(fixture, &fixture.manifest.roots[kind as usize]).await;
    assert_eq!(result.height, 0, "tiny real fixture must have a leaf root");
    result
}

async fn install_namespace_page(
    fixture: &mut Fixture,
    kind: SnapshotRootKind,
    mut index: SnapshotIndexPage,
    prefix: &str,
) {
    index
        .records
        .sort_by(|left, right| left.first_key.cmp(&right.first_key));
    let reference = upload_index_page(fixture, &format!("{prefix}/page"), &index).await;
    replace_root(fixture, kind, reference, &format!("{prefix}/manifest")).await;
}

async fn assert_namespace_rejected(fixture: &Fixture) {
    // All reachable physical bytes and local wire values authenticate before
    // the namespace relation is examined. Hash failure is not the adversary.
    assert_physical_precondition(fixture).await;
    assert_index_relation_error(checked_index_audit(fixture).await);
}

async fn mixed_namespace_fixture() -> Fixture {
    let objects = tempfile::tempdir().unwrap();
    let producer_scratch = tempfile::tempdir().unwrap();
    let capture_scratch = tempfile::tempdir().unwrap();
    let source = tempfile::tempdir().unwrap();
    let path = source.path().join("external");
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(&[37; 4096]).unwrap();
    file.set_len(8 << 20).unwrap();
    file.sync_all().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
    let options = options(PackedCodec::Raw, false);
    let mut producer = SnapshotProducer::new(
        client.clone(),
        producer_scratch.path(),
        "namespace-mixed".into(),
        options.clone(),
    )
    .await
    .unwrap();
    // Directory nlink is a source attribute, not an incoming-alias count.
    let mut root = root_attributes();
    root.nlink = 17;
    producer.set_root_attributes(root).unwrap();
    let mut captured = CapturedSourceLayout::capture_with_policy(
        &path,
        capture_scratch.path(),
        5,
        options.profile,
        options.size_classes,
        options.build_policy,
    )
    .await
    .unwrap();
    assert_eq!(captured.data_bytes(), 4096);
    let external_blocks = captured.source_blocks();
    let mut external_entry = captured.entry().clone();
    external_entry.name = b"zz-external".to_vec();
    assert_eq!(
        producer.add_external_source(&mut captured).await.unwrap(),
        1
    );

    let (mut group, frames) = pack_group_files_with_policy(
        1,
        options.root_dir_key,
        vec![
            namespace_input(b"directory", 2, 2, 11),
            namespace_input(b"link-root", 3, 1, 2),
            namespace_input(b"plain", 4, 1, 1),
            // Its captured entry replaces this placeholder before upload.
            PackedFileInput {
                data: vec![],
                ..namespace_input(b"zz-external", 5, 1, 1)
            },
        ],
        options.profile,
        options.size_classes,
        options.build_policy,
    )
    .unwrap();
    let mut metadata = GroupMeta::decode(&group.metadata).unwrap();
    *metadata
        .entries_mut()
        .iter_mut()
        .find(|entry| entry.inode == 5)
        .unwrap() = external_entry;
    group.metadata = metadata.encode().unwrap();
    producer
        .add_container(1, &[group], &frames, &[1])
        .await
        .unwrap();
    let (group, frames) = pack_group_files_with_policy(
        2,
        directory_key(options.snapshot_id, 2),
        vec![namespace_input(b"link-child", 3, 1, 2)],
        options.profile,
        options.size_classes,
        options.build_policy,
    )
    .unwrap();
    producer
        .add_container(2, &[group], &frames, &[2])
        .await
        .unwrap();
    for (inode, blocks) in [(2, 0), (3, 1), (4, 1), (5, external_blocks)] {
        producer.set_inode_blocks(inode, blocks).await.unwrap();
    }
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    drop(captured);
    assert_eq!(
        std::fs::read_dir(capture_scratch.path()).unwrap().count(),
        0
    );
    assert_eq!(
        std::fs::read_dir(producer_scratch.path()).unwrap().count(),
        0
    );
    Fixture {
        objects,
        client,
        reference,
        manifest: snapshot.manifest().clone(),
    }
}

async fn directory_namespace_fixture(cycle: bool) -> Fixture {
    let objects = tempfile::tempdir().unwrap();
    let producer_scratch = tempfile::tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
    let options = options(PackedCodec::Raw, false);
    let mut producer = SnapshotProducer::new(
        client.clone(),
        producer_scratch.path(),
        "namespace-directories".into(),
        options.clone(),
    )
    .await
    .unwrap();
    let mut root = root_attributes();
    root.nlink = 17;
    producer.set_root_attributes(root).unwrap();
    for (group_id, inode, parent, name) in [
        (1, 2, if cycle { 3 } else { 1 }, b"a".as_slice()),
        (2, 3, 2, b"b".as_slice()),
    ] {
        let parent_key = if parent == 1 {
            options.root_dir_key
        } else {
            directory_key(options.snapshot_id, parent)
        };
        let (group, frames) = pack_group_files_with_policy(
            group_id,
            parent_key,
            vec![namespace_input(name, inode, 2, 11)],
            options.profile,
            options.size_classes,
            options.build_policy,
        )
        .unwrap();
        producer
            .add_container(group_id, &[group], &frames, &[parent])
            .await
            .unwrap();
        producer.set_inode_blocks(inode, 0).await.unwrap();
    }
    // The producer validates hot/link/source closure but does not certify
    // directory reachability. Both variants are genuine uploaded graphs.
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_dir(producer_scratch.path()).unwrap().count(),
        0
    );
    Fixture {
        objects,
        client,
        reference,
        manifest: snapshot.manifest().clone(),
    }
}

async fn rewrite_group_parent(
    fixture: &mut Fixture,
    group_id: u64,
    parent: u64,
    parent_key: [u8; 32],
    prefix: &str,
) {
    let mut groups = namespace_page(fixture, SnapshotRootKind::Groups).await;
    for record in &mut groups.records {
        let SnapshotIndexValue::Leaf(value) = &record.value else {
            unreachable!()
        };
        let mut group = SnapshotGroupRef::decode_value(value).unwrap();
        if group.group_id == group_id {
            group.parent_dir_key = parent_key;
            record.first_key = parent_key.to_vec();
            record.first_key.extend_from_slice(&group.first_name);
            record.last_key = parent_key.to_vec();
            record.last_key.extend_from_slice(&group.last_name);
            record.value = SnapshotIndexValue::Leaf(group.encode_value().unwrap());
        }
    }
    install_namespace_page(
        fixture,
        SnapshotRootKind::Groups,
        groups,
        &format!("{prefix}/groups"),
    )
    .await;
    for (kind, reverse) in [
        (SnapshotRootKind::Inodes, false),
        (SnapshotRootKind::ReverseNames, true),
    ] {
        let mut index = namespace_page(fixture, kind).await;
        for record in &mut index.records {
            let mut location = locator(record);
            if location.hot.group_id == group_id {
                location.hot.parent_inode = parent;
                location.hot.parent_dir_key = parent_key;
                location.group.parent_dir_key = parent_key;
                encode_locator(record, &location, reverse);
            }
        }
        install_namespace_page(
            fixture,
            kind,
            index,
            &format!("{prefix}/index-{}", kind as u8),
        )
        .await;
    }
}

#[tokio::test]
async fn namespace_relations_accept_real_hardlinks_source_external_and_root_special_nlink() {
    let fixture = mixed_namespace_fixture().await;
    assert_physical_precondition(&fixture).await;
    assert_eq!(fixture.manifest.source.as_ref().unwrap().root.nlink, 17);
    assert!(fixture.manifest.source.as_ref().unwrap().placement_contract);
    let inodes = namespace_page(&fixture, SnapshotRootKind::Inodes).await;
    let directory = inodes
        .records
        .iter()
        .map(locator)
        .find(|location| location.hot.inode == 2)
        .unwrap();
    assert_eq!((directory.hot.kind, directory.hot.nlink), (2, 11));
    let reverse = namespace_page(&fixture, SnapshotRootKind::ReverseNames).await;
    let links: Vec<_> = reverse
        .records
        .iter()
        .map(locator)
        .filter(|location| location.hot.inode == 3)
        .collect();
    assert_eq!(links.len(), 2);
    assert_ne!(links[0].hot.parent_inode, links[1].hot.parent_inode);
    assert!(links.iter().all(|location| location.hot.nlink == 2));
    let accepted = checked_index_audit(&fixture).await.unwrap();
    assert_eq!(accepted.manifest_reference(), &fixture.reference);
    assert_eq!(accepted.counts().contexts, 9);
}

#[tokio::test]
async fn namespace_relations_reject_canonical_reverse_hot_disagreement() {
    for mismatch in ["uid", "nlink", "size"] {
        let mut fixture = fixture(PackedCodec::Raw, false, 4).await;
        let mut reverse = namespace_page(&fixture, SnapshotRootKind::ReverseNames).await;
        let record = reverse
            .records
            .iter_mut()
            .find(|record| locator(record).hot.inode == 2)
            .unwrap();
        let mut changed = locator(record);
        match mismatch {
            "uid" => changed.hot.uid += 1,
            "nlink" => changed.hot.nlink += 1,
            "size" => changed.hot.size += 1,
            _ => unreachable!(),
        }
        encode_locator(record, &changed, true);
        install_namespace_page(
            &mut fixture,
            SnapshotRootKind::ReverseNames,
            reverse,
            &format!("namespace/hot-{mismatch}"),
        )
        .await;
        assert_namespace_rejected(&fixture).await;
    }
}

#[tokio::test]
async fn namespace_relations_require_an_exact_alias_for_the_canonical_locator() {
    let mut fixture = fixture(PackedCodec::Raw, false, 4).await;
    let mut inodes = namespace_page(&fixture, SnapshotRootKind::Inodes).await;
    let record = inodes
        .records
        .iter_mut()
        .find(|record| locator(record).hot.inode == 2)
        .unwrap();
    let mut changed = locator(record);
    assert_eq!(changed.hot.entry_ordinal, 0);
    changed.hot.entry_ordinal = 3;
    encode_locator(record, &changed, false);
    // Every actual reverse alias and its hot attributes remain unchanged.
    // Canonical inode 2 now selects a different valid group ordinal, so no
    // actual alias is byte-for-byte the claimed canonical locator.
    install_namespace_page(
        &mut fixture,
        SnapshotRootKind::Inodes,
        inodes,
        "namespace/no-exact-canonical-alias",
    )
    .await;
    assert_namespace_rejected(&fixture).await;
}

#[tokio::test]
async fn namespace_relations_compare_the_complete_groups_ref_with_il05() {
    for mismatch in ["digest", "offset", "frame"] {
        let mut fixture = fixture(PackedCodec::Raw, false, 4).await;
        for (kind, reverse) in [
            (SnapshotRootKind::Inodes, false),
            (SnapshotRootKind::ReverseNames, true),
        ] {
            let mut index = namespace_page(&fixture, kind).await;
            let record = index
                .records
                .iter_mut()
                .find(|record| locator(record).hot.inode == 2)
                .unwrap();
            let mut changed = locator(record);
            match mismatch {
                "digest" => changed.group.meta_digest[0] ^= 1,
                "offset" => changed.group.meta_offset += 1,
                "frame" => changed.group.first_frame += 1,
                _ => unreachable!(),
            }
            encode_locator(record, &changed, reverse);
            install_namespace_page(
                &mut fixture,
                kind,
                index,
                &format!("namespace/gr05-{mismatch}/index-{}", kind as u8),
            )
            .await;
        }
        // Canonical and its actual alias now agree byte-for-byte; Groups is
        // still authentic and unchanged. Only complete GR05 relation differs.
        assert_namespace_rejected(&fixture).await;
    }
}

#[tokio::test]
async fn namespace_relations_resolve_actual_directory_parents_and_the_explicit_root_key() {
    for mismatch in ["missing", "nondirectory", "root-key"] {
        let mut fixture = fixture(PackedCodec::Raw, false, 4).await;
        let parent = match mismatch {
            "missing" => 999,
            "nondirectory" => 2,
            _ => 1,
        };
        let key = directory_key(fixture.manifest.snapshot_id, parent);
        assert_ne!(key, fixture.manifest.root_dir_key);
        rewrite_group_parent(
            &mut fixture,
            1,
            parent,
            key,
            &format!("namespace/parent-{mismatch}"),
        )
        .await;
        // Groups/IL05/Reverse agree on every modified parent/ref/key locally.
        // Resolution must use actual canonical directories and PM11 root key.
        assert_namespace_rejected(&fixture).await;
    }
    let mut nested = mixed_namespace_fixture().await;
    let wrong_key = [9; 32];
    assert_ne!(wrong_key, directory_key(nested.manifest.snapshot_id, 2));
    rewrite_group_parent(&mut nested, 2, 2, wrong_key, "namespace/nonroot-parent-key").await;
    assert_namespace_rejected(&nested).await;
}

#[tokio::test]
async fn namespace_relations_match_non_directory_nlink_to_all_visible_aliases() {
    let mut fixture = mixed_namespace_fixture().await;
    for (kind, reverse) in [
        (SnapshotRootKind::Inodes, false),
        (SnapshotRootKind::ReverseNames, true),
    ] {
        let mut index = namespace_page(&fixture, kind).await;
        for record in &mut index.records {
            let mut changed = locator(record);
            if changed.hot.inode == 3 {
                assert_eq!(changed.hot.nlink, 2);
                changed.hot.nlink = 3;
                encode_locator(record, &changed, reverse);
            }
        }
        install_namespace_page(
            &mut fixture,
            kind,
            index,
            &format!("namespace/nlink/index-{}", kind as u8),
        )
        .await;
    }
    // Canonical and both aliases have identical hot attributes. Their nlink
    // still disagrees with the two real visible aliases of regular inode 3.
    assert_namespace_rejected(&fixture).await;
}

#[tokio::test]
async fn namespace_relations_require_exact_nonroot_si05_allocation_closure() {
    for mismatch in ["missing", "extra", "root"] {
        let mut fixture = fixture(PackedCodec::Raw, false, 4).await;
        let source = fixture.manifest.source.as_ref().unwrap();
        let mut index = page(&fixture, &source.allocations).await;
        assert_eq!(index.height, 0);
        if mismatch == "missing" {
            index
                .records
                .retain(|record| record.first_key != 2u64.to_be_bytes());
        } else {
            let inode = if mismatch == "root" {
                fixture.manifest.root_inode
            } else {
                999
            };
            let key = inode.to_be_bytes().to_vec();
            index.records.push(SnapshotIndexRecord {
                first_key: key.clone(),
                last_key: key,
                value: SnapshotIndexValue::Leaf(
                    crate::workspace::packed::snapshot::source_stat::encode_allocation(inode, 8)
                        .unwrap(),
                ),
            });
        }
        index
            .records
            .sort_by(|left, right| left.first_key.cmp(&right.first_key));
        let root =
            upload_index_page(&fixture, &format!("namespace/si05-{mismatch}/page"), &index).await;
        let mut manifest = fixture.manifest.clone();
        manifest.source.as_mut().unwrap().allocations = root;
        replace_manifest(
            &mut fixture,
            manifest,
            &format!("namespace/si05-{mismatch}/manifest"),
        )
        .await;
        assert_namespace_rejected(&fixture).await;
    }
}

#[tokio::test]
async fn namespace_relations_require_exact_regular_selector_closure_and_feature_binding() {
    for mismatch in ["missing", "extra", "nonregular", "feature"] {
        let mut fixture = mixed_namespace_fixture().await;
        if mismatch == "feature" {
            let mut manifest = fixture.manifest.clone();
            manifest.source.as_mut().unwrap().placement_contract = false;
            replace_manifest(
                &mut fixture,
                manifest,
                "namespace/selectors-feature/manifest",
            )
            .await;
        } else {
            let mut index = namespace_page(&fixture, SnapshotRootKind::LargePlacements).await;
            if mismatch == "missing" {
                index
                    .records
                    .retain(|record| record.first_key != 3u64.to_be_bytes());
            } else {
                let inode: u64 = if mismatch == "nonregular" { 2 } else { 999 };
                let key = inode.to_be_bytes().to_vec();
                index.records.push(SnapshotIndexRecord {
                    first_key: key.clone(),
                    last_key: key,
                    value: SnapshotIndexValue::Leaf(
                        SnapshotPlacement::Group { inode, size: 0 }
                            .encode()
                            .unwrap(),
                    ),
                });
            }
            install_namespace_page(
                &mut fixture,
                SnapshotRootKind::LargePlacements,
                index,
                &format!("namespace/selectors-{mismatch}"),
            )
            .await;
        }
        assert_namespace_rejected(&fixture).await;
    }
}

#[tokio::test]
async fn namespace_relations_reject_a_locally_consistent_orphan_directory_cycle() {
    let connected = directory_namespace_fixture(false).await;
    assert_physical_precondition(&connected).await;
    assert_eq!(
        checked_index_audit(&connected)
            .await
            .unwrap()
            .counts()
            .contexts,
        8
    );

    let cycle = directory_namespace_fixture(true).await;
    let groups = namespace_page(&cycle, SnapshotRootKind::Groups).await;
    let inodes = namespace_page(&cycle, SnapshotRootKind::Inodes).await;
    let reverse = namespace_page(&cycle, SnapshotRootKind::ReverseNames).await;
    assert_eq!(
        (
            groups.records.len(),
            inodes.records.len(),
            reverse.records.len()
        ),
        (2, 2, 2)
    );
    for canonical in &inodes.records {
        let location = locator(canonical);
        assert_eq!(location.hot.kind, 2);
        assert_ne!(location.hot.parent_inode, cycle.manifest.root_inode);
        assert_eq!(
            location.hot.parent_dir_key,
            directory_key(cycle.manifest.snapshot_id, location.hot.parent_inode)
        );
        let parent = inodes
            .records
            .iter()
            .map(locator)
            .find(|candidate| candidate.hot.inode == location.hot.parent_inode)
            .unwrap();
        assert_eq!(parent.hot.kind, 2);
        let alias = reverse
            .records
            .iter()
            .find(|record| record.first_key == location.reverse_key())
            .unwrap();
        assert_eq!(alias.value, canonical.value);
        let group = groups
            .records
            .iter()
            .find_map(|record| {
                let SnapshotIndexValue::Leaf(value) = &record.value else {
                    unreachable!()
                };
                let group = SnapshotGroupRef::decode_value(value).unwrap();
                (group.group_id == location.hot.group_id).then_some(group)
            })
            .unwrap();
        assert_eq!(location.group, group);
    }
    // Both parents exist and are directories, every locator has an exact
    // matching alias/group, and SI05 closure is complete. Reachability from
    // the explicit root is the missing invariant; the 2<->3 cycle is orphaned.
    assert_namespace_rejected(&cycle).await;
}
