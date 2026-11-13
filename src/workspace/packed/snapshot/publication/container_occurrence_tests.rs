//! SHA-correct real-producer graphs accepted by the selected-content slice.
//! Every control/adversary runs the public audit and verifies owner cleanup.
use super::*;
use crate::workspace::packed::snapshot::{
    SNAPSHOT_HEADER_LEN, SNAPSHOT_MAX_BODY_BYTES, SnapshotFrameDirectoryPage, SnapshotGroupRef,
    encode_snapshot_object,
};
use crate::workspace::packed::{GroupMeta, PackedFrameInput};

async fn occurrence_fixture() -> Fixture {
    let objects = tempfile::tempdir().unwrap();
    let producer_scratch = tempfile::tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
    let options = options(PackedCodec::Raw, false);
    let (mut group, mut frames) = pack_group_files_with_policy(
        1,
        options.root_dir_key,
        vec![PackedFileInput {
            name: b"a".to_vec(),
            inode: 2,
            kind: 1,
            mode: 0o100644,
            uid: 0,
            gid: 0,
            rdev: 0,
            nlink: 1,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            flags: 0,
            data: vec![41; 4096],
        }],
        options.profile,
        options.size_classes,
        options.build_policy,
    )
    .unwrap();
    assert_eq!(frames.len(), 1);
    let metadata = GroupMeta::decode(&group.metadata).unwrap();
    assert_eq!(metadata.entries().len(), 1);
    assert_eq!(metadata.entries()[0].extents.len(), 1);
    assert_eq!(metadata.entries()[0].extents[0].frame_ordinal, 0);
    // The producer receives and owns both real frames, while the entry only
    // selects frame zero. This is not an invalid hash or an omitted frame ref.
    group.frame_ordinals.push(1);
    frames.push(PackedFrameInput {
        raw: vec![93; 4096],
        size_class: frames[0].size_class,
        codec: 0,
        first_file_slot: 0,
        last_file_slot: 0,
    });
    let mut producer = SnapshotProducer::new(
        client.clone(),
        producer_scratch.path(),
        "occurrence-real".into(),
        options,
    )
    .await
    .unwrap();
    producer.set_root_attributes(root_attributes()).unwrap();
    producer
        .add_container(1, &[group], &frames, &[1])
        .await
        .unwrap();
    producer.set_inode_blocks(2, 8).await.unwrap();
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    assert_eq!(snapshot.manifest().build.frame_count, 2);
    assert_eq!(snapshot.manifest().build.frame_raw_bytes, 8192);
    assert_eq!(snapshot.manifest().build.frame_codec_counts, [2, 0]);
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

async fn occurrence_root(fixture: &Fixture, kind: SnapshotRootKind) -> SnapshotIndexPage {
    let index = page(fixture, &fixture.manifest.roots[kind as usize]).await;
    assert_eq!(index.height, 0);
    index
}
fn occurrence_ref(record: &SnapshotIndexRecord) -> SnapshotObjectRef {
    let SnapshotIndexValue::Leaf(value) = &record.value else {
        unreachable!()
    };
    SnapshotObjectRef::decode_value(value).unwrap()
}
async fn occurrence_replace_root(
    fixture: &mut Fixture,
    kind: SnapshotRootKind,
    index: &SnapshotIndexPage,
    prefix: &str,
) {
    let reference = upload_index_page(fixture, &format!("{prefix}/page"), index).await;
    replace_root(fixture, kind, reference, &format!("{prefix}/manifest")).await;
}
async fn occurrence_fd(fixture: &Fixture) -> SnapshotFrameDirectoryPage {
    let index = occurrence_root(fixture, SnapshotRootKind::Frames).await;
    assert_eq!(index.records.len(), 1);
    let reference = occurrence_ref(&index.records[0]);
    let bytes = fixture
        .client
        .get_object(&reference.key)
        .await
        .unwrap()
        .unwrap();
    let page = SnapshotFrameDirectoryPage::decode(&reference, &bytes).unwrap();
    assert_eq!(page.frames.len(), 2);
    page
}
async fn occurrence_replace_fd(
    fixture: &mut Fixture,
    fd: &SnapshotFrameDirectoryPage,
    prefix: &str,
) {
    let bytes = fd.encode().unwrap();
    let reference = SnapshotObjectRef::from_bytes(
        format!("{prefix}/fd"),
        SnapshotObjectKind::FrameDirectory,
        &bytes,
    )
    .unwrap();
    assert_eq!(
        SnapshotFrameDirectoryPage::decode(&reference, &bytes).unwrap(),
        *fd
    );
    fixture
        .client
        .put_object_create_only(&reference.key, &bytes)
        .await
        .unwrap();
    let mut index = occurrence_root(fixture, SnapshotRootKind::Frames).await;
    index.records[0].value = SnapshotIndexValue::Leaf(reference.encode_value().unwrap());
    occurrence_replace_root(fixture, SnapshotRootKind::Frames, &index, prefix).await;
}
async fn occurrence_rejected(fixture: &Fixture, diagnostic: &str) {
    // The whole physical graph must authenticate first. A hash/HEAD/resource
    // failure cannot make one of these semantic adversaries pass its test.
    assert_physical_precondition(fixture).await;
    let error = checked_index_audit(fixture).await.unwrap_err();
    assert!(
        matches!(error, PackedWireError::Invalid(_)),
        "unexpected non-relation failure: {error}"
    );
    assert!(
        error.to_string().contains(diagnostic),
        "wanted {diagnostic}, got {error}"
    );
}

#[tokio::test]
async fn container_occurrences_accept_real_producer_unused_raw_frame_control() {
    let fixture = occurrence_fixture().await;
    assert_physical_precondition(&fixture).await;
    let accepted = checked_index_audit(&fixture).await.unwrap();
    assert_eq!(accepted.manifest_reference(), &fixture.reference);
    let group_index = occurrence_root(&fixture, SnapshotRootKind::Groups).await;
    let SnapshotIndexValue::Leaf(value) = &group_index.records[0].value else {
        unreachable!()
    };
    let group = SnapshotGroupRef::decode_value(value).unwrap();
    assert_eq!(group.frame_count, 2);
    let container = leaf_refs(&fixture, SnapshotRootKind::Containers)
        .await
        .pop()
        .unwrap();
    let metadata = group
        .read_metadata(&fixture.client, &container, 512 << 10)
        .await
        .unwrap();
    assert!(
        metadata
            .entries()
            .iter()
            .flat_map(|entry| &entry.extents)
            .all(|extent| extent.frame_ordinal == 0)
    );
}

#[tokio::test]
async fn container_occurrences_decode_unused_frame_with_authentic_fd_and_payload_hashes() {
    let mut fixture = occurrence_fixture().await;
    let mut fd = occurrence_fd(&fixture).await;
    fd.frames[1].codec = PackedCodec::Zstd as u8;
    occurrence_replace_fd(&mut fixture, &fd, "occurrence/unused-codec").await;
    assert_physical_precondition(&fixture).await;
    let error = checked_index_audit(&fixture).await.unwrap_err();
    assert!(
        matches!(
            error,
            PackedWireError::Invalid(_) | PackedWireError::Backend(_)
        ),
        "unused frame must fail codec, not hash or quota: {error}"
    );
    let message = error.to_string();
    assert!(
        message.contains("zstd") || message.contains("decode") || message.contains("compressed"),
        "unexpected unused-frame error: {error}"
    );
}

#[tokio::test]
async fn container_occurrences_reject_payload_gap_and_trailing_bytes_after_selected_content() {
    for trailing in [false, true] {
        let mut fixture = occurrence_fixture().await;
        let mut fd = occurrence_fd(&fixture).await;
        let mut containers = occurrence_root(&fixture, SnapshotRootKind::Containers).await;
        let old = occurrence_ref(&containers.records[0]);
        let bytes = fixture.client.get_object(&old.key).await.unwrap().unwrap();
        let mut body = old
            .verify(&bytes, SNAPSHOT_MAX_BODY_BYTES)
            .unwrap()
            .to_vec();
        if trailing {
            body.push(17);
        } else {
            body.insert(
                fd.frames[1].object_offset as usize - SNAPSHOT_HEADER_LEN,
                17,
            );
            fd.frames[1].object_offset += 1;
        }
        let replacement = encode_snapshot_object(
            SnapshotObjectKind::GroupContainer,
            &body,
            SNAPSHOT_MAX_BODY_BYTES,
        )
        .unwrap();
        let prefix = format!("occurrence/body-{}", if trailing { "tail" } else { "gap" });
        let container = SnapshotObjectRef::from_bytes(
            format!("{prefix}/container"),
            SnapshotObjectKind::GroupContainer,
            &replacement,
        )
        .unwrap();
        container
            .verify(&replacement, SNAPSHOT_MAX_BODY_BYTES)
            .unwrap();
        fixture
            .client
            .put_object_create_only(&container.key, &replacement)
            .await
            .unwrap();
        fd.container_digest = container.digest;
        fd.container_len = container.object_len;
        // Every declared stored frame still hashes and decodes to its original
        // raw bytes; only exact contiguous body coverage is false.
        for ordinal in 0..2 {
            let raw = fd
                .read_frame(&fixture.client, &container, ordinal, 1 << 20)
                .await
                .unwrap();
            assert_eq!(raw, vec![if ordinal == 0 { 41 } else { 93 }; 4096]);
        }
        containers.records[0].value = SnapshotIndexValue::Leaf(container.encode_value().unwrap());
        occurrence_replace_root(
            &mut fixture,
            SnapshotRootKind::Containers,
            &containers,
            &format!("{prefix}/ci"),
        )
        .await;
        occurrence_replace_fd(&mut fixture, &fd, &format!("{prefix}/fi")).await;
        occurrence_rejected(
            &fixture,
            if trailing {
                "through footer"
            } else {
                "contiguous body"
            },
        )
        .await;
    }
}

#[tokio::test]
async fn container_occurrences_recompute_bp11_instead_of_accepting_self_consistent_claims() {
    for wrong in ["raw-total", "metadata-count", "class-count", "codec-count"] {
        let mut fixture = occurrence_fixture().await;
        let mut manifest = fixture.manifest.clone();
        match wrong {
            "raw-total" => manifest.build.frame_raw_bytes += 1,
            "metadata-count" => {
                assert_eq!(manifest.build.metadata_codec_counts, [1, 0]);
                manifest.build.metadata_codec_counts = [0, 0];
            }
            "class-count" => {
                assert_eq!(manifest.build.frame_class_counts, [2, 0, 0, 0]);
                manifest.build.frame_class_counts = [1, 1, 0, 0];
            }
            _ => {
                manifest.build.requested_data_codec = PackedCodec::Zstd as u8;
                manifest.build.frame_codec_counts = [1, 1];
            }
        }
        // encode/open authenticate the complete PM11 and accept its local BP11
        // distribution rules. A real observation must find the false claim.
        replace_manifest(
            &mut fixture,
            manifest,
            &format!("occurrence/bp11-{wrong}/manifest"),
        )
        .await;
        occurrence_rejected(&fixture, "provenance").await;
    }
}

#[tokio::test]
async fn container_occurrences_require_orphan_fi_and_every_incoming_gc_group_join() {
    for orphan_fi in [true, false] {
        let mut fixture = occurrence_fixture().await;
        let kind = if orphan_fi {
            SnapshotRootKind::Frames
        } else {
            SnapshotRootKind::Containers
        };
        let mut index = occurrence_root(&fixture, kind).await;
        let mut extra = index.records[0].clone();
        extra.first_key[..4].copy_from_slice(&1u32.to_be_bytes());
        extra.last_key[..4].copy_from_slice(&1u32.to_be_bytes());
        index.records.push(extra);
        // The second incoming occurrence uses the identical physical ref. The
        // existing used group still binds ordinal zero, so a physical-key-only
        // shortcut would hide this absent owner/group join.
        occurrence_replace_root(
            &mut fixture,
            kind,
            &index,
            &format!("occurrence/orphan-{}", if orphan_fi { "fi" } else { "gc" }),
        )
        .await;
        occurrence_rejected(&fixture, if orphan_fi { "unmatched" } else { "Groups" }).await;
    }
}

#[tokio::test]
async fn container_occurrences_require_exact_fi_fences_even_for_unused_frames() {
    let mut fixture = occurrence_fixture().await;
    let mut index = occurrence_root(&fixture, SnapshotRootKind::Frames).await;
    assert_eq!(&index.records[0].last_key[4..], &1u32.to_be_bytes());
    // The FD page actually ends at frame one. The selected extent at frame
    // zero still has a covering FI route, even when its declared fence is two.
    index.records[0].last_key[4..].copy_from_slice(&2u32.to_be_bytes());
    occurrence_replace_root(
        &mut fixture,
        SnapshotRootKind::Frames,
        &index,
        "occurrence/fi-fences",
    )
    .await;
    occurrence_rejected(&fixture, "exact fences").await;
}
