//! Actual producer content adversaries. Every mutation recomputes authentic
//! object/index/manifest references before the public relation audit runs.
use super::*;
use crate::workspace::packed::snapshot::{
    SNAPSHOT_HEADER_LEN, SNAPSHOT_MAX_BODY_BYTES, SnapshotFrameDirectoryPage,
    SnapshotInodeLocation, encode_snapshot_object,
};
use sha2::{Digest, Sha256};

async fn content_root(fixture: &Fixture, kind: SnapshotRootKind) -> SnapshotIndexPage {
    let index = page(fixture, &fixture.manifest.roots[kind as usize]).await;
    assert_eq!(index.height, 0);
    index
}

async fn content_replace_root(
    fixture: &mut Fixture,
    kind: SnapshotRootKind,
    index: &SnapshotIndexPage,
    prefix: &str,
) {
    let reference = upload_index_page(fixture, &format!("{prefix}/page"), index).await;
    replace_root(fixture, kind, reference, &format!("{prefix}/manifest")).await;
}

async fn assert_content_rejected(fixture: &Fixture) {
    assert_physical_precondition(fixture).await;
    let error = checked_index_audit(fixture).await.unwrap_err();
    match &error {
        PackedWireError::Invalid(_)
        | PackedWireError::UnsupportedFormat(_)
        | PackedWireError::HashMismatch { .. } => {}
        PackedWireError::Backend(message)
            if message.contains("decode") || message.contains("zstd") => {}
        _ => panic!("content adversary failed for unrelated backend/resource reason: {error}"),
    }
}

async fn content_hardlink_fixture() -> Fixture {
    let objects = tempfile::tempdir().unwrap();
    let producer_scratch = tempfile::tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
    let options = options(PackedCodec::Raw, false);
    let mut producer = SnapshotProducer::new(
        client.clone(),
        producer_scratch.path(),
        "content-hardlinks".into(),
        options.clone(),
    )
    .await
    .unwrap();
    producer.set_root_attributes(root_attributes()).unwrap();
    for (group_id, name) in [(1, b"a".as_slice()), (2, b"b".as_slice())] {
        let (group, frames) = pack_group_files_with_policy(
            group_id,
            options.root_dir_key,
            vec![PackedFileInput {
                name: name.to_vec(),
                inode: 3,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 2,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                flags: 0,
                data: vec![41; 64],
            }],
            options.profile,
            options.size_classes,
            options.build_policy,
        )
        .unwrap();
        producer
            .add_container(group_id, &[group], &frames, &[1])
            .await
            .unwrap();
    }
    producer.set_inode_blocks(3, 1).await.unwrap();
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

async fn mutate_fd(fixture: &mut Fixture, frame_record: usize, mismatch: &str, prefix: &str) {
    let mut frames = content_root(fixture, SnapshotRootKind::Frames).await;
    let SnapshotIndexValue::Leaf(value) = &frames.records[frame_record].value else {
        unreachable!()
    };
    let old = SnapshotObjectRef::decode_value(value).unwrap();
    let bytes = fixture.client.get_object(&old.key).await.unwrap().unwrap();
    let mut fd = SnapshotFrameDirectoryPage::decode(&old, &bytes).unwrap();
    assert_eq!(fd.frames.len(), 1);
    if mismatch == "codec" {
        assert_eq!(fd.frames[0].codec, PackedCodec::Raw as u8);
        fd.frames[0].codec = PackedCodec::Zstd as u8;
    } else {
        let containers = content_root(fixture, SnapshotRootKind::Containers).await;
        let ordinal = &frames.records[frame_record].first_key[..4];
        let container_record = containers
            .records
            .iter()
            .find(|record| record.first_key == ordinal)
            .unwrap();
        let SnapshotIndexValue::Leaf(value) = &container_record.value else {
            unreachable!()
        };
        let container = SnapshotObjectRef::decode_value(value).unwrap();
        let bytes = fixture
            .client
            .get_object(&container.key)
            .await
            .unwrap()
            .unwrap();
        fd.frames[0].object_offset -= 1;
        let first = fd.frames[0].object_offset as usize;
        let last = first + fd.frames[0].stored_len as usize;
        fd.frames[0].frame_digest = Sha256::digest(&bytes[first..last])[..16]
            .try_into()
            .unwrap();
    }
    let bytes = fd.encode().unwrap();
    let reference = SnapshotObjectRef::from_bytes(
        format!("{prefix}/fd"),
        SnapshotObjectKind::FrameDirectory,
        &bytes,
    )
    .unwrap();
    assert_eq!(
        SnapshotFrameDirectoryPage::decode(&reference, &bytes).unwrap(),
        fd
    );
    fixture
        .client
        .put_object_create_only(&reference.key, &bytes)
        .await
        .unwrap();
    frames.records[frame_record].value =
        SnapshotIndexValue::Leaf(reference.encode_value().unwrap());
    content_replace_root(fixture, SnapshotRootKind::Frames, &frames, prefix).await;
}

#[tokio::test]
async fn content_relations_accept_real_raw_zstd_inline_and_hardlink_content() {
    for (codec, inline) in [
        (PackedCodec::Raw, false),
        (PackedCodec::Zstd, false),
        (PackedCodec::Zstd, true),
    ] {
        let fixture = fixture(codec, inline, 4).await;
        assert_physical_precondition(&fixture).await;
        assert_eq!(
            checked_index_audit(&fixture)
                .await
                .unwrap()
                .manifest_reference(),
            &fixture.reference
        );
    }
    let links = content_hardlink_fixture().await;
    assert_physical_precondition(&links).await;
    assert_eq!(
        checked_index_audit(&links)
            .await
            .unwrap()
            .manifest_reference(),
        &links.reference
    );
}

#[tokio::test]
async fn content_relations_read_every_actual_gm_entry_even_with_consistent_hot_indexes() {
    let mut fixture = fixture(PackedCodec::Raw, false, 4).await;
    for kind in [SnapshotRootKind::Inodes, SnapshotRootKind::ReverseNames] {
        let mut index = content_root(&fixture, kind).await;
        for record in &mut index.records {
            let SnapshotIndexValue::Leaf(value) = &record.value else {
                unreachable!()
            };
            let mut location = SnapshotInodeLocation::decode_value(value).unwrap();
            if location.hot.inode == 2 {
                location.hot.uid = 77;
                record.value = SnapshotIndexValue::Leaf(location.encode_value().unwrap());
            }
        }
        content_replace_root(
            &mut fixture,
            kind,
            &index,
            &format!("content/gm-hot/index-{}", kind as u8),
        )
        .await;
    }
    // Canonical, Reverse, Groups, parent, alias and SI05 remain consistent.
    // Only reading the actual GM07 discovers its inode 2 still has uid=0.
    assert_content_rejected(&fixture).await;
}

#[tokio::test]
async fn content_relations_decode_frames_and_require_exact_payload_body_coverage() {
    for mismatch in ["codec", "metadata-overlap"] {
        let mut fixture = content_hardlink_fixture().await;
        mutate_fd(&mut fixture, 0, mismatch, &format!("content/fd-{mismatch}")).await;
        // FD06 and whole GC05 hashes are valid. The overlap adversary even
        // recomputes the declared stored frame's own digest for its new range.
        assert_content_rejected(&fixture).await;
    }
    let mut missing = content_hardlink_fixture().await;
    let mut index = content_root(&missing, SnapshotRootKind::Frames).await;
    assert_eq!(index.records.len(), 2);
    index.records.remove(0);
    content_replace_root(
        &mut missing,
        SnapshotRootKind::Frames,
        &index,
        "content/missing-fd",
    )
    .await;
    assert_content_rejected(&missing).await;
}

#[tokio::test]
async fn content_relations_compare_actual_bytes_across_every_hardlink_alias() {
    let mut fixture = content_hardlink_fixture().await;
    let mut containers = content_root(&fixture, SnapshotRootKind::Containers).await;
    let mut frames = content_root(&fixture, SnapshotRootKind::Frames).await;
    let SnapshotIndexValue::Leaf(value) = &containers.records[1].value else {
        unreachable!()
    };
    let old_container = SnapshotObjectRef::decode_value(value).unwrap();
    let bytes = fixture
        .client
        .get_object(&old_container.key)
        .await
        .unwrap()
        .unwrap();
    let SnapshotIndexValue::Leaf(value) = &frames.records[1].value else {
        unreachable!()
    };
    let old_fd = SnapshotObjectRef::decode_value(value).unwrap();
    let fd_bytes = fixture
        .client
        .get_object(&old_fd.key)
        .await
        .unwrap()
        .unwrap();
    let mut fd = SnapshotFrameDirectoryPage::decode(&old_fd, &fd_bytes).unwrap();
    assert_eq!(fd.frames.len(), 1);
    assert_eq!(fd.frames[0].codec, PackedCodec::Raw as u8);
    let mut body = old_container
        .verify(&bytes, SNAPSHOT_MAX_BODY_BYTES)
        .unwrap()
        .to_vec();
    let offset = fd.frames[0].object_offset as usize - SNAPSHOT_HEADER_LEN;
    body[offset] ^= 1;
    let replacement = encode_snapshot_object(
        SnapshotObjectKind::GroupContainer,
        &body,
        SNAPSHOT_MAX_BODY_BYTES,
    )
    .unwrap();
    let container = SnapshotObjectRef::from_bytes(
        "content/divergent-hardlink/container".into(),
        SnapshotObjectKind::GroupContainer,
        &replacement,
    )
    .unwrap();
    fixture
        .client
        .put_object_create_only(&container.key, &replacement)
        .await
        .unwrap();
    fd.container_digest = container.digest;
    fd.container_len = container.object_len;
    let first = fd.frames[0].object_offset as usize;
    let last = first + fd.frames[0].stored_len as usize;
    fd.frames[0].frame_digest = Sha256::digest(&replacement[first..last])[..16]
        .try_into()
        .unwrap();
    let fd_bytes = fd.encode().unwrap();
    let fd_ref = SnapshotObjectRef::from_bytes(
        "content/divergent-hardlink/fd".into(),
        SnapshotObjectKind::FrameDirectory,
        &fd_bytes,
    )
    .unwrap();
    fixture
        .client
        .put_object_create_only(&fd_ref.key, &fd_bytes)
        .await
        .unwrap();
    containers.records[1].value = SnapshotIndexValue::Leaf(container.encode_value().unwrap());
    frames.records[1].value = SnapshotIndexValue::Leaf(fd_ref.encode_value().unwrap());
    content_replace_root(
        &mut fixture,
        SnapshotRootKind::Containers,
        &containers,
        "content/divergent-hardlink/containers",
    )
    .await;
    content_replace_root(
        &mut fixture,
        SnapshotRootKind::Frames,
        &frames,
        "content/divergent-hardlink/frames",
    )
    .await;
    // GM07 entries and every hot locator still agree exactly. Both aliases
    // decode, but the second immutable payload contains a different byte.
    assert_content_rejected(&fixture).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn content_relations_verify_real_external_run_digest_and_all_hole_eof_commitment() {
    for all_hole in [false, true] {
        let mut fixture = external_fixture(all_hole).await;
        assert_physical_precondition(&fixture).await;
        assert_eq!(
            checked_index_audit(&fixture)
                .await
                .unwrap()
                .manifest_reference(),
            &fixture.reference
        );
        let (mut selectors, ordinal, mut placement) = current_selector(&fixture, 2).await;
        let SnapshotPlacement::External { logical_digest, .. } = &mut placement else {
            unreachable!()
        };
        *logical_digest = [7; 32];
        selectors.records[ordinal].value = SnapshotIndexValue::Leaf(placement.encode().unwrap());
        content_replace_root(
            &mut fixture,
            SnapshotRootKind::LargePlacements,
            &selectors,
            &format!("content/external-digest-{all_hole}"),
        )
        .await;
        assert_content_rejected(&fixture).await;
    }
}
