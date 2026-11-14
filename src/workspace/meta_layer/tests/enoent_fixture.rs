//! Tiny authenticated packed input built from wire records.
//! Catalog mutations and mounted read paths use production code.
use super::*;
use crate::cadapter::client::ObjectClient;
use crate::cadapter::localfs::LocalFsBackend;
use crate::workspace::packed::snapshot::*;
use crate::workspace::packed::{
    AccessProfile, GroupMeta, GroupMetaEntry, GroupMetaExtent, PackedCodec, PackedFrameInput,
    PackedGroupInput, PackedInodeIndexEntry, SizeClass, SizeClassTable,
};
use crate::workspace::publish::binding::VerifiedPackedLower;

async fn object(
    client: &ObjectClient<LocalFsBackend>,
    key: &str,
    kind: SnapshotObjectKind,
    bytes: &[u8],
) -> SnapshotObjectRef {
    let reference = SnapshotObjectRef::from_bytes(key.into(), kind, bytes).unwrap();
    client.put_object(key, bytes).await.unwrap();
    reference
}

fn leaf(key: Vec<u8>, value: Vec<u8>) -> SnapshotIndexRecord {
    SnapshotIndexRecord {
        first_key: key.clone(),
        last_key: key,
        value: SnapshotIndexValue::Leaf(value),
    }
}

async fn lower() -> (
    tempfile::TempDir,
    ObjectClient<LocalFsBackend>,
    AuthenticatedSnapshot,
    VerifiedPackedLower,
) {
    let directory = tempfile::tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(directory.path().join("objects")));
    let payload: Vec<u8> = (0..4096).map(|i| (i % 251 + 1) as u8).collect();
    let entry = GroupMetaEntry {
        name: b"nonzero".to_vec(),
        inode: 400,
        kind: 1,
        mode: 0o100644,
        uid: 1,
        gid: 2,
        rdev: 0,
        nlink: 1,
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
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
    let group = PackedGroupInput {
        group_id: 1,
        parent_dir_key: [7; 32],
        metadata: GroupMeta::new(vec![entry.clone()])
            .unwrap()
            .encode()
            .unwrap(),
        frame_ordinals: vec![0],
        entry_count: 1,
        file_count: 1,
        layout_profile: AccessProfile::RandomSmallFile,
    };
    let built = build_snapshot_container(
        0,
        1,
        AccessProfile::RandomSmallFile,
        SizeClassTable::default(),
        (PackedCodec::Raw, PackedCodec::Raw),
        &[group],
        &[PackedFrameInput {
            raw: payload,
            size_class: SizeClass::Tiny,
            codec: 0,
            first_file_slot: 0,
            last_file_slot: 0,
        }],
    )
    .unwrap();
    let container = object(
        &client,
        "container",
        SnapshotObjectKind::GroupContainer,
        &built.bytes,
    )
    .await;
    let frame = object(
        &client,
        "frame-directory",
        SnapshotObjectKind::FrameDirectory,
        &built.frame_pages[0].encode().unwrap(),
    )
    .await;
    let group = built.groups[0].clone();
    let location = SnapshotInodeLocation {
        group: group.clone(),
        hot: PackedInodeIndexEntry {
            inode: entry.inode,
            parent_inode: 1,
            parent_dir_key: group.parent_dir_key,
            group_id: group.group_id,
            entry_ordinal: 0,
            name: entry.name.clone(),
            kind: entry.kind,
            mode: entry.mode,
            uid: entry.uid,
            gid: entry.gid,
            rdev: entry.rdev,
            nlink: entry.nlink,
            atime_ns: entry.atime_ns,
            mtime_ns: entry.mtime_ns,
            ctime_ns: entry.ctime_ns,
            size: entry.size,
        },
    };
    let mut group_key = group.parent_dir_key.to_vec();
    group_key.extend(&entry.name);
    let mut reverse = entry.inode.to_be_bytes().to_vec();
    reverse.extend(1u64.to_be_bytes());
    reverse.extend(&entry.name);
    let records = [
        (
            SnapshotRootKind::Groups,
            vec![leaf(group_key, group.encode_value().unwrap())],
        ),
        (
            SnapshotRootKind::Inodes,
            vec![leaf(
                entry.inode.to_be_bytes().to_vec(),
                location.encode_value().unwrap(),
            )],
        ),
        (
            SnapshotRootKind::Containers,
            vec![leaf(
                0u32.to_be_bytes().to_vec(),
                container.encode_value().unwrap(),
            )],
        ),
        (
            SnapshotRootKind::Frames,
            vec![leaf(vec![0; 8], frame.encode_value().unwrap())],
        ),
        (SnapshotRootKind::ColdAttributes, vec![]),
        (
            SnapshotRootKind::ReverseNames,
            vec![leaf(reverse, location.encode_value().unwrap())],
        ),
        (SnapshotRootKind::LargePlacements, vec![]),
    ];
    let mut roots = Vec::new();
    for (kind, records) in records {
        let bytes = SnapshotIndexPage {
            kind: kind.object_kind(),
            height: 0,
            records,
        }
        .encode()
        .unwrap();
        roots.push(
            object(
                &client,
                &format!("index-{}", kind as usize),
                kind.object_kind(),
                &bytes,
            )
            .await,
        );
    }
    assert_eq!(built.frame_pages.len(), 1);
    assert_eq!(built.frame_pages[0].frames.len(), 1);
    let frame = &built.frame_pages[0].frames[0];
    assert_eq!(frame.size_class, SizeClass::Tiny);
    assert_eq!(frame.codec, 0);
    let build = SnapshotBuildProvenance {
        frame_count: 1,
        frame_raw_bytes: u64::from(frame.raw_len),
        frame_stored_bytes: u64::from(frame.stored_len),
        frame_raw_size_counts: [1, 0, 0, 0],
        frame_class_counts: [1, 0, 0, 0],
        frame_codec_counts: [1, 0],
        metadata_codec_counts: [1, 0],
        ..Default::default()
    };
    let manifest = SnapshotManifest {
        snapshot_id: [9; 32],
        root_dir_key: [7; 32],
        root_inode: 1,
        group_dentry_count: 1,
        profile: AccessProfile::RandomSmallFile,
        size_classes: SizeClassTable::default(),
        build,
        roots: roots.try_into().unwrap(),
        source: None,
    };
    let reference = object(
        &client,
        "manifest",
        SnapshotObjectKind::Manifest,
        &manifest.encode().unwrap(),
    )
    .await;
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    let reader = SnapshotIndexReader::new(client.clone(), 0);
    let proof = VerifiedPackedLower::from_authenticated_snapshot(&snapshot, &reader)
        .await
        .unwrap();
    assert_eq!(proof.highest_inode(), 400);
    (directory, client, snapshot, proof)
}

pub(super) async fn fixture_with_cache(bytes: u64) -> Fixture<impl WorkspaceKvBackend> {
    fixture_with_backend(bytes, Arc::new(packed_open_preparation_test_backend())).await
}

pub(super) async fn fixture_with_backend<B: WorkspaceKvBackend>(
    bytes: u64,
    inner: Arc<B>,
) -> Fixture<B> {
    let (objects, client, snapshot, proof) = lower().await;
    let lower = Arc::new(PackedReadonlyMeta::from_snapshot(
        client, snapshot, 4096, bytes,
    ));
    let control = Arc::new(ValidationControl::new());
    let store = Arc::new(
        KvWorkspaceStore::new(
            crate::workspace::stores::local_metadata::LocalMetadataBackend::new(
                ValidationBackend {
                    inner: inner.clone(),
                    control: control.clone(),
                },
                None,
                b"packed-meta-integration",
            )
            .await
            .unwrap(),
        )
        .with_packed_reader_pin_budget(lower.mount_budget()),
    );
    let install = request(store.as_ref(), proof).await;
    store
        .install_packed_lower_binding(install.clone())
        .await
        .unwrap();
    store
        .release_lease(ReleaseLease {
            lease_id: install.guard.lease_id,
            holder_generation: install.guard.holder_generation,
        })
        .await
        .unwrap();
    let authority = store
        .clone()
        .acquire_packed_mount_if_present(PackedMountSessionRequest {
            workspace_id: install.guard.workspace_id,
            holder_generation: 8,
            ttl_ns: 30_000_000_000,
            operator_managed: false,
            budget: lower.mount_budget(),
        })
        .await
        .unwrap()
        .expect("actual packed mount authority");
    let meta = WorkspaceMetaLayer::with_chunk_size(store, authority.view(), 4096)
        .with_packed_lower_from_store(
            lower.clone(),
            Arc::new(UnusedUpper),
            ChunkLayout {
                chunk_size: 4096,
                block_size: 4096,
            },
        )
        .await
        .unwrap();
    meta.initialize().await.unwrap();
    // Assert this is the intended real lower, not an upper-only stand-in.
    assert_eq!(meta.lookup(1, "nonzero").await.unwrap(), Some(400));
    assert_eq!(meta.stat(400).await.unwrap().unwrap().size, 4096);
    Fixture {
        _objects: objects,
        lower,
        meta,
        inner,
        control,
        authority,
    }
}
