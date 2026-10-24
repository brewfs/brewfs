//! Regression for byte-limited packed pages beneath the entity page contract.

use super::*;
use crate::workspace_overlay::entity::model::{EntityDentry, EntityKind, LowerBinding};
use crate::workspace_overlay::entity::packed_lower::PackedLower;
use crate::workspace_overlay::entity::resolver::ImmutableLower;
use crate::workspace_overlay::packed_v3::wire005::{
    AuthenticatedV3Snapshot, V3IndexReader, V3ProducerOptions, V3SnapshotProducer,
};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, PackedCodec, PackedFileInput, SizeClassTable, pack_group_files,
};
use crate::workspace_overlay::publish::binding::VerifiedPackedLower;
use crate::{LocalFsBackend, ObjectClient};

#[tokio::test]
async fn large_directory_packed_byte_limited_pages_preserve_all_names() {
    // Reuse the PR #327 review probe: 200 files with 200 KiB of valid inline
    // data exceed the adapter's 32 MiB packed-page budget before 256 names.
    let objects = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
    let parent = [2; 32];
    let profile = AccessProfile::RandomSmallFile;
    let classes = SizeClassTable::default();
    let mut producer = V3SnapshotProducer::new(
        client.clone(),
        scratch.path(),
        "pr327-short-page".into(),
        V3ProducerOptions {
            snapshot_id: [1; 32],
            root_dir_key: parent,
            root_inode: 1,
            profile,
            size_classes: classes,
            build_policy: Default::default(),
            metadata_codec: PackedCodec::Raw,
            data_codec: PackedCodec::Raw,
        },
    )
    .await
    .unwrap();
    let mut expected = Vec::new();
    for index in 0..200u64 {
        let name = format!("file-{index:04}").into_bytes();
        expected.push((
            name.clone(),
            EntityDentry::Present {
                ino: (index + 2) as i64,
                kind: EntityKind::File,
            },
        ));
        let (group, frames) = pack_group_files(
            index + 1,
            parent,
            vec![PackedFileInput {
                name,
                inode: index + 2,
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
                data: vec![7; 200 * 1024],
            }],
            profile,
            classes,
            None,
        )
        .unwrap();
        assert!(frames.is_empty(), "fixture must use valid inline data");
        producer
            .add_container(index + 1, &[group], &frames, &[1])
            .await
            .unwrap();
    }
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedV3Snapshot::open(&client, &reference)
        .await
        .unwrap();
    let reader = V3IndexReader::new(client.clone(), 0);
    let first = snapshot
        .readdir_page(&client, &reader, parent, 0, 256, 32 << 20)
        .await
        .unwrap();
    assert!(!first.is_empty() && first.len() < 200);
    let tail = snapshot
        .readdir_page(&client, &reader, parent, first.len() as u64, 256, 32 << 20)
        .await
        .unwrap();
    assert_eq!(first.len() + tail.len(), 200);
    eprintln!(
        "packed: requested=256 first={} remaining={}",
        first.len(),
        tail.len()
    );
    drop((first, tail));

    let verified = VerifiedPackedLower::from_authenticated_snapshot(&snapshot, &reader)
        .await
        .unwrap();
    let store = Arc::new(
        KvWorkspaceStore::new(PinMemoryBackend::default())
            .with_packed_reader_pin_budget(V3MountBudget::defaults()),
    );
    let initial = request(store.as_ref(), verified).await;
    let binding_record = store
        .install_packed_lower_binding(initial.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: binding_record.head_epoch,
        ..initial.guard
    };
    let layers = store
        .load_layer_chain(guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let source = store
        .acquire_packed_reader_pin(&AcquirePackedReaderPin {
            slot: 0,
            expected_slot_generation: 0,
            pin_id: Uuid::new_v4(),
            owner_id: "entity-short-page-test".into(),
            holder_generation: 1,
            ttl_ns: 10_000_000_000,
            gc_grace_ns: 0,
            guard,
            expected_layers: layers,
            expected_binding: binding_record.clone(),
        })
        .await
        .unwrap();
    let lower = PackedLower::open(
        store,
        client,
        Uuid::new_v4(),
        Uuid::new_v4(),
        &source,
        LowerBinding {
            manifest_key: reference.key.into_bytes(),
            manifest_digest: reference.digest,
            root_ino: 1,
            max_ino: binding_record.highest_inode,
        },
    )
    .await
    .unwrap();

    // Do not warm the directory memo: exercise the uncached packed adapter.
    let entries = lower.readdir_page(1, 0, 256).await.unwrap();
    eprintln!("adapter: requested=256 returned={}", entries.len());
    assert_eq!(entries.len(), 200);
    assert_eq!(entries, expected);
    assert!(lower.readdir_page(1, 200, 256).await.unwrap().is_empty());

    // A full adapter page must also span packed pages without skipping or
    // duplicating names, and a nonzero offset must resume at the next name.
    let mut entries = lower.readdir_page(1, 0, 180).await.unwrap();
    assert_eq!(entries.len(), 180);
    let tail = lower.readdir_page(1, 180, 180).await.unwrap();
    assert_eq!(tail.len(), 20);
    eprintln!(
        "adapter: requested=180 first={} remaining={}",
        entries.len(),
        tail.len()
    );
    entries.extend(tail);
    assert_eq!(entries, expected);
}
