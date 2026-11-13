use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::cadapter::localfs::LocalFsBackend;
use crate::workspace::packed::snapshot::{
    AuthenticatedSnapshot, SnapshotBuildPolicy, SnapshotFramePolicy, SnapshotIndexReader,
    SnapshotProducer, SnapshotProducerOptions,
};
use crate::workspace::packed::{
    AccessProfile, PackedCodec, PackedFileInput, SizeClassTable, pack_group_files_with_policy,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Clone)]
struct CountingBackend {
    inner: LocalFsBackend,
    gets: Arc<AtomicUsize>,
    corrupt_payload: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl ObjectBackend for CountingBackend {
    async fn put_object(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object(key, bytes).await
    }
    async fn put_object_create_only(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object_create_only(key, bytes).await
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get_object(key).await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        bytes: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let read = self.inner.get_object_range(key, offset, bytes).await?;
        // This fixture's external frame is one raw 256 KiB payload. Metadata
        // responses remain intact, so rejection must come from payload hashing.
        if read == 256 << 10 && self.corrupt_payload.load(Ordering::SeqCst) {
            bytes[0] ^= 1;
        }
        Ok(read)
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(key).await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete_object(key).await
    }
}

#[tokio::test]
async fn repeated_authenticated_inode_read_fetches_only_payload() {
    assert_repeated_authenticated_inode_read(0, 1).await;
}

#[tokio::test]
async fn retained_authenticated_inode_frame_needs_no_second_backend_get() {
    assert_repeated_authenticated_inode_read(1 << 20, 0).await;
}

async fn assert_repeated_authenticated_inode_read(retention_bytes: u64, repeat_gets: usize) {
    let objects = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let gets = Arc::new(AtomicUsize::new(0));
    let corrupt_payload = Arc::new(AtomicBool::new(false));
    let client = ObjectClient::new(CountingBackend {
        inner: LocalFsBackend::new(objects.path()),
        gets: gets.clone(),
        corrupt_payload: corrupt_payload.clone(),
    });
    let policy = SnapshotBuildPolicy {
        frames: SnapshotFramePolicy::Static256Kib,
        inline_data: false,
        p90: None,
    };
    let profile = AccessProfile::RandomSmallFile;
    let classes = SizeClassTable::default();
    let (group, frames) = pack_group_files_with_policy(
        7,
        [2; 32],
        vec![PackedFileInput {
            name: b"file".to_vec(),
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
            data: vec![0x5a; 256 << 10],
        }],
        profile,
        classes,
        policy,
    )
    .unwrap();
    let mut producer = SnapshotProducer::new(
        client.clone(),
        scratch.path(),
        "metadata-contract".into(),
        SnapshotProducerOptions {
            snapshot_id: [1; 32],
            root_dir_key: [2; 32],
            root_inode: 1,
            profile,
            size_classes: classes,
            build_policy: policy,
            metadata_codec: PackedCodec::Raw,
            data_codec: PackedCodec::Raw,
        },
    )
    .await
    .unwrap();
    producer
        .add_container(1, &[group], &frames, &[1])
        .await
        .unwrap();
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    let reader = SnapshotIndexReader::new(client.clone(), 8 << 20)
        .with_payload_retention(retention_bytes)
        .unwrap();
    let before = gets.load(Ordering::SeqCst);
    let mut output = [0; 4096];
    snapshot
        .read_inode_range(&client, &reader, 2, 0, &mut output, 32 << 20)
        .await
        .unwrap();
    assert_eq!(output, [0x5a; 4096]);
    assert!(gets.load(Ordering::SeqCst) > before);
    let before = gets.load(Ordering::SeqCst);
    snapshot
        .read_inode_range(&client, &reader, 2, 4096, &mut output, 32 << 20)
        .await
        .unwrap();
    assert_eq!(output, [0x5a; 4096]);
    assert_eq!(
        gets.load(Ordering::SeqCst) - before,
        repeat_gets,
        "repeat fetches only the payload when retention is disabled"
    );
    if retention_bytes > 0 {
        let mut stats = String::new();
        reader.cache_stats().render_payload_retention(&mut stats);
        assert!(stats.contains("brewfs_packed_v3_payload_retention_hits_total 1\n"));
    }
    let before = gets.load(Ordering::SeqCst);
    reader.budget().invalidate_read_caches();
    snapshot
        .read_inode_range(&client, &reader, 2, 0, &mut output, 32 << 20)
        .await
        .unwrap();
    assert_eq!(output, [0x5a; 4096]);
    assert_eq!(
        gets.load(Ordering::SeqCst) - before,
        3,
        "lease fence retires both metadata caches"
    );
    if retention_bytes > 0 {
        // An evicted frame is fetched again under the same immutable identity.
        // A verification memo must not admit a corrupt replacement to retention.
        corrupt_payload.store(true, Ordering::SeqCst);
        reader.budget().invalidate_read_caches();
        assert!(
            snapshot
                .read_inode_range(&client, &reader, 2, 0, &mut output, 32 << 20)
                .await
                .is_err(),
            "retained frames must authenticate every newly fetched response"
        );
        corrupt_payload.store(false, Ordering::SeqCst);
        snapshot
            .read_inode_range(&client, &reader, 2, 0, &mut output, 32 << 20)
            .await
            .unwrap();
        assert_eq!(
            output, [0x5a; 4096],
            "a failed refill must not populate the cache"
        );
    }
}

async fn sequential_smallfile_cache_fixture() -> (
    tempfile::TempDir,
    Arc<AtomicUsize>,
    crate::workspace::packed::PackedReadonlyMeta<CountingBackend>,
) {
    let objects = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let gets = Arc::new(AtomicUsize::new(0));
    let client = ObjectClient::new(CountingBackend {
        inner: LocalFsBackend::new(objects.path()),
        gets: gets.clone(),
        corrupt_payload: Arc::new(AtomicBool::new(false)),
    });
    let options = SnapshotProducerOptions {
        snapshot_id: [1; 32],
        root_dir_key: [2; 32],
        root_inode: 1,
        profile: AccessProfile::SequentialSmallFile,
        size_classes: SizeClassTable::default(),
        build_policy: SnapshotBuildPolicy::default(),
        metadata_codec: PackedCodec::Raw,
        data_codec: PackedCodec::Raw,
    };
    let mut producer = SnapshotProducer::new(
        client.clone(),
        scratch.path(),
        "sequential-cache".into(),
        options.clone(),
    )
    .await
    .unwrap();
    // More than the old 8 MiB GroupMeta cap, within the configured 256 MiB
    // metadata budget. Inline payload ownership must count toward retention.
    for index in 0..40u64 {
        let (group, frames) = pack_group_files_with_policy(
            index + 1,
            [2; 32],
            vec![PackedFileInput {
                name: format!("f{index:06}").into_bytes(),
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
                data: vec![index as u8; 224 << 10],
            }],
            options.profile,
            options.size_classes,
            options.build_policy,
        )
        .unwrap();
        assert!(frames.is_empty());
        producer
            .add_container(index + 1, &[group], &frames, &[1])
            .await
            .unwrap();
    }
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    let meta = crate::workspace::packed::PackedReadonlyMeta::from_snapshot(
        client,
        snapshot,
        64 << 20,
        256 << 20,
    );
    (objects, gets, meta)
}

async fn read_sequential_inline_files(
    meta: &crate::workspace::packed::PackedReadonlyMeta<CountingBackend>,
) {
    use crate::chunk::store::BlockStore;
    let store = meta.block_store(4096).unwrap();
    for index in 0..40 {
        let mut bytes = [0; 4096];
        store
            .read_range(
                (crate::vfs::chunk_id_for(index + 2, 0).unwrap(), 0),
                0,
                &mut bytes,
            )
            .await
            .unwrap();
        assert_eq!(bytes, [index as u8; 4096]);
    }
}

#[tokio::test]
async fn sequential_smallfile_warm_scan_retains_inline_groups() {
    let (_objects, gets, meta) = sequential_smallfile_cache_fixture().await;
    read_sequential_inline_files(&meta).await;
    let before = gets.load(Ordering::SeqCst);
    read_sequential_inline_files(&meta).await;
    assert_eq!(gets.load(Ordering::SeqCst) - before, 0);
}

#[tokio::test]
async fn readdir_8192_inline_files_does_not_decode_file_contents() {
    use crate::chunk::store::BlockStore;
    use crate::meta::layer::MetaLayer;
    use crate::workspace::packed::meta::FULL_GROUP_DECODES;

    let objects = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
    let options = SnapshotProducerOptions {
        snapshot_id: [1; 32],
        root_dir_key: [2; 32],
        root_inode: 1,
        profile: AccessProfile::SequentialSmallFile,
        size_classes: SizeClassTable::default(),
        build_policy: SnapshotBuildPolicy::default(),
        metadata_codec: PackedCodec::Raw,
        data_codec: PackedCodec::Raw,
    };
    let mut producer = SnapshotProducer::new(
        client.clone(),
        scratch.path(),
        "readdir-8192".into(),
        options.clone(),
    )
    .await
    .unwrap();
    let mut groups = 0;
    for start in (0..8192u64).step_by(56) {
        let files = (start..(start + 56).min(8192))
            .map(|index| PackedFileInput {
                name: format!("f{index:06}").into_bytes(),
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
                data: vec![index as u8; 4096],
            })
            .collect();
        groups += 1;
        let (group, frames) = pack_group_files_with_policy(
            groups,
            [2; 32],
            files,
            options.profile,
            options.size_classes,
            options.build_policy,
        )
        .unwrap();
        assert!(frames.is_empty());
        producer
            .add_container(groups, &[group], &frames, &[1])
            .await
            .unwrap();
    }
    assert_eq!(groups, 147);
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    let meta = crate::workspace::packed::PackedReadonlyMeta::from_snapshot(
        client,
        snapshot,
        64 << 20,
        256 << 20,
    );
    FULL_GROUP_DECODES
        .scope(std::cell::Cell::new(0), async {
            let directory = meta.opendir(1).await.unwrap();
            let mut offset = 0;
            // 32-entry merge pages straddle the 56-file inline-group boundaries.
            while offset < 8192 {
                let page = directory
                    .get_entries_page_raw_owned(offset, 32)
                    .await
                    .unwrap();
                assert_eq!(page.entries.len(), 32);
                for (ordinal, entry) in page.entries.iter().enumerate() {
                    let index = offset + ordinal as u64;
                    assert_eq!(entry.name, format!("f{index:06}").as_bytes());
                    assert_eq!(entry.ino, index as i64 + 2);
                }
                offset += page.entries.len() as u64;
            }
            assert!(
                directory
                    .get_entries_page_raw_owned(offset, 32)
                    .await
                    .unwrap()
                    .entries
                    .is_empty()
            );
            // Rewind with wider FUSE pages; these request several groups at
            // once and must preserve ordinal order without decoding payloads.
            for offset in (0..8192u64).step_by(256) {
                let page = directory
                    .get_entries_page_raw_owned(offset, 256)
                    .await
                    .unwrap();
                assert_eq!(page.entries.len(), 256);
                for (ordinal, entry) in page.entries.iter().enumerate() {
                    let index = offset + ordinal as u64;
                    assert_eq!(entry.name, format!("f{index:06}").as_bytes());
                    assert_eq!(entry.ino, index as i64 + 2);
                }
            }
            assert_eq!(
                FULL_GROUP_DECODES.with(std::cell::Cell::get),
                0,
                "pure readdir eagerly decoded inline file contents across {groups} groups"
            );
            // The payload must still be available, decoded only on a real read.
            let store = meta.block_store(4096).unwrap();
            for index in [0, 55, 56, 8191] {
                let mut bytes = [0; 4096];
                store
                    .read_range(
                        (crate::vfs::chunk_id_for(index + 2, 0).unwrap(), 0),
                        0,
                        &mut bytes,
                    )
                    .await
                    .unwrap();
                assert_eq!(bytes, [index as u8; 4096]);
            }
            assert!(FULL_GROUP_DECODES.with(std::cell::Cell::get) > 0);
        })
        .await;
}

#[tokio::test]
async fn directory_page_reduces_parallelism_when_other_users_hold_metadata_budget() {
    use super::SnapshotBudgetPool;
    use crate::meta::layer::MetaLayer;
    let (_objects, _gets, meta) = sequential_smallfile_cache_fixture().await;
    let budget = meta.mount_budget();
    // A serial group read still fits. Eight worst-case decoder reservations
    // would fail despite this page retaining only forty small directory names.
    let _other_user = budget
        .admit(&[(SnapshotBudgetPool::Metadata, 240 << 20)])
        .unwrap();
    let directory = meta.opendir(1).await.unwrap();
    let page = directory.get_entries_page_raw_owned(0, 40).await.unwrap();
    assert_eq!(page.entries.len(), 40);
    for (index, entry) in page.entries.iter().enumerate() {
        assert_eq!(entry.name, format!("f{index:06}").as_bytes());
        assert_eq!(entry.ino, index as i64 + 2);
    }
}

#[tokio::test]
async fn sequential_smallfile_directory_scan_defers_payloads_until_read() {
    use crate::meta::layer::MetaLayer;
    let (_objects, gets, meta) = sequential_smallfile_cache_fixture().await;
    // Prime index pages, then fence retained GroupMeta before enumeration.
    read_sequential_inline_files(&meta).await;
    meta.mount_budget().invalidate_read_caches();
    let directory = meta.opendir(1).await.unwrap();
    let mut offset = 0;
    loop {
        let page = directory
            .get_entries_page_raw_owned(offset, 8)
            .await
            .unwrap();
        if page.entries.is_empty() {
            break;
        }
        offset += page.entries.len() as u64;
    }
    assert_eq!(offset, 40);
    drop(directory);
    let before = gets.load(Ordering::SeqCst);
    read_sequential_inline_files(&meta).await;
    assert_eq!(
        gets.load(Ordering::SeqCst) - before,
        40,
        "each payload group is fetched on demand"
    );
    let before = gets.load(Ordering::SeqCst);
    read_sequential_inline_files(&meta).await;
    assert_eq!(
        gets.load(Ordering::SeqCst),
        before,
        "demand reads still retain complete inline groups"
    );
}
