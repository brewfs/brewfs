use super::*;
use crate::cadapter::client::ObjectClient;
use crate::cadapter::localfs::LocalFsBackend;
use crate::workspace::packed::{
    AccessProfile, GroupMeta, GroupMetaExtent, PackedFrameInput, PackedGroupInput, SizeClass,
    SizeClassTable,
};
use tempfile::tempdir;

#[test]
fn packed_read_view_change_preserves_retryable_error_type() {
    let error = super::packed_error_to_anyhow(PackedWireError::ReadViewChanged);
    assert!(crate::chunk::read_plan::is_read_view_changed(&error));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn source_allocation_blocks_survive_packed_getattr_and_lookup() {
    use super::super::snapshot::{
        AuthenticatedSnapshot, CapturedSourceFile, CapturedSourceRoot, SnapshotProducer,
        SnapshotProducerOptions, SnapshotSourceFileLimits,
    };
    use std::os::unix::fs::{FileExt, MetadataExt};
    let temp = tempdir().unwrap();
    let source_dir = temp.path().join("source-dir");
    std::fs::create_dir(&source_dir).unwrap();
    let path = source_dir.join("sparse");
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(8 * 1024 * 1024).unwrap();
    file.write_all_at(b"source data", 4096).unwrap();
    file.sync_all().unwrap();
    let expected_blocks = file.metadata().unwrap().blocks();
    assert!(expected_blocks < file.metadata().unwrap().len().div_ceil(512));
    let captured = CapturedSourceFile::capture(
        &path,
        2,
        AccessProfile::RandomSmallFile,
        SizeClassTable::default(),
        None,
        SnapshotSourceFileLimits::default(),
    )
    .unwrap();
    let source_root = CapturedSourceRoot::capture(&source_dir, 1).unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(temp.path().join("objects")));
    let mut producer = SnapshotProducer::new(
        client.clone(),
        temp.path(),
        "source".into(),
        SnapshotProducerOptions {
            snapshot_id: [1; 32],
            root_dir_key: [2; 32],
            root_inode: 1,
            profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            build_policy: Default::default(),
            metadata_codec: super::super::PackedCodec::Raw,
            data_codec: super::super::PackedCodec::Raw,
        },
    )
    .await
    .unwrap();
    producer
        .set_root_attributes(source_root.attributes().clone())
        .unwrap();
    producer
        .set_inode_blocks(2, captured.source_blocks())
        .await
        .unwrap();
    producer
        .add_container(
            1,
            &[captured
                .group(1, [2; 32], AccessProfile::RandomSmallFile)
                .unwrap()],
            captured.frames(),
            &[1],
        )
        .await
        .unwrap();
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    let meta = PackedReadonlyMeta::from_snapshot(client, snapshot, 64 * 1024 * 1024, 0);
    let actual = meta.stat_fresh(1).await.unwrap().unwrap();
    let root = source_root.attributes();
    assert_eq!(actual.ino, root.inode as i64);
    assert_eq!((actual.size, actual.blocks), (root.size, root.blocks));
    assert_eq!(
        (actual.mode, actual.uid, actual.gid, actual.nlink),
        (root.mode, root.uid, root.gid, root.nlink)
    );
    assert_eq!(
        (actual.atime, actual.mtime, actual.ctime),
        (root.atime_ns, root.mtime_ns, root.ctime_ns)
    );
    assert_eq!(
        meta.stat_fresh(2).await.unwrap().unwrap().blocks,
        expected_blocks
    );
    assert_eq!(
        meta.lookup_with_attr(1, "sparse")
            .await
            .unwrap()
            .unwrap()
            .1
            .blocks,
        expected_blocks
    );
    assert_eq!(
        meta.lookup_with_attr_bytes(1, b"sparse")
            .await
            .unwrap()
            .unwrap()
            .1
            .blocks,
        expected_blocks
    );
    let handle = meta.open(2, OpenFlags::empty()).await.unwrap();
    assert_eq!(handle.blocks, expected_blocks);
}

#[derive(Clone, Default)]
struct InterruptedBackend {
    fail_head: bool,
    fail_open: bool,
}
#[async_trait]
impl ObjectBackend for InterruptedBackend {
    async fn put_object(&self, _key: &str, _data: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("unused")
    }
    async fn get_object(&self, _key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        anyhow::bail!("unexpected whole GET")
    }
    async fn get_object_size(&self, _key: &str) -> anyhow::Result<Option<u64>> {
        anyhow::ensure!(!self.fail_head, "injected HEAD failure");
        Ok(Some(4))
    }
    async fn get_object_stream(
        &self,
        key: &str,
    ) -> anyhow::Result<Option<crate::cadapter::client::ObjectByteStream>> {
        anyhow::ensure!(!self.fail_open, "injected GET opening failure");
        Ok(Some(self.get_object_range_stream(key, 0, 4).await?))
    }
    async fn get_object_range(
        &self,
        _key: &str,
        _offset: u64,
        _buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        anyhow::bail!("unused")
    }
    async fn get_object_range_stream(
        &self,
        _key: &str,
        _offset: u64,
        _length: u64,
    ) -> anyhow::Result<crate::cadapter::client::ObjectByteStream> {
        anyhow::ensure!(!self.fail_open, "injected GET opening failure");
        Ok(Box::pin(futures_util::stream::iter(vec![
            Ok(bytes::Bytes::from_static(b"ab")),
            Err(anyhow::anyhow!("injected stream interruption")),
        ])))
    }
    async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
        anyhow::ensure!(!self.fail_head, "injected HEAD failure");
        Ok(String::new())
    }
    async fn delete_object(&self, _key: &str) -> anyhow::Result<()> {
        anyhow::bail!("unused")
    }
}

#[tokio::test]
async fn snapshot_observer_counts_actual_received_bytes_on_failed_streams() {
    let metrics = Arc::new(SnapshotReadonlyMetrics::default());
    let client = ObjectClient::new(SnapshotReadonlyBackend {
        client: ObjectClient::new(InterruptedBackend::default()),
        metrics: Arc::clone(&metrics),
        budget: super::super::snapshot::SnapshotMountBudget::defaults(),
    });
    assert!(
        super::super::remote::read_exact_range(&client, "object", 0, 4)
            .await
            .is_err()
    );
    assert_eq!(metrics.io.range_gets.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.io.requested_bytes.load(Ordering::Relaxed), 4);
    assert_eq!(metrics.io.received_bytes.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.io.failures.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.logical_bytes.load(Ordering::Relaxed), 0);
    assert!(client.get_object("object").await.is_err());
}

#[tokio::test]
async fn snapshot_cache_counts_backend_bytes_without_charging_local_hits() {
    use crate::cadapter::read_observer::{Engine, Origin, Phase, ReadClass, ReadContext};
    use futures_util::StreamExt;

    let temp = tempdir().unwrap();
    let inner = LocalFsBackend::new(temp.path().join("objects"));
    let body: Vec<_> = (0..128u8).collect();
    for key in ["buffer", "stream", "observed"] {
        inner.put_object(key, &body).await.unwrap();
    }
    let metrics = Arc::new(SnapshotReadonlyMetrics::default());
    let budget = super::super::snapshot::SnapshotMountBudget::defaults();
    let cache = super::pack_cache::PackCache::new(
        384,
        budget.clone(),
        metrics.transport.clone(),
        metrics.io.clone(),
    );
    metrics.pack_cache.set(cache.clone()).unwrap();
    let backend = SnapshotReadonlyBackend {
        client: ObjectClient::new(inner),
        metrics: metrics.clone(),
        budget: budget.clone(),
    };
    let mut output = vec![0; 128];
    assert_eq!(
        backend
            .get_object_range("buffer", 0, &mut output)
            .await
            .unwrap(),
        128
    );
    assert_eq!(output, body);
    assert_eq!(
        metrics.io.received_bytes.load(Ordering::Relaxed),
        128,
        "whole-object fills must reach the native backend byte counter"
    );
    for observed in [false, true] {
        let key = if observed { "observed" } else { "stream" };
        let mut stream = if observed {
            backend
                .get_object_range_stream_observed(
                    key,
                    0,
                    128,
                    ReadContext {
                        engine: Engine::Packed,
                        phase: Phase::Runtime,
                        class: ReadClass::PackedPayload,
                        origin: Origin::Demand,
                    },
                    budget.read_observer(None).unwrap(),
                )
                .await
                .unwrap()
        } else {
            backend.get_object_range_stream(key, 0, 128).await.unwrap()
        };
        let mut received = Vec::new();
        while let Some(chunk) = stream.next().await {
            received.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(received, body);
    }
    assert_eq!(metrics.io.received_bytes.load(Ordering::Relaxed), 384);
    for key in ["buffer", "stream", "observed"] {
        assert_eq!(
            backend.get_object_range(key, 0, &mut output).await.unwrap(),
            128
        );
    }
    assert_eq!(metrics.io.received_bytes.load(Ordering::Relaxed), 384);
    assert_eq!(metrics.io.requested_bytes.load(Ordering::Relaxed), 384);
    assert_eq!(metrics.io.full_gets.load(Ordering::Relaxed), 3);
    assert_eq!(metrics.io.heads.load(Ordering::Relaxed), 3);
    assert_eq!(metrics.io.range_gets.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.io.failures.load(Ordering::Relaxed), 0);
    cache.drain().await.unwrap();
}

#[tokio::test]
async fn snapshot_cache_counts_failed_heads_full_gets_and_fallback_ranges_once() {
    use futures_util::StreamExt;

    // Opening failures have no body bytes; an interrupted body retains its
    // actually consumed prefix. The oversized case uses a real range fallback.
    for (fail_head, fail_open, capacity) in [
        (true, false, 4),
        (false, true, 4),
        (false, false, 4),
        (false, true, 1),
        (false, false, 1),
    ] {
        let metrics = Arc::new(SnapshotReadonlyMetrics::default());
        let budget = super::super::snapshot::SnapshotMountBudget::defaults();
        let cache = super::pack_cache::PackCache::new(
            capacity,
            budget.clone(),
            metrics.transport.clone(),
            metrics.io.clone(),
        );
        metrics.pack_cache.set(cache.clone()).unwrap();
        let backend = SnapshotReadonlyBackend {
            client: ObjectClient::new(InterruptedBackend {
                fail_head,
                fail_open,
            }),
            metrics: metrics.clone(),
            budget,
        };
        let failed = match backend.get_object_range_stream("object", 0, 4).await {
            Err(_) => true,
            Ok(mut stream) => {
                let mut failed = false;
                while let Some(chunk) = stream.next().await {
                    if chunk.is_err() {
                        failed = true;
                        break;
                    }
                }
                failed
            }
        };
        assert!(failed);
        cache.drain().await.unwrap();
        assert_eq!(metrics.io.heads.load(Ordering::Relaxed), 1);
        assert_eq!(
            metrics.io.full_gets.load(Ordering::Relaxed),
            u64::from(!fail_head && capacity == 4)
        );
        assert_eq!(
            metrics.io.range_gets.load(Ordering::Relaxed),
            u64::from(capacity == 1)
        );
        assert_eq!(
            metrics.io.received_bytes.load(Ordering::Relaxed),
            if fail_head || fail_open { 0 } else { 2 }
        );
        assert_eq!(metrics.io.failures.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn snapshot_admission_rejection_does_not_invent_backend_attempts() {
    for reject_budget in [false, true] {
        for cache_enabled in [false, true] {
            let metrics = Arc::new(SnapshotReadonlyMetrics::default());
            let budget = super::super::snapshot::SnapshotMountBudget::defaults();
            if cache_enabled {
                metrics
                    .pack_cache
                    .set(super::pack_cache::PackCache::new(
                        4,
                        budget.clone(),
                        metrics.transport.clone(),
                        metrics.io.clone(),
                    ))
                    .unwrap();
            }
            if reject_budget {
                budget.close();
            } else {
                metrics.transport.stop_admission();
            }
            let backend = SnapshotReadonlyBackend {
                client: ObjectClient::new(InterruptedBackend::default()),
                metrics: metrics.clone(),
                budget,
            };
            assert!(
                backend
                    .get_object_range("object", 0, &mut [0; 4])
                    .await
                    .is_err()
            );
            assert!(
                backend
                    .get_object_range_stream("object", 0, 4)
                    .await
                    .is_err()
            );
            assert!(backend.get_etag("object").await.is_err());
            assert_eq!(metrics.io.heads.load(Ordering::Relaxed), 0);
            assert_eq!(metrics.io.full_gets.load(Ordering::Relaxed), 0);
            assert_eq!(metrics.io.range_gets.load(Ordering::Relaxed), 0);
            assert_eq!(metrics.io.requested_bytes.load(Ordering::Relaxed), 0);
            assert_eq!(metrics.io.received_bytes.load(Ordering::Relaxed), 0);
            assert_eq!(metrics.io.failures.load(Ordering::Relaxed), 0);
        }
    }
}

#[tokio::test]
async fn readonly_rejects_unowned_or_foreign_observer_and_reuses_same_budget_without_double_charge()
{
    let entry = GroupMetaEntry {
        name: b"empty".to_vec(),
        inode: 2,
        kind: 1,
        mode: 0o100644,
        uid: 1,
        gid: 2,
        rdev: 0,
        nlink: 1,
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        size: 0,
        flags: 0,
        inline_data: Arc::from([]),
        extents: vec![],
    };
    let (_temp, meta) = current_readonly_fixture(entry, vec![]).await;
    let context = meta.context.clone();
    let budget = super::super::snapshot::SnapshotMountBudget::defaults();
    assert!(
        PackedReadonlyMeta::from_snapshot_budget(
            context.client.clone(),
            context.snapshot.clone(),
            4096,
            0,
            budget.clone()
        )
        .is_err()
    );
    assert_eq!(budget.state().used, [0; 8]);
    let old = context.budget.state().used;
    let unowned = context.client.clone().with_read_observer(
        Arc::new(crate::cadapter::read_observer::ReadObserver::default()),
        crate::cadapter::read_observer::Engine::Packed,
        crate::cadapter::read_observer::Phase::Runtime,
        crate::cadapter::read_observer::Origin::Demand,
    );
    assert!(
        PackedReadonlyMeta::from_snapshot_budget(
            unowned,
            context.snapshot.clone(),
            4096,
            0,
            context.budget.clone()
        )
        .is_err()
    );
    assert_eq!(context.budget.state().used, old);
    let same = PackedReadonlyMeta::from_snapshot_budget(
        context.client.clone(),
        context.snapshot.clone(),
        4096,
        0,
        context.budget.clone(),
    )
    .unwrap();
    assert_eq!(
        context.budget.state().used[super::super::snapshot::SnapshotBudgetPool::Roots as usize],
        old[super::super::snapshot::SnapshotBudgetPool::Roots as usize] + (128 << 10)
    );
    drop(same);
    assert_eq!(context.budget.state().used, old);
}
async fn current_readonly_fixture(
    entry: GroupMetaEntry,
    frames: Vec<PackedFrameInput>,
) -> (tempfile::TempDir, PackedReadonlyMeta<LocalFsBackend>) {
    use super::super::snapshot::{
        AuthenticatedSnapshot, SNAPSHOT_HEADER_LEN, SnapshotProducer, SnapshotProducerOptions,
    };
    let temp = tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(temp.path().join("objects")));
    let frame_ordinals = (0..frames.len()).map(|ordinal| ordinal as u32).collect();
    let metadata = GroupMeta::new(vec![entry]).unwrap();
    let group = PackedGroupInput {
        group_id: 1,
        parent_dir_key: [7; 32],
        metadata: metadata.encode().unwrap(),
        frame_ordinals,
        entry_count: 1,
        file_count: 1,
        layout_profile: AccessProfile::RandomSmallFile,
    };
    let mut producer = SnapshotProducer::new(
        client.clone(),
        temp.path(),
        "readonly".into(),
        SnapshotProducerOptions {
            snapshot_id: [9; 32],
            root_dir_key: [7; 32],
            root_inode: 1,
            profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            build_policy: Default::default(),
            metadata_codec: super::super::PackedCodec::Raw,
            data_codec: super::super::PackedCodec::Raw,
        },
    )
    .await
    .unwrap();
    producer
        .add_container(1, &[group], &frames, &[1])
        .await
        .unwrap();
    let reference = producer.finish().await.unwrap();
    let published = client.get_object(&reference.key).await.unwrap().unwrap();
    assert_eq!(
        &published[SNAPSHOT_HEADER_LEN..SNAPSHOT_HEADER_LEN + 4],
        b"PM11"
    );
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    (
        temp,
        PackedReadonlyMeta::from_snapshot(client, snapshot, 4096, 0),
    )
}

async fn current_readonly_directory_fixture(
    entries: Vec<GroupMetaEntry>,
) -> (tempfile::TempDir, PackedReadonlyMeta<LocalFsBackend>) {
    readonly_directory_fixture_with_group_size(entries, 512).await
}

async fn readonly_directory_fixture_with_group_size(
    entries: Vec<GroupMetaEntry>,
    group_size: usize,
) -> (tempfile::TempDir, PackedReadonlyMeta<LocalFsBackend>) {
    use super::super::snapshot::{
        AuthenticatedSnapshot, SNAPSHOT_HEADER_LEN, SnapshotProducer, SnapshotProducerOptions,
    };
    let temp = tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(temp.path().join("objects")));
    // Keep every authenticated GroupMeta page below its 256 KiB wire bound
    // while preserving one parent directory across all groups.
    let mut groups = Vec::new();
    for (group_id, chunk) in entries.chunks(group_size).enumerate() {
        let chunk = chunk.to_vec();
        let metadata = GroupMeta::new(chunk.clone()).unwrap();
        groups.push(PackedGroupInput {
            group_id: group_id as u64 + 1,
            parent_dir_key: [7; 32],
            metadata: metadata.encode().unwrap(),
            frame_ordinals: Vec::new(),
            entry_count: chunk.len() as u32,
            file_count: chunk.iter().filter(|entry| entry.kind == 1).count() as u32,
            layout_profile: AccessProfile::RandomSmallFile,
        });
    }
    let parent_inodes = vec![1; groups.len()];
    let mut producer = SnapshotProducer::new(
        client.clone(),
        temp.path(),
        "readonly-large-directory".into(),
        SnapshotProducerOptions {
            snapshot_id: [9; 32],
            root_dir_key: [7; 32],
            root_inode: 1,
            profile: AccessProfile::RandomSmallFile,
            size_classes: SizeClassTable::default(),
            build_policy: Default::default(),
            metadata_codec: super::super::PackedCodec::Raw,
            data_codec: super::super::PackedCodec::Raw,
        },
    )
    .await
    .unwrap();
    producer
        .add_container(1, &groups, &[], &parent_inodes)
        .await
        .unwrap();
    let reference = producer.finish().await.unwrap();
    let published = client.get_object(&reference.key).await.unwrap().unwrap();
    assert_eq!(
        &published[SNAPSHOT_HEADER_LEN..SNAPSHOT_HEADER_LEN + 4],
        b"PM11"
    );
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    (
        temp,
        PackedReadonlyMeta::from_snapshot(client, snapshot, 4096, 0),
    )
}

#[tokio::test]
async fn opendir_uses_inode_identity_without_decoding_parent_contents() {
    use super::super::meta::FULL_GROUP_DECODES;
    let entries = [
        (b"dir".as_slice(), 2, 2, 0o040755),
        (b"file".as_slice(), 3, 1, 0o100644),
    ]
    .into_iter()
    .map(|(name, inode, kind, mode)| GroupMetaEntry {
        name: name.to_vec(),
        inode,
        kind,
        mode,
        uid: 0,
        gid: 0,
        rdev: 0,
        nlink: 1,
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        size: 0,
        flags: 0,
        inline_data: Arc::from([]),
        extents: Vec::new(),
    })
    .collect();
    let (_temp, meta) = current_readonly_directory_fixture(entries).await;
    FULL_GROUP_DECODES
        .scope(std::cell::Cell::new(0), async {
            let directory = meta.opendir(2).await.unwrap();
            assert!(
                directory
                    .get_entries_page_raw_owned(0, 32)
                    .await
                    .unwrap()
                    .entries
                    .is_empty()
            );
            assert!(matches!(
                meta.opendir(3).await,
                Err(MetaError::NotDirectory(3))
            ));
            assert!(matches!(meta.opendir(4).await, Err(MetaError::NotFound(4))));
            assert_eq!(FULL_GROUP_DECODES.with(std::cell::Cell::get), 0);
        })
        .await;
}

#[tokio::test]
async fn paged_directory_reuses_authenticated_group_metadata() {
    use super::super::snapshot::SnapshotBudgetPool;
    use crate::cadapter::read_observer::{Ledger, ReadClass, ReadWork};

    let entries: Vec<_> = (0..1056)
        .map(|index| GroupMetaEntry {
            name: format!("f{index:04}").into_bytes(),
            inode: index as u64 + 2,
            kind: 1,
            mode: 0o100644,
            uid: 1,
            gid: 2,
            rdev: 0,
            nlink: 1,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            size: 0,
            flags: 0,
            inline_data: Arc::from([]),
            extents: Vec::new(),
        })
        .collect();
    let (_temp, meta) = readonly_directory_fixture_with_group_size(entries.clone(), 1024).await;
    let observer = meta.context.client.read_observer().unwrap();
    let group_fetches = || {
        observer
            .snapshot()
            .rows
            .iter()
            .filter(|((ledger, context), _)| {
                *ledger == Ledger::BackendBody && context.class == ReadClass::GroupMetadata
            })
            .map(|(_, counters)| counters.started)
            .sum::<u64>()
    };
    let budget = &meta.context.budget;
    let baseline = budget.state().used;
    let directory = meta.opendir(1).await.unwrap();
    // This is the lower-page shape used by the head/base/lower merge.
    for offset in (0..1024).step_by(32) {
        let page = directory
            .get_entries_page_raw_owned(offset as u64, 32)
            .await
            .unwrap();
        assert_eq!(page.entries.len(), 32);
        for (actual, expected) in page.entries.iter().zip(&entries[offset..offset + 32]) {
            assert_eq!(actual.name, expected.name);
            assert_eq!(actual.ino, expected.inode as i64);
        }
    }
    assert_eq!(
        group_fetches(),
        1,
        "sequential pages refetched the same group"
    );
    let retained = budget.state().used[SnapshotBudgetPool::Metadata as usize];
    assert!(retained > baseline[SnapshotBudgetPool::Metadata as usize]);
    for work in [ReadWork::Authentication, ReadWork::Decode] {
        assert_eq!(
            observer
                .snapshot()
                .work
                .iter()
                .filter(
                    |((context, actual), _)| context.class == ReadClass::GroupMetadata
                        && *actual == work
                )
                .map(|(_, latency)| latency.count)
                .sum::<u64>(),
            1
        );
    }
    let page = directory.get_entries_page_raw(1024, 32).await.unwrap();
    assert_eq!(page[0].name, entries[1024].name);
    assert_eq!(page.len(), 32);
    assert_eq!(group_fetches(), 2);
    assert!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize] < retained,
        "advancing groups retained the previous large group"
    );
    assert!(
        directory
            .get_entries_page_raw(1056, 32)
            .await
            .unwrap()
            .is_empty()
    );
    // A rewind refetches the evicted group, then reuses it within that group.
    directory.get_entries_page_raw(0, 32).await.unwrap();
    directory.get_entries_page_raw(32, 32).await.unwrap();
    assert_eq!(group_fetches(), 3);
    drop(directory);
    assert_eq!(
        budget.state().used,
        baseline,
        "closing the directory leaked its owners"
    );

    let directory = meta.opendir(1).await.unwrap();
    let pages = futures::future::join_all(
        (0..32).map(|page| directory.get_entries_page_raw(page * 32, 32)),
    )
    .await;
    for (page, result) in pages.into_iter().enumerate() {
        let result = result.unwrap();
        assert_eq!(result.len(), 32);
        assert_eq!(result[0].name, entries[page * 32].name);
    }
    assert_eq!(
        group_fetches(),
        4,
        "concurrent pages did not share the group fill"
    );
    drop(directory);
    assert_eq!(budget.state().used, baseline);
}

#[tokio::test]
async fn legacy_readdir_rejects_large_directory_and_paged_api_remains_available() {
    let entries = (0..4097)
        .map(|index| GroupMetaEntry {
            name: format!("entry-{index:04}").into_bytes(),
            inode: index as u64 + 2,
            kind: 1,
            mode: 0o100644,
            uid: 1,
            gid: 2,
            rdev: 0,
            nlink: 1,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            size: 0,
            flags: 0,
            inline_data: Arc::from([]),
            extents: Vec::new(),
        })
        .collect();
    let (_temp, meta) = current_readonly_directory_fixture(entries).await;
    let error = meta.readdir(1).await.unwrap_err();
    assert!(matches!(error, MetaError::Io(error) if error.raw_os_error() == Some(libc::E2BIG)));
    let directory = meta.opendir(1).await.unwrap();
    assert_eq!(
        directory.get_entries_page_raw(4096, 1).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn readonly_meta_and_block_store_read_one_current_packed_file() {
    use crate::chunk::read_plan::WorkspaceReadPlanProvider;

    let entry = GroupMetaEntry {
        name: b"file".to_vec(),
        inode: 2,
        kind: 1,
        mode: 0o100644,
        uid: 1,
        gid: 2,
        rdev: 0,
        nlink: 1,
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        size: 5,
        flags: 0,
        inline_data: Arc::from([]),
        extents: vec![GroupMetaExtent {
            file_offset: 0,
            logical_len: 5,
            frame_ordinal: 0,
            raw_offset: 0,
            raw_len: 5,
        }],
    };
    let (_temp, meta) = current_readonly_fixture(
        entry,
        vec![PackedFrameInput {
            raw: b"hello".to_vec(),
            size_class: SizeClass::Tiny,
            codec: 0,
            first_file_slot: 0,
            last_file_slot: 0,
        }],
    )
    .await;
    assert_eq!(meta.lookup(1, "file").await.unwrap(), Some(2));
    assert_eq!(meta.stat(2).await.unwrap().unwrap().size, 5);
    assert_eq!(
        meta.get_paths(2).await.unwrap(),
        vec![String::from("/file")]
    );
    assert_eq!(
        meta.get_names(2).await.unwrap(),
        vec![(Some(1), String::from("file"))]
    );
    let directory = meta.opendir(1).await.unwrap();
    assert!(directory.is_paged());
    assert_eq!(
        directory.get_entries_page_raw(0, 1).await.unwrap(),
        vec![RawDirEntry {
            name: b"file".to_vec(),
            ino: 2,
            kind: FileType::File
        }]
    );
    assert!(
        directory
            .get_entries_page_raw(1, 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(meta.supports_prepared_unified_read());
    assert!(
        meta.prepare_unified_read(2, 0, 0, 5)
            .await
            .unwrap()
            .is_some()
    );
    let error = meta.open(2, OpenFlags::WRONLY).await.unwrap_err();
    assert!(matches!(error, MetaError::Io(error) if error.raw_os_error() == Some(libc::EROFS)));
    let store = meta.block_store(4096).unwrap();
    let key = (chunk_id_for(2, 0).unwrap(), 0);
    let mut output = [0; 5];
    store.read_range(key, 0, &mut output).await.unwrap();
    assert_eq!(&output, b"hello");
}

#[tokio::test]
async fn current_inline_meta_exposes_a_logical_slice_and_reads_payload() {
    use crate::chunk::read_plan::WorkspaceReadPlanProvider;
    let entry = GroupMetaEntry {
        name: b"inline.bin".to_vec(),
        inode: 8,
        kind: 1,
        mode: 0o100644,
        uid: 0,
        gid: 0,
        rdev: 0,
        nlink: 1,
        atime_ns: 0,
        mtime_ns: 0,
        ctime_ns: 0,
        size: 7,
        flags: super::super::meta::INLINE_DATA_FLAG,
        inline_data: Arc::from(b"payload".as_slice()),
        extents: Vec::new(),
    };
    let (_temp, meta) = current_readonly_fixture(entry, Vec::new()).await;
    assert!(
        meta.prepare_unified_read(8, 0, 0, 0)
            .await
            .unwrap()
            .unwrap()
            .plan
            .segments
            .is_empty()
    );
    let chunk_id = chunk_id_for(8, 0).unwrap();
    assert_eq!(
        meta.get_slices(chunk_id).await.unwrap(),
        vec![SliceDesc {
            slice_id: chunk_id,
            chunk_id,
            offset: 0,
            length: 7,
        }]
    );
    assert!(
        meta.get_slices(chunk_id_for(8, 1).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    let mut payload = [0; 7];
    meta.block_store(4096)
        .unwrap()
        .read_range((chunk_id, 0), 0, &mut payload)
        .await
        .unwrap();
    assert_eq!(&payload, b"payload");
}
