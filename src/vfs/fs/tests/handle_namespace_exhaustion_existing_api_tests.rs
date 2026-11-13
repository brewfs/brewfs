//! Direct VFS handle-exhaustion tests with workspace enabled.
//! Rejection snapshots cover namespace, attributes, handles and budget state.
use super::*;
use crate::meta::client::{BatchPrefetchConfig, MetaClientMetricsSnapshot};
use crate::meta::store::{FileAttr, FileType};
use crate::vfs::Inode;
use crate::vfs::config::VFSConfig;
use crate::vfs::error::VfsError;
use crate::workspace::packed::snapshot::{
    AuthenticatedSnapshot, SnapshotMountBudget, SnapshotProducerOptions, SnapshotSourceConsistency,
    SnapshotSourceFileLimits, SnapshotSourceHardlinkPolicy, SnapshotSourceNamespaceInventory,
    SnapshotSourceNamespaceOptions,
};
use crate::workspace::packed::{
    AccessProfile, PackedBlockStore, PackedCodec, PackedReadonlyMeta, SizeClassTable,
};
use std::sync::atomic::Ordering;

#[derive(Clone, Copy, Debug)]
enum OpenPath {
    File,
    Cached,
    Fresh,
    Dir,
    Stats,
}

type AttrFingerprint = (
    i64,
    u64,
    u64,
    FileType,
    u32,
    u32,
    u32,
    u32,
    i64,
    i64,
    i64,
    u32,
);

fn attr_fingerprint(attr: &FileAttr) -> AttrFingerprint {
    (
        attr.ino,
        attr.size,
        attr.blocks,
        attr.kind,
        attr.mode,
        attr.rdev,
        attr.uid,
        attr.gid,
        attr.atime,
        attr.mtime,
        attr.ctime,
        attr.nlink,
    )
}

type FileHandleFingerprint = (u64, usize, AttrFingerprint, (bool, bool, bool));

#[derive(Debug, Eq, PartialEq)]
struct RegistrySnapshot {
    inodes: Vec<(i64, usize, u64)>,
    files: Vec<FileHandleFingerprint>,
    inode_handles: Vec<(i64, Vec<u64>)>,
    dirs: Vec<(u64, usize, i64, Option<AttrFingerprint>, usize)>,
    stats: Vec<(u64, usize, usize)>,
}

fn registry_snapshot<B, M>(fs: &VFS<B, M>) -> RegistrySnapshot
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    let mut inodes: Vec<_> = fs
        .state
        .inodes
        .iter()
        .map(|entry| {
            (
                *entry.key(),
                Arc::as_ptr(entry.value()) as usize,
                entry.file_size(),
            )
        })
        .collect();
    let mut files: Vec<_> = fs
        .state
        .handles
        .handles
        .iter()
        .map(|entry| {
            let handle = entry.value();
            (
                *entry.key(),
                Arc::as_ptr(handle) as usize,
                attr_fingerprint(&handle.attr()),
                (handle.flags.read, handle.flags.write, handle.flags.append),
            )
        })
        .collect();
    let mut inode_handles: Vec<_> = fs
        .state
        .handles
        .inode_handles
        .iter()
        .map(|entry| {
            let mut handles = entry.value().clone();
            handles.sort_unstable();
            (*entry.key(), handles)
        })
        .collect();
    let mut dirs: Vec<_> = fs
        .state
        .handles
        .dir_handles
        .iter()
        .map(|entry| {
            let handle = entry.value();
            (
                *entry.key(),
                Arc::as_ptr(handle) as usize,
                handle.ino,
                handle.attr.as_ref().map(attr_fingerprint),
                handle.len(),
            )
        })
        .collect();
    let mut stats: Vec<_> = fs
        .state
        .handles
        .stats_handles
        .iter()
        .map(|entry| {
            (
                *entry.key(),
                entry.value().as_ptr() as usize,
                entry.value().len(),
            )
        })
        .collect();
    inodes.sort_unstable_by_key(|row| row.0);
    files.sort_unstable_by_key(|row| row.0);
    inode_handles.sort_unstable_by_key(|row| row.0);
    dirs.sort_unstable_by_key(|row| row.0);
    stats.sort_unstable_by_key(|row| row.0);
    RegistrySnapshot {
        inodes,
        files,
        inode_handles,
        dirs,
        stats,
    }
}

fn meta_metrics<B, M>(fs: &VFS<B, M>) -> Option<MetaClientMetricsSnapshot>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    fs.meta_layer().metrics().map(|metrics| metrics.snapshot())
}

async fn open_path<B, M>(fs: &VFS<B, M>, path: OpenPath, attr: &FileAttr) -> Result<u64, VfsError>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    match path {
        OpenPath::File => fs.open(attr.ino, attr.clone(), true, false, false).await,
        OpenPath::Cached => {
            fs.open_with_cached_attr(attr.ino, attr.clone(), true, false, false)
                .await
        }
        OpenPath::Fresh => fs.open_fresh_ino(attr.ino, true, false, false).await,
        OpenPath::Dir => fs.opendir(fs.root_ino()).await,
        OpenPath::Stats => fs.open_virtual_stats(),
    }
}

async fn release_path<B, M>(fs: &VFS<B, M>, path: OpenPath, fh: u64)
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    match path {
        OpenPath::Dir => fs.closedir(fh).unwrap(),
        OpenPath::Stats => fs.release_virtual_stats(fh),
        _ => fs.close(fh).await.unwrap(),
    }
}

async fn assert_denied_unchanged<B, M>(
    fs: &VFS<B, M>,
    path: OpenPath,
    attr: &FileAttr,
    budget: Option<&Arc<SnapshotMountBudget>>,
) where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    let registry_before = registry_snapshot(fs);
    let metrics_before = meta_metrics(fs);
    let budget_before = budget.map(|budget| budget.state());
    for attempt in 0..3 {
        let error = open_path(fs, path, attr)
            .await
            .expect_err("exhausted OPEN succeeded");
        assert!(
            error
                .to_string()
                .contains("FUSE handle namespace exhausted"),
            "path={path:?} attempt={attempt}: unexpected error: {error}"
        );
        assert_eq!(fs.state.handles.next_fh.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(
            registry_snapshot(fs),
            registry_before,
            "path={path:?} attempt={attempt}"
        );
        assert_eq!(
            meta_metrics(fs),
            metrics_before,
            "path={path:?} attempt={attempt}"
        );
        if let (Some(budget), Some(before)) = (budget, &budget_before) {
            let after = budget.state();
            assert_eq!(after.used, before.used, "path={path:?} owned bytes changed");
            assert_eq!(
                after.peak, before.peak,
                "path={path:?} admission happened before denial"
            );
            assert_eq!(after.rejections, before.rejections);
            assert_eq!(after.closed, before.closed);
        }
    }
}

async fn exercise_namespace_end<B, M>(
    fs: &VFS<B, M>,
    path: OpenPath,
    successful_attr: FileAttr,
    failed_attr: FileAttr,
    budget: Option<&Arc<SnapshotMountBudget>>,
) where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    // This real low handle must survive every failed OPEN and must never be reused.
    let low_fh = fs
        .open(
            successful_attr.ino,
            successful_attr.clone(),
            true,
            false,
            false,
        )
        .await
        .unwrap();
    assert_eq!(low_fh, 1);
    let low_attr = attr_fingerprint(&fs.handle_attr(low_fh).unwrap());
    fs.state.inodes.remove(&failed_attr.ino);
    fs.state
        .handles
        .next_fh
        .store(u64::MAX - 1, Ordering::Relaxed);
    let last_fh = open_path(fs, path, &successful_attr).await.unwrap();
    assert_eq!(last_fh, u64::MAX - 1, "path={path:?}");
    assert_ne!(last_fh, low_fh);
    assert_eq!(fs.state.handles.next_fh.load(Ordering::Relaxed), u64::MAX);

    // The failed target starts without a registered inode. Old late allocation
    // could return Err while retaining the newly inserted inode/cache state.
    assert_denied_unchanged(fs, path, &failed_attr, budget).await;
    assert!(!fs.state.inodes.contains_key(&failed_attr.ino));
    assert_eq!(attr_fingerprint(&fs.handle_attr(low_fh).unwrap()), low_attr);

    // Also protect an already registered inode from stale cached-attr growth.
    fs.state
        .inodes
        .insert(failed_attr.ino, Inode::new(failed_attr.ino, 7));
    let mut stale_attr = failed_attr.clone();
    stale_attr.size = stale_attr.size.max(4096);
    assert_denied_unchanged(fs, path, &stale_attr, budget).await;
    assert_eq!(
        fs.state.inodes.get(&failed_attr.ino).unwrap().file_size(),
        7
    );

    // Removing the last issued handle cannot reset or recycle this namespace.
    release_path(fs, path, last_fh).await;
    assert_denied_unchanged(fs, path, &failed_attr, budget).await;
    assert_eq!(attr_fingerprint(&fs.handle_attr(low_fh).unwrap()), low_attr);
    fs.close(low_fh).await.unwrap();
    assert_denied_unchanged(fs, path, &failed_attr, budget).await;
}

async fn generic_fixture() -> (VFS<InMemoryBlockStore, impl MetaLayer>, FileAttr, FileAttr) {
    // Same SQLite/InMemoryBlockStore construction as basic_tests, with the
    // existing cached-open test's explicit read-only open-cache configuration.
    let meta = create_meta_store_from_url("sqlite::memory:").await.unwrap();
    let fs = VFS::with_meta_client_config(
        ChunkLayout::default(),
        InMemoryBlockStore::new(),
        meta.store(),
        MetaClientConfig {
            options: MetaClientOptions {
                batch_prefetch: BatchPrefetchConfig {
                    enabled: false,
                    ..Default::default()
                },
                open_file_cache: OpenFileCacheConfig {
                    ttl: Duration::from_secs(60),
                    capacity: 128,
                    allow_write: false,
                },
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let root = fs.root_ino();
    let successful_ino = fs.create_file_at(root, "last-valid", false).await.unwrap();
    let failed_ino = fs.create_file_at(root, "failed-open", false).await.unwrap();
    let successful_attr = fs.stat_ino(successful_ino).await.unwrap();
    let failed_attr = fs.stat_ino(failed_ino).await.unwrap();
    (fs, successful_attr, failed_attr)
}

async fn run_generic(path: OpenPath) {
    let (fs, successful_attr, failed_attr) = generic_fixture().await;
    exercise_namespace_end(&fs, path, successful_attr, failed_attr.clone(), None).await;
    // Metrics are not live-open counts. This separate behavioral probe detects
    // a cached record_open left behind on the never successfully opened target.
    let before = meta_metrics(&fs).unwrap();
    let attr = fs
        .meta_layer()
        .stat_for_open(failed_attr.ino, true, false, false)
        .await
        .unwrap()
        .unwrap();
    let after = meta_metrics(&fs).unwrap();
    assert_eq!(attr_fingerprint(&attr), attr_fingerprint(&failed_attr));
    assert_eq!(after.open_file_cache_hit, before.open_file_cache_hit);
    assert_eq!(after.open_file_cache_miss, before.open_file_cache_miss + 1);
    assert_eq!(after.open_fresh_stat, before.open_fresh_stat + 1);
}

macro_rules! generic_case {
    ($name:ident, $path:ident) => {
        #[tokio::test]
        async fn $name() {
            run_generic(OpenPath::$path).await;
        }
    };
}
generic_case!(
    generic_file_exhaustion_preserves_state_and_never_reuses_fh,
    File
);
generic_case!(
    generic_cached_exhaustion_preserves_state_and_never_reuses_fh,
    Cached
);
generic_case!(
    generic_fresh_exhaustion_preserves_state_and_never_reuses_fh,
    Fresh
);
generic_case!(
    generic_dir_exhaustion_preserves_state_and_never_reuses_fh,
    Dir
);
generic_case!(
    generic_stats_exhaustion_preserves_state_and_never_reuses_fh,
    Stats
);

type PackedFs = VFS<
    PackedBlockStore<crate::cadapter::localfs::LocalFsBackend>,
    PackedReadonlyMeta<crate::cadapter::localfs::LocalFsBackend>,
>;

struct PackedFixture {
    fs: PackedFs,
    successful_attr: FileAttr,
    failed_attr: FileAttr,
    budget: Arc<SnapshotMountBudget>,
    _source: tempfile::TempDir,
    _spool: tempfile::TempDir,
    _objects: tempfile::TempDir,
}

async fn packed_fixture() -> PackedFixture {
    // Real v3 metadata and its actual SnapshotMountBudget; no synthetic budget guard.
    let source = tempfile::tempdir().unwrap();
    let spool = tempfile::tempdir().unwrap();
    let objects = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("last-valid"), b"payload-a").unwrap();
    std::fs::write(source.path().join("failed-open"), b"payload-b").unwrap();
    let client = crate::cadapter::client::ObjectClient::new(
        crate::cadapter::localfs::LocalFsBackend::new(objects.path()),
    );
    let inventory = SnapshotSourceNamespaceInventory::capture(
        source.path(),
        spool.path(),
        SnapshotSourceNamespaceOptions {
            root_inode: 1,
            consistency: SnapshotSourceConsistency::BestEffortDetected,
            hardlink_policy: SnapshotSourceHardlinkPolicy::VisibleLinks,
            file_limits: SnapshotSourceFileLimits::default(),
        },
    )
    .await
    .unwrap();
    let built = inventory
        .build_snapshot(
            client.clone(),
            "fh-exhaustion".into(),
            SnapshotProducerOptions {
                snapshot_id: [41; 32],
                root_dir_key: [42; 32],
                root_inode: 1,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build_policy: Default::default(),
                metadata_codec: PackedCodec::Raw,
                data_codec: PackedCodec::Raw,
            },
        )
        .await
        .unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &built.reference)
        .await
        .unwrap();
    let layout = ChunkLayout::default();
    let budget = SnapshotMountBudget::defaults();
    let meta = Arc::new(
        PackedReadonlyMeta::from_snapshot_budget(
            client,
            snapshot,
            layout.chunk_size,
            0,
            budget.clone(),
        )
        .unwrap(),
    );
    let store = Arc::new(meta.block_store(layout.block_size).unwrap());
    let fs = VFS::from_workspace_components(VFSConfig::new(layout), store, meta).unwrap();
    let (_, successful_attr) = fs
        .meta_layer()
        .lookup_with_attr(1, "last-valid")
        .await
        .unwrap()
        .unwrap();
    let (_, failed_attr) = fs
        .meta_layer()
        .lookup_with_attr(1, "failed-open")
        .await
        .unwrap()
        .unwrap();
    PackedFixture {
        fs,
        successful_attr,
        failed_attr,
        budget,
        _source: source,
        _spool: spool,
        _objects: objects,
    }
}

async fn run_packed(path: OpenPath) {
    let fixture = packed_fixture().await;
    exercise_namespace_end(
        &fixture.fs,
        path,
        fixture.successful_attr.clone(),
        fixture.failed_attr.clone(),
        Some(&fixture.budget),
    )
    .await;
}

macro_rules! packed_case {
    ($name:ident, $path:ident) => {
        #[tokio::test]
        async fn $name() {
            run_packed(OpenPath::$path).await;
        }
    };
}
packed_case!(
    packed_file_exhaustion_preserves_all_budget_fields_and_never_reuses_fh,
    File
);
packed_case!(
    packed_cached_exhaustion_preserves_all_budget_fields_and_never_reuses_fh,
    Cached
);
packed_case!(
    packed_fresh_exhaustion_preserves_all_budget_fields_and_never_reuses_fh,
    Fresh
);
packed_case!(
    packed_dir_exhaustion_preserves_all_budget_fields_and_never_reuses_fh,
    Dir
);
packed_case!(
    packed_stats_exhaustion_preserves_all_budget_fields_and_never_reuses_fh,
    Stats
);

#[path = "handle_namespace_exhaustion_existing_api_tests/packed_dir_full_metadata_order_existing_api_tests.rs"]
mod packed_dir_full_metadata_order_existing_api_tests;
