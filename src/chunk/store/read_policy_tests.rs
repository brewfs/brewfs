use super::*;
use crate::cadapter::localfs::LocalFsBackend;
use crate::chunk::compress::encode_persisted_block;

struct CountingBackend {
    inner: LocalFsBackend,
    prefer_full: bool,
}

#[async_trait]
impl ObjectBackend for CountingBackend {
    fn prefers_full_object_reads(&self) -> bool {
        self.prefer_full
    }

    async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object(key, data).await
    }

    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }

    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.inner.get_object_range(key, offset, buf).await
    }

    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }

    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete_object(key).await
    }
}

#[tokio::test]
async fn sparse_fetch_policy_overrides_backend_and_both_prefetch_paths() -> anyhow::Result<()> {
    // Exercise both the aligned direct-target branch and the partial-page branch,
    // with and without a whole-object backend preference (e.g. an EC hint).
    for prefer_full in [true, false] {
        for size in [4096, DEFAULT_PAGE_SIZE] {
            let objects = tempfile::tempdir()?;
            let cache = tempfile::tempdir()?;
            let raw = vec![37; 4 * 1024 * 1024];
            let backend = CountingBackend {
                inner: LocalFsBackend::new(objects.path()),
                prefer_full,
            };
            backend
                .put_object(
                    "chunks-v2/1/0",
                    &encode_persisted_block(&raw, Compression::None),
                )
                .await?;
            let store = ObjectBlockStore::new_with_configs_async(
                ObjectClient::new(backend),
                ChunksCacheConfig::with_budgets(8 << 20, 0, cache.path().to_path_buf()),
                BlockStoreConfig {
                    compression: Compression::None,
                    // Deliberately leave range_background_prefetch at its default true.
                    ..Default::default()
                },
            )
            .await?
            .with_read_fetch_policy(ReadFetchPolicy::Sparse);
            let mut output = vec![0; size];
            store.read_range((1, 0), 0, &mut output).await?;
            assert_eq!(output, raw[..size]);
            let stats = store.object_metrics.snapshot();
            assert_eq!(stats.read_full_gets, 0, "sparse must override the EC hint");
            assert_eq!(stats.read_range_gets, 1);
            assert_eq!(
                stats.get_bytes,
                (PERSISTED_HEADER_LEN + DEFAULT_PAGE_SIZE) as u64
            );
            assert_eq!(stats.get_ops, 2, "cold format probe plus page GET");
            assert_eq!(
                stats.read_background_prefetches, 0,
                "neither sparse branch may prefetch a full object"
            );

            // A page hit must survive independent format-cache eviction.
            store.format_cache.invalidate(&(1, 0)).await;
            store.read_range((1, 0), 0, &mut output).await?;
            let stats = store.object_metrics.snapshot();
            assert_eq!(stats.get_ops, 2);
            assert_eq!(stats.read_page_cache_hits, 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn read_fetch_policy_preserves_scans_compressed_objects_and_tails() -> anyhow::Result<()> {
    for policy in [
        ReadFetchPolicy::BackendDefault,
        ReadFetchPolicy::WholeObject,
        ReadFetchPolicy::Sparse,
    ] {
        for compression in [Compression::None, Compression::Lz4, Compression::Zstd(1)] {
            let objects = tempfile::tempdir()?;
            let cache = tempfile::tempdir()?;
            let raw = vec![37; 4 << 20];
            let backend = CountingBackend {
                inner: LocalFsBackend::new(objects.path()),
                prefer_full: policy == ReadFetchPolicy::BackendDefault,
            };
            for (id, bytes) in [(1, raw.as_slice()), (2, &raw[..raw.len() - 17])] {
                backend
                    .put_object(
                        &format!("chunks-v2/{id}/0"),
                        &encode_persisted_block(bytes, compression),
                    )
                    .await?;
            }
            let store = ObjectBlockStore::new_with_configs_async(
                ObjectClient::new(backend),
                ChunksCacheConfig::with_budgets(16 << 20, 0, cache.path().to_path_buf()),
                BlockStoreConfig {
                    compression: Compression::None,
                    ..Default::default()
                },
            )
            .await?
            .with_read_fetch_policy(policy);
            // A cold full scan retains one whole GET, including under Sparse.
            let mut full = vec![0; raw.len()];
            store.read_range((1, 0), 0, &mut full).await?;
            assert_eq!(full, raw);
            assert_eq!(store.object_metrics.snapshot().get_ops, 1);
            assert_eq!(store.object_metrics.snapshot().read_full_gets, 1);
            let mut part = vec![0; 4096];
            store.read_range((1, 0), 65536, &mut part).await?;
            assert_eq!(part, raw[65536..69632]);
            assert_eq!(store.object_metrics.snapshot().get_ops, 1);
            let tail_start = raw.len() - 17 - part.len();
            store
                .read_range((2, 0), tail_start as u64, &mut part)
                .await?;
            assert_eq!(part, raw[tail_start..raw.len() - 17]);
            assert!(
                store
                    .read_range((2, 0), (raw.len() - 17 - 4095) as u64, &mut part)
                    .await
                    .is_err()
            );
            assert_eq!(
                store.object_metrics.snapshot().read_background_prefetches,
                0
            );
            if compression != Compression::None {
                assert_eq!(
                    store.object_metrics.snapshot().read_full_gets,
                    2,
                    "compressed tails must decode the full persisted frame"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn read_fetch_policy_does_not_change_written_frame_bytes() -> anyhow::Result<()> {
    let objects = tempfile::tempdir()?;
    let raw = Bytes::from(vec![37; 131071]);
    for compression in [Compression::None, Compression::Lz4, Compression::Zstd(1)] {
        let expected = encode_persisted_block(&raw, compression);
        for policy in [
            ReadFetchPolicy::BackendDefault,
            ReadFetchPolicy::WholeObject,
            ReadFetchPolicy::Sparse,
        ] {
            let cache = tempfile::tempdir()?;
            let store = ObjectBlockStore::new_with_configs_async(
                ObjectClient::new(LocalFsBackend::new(objects.path())),
                ChunksCacheConfig::with_budgets(8 << 20, 0, cache.path().to_path_buf()),
                BlockStoreConfig {
                    compression,
                    ..Default::default()
                },
            )
            .await?
            .with_read_fetch_policy(policy);
            store.write_fresh_range((1, 0), 0, &raw).await?;
            assert_eq!(
                tokio::fs::read(objects.path().join("chunks-v2/1/0")).await?,
                expected
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn sparse_fetch_policy_piggybacks_the_whole_object_publication_flight() -> anyhow::Result<()>
{
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let raw = vec![37; 4 << 20];
    let backend = CountingBackend {
        inner: LocalFsBackend::new(objects.path()),
        prefer_full: true,
    };
    backend
        .put_object(
            "chunks-v2/1/0",
            &encode_persisted_block(&raw, Compression::None),
        )
        .await?;
    let store = ObjectBlockStore::new_with_configs_async(
        ObjectClient::new(backend),
        ChunksCacheConfig::with_budgets(8 << 20, 0, cache.path().to_path_buf()),
        BlockStoreConfig::default(),
    )
    .await?
    .with_read_fetch_policy(ReadFetchPolicy::Sparse);
    let gate = store.block_cache.hold_publication("chunks/1/0").await;
    let mut full = vec![0; raw.len()];
    let mut small = vec![0; 4096];
    let mut leader = Box::pin(store.read_range((1, 0), 0, &mut full));
    // LocalFs is asynchronous; drive it until publication is blocked.
    tokio::select! {
        result = &mut leader => panic!("publication gate unexpectedly completed: {result:?}"),
        _ = async {
            while store.object_metrics.snapshot().read_full_gets == 0 {
                tokio::task::yield_now().await;
            }
        } => {}
    }
    let mut follower = Box::pin(store.read_range((1, 0), 65536, &mut small));
    assert!(futures::poll!(&mut follower).is_pending());
    drop(gate);
    let (a, b) = tokio::join!(leader, follower);
    a?;
    b?;
    assert_eq!(full, raw);
    assert_eq!(small, raw[65536..69632]);
    assert_eq!(store.object_metrics.snapshot().get_ops, 1);
    assert_eq!(store.object_metrics.snapshot().read_piggyback_full, 1);
    Ok(())
}

async fn sparse_store_with_blocks(
    objects: &std::path::Path,
    cache: &std::path::Path,
    blocks: u32,
) -> anyhow::Result<(ObjectBlockStore<CountingBackend>, Vec<u8>)> {
    let raw: Vec<u8> = (0..4 << 20).map(|i: u32| (i % 251) as u8).collect();
    let backend = CountingBackend {
        inner: LocalFsBackend::new(objects),
        prefer_full: true,
    };
    for index in 0..blocks {
        backend
            .put_object(
                &format!("chunks-v2/1/{index}"),
                &encode_persisted_block(&raw, Compression::None),
            )
            .await?;
    }
    let store = ObjectBlockStore::new_with_configs_async(
        ObjectClient::new(backend),
        ChunksCacheConfig::with_budgets(16 << 20, 0, cache.to_path_buf()),
        BlockStoreConfig {
            compression: Compression::None,
            ..Default::default()
        },
    )
    .await?
    .with_read_fetch_policy(ReadFetchPolicy::Sparse);
    Ok((store, raw))
}

async fn wait_for_block_cache(store: &ObjectBlockStore<CountingBackend>, key: &str) {
    for _ in 0..1000 {
        if store.block_cache.get(&key.to_string()).await.is_some() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("background prefetch did not publish {key}");
}

#[tokio::test]
async fn sparse_fetch_policy_prefetches_the_block_once_reads_become_sequential()
-> anyhow::Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let (store, raw) = sparse_store_with_blocks(objects.path(), cache.path(), 1).await?;
    let mut window = vec![0; 128 << 10];
    store.read_range((1, 0), 0, &mut window).await?;
    assert_eq!(window, raw[..128 << 10]);
    assert_eq!(
        store.object_metrics.snapshot().read_background_prefetches,
        0
    );
    // Kernel readahead continues exactly where the previous read ended.
    store.read_range((1, 0), 128 << 10, &mut window).await?;
    assert_eq!(window, raw[128 << 10..256 << 10]);
    assert_eq!(
        store.object_metrics.snapshot().read_background_prefetches,
        1
    );
    wait_for_block_cache(&store, "chunks/1/0").await;
    let gets = store.object_metrics.snapshot().get_ops;
    store.read_range((1, 0), 256 << 10, &mut window).await?;
    assert_eq!(window, raw[256 << 10..384 << 10]);
    assert_eq!(
        store.object_metrics.snapshot().get_ops,
        gets,
        "the stream continues from the block cache"
    );
    Ok(())
}

#[tokio::test]
async fn sparse_fetch_policy_keeps_random_reads_sparse() -> anyhow::Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let (store, raw) = sparse_store_with_blocks(objects.path(), cache.path(), 1).await?;
    let mut small = vec![0; 4096];
    // Adjacent pages and a re-read of the same bytes are not a forward stream.
    for offset in [1 << 20, 0, 1 << 20, 65536, 3 << 20, 2 << 20] {
        store.read_range((1, 0), offset, &mut small).await?;
        assert_eq!(small, raw[offset as usize..offset as usize + 4096]);
    }
    let stats = store.object_metrics.snapshot();
    assert_eq!(stats.read_background_prefetches, 0);
    assert_eq!(stats.read_full_gets, 0);
    Ok(())
}

#[tokio::test]
async fn sparse_fetch_policy_carries_a_sequential_stream_into_the_next_block() -> anyhow::Result<()>
{
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let (store, raw) = sparse_store_with_blocks(objects.path(), cache.path(), 2).await?;
    let mut window = vec![0; 128 << 10];
    let tail = raw.len() - window.len();
    store.read_range((1, 0), tail as u64, &mut window).await?;
    assert_eq!(window, raw[tail..]);
    assert_eq!(
        store.object_metrics.snapshot().read_background_prefetches,
        0
    );
    // The first read of the following block of the same slice continues the stream.
    store.read_range((1, 1), 0, &mut window).await?;
    assert_eq!(window, raw[..128 << 10]);
    assert_eq!(
        store.object_metrics.snapshot().read_background_prefetches,
        1
    );
    wait_for_block_cache(&store, "chunks/1/1").await;
    Ok(())
}

async fn sparse_store_with_slices(
    objects: &std::path::Path,
    cache: &std::path::Path,
    slices: u64,
) -> anyhow::Result<(ObjectBlockStore<CountingBackend>, Vec<u8>)> {
    let raw: Vec<u8> = (0..4 << 20).map(|i: u32| (i % 251) as u8).collect();
    let backend = CountingBackend {
        inner: LocalFsBackend::new(objects),
        prefer_full: true,
    };
    for slice in 1..=slices {
        backend
            .put_object(
                &format!("chunks-v2/{slice}/0"),
                &encode_persisted_block(&raw, Compression::None),
            )
            .await?;
    }
    let store = ObjectBlockStore::new_with_configs_async(
        ObjectClient::new(backend),
        ChunksCacheConfig::with_budgets(16 << 20, 0, cache.to_path_buf()),
        BlockStoreConfig {
            compression: Compression::None,
            ..Default::default()
        },
    )
    .await?
    .with_read_fetch_policy(ReadFetchPolicy::Sparse);
    Ok((store, raw))
}

#[tokio::test]
async fn sparse_fetch_policy_prefetches_blocks_of_a_densely_missed_slice() -> anyhow::Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let (store, raw) = sparse_store_with_slices(objects.path(), cache.path(), 1).await?;
    let mut small = vec![0; 4096];
    // Non-adjacent pages, so the sequential-stream rule never applies.
    for page in 0..SPARSE_DENSE_SLICE_MISSES as u64 {
        assert_eq!(
            store.object_metrics.snapshot().read_background_prefetches,
            0
        );
        let offset = page * 2 * DEFAULT_PAGE_SIZE as u64 + 512;
        store.read_range((1, 0), offset, &mut small).await?;
        assert_eq!(small, raw[offset as usize..offset as usize + 4096]);
    }
    assert_eq!(
        store.object_metrics.snapshot().read_background_prefetches,
        1
    );
    wait_for_block_cache(&store, "chunks/1/0").await;
    Ok(())
}

#[tokio::test]
async fn sparse_fetch_policy_keeps_misses_spread_over_slices_sparse() -> anyhow::Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let slices = u64::from(SPARSE_DENSE_SLICE_MISSES) * 2;
    let (store, raw) = sparse_store_with_slices(objects.path(), cache.path(), slices).await?;
    let mut small = vec![0; 4096];
    for slice in 1..=slices {
        let offset = (slice % 60) * DEFAULT_PAGE_SIZE as u64 + 512;
        store.read_range((slice, 0), offset, &mut small).await?;
        assert_eq!(small, raw[offset as usize..offset as usize + 4096]);
    }
    assert_eq!(
        store.object_metrics.snapshot().read_background_prefetches,
        0
    );
    Ok(())
}

#[tokio::test]
async fn sparse_fetch_policy_density_window_expires() -> anyhow::Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let (store, _) = sparse_store_with_slices(objects.path(), cache.path(), 1).await?;
    let mut small = vec![0; 4096];
    for page in 0..u64::from(SPARSE_DENSE_SLICE_MISSES) - 1 {
        store
            .read_range((1, 0), page * 2 * DEFAULT_PAGE_SIZE as u64, &mut small)
            .await?;
    }
    tokio::time::sleep(SPARSE_DENSITY_WINDOW + std::time::Duration::from_millis(100)).await;
    store
        .read_range((1, 0), 40 * DEFAULT_PAGE_SIZE as u64, &mut small)
        .await?;
    assert_eq!(
        store.object_metrics.snapshot().read_background_prefetches,
        0
    );
    Ok(())
}
