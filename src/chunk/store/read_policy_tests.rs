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
