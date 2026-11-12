//! Read-amplification regression tests for the background whole-block prefetch.
//!
//! A random small read that misses the page cache used to schedule a
//! background download of the whole block. When the working set is much larger
//! than the cache, that block is evicted before anybody reads it again, so
//! every small read paid for a whole block of traffic.
//!
//! The tests below run the same workload twice — once with the legacy
//! unconditional prefetch and once with the working-set guard — and require the
//! guarded run to transfer less than a small fraction of the legacy traffic.

use super::*;
use crate::cadapter::localfs::LocalFsBackend;
use crate::chunk::compress::encode_persisted_block;
use tokio::sync::Notify;

/// Backend that records the traffic it served.
///
/// `get_object` transfers a whole block, `get_object_range` transfers a range.
#[derive(Clone)]
struct CountingBackend {
    inner: LocalFsBackend,
    prefer_full: bool,
    full_gets: Arc<AtomicU64>,
    full_gets_completed: Arc<AtomicU64>,
    full_gets_changed: Arc<Notify>,
    full_get_gate: Arc<Semaphore>,
    range_gets: Arc<AtomicU64>,
    full_bytes: Arc<AtomicU64>,
    range_bytes: Arc<AtomicU64>,
}

impl CountingBackend {
    fn new(root: &std::path::Path, prefer_full: bool) -> Self {
        Self {
            inner: LocalFsBackend::new(root),
            prefer_full,
            full_gets: Arc::new(AtomicU64::new(0)),
            full_gets_completed: Arc::new(AtomicU64::new(0)),
            full_gets_changed: Arc::new(Notify::new()),
            full_get_gate: Arc::new(Semaphore::new(1)),
            range_gets: Arc::new(AtomicU64::new(0)),
            full_bytes: Arc::new(AtomicU64::new(0)),
            range_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    fn total_gets(&self) -> u64 {
        self.full_gets.load(Ordering::Relaxed) + self.range_gets.load(Ordering::Relaxed)
    }

    fn total_bytes(&self) -> u64 {
        self.full_bytes.load(Ordering::Relaxed) + self.range_bytes.load(Ordering::Relaxed)
    }

    fn full_gets(&self) -> u64 {
        self.full_gets.load(Ordering::Relaxed)
    }
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
        self.full_gets.fetch_add(1, Ordering::Release);
        self.full_gets_changed.notify_waiters();
        let _gate = self.full_get_gate.acquire().await?;
        let data = self.inner.get_object(key).await?;
        if let Some(bytes) = data.as_ref() {
            self.full_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        self.full_gets_completed.fetch_add(1, Ordering::Release);
        self.full_gets_changed.notify_waiters();
        Ok(data)
    }

    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.range_gets.fetch_add(1, Ordering::Relaxed);
        let read = self.inner.get_object_range(key, offset, buf).await?;
        self.range_bytes.fetch_add(read as u64, Ordering::Relaxed);
        Ok(read)
    }

    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }

    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete_object(key).await
    }
}

/// One measurement of a random small-read workload.
struct Workload {
    backend: CountingBackend,
    stats: ObjectStoreStatsSnapshot,
}

impl Workload {
    fn bytes(&self) -> u64 {
        self.backend.total_bytes()
    }

    fn gets(&self) -> u64 {
        self.backend.total_gets()
    }
}

/// Builds a store whose objects hold one block per slice.
async fn store_with_slices(
    objects: &std::path::Path,
    cache: &std::path::Path,
    slices: u64,
    block_size: usize,
    working_set_factor: u32,
    read_memory_bytes: u64,
) -> anyhow::Result<(ObjectBlockStore<CountingBackend>, Vec<u8>, CountingBackend)> {
    let raw: Vec<u8> = (0..block_size).map(|i: usize| (i % 251) as u8).collect();
    let backend = CountingBackend::new(objects, false);
    for slice in 1..=slices {
        backend
            .put_object(
                &format!("chunks-v2/{slice}/0"),
                &encode_persisted_block(&raw, Compression::None),
            )
            .await?;
    }
    let store = ObjectBlockStore::new_with_configs_async(
        ObjectClient::new(backend.clone()),
        ChunksCacheConfig::with_budgets(read_memory_bytes, 0, cache.to_path_buf()),
        BlockStoreConfig {
            compression: Compression::None,
            block_size,
            range_prefetch_working_set_factor: working_set_factor,
            ..Default::default()
        },
    )
    .await?;
    Ok((store, raw, backend))
}

/// Reads one 4 KiB page from every slice, each from a different block.
async fn random_small_reads(
    store: &ObjectBlockStore<CountingBackend>,
    backend: &CountingBackend,
    slices: u64,
    raw: &[u8],
) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 4096];
    for slice in 1..=slices {
        // A distinct page per slice, so no two reads share a block.
        let offset = ((slice * 7) % (raw.len() / 4096) as u64) * 4096;
        store.read_range((slice, 0), offset, &mut buf).await?;
        let start = offset as usize;
        assert_eq!(buf, raw[start..start + 4096]);
        // This is a serial traffic property, not a concurrency benchmark.
        // Drain each accepted prefetch before the next miss: the production
        // eight-slot limiter deliberately drops speculation when it is busy.
        wait_for_prefetch_settle(store, backend).await;
    }
    Ok(())
}

/// Wait for real backend completion, then for cache publication to release
/// the worker's permit. The scheduling counter alone is never a drain signal.
async fn wait_for_prefetch_settle(
    store: &ObjectBlockStore<CountingBackend>,
    backend: &CountingBackend,
) {
    let scheduled = store.object_metrics.snapshot().read_background_prefetches;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let changed = backend.full_gets_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if backend.full_gets_completed.load(Ordering::Acquire) == scheduled {
                break;
            }
            changed.await;
        }
        // Do not acquire these before observing backend completion: doing so
        // could make an unpolled spawned worker fail try_acquire and be dropped.
        let _drained = store
            .range_prefetch_limit
            .clone()
            .acquire_many_owned(8)
            .await
            .unwrap();
        assert_eq!(
            store
                .object_metrics
                .snapshot()
                .read_background_prefetch_dropped,
            0
        );
        assert_eq!(backend.full_gets(), scheduled);
    })
    .await
    .expect("scheduled prefetches must finish backend IO and cache publication");
}

#[tokio::test]
async fn prefetch_settle_waits_for_backend_completion_and_cache_publication() -> anyhow::Result<()>
{
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let (store, raw, backend) =
        store_with_slices(objects.path(), cache.path(), 1, 64 << 10, 0, 64 << 10).await?;
    let gate = backend.full_get_gate.acquire().await?;
    let mut page = vec![0; 4096];
    store.read_range((1, 0), 4096, &mut page).await?;
    assert_eq!(page, raw[4096..8192]);

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let changed = backend.full_gets_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if backend.full_gets() == 1 {
                break;
            }
            changed.await;
        }
    })
    .await
    .expect("the real prefetch must enter the gated full GET");
    assert_eq!(
        store.object_metrics.snapshot().read_background_prefetches,
        1
    );
    assert_eq!(store.range_prefetch_limit.available_permits(), 7);
    assert_eq!(backend.full_gets_completed.load(Ordering::Acquire), 0);

    let settle = wait_for_prefetch_settle(&store, &backend);
    tokio::pin!(settle);
    // The old helper returned after 20 ms of an unchanged scheduling count.
    // A held backend gate must keep the drain pending regardless of that count.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut settle)
            .await
            .is_err(),
        "settle returned before the gated backend completed"
    );
    drop(gate);
    settle.await;
    assert_eq!(backend.full_gets_completed.load(Ordering::Acquire), 1);
    assert_eq!(store.range_prefetch_limit.available_permits(), 8);
    assert_eq!(
        store
            .block_cache
            .get(&ObjectBlockStore::<CountingBackend>::key_for((1, 0)))
            .await,
        Some(Bytes::from(raw)),
        "the worker must finish cache publication before drain returns"
    );
    Ok(())
}

/// Runs the workload with the given working-set factor.
async fn run_workload(
    working_set_factor: u32,
    slices: u64,
    block_size: usize,
    read_memory_bytes: u64,
) -> anyhow::Result<Workload> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let (store, raw, backend) = store_with_slices(
        objects.path(),
        cache.path(),
        slices,
        block_size,
        working_set_factor,
        read_memory_bytes,
    )
    .await?;
    random_small_reads(&store, &backend, slices, &raw).await?;
    let stats = store.object_metrics.snapshot();
    Ok(Workload { backend, stats })
}

#[tokio::test]
async fn a_working_set_above_the_cache_budget_does_not_amplify_reads() -> anyhow::Result<()> {
    let block_size = 4 << 20;
    let slices = 64u64;
    // The block cache holds a single block, so a working set of many blocks
    // can never be served from it: every prefetched block is dead weight.
    let read_memory_bytes = block_size as u64;

    let legacy = run_workload(0, slices, block_size, read_memory_bytes).await?;
    let guarded = run_workload(4, slices, block_size, read_memory_bytes).await?;

    assert_eq!(
        legacy.stats.read_background_prefetches, slices,
        "the legacy path prefetches a whole block for every cold miss"
    );
    assert_eq!(
        legacy.backend.full_gets(),
        slices,
        "the legacy path downloads one whole object per read"
    );

    assert!(
        guarded.stats.read_background_prefetch_suppressed > 0,
        "the guard must report the prefetches it suppressed"
    );
    assert!(
        guarded.backend.full_gets() < slices,
        "the guarded path must not download a whole object per read, got {}",
        guarded.backend.full_gets()
    );

    // The FINDING measured 12.1 MB pulled for every 4 KiB read. The guarded
    // path must stay close to the page granularity the read actually needs.
    let legacy_amplification = legacy.bytes() as f64 / (slices * 4096) as f64;
    let guarded_amplification = guarded.bytes() as f64 / (slices * 4096) as f64;
    println!(
        "legacy: bytes={} gets={} full_gets={} ampl={legacy_amplification:.1}",
        legacy.bytes(),
        legacy.gets(),
        legacy.backend.full_gets()
    );
    println!(
        "guarded: bytes={} gets={} full_gets={} ampl={guarded_amplification:.1}",
        guarded.bytes(),
        guarded.gets(),
        guarded.backend.full_gets()
    );
    assert!(
        guarded_amplification * 2.0 < legacy_amplification,
        "the guarded path ({guarded_amplification:.1}x) must transfer far less than \
         the legacy path ({legacy_amplification:.1}x)"
    );
    assert!(
        guarded_amplification * 4.0 < legacy_amplification,
        "the guarded path ({guarded_amplification:.1}x) must transfer far less \
         than the legacy path ({legacy_amplification:.1}x)"
    );

    // The property that matters: the traffic the guard spends on speculative
    // whole-block downloads is bounded by the cache, not by the working set.
    // The legacy path scales with the working set — 64 blocks in, 64 whole
    // blocks out — while the guarded path stops after the cache budget is
    // reached, so the same reads cost a bounded number of blocks.
    let speculative_bytes = guarded.backend.full_bytes.load(Ordering::Relaxed);
    // The persisted frame adds a small header per object, so the traffic is
    // the cache budget plus one header per prefetched block.
    let cache_budget = read_memory_bytes * 4 + slices * PERSISTED_HEADER_LEN as u64;
    assert!(
        speculative_bytes <= cache_budget,
        "speculative whole-block traffic {speculative_bytes} must stay within \
         the cache budget {cache_budget}"
    );
    assert!(
        guarded.backend.full_gets() <= 5,
        "the guard must stop prefetching once the cache budget is reached, \
         got {} whole-object GETs",
        guarded.backend.full_gets()
    );
    Ok(())
}

#[tokio::test]
async fn a_working_set_inside_the_cache_budget_still_prefetches() -> anyhow::Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let block_size = 4 << 20;
    let slices = 4u64;
    // A cache that holds every block of the working set many times over: the
    // prefetch pays off and must not be suppressed.
    let read_memory_bytes = block_size as u64 * slices * 4;
    let (store, raw, backend) = store_with_slices(
        objects.path(),
        cache.path(),
        slices,
        block_size,
        4,
        read_memory_bytes,
    )
    .await?;

    random_small_reads(&store, &backend, slices, &raw).await?;

    let stats = store.object_metrics.snapshot();
    assert_eq!(
        stats.read_background_prefetch_suppressed, 0,
        "a working set inside the cache budget must not suppress prefetches"
    );
    assert_eq!(
        stats.read_background_prefetches, slices,
        "each cold block miss still schedules its background prefetch"
    );
    Ok(())
}

#[tokio::test]
async fn the_zero_factor_keeps_the_unconditional_prefetch() -> anyhow::Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let block_size = 4 << 20;
    let slices = 8u64;
    let (store, raw, backend) = store_with_slices(
        objects.path(),
        cache.path(),
        slices,
        block_size,
        0,
        block_size as u64,
    )
    .await?;

    random_small_reads(&store, &backend, slices, &raw).await?;

    let stats = store.object_metrics.snapshot();
    assert_eq!(
        stats.read_background_prefetch_suppressed, 0,
        "a zero factor disables the estimate and keeps the legacy behavior"
    );
    assert_eq!(
        stats.read_background_prefetches, slices,
        "every range miss still schedules its background prefetch"
    );
    Ok(())
}

#[tokio::test]
async fn the_disabled_prefetch_never_downloads_a_whole_block() -> anyhow::Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let block_size = 4 << 20;
    let slices = 8u64;
    let raw: Vec<u8> = (0..block_size).map(|i: usize| (i % 251) as u8).collect();
    let backend = CountingBackend::new(objects.path(), false);
    for slice in 1..=slices {
        backend
            .put_object(
                &format!("chunks-v2/{slice}/0"),
                &encode_persisted_block(&raw, Compression::None),
            )
            .await?;
    }
    let store = ObjectBlockStore::new_with_configs_async(
        ObjectClient::new(backend.clone()),
        ChunksCacheConfig::with_budgets(block_size as u64, 0, cache.path().to_path_buf()),
        BlockStoreConfig {
            compression: Compression::None,
            block_size,
            range_background_prefetch: false,
            ..Default::default()
        },
    )
    .await?;

    random_small_reads(&store, &backend, slices, &raw).await?;

    let stats = store.object_metrics.snapshot();
    assert_eq!(stats.read_background_prefetches, 0);
    assert_eq!(stats.read_full_gets, 0);
    assert_eq!(backend.full_gets(), 0);
    Ok(())
}

#[tokio::test]
async fn full_object_read_without_probe_populates_format_cache_for_subsequent_range_reads()
-> anyhow::Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let block_size = 64 << 10;
    let raw: Vec<u8> = (0..block_size).map(|i: usize| (i % 251) as u8).collect();
    let backend = CountingBackend::new(objects.path(), true);
    backend
        .put_object(
            "chunks-v2/42/0",
            &encode_persisted_block(&raw, Compression::None),
        )
        .await?;

    let store = ObjectBlockStore::new_with_configs_async(
        ObjectClient::new(backend.clone()),
        // Zero block cache budget so the subsequent range read misses block_cache.
        ChunksCacheConfig::with_budgets(0, 0, cache.path().to_path_buf()),
        BlockStoreConfig {
            compression: Compression::None,
            block_size,
            range_background_prefetch: false,
            ..Default::default()
        },
    )
    .await?;

    // First read uses WholeObject (skip_full_read_probe = true), issuing 1 full GET
    // and 0 range GETs while learning the persisted ObjectLayout from the 4-byte header.
    let mut full_buf = vec![0u8; block_size];
    store.read_range((42, 0), 0, &mut full_buf).await?;
    assert_eq!(full_buf, raw);
    assert_eq!(backend.full_gets(), 1);
    assert_eq!(backend.range_gets.load(Ordering::Relaxed), 0);

    // Switch to Sparse range reads: since format_cache was populated by the full GET,
    // a 4 KiB range read must issue exactly 1 payload range GET and 0 layout-probe GETs.
    let store = store.with_read_fetch_policy(ReadFetchPolicy::Sparse);
    let mut page = vec![0u8; 4096];
    store.read_range((42, 0), 4096, &mut page).await?;
    assert_eq!(&page[..], &raw[4096..8192]);
    assert_eq!(backend.full_gets(), 1);
    assert_eq!(
        backend.range_gets.load(Ordering::Relaxed),
        1,
        "format_cache hit from prior full GET must avoid a 4-byte layout probe range GET"
    );
    Ok(())
}
