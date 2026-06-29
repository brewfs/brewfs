//! Storage backends: asynchronous block-level IO traits and in-memory implementations.

use crate::chunk::bandwidth::BandwidthLimiter;
use crate::chunk::compress::{Compression, compress, decompress};
use crate::chunk::page_cache::{PageKey, ReadPageCache};
use crate::chunk::singleflight::SingleFlight;
use crate::utils::NumCastExt;
use crate::utils::zero::make_zero_bytes;
use crate::{
    cadapter::client::{ObjectBackend, ObjectClient},
    chunk::cache::{ChunksCache, ChunksCacheConfig},
};
use anyhow::{self, Context};
use async_trait::async_trait;
use bytes::Bytes;
use futures::executor::block_on;
use hex::encode;
use moka::{Entry, ops::compute::Op};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    io::SeekFrom,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};
use tokio::{
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{RwLock, Semaphore},
};

/// Abstract block store interface (cadapter/S3/etc. can implement this).
#[async_trait]
// ensure offset_in_block + data.len() <= block_size
pub trait BlockStore {
    /// Write a new block without reading any existing data.
    ///
    /// All writes use copy-on-write semantics: every write targets a fresh
    /// object/key.  There is no read-modify-write path — callers must ensure
    /// the target key is fresh; using this on an existing object would drop
    /// any previous content outside the written range.
    #[tracing::instrument(level = "trace", skip(self, chunks), fields(key = ?key, offset, chunk_count = chunks.len()))]
    async fn write_fresh_vectored(
        &self,
        key: BlockKey,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> anyhow::Result<u64> {
        let data = chunks
            .into_iter()
            .flat_map(|e| e.to_vec())
            .collect::<Vec<_>>();
        self.write_fresh_range(key, offset, &data).await
    }

    /// Write a new block without reading any existing data.
    /// Required — every store must implement COW writes directly.
    async fn write_fresh_range(
        &self,
        key: BlockKey,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<u64>;

    async fn read_range(&self, key: BlockKey, offset: u64, buf: &mut [u8]) -> anyhow::Result<()>;

    /// Delete `block_count` blocks starting from `key.1` (block_index) for slice `key.0`.
    async fn delete_range(&self, key: BlockKey, block_count: u64) -> anyhow::Result<()>;

    /// Proactively insert a block into the read cache after upload.
    /// Default is a no-op; ObjectBlockStore overrides to populate ChunksCache.
    #[allow(dead_code)]
    async fn cache_block(&self, _key: BlockKey, _data: &[u8]) -> anyhow::Result<()> {
        Ok(())
    }

    /// Returns shared cache hit/miss counters for diagnostics (.stats file).
    /// Default returns (None, None); ObjectBlockStore overrides.
    fn cache_counters(&self) -> (Option<Arc<AtomicU64>>, Option<Arc<AtomicU64>>) {
        (None, None)
    }

    /// Returns object-store request counters for diagnostics.
    /// Default returns None; ObjectBlockStore overrides.
    fn object_store_metrics(&self) -> Option<Arc<ObjectStoreMetrics>> {
        None
    }
}

pub type BlockKey = (u64 /*slice_id*/, u32 /*block_index*/);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectStoreStatsSnapshot {
    pub get_ops: u64,
    pub get_bytes: u64,
    pub get_lat_us: u64,
    pub put_ops: u64,
    pub put_bytes: u64,
    pub put_lat_us: u64,
    pub put_prepare_lat_us: u64,
    pub put_cache_lat_us: u64,
    pub del_ops: u64,
    pub read_block_cache_hits: u64,
    pub read_page_cache_hits: u64,
    pub read_page_cache_misses: u64,
    pub read_range_gets: u64,
    pub read_full_gets: u64,
    pub read_piggyback_full: u64,
    pub read_background_prefetches: u64,
    pub read_background_prefetch_dropped: u64,
}

#[derive(Debug, Default)]
pub struct ObjectStoreMetrics {
    get_ops: AtomicU64,
    get_bytes: AtomicU64,
    get_lat_us: AtomicU64,
    put_ops: AtomicU64,
    put_bytes: AtomicU64,
    put_lat_us: AtomicU64,
    put_prepare_lat_us: AtomicU64,
    put_cache_lat_us: AtomicU64,
    del_ops: AtomicU64,
    read_block_cache_hits: AtomicU64,
    read_page_cache_hits: AtomicU64,
    read_page_cache_misses: AtomicU64,
    read_range_gets: AtomicU64,
    read_full_gets: AtomicU64,
    read_piggyback_full: AtomicU64,
    read_background_prefetches: AtomicU64,
    read_background_prefetch_dropped: AtomicU64,
}

impl ObjectStoreMetrics {
    pub fn snapshot(&self) -> ObjectStoreStatsSnapshot {
        ObjectStoreStatsSnapshot {
            get_ops: self.get_ops.load(Ordering::Relaxed),
            get_bytes: self.get_bytes.load(Ordering::Relaxed),
            get_lat_us: self.get_lat_us.load(Ordering::Relaxed),
            put_ops: self.put_ops.load(Ordering::Relaxed),
            put_bytes: self.put_bytes.load(Ordering::Relaxed),
            put_lat_us: self.put_lat_us.load(Ordering::Relaxed),
            put_prepare_lat_us: self.put_prepare_lat_us.load(Ordering::Relaxed),
            put_cache_lat_us: self.put_cache_lat_us.load(Ordering::Relaxed),
            del_ops: self.del_ops.load(Ordering::Relaxed),
            read_block_cache_hits: self.read_block_cache_hits.load(Ordering::Relaxed),
            read_page_cache_hits: self.read_page_cache_hits.load(Ordering::Relaxed),
            read_page_cache_misses: self.read_page_cache_misses.load(Ordering::Relaxed),
            read_range_gets: self.read_range_gets.load(Ordering::Relaxed),
            read_full_gets: self.read_full_gets.load(Ordering::Relaxed),
            read_piggyback_full: self.read_piggyback_full.load(Ordering::Relaxed),
            read_background_prefetches: self.read_background_prefetches.load(Ordering::Relaxed),
            read_background_prefetch_dropped: self
                .read_background_prefetch_dropped
                .load(Ordering::Relaxed),
        }
    }

    fn record_get(&self, bytes: u64, duration: Duration) {
        self.get_ops.fetch_add(1, Ordering::Relaxed);
        self.get_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.get_lat_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    fn record_put(&self, bytes: u64, duration: Duration) {
        self.put_ops.fetch_add(1, Ordering::Relaxed);
        self.put_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.put_lat_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    fn record_put_prepare(&self, duration: Duration) {
        self.put_prepare_lat_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    fn record_put_cache(&self, duration: Duration) {
        self.put_cache_lat_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    fn record_delete(&self) {
        self.del_ops.fetch_add(1, Ordering::Relaxed);
    }

    fn record_read_block_cache_hit(&self) {
        self.read_block_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_read_page_cache_hit(&self) {
        self.read_page_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_read_page_cache_miss(&self) {
        self.read_page_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    fn record_read_range_get(&self) {
        self.read_range_gets.fetch_add(1, Ordering::Relaxed);
    }

    fn record_read_full_get(&self) {
        self.read_full_gets.fetch_add(1, Ordering::Relaxed);
    }

    fn record_read_piggyback_full(&self) {
        self.read_piggyback_full.fetch_add(1, Ordering::Relaxed);
    }

    fn record_read_background_prefetch(&self) {
        self.read_background_prefetches
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_read_background_prefetch_dropped(&self) {
        self.read_background_prefetch_dropped
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Simple in-memory implementation for local development/testing.
#[derive(Default)]
#[allow(dead_code)]
pub struct InMemoryBlockStore {
    map: RwLock<HashMap<BlockKey, Vec<u8>>>,
}

#[allow(dead_code)]
impl InMemoryBlockStore {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl BlockStore for InMemoryBlockStore {
    async fn write_fresh_range(
        &self,
        key: BlockKey,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<u64> {
        let mut guard = self.map.write().await;
        let entry = guard.entry(key).or_insert_with(Vec::new);
        let start = offset.as_usize();
        let end = start + data.len();
        if entry.len() < end {
            entry.resize(end, 0);
        }
        entry[start..end].copy_from_slice(data);
        Ok(data.len() as u64)
    }

    // Caller is responsible for zero-filling buf; this method only overwrites existing bytes.
    async fn read_range(&self, key: BlockKey, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        let guard = self.map.read().await;
        if let Some(src) = guard.get(&key) {
            let start = offset.as_usize();
            let end = start + buf.len();
            let copy_end = end.min(src.len());
            if copy_end > start {
                let len = copy_end - start;
                buf[..len].copy_from_slice(&src[start..copy_end]);
            }
        }
        Ok(())
    }

    async fn delete_range(&self, key: BlockKey, block_count: u64) -> anyhow::Result<()> {
        let (chunk_id, block_index) = key;
        let mut guard = self.map.write().await;
        let start = block_index;
        let end = start + block_count.as_u32();
        for i in start..end {
            guard.remove(&(chunk_id, i));
        }
        Ok(())
    }
}

/// BlockStore backed by cadapter::client (key space `chunks/{chunk_id}/{block_index}`).
pub struct ObjectBlockStore<B: ObjectBackend> {
    client: Arc<ObjectClient<B>>,
    block_cache: ChunksCache,
    /// Page-granularity (64KB) read cache for small range reads that would
    /// otherwise be discarded.  Intercepts repeated small random reads so they
    /// hit memory instead of making a network round-trip every time.
    page_cache: ReadPageCache,
    /// SingleFlight controller for coalescing concurrent page-cache misses.
    page_flight: SingleFlight<PageKey, Bytes>,
    /// SingleFlight controller for coalescing concurrent reads to the same block
    /// Thread-safe and shared across the store lifetime so concurrent requests can coalesce.
    read_flight: Arc<SingleFlight<BlockKey, Bytes>>,
    /// Limits range-triggered background full-block prefetches. JuiceFS defaults
    /// to a single prefetch worker; foreground reads are still allowed to use
    /// read_flight directly and are not throttled by this semaphore.
    range_prefetch_limit: Arc<Semaphore>,
    /// Configuration for read strategy
    config: BlockStoreConfig,
    /// Network bandwidth rate limiter for uploads/downloads
    bandwidth: BandwidthLimiter,
    /// Object store request counters exposed through VFS `.stats`.
    object_metrics: Arc<ObjectStoreMetrics>,
}

/// Configuration for ObjectBlockStore read strategy
#[derive(Debug, Clone)]
pub struct BlockStoreConfig {
    /// Block size in bytes (default: 4MB)
    pub block_size: usize,
    /// For ranges smaller than this threshold, use direct range read instead of full block read
    /// Default is 25% of block size (1MB for 4MB blocks)
    pub range_read_threshold: f32,
    /// Page size for the page-granularity read cache (default: 64KB).
    /// Small range reads are aligned to page boundaries, fetched, and cached at this granularity.
    pub page_size: usize,
    /// Maximum number of pages in the read cache (default: 4096 → 256MB with 64KB pages).
    pub page_cache_capacity: usize,
    /// Whether a page-cache range miss should schedule a best-effort full-block prefetch.
    pub range_background_prefetch: bool,
    /// Block compression algorithm for storage and transfer.
    /// Blocks are compressed before S3 upload and decompressed on read.
    pub compression: Compression,
    /// Whether freshly uploaded write blocks should be inserted into the read cache.
    pub populate_write_cache_after_upload: bool,
    /// Whether uploaded write blocks should also be persisted into the disk read cache.
    pub persist_write_cache_after_upload: bool,
}

impl Default for BlockStoreConfig {
    fn default() -> Self {
        Self {
            block_size: 4 * 1024 * 1024, // 4MB
            range_read_threshold: 0.25,  // 25% = 1MB for 4MB blocks
            page_size: 64 * 1024,        // 64KB
            page_cache_capacity: 4096,   // 4096 pages × 64KB = 256MB
            range_background_prefetch: true,
            compression: Compression::Lz4,
            populate_write_cache_after_upload: true,
            persist_write_cache_after_upload: false,
        }
    }
}

impl BlockStoreConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.block_size == 0 {
            anyhow::bail!("block_size must be greater than 0");
        }
        if !(0.0..=1.0).contains(&self.range_read_threshold) {
            anyhow::bail!("range_read_threshold must be between 0.0 and 1.0");
        }
        if self.page_size == 0 {
            anyhow::bail!("page_size must be greater than 0");
        }
        if self.page_cache_capacity == 0 {
            anyhow::bail!("page_cache_capacity must be greater than 0");
        }
        Ok(())
    }

    fn range_size_threshold(&self) -> usize {
        (self.block_size as f32 * self.range_read_threshold) as usize
    }
}

impl<B: ObjectBackend + 'static> ObjectBlockStore<B> {
    #[allow(dead_code)]
    pub fn new(client: ObjectClient<B>) -> Self {
        let cache_dir = dirs::cache_dir().unwrap().join("brewfs");

        let _ = fs::create_dir_all(cache_dir.clone());

        let block_cache = block_on(ChunksCache::new_with_config(ChunksCacheConfig::default()))
            .map_err(|e| anyhow::anyhow!("Failed to create cache: {}", e))
            .unwrap();
        let config = BlockStoreConfig::default();
        config.validate().expect("default config must be valid");
        let page_cache = ReadPageCache::new(config.page_cache_capacity, config.page_size);
        Self {
            client: Arc::new(client),
            block_cache,
            page_cache,
            page_flight: SingleFlight::new(),
            read_flight: Arc::new(SingleFlight::new()),
            range_prefetch_limit: Arc::new(Semaphore::new(8)),
            config,
            bandwidth: BandwidthLimiter::unlimited(),
            object_metrics: Arc::new(ObjectStoreMetrics::default()),
        }
    }

    #[allow(dead_code)]
    pub async fn new_async(client: ObjectClient<B>) -> anyhow::Result<Self> {
        Self::new_with_configs_async(
            client,
            ChunksCacheConfig::default(),
            BlockStoreConfig::default(),
        )
        .await
    }

    /// Creates a new ObjectBlockStore with custom cache configuration
    #[allow(unused)]
    pub fn new_with_config(
        client: ObjectClient<B>,
        cache_config: ChunksCacheConfig,
    ) -> anyhow::Result<Self> {
        Self::new_with_configs(client, cache_config, BlockStoreConfig::default())
    }

    /// Creates a new ObjectBlockStore with custom cache and block store configurations
    #[allow(unused)]
    pub fn new_with_configs(
        client: ObjectClient<B>,
        cache_config: ChunksCacheConfig,
        store_config: BlockStoreConfig,
    ) -> anyhow::Result<Self> {
        store_config.validate()?;
        let cache_dir = dirs::cache_dir().unwrap().join("brewfs");
        let _ = fs::create_dir_all(cache_dir.clone());

        let block_cache = block_on(ChunksCache::new_with_config(cache_config))
            .map_err(|e| anyhow::anyhow!("Failed to create cache: {}", e))?;
        let page_cache =
            ReadPageCache::new(store_config.page_cache_capacity, store_config.page_size);
        Ok(Self {
            client: Arc::new(client),
            block_cache,
            page_cache,
            page_flight: SingleFlight::new(),
            read_flight: Arc::new(SingleFlight::new()),
            range_prefetch_limit: Arc::new(Semaphore::new(8)),
            config: store_config,
            bandwidth: BandwidthLimiter::unlimited(),
            object_metrics: Arc::new(ObjectStoreMetrics::default()),
        })
    }

    pub async fn new_with_configs_async(
        client: ObjectClient<B>,
        cache_config: ChunksCacheConfig,
        store_config: BlockStoreConfig,
    ) -> anyhow::Result<Self> {
        store_config.validate()?;
        let cache_dir = dirs::cache_dir().unwrap().join("brewfs");
        let _ = fs::create_dir_all(cache_dir.clone());

        let block_cache = ChunksCache::new_with_config(cache_config).await?;
        let page_cache =
            ReadPageCache::new(store_config.page_cache_capacity, store_config.page_size);
        Ok(Self {
            client: Arc::new(client),
            block_cache,
            page_cache,
            page_flight: SingleFlight::new(),
            read_flight: Arc::new(SingleFlight::new()),
            range_prefetch_limit: Arc::new(Semaphore::new(8)),
            config: store_config,
            bandwidth: BandwidthLimiter::unlimited(),
            object_metrics: Arc::new(ObjectStoreMetrics::default()),
        })
    }

    /// Set the bandwidth limiter for this store
    #[allow(unused)]
    pub fn with_bandwidth(mut self, limiter: BandwidthLimiter) -> Self {
        self.bandwidth = limiter;
        self
    }

    fn key_for(key: BlockKey) -> String {
        let (chunk_id, block_index) = key;
        format!("chunks/{chunk_id}/{block_index}")
    }

    async fn populate_write_cache_after_upload(&self, key: String, data: Bytes) {
        // Make freshly uploaded data immediately visible through a protected
        // read-after-write tier. The normal hot cache uses admission control,
        // and older read-hot blocks can otherwise reject newly written data
        // before it has built read frequency.
        self.block_cache
            .insert_recent_write_hot(&key, data.clone())
            .await;

        if !self.config.persist_write_cache_after_upload {
            return;
        }

        // Persist to disk if a write permit is available. Skipping under
        // extreme I/O pressure avoids queuing hundreds of background tasks
        // that compete with foreground uploads.
        let cache = self.block_cache.clone();
        if let Some(permit) = cache.try_disk_store_permit(&key) {
            tokio::spawn(async move {
                let _ = cache.store_to_disk_with_permit(&key, data, permit).await;
            });
        }
    }

    async fn populate_page_cache_from_block(&self, key: BlockKey, block_data: &[u8]) {
        let page_size = self.page_cache.page_size();
        for (page_idx, page) in block_data.chunks(page_size).enumerate() {
            self.page_cache
                .insert(
                    (key.0, key.1, page_idx as u32),
                    Bytes::copy_from_slice(page),
                )
                .await;
        }
    }

    async fn try_promote_page_cache_to_block_cache(&self, key: BlockKey) -> bool {
        let page_size = self.page_cache.page_size();
        let page_count = self.config.block_size.div_ceil(page_size);
        let mut block = Vec::with_capacity(self.config.block_size);

        for page_idx in 0..page_count {
            let Some(page) = self.page_cache.get(&(key.0, key.1, page_idx as u32)).await else {
                return false;
            };
            block.extend_from_slice(page.as_ref());
        }
        block.truncate(self.config.block_size);

        self.block_cache
            .insert_opportunistic(Self::key_for(key), bytes::Bytes::from(block))
            .await;
        true
    }

    fn prefetch_full_block_background(&self, key: BlockKey, key_str: String) {
        self.object_metrics.record_read_background_prefetch();
        let cache = self.block_cache.clone();
        let client = self.client.clone();
        let read_flight = self.read_flight.clone();
        let prefetch_limit = self.range_prefetch_limit.clone();
        let bandwidth = self.bandwidth.clone();
        let compression = self.config.compression;
        let block_size = self.config.block_size;
        let object_metrics = self.object_metrics.clone();

        tokio::spawn(async move {
            if cache.get(&key_str).await.is_some() {
                return;
            }

            let Ok(_permit) = prefetch_limit.try_acquire_owned() else {
                object_metrics.record_read_background_prefetch_dropped();
                return;
            };
            if cache.get(&key_str).await.is_some() {
                return;
            }

            let block_data = read_flight
                .execute(key, || async move {
                    bandwidth.acquire_download(block_size).await;
                    let started = Instant::now();
                    let raw = client.get_object(&key_str).await.map_err(|e| {
                        anyhow::anyhow!("object store get failed: {key_str}, {e:?}")
                    })?;
                    let raw_bytes = match raw {
                        Some(data) => {
                            object_metrics.record_get(data.len() as u64, started.elapsed());
                            data
                        }
                        None => {
                            object_metrics.record_get(0, started.elapsed());
                            return Ok(Bytes::new());
                        }
                    };
                    let decompressed = if !matches!(compression, Compression::None) {
                        decompress(&raw_bytes)
                            .map_err(|e| anyhow::anyhow!("block decompression failed: {e}"))?
                    } else {
                        raw_bytes
                    };
                    Ok::<_, anyhow::Error>(Bytes::from(decompressed))
                })
                .await;

            match block_data {
                Ok(block_data) => {
                    cache
                        .insert_opportunistic(Self::key_for(key), (*block_data).clone())
                        .await;
                }
                Err(err) => {
                    tracing::debug!(error = %err, ?key, "background block prefetch failed");
                }
            }
        });
    }
}

impl<B: ObjectBackend + Send + Sync + 'static> ObjectBlockStore<B> {
    fn concat_bytes(parts: &[Bytes]) -> Bytes {
        match parts {
            [] => Bytes::new(),
            [single] => single.clone(),
            _ => {
                let total_len = parts.iter().map(|part| part.len()).sum::<usize>();
                let mut out = Vec::with_capacity(total_len);
                for part in parts {
                    out.extend_from_slice(part);
                }
                Bytes::from(out)
            }
        }
    }

    async fn write_fresh_vectored_inner(
        &self,
        key: BlockKey,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> anyhow::Result<u64> {
        let prepare_started = Instant::now();
        let key_str = Self::key_for(key);
        let total_len = chunks.iter().map(|c| c.len()).sum::<usize>();
        if total_len == 0 {
            return Ok(0);
        }

        let offset_usize = offset.as_usize();
        let mut parts: Vec<Bytes> = Vec::new();
        if offset_usize > 0 {
            parts.extend(make_zero_bytes(offset_usize));
        }
        parts.extend(chunks);

        let cache_block = if matches!(self.config.compression, Compression::None) {
            let full_block = Self::concat_bytes(&parts);
            let upload_len = full_block.len();
            self.bandwidth.acquire_upload(upload_len).await;
            self.object_metrics
                .record_put_prepare(prepare_started.elapsed());
            let started = Instant::now();
            self.client
                .put_object_vectored(&key_str, vec![full_block.clone()])
                .await
                .map_err(|e| anyhow::anyhow!("object store put failed: {key_str}, {e:?}"))?;
            self.object_metrics
                .record_put(upload_len as u64, started.elapsed());
            full_block
        } else {
            let full_block = Self::concat_bytes(&parts);
            let compressed = compress(&full_block, self.config.compression);
            let upload_bytes = match compressed {
                std::borrow::Cow::Borrowed(_) => full_block.clone(),
                std::borrow::Cow::Owned(v) => Bytes::from(v),
            };
            self.bandwidth.acquire_upload(upload_bytes.len()).await;
            let upload_len = upload_bytes.len() as u64;
            self.object_metrics
                .record_put_prepare(prepare_started.elapsed());
            let started = Instant::now();
            self.client
                .put_object_vectored(&key_str, vec![upload_bytes])
                .await
                .map_err(|e| anyhow::anyhow!("object store put failed: {key_str}, {e:?}"))?;
            self.object_metrics
                .record_put(upload_len, started.elapsed());
            full_block
        };

        if self.config.populate_write_cache_after_upload
            && cache_block.len() <= self.config.block_size
        {
            let cache_started = Instant::now();
            self.populate_write_cache_after_upload(key_str, cache_block)
                .await;
            self.object_metrics
                .record_put_cache(cache_started.elapsed());
        }

        Ok(total_len as u64)
    }
}

#[async_trait]
impl<B: ObjectBackend + Send + Sync + 'static> BlockStore for ObjectBlockStore<B> {
    #[tracing::instrument(name = "ObjectBlockStore.write_fresh_vectored", level = "trace", skip(self, chunks), fields(key = ?key, offset, chunk_count = chunks.len()))]
    async fn write_fresh_vectored(
        &self,
        key: BlockKey,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> anyhow::Result<u64> {
        self.write_fresh_vectored_inner(key, offset, chunks).await
    }

    async fn write_fresh_range(
        &self,
        key: BlockKey,
        offset: u64,
        data: &[u8],
    ) -> anyhow::Result<u64> {
        let chunks = vec![Bytes::copy_from_slice(data)];
        self.write_fresh_vectored_inner(key, offset, chunks)
            .await
            .map(|_| data.len() as u64)
    }

    #[tracing::instrument(
        name = "ObjectBlockStore.read_range",
        level = "trace",
        skip(self, buf),
        fields(key = ?key, offset, len = buf.len(), read_len = tracing::field::Empty, strategy = tracing::field::Empty)
    )]
    // Caller is responsible for zero-filling buf; this method only overwrites existing bytes.
    async fn read_range(&self, key: BlockKey, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        let len = buf.len();
        let key_str = Self::key_for(key);

        // Try cache first — blocks are immutable once committed, so a cache
        // hit is always valid regardless of read size or offset.
        if let Some(cached) = self.block_cache.get(&key_str).await {
            tracing::trace!(key = %key_str, len = cached.len(), "block_cache HIT");
            tracing::Span::current().record("strategy", "cache_hit");
            self.object_metrics.record_read_block_cache_hit();
            let offset_usize = offset as usize;
            let end = (offset_usize + len).min(cached.len());
            if offset_usize < cached.len() {
                let copy_len = end - offset_usize;
                buf[..copy_len].copy_from_slice(&cached[offset_usize..end]);
                tracing::Span::current().record("read_len", copy_len);
            }
            return Ok(());
        }

        let range_size_threshold = self.config.range_size_threshold();

        if matches!(self.config.compression, Compression::None)
            && offset > 0
            && len <= range_size_threshold
        {
            if let Some(block_data) = self.read_flight.try_piggyback(&key).await {
                tracing::Span::current().record("strategy", "piggyback_full");
                self.object_metrics.record_read_piggyback_full();
                let block_data = block_data
                    .map_err(|e| anyhow::anyhow!("SingleFlight piggyback read failed: {e}"))?;

                let offset_usize = offset as usize;
                let end = offset_usize + len;
                let mut copy_len = 0;
                if offset_usize < block_data.len() {
                    let copy_end = end.min(block_data.len());
                    copy_len = copy_end - offset_usize;
                    buf[..copy_len].copy_from_slice(&block_data.as_ref()[offset_usize..copy_end]);
                }
                tracing::Span::current().record("read_len", copy_len);
                return Ok(());
            }

            // Small range read — serve via page-granularity cache so that
            // repeated small reads within the same 64KB page avoid a network
            // round-trip.
            let page_size = self.page_cache.page_size();
            let start_page = offset as usize / page_size;
            // end_page is inclusive
            let end_page = (offset as usize + len - 1) / page_size;

            let client = &self.client;
            let page_cache = &self.page_cache;
            let object_metrics = self.object_metrics.clone();
            let mut pos: usize = 0;
            let mut total_read: usize = 0;
            let mut range_missed = false;

            for page_idx in start_page..=end_page {
                let page_start = page_idx * page_size;
                let page_end = (page_start + page_size).min(self.config.block_size);

                let cache_key: PageKey = (key.0, key.1, page_idx as u32);

                let page_data = if let Some(cached) = page_cache.get(&cache_key).await {
                    tracing::Span::current().record("strategy", "page_cache_hit");
                    cached
                } else {
                    tracing::Span::current().record("strategy", "page_cache_miss");
                    range_missed = true;
                    let range_offset = page_start as u64;
                    let range_len = page_end - page_start;
                    let page_key_str = key_str.clone();
                    let page_object_metrics = object_metrics.clone();
                    let page = self
                        .page_flight
                        .execute(cache_key, || async move {
                            if let Some(cached) = page_cache.get(&cache_key).await {
                                return Ok::<_, anyhow::Error>(cached);
                            }

                            let mut page_buf = vec![0u8; range_len];
                            self.bandwidth.acquire_download(range_len).await;
                            let started = Instant::now();
                            let read_len = client
                                .get_object_range(&page_key_str, range_offset, &mut page_buf)
                                .await
                                .map_err(|e| {
                                    anyhow::anyhow!(
                                        "object store range read failed: {page_key_str}, {e:?}"
                                    )
                                })?;
                            page_object_metrics.record_get(read_len as u64, started.elapsed());
                            page_buf.truncate(read_len);
                            let page_bytes = Bytes::from(page_buf);
                            page_cache.insert(cache_key, page_bytes.clone()).await;
                            Ok(page_bytes)
                        })
                        .await
                        .map_err(|e| anyhow::anyhow!("SingleFlight page read failed: {e}"))?;
                    page.as_ref().clone()
                };

                // Determine the byte range within this page that the caller needs
                let copy_start = if page_idx == start_page {
                    offset as usize - page_start
                } else {
                    0
                };
                let copy_end = if page_idx == end_page {
                    (offset as usize + len).saturating_sub(page_start)
                } else {
                    page_data.len()
                };
                let copy_end = copy_end.min(page_data.len());
                if copy_end > copy_start {
                    let copy_len = copy_end - copy_start;
                    buf[pos..pos + copy_len].copy_from_slice(&page_data[copy_start..copy_end]);
                    pos += copy_len;
                    total_read += copy_len;
                }
            }

            tracing::Span::current().record("read_len", total_read);
            if range_missed {
                self.object_metrics.record_read_range_get();
                self.object_metrics.record_read_page_cache_miss();
            } else {
                self.object_metrics.record_read_page_cache_hit();
            }
            if range_missed
                && total_read > 0
                && self.config.range_background_prefetch
                && !self.try_promote_page_cache_to_block_cache(key).await
            {
                self.prefetch_full_block_background(key, key_str);
            }
            return Ok(());
        }

        // Large read — fetch full block via SingleFlight, then cache it.
        tracing::Span::current().record("strategy", "coalesced_full");
        let client = &self.client;
        let compression = self.config.compression;
        let object_metrics = self.object_metrics.clone();

        let block_data =
            self.read_flight
                .execute(key, || async move {
                    let key_str = Self::key_for(key);
                    self.bandwidth
                        .acquire_download(self.config.block_size)
                        .await;
                    let started = Instant::now();
                    let raw = client.get_object(&key_str).await.map_err(|e| {
                        anyhow::anyhow!("object store get failed: {key_str}, {e:?}")
                    })?;
                    object_metrics.record_read_full_get();
                    let raw_bytes = match raw {
                        Some(data) => {
                            object_metrics.record_get(data.len() as u64, started.elapsed());
                            data
                        }
                        None => {
                            object_metrics.record_get(0, started.elapsed());
                            return Ok(Bytes::new());
                        }
                    };
                    // Decompress if compression is enabled (auto-detects from header)
                    let decompressed = if !matches!(compression, Compression::None) {
                        decompress(&raw_bytes)
                            .map_err(|e| anyhow::anyhow!("block decompression failed: {e}"))?
                    } else {
                        raw_bytes
                    };
                    Ok::<_, anyhow::Error>(Bytes::from(decompressed))
                })
                .await
                .map_err(|e| anyhow::anyhow!("SingleFlight read failed: {e}"))?;

        // Copy data to caller's buffer first — minimize read latency.
        let offset_usize = offset as usize;
        let end = offset_usize + len;
        let mut copy_len = 0;
        if offset_usize < block_data.len() {
            let copy_end = end.min(block_data.len());
            copy_len = copy_end - offset_usize;
            buf[..copy_len].copy_from_slice(&block_data.as_ref()[offset_usize..copy_end]);
        }
        tracing::Span::current().record("read_len", copy_len);

        if !matches!(compression, Compression::None) {
            self.populate_page_cache_from_block(key, block_data.as_ref())
                .await;
        }

        // Populate caches after serving the read — the hot cache insert is
        // fast (in-memory) so we await it to ensure subsequent reads hit.
        // Disk persistence is spawned in the background by insert_opportunistic.
        self.block_cache
            .insert_opportunistic(key_str.clone(), (*block_data).clone())
            .await;

        Ok(())
    }

    async fn delete_range(&self, key: BlockKey, block_count: u64) -> anyhow::Result<()> {
        let (chunk_id, block_index) = key;
        let start = block_index;
        let end = start + block_count.as_u32();
        for i in start..end {
            let key_str = Self::key_for((chunk_id, i));
            self.client
                .delete_object(&key_str)
                .await
                .map_err(|e| anyhow::anyhow!("object store delete failed: {key_str}, {e:?}"))?;
            self.object_metrics.record_delete();
        }
        Ok(())
    }

    #[allow(dead_code)]
    async fn cache_block(&self, key: BlockKey, data: &[u8]) -> anyhow::Result<()> {
        let key_str = Self::key_for(key);
        let _ = self.block_cache.insert(&key_str, &data.to_vec()).await;
        Ok(())
    }

    fn cache_counters(&self) -> (Option<Arc<AtomicU64>>, Option<Arc<AtomicU64>>) {
        (
            Some(self.block_cache.cache_hits.clone()),
            Some(self.block_cache.cache_misses.clone()),
        )
    }

    fn object_store_metrics(&self) -> Option<Arc<ObjectStoreMetrics>> {
        Some(self.object_metrics.clone())
    }
}

/// Convenience alias: BlockStore backed by the real S3 backend.
#[allow(dead_code)]
pub type S3BlockStore = ObjectBlockStore<crate::cadapter::s3::S3Backend>;
/// Convenience alias: BlockStore backed by the LocalFs mock backend.
#[allow(dead_code)]
pub type LocalFsBlockStore = ObjectBlockStore<crate::cadapter::localfs::LocalFsBackend>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::client::ObjectClient;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::chunk::layout::ChunkLayout;

    #[tokio::test]
    async fn test_localfs_block_store_put_get() {
        let tmp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(tmp.path()));
        let store = ObjectBlockStore::new_async(client).await.unwrap();
        let layout = ChunkLayout::default();

        let data = vec![7u8; layout.block_size as usize / 2];
        store
            .write_fresh_range((42, 3), (layout.block_size / 4) as u64, &data)
            .await
            .unwrap();

        let mut out = vec![0u8; data.len()];
        store
            .read_range((42, 3), (layout.block_size / 4) as u64, &mut out)
            .await
            .unwrap();
        assert_eq!(out, data);
    }

    #[tokio::test]
    async fn test_cache_effectiveness() -> io::Result<()> {
        let tmp = tempfile::tempdir()?;
        let client = ObjectClient::new(LocalFsBackend::new(tmp.path()));
        let store = ObjectBlockStore::new_async(client)
            .await
            .map_err(io::Error::other)?;
        let layout = ChunkLayout::default();
        let data = vec![7u8; layout.block_size as usize / 2];
        store
            .write_fresh_range((42, 3), (layout.block_size / 4) as u64, &data)
            .await
            .unwrap();
        // First read should miss the cache.
        let mut data1 = vec![0u8; data.len()];
        store
            .read_range((42, 3), (layout.block_size / 4) as u64, &mut data1)
            .await
            .unwrap();

        // Second read of the same data should hit the cache.
        let mut data2 = vec![0u8; data.len()];
        store
            .read_range((42, 3), (layout.block_size / 4) as u64, &mut data2)
            .await
            .unwrap();
        assert_eq!(data1, data2);

        Ok(())
    }

    #[tokio::test]
    async fn test_intelligent_read_strategy() -> Result<(), Box<dyn std::error::Error>> {
        use crate::cadapter::client::{ObjectBackend, ObjectClient};
        use async_trait::async_trait;
        use futures::future;
        use std::{
            collections::HashMap,
            sync::{Arc, Mutex},
        };
        use tokio::time::{Duration, sleep};

        #[derive(Debug, Clone)]
        struct MockStats {
            get_object_calls: usize,
            get_object_range_calls: usize,
        }

        #[derive(Clone)]
        struct MockBackend {
            data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
            stats: Arc<Mutex<MockStats>>,
        }

        impl MockBackend {
            fn new() -> Self {
                let mut data = HashMap::new();
                // Create a 4MB block with known pattern
                let block_data: Vec<u8> = (0..4_194_304).map(|i| (i % 256) as u8).collect();
                data.insert("chunks/42/3".to_string(), block_data.clone());
                data.insert("chunks/42/4".to_string(), block_data.clone());
                data.insert("chunks/42/5".to_string(), block_data);

                Self {
                    data: Arc::new(Mutex::new(data)),
                    stats: Arc::new(Mutex::new(MockStats {
                        get_object_calls: 0,
                        get_object_range_calls: 0,
                    })),
                }
            }

            fn get_stats(&self) -> MockStats {
                self.stats.lock().unwrap().clone()
            }

            fn reset_stats(&self) {
                let mut stats = self.stats.lock().unwrap();
                stats.get_object_calls = 0;
                stats.get_object_range_calls = 0;
            }
        }

        #[async_trait]
        impl ObjectBackend for MockBackend {
            async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
                self.data
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), data.to_vec());
                Ok(())
            }

            async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                // Simulate some latency so SingleFlight can coalesce concurrent requests
                sleep(Duration::from_millis(10)).await;
                self.stats.lock().unwrap().get_object_calls += 1;
                Ok(self.data.lock().unwrap().get(key).cloned())
            }

            async fn get_object_range(
                &self,
                key: &str,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<usize> {
                self.stats.lock().unwrap().get_object_range_calls += 1;
                if let Some(data) = self.data.lock().unwrap().get(key) {
                    let offset = offset as usize;
                    let end = (offset + buf.len()).min(data.len());
                    if offset < data.len() {
                        let copy_len = end - offset;
                        buf[..copy_len].copy_from_slice(&data[offset..end]);
                        Ok(copy_len)
                    } else {
                        Ok(0)
                    }
                } else {
                    Ok(0)
                }
            }

            async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
                Ok("test_etag".to_string())
            }

            async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
                self.data.lock().unwrap().remove(key);
                Ok(())
            }
        }

        // Test small range uses direct read
        let backend = MockBackend::new();
        let client = ObjectClient::new(backend.clone());
        let config = BlockStoreConfig {
            block_size: 4 * 1024 * 1024,
            range_read_threshold: 0.25, // 1MB threshold
            compression: Compression::None,
            ..Default::default()
        };
        let cache_dir = tempfile::tempdir()?;
        let store = Arc::new(
            ObjectBlockStore::new_with_configs_async(
                client,
                ChunksCacheConfig::with_budgets(
                    16 * 1024 * 1024,
                    16 * 1024 * 1024,
                    cache_dir.path().to_path_buf(),
                ),
                config,
            )
            .await?,
        );

        backend.reset_stats();

        // Small read at block start should load the full block. JuiceFS only
        // uses loadRange() when offset > 0, so sequential reads from the start
        // warm the block cache immediately instead of fragmenting into pages.
        let mut small_buf = vec![0u8; 512 * 1024];
        store.read_range((42, 3), 0, &mut small_buf).await?;

        let stats = backend.get_stats();
        assert_eq!(
            stats.get_object_calls, 1,
            "Small read at block start should use full block read"
        );
        assert_eq!(
            stats.get_object_range_calls, 0,
            "Small read at block start should not use range reads"
        );

        // Same read again should hit the full block cache.
        backend.reset_stats();
        let mut small_buf2 = vec![0u8; 512 * 1024];
        store.read_range((42, 3), 0, &mut small_buf2).await?;
        assert_eq!(small_buf, small_buf2);

        let stats = backend.get_stats();
        assert_eq!(
            stats.get_object_range_calls, 0,
            "Re-read of same range should hit block cache (no new range reads)"
        );
        assert_eq!(
            stats.get_object_calls, 0,
            "Re-read should not issue another full block read"
        );

        // Non-zero small read still uses the page range path.
        backend.reset_stats();
        let mut range_buf = vec![0u8; 512 * 1024];
        store.read_range((42, 4), 64 * 1024, &mut range_buf).await?;

        let stats = backend.get_stats();
        let snapshot = store
            .object_store_metrics()
            .expect("ObjectBlockStore exposes object metrics")
            .snapshot();
        assert_eq!(
            snapshot.read_range_gets, 1,
            "Non-zero small read should record one range-read strategy event"
        );
        assert_eq!(
            snapshot.read_page_cache_misses, 1,
            "Non-zero small read should record one page-cache miss event per request"
        );
        assert_eq!(
            snapshot.read_background_prefetches, 1,
            "Range miss should schedule one background full-block prefetch"
        );
        assert_eq!(
            stats.get_object_range_calls, 8,
            "Non-zero 512KB read should fetch 8 pages (8 x 64KB range reads)"
        );
        for _ in 0..100 {
            if backend.get_stats().get_object_calls >= 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            backend.get_stats().get_object_calls,
            1,
            "Non-zero small read should trigger one background full-block prefetch"
        );

        let disabled_backend = MockBackend::new();
        let disabled_client = ObjectClient::new(disabled_backend.clone());
        let disabled_config = BlockStoreConfig {
            block_size: 4 * 1024 * 1024,
            range_read_threshold: 0.25,
            compression: Compression::None,
            range_background_prefetch: false,
            ..Default::default()
        };
        let disabled_cache_dir = tempfile::tempdir()?;
        let disabled_store = ObjectBlockStore::new_with_configs_async(
            disabled_client,
            ChunksCacheConfig::with_budgets(
                16 * 1024 * 1024,
                16 * 1024 * 1024,
                disabled_cache_dir.path().to_path_buf(),
            ),
            disabled_config,
        )
        .await?;

        let mut disabled_range_buf = vec![0u8; 512 * 1024];
        disabled_store
            .read_range((42, 4), 64 * 1024, &mut disabled_range_buf)
            .await?;
        sleep(Duration::from_millis(50)).await;

        let disabled_stats = disabled_backend.get_stats();
        let disabled_snapshot = disabled_store
            .object_store_metrics()
            .expect("ObjectBlockStore exposes object metrics")
            .snapshot();
        assert_eq!(
            disabled_stats.get_object_range_calls, 8,
            "Disabled background prefetch should still serve the request via page ranges"
        );
        assert_eq!(
            disabled_stats.get_object_calls, 0,
            "Disabled background prefetch should not issue a full-object GET"
        );
        assert_eq!(
            disabled_snapshot.read_background_prefetches, 0,
            "Disabled background prefetch should not schedule prefetch work"
        );

        backend.reset_stats();

        // Large read (2MB > 1MB threshold) — should use full block read.
        let mut large_buf = vec![0u8; 2 * 1024 * 1024];
        store.read_range((42, 5), 0, &mut large_buf).await?;

        let stats = backend.get_stats();
        assert_eq!(stats.get_object_calls, 1, "Large read should use full read");
        assert_eq!(
            stats.get_object_range_calls, 0,
            "Large read should not use range read"
        );

        // Concurrent large reads for a DIFFERENT (uncached) block should
        // coalesce to a single backend call via SingleFlight.
        backend.reset_stats();
        let handles: Vec<_> = (0..5)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 2 * 1024 * 1024];
                    store.read_range((99, 1), 0, &mut buf).await
                })
            })
            .collect();

        future::try_join_all(handles).await?;

        let stats = backend.get_stats();
        assert_eq!(
            stats.get_object_calls, 1,
            "Concurrent reads should coalesce to 1 call"
        );
        assert_eq!(
            stats.get_object_range_calls, 0,
            "Coalesced path should not fall back to range reads",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_compressed_small_read_uses_full_object() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::cadapter::client::{ObjectBackend, ObjectClient};
        use crate::chunk::compress::{Compression, compress};
        use async_trait::async_trait;
        use std::{
            collections::HashMap,
            sync::{Arc, Mutex},
        };

        #[derive(Clone, Default)]
        struct MockBackend {
            data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
            get_object_calls: Arc<Mutex<usize>>,
            get_object_range_calls: Arc<Mutex<usize>>,
        }

        #[async_trait]
        impl ObjectBackend for MockBackend {
            async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
                self.data
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), data.to_vec());
                Ok(())
            }

            async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                *self.get_object_calls.lock().unwrap() += 1;
                Ok(self.data.lock().unwrap().get(key).cloned())
            }

            async fn get_object_range(
                &self,
                key: &str,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<usize> {
                *self.get_object_range_calls.lock().unwrap() += 1;
                if let Some(data) = self.data.lock().unwrap().get(key) {
                    let offset = offset as usize;
                    let end = (offset + buf.len()).min(data.len());
                    if offset < data.len() {
                        let copy_len = end - offset;
                        buf[..copy_len].copy_from_slice(&data[offset..end]);
                        return Ok(copy_len);
                    }
                }
                Ok(0)
            }

            async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
                Ok("test_etag".to_string())
            }

            async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
                self.data.lock().unwrap().remove(key);
                Ok(())
            }
        }

        let backend = MockBackend::default();
        let raw = vec![0u8; 4 * 1024 * 1024];
        let stored = compress(&raw, Compression::Lz4).into_owned();
        assert!(stored.len() < raw.len() / 8);
        backend
            .data
            .lock()
            .unwrap()
            .insert("chunks/7/0".to_string(), stored);

        let config = BlockStoreConfig {
            block_size: 4 * 1024 * 1024,
            range_read_threshold: 0.25,
            compression: Compression::Lz4,
            ..Default::default()
        };
        let cache_dir = tempfile::tempdir()?;
        let store = ObjectBlockStore::new_with_configs_async(
            ObjectClient::new(backend.clone()),
            ChunksCacheConfig::with_budgets(
                16 * 1024 * 1024,
                16 * 1024 * 1024,
                cache_dir.path().to_path_buf(),
            ),
            config,
        )
        .await?;

        let mut out = vec![1u8; 512 * 1024];
        store.read_range((7, 0), 2 * 1024 * 1024, &mut out).await?;

        assert_eq!(out, vec![0u8; 512 * 1024]);
        assert_eq!(*backend.get_object_calls.lock().unwrap(), 1);
        assert_eq!(*backend.get_object_range_calls.lock().unwrap(), 0);
        assert!(
            store.page_cache.get(&(7, 0, 32)).await.is_some(),
            "decompressed full-block reads should populate page_cache for future range reads"
        );

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_write_fresh_range_write_cache_population_is_configurable()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::cadapter::client::{ObjectBackend, ObjectClient};
        use async_trait::async_trait;
        use std::{
            collections::HashMap,
            sync::{Arc, Mutex},
        };

        #[derive(Clone, Default)]
        struct MockBackend {
            data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        }

        #[async_trait]
        impl ObjectBackend for MockBackend {
            async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
                self.data
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), data.to_vec());
                Ok(())
            }

            async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                Ok(self.data.lock().unwrap().get(key).cloned())
            }

            async fn get_object_range(
                &self,
                key: &str,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<usize> {
                if let Some(data) = self.data.lock().unwrap().get(key) {
                    let offset = offset as usize;
                    let end = (offset + buf.len()).min(data.len());
                    if offset < data.len() {
                        let copy_len = end - offset;
                        buf[..copy_len].copy_from_slice(&data[offset..end]);
                        return Ok(copy_len);
                    }
                }
                Ok(0)
            }

            async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
                Ok("test_etag".to_string())
            }

            async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
                self.data.lock().unwrap().remove(key);
                Ok(())
            }
        }

        let default_cache_dir = tempfile::tempdir()?;
        let block_size = 128 * 1024;
        let default_store = ObjectBlockStore::new_with_configs_async(
            ObjectClient::new(MockBackend::default()),
            ChunksCacheConfig::with_budgets(
                16 * 1024 * 1024,
                16 * 1024 * 1024,
                default_cache_dir.path().to_path_buf(),
            ),
            BlockStoreConfig {
                block_size,
                compression: Compression::None,
                ..Default::default()
            },
        )
        .await?;

        let data = vec![3u8; block_size / 2];
        default_store.write_fresh_range((122, 0), 0, &data).await?;
        assert_eq!(
            default_store.block_cache.stats().write_hot_entries,
            1,
            "partial-block uploads should populate the recent-write hot tier by default"
        );
        assert!(
            !default_store
                .block_cache
                .is_disk_cached(&"chunks/122/0".to_string())
                .await,
            "default upload-time read cache population should not persist blocks to disk"
        );

        let mut out = vec![0u8; data.len()];
        default_store.read_range((122, 0), 0, &mut out).await?;
        assert_eq!(
            out, data,
            "default upload-time read cache population must preserve read-after-write"
        );

        let large_data = vec![5u8; block_size];
        default_store
            .write_fresh_range((122, 1), 0, &large_data)
            .await?;
        assert_eq!(
            default_store.block_cache.stats().write_hot_entries,
            2,
            "full-block-sized writes should populate the upload-time recent-write hot tier"
        );
        let mut large_out = vec![0u8; large_data.len()];
        default_store
            .read_range((122, 1), 0, &mut large_out)
            .await?;
        assert_eq!(large_out, large_data);

        let disabled_cache_dir = tempfile::tempdir()?;
        let disabled_store = ObjectBlockStore::new_with_configs_async(
            ObjectClient::new(MockBackend::default()),
            ChunksCacheConfig::with_budgets(
                16 * 1024 * 1024,
                16 * 1024 * 1024,
                disabled_cache_dir.path().to_path_buf(),
            ),
            BlockStoreConfig {
                block_size,
                compression: Compression::None,
                populate_write_cache_after_upload: false,
                ..Default::default()
            },
        )
        .await?;

        disabled_store.write_fresh_range((123, 0), 0, &data).await?;

        assert_eq!(
            disabled_store.block_cache.stats().write_hot_entries,
            0,
            "write_fresh_range should skip upload-time read cache population when disabled"
        );

        let cache_dir = tempfile::tempdir()?;
        let store = ObjectBlockStore::new_with_configs_async(
            ObjectClient::new(MockBackend::default()),
            ChunksCacheConfig::with_budgets(
                16 * 1024 * 1024,
                16 * 1024 * 1024,
                cache_dir.path().to_path_buf(),
            ),
            BlockStoreConfig {
                block_size,
                compression: Compression::None,
                populate_write_cache_after_upload: true,
                persist_write_cache_after_upload: true,
                ..Default::default()
            },
        )
        .await?;

        store.write_fresh_range((123, 0), 0, &large_data).await?;

        assert_eq!(
            store.block_cache.stats().write_hot_entries,
            1,
            "write_fresh_range should synchronously populate the recent-write hot tier before returning"
        );
        assert_eq!(
            store.block_cache.get(&"chunks/123/0".to_string()).await,
            Some(large_data.into())
        );
        let disk_key = "chunks/123/0".to_string();
        for _ in 0..20 {
            if store.block_cache.is_disk_cached(&disk_key).await {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Err("explicit disk persistence should populate the local disk read cache".into())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_object_store_metrics_record_backend_ops() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::cadapter::client::{ObjectBackend, ObjectClient};
        use async_trait::async_trait;
        use std::{
            collections::HashMap,
            sync::{Arc, Mutex},
        };

        #[derive(Clone, Default)]
        struct MockBackend {
            data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        }

        #[async_trait]
        impl ObjectBackend for MockBackend {
            async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
                self.data
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), data.to_vec());
                Ok(())
            }

            async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                Ok(self.data.lock().unwrap().get(key).cloned())
            }

            async fn get_object_range(
                &self,
                key: &str,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<usize> {
                if let Some(data) = self.data.lock().unwrap().get(key) {
                    let offset = offset as usize;
                    let end = (offset + buf.len()).min(data.len());
                    if offset < data.len() {
                        let copy_len = end - offset;
                        buf[..copy_len].copy_from_slice(&data[offset..end]);
                        return Ok(copy_len);
                    }
                }
                Ok(0)
            }

            async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
                Ok("test_etag".to_string())
            }

            async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
                self.data.lock().unwrap().remove(key);
                Ok(())
            }
        }

        let backend = MockBackend::default();
        let cache_dir = tempfile::tempdir()?;
        let store = ObjectBlockStore::new_with_configs_async(
            ObjectClient::new(backend.clone()),
            ChunksCacheConfig::with_budgets(
                16 * 1024 * 1024,
                16 * 1024 * 1024,
                cache_dir.path().to_path_buf(),
            ),
            BlockStoreConfig {
                block_size: 64 * 1024,
                page_size: 4 * 1024,
                compression: Compression::None,
                ..Default::default()
            },
        )
        .await?;
        let metrics = store
            .object_store_metrics()
            .expect("ObjectBlockStore exposes object metrics");

        let uploaded = vec![5u8; 8 * 1024];
        store.write_fresh_range((10, 0), 0, &uploaded).await?;
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.put_ops, 1);
        assert_eq!(snapshot.put_bytes, uploaded.len() as u64);
        assert_eq!(snapshot.get_ops, 0);

        let full_block = vec![7u8; 64 * 1024];
        backend
            .data
            .lock()
            .unwrap()
            .insert("chunks/11/0".to_string(), full_block.clone());
        let mut out = vec![0u8; 4096];
        store.read_range((11, 0), 0, &mut out).await?;
        assert_eq!(out, vec![7u8; 4096]);

        store.delete_range((11, 0), 1).await?;

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.get_ops, 1);
        assert_eq!(snapshot.get_bytes, full_block.len() as u64);
        assert_eq!(snapshot.put_ops, 1);
        assert_eq!(snapshot.del_ops, 1);
        assert_eq!(snapshot.read_full_gets, 1);
        assert_eq!(snapshot.read_block_cache_hits, 0);
        assert_eq!(snapshot.read_range_gets, 0);

        let mut cached_out = vec![0u8; 4096];
        store.read_range((11, 0), 0, &mut cached_out).await?;
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.read_block_cache_hits, 1);
        assert_eq!(snapshot.get_ops, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_complete_page_cache_promotes_to_block_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::cadapter::client::{ObjectBackend, ObjectClient};
        use async_trait::async_trait;

        #[derive(Clone, Default)]
        struct MockBackend;

        #[async_trait]
        impl ObjectBackend for MockBackend {
            async fn put_object(&self, _key: &str, _data: &[u8]) -> anyhow::Result<()> {
                Ok(())
            }

            async fn get_object(&self, _key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                Ok(None)
            }

            async fn get_object_range(
                &self,
                _key: &str,
                _offset: u64,
                _buf: &mut [u8],
            ) -> anyhow::Result<usize> {
                Ok(0)
            }

            async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
                Ok("test_etag".to_string())
            }

            async fn delete_object(&self, _key: &str) -> anyhow::Result<()> {
                Ok(())
            }
        }

        let cache_dir = tempfile::tempdir()?;
        let store = ObjectBlockStore::new_with_configs_async(
            ObjectClient::new(MockBackend),
            ChunksCacheConfig::with_budgets(
                16 * 1024 * 1024,
                16 * 1024 * 1024,
                cache_dir.path().to_path_buf(),
            ),
            BlockStoreConfig {
                block_size: 128 * 1024,
                page_size: 64 * 1024,
                compression: Compression::None,
                ..Default::default()
            },
        )
        .await?;

        let first = Bytes::from(vec![1u8; 64 * 1024]);
        let second = Bytes::from(vec![2u8; 64 * 1024]);
        store.page_cache.insert((200, 0, 0), first).await;
        store.page_cache.insert((200, 0, 1), second).await;

        assert!(store.try_promote_page_cache_to_block_cache((200, 0)).await);

        let cached = store
            .block_cache
            .get(&"chunks/200/0".to_string())
            .await
            .expect("complete page cache should promote to full block cache");
        assert_eq!(cached[..64 * 1024], vec![1u8; 64 * 1024]);
        assert_eq!(cached[64 * 1024..], vec![2u8; 64 * 1024]);

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_small_reads_coalesce_same_page_miss()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::cadapter::client::{ObjectBackend, ObjectClient};
        use async_trait::async_trait;
        use futures::future;
        use std::{
            collections::HashMap,
            sync::{Arc, Mutex},
        };
        use tokio::time::{Duration, sleep};

        #[derive(Clone, Default)]
        struct MockBackend {
            data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
            get_object_calls: Arc<Mutex<usize>>,
            get_object_range_calls: Arc<Mutex<usize>>,
        }

        #[async_trait]
        impl ObjectBackend for MockBackend {
            async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
                self.data
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), data.to_vec());
                Ok(())
            }

            async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                *self.get_object_calls.lock().unwrap() += 1;
                Ok(self.data.lock().unwrap().get(key).cloned())
            }

            async fn get_object_range(
                &self,
                key: &str,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<usize> {
                sleep(Duration::from_millis(25)).await;
                *self.get_object_range_calls.lock().unwrap() += 1;
                if let Some(data) = self.data.lock().unwrap().get(key) {
                    let offset = offset as usize;
                    let end = (offset + buf.len()).min(data.len());
                    if offset < data.len() {
                        let copy_len = end - offset;
                        buf[..copy_len].copy_from_slice(&data[offset..end]);
                        return Ok(copy_len);
                    }
                }
                Ok(0)
            }

            async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
                Ok("test_etag".to_string())
            }

            async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
                self.data.lock().unwrap().remove(key);
                Ok(())
            }
        }

        let backend = MockBackend::default();
        let block: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        backend
            .data
            .lock()
            .unwrap()
            .insert("chunks/55/0".to_string(), block);

        let config = BlockStoreConfig {
            block_size: 4 * 1024 * 1024,
            range_read_threshold: 0.25,
            compression: Compression::None,
            ..Default::default()
        };
        let cache_dir = tempfile::tempdir()?;
        let store = Arc::new(
            ObjectBlockStore::new_with_configs_async(
                ObjectClient::new(backend.clone()),
                ChunksCacheConfig::with_budgets(
                    16 * 1024 * 1024,
                    16 * 1024 * 1024,
                    cache_dir.path().to_path_buf(),
                ),
                config,
            )
            .await?,
        );

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    store.read_range((55, 0), 1024, &mut buf).await?;
                    anyhow::Ok(buf)
                })
            })
            .collect();

        let results = future::try_join_all(handles).await?;
        for result in results {
            assert_eq!(
                result?,
                (1024..1024 + 4096)
                    .map(|i| (i % 251) as u8)
                    .collect::<Vec<_>>()
            );
        }

        assert_eq!(
            *backend.get_object_range_calls.lock().unwrap(),
            1,
            "concurrent small reads for one page should share one range GET"
        );

        for _ in 0..100 {
            if *backend.get_object_calls.lock().unwrap() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            *backend.get_object_calls.lock().unwrap() <= 8,
            "concurrent small range misses should be bounded by prefetch concurrency limit"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_small_read_piggybacks_in_flight_full_block_read()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::cadapter::client::{ObjectBackend, ObjectClient};
        use async_trait::async_trait;
        use futures::future;
        use std::{
            collections::HashMap,
            sync::{Arc, Mutex},
        };
        use tokio::{
            sync::Notify,
            time::{Duration, sleep},
        };

        #[derive(Clone)]
        struct MockBackend {
            data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
            get_object_calls: Arc<Mutex<usize>>,
            get_object_range_calls: Arc<Mutex<usize>>,
            full_read_started: Arc<Notify>,
            release_full_read: Arc<Notify>,
        }

        impl Default for MockBackend {
            fn default() -> Self {
                Self {
                    data: Arc::new(Mutex::new(HashMap::new())),
                    get_object_calls: Arc::new(Mutex::new(0)),
                    get_object_range_calls: Arc::new(Mutex::new(0)),
                    full_read_started: Arc::new(Notify::new()),
                    release_full_read: Arc::new(Notify::new()),
                }
            }
        }

        #[async_trait]
        impl ObjectBackend for MockBackend {
            async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
                self.data
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), data.to_vec());
                Ok(())
            }

            async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                *self.get_object_calls.lock().unwrap() += 1;
                self.full_read_started.notify_one();
                self.release_full_read.notified().await;
                Ok(self.data.lock().unwrap().get(key).cloned())
            }

            async fn get_object_range(
                &self,
                key: &str,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<usize> {
                *self.get_object_range_calls.lock().unwrap() += 1;
                if let Some(data) = self.data.lock().unwrap().get(key) {
                    let offset = offset as usize;
                    let end = (offset + buf.len()).min(data.len());
                    if offset < data.len() {
                        let copy_len = end - offset;
                        buf[..copy_len].copy_from_slice(&data[offset..end]);
                        return Ok(copy_len);
                    }
                }
                Ok(0)
            }

            async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
                Ok("test_etag".to_string())
            }

            async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
                self.data.lock().unwrap().remove(key);
                Ok(())
            }
        }

        let backend = MockBackend::default();
        let block: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        backend
            .data
            .lock()
            .unwrap()
            .insert("chunks/77/0".to_string(), block);

        let config = BlockStoreConfig {
            block_size: 4 * 1024 * 1024,
            range_read_threshold: 0.25,
            compression: Compression::None,
            ..Default::default()
        };
        let cache_dir = tempfile::tempdir()?;
        let store = Arc::new(
            ObjectBlockStore::new_with_configs_async(
                ObjectClient::new(backend.clone()),
                ChunksCacheConfig::with_budgets(
                    16 * 1024 * 1024,
                    16 * 1024 * 1024,
                    cache_dir.path().to_path_buf(),
                ),
                config,
            )
            .await?,
        );

        let large_store = store.clone();
        let large_read = tokio::spawn(async move {
            let mut buf = vec![0u8; 2 * 1024 * 1024];
            large_store.read_range((77, 0), 0, &mut buf).await?;
            anyhow::Ok(buf)
        });

        backend.full_read_started.notified().await;

        let small_store = store.clone();
        let small_read = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            small_store.read_range((77, 0), 1024, &mut buf).await?;
            anyhow::Ok(buf)
        });

        sleep(Duration::from_millis(20)).await;
        backend.release_full_read.notify_waiters();

        let large_buf = large_read.await??;
        let small_buf = small_read.await??;

        assert_eq!(
            large_buf[..4096],
            (0..4096).map(|i| (i % 251) as u8).collect::<Vec<_>>()
        );
        assert_eq!(
            small_buf,
            (1024..1024 + 4096)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>()
        );
        assert_eq!(*backend.get_object_calls.lock().unwrap(), 1);
        assert_eq!(
            *backend.get_object_range_calls.lock().unwrap(),
            0,
            "small read should join the in-flight full-block read instead of issuing range GET"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_small_range_read_prefetches_full_block_in_background()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::cadapter::client::{ObjectBackend, ObjectClient};
        use async_trait::async_trait;
        use std::{
            collections::HashMap,
            sync::{Arc, Mutex},
        };
        use tokio::time::Duration;

        #[derive(Clone, Default)]
        struct MockBackend {
            data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
            get_object_calls: Arc<Mutex<usize>>,
            get_object_range_calls: Arc<Mutex<usize>>,
        }

        #[async_trait]
        impl ObjectBackend for MockBackend {
            async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
                self.data
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), data.to_vec());
                Ok(())
            }

            async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                *self.get_object_calls.lock().unwrap() += 1;
                Ok(self.data.lock().unwrap().get(key).cloned())
            }

            async fn get_object_range(
                &self,
                key: &str,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<usize> {
                *self.get_object_range_calls.lock().unwrap() += 1;
                if let Some(data) = self.data.lock().unwrap().get(key) {
                    let offset = offset as usize;
                    let end = (offset + buf.len()).min(data.len());
                    if offset < data.len() {
                        let copy_len = end - offset;
                        buf[..copy_len].copy_from_slice(&data[offset..end]);
                        return Ok(copy_len);
                    }
                }
                Ok(0)
            }

            async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
                Ok("test_etag".to_string())
            }

            async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
                self.data.lock().unwrap().remove(key);
                Ok(())
            }
        }

        let backend = MockBackend::default();
        let block: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        backend
            .data
            .lock()
            .unwrap()
            .insert("chunks/88/0".to_string(), block);

        let cache_dir = tempfile::tempdir()?;
        let config = BlockStoreConfig {
            block_size: 4 * 1024 * 1024,
            range_read_threshold: 0.25,
            compression: Compression::None,
            ..Default::default()
        };
        let store = ObjectBlockStore::new_with_configs_async(
            ObjectClient::new(backend.clone()),
            ChunksCacheConfig::with_budgets(
                16 * 1024 * 1024,
                16 * 1024 * 1024,
                cache_dir.path().to_path_buf(),
            ),
            config,
        )
        .await?;

        let mut small = vec![0u8; 4096];
        store.read_range((88, 0), 1024, &mut small).await?;
        assert_eq!(
            small,
            (1024..1024 + 4096)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>()
        );
        assert_eq!(*backend.get_object_range_calls.lock().unwrap(), 1);

        for _ in 0..100 {
            if *backend.get_object_calls.lock().unwrap() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            *backend.get_object_calls.lock().unwrap(),
            1,
            "range read should asynchronously prefetch the full block"
        );
        let block_cache_key = "chunks/88/0".to_string();
        for _ in 0..50 {
            if store.block_cache.get(&block_cache_key).await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            store.block_cache.get(&block_cache_key).await.is_some(),
            "range read should asynchronously prefetch and cache the full block"
        );
        assert_eq!(*backend.get_object_calls.lock().unwrap(), 1);

        let mut large = vec![0u8; 2 * 1024 * 1024];
        store.read_range((88, 0), 0, &mut large).await?;
        assert_eq!(
            large[..4096],
            (0..4096).map(|i| (i % 251) as u8).collect::<Vec<_>>()
        );
        assert_eq!(
            *backend.get_object_calls.lock().unwrap(),
            1,
            "large read after background prefetch should hit block cache"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_background_range_prefetch_is_serialized() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::cadapter::client::{ObjectBackend, ObjectClient};
        use async_trait::async_trait;
        use futures::future;
        use std::{
            collections::HashMap,
            sync::{
                Arc, Mutex,
                atomic::{AtomicBool, AtomicUsize, Ordering},
            },
        };
        use tokio::time::Duration;

        #[derive(Clone)]
        struct MockBackend {
            data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
            get_object_calls: Arc<AtomicUsize>,
            get_object_range_calls: Arc<AtomicUsize>,
            current_full_gets: Arc<AtomicUsize>,
            max_full_gets: Arc<AtomicUsize>,
            block_full_gets: Arc<AtomicBool>,
        }

        impl Default for MockBackend {
            fn default() -> Self {
                Self {
                    data: Arc::new(Mutex::new(HashMap::new())),
                    get_object_calls: Arc::new(AtomicUsize::new(0)),
                    get_object_range_calls: Arc::new(AtomicUsize::new(0)),
                    current_full_gets: Arc::new(AtomicUsize::new(0)),
                    max_full_gets: Arc::new(AtomicUsize::new(0)),
                    block_full_gets: Arc::new(AtomicBool::new(true)),
                }
            }
        }

        #[async_trait]
        impl ObjectBackend for MockBackend {
            async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
                self.data
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), data.to_vec());
                Ok(())
            }

            async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                self.get_object_calls.fetch_add(1, Ordering::SeqCst);
                let active = self.current_full_gets.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_full_gets.fetch_max(active, Ordering::SeqCst);

                while self.block_full_gets.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                self.current_full_gets.fetch_sub(1, Ordering::SeqCst);
                Ok(self.data.lock().unwrap().get(key).cloned())
            }

            async fn get_object_range(
                &self,
                key: &str,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<usize> {
                self.get_object_range_calls.fetch_add(1, Ordering::SeqCst);
                if let Some(data) = self.data.lock().unwrap().get(key) {
                    let offset = offset as usize;
                    let end = (offset + buf.len()).min(data.len());
                    if offset < data.len() {
                        let copy_len = end - offset;
                        buf[..copy_len].copy_from_slice(&data[offset..end]);
                        return Ok(copy_len);
                    }
                }
                Ok(0)
            }

            async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
                Ok("test_etag".to_string())
            }

            async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
                self.data.lock().unwrap().remove(key);
                Ok(())
            }
        }

        let backend = MockBackend::default();
        let block: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        {
            let mut data = backend.data.lock().unwrap();
            data.insert("chunks/90/0".to_string(), block.clone());
            data.insert("chunks/91/0".to_string(), block);
        }

        let cache_dir = tempfile::tempdir()?;
        let config = BlockStoreConfig {
            block_size: 4 * 1024 * 1024,
            range_read_threshold: 0.25,
            compression: Compression::None,
            ..Default::default()
        };
        let store = Arc::new(
            ObjectBlockStore::new_with_configs_async(
                ObjectClient::new(backend.clone()),
                ChunksCacheConfig::with_budgets(
                    16 * 1024 * 1024,
                    16 * 1024 * 1024,
                    cache_dir.path().to_path_buf(),
                ),
                config,
            )
            .await?,
        );

        let reads = [(90, 0), (91, 0)]
            .into_iter()
            .map(|key| {
                let store = store.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    store.read_range(key, 1024, &mut buf).await?;
                    anyhow::Ok(())
                })
            })
            .collect::<Vec<_>>();
        future::try_join_all(reads).await?;
        assert_eq!(backend.get_object_range_calls.load(Ordering::SeqCst), 2);

        for _ in 0..100 {
            if backend.get_object_calls.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        backend.block_full_gets.store(false, Ordering::SeqCst);
        for _ in 0..100 {
            if backend.get_object_calls.load(Ordering::SeqCst) >= 2
                && backend.current_full_gets.load(Ordering::SeqCst) == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(
            backend.max_full_gets.load(Ordering::SeqCst) <= 8,
            "background range prefetch should respect concurrency limit (max 8)"
        );
        assert_eq!(backend.get_object_calls.load(Ordering::SeqCst), 2);
        assert_eq!(backend.current_full_gets.load(Ordering::SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_background_range_prefetch_drops_when_limit_saturated()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::cadapter::client::{ObjectBackend, ObjectClient};
        use async_trait::async_trait;
        use std::{
            collections::HashMap,
            sync::{
                Arc, Mutex,
                atomic::{AtomicUsize, Ordering},
            },
        };
        use tokio::time::Duration;

        #[derive(Clone, Default)]
        struct MockBackend {
            data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
            get_object_calls: Arc<AtomicUsize>,
            get_object_range_calls: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl ObjectBackend for MockBackend {
            async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
                self.data
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), data.to_vec());
                Ok(())
            }

            async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                self.get_object_calls.fetch_add(1, Ordering::SeqCst);
                Ok(self.data.lock().unwrap().get(key).cloned())
            }

            async fn get_object_range(
                &self,
                key: &str,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<usize> {
                self.get_object_range_calls.fetch_add(1, Ordering::SeqCst);
                if let Some(data) = self.data.lock().unwrap().get(key) {
                    let offset = offset as usize;
                    let end = (offset + buf.len()).min(data.len());
                    if offset < data.len() {
                        let copy_len = end - offset;
                        buf[..copy_len].copy_from_slice(&data[offset..end]);
                        return Ok(copy_len);
                    }
                }
                Ok(0)
            }

            async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
                Ok("test_etag".to_string())
            }

            async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
                self.data.lock().unwrap().remove(key);
                Ok(())
            }
        }

        let backend = MockBackend::default();
        let block: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        backend
            .data
            .lock()
            .unwrap()
            .insert("chunks/92/0".to_string(), block);

        let cache_dir = tempfile::tempdir()?;
        let store = ObjectBlockStore::new_with_configs_async(
            ObjectClient::new(backend.clone()),
            ChunksCacheConfig::with_budgets(
                16 * 1024 * 1024,
                16 * 1024 * 1024,
                cache_dir.path().to_path_buf(),
            ),
            BlockStoreConfig {
                block_size: 4 * 1024 * 1024,
                range_read_threshold: 0.25,
                compression: Compression::None,
                ..Default::default()
            },
        )
        .await?;

        let _permits = store
            .range_prefetch_limit
            .clone()
            .acquire_many_owned(8)
            .await?;

        let mut buf = vec![0u8; 4096];
        store.read_range((92, 0), 1024, &mut buf).await?;
        assert_eq!(backend.get_object_range_calls.load(Ordering::SeqCst), 1);

        for _ in 0..20 {
            let snapshot = store
                .object_store_metrics()
                .expect("ObjectBlockStore exposes object metrics")
                .snapshot();
            if snapshot.read_background_prefetch_dropped >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let snapshot = store
            .object_store_metrics()
            .expect("ObjectBlockStore exposes object metrics")
            .snapshot();
        assert_eq!(snapshot.read_background_prefetches, 1);
        assert_eq!(
            snapshot.read_background_prefetch_dropped, 1,
            "background prefetch should be dropped immediately when all permits are busy"
        );
        assert_eq!(
            backend.get_object_calls.load(Ordering::SeqCst),
            0,
            "dropped background prefetch should not issue a full-object GET"
        );

        Ok(())
    }
}
