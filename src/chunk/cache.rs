#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{
    hash,
    path::{Path, PathBuf},
};

use crate::chunk::cache_health::DiskHealth;
use crate::chunk::cache_integrity::CacheIntegrityMode;
use anyhow::anyhow;
use dashmap::DashSet;
use dirs::cache_dir;
use parking_lot::Mutex as ParkingMutex;
use sea_orm::sea_query::WindowSelectType;
use sha2::{Digest, Sha256, digest::KeyInit};
use tokio::fs;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tracing::{debug, error, info, trace, warn};

static DISK_CACHE_TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
const DISK_CACHE_READ_CONCURRENCY: usize = 16;
const DISK_CACHE_WRITE_CONCURRENCY: usize = 16;
const DISK_HIT_FAST_PROMOTE_MAX_UTILIZATION_PER_MILLE: u64 = 800;

/// Configuration for the intelligent dual-layer cache system.
///
/// This cache implements an adaptive promotion strategy that combines:
/// - **Dual time windows** for burst detection and trend analysis
/// - **Dynamic threshold adjustment** based on system metrics
/// - **Weighted frequency calculation** for intelligent promotion decisions
///
/// # Architecture Overview
///
/// ```text
/// +-----------------+    +-----------------+    +-----------------+
/// |   Hot Cache     |    |   Cold Cache    |    |  Disk Storage   |
/// |   (1024 items)  |<-->|   (1024 items)  |<-->|   (Persistent)  |
/// |   Fast Access   |    |   Metadata Only |    |   SHA256 Files  |
/// +-----------------+    +-----------------+    +-----------------+
///         ^                       |                       ^
///         |                       |                       |
///         v                       v                       v
///    Adaptive Promotion     Access Pattern Tracking    Fallback Storage
///    Strategy Engine        & Frequency Analysis       for Large Data
/// ```
///
/// # Promotion Strategy Details
///
/// The promotion decision uses a multi-dimensional scoring system:
///
/// ```text
/// weighted_frequency = short_freq * short_weight + medium_freq * medium_weight
/// adaptive_threshold = base_threshold * system_factor * hitrate_factor
/// promote_if = weighted_frequency >= adaptive_threshold
/// ```
///
/// ## Time Windows
///
/// - **Short Window (10s)**: Detects burst access patterns with 1-second granularity
/// - **Medium Window (60s)**: Analyzes medium-term trends with 5-second granularity
///
/// ## Adaptive Threshold Logic
///
/// - **High Load** (>0.8): Reduce threshold by 30% for aggressive promotion
/// - **Low Hit Rate** (<0.6): Reduce threshold to accelerate cold-cache warmup
/// - **High Hit Rate** (>0.8): Reduce threshold by 10% to maintain performance
///
/// # Example Configurations
///
/// ## Performance-Optimized (Low Latency)
/// ```text
/// ChunksCacheConfig {
///     base_promotion_threshold: 5.0,
///     short_window_weight: 0.8,
///     enable_adaptive_threshold: true,
///     // ... other settings
/// }
/// ```
///
/// ## Memory-Conservative (Resource Constrained)
/// ```text
/// ChunksCacheConfig {
///     base_promotion_threshold: 15.0,
///     short_window_weight: 0.6,
///     conservative_promotion_hit_rate_threshold: 0.7,
///     // ... other settings
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ChunksCacheConfig {
    /// Maximum number of entries in hot cache (fastest access tier)
    ///
    /// **Recommended**: 512-2048 depending on available memory
    /// **Impact**: Higher values = more hot data but more memory usage
    pub hot_cache_size: usize,

    /// Maximum number of entries in cold cache (metadata tracking tier)
    ///
    /// **Recommended**: Same as hot_cache_size or 2x for comprehensive tracking
    /// **Impact**: Higher values = better access pattern visibility but more metadata overhead
    pub cold_cache_size: usize,

    /// Base access frequency threshold for promoting items to hot cache
    ///
    /// **Units**: accesses per second
    /// **Typical Range**: 5.0 - 20.0
    /// **Lower Values**: More aggressive promotion (better for bursty workloads)
    /// **Higher Values**: More conservative (better for stable workloads)
    pub base_promotion_threshold: f64,

    /// Short time window for burst access detection
    ///
    /// **Purpose**: Capture rapid, recent access patterns
    /// **Recommended**: 5-15 seconds
    /// **Trade-off**: Shorter = more responsive but noisier
    pub short_window_size: Duration,

    /// Medium time window for trend analysis
    ///
    /// **Purpose**: Identify sustained access patterns and trends
    /// **Recommended**: 30-120 seconds
    /// **Trade-off**: Longer = more stable but slower to adapt
    pub medium_window_size: Duration,

    /// Maximum number of access records to keep per key
    ///
    /// **Note**: Legacy parameter for compatibility. Actual bucket count is
    ///         calculated dynamically based on window sizes.
    pub max_access_entries: usize,

    /// Maximum bytes for the hot (in-memory) cache tier.
    ///
    /// **Default**: 1 GiB (maps to CacheConfig.read_memory_bytes)
    /// **Impact**: Controls how much RAM is used for frequently-accessed blocks.
    /// Uses moka's byte-weighted eviction (weigher returns entry byte size).
    pub max_hot_bytes: u64,

    /// Maximum bytes for on-disk cache storage.
    ///
    /// **Default**: 20 GiB (maps to CacheConfig.read_ssd_bytes)
    /// **Impact**: Controls SSD usage for the persistent read cache.
    /// When exceeded, oldest files (by access time) are evicted on insert.
    pub max_disk_bytes: u64,

    /// Custom disk storage directory (optional)
    ///
    /// If None, uses system cache directory via `dirs::cache_dir()`
    /// Files are stored using SHA256 hash of the key for unique naming
    pub disk_storage_dir: Option<PathBuf>,

    /// Integrity mode for on-disk clean block cache entries.
    ///
    /// Full uses CRC32C framing on every disk cache store/load. None stores raw
    /// bytes for trusted local SSD profiles where avoiding checksum CPU is more
    /// important than detecting local cache corruption.
    pub disk_integrity_mode: CacheIntegrityMode,

    /// Weight for short window access frequency in promotion decisions
    ///
    /// **Range**: 0.0 - 1.0
    /// **Higher Values**: Prioritize recent burst access (good for interactive workloads)
    /// **Lower Values**: Prioritize sustained trends (good for batch workloads)
    pub short_window_weight: f64,

    /// Weight for medium window access frequency in promotion decisions
    ///
    /// **Range**: 0.0 - 1.0
    /// **Note**: short_window_weight + medium_window_weight should typically sum to 1.0
    pub medium_window_weight: f64,

    /// Enable adaptive threshold adjustment based on system load and hit rate
    ///
    /// **When true**: Dynamically adjusts promotion threshold based on:
    ///   - System load (cache utilization + request rate)
    ///   - Cache hit rate
    ///
    /// **When false**: Uses fixed base_promotion_threshold
    pub enable_adaptive_threshold: bool,

    /// System load threshold for triggering aggressive promotion mode
    ///
    /// **Range**: 0.0 - 1.0
    /// **When exceeded**: Reduces promotion threshold by 30% to cache more data
    /// **Purpose**: Improve performance under high load by increasing cache hit rate
    pub aggressive_promotion_load_threshold: f64,

    /// Cache hit rate threshold for triggering conservative promotion mode
    ///
    /// **Range**: 0.0 - 1.0
    /// **When below**: Increases promotion threshold by 30% to prevent cache pollution
    /// **Purpose**: Maintain cache efficiency when hit rate is already low
    pub conservative_promotion_hit_rate_threshold: f64,
}

impl Default for ChunksCacheConfig {
    fn default() -> Self {
        Self {
            hot_cache_size: 1024,
            cold_cache_size: 1024,
            max_hot_bytes: 1024 * 1024 * 1024,       // 1 GiB
            max_disk_bytes: 20 * 1024 * 1024 * 1024, // 20 GiB
            base_promotion_threshold: 5.0,
            short_window_size: Duration::from_secs(10),
            medium_window_size: Duration::from_secs(60),
            max_access_entries: 100,
            disk_storage_dir: None,
            disk_integrity_mode: CacheIntegrityMode::Full,
            short_window_weight: 0.75,
            medium_window_weight: 0.25,
            enable_adaptive_threshold: true,
            aggressive_promotion_load_threshold: 0.8,
            conservative_promotion_hit_rate_threshold: 0.6,
        }
    }
}

impl ChunksCacheConfig {
    /// Create a config with explicit byte budgets and cache directory.
    pub fn with_budgets(read_memory_bytes: u64, read_ssd_bytes: u64, cache_dir: PathBuf) -> Self {
        Self {
            max_hot_bytes: read_memory_bytes,
            max_disk_bytes: read_ssd_bytes,
            disk_storage_dir: Some(cache_dir),
            ..Default::default()
        }
    }

    pub fn with_integrity_mode(mut self, mode: CacheIntegrityMode) -> Self {
        self.disk_integrity_mode = mode;
        self
    }
}

fn recent_write_hot_capacity(max_hot_bytes: u64) -> u64 {
    const MAX_RECENT_WRITE_HOT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
    max_hot_bytes.min(MAX_RECENT_WRITE_HOT_BYTES)
}

#[derive(Clone)]
struct RecentWriteEntry {
    generation: u64,
    data: bytes::Bytes,
}

#[derive(Clone)]
struct RecentWriteHotCache {
    entries: Arc<dashmap::DashMap<String, RecentWriteEntry>>,
    order: Arc<ParkingMutex<VecDeque<(String, u64)>>>,
    bytes: Arc<AtomicU64>,
    generation: Arc<AtomicU64>,
    max_bytes: u64,
}

impl RecentWriteHotCache {
    fn new(max_bytes: u64) -> Self {
        Self {
            entries: Arc::new(dashmap::DashMap::new()),
            order: Arc::new(ParkingMutex::new(VecDeque::new())),
            bytes: Arc::new(AtomicU64::new(0)),
            generation: Arc::new(AtomicU64::new(0)),
            max_bytes,
        }
    }

    fn get(&self, key: &str) -> Option<bytes::Bytes> {
        self.entries.get(key).map(|entry| entry.data.clone())
    }

    fn insert(&self, key: String, data: bytes::Bytes) {
        let len = data.len() as u64;
        if self.max_bytes == 0 || len > self.max_bytes {
            if let Some((_, old)) = self.entries.remove(&key) {
                self.bytes
                    .fetch_sub(old.data.len() as u64, Ordering::Relaxed);
            }
            return;
        }

        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(old) = self
            .entries
            .insert(key.clone(), RecentWriteEntry { generation, data })
        {
            self.bytes
                .fetch_sub(old.data.len() as u64, Ordering::Relaxed);
        }
        self.bytes.fetch_add(len, Ordering::Relaxed);

        let mut order = self.order.lock();
        order.push_back((key, generation));
        self.evict_locked(&mut order);
    }

    fn evict_locked(&self, order: &mut VecDeque<(String, u64)>) {
        while self.bytes.load(Ordering::Relaxed) > self.max_bytes {
            let Some((key, generation)) = order.pop_front() else {
                break;
            };
            let should_remove = self
                .entries
                .get(&key)
                .map(|entry| entry.generation == generation)
                .unwrap_or(false);
            if should_remove && let Some((_, removed)) = self.entries.remove(&key) {
                self.bytes
                    .fetch_sub(removed.data.len() as u64, Ordering::Relaxed);
            }
        }
    }

    fn weighted_size(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    fn entry_count(&self) -> u64 {
        self.entries.len() as u64
    }

    fn max_bytes(&self) -> u64 {
        self.max_bytes
    }
}

fn copy_full_cached_range(value: &bytes::Bytes, offset: usize, buf: &mut [u8]) -> Option<usize> {
    let end = offset.checked_add(buf.len())?;
    if end > value.len() {
        return None;
    }

    buf.copy_from_slice(&value[offset..end]);
    Some(buf.len())
}

#[derive(Debug, Clone)]
struct DiskStorage {
    base_dir: PathBuf,
    health: Arc<DiskHealth>,
    /// Current total bytes used on disk
    bytes_used: Arc<AtomicU64>,
    /// Maximum bytes allowed on disk (0 = unlimited)
    max_bytes: u64,
    /// Semaphore for read operations (higher priority, separate pool)
    read_sem: Arc<Semaphore>,
    /// Semaphore for write/store operations
    write_sem: Arc<Semaphore>,
    integrity_mode: CacheIntegrityMode,
}

impl DiskStorage {
    pub async fn new<P: AsRef<Path>>(base_dir: P, max_bytes: u64) -> anyhow::Result<Self> {
        Self::new_with_integrity(base_dir, max_bytes, CacheIntegrityMode::Full).await
    }

    pub async fn new_with_integrity<P: AsRef<Path>>(
        base_dir: P,
        max_bytes: u64,
        integrity_mode: CacheIntegrityMode,
    ) -> anyhow::Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        debug!(
            "Initializing disk storage at: {:?}, max_bytes: {}, integrity_mode: {:?}",
            base_dir, max_bytes, integrity_mode
        );

        if !base_dir.exists() {
            info!("Creating cache directory: {:?}", base_dir);
            fs::create_dir_all(&base_dir).await?;
        } else {
            debug!("Cache directory already exists: {:?}", base_dir);
        }

        // Scan existing files to calculate initial bytes_used
        let initial_bytes = Self::scan_dir_size(&base_dir).await;
        debug!(
            "Initial disk cache usage: {} bytes ({:.1} MiB)",
            initial_bytes,
            initial_bytes as f64 / 1048576.0
        );

        Ok(Self {
            base_dir,
            health: Arc::new(DiskHealth::new()),
            bytes_used: Arc::new(AtomicU64::new(initial_bytes)),
            max_bytes,
            read_sem: Arc::new(Semaphore::new(DISK_CACHE_READ_CONCURRENCY)),
            write_sem: Arc::new(Semaphore::new(DISK_CACHE_WRITE_CONCURRENCY)),
            integrity_mode,
        })
    }

    /// Scan directory to calculate total size of cached files
    async fn scan_dir_size(dir: &Path) -> u64 {
        let mut total = 0u64;
        let mut entries = match tokio::fs::read_dir(dir).await {
            Ok(e) => e,
            Err(_) => return 0,
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(meta) = entry.metadata().await
                && meta.is_file()
            {
                total += meta.len();
            }
        }
        total
    }

    pub fn key_to_filename(key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        let hash_result = hasher.finalize();

        hex::encode(hash_result)
    }

    /// Get current bytes used on disk
    pub fn bytes_used(&self) -> u64 {
        self.bytes_used.load(Ordering::Relaxed)
    }

    pub async fn store(&self, key: &str, data: impl AsRef<[u8]>) -> anyhow::Result<()> {
        let permit = self.write_sem.clone().acquire_owned().await?;
        self.store_with_permit(key, bytes::Bytes::copy_from_slice(data.as_ref()), permit)
            .await
    }

    pub async fn store_with_health(
        &self,
        key: &str,
        data: impl AsRef<[u8]>,
    ) -> anyhow::Result<bool> {
        if self.health.is_bypassed() {
            return Ok(false);
        }

        match self.store(key, data).await {
            Ok(()) => {
                self.health.record_success();
                Ok(true)
            }
            Err(err) => {
                self.health.record_error();
                warn!(key, error = ?err, "disk cache store failed; treating as cache miss");
                Ok(false)
            }
        }
    }

    async fn store_with_permit_health(
        &self,
        key: &str,
        data: bytes::Bytes,
        permit: OwnedSemaphorePermit,
    ) -> anyhow::Result<bool> {
        if self.health.is_bypassed() {
            return Ok(false);
        }

        match self.store_with_permit(key, data, permit).await {
            Ok(()) => {
                self.health.record_success();
                Ok(true)
            }
            Err(err) => {
                self.health.record_error();
                warn!(key, error = ?err, "disk cache store failed; treating as cache miss");
                Ok(false)
            }
        }
    }

    async fn store_with_permit(
        &self,
        key: &str,
        data: bytes::Bytes,
        _permit: OwnedSemaphorePermit,
    ) -> anyhow::Result<()> {
        let filename = Self::key_to_filename(key);
        let filepath = self.base_dir.join(&filename);

        // Compute CRC32C framing without copying the data block when enabled.
        let (header, checksums) = match self.integrity_mode {
            CacheIntegrityMode::Full => super::cache_integrity::compute_framing(&data),
            CacheIntegrityMode::None => (Vec::new(), Vec::new()),
        };
        let total_len = (header.len() + data.len() + checksums.len()) as u64;

        // Atomically reserve space and trigger eviction if over budget.
        // fetch_add is atomic — no race between multiple concurrent store() calls.
        if self.max_bytes > 0 {
            let prev = self.bytes_used.fetch_add(total_len, Ordering::Relaxed);
            if prev + total_len > self.max_bytes {
                self.evict_lru(total_len).await;
            }
        } else {
            self.bytes_used.fetch_add(total_len, Ordering::Relaxed);
        }

        // If the file already exists, subtract its old size (we already added total_len)
        if let Ok(meta) = tokio::fs::metadata(&filepath).await {
            self.bytes_used.fetch_sub(meta.len(), Ordering::Relaxed);
        }

        // Write to a private temp file first, then atomically publish it with
        // rename. Readers either see the previous complete file or the new
        // complete file, never a partially-written cache entry.
        let tmp_id = DISK_CACHE_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = self.base_dir.join(format!(".{}.{}.tmp", filename, tmp_id));
        let tmp_path_for_write = tmp_path.clone();
        let write_result = match tokio::task::spawn_blocking(move || {
            use std::io::Write;

            let mut file = std::fs::File::create(&tmp_path_for_write)?;
            file.write_all(&header)?;
            file.write_all(&data)?;
            file.write_all(&checksums)?;
            drop(file);
            std::fs::rename(&tmp_path_for_write, &filepath)?;
            Ok::<(), anyhow::Error>(())
        })
        .await
        {
            Ok(result) => result,
            Err(err) => Err(err.into()),
        };

        if let Err(e) = write_result {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            // Roll back the pre-reserved bytes on write failure
            self.bytes_used.fetch_sub(total_len, Ordering::Relaxed);
            return Err(e);
        }

        Ok(())
    }

    fn try_io_permit(&self, key: &str) -> Option<OwnedSemaphorePermit> {
        let Ok(permit) = self.write_sem.clone().try_acquire_owned() else {
            trace!(
                "Skipping disk cache store for key '{}' because IO is busy",
                key
            );
            return None;
        };
        Some(permit)
    }

    /// Clone the write IO semaphore Arc for deferred disk cache writes.
    fn io_sem_clone(&self) -> Arc<Semaphore> {
        self.write_sem.clone()
    }

    fn touch_atime(filepath: &Path) {
        // Touch atime so LRU eviction keeps hot data longer.
        // Uses futimens with UTIME_NOW on atime only (mtime unchanged).
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let _ = std::fs::metadata(filepath).map(|m| {
                let mtime = libc::timespec {
                    tv_sec: m.mtime(),
                    tv_nsec: m.mtime_nsec(),
                };
                let times = [
                    libc::timespec {
                        tv_sec: 0,
                        tv_nsec: libc::UTIME_NOW,
                    },
                    mtime,
                ];
                let c_path = std::ffi::CString::new(filepath.as_os_str().as_encoded_bytes()).ok();
                if let Some(p) = c_path {
                    unsafe {
                        // SAFETY: valid null-terminated path and timespec array.
                        libc::utimensat(libc::AT_FDCWD, p.as_ptr(), times.as_ptr(), 0);
                    }
                }
            });
        }
    }

    /// Evict oldest files (by access time) to free at least `needed_bytes`
    async fn evict_lru(&self, needed_bytes: u64) {
        let target = self.max_bytes.saturating_sub(needed_bytes);
        let current = self.bytes_used.load(Ordering::Relaxed);
        if current <= target {
            return;
        }
        let to_free = current - target;
        debug!(
            "Disk cache eviction: need to free {} bytes ({:.1} MiB)",
            to_free,
            to_free as f64 / 1048576.0
        );

        // Collect files with their access times
        let mut files: Vec<(PathBuf, u64, u64)> = Vec::new(); // (path, size, atime_secs)
        let mut entries = match tokio::fs::read_dir(&self.base_dir).await {
            Ok(e) => e,
            Err(_) => return,
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(meta) = entry.metadata().await
                && meta.is_file()
            {
                let atime = meta
                    .accessed()
                    .unwrap_or(SystemTime::UNIX_EPOCH)
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                files.push((entry.path(), meta.len(), atime));
            }
        }

        // Sort by access time (oldest first)
        files.sort_by_key(|f| f.2);

        let mut freed = 0u64;
        for (path, size, _) in &files {
            if freed >= to_free {
                break;
            }
            if tokio::fs::remove_file(path).await.is_ok() {
                freed += size;
                self.bytes_used.fetch_sub(*size, Ordering::Relaxed);
                trace!("Evicted cache file: {:?} ({} bytes)", path, size);
            }
        }
        debug!(
            "Disk cache eviction complete: freed {} bytes ({:.1} MiB)",
            freed,
            freed as f64 / 1048576.0
        );
    }

    pub async fn load(&self, key: &str) -> anyhow::Result<bytes::Bytes> {
        let filename = Self::key_to_filename(key);
        let filepath = self.base_dir.join(filename);

        let _permit = self.read_sem.clone().acquire_owned().await?;
        let raw = match tokio::fs::read(&filepath).await {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(anyhow!("file {} does not exist", filepath.display()));
            }
            Err(e) => {
                return Err(e.into());
            }
        };

        // Decode with CRC32C verification (handles legacy unencoded files too)
        match super::cache_integrity::decode_bytes(raw) {
            Some(data) => {
                Self::touch_atime(&filepath);
                Ok(data)
            }
            None => {
                // Corrupted — delete the file and return error
                let _ = tokio::fs::remove_file(&filepath).await;
                Err(anyhow!(
                    "CRC32C verification failed for cache key '{}', file deleted",
                    key
                ))
            }
        }
    }

    pub async fn load_range_with_health(
        &self,
        key: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<Option<usize>> {
        if self.health.is_bypassed() {
            return Ok(None);
        }

        let filename = Self::key_to_filename(key);
        let filepath = self.base_dir.join(filename);
        if tokio::fs::metadata(&filepath).await.is_err() {
            return Ok(None);
        }

        match self.load_range(key, offset, buf).await {
            Ok(read_len) => {
                self.health.record_success();
                Ok(Some(read_len))
            }
            Err(err) => {
                self.health.record_error();
                warn!(key, error = ?err, "disk cache range load failed; treating as cache miss");
                Ok(None)
            }
        }
    }

    async fn load_range(&self, key: &str, offset: u64, buf: &mut [u8]) -> anyhow::Result<usize> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        if buf.is_empty() {
            return Ok(0);
        }

        let filename = Self::key_to_filename(key);
        let filepath = self.base_dir.join(filename);

        let _permit = self.read_sem.clone().acquire_owned().await?;
        let mut file = match tokio::fs::File::open(&filepath).await {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(anyhow!("file {} does not exist", filepath.display()));
            }
            Err(e) => return Err(e.into()),
        };
        let file_len = file.metadata().await?.len();

        let mut header = [0u8; super::cache_integrity::HEADER_LEN];
        let header_len = file.read(&mut header).await?;
        if header_len < super::cache_integrity::HEADER_LEN
            || header[..4] != super::cache_integrity::MAGIC
        {
            let read_len = Self::read_legacy_range(&mut file, file_len, offset, buf).await?;
            Self::touch_atime(&filepath);
            return Ok(read_len);
        }

        let data_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
        let checksum_blocks = data_len.div_ceil(super::cache_integrity::CS_BLOCK);
        let expected_total = super::cache_integrity::HEADER_LEN + data_len + checksum_blocks * 4;
        if file_len < expected_total as u64 {
            let _ = tokio::fs::remove_file(&filepath).await;
            return Err(anyhow!(
                "truncated cache key '{}': file_len={}, expected={}",
                key,
                file_len,
                expected_total
            ));
        }

        if offset >= data_len as u64 {
            Self::touch_atime(&filepath);
            return Ok(0);
        }

        let offset = offset as usize;
        let copy_len = buf.len().min(data_len - offset);
        if copy_len == 0 {
            Self::touch_atime(&filepath);
            return Ok(0);
        }

        let first_block = offset / super::cache_integrity::CS_BLOCK;
        let last_block = (offset + copy_len - 1) / super::cache_integrity::CS_BLOCK;
        let aligned_start = first_block * super::cache_integrity::CS_BLOCK;
        let aligned_end = ((last_block + 1) * super::cache_integrity::CS_BLOCK).min(data_len);
        let block_count = last_block - first_block + 1;

        let mut aligned = vec![0u8; aligned_end - aligned_start];
        file.seek(std::io::SeekFrom::Start(
            (super::cache_integrity::HEADER_LEN + aligned_start) as u64,
        ))
        .await?;
        file.read_exact(&mut aligned).await?;

        let mut checksums = vec![0u8; block_count * 4];
        file.seek(std::io::SeekFrom::Start(
            (super::cache_integrity::HEADER_LEN + data_len + first_block * 4) as u64,
        ))
        .await?;
        file.read_exact(&mut checksums).await?;

        for local_block in 0..block_count {
            let global_block = first_block + local_block;
            let block_start = global_block * super::cache_integrity::CS_BLOCK;
            let block_end = (block_start + super::cache_integrity::CS_BLOCK).min(data_len);
            let local_start = block_start - aligned_start;
            let local_end = block_end - aligned_start;
            let expected = u32::from_le_bytes([
                checksums[local_block * 4],
                checksums[local_block * 4 + 1],
                checksums[local_block * 4 + 2],
                checksums[local_block * 4 + 3],
            ]);
            let actual = crc32c::crc32c(&aligned[local_start..local_end]);
            if actual != expected {
                let _ = tokio::fs::remove_file(&filepath).await;
                return Err(anyhow!(
                    "CRC32C verification failed for cache key '{}', file deleted",
                    key
                ));
            }
        }

        let local_offset = offset - aligned_start;
        buf[..copy_len].copy_from_slice(&aligned[local_offset..local_offset + copy_len]);
        Self::touch_atime(&filepath);
        Ok(copy_len)
    }

    async fn read_legacy_range(
        file: &mut tokio::fs::File,
        file_len: u64,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        if offset >= file_len {
            return Ok(0);
        }
        let read_len = buf.len().min((file_len - offset) as usize);
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        file.read_exact(&mut buf[..read_len]).await?;
        Ok(read_len)
    }

    pub async fn load_with_health(&self, key: &str) -> anyhow::Result<Option<bytes::Bytes>> {
        if self.health.is_bypassed() {
            return Ok(None);
        }

        let filename = Self::key_to_filename(key);
        let filepath = self.base_dir.join(filename);
        if tokio::fs::metadata(&filepath).await.is_err() {
            return Ok(None);
        }

        match self.load(key).await {
            Ok(data) => {
                self.health.record_success();
                Ok(Some(data))
            }
            Err(err) => {
                self.health.record_error();
                warn!(key, error = ?err, "disk cache load failed; treating as cache miss");
                Ok(None)
            }
        }
    }

    pub async fn remove(&self, key: &str) -> anyhow::Result<()> {
        let filename = Self::key_to_filename(key);
        let filepath = self.base_dir.join(filename);

        trace!("Removing file for key '{}': {:?}", key, filepath);

        if !filepath.exists() {
            warn!(
                "Attempted to remove non-existent file for key '{}': {:?}",
                key, filepath
            );
            return Err(anyhow!("file {} does not exist", filepath.display()));
        }

        // Get size before removing
        let file_size = tokio::fs::metadata(&filepath)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        let _permit = self.write_sem.clone().acquire_owned().await?;
        match tokio::fs::remove_file(&filepath).await {
            Ok(_) => {
                self.bytes_used.fetch_sub(file_size, Ordering::Relaxed);
                debug!("Successfully removed file for key '{}'", key);
                Ok(())
            }
            Err(e) => {
                error!("Failed to remove file for key '{}': {}", key, e);
                Err(e.into())
            }
        }
    }
}

/// Lock-free access statistics tracker with dual time window analysis.
///
/// This structure implements a high-performance, concurrent-safe access pattern
/// analysis system using atomic operations and circular time buckets.
///
/// # Architecture
///
/// ```text
/// Time Progress ----------------------------------------------->
///
/// Short Window (10s, 1s granularity):
/// +--+--+--+--+--+--+--+--+--+--+
/// |0 |1 |2 |3 |4 |5 |6 |7 |8 |9 |
/// +--+--+--+--+--+--+--+--+--+--+
///  |                            |
///  v                            v
/// Old Data                  Recent Data
///
/// Medium Window (60s, 5s granularity):
/// +---+---+---+---+---+---+---+---+---+---+---+---+
/// | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |10 |11 |
/// +---+---+---+---+---+---+---+---+---+---+---+---+
///  0-5s  5-10s                     55-60s
/// ```
///
/// # Performance Characteristics
///
/// - **O(1) access recording**: Single atomic increment per access
/// - **Lock-free design**: No mutexes or RwLocks
/// - **Memory efficient**: Circular buffer with automatic cleanup
/// - **Cache-friendly**: Sequential memory layout
///
/// # Usage Examples
///
/// ```ignore
/// use std::time::Duration;
///
/// let stats = AccessStats::new(
///     Duration::from_secs(10),  // Short window
///     Duration::from_secs(60),  // Medium window
///     100,                      // Legacy compat parameter
/// );
///
/// // Record an access (thread-safe, O(1))
/// stats.record_access();
///
/// // Calculate weighted frequency
/// let weighted = stats.get_weighted_access_frequency(0.7, 0.3);
/// ```
///
/// # Thread Safety
///
/// This struct is designed for concurrent access from multiple threads.
/// All operations use atomic primitives and are completely lock-free.
#[derive(Debug)]
struct AccessStats {
    /// Short window buckets for burst access detection
    ///
    /// - **Granularity**: 1 second per bucket
    /// - **Window Size**: Up to 60 buckets (60 seconds total)
    /// - **Purpose**: Capture rapid access patterns and spikes
    short_buckets: Box<[AtomicU64]>,

    /// Current active bucket index for short window (circular buffer)
    short_current_bucket: AtomicUsize,

    /// Duration each short bucket represents (always 1 second)
    short_bucket_duration_secs: u64,

    /// Total number of short window buckets
    short_bucket_count: usize,

    /// Medium window buckets for trend analysis
    ///
    /// - **Granularity**: 5 seconds per bucket
    /// - **Window Size**: Up to 72 buckets (6 minutes total)
    /// - **Purpose**: Identify sustained access patterns
    medium_buckets: Box<[AtomicU64]>,

    /// Current active bucket index for medium window (circular buffer)
    medium_current_bucket: AtomicUsize,

    /// Duration each medium bucket represents (always 5 seconds)
    medium_bucket_duration_secs: u64,

    /// Total number of medium window buckets
    medium_bucket_count: usize,

    /// Last update timestamp (Unix epoch seconds)
    /// Used for bucket rotation and cleanup
    last_update: AtomicU64,

    /// Short window time span for frequency calculations
    short_window_size: Duration,

    /// Medium window time span for frequency calculations
    medium_window_size: Duration,
}

impl AccessStats {
    fn new(short_window_size: Duration, medium_window_size: Duration, _max_entries: usize) -> Self {
        // Short window: 1 second per bucket, up to 60 buckets (1 minute)
        let short_bucket_duration_secs = 1u64;
        let short_bucket_count =
            (short_window_size.as_secs() / short_bucket_duration_secs).clamp(10, 300) as usize;

        // Medium window: 5 seconds per bucket, up to 72 buckets (6 minutes)
        let medium_bucket_duration_secs = 5u64;
        let medium_bucket_count =
            (medium_window_size.as_secs() / medium_bucket_duration_secs).clamp(12, 180) as usize;

        debug!(
            "Creating AccessStats: short_window={:?} ({} buckets), medium_window={:?} ({} buckets)",
            short_window_size, short_bucket_count, medium_window_size, medium_bucket_count
        );

        let short_buckets = (0..short_bucket_count)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let medium_buckets = (0..medium_bucket_count)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            short_buckets,
            short_current_bucket: AtomicUsize::new(0),
            short_bucket_duration_secs,
            short_bucket_count,
            medium_buckets,
            medium_current_bucket: AtomicUsize::new(0),
            medium_bucket_duration_secs,
            medium_bucket_count,
            last_update: AtomicU64::new(now),
            short_window_size,
            medium_window_size,
        }
    }

    /// Record one access (lock-free operation)
    fn record_access(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        trace!("Recording access at timestamp: {}", now);

        // Update last access time
        self.last_update.store(now, Ordering::Relaxed);

        // Record in short window
        self.maybe_reset_short_bucket(now);
        let short_bucket_idx = self.calculate_short_bucket_index(now);
        let short_count = self.short_buckets[short_bucket_idx].fetch_add(1, Ordering::Relaxed);
        trace!(
            "Recorded in short bucket {}: count = {}",
            short_bucket_idx,
            short_count + 1
        );

        // Record in medium window
        self.maybe_reset_medium_bucket(now);
        let medium_bucket_idx = self.calculate_medium_bucket_index(now);
        let medium_count = self.medium_buckets[medium_bucket_idx].fetch_add(1, Ordering::Relaxed);
        trace!(
            "Recorded in medium bucket {}: count = {}",
            medium_bucket_idx,
            medium_count + 1
        );
    }

    /// Get weighted access frequency using both short and medium windows
    fn get_weighted_access_frequency(&self, short_weight: f64, medium_weight: f64) -> f64 {
        let short_freq = self.get_short_window_frequency();
        let medium_freq = self.get_medium_window_frequency();

        // Normalize weights
        let total_weight = short_weight + medium_weight;
        if total_weight == 0.0 {
            trace!("Both weights are 0, returning 0 frequency");
            return 0.0;
        }
        let short_norm = short_weight / total_weight;
        let medium_norm = medium_weight / total_weight;

        let weighted_freq = short_freq * short_norm + medium_freq * medium_norm;
        trace!(
            "Weighted frequency: short={} ({:.2}), medium={} ({:.2}) -> weighted={:.2}",
            short_freq, short_norm, medium_freq, medium_norm, weighted_freq
        );

        weighted_freq
    }

    /// Get short window access frequency
    fn get_short_window_frequency(&self) -> f64 {
        self.get_window_frequency(
            &self.short_buckets,
            self.short_current_bucket.load(Ordering::Relaxed),
            self.short_bucket_duration_secs,
            self.short_bucket_count,
            self.short_window_size,
        )
    }

    /// Get medium window access frequency
    fn get_medium_window_frequency(&self) -> f64 {
        self.get_window_frequency(
            &self.medium_buckets,
            self.medium_current_bucket.load(Ordering::Relaxed),
            self.medium_bucket_duration_secs,
            self.medium_bucket_count,
            self.medium_window_size,
        )
    }

    /// Generic method to calculate frequency for any window
    fn get_window_frequency(
        &self,
        buckets: &[AtomicU64],
        current_bucket_idx: usize,
        bucket_duration_secs: u64,
        bucket_count: usize,
        window_size: Duration,
    ) -> f64 {
        let window_bucket_count =
            (window_size.as_secs() / bucket_duration_secs).min(bucket_count as u64) as usize;

        if window_bucket_count == 0 {
            return 0.0;
        }

        let mut total = 0u64;

        // Traverse the last few buckets
        for i in 0..window_bucket_count {
            let bucket_idx = if current_bucket_idx >= i {
                current_bucket_idx - i
            } else {
                bucket_count - i + current_bucket_idx
            };
            total += buckets[bucket_idx].load(Ordering::Relaxed);
        }

        total as f64 / window_size.as_secs_f64()
    }

    /// Calculate the short window bucket index
    fn calculate_short_bucket_index(&self, timestamp: u64) -> usize {
        let bucket_num = timestamp / self.short_bucket_duration_secs;
        (bucket_num as usize) % self.short_bucket_count
    }

    /// Calculate the medium window bucket index
    fn calculate_medium_bucket_index(&self, timestamp: u64) -> usize {
        let bucket_num = timestamp / self.medium_bucket_duration_secs;
        (bucket_num as usize) % self.medium_bucket_count
    }

    /// Reset short bucket if needed
    fn maybe_reset_short_bucket(&self, now: u64) {
        let expected_bucket = self.calculate_short_bucket_index(now);
        let current = self.short_current_bucket.load(Ordering::Relaxed);

        if current != expected_bucket
            && self
                .short_current_bucket
                .compare_exchange_weak(
                    current,
                    expected_bucket,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            self.short_buckets[expected_bucket].store(0, Ordering::Relaxed);
            self.cleanup_old_short_buckets(now);
        }
    }

    /// Reset medium bucket if needed
    fn maybe_reset_medium_bucket(&self, now: u64) {
        let expected_bucket = self.calculate_medium_bucket_index(now);
        let current = self.medium_current_bucket.load(Ordering::Relaxed);

        if current != expected_bucket
            && self
                .medium_current_bucket
                .compare_exchange_weak(
                    current,
                    expected_bucket,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            self.medium_buckets[expected_bucket].store(0, Ordering::Relaxed);
            self.cleanup_old_medium_buckets(now);
        }
    }

    /// Clean up expired short window buckets
    fn cleanup_old_short_buckets(&self, _now: u64) {
        let window_buckets =
            (self.short_window_size.as_secs() / self.short_bucket_duration_secs) as usize;
        let current_bucket_idx = self.short_current_bucket.load(Ordering::Relaxed);

        for (i, bucket) in self.short_buckets.iter().enumerate() {
            let bucket_age =
                (current_bucket_idx + self.short_bucket_count - i) % self.short_bucket_count;

            if bucket_age >= window_buckets {
                bucket.store(0, Ordering::Relaxed);
            }
        }
    }

    /// Clean up expired medium window buckets
    fn cleanup_old_medium_buckets(&self, _now: u64) {
        let window_buckets =
            (self.medium_window_size.as_secs() / self.medium_bucket_duration_secs) as usize;
        let current_bucket_idx = self.medium_current_bucket.load(Ordering::Relaxed);

        for (i, bucket) in self.medium_buckets.iter().enumerate() {
            let bucket_age =
                (current_bucket_idx + self.medium_bucket_count - i) % self.medium_bucket_count;

            if bucket_age >= window_buckets {
                bucket.store(0, Ordering::Relaxed);
            }
        }
    }
}

/// System performance metrics collector for adaptive cache optimization.
///
/// This structure tracks key performance indicators that influence the cache's
/// promotion strategy, enabling dynamic adaptation to changing workload patterns.
///
/// # Metrics Tracked
///
/// - **Hit Rate**: Overall cache effectiveness (0.0 - 1.0)
/// - **Request Volume**: Total system load indicator
/// - **Cache Utilization**: Memory pressure indicator
///
/// # Adaptive Decision Logic
///
/// ```ignore
/// // High load scenario
/// if system_load > 0.8 {
///     // Be more aggressive: lower threshold by 30%
///     threshold *= 0.7;
/// }
///
/// // Low hit rate scenario
/// if hit_rate < 0.6 {
///     // Be more conservative: raise threshold by 30%
///     threshold *= 1.3;
/// }
/// ```
///
/// # Implementation Notes
///
/// All metrics are stored as scaled integers to avoid floating-point operations
/// in hot paths. Values are typically scaled by 10000 to maintain 4 decimal places.
#[derive(Debug)]
struct SystemMetrics {
    /// Cache hit rate stored as scaled integer (0-10000 representing 0.0-1.0)
    ///
    /// **Calculation**: (cache_hits * 10000) / total_requests
    /// **Usage**: Determines if cache strategy should be conservative or aggressive
    /// **Impact**: Low hit rates trigger conservative promotion to prevent pollution
    hit_rate: AtomicU64,

    /// Total number of cache requests (hits + misses)
    ///
    /// **Purpose**: System load indicator and hit rate denominator
    /// **Trend**: Increasing values indicate higher system activity
    total_requests: AtomicU64,

    /// Number of successful cache hits
    ///
    /// **Purpose**: Cache effectiveness measurement and hit rate numerator
    /// **Optimization Goal**: Maximize this value relative to total_requests
    cache_hits: AtomicU64,

    /// Hot cache utilization stored as scaled integer (0-10000 representing 0.0-1.0)
    ///
    /// **Calculation**: (current_size * 10000) / max_capacity
    /// **Purpose**: Memory pressure indicator for adaptive thresholding
    /// **Impact**: High utilization may trigger aggressive promotion to improve hit rate
    hot_cache_utilization: AtomicU64,

    /// Sliding window request tracking for accurate rate calculation
    ///
    /// **Purpose**: Track requests in recent time window to compute true request rate
    /// **Implementation**: Fixed-size circular buffer with time buckets
    /// **Benefit**: Prevents permanent drift in load calculation
    request_buckets: [AtomicU64; 60], // 60 buckets for 1-minute sliding window
    current_request_bucket: AtomicU64,
    last_request_advance: AtomicU64,
}

impl SystemMetrics {
    fn new() -> Self {
        debug!("Initializing SystemMetrics with 60 request buckets");
        Self {
            hit_rate: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            hot_cache_utilization: AtomicU64::new(0),
            request_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            current_request_bucket: AtomicU64::new(0),
            last_request_advance: AtomicU64::new(0),
        }
    }

    fn record_request(&self, hit: bool) {
        let total = self.total_requests.fetch_add(1, Ordering::Relaxed) + 1;
        let hits = if hit {
            self.cache_hits.fetch_add(1, Ordering::Relaxed) + 1
        } else {
            self.cache_hits.load(Ordering::Relaxed)
        };

        trace!(
            "Recording cache request: hit={}, total_requests={}, cache_hits={}",
            hit, total, hits
        );

        // Update sliding window request tracking
        self.advance_request_buckets();
        let current_bucket = self.current_request_bucket.load(Ordering::Relaxed) as usize;
        self.request_buckets[current_bucket].fetch_add(1, Ordering::Relaxed);

        self.update_hit_rate();
    }

    /// Advance request buckets based on current time
    fn advance_request_buckets(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let last_advance = self.last_request_advance.load(Ordering::Relaxed);

        if now <= last_advance {
            return;
        }

        // Calculate how many buckets to advance
        let buckets_to_advance = (now - last_advance).min(60) as usize; // Cap at 60 to avoid wrapping multiple times

        if buckets_to_advance == 0 {
            return;
        }

        // Try to update last_advance time, bail out if another thread beat us to it
        match self.last_request_advance.compare_exchange(
            last_advance,
            now,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                // We successfully updated the time, now advance the buckets
                let mut bucket = self.current_request_bucket.load(Ordering::Relaxed) as usize;
                for _ in 0..buckets_to_advance {
                    bucket = (bucket + 1) % 60;
                    self.current_request_bucket
                        .store(bucket as u64, Ordering::Relaxed);
                    // Clear the new bucket
                    self.request_buckets[bucket].store(0, Ordering::Relaxed);
                }
            }
            Err(_) => {
                // Another thread updated the time, just return
                // The next call will advance if needed
            }
        }
    }

    /// Get request rate from sliding window (requests per second)
    fn get_request_rate(&self) -> f64 {
        self.advance_request_buckets(); // Ensure buckets are up to date

        let mut total_requests = 0u64;
        for bucket in &self.request_buckets {
            total_requests += bucket.load(Ordering::Relaxed);
        }

        total_requests as f64 / 60.0 // requests per second over 1-minute window
    }

    fn update_hit_rate(&self) {
        let total = self.total_requests.load(Ordering::Relaxed);
        if total > 0 {
            let hits = self.cache_hits.load(Ordering::Relaxed);
            let rate = hits
                .checked_mul(10000)
                .and_then(|value| value.checked_div(total))
                .unwrap_or(0); // Scale to 0-10000 for 0.0-1.0
            self.hit_rate.store(rate, Ordering::Relaxed);
        }
    }

    fn get_hit_rate(&self) -> f64 {
        self.hit_rate.load(Ordering::Relaxed) as f64 / 10000.0
    }

    fn get_system_load(&self) -> f64 {
        // Simple heuristic: high cache utilization = high system load
        let utilization = self.hot_cache_utilization.load(Ordering::Relaxed) as f64 / 10000.0;
        let request_rate = self.get_request_rate(); // Use sliding window rate instead of total count

        // Combine utilization and request rate for load estimate
        // This is a simplified calculation - in practice you might use CPU/memory metrics
        utilization * 0.7 + (request_rate / 100.0).min(1.0) * 0.3 // Adjusted scaling for RPS
    }

    fn get_hot_cache_utilization(&self) -> f64 {
        self.hot_cache_utilization.load(Ordering::Relaxed) as f64 / 10000.0
    }

    fn update_cache_utilization(&self, current_size: u64, max_size: u64) {
        if max_size > 0 {
            let utilization = current_size
                .checked_mul(10000)
                .and_then(|value| value.checked_div(max_size))
                .unwrap_or(0);
            self.hot_cache_utilization
                .store(utilization, Ordering::Relaxed);
        }
    }
}

/// Intelligent cache promotion policy engine.
///
/// This is the brain of the adaptive caching system, combining access pattern analysis
/// with system performance metrics to make intelligent promotion decisions.
///
/// # Decision Algorithm
///
/// The promotion decision follows this multi-step process:
///
/// ```ignore
/// // 1. Calculate adaptive threshold based on system state
/// let adaptive_threshold = base_threshold * load_factor * hitrate_factor;
///
/// // 2. Get weighted access frequency from multiple time windows
/// let weighted_freq = short_freq * short_weight + medium_freq * medium_weight;
///
/// // 3. Make final decision
/// let should_promote = weighted_freq >= adaptive_threshold;
/// ```
///
/// # Adaptive Threshold Factors
///
/// ## Load Factor
/// - **High Load** (>0.8): `factor = 0.7` (30% more aggressive)
/// - **Normal Load**: `factor = 1.0` (baseline)
///
/// ## Hit Rate Factor
/// - **Low Hit Rate** (<0.6): `factor = 0.5..0.75` (faster warmup)
/// - **High Hit Rate** (>0.8): `factor = 0.9` (10% more aggressive)
/// - **Normal Hit Rate**: `factor = 1.0` (baseline)
///
/// # Thread Safety
///
/// This struct uses `Arc<RwLock<>>` for access stats and `Arc<>` for system metrics,
/// allowing safe concurrent access from multiple threads while maintaining consistency.
#[derive(Debug, Clone)]
struct Policy {
    /// Per-key access statistics with dual time window analysis
    ///
    /// Stores `AccessStats` for each cache key, tracking both short-term burst
    /// patterns and medium-term trends. Protected by RwLock for safe concurrent access.
    access_stats: Arc<RwLock<HashMap<String, AccessStats>>>,

    /// Global system performance metrics
    ///
    /// Tracks hit rates, request volumes, and cache utilization to inform
    /// adaptive threshold decisions. Uses atomic operations for lock-free access.
    system_metrics: Arc<SystemMetrics>,

    /// Short time window configuration for burst detection
    short_window_size: Duration,

    /// Medium time window configuration for trend analysis
    medium_window_size: Duration,

    /// Maximum number of entries to track (legacy parameter)
    max_entries: usize,

    /// Base promotion threshold before adaptive adjustments
    base_promotion_threshold: f64,

    /// Weight for short window frequency in promotion decisions
    short_window_weight: f64,

    /// Weight for medium window frequency in promotion decisions
    medium_window_weight: f64,

    /// Enable/disable adaptive threshold adjustment
    enable_adaptive_threshold: bool,

    /// System load threshold triggering aggressive promotion mode
    aggressive_promotion_load_threshold: f64,

    /// Hit rate threshold triggering conservative promotion mode
    conservative_promotion_hit_rate_threshold: f64,
}

impl Policy {
    #[allow(clippy::too_many_arguments)]
    fn new(
        short_window_size: Duration,
        medium_window_size: Duration,
        max_entries: usize,
        base_promotion_threshold: f64,
        short_window_weight: f64,
        medium_window_weight: f64,
        enable_adaptive_threshold: bool,
        aggressive_promotion_load_threshold: f64,
        conservative_promotion_hit_rate_threshold: f64,
    ) -> Self {
        info!(
            "Creating Policy with configuration: base_threshold={:.2}, short_weight={:.2}, medium_weight={:.2}, adaptive={}",
            base_promotion_threshold,
            short_window_weight,
            medium_window_weight,
            enable_adaptive_threshold
        );

        Policy {
            access_stats: Arc::new(RwLock::new(HashMap::new())),
            system_metrics: Arc::new(SystemMetrics::new()),
            short_window_size,
            medium_window_size,
            max_entries,
            base_promotion_threshold,
            short_window_weight,
            medium_window_weight,
            enable_adaptive_threshold,
            aggressive_promotion_load_threshold,
            conservative_promotion_hit_rate_threshold,
        }
    }

    /// Calculate adaptive promotion threshold based on system conditions
    fn calculate_adaptive_threshold(&self) -> f64 {
        if !self.enable_adaptive_threshold {
            trace!(
                "Adaptive threshold disabled, using base threshold: {:.2}",
                self.base_promotion_threshold
            );
            return self.base_promotion_threshold;
        }

        let system_load = self.system_metrics.get_system_load();
        let hit_rate = self.system_metrics.get_hit_rate();

        let mut threshold = self.base_promotion_threshold;
        let mut adjustments = Vec::new();

        // Adjust based on hit rate. A miss-heavy startup needs faster
        // promotion; once hit rate recovers, return to the normal threshold.
        if hit_rate < 0.3 {
            threshold *= 0.5;
            adjustments.push(format!("very low hit rate ({:.2}): *0.5", hit_rate));
        } else if hit_rate < self.conservative_promotion_hit_rate_threshold {
            threshold *= 0.75;
            adjustments.push(format!("low hit rate ({:.2}): *0.75", hit_rate));
        } else if hit_rate > 0.8 {
            // High hit rate: be more aggressive to maintain good performance
            threshold *= 0.9;
            adjustments.push(format!("high hit rate ({:.2}): *0.9", hit_rate));
        }

        let hot_utilization = self.system_metrics.get_hot_cache_utilization();
        if hot_utilization > 0.95 {
            threshold *= 1.6;
            adjustments.push(format!("hot cache critical ({:.2}): *1.6", hot_utilization));
        } else if hot_utilization > 0.85 {
            threshold *= 1.3;
            adjustments.push(format!("hot cache high ({:.2}): *1.3", hot_utilization));
        }

        if system_load > self.aggressive_promotion_load_threshold {
            threshold *= 1.2;
            adjustments.push(format!("high system load ({:.2}): *1.2", system_load));
        }

        // Ensure threshold stays within reasonable bounds
        let final_threshold = threshold.clamp(2.0, 50.0);

        debug!(
            "Adaptive threshold calculation: base={:.2}, system_load={:.2}, hit_rate={:.2}, adjustments=[{}], final={:.2}",
            self.base_promotion_threshold,
            system_load,
            hit_rate,
            adjustments.join(", "),
            final_threshold
        );

        final_threshold
    }

    async fn record_access(&self, key: String) {
        trace!("Recording access for key: {}", key);

        // Use read lock to get or create AccessStats
        {
            let stats = self.access_stats.read().await;
            if let Some(entry) = stats.get(&key) {
                // If exists, directly record access (no need for write lock)
                trace!("Found existing access stats for key: {}", key);
                entry.record_access();
                return;
            }
        }

        // If not exists, get write lock to create
        trace!("Creating new access stats for key: {}", key);
        let mut stats = self.access_stats.write().await;
        // Double check, prevent other threads from creating in the meantime
        if let Some(entry) = stats.get(&key) {
            trace!("Access stats created by another thread for key: {}", key);
            entry.record_access();
        } else {
            debug!("Creating new AccessStats entry for key: {}", key);
            let entry = AccessStats::new(
                self.short_window_size,
                self.medium_window_size,
                self.max_entries,
            );
            entry.record_access();
            stats.insert(key, entry);
        }
    }

    async fn should_promote(&self, key: String) -> bool {
        // Calculate adaptive threshold
        let threshold = self.calculate_adaptive_threshold();
        trace!(
            "Calculated promotion threshold for key '{}': {:.2}",
            key, threshold
        );

        // Use read lock to check promotion conditions, reduce lock contention
        let stats = self.access_stats.read().await;
        if let Some(entry) = stats.get(&key) {
            // Use weighted frequency from both time windows
            let weighted_frequency = entry
                .get_weighted_access_frequency(self.short_window_weight, self.medium_window_weight);

            let should_promote = weighted_frequency >= threshold;
            debug!(
                "Promotion decision for key '{}': frequency={:.2}, threshold={:.2}, should_promote={}",
                key, weighted_frequency, threshold, should_promote
            );

            should_promote
        } else {
            trace!("No access stats found for key '{}', not promoting", key);
            false
        }
    }

    /// Record cache request for metrics tracking
    fn record_cache_request(&self, hit: bool) {
        trace!("Recording cache request: hit={}", hit);
        self.system_metrics.record_request(hit);
    }

    /// Update cache utilization metrics
    fn update_cache_utilization(&self, current_size: u64, max_size: u64) {
        let utilization = if max_size > 0 {
            current_size as f64 / max_size as f64
        } else {
            0.0
        };
        trace!(
            "Updating cache utilization: {}/{} ({:.2}%)",
            current_size,
            max_size,
            utilization * 100.0
        );
        self.system_metrics
            .update_cache_utilization(current_size, max_size);
    }

    /// Clean up old entries
    async fn cleanup_old_entries(&self, max_idle_duration: Duration) {
        let mut stats = self.access_stats.write().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        stats.retain(|_, entry| {
            let last_update = entry.last_update.load(Ordering::Relaxed);
            let idle_duration = now.saturating_sub(last_update);
            idle_duration < max_idle_duration.as_secs()
        });
    }
}

/// High-performance adaptive dual-layer cache system.
///
/// This cache implements a sophisticated three-tier storage architecture with intelligent
/// promotion strategies based on access patterns and system performance metrics.
///
/// # Architecture Tiers
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────┐
/// │                    Request Flow                              │
/// ├─────────────────────────────────────────────────────────────┤
/// │ 1. Hot Cache (Memory)     ← Fastest, O(1) lookup            │
/// │    - Size: 1024 items      - Stores actual data             │
/// │    - TTL: 120s            - For frequently accessed items   │
/// │    - TTI: 30s             - Adaptive promotion based on     │
/// │                           - access frequency & system load  │
/// │                                                             │
/// │ 2. Cold Cache (Memory)     ← Fast, O(1) metadata lookup     │
/// │    - Size: 1024 items      - Tracks all accessed keys       │
/// │    - TTL: 120s            - Enables access pattern analysis │
/// │    - TTI: 30s             - Lightweight key tracking        │
/// │                                                             │
/// │ 3. Disk Storage (SSD/HDD)  ← Slower, but persistent        │
/// │    - Unlimited size       - SHA256-based file naming       │
/// │    - System cache dir     - Fallback for all data           │
/// │    - Async I/O            - Handles large files efficiently │
/// └─────────────────────────────────────────────────────────────┘
/// ```
///
/// # Promotion Strategy
///
/// The cache uses an intelligent promotion algorithm that considers:
///
/// 1. **Dual Time Windows**: Short-term (10s) and medium-term (60s) access patterns
/// 2. **Weighted Frequency**: Combines burst detection with trend analysis
/// 3. **Adaptive Thresholding**: Adjusts based on system load and hit rates
/// 4. **System Metrics**: Real-time performance feedback
///
/// # Performance Characteristics
///
/// - **Hot Cache Hit**: ~50ns (memory access)
/// - **Cold Cache Hit + Promotion**: ~1-10μs (memory + disk I/O)
/// - **Cold Cache Miss + Disk Load**: ~1-10ms (disk I/O)
/// - **Concurrent Access**: Lock-free for reads, minimal contention for writes
///
/// # Usage Examples
///
/// ## Basic Usage
/// ```ignore
/// # use brewfs::chunk::cache::ChunksCache;
/// # async fn demo() -> anyhow::Result<()> {
/// let cache = ChunksCache::new().await?;
/// cache.insert("key1", &data).await?;
/// let value = cache.get(&"key1".to_string()).await?;
/// # Ok(())
/// # }
/// ```
///
/// ## Custom Configuration
/// ```ignore
/// let config = ChunksCacheConfig {
///     base_promotion_threshold: 5.0,        // More aggressive
///     short_window_weight: 0.8,            // Prioritize bursts
///     enable_adaptive_threshold: true,      // Enable adaptation
///     ..Default::default()
/// };
/// let cache = ChunksCache::new_with_config(config).await?;
/// ```
///
/// ## Performance-Tuned Configuration
/// ```ignore
/// use std::time::Duration;
///
/// let config = ChunksCacheConfig {
///     hot_cache_size: 2048,                 // Larger hot cache
///     base_promotion_threshold: 3.0,        // Very aggressive
///     short_window_size: Duration::from_millis(500),   // Faster response
///     short_window_weight: 0.9,            // Heavily prefer bursts
///     aggressive_promotion_load_threshold: 0.6,    // Earlier aggression
///     ..Default::default()
/// };
/// ```
///
/// # Thread Safety
///
/// This cache is fully thread-safe and designed for high-concurrency environments:
/// - All operations are async and non-blocking
/// - Access statistics use lock-free atomic operations
/// - Cache operations use Moka's concurrent-safe implementation
///
/// Cache statistics for monitoring and diagnostics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hot_bytes: u64,
    pub hot_entries: u64,
    pub max_hot_bytes: u64,
    pub write_hot_bytes: u64,
    pub write_hot_entries: u64,
    pub max_write_hot_bytes: u64,
    pub disk_bytes: u64,
    pub max_disk_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

/// # Memory Management
///
/// - **Hot Cache**: Stores actual data, limited by `hot_cache_size`
/// - **Cold Cache**: Stores only `()` markers, minimal memory overhead
/// - **Access Stats**: Per-key statistics, automatically cleaned up when idle
/// - **Disk Storage**: Uses system temp directory, respects available space
#[derive(Clone)]
pub struct ChunksCache {
    /// Persistent disk storage backend with SHA256-based file naming
    disk_storage: DiskStorage,

    /// Hot cache tier storing frequently accessed data in memory
    /// Uses Moka's high-performance concurrent cache implementation
    hot_cache: moka::future::Cache<String, bytes::Bytes>,

    /// Approximate hot cache bytes (sum of Bytes lengths)
    hot_bytes: Arc<AtomicU64>,

    /// Recently uploaded write data. This protects read-after-write workloads
    /// from the normal TinyLFU admission policy, where older read-hot blocks can
    /// reject newly written blocks that have not yet built read frequency.
    write_hot_cache: RecentWriteHotCache,

    /// Cold cache tier tracking all accessed keys for pattern analysis
    /// Stores empty tuples () as lightweight metadata markers
    cold_cache: moka::future::Cache<String, ()>,

    /// Keys currently being persisted to disk by opportunistic inserts.
    disk_insert_inflight: Arc<DashSet<String>>,

    /// Intelligent promotion policy engine with adaptive thresholding
    policy: Policy,

    /// Cache configuration parameters (stored for runtime adjustments)
    config: ChunksCacheConfig,

    /// Read-path cache hit counter (hot cache + disk cache)
    pub cache_hits: Arc<AtomicU64>,
    /// Read-path cache miss counter (fell through to S3)
    pub cache_misses: Arc<AtomicU64>,
}

impl ChunksCache {
    /// Creates a new ChunksCache with default configuration
    pub async fn new() -> anyhow::Result<Self> {
        Self::new_with_config(ChunksCacheConfig::default()).await
    }

    /// Creates a new ChunksCache with custom configuration
    pub async fn new_with_config(mut config: ChunksCacheConfig) -> anyhow::Result<Self> {
        debug!(
            "Creating new ChunksCache with configuration: hot_cache_size={}, cold_cache_size={}, max_hot_bytes={}, max_disk_bytes={}, base_promotion_threshold={}",
            config.hot_cache_size,
            config.cold_cache_size,
            config.max_hot_bytes,
            config.max_disk_bytes,
            config.base_promotion_threshold
        );

        let cache_dir = config
            .disk_storage_dir
            .take()
            .unwrap_or_else(|| cache_dir().unwrap());
        debug!("Using cache directory: {:?}", cache_dir);
        let disk_storage = DiskStorage::new_with_integrity(
            cache_dir,
            config.max_disk_bytes,
            config.disk_integrity_mode,
        )
        .await?;

        let hot_bytes = Arc::new(AtomicU64::new(0));
        let hot_bytes_evict = hot_bytes.clone();
        let max_write_hot_bytes = recent_write_hot_capacity(config.max_hot_bytes);
        // Use byte-weighted capacity: moka evicts entries when total weight exceeds max_capacity.
        // The weigher returns the byte size of each entry (clamped to u32::MAX).
        let hot_cache_builder = moka::future::Cache::builder()
            .max_capacity(config.max_hot_bytes)
            .weigher(|_key: &String, value: &bytes::Bytes| -> u32 {
                // Each entry's weight is its byte size (with overhead estimate for key + metadata)
                (value.len() as u64 + 64).min(u32::MAX as u64) as u32
            })
            .time_to_idle(Duration::from_secs(300))
            .time_to_live(Duration::from_secs(3600))
            .eviction_listener(move |_key, value: bytes::Bytes, _cause| {
                // Saturating sub to prevent underflow from racing insert_hot/eviction
                let _ = hot_bytes_evict.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(value.len() as u64))
                });
            });
        let cold_cache_builder = moka::future::Cache::builder()
            .max_capacity(config.cold_cache_size as u64)
            .time_to_idle(Duration::from_secs(300))
            .time_to_live(Duration::from_secs(3600));

        debug!(
            "Creating policy with adaptive threshold: {}",
            config.enable_adaptive_threshold
        );
        let policy = Policy::new(
            config.short_window_size,
            config.medium_window_size,
            config.max_access_entries,
            config.base_promotion_threshold,
            config.short_window_weight,
            config.medium_window_weight,
            config.enable_adaptive_threshold,
            config.aggressive_promotion_load_threshold,
            config.conservative_promotion_hit_rate_threshold,
        );

        info!("ChunksCache created successfully");
        Ok(Self {
            disk_storage,
            hot_cache: hot_cache_builder.build(),
            hot_bytes,
            write_hot_cache: RecentWriteHotCache::new(max_write_hot_bytes),
            cold_cache: cold_cache_builder.build(),
            disk_insert_inflight: Arc::new(DashSet::new()),
            policy,
            config,
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn get(&self, key: &String) -> Option<bytes::Bytes> {
        if let Some(value) = self.write_hot_cache.get(key) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            trace!(
                "Recent-write hot cache HIT: {} ({} bytes)",
                key,
                value.len()
            );
            self.policy.record_cache_request(true);
            return Some(value);
        }

        // Check hot cache first — fastest path, no promotion tracking needed.
        if let Some(value) = self.hot_cache.get(key).await {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            trace!("Hot cache HIT: {} ({} bytes)", key, value.len());
            self.policy.record_cache_request(true);
            return Some(value);
        }

        trace!("Hot cache MISS: {}", key);
        self.policy.record_cache_request(false);

        // Try loading from disk directly — the cold_cache index may have
        // evicted the key marker but the file can still exist on disk
        // (populated by write-through or prior reads).
        let value = match self.disk_storage.load_with_health(key).await {
            Ok(Some(value)) if !value.is_empty() => value,
            Ok(_) => {
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            Err(_) => {
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };

        self.cache_hits.fetch_add(1, Ordering::Relaxed);
        debug!("Loaded {} bytes from disk for key: {}", value.len(), key);

        // Record access only for disk hits — drives promotion decisions.
        self.policy.record_access(key.clone()).await;

        // Re-populate cold cache index so future lookups are faster
        self.cold_cache.insert(key.clone(), ()).await;

        if self.should_fast_promote_disk_hit(value.len())
            || self.policy.should_promote(key.clone()).await
        {
            debug!("Promoting key to hot cache: {}", key);
            self.insert_hot(key, value.clone()).await;
        }

        self.update_utilization_metrics();
        Some(value)
    }

    pub async fn get_range_into(
        &self,
        key: &String,
        offset: usize,
        buf: &mut [u8],
    ) -> Option<usize> {
        if buf.is_empty() {
            return Some(0);
        }

        if let Some(value) = self.write_hot_cache.get(key) {
            if let Some(read_len) = copy_full_cached_range(&value, offset, buf) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                trace!(
                    "Recent-write hot cache range HIT: {} ({} bytes)",
                    key,
                    value.len()
                );
                self.policy.record_cache_request(true);
                return Some(read_len);
            }

            trace!(
                "Recent-write hot cache range MISS: {} ({} bytes too short for offset={} len={})",
                key,
                value.len(),
                offset,
                buf.len()
            );
        }

        if let Some(value) = self.hot_cache.get(key).await {
            if let Some(read_len) = copy_full_cached_range(&value, offset, buf) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                trace!("Hot cache range HIT: {} ({} bytes)", key, value.len());
                self.policy.record_cache_request(true);
                return Some(read_len);
            }

            trace!(
                "Hot cache range MISS: {} ({} bytes too short for offset={} len={})",
                key,
                value.len(),
                offset,
                buf.len()
            );
        }

        trace!("Hot cache range MISS: {}", key);
        self.policy.record_cache_request(false);

        let mut disk_buf = vec![0u8; buf.len()];
        let read_len = match self
            .disk_storage
            .load_range_with_health(key, offset as u64, &mut disk_buf)
            .await
        {
            Ok(Some(read_len)) => read_len,
            Ok(None) | Err(_) => {
                self.cache_misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };

        if read_len != buf.len() {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            trace!(
                "Disk cache range MISS: {} (read {} bytes, requested {})",
                key,
                read_len,
                buf.len()
            );
            return None;
        }

        buf.copy_from_slice(&disk_buf);

        self.cache_hits.fetch_add(1, Ordering::Relaxed);
        debug!(
            "Loaded {} ranged bytes from disk for key: {}",
            read_len, key
        );
        self.policy.record_access(key.clone()).await;
        self.cold_cache.insert(key.clone(), ()).await;
        self.update_utilization_metrics();
        Some(read_len)
    }

    fn should_fast_promote_disk_hit(&self, value_len: usize) -> bool {
        if value_len == 0 || self.config.max_hot_bytes == 0 {
            return false;
        }

        let current_bytes = self.hot_cache.weighted_size();
        let next_bytes = current_bytes
            .saturating_add(value_len as u64)
            .saturating_add(64);
        let fast_promote_ceiling = self
            .config
            .max_hot_bytes
            .saturating_mul(DISK_HIT_FAST_PROMOTE_MAX_UTILIZATION_PER_MILLE)
            / 1000;

        next_bytes <= fast_promote_ceiling
    }

    /// Update cache utilization metrics (using byte-based utilization)
    fn update_utilization_metrics(&self) {
        let hot_bytes = self.hot_cache.weighted_size();
        let max_bytes = self.config.max_hot_bytes;
        // Convert byte utilization to entry-like scale for the policy
        let utilization_scaled = hot_bytes
            .checked_mul(10000)
            .and_then(|bytes| bytes.checked_div(max_bytes))
            .unwrap_or(0)
            .min(10000);
        trace!(
            "Updating cache utilization: {:.1} MiB / {:.1} MiB hot, disk: {:.1} MiB / {:.1} MiB",
            hot_bytes as f64 / 1048576.0,
            max_bytes as f64 / 1048576.0,
            self.disk_storage.bytes_used() as f64 / 1048576.0,
            self.config.max_disk_bytes as f64 / 1048576.0,
        );
        self.policy
            .update_cache_utilization(utilization_scaled, 10000);
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hot_bytes: self.hot_cache.weighted_size(),
            hot_entries: self.hot_cache.entry_count(),
            max_hot_bytes: self.config.max_hot_bytes,
            write_hot_bytes: self.write_hot_cache.weighted_size(),
            write_hot_entries: self.write_hot_cache.entry_count(),
            max_write_hot_bytes: self.write_hot_cache.max_bytes(),
            disk_bytes: self.disk_storage.bytes_used(),
            max_disk_bytes: self.config.max_disk_bytes,
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
        }
    }

    pub async fn insert_hot(&self, key: &str, data: bytes::Bytes) {
        let len = data.len() as u64;
        self.hot_cache.insert(key.to_owned(), data).await;
        self.hot_cache.run_pending_tasks().await;
        self.hot_bytes.fetch_add(len, Ordering::Relaxed);
    }

    pub async fn insert_recent_write_hot(&self, key: &str, data: bytes::Bytes) {
        self.write_hot_cache.insert(key.to_owned(), data);
    }

    pub async fn insert_opportunistic(&self, key: String, data: bytes::Bytes) {
        // Insert into hot memory cache (fast path for subsequent reads).
        // Bytes::clone() is an Arc bump — zero-copy.
        self.insert_hot(&key, data.clone()).await;

        // Persist to disk so future cold starts / hot cache evictions avoid
        // S3, but keep this genuinely opportunistic. If local cache I/O is
        // saturated, skip the disk write instead of queuing more background
        // work that can compete with foreground reads and writes.
        if self.cold_cache.get(&key).await.is_some() {
            return;
        }

        let Some(permit) = self.disk_storage.try_io_permit(&key) else {
            tracing::debug!(
                key = %key,
                "disk cache insert skipped: write_sem saturated"
            );
            return;
        };

        if !self.disk_insert_inflight.insert(key.clone()) {
            return;
        }

        let disk_storage = self.disk_storage.clone();
        let cold_cache = self.cold_cache.clone();
        let inflight = self.disk_insert_inflight.clone();
        let cached_key = key.clone();
        tokio::spawn(async move {
            // Ensure inflight is always cleared when this task exits.
            struct ClearInFlight {
                inflight: Arc<DashSet<String>>,
                key: String,
            }
            impl Drop for ClearInFlight {
                fn drop(&mut self) {
                    self.inflight.remove(&self.key);
                }
            }
            let _clear = ClearInFlight {
                inflight: inflight.clone(),
                key: cached_key.clone(),
            };

            if cold_cache.get(&cached_key).await.is_some() {
                return;
            }

            let res = disk_storage
                .store_with_permit_health(&cached_key, data, permit)
                .await;

            match res {
                Ok(true) => {
                    cold_cache.insert(cached_key.clone(), ()).await;
                }
                Ok(false) => {
                    // Bypassed by health check — no error, no cold marker
                }
                Err(err) => {
                    warn!(error = ?err, key = %cached_key, "disk cache store failed");
                }
            }
        });
    }

    pub async fn is_disk_cached(&self, key: &str) -> bool {
        self.cold_cache.get(key).await.is_some()
    }

    pub fn hit_rate(&self) -> f64 {
        self.policy.system_metrics.get_hit_rate()
    }

    pub async fn insert(&self, key: &str, data: &Vec<u8>) -> anyhow::Result<()> {
        self.insert_hot(key, bytes::Bytes::from(data.clone())).await;
        if self.disk_storage.store_with_health(key, data).await? {
            self.cold_cache.insert(key.to_owned(), ()).await;
        }
        Ok(())
    }

    pub async fn remove(&self, key: &String) -> anyhow::Result<()> {
        debug!("Cache REMOVE request for key: {}", key);
        trace!("Invalidating from hot cache: {}", key);
        self.hot_cache.invalidate(key).await;
        // self.disk_storage.remove(key).await?;
        trace!("Invalidating from cold cache: {}", key);
        self.cold_cache.invalidate(key).await;

        debug!("Successfully removed key: {}", key);
        Ok(())
    }

    /// Try to acquire a disk write permit without blocking.
    /// Returns None if disk I/O is saturated (all write_sem permits taken).
    pub fn try_disk_store_permit(&self, key: &str) -> Option<OwnedSemaphorePermit> {
        self.disk_storage.try_io_permit(key)
    }

    /// Store data to disk cache, awaiting a write permit if necessary.
    /// Used by background write-cache population tasks.
    pub async fn store_to_disk(&self, key: &str, data: bytes::Bytes) -> anyhow::Result<()> {
        let permit = self.disk_storage.write_sem.clone().acquire_owned().await?;
        if self
            .disk_storage
            .store_with_permit_health(key, data, permit)
            .await?
        {
            self.cold_cache.insert(key.to_owned(), ()).await;
        }
        Ok(())
    }

    /// Store data to disk cache using a pre-acquired permit.
    /// Used by write-path cache population to avoid blocking on permit acquisition.
    pub async fn store_to_disk_with_permit(
        &self,
        key: &str,
        data: bytes::Bytes,
        permit: OwnedSemaphorePermit,
    ) -> anyhow::Result<()> {
        if self
            .disk_storage
            .store_with_permit_health(key, data, permit)
            .await?
        {
            self.cold_cache.insert(key.to_owned(), ()).await;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::cache_health::DiskHealth;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio;

    // Test helper: create a temporary storage directory
    async fn setup_test_storage() -> (DiskStorage, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let storage = DiskStorage::new(temp_dir.path(), 0).await.unwrap();
        (storage, temp_dir)
    }

    // Test helper: generate sample data
    fn generate_test_data(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 256) as u8).collect()
    }

    #[tokio::test]
    async fn test_new_creates_directory() {
        let temp_dir = tempdir().unwrap();
        let dir_path = temp_dir.path().join("subdir");

        // Ensure the directory does not exist
        assert!(!dir_path.exists());

        let _storage = DiskStorage::new(&dir_path, 0).await.unwrap();
        assert!(dir_path.exists());
        assert!(dir_path.is_dir());
    }

    #[tokio::test]
    async fn test_new_existing_directory() {
        let temp_dir = tempdir().unwrap();

        // Directory already exists
        assert!(temp_dir.path().exists());

        let _storage = DiskStorage::new(temp_dir.path(), 0).await.unwrap();
        assert!(temp_dir.path().exists());
    }

    #[test]
    fn test_etag_to_filename_special_characters() {
        let binding = "a".repeat(1000);
        let etags = vec![
            "normal",
            "etag-with-dashes",
            "etag_with_underscores",
            "etag with spaces",
            "etag@with#special$chars%",
            "ChineseLabel",
            "🚀emoji-etag",
            "",       // Empty string
            "a",      // Single character
            &binding, // Long string
        ];

        for etag in etags {
            let filename = DiskStorage::key_to_filename(etag);
            assert!(!filename.is_empty());
            // Filenames should be valid (no path separators, etc.)
            assert!(!filename.contains('/'));
            assert!(!filename.contains('\\'));
            assert!(!filename.contains(':'));
        }
    }

    #[tokio::test]
    async fn test_store_and_load_basic() {
        let (storage, _temp_dir) = setup_test_storage().await;
        let etag = "test_etag_1";
        let test_data = b"Hello, World!".to_vec();

        // Store the data
        storage.store(etag, &test_data).await.unwrap();

        // Load the data
        let loaded_data = storage.load(etag).await.unwrap();
        assert_eq!(loaded_data, test_data);
    }

    #[tokio::test]
    async fn test_store_and_load_large_data() {
        let (storage, _temp_dir) = setup_test_storage().await;
        let etag = "large_data_etag";

        // Generate 1 MiB of test data
        let large_data = generate_test_data(1024 * 1024);

        storage.store(etag, &large_data).await.unwrap();
        let loaded_data = storage.load(etag).await.unwrap();
        assert_eq!(loaded_data, large_data);
    }

    #[tokio::test]
    async fn test_load_range_ignores_corruption_outside_requested_checksum_block() {
        use std::io::{Seek, SeekFrom, Write};

        const HEADER_LEN: u64 = 8;
        const CHECKSUM_BLOCK: usize = 32 * 1024;

        let temp_dir = tempdir().unwrap();
        let storage = DiskStorage::new_with_integrity(temp_dir.path(), 0, CacheIntegrityMode::Full)
            .await
            .unwrap();
        let etag = "range_read_etag";
        let test_data = generate_test_data(CHECKSUM_BLOCK * 3);

        storage.store(etag, &test_data).await.unwrap();

        let filename = DiskStorage::key_to_filename(etag);
        let filepath = temp_dir.path().join(filename);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&filepath)
            .unwrap();
        file.seek(SeekFrom::Start(HEADER_LEN + (CHECKSUM_BLOCK * 2) as u64))
            .unwrap();
        file.write_all(&[0xFF]).unwrap();

        let offset = 1024;
        let mut out = vec![0u8; 4096];
        let read_len = storage
            .load_range_with_health(etag, offset as u64, &mut out)
            .await
            .unwrap();

        assert_eq!(read_len, Some(out.len()));
        assert_eq!(out, test_data[offset..offset + 4096]);
    }

    #[tokio::test]
    async fn test_store_and_load_empty_data() {
        let (storage, _temp_dir) = setup_test_storage().await;
        let etag = "empty_data_etag";
        let empty_data = vec![];

        storage.store(etag, &empty_data).await.unwrap();
        let loaded_data = storage.load(etag).await.unwrap();
        assert_eq!(loaded_data, empty_data);
    }

    #[tokio::test]
    async fn test_store_overwrite() {
        let (storage, _temp_dir) = setup_test_storage().await;
        let etag = "overwrite_etag";

        let data1 = b"First version".to_vec();
        let data2 = b"Second version".to_vec();

        storage.store(etag, &data1).await.unwrap();
        storage.store(etag, &data2).await.unwrap(); // Should overwrite the first copy

        let loaded_data = storage.load(etag).await.unwrap();
        assert_eq!(loaded_data, data2); // Should match the second version
    }

    #[tokio::test]
    async fn test_load_nonexistent_file() {
        let (storage, _temp_dir) = setup_test_storage().await;
        let etag = "nonexistent_etag";

        let result = storage.load(etag).await;
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("does not exist"));
    }

    #[tokio::test]
    async fn test_remove_existing_file() {
        let (storage, _temp_dir) = setup_test_storage().await;
        let etag = "to_remove_etag";
        let test_data = b"Data to remove".to_vec();

        storage.store(etag, &test_data).await.unwrap();
        assert!(storage.load(etag).await.is_ok()); // File exists

        storage.remove(etag).await.unwrap();
        assert!(storage.load(etag).await.is_err()); // File should have been removed
    }

    #[tokio::test]
    async fn test_remove_nonexistent_file() {
        let (storage, _temp_dir) = setup_test_storage().await;
        let etag = "nonexistent_remove_etag";

        let result = storage.remove(etag).await;
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("does not exist"));
    }

    #[tokio::test]
    async fn test_multiple_operations_same_etag() {
        let (storage, _temp_dir) = setup_test_storage().await;
        let etag = "multi_op_etag";
        let data1 = b"Data 1".to_vec();
        let data2 = b"Data 2".to_vec();

        // Store → load → store → load → delete → attempt load
        storage.store(etag, &data1).await.unwrap();
        assert_eq!(storage.load(etag).await.unwrap(), data1);

        storage.store(etag, &data2).await.unwrap();
        assert_eq!(storage.load(etag).await.unwrap(), data2);

        storage.remove(etag).await.unwrap();
        assert!(storage.load(etag).await.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let (storage, _temp_dir) = setup_test_storage().await;

        let mut handles = vec![];

        // Launch multiple concurrent tasks
        for i in 0..10 {
            let storage_clone = storage.clone();
            let etag = format!("concurrent_etag_{}", i);
            let data = format!("Data for {}", i).into_bytes();

            handles.push(tokio::spawn(async move {
                storage_clone.store(&etag, &data).await.unwrap();
                let loaded = storage_clone.load(&etag).await.unwrap();
                assert_eq!(loaded, data);
                storage_clone.remove(&etag).await.unwrap();
            }));
        }

        // Wait for every task to finish
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_insert_opportunistic_persists_when_write_permit_available() {
        let temp_dir = tempdir().unwrap();
        let cache = ChunksCache::new_with_config(ChunksCacheConfig::with_budgets(
            16 * 1024 * 1024,
            16 * 1024 * 1024,
            temp_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();

        let key = "available-opportunistic-key".to_string();
        for _ in 0..16 {
            cache
                .insert_opportunistic(key.clone(), vec![7u8; 128 * 1024].into())
                .await;
        }

        for _ in 0..50 {
            if cache.disk_insert_inflight.is_empty() && cache.is_disk_cached(&key).await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(cache.disk_insert_inflight.is_empty());
        assert!(cache.is_disk_cached(&key).await);
    }

    #[tokio::test]
    async fn test_insert_opportunistic_skips_disk_when_write_permits_saturated() {
        let temp_dir = tempdir().unwrap();
        let cache = ChunksCache::new_with_config(ChunksCacheConfig::with_budgets(
            16 * 1024 * 1024,
            16 * 1024 * 1024,
            temp_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();

        let mut permits = Vec::new();
        while let Ok(permit) = cache.disk_storage.write_sem.clone().try_acquire_owned() {
            permits.push(permit);
        }
        assert!(
            !permits.is_empty(),
            "test must saturate at least one disk cache write permit"
        );

        let key = "saturated-opportunistic-key".to_string();
        cache
            .insert_opportunistic(key.clone(), vec![9u8; 128 * 1024].into())
            .await;

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            cache.disk_insert_inflight.is_empty(),
            "opportunistic disk cache writes should not queue behind saturated local I/O"
        );
        assert!(
            !cache.is_disk_cached(&key).await,
            "disk cache insert should be skipped while write permits are saturated"
        );
        assert!(
            cache.get(&key).await.is_some(),
            "hot cache insert should still happen before the opportunistic disk skip"
        );

        drop(permits);
    }

    #[test]
    fn test_recent_write_hot_capacity_is_bounded() {
        assert_eq!(recent_write_hot_capacity(0), 0);
        assert_eq!(
            recent_write_hot_capacity(32 * 1024 * 1024),
            32 * 1024 * 1024
        );
        assert_eq!(
            recent_write_hot_capacity(4 * 1024 * 1024 * 1024),
            4 * 1024 * 1024 * 1024
        );
        assert_eq!(
            recent_write_hot_capacity(16 * 1024 * 1024 * 1024),
            4 * 1024 * 1024 * 1024
        );
    }

    #[tokio::test]
    async fn test_recent_write_hot_cache_serves_read_after_write_under_hot_pressure() {
        let temp_dir = tempdir().unwrap();
        let cache = ChunksCache::new_with_config(ChunksCacheConfig::with_budgets(
            512 * 1024,
            16 * 1024 * 1024,
            temp_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();

        for idx in 0..8 {
            cache
                .insert_hot(
                    &format!("old-read-hot-{idx}"),
                    vec![idx as u8; 256 * 1024].into(),
                )
                .await;
        }

        let key = "fresh-write-block";
        let data = bytes::Bytes::from(vec![42u8; 256 * 1024]);
        cache.insert_recent_write_hot(key, data.clone()).await;

        assert_eq!(
            cache.get(&key.to_string()).await.as_deref(),
            Some(data.as_ref())
        );
        let stats = cache.stats();
        assert!(
            stats.write_hot_entries >= 1,
            "recent write tier should retain freshly uploaded blocks independently of normal hot admission"
        );
    }

    #[tokio::test]
    async fn test_recent_write_hot_cache_serves_range_before_hot_cache() {
        let temp_dir = tempdir().unwrap();
        let cache = ChunksCache::new_with_config(ChunksCacheConfig::with_budgets(
            16 * 1024 * 1024,
            16 * 1024 * 1024,
            temp_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();

        let key = "fresh-write-range-block".to_string();
        cache
            .insert_hot(&key, bytes::Bytes::from_static(b"stale-data"))
            .await;
        cache
            .insert_recent_write_hot(&key, bytes::Bytes::from_static(b"fresh-data"))
            .await;

        let mut out = vec![0u8; 5];
        assert_eq!(cache.get_range_into(&key, 0, &mut out).await, Some(5));
        assert_eq!(&out, b"fresh");
    }

    #[tokio::test]
    async fn test_range_cache_miss_when_cached_value_does_not_cover_request() {
        let temp_dir = tempdir().unwrap();
        let cache = ChunksCache::new_with_config(ChunksCacheConfig::with_budgets(
            16 * 1024 * 1024,
            16 * 1024 * 1024,
            temp_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();

        let key = "short-range-cache-entry".to_string();
        cache
            .insert_hot(&key, bytes::Bytes::from_static(b"abc"))
            .await;

        let mut out = vec![9u8; 5];
        assert_eq!(cache.get_range_into(&key, 0, &mut out).await, None);
        assert_eq!(out, vec![9u8; 5]);
    }

    #[tokio::test]
    async fn test_disk_range_cache_miss_when_cached_value_does_not_cover_request() {
        let temp_dir = tempdir().unwrap();
        let cache = ChunksCache::new_with_config(ChunksCacheConfig::with_budgets(
            16 * 1024 * 1024,
            16 * 1024 * 1024,
            temp_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();

        let key = "short-disk-range-cache-entry".to_string();
        cache.disk_storage.store(&key, b"abc").await.unwrap();

        let mut out = vec![9u8; 5];
        assert_eq!(cache.get_range_into(&key, 0, &mut out).await, None);
        assert_eq!(out, vec![9u8; 5]);
    }

    #[tokio::test]
    async fn test_store_to_disk_with_permit_accepts_bytes() {
        let temp_dir = tempdir().unwrap();
        let cache = ChunksCache::new_with_config(ChunksCacheConfig::with_budgets(
            16 * 1024 * 1024,
            16 * 1024 * 1024,
            temp_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();

        let key = "bytes-store-key";
        let data = bytes::Bytes::from_static(b"store bytes without vec roundtrip");
        let permit = cache
            .try_disk_store_permit(key)
            .expect("disk cache write permit should be available");

        cache
            .store_to_disk_with_permit(key, data.clone(), permit)
            .await
            .unwrap();

        assert_eq!(cache.get(&key.to_string()).await, Some(data));
    }

    #[tokio::test]
    async fn disk_hit_promotes_to_hot_cache_when_hot_tier_has_room() {
        let temp_dir = tempdir().unwrap();
        let cache = ChunksCache::new_with_config(ChunksCacheConfig::with_budgets(
            4 * 1024 * 1024,
            16 * 1024 * 1024,
            temp_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();

        let key = "disk-hit-fast-promote-key".to_string();
        let data = vec![42u8; 128 * 1024];
        cache.insert(&key, &data).await.unwrap();

        cache.hot_cache.invalidate(&key).await;
        cache.hot_cache.run_pending_tasks().await;
        cache.hot_bytes.store(0, Ordering::Relaxed);
        assert!(cache.hot_cache.get(&key).await.is_none());

        assert_eq!(
            cache
                .get(&key)
                .await
                .expect("disk cache should contain key"),
            bytes::Bytes::from(data)
        );

        cache.hot_cache.run_pending_tasks().await;
        assert!(
            cache.hot_cache.get(&key).await.is_some(),
            "disk cache hits should warm the hot cache while memory budget is available"
        );
    }

    #[tokio::test]
    async fn disk_hit_fast_promotion_respects_hot_tier_budget() {
        let temp_dir = tempdir().unwrap();
        let cache = ChunksCache::new_with_config(ChunksCacheConfig::with_budgets(
            4 * 1024 * 1024,
            16 * 1024 * 1024,
            temp_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();

        cache
            .insert_hot("hot-filler", bytes::Bytes::from(vec![1u8; 3 * 1024 * 1024]))
            .await;
        assert!(cache.hot_cache.weighted_size() > cache.config.max_hot_bytes * 700 / 1000);

        let key = "disk-hit-budget-key".to_string();
        let data = vec![24u8; 512 * 1024];
        let next_weight = cache
            .hot_cache
            .weighted_size()
            .saturating_add(data.len() as u64)
            .saturating_add(64);
        let fast_promote_ceiling =
            cache.config.max_hot_bytes * DISK_HIT_FAST_PROMOTE_MAX_UTILIZATION_PER_MILLE / 1000;
        assert!(next_weight > fast_promote_ceiling);

        cache.disk_storage.store(&key, &data).await.unwrap();
        cache.cold_cache.insert(key.clone(), ()).await;

        assert_eq!(
            cache
                .get(&key)
                .await
                .expect("disk cache should contain key"),
            bytes::Bytes::from(data)
        );

        cache.hot_cache.run_pending_tasks().await;
        assert!(
            cache.hot_cache.get(&key).await.is_none(),
            "fast disk-hit promotion should stop before the hot tier is too full"
        );
    }

    #[test]
    fn test_filename_uniqueness() {
        let etag1 = "test1";
        let etag2 = "test2";

        let filename1 = DiskStorage::key_to_filename(etag1);
        let filename2 = DiskStorage::key_to_filename(etag2);

        // Different etags should produce different filenames
        assert_ne!(filename1, filename2);
    }

    #[tokio::test]
    async fn test_files_actually_created() {
        let (storage, _temp_dir) = setup_test_storage().await;
        let etag = "file_creation_test";
        let test_data = b"Test data".to_vec();

        // Ensure the directory is empty before storing (except system files)
        let mut entries = fs::read_dir(&storage.base_dir).await.unwrap();
        let mut initial_count = 0;
        while entries.next_entry().await.unwrap().is_some() {
            initial_count += 1;
        }

        storage.store(etag, &test_data).await.unwrap();

        // Verify that the file is actually created
        let mut entries = fs::read_dir(&storage.base_dir).await.unwrap();
        let mut final_count = 0;
        while entries.next_entry().await.unwrap().is_some() {
            final_count += 1;
        }
        assert_eq!(final_count, initial_count + 1);
    }

    #[tokio::test]
    async fn test_error_messages() {
        let (storage, _temp_dir) = setup_test_storage().await;
        let etag = "error_test_etag";

        // Test error message when loading a missing file
        let load_error = storage.load(etag).await.unwrap_err();
        let error_string = load_error.to_string();
        assert!(error_string.contains("does not exist"));

        // Test error message when deleting a missing file
        let remove_error = storage.remove(etag).await.unwrap_err();
        let error_string = remove_error.to_string();
        assert!(error_string.contains("does not exist"));
    }

    #[test]
    fn test_disk_health_bypass_and_recovery() {
        let health = DiskHealth::new();

        health.record_error();
        health.record_error();
        health.record_error();
        assert!(health.is_bypassed());

        for _ in 0..10 {
            health.record_success();
        }
        assert!(!health.is_bypassed());
    }

    #[tokio::test]
    async fn test_disk_health_bypass_on_repeated_store_errors() {
        let temp_dir = tempdir().unwrap();
        let not_dir = temp_dir.path().join("not-a-directory");
        fs::write(&not_dir, b"not a directory").await.unwrap();
        let storage = DiskStorage::new(&not_dir, 0).await.unwrap();

        for i in 0..3 {
            storage
                .store_with_health(&format!("bad-key-{i}"), b"data")
                .await
                .unwrap();
        }

        assert!(storage.health.is_bypassed());
        storage
            .store_with_health("skipped-after-bypass", b"data")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_chunks_cache_insert_swallows_disk_cache_errors() {
        let temp_dir = tempdir().unwrap();
        let not_dir = temp_dir.path().join("not-a-directory");
        fs::write(&not_dir, b"not a directory").await.unwrap();
        let cache = ChunksCache::new_with_config(ChunksCacheConfig::with_budgets(
            16 * 1024 * 1024,
            16 * 1024 * 1024,
            not_dir,
        ))
        .await
        .unwrap();

        for i in 0..3 {
            cache
                .insert(&format!("bad-cache-key-{i}"), &vec![1u8; 4096])
                .await
                .unwrap();
        }

        assert!(cache.disk_storage.health.is_bypassed());
        assert_eq!(cache.get(&"missing-key".to_string()).await, None);
    }

    // ========== AccessStats tests ==========

    #[test]
    fn test_access_stats_basic_functionality() {
        let short_window_size = Duration::from_secs(10);
        let medium_window_size = Duration::from_secs(60);
        let max_entries = 100;
        let stats = AccessStats::new(short_window_size, medium_window_size, max_entries);

        // Should have zero access initially
        assert_eq!(stats.get_short_window_frequency(), 0.0);
        assert_eq!(stats.get_medium_window_frequency(), 0.0);

        // Record a few accesses
        for _ in 0..5 {
            stats.record_access();
        }

        // Check the access frequency
        assert!(stats.get_short_window_frequency() > 0.0);
        assert!(stats.get_medium_window_frequency() > 0.0);

        // Check the weighted frequency
        let weighted_freq = stats.get_weighted_access_frequency(0.7, 0.3);
        assert!(weighted_freq > 0.0);
    }

    #[tokio::test]
    async fn test_access_stats_concurrent_access() {
        let short_window_size = Duration::from_secs(10);
        let medium_window_size = Duration::from_secs(60);
        let max_entries = 100;
        let stats = Arc::new(AccessStats::new(
            short_window_size,
            medium_window_size,
            max_entries,
        ));

        let mut handles = vec![];

        // Launch multiple concurrent tasks to record accesses
        for _ in 0..10 {
            let stats_clone = stats.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    stats_clone.record_access();
                }
            }));
        }

        // Wait for every task to finish
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify total access count via frequency calculation
        let frequency = stats.get_short_window_frequency();
        assert!(frequency > 0.0); // There should be recorded accesses
    }

    #[tokio::test]
    async fn test_access_stats_time_window() {
        let short_window_size = Duration::from_secs(5);
        let medium_window_size = Duration::from_secs(60);
        let max_entries = 100;
        let stats = AccessStats::new(short_window_size, medium_window_size, max_entries);

        // Record an access
        stats.record_access();
        stats.record_access();

        // There should be accesses in the short window
        assert!(stats.get_short_window_frequency() > 0.0);

        // Wait for the time bucket to expire (simulated here; real code would wait)
        // Note: this test may need tuning because our buckets are 1-second each
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Accesses should remain within the 2-second window
        let frequency_2s = stats.get_short_window_frequency();
        assert!(frequency_2s > 0.0);
    }

    #[tokio::test]
    async fn test_policy_basic_operations() {
        let short_window_size = Duration::from_secs(10);
        let medium_window_size = Duration::from_secs(60);
        let max_entries = 100;
        let base_promotion_threshold = 5.0;
        let policy = Policy::new(
            short_window_size,
            medium_window_size,
            max_entries,
            base_promotion_threshold,
            0.7,  // short_window_weight
            0.3,  // medium_window_weight
            true, // enable_adaptive_threshold
            0.8,  // aggressive_promotion_load_threshold
            0.6,  // conservative_promotion_hit_rate_threshold
        );

        let key = "test_key".to_string();

        // Should not promote initially
        assert!(!policy.should_promote(key.clone()).await);

        // Record multiple accesses
        for _ in 0..10 {
            policy.record_access(key.clone()).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Wait briefly so the access records take effect
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Promotion conditions should now be satisfied
        // Note: due to bucket implementation, more accesses may be required
        let additional_accesses = 50;
        for _ in 0..additional_accesses {
            policy.record_access(key.clone()).await;
        }

        // Check whether promotion conditions are met
        let should_promote = policy.should_promote(key.clone()).await;
        // If the frequency is high enough, promotion should occur
        if should_promote {
            println!("Key promoted successfully");
        } else {
            println!("Key not promoted - this is normal for low frequency access");
        }
    }

    #[test]
    fn test_default_promotion_policy_is_more_aggressive_for_read_cache_warmup() {
        let config = ChunksCacheConfig::default();

        assert_eq!(config.base_promotion_threshold, 5.0);
        assert_eq!(config.short_window_weight, 0.75);
        assert_eq!(config.medium_window_weight, 0.25);
    }

    #[tokio::test]
    async fn disk_storage_can_bypass_integrity_framing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = DiskStorage::new_with_integrity(
            temp_dir.path(),
            0,
            crate::chunk::cache_integrity::CacheIntegrityMode::None,
        )
        .await
        .unwrap();

        storage.store("raw-cache-entry", b"abcdef").await.unwrap();

        let filename = DiskStorage::key_to_filename("raw-cache-entry");
        let raw = tokio::fs::read(temp_dir.path().join(filename))
            .await
            .unwrap();
        assert_eq!(raw, b"abcdef");
        assert_eq!(
            storage.load("raw-cache-entry").await.unwrap(),
            b"abcdef".to_vec()
        );
    }

    #[test]
    fn test_adaptive_threshold_lowers_when_hit_rate_is_poor() {
        let policy = Policy::new(
            Duration::from_secs(10),
            Duration::from_secs(60),
            100,
            5.0,
            0.75,
            0.25,
            true,
            0.8,
            0.6,
        );

        for _ in 0..10 {
            policy.record_cache_request(false);
        }

        assert!(
            policy.calculate_adaptive_threshold() < 5.0,
            "cold-start miss-heavy workloads should promote faster"
        );
    }

    #[test]
    fn test_access_stats_frequency_calculation() {
        let short_window_size = Duration::from_secs(10);
        let medium_window_size = Duration::from_secs(60);
        let max_entries = 100;
        let stats = AccessStats::new(short_window_size, medium_window_size, max_entries);

        // Quickly record 10 accesses
        for _ in 0..10 {
            stats.record_access();
        }

        // Compute the short-term frequency
        let short_frequency = stats.get_short_window_frequency();
        assert!(short_frequency > 0.0);

        // Compute the mid-term frequency
        let medium_frequency = stats.get_medium_window_frequency();
        assert!(medium_frequency > 0.0);

        // Mid-term frequency should be lower or equal (larger window)
        assert!(medium_frequency <= short_frequency);

        // Test the weighted frequency calculation
        let weighted_freq = stats.get_weighted_access_frequency(0.7, 0.3);
        assert!(weighted_freq > 0.0);
        // Weighted frequency should fall between short and mid-term values
        assert!(weighted_freq >= medium_frequency);
        assert!(weighted_freq <= short_frequency);
    }
}
