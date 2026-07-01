use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

use crate::chunk::bandwidth::BandwidthConfig;
use crate::chunk::cache_integrity::CacheIntegrityMode;
use crate::chunk::compress::Compression;

#[cfg(test)]
static TEST_CACHE_ROOT_SEQ: AtomicU64 = AtomicU64::new(0);

fn default_cache_root() -> PathBuf {
    #[cfg(test)]
    {
        let seq = TEST_CACHE_ROOT_SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("brewfs-test-cache-{}-{seq}", std::process::id()))
    }

    #[cfg(not(test))]
    {
        dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("brewfs")
    }
}

/// Write-back mode controls when data becomes globally visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteBackMode {
    /// Default safe mode: upload to S3 first, then metadata commit.
    /// fsync/close success guarantees data is in object store + metadata committed.
    #[default]
    UploadBeforeCommit,
    /// High-performance mode: metadata commit first, async upload.
    /// Risk: local SSD loss before upload = data loss. Other clients may
    /// see metadata-visible slices whose objects don't exist yet.
    /// Must be explicitly opted in.
    CommitBeforeUpload,
}

/// Configuration for the BrewFS local cache system.
///
/// Controls memory and SSD budgets for both read (clean block) and write
/// (dirty slice) caches, as well as prefetch and upload parameters.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub cache_root: PathBuf,

    // Read cache budgets
    pub read_memory_bytes: u64,
    pub read_ssd_bytes: u64,

    // Write cache budgets
    pub write_memory_bytes: u64,
    pub write_ssd_bytes: u64,

    // Dirty slice parameters
    pub dirty_slice_target_size: u64,
    pub dirty_slice_max_age_ms: u64,

    // Upload parameters
    pub upload_concurrency: usize,

    // Prefetch parameters
    pub prefetch_enabled: bool,
    pub prefetch_initial_bytes: u64,
    pub prefetch_max_bytes: u64,
    pub prefetch_concurrency: usize,
    pub range_background_prefetch: bool,
    pub populate_write_cache_after_upload: bool,
    pub persist_write_cache_after_upload: bool,

    // Semantics
    pub strict_posix: bool,
    pub writeback_mode: WriteBackMode,
    pub writeback_recent_pending_soft_bytes: u64,
    pub writeback_recent_pending_hard_bytes: u64,

    // Disk safety
    pub min_free_disk_bytes: u64,
    pub writeback_persist_sync: bool,

    // Compression
    pub compression: Compression,
    pub verify_cache_checksum: CacheIntegrityMode,

    // Bandwidth limiting
    pub bandwidth: BandwidthConfig,

    // VFS reader/writer buffer budget. Object/page caches are configured separately.
    pub memory_budget_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            cache_root: default_cache_root(),
            read_memory_bytes: 4096 * 1024 * 1024,
            read_ssd_bytes: 20 * 1024 * 1024 * 1024,
            write_memory_bytes: 384 * 1024 * 1024,
            write_ssd_bytes: 20 * 1024 * 1024 * 1024,
            dirty_slice_target_size: 32 * 1024 * 1024,
            dirty_slice_max_age_ms: 2000,
            upload_concurrency: 10,
            prefetch_enabled: true,
            prefetch_initial_bytes: 4 * 1024 * 1024,
            prefetch_max_bytes: 64 * 1024 * 1024,
            prefetch_concurrency: 64,
            range_background_prefetch: true,
            populate_write_cache_after_upload: true,
            persist_write_cache_after_upload: false,
            strict_posix: true,
            writeback_mode: WriteBackMode::UploadBeforeCommit,
            writeback_recent_pending_soft_bytes: 0,
            writeback_recent_pending_hard_bytes: 0,
            min_free_disk_bytes: 1024 * 1024 * 1024,
            writeback_persist_sync: true,
            compression: Compression::Lz4,
            verify_cache_checksum: CacheIntegrityMode::Full,
            bandwidth: BandwidthConfig::default(),
            // Keep enough foreground write headroom for full 32MiB slice batches
            // without letting close absorb a large upload backlog.
            memory_budget_bytes: 1280 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_config_defaults_raise_prefetch_concurrency() {
        let config = CacheConfig::default();

        assert_eq!(config.prefetch_concurrency, 64);
    }

    #[test]
    fn cache_config_defaults_preserve_hot_path_settings() {
        let config = CacheConfig::default();

        assert_eq!(config.compression, Compression::Lz4);
        assert_eq!(config.read_memory_bytes, 4096 * 1024 * 1024);
        assert_eq!(config.write_memory_bytes, 384 * 1024 * 1024);
        assert_eq!(config.memory_budget_bytes, 1280 * 1024 * 1024);
        assert_eq!(config.dirty_slice_target_size, 32 * 1024 * 1024);
        assert_eq!(config.dirty_slice_max_age_ms, 2000);
        assert_eq!(config.upload_concurrency, 10);
        assert_eq!(config.prefetch_max_bytes, 64 * 1024 * 1024);
        assert!(config.range_background_prefetch);
        assert!(config.populate_write_cache_after_upload);
        assert!(!config.persist_write_cache_after_upload);
        assert!(config.writeback_persist_sync);
    }
}
