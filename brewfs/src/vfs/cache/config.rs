use std::path::PathBuf;

use crate::chunk::bandwidth::BandwidthConfig;
use crate::chunk::compress::Compression;

/// Write-back mode controls when data becomes globally visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteBackMode {
    /// Default safe mode: upload to S3 first, then metadata commit.
    /// fsync/close success guarantees data is in object store + metadata committed.
    UploadBeforeCommit,
    /// High-performance mode: metadata commit first, async upload.
    /// Risk: local SSD loss before upload = data loss. Other clients may
    /// see metadata-visible slices whose objects don't exist yet.
    /// Must be explicitly opted in.
    CommitBeforeUpload,
}

impl Default for WriteBackMode {
    fn default() -> Self {
        Self::UploadBeforeCommit
    }
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

    // Semantics
    pub strict_posix: bool,
    pub writeback_mode: WriteBackMode,

    // Disk safety
    pub min_free_disk_bytes: u64,

    // Compression
    pub compression: Compression,

    // Bandwidth limiting
    pub bandwidth: BandwidthConfig,

    // VFS reader/writer buffer budget. Object/page caches are configured separately.
    pub memory_budget_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            cache_root: dirs::cache_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join("brewfs"),
            read_memory_bytes: 4096 * 1024 * 1024,
            read_ssd_bytes: 20 * 1024 * 1024 * 1024,
            write_memory_bytes: 300 * 1024 * 1024,
            write_ssd_bytes: 20 * 1024 * 1024 * 1024,
            dirty_slice_target_size: 8 * 1024 * 1024,
            dirty_slice_max_age_ms: 500,
            upload_concurrency: 32,
            prefetch_enabled: true,
            prefetch_initial_bytes: 4 * 1024 * 1024,
            prefetch_max_bytes: 64 * 1024 * 1024,
            prefetch_concurrency: 64,
            strict_posix: true,
            writeback_mode: WriteBackMode::UploadBeforeCommit,
            min_free_disk_bytes: 1024 * 1024 * 1024,
            compression: Compression::Lz4,
            bandwidth: BandwidthConfig::default(),
            // Default: read/write soft buffers plus headroom for transient overlap.
            memory_budget_bytes: 1024 * 1024 * 1024,
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
        assert_eq!(config.write_memory_bytes, 300 * 1024 * 1024);
        assert_eq!(config.dirty_slice_target_size, 8 * 1024 * 1024);
        assert_eq!(config.prefetch_max_bytes, 64 * 1024 * 1024);
    }
}
