use crate::chunk::ChunkLayout;
use crate::vfs::cache::config::CacheConfig;
use std::sync::Arc;
use std::time::Duration;

pub const DEFAULT_PAGE_SIZE: u32 = 64 * 1024; // 64KB
pub const DEFAULT_MAX_AHEAD: u64 = 64 * 1024 * 1024; // 64MB — 16 blocks pipeline depth
pub const DEFAULT_BUFFER_SIZE: u64 = 1024 * 1024 * 300; // 300MB
pub const DEFAULT_WRITE_BUFFER_SIZE: u64 = 1024 * 1024 * 300; // 300MB
pub const DEFAULT_FLUSH_ALL_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct ReadConfig {
    pub layout: ChunkLayout,
    /// Maximum buffer size for read operations (soft limit).
    /// When exceeded, reads will be throttled. Hard limit is 2x this value.
    /// Default: 300MB. Increase for high-throughput sequential reads.
    /// Decrease for memory-constrained environments.
    pub buffer_size: u64,

    /// Maximum readahead distance for sequential reads.
    /// Limits how far ahead the session will predict. Too large values
    /// can waste memory on random access patterns.
    /// Default: 32MB. Adjust based on typical sequential read sizes.
    pub max_ahead: u64,
}

impl Default for ReadConfig {
    fn default() -> Self {
        Self {
            layout: ChunkLayout::default(),
            buffer_size: DEFAULT_BUFFER_SIZE,
            max_ahead: DEFAULT_MAX_AHEAD,
        }
    }
}

#[allow(dead_code)]
impl ReadConfig {
    pub fn new(layout: ChunkLayout) -> Self {
        Self {
            layout,
            ..Default::default()
        }
    }

    pub fn buffer_size(self, buffer_size: u64) -> Self {
        Self {
            buffer_size,
            ..self
        }
    }

    pub fn max_ahead(self, max_ahead: u64) -> Self {
        Self { max_ahead, ..self }
    }
}

#[derive(Clone)]
pub struct WriteConfig {
    pub layout: ChunkLayout,
    pub page_size: u32,
    /// Maximum buffer size for write operations (soft limit).
    /// When exceeded, writes will be throttled. Hard limit is 2x this value.
    /// Default: 300MB. Set to 0 to disable throttling.
    pub buffer_size: u64,
    pub flush_all_interval: Duration,
    /// Minimum bytes before auto_flush freezes a slice on size.
    /// Higher values aggregate more data per S3 PUT (reduces small-object amplification).
    pub freeze_min_bytes: u64,
    /// Maximum age of a Writable slice before auto_flush freezes it.
    pub auto_flush_max_age: Duration,
    /// Controls ordering of upload vs metadata commit.
    pub writeback_mode: crate::vfs::cache::config::WriteBackMode,
}

impl Default for WriteConfig {
    fn default() -> Self {
        let writeback_mode = std::env::var("BREWFS_WRITEBACK_MODE")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase().replace('-', "_"))
            .filter(|value| {
                matches!(
                    value.as_str(),
                    "commit_before_upload" | "commit_first" | "writeback" | "s3_writeback"
                )
            })
            .map(|_| crate::vfs::cache::config::WriteBackMode::CommitBeforeUpload)
            .unwrap_or(crate::vfs::cache::config::WriteBackMode::UploadBeforeCommit);

        Self {
            layout: ChunkLayout::default(),
            page_size: DEFAULT_PAGE_SIZE,
            buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            flush_all_interval: DEFAULT_FLUSH_ALL_INTERVAL,
            #[cfg(not(test))]
            freeze_min_bytes: 8 * 1024 * 1024,
            #[cfg(test)]
            freeze_min_bytes: 4096,
            // Balance between flush latency and sustained write throughput.
            // 500ms at ~160 MiB/s accumulates ~80MB, but the 8MiB freeze_min_bytes
            // triggers inline during fast writes (at ~50ms). This gives 2 blocks
            // per upload batch as minimum, with pipeline upload handling concurrency.
            #[cfg(not(test))]
            auto_flush_max_age: Duration::from_millis(500),
            #[cfg(test)]
            auto_flush_max_age: Duration::from_millis(5),
            writeback_mode,
        }
    }
}

#[allow(dead_code)]
impl WriteConfig {
    pub fn new(layout: ChunkLayout) -> Self {
        Self {
            layout,
            ..Default::default()
        }
    }

    pub fn page_size(self, page_size: u32) -> Self {
        Self { page_size, ..self }
    }

    pub fn buffer_size(self, buffer_size: u64) -> Self {
        Self {
            buffer_size,
            ..self
        }
    }

    pub fn flush_all_interval(self, flush_all_interval: Duration) -> Self {
        Self {
            flush_all_interval,
            ..self
        }
    }

    pub fn freeze_min_bytes(self, freeze_min_bytes: u64) -> Self {
        Self {
            freeze_min_bytes,
            ..self
        }
    }

    pub fn auto_flush_max_age(self, auto_flush_max_age: Duration) -> Self {
        Self {
            auto_flush_max_age,
            ..self
        }
    }

    pub fn writeback_mode(self, writeback_mode: crate::vfs::cache::config::WriteBackMode) -> Self {
        Self {
            writeback_mode,
            ..self
        }
    }
}

#[derive(Clone, Default)]
pub struct VFSConfig {
    pub read: Arc<ReadConfig>,
    pub write: Arc<WriteConfig>,
    pub cache: Arc<CacheConfig>,
}

#[allow(dead_code)]
impl VFSConfig {
    pub fn read_config(self, read: ReadConfig) -> Self {
        Self {
            read: Arc::new(read),
            ..self
        }
    }

    pub fn write_config(self, write: WriteConfig) -> Self {
        Self {
            write: Arc::new(write),
            ..self
        }
    }

    pub fn new(layout: ChunkLayout) -> Self {
        Self::new_with_cache_config(layout, CacheConfig::default())
    }

    pub fn new_with_cache_config(layout: ChunkLayout, cache: CacheConfig) -> Self {
        let cache = Arc::new(cache);
        let page_size = if layout.block_size.is_multiple_of(DEFAULT_PAGE_SIZE) {
            DEFAULT_PAGE_SIZE
        } else {
            layout.block_size
        };

        let read = Arc::new(
            ReadConfig::new(layout)
                .buffer_size(cache.read_memory_bytes)
                .max_ahead(cache.prefetch_max_bytes),
        );
        let write = Arc::new(
            WriteConfig::new(layout)
                .page_size(page_size)
                .buffer_size(cache.write_memory_bytes)
                .freeze_min_bytes(cache.dirty_slice_target_size)
                .auto_flush_max_age(Duration::from_millis(cache.dirty_slice_max_age_ms))
                .writeback_mode(cache.writeback_mode),
        );

        Self { read, write, cache }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::cache::config::WriteBackMode;

    #[test]
    fn vfs_config_applies_cache_budget_knobs() {
        let layout = ChunkLayout {
            chunk_size: 16 * 1024 * 1024,
            block_size: 4 * 1024 * 1024,
        };
        let cache = CacheConfig {
            read_memory_bytes: 11 * 1024 * 1024,
            write_memory_bytes: 12 * 1024 * 1024,
            dirty_slice_target_size: 2 * 1024 * 1024,
            dirty_slice_max_age_ms: 123,
            prefetch_max_bytes: 3 * 1024 * 1024,
            writeback_mode: WriteBackMode::CommitBeforeUpload,
            ..CacheConfig::default()
        };

        let config = VFSConfig::new_with_cache_config(layout, cache.clone());

        assert_eq!(config.read.buffer_size, cache.read_memory_bytes);
        assert_eq!(config.read.max_ahead, cache.prefetch_max_bytes);
        assert_eq!(config.write.buffer_size, cache.write_memory_bytes);
        assert_eq!(config.write.freeze_min_bytes, cache.dirty_slice_target_size);
        assert_eq!(
            config.write.auto_flush_max_age,
            Duration::from_millis(cache.dirty_slice_max_age_ms)
        );
        assert_eq!(config.write.writeback_mode, cache.writeback_mode);
        assert_eq!(config.cache.memory_budget_bytes, cache.memory_budget_bytes);
    }
}
