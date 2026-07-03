use crate::chunk::ChunkLayout;
use crate::vfs::cache::config::CacheConfig;
use std::sync::Arc;
use std::time::Duration;

pub const DEFAULT_PAGE_SIZE: u32 = 64 * 1024; // 64KB
pub const DEFAULT_MAX_AHEAD: u64 = 64 * 1024 * 1024; // 64MB — 16 blocks pipeline depth
pub const DEFAULT_BUFFER_SIZE: u64 = 1024 * 1024 * 300; // 300MB
pub const DEFAULT_WRITE_BUFFER_SIZE: u64 = 1024 * 1024 * 300; // 300MB
pub const DEFAULT_FLUSH_ALL_INTERVAL: Duration = Duration::from_secs(5);

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .map(|value| match value.as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        })
        .unwrap_or(default)
}

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
    /// Maximum in-flight block uploads per writer.
    pub upload_concurrency: usize,
    /// Controls ordering of upload vs metadata commit.
    pub writeback_mode: crate::vfs::cache::config::WriteBackMode,
    /// Experimental page-to-block assembler for cached writeback writes.
    pub cached_block_assembler: bool,
    /// Soft limit for committed-but-not-uploaded dirty bytes. 0 disables this gate.
    pub writeback_recent_pending_soft_limit: u64,
    /// Hard limit for committed-but-not-uploaded dirty bytes. 0 falls back to the soft limit.
    pub writeback_recent_pending_hard_limit: u64,
    /// Require local writeback stage to be sealed before publishing metadata.
    pub writeback_require_stage_before_commit: bool,
    /// Allow upload-before-commit writers to publish uploaded full-block
    /// prefixes before a writable partial tail is closed. Disabled by default
    /// because build tools rely on close-to-open artifact publication.
    pub upload_before_commit_prefix_split: bool,
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
        let writeback_recent_pending_soft_limit =
            std::env::var("BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
        let writeback_recent_pending_hard_limit =
            std::env::var("BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);

        Self {
            layout: ChunkLayout::default(),
            page_size: DEFAULT_PAGE_SIZE,
            buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            flush_all_interval: DEFAULT_FLUSH_ALL_INTERVAL,
            #[cfg(not(test))]
            freeze_min_bytes: 32 * 1024 * 1024,
            #[cfg(test)]
            freeze_min_bytes: 4096,
            // Balance between flush latency and sustained write throughput.
            // Sequential writes still freeze on the 32MiB size target, while a
            // longer age window lets random 4MiB writes coalesce before S3 PUT.
            #[cfg(not(test))]
            auto_flush_max_age: Duration::from_millis(2000),
            #[cfg(test)]
            auto_flush_max_age: Duration::from_millis(5),
            // RustFS/S3 latency rises sharply when each writer fans out too
            // many concurrent foreground PUTs. Eight permits preserves
            // sequential throughput and lowers random-write tail latency.
            upload_concurrency: 10,
            writeback_mode,
            cached_block_assembler: env_flag_enabled("BREWFS_CACHED_BLOCK_ASSEMBLER"),
            writeback_recent_pending_soft_limit,
            writeback_recent_pending_hard_limit,
            writeback_require_stage_before_commit: env_bool(
                "BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT",
                true,
            ),
            upload_before_commit_prefix_split: env_bool(
                "BREWFS_UPLOAD_BEFORE_COMMIT_PREFIX_SPLIT",
                false,
            ),
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

    pub fn upload_concurrency(self, upload_concurrency: usize) -> Self {
        Self {
            upload_concurrency: upload_concurrency.max(1),
            ..self
        }
    }

    pub fn writeback_mode(self, writeback_mode: crate::vfs::cache::config::WriteBackMode) -> Self {
        Self {
            writeback_mode,
            ..self
        }
    }

    pub fn cached_block_assembler(self, cached_block_assembler: bool) -> Self {
        Self {
            cached_block_assembler,
            ..self
        }
    }

    pub fn writeback_recent_pending_soft_limit(self, limit: u64) -> Self {
        Self {
            writeback_recent_pending_soft_limit: limit,
            ..self
        }
    }

    pub fn writeback_recent_pending_hard_limit(self, limit: u64) -> Self {
        Self {
            writeback_recent_pending_hard_limit: limit,
            ..self
        }
    }

    pub fn writeback_require_stage_before_commit(self, require: bool) -> Self {
        Self {
            writeback_require_stage_before_commit: require,
            ..self
        }
    }

    pub fn upload_before_commit_prefix_split(self, enabled: bool) -> Self {
        Self {
            upload_before_commit_prefix_split: enabled,
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
                .upload_concurrency(cache.upload_concurrency)
                .writeback_mode(cache.writeback_mode)
                .writeback_recent_pending_soft_limit(cache.writeback_recent_pending_soft_bytes)
                .writeback_recent_pending_hard_limit(cache.writeback_recent_pending_hard_bytes)
                .writeback_require_stage_before_commit(cache.writeback_require_stage_before_commit),
        );

        Self { read, write, cache }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::cache::config::WriteBackMode;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn with_cached_block_assembler_env(value: Option<&str>, f: impl FnOnce()) {
        let _guard = env_lock().lock().unwrap();
        let previous = std::env::var_os("BREWFS_CACHED_BLOCK_ASSEMBLER");
        match value {
            Some(value) => {
                // SAFETY: This test serializes access to this process-wide env var
                // and restores the previous value before releasing the lock.
                unsafe { std::env::set_var("BREWFS_CACHED_BLOCK_ASSEMBLER", value) };
            }
            None => {
                // SAFETY: This test serializes access to this process-wide env var
                // and restores the previous value before releasing the lock.
                unsafe { std::env::remove_var("BREWFS_CACHED_BLOCK_ASSEMBLER") };
            }
        }

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));

        match previous {
            Some(previous) => {
                // SAFETY: The lock above serializes writes to this env var.
                unsafe { std::env::set_var("BREWFS_CACHED_BLOCK_ASSEMBLER", previous) };
            }
            None => {
                // SAFETY: The lock above serializes writes to this env var.
                unsafe { std::env::remove_var("BREWFS_CACHED_BLOCK_ASSEMBLER") };
            }
        }

        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    }

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
            upload_concurrency: 7,
            writeback_mode: WriteBackMode::CommitBeforeUpload,
            writeback_recent_pending_soft_bytes: 123,
            writeback_recent_pending_hard_bytes: 456,
            writeback_require_stage_before_commit: false,
            ..CacheConfig::default()
        };

        let config = VFSConfig::new_with_cache_config(layout, cache.clone());

        assert_eq!(config.read.buffer_size, cache.read_memory_bytes);
        assert_eq!(config.read.max_ahead, cache.prefetch_max_bytes);
        assert_eq!(config.write.buffer_size, cache.write_memory_bytes);
        assert_eq!(config.write.page_size, DEFAULT_PAGE_SIZE);
        assert_eq!(config.write.freeze_min_bytes, cache.dirty_slice_target_size);
        assert_eq!(config.write.upload_concurrency, cache.upload_concurrency);
        assert_eq!(config.write.writeback_recent_pending_soft_limit, 123);
        assert_eq!(config.write.writeback_recent_pending_hard_limit, 456);
        assert!(!config.write.writeback_require_stage_before_commit);
        assert_eq!(
            config.write.auto_flush_max_age,
            Duration::from_millis(cache.dirty_slice_max_age_ms)
        );
        assert_eq!(config.write.writeback_mode, cache.writeback_mode);
        assert_eq!(config.cache.memory_budget_bytes, cache.memory_budget_bytes);
    }

    #[test]
    fn vfs_config_default_uses_ten_upload_workers() {
        let config =
            VFSConfig::new_with_cache_config(ChunkLayout::default(), CacheConfig::default());

        assert_eq!(config.write.freeze_min_bytes, 32 * 1024 * 1024);
        assert_eq!(config.write.upload_concurrency, 10);
        assert_eq!(config.write.auto_flush_max_age, Duration::from_millis(2000));
    }

    #[test]
    fn write_config_defaults_and_sets_recent_pending_backpressure_limits() {
        with_cached_block_assembler_env(None, || {
            let default_config = WriteConfig::new(ChunkLayout::default());
            assert_eq!(default_config.writeback_recent_pending_soft_limit, 0);
            assert_eq!(default_config.writeback_recent_pending_hard_limit, 0);
            assert!(default_config.writeback_require_stage_before_commit);
            assert!(!default_config.upload_before_commit_prefix_split);
            assert!(!default_config.cached_block_assembler);
        });

        let configured = WriteConfig::new(ChunkLayout::default())
            .writeback_recent_pending_soft_limit(123)
            .writeback_recent_pending_hard_limit(456)
            .writeback_require_stage_before_commit(false)
            .upload_before_commit_prefix_split(true)
            .cached_block_assembler(true);
        assert_eq!(configured.writeback_recent_pending_soft_limit, 123);
        assert_eq!(configured.writeback_recent_pending_hard_limit, 456);
        assert!(!configured.writeback_require_stage_before_commit);
        assert!(configured.upload_before_commit_prefix_split);
        assert!(configured.cached_block_assembler);
    }

    #[test]
    fn write_config_parses_cached_block_assembler_env() {
        with_cached_block_assembler_env(Some("1"), || {
            let config = WriteConfig::new(ChunkLayout::default());
            assert!(config.cached_block_assembler);
        });
    }
}
