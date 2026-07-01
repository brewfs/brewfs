//! Mount helpers for starting/stopping FUSE
//!
//! Notes:
//! - Only supported on Unix-like systems. On Linux we support unprivileged mount via fusermount3
//!   and privileged mount via /dev/fuse.
//! - These helpers are thin wrappers over asyncfuse raw Session APIs.

use std::num::NonZeroU32;
use std::path::Path;

use asyncfuse::MountOptions;
#[cfg(target_os = "linux")]
use asyncfuse::raw::logfs::LoggingFileSystem;

use crate::chunk::store::BlockStore;
use crate::fuse::BREWFS_FUSE_MAX_WRITE;
use crate::meta::MetaLayer;
use crate::vfs::fs::VFS;

#[derive(Debug, Clone, Copy, Default)]
pub struct FuseConcurrencyConfig {
    pub worker_count: usize,
    pub max_background: usize,
}

/// Build default mount options for BrewFS.
fn default_mount_options() -> MountOptions {
    let mut mo = MountOptions::default();
    mo.fs_name("brewfs");
    mo.default_permissions(true);
    // BrewFS userspace writeback currently coalesces large writes more predictably without
    // kernel writeback-cache. Keep the kernel mode opt-in for workloads that need it.
    mo.write_back(fuse_writeback_enabled());
    // Allow other users to access the filesystem (required for multi-user scenarios and xfstests)
    // Note: Requires 'user_allow_other' in /etc/fuse.conf for non-root mounts
    mo.allow_other(true);
    // Default to 4 MiB for higher throughput while keeping memory usage reasonable.
    mo.max_write(NonZeroU32::new(BREWFS_FUSE_MAX_WRITE).unwrap());
    mo.custom_options(format!("max_read={BREWFS_FUSE_MAX_WRITE}"));
    // Set kernel readahead to 16 MiB (4 blocks). Larger values cause excessive
    // concurrent FUSE reads that create scheduling contention. 16 MiB lets the
    // kernel pipeline 4 read requests while our userspace prefetcher handles
    // deeper look-ahead independently.
    mo.max_readahead(Some(16 * 1024 * 1024));
    mo
}

fn fuse_writeback_enabled() -> bool {
    parse_fuse_writeback_enabled(std::env::var("BREWFS_FUSE_WRITEBACK").ok())
}

fn parse_fuse_writeback_enabled(value: Option<String>) -> bool {
    value
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

fn configure_session<FS>(
    session: asyncfuse::raw::Session<FS>,
    config: FuseConcurrencyConfig,
) -> asyncfuse::raw::Session<FS>
where
    FS: asyncfuse::raw::Filesystem + Send + Sync + 'static,
{
    if config.worker_count > 1 {
        session.with_workers(config.worker_count, config.max_background.max(1))
    } else {
        session
    }
}

#[cfg(target_os = "linux")]
fn fuse_op_log_enabled() -> bool {
    std::env::var("BREWFS_FUSE_OP_LOG")
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

/// Mount a VFS instance to the given empty directory using unprivileged mode when available.
#[cfg(target_os = "linux")]
pub async fn mount_vfs_unprivileged<S, M>(
    fs: VFS<S, M>,
    mount_point: impl AsRef<Path>,
    concurrency: FuseConcurrencyConfig,
) -> std::io::Result<asyncfuse::raw::MountHandle>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    let mount_point = mount_point.as_ref();
    // Prefer unprivileged mount on Linux (requires fusermount3 in PATH)
    if fuse_op_log_enabled() {
        configure_session(
            asyncfuse::raw::Session::new(default_mount_options()),
            concurrency,
        )
        .mount_with_unprivileged(LoggingFileSystem::new(fs), mount_point)
        .await
    } else {
        configure_session(
            asyncfuse::raw::Session::new(default_mount_options()),
            concurrency,
        )
        .mount_with_unprivileged(fs, mount_point)
        .await
    }
}

/// Mount a VFS instance to the given empty directory using privileged mode (via /dev/fuse).
/// Requires root or fuse group membership. Supports allow_other without /etc/fuse.conf tweaks.
#[cfg(target_os = "linux")]
pub async fn mount_vfs_privileged<S, M>(
    fs: VFS<S, M>,
    mount_point: impl AsRef<Path>,
    concurrency: FuseConcurrencyConfig,
) -> std::io::Result<asyncfuse::raw::MountHandle>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    let mount_point = mount_point.as_ref();
    if fuse_op_log_enabled() {
        configure_session(
            asyncfuse::raw::Session::new(default_mount_options()),
            concurrency,
        )
        .mount(LoggingFileSystem::new(fs), mount_point)
        .await
    } else {
        configure_session(
            asyncfuse::raw::Session::new(default_mount_options()),
            concurrency,
        )
        .mount(fs, mount_point)
        .await
    }
}

/// Fallback stub for non-Linux targets (unprivileged).
#[cfg(not(target_os = "linux"))]
pub async fn mount_vfs_unprivileged<S, M>(
    _fs: VFS<S, M>,
    _mount_point: impl AsRef<Path>,
    _concurrency: FuseConcurrencyConfig,
) -> std::io::Result<asyncfuse::raw::MountHandle>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "FUSE mount is only supported on Linux in this build",
    ))
}

/// Fallback stub for non-Linux targets (privileged).
#[cfg(not(target_os = "linux"))]
pub async fn mount_vfs_privileged<S, M>(
    _fs: VFS<S, M>,
    _mount_point: impl AsRef<Path>,
    _concurrency: FuseConcurrencyConfig,
) -> std::io::Result<asyncfuse::raw::MountHandle>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "FUSE mount is only supported on Linux in this build",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_mount_options_request_large_read_requests() {
        let options = default_mount_options();
        let debug = format!("{options:?}");

        assert!(
            debug.contains("max_read=4194304"),
            "Linux mount options should request 4 MiB FUSE read requests: {debug}"
        );
    }

    #[test]
    fn default_mount_options_enable_kernel_permission_checks() {
        let options = default_mount_options();
        let debug = format!("{options:?}");

        assert!(
            debug.contains("default_permissions: true"),
            "BrewFS needs kernel checks for special-node opens such as FIFO permissions: {debug}"
        );
    }

    #[test]
    fn fuse_writeback_cache_is_opt_in() {
        assert!(!parse_fuse_writeback_enabled(None));
        assert!(parse_fuse_writeback_enabled(Some("1".to_string())));
        assert!(parse_fuse_writeback_enabled(Some("true".to_string())));
        assert!(!parse_fuse_writeback_enabled(Some("0".to_string())));
    }
}
