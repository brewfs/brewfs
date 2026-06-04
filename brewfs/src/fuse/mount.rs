//! Mount helpers for starting/stopping FUSE
//!
//! Notes:
//! - Only supported on Unix-like systems. On Linux we support unprivileged mount via fusermount3
//!   and privileged mount via /dev/fuse.
//! - These helpers are thin wrappers over rfuse3 raw Session APIs.

use std::num::NonZeroU32;
use std::path::Path;

use rfuse3::MountOptions;
#[cfg(target_os = "linux")]
use rfuse3::raw::logfs::LoggingFileSystem;

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
    // Enable kernel-side permission checking (recommended for most filesystems)
    mo.default_permissions(true);
    // Required for coherent mmap/page-cache writeback under fsx-style workloads.
    mo.write_back(true);
    // Allow other users to access the filesystem (required for multi-user scenarios and xfstests)
    // Note: Requires 'user_allow_other' in /etc/fuse.conf for non-root mounts
    mo.allow_other(true);
    // Default to 4 MiB for higher throughput while keeping memory usage reasonable.
    mo.max_write(NonZeroU32::new(BREWFS_FUSE_MAX_WRITE).unwrap());
    // Set kernel readahead to 16 MiB (4 blocks). Larger values cause excessive
    // concurrent FUSE reads that create scheduling contention. 16 MiB lets the
    // kernel pipeline 4 read requests while our userspace prefetcher handles
    // deeper look-ahead independently.
    mo.max_readahead(Some(16 * 1024 * 1024));
    mo
}

fn configure_session<FS>(
    session: rfuse3::raw::Session<FS>,
    config: FuseConcurrencyConfig,
) -> rfuse3::raw::Session<FS>
where
    FS: rfuse3::raw::Filesystem + Send + Sync + 'static,
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
) -> std::io::Result<rfuse3::raw::MountHandle>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    let mount_point = mount_point.as_ref();
    // Prefer unprivileged mount on Linux (requires fusermount3 in PATH)
    if fuse_op_log_enabled() {
        configure_session(
            rfuse3::raw::Session::new(default_mount_options()),
            concurrency,
        )
        .mount_with_unprivileged(LoggingFileSystem::new(fs), mount_point)
        .await
    } else {
        configure_session(
            rfuse3::raw::Session::new(default_mount_options()),
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
) -> std::io::Result<rfuse3::raw::MountHandle>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    let mount_point = mount_point.as_ref();
    if fuse_op_log_enabled() {
        configure_session(
            rfuse3::raw::Session::new(default_mount_options()),
            concurrency,
        )
        .mount(LoggingFileSystem::new(fs), mount_point)
        .await
    } else {
        configure_session(
            rfuse3::raw::Session::new(default_mount_options()),
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
) -> std::io::Result<rfuse3::raw::MountHandle>
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
) -> std::io::Result<rfuse3::raw::MountHandle>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "FUSE mount is only supported on Linux in this build",
    ))
}
