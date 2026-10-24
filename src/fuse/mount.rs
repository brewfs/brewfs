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
    mount_options_with_max_read(BREWFS_FUSE_MAX_WRITE as usize)
}
fn mount_options_with_max_read(max_read: usize) -> MountOptions {
    let mut mo = MountOptions::default();
    mo.fs_name("brewfs");
    mo.default_permissions(true);
    // Keep the kernel writeback-cache opt-in.  It can help mmap-heavy
    // single-client workloads, but Linux can zero already-written mmap bytes
    // around byte-granular fallocate extension (xfstests generic/438), so the
    // correctness default is the write-through FUSE page-cache path.
    mo.write_back(fuse_writeback_enabled());
    // Allow other users to access the filesystem (required for multi-user scenarios and xfstests)
    // Note: Requires 'user_allow_other' in /etc/fuse.conf for non-root mounts
    mo.allow_other(true);
    // Default to 4 MiB for higher throughput while keeping memory usage reasonable.
    mo.max_write(NonZeroU32::new(BREWFS_FUSE_MAX_WRITE).unwrap());
    mo.custom_options(format!("max_read={max_read}"));
    // Set kernel readahead to 16 MiB (4 blocks). Larger values cause excessive
    // concurrent FUSE reads that create scheduling contention. 16 MiB lets the
    // kernel pipeline 4 read requests while our userspace prefetcher handles
    // deeper look-ahead independently.
    mo.max_readahead(Some(16 * 1024 * 1024));
    mo
}

fn mount_options_for_fs<S, M>(fs: &VFS<S, M>) -> MountOptions
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    let mut options = mount_options_with_max_read(
        fs.prepared_max_read_bytes()
            .unwrap_or(BREWFS_FUSE_MAX_WRITE as usize),
    );
    #[cfg(feature = "workspace-overlay")]
    if fs.requires_packed_mutation_drain() {
        options.write_back(false);
    }
    options.posix_acl(
        fs.meta_layer().posix_acl_capability()
            != crate::meta::layer::PosixAclCapability::Unsupported,
    );
    options.dont_mask(
        fs.meta_layer().posix_acl_capability() == crate::meta::layer::PosixAclCapability::ReadWrite,
    );
    options
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
    budgeted: bool,
) -> asyncfuse::raw::Session<FS>
where
    FS: asyncfuse::raw::Filesystem + Send + Sync + 'static,
{
    if config.worker_count > 1 || budgeted {
        session.with_workers(config.worker_count.max(1), config.max_background.max(1))
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
    let mount_point = std::fs::canonicalize(mount_point.as_ref())?;
    let identity = fs.clone();
    let options = mount_options_for_fs(&fs);
    let budgeted = fs.prepared_max_read_bytes().is_some();
    // Prefer unprivileged mount on Linux (requires fusermount3 in PATH)
    let handle = if fuse_op_log_enabled() {
        configure_session(asyncfuse::raw::Session::new(options), concurrency, budgeted)
            .mount_with_unprivileged(LoggingFileSystem::new(fs), &mount_point)
            .await
    } else {
        configure_session(asyncfuse::raw::Session::new(options), concurrency, budgeted)
            .mount_with_unprivileged(fs, &mount_point)
            .await
    }?;
    bind_mount_device(&identity, &mount_point, handle).await
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
    let mount_point = std::fs::canonicalize(mount_point.as_ref())?;
    let identity = fs.clone();
    let options = mount_options_for_fs(&fs);
    let budgeted = fs.prepared_max_read_bytes().is_some();
    let handle = if fuse_op_log_enabled() {
        configure_session(asyncfuse::raw::Session::new(options), concurrency, budgeted)
            .mount(LoggingFileSystem::new(fs), &mount_point)
            .await
    } else {
        configure_session(asyncfuse::raw::Session::new(options), concurrency, budgeted)
            .mount(fs, &mount_point)
            .await
    }?;
    bind_mount_device(&identity, &mount_point, handle).await
}

#[cfg(target_os = "linux")]
async fn bind_mount_device<S, M>(
    fs: &VFS<S, M>,
    mount_point: &Path,
    handle: asyncfuse::raw::MountHandle,
) -> std::io::Result<asyncfuse::raw::MountHandle>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    match super::clone_ioctl::mounted_device(mount_point) {
        Ok(device) => {
            fs.set_fuse_mount_device(Some(device));
            Ok(handle)
        }
        Err(error) => {
            handle.unmount().await?;
            Err(error)
        }
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
    fn fuse_writeback_cache_defaults_off_and_can_be_enabled() {
        assert!(!parse_fuse_writeback_enabled(None));
        assert!(parse_fuse_writeback_enabled(Some("1".to_string())));
        assert!(parse_fuse_writeback_enabled(Some("true".to_string())));
        assert!(parse_fuse_writeback_enabled(Some("yes".to_string())));
        assert!(parse_fuse_writeback_enabled(Some("on".to_string())));
        assert!(!parse_fuse_writeback_enabled(Some("0".to_string())));
        assert!(!parse_fuse_writeback_enabled(Some("false".to_string())));
        assert!(!parse_fuse_writeback_enabled(Some("maybe".to_string())));
    }
}
