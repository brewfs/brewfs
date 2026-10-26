#[cfg(feature = "profiling")]
use std::fs::File;
#[cfg(feature = "profiling")]
use std::io::BufWriter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
#[cfg(feature = "profiling")]
use std::sync::{LazyLock, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime};

use crate::config::*;
use crate::console;
#[cfg(feature = "workspace-overlay")]
mod entity_mount;
#[cfg(feature = "workspace-overlay")]
mod mount_store;
#[cfg(feature = "workspace-overlay")]
mod native_reverse_maintenance;
#[cfg(feature = "workspace-overlay")]
pub(crate) mod packed_mount_cutoff;
#[cfg(feature = "workspace-overlay")]
mod packed_mount_recovery;
#[cfg(all(test, feature = "workspace-overlay", target_os = "linux"))]
mod packed_mount_session_tests;
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay;

use bytes::Bytes;
use clap::Parser;
#[cfg(not(feature = "profiling"))]
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::cadapter::localfs::LocalFsBackend;
use crate::cadapter::s3::{S3Backend, S3Config};
use crate::chunk::bandwidth::BandwidthLimiter;
use crate::chunk::cache::ChunksCacheConfig;
use crate::chunk::layout::ChunkLayout;
use crate::chunk::store::{BlockStore, BlockStoreConfig, ObjectBlockStore};
use crate::control::client::send_request;
use crate::control::job::JobOutcome;
use crate::control::protocol::{ControlRequest, ControlResponse};
use crate::control::runtime::RuntimeRegistry;
use crate::fuse::mount::{FuseConcurrencyConfig, mount_vfs_privileged, mount_vfs_unprivileged};
use crate::meta::MetaStore;
use crate::meta::client::MetaClient;
#[cfg(feature = "gateway-webdav")]
use crate::meta::config::CacheTtl;
use crate::meta::config::{
    CacheConfig as MetaCacheConfig, ClientOptions, Config, DatabaseConfig, DatabaseType,
    MetaClientConfig,
};
use crate::meta::layer::MetaLayer;
use crate::meta::stores::{DatabaseMetaStore, EtcdMetaStore, RedisMetaStore, TiKvMetaStore};
#[cfg(feature = "native-packed-base")]
use crate::native_base::runtime::{
    BackendObjectRepository, FROZEN_METADATA_FEATURE, NativeDataRuntime, NativeRuntimeCapabilities,
    NativeVolumeHeader, WorkspaceBaseDataSource, initialize_volume, load_volume_header,
};
#[cfg(feature = "native-packed-base")]
#[cfg(feature = "native-packed-base")]
use crate::native_base::write::keys::Keys;
#[cfg(feature = "native-packed-base")]
use crate::native_base::write::overlay::{OverlayParams, WriteOverlay};
#[cfg(feature = "native-packed-base")]
use crate::native_base::write::receipts::ObjectSink;
#[cfg(feature = "native-packed-base")]
use crate::native_base::write::records::HeadState;
#[cfg(feature = "native-packed-base")]
use crate::native_base::write::redis::RedisControlStore;
#[cfg(feature = "native-packed-base")]
use crate::native_base::write::store::ControlStore;
#[cfg(feature = "native-packed-base")]
use crate::native_base::write::tikv::TiKvControlStore;
use crate::vfs::fs::VFS;
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::catalog::{CreateVolumeRoot, WorkspaceStore};
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::control::WorkspaceControl;
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::gc::WorkspaceGc;
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::ids::{LayerId, WorkspaceId};
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::lifecycle::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_LEASE_TTL, WorkspaceLifecycle, WorkspaceMountSession,
};
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::model::WORKSPACE_SCHEMA_VERSION;
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::packed_v3::PackedV3ReadonlyMeta;
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::publish::diff::WorkspaceDiff;
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::stores::database::SqliteWorkspaceStore;
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::stores::kv_store::KvWorkspaceStore;
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
#[cfg(feature = "workspace-overlay")]
use crate::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;
#[cfg(feature = "native-packed-base")]
use sha2::{Digest, Sha256};

/// Working-set lifetime of the lease-bound upper row/namespace caches. Not a
/// staleness bound: those caches stop serving as soon as the mount's lease
/// can no longer be proven held (see `LeaseValidity`).
#[cfg(feature = "workspace-overlay")]
const LEASE_BOUND_ROW_CACHE_TTL: Duration = Duration::from_secs(60);

pub async fn run() -> anyhow::Result<()> {
    init_tracing();
    raise_nofile_limit();

    let cli = Cli::parse();
    let result = match cli.cmd {
        Command::Mount(args) => mount_cmd(MountConfig::from_sources(*args)?).await,
        #[cfg(feature = "workspace-overlay")]
        Command::Workspace(args) => workspace_cmd(args).await,
        Command::Gc(args) => gc_cmd(args).await,
        Command::Info(args) => info_cmd(args).await,
        Command::Console(args) => console::serve_cmd(args).await,
        #[cfg(any(feature = "gateway-s3", feature = "gateway-webdav"))]
        Command::Gateway(args) => gateway_cmd(*args).await,
        Command::ObjectPutBench(args) => object_put_bench_cmd(args).await,
    };
    shutdown_flame();
    shutdown_chrome();
    result
}

#[cfg(unix)]
fn raise_nofile_limit() {
    const DEFAULT_NOFILE_LIMIT: u64 = 1_048_576;

    let target = std::env::var("BREWFS_NOFILE_LIMIT")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_NOFILE_LIMIT) as libc::rlim_t;

    // SAFETY: getrlimit/setrlimit are process-local libc calls. We pass valid
    // pointers to stack-allocated rlimit values and do not retain those pointers.
    unsafe {
        let mut current = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut current) != 0 {
            tracing::warn!(
                error = ?std::io::Error::last_os_error(),
                "failed to read RLIMIT_NOFILE"
            );
            return;
        }

        if current.rlim_cur >= target {
            tracing::debug!(
                soft = current.rlim_cur,
                hard = current.rlim_max,
                "RLIMIT_NOFILE already sufficient"
            );
            return;
        }

        let requested_hard = if current.rlim_max == libc::RLIM_INFINITY {
            current.rlim_max
        } else {
            current.rlim_max.max(target)
        };
        let requested = libc::rlimit {
            rlim_cur: target,
            rlim_max: requested_hard,
        };
        if libc::setrlimit(libc::RLIMIT_NOFILE, &requested) == 0 {
            tracing::info!(
                soft = requested.rlim_cur,
                hard = requested.rlim_max,
                "raised RLIMIT_NOFILE"
            );
            return;
        }

        let fallback_soft = if current.rlim_max == libc::RLIM_INFINITY {
            target
        } else {
            target.min(current.rlim_max)
        };
        if fallback_soft > current.rlim_cur {
            let fallback = libc::rlimit {
                rlim_cur: fallback_soft,
                rlim_max: current.rlim_max,
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &fallback) == 0 {
                tracing::info!(
                    soft = fallback.rlim_cur,
                    hard = fallback.rlim_max,
                    "raised RLIMIT_NOFILE to hard limit"
                );
                return;
            }
        }

        tracing::warn!(
            soft = current.rlim_cur,
            hard = current.rlim_max,
            target,
            error = ?std::io::Error::last_os_error(),
            "failed to raise RLIMIT_NOFILE"
        );
    }
}

#[cfg(not(unix))]
fn raise_nofile_limit() {}

#[cfg(feature = "profiling")]
fn init_tracing() {
    let flame_layer = std::env::var("BREWFS_TRACE_FLAME").ok().and_then(|path| {
        let path_for_log = path.clone();
        match tracing_flame::FlameLayer::with_file(path) {
            Ok((layer, guard)) => {
                let layer = layer.with_empty_samples(false).with_threads_collapsed(true);
                eprintln!("[brewfs] tracing-flame enabled: {}", path_for_log);
                register_flame_guard(guard);
                Some(layer)
            }
            Err(err) => {
                eprintln!(
                    "[brewfs] failed to enable tracing-flame for {}: {err}",
                    path_for_log
                );
                None
            }
        }
    });
    let chrome_layer = std::env::var("BREWFS_TRACE_CHROME").ok().map(|path| {
        let path_for_log = path.clone();
        let builder = tracing_chrome::ChromeLayerBuilder::new()
            .file(path)
            .trace_style(tracing_chrome::TraceStyle::Async)
            .include_args(true);
        let (layer, guard) = builder.build();
        eprintln!("[brewfs] tracing-chrome enabled: {}", path_for_log);
        register_chrome_guard(guard);
        layer
    });
    let env_filter = tracing_subscriber::EnvFilter::new(
        std::env::var("RUST_LOG").unwrap_or_else(|_| "brewfs=info".to_string()),
    );
    let console_layer = std::env::var_os("TOKIO_CONSOLE").map(|_| console_subscriber::spawn());

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().pretty())
        .with(env_filter)
        .with(flame_layer)
        .with(chrome_layer)
        .with(console_layer)
        .init();
}

#[cfg(not(feature = "profiling"))]
fn init_tracing() {
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::Registry;

    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "brewfs=info".to_string());

    let fuse_log_path = std::env::var("BREWFS_FUSE_LOG_FILE").ok();
    let main_log_path = std::env::var("BREWFS_LOG_FILE").ok();

    if let Some(fuse_path) = fuse_log_path {
        let mut layers: Vec<Box<dyn tracing_subscriber::Layer<Registry> + Send + Sync>> =
            Vec::new();

        // --- logfs layer: only asyncfuse::raw::logfs events ----------------------
        let fuse_dir = std::path::Path::new(&fuse_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        let fuse_name = std::path::Path::new(&fuse_path)
            .file_name()
            .unwrap_or(std::ffi::OsStr::new("fuse_ops.log"));
        let fuse_appender = tracing_appender::rolling::never(fuse_dir, fuse_name);
        let (fuse_writer, _fuse_guard) = tracing_appender::non_blocking(fuse_appender);
        std::mem::forget(_fuse_guard);

        let fuse_filter = tracing_subscriber::filter::Targets::new()
            .with_target("asyncfuse::raw::logfs", tracing::Level::TRACE);

        layers.push(Box::new(
            tracing_subscriber::fmt::layer()
                .with_writer(fuse_writer)
                .with_ansi(false)
                .with_filter(fuse_filter),
        ));

        // --- main layer: everything EXCEPT asyncfuse::raw::logfs -----------------
        let main_filter = tracing_subscriber::EnvFilter::new(&rust_log)
            .add_directive("asyncfuse::raw::logfs=off".parse().unwrap());

        if let Some(ref main_path) = main_log_path {
            let main_dir = std::path::Path::new(main_path.as_str())
                .parent()
                .unwrap_or(std::path::Path::new("."));
            let main_name = std::path::Path::new(main_path.as_str())
                .file_name()
                .unwrap_or(std::ffi::OsStr::new("brewfs.log"));
            let main_appender = tracing_appender::rolling::never(main_dir, main_name);
            let (main_writer, _main_guard) = tracing_appender::non_blocking(main_appender);
            std::mem::forget(_main_guard);

            layers.push(Box::new(
                tracing_subscriber::fmt::layer()
                    .pretty()
                    .with_span_events(FmtSpan::CLOSE)
                    .with_writer(main_writer)
                    .with_ansi(false)
                    .with_filter(main_filter),
            ));
        } else {
            layers.push(Box::new(
                tracing_subscriber::fmt::layer()
                    .pretty()
                    .with_span_events(FmtSpan::CLOSE)
                    .with_filter(main_filter),
            ));
        }

        tracing_subscriber::registry().with(layers).init();

        eprintln!("[brewfs] FUSE op log -> {fuse_path}");
        if let Some(ref p) = main_log_path {
            eprintln!("[brewfs] main log -> {p}");
        }
    } else {
        // No split: everything goes to stderr (or BREWFS_LOG_FILE).
        let env_filter = tracing_subscriber::EnvFilter::new(&rust_log);

        if let Some(main_path) = main_log_path {
            let main_dir = std::path::Path::new(&main_path)
                .parent()
                .unwrap_or(std::path::Path::new("."));
            let main_name = std::path::Path::new(&main_path)
                .file_name()
                .unwrap_or(std::ffi::OsStr::new("brewfs.log"));
            let main_appender = tracing_appender::rolling::never(main_dir, main_name);
            let (main_writer, _main_guard) = tracing_appender::non_blocking(main_appender);
            std::mem::forget(_main_guard);

            tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer()
                        .pretty()
                        .with_span_events(FmtSpan::CLOSE)
                        .with_writer(main_writer)
                        .with_ansi(false),
                )
                .with(env_filter)
                .init();

            eprintln!("[brewfs] main log -> {main_path}");
        } else {
            tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer()
                        .pretty()
                        .with_span_events(FmtSpan::CLOSE),
                )
                .with(env_filter)
                .init();
        }
    }
}

async fn mount_cmd(mut args: MountConfig) -> anyhow::Result<()> {
    #[cfg(feature = "workspace-overlay")]
    if args.volume_format == VolumeFormat::WorkspaceEntityV1 && args.experimental_entity_mount {
        return entity_mount::mount(args).await;
    }
    validate_volume_format_support(args.volume_format)?;
    if !args.mount_point.exists() {
        std::fs::create_dir_all(&args.mount_point)?;
    }
    if !args.mount_point.is_dir() {
        anyhow::bail!("mount point must be a directory");
    }

    if args.chunk_size < args.block_size as u64 {
        anyhow::bail!("chunk_size must be >= block_size");
    }

    namespace_flat_volume_cache(&mut args)?;

    let layout = ChunkLayout {
        chunk_size: args.chunk_size,
        block_size: args.block_size,
    };

    tracing::info!(
        mount_point = %args.mount_point.display(),
        meta_backend = ?args.meta_backend,
        data_backend = ?args.data_backend,
        "mount startup begin"
    );
    match args.data_backend {
        DataBackendKind::LocalFs => {
            let client = create_localfs_client(&args)?;
            tracing::info!("mount startup localfs client ready");
            #[cfg(feature = "native-packed-base")]
            if args.volume_format == VolumeFormat::WorkspaceNativeV2 {
                return mount_native_with_client(client, layout, &args).await;
            }
            #[cfg(feature = "workspace-overlay")]
            if args.volume_format == VolumeFormat::PackedMetadataV3 {
                return mount_packed_v3_readonly_with_client(client, layout, &args).await;
            }
            let store = create_object_store(
                client,
                layout,
                &args.cache,
                args.volume_format != VolumeFormat::FlatV1,
            )
            .await?;
            dispatch_mount(layout, store, &args).await
        }
        DataBackendKind::S3 => {
            let client = create_s3_client(&args).await?;
            tracing::info!("mount startup s3 client ready");
            #[cfg(feature = "native-packed-base")]
            if args.volume_format == VolumeFormat::WorkspaceNativeV2 {
                return mount_native_with_client(client, layout, &args).await;
            }
            #[cfg(feature = "workspace-overlay")]
            if args.volume_format == VolumeFormat::PackedMetadataV3 {
                return mount_packed_v3_readonly_with_client(client, layout, &args).await;
            }
            let store = create_object_store(
                client,
                layout,
                &args.cache,
                args.volume_format != VolumeFormat::FlatV1,
            )
            .await?;
            dispatch_mount(layout, store, &args).await
        }
    }
}

#[cfg(any(feature = "gateway-s3", feature = "gateway-webdav"))]
async fn gateway_cmd(args: GatewayArgs) -> anyhow::Result<()> {
    match args.protocol {
        #[cfg(feature = "gateway-s3")]
        GatewayProtocol::S3(s3) => gateway_s3_cmd(s3).await,
        #[cfg(feature = "gateway-webdav")]
        GatewayProtocol::WebDav(webdav) => gateway_webdav_cmd(webdav).await,
    }
}

#[cfg(feature = "gateway-s3")]
async fn gateway_s3_cmd(args: S3GatewayArgs) -> anyhow::Result<()> {
    use crate::gateway::s3::path::{BucketMode, is_valid_bucket_name};
    use crate::gateway::s3::{S3GatewayOptions, serve};

    // The gateway does not own a FUSE mount point; use a placeholder so
    // MountConfig::from_sources validation passes.
    let mut mount_args = args.mount;
    if mount_args.mount_point.is_none() {
        mount_args.mount_point = Some(std::path::PathBuf::from("/brewfs-s3-gateway"));
    }
    let mut cfg = MountConfig::from_sources(mount_args)?;
    if cfg.volume_format != VolumeFormat::FlatV1 {
        anyhow::bail!("s3 gateway only supports volume_format=flat-v1");
    }
    validate_volume_format_support(cfg.volume_format)?;

    if cfg.chunk_size < cfg.block_size as u64 {
        anyhow::bail!("chunk_size must be >= block_size");
    }
    namespace_flat_volume_cache(&mut cfg)?;
    let layout = ChunkLayout {
        chunk_size: cfg.chunk_size,
        block_size: cfg.block_size,
    };

    let access_key = match args.access_key.as_deref() {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => anyhow::bail!(
            "s3 gateway requires an access key (--access-key or BREWFS_S3_ACCESS_KEY)"
        ),
    };
    let secret_key = match args.secret_key.as_deref() {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => {
            anyhow::bail!("s3 gateway requires a secret key (--secret-key or BREWFS_S3_SECRET_KEY)")
        }
    };

    let bucket_mode = if args.multi_buckets {
        BucketMode::Multi
    } else {
        if !is_valid_bucket_name(&args.bucket) {
            anyhow::bail!("invalid S3 bucket name: {}", args.bucket);
        }
        BucketMode::Single {
            bucket: args.bucket,
        }
    };
    let opts = S3GatewayOptions {
        listen_addr: args.listen,
        access_key,
        secret_key,
        bucket_mode,
        hide_dir_objects: args.hide_dir_objects,
    };

    tracing::info!(
        listen = %opts.listen_addr,
        data_backend = ?cfg.data_backend,
        meta_backend = ?cfg.meta_backend,
        "s3 gateway startup begin"
    );
    match cfg.data_backend {
        DataBackendKind::LocalFs => {
            let client = create_localfs_client(&cfg)?;
            let store = create_object_store(client, layout, &cfg.cache, false).await?;
            serve(
                store,
                create_meta_store(&cfg).await?,
                layout,
                cfg.compact.clone(),
                cfg.cache.clone(),
                opts,
            )
            .await
        }
        DataBackendKind::S3 => {
            let client = create_s3_client(&cfg).await?;
            let store = create_object_store(client, layout, &cfg.cache, false).await?;
            serve(
                store,
                create_meta_store(&cfg).await?,
                layout,
                cfg.compact.clone(),
                cfg.cache.clone(),
                opts,
            )
            .await
        }
    }
}

fn namespace_flat_volume_cache(args: &mut MountConfig) -> anyhow::Result<()> {
    if args.volume_format != VolumeFormat::FlatV1 {
        return Ok(());
    }

    let unscoped_root = args.cache.cache_root.clone();
    let scope = args.flat_volume_cache_scope()?;
    for legacy_path in [
        unscoped_root.join("chunks"),
        unscoped_root.join("writeback"),
    ] {
        if legacy_path.exists() {
            tracing::warn!(
                path = %legacy_path.display(),
                "ignoring legacy unscoped flat-volume cache state"
            );
        }
    }
    args.cache.cache_root = unscoped_root.join("flat-v1").join(&scope);
    args.cache.volume_scope = Some(scope.clone());
    tracing::info!(
        volume_scope = %scope,
        cache_root = %args.cache.cache_root.display(),
        "flat-volume cache namespace selected"
    );
    Ok(())
}

#[cfg(test)]
mod flat_cache_namespace_tests {
    use super::*;

    fn flat_config(
        mount_point: &std::path::Path,
        data_dir: &std::path::Path,
        meta_url: &str,
        shared_cache_root: &std::path::Path,
    ) -> MountConfig {
        let cli = Cli::parse_from([
            "brewfs",
            "mount",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--meta-url",
            meta_url,
            mount_point.to_str().unwrap(),
        ]);
        let Command::Mount(args) = cli.cmd else {
            unreachable!()
        };
        let mut config = MountConfig::from_sources(*args).unwrap();
        config.block_size = 16;
        config.chunk_size = 64;
        config.cache.cache_root = shared_cache_root.to_path_buf();
        config.cache.read_memory_bytes = 1024 * 1024;
        config.cache.read_ssd_bytes = 1024 * 1024;
        config.cache.persist_write_cache_after_upload = true;
        namespace_flat_volume_cache(&mut config).unwrap();
        config
    }

    #[tokio::test]
    async fn flat_volumes_isolate_clean_cache_for_overlapping_slice_ids() {
        let temp = tempfile::tempdir().unwrap();
        let cache_root = temp.path().join("cache");
        let legacy_chunks = cache_root.join("chunks");
        std::fs::create_dir_all(&legacy_chunks).unwrap();
        std::fs::write(legacy_chunks.join("legacy-entry"), b"untouched").unwrap();

        let first = flat_config(
            &temp.path().join("mount-a"),
            &temp.path().join("objects-a"),
            "postgres://metadata.example.test/volume-a",
            &cache_root,
        );
        let second = flat_config(
            &temp.path().join("mount-b"),
            &temp.path().join("objects-b"),
            "postgres://metadata.example.test/volume-b",
            &cache_root,
        );
        assert_ne!(first.cache.cache_root, second.cache.cache_root);
        assert!(
            first
                .cache
                .cache_root
                .starts_with(cache_root.join("flat-v1"))
        );
        assert!(
            second
                .cache
                .cache_root
                .starts_with(cache_root.join("flat-v1"))
        );

        std::fs::create_dir_all(&first.data_dir).unwrap();
        std::fs::create_dir_all(&second.data_dir).unwrap();
        let layout = ChunkLayout {
            chunk_size: 64,
            block_size: 16,
        };
        let first_writer = create_object_store(
            ObjectClient::new(LocalFsBackend::new(&first.data_dir)),
            layout,
            &first.cache,
            false,
        )
        .await
        .unwrap();
        let second_writer = create_object_store(
            ObjectClient::new(LocalFsBackend::new(&second.data_dir)),
            layout,
            &second.cache,
            false,
        )
        .await
        .unwrap();
        first_writer
            .write_fresh_range((77, 0), 0, b"first-volume-123")
            .await
            .unwrap();
        second_writer
            .write_fresh_range((77, 0), 0, b"second-volume-12")
            .await
            .unwrap();
        drop(first_writer);
        drop(second_writer);

        // Force both remounts to rely on their persistent clean-cache trees.
        std::fs::remove_dir_all(&first.data_dir).unwrap();
        std::fs::remove_dir_all(&second.data_dir).unwrap();
        let first_reader = create_object_store(
            ObjectClient::new(LocalFsBackend::new(&first.data_dir)),
            layout,
            &first.cache,
            false,
        )
        .await
        .unwrap();
        let second_reader = create_object_store(
            ObjectClient::new(LocalFsBackend::new(&second.data_dir)),
            layout,
            &second.cache,
            false,
        )
        .await
        .unwrap();
        let mut first_out = [0_u8; 16];
        let mut second_out = [0_u8; 16];
        first_reader
            .read_range((77, 0), 0, &mut first_out)
            .await
            .unwrap();
        second_reader
            .read_range((77, 0), 0, &mut second_out)
            .await
            .unwrap();

        assert_eq!(&first_out, b"first-volume-123");
        assert_eq!(&second_out, b"second-volume-12");
        assert_eq!(
            std::fs::read(legacy_chunks.join("legacy-entry")).unwrap(),
            b"untouched"
        );
    }
}

#[cfg(feature = "gateway-webdav")]
async fn gateway_webdav_cmd(args: WebDavGatewayArgs) -> anyhow::Result<()> {
    use crate::gateway::webdav::{TlsOptions, WebDavGatewayOptions, serve};

    let WebDavGatewayArgs {
        listen,
        user,
        password,
        tls_cert,
        tls_key,
        allow_anonymous,
        atomic_put,
        mut mount,
    } = args;

    let credentials = match (user, password, allow_anonymous) {
        (Some(user), Some(password), false) if !user.is_empty() && !password.is_empty() => {
            Some((user, password))
        }
        (None, None, true) => None,
        (None, None, false) => anyhow::bail!(
            "webdav gateway requires --user and --password; pass --allow-anonymous to explicitly allow unauthenticated access"
        ),
        (Some(_), Some(_), true) => {
            anyhow::bail!("webdav gateway cannot combine --allow-anonymous with --user/--password")
        }
        _ => anyhow::bail!("webdav gateway requires nonempty --user and --password together"),
    };
    let tls = match (tls_cert, tls_key) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some(TlsOptions { cert, key }),
        _ => anyhow::bail!("webdav gateway requires both --tls-cert and --tls-key"),
    };
    if credentials.is_some() && tls.is_none() && !listen.ip().is_loopback() {
        anyhow::bail!(
            "webdav Basic authentication requires TLS when --listen is not a loopback address"
        );
    }

    if mount.mount_point.is_none() {
        mount.mount_point = Some(std::path::PathBuf::from("/brewfs-webdav-gateway"));
    }
    let mut cfg = MountConfig::from_sources(mount)?;
    if cfg.volume_format != VolumeFormat::FlatV1 {
        anyhow::bail!("webdav gateway only supports volume_format=flat-v1");
    }
    validate_volume_format_support(cfg.volume_format)?;
    namespace_flat_volume_cache(&mut cfg)?;

    if cfg.chunk_size < cfg.block_size as u64 {
        anyhow::bail!("chunk_size must be >= block_size");
    }
    let layout = ChunkLayout {
        chunk_size: cfg.chunk_size,
        block_size: cfg.block_size,
    };
    let opts = WebDavGatewayOptions {
        listen_addr: listen,
        credentials,
        tls,
        atomic_put,
    };
    let meta_ttl = match cfg.meta_backend {
        MetaBackendKind::Sqlx => {
            CacheTtl::for_backend(database_type_from_url(&cfg.meta_url).backend_type())
        }
        MetaBackendKind::Etcd => CacheTtl::for_backend("etcd"),
        MetaBackendKind::Redis => CacheTtl::for_backend("redis"),
        MetaBackendKind::TiKv => CacheTtl::for_backend("tikv"),
    };

    tracing::info!(
        listen = %opts.listen_addr,
        data_backend = ?cfg.data_backend,
        meta_backend = ?cfg.meta_backend,
        tls = opts.tls.is_some(),
        atomic_put = opts.atomic_put,
        "webdav gateway startup begin"
    );
    match cfg.data_backend {
        DataBackendKind::LocalFs => {
            let client = create_localfs_client(&cfg)?;
            let store = create_object_store(client, layout, &cfg.cache, false).await?;
            serve(
                store,
                create_meta_store(&cfg).await?,
                layout,
                cfg.compact.clone(),
                cfg.cache.clone(),
                meta_ttl.clone(),
                opts,
            )
            .await
        }
        DataBackendKind::S3 => {
            let client = create_s3_client(&cfg).await?;
            let store = create_object_store(client, layout, &cfg.cache, false).await?;
            serve(
                store,
                create_meta_store(&cfg).await?,
                layout,
                cfg.compact.clone(),
                cfg.cache.clone(),
                meta_ttl,
                opts,
            )
            .await
        }
    }
}

fn validate_volume_format_support(format: VolumeFormat) -> anyhow::Result<()> {
    match format {
        VolumeFormat::FlatV1 => Ok(()),
        VolumeFormat::WorkspaceEntityV1 => validate_entity_mount_support(),
        #[cfg(feature = "workspace-overlay")]
        VolumeFormat::WorkspaceV1 => Ok(()),
        #[cfg(not(feature = "workspace-overlay"))]
        VolumeFormat::WorkspaceV1 => {
            Err(anyhow::anyhow!("feature not compiled: workspace-overlay"))
        }
        #[cfg(feature = "native-packed-base")]
        VolumeFormat::WorkspaceNativeV2 => Ok(()),
        #[cfg(not(feature = "native-packed-base"))]
        VolumeFormat::WorkspaceNativeV2 => {
            Err(anyhow::anyhow!("feature not compiled: native-packed-base"))
        }
        #[cfg(feature = "native-packed-base")]
        VolumeFormat::PackedMetadataV1 => Err(anyhow::anyhow!(
            "packed-metadata-v1 is unsupported; use packed-metadata-v3"
        )),
        #[cfg(feature = "workspace-overlay")]
        VolumeFormat::PackedMetadataV2 => Err(anyhow::anyhow!(
            "packed-metadata-v2 is unsupported; use packed-metadata-v3"
        )),
        #[cfg(feature = "workspace-overlay")]
        VolumeFormat::PackedMetadataV3 => Ok(()),
    }
}

fn validate_entity_mount_support() -> anyhow::Result<()> {
    #[cfg(feature = "workspace-overlay")]
    anyhow::bail!(
        "workspace-entity-v1 requires --experimental-entity-mount for a fresh namespace; production admission and process-crash takeover remain disabled"
    );
    #[cfg(not(feature = "workspace-overlay"))]
    anyhow::bail!("feature not compiled: workspace-overlay");
}

async fn dispatch_mount<S>(layout: ChunkLayout, store: S, args: &MountConfig) -> anyhow::Result<()>
where
    S: BlockStore + Send + Sync + 'static,
{
    match args.volume_format {
        VolumeFormat::WorkspaceEntityV1 => validate_entity_mount_support(),
        VolumeFormat::FlatV1 => {
            let meta_store = create_meta_store(args).await?;
            tracing::info!("mount startup meta store ready");
            mount_with_store(layout, store, meta_store, args).await
        }
        #[cfg(feature = "workspace-overlay")]
        VolumeFormat::WorkspaceV1 => mount_workspace_with_store(layout, store, args).await,
        #[cfg(not(feature = "workspace-overlay"))]
        VolumeFormat::WorkspaceV1 => {
            anyhow::bail!("feature not compiled: workspace-overlay")
        }
        #[cfg(feature = "native-packed-base")]
        VolumeFormat::WorkspaceNativeV2 => {
            anyhow::bail!("native mount must be dispatched with its object client")
        }
        #[cfg(not(feature = "native-packed-base"))]
        VolumeFormat::WorkspaceNativeV2 => {
            anyhow::bail!("feature not compiled: native-packed-base")
        }
        #[cfg(feature = "native-packed-base")]
        VolumeFormat::PackedMetadataV1 => {
            anyhow::bail!("packed-metadata-v1 is unsupported; use packed-metadata-v3")
        }
        #[cfg(feature = "workspace-overlay")]
        VolumeFormat::PackedMetadataV2 => {
            anyhow::bail!("packed-metadata-v2 is unsupported; use packed-metadata-v3")
        }
        #[cfg(feature = "workspace-overlay")]
        VolumeFormat::PackedMetadataV3 => {
            anyhow::bail!("packed-metadata-v3 must be dispatched with its object client")
        }
    }
}

#[cfg(feature = "workspace-overlay")]
fn native_placement_arm() -> anyhow::Result<bool> {
    match std::env::var("BREWFS_PACKED_METADATA_ARM").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("packed") => Ok(false),
        Ok("native-tikv") => Ok(true),
        _ => anyhow::bail!("BREWFS_PACKED_METADATA_ARM must be packed or native-tikv"),
    }
}

#[cfg(feature = "workspace-overlay")]
async fn mount_authenticated_packed_v3_readonly_with_client<
    B: ObjectBackend + Clone + Send + Sync + 'static,
>(
    client: ObjectClient<B>,
    layout: ChunkLayout,
    args: &MountConfig,
    reference: workspace_overlay::packed_v3::wire005::V3ObjectRef,
    budget: Arc<workspace_overlay::packed_v3::wire005::V3MountBudget>,
) -> anyhow::Result<()> {
    use workspace_overlay::packed_v3::wire005::AuthenticatedV3Snapshot;
    if args.cache.read_memory_bytes != 0
        || args.cache.read_ssd_bytes != 0
        || args.cache.prefetch_enabled
        || args.cache.range_background_prefetch
    {
        anyhow::bail!("wire 005 currently requires explicit zero payload-cache/prefetch budgets");
    }
    let startup = budget.admit(&[
        (
            workspace_overlay::packed_v3::wire005::V3BudgetPool::Roots,
            128 << 10,
        ),
        (
            workspace_overlay::packed_v3::wire005::V3BudgetPool::Stored,
            reference.object_len * 2,
        ),
    ])?;
    let observer = match client.read_observer() {
        Some(observer) if observer.owned_by_budget(Arc::as_ptr(&budget) as usize) => observer,
        Some(_) => anyhow::bail!("read observer belongs to no mount budget or a different mount"),
        None => Arc::new(crate::cadapter::read_observer::ReadObserver::with_mount_budget(&budget)?),
    };
    let client = client.with_read_observer(
        Arc::clone(&observer),
        if native_placement_arm()? {
            crate::cadapter::read_observer::Engine::Native
        } else {
            crate::cadapter::read_observer::Engine::PackedV3
        },
        crate::cadapter::read_observer::Phase::Startup,
        crate::cadapter::read_observer::Origin::Demand,
    );
    let snapshot = AuthenticatedV3Snapshot::open(&client, &reference).await?;
    drop(startup);
    let metadata_bytes = std::env::var("BREWFS_PACKED_METADATA_CACHE_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(256 * 1024 * 1024);
    if native_placement_arm()? {
        return mount_native_placement_readonly(
            client,
            snapshot,
            layout,
            args,
            budget,
            metadata_bytes,
        )
        .await;
    }
    let meta_layer = Arc::new(PackedV3ReadonlyMeta::from_v3_budget(
        client,
        snapshot,
        layout.chunk_size,
        metadata_bytes,
        budget,
    )?);
    let store = Arc::new(meta_layer.block_store(layout.block_size)?);
    meta_layer.initialize().await?;
    let config = crate::vfs::config::VFSConfig::new_with_cache_config(layout, args.cache.clone());
    let fs = VFS::from_workspace_components(config, store, Arc::clone(&meta_layer))?;
    fs.set_fuse_cache_ttl(Duration::from_millis(args.fuse_cache_ttl_ms));
    meta_layer.install_v3_stats(fs.stats());
    let concurrency = FuseConcurrencyConfig {
        worker_count: args.fuse_workers,
        max_background: args.fuse_max_background,
    };
    let mut handle = if args.privileged {
        mount_vfs_privileged(fs, &args.mount_point, concurrency).await?
    } else {
        mount_vfs_unprivileged(fs, &args.mount_point, concurrency).await?
    };
    println!(
        "mounted authenticated packed-v3 at {}",
        args.mount_point.display()
    );
    tokio::select! {
        signal=shutdown_signal()=>{signal?;handle.unmount().await?;}
        result=&mut handle=>{result?;}
    }
    Ok(())
}

#[cfg(feature = "workspace-overlay")]
async fn mount_packed_v3_readonly_with_client<B>(
    client: ObjectClient<B>,
    layout: ChunkLayout,
    args: &MountConfig,
) -> anyhow::Result<()>
where
    B: ObjectBackend + Clone + Send + Sync + 'static,
{
    let manifest_key = args
        .packed_manifest_key
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("packed-metadata-v3 manifest key is missing"))?;
    let budget = workspace_overlay::packed_v3::wire005::V3MountBudget::from_env()?;
    let probe_permit = budget.admit(&[
        (
            workspace_overlay::packed_v3::wire005::V3BudgetPool::Stored,
            128,
        ),
        (
            workspace_overlay::packed_v3::wire005::V3BudgetPool::Control,
            1024,
        ),
    ])?;
    let observer =
        Arc::new(crate::cadapter::read_observer::ReadObserver::with_mount_budget(&budget)?);
    let _observer_session = crate::cadapter::read_observer::ObserverSession(Arc::clone(&observer));
    let client = client.with_read_observer(
        observer,
        if native_placement_arm()? {
            crate::cadapter::read_observer::Engine::Native
        } else {
            crate::cadapter::read_observer::Engine::PackedV3
        },
        crate::cadapter::read_observer::Phase::Startup,
        crate::cadapter::read_observer::Origin::Demand,
    );
    let probe = client
        .typed_exact(
            crate::cadapter::read_observer::ReadClass::ManifestProbe,
            manifest_key,
            0,
            64,
            64,
            |bytes| {
                if bytes.get(..8) != Some(b"BRFPM005".as_slice()) {
                    return Err((
                        crate::cadapter::read_observer::FailureClass::Schema,
                        anyhow::anyhow!(
                            "unsupported packed manifest probe: only wire 005 is supported"
                        ),
                    ));
                }
                Ok(bytes)
            },
        )
        .await?;
    if probe.get(..8) != Some(b"BRFPM005".as_slice()) {
        anyhow::bail!("unsupported packed metadata manifest: only wire 005 is supported");
    }
    // The caller-selected content-addressed key is the trust anchor. Never
    // accept a digest calculated from the response itself as authentication.
    let component = manifest_key.rsplit('/').next().unwrap_or_default();
    if component.len() != 64 || !component.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("wire 005 mount requires a SHA-256 content-addressed manifest key");
    }
    let digest: [u8; 32] = hex::decode(component)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("wire 005 manifest digest has invalid length"))?;
    let object_len = u64::from_le_bytes(probe[24..32].try_into().unwrap());
    let reference = workspace_overlay::packed_v3::wire005::V3ObjectRef {
        key: manifest_key.to_owned(),
        kind: workspace_overlay::packed_v3::wire005::V3ObjectKind::Manifest,
        object_len,
        digest,
    };
    drop(probe);
    drop(probe_permit);
    mount_authenticated_packed_v3_readonly_with_client(client, layout, args, reference, budget)
        .await
}

#[cfg(feature = "workspace-overlay")]
async fn mount_native_placement_readonly<B: ObjectBackend + Clone + Send + Sync + 'static>(
    client: ObjectClient<B>,
    snapshot: workspace_overlay::packed_v3::wire005::AuthenticatedV3Snapshot,
    layout: ChunkLayout,
    args: &MountConfig,
    budget: Arc<workspace_overlay::packed_v3::wire005::V3MountBudget>,
    metadata_bytes: u64,
) -> anyhow::Result<()> {
    use workspace_overlay::packed_v3::wire005::{
        NativePackedPlacementProvider, NativePlacementRepository,
    };
    if !matches!(args.meta_backend, crate::config::MetaBackendKind::TiKv) {
        anyhow::bail!("native-tikv metadata arm requires --meta-backend tikv");
    }
    let repository = NativePlacementRepository::connect(
        args.meta_tikv_pd_endpoints.clone(),
        &args.meta_tikv_namespace,
        snapshot.read_generation().lower_snapshot,
        Arc::clone(&budget),
    )
    .await?;
    match std::env::var("BREWFS_PACKED_NATIVE_IMPORT").as_deref() {
        Ok("1") => {
            repository
                .import_from(
                    client.clone(),
                    snapshot.clone(),
                    layout.chunk_size,
                    metadata_bytes,
                )
                .await?;
            println!("native TiKV immutable namespace import is ready for the pinned manifest");
            // Construction is a separate operation from the measured mount.
            return Ok(());
        }
        Err(std::env::VarError::NotPresent) | Ok("0") => {}
        _ => anyhow::bail!("BREWFS_PACKED_NATIVE_IMPORT must be 0 or 1"),
    }
    let metadata = NativePackedPlacementProvider::open(
        repository.clone(),
        client,
        snapshot,
        layout.chunk_size,
        metadata_bytes,
    )
    .await?;
    metadata.initialize().await?;
    let config = crate::vfs::config::VFSConfig::new_with_cache_config(layout, args.cache.clone());
    let store = Arc::new(metadata.block_store());
    let provider: Arc<dyn crate::chunk::read_plan::WorkspaceReadPlanProvider> = metadata.clone();
    let fs =
        VFS::from_readonly_components_with_provider(config, store, metadata.clone(), provider)?;
    fs.set_fuse_cache_ttl(Duration::from_millis(args.fuse_cache_ttl_ms));
    metadata.install_stats(fs.stats());
    metadata.start_runtime();
    let concurrency = FuseConcurrencyConfig {
        worker_count: args.fuse_workers,
        max_background: args.fuse_max_background,
    };
    let mut handle = if args.privileged {
        mount_vfs_privileged(fs, &args.mount_point, concurrency).await?
    } else {
        mount_vfs_unprivileged(fs, &args.mount_point, concurrency).await?
    };
    println!(
        "mounted native TiKV namespace with pinned shared packed data at {}",
        args.mount_point.display()
    );
    tokio::select! {signal=shutdown_signal()=>{signal?;handle.unmount().await?;}result=&mut handle=>{result?;}}
    Ok(())
}

#[cfg(feature = "native-packed-base")]
async fn mount_native_with_client<B>(
    client: ObjectClient<B>,
    layout: ChunkLayout,
    args: &MountConfig,
) -> anyhow::Result<()>
where
    B: ObjectBackend + Clone + Send + Sync + 'static,
{
    let observer = Arc::new(crate::cadapter::read_observer::ReadObserver::default());
    let _observer_session = crate::cadapter::read_observer::ObserverSession(Arc::clone(&observer));
    let phase = Arc::new(std::sync::atomic::AtomicU8::new(0));
    let client = client
        .with_read_observer(
            Arc::clone(&observer),
            crate::cadapter::read_observer::Engine::Native,
            crate::cadapter::read_observer::Phase::Startup,
            crate::cadapter::read_observer::Origin::Demand,
        )
        .with_phase_control(Arc::clone(&phase));
    let store = create_object_store(client.clone(), layout, &args.cache, true).await?;
    let sink: Arc<dyn ObjectSink> = Arc::new(BackendObjectRepository::new(client));
    let packed_budget = workspace_overlay::packed_v3::wire005::V3MountBudget::from_env()?;
    match args.meta_backend {
        MetaBackendKind::Redis => {
            let control: Arc<dyn ControlStore> = Arc::new(
                RedisControlStore::connect(&args.meta_url)
                    .await
                    .map_err(|error| anyhow::anyhow!(error))?,
            );
            let backend =
                RedisWorkspaceBackend::connect(&args.meta_url, &args.workspace_namespace).await?;
            let catalog = Arc::new(
                KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(packed_budget.clone()),
            );
            mount_native_with_catalog(layout, store, sink, args, control, catalog, observer, phase)
                .await
        }
        MetaBackendKind::TiKv => {
            let control: Arc<dyn ControlStore> = Arc::new(
                TiKvControlStore::connect(&args.meta_tikv_pd_endpoints)
                    .await
                    .map_err(|error| anyhow::anyhow!(error))?,
            );
            let backend = connect_workspace_tikv(
                args.meta_tikv_pd_endpoints.clone(),
                &args.workspace_namespace,
                packed_budget.clone(),
                args.meta_tikv_tls.as_ref(),
            )
            .await?;
            let catalog = Arc::new(
                KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(packed_budget.clone()),
            );
            let result = mount_native_with_catalog(
                layout,
                store,
                sink,
                args,
                control,
                catalog.clone(),
                observer,
                phase,
            )
            .await;
            let shutdown = catalog.shutdown_metadata_backend().await;
            result?;
            shutdown?;
            Ok(())
        }
        MetaBackendKind::Sqlx | MetaBackendKind::Etcd => {
            anyhow::bail!(
                "workspace-native-v2 supports only Redis or TiKV control/catalog backends"
            )
        }
    }
}

#[cfg(feature = "native-packed-base")]
async fn mount_native_with_catalog<S, W>(
    layout: ChunkLayout,
    store: S,
    sink: Arc<dyn ObjectSink>,
    args: &MountConfig,
    control: Arc<dyn ControlStore>,
    workspace_store: Arc<W>,
    observer: Arc<crate::cadapter::read_observer::ReadObserver>,
    phase: Arc<std::sync::atomic::AtomicU8>,
) -> anyhow::Result<()>
where
    S: BlockStore + Send + Sync + 'static,
    W: WorkspaceStore + 'static,
{
    let workspace_id = args
        .workspace
        .map(WorkspaceId::from_uuid)
        .ok_or_else(|| anyhow::anyhow!("--workspace is required for workspace-native-v2"))?;
    let workspace_header = workspace_store
        .load_volume_header()
        .await?
        .ok_or_else(|| anyhow::anyhow!("corrupt workspace metadata: volume marker is missing"))?;
    if workspace_header.volume_format != "workspace-v1"
        || workspace_header.schema_version != WORKSPACE_SCHEMA_VERSION
    {
        anyhow::bail!(
            "native P1 requires a workspace-v1 schema-1 metadata base; found {} schema {}",
            workspace_header.volume_format,
            workspace_header.schema_version
        );
    }

    let native_header = load_volume_header(
        control.as_ref(),
        &args.workspace_namespace,
        NativeRuntimeCapabilities::compiled(),
    )
    .await
    .map_err(|error| anyhow::anyhow!(error))?;
    if native_header.required_features & FROZEN_METADATA_FEATURE != 0 {
        anyhow::bail!(
            "native volume requires frozen metadata, but the mutable P1 KV mount is not a valid fallback"
        );
    }
    let volume_id = *workspace_header.volume_id.as_bytes();
    if native_header.volume_id != volume_id {
        anyhow::bail!("native volume header volume_id does not match workspace metadata volume_id");
    }
    let expected_namespace_id = native_namespace_id(&args.workspace_namespace, &volume_id);
    if native_header.storage_namespace_id != expected_namespace_id {
        anyhow::bail!(
            "native volume header storage namespace does not match the configured workspace namespace"
        );
    }

    let generation = new_workspace_holder_generation();
    let session = WorkspaceMountSession::acquire(
        workspace_store.clone(),
        workspace_id,
        generation,
        DEFAULT_LEASE_TTL,
        DEFAULT_HEARTBEAT_INTERVAL,
    )
    .await?;

    let keys = Keys::new(&volume_id);
    let workspace_bytes = *workspace_id.as_bytes();
    let domain_id = match control.get(&keys.head(&workspace_bytes)).await? {
        Some(bytes) => HeadState::decode(&bytes)?.write_domain_id,
        None => *uuid::Uuid::now_v7().as_bytes(),
    };
    let params = OverlayParams {
        volume_id,
        workspace_id: workspace_bytes,
        domain_id,
        writer_generation: session.view.holder_generation,
        block_size: layout.block_size as u64,
    };
    let block_store = Arc::new(store);
    let mut meta_layer = WorkspaceMetaLayer::with_chunk_size(
        workspace_store,
        session.view.clone(),
        layout.chunk_size,
    );
    if let Some(max_weight) = args.meta_read_plan_cache_max_weight {
        meta_layer = meta_layer.with_read_plan_cache_max_weight(max_weight);
    }
    let meta_layer = Arc::new(meta_layer);
    meta_layer.initialize().await?;
    let overlay = Arc::new(WriteOverlay::new(control.clone(), sink, params));
    let runtime = Arc::new(NativeDataRuntime::new(
        overlay.clone(),
        Arc::new(WorkspaceBaseDataSource::new(
            block_store.clone(),
            meta_layer.clone(),
            layout,
        )),
    ));
    runtime
        .initialize(*uuid::Uuid::now_v7().as_bytes(), session.view.head_epoch)
        .await
        .map_err(|error| anyhow::anyhow!(error))?;

    let writeback_root = crate::workspace_overlay::cache_scope::writeback_root(
        &args.cache.cache_root,
        uuid::Uuid::from_bytes(native_header.volume_id),
        workspace_id,
        session.view.head_epoch,
    )?;
    let vfs_config =
        crate::vfs::config::VFSConfig::new_with_cache_config(layout, args.cache.clone())
            .workspace_writeback_root(writeback_root)
            .workspace_writer_epoch(session.view.holder_generation);
    phase.store(1, std::sync::atomic::Ordering::Release);
    let fs = VFS::from_workspace_components(vfs_config, block_store, meta_layer)?;
    fs.set_fuse_cache_ttl(Duration::from_millis(args.fuse_cache_ttl_ms));
    fs.stats().set_extension(Arc::new(
        crate::cadapter::read_observer::NativeReadStatsExtension(observer),
    ));
    fs.attach_native_runtime(runtime)
        .map_err(anyhow::Error::from)?;
    let concurrency = FuseConcurrencyConfig {
        worker_count: args.fuse_workers,
        max_background: args.fuse_max_background,
    };
    let mount_result = async {
        let handle = if args.privileged {
            mount_vfs_privileged(fs, &args.mount_point, concurrency).await?
        } else {
            mount_vfs_unprivileged(fs, &args.mount_point, concurrency).await?
        };
        println!(
            "mounted native workspace {} at {}",
            workspace_id,
            args.mount_point.display()
        );
        let mut handle = handle;
        tokio::select! {
            signal = shutdown_signal() => {
                signal?;
                println!("unmounting...");
                handle.unmount().await?;
            }
            result = &mut handle => {
                result?;
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let release_result = session.release().await;
    mount_result?;
    release_result?;
    Ok(())
}

#[cfg(feature = "native-packed-base")]
fn native_namespace_id(namespace: &str, volume_id: &[u8; 16]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(b"brewfs/native-storage-namespace/v2");
    hasher.update(namespace.as_bytes());
    hasher.update(volume_id);
    hasher.finalize()[..16]
        .try_into()
        .expect("sha256 prefix length")
}

async fn create_object_store<B>(
    client: ObjectClient<B>,
    layout: ChunkLayout,
    cache: &crate::vfs::cache::config::CacheConfig,
    create_only_writes: bool,
) -> anyhow::Result<ObjectBlockStore<B>>
where
    B: ObjectBackend + Send + Sync + 'static,
{
    let reuse_writeback_stage = cache.persist_write_cache_after_upload
        && matches!(
            cache.writeback_mode,
            crate::vfs::cache::config::WriteBackMode::CommitBeforeUpload
        );
    let chunks_cache_config = ChunksCacheConfig::with_budgets(
        cache.read_memory_bytes,
        cache.read_ssd_bytes,
        cache.cache_root.join("chunks"),
    )
    .with_integrity_mode(cache.verify_cache_checksum);
    let mut block_store_config = BlockStoreConfig {
        block_size: layout.block_size as usize,
        compression: cache.compression,
        range_background_prefetch: cache.range_background_prefetch,
        populate_write_cache_after_upload: cache.populate_write_cache_after_upload,
        persist_write_cache_after_upload: cache.persist_write_cache_after_upload
            && !reuse_writeback_stage,
        persistent_slice_cache_dir: reuse_writeback_stage.then(|| cache.cache_root.join("chunks")),
        create_only_writes,
        ..BlockStoreConfig::default()
    };
    if cache.read_memory_bytes == 0 {
        // A zero memory budget is the explicit no-read-cache profile. The
        // range/page cache is independent of the disk block cache, so disable
        // it explicitly instead of retaining the default 256 MiB page tier.
        block_store_config.page_cache_capacity = 0;
    }
    let bandwidth = BandwidthLimiter::new(&cache.bandwidth);

    Ok(
        ObjectBlockStore::new_with_configs_async(client, chunks_cache_config, block_store_config)
            .await?
            .with_bandwidth(bandwidth),
    )
}

fn create_localfs_client(args: &MountConfig) -> anyhow::Result<ObjectClient<LocalFsBackend>> {
    if !args.data_dir.exists() {
        std::fs::create_dir_all(&args.data_dir)?;
    }
    if !args.data_dir.is_dir() {
        anyhow::bail!("data dir must be a directory");
    }
    Ok(ObjectClient::new(LocalFsBackend::new(&args.data_dir)))
}

async fn create_s3_client(args: &MountConfig) -> anyhow::Result<ObjectClient<S3Backend>> {
    let bucket = args
        .s3_bucket
        .clone()
        .ok_or_else(|| anyhow::anyhow!("s3 bucket must be set when data backend is s3"))?;

    create_s3_client_from_config(S3Config {
        bucket,
        region: args.s3_region.clone(),
        endpoint: args.s3_endpoint.clone(),
        part_size: args.s3_part_size,
        max_concurrency: args.s3_max_concurrency,
        force_path_style: args.s3_force_path_style,
        disable_payload_checksum: args.s3_disable_payload_checksum,
        rustfs_ec_block_size_hint: args.s3_rustfs_ec_block_size_hint,
        ..Default::default()
    })
    .await
}

async fn create_s3_client_from_config(config: S3Config) -> anyhow::Result<ObjectClient<S3Backend>> {
    if config.bucket.is_empty() {
        anyhow::bail!("s3 bucket must not be empty");
    }

    if config.part_size == 0 {
        anyhow::bail!("--s3-part-size must be greater than 0");
    }
    if config.max_concurrency == 0 {
        anyhow::bail!("--s3-max-concurrency must be greater than 0");
    }

    let backend = S3Backend::with_config(config).await?;
    Ok(ObjectClient::new(backend))
}

async fn object_put_bench_cmd(args: ObjectPutBenchArgs) -> anyhow::Result<()> {
    if args.object_size == 0 {
        anyhow::bail!("--object-size must be greater than 0");
    }
    if args.workers == 0 {
        anyhow::bail!("--workers must be greater than 0");
    }
    if args.duration_secs == 0 && args.objects == 0 {
        anyhow::bail!("either --duration-secs or --objects must be greater than 0");
    }

    let client = create_s3_client_from_config(S3Config {
        bucket: args.s3_bucket.clone(),
        region: Some(args.s3_region.clone()),
        endpoint: args.s3_endpoint.clone(),
        part_size: args.s3_part_size,
        max_concurrency: args.s3_max_concurrency,
        force_path_style: args.s3_force_path_style,
        disable_payload_checksum: args.s3_disable_payload_checksum,
        rustfs_ec_block_size_hint: args.s3_rustfs_ec_block_size_hint,
        ..Default::default()
    })
    .await?;
    let payload = Bytes::from(pattern_payload(args.object_size));
    let prefix = format!(
        "{}/{}",
        args.prefix.trim_end_matches('/'),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );

    let issued = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let lat_us_total = Arc::new(AtomicU64::new(0));
    let lat_us_max = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(Mutex::new(Vec::new()));
    let started = Instant::now();
    let deadline = if args.duration_secs == 0 {
        None
    } else {
        Some(started + Duration::from_secs(args.duration_secs))
    };
    let max_objects = if args.objects == 0 {
        None
    } else {
        Some(args.objects)
    };

    let mut handles = Vec::with_capacity(args.workers);
    for worker in 0..args.workers {
        let client = client.clone();
        let payload = payload.clone();
        let prefix = prefix.clone();
        let issued = issued.clone();
        let completed = completed.clone();
        let bytes = bytes.clone();
        let lat_us_total = lat_us_total.clone();
        let lat_us_max = lat_us_max.clone();
        let latencies = latencies.clone();
        let object_size = args.object_size as u64;

        handles.push(tokio::spawn(async move {
            loop {
                if let Some(deadline) = deadline
                    && Instant::now() >= deadline
                {
                    break;
                }

                let index = issued.fetch_add(1, Ordering::Relaxed);
                if let Some(max_objects) = max_objects
                    && index >= max_objects
                {
                    break;
                }

                let key = format!("{prefix}/worker-{worker}/{index:020}");
                let started = Instant::now();
                client
                    .put_object_vectored(&key, vec![payload.clone()])
                    .await?;
                let elapsed_us = started.elapsed().as_micros() as u64;

                completed.fetch_add(1, Ordering::Relaxed);
                bytes.fetch_add(object_size, Ordering::Relaxed);
                lat_us_total.fetch_add(elapsed_us, Ordering::Relaxed);
                lat_us_max.fetch_max(elapsed_us, Ordering::Relaxed);
                latencies
                    .lock()
                    .expect("object-put latency vector poisoned")
                    .push(elapsed_us);
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for handle in handles {
        match handle.await {
            Ok(result) => result?,
            Err(err) => anyhow::bail!("object PUT worker join failed: {err}"),
        }
    }

    let elapsed = started.elapsed().as_secs_f64();
    let completed = completed.load(Ordering::Relaxed);
    let bytes = bytes.load(Ordering::Relaxed);
    let avg_ms = if completed == 0 {
        0.0
    } else {
        lat_us_total.load(Ordering::Relaxed) as f64 / completed as f64 / 1000.0
    };
    let mut latencies = latencies
        .lock()
        .expect("object-put latency vector poisoned")
        .clone();
    latencies.sort_unstable();

    println!(
        "object_put_bench_summary objects={} bytes={} seconds={:.6} throughput_mib_s={:.3} workers={} object_size={} avg_ms={:.3} p50_ms={:.3} p90_ms={:.3} p95_ms={:.3} p99_ms={:.3} max_ms={:.3} endpoint={} bucket={} prefix={}",
        completed,
        bytes,
        elapsed,
        if elapsed > 0.0 {
            bytes as f64 / 1048576.0 / elapsed
        } else {
            0.0
        },
        args.workers,
        args.object_size,
        avg_ms,
        percentile_ms(&latencies, 0.50),
        percentile_ms(&latencies, 0.90),
        percentile_ms(&latencies, 0.95),
        percentile_ms(&latencies, 0.99),
        lat_us_max.load(Ordering::Relaxed) as f64 / 1000.0,
        args.s3_endpoint.as_deref().unwrap_or("default"),
        args.s3_bucket,
        prefix,
    );

    Ok(())
}

fn pattern_payload(size: usize) -> Vec<u8> {
    (0..size).map(|idx| (idx % 251) as u8).collect()
}

fn percentile_ms(sorted_latencies_us: &[u64], percentile: f64) -> f64 {
    if sorted_latencies_us.is_empty() {
        return 0.0;
    }
    let index = ((sorted_latencies_us.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted_latencies_us.len() - 1);
    sorted_latencies_us[index] as f64 / 1000.0
}

async fn mount_with_store<S>(
    layout: ChunkLayout,
    store: S,
    meta_store: Arc<dyn MetaStore>,
    args: &MountConfig,
) -> anyhow::Result<()>
where
    S: BlockStore + Send + Sync + 'static,
{
    let mount_point = &args.mount_point;
    let store = Arc::new(store);
    let mut meta_config = MetaClientConfig::default();
    meta_config.options.mount_point = Some(mount_point.display().to_string());
    if let Some(ttl_ms) = args.meta_open_file_cache_ttl_ms {
        meta_config.options.open_file_cache.ttl = Duration::from_millis(ttl_ms);
    }
    if let Some(capacity) = args.meta_open_file_cache_capacity {
        meta_config.options.open_file_cache.capacity = capacity;
    }
    meta_config.options.open_file_cache.allow_write = args.meta_allow_write_open_cache;
    if let Some(interval_ms) = args.meta_slice_version_check_interval_ms {
        meta_config.options.slice_version_check_interval = Duration::from_millis(interval_ms);
    }
    meta_config.compact = args.compact.clone();

    tracing::info!("mount startup meta client create begin");
    let meta_client = MetaClient::with_options(
        meta_store,
        meta_config.capacity.clone(),
        meta_config.effective_ttl(),
        meta_config.options,
    );
    tracing::info!("mount startup meta client create complete");
    tracing::info!("mount startup meta client initialize begin");
    meta_client
        .initialize()
        .await
        .map_err(anyhow::Error::from)?;
    tracing::info!("mount startup meta client initialize complete");
    tracing::info!("mount startup control plane begin");
    meta_client
        .start_control_plane()
        .await
        .map_err(anyhow::Error::from)?;
    tracing::info!("mount startup control plane complete");

    tracing::info!("mount startup vfs create begin");
    let fs = VFS::with_meta_layer_with_cache_config(
        layout,
        store,
        meta_client.clone(),
        meta_config.compact.clone(),
        args.cache.clone(),
    )
    .map_err(anyhow::Error::from)?;
    tracing::info!("mount startup vfs create complete");
    fs.set_fuse_cache_ttl(Duration::from_millis(args.fuse_cache_ttl_ms));
    let concurrency = FuseConcurrencyConfig {
        worker_count: args.fuse_workers,
        max_background: args.fuse_max_background,
    };
    tracing::info!(
        privileged = args.privileged,
        worker_count = args.fuse_workers,
        max_background = args.fuse_max_background,
        "mount startup fuse mount begin"
    );
    let handle = if args.privileged {
        mount_vfs_privileged(fs, mount_point, concurrency).await?
    } else {
        mount_vfs_unprivileged(fs, mount_point, concurrency).await?
    };

    println!("mounted at {}", mount_point.display());
    let mut handle = handle;
    tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            println!("unmounting...");
            handle.unmount().await?;
        }
        result = &mut handle => {
            result?;
        }
    }
    meta_client.shutdown_runtime().await;
    Ok(())
}

#[cfg(feature = "workspace-overlay")]
async fn connect_workspace_tikv(
    endpoints: Vec<String>,
    namespace: &str,
    budget: Arc<workspace_overlay::packed_v3::wire005::V3MountBudget>,
    tls: Option<&crate::config::TiKvTlsFileConfig>,
) -> anyhow::Result<TiKvWorkspaceBackend> {
    match tls {
        Some(tls) => {
            tls.validate()?;
            let security = workspace_overlay::stores::tikv::TiKvTlsConfig::from_paths(
                tls.ca_path.clone(),
                tls.cert_path.clone(),
                tls.key_path.clone(),
            )?;
            Ok(TiKvWorkspaceBackend::connect_with_tls_and_budget(
                endpoints, namespace, budget, security,
            )
            .await?)
        }
        None => Ok(TiKvWorkspaceBackend::connect_with_budget(endpoints, namespace, budget).await?),
    }
}

#[cfg(feature = "workspace-overlay")]
async fn mount_workspace_with_store<S>(
    layout: ChunkLayout,
    store: S,
    args: &MountConfig,
) -> anyhow::Result<()>
where
    S: BlockStore + Send + Sync + 'static,
{
    let store = if args.workspace_operator_managed {
        mount_store::MountStore::Operator(crate::chunk::runtime_store::RuntimeBlockStore::new(
            store,
        ))
    } else {
        mount_store::MountStore::Standalone(store)
    };
    mount_workspace_with_store_dispatch(layout, store, args).await
}

#[cfg(feature = "workspace-overlay")]
async fn mount_workspace_with_store_dispatch<S>(
    layout: ChunkLayout,
    store: S,
    args: &MountConfig,
) -> anyhow::Result<()>
where
    S: BlockStore + Send + Sync + 'static,
{
    let packed_budget = workspace_overlay::packed_v3::wire005::V3MountBudget::from_env()?;
    match args.meta_backend {
        MetaBackendKind::Sqlx => {
            if !args.meta_url.starts_with("sqlite:") {
                anyhow::bail!("workspace-v1 sqlx catalog requires a SQLite URL")
            }
            let catalog = Arc::new(SqliteWorkspaceStore::connect(&args.meta_url).await?);
            mount_workspace_with_catalog(layout, store, args, catalog, packed_budget).await
        }
        MetaBackendKind::Redis => {
            let backend =
                RedisWorkspaceBackend::connect(&args.meta_url, &args.workspace_namespace).await?;
            let catalog = KvWorkspaceStore::new(backend)
                .with_packed_reader_pin_budget(packed_budget.clone())
                .with_inode_id_blocks();
            let catalog = Arc::new(if args.workspace_operator_managed {
                catalog.into_runtime()
            } else {
                catalog
            });
            mount_workspace_with_catalog(layout, store, args, catalog, packed_budget).await
        }
        MetaBackendKind::TiKv => {
            let backend = connect_workspace_tikv(
                args.meta_tikv_pd_endpoints.clone(),
                &args.workspace_namespace,
                packed_budget.clone(),
                args.meta_tikv_tls.as_ref(),
            )
            .await?;
            let catalog = KvWorkspaceStore::new(backend)
                .with_packed_reader_pin_budget(packed_budget.clone())
                .with_inode_id_blocks();
            let catalog = Arc::new(if args.workspace_operator_managed {
                catalog.into_runtime()
            } else {
                catalog
            });
            let result =
                mount_workspace_with_catalog(layout, store, args, catalog.clone(), packed_budget)
                    .await;
            if packed_mount_cutoff::requires_retention(&result) {
                // An uncertain kernel cutoff or unfinished packed drain still
                // needs the real SDK drivers for writer/reader renewal and I/O.
                return result;
            }
            let shutdown = catalog.shutdown_metadata_backend().await;
            result?;
            shutdown?;
            Ok(())
        }
        MetaBackendKind::Etcd => {
            anyhow::bail!("workspace-v1 does not support the etcd catalog backend")
        }
    }
}

#[cfg(feature = "workspace-overlay")]
async fn mount_workspace_with_catalog<S, W>(
    layout: ChunkLayout,
    store: S,
    args: &MountConfig,
    workspace_store: Arc<W>,
    packed_budget: Arc<workspace_overlay::packed_v3::wire005::V3MountBudget>,
) -> anyhow::Result<()>
where
    S: BlockStore + Send + Sync + 'static,
    W: workspace_overlay::lifecycle::PackedMountCatalog,
{
    let workspace_id = args.workspace.map(WorkspaceId::from_uuid).ok_or_else(|| {
        anyhow::anyhow!("--workspace is required when volume_format is workspace-v1")
    })?;
    let packed_header = workspace_store.packed_mount_header(workspace_id).await?;
    let header = match packed_header {
        Some(header) => header,
        None => workspace_store.load_volume_header().await?.ok_or_else(|| {
            anyhow::anyhow!("corrupt workspace metadata: volume marker is missing")
        })?,
    };
    if header.volume_format != "workspace-v1" {
        anyhow::bail!(
            "workspace volume marker mismatch: expected workspace-v1, found {}",
            header.volume_format
        )
    }
    if header.schema_version != WORKSPACE_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported workspace schema version {}",
            header.schema_version
        )
    }

    let generation = new_workspace_holder_generation();
    let mut session = Some(
        WorkspaceMountSession::acquire_for_mount(
            workspace_store.clone(),
            workspace_id,
            generation,
            DEFAULT_LEASE_TTL,
            DEFAULT_HEARTBEAT_INTERVAL,
            args.workspace_operator_managed,
            packed_budget.clone(),
        )
        .await?,
    );
    let mount_view = session
        .as_ref()
        .expect("mount session acquired")
        .view
        .clone();
    let block_store = Arc::new(store);
    let gc_cancel = tokio_util::sync::CancellationToken::new();
    let gc_task = if args.workspace_operator_managed {
        None
    } else {
        let cancel = gc_cancel.clone();
        let gc = WorkspaceGc::new(
            workspace_store.clone(),
            block_store.clone(),
            layout,
            DEFAULT_LEASE_TTL.saturating_mul(2),
            DEFAULT_LEASE_TTL.saturating_mul(2),
        );
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(DEFAULT_LEASE_TTL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(error) = gc.run_once().await {
                            tracing::warn!(?error, "workspace layer/orphan GC cycle failed");
                        }
                    }
                }
            }
        }))
    };
    let mut packed_metadata = None;
    let mut packed_vfs = None;
    let mut packed_mount_attempted = false;
    let mut packed_cutoff_verified = false;
    let mut packed_kernel_cutoff = None;
    let mut packed_clean_reference = None;
    let mount_result = async {
        let mut meta_layer = WorkspaceMetaLayer::with_chunk_size(
            workspace_store.clone(),
            mount_view.clone(),
            layout.chunk_size,
        );
        let view = &mount_view;
        let binding = workspace_store
            .load_packed_lower_binding(workspace_overlay::catalog::HeadGuard {
                workspace_id: view.workspace_id,
                expected_head_layer_id: view.head_layer_id,
                expected_head_epoch: view.head_epoch,
                lease_id: view.lease_id,
                holder_generation: view.holder_generation,
            })
            .await?;
        if let Some(binding) = binding {
            if !workspace_store.supports_packed_workspace_mount() {
                anyhow::bail!("packed workspace binding requires complete catalog permission/ID/lifecycle capabilities");
            }
            meta_layer = match args.data_backend {
                DataBackendKind::LocalFs => {
                    attach_workspace_packed_lower(
                        meta_layer,
                        binding,
                        create_localfs_client(args)?,
                        block_store.clone(),
                        layout,
                        packed_budget.clone(),
                    )
                    .await?
                }
                DataBackendKind::S3 => {
                    attach_workspace_packed_lower(
                        meta_layer,
                        binding,
                        create_s3_client(args).await?,
                        block_store.clone(),
                        layout,
                        packed_budget.clone(),
                    )
                    .await?
                }
            };
        }
        if let Some(max_weight) = args.meta_read_plan_cache_max_weight {
            meta_layer = meta_layer.with_read_plan_cache_max_weight(max_weight);
        }
        // This mount holds the exclusive workspace lease, so it is the only
        // upper writer. The caches are bound to that lease: they serve rows
        // only while a renewal proves it is held and are cleared once it
        // lapses or fails, so the TTL here is a working-set bound, not the
        // staleness bound.
        let row_cache_ttl = std::env::var("BREWFS_WORKSPACE_ROW_CACHE_TTL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(LEASE_BOUND_ROW_CACHE_TTL);
        let lease_validity = session
            .as_ref()
            .expect("mount session acquired")
            .lease_validity();
        meta_layer = meta_layer
            .with_upper_row_cache(row_cache_ttl)
            .with_lease_bound_row_cache(lease_validity);
        let meta_layer = Arc::new(meta_layer);
        let packed = meta_layer.packed_reader_session().is_some();
        if packed {
            packed_metadata = Some(meta_layer.clone());
            packed_clean_reference = session.as_ref().and_then(|session| session.packed_mount_reference());
            if packed_clean_reference.is_none() {
                return Err(anyhow::anyhow!("packed-v3 joint mount authority missing"));
            }
        }
        meta_layer.initialize().await?;
        let writeback_root = crate::workspace_overlay::cache_scope::writeback_root(
            &args.cache.cache_root,
            header.volume_id,
            workspace_id,
            mount_view.head_epoch,
        )?;
        let vfs_config =
            crate::vfs::config::VFSConfig::new_with_cache_config(layout, args.cache.clone())
                .workspace_writeback_root(writeback_root)
                .workspace_writer_epoch(mount_view.holder_generation);
        session.as_ref().expect("mount session retained").initialize_packed_writeback_identity(&vfs_config).await?;
        let fs = VFS::from_workspace_components(vfs_config, block_store, meta_layer.clone())?;
        fs.set_fuse_cache_ttl(Duration::from_millis(args.fuse_cache_ttl_ms));
        if packed {
            meta_layer.install_packed_lower_stats(fs.stats());
            packed_vfs = Some(fs.clone());
        }
        let concurrency = FuseConcurrencyConfig {
            worker_count: args.fuse_workers,
            max_background: args.fuse_max_background,
        };
        let packed_mount_path = if packed {
            Some(packed_mount_cutoff::prepare(&args.mount_point, &packed_budget)?)
        } else {
            None
        };
        packed_mount_attempted = packed;
        if packed { session.as_mut().expect("mount session retained").mark_packed_attachment_attempted(); }
        let handle = if args.privileged {
            mount_vfs_privileged(fs, &args.mount_point, concurrency).await?
        } else {
            mount_vfs_unprivileged(fs, &args.mount_point, concurrency).await?
        };
        let mut packed_mount_identity = if let Some(path) = &packed_mount_path {
            let original = packed_vfs.as_ref().expect("packed original VFS retained").clone();
            match packed_mount_cutoff::OwnedPackedMountIdentity::capture(
                path, original, packed_budget.clone()) {
                Ok(identity) => Some(identity),
                Err(primary) => {
                    let _ = handle.unmount().await;
                    return Err(anyhow::Error::from(primary));
                }
            }
        } else { None };
        println!(
            "mounted workspace {} at {}",
            workspace_id,
            args.mount_point.display()
        );
        let mut handle = handle;
        tokio::select! {
            signal = shutdown_signal() => {
                println!("unmounting...");
                // Await physical unmount and the Session/ordinary-worker joins
                // before freezing admission, even if signal subscription failed.
                let unmount_result = handle.unmount().await;
                let cutoff_result = if unmount_result.is_ok() {
                    if let Some(identity) = packed_mount_identity.take() {
                        identity.after_worker_join().map(|cutoff| {
                            packed_kernel_cutoff = Some(cutoff);
                            true
                        })
                    } else { Ok(!packed) }
                } else { Ok(false) };
                packed_cutoff_verified = packed && matches!(cutoff_result, Ok(true));
                signal?;
                unmount_result?;
                cutoff_result?;
            }
            result = &mut handle => {
                result?;
                // Session completion proves worker joins. Only absence of the
                // actual captured kernel mount ID also proves physical cutoff.
                if let Some(identity) = packed_mount_identity.take() {
                    packed_kernel_cutoff = Some(identity.after_worker_join()?);
                    packed_cutoff_verified = true;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    gc_cancel.cancel();
    if let Some(gc_task) = gc_task {
        let _ = gc_task.await;
    }
    if packed_mount_attempted && !packed_cutoff_verified {
        let primary = mount_result.err().unwrap_or_else(|| {
            anyhow::anyhow!("packed FUSE Session ended while its physical mount is still present")
        });
        return Err(
            primary.context(packed_mount_cutoff::RetainPackedRuntime::holding((
                session,
                packed_metadata,
                packed_vfs,
            ))),
        );
    }
    // Stop the joint lease/open heartbeat before taking the VFS drain fence.
    // The heartbeat mutates the same workspace authority packet that native
    // Prepare/Quiesced uses for its exact CAS. If it remains active while the
    // drain is captured, a slower TiKV round trip can repeatedly observe a
    // fresh lease generation and exhaust the native-begin retry budget.
    // Physical workers have already joined and the kernel cutoff is verified,
    // so closing renewal admission here leaves the bounded drain window
    // protected by the existing lease deadline.
    let renewal_shutdown_result = session
        .as_ref()
        .expect("mount session retained")
        .close_packed_renewals_for_shutdown()
        .await
        .map_err(anyhow::Error::from);
    let mut packed_drain_result = if renewal_shutdown_result.is_ok() {
        if let Some(fs) = &packed_vfs {
            fs.quiesce_packed_vfs()
                .await
                .map(Some)
                .map_err(anyhow::Error::from)
        } else {
            Ok(None)
        }
    } else {
        Err(anyhow::anyhow!(
            "packed renewal shutdown has no terminal witness"
        ))
    };
    let mut metadata_shutdown_result = if packed_drain_result.is_ok()
        && renewal_shutdown_result.is_ok()
    {
        if !packed_mount_attempted && let Some(metadata) = packed_metadata.as_ref() {
            metadata
                .shutdown_packed_runtime_for_clean_release()
                .await
                .map(|_| None)
                .map_err(anyhow::Error::from)
        } else if let Some(reference) = packed_clean_reference {
            match (packed_drain_result.as_mut().ok().and_then(Option::take),
                   packed_kernel_cutoff.take()) {
                (Some(drain), Some(cutoff)) => {
                    workspace_overlay::packed_shutdown::VerifiedCleanPackedShutdown::from_original_shutdown(
                        reference, drain, cutoff, &workspace_store).await
                        .map(Some).map_err(anyhow::Error::from)
                }
                _ => Err(anyhow::anyhow!("packed original shutdown authority missing")),
            }
        } else {
            match packed_metadata.as_ref() {
                Some(metadata) => metadata
                    .shutdown_session()
                    .await
                    .map(|_| None)
                    .map_err(anyhow::Error::from),
                None => Ok(None),
            }
        }
    } else {
        Err(anyhow::anyhow!(
            "packed VFS drain failed; lower pin retained"
        ))
    };
    let release_result = if metadata_shutdown_result.is_ok() {
        match metadata_shutdown_result
            .as_mut()
            .ok()
            .and_then(Option::take)
        {
            Some(proof) => session
                .take()
                .expect("mount session retained")
                .release_clean(proof)
                .await
                .map_err(anyhow::Error::from),
            None => session
                .take()
                .expect("mount session retained")
                .release()
                .await
                .map_err(anyhow::Error::from),
        }
    } else {
        Err(anyhow::anyhow!(
            "packed metadata teardown failed; workspace lease not explicitly released"
        ))
    };
    if packed_drain_result.is_err()
        || renewal_shutdown_result.is_err()
        || metadata_shutdown_result.is_err()
    {
        let primary = mount_result
            .err()
            .or_else(|| packed_drain_result.err())
            .or_else(|| renewal_shutdown_result.err())
            .or_else(|| metadata_shutdown_result.err())
            .expect("failed packed cleanup has an error");
        return Err(
            primary.context(packed_mount_cutoff::RetainPackedRuntime::holding((
                session,
                packed_metadata,
                packed_vfs,
            ))),
        );
    }
    mount_result?;
    packed_drain_result?;
    renewal_shutdown_result?;
    metadata_shutdown_result?;
    release_result?;
    Ok(())
}

#[cfg(feature = "workspace-overlay")]
async fn attach_workspace_packed_lower<B, S, W>(
    meta: WorkspaceMetaLayer<W>,
    binding: workspace_overlay::catalog::PackedLowerBinding,
    client: ObjectClient<B>,
    upper: Arc<S>,
    layout: ChunkLayout,
    budget: Arc<workspace_overlay::packed_v3::wire005::V3MountBudget>,
) -> anyhow::Result<WorkspaceMetaLayer<W>>
where
    B: ObjectBackend + Clone + Send + Sync + 'static,
    S: BlockStore + Send + Sync + 'static,
    W: WorkspaceStore + 'static,
{
    use workspace_overlay::packed_v3::wire005::{AuthenticatedV3Snapshot, V3BudgetPool};
    let view = meta.view_context().await;
    let reader_guard = workspace_overlay::catalog::HeadGuard {
        workspace_id: view.workspace_id,
        expected_head_layer_id: view.head_layer_id,
        expected_head_epoch: view.head_epoch,
        lease_id: view.lease_id,
        holder_generation: view.holder_generation,
    };
    let reader = meta
        .store()
        .clone()
        .open_packed_reader_session(
            reader_guard.clone(),
            budget,
            workspace_overlay::packed_reader_lifecycle::PackedReaderLeaseOptions {
                holder_generation: view.holder_generation,
                ..Default::default()
            },
        )
        .await?;
    let authority = Arc::new(
        workspace_overlay::meta_layer::PinnedCatalogPackedBindingAuthority {
            store: meta.store().clone(),
            reader: reader.clone(),
        },
    );
    let budget = reader.mount_budget();
    let result = async {
        let _startup_owner = reader.retain_request()?;
        if reader.binding() != &binding {
            anyhow::bail!("packed current binding changed during reader open");
        }

        let stored = binding
            .manifest
            .object_len
            .checked_mul(2)
            .ok_or_else(|| anyhow::anyhow!("packed binding manifest allocation overflows"))?;
        let startup = budget.admit(&[
            (V3BudgetPool::Roots, 128 << 10),
            (V3BudgetPool::Stored, stored),
        ])?;
        let observer =
            Arc::new(crate::cadapter::read_observer::ReadObserver::with_mount_budget(&budget)?);
        let client = client.with_read_observer(
            observer,
            crate::cadapter::read_observer::Engine::PackedV3,
            crate::cadapter::read_observer::Phase::Startup,
            crate::cadapter::read_observer::Origin::Demand,
        );
        let snapshot = AuthenticatedV3Snapshot::open(&client, &binding.manifest).await?;
        drop(startup);
        let metadata_bytes = std::env::var("BREWFS_PACKED_METADATA_CACHE_BYTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(256 << 20);
        let lower = Arc::new(PackedV3ReadonlyMeta::from_v3_budget(
            client,
            snapshot,
            layout.chunk_size,
            metadata_bytes,
            budget,
        )?);
        workspace_overlay::meta_layer::WorkspacePackedBindingAuthority::validate(
            authority.as_ref(),
            &reader_guard,
            &binding,
        )
        .await?;
        Ok(meta.with_packed_v3_lower(binding, lower, authority, upper, layout)?)
    }
    .await;
    if result.is_err()
        && let Err(error) = reader.shutdown().await
    {
        tracing::warn!(?error, "packed reader startup cleanup failed");
    }
    result
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

#[cfg(feature = "workspace-overlay")]
async fn workspace_cmd(args: WorkspaceArgs) -> anyhow::Result<()> {
    let WorkspaceArgs {
        meta_backend,
        meta_url,
        meta_tikv_pd_endpoints,
        meta_tikv_ca_path,
        meta_tikv_cert_path,
        meta_tikv_key_path,
        workspace_namespace,
        command,
    } = args;
    let tls = match (meta_tikv_ca_path, meta_tikv_cert_path, meta_tikv_key_path) {
        (None, None, None) => None,
        (Some(ca_path), Some(cert_path), Some(key_path)) => {
            let tls = crate::config::TiKvTlsFileConfig {
                ca_path,
                cert_path,
                key_path,
            };
            tls.validate()?;
            Some(tls)
        }
        _ => anyhow::bail!("TiKV TLS requires all three certificate paths"),
    };
    let command = match command {
        WorkspaceCommand::IndexNativeReverse(arguments) => {
            return native_reverse_maintenance::run(
                *arguments,
                meta_backend,
                meta_url,
                meta_tikv_pd_endpoints,
                tls,
                workspace_namespace,
            )
            .await;
        }
        WorkspaceCommand::RecoverPackedMount(arguments) => {
            return packed_mount_recovery::run(*arguments, meta_url, tls).await;
        }
        command => command,
    };
    if tls.is_some() && meta_backend != WorkspaceMetaBackendKind::TiKv {
        anyhow::bail!("TiKV TLS paths require the TiKV workspace catalog");
    }
    #[cfg(feature = "native-packed-base")]
    if tls.is_some() && matches!(&command, WorkspaceCommand::InitNative) {
        anyhow::bail!(
            "native control initialization does not support TiKV TLS; refusing plaintext fallback"
        );
    }
    let packed_budget = workspace_overlay::packed_v3::wire005::V3MountBudget::from_env()?;
    match meta_backend {
        WorkspaceMetaBackendKind::Sqlx => {
            #[cfg(feature = "native-packed-base")]
            if matches!(&command, WorkspaceCommand::InitNative) {
                anyhow::bail!(
                    "workspace-native-v2 initialization requires Redis or TiKV control backend"
                )
            }
            if !meta_url.starts_with("sqlite:") {
                anyhow::bail!("workspace-v1 sqlx catalog requires a SQLite URL")
            }
            workspace_cmd_with_catalog(
                command,
                Arc::new(SqliteWorkspaceStore::connect(&meta_url).await?),
            )
            .await
        }
        WorkspaceMetaBackendKind::Redis => {
            let backend = RedisWorkspaceBackend::connect(&meta_url, &workspace_namespace).await?;
            let catalog = Arc::new(
                KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(packed_budget.clone()),
            );
            #[cfg(feature = "native-packed-base")]
            if matches!(&command, WorkspaceCommand::InitNative) {
                let control: Arc<dyn ControlStore> = Arc::new(
                    RedisControlStore::connect(&meta_url)
                        .await
                        .map_err(|error| anyhow::anyhow!(error))?,
                );
                return init_native_volume_with_catalog(catalog, control, &workspace_namespace)
                    .await;
            }
            workspace_cmd_with_catalog(command, catalog).await
        }
        WorkspaceMetaBackendKind::TiKv => {
            let backend = connect_workspace_tikv(
                meta_tikv_pd_endpoints.clone(),
                &workspace_namespace,
                packed_budget.clone(),
                tls.as_ref(),
            )
            .await?;
            let catalog = Arc::new(
                KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(packed_budget.clone()),
            );
            let result = async {
                #[cfg(feature = "native-packed-base")]
                if matches!(&command, WorkspaceCommand::InitNative) {
                    let control: Arc<dyn ControlStore> = Arc::new(
                        TiKvControlStore::connect(
                            // The command consumes the endpoint vector above; the
                            // catalog backend has its own client, so read the
                            // canonical endpoint list from the CLI args instead.
                            &meta_tikv_pd_endpoints,
                        )
                        .await
                        .map_err(|error| anyhow::anyhow!(error))?,
                    );
                    return init_native_volume_with_catalog(
                        catalog.clone(),
                        control,
                        &workspace_namespace,
                    )
                    .await;
                }
                workspace_cmd_with_catalog(command, catalog.clone()).await
            }
            .await;
            let shutdown = catalog.shutdown_metadata_backend().await;
            result?;
            shutdown?;
            Ok(())
        }
    }
}

/// Create a workspace-v1 volume whose default workspace's sealed base is an
/// authenticated packed-v3 lower. The manifest is opened and verified through
/// the selected object store before any binding is written; the install lease
/// is released afterwards so a normal mount can acquire the workspace.
#[cfg(feature = "workspace-overlay")]
async fn init_packed_volume<W>(
    store: Arc<W>,
    manifest_key: &str,
    data_config: &std::path::Path,
    owner: Option<String>,
) -> anyhow::Result<crate::workspace_overlay::model::WorkspaceRecord>
where
    W: WorkspaceStore + 'static,
{
    use crate::workspace_overlay::catalog::{AcquireLease, HeadGuard, ReleaseLease};
    use crate::workspace_overlay::ids::LeaseId;
    use crate::workspace_overlay::model::BaseRevision;
    use crate::workspace_overlay::packed_v3::wire005::{
        AuthenticatedV3Snapshot, V3IndexReader, V3ObjectKind, V3ObjectRef,
    };
    use crate::workspace_overlay::publish::binding::{
        InstallPackedLowerBinding, VerifiedPackedLower,
    };

    if store.load_volume_header().await?.is_some() {
        anyhow::bail!("workspace volume is already initialized")
    }
    let component = manifest_key.rsplit('/').next().unwrap_or_default();
    let digest: [u8; 32] = hex::decode(component)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            anyhow::anyhow!("manifest key must end in its 64-hex SHA-256 content address")
        })?;
    // Reuse the mount configuration parser; only the data section matters.
    let cli = Cli::try_parse_from([
        std::ffi::OsStr::new("brewfs"),
        std::ffi::OsStr::new("mount"),
        std::ffi::OsStr::new("--config"),
        data_config.as_os_str(),
        std::ffi::OsStr::new("/nonexistent-init-packed-volume"),
    ])?;
    let Command::Mount(args) = cli.cmd else {
        anyhow::bail!("internal: mount argument parsing produced another command")
    };
    let config = MountConfig::from_sources(*args)?;

    async fn verify<B: crate::cadapter::client::ObjectBackend + Clone + 'static>(
        client: ObjectClient<B>,
        key: &str,
        digest: [u8; 32],
    ) -> anyhow::Result<VerifiedPackedLower> {
        let object_len = client
            .typed_object_size(
                crate::cadapter::read_observer::ReadClass::ManifestProbe,
                key,
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("packed manifest object is absent"))?;
        let reference = V3ObjectRef {
            key: key.to_owned(),
            kind: V3ObjectKind::Manifest,
            object_len,
            digest,
        };
        let snapshot = AuthenticatedV3Snapshot::open(&client, &reference).await?;
        Ok(VerifiedPackedLower::from_authenticated_snapshot(
            &snapshot,
            &V3IndexReader::new(client.clone(), 0),
        )
        .await?)
    }
    let lower = match config.data_backend {
        DataBackendKind::LocalFs => {
            verify(create_localfs_client(&config)?, manifest_key, digest).await?
        }
        DataBackendKind::S3 => {
            verify(create_s3_client(&config).await?, manifest_key, digest).await?
        }
    };

    store.initialize_workspace_schema().await?;
    let workspace = store
        .create_volume_root(CreateVolumeRoot {
            volume_format: "workspace-v1".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: uuid::Uuid::now_v7(),
            workspace_id: WorkspaceId::new(),
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: owner,
        })
        .await?;
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::new(),
            holder_generation: 1,
            ttl_ns: 60_000_000_000,
        })
        .await?;
    let expected_layers: [crate::workspace_overlay::model::LayerRecord; 2] = store
        .load_layer_chain(workspace.head_layer_id)
        .await?
        .try_into()
        .map_err(|_| anyhow::anyhow!("new workspace head is not a two-layer view"))?;
    let base = &expected_layers[1];
    let expected_base = BaseRevision {
        layer_id: base.layer_id,
        sealed_version: base
            .sealed_version
            .ok_or_else(|| anyhow::anyhow!("new sealed base has no version"))?,
        root_hash: base
            .root_hash
            .ok_or_else(|| anyhow::anyhow!("new sealed base has no root hash"))?,
    };
    let guard = HeadGuard {
        workspace_id: workspace.workspace_id,
        expected_head_layer_id: workspace.head_layer_id,
        expected_head_epoch: workspace.head_epoch,
        lease_id: lease.lease_id,
        holder_generation: lease.holder_generation,
    };
    let installed = store
        .install_packed_lower_binding(InstallPackedLowerBinding {
            guard,
            expected_layers,
            expected_base,
            expected_binding: None,
            lower,
        })
        .await;
    // Always end the install lease; a failed install must not strand it.
    let released = store
        .release_lease(ReleaseLease {
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        })
        .await;
    installed?;
    released?;
    Ok(store.load_workspace(workspace.workspace_id).await?)
}

#[cfg(feature = "workspace-overlay")]
async fn workspace_cmd_with_catalog<W>(
    command: WorkspaceCommand,
    store: Arc<W>,
) -> anyhow::Result<()>
where
    W: WorkspaceStore + 'static,
{
    match command {
        WorkspaceCommand::IndexNativeReverse(_) => {
            anyhow::bail!(
                "native reverse maintenance requires authenticated Redis or TiKV administration"
            );
        }
        WorkspaceCommand::RecoverPackedMount(_) => {
            anyhow::bail!(
                "packed-v3 mounted recovery requires its concrete Redis/TiKV and original PVC driver"
            );
        }
        WorkspaceCommand::InitVolume { owner } => {
            if store.load_volume_header().await?.is_some() {
                anyhow::bail!("workspace volume is already initialized")
            }
            store.initialize_workspace_schema().await?;
            let workspace = store
                .create_volume_root(CreateVolumeRoot {
                    volume_format: "workspace-v1".into(),
                    schema_version: WORKSPACE_SCHEMA_VERSION,
                    volume_id: uuid::Uuid::now_v7(),
                    workspace_id: WorkspaceId::new(),
                    root_layer_id: LayerId::new(),
                    writable_layer_id: LayerId::new(),
                    owner_id: owner,
                })
                .await?;
            print_json(&workspace)?;
        }
        WorkspaceCommand::InitPackedVolume {
            manifest_key,
            data_config,
            owner,
        } => {
            let workspace = init_packed_volume(store, &manifest_key, &data_config, owner).await?;
            print_json(&workspace)?;
        }
        #[cfg(feature = "native-packed-base")]
        WorkspaceCommand::InitNative => {
            anyhow::bail!("native initialization must use the backend-aware command path")
        }
        WorkspaceCommand::Migrate => {
            store.initialize_workspace_schema().await?;
            validate_workspace_header(&store).await?;
        }
        WorkspaceCommand::Create { revision, owner } => {
            validate_workspace_header(&store).await?;
            let revision = match revision {
                Some(revision) => revision,
                None => store
                    .list_workspaces()
                    .await?
                    .into_iter()
                    .filter_map(|workspace| workspace.fork_base)
                    .next()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "volume has no initial sealed revision; pass --from <revision>"
                        )
                    })?,
            };
            let mut created = WorkspaceLifecycle::new(store)
                .fork_revision(revision, 1, owner)
                .await?;
            print_json(&created.remove(0))?;
        }
        WorkspaceCommand::Fork {
            source,
            count,
            owner,
        } => {
            validate_workspace_header(&store).await?;
            if count == 0 {
                anyhow::bail!("--count must be greater than zero")
            }
            let revision = source;
            let created = WorkspaceLifecycle::new(store)
                .fork_revision(revision, count, owner)
                .await?;
            print_json(&created)?;
        }
        WorkspaceCommand::List => {
            validate_workspace_header(&store).await?;
            print_json(&store.list_workspaces().await?)?;
        }
        WorkspaceCommand::Inspect { workspace } => {
            validate_workspace_header(&store).await?;
            print_json(&WorkspaceControl::new(store).inspect(workspace).await?)?;
        }
        WorkspaceCommand::Diff {
            workspace,
            against,
            chunk_size,
        } => {
            validate_workspace_header(&store).await?;
            let record = store.load_workspace(workspace).await?;
            let base = against.or(record.fork_base).ok_or_else(|| {
                anyhow::anyhow!("workspace has no fork base; pass --against <revision>")
            })?;
            let changes = WorkspaceDiff::new(store, chunk_size)
                .diff(&base, record.head_layer_id)
                .await?;
            print_json(&changes)?;
        }
        WorkspaceCommand::Discard { workspace, force } => {
            validate_workspace_header(&store).await?;
            WorkspaceLifecycle::new(store)
                .discard(workspace, force)
                .await?;
            println!("discarded {workspace}");
        }
    }
    Ok(())
}

#[cfg(feature = "native-packed-base")]
async fn init_native_volume_with_catalog<W>(
    catalog: Arc<W>,
    control: Arc<dyn ControlStore>,
    namespace: &str,
) -> anyhow::Result<()>
where
    W: WorkspaceStore + 'static,
{
    let header = catalog
        .load_volume_header()
        .await?
        .ok_or_else(|| anyhow::anyhow!("initialize workspace-v1 catalog before native volume"))?;
    if header.volume_format != "workspace-v1" || header.schema_version != WORKSPACE_SCHEMA_VERSION {
        anyhow::bail!(
            "native initialization requires workspace-v1 schema-1 catalog; found {} schema {}",
            header.volume_format,
            header.schema_version
        );
    }
    let volume_id = *header.volume_id.as_bytes();
    let native_header =
        NativeVolumeHeader::p1(volume_id, native_namespace_id(namespace, &volume_id));
    initialize_volume(
        control.as_ref(),
        namespace,
        &native_header,
        NativeRuntimeCapabilities::compiled(),
    )
    .await
    .map_err(|error| anyhow::anyhow!(error))?;
    print_json(&native_header)
}

#[cfg(feature = "workspace-overlay")]
async fn validate_workspace_header<W: WorkspaceStore + ?Sized>(
    store: &Arc<W>,
) -> anyhow::Result<()> {
    let header = store
        .load_volume_header()
        .await?
        .ok_or_else(|| anyhow::anyhow!("corrupt workspace metadata: volume marker is missing"))?;
    if header.volume_format != "workspace-v1" {
        anyhow::bail!("unsupported volume format {}", header.volume_format)
    }
    if header.schema_version != WORKSPACE_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported workspace schema version {}",
            header.schema_version
        )
    }
    Ok(())
}

#[cfg(feature = "workspace-overlay")]
fn new_workspace_holder_generation() -> u64 {
    let suffix = u64::from_be_bytes(
        uuid::Uuid::now_v7().as_bytes()[8..16]
            .try_into()
            .expect("UUID suffix has eight bytes"),
    );
    (suffix & i64::MAX as u64).max(1)
}

#[cfg(feature = "workspace-overlay")]
fn print_json(value: &impl serde::Serialize) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

async fn gc_cmd(args: GcArgs) -> anyhow::Result<()> {
    let registry = RuntimeRegistry::new(RuntimeRegistry::default_root());
    let mount_point = args.mount_point.as_ref().map(|path| path.to_string_lossy());
    let record = registry.select_instance(mount_point.as_deref()).await?;

    let accepted = send_request(
        &record.socket_path,
        &ControlRequest::RunGc {
            dry_run: args.dry_run,
        },
    )
    .await?;

    let ControlResponse::Accepted { job_id } = accepted else {
        anyhow::bail!("unexpected response: {accepted:?}");
    };

    loop {
        let status = send_request(
            &record.socket_path,
            &ControlRequest::GetJob {
                job_id: job_id.clone(),
            },
        )
        .await?;

        match status {
            ControlResponse::JobStatus {
                state,
                detail,
                outcome,
                ..
            } => {
                if matches!(
                    state,
                    crate::control::job::JobState::Pending | crate::control::job::JobState::Running
                ) {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue;
                }

                match outcome {
                    Some(JobOutcome::Gc(result)) => {
                        println!(
                            "gc finished: state={state:?} orphan_slices={} orphan_objects={} deleted_objects={} errors={}",
                            result.orphan_slice_count,
                            result.orphan_object_count,
                            result.deleted_object_count,
                            result.error_count
                        );
                    }
                    None => println!("gc finished: state={state:?}"),
                }

                if let Some(detail) = detail {
                    println!("{detail}");
                }

                return Ok(());
            }
            ControlResponse::Error { code, message } => {
                anyhow::bail!("gc failed: {code}: {message}");
            }
            other => anyhow::bail!("unexpected response: {other:?}"),
        }
    }
}

async fn info_cmd(args: InfoArgs) -> anyhow::Result<()> {
    let registry = RuntimeRegistry::new(RuntimeRegistry::default_root());
    let mount_point = args.mount_point.as_ref().map(|path| path.to_string_lossy());
    let record = registry.select_instance(mount_point.as_deref()).await?;

    let response = send_request(&record.socket_path, &ControlRequest::GetInfo).await?;

    match response {
        ControlResponse::Info {
            pid,
            mount_point,
            started_at,
            version,
            meta_backend,
            capabilities,
        } => {
            let started_at = chrono::DateTime::from_timestamp_millis(started_at)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| started_at.to_string());

            println!("mount_point: {mount_point}");
            println!("pid: {pid}");
            println!("started_at: {started_at}");
            println!("version: {version}");
            println!("meta_backend: {meta_backend}");
            println!("capabilities: {}", serde_json::to_string(&capabilities)?);
            Ok(())
        }
        ControlResponse::Error { code, message } => {
            anyhow::bail!("info failed: {code}: {message}");
        }
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
}

#[cfg(feature = "profiling")]
static FLAME_GUARD: LazyLock<StdMutex<Option<tracing_flame::FlushGuard<BufWriter<File>>>>> =
    LazyLock::new(|| StdMutex::new(None));
#[cfg(feature = "profiling")]
static CHROME_GUARD: LazyLock<StdMutex<Option<tracing_chrome::FlushGuard>>> =
    LazyLock::new(|| StdMutex::new(None));

#[cfg(feature = "profiling")]
fn register_flame_guard(guard: tracing_flame::FlushGuard<BufWriter<File>>) {
    if let Ok(mut slot) = FLAME_GUARD.lock() {
        *slot = Some(guard);
    }
}

#[cfg(feature = "profiling")]
fn shutdown_flame() {
    if let Ok(mut slot) = FLAME_GUARD.lock()
        && let Some(guard) = slot.take()
        && let Err(err) = guard.flush()
    {
        eprintln!("tracing-flame flush failed: {err}");
    }
}

#[cfg(not(feature = "profiling"))]
fn shutdown_flame() {}

#[cfg(feature = "profiling")]
fn register_chrome_guard(guard: tracing_chrome::FlushGuard) {
    if let Ok(mut slot) = CHROME_GUARD.lock() {
        *slot = Some(guard);
    }
}

#[cfg(feature = "profiling")]
fn shutdown_chrome() {
    if let Ok(mut slot) = CHROME_GUARD.lock() {
        slot.take();
    }
}

#[cfg(not(feature = "profiling"))]
fn shutdown_chrome() {}

async fn create_meta_store(args: &MountConfig) -> anyhow::Result<Arc<dyn MetaStore>> {
    match args.meta_backend {
        MetaBackendKind::Sqlx => {
            let client = ClientOptions::default();
            let compact = args.compact.clone();

            let config = Config {
                database: DatabaseConfig {
                    db_config: database_type_from_url(&args.meta_url),
                },
                cache: MetaCacheConfig::default(),
                client,
                compact,
            };
            Ok(Arc::new(DatabaseMetaStore::from_config(config).await?) as Arc<dyn MetaStore>)
        }
        MetaBackendKind::Etcd => {
            if args.meta_etcd_urls.is_empty() {
                anyhow::bail!("etcd urls must be set when meta backend is etcd");
            }

            let client = ClientOptions::default();
            let compact = args.compact.clone();

            let config = Config {
                database: DatabaseConfig {
                    db_config: DatabaseType::Etcd {
                        urls: args.meta_etcd_urls.clone(),
                    },
                },
                cache: MetaCacheConfig::default(),
                client,
                compact,
            };
            Ok(Arc::new(EtcdMetaStore::from_config(config).await?) as Arc<dyn MetaStore>)
        }
        MetaBackendKind::Redis => {
            let client = ClientOptions::default();
            let compact = args.compact.clone();

            let config = Config {
                database: DatabaseConfig {
                    db_config: DatabaseType::Redis {
                        url: args.meta_url.clone(),
                    },
                },
                cache: MetaCacheConfig::default(),
                client,
                compact,
            };
            Ok(Arc::new(RedisMetaStore::from_config(config).await?) as Arc<dyn MetaStore>)
        }
        MetaBackendKind::TiKv => {
            if args.meta_tikv_pd_endpoints.is_empty() {
                anyhow::bail!("tikv PD endpoints must be set when meta backend is tikv");
            }

            let client = ClientOptions::default();
            let compact = args.compact.clone();

            let config = Config {
                database: DatabaseConfig {
                    db_config: DatabaseType::TiKv {
                        pd_endpoints: args.meta_tikv_pd_endpoints.clone(),
                        namespace: args.meta_tikv_namespace.clone(),
                    },
                },
                cache: MetaCacheConfig::default(),
                client,
                compact,
            };
            Ok(Arc::new(TiKvMetaStore::from_config(config).await?) as Arc<dyn MetaStore>)
        }
    }
}

fn database_type_from_url(url: &str) -> DatabaseType {
    let lower = url.to_ascii_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        DatabaseType::Postgres {
            url: url.to_string(),
        }
    } else {
        DatabaseType::Sqlite {
            url: url.to_string(),
        }
    }
}

#[cfg(test)]
mod volume_format_tests {
    use super::*;

    #[test]
    fn entity_format_is_explicit_and_mount_fails_closed() {
        use clap::ValueEnum;
        assert_eq!(
            VolumeFormat::from_str("workspace-entity-v1", false).unwrap(),
            VolumeFormat::WorkspaceEntityV1
        );
        assert_eq!(VolumeFormat::default(), VolumeFormat::FlatV1);
        let error = validate_volume_format_support(VolumeFormat::WorkspaceEntityV1)
            .unwrap_err()
            .to_string();
        #[cfg(feature = "workspace-overlay")]
        assert!(error.contains("--experimental-entity-mount"), "{error}");
        #[cfg(not(feature = "workspace-overlay"))]
        assert!(error.contains("feature not compiled"), "{error}");
    }

    #[test]
    fn flat_format_is_always_supported() {
        validate_volume_format_support(VolumeFormat::FlatV1).unwrap();
    }

    #[cfg(feature = "native-packed-base")]
    #[test]
    fn native_format_requires_its_explicit_feature() {
        validate_volume_format_support(VolumeFormat::WorkspaceNativeV2).unwrap();
    }

    #[cfg(not(feature = "native-packed-base"))]
    #[test]
    fn binary_without_native_feature_fails_closed_for_native_format() {
        assert_eq!(
            validate_volume_format_support(VolumeFormat::WorkspaceNativeV2)
                .unwrap_err()
                .to_string(),
            "feature not compiled: native-packed-base"
        );
    }

    #[cfg(feature = "native-packed-base")]
    #[test]
    fn legacy_packed_v1_format_is_rejected_even_when_compiled() {
        let error = validate_volume_format_support(VolumeFormat::PackedMetadataV1)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("unsupported") && error.contains("packed-metadata-v3"),
            "{error}"
        );
    }

    #[cfg(feature = "workspace-overlay")]
    #[test]
    fn legacy_packed_v2_format_is_rejected_even_when_compiled() {
        let error = validate_volume_format_support(VolumeFormat::PackedMetadataV2)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("unsupported") && error.contains("packed-metadata-v3"),
            "{error}"
        );
        validate_volume_format_support(VolumeFormat::PackedMetadataV3).unwrap();
    }

    #[cfg(feature = "workspace-overlay")]
    #[derive(Clone)]
    struct ProbeOnlyBackend {
        probe: [u8; 64],
        range_reads: Arc<AtomicU64>,
        full_reads: Arc<AtomicU64>,
    }

    #[cfg(feature = "workspace-overlay")]
    #[async_trait::async_trait]
    impl ObjectBackend for ProbeOnlyBackend {
        async fn put_object(&self, _key: &str, _data: &[u8]) -> anyhow::Result<()> {
            anyhow::bail!("mount probe must not write")
        }
        async fn get_object(&self, _key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            self.full_reads.fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("non-005 probe attempted a forbidden full GET")
        }
        async fn get_object_range(
            &self,
            _key: &str,
            offset: u64,
            bytes: &mut [u8],
        ) -> anyhow::Result<usize> {
            self.range_reads.fetch_add(1, Ordering::Relaxed);
            assert_eq!(offset, 0, "only the initial 64-byte probe is allowed");
            assert_eq!(bytes.len(), 64, "only the initial 64-byte probe is allowed");
            bytes.copy_from_slice(&self.probe);
            Ok(bytes.len())
        }
        async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
            anyhow::bail!("mount probe must not fetch an etag")
        }
        async fn delete_object(&self, _key: &str) -> anyhow::Result<()> {
            anyhow::bail!("mount probe must not delete")
        }
    }

    #[cfg(feature = "workspace-overlay")]
    fn packed_probe_config() -> MountConfig {
        let cli = Cli::parse_from([
            "brewfs",
            "mount",
            "/unused-probe-mount",
            "--volume-format",
            "packed-metadata-v3",
            "--packed-manifest-key",
            "probe/legacy-manifest",
        ]);
        let Command::Mount(args) = cli.cmd else {
            panic!("expected mount command");
        };
        MountConfig::from_sources(*args).unwrap()
    }

    #[cfg(feature = "workspace-overlay")]
    #[tokio::test]
    async fn packed_v3_mount_rejects_old_and_unknown_magic_before_full_get() {
        for magic in [b"BRFPM004", b"BRFPM003", b"BRFCA005", b"UNKNOWN!"] {
            let mut probe = [0; 64];
            probe[..8].copy_from_slice(magic);
            let backend = ProbeOnlyBackend {
                probe,
                range_reads: Arc::new(AtomicU64::new(0)),
                full_reads: Arc::new(AtomicU64::new(0)),
            };
            let error = mount_packed_v3_readonly_with_client(
                ObjectClient::new(backend.clone()),
                ChunkLayout::default(),
                &packed_probe_config(),
            )
            .await
            .unwrap_err()
            .to_string();
            assert_eq!(backend.range_reads.load(Ordering::Relaxed), 1);
            assert_eq!(
                backend.full_reads.load(Ordering::Relaxed),
                0,
                "unsupported magic must not trigger a whole object read: {magic:?}"
            );
            assert!(error.contains("only wire 005"), "{error}");
        }
    }

    #[cfg(feature = "workspace-overlay")]
    #[tokio::test]
    async fn packed_v3_mount_still_requires_the_selected_manifest_digest() {
        let mut probe = [0; 64];
        probe[..8].copy_from_slice(b"BRFPM005");
        let backend = ProbeOnlyBackend {
            probe,
            range_reads: Arc::new(AtomicU64::new(0)),
            full_reads: Arc::new(AtomicU64::new(0)),
        };
        let error = mount_packed_v3_readonly_with_client(
            ObjectClient::new(backend.clone()),
            ChunkLayout::default(),
            &packed_probe_config(),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("SHA-256 content-addressed manifest key"),
            "{error}"
        );
        assert_eq!(backend.range_reads.load(Ordering::Relaxed), 1);
        assert_eq!(backend.full_reads.load(Ordering::Relaxed), 0);
    }

    #[cfg(feature = "workspace-overlay")]
    #[test]
    fn workspace_holder_generation_fits_the_sqlite_integer_domain() {
        for _ in 0..256 {
            let generation = new_workspace_holder_generation();
            assert!((1..=i64::MAX as u64).contains(&generation));
        }
    }

    #[cfg(feature = "workspace-overlay")]
    #[test]
    fn g08_existing_api_native_selection_attributes_manifest_attempt_to_native() {
        const CHILD: &str = "BREWFS_G08_EXISTING_API_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("cli::volume_format_tests::g08_existing_api_native_selection_attributes_manifest_attempt_to_native")
                .arg("--nocapture")
                .env(CHILD, "1")
                .env("BREWFS_PACKED_METADATA_ARM", "native-tikv")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "isolated native-arm startup behavior failed:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
                "isolated startup check must run exactly one test:\n{}",
                String::from_utf8_lossy(&output.stdout)
            );
            return;
        }
        #[derive(Clone)]
        struct RejectManifest(Arc<AtomicU64>);
        #[async_trait::async_trait]
        impl ObjectBackend for RejectManifest {
            async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
                anyhow::bail!("startup test must not write")
            }
            async fn get_object(&self, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
                anyhow::bail!("startup test forbids full GET")
            }
            async fn get_object_range(
                &self,
                _: &str,
                _: u64,
                _: &mut [u8],
            ) -> anyhow::Result<usize> {
                self.0.fetch_add(1, Ordering::SeqCst);
                anyhow::bail!("controlled manifest transport rejection")
            }
            async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
                anyhow::bail!("startup test must not request an etag")
            }
            async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
                anyhow::bail!("startup test must not delete")
            }
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                use crate::cadapter::read_observer::{Engine, Phase, ReadClass, ReadObserver};
                use workspace_overlay::packed_v3::wire005::{
                    V3MountBudget, V3ObjectKind, V3ObjectRef, encode_v3_object,
                };
                let budget = V3MountBudget::defaults();
                let observer = Arc::new(ReadObserver::with_mount_budget(&budget).unwrap());
                let calls = Arc::new(AtomicU64::new(0));
                let client = ObjectClient::new(RejectManifest(calls.clone())).with_read_observer(
                    observer.clone(),
                    Engine::PackedV3,
                    Phase::Startup,
                    crate::cadapter::read_observer::Origin::Demand,
                );
                let bytes = encode_v3_object(V3ObjectKind::Manifest, &[0; 64], 4096).unwrap();
                let reference =
                    V3ObjectRef::from_bytes("g08/manifest".into(), V3ObjectKind::Manifest, &bytes)
                        .unwrap();
                let mut config = packed_probe_config();
                config.cache.read_memory_bytes = 0;
                config.cache.read_ssd_bytes = 0;
                config.cache.prefetch_enabled = false;
                config.cache.range_background_prefetch = false;
                let error = mount_authenticated_packed_v3_readonly_with_client(
                    client,
                    ChunkLayout::default(),
                    &config,
                    reference,
                    budget,
                )
                .await
                .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("controlled manifest transport rejection")
                );
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                let snapshot = observer.snapshot();
                let contexts: Vec<_> = snapshot
                    .rows
                    .keys()
                    .filter(|(_, context)| context.class == ReadClass::Manifest)
                    .map(|(_, context)| context)
                    .collect();
                assert!(
                    !contexts.is_empty(),
                    "real manifest attempt was not recorded"
                );
                assert!(
                    contexts
                        .iter()
                        .all(|context| context.engine == Engine::Native),
                    "explicit native-tikv arm silently used PackedV3 attribution: {contexts:?}"
                );
            });
    }

    #[cfg(not(feature = "workspace-overlay"))]
    #[test]
    fn flat_only_binary_fails_closed_for_workspace_format() {
        assert_eq!(
            validate_volume_format_support(VolumeFormat::WorkspaceV1)
                .unwrap_err()
                .to_string(),
            "feature not compiled: workspace-overlay"
        );
    }
}
