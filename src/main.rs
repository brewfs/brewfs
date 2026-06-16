mod cadapter;
mod chunk;
mod console;
mod control;
mod daemon;
#[allow(dead_code)]
mod fs;
mod fuse;
mod meta;
mod posix;
mod utils;
#[allow(dead_code)]
mod vfs;

#[cfg(all(feature = "jemalloc", target_os = "linux"))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", target_os = "linux"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(feature = "profiling")]
use std::fs::File;
#[cfg(feature = "profiling")]
use std::io::BufWriter;
use std::sync::Arc;
#[cfg(feature = "profiling")]
use std::sync::{LazyLock, Mutex as StdMutex};
use std::time::Duration;

pub mod config;
use config::*;

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
use crate::meta::config::{
    CacheConfig as MetaCacheConfig, ClientOptions, Config, DatabaseConfig, DatabaseType,
    MetaClientConfig,
};
use crate::meta::factory::MetaStoreFactory;
use crate::meta::layer::MetaLayer;
use crate::meta::stores::{DatabaseMetaStore, EtcdMetaStore, RedisMetaStore, TiKvMetaStore};
use crate::vfs::fs::VFS;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    raise_nofile_limit();

    let cli = Cli::parse();
    let result = match cli.cmd {
        Command::Mount(args) => mount_cmd(MountConfig::from_sources(*args)?).await,
        Command::Gc(args) => gc_cmd(args).await,
        Command::Info(args) => info_cmd(args).await,
        Command::Console(args) => console::serve_cmd(args).await,
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

async fn mount_cmd(args: MountConfig) -> anyhow::Result<()> {
    if !args.mount_point.exists() {
        std::fs::create_dir_all(&args.mount_point)?;
    }
    if !args.mount_point.is_dir() {
        anyhow::bail!("mount point must be a directory");
    }

    if args.chunk_size < args.block_size as u64 {
        anyhow::bail!("chunk_size must be >= block_size");
    }

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
    let meta_store = create_meta_store(&args).await?;
    tracing::info!("mount startup meta store ready");

    match args.data_backend {
        DataBackendKind::LocalFs => {
            let client = create_localfs_client(&args)?;
            tracing::info!("mount startup localfs client ready");
            let store = create_object_store(client, layout, &args.cache).await?;
            mount_with_store(layout, store, meta_store, &args).await
        }
        DataBackendKind::S3 => {
            let client = create_s3_client(&args).await?;
            tracing::info!("mount startup s3 client ready");
            let store = create_object_store(client, layout, &args.cache).await?;
            mount_with_store(layout, store, meta_store, &args).await
        }
    }
}

async fn create_object_store<B>(
    client: ObjectClient<B>,
    layout: ChunkLayout,
    cache: &crate::vfs::cache::config::CacheConfig,
) -> anyhow::Result<ObjectBlockStore<B>>
where
    B: ObjectBackend + Send + Sync + 'static,
{
    let chunks_cache_config = ChunksCacheConfig::with_budgets(
        cache.read_memory_bytes,
        cache.read_ssd_bytes,
        cache.cache_root.join("chunks"),
    )
    .with_integrity_mode(cache.verify_cache_checksum);
    let block_store_config = BlockStoreConfig {
        block_size: layout.block_size as usize,
        compression: cache.compression,
        range_background_prefetch: cache.range_background_prefetch,
        ..BlockStoreConfig::default()
    };
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

    if args.s3_part_size == 0 {
        anyhow::bail!("--s3-part-size must be greater than 0");
    }
    if args.s3_max_concurrency == 0 {
        anyhow::bail!("--s3-max-concurrency must be greater than 0");
    }

    let config = S3Config {
        bucket,
        region: args.s3_region.clone(),
        part_size: args.s3_part_size,
        max_concurrency: args.s3_max_concurrency,
        endpoint: args.s3_endpoint.clone(),
        force_path_style: args.s3_force_path_style,
        disable_payload_checksum: args.s3_disable_payload_checksum,
        ..Default::default()
    };

    let backend = S3Backend::with_config(config).await?;
    Ok(ObjectClient::new(backend))
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
        signal = tokio::signal::ctrl_c() => {
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
            let handle = MetaStoreFactory::<DatabaseMetaStore>::create_from_config(config).await?;
            Ok(handle.store() as Arc<dyn MetaStore>)
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
            let handle = MetaStoreFactory::<EtcdMetaStore>::create_from_config(config).await?;
            Ok(handle.store() as Arc<dyn MetaStore>)
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
            let handle = MetaStoreFactory::<RedisMetaStore>::create_from_config(config).await?;
            Ok(handle.store() as Arc<dyn MetaStore>)
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
            let handle = MetaStoreFactory::<TiKvMetaStore>::create_from_config(config).await?;
            Ok(handle.store() as Arc<dyn MetaStore>)
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
