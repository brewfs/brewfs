use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use std::path::PathBuf;

use crate::chunk::bandwidth::BandwidthConfig;
use crate::chunk::cache_integrity::CacheIntegrityMode;
use crate::chunk::compress::Compression;
use crate::chunk::layout::{DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE};
use crate::vfs::cache::config::{CacheConfig as VfsCacheConfig, WriteBackMode};

pub const DEFAULT_DATA_DIR: &str = "./data";
pub const DEFAULT_META_URL: &str = "sqlite::memory:";
pub const DEFAULT_S3_PART_SIZE: usize = 16 * 1024 * 1024;
pub const DEFAULT_S3_MAX_CONCURRENCY: usize = 32;
pub const DEFAULT_FUSE_MAX_BACKGROUND: usize = 512;

fn default_fuse_workers() -> usize {
    1
}

fn long_version() -> &'static str {
    concat!(
        env!("CARGO_PKG_VERSION"),
        "\ncommit: ",
        env!("BREWFS_GIT_COMMIT"),
        "\ncommit_short: ",
        env!("BREWFS_GIT_COMMIT_SHORT"),
        "\nbranch: ",
        env!("BREWFS_GIT_BRANCH"),
        "\ndirty: ",
        env!("BREWFS_GIT_DIRTY"),
        "\nbuilt: ",
        env!("BREWFS_BUILD_TIMESTAMP")
    )
}

#[derive(Parser)]
#[command(
    name = "brewfs",
    version,
    long_version = long_version(),
    about = "BrewFS FUSE CLI"
)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Mount BrewFS via FUSE.
    #[command(
        after_help = "Examples:\n  brewfs mount --config examples/mount-config.local.yaml\n  brewfs mount --config examples/mount-config.s3.yaml\n  brewfs mount --config examples/mount-config.local.yaml /mnt/slayer\n  brewfs mount --config examples/mount-config.s3.yaml --s3-bucket override-bucket"
    )]
    Mount(Box<MountArgs>),

    /// Talk to a mounted BrewFS instance and run orphan gc.
    Gc(GcArgs),

    /// Talk to a mounted BrewFS instance and print mount information.
    Info(InfoArgs),

    /// Run the BrewFS web console.
    Console(ConsoleArgs),
}

#[derive(Args, Debug, Clone)]
pub struct MountArgs {
    /// YAML config file path.
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Directory to mount the filesystem.
    #[arg(value_name = "MOUNT_POINT")]
    pub mount_point: Option<PathBuf>,

    /// Data storage backend type.
    #[arg(long, value_enum)]
    pub data_backend: Option<DataBackendKind>,

    /// Local directory used as object storage backend (only for localfs backend).
    #[arg(long, value_name = "DIR")]
    pub data_dir: Option<PathBuf>,

    /// S3 bucket name (only for s3 backend).
    #[arg(long, value_name = "BUCKET")]
    pub s3_bucket: Option<String>,

    /// S3-compatible endpoint URL (only for s3 backend).
    #[arg(long, value_name = "URL")]
    pub s3_endpoint: Option<String>,

    /// S3 region (optional, for s3 backend).
    #[arg(long, value_name = "REGION")]
    pub s3_region: Option<String>,

    /// S3 part size in bytes for multipart upload (only for s3 backend).
    #[arg(long)]
    pub s3_part_size: Option<usize>,

    /// S3 maximum concurrent multipart upload parts (only for s3 backend).
    #[arg(long)]
    pub s3_max_concurrency: Option<usize>,

    /// Force path-style S3 access (required for MinIO, localstack, etc.).
    #[arg(long)]
    pub s3_force_path_style: Option<bool>,

    /// Disable S3 payload checksum (SigV4 SHA-256 signing of request body).
    /// Reduces CPU usage by ~20% on write paths. Safe for self-hosted S3 backends.
    #[arg(long)]
    pub s3_disable_payload_checksum: Option<bool>,

    /// Metadata backend (sqlx, etcd, redis or tikv).
    #[arg(long, value_enum)]
    pub meta_backend: Option<MetaBackendKind>,

    /// Metadata backend URL (sqlx or redis, e.g. sqlite::memory:, postgres://... or redis://...).
    #[arg(long, value_name = "URL")]
    pub meta_url: Option<String>,

    /// Etcd endpoint URLs (comma-separated).
    #[arg(long, value_name = "URLS", value_delimiter = ',')]
    pub meta_etcd_urls: Option<Vec<String>>,

    /// TiKV PD endpoint URLs (comma-separated).
    #[arg(long, value_name = "URLS", value_delimiter = ',')]
    pub meta_tikv_pd_endpoints: Option<Vec<String>>,

    /// TiKV metadata key namespace.
    #[arg(long, value_name = "NAMESPACE")]
    pub meta_tikv_namespace: Option<String>,

    /// Chunk size in bytes.
    #[arg(long)]
    pub chunk_size: Option<u64>,

    /// Block size in bytes.
    #[arg(long)]
    pub block_size: Option<u32>,

    /// Number of asyncfuse worker tasks. Use 0 or 1 to keep legacy session dispatch.
    #[arg(long)]
    pub fuse_workers: Option<usize>,

    /// Maximum in-flight FUSE requests when asyncfuse worker mode is enabled.
    #[arg(long)]
    pub fuse_max_background: Option<usize>,

    /// Use privileged mount mode (requires root or fuse group membership).
    /// Uses /dev/fuse directly instead of fusermount3.
    #[arg(long, default_value_t = false)]
    pub privileged: bool,
}

#[derive(Args, Debug, Clone)]
pub struct GcArgs {
    /// Optional mount point used to locate the target instance.
    #[arg(value_name = "MOUNT_POINT")]
    pub mount_point: Option<PathBuf>,

    /// Scan only; do not delete orphan data.
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

#[derive(Args, Debug, Clone)]
pub struct InfoArgs {
    /// Optional mount point used to locate the target instance.
    #[arg(value_name = "MOUNT_POINT")]
    pub mount_point: Option<PathBuf>,
}

#[derive(Args, Debug, Clone)]
pub struct ConsoleArgs {
    /// HTTP listen address for the console server.
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen: std::net::SocketAddr,

    /// Console registry and runtime state directory.
    #[arg(long, value_name = "DIR")]
    pub state_dir: Option<PathBuf>,

    /// BrewFS runtime registry directory.
    #[arg(long, value_name = "DIR")]
    pub runtime_dir: Option<PathBuf>,

    /// Pre-built frontend static asset directory.
    #[arg(long, value_name = "DIR")]
    pub static_dir: Option<PathBuf>,

    /// Bearer-token file used for console API authentication.
    #[arg(long, value_name = "FILE")]
    pub auth_token_file: Option<PathBuf>,

    /// Kubernetes config path for CSI dashboard discovery.
    #[arg(long, value_name = "FILE")]
    pub kubeconfig: Option<PathBuf>,

    /// Kubernetes CSI driver name used to discover BrewFS resources.
    #[arg(long, default_value = "csi.brewfs.io")]
    pub csi_driver_name: String,

    /// Disable auth for local development. Only allowed with loopback listeners.
    #[arg(long, default_value_t = false)]
    pub dev_no_auth: bool,

    /// Enable read-only Kubernetes CSI dashboard endpoints.
    #[arg(long, default_value_t = false)]
    pub enable_csi_dashboard: bool,
}

#[derive(ValueEnum, Deserialize, Clone, Copy, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum DataBackendKind {
    LocalFs,
    S3,
}

#[derive(ValueEnum, Deserialize, Clone, Copy, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum MetaBackendKind {
    Sqlx,
    Etcd,
    Redis,
    #[value(name = "tikv", alias = "ti-kv")]
    #[serde(rename = "tikv", alias = "ti-kv")]
    TiKv,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MountFileConfig {
    pub mount_point: Option<PathBuf>,
    pub data: Option<DataFileConfig>,
    pub meta: Option<MetaFileConfig>,
    pub layout: Option<LayoutFileConfig>,
    pub fuse: Option<FuseFileConfig>,
    pub cache: Option<CacheFileConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DataFileConfig {
    pub backend: Option<DataBackendKind>,
    pub localfs: Option<LocalFsFileConfig>,
    pub s3: Option<S3FileConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct LocalFsFileConfig {
    pub data_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct S3FileConfig {
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub part_size: Option<usize>,
    pub max_concurrency: Option<usize>,
    pub force_path_style: Option<bool>,
    pub disable_payload_checksum: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetaFileConfig {
    pub backend: Option<MetaBackendKind>,
    pub sqlx: Option<UrlBackedMetaFileConfig>,
    pub redis: Option<UrlBackedMetaFileConfig>,
    pub etcd: Option<EtcdMetaFileConfig>,
    pub tikv: Option<TiKvMetaFileConfig>,
    pub open_file_cache_ttl_ms: Option<u64>,
    pub open_file_cache_capacity: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct UrlBackedMetaFileConfig {
    pub url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct EtcdMetaFileConfig {
    pub urls: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TiKvMetaFileConfig {
    pub pd_endpoints: Option<Vec<String>>,
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct LayoutFileConfig {
    pub chunk_size: Option<u64>,
    pub block_size: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct FuseFileConfig {
    pub workers: Option<usize>,
    pub max_background: Option<usize>,
    pub privileged: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct CacheFileConfig {
    #[serde(alias = "root")]
    pub cache_root: Option<PathBuf>,
    pub read_memory_bytes: Option<u64>,
    pub read_ssd_bytes: Option<u64>,
    pub write_memory_bytes: Option<u64>,
    pub write_ssd_bytes: Option<u64>,
    pub dirty_slice_target_size: Option<u64>,
    pub dirty_slice_max_age_ms: Option<u64>,
    pub upload_concurrency: Option<usize>,
    pub prefetch_enabled: Option<bool>,
    pub prefetch_max_bytes: Option<u64>,
    pub prefetch_concurrency: Option<usize>,
    pub range_background_prefetch: Option<bool>,
    pub memory_budget_bytes: Option<u64>,
    pub compression: Option<String>,
    pub zstd_level: Option<i32>,
    pub verify_cache_checksum: Option<String>,
    pub writeback_mode: Option<String>,
    pub writeback_persist_sync: Option<bool>,
    pub bandwidth: Option<BandwidthFileConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct BandwidthFileConfig {
    pub upload_limit_mibps: Option<u64>,
    pub download_limit_mibps: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct MountConfig {
    pub mount_point: PathBuf,
    pub data_backend: DataBackendKind,
    pub data_dir: PathBuf,
    pub s3_bucket: Option<String>,
    pub s3_endpoint: Option<String>,
    pub s3_region: Option<String>,
    pub s3_part_size: usize,
    pub s3_max_concurrency: usize,
    pub s3_force_path_style: bool,
    pub s3_disable_payload_checksum: bool,
    pub meta_backend: MetaBackendKind,
    pub meta_url: String,
    pub meta_etcd_urls: Vec<String>,
    pub meta_tikv_pd_endpoints: Vec<String>,
    pub meta_tikv_namespace: String,
    pub meta_open_file_cache_ttl_ms: Option<u64>,
    pub meta_open_file_cache_capacity: Option<u64>,
    pub chunk_size: u64,
    pub block_size: u32,
    pub fuse_workers: usize,
    pub fuse_max_background: usize,
    pub privileged: bool,
    pub cache: VfsCacheConfig,
}

impl MountConfig {
    pub fn from_sources(args: MountArgs) -> anyhow::Result<Self> {
        let file_cfg = match args.config.as_ref() {
            Some(path) => {
                let content = std::fs::read_to_string(path)?;
                serde_yaml::from_str::<MountFileConfig>(&content)?
            }
            None => MountFileConfig::default(),
        };

        let data_cfg = file_cfg.data.unwrap_or_default();
        let localfs_cfg = data_cfg.localfs.unwrap_or_default();
        let s3_cfg = data_cfg.s3.unwrap_or_default();
        let meta_cfg = file_cfg.meta.unwrap_or_default();
        let sqlx_cfg = meta_cfg.sqlx.unwrap_or_default();
        let redis_cfg = meta_cfg.redis.unwrap_or_default();
        let etcd_cfg = meta_cfg.etcd.unwrap_or_default();
        let tikv_cfg = meta_cfg.tikv.unwrap_or_default();
        let layout_cfg = file_cfg.layout.unwrap_or_default();
        let fuse_cfg = file_cfg.fuse.unwrap_or_default();
        let cache_cfg = file_cfg.cache.unwrap_or_default();
        let cache = cache_cfg.into_cache_config()?;
        let data_backend = args
            .data_backend
            .or(data_cfg.backend)
            .unwrap_or(DataBackendKind::LocalFs);

        if matches!(cache.writeback_mode, WriteBackMode::CommitBeforeUpload)
            && !matches!(data_backend, DataBackendKind::S3)
        {
            anyhow::bail!("cache.writeback_mode=commit_before_upload requires data.backend=s3");
        }

        let mount_point = args.mount_point.or(file_cfg.mount_point).ok_or_else(|| {
            anyhow::anyhow!("mount point is required (positional arg or config.mount_point)")
        })?;

        let meta_backend = args
            .meta_backend
            .or(meta_cfg.backend)
            .unwrap_or(MetaBackendKind::Sqlx);

        let meta_url_from_file = match meta_backend {
            MetaBackendKind::Sqlx => sqlx_cfg.url,
            MetaBackendKind::Redis => redis_cfg.url,
            MetaBackendKind::Etcd => None,
            MetaBackendKind::TiKv => None,
        };

        Ok(Self {
            mount_point,
            data_backend,
            data_dir: args
                .data_dir
                .or(localfs_cfg.data_dir)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIR)),
            s3_bucket: args.s3_bucket.or(s3_cfg.bucket),
            s3_endpoint: args.s3_endpoint.or(s3_cfg.endpoint),
            s3_region: args.s3_region.or(s3_cfg.region),
            s3_part_size: args
                .s3_part_size
                .or(s3_cfg.part_size)
                .unwrap_or(DEFAULT_S3_PART_SIZE),
            s3_max_concurrency: args
                .s3_max_concurrency
                .or(s3_cfg.max_concurrency)
                .unwrap_or(DEFAULT_S3_MAX_CONCURRENCY),
            s3_force_path_style: args
                .s3_force_path_style
                .or(s3_cfg.force_path_style)
                .unwrap_or(false),
            s3_disable_payload_checksum: args
                .s3_disable_payload_checksum
                .or(s3_cfg.disable_payload_checksum)
                .unwrap_or(true),
            meta_backend,
            meta_url: args
                .meta_url
                .or(meta_url_from_file)
                .unwrap_or_else(|| DEFAULT_META_URL.to_string()),
            meta_etcd_urls: args.meta_etcd_urls.or(etcd_cfg.urls).unwrap_or_default(),
            meta_tikv_pd_endpoints: args
                .meta_tikv_pd_endpoints
                .or(tikv_cfg.pd_endpoints)
                .unwrap_or_default(),
            meta_tikv_namespace: args
                .meta_tikv_namespace
                .or(tikv_cfg.namespace)
                .unwrap_or_else(crate::meta::config::default_tikv_namespace),
            meta_open_file_cache_ttl_ms: meta_cfg.open_file_cache_ttl_ms,
            meta_open_file_cache_capacity: meta_cfg.open_file_cache_capacity,
            chunk_size: args
                .chunk_size
                .or(layout_cfg.chunk_size)
                .unwrap_or(DEFAULT_CHUNK_SIZE),
            block_size: args
                .block_size
                .or(layout_cfg.block_size)
                .unwrap_or(DEFAULT_BLOCK_SIZE),
            fuse_workers: args
                .fuse_workers
                .or(fuse_cfg.workers)
                .unwrap_or_else(default_fuse_workers),
            fuse_max_background: args
                .fuse_max_background
                .or(fuse_cfg.max_background)
                .unwrap_or(DEFAULT_FUSE_MAX_BACKGROUND),
            privileged: args.privileged || fuse_cfg.privileged.unwrap_or(false),
            cache,
        })
    }
}

impl CacheFileConfig {
    fn into_cache_config(self) -> anyhow::Result<VfsCacheConfig> {
        let mut cache = VfsCacheConfig::default();

        if let Some(cache_root) = self.cache_root {
            cache.cache_root = cache_root;
        }
        if let Some(read_memory_bytes) = self.read_memory_bytes {
            cache.read_memory_bytes = read_memory_bytes;
        }
        if let Some(read_ssd_bytes) = self.read_ssd_bytes {
            cache.read_ssd_bytes = read_ssd_bytes;
        }
        if let Some(write_memory_bytes) = self.write_memory_bytes {
            cache.write_memory_bytes = write_memory_bytes;
        }
        if let Some(write_ssd_bytes) = self.write_ssd_bytes {
            cache.write_ssd_bytes = write_ssd_bytes;
        }
        if let Some(dirty_slice_target_size) = self.dirty_slice_target_size {
            cache.dirty_slice_target_size = dirty_slice_target_size;
        }
        if let Some(dirty_slice_max_age_ms) = self.dirty_slice_max_age_ms {
            cache.dirty_slice_max_age_ms = dirty_slice_max_age_ms;
        }
        if let Some(upload_concurrency) = self.upload_concurrency {
            cache.upload_concurrency = upload_concurrency.max(1);
        }
        if let Some(prefetch_enabled) = self.prefetch_enabled {
            cache.prefetch_enabled = prefetch_enabled;
        }
        if let Some(prefetch_max_bytes) = self.prefetch_max_bytes {
            cache.prefetch_max_bytes = prefetch_max_bytes;
        }
        if let Some(prefetch_concurrency) = self.prefetch_concurrency {
            cache.prefetch_concurrency = prefetch_concurrency;
        }
        if let Some(range_background_prefetch) = self.range_background_prefetch {
            cache.range_background_prefetch = range_background_prefetch;
        }
        if let Some(memory_budget_bytes) = self.memory_budget_bytes {
            cache.memory_budget_bytes = memory_budget_bytes;
        }
        if let Some(compression) = self.compression {
            cache.compression = parse_compression(&compression, self.zstd_level)?;
        }
        if let Some(verify_cache_checksum) = self.verify_cache_checksum {
            cache.verify_cache_checksum = parse_cache_integrity_mode(&verify_cache_checksum)?;
        }
        if let Some(writeback_mode) = self.writeback_mode {
            cache.writeback_mode = parse_writeback_mode(&writeback_mode)?;
        }
        if let Some(writeback_persist_sync) = self.writeback_persist_sync {
            cache.writeback_persist_sync = writeback_persist_sync;
        }
        if let Some(bandwidth) = self.bandwidth {
            cache.bandwidth = BandwidthConfig {
                upload_limit_mibps: bandwidth.upload_limit_mibps,
                download_limit_mibps: bandwidth.download_limit_mibps,
            };
        }

        Ok(cache)
    }
}

fn parse_cache_integrity_mode(value: &str) -> anyhow::Result<CacheIntegrityMode> {
    match value.to_ascii_lowercase().replace('-', "_").as_str() {
        "none" | "off" | "false" | "disable" | "disabled" => Ok(CacheIntegrityMode::None),
        "full" | "on" | "true" | "enable" | "enabled" | "crc32c" | "extend" | "shrink" => {
            Ok(CacheIntegrityMode::Full)
        }
        other => {
            anyhow::bail!(
                "unsupported cache.verify_cache_checksum '{other}' (expected none or full)"
            )
        }
    }
}

fn parse_compression(value: &str, zstd_level: Option<i32>) -> anyhow::Result<Compression> {
    match value.to_ascii_lowercase().as_str() {
        "none" | "off" | "disable" | "disabled" => Ok(Compression::None),
        "lz4" => Ok(Compression::Lz4),
        "zstd" | "zstd-default" => Ok(Compression::Zstd(zstd_level.unwrap_or(3))),
        other => {
            anyhow::bail!("unsupported cache.compression '{other}' (expected none, lz4, or zstd)")
        }
    }
}

fn parse_writeback_mode(value: &str) -> anyhow::Result<WriteBackMode> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "upload_before_commit" | "upload_first" | "safe" | "default" => {
            Ok(WriteBackMode::UploadBeforeCommit)
        }
        "commit_before_upload" | "commit_first" | "writeback" | "s3_writeback" => {
            Ok(WriteBackMode::CommitBeforeUpload)
        }
        other => anyhow::bail!(
            "unsupported cache.writeback_mode '{other}' (expected upload_before_commit or commit_before_upload)"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use clap::Parser;
    use clap::error::ErrorKind;

    fn empty_mount_args(config: Option<PathBuf>, mount_point: Option<PathBuf>) -> MountArgs {
        MountArgs {
            config,
            mount_point,
            data_backend: None,
            data_dir: None,
            s3_bucket: None,
            s3_endpoint: None,
            s3_region: None,
            s3_part_size: None,
            s3_max_concurrency: None,
            s3_force_path_style: None,
            s3_disable_payload_checksum: None,
            meta_backend: None,
            meta_url: None,
            meta_etcd_urls: None,
            meta_tikv_pd_endpoints: None,
            meta_tikv_namespace: None,
            chunk_size: None,
            block_size: None,
            fuse_workers: None,
            fuse_max_background: None,
            privileged: false,
        }
    }

    #[test]
    fn info_subcommand_parses_mount_point() {
        let cli = Cli::parse_from(["brewfs", "info", "/mnt/slayer"]);

        match cli.cmd {
            Command::Info(args) => {
                assert_eq!(args.mount_point, Some(PathBuf::from("/mnt/slayer")));
            }
            other => panic!("expected info command, got {other:?}"),
        }
    }

    #[test]
    fn parses_console_command_defaults() {
        let cli = Cli::parse_from(["brewfs", "console", "--dev-no-auth"]);

        let Command::Console(args) = cli.cmd else {
            panic!("expected console command");
        };

        assert_eq!(
            args.listen,
            std::net::SocketAddr::from(([127, 0, 0, 1], 8080))
        );
        assert!(args.dev_no_auth);
        assert!(!args.enable_csi_dashboard);
        assert!(args.state_dir.is_none());
        assert!(args.runtime_dir.is_none());
        assert!(args.static_dir.is_none());
        assert!(args.kubeconfig.is_none());
        assert_eq!(args.csi_driver_name, "csi.brewfs.io");
        assert!(args.auth_token_file.is_none());
    }

    #[test]
    fn mount_subcommand_parses_fuse_worker_args() {
        let cli = Cli::parse_from([
            "brewfs",
            "mount",
            "/mnt/slayer",
            "--fuse-workers",
            "4",
            "--fuse-max-background",
            "64",
        ]);

        match cli.cmd {
            Command::Mount(args) => {
                assert_eq!(args.mount_point, Some(PathBuf::from("/mnt/slayer")));
                assert_eq!(args.fuse_workers, Some(4));
                assert_eq!(args.fuse_max_background, Some(64));
            }
            other => panic!("expected mount command, got {other:?}"),
        }
    }

    #[test]
    fn version_output_includes_build_commit() {
        let mut cmd = Cli::command();
        let err = cmd
            .try_get_matches_from_mut(["brewfs", "--version"])
            .expect_err("--version should exit through clap DisplayVersion");
        assert_eq!(err.kind(), ErrorKind::DisplayVersion);

        let version = err.to_string();
        assert!(
            version.starts_with(&format!("brewfs {}\n", env!("CARGO_PKG_VERSION"))),
            "version output should start with package version, got: {version}"
        );

        assert!(
            version.contains("commit:"),
            "version output should include git commit metadata, got: {version}"
        );
        assert!(
            version.contains(env!("BREWFS_GIT_COMMIT")),
            "version output should include concrete git commit, got: {version}"
        );
    }

    #[test]
    fn mount_config_defaults_use_low_overhead_fuse_dispatch() {
        let config = MountConfig::from_sources(MountArgs {
            config: None,
            mount_point: Some(PathBuf::from("/mnt/slayer")),
            data_backend: None,
            data_dir: None,
            s3_bucket: None,
            s3_endpoint: None,
            s3_region: None,
            s3_part_size: None,
            s3_max_concurrency: None,
            s3_force_path_style: None,
            s3_disable_payload_checksum: None,
            meta_backend: None,
            meta_url: None,
            meta_etcd_urls: None,
            meta_tikv_pd_endpoints: None,
            meta_tikv_namespace: None,
            chunk_size: None,
            block_size: None,
            fuse_workers: None,
            fuse_max_background: None,
            privileged: false,
        })
        .unwrap();

        assert_eq!(config.fuse_workers, 1);
        assert_eq!(config.fuse_max_background, DEFAULT_FUSE_MAX_BACKGROUND);
    }

    #[test]
    fn mount_config_defaults_raise_s3_concurrency() {
        let config = MountConfig::from_sources(MountArgs {
            config: None,
            mount_point: Some(PathBuf::from("/mnt/slayer")),
            data_backend: None,
            data_dir: None,
            s3_bucket: None,
            s3_endpoint: None,
            s3_region: None,
            s3_part_size: None,
            s3_max_concurrency: None,
            s3_force_path_style: None,
            s3_disable_payload_checksum: None,
            meta_backend: None,
            meta_url: None,
            meta_etcd_urls: None,
            meta_tikv_pd_endpoints: None,
            meta_tikv_namespace: None,
            chunk_size: None,
            block_size: None,
            fuse_workers: None,
            fuse_max_background: None,
            privileged: false,
        })
        .unwrap();

        assert_eq!(config.s3_max_concurrency, DEFAULT_S3_MAX_CONCURRENCY);
        assert_eq!(config.s3_max_concurrency, 32);
    }

    #[test]
    fn mount_config_parses_tikv_meta_section() {
        let path = std::env::temp_dir().join(format!(
            "brewfs-tikv-meta-config-{}-{}.yaml",
            std::process::id(),
            "parse"
        ));
        std::fs::write(
            &path,
            r#"
mount_point: /mnt/slayer
meta:
  backend: tikv
  tikv:
    pd_endpoints:
      - 127.0.0.1:2379
      - 127.0.0.1:2380
    namespace: tenant-a
"#,
        )
        .unwrap();

        let config = MountConfig::from_sources(empty_mount_args(Some(path.clone()), None)).unwrap();
        let _ = std::fs::remove_file(path);

        assert!(matches!(config.meta_backend, MetaBackendKind::TiKv));
        assert_eq!(
            config.meta_tikv_pd_endpoints,
            vec!["127.0.0.1:2379", "127.0.0.1:2380"]
        );
        assert_eq!(config.meta_tikv_namespace, "tenant-a");
    }

    #[test]
    fn mount_config_parses_cache_section() {
        let path = std::env::temp_dir().join(format!(
            "brewfs-cache-config-{}-{}.yaml",
            std::process::id(),
            "parse"
        ));
        std::fs::write(
            &path,
            r#"
mount_point: /mnt/slayer
cache:
  root: /tmp/slayer-cache
  read_memory_bytes: 1048576
  read_ssd_bytes: 2097152
  write_memory_bytes: 3145728
  write_ssd_bytes: 4194304
  dirty_slice_target_size: 524288
  dirty_slice_max_age_ms: 250
  upload_concurrency: 7
  prefetch_enabled: false
  prefetch_max_bytes: 8388608
  prefetch_concurrency: 7
  range_background_prefetch: false
  memory_budget_bytes: 9437184
  compression: zstd
  zstd_level: 5
  verify_cache_checksum: none
  writeback_mode: upload_before_commit
  writeback_persist_sync: false
  bandwidth:
    upload_limit_mibps: 10
    download_limit_mibps: 20
"#,
        )
        .unwrap();

        let config = MountConfig::from_sources(empty_mount_args(Some(path.clone()), None)).unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(config.cache.cache_root, PathBuf::from("/tmp/slayer-cache"));
        assert_eq!(config.cache.read_memory_bytes, 1048576);
        assert_eq!(config.cache.read_ssd_bytes, 2097152);
        assert_eq!(config.cache.write_memory_bytes, 3145728);
        assert_eq!(config.cache.write_ssd_bytes, 4194304);
        assert_eq!(config.cache.dirty_slice_target_size, 524288);
        assert_eq!(config.cache.dirty_slice_max_age_ms, 250);
        assert_eq!(config.cache.upload_concurrency, 7);
        assert!(!config.cache.prefetch_enabled);
        assert_eq!(config.cache.prefetch_max_bytes, 8388608);
        assert_eq!(config.cache.prefetch_concurrency, 7);
        assert!(!config.cache.range_background_prefetch);
        assert_eq!(config.cache.memory_budget_bytes, 9437184);
        assert_eq!(config.cache.compression, Compression::Zstd(5));
        assert_eq!(config.cache.verify_cache_checksum, CacheIntegrityMode::None);
        assert_eq!(
            config.cache.writeback_mode,
            WriteBackMode::UploadBeforeCommit
        );
        assert!(!config.cache.writeback_persist_sync);
        assert_eq!(config.cache.bandwidth.upload_limit_mibps, Some(10));
        assert_eq!(config.cache.bandwidth.download_limit_mibps, Some(20));
    }

    #[test]
    fn parse_compression_rejects_unknown_values() {
        assert!(parse_compression("gzip", None).is_err());
    }

    #[test]
    fn mount_config_parses_s3_writeback_mode() {
        let path = std::env::temp_dir().join(format!(
            "brewfs-writeback-config-{}-{}.yaml",
            std::process::id(),
            "parse"
        ));
        std::fs::write(
            &path,
            r#"
mount_point: /mnt/slayer
data:
  backend: s3
cache:
  writeback_mode: commit_before_upload
"#,
        )
        .unwrap();

        let config = MountConfig::from_sources(empty_mount_args(Some(path.clone()), None)).unwrap();
        let _ = std::fs::remove_file(path);

        assert!(matches!(config.data_backend, DataBackendKind::S3));
        assert_eq!(
            config.cache.writeback_mode,
            WriteBackMode::CommitBeforeUpload
        );
    }

    #[test]
    fn mount_config_rejects_commit_before_upload_for_localfs() {
        let path = std::env::temp_dir().join(format!(
            "brewfs-writeback-config-{}-{}.yaml",
            std::process::id(),
            "reject"
        ));
        std::fs::write(
            &path,
            r#"
mount_point: /mnt/slayer
cache:
  writeback_mode: commit_before_upload
"#,
        )
        .unwrap();

        let err = MountConfig::from_sources(empty_mount_args(Some(path.clone()), None))
            .expect_err("commit-before-upload should require s3");
        let _ = std::fs::remove_file(path);

        assert!(err.to_string().contains("requires data.backend=s3"));
    }

    #[test]
    fn parse_writeback_mode_accepts_aliases() {
        assert_eq!(
            parse_writeback_mode("upload-first").unwrap(),
            WriteBackMode::UploadBeforeCommit
        );
        assert_eq!(
            parse_writeback_mode("s3_writeback").unwrap(),
            WriteBackMode::CommitBeforeUpload
        );
        assert!(parse_writeback_mode("fastest").is_err());
    }
}
