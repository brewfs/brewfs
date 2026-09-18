use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use url::Url;

use crate::chunk::bandwidth::BandwidthConfig;
use crate::chunk::cache_integrity::CacheIntegrityMode;
use crate::chunk::compress::Compression;
use crate::chunk::layout::{DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE};
use crate::meta::config::CompactConfig;
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

    /// Manage isolated BrewFS workspaces.
    #[cfg(feature = "workspace-overlay")]
    Workspace(WorkspaceArgs),

    /// Talk to a mounted BrewFS instance and run orphan gc.
    Gc(GcArgs),

    /// Talk to a mounted BrewFS instance and print mount information.
    Info(InfoArgs),

    /// Run the BrewFS web console.
    Console(ConsoleArgs),

    /// Run protocol gateways that expose a BrewFS volume over S3/WebDAV/NFS
    /// without a FUSE mount.
    #[cfg(any(feature = "gateway-s3", feature = "gateway-webdav"))]
    Gateway(Box<GatewayArgs>),

    /// Run a direct S3 object PUT benchmark without going through FUSE.
    #[command(hide = true)]
    ObjectPutBench(ObjectPutBenchArgs),
}

#[cfg(any(feature = "gateway-s3", feature = "gateway-webdav"))]
#[derive(Subcommand, Debug)]
pub enum GatewayProtocol {
    /// S3-compatible gateway (see doc/protocols/s3-gateway.md).
    #[cfg(feature = "gateway-s3")]
    S3(S3GatewayArgs),

    /// WebDAV gateway for filesystem clients.
    #[cfg(feature = "gateway-webdav")]
    #[command(name = "webdav")]
    WebDav(WebDavGatewayArgs),
}

#[cfg(any(feature = "gateway-s3", feature = "gateway-webdav"))]
#[derive(Args, Debug)]
pub struct GatewayArgs {
    #[command(subcommand)]
    pub protocol: GatewayProtocol,
}

#[cfg(feature = "gateway-webdav")]
#[derive(Args, Debug)]
pub struct WebDavGatewayArgs {
    /// HTTP or HTTPS listen address of the WebDAV endpoint.
    #[arg(long, value_name = "ADDR", default_value = "0.0.0.0:9001")]
    pub listen: std::net::SocketAddr,

    /// Basic authentication username (or env BREWFS_WEBDAV_USER).
    #[arg(long, value_name = "USER", env = "BREWFS_WEBDAV_USER")]
    pub user: Option<String>,

    /// Basic authentication password (or env BREWFS_WEBDAV_PASSWORD).
    #[arg(long, value_name = "PASSWORD", env = "BREWFS_WEBDAV_PASSWORD")]
    pub password: Option<String>,

    /// PEM certificate chain for HTTPS.
    #[arg(long, value_name = "FILE", env = "BREWFS_WEBDAV_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// PEM private key for HTTPS.
    #[arg(long, value_name = "FILE", env = "BREWFS_WEBDAV_TLS_KEY")]
    pub tls_key: Option<PathBuf>,

    /// Allow unauthenticated read/write access.
    #[arg(long, default_value_t = false)]
    pub allow_anonymous: bool,

    /// Publish PUT/PATCH content atomically after a successful flush.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub atomic_put: bool,

    /// Volume backend options; the same set accepted by `brewfs mount`.
    #[command(flatten)]
    pub mount: MountArgs,
}

#[cfg(feature = "gateway-s3")]
#[derive(Args, Debug)]
pub struct S3GatewayArgs {
    /// HTTP listen address of the S3 endpoint.
    #[arg(long, value_name = "ADDR", default_value = "0.0.0.0:9000")]
    pub listen: std::net::SocketAddr,

    /// Static access key for SigV4 (or env BREWFS_S3_ACCESS_KEY).
    #[arg(long, value_name = "KEY", env = "BREWFS_S3_ACCESS_KEY")]
    pub access_key: Option<String>,

    /// Static secret key for SigV4 (or env BREWFS_S3_SECRET_KEY).
    #[arg(long, value_name = "KEY", env = "BREWFS_S3_SECRET_KEY")]
    pub secret_key: Option<String>,

    /// Bucket name exposed in single-bucket mode (defaults to "brewfs").
    #[arg(long, value_name = "NAME", default_value = "brewfs")]
    pub bucket: String,

    /// Expose top-level directories as buckets instead of a single bucket.
    #[arg(long, default_value_t = false)]
    pub multi_buckets: bool,

    /// Hide directory objects in listings.
    #[arg(long, default_value_t = false)]
    pub hide_dir_objects: bool,

    /// Volume backend options; the same set accepted by `brewfs mount`.
    #[command(flatten)]
    pub mount: MountArgs,
}

#[derive(Args, Debug, Clone)]
pub struct MountArgs {
    /// YAML config file path.
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// On-disk metadata format. Existing configurations default to flat-v1.
    #[cfg_attr(feature = "workspace-overlay", arg(long, value_enum))]
    #[cfg_attr(not(feature = "workspace-overlay"), arg(skip))]
    pub volume_format: Option<VolumeFormat>,

    /// Workspace mounted by a workspace-v1 volume.
    #[cfg_attr(feature = "workspace-overlay", arg(long, value_name = "WORKSPACE_ID"))]
    #[cfg_attr(not(feature = "workspace-overlay"), arg(skip))]
    pub workspace: Option<uuid::Uuid>,

    /// Key namespace for the workspace overlay catalog.
    #[cfg_attr(feature = "workspace-overlay", arg(long, value_name = "NAMESPACE"))]
    #[cfg_attr(not(feature = "workspace-overlay"), arg(skip))]
    pub workspace_namespace: Option<String>,

    /// Disable mount-local workspace recovery and garbage collection. The
    /// Kubernetes operator owns those control-plane duties in this mode.
    #[cfg_attr(feature = "workspace-overlay", arg(long, default_value_t = false))]
    #[cfg_attr(not(feature = "workspace-overlay"), arg(skip))]
    pub workspace_operator_managed: bool,

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

#[derive(ValueEnum, Deserialize, Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum VolumeFormat {
    #[default]
    FlatV1,
    WorkspaceV1,
    WorkspaceNativeV2,
}

#[cfg(feature = "workspace-overlay")]
#[derive(Args, Debug, Clone)]
pub struct WorkspaceArgs {
    /// Workspace catalog backend (sqlx, redis or tikv).
    #[arg(long, global = true, value_enum, default_value = "sqlx")]
    pub meta_backend: WorkspaceMetaBackendKind,

    /// SQLite or Redis URL containing the workspace-v1 catalog.
    #[arg(long, global = true, default_value = DEFAULT_META_URL)]
    pub meta_url: String,

    /// TiKV PD endpoint URLs (comma-separated).
    #[arg(long, global = true, value_name = "URLS", value_delimiter = ',')]
    pub meta_tikv_pd_endpoints: Vec<String>,

    /// Key namespace for the workspace overlay catalog.
    #[arg(long, global = true, default_value = "brewfs")]
    pub workspace_namespace: String,

    #[command(subcommand)]
    pub command: WorkspaceCommand,
}

#[cfg(feature = "workspace-overlay")]
#[derive(Subcommand, Debug, Clone)]
pub enum WorkspaceCommand {
    /// Initialize a new workspace-v1 volume and its default workspace.
    InitVolume {
        #[arg(long)]
        owner: Option<String>,
    },
    /// Add the explicit native-v2 control header for an existing workspace-v1
    /// catalog. This never rewrites the catalog header or migrates data.
    #[cfg(feature = "native-packed-base")]
    InitNative,
    /// Create a workspace from an exact sealed revision.
    Create {
        #[arg(long = "from", value_name = "REVISION")]
        revision: Option<crate::workspace_overlay::model::BaseRevision>,
        #[arg(long)]
        owner: Option<String>,
    },
    /// Seal a workspace and create a named snapshot root.
    Snapshot {
        workspace: crate::workspace_overlay::ids::WorkspaceId,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        owner: Option<String>,
    },
    /// Fork one or more workspaces from a workspace UUID or exact revision.
    Fork {
        source: String,
        #[arg(long, default_value_t = 1)]
        count: usize,
        #[arg(long)]
        owner: Option<String>,
    },
    /// List workspaces.
    List,
    /// Inspect one workspace and its active lease/seal state.
    Inspect {
        workspace: crate::workspace_overlay::ids::WorkspaceId,
    },
    /// Show path-level changes against the fork base or an exact revision.
    Diff {
        workspace: crate::workspace_overlay::ids::WorkspaceId,
        #[arg(long, value_name = "REVISION")]
        against: Option<crate::workspace_overlay::model::BaseRevision>,
        #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
        chunk_size: u64,
    },
    /// Mark a workspace for lease-aware garbage collection.
    Discard {
        workspace: crate::workspace_overlay::ids::WorkspaceId,
        #[arg(long)]
        force: bool,
    },
    /// Seal and fast-forward a target workspace if its base is unchanged.
    Commit {
        workspace: crate::workspace_overlay::ids::WorkspaceId,
        #[arg(long = "to")]
        target: crate::workspace_overlay::ids::WorkspaceId,
    },
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

#[derive(Args, Debug, Clone)]
pub struct ObjectPutBenchArgs {
    /// S3 bucket name.
    #[arg(long, value_name = "BUCKET")]
    pub s3_bucket: String,

    /// S3-compatible endpoint URL.
    #[arg(long, value_name = "URL")]
    pub s3_endpoint: Option<String>,

    /// S3 region.
    #[arg(long, value_name = "REGION", default_value = "us-east-1")]
    pub s3_region: String,

    /// S3 part size in bytes.
    #[arg(long, default_value_t = DEFAULT_S3_PART_SIZE)]
    pub s3_part_size: usize,

    /// S3 maximum concurrent multipart upload parts.
    #[arg(long, default_value_t = DEFAULT_S3_MAX_CONCURRENCY)]
    pub s3_max_concurrency: usize,

    /// Force path-style S3 access.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub s3_force_path_style: bool,

    /// Disable S3 payload checksum/signing.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub s3_disable_payload_checksum: bool,

    /// Object payload size in bytes.
    #[arg(long, default_value_t = DEFAULT_BLOCK_SIZE as usize)]
    pub object_size: usize,

    /// Number of concurrent object PUT workers.
    #[arg(long, default_value_t = DEFAULT_S3_MAX_CONCURRENCY)]
    pub workers: usize,

    /// Maximum benchmark duration. Use 0 to rely only on --objects.
    #[arg(long, default_value_t = 60)]
    pub duration_secs: u64,

    /// Maximum objects to upload. Use 0 for duration-only mode.
    #[arg(long, default_value_t = 0)]
    pub objects: u64,

    /// Object key prefix.
    #[arg(long, default_value = "bench/direct-put")]
    pub prefix: String,
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

#[cfg(feature = "workspace-overlay")]
#[derive(ValueEnum, Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceMetaBackendKind {
    Sqlx,
    Redis,
    #[value(name = "tikv", alias = "ti-kv")]
    TiKv,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MountFileConfig {
    pub mount_point: Option<PathBuf>,
    pub volume_format: Option<VolumeFormat>,
    pub volume: Option<VolumeFileConfig>,
    pub native_base: Option<NativeBaseFileConfig>,
    pub workspace: Option<uuid::Uuid>,
    pub workspace_namespace: Option<String>,
    #[serde(default)]
    pub workspace_operator_managed: bool,
    pub data: Option<DataFileConfig>,
    pub meta: Option<MetaFileConfig>,
    pub layout: Option<LayoutFileConfig>,
    pub fuse: Option<FuseFileConfig>,
    pub cache: Option<CacheFileConfig>,
    pub compact: Option<CompactConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct VolumeFileConfig {
    pub format: Option<VolumeFormat>,
    pub schema_version: Option<u32>,
    pub native_control_version: Option<u32>,
    pub chunk_size: Option<u64>,
    pub block_size: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBaseFileConfig {
    pub release_profile: String,
    pub format_version: u16,
    pub namespace_mode: String,
    pub packing: NativePackingFileConfig,
    pub reader: NativeReaderFileConfig,
    pub cache: NativeCacheFileConfig,
    pub prefetch: NativePrefetchFileConfig,
    pub commit: NativeCommitFileConfig,
    pub durability: NativeDurabilityFileConfig,
    pub retention: NativeRetentionFileConfig,
    pub cleanup: NativeCleanupFileConfig,
    pub repack: NativeRepackFileConfig,
    pub limits: NativeLimitsFileConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePackingFileConfig {
    pub payload_format: String,
    pub pack_target_bytes: u64,
    pub codec: String,
    pub multipart_part_target_bytes: u64,
    pub retention_grouping: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeReaderFileConfig {
    pub capture_protocol: String,
    pub ordered_inode_commits: bool,
    pub capture_deadline_ms: u64,
    pub capture_max_retries: u32,
    pub max_read_capture_bytes: u64,
    pub max_plan_segments: u64,
    pub max_plan_bytes: u64,
    pub demand_get_concurrency: usize,
    pub request_deadline_ms: u64,
    pub inflight_encoded_bytes: u64,
    pub inflight_decoded_bytes: u64,
    pub physical_coalescing: bool,
    pub exact_range_response_required: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCacheFileConfig {
    pub scope: String,
    pub shared_service: bool,
    pub metadata_ram_bytes: u64,
    pub decoded_data_bytes: u64,
    pub encoded_disk_bytes: u64,
    pub trust_domain_isolation: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePrefetchFileConfig {
    pub enabled: bool,
    pub get_concurrency: usize,
    pub metadata_only_ops_fetch_data: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCommitFileConfig {
    pub max_receipts: usize,
    pub max_payload_bytes: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeDurabilityFileConfig {
    pub write_mode: String,
    pub remote_verification: String,
    pub require_atomic_create_only: bool,
    pub coordinator_profile: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRetentionFileConfig {
    pub published: String,
    pub published_gc: bool,
    pub read_retention_leases: bool,
    pub promotion: String,
    pub private_write_domain: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCleanupFileConfig {
    pub mode: String,
    pub closed_private_domains_only: bool,
    pub require_close_certificate: bool,
    pub backend_mode: String,
    pub allow_legacy_slice_gc: bool,
    pub unknown_upload_policy: String,
    pub count_delete_markers_as_freed_bytes: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRepackFileConfig {
    pub automatic: bool,
    pub reclaim_published: bool,
    pub require_explicit_extra_bytes_budget: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeLimitsFileConfig {
    pub namespace_hard_bytes: u64,
    pub private_domain_soft_bytes: u64,
    pub private_domain_hard_bytes: u64,
    pub completion_reserve_bytes: u64,
    pub on_hard_limit: String,
}

impl NativeBaseFileConfig {
    fn validate_p1(&self) -> anyhow::Result<()> {
        if !matches!(
            self.release_profile.as_str(),
            "p1-retained-trial" | "p1-private-cleanup"
        ) {
            anyhow::bail!(
                "unsupported native_base.release_profile {}",
                self.release_profile
            )
        }
        if self.format_version != 3 {
            anyhow::bail!(
                "unsupported native_base.format_version {}",
                self.format_version
            )
        }
        if self.namespace_mode != "kv" {
            anyhow::bail!(
                "PR07 requires native_base.namespace_mode=kv; found {}",
                self.namespace_mode
            )
        }
        if self.packing.payload_format != "native-block-v1"
            || self.packing.codec != "none"
            || self.packing.retention_grouping != "required"
        {
            anyhow::bail!(
                "PR07 supports only native-block-v1/none packing with required retention grouping"
            )
        }
        if self.packing.pack_target_bytes == 0
            || self.packing.multipart_part_target_bytes == 0
            || self.reader.capture_deadline_ms == 0
            || self.reader.capture_max_retries == 0
            || self.reader.max_read_capture_bytes == 0
            || self.reader.max_plan_segments == 0
            || self.reader.max_plan_bytes == 0
            || self.reader.demand_get_concurrency == 0
            || self.reader.request_deadline_ms == 0
            || self.reader.inflight_encoded_bytes == 0
            || self.reader.inflight_decoded_bytes == 0
            || self.cache.metadata_ram_bytes == 0
            || self.cache.decoded_data_bytes == 0
            || self.cache.encoded_disk_bytes == 0
            || self.commit.max_receipts == 0
            || self.commit.max_payload_bytes == 0
        {
            anyhow::bail!("native_base P1 budgets and limits must be greater than zero")
        }
        if self.reader.capture_protocol != "single-writer-bounded-capture"
            || !self.reader.ordered_inode_commits
            || self.reader.physical_coalescing
            || !self.reader.exact_range_response_required
        {
            anyhow::bail!("native_base.reader does not match the supported PR07 P1 profile")
        }
        if self.cache.scope != "per-mount"
            || self.cache.shared_service
            || !self.cache.trust_domain_isolation
        {
            anyhow::bail!("native_base.cache does not match the supported PR07 P1 profile")
        }
        if self.prefetch.enabled
            || self.prefetch.get_concurrency != 0
            || self.prefetch.metadata_only_ops_fetch_data
        {
            anyhow::bail!("native_base.prefetch must be disabled for the PR07 P1 profile")
        }
        if self.durability.write_mode != "upload-before-commit"
            || self.durability.remote_verification != "exact-readback"
            || !self.durability.require_atomic_create_only
            || self.durability.coordinator_profile.trim().is_empty()
        {
            anyhow::bail!("native_base.durability does not satisfy the PR07 P1 contract")
        }
        if self.retention.published != "forever"
            || self.retention.published_gc
            || self.retention.read_retention_leases
            || self.retention.promotion != "exact-object-retain-batches"
            || self.retention.private_write_domain != "workspace-lifetime"
        {
            anyhow::bail!("native_base.retention violates permanent published retention")
        }
        if !matches!(self.cleanup.mode.as_str(), "disabled" | "dry-run" | "apply")
            || !self.cleanup.closed_private_domains_only
            || !self.cleanup.require_close_certificate
            || self.cleanup.backend_mode != "unversioned-only"
            || self.cleanup.allow_legacy_slice_gc
            || self.cleanup.unknown_upload_policy != "quarantine"
            || self.cleanup.count_delete_markers_as_freed_bytes
        {
            anyhow::bail!("native_base.cleanup violates the private-domain cleanup contract")
        }
        if self.release_profile == "p1-retained-trial" && self.cleanup.mode != "disabled" {
            anyhow::bail!("p1-retained-trial requires native_base.cleanup.mode=disabled")
        }
        if self.release_profile == "p1-private-cleanup" && self.cleanup.mode == "disabled" {
            anyhow::bail!("p1-private-cleanup requires dry-run or apply cleanup mode")
        }
        if self.repack.automatic
            || self.repack.reclaim_published
            || !self.repack.require_explicit_extra_bytes_budget
        {
            anyhow::bail!("native_base.repack cannot reclaim published objects")
        }
        if self.limits.namespace_hard_bytes == 0
            || self.limits.private_domain_soft_bytes == 0
            || self.limits.private_domain_hard_bytes == 0
            || self.limits.completion_reserve_bytes == 0
            || self.limits.private_domain_soft_bytes > self.limits.private_domain_hard_bytes
            || self.limits.private_domain_hard_bytes > self.limits.namespace_hard_bytes
            || self.limits.on_hard_limit != "reject-new-admissions-not-accepted-drain"
        {
            anyhow::bail!("native_base.limits are invalid for the PR07 P1 profile")
        }
        Ok(())
    }
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
    pub read_plan_cache_max_weight: Option<u64>,
    pub allow_write_open_cache: Option<bool>,
    pub slice_version_check_interval_ms: Option<u64>,
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
    pub populate_write_cache_after_upload: Option<bool>,
    pub persist_write_cache_after_upload: Option<bool>,
    pub memory_budget_bytes: Option<u64>,
    pub compression: Option<String>,
    pub zstd_level: Option<i32>,
    pub verify_cache_checksum: Option<String>,
    pub writeback_mode: Option<String>,
    pub writeback_persist_sync: Option<bool>,
    pub writeback_require_stage_before_commit: Option<bool>,
    pub writeback_recent_pending_soft_bytes: Option<u64>,
    pub writeback_recent_pending_hard_bytes: Option<u64>,
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
    pub volume_format: VolumeFormat,
    pub workspace: Option<uuid::Uuid>,
    pub workspace_namespace: String,
    pub workspace_operator_managed: bool,
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
    pub meta_read_plan_cache_max_weight: Option<u64>,
    pub meta_allow_write_open_cache: bool,
    pub meta_slice_version_check_interval_ms: Option<u64>,
    pub chunk_size: u64,
    pub block_size: u32,
    pub fuse_workers: usize,
    pub fuse_max_background: usize,
    pub privileged: bool,
    pub cache: VfsCacheConfig,
    pub compact: CompactConfig,
    pub native_base: Option<NativeBaseFileConfig>,
}

fn resolve_volume_size<T>(
    name: &str,
    cli: Option<T>,
    legacy: Option<T>,
    volume: Option<T>,
    default: T,
) -> anyhow::Result<T>
where
    T: Copy + Eq + std::fmt::Display,
{
    if let Some(contract) = volume {
        for (source, value) in [("CLI", cli), ("layout", legacy)] {
            if value.is_some_and(|value| value != contract) {
                anyhow::bail!(
                    "{source} {name} conflicts with the declared volume {name} {contract}"
                );
            }
        }
        return Ok(contract);
    }
    Ok(cli.or(legacy).unwrap_or(default))
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

        let volume_cfg = file_cfg.volume.unwrap_or_default();
        let native_base = file_cfg.native_base;
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
        let compact = file_cfg.compact.unwrap_or_default();
        let workspace = args.workspace.or(file_cfg.workspace);
        if let (Some(legacy), Some(nested)) = (file_cfg.volume_format, volume_cfg.format) {
            if legacy != nested {
                anyhow::bail!("volume_format conflicts with volume.format");
            }
        }
        let file_volume_format = volume_cfg.format.or(file_cfg.volume_format);
        if let (Some(cli), Some(nested)) = (args.volume_format, volume_cfg.format) {
            if cli != nested {
                anyhow::bail!("--volume-format conflicts with config volume.format");
            }
        }
        let volume_format = args
            .volume_format
            .or(file_volume_format)
            .unwrap_or_else(|| {
                if workspace.is_some() {
                    VolumeFormat::WorkspaceV1
                } else {
                    VolumeFormat::FlatV1
                }
            });
        if workspace.is_some() && volume_format == VolumeFormat::FlatV1 {
            anyhow::bail!("workspace id cannot be used with volume_format=flat-v1");
        }
        if workspace.is_none() && volume_format == VolumeFormat::WorkspaceNativeV2 {
            anyhow::bail!("workspace id is required with volume_format=workspace-native-v2");
        }
        if volume_format == VolumeFormat::WorkspaceNativeV2 {
            if volume_cfg.format != Some(VolumeFormat::WorkspaceNativeV2) {
                anyhow::bail!("workspace-native-v2 requires an explicit volume.format declaration");
            }
            if volume_cfg.schema_version != Some(2) {
                anyhow::bail!("workspace-native-v2 requires volume.schema_version=2");
            }
            if volume_cfg.native_control_version != Some(2) {
                anyhow::bail!("workspace-native-v2 requires volume.native_control_version=2");
            }
            if volume_cfg.chunk_size.is_none() || volume_cfg.block_size.is_none() {
                anyhow::bail!(
                    "workspace-native-v2 requires volume.chunk_size and volume.block_size"
                );
            }
            native_base
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("workspace-native-v2 requires native_base config"))?
                .validate_p1()?;
        } else if native_base.is_some()
            || volume_cfg.schema_version.is_some()
            || volume_cfg.native_control_version.is_some()
        {
            anyhow::bail!("native volume controls require volume.format=workspace-native-v2");
        }
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
        if volume_format == VolumeFormat::WorkspaceNativeV2
            && matches!(cache.writeback_mode, WriteBackMode::CommitBeforeUpload)
        {
            anyhow::bail!(
                "volume_format=workspace-native-v2 requires cache.writeback_mode=upload_before_commit"
            );
        }

        let mount_point = args.mount_point.or(file_cfg.mount_point).ok_or_else(|| {
            anyhow::anyhow!("mount point is required (positional arg or config.mount_point)")
        })?;

        let meta_backend = args
            .meta_backend
            .or(meta_cfg.backend)
            .unwrap_or(MetaBackendKind::Sqlx);

        if volume_format == VolumeFormat::WorkspaceNativeV2
            && !matches!(meta_backend, MetaBackendKind::Redis | MetaBackendKind::TiKv)
        {
            anyhow::bail!("volume_format=workspace-native-v2 requires meta.backend=redis or tikv");
        }

        let meta_url_from_file = match meta_backend {
            MetaBackendKind::Sqlx => sqlx_cfg.url,
            MetaBackendKind::Redis => redis_cfg.url,
            MetaBackendKind::Etcd => None,
            MetaBackendKind::TiKv => None,
        };
        if meta_cfg.read_plan_cache_max_weight == Some(0) {
            anyhow::bail!("meta.read_plan_cache_max_weight must be greater than 0");
        }
        let chunk_size = resolve_volume_size(
            "chunk_size",
            args.chunk_size,
            layout_cfg.chunk_size,
            volume_cfg.chunk_size,
            DEFAULT_CHUNK_SIZE,
        )?;
        let block_size = resolve_volume_size(
            "block_size",
            args.block_size,
            layout_cfg.block_size,
            volume_cfg.block_size,
            DEFAULT_BLOCK_SIZE,
        )?;
        if block_size == 0 || chunk_size == 0 || chunk_size % u64::from(block_size) != 0 {
            anyhow::bail!("chunk_size must be a non-zero multiple of block_size");
        }

        Ok(Self {
            mount_point,
            volume_format,
            workspace,
            workspace_namespace: args
                .workspace_namespace
                .or(file_cfg.workspace_namespace)
                .unwrap_or_else(|| "brewfs".to_string()),
            workspace_operator_managed: args.workspace_operator_managed
                || file_cfg.workspace_operator_managed,
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
            meta_read_plan_cache_max_weight: meta_cfg.read_plan_cache_max_weight,
            meta_allow_write_open_cache: meta_cfg.allow_write_open_cache.unwrap_or(false),
            meta_slice_version_check_interval_ms: meta_cfg.slice_version_check_interval_ms,
            chunk_size,
            block_size,
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
            compact,
            native_base,
        })
    }

    /// Derive a stable, non-secret identity for the persistent state owned by
    /// a flat volume. Both halves are required: metadata identifies the inode
    /// and slice namespace, while the data backend identifies the objects to
    /// which those slice IDs refer.
    pub(crate) fn flat_volume_cache_scope(&self) -> anyhow::Result<String> {
        let mut parts = vec!["flat-v1".to_string()];

        match self.meta_backend {
            MetaBackendKind::Sqlx => {
                parts.push("meta:sqlx".to_string());
                if crate::meta::stores::database::is_sqlite_memory_url(&self.meta_url) {
                    // In-memory metadata has no identity that survives a
                    // restart, so it must never recover persistent cache state
                    // produced by an earlier mount.
                    parts.push(format!("ephemeral:{}", uuid::Uuid::new_v4()));
                } else {
                    parts.push(scrubbed_url_identity(&self.meta_url));
                }
            }
            MetaBackendKind::Redis => {
                parts.push("meta:redis".to_string());
                parts.push(scrubbed_url_identity(&self.meta_url));
            }
            MetaBackendKind::Etcd => {
                parts.push("meta:etcd".to_string());
                let mut endpoints = self
                    .meta_etcd_urls
                    .iter()
                    .map(|endpoint| scrubbed_url_identity(endpoint))
                    .collect::<Vec<_>>();
                endpoints.sort_unstable();
                parts.extend(endpoints);
            }
            MetaBackendKind::TiKv => {
                parts.push("meta:tikv".to_string());
                let mut endpoints = self
                    .meta_tikv_pd_endpoints
                    .iter()
                    .map(|endpoint| scrubbed_url_identity(endpoint))
                    .collect::<Vec<_>>();
                endpoints.sort_unstable();
                parts.extend(endpoints);
                parts.push(format!("namespace:{}", self.meta_tikv_namespace));
            }
        }

        match self.data_backend {
            DataBackendKind::LocalFs => {
                parts.push("data:localfs".to_string());
                // Keep this lexical rather than canonical: the identity must
                // not change merely because the directory is created between
                // two mounts, and treating symlink aliases as separate cache
                // owners is safe.
                let identity = std::path::absolute(&self.data_dir)?;
                parts.push(identity.to_string_lossy().into_owned());
            }
            DataBackendKind::S3 => {
                parts.push("data:s3".to_string());
                parts.push(format!(
                    "endpoint:{}",
                    self.s3_endpoint
                        .as_deref()
                        .map(scrubbed_url_identity)
                        // Without an explicit endpoint the AWS SDK may resolve
                        // a different service from the environment or active
                        // profile. Do not persistently reuse cache state when
                        // that identity cannot be derived from MountConfig.
                        .unwrap_or_else(|| format!("ephemeral:{}", uuid::Uuid::new_v4()))
                ));
                parts.push(format!(
                    "bucket:{}",
                    self.s3_bucket.as_deref().unwrap_or_default()
                ));
                parts.push(format!(
                    "region:{}",
                    self.s3_region.as_deref().unwrap_or_default()
                ));
            }
        }

        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }
        Ok(hex::encode(hasher.finalize()))
    }
}

fn scrubbed_url_identity(raw: &str) -> String {
    fn is_secret_query_key(key: &str) -> bool {
        let key = key.to_ascii_lowercase().replace('-', "_");
        key.contains("password")
            || key.contains("passwd")
            || key.contains("secret")
            || key.contains("credential")
            || key == "token"
            || key.starts_with("token_")
            || key.ends_with("_token")
            || key == "api_key"
            || key == "access_key"
            || key == "access_key_id"
            || key == "signature"
            || key.ends_with("_signature")
            || key == "authorization"
    }

    fn scrub(mut url: Url) -> String {
        let _ = url.set_username("");
        let _ = url.set_password(None);
        let mut query = url
            .query_pairs()
            .map(|(key, value)| {
                let key = key.into_owned();
                let value = if is_secret_query_key(&key) {
                    "<redacted>".to_string()
                } else {
                    value.into_owned()
                };
                (key, value)
            })
            .collect::<Vec<_>>();
        query.sort_unstable();
        url.set_query(None);
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(
                query
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            );
        }
        url.set_fragment(None);
        url.to_string()
    }

    let raw = raw.trim();
    if let Ok(url) = Url::parse(raw)
        && url.has_host()
    {
        return scrub(url);
    }
    if let Ok(url) = Url::parse(&format!("http://{raw}"))
        && url.has_host()
    {
        return scrub(url)
            .strip_prefix("http://")
            .unwrap_or(raw)
            .to_string();
    }
    raw.to_string()
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
        if let Some(populate_write_cache_after_upload) = self.populate_write_cache_after_upload {
            cache.populate_write_cache_after_upload = populate_write_cache_after_upload;
        }
        if let Some(persist_write_cache_after_upload) = self.persist_write_cache_after_upload {
            cache.persist_write_cache_after_upload = persist_write_cache_after_upload;
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
        if let Some(writeback_require_stage_before_commit) =
            self.writeback_require_stage_before_commit
        {
            cache.writeback_require_stage_before_commit = writeback_require_stage_before_commit;
        }
        if let Some(writeback_recent_pending_soft_bytes) = self.writeback_recent_pending_soft_bytes
        {
            cache.writeback_recent_pending_soft_bytes = writeback_recent_pending_soft_bytes;
        }
        if let Some(writeback_recent_pending_hard_bytes) = self.writeback_recent_pending_hard_bytes
        {
            cache.writeback_recent_pending_hard_bytes = writeback_recent_pending_hard_bytes;
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
            volume_format: None,
            workspace: None,
            workspace_namespace: None,
            workspace_operator_managed: false,
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
    fn object_put_bench_parses_s3_options() {
        let cli = Cli::parse_from([
            "brewfs",
            "object-put-bench",
            "--s3-bucket",
            "bench-bucket",
            "--s3-endpoint",
            "http://rustfs:9000",
            "--s3-force-path-style",
            "false",
            "--s3-disable-payload-checksum",
            "false",
            "--workers",
            "7",
            "--duration-secs",
            "3",
        ]);

        let Command::ObjectPutBench(args) = cli.cmd else {
            panic!("expected object-put-bench command");
        };

        assert_eq!(args.s3_bucket, "bench-bucket");
        assert_eq!(args.s3_endpoint.as_deref(), Some("http://rustfs:9000"));
        assert!(!args.s3_force_path_style);
        assert!(!args.s3_disable_payload_checksum);
        assert_eq!(args.workers, 7);
        assert_eq!(args.duration_secs, 3);
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
            volume_format: None,
            workspace: None,
            workspace_namespace: None,
            workspace_operator_managed: false,
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
        assert_eq!(config.volume_format, VolumeFormat::FlatV1);
        assert!(config.workspace.is_none());
    }

    #[test]
    fn default_cli_surface_does_not_expose_workspace_commands() {
        let help = Cli::command().render_long_help().to_string();

        #[cfg(not(feature = "workspace-overlay"))]
        assert!(!help.contains("workspace"));
        #[cfg(feature = "workspace-overlay")]
        assert!(help.contains("workspace"));
    }

    #[cfg(feature = "workspace-overlay")]
    #[test]
    fn workspace_mount_flags_are_feature_gated_and_parse() {
        let id = uuid::Uuid::from_u128(77);
        let cli = Cli::parse_from([
            "brewfs",
            "mount",
            "/mnt/workspace",
            "--volume-format",
            "workspace-v1",
            "--workspace",
            &id.to_string(),
        ]);
        let Command::Mount(args) = cli.cmd else {
            panic!("expected mount command");
        };
        assert_eq!(args.volume_format, Some(VolumeFormat::WorkspaceV1));
        assert_eq!(args.workspace, Some(id));
    }

    #[test]
    fn mount_config_defaults_raise_s3_concurrency() {
        let config = MountConfig::from_sources(MountArgs {
            config: None,
            volume_format: None,
            workspace: None,
            workspace_namespace: None,
            workspace_operator_managed: false,
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
    fn legacy_config_defaults_flat_but_workspace_marker_is_parseable() {
        let path = std::env::temp_dir().join(format!(
            "brewfs-workspace-format-config-{}-{}.yaml",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::write(
            &path,
            "mount_point: /mnt/workspace\nvolume_format: workspace-v1\nworkspace: 00000000-0000-0000-0000-000000000077\n",
        )
        .unwrap();
        let config = MountConfig::from_sources(empty_mount_args(Some(path.clone()), None)).unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(config.volume_format, VolumeFormat::WorkspaceV1);
        assert_eq!(config.workspace, Some(uuid::Uuid::from_u128(0x77)));
    }

    #[test]
    fn workspace_operator_managed_mode_is_opt_in() {
        let mut args = empty_mount_args(None, Some(PathBuf::from("/mnt/workspace")));
        assert!(
            !MountConfig::from_sources(args.clone())
                .unwrap()
                .workspace_operator_managed
        );

        args.workspace_operator_managed = true;
        assert!(
            MountConfig::from_sources(args)
                .unwrap()
                .workspace_operator_managed
        );
    }

    #[test]
    fn workspace_id_selects_workspace_format_and_rejects_explicit_flat_format() {
        let workspace = uuid::Uuid::from_u128(88);
        let mut args = empty_mount_args(None, Some(PathBuf::from("/mnt/workspace")));
        args.workspace = Some(workspace);
        let config = MountConfig::from_sources(args.clone()).unwrap();
        assert_eq!(config.volume_format, VolumeFormat::WorkspaceV1);
        assert_eq!(config.workspace, Some(workspace));

        args.volume_format = Some(VolumeFormat::FlatV1);
        assert!(
            MountConfig::from_sources(args)
                .unwrap_err()
                .to_string()
                .contains("workspace id cannot be used")
        );
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
    fn mount_config_parses_write_open_cache_opt_in() {
        let path = std::env::temp_dir().join(format!(
            "brewfs-write-open-cache-config-{}-{}.yaml",
            std::process::id(),
            "parse"
        ));
        std::fs::write(
            &path,
            r#"
mount_point: /mnt/slayer
meta:
  open_file_cache_ttl_ms: 1000
  open_file_cache_capacity: 65536
  read_plan_cache_max_weight: 32768
  allow_write_open_cache: true
  slice_version_check_interval_ms: 250
"#,
        )
        .unwrap();

        let config = MountConfig::from_sources(empty_mount_args(Some(path.clone()), None)).unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(config.meta_open_file_cache_ttl_ms, Some(1000));
        assert_eq!(config.meta_open_file_cache_capacity, Some(65536));
        assert_eq!(config.meta_read_plan_cache_max_weight, Some(32768));
        assert!(config.meta_allow_write_open_cache);
        assert_eq!(config.meta_slice_version_check_interval_ms, Some(250));
    }

    #[test]
    fn mount_config_rejects_zero_read_plan_cache_weight() {
        let path = std::env::temp_dir().join(format!(
            "brewfs-read-plan-cache-config-{}-{}",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::write(
            &path,
            "mount_point: /mnt/slayer\nmeta:\n  read_plan_cache_max_weight: 0\n",
        )
        .unwrap();

        let error = MountConfig::from_sources(empty_mount_args(Some(path.clone()), None))
            .expect_err("zero read-plan cache weight should be rejected");
        let _ = std::fs::remove_file(path);

        assert!(
            error
                .to_string()
                .contains("meta.read_plan_cache_max_weight must be greater than 0")
        );
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
  populate_write_cache_after_upload: true
  persist_write_cache_after_upload: true
  memory_budget_bytes: 9437184
  compression: zstd
  zstd_level: 5
  verify_cache_checksum: none
  writeback_mode: upload_before_commit
  writeback_persist_sync: false
  writeback_require_stage_before_commit: false
  writeback_recent_pending_soft_bytes: 1073741824
  writeback_recent_pending_hard_bytes: 2147483648
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
        assert!(config.cache.populate_write_cache_after_upload);
        assert!(config.cache.persist_write_cache_after_upload);
        assert_eq!(config.cache.memory_budget_bytes, 9437184);
        assert_eq!(config.cache.compression, Compression::Zstd(5));
        assert_eq!(config.cache.verify_cache_checksum, CacheIntegrityMode::None);
        assert_eq!(
            config.cache.writeback_mode,
            WriteBackMode::UploadBeforeCommit
        );
        assert!(!config.cache.writeback_persist_sync);
        assert!(!config.cache.writeback_require_stage_before_commit);
        assert_eq!(config.cache.writeback_recent_pending_soft_bytes, 1073741824);
        assert_eq!(config.cache.writeback_recent_pending_hard_bytes, 2147483648);
        assert_eq!(config.cache.bandwidth.upload_limit_mibps, Some(10));
        assert_eq!(config.cache.bandwidth.download_limit_mibps, Some(20));
    }

    #[test]
    fn mount_config_parses_compact_section() {
        let path = std::env::temp_dir().join(format!(
            "brewfs-compact-config-{}-{}.yaml",
            std::process::id(),
            "parse"
        ));
        std::fs::write(
            &path,
            r#"
mount_point: /mnt/slayer
compact:
  min_slice_count: 7
  min_fragment_ratio: 0.25
  async_threshold: 64
  sync_threshold: 128
  interval:
    secs: 2
    nanos: 0
  max_chunks_per_run: 32
  max_concurrent_tasks: 3
  light_enabled: true
  light_threshold: 3
  heavy_enabled: false
  heavy_fragment_threshold: 0.4
  heavy_slice_threshold: 48
  heavy_force_fragment_threshold: 0.8
  lock_ttl:
    async_ttl_secs: 4
    sync_ttl_secs: 12
    ttl_per_slice_ms: 25
    min_ttl_secs: 3
    max_ttl_secs: 60
"#,
        )
        .unwrap();

        let config = MountConfig::from_sources(empty_mount_args(Some(path.clone()), None)).unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(config.compact.min_slice_count, 7);
        assert_eq!(config.compact.min_fragment_ratio, 0.25);
        assert_eq!(config.compact.async_threshold, 64);
        assert_eq!(config.compact.sync_threshold, 128);
        assert_eq!(config.compact.interval, std::time::Duration::from_secs(2));
        assert_eq!(config.compact.max_chunks_per_run, 32);
        assert_eq!(config.compact.max_concurrent_tasks, 3);
        assert!(config.compact.light_enabled);
        assert_eq!(config.compact.light_threshold, 3);
        assert!(!config.compact.heavy_enabled);
        assert_eq!(config.compact.heavy_fragment_threshold, 0.4);
        assert_eq!(config.compact.heavy_slice_threshold, 48);
        assert_eq!(config.compact.heavy_force_fragment_threshold, 0.8);
        assert_eq!(config.compact.lock_ttl.async_ttl_secs, 4);
        assert_eq!(config.compact.lock_ttl.sync_ttl_secs, 12);
        assert_eq!(config.compact.lock_ttl.ttl_per_slice_ms, 25);
        assert_eq!(config.compact.lock_ttl.min_ttl_secs, 3);
        assert_eq!(config.compact.lock_ttl.max_ttl_secs, 60);
    }

    #[test]
    fn mount_config_parses_partial_compact_section_with_defaults() {
        let path = std::env::temp_dir().join(format!(
            "brewfs-partial-compact-config-{}-{}.yaml",
            std::process::id(),
            "parse"
        ));
        std::fs::write(
            &path,
            r#"
mount_point: /mnt/slayer
compact:
  interval:
    secs: 2
    nanos: 0
  async_threshold: 64
"#,
        )
        .unwrap();

        let config = MountConfig::from_sources(empty_mount_args(Some(path.clone()), None)).unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(config.compact.interval, std::time::Duration::from_secs(2));
        assert_eq!(config.compact.async_threshold, 64);
        assert_eq!(
            config.compact.min_slice_count,
            CompactConfig::default().min_slice_count
        );
        assert_eq!(
            config.compact.sync_threshold,
            CompactConfig::default().sync_threshold
        );
        assert_eq!(
            config.compact.lock_ttl.async_ttl_secs,
            CompactConfig::default().lock_ttl.async_ttl_secs
        );
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
    fn flat_cache_scope_is_stable_across_credential_rotation() {
        let data_dir = std::env::temp_dir().join("brewfs-flat-cache-scope-data");
        let mut first = empty_mount_args(None, Some(PathBuf::from("/mnt/first")));
        first.data_backend = Some(DataBackendKind::S3);
        first.s3_bucket = Some("volume-bucket".to_string());
        first.s3_region = Some("us-test-1".to_string());
        first.s3_endpoint =
            Some("https://alice:old-secret@objects.example.test?token=old".to_string());
        first.meta_url = Some(
            "postgres://alice:old-secret@metadata.example.test/brewfs?password=old".to_string(),
        );
        first.data_dir = Some(data_dir.clone());

        let mut second = first.clone();
        second.s3_endpoint =
            Some("https://bob:new-secret@objects.example.test?token=new".to_string());
        second.meta_url =
            Some("postgres://bob:new-secret@metadata.example.test/brewfs?password=new".to_string());

        let first = MountConfig::from_sources(first).unwrap();
        let second = MountConfig::from_sources(second).unwrap();
        assert_eq!(
            first.flat_volume_cache_scope().unwrap(),
            second.flat_volume_cache_scope().unwrap()
        );
    }

    #[test]
    fn flat_cache_scope_changes_with_either_volume_identity() {
        let mut base = empty_mount_args(None, Some(PathBuf::from("/mnt/base")));
        base.data_dir = Some(PathBuf::from("/var/lib/brewfs/objects-a"));
        base.meta_url = Some("postgres://metadata.example.test/volume-a".to_string());

        let mut other_metadata = base.clone();
        other_metadata.meta_url = Some("postgres://metadata.example.test/volume-b".to_string());
        let mut other_objects = base.clone();
        other_objects.data_dir = Some(PathBuf::from("/var/lib/brewfs/objects-b"));

        let base = MountConfig::from_sources(base).unwrap();
        let other_metadata = MountConfig::from_sources(other_metadata).unwrap();
        let other_objects = MountConfig::from_sources(other_objects).unwrap();
        let base_scope = base.flat_volume_cache_scope().unwrap();

        assert_ne!(
            base_scope,
            other_metadata.flat_volume_cache_scope().unwrap()
        );
        assert_ne!(base_scope, other_objects.flat_volume_cache_scope().unwrap());
    }

    #[test]
    fn flat_cache_scope_preserves_non_secret_metadata_query_identity() {
        let mut first = empty_mount_args(None, Some(PathBuf::from("/mnt/first")));
        first.data_dir = Some(PathBuf::from("/var/lib/brewfs/objects"));
        first.meta_url = Some(
            "postgres://metadata.example.test/brewfs?options=-csearch_path%3Dvolume_a&password=old"
                .to_string(),
        );

        let mut second = first.clone();
        second.meta_url = Some(
            "postgres://metadata.example.test/brewfs?password=new&options=-csearch_path%3Dvolume_b"
                .to_string(),
        );
        let mut reordered = first.clone();
        reordered.meta_url = Some(
            "postgres://metadata.example.test/brewfs?password=rotated&options=-csearch_path%3Dvolume_a"
                .to_string(),
        );

        let first = MountConfig::from_sources(first).unwrap();
        let second = MountConfig::from_sources(second).unwrap();
        let reordered = MountConfig::from_sources(reordered).unwrap();
        assert_eq!(
            first.flat_volume_cache_scope().unwrap(),
            reordered.flat_volume_cache_scope().unwrap()
        );
        assert_ne!(
            first.flat_volume_cache_scope().unwrap(),
            second.flat_volume_cache_scope().unwrap()
        );
    }

    #[test]
    fn implicit_s3_endpoint_never_reuses_persistent_cache_scope() {
        let mut args = empty_mount_args(None, Some(PathBuf::from("/mnt/first")));
        args.data_backend = Some(DataBackendKind::S3);
        args.s3_bucket = Some("volume-bucket".to_string());
        args.s3_region = Some("us-test-1".to_string());
        args.meta_url = Some("postgres://metadata.example.test/brewfs".to_string());

        let first = MountConfig::from_sources(args).unwrap();
        let second = first.clone();
        assert_ne!(
            first.flat_volume_cache_scope().unwrap(),
            second.flat_volume_cache_scope().unwrap()
        );
    }

    #[test]
    fn in_memory_metadata_never_reuses_persistent_cache_scope() {
        let first =
            MountConfig::from_sources(empty_mount_args(None, Some(PathBuf::from("/mnt/first"))))
                .unwrap();
        let second = first.clone();

        assert_ne!(
            first.flat_volume_cache_scope().unwrap(),
            second.flat_volume_cache_scope().unwrap()
        );
    }

    #[test]
    fn sqlite_file_memory_metadata_never_reuses_persistent_cache_scope() {
        let mut args = empty_mount_args(None, Some(PathBuf::from("/mnt/first")));
        args.meta_url = Some("sqlite:file::memory:?cache=shared".to_string());

        let first = MountConfig::from_sources(args).unwrap();
        let second = first.clone();
        assert_ne!(
            first.flat_volume_cache_scope().unwrap(),
            second.flat_volume_cache_scope().unwrap()
        );
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

    const NATIVE_P1_CONFIG: &str = r#"
volume:
  format: workspace-native-v2
  schema_version: 2
  native_control_version: 2
  chunk_size: 67108864
  block_size: 4194304
native_base:
  release_profile: p1-retained-trial
  format_version: 3
  namespace_mode: kv
  packing:
    payload_format: native-block-v1
    pack_target_bytes: 134217728
    codec: none
    multipart_part_target_bytes: 16777216
    retention_grouping: required
  reader:
    capture_protocol: single-writer-bounded-capture
    ordered_inode_commits: true
    capture_deadline_ms: 5000
    capture_max_retries: 3
    max_read_capture_bytes: 16777216
    max_plan_segments: 65536
    max_plan_bytes: 8388608
    demand_get_concurrency: 16
    request_deadline_ms: 30000
    inflight_encoded_bytes: 134217728
    inflight_decoded_bytes: 268435456
    physical_coalescing: false
    exact_range_response_required: true
  cache:
    scope: per-mount
    shared_service: false
    metadata_ram_bytes: 134217728
    decoded_data_bytes: 268435456
    encoded_disk_bytes: 4294967296
    trust_domain_isolation: true
  prefetch:
    enabled: false
    get_concurrency: 0
    metadata_only_ops_fetch_data: false
  commit:
    max_receipts: 128
    max_payload_bytes: 1048576
  durability:
    write_mode: upload-before-commit
    remote_verification: exact-readback
    require_atomic_create_only: true
    coordinator_profile: test-profile
  retention:
    published: forever
    published_gc: false
    read_retention_leases: false
    promotion: exact-object-retain-batches
    private_write_domain: workspace-lifetime
  cleanup:
    mode: disabled
    closed_private_domains_only: true
    require_close_certificate: true
    backend_mode: unversioned-only
    allow_legacy_slice_gc: false
    unknown_upload_policy: quarantine
    count_delete_markers_as_freed_bytes: false
  repack:
    automatic: false
    reclaim_published: false
    require_explicit_extra_bytes_budget: true
  limits:
    namespace_hard_bytes: 68719476736
    private_domain_soft_bytes: 8589934592
    private_domain_hard_bytes: 17179869184
    completion_reserve_bytes: 536870912
    on_hard_limit: reject-new-admissions-not-accepted-drain
"#;

    const NATIVE_PREFIX: &str = "mount_point: /mnt/native\nworkspace: 00000000-0000-0000-0000-000000000077\nmeta:\n  backend: redis";

    fn native_mount_config(prefix: &str) -> anyhow::Result<MountConfig> {
        mount_config_from_yaml(&format!("{prefix}\n{NATIVE_P1_CONFIG}"))
    }

    /// Parse a mount config whose native_base block is `native_block`, so a
    /// test can mutate exactly one shipped field and assert the gate refuses it.
    fn native_mount_config_from(native_block: &str) -> anyhow::Result<MountConfig> {
        mount_config_from_yaml(&format!("{NATIVE_PREFIX}\n{native_block}"))
    }

    fn mount_config_from_yaml(yaml: &str) -> anyhow::Result<MountConfig> {
        let path = std::env::temp_dir().join(format!(
            "brewfs-native-mount-config-{}-{}.yaml",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::write(&path, yaml).unwrap();
        let result = MountConfig::from_sources(empty_mount_args(Some(path.clone()), None));
        let _ = std::fs::remove_file(path);
        result
    }

    #[test]
    fn native_volume_requires_explicit_workspace_and_transactional_control_backend() {
        let missing_workspace =
            native_mount_config("mount_point: /mnt/native\nmeta:\n  backend: redis").unwrap_err();
        assert!(
            missing_workspace
                .to_string()
                .contains("workspace id is required")
        );

        let unsupported_backend = native_mount_config(
            "mount_point: /mnt/native\nworkspace: 00000000-0000-0000-0000-000000000077",
        )
        .unwrap_err();
        assert!(
            unsupported_backend
                .to_string()
                .contains("requires meta.backend=redis or tikv")
        );

        let config = native_mount_config(
            "mount_point: /mnt/native\nworkspace: 00000000-0000-0000-0000-000000000077\nmeta:\n  backend: redis",
        )
        .unwrap();
        assert_eq!(config.volume_format, VolumeFormat::WorkspaceNativeV2);
        assert!(matches!(config.meta_backend, MetaBackendKind::Redis));
    }

    #[test]
    fn native_volume_rejects_commit_before_upload_even_for_s3() {
        let error = native_mount_config(
            "mount_point: /mnt/native\nworkspace: 00000000-0000-0000-0000-000000000077\ndata:\n  backend: s3\nmeta:\n  backend: redis\ncache:\n  writeback_mode: commit_before_upload",
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("requires cache.writeback_mode=upload_before_commit")
        );
    }

    /// GATE-001: only the shipped P1 release profiles are admissible, the
    /// retained-trial profile must not claim private cleanup, and the P1
    /// capacity limits must stay coherent.  This admits the retained-trial
    /// profile only; it does not certify the full P1 private-cleanup delivery.
    #[test]
    fn native_release_profile_gate_admits_only_the_shipped_p1_profiles() {
        let unshipped = native_mount_config_from(&NATIVE_P1_CONFIG.replace(
            "release_profile: p1-retained-trial",
            "release_profile: p2-frozen-read",
        ))
        .unwrap_err();
        assert!(
            unshipped
                .to_string()
                .contains("unsupported native_base.release_profile p2-frozen-read"),
            "{unshipped}"
        );

        let cleanup_claimed = native_mount_config_from(
            &NATIVE_P1_CONFIG.replace("    mode: disabled", "    mode: apply"),
        )
        .unwrap_err();
        assert!(
            cleanup_claimed
                .to_string()
                .contains("p1-retained-trial requires native_base.cleanup.mode=disabled"),
            "{cleanup_claimed}"
        );

        let incoherent_limits = native_mount_config_from(&NATIVE_P1_CONFIG.replace(
            "    private_domain_hard_bytes: 17179869184",
            "    private_domain_hard_bytes: 137438953472",
        ))
        .unwrap_err();
        assert!(
            incoherent_limits
                .to_string()
                .contains("native_base.limits are invalid"),
            "{incoherent_limits}"
        );

        native_mount_config_from(NATIVE_P1_CONFIG).unwrap();
    }

    /// GATE-004: durability is an explicit verified contract.  A brand-name
    /// or no-op claim never admits a native volume.
    #[test]
    fn native_durability_profile_must_be_an_explicit_verified_contract() {
        for (from, to) in [
            (
                "write_mode: upload-before-commit",
                "write_mode: commit-before-upload",
            ),
            (
                "remote_verification: exact-readback",
                "remote_verification: trust-the-brand",
            ),
            (
                "require_atomic_create_only: true",
                "require_atomic_create_only: false",
            ),
            (
                "coordinator_profile: test-profile",
                "coordinator_profile: \"  \"",
            ),
        ] {
            let error = native_mount_config_from(&NATIVE_P1_CONFIG.replace(from, to))
                .expect_err(&format!("{from} must be refused"));
            assert!(
                error
                    .to_string()
                    .contains("native_base.durability does not satisfy the PR07 P1 contract"),
                "{from} produced {error}"
            );
        }
    }

    /// RET-021: the retired TTL / published_gc / read-retention-lease options
    /// fail closed instead of being silently ignored.
    #[test]
    fn native_retention_rejects_retired_ttl_gc_and_lease_options() {
        for (from, to) in [
            ("    published_gc: false", "    published_gc: true"),
            (
                "    read_retention_leases: false",
                "    read_retention_leases: true",
            ),
        ] {
            let error = native_mount_config_from(&NATIVE_P1_CONFIG.replace(from, to))
                .expect_err(&format!("{from} must be refused"));
            assert!(
                error
                    .to_string()
                    .contains("native_base.retention violates permanent published retention"),
                "{from} produced {error}"
            );
        }

        let retired = NATIVE_P1_CONFIG.replace(
            "  retention:\n    published: forever",
            "  retention:\n    retention_ttl_seconds: 60\n    published: forever",
        );
        let error = native_mount_config_from(&retired).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unknown field `retention_ttl_seconds`"),
            "{error}"
        );
    }

    /// CLN-024: purge / force / age-based cleanup rules are not part of this
    /// release, and there is no hidden switch that silently enables them.
    #[test]
    fn native_cleanup_rejects_purge_force_and_age_rules() {
        for mode in ["purge", "force", "by-age"] {
            let block = NATIVE_P1_CONFIG
                .replace(
                    "release_profile: p1-retained-trial",
                    "release_profile: p1-private-cleanup",
                )
                .replace("    mode: disabled", &format!("    mode: {mode}"));
            let error = native_mount_config_from(&block)
                .expect_err(&format!("cleanup mode {mode} must be refused"));
            assert!(
                error
                    .to_string()
                    .contains("native_base.cleanup violates the private-domain cleanup contract"),
                "cleanup mode {mode} produced {error}"
            );
        }

        let hidden = NATIVE_P1_CONFIG.replace(
            "    mode: disabled",
            "    mode: disabled\n    by_age_days: 30",
        );
        let error = native_mount_config_from(&hidden).unwrap_err();
        assert!(
            error.to_string().contains("unknown field `by_age_days`"),
            "{error}"
        );
    }

    #[test]
    fn native_volume_control_versions_and_unknown_fields_fail_closed() {
        let invalid_version =
            NATIVE_P1_CONFIG.replace("native_control_version: 2", "native_control_version: 1");
        let prefix = "mount_point: /mnt/native\nworkspace: 00000000-0000-0000-0000-000000000077\nmeta:\n  backend: redis";
        let path = std::env::temp_dir().join(format!(
            "brewfs-native-invalid-config-{}-{}.yaml",
            std::process::id(),
            uuid::Uuid::now_v7()
        ));
        std::fs::write(&path, format!("{prefix}\n{invalid_version}")).unwrap();
        let error =
            MountConfig::from_sources(empty_mount_args(Some(path.clone()), None)).unwrap_err();
        let _ = std::fs::remove_file(&path);
        assert!(error.to_string().contains("native_control_version=2"));

        let unknown = NATIVE_P1_CONFIG.replace(
            "  namespace_mode: kv",
            "  namespace_mode: kv\n  published_ttl_days: 30",
        );
        std::fs::write(&path, format!("{prefix}\n{unknown}")).unwrap();
        let error =
            MountConfig::from_sources(empty_mount_args(Some(path.clone()), None)).unwrap_err();
        let _ = std::fs::remove_file(path);
        assert!(
            error
                .to_string()
                .contains("unknown field `published_ttl_days`")
        );
    }
}
