mod cache;
mod path_trie;
pub mod session;

use crate::chunk::SliceDesc;
use crate::control::job::{GcJobResult, JobManager};
use crate::control::protocol::{
    ControlAclEntry, ControlDirectoryEntry, ControlFileKind, ControlPathMetadata, ControlRequest,
    ControlResponse, ControlTrashEntry, validate_acl_entries,
};
use crate::control::runtime::{InstanceRecord, RuntimeRegistry};
use crate::control::server::{ControlHandler, ControlServer};
use crate::meta::config::{CacheCapacity, CacheTtl};
use crate::meta::file_lock::{FileLockInfo, FileLockQuery, FileLockRange, FileLockType};
use crate::meta::layer::MetaLayer;
use crate::meta::store::{
    AclRule, DirEntry, FileAttr, MetaError, MetaStore, OpenFlags, SetAttrFlags, SetAttrRequest,
    StatFsSnapshot,
};
use crate::meta::stores::{CacheInvalidationEvent, EtcdMetaStore, EtcdWatchWorker, WatchConfig};
use crate::posix::NAME_MAX;
use crate::vfs::fs::FileType;
use crate::vfs::handles::DirHandle;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::stream;
use if_addrs::get_if_addrs;
use moka::future::Cache;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;
use std::{collections::HashSet, process};
use tokio::sync::{Mutex, mpsc};
use tracing::{Instrument, debug, info, trace, warn};
use uuid::Uuid;

use crate::vfs::extract_ino_and_chunk_index;
use cache::{InodeCache, OpenFileCache};
use chrono::Utc;
use hostname::get as get_hostname;

const CONTROL_ACL_XATTR_NAME: &str = "system.brewfs.acl";
const CONTROL_TRASH_XATTR_NAME: &str = "system.brewfs.trash";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ControlTrashMetadata {
    original_path: String,
    deleted_at: String,
}
use path_trie::PathTrie;
use session::{SessionInfo, SessionManager};

const ROOT_INODE: i64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenFileCacheConfig {
    /// Attribute reuse window for repeated non-append opens. `Duration::ZERO`
    /// disables this cache and preserves strict close-to-open refresh.
    pub ttl: Duration,
    /// Maximum number of recently opened inodes retained by the cache.
    pub capacity: u64,
}

impl OpenFileCacheConfig {
    fn enabled(&self) -> bool {
        !self.ttl.is_zero() && self.capacity > 0
    }
}

impl Default for OpenFileCacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::ZERO,
            capacity: 8192,
        }
    }
}

/// Configuration options for `MetaClient` that correspond to the core metadata
/// behaviours implemented by the Go `baseMeta`. Only a minimal subset of
/// fields is supported for now; additional knobs can be added as the Rust
/// client gains feature parity.
#[derive(Debug, Clone)]
pub struct MetaClientOptions {
    /// Optional mount point string used for diagnostics and session payloads.
    pub mount_point: Option<String>,
    /// Optional override for control-plane runtime registry directory.
    pub control_runtime_dir: Option<PathBuf>,
    /// Interval used by the background session heartbeat task.
    pub session_heartbeat: Duration,
    /// When true, metadata mutating operations return `MetaError::NotSupported`.
    pub read_only: bool,
    /// Disable background maintenance tasks (reserved for future use).
    pub no_background_jobs: bool,
    /// When true, lookups fall back to case-insensitive matching similar to
    /// JuiceFS `CaseInsensi`.
    pub case_insensitive: bool,
    /// Maximum symlink follow depth (POSIX SYMLOOP_MAX).
    pub max_symlinks: usize,
    /// Batch attribute prefetch configuration
    pub batch_prefetch: BatchPrefetchConfig,
    /// Opt-in JuiceFS-style cache scoped to recently opened files.
    pub open_file_cache: OpenFileCacheConfig,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetaClientMetricsSnapshot {
    pub stat_cache_hit: u64,
    pub stat_cache_miss: u64,
    pub stat_fresh_store_hit: u64,
    pub lookup_cache_hit: u64,
    pub lookup_cache_miss: u64,
    pub get_slices_cache_hit: u64,
    pub get_slices_cache_miss: u64,
    pub open_fresh_stat: u64,
    pub open_file_cache_hit: u64,
    pub open_file_cache_miss: u64,
    pub lookup_attr_fused_hit: u64,
    pub lookup_attr_fused_miss: u64,
    pub lookup_attr_fused_error: u64,
}

#[derive(Debug, Default)]
pub struct MetaClientMetrics {
    stat_cache_hit: AtomicU64,
    stat_cache_miss: AtomicU64,
    stat_fresh_store_hit: AtomicU64,
    lookup_cache_hit: AtomicU64,
    lookup_cache_miss: AtomicU64,
    get_slices_cache_hit: AtomicU64,
    get_slices_cache_miss: AtomicU64,
    open_fresh_stat: AtomicU64,
    open_file_cache_hit: AtomicU64,
    open_file_cache_miss: AtomicU64,
    lookup_attr_fused_hit: AtomicU64,
    lookup_attr_fused_miss: AtomicU64,
    lookup_attr_fused_error: AtomicU64,
}

impl MetaClientMetrics {
    pub fn snapshot(&self) -> MetaClientMetricsSnapshot {
        MetaClientMetricsSnapshot {
            stat_cache_hit: self.stat_cache_hit.load(Ordering::Relaxed),
            stat_cache_miss: self.stat_cache_miss.load(Ordering::Relaxed),
            stat_fresh_store_hit: self.stat_fresh_store_hit.load(Ordering::Relaxed),
            lookup_cache_hit: self.lookup_cache_hit.load(Ordering::Relaxed),
            lookup_cache_miss: self.lookup_cache_miss.load(Ordering::Relaxed),
            get_slices_cache_hit: self.get_slices_cache_hit.load(Ordering::Relaxed),
            get_slices_cache_miss: self.get_slices_cache_miss.load(Ordering::Relaxed),
            open_fresh_stat: self.open_fresh_stat.load(Ordering::Relaxed),
            open_file_cache_hit: self.open_file_cache_hit.load(Ordering::Relaxed),
            open_file_cache_miss: self.open_file_cache_miss.load(Ordering::Relaxed),
            lookup_attr_fused_hit: self.lookup_attr_fused_hit.load(Ordering::Relaxed),
            lookup_attr_fused_miss: self.lookup_attr_fused_miss.load(Ordering::Relaxed),
            lookup_attr_fused_error: self.lookup_attr_fused_error.load(Ordering::Relaxed),
        }
    }

    fn record_stat_cache_hit(&self) {
        self.stat_cache_hit.fetch_add(1, Ordering::Relaxed);
    }

    fn record_stat_cache_miss(&self) {
        self.stat_cache_miss.fetch_add(1, Ordering::Relaxed);
    }

    fn record_stat_fresh_store_hit(&self) {
        self.stat_fresh_store_hit.fetch_add(1, Ordering::Relaxed);
    }

    fn record_lookup_cache_hit(&self) {
        self.lookup_cache_hit.fetch_add(1, Ordering::Relaxed);
    }

    fn record_lookup_cache_miss(&self) {
        self.lookup_cache_miss.fetch_add(1, Ordering::Relaxed);
    }

    fn record_get_slices_cache_hit(&self) {
        self.get_slices_cache_hit.fetch_add(1, Ordering::Relaxed);
    }

    fn record_get_slices_cache_miss(&self) {
        self.get_slices_cache_miss.fetch_add(1, Ordering::Relaxed);
    }

    fn record_open_fresh_stat(&self) {
        self.open_fresh_stat.fetch_add(1, Ordering::Relaxed);
    }

    fn record_open_file_cache_hit(&self) {
        self.open_file_cache_hit.fetch_add(1, Ordering::Relaxed);
    }

    fn record_open_file_cache_miss(&self) {
        self.open_file_cache_miss.fetch_add(1, Ordering::Relaxed);
    }

    fn record_lookup_attr_fused_hit(&self) {
        self.lookup_attr_fused_hit.fetch_add(1, Ordering::Relaxed);
    }

    fn record_lookup_attr_fused_miss(&self) {
        self.lookup_attr_fused_miss.fetch_add(1, Ordering::Relaxed);
    }

    fn record_lookup_attr_fused_error(&self) {
        self.lookup_attr_fused_error.fetch_add(1, Ordering::Relaxed);
    }
}

/// Configuration for batch attribute prefetching during opendir
#[derive(Debug, Clone)]
pub struct BatchPrefetchConfig {
    /// Enable batch prefetching
    pub enabled: bool,
    /// Batch size for each query (default: 200)
    pub batch_size: usize,
    /// Maximum concurrent batches (default: 3)
    pub max_concurrency: usize,
}

impl Default for BatchPrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            batch_size: 200,
            max_concurrency: 3,
        }
    }
}

impl BatchPrefetchConfig {
    /// Create optimized config for traditional databases like Postgres/sqlite
    pub fn for_database() -> Self {
        Self {
            enabled: true,
            batch_size: 500,
            max_concurrency: 5,
        }
    }

    /// Create optimized config for Redis
    pub fn for_redis() -> Self {
        Self {
            enabled: true,
            batch_size: 300,
            max_concurrency: 10,
        }
    }

    /// Create optimized config for Etcd
    pub fn for_etcd() -> Self {
        Self {
            enabled: true,
            batch_size: 100, // Etcd Txn limited to ~128 ops
            max_concurrency: 3,
        }
    }

    /// Automatically select optimal config based on backend store name
    pub fn for_store(store_name: &str) -> Self {
        match store_name {
            name if name.contains("database") => Self::for_database(),
            name if name.contains("redis") => Self::for_redis(),
            name if name.contains("etcd") => Self::for_etcd(),
            _ => Self::default(),
        }
    }
}

impl Default for MetaClientOptions {
    fn default() -> Self {
        Self {
            mount_point: None,
            control_runtime_dir: None,
            session_heartbeat: DEFAULT_SESSION_HEARTBEAT,
            read_only: false,
            no_background_jobs: false,
            case_insensitive: false,
            max_symlinks: 40,
            batch_prefetch: BatchPrefetchConfig::default(),
            open_file_cache: OpenFileCacheConfig::default(),
        }
    }
}
const DEFAULT_SESSION_HEARTBEAT: Duration = Duration::from_secs(30);

/// Metadata client with intelligent caching
///
/// This client wraps a MetaStore and provides transparent caching for:
/// - Inode attributes (file metadata)
/// - Directory children (directory listings)
/// - Path-to-inode mappings (path resolution)
pub struct MetaClient<T: MetaStore + ?Sized> {
    store: Arc<T>,
    options: MetaClientOptions,
    root: AtomicI64,
    umounting: AtomicBool,
    inode_cache: Arc<InodeCache>,
    open_file_cache: Option<Arc<OpenFileCache>>,
    /// it's absolute path.
    /// Used for quick lookups and invalidation
    path_cache: Cache<String, i64>,
    /// Path trie for efficient prefix-based invalidation
    /// Replaces the old flat inode_to_paths mapping with O(depth) operations
    path_trie: Arc<PathTrie>,
    /// Reverse index: inode -> paths (for quick lookup during invalidation)
    /// Kept separate from trie for O(1) inode-to-paths lookup
    /// it's absolute path.
    inode_to_paths: Arc<DashMap<i64, Vec<String>>>,
    metrics: Arc<MetaClientMetrics>,

    /// Manages background session heartbeats when enabled by callers.
    session_manager: Arc<SessionManager<T>>,
    job_manager: Arc<JobManager>,
    control_plane: Mutex<Option<ControlPlaneState>>,

    /// Watch Worker for etcd cache invalidation (for now only used for etcd).
    /// TODO: Now that we use the watch worker to invalidate cache in real-time,
    /// may want to consider a more detailed data caching approach.
    #[allow(dead_code)]
    watch_worker: Option<Arc<EtcdWatchWorker>>,
}

struct ControlPlaneState {
    registry: RuntimeRegistry,
    record: InstanceRecord,
    server: ControlServer,
}

impl<T: MetaStore + ?Sized + 'static> MetaClient<T> {
    /// Creates a new MetaClient with cache configuration.
    ///
    /// # Arguments
    ///
    /// * `store` - The underlying metadata storage implementation
    /// * `capacity` - Cache capacity configuration (inode and path)
    /// * `ttl` - Cache TTL (time-to-live) configuration
    ///
    /// # Returns
    ///
    /// A new `MetaClient` instance with initialized caches
    #[allow(dead_code)]
    pub fn new(store: Arc<T>, capacity: CacheCapacity, ttl: CacheTtl) -> Arc<Self> {
        Self::with_options(store, capacity, ttl, MetaClientOptions::default())
    }

    /// Creates a new `MetaClient` with cache configuration and additional
    /// behavioural options ported from the JuiceFS `baseMeta` implementation.
    pub fn with_options(
        store: Arc<T>,
        capacity: CacheCapacity,
        ttl: CacheTtl,
        mut options: MetaClientOptions,
    ) -> Arc<Self> {
        debug!("MetaClient::with_options begin");
        let store_name = store.name();
        debug!(store_name, "MetaClient::with_options store ready");
        // Always use the predefined configuration values.
        // TODO: Make the values configurable.
        options.batch_prefetch = BatchPrefetchConfig::for_store(store_name);
        debug!(
            "store_name: {} Batch prefetch config: size={}, concurrency={}",
            store_name, options.batch_prefetch.batch_size, options.batch_prefetch.max_concurrency
        );

        // Detect if this is an etcd backend and start Watch Worker
        let watch_worker = if options.no_background_jobs {
            debug!("Watch Worker disabled: no_background_jobs=true");
            None
        } else if let Some(etcd_store) = store.as_any().downcast_ref::<EtcdMetaStore>() {
            let client = etcd_store.get_client();
            let config = WatchConfig::from_env_or_default();

            if !config.enabled {
                debug!("Watch Worker disabled: BREWFS_WATCH_ENABLED=false or default");
                None
            } else {
                let (mut worker, invalidation_rx) = EtcdWatchWorker::new(client, config);

                if let Err(e) = worker.start() {
                    warn!("Failed to start Watch Worker: {}", e);
                    None
                } else {
                    debug!("Watch Worker started for etcd backend");

                    let worker_arc = Arc::new(worker);
                    let rx = Arc::new(Mutex::new(invalidation_rx));
                    Some((worker_arc, rx))
                }
            }
        } else {
            None
        };
        debug!("MetaClient::with_options watch worker ready");

        let root_ino = store.root_ino();
        debug!(root_ino, "MetaClient::with_options root ready");

        let open_file_cache = options.open_file_cache.enabled().then(|| {
            Arc::new(OpenFileCache::new(
                options.open_file_cache.capacity,
                options.open_file_cache.ttl,
            ))
        });

        // Create MetaClient
        debug!("MetaClient::with_options cache structures begin");
        let client = Arc::new(Self {
            store: store.clone(),
            options,
            root: AtomicI64::new(root_ino),
            umounting: AtomicBool::new(false),
            inode_cache: Arc::new(InodeCache::new(capacity.inode as u64, ttl.inode_ttl)),
            open_file_cache,
            path_cache: Cache::builder()
                .max_capacity(capacity.path as u64)
                .time_to_live(ttl.path_ttl)
                .build(),
            path_trie: Arc::new(PathTrie::new()),
            inode_to_paths: Arc::new(DashMap::new()),
            metrics: Arc::new(MetaClientMetrics::default()),
            session_manager: Arc::new(SessionManager::new(store.clone())),
            job_manager: Arc::new(JobManager::default()),
            control_plane: Mutex::new(None),
            watch_worker: watch_worker.as_ref().map(|(w, _)| w.clone()),
        });
        debug!("MetaClient::with_options cache structures complete");

        // Start cache invalidation handler if Watch Worker is active
        if let Some((_, rx)) = watch_worker.clone() {
            let client_clone = client.clone();
            tokio::spawn(async move {
                client_clone.handle_cache_invalidation(rx).await;
            });
        }

        if !client.options.read_only && !client.options.no_background_jobs {
            let client_clone = client.clone();
            tokio::spawn(async move {
                if let Err(err) = client_clone.start_default_session().await {
                    warn!("MetaClient: failed to auto-start session: {err}");
                }
            });
        }

        debug!("MetaClient::with_options complete");
        client
    }

    /// Returns the current root inode honoured by the client. This mirrors
    /// `baseMeta.root` which may differ from the physical root after `chroot`.
    pub fn root(&self) -> i64 {
        self.root.load(Ordering::SeqCst)
    }

    /// Returns the client options used when constructing this instance.
    #[allow(dead_code)]
    pub fn options(&self) -> &MetaClientOptions {
        &self.options
    }

    /// Returns a clone of the underlying raw `MetaStore` handle.
    #[allow(dead_code)]
    pub fn store(&self) -> Arc<T> {
        self.store.clone()
    }

    pub fn metrics(&self) -> Arc<MetaClientMetrics> {
        self.metrics.clone()
    }

    async fn list_directory_for_control(&self, path: &str) -> ControlResponse {
        let path = match Self::normalize_control_path(path) {
            Ok(path) => path,
            Err(err) => return control_meta_error(err),
        };
        let (ino, attr) = match self.lookup_path_with_attr(&path).await {
            Ok(Some(result)) => result,
            Ok(None) => {
                return control_meta_error(MetaError::NotFound(self.root()));
            }
            Err(err) => return control_meta_error(err),
        };

        if attr.kind != FileType::Dir {
            return control_meta_error(MetaError::NotDirectory(ino));
        }

        let entries = match self.readdir(ino).await {
            Ok(entries) => entries,
            Err(err) => return control_meta_error(err),
        };
        let mut response_entries = Vec::with_capacity(entries.len());
        for entry in entries {
            let attr = match self.cached_stat(entry.ino).await {
                Ok(Some(attr)) => attr,
                Ok(None) => return control_meta_error(MetaError::NotFound(entry.ino)),
                Err(err) => return control_meta_error(err),
            };
            let has_acl = match self
                .store
                .get_xattr(entry.ino, CONTROL_ACL_XATTR_NAME)
                .await
            {
                Ok(Some(raw)) => !raw.is_empty(),
                Ok(None) | Err(MetaError::NotImplemented) => false,
                Err(err) => return control_meta_error(err),
            };
            response_entries.push(ControlDirectoryEntry {
                name: entry.name,
                inode: entry.ino,
                kind: ControlFileKind::from(attr.kind),
                size: attr.size,
                mode: attr.mode,
                uid: attr.uid,
                gid: attr.gid,
                mtime_ns: attr.mtime,
                has_acl,
            });
        }

        ControlResponse::DirectoryListing {
            path,
            entries: response_entries,
        }
    }

    async fn stat_path_for_control(&self, path: &str) -> ControlResponse {
        let path = match Self::normalize_control_path(path) {
            Ok(path) => path,
            Err(err) => return control_meta_error(err),
        };
        let metadata = match self.lookup_path_with_attr(&path).await {
            Ok(Some((_, attr))) => ControlPathMetadata::from(attr),
            Ok(None) => return control_meta_error(MetaError::NotFound(self.root())),
            Err(err) => return control_meta_error(err),
        };

        ControlResponse::PathMetadata { path, metadata }
    }

    async fn readlink_for_control(&self, path: &str) -> ControlResponse {
        let path = match Self::normalize_control_path(path) {
            Ok(path) => path,
            Err(err) => return control_meta_error(err),
        };
        let (ino, attr) = match self.lookup_path_with_attr(&path).await {
            Ok(Some(result)) => result,
            Ok(None) => return control_meta_error(MetaError::NotFound(self.root())),
            Err(err) => return control_meta_error(err),
        };
        if attr.kind != FileType::Symlink {
            return control_meta_error(MetaError::InvalidPath(format!(
                "path is not a symlink: {path}"
            )));
        }

        match self.read_symlink(ino).await {
            Ok(target) => ControlResponse::SymlinkTarget { path, target },
            Err(err) => control_meta_error(err),
        }
    }

    async fn resolve_acl_control_path(&self, path: &str) -> Result<(String, i64), MetaError> {
        let path = match Self::normalize_control_path(path) {
            Ok(path) => path,
            Err(err) => return Err(err),
        };
        match self.lookup_path_with_attr(&path).await {
            Ok(Some((ino, _))) => Ok((path, ino)),
            Ok(None) => Err(MetaError::NotFound(self.root())),
            Err(err) => Err(err),
        }
    }

    async fn get_acl_for_control(&self, path: &str) -> ControlResponse {
        let (path, ino) = match self.resolve_acl_control_path(path).await {
            Ok(result) => result,
            Err(err) => return control_meta_error(err),
        };

        match self.store.get_xattr(ino, CONTROL_ACL_XATTR_NAME).await {
            Ok(Some(raw)) => match serde_json::from_slice::<Vec<ControlAclEntry>>(&raw) {
                Ok(entries) => ControlResponse::Acl { path, entries },
                Err(err) => control_meta_error(MetaError::Internal(format!(
                    "invalid ACL metadata for {path}: {err}"
                ))),
            },
            Ok(None) => ControlResponse::Acl {
                path,
                entries: Vec::new(),
            },
            Err(err) => control_meta_error(err),
        }
    }

    async fn put_acl_for_control(
        &self,
        path: &str,
        entries: Vec<ControlAclEntry>,
    ) -> ControlResponse {
        if let Err(err) = self.ensure_writable() {
            return control_meta_error(err);
        }
        if let Err(message) = validate_acl_entries(&entries) {
            return control_invalid_request(message);
        }
        let (path, ino) = match self.resolve_acl_control_path(path).await {
            Ok(result) => result,
            Err(err) => return control_meta_error(err),
        };
        let raw = match serde_json::to_vec(&entries) {
            Ok(raw) => raw,
            Err(err) => return control_meta_error(MetaError::Internal(err.to_string())),
        };

        match self
            .store
            .set_xattr(ino, CONTROL_ACL_XATTR_NAME, &raw, 0)
            .await
        {
            Ok(()) => ControlResponse::Acl { path, entries },
            Err(err) => control_meta_error(err),
        }
    }

    async fn delete_acl_for_control(&self, path: &str) -> ControlResponse {
        if let Err(err) = self.ensure_writable() {
            return control_meta_error(err);
        }
        let (path, ino) = match self.resolve_acl_control_path(path).await {
            Ok(result) => result,
            Err(err) => return control_meta_error(err),
        };

        match self.store.remove_xattr(ino, CONTROL_ACL_XATTR_NAME).await {
            Ok(()) | Err(MetaError::NotFound(_)) => ControlResponse::AclDeleted { path },
            Err(err) => control_meta_error(err),
        }
    }

    async fn list_trash_for_control(&self) -> ControlResponse {
        let deleted_files = match self.store.get_deleted_files().await {
            Ok(inodes) => inodes,
            Err(err) => return control_meta_error(err),
        };

        let mut entries = Vec::with_capacity(deleted_files.len());
        for ino in deleted_files {
            let attr = match self.store.stat(ino).await {
                Ok(Some(attr)) => attr,
                Ok(None) => continue,
                Err(err) => return control_meta_error(err),
            };
            let metadata = match self.store.get_xattr(ino, CONTROL_TRASH_XATTR_NAME).await {
                Ok(Some(raw)) => match serde_json::from_slice::<ControlTrashMetadata>(&raw) {
                    Ok(metadata) => Some(metadata),
                    Err(err) => {
                        warn!(inode = ino, error = %err, "invalid trash metadata");
                        None
                    }
                },
                Ok(None) | Err(MetaError::NotImplemented) => None,
                Err(err) => return control_meta_error(err),
            };

            entries.push(ControlTrashEntry {
                id: ino.to_string(),
                original_path: metadata
                    .as_ref()
                    .map(|metadata| metadata.original_path.clone())
                    .unwrap_or_else(|| format!("inode:{ino}")),
                size: Some(attr.size),
                deleted_at: metadata.map(|metadata| metadata.deleted_at),
            });
        }
        entries.sort_by(|left, right| left.id.cmp(&right.id));

        ControlResponse::Trash { entries }
    }

    async fn restore_trash_for_control(&self, entry_id: &str) -> ControlResponse {
        if let Err(err) = self.ensure_writable() {
            return control_meta_error(err);
        }
        let ino = match Self::parse_trash_entry_id(entry_id) {
            Ok(ino) => ino,
            Err(err) => return control_meta_error(err),
        };
        if let Err(err) = self.ensure_trash_inode_for_control(ino).await {
            return control_meta_error(err);
        }
        let metadata = match self.load_trash_metadata(ino).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => return control_meta_error(MetaError::NotFound(ino)),
            Err(err) => return control_meta_error(err),
        };
        let (parent_path, name) = match Self::split_restored_child_path(&metadata.original_path) {
            Ok(parts) => parts,
            Err(err) => return control_meta_error(err),
        };
        let parent = match self.resolve_path(&parent_path).await {
            Ok(parent) => parent,
            Err(err) => return control_meta_error(err),
        };

        match self
            .store
            .restore_deleted_file(ino, parent, name.clone())
            .await
        {
            Ok(()) => {
                if let Err(err) = self.store.remove_xattr(ino, CONTROL_TRASH_XATTR_NAME).await {
                    warn!(inode = ino, error = ?err, "failed to remove restored trash metadata");
                }
                if let Ok(Some(attr)) = self.store.stat(ino).await {
                    self.inode_cache.insert_node(ino, attr, Some(parent)).await;
                }
                self.inode_cache.add_child(parent, name, ino).await;
                self.invalidate_parent_path(parent).await;
                ControlResponse::TrashRestored {
                    entry_id: entry_id.to_string(),
                }
            }
            Err(err) => control_meta_error(err),
        }
    }

    async fn delete_trash_for_control(&self, entry_id: &str) -> ControlResponse {
        if let Err(err) = self.ensure_writable() {
            return control_meta_error(err);
        }
        let ino = match Self::parse_trash_entry_id(entry_id) {
            Ok(ino) => ino,
            Err(err) => return control_meta_error(err),
        };
        if let Err(err) = self.ensure_trash_inode_for_control(ino).await {
            return control_meta_error(err);
        }
        match self.load_trash_metadata(ino).await {
            Ok(Some(_)) => {}
            Ok(None) => return control_meta_error(MetaError::NotFound(ino)),
            Err(err) => return control_meta_error(err),
        }

        match self.remove_file_metadata(ino).await {
            Ok(()) => {
                self.inode_cache.invalidate_inode(ino).await;
                ControlResponse::TrashDeleted {
                    entry_id: entry_id.to_string(),
                }
            }
            Err(err) => control_meta_error(err),
        }
    }

    async fn ensure_trash_inode_for_control(&self, ino: i64) -> Result<(), MetaError> {
        let deleted_files = self.store.get_deleted_files().await?;
        if deleted_files.contains(&ino) {
            Ok(())
        } else {
            Err(MetaError::NotFound(ino))
        }
    }

    async fn load_trash_metadata(
        &self,
        ino: i64,
    ) -> Result<Option<ControlTrashMetadata>, MetaError> {
        let Some(raw) = self.store.get_xattr(ino, CONTROL_TRASH_XATTR_NAME).await? else {
            return Ok(None);
        };
        serde_json::from_slice::<ControlTrashMetadata>(&raw)
            .map(Some)
            .map_err(|err| MetaError::Internal(format!("invalid trash metadata: {err}")))
    }

    fn parse_trash_entry_id(entry_id: &str) -> Result<i64, MetaError> {
        entry_id
            .parse::<i64>()
            .map_err(|_| MetaError::InvalidPath(format!("invalid trash entry id: {entry_id}")))
    }

    fn split_restored_child_path(path: &str) -> Result<(String, String), MetaError> {
        let path = Self::normalize_control_path(path)?;
        if path == "/" {
            return Err(MetaError::InvalidPath(path));
        }
        let Some((parent, name)) = path.rsplit_once('/') else {
            return Err(MetaError::InvalidPath(path));
        };
        Self::validate_entry_name(name)?;
        let parent = if parent.is_empty() { "/" } else { parent };
        Ok((parent.to_string(), name.to_string()))
    }

    async fn child_original_path_for_trash(&self, parent: i64, name: &str) -> String {
        let parent_path = MetaLayer::get_paths(self, parent)
            .await
            .ok()
            .and_then(|paths| paths.into_iter().next())
            .unwrap_or_else(|| "/".to_string());
        if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent_path.trim_end_matches('/'))
        }
    }

    async fn remember_trash_entry(&self, ino: i64, original_path: String) {
        let metadata = ControlTrashMetadata {
            original_path,
            deleted_at: Utc::now().to_rfc3339(),
        };
        let raw = match serde_json::to_vec(&metadata) {
            Ok(raw) => raw,
            Err(err) => {
                warn!(inode = ino, error = %err, "failed to encode trash metadata");
                return;
            }
        };
        if let Err(err) = self
            .store
            .set_xattr(ino, CONTROL_TRASH_XATTR_NAME, &raw, 0)
            .await
        {
            warn!(inode = ino, error = ?err, "failed to persist trash metadata");
        }
    }

    fn normalize_control_path(path: &str) -> Result<String, MetaError> {
        let path = path.trim();
        if path.is_empty() {
            return Ok("/".to_string());
        }
        if !path.starts_with('/') {
            return Err(MetaError::InvalidPath(path.to_string()));
        }

        let normalized = Self::normalize_path(path);
        if normalized.is_empty() {
            Ok("/".to_string())
        } else {
            Ok(normalized)
        }
    }

    pub(crate) async fn invalidate_chunk_slices(&self, chunk_id: u64) {
        let (inode, chunk_index) = extract_ino_and_chunk_index(chunk_id);
        self.inode_cache.invalidate_slices(inode, chunk_index).await;
        self.invalidate_open_file_cache_inode(inode).await;
    }

    /// Update the logical root inode. All subsequent metadata lookups treat
    /// `ROOT_INODE` as an alias for `inode`.
    #[allow(dead_code)]
    pub fn chroot(&self, inode: i64) {
        self.root.store(inode, Ordering::SeqCst);
    }

    fn check_root(&self, inode: i64) -> i64 {
        match inode {
            0 => ROOT_INODE,
            ROOT_INODE => self.root(),
            _ => inode,
        }
    }

    fn validate_entry_name(name: &str) -> Result<(), MetaError> {
        if name.is_empty() || name.len() > NAME_MAX {
            return Err(MetaError::InvalidFilename);
        }

        if name.contains('/') || name.contains('\0') {
            return Err(MetaError::InvalidFilename);
        }

        Ok(())
    }

    fn validate_symlink_target(target: &str) -> Result<(), MetaError> {
        // Symlink payload is not a directory entry name. It may legitimately be
        // much longer than NAME_MAX, including slash-separated paths.
        if target.contains('\0') {
            return Err(MetaError::InvalidFilename);
        }

        Ok(())
    }

    fn ensure_writable(&self) -> Result<(), MetaError> {
        if self.options.read_only {
            Err(MetaError::NotSupported(
                "metadata client configured read-only".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn ensure_background_jobs(&self) -> Result<(), MetaError> {
        if self.options.no_background_jobs {
            Err(MetaError::NotSupported(
                "background jobs disabled".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    #[allow(dead_code)]
    fn mark_umounting(&self) {
        self.umounting.store(true, Ordering::SeqCst);
    }

    fn clear_umounting(&self) {
        self.umounting.store(false, Ordering::SeqCst);
    }

    fn is_umounting(&self) -> bool {
        self.umounting.load(Ordering::SeqCst)
    }

    fn open_file_cache_eligible(read: bool, write: bool, append: bool) -> bool {
        (read || write) && !append
    }

    async fn invalidate_open_file_cache_inode(&self, inode: i64) {
        if let Some(cache) = &self.open_file_cache {
            cache.invalidate_inode(inode).await;
        }
    }

    async fn invalidate_open_file_cache_checked(&self, ino: i64) {
        let inode = self.check_root(ino);
        self.invalidate_open_file_cache_inode(inode).await;
    }

    /// Starts a background heartbeat session with the underlying store.
    ///
    /// Callers provide a `SessionInfo` struct containing session parameters understood by the backend;
    /// the client will register or update the session and then begin periodic heartbeats.
    pub async fn start_session(&self, session_info: SessionInfo) -> Result<(), MetaError> {
        if self.options.read_only {
            info!("MetaClient: read-only mode, skipping session start");
            return Ok(());
        }
        self.ensure_background_jobs()?;
        self.clear_umounting();
        let session_manager = self.session_manager.clone();
        session_manager.start(session_info).await
    }

    /// Builds a default session payload and starts the heartbeat task.
    pub async fn start_default_session(&self) -> Result<(), MetaError> {
        let payload = self.build_session_payload()?;
        self.start_session(payload).await
    }

    /// Stops the background heartbeat session if it was previously started.
    #[allow(dead_code)]
    pub async fn shutdown_session(&self) {
        self.mark_umounting();
        self.shutdown_control_plane().await;
        self.session_manager.shutdown().await;
    }

    pub async fn start_control_plane(self: &Arc<Self>) -> Result<(), MetaError> {
        self.ensure_background_jobs()?;

        let mount_point = self.options.mount_point.clone().ok_or_else(|| {
            MetaError::NotSupported("control plane requires mount point".to_string())
        })?;

        let mut control_plane = self.control_plane.lock().await;
        if control_plane.is_some() {
            return Ok(());
        }

        let registry = RuntimeRegistry::new(
            self.options
                .control_runtime_dir
                .clone()
                .unwrap_or_else(RuntimeRegistry::default_root),
        );
        let pid = process::id();
        let socket_path = registry.socket_path(pid);
        let record = InstanceRecord::new(pid, mount_point, socket_path.clone(), Utc::now());
        let server = ControlServer::bind(socket_path, Arc::clone(self))
            .await
            .map_err(|err| MetaError::Internal(err.to_string()))?;

        registry
            .write_record(&record)
            .await
            .map_err(|err| MetaError::Internal(err.to_string()))?;

        *control_plane = Some(ControlPlaneState {
            registry,
            record,
            server,
        });

        Ok(())
    }

    pub async fn shutdown_runtime(&self) {
        self.shutdown_control_plane().await;
        self.session_manager.shutdown().await;
    }

    async fn shutdown_control_plane(&self) {
        let state = self.control_plane.lock().await.take();

        if let Some(state) = state {
            let _ = state.registry.remove_record(state.record.pid).await;
            drop(state.server);
        }
    }

    async fn enqueue_gc_job(self: &Arc<Self>, dry_run: bool) -> String {
        let job_id = self.job_manager.create_gc_job(dry_run).await;
        let jobs = Arc::clone(&self.job_manager);
        let job_id_clone = job_id.clone();

        tokio::spawn(async move {
            let _ = jobs.mark_running(&job_id_clone).await;
            let _ = jobs
                .finish(
                    &job_id_clone,
                    GcJobResult {
                        dry_run,
                        orphan_slice_count: 0,
                        orphan_object_count: 0,
                        deleted_object_count: 0,
                        error_count: 0,
                        detail: Some(
                            "gc execution is not implemented yet; control plane only".to_string(),
                        ),
                    },
                )
                .await;
        });

        job_id
    }

    /// Get the current session ID if a session is active.
    #[allow(dead_code)]
    pub async fn session_id(&self) -> Option<Uuid> {
        *self.session_manager.session_id.read().await
    }

    /// Get the current process ID.
    #[allow(dead_code)]
    pub fn process_id(&self) -> u32 {
        std::process::id()
    }

    /// Finds and removes stale sessions using store-provided helpers.
    ///
    /// Returns the number of sessions successfully cleaned. Failures are
    /// logged and skipped to keep the maintenance loop best-effort.
    fn build_session_payload(&self) -> Result<SessionInfo, MetaError> {
        let host_name = get_hostname()
            .map_err(MetaError::from)?
            .into_string()
            .unwrap_or_else(|_| "unknown-host".to_string());
        let ip_addrs = Self::collect_local_ip_addrs()?;

        Ok(SessionInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            host_name,
            ip_addrs,
            mount_point: self.options.mount_point.clone(),
            mount_time: Utc::now(),
            process_id: process::id(),
            created_at: Utc::now(),
        })
    }

    fn collect_local_ip_addrs() -> Result<Vec<String>, MetaError> {
        let interfaces = get_if_addrs().map_err(MetaError::from)?;
        let mut addrs = HashSet::new();

        for iface in interfaces {
            let ip = iface.ip();
            if !ip.is_loopback() {
                addrs.insert(ip.to_string());
            }
        }

        let mut addrs: Vec<String> = addrs.into_iter().collect();
        addrs.sort();
        Ok(addrs)
    }
    /// Handle cache invalidation events from Watch Worker
    ///
    /// This runs in a background task and processes events from etcd Watch Worker
    /// to maintain cache consistency across multiple clients.
    async fn handle_cache_invalidation(
        self: Arc<Self>,
        rx: Arc<Mutex<mpsc::Receiver<CacheInvalidationEvent>>>,
    ) {
        let mut rx = rx.lock().await;

        info!("Cache invalidation handler started");

        while let Some(event) = rx.recv().await {
            if self.is_umounting() {
                break;
            }
            match event {
                CacheInvalidationEvent::InvalidateInode(ino) => {
                    self.inode_cache.invalidate_inode(ino).await;

                    if let Some(paths_entry) = self.inode_to_paths.get(&ino) {
                        for path in paths_entry.value() {
                            self.path_cache.invalidate(path).await;
                        }
                    }
                }

                CacheInvalidationEvent::InvalidateParentChildren(parent_ino) => {
                    self.invalidate_parent_path(parent_ino).await;
                }
                CacheInvalidationEvent::AddChild {
                    parent_ino,
                    name,
                    child_ino,
                } => {
                    self.inode_cache
                        .add_child(parent_ino, name, child_ino)
                        .await;
                    self.invalidate_parent_path(parent_ino).await;
                }

                CacheInvalidationEvent::RemoveChild { parent_ino, name } => {
                    self.inode_cache.remove_child(parent_ino, &name).await;
                    self.invalidate_parent_path(parent_ino).await;
                }

                CacheInvalidationEvent::UpdateInodeMetadata { ino, metadata } => {
                    self.inode_cache.update_metadata(ino, metadata).await;
                }
            }
        }

        info!("Cache invalidation handler stopped (channel closed)");
    }

    /// Preloads commonly accessed cache entries after operations that might benefit from caching.
    /// This helps maintain cache consistency and improves performance for subsequent operations.
    async fn preload_cache_entries(&self, inodes: &[i64]) -> Result<(), MetaError> {
        for &ino in inodes {
            // Preload inode attributes if not already cached
            if self.inode_cache.get_attr(ino).await.is_none()
                && let Ok(Some(attr)) = self.store.stat(ino).await
            {
                // For single-link files, we can cache the parent relationship
                // For multi-link files, parent is stored in LinkParentMeta
                let cache_parent = if attr.nlink <= 1 {
                    // Try to find parent from ContentMeta (best effort)
                    // This is a performance optimization - not critical for correctness
                    None // We'll let it be resolved lazily
                } else {
                    None // Multi-link files don't cache parent
                };

                self.inode_cache.insert_node(ino, attr, cache_parent).await;
            }
        }
        Ok(())
    }

    /// Intelligently invalidates path cache entries for a parent directory.
    ///
    /// # Strategy (Trie-based approach)
    ///
    /// When a modification occurs (create/delete/rename), we:
    /// 1. Find all paths that resolve to this parent inode (O(1) using reverse index)
    /// 2. For each path, remove its entire subtree from the trie (O(depth))
    /// 3. Invalidate all affected paths from the path cache
    /// 4. Clean up the reverse index for all removed paths
    ///
    /// # Arguments
    ///
    /// * `parent_ino` - The parent directory inode that was modified
    async fn invalidate_parent_path(&self, parent_ino: i64) {
        let parent_ino = self.check_root(parent_ino);
        // Step 1: Get all paths that resolve to this parent inode (O(1))
        if let Some(entry) = self.inode_to_paths.get(&parent_ino) {
            let paths = entry.value().clone();
            drop(entry);

            // Step 2: Remove each path and its descendants from the trie
            for parent_path in &paths {
                // Remove from trie - this automatically removes all child paths
                // E.g., removing "/a/b" also removes "/a/b/c", "/a/b/d", etc.
                // Returns Vec<(String, Vec<i64>)> with path and inodes BEFORE deletion
                let removed_info = self.path_trie.remove_by_prefix(parent_path).await;

                // Step 3: Invalidate all removed paths from Moka cache and clean up reverse index
                for (removed_path, inodes) in &removed_info {
                    // Invalidate from path cache
                    self.path_cache.invalidate(removed_path).await;

                    // Step 4: Clean up reverse index for all removed paths
                    // This fixes the memory leak where child path entries weren't cleaned up
                    for ino in inodes {
                        // Remove this specific path from the inode's path list
                        if let Some(mut entry) = self.inode_to_paths.get_mut(ino) {
                            entry.retain(|p| p != removed_path);
                            // If no more paths point to this inode, remove the entry
                            if entry.is_empty() {
                                drop(entry);
                                self.inode_to_paths.remove(ino);
                            }
                        }
                    }
                }
            }

            // Clean up the parent's reverse index entry
            self.inode_to_paths.remove(&parent_ino);
        } else {
            // Fallback: if we don't have reverse mapping, invalidate all
            // This maintains correctness even if the reverse mapping is incomplete
            self.path_cache.invalidate_all();
        }
    }

    /// Normalizes a path by resolving `.` and `..` components.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to normalize (can be absolute or relative)
    ///
    /// # Returns
    ///
    /// A normalized absolute path with `.` and `..` resolved.
    fn normalize_path(path: &str) -> String {
        let mut components: Vec<&str> = Vec::new();
        let is_absolute = path.starts_with('/');

        for part in path.split('/') {
            match part {
                "" | "." => continue, // Skip empty and current directory
                ".." => {
                    if !(components.is_empty()) {
                        components.pop();
                    }
                }
                _ => components.push(part),
            }
        }

        if is_absolute {
            if components.is_empty() {
                "/".to_string()
            } else {
                format!("/{}", components.join("/"))
            }
        } else {
            components.join("/")
        }
    }

    /// Resolves a file path to its corresponding inode number (**lstat semantics**).
    ///
    /// This method walks through the path components from root to leaf,
    /// utilizing both inode cache and path cache for performance optimization.
    /// When encountering a symlink in an intermediate path component,
    /// it follows the symlink to resolve the target path.
    ///
    /// # Arguments
    ///
    /// * `path` - The absolute path to resolve (must start with '/')
    ///
    /// # Returns
    ///
    /// * `Ok(i64)` - The inode number of the file/directory/symlink
    /// * `Err(MetaError::NotFound)` - If any component in the path doesn't exist
    /// * `Err(MetaError::...)` - Other metadata errors
    #[tracing::instrument(level = "trace", skip(self), fields(path))]
    pub async fn resolve_path(&self, path: &str) -> Result<i64, MetaError> {
        self.resolve_path_impl(path, false).await
    }
    /// Resolves a file path to its corresponding inode number (**stat semantics**).
    ///
    /// This method is similar to [`resolve_path`], but follows all symlinks
    /// including the final path component.
    #[tracing::instrument(level = "trace", skip(self), fields(path))]
    pub async fn resolve_path_follow(&self, path: &str) -> Result<i64, MetaError> {
        self.resolve_path_impl(path, true).await
    }

    /// Internal implementation of path resolution with configurable symlink behavior.
    ///
    /// # Arguments
    ///
    /// * `path` - The absolute path to resolve
    /// * `follow_final` - If true, follow stat semantics, false for lstat semantics
    #[tracing::instrument(level = "trace", skip(self), fields(path, follow_final))]
    async fn resolve_path_impl(&self, path: &str, follow_final: bool) -> Result<i64, MetaError> {
        trace!("MetaClient: Resolving path: {}", path);

        let root = self.root();
        if path == "/" {
            return Ok(root);
        }

        if let Some(ino) = self.path_cache.get(path).await {
            if !follow_final {
                trace!("MetaClient: Path cache HIT for '{}' -> inode {}", path, ino);
                return Ok(ino);
            }

            match self.cached_stat(ino).await {
                Ok(Some(attr)) if attr.kind == FileType::Symlink => {
                    info!(
                        "MetaClient: Path cache HIT for '{}' -> symlink inode {}, need to follow",
                        path, ino
                    );
                }

                _ => {
                    trace!("MetaClient: Path cache HIT for '{}' -> inode {}", path, ino);
                    return Ok(ino);
                }
            }
        }

        trace!("MetaClient: Path cache MISS for '{}'", path);

        let mut current_path = path.to_string();
        let mut symlink_depth = 0;
        let max_symlinks = self.options.max_symlinks;

        loop {
            if symlink_depth >= max_symlinks {
                return Err(MetaError::TooManySymlinks);
            }
            let segments: Vec<&str> = current_path
                .trim_start_matches('/')
                .split('/')
                .filter(|s| !s.is_empty())
                .collect();

            let segment_count = segments.len();
            let mut current_ino = root;
            let mut symlink_encountered = false;

            for (idx, seg) in segments.iter().enumerate() {
                // POSIX: check parent is a directory before lookup
                if let Ok(Some(attr)) = self.cached_stat(current_ino).await
                    && attr.kind != FileType::Dir
                {
                    return Err(MetaError::NotDirectory(current_ino));
                }

                let child_ino = self
                    .cached_lookup(current_ino, seg)
                    .await?
                    .ok_or_else(|| MetaError::NotFound(current_ino))?;

                let is_tail = idx == segment_count - 1;
                let should_follow = !is_tail || follow_final;

                // Follow symlinks based on position and follow_final flag
                if should_follow
                    && let Ok(Some(attr)) = self.cached_stat(child_ino).await
                    && attr.kind == FileType::Symlink
                {
                    info!(
                        "MetaClient: Following symlink at segment {} (inode {})",
                        seg, child_ino
                    );

                    let target = self.store.read_symlink(child_ino).await?;
                    let remaining = segments[idx + 1..].join("/");

                    // Resolve absolute vs relative target
                    let resolved_target = if target.starts_with('/') {
                        target
                    } else {
                        let parent_path = MetaLayer::get_paths(self, current_ino)
                            .await?
                            .into_iter()
                            .next()
                            .unwrap_or_else(|| "/".to_string());
                        if parent_path == "/" {
                            format!("/{}", target)
                        } else {
                            format!("{}/{}", parent_path, target)
                        }
                    };

                    current_path = if remaining.is_empty() {
                        Self::normalize_path(&resolved_target)
                    } else {
                        Self::normalize_path(&format!("{}/{}", resolved_target, remaining))
                    };

                    symlink_encountered = true;
                    symlink_depth += 1;
                    break;
                }

                current_ino = child_ino;
            }

            // If no symlink was encountered, we're done
            if !symlink_encountered {
                self.path_cache.insert(path.to_string(), current_ino).await;
                self.path_trie.insert(path, current_ino).await;
                self.inode_to_paths
                    .entry(current_ino)
                    .or_default()
                    .push(path.to_string());

                return Ok(current_ino);
            }
        }
    }

    /// Retrieves file attributes (metadata) for a given inode with caching.
    ///
    /// This is a cache-aware wrapper around the underlying store's stat operation.
    ///
    /// # Arguments
    ///
    /// * `ino` - The inode number to query
    ///
    /// # Returns
    ///
    /// * `Ok(Some(FileAttr))` - The file attributes if the inode exists
    /// * `Ok(None)` - If the inode doesn't exist
    /// * `Err(MetaError)` - On storage errors
    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn cached_stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        let inode = self.check_root(ino);

        if let Some(attr) = self.inode_cache.get_attr(inode).await {
            trace!("MetaClient: Inode cache HIT for inode {}", inode);
            self.metrics.record_stat_cache_hit();
            return Ok(Some(attr));
        }

        trace!("MetaClient: Inode cache MISS for inode {}", inode);
        self.metrics.record_stat_cache_miss();

        let attr = self.store.stat(inode).await?;

        if let Some(ref a) = attr {
            self.inode_cache.insert_node(inode, a.clone(), None).await;
        }

        Ok(attr)
    }

    /// Looks up a child entry by name within a parent directory with caching.
    ///
    /// This is a cache-aware wrapper around the underlying store's lookup operation.
    ///
    /// # Arguments
    ///
    /// * `parent` - The inode number of the parent directory
    /// * `name` - The name of the child entry to look up
    ///
    /// # Returns
    ///
    /// * `Ok(Some(i64))` - The inode number of the child entry if found
    /// * `Ok(None)` - If no entry with the given name exists in the parent
    /// * `Err(MetaError)` - On storage errors
    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn cached_lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        let parent = self.check_root(parent);

        if let Some(result) = self.inode_cache.lookup_if_loaded(parent, name).await {
            match result {
                Some(ino) => {
                    trace!(
                        "MetaClient: lookup HIT ({}, '{}') -> inode {}",
                        parent, name, ino
                    );
                    self.metrics.record_lookup_cache_hit();
                    return Ok(Some(ino));
                }
                None if !self.options.case_insensitive => {
                    trace!("MetaClient: complete lookup MISS ({}, '{}')", parent, name);
                    self.metrics.record_lookup_cache_hit();
                    return Ok(None);
                }
                None => {
                    trace!(
                        "MetaClient: complete lookup MISS ({}, '{}'), checking case-insensitive fallback",
                        parent, name
                    );
                }
            }
        }

        trace!("MetaClient: lookup MISS ({}, '{}')", parent, name);
        self.metrics.record_lookup_cache_miss();

        let result = self.store.lookup(parent, name).await?;

        if let Some(ino) = result {
            if let Ok(Some(attr)) = self.store.stat(ino).await {
                self.cache_lookup_attr(parent, name, ino, attr).await;
            } else {
                self.inode_cache
                    .add_child(parent, name.to_string(), ino)
                    .await;
            }
            Ok(Some(ino))
        } else if self.options.case_insensitive {
            self.resolve_case(parent, name).await
        } else {
            Ok(None)
        }
    }

    async fn cache_lookup_attr(&self, parent: i64, name: &str, ino: i64, attr: FileAttr) {
        let cache_parent = matches!(attr.kind, FileType::Dir).then_some(parent);

        self.inode_cache.insert_node(ino, attr, cache_parent).await;
        self.inode_cache
            .add_child(parent, name.to_string(), ino)
            .await;
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn cached_lookup_with_attr(
        &self,
        parent: i64,
        name: &str,
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        let parent = self.check_root(parent);

        if let Some(result) = self.inode_cache.lookup_if_loaded(parent, name).await {
            match result {
                Some(ino) => {
                    self.metrics.record_lookup_cache_hit();
                    if let Some(attr) = self.inode_cache.get_attr(ino).await {
                        self.metrics.record_lookup_attr_fused_hit();
                        return Ok(Some((ino, attr)));
                    }
                    self.metrics.record_lookup_attr_fused_miss();
                    let attr = match self.store.stat(ino).await {
                        Ok(Some(attr)) => attr,
                        Ok(None) => return Err(MetaError::NotFound(ino)),
                        Err(err) => {
                            self.metrics.record_lookup_attr_fused_error();
                            return Err(err);
                        }
                    };
                    self.cache_lookup_attr(parent, name, ino, attr.clone())
                        .await;
                    return Ok(Some((ino, attr)));
                }
                None if !self.options.case_insensitive => {
                    self.metrics.record_lookup_cache_hit();
                    self.metrics.record_lookup_attr_fused_hit();
                    return Ok(None);
                }
                None => {
                    self.metrics.record_lookup_cache_hit();
                }
            }
        }

        self.metrics.record_lookup_cache_miss();
        self.metrics.record_lookup_attr_fused_miss();

        let result = match self.store.lookup_with_attr(parent, name).await {
            Ok(result) => result,
            Err(err) => {
                self.metrics.record_lookup_attr_fused_error();
                return Err(err);
            }
        };

        if let Some((ino, attr)) = result {
            self.cache_lookup_attr(parent, name, ino, attr.clone())
                .await;
            Ok(Some((ino, attr)))
        } else if self.options.case_insensitive {
            let Some(ino) = self.resolve_case(parent, name).await? else {
                return Ok(None);
            };
            let attr = self
                .cached_stat(ino)
                .await?
                .ok_or(MetaError::NotFound(ino))?;
            self.cache_lookup_attr(parent, name, ino, attr.clone())
                .await;
            Ok(Some((ino, attr)))
        } else {
            Ok(None)
        }
    }

    async fn cached_lookup_required(&self, parent: i64, name: &str) -> Result<i64, MetaError> {
        let parent = self.check_root(parent);

        self.cached_lookup(parent, name)
            .await?
            .ok_or(MetaError::NotFound(parent))
    }

    async fn ensure_directory_exists(&self, ino: i64) -> Result<(), MetaError> {
        let ino = self.check_root(ino);
        let Some(parent_attr) = self.cached_stat(ino).await? else {
            return Err(MetaError::NotFound(ino));
        };

        if parent_attr.kind != FileType::Dir {
            return Err(MetaError::NotDirectory(ino));
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn resolve_case(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        let entries = self.store.readdir(parent).await?;
        for entry in entries {
            if entry.name.eq_ignore_ascii_case(name) {
                if let Ok(Some(attr)) = self.store.stat(entry.ino).await {
                    let cache_parent = matches!(attr.kind, FileType::Dir).then_some(parent);

                    self.inode_cache
                        .insert_node(entry.ino, attr, cache_parent)
                        .await;
                }
                self.inode_cache
                    .add_child(parent, entry.name.clone(), entry.ino)
                    .await;
                return Ok(Some(entry.ino));
            }
        }
        Ok(None)
    }

    /// Batch prefetch attributes for directory entries in background
    ///
    /// This method starts a background task that:
    /// 1. Collects inodes that need prefetching
    /// 2. Splits them into batches
    /// 3. Queries each batch concurrently
    /// 4. Inserts results into cache
    ///
    /// Returns a tuple of (done_flag, task_handle)
    pub fn spawn_batch_prefetch(
        &self,
        ino: i64,
        entries: &[DirEntry],
    ) -> (Arc<AtomicBool>, tokio::task::JoinHandle<()>) {
        let config = self.options.batch_prefetch.clone();

        if !config.enabled || entries.is_empty() {
            let done = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let handle = tokio::spawn(async {});
            return (done, handle);
        }

        // Collect inodes that need to be fetched
        let inodes_to_fetch: Vec<i64> = entries.iter().map(|e| e.ino).collect();

        let batch_size = config.batch_size;
        let max_concurrency = config.max_concurrency;
        let done_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done_flag_clone = Arc::clone(&done_flag);

        let store = Arc::clone(&self.store);
        let inode_cache = Arc::clone(&self.inode_cache);
        let parent_ino = ino; // Capture parent directory inode for the async block

        let task = tokio::spawn(async move {
            let start = std::time::Instant::now();
            debug!(
                "Starting batch prefetch for directory inode {}: {} entries, batch_size={}, max_concurrency={}",
                parent_ino,
                inodes_to_fetch.len(),
                batch_size,
                max_concurrency
            );

            // Split into batches
            let chunks: Vec<Vec<i64>> = inodes_to_fetch
                .chunks(batch_size)
                .map(|chunk| chunk.to_vec())
                .collect();

            let total_batches = chunks.len();

            // Process batches with controlled concurrency using stream
            // This is a single-layer spawn - abort will properly cancel all work
            use futures::stream::StreamExt;
            stream::iter(chunks.into_iter().enumerate())
                .map(|(batch_idx, chunk)| {
                    let store = Arc::clone(&store);
                    let inode_cache = Arc::clone(&inode_cache);
                    async move {
                        let batch_start = std::time::Instant::now();
                        match store.batch_stat(&chunk).await {
                            Ok(attrs) => {
                                let mut cached_count = 0;
                                // Insert results into cache
                                for (child_ino, attr_opt) in chunk.iter().zip(attrs.iter()) {
                                    if let Some(attr) = attr_opt {
                                        inode_cache
                                            .insert_node(*child_ino, attr.clone(), None)
                                            .await;
                                        cached_count += 1;
                                    }
                                }
                                debug!(
                                    "Batch {}/{} completed: {} inodes queried, {} cached in {:?}",
                                    batch_idx + 1,
                                    total_batches,
                                    chunk.len(),
                                    cached_count,
                                    batch_start.elapsed()
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Batch {}/{} failed: {} - continuing with remaining batches",
                                    batch_idx + 1,
                                    total_batches,
                                    e
                                );
                            }
                        }
                    }
                })
                .buffer_unordered(max_concurrency)
                .collect::<Vec<_>>()
                .await;

            debug!(
                "Prefetch completed for directory inode {}: {} total inodes in {:?}",
                parent_ino,
                inodes_to_fetch.len(),
                start.elapsed()
            );

            done_flag_clone.store(true, Ordering::Release);
        });

        (done_flag, task)
    }
}

#[async_trait]
impl<T: MetaStore + ?Sized + 'static> ControlHandler for Arc<MetaClient<T>> {
    async fn handle(&self, request: ControlRequest) -> ControlResponse {
        match request {
            ControlRequest::Ping => ControlResponse::Pong,
            ControlRequest::GetInfo => {
                let control_plane = self.control_plane.lock().await;

                if let Some(state) = control_plane.as_ref() {
                    ControlResponse::Info {
                        pid: state.record.pid,
                        mount_point: state.record.mount_point.clone(),
                        started_at: state.record.started_at.timestamp_millis(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                        meta_backend: self.store.name().to_string(),
                        capabilities: self.store.capabilities(),
                    }
                } else {
                    ControlResponse::Error {
                        code: "control_plane_not_running".to_string(),
                        message: "control plane is not running".to_string(),
                    }
                }
            }
            ControlRequest::RunGc { dry_run } => {
                let job_id = self.enqueue_gc_job(dry_run).await;
                ControlResponse::Accepted { job_id }
            }
            ControlRequest::GetJob { job_id } => match self.job_manager.get(&job_id).await {
                Some(job) => job.into(),
                None => ControlResponse::Error {
                    code: "job_not_found".to_string(),
                    message: format!("job not found: {job_id}"),
                },
            },
            ControlRequest::ListDirectory { path } => self.list_directory_for_control(&path).await,
            ControlRequest::StatPath { path } => self.stat_path_for_control(&path).await,
            ControlRequest::ReadLink { path } => self.readlink_for_control(&path).await,
            ControlRequest::GetAcl { path } => self.get_acl_for_control(&path).await,
            ControlRequest::PutAcl { path, entries } => {
                self.put_acl_for_control(&path, entries).await
            }
            ControlRequest::DeleteAcl { path } => self.delete_acl_for_control(&path).await,
            ControlRequest::ListTrash => self.list_trash_for_control().await,
            ControlRequest::RestoreTrashEntry { entry_id } => {
                self.restore_trash_for_control(&entry_id).await
            }
            ControlRequest::DeleteTrashEntry { entry_id } => {
                self.delete_trash_for_control(&entry_id).await
            }
        }
    }
}

fn control_invalid_request(message: impl Into<String>) -> ControlResponse {
    ControlResponse::Error {
        code: "invalid_request".to_string(),
        message: message.into(),
    }
}

fn control_meta_error(err: MetaError) -> ControlResponse {
    let code = match &err {
        MetaError::NotFound(_) => "not_found",
        MetaError::NotDirectory(_) => "not_directory",
        MetaError::AlreadyExists { .. } => "already_exists",
        MetaError::InvalidPath(_) | MetaError::InvalidFilename => "invalid_path",
        MetaError::NotSupported(_) | MetaError::NotImplemented => "unsupported",
        _ => "meta_error",
    };
    ControlResponse::Error {
        code: code.to_string(),
        message: err.to_string(),
    }
}

#[async_trait]
impl<T: MetaStore + ?Sized + 'static> MetaLayer for MetaClient<T> {
    fn name(&self) -> &'static str {
        self.store.name()
    }

    fn metrics(&self) -> Option<Arc<MetaClientMetrics>> {
        Some(self.metrics())
    }

    fn root_ino(&self) -> i64 {
        self.root()
    }

    fn chroot(&self, inode: i64) {
        MetaClient::chroot(self, inode);
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn initialize(&self) -> Result<(), MetaError> {
        self.store.initialize().await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn stat_fs(&self) -> Result<StatFsSnapshot, MetaError> {
        self.store.stat_fs().await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        self.cached_stat(ino).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn stat_fresh(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        let inode = self.check_root(ino);

        self.metrics.record_open_fresh_stat();
        let attr = self.store.stat(inode).await?;
        match &attr {
            Some(a) => {
                self.metrics.record_stat_fresh_store_hit();
                if !self
                    .inode_cache
                    .refresh_cached_node_for_fresh_stat(inode, a.clone())
                    .await
                {
                    self.inode_cache.insert_node(inode, a.clone(), None).await;
                }
            }
            None => {
                self.inode_cache.invalidate_inode(inode).await;
            }
        }
        Ok(attr)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino, read, write, append))]
    async fn stat_for_open(
        &self,
        ino: i64,
        read: bool,
        write: bool,
        append: bool,
    ) -> Result<Option<FileAttr>, MetaError> {
        let inode = self.check_root(ino);
        let eligible = Self::open_file_cache_eligible(read, write, append);

        if eligible && let Some(cache) = &self.open_file_cache {
            if let Some(attr) = cache.attr(inode).await {
                self.metrics.record_open_file_cache_hit();
                return Ok(Some(attr));
            }
            self.metrics.record_open_file_cache_miss();
        }

        self.stat_fresh(inode).await
    }

    #[tracing::instrument(level = "trace", skip(self, attr), fields(ino, read, write, append))]
    async fn record_open(
        &self,
        ino: i64,
        attr: FileAttr,
        read: bool,
        write: bool,
        append: bool,
    ) -> Result<(), MetaError> {
        let inode = self.check_root(ino);
        let Some(cache) = &self.open_file_cache else {
            return Ok(());
        };

        if Self::open_file_cache_eligible(read, write, append) {
            cache.open(inode, attr).await;
        } else {
            cache.invalidate_inode(inode).await;
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn record_close(&self, ino: i64) -> Result<(), MetaError> {
        let inode = self.check_root(ino);
        if let Some(cache) = &self.open_file_cache {
            cache.close(inode).await;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        self.cached_lookup(parent, name).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn lookup_with_attr(
        &self,
        parent: i64,
        name: &str,
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        self.cached_lookup_with_attr(parent, name).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(path))]
    async fn lookup_path(&self, path: &str) -> Result<Option<(i64, FileType)>, MetaError> {
        let ino = match self.resolve_path(path).await {
            Ok(ino) => ino,
            Err(MetaError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };

        let attr = self
            .cached_stat(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;

        Ok(Some((ino, attr.kind)))
    }

    #[tracing::instrument(level = "trace", skip(self), fields(path))]
    async fn lookup_path_with_attr(
        &self,
        path: &str,
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        let ino = match self.resolve_path(path).await {
            Ok(ino) => ino,
            Err(MetaError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };

        let attr = self
            .cached_stat(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;

        Ok(Some((ino, attr)))
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn readdir(&self, ino: i64) -> Result<Vec<DirEntry>, MetaError> {
        let inode = self.check_root(ino);
        debug!("MetaClient: readdir request for inode {}", inode);

        if let Some(entries) = self.inode_cache.readdir(inode).await {
            debug!(
                "MetaClient: Inode cache HIT for readdir inode {} ({} entries)",
                inode,
                entries.len()
            );
            return Ok(entries);
        }

        trace!("MetaClient: Inode cache MISS for readdir inode {}", inode);

        // Keep the directory node cached before taking a snapshot so we can detect
        // concurrent child mutations and avoid marking a stale listing as complete.
        self.inode_cache
            .ensure_node_in_cache(inode, &&*self.store, None)
            .await?;
        let children_generation = self
            .inode_cache
            .children_generation(inode)
            .await
            .unwrap_or(0);

        let mut entries = self.store.readdir(inode).await?;
        // Sort once before caching so readops always return stable ordering by name.
        entries.sort_by(|a, b| a.name.cmp(&b.name));

        debug!(
            "MetaClient: Caching readdir result for inode {} ({} entries)",
            inode,
            entries.len()
        );

        // Ensure parent directory node is in cache before loading children
        self.inode_cache
            .ensure_node_in_cache(inode, &&*self.store, None)
            .await?;

        // Load all children from database into cache, replacing any stale data
        let children_data: Vec<(String, i64)> =
            entries.iter().map(|e| (e.name.clone(), e.ino)).collect();
        if !self
            .inode_cache
            .load_children_if_fresh(inode, children_data, children_generation)
            .await
        {
            trace!(
                "MetaClient: skipped stale readdir cache write for inode {}",
                inode
            );
        }

        // Note: We shouldn't pre-fetch attributes here; use batch prefetch instead.
        Ok(entries)
    }

    async fn opendir(&self, ino: i64) -> Result<DirHandle, MetaError> {
        let inode = self.check_root(ino);
        let attr = self
            .cached_stat(inode)
            .await?
            .ok_or(MetaError::NotFound(inode))?;
        if attr.kind != FileType::Dir {
            return Err(MetaError::NotDirectory(inode));
        }

        let entries = self.readdir(inode).await?;
        let (done_flag, prefetch_task) = MetaClient::spawn_batch_prefetch(self, inode, &entries);
        Ok(DirHandle::with_prefetch_task(
            inode,
            entries,
            prefetch_task,
            done_flag,
        ))
    }
    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn mkdir(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        self.ensure_writable()?;
        let parent = self.check_root(parent);

        Self::validate_entry_name(&name)?;

        info!("MetaClient: mkdir operation for ({}, '{}')", parent, name);

        let ino = self.store.mkdir(parent, name.clone()).await?;

        debug!("MetaClient: mkdir created inode {}, updating cache", ino);

        // Cache the new directory node
        if let Ok(Some(attr)) = self.store.stat(ino).await {
            self.inode_cache.insert_node(ino, attr, Some(parent)).await;
            self.inode_cache.mark_children_complete_empty(ino).await;
        }
        self.inode_cache.add_child(parent, name, ino).await;

        self.invalidate_parent_path(parent).await;

        Ok(ino)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn rmdir(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        self.ensure_writable()?;
        let parent = self.check_root(parent);
        info!("MetaClient: rmdir operation for ({}, '{}')", parent, name);

        self.store.rmdir(parent, name).await?;

        debug!("MetaClient: rmdir completed, updating cache");

        // Keep the deleted directory inode cached for a short time so open
        // directory handles can still service getattr/fstat after replacement.
        if let Some(child_ino) = self
            .inode_cache
            .remove_child_but_keep_inode(parent, name)
            .await
            && let Some(child_node) = self.inode_cache.get_node(child_ino).await
        {
            child_node.attr.write().await.nlink = 0;
            child_node.clear_parent().await;
        }
        self.invalidate_parent_path(parent).await;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn create_file(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        self.ensure_writable()?;
        let parent = self.check_root(parent);
        info!(
            "MetaClient: create_file operation for ({}, '{}')",
            parent, name
        );

        let ino = self.store.create_file(parent, name.clone()).await?;

        info!(
            "MetaClient: create_file created inode {}, updating cache",
            ino
        );

        if let Ok(Some(attr)) = self.store.stat(ino).await {
            let cache_parent = (attr.nlink <= 1).then_some(parent);
            self.inode_cache.insert_node(ino, attr, cache_parent).await;
        }
        self.inode_cache.add_child(parent, name, ino).await;

        self.invalidate_parent_path(parent).await;

        Ok(ino)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino, parent, name))]
    async fn link(&self, ino: i64, parent: i64, name: &str) -> Result<FileAttr, MetaError> {
        self.ensure_writable()?;
        let inode = self.check_root(ino);
        let parent = self.check_root(parent);

        Self::validate_entry_name(name)?;

        info!(
            "MetaClient: link operation for inode {} into ({}, '{}')",
            inode, parent, name
        );

        let attr = self.store.link(inode, parent, name).await?;

        self.inode_cache
            .ensure_node_in_cache(parent, &self.store, None)
            .await?;

        self.inode_cache
            .insert_node(inode, attr.clone(), None)
            .await;
        self.inode_cache
            .add_child(parent, name.to_string(), inode)
            .await;

        self.invalidate_parent_path(parent).await;

        Ok(attr)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name, target))]
    async fn symlink(
        &self,
        parent: i64,
        name: &str,
        target: &str,
    ) -> Result<(i64, FileAttr), MetaError> {
        self.ensure_writable()?;
        let parent = self.check_root(parent);

        Self::validate_entry_name(name)?;

        // POSIX: symlink target path component must also respect NAME_MAX
        Self::validate_symlink_target(target)?;

        info!(
            "MetaClient: symlink operation for ({}, '{}') -> '{}'",
            parent, name, target
        );

        let (ino, attr) = self.store.symlink(parent, name, target).await?;

        debug!("MetaClient: symlink created inode {}, updating cache", ino);

        self.inode_cache
            .ensure_node_in_cache(parent, &self.store, None)
            .await?;

        let cache_parent = (attr.nlink <= 1).then_some(parent);
        self.inode_cache
            .insert_node(ino, attr.clone(), cache_parent)
            .await;
        self.inode_cache
            .add_child(parent, name.to_string(), ino)
            .await;

        self.invalidate_parent_path(parent).await;

        Ok((ino, attr))
    }

    #[tracing::instrument(level = "trace", skip(self), fields(parent, name))]
    async fn unlink(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        self.ensure_writable()?;

        // Validate filename length BEFORE lookup to return ENAMETOOLONG instead of ENOENT
        Self::validate_entry_name(name)?;

        let parent = self.check_root(parent);
        info!("MetaClient: unlink operation for ({}, '{}')", parent, name);

        let target_ino = self.cached_lookup(parent, name).await?;
        let original_path = self.child_original_path_for_trash(parent, name).await;
        self.store.unlink(parent, name).await?;
        if let Some(ino) = target_ino {
            self.remember_trash_entry(ino, original_path).await;
        }

        debug!("MetaClient: unlink completed, updating cache");

        self.inode_cache.remove_child(parent, name).await;
        if let Some(ino) = target_ino {
            self.invalidate_open_file_cache_inode(ino).await;
        }
        self.invalidate_parent_path(parent).await;

        Ok(())
    }

    #[tracing::instrument(
        level = "trace",
        skip(self),
        fields(old_parent, old_name, new_parent, new_name)
    )]
    async fn rename(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
    ) -> Result<(), MetaError> {
        self.ensure_writable()?;

        let old_parent = self.check_root(old_parent);
        let new_parent = self.check_root(new_parent);

        // Fast path: if renaming to same location, return success (POSIX no-op)
        if old_parent == new_parent && old_name == new_name {
            return Ok(());
        }

        debug!(
            "MetaClient: rename operation from ({}, '{}') to ({}, '{}')",
            old_parent, old_name, new_parent, new_name
        );

        // Comprehensive pre-validation
        let src_ino = self.cached_lookup_required(old_parent, old_name).await?;

        let src_attr = self.cached_stat(src_ino).await?;

        // Validate new parent exists and is a directory
        self.ensure_directory_exists(new_parent).await?;

        // Validate name constraints
        Self::validate_entry_name(&new_name)?;

        // Resolve destination inode before store rename so we can invalidate its
        // cache entry afterwards.  When the store replaces an existing destination,
        // its nlink is decremented (possibly to 0, which deletes the node).  The
        // cache must reflect this, otherwise a subsequent stat on an fd that was
        // open before the overwrite returns a stale (non-zero) nlink.
        let dest_ino = self.cached_lookup(new_parent, &new_name).await?;

        // Execute the store-level rename with atomic cache updates
        self.store
            .rename(old_parent, old_name, new_parent, new_name.clone())
            .await?;
        self.invalidate_open_file_cache_inode(src_ino).await;
        if let Some(dest_ino) = dest_ino {
            self.invalidate_open_file_cache_inode(dest_ino).await;
        }

        debug!("MetaClient: rename completed, updating cache");

        // Update cache atomically with enhanced consistency management
        let cache_result = async {
            // Step 1: Store pre-rename cache state for precise invalidation
            let _old_parent_cached = self.inode_cache.get_node(old_parent).await;
            let _new_parent_cached = if old_parent != new_parent {
                self.inode_cache.get_node(new_parent).await
            } else {
                None
            };

            // Step 2: Remove child from old parent (keep inode for later use)
            let child_info = self
                .inode_cache
                .remove_child_but_keep_inode(old_parent, old_name)
                .await;

            if let Some(child_ino) = child_info {
                // Step 3: Ensure new parent is in cache with up-to-date metadata
                self.inode_cache
                    .ensure_node_in_cache(new_parent, &self.store, None)
                    .await?;

                // Step 4: Add child to new parent
                self.inode_cache
                    .add_child(new_parent, new_name.clone(), child_ino)
                    .await;

                // Directories keep their inline parent even though nlink is >= 2.
                if let Some(attr) = &src_attr {
                    if attr.kind == FileType::Dir || attr.nlink <= 1 {
                        if let Some(child_node) = self.inode_cache.get_node(child_ino).await {
                            child_node.set_parent(new_parent).await;
                        }
                    } else if let Some(child_node) = self.inode_cache.get_node(child_ino).await {
                        child_node.clear_parent().await;
                    }
                }
            }

            // Step 5: Keep an overwritten destination inode addressable while
            // it may still be held open by the kernel, but expose it as
            // unlinked.  This lets fstat() on an open-but-replaced directory
            // succeed with nlink=0 instead of returning ENOENT.
            if let Some(dest) = dest_ino {
                if let Some(dest_node) = self.inode_cache.get_node(dest).await {
                    dest_node.attr.write().await.nlink = 0;
                    dest_node.clear_parent().await;
                } else {
                    self.inode_cache.invalidate_inode(dest).await;
                }
            }

            // Step 6: Precise path cache invalidation
            // Invalidate paths affected by this rename
            self.invalidate_parent_path(old_parent).await;
            if old_parent != new_parent {
                self.invalidate_parent_path(new_parent).await;
            }

            // Step 7: Invalidate directory stat caches (mtime/ctime changed)
            self.inode_cache.invalidate_inode(old_parent).await;
            if old_parent != new_parent {
                self.inode_cache.invalidate_inode(new_parent).await;
            }

            // Step 8: Preload commonly accessed cache entries for better performance
            if let Some(child_ino) = child_info {
                // Preload the renamed entry and its new parent for better subsequent access
                let _ = self.preload_cache_entries(&[child_ino, new_parent]).await;
            }

            Ok::<(), MetaError>(())
        }
        .await;

        if let Err(cache_err) = cache_result {
            warn!(
                "MetaClient: cache update failed after successful store rename: {}",
                cache_err
            );
            // Cache inconsistency is logged but not fatal
        }

        Ok(())
    }

    async fn rename_exchange(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: &str,
    ) -> Result<(), MetaError> {
        self.ensure_writable()?;
        let old_parent = self.check_root(old_parent);
        let new_parent = self.check_root(new_parent);

        // Fast path: exchanging with itself is a no-op
        if old_parent == new_parent && old_name == new_name {
            return Ok(());
        }

        debug!(
            "MetaClient: rename_exchange operation between ({}, '{}') and ({}, '{}')",
            old_parent, old_name, new_parent, new_name
        );

        // Both entries must exist
        let old_ino = self.cached_lookup_required(old_parent, old_name).await?;
        let new_ino = self.cached_lookup_required(new_parent, new_name).await?;

        // Execute the store-level exchange
        self.store
            .rename_exchange(old_parent, old_name, new_parent, new_name)
            .await?;
        self.invalidate_open_file_cache_inode(old_ino).await;
        self.invalidate_open_file_cache_inode(new_ino).await;

        debug!("MetaClient: rename_exchange completed, updating cache");

        // Update cache to reflect the exchange
        let cache_result = async {
            // Invalidate all affected caches
            self.inode_cache.invalidate_inode(old_ino).await;
            self.inode_cache.invalidate_inode(new_ino).await;
            self.inode_cache.invalidate_inode(old_parent).await;
            if old_parent != new_parent {
                self.inode_cache.invalidate_inode(new_parent).await;
            }

            // Invalidate path caches
            self.invalidate_parent_path(old_parent).await;
            if old_parent != new_parent {
                self.invalidate_parent_path(new_parent).await;
            }

            // Update directory entries
            // Remove old entries
            self.inode_cache.remove_child(old_parent, old_name).await;
            self.inode_cache.remove_child(new_parent, new_name).await;

            // Add swapped entries
            self.inode_cache
                .ensure_node_in_cache(old_parent, &self.store, None)
                .await?;
            self.inode_cache
                .ensure_node_in_cache(new_parent, &self.store, None)
                .await?;

            self.inode_cache
                .add_child(old_parent, old_name.to_string(), new_ino)
                .await;
            self.inode_cache
                .add_child(new_parent, new_name.to_string(), old_ino)
                .await;

            Ok::<(), MetaError>(())
        }
        .await;

        if let Err(cache_err) = cache_result {
            warn!(
                "MetaClient: cache update failed after successful rename_exchange: {}",
                cache_err
            );
        }

        Ok(())
    }

    async fn can_rename(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: &str,
    ) -> Result<(), MetaError> {
        self.ensure_writable()?;

        // Basic validation
        let src_ino = self.cached_lookup_required(old_parent, old_name).await?;

        let src_attr = self.cached_stat(src_ino).await?;

        // Validate new parent exists and is a directory
        self.ensure_directory_exists(new_parent).await?;

        // Validate name constraints
        Self::validate_entry_name(new_name)?;

        // Check destination constraints
        if let Some(dest_ino) = self.cached_lookup(new_parent, new_name).await? {
            let dest_attr = self
                .cached_stat(dest_ino)
                .await?
                .ok_or(MetaError::NotFound(dest_ino))?;

            match (src_attr.map(|a| a.kind), dest_attr.kind) {
                // Directory replacing directory
                (Some(FileType::Dir), FileType::Dir) => {
                    let children = self.readdir(dest_ino).await?;
                    if !children.is_empty() {
                        return Err(MetaError::DirectoryNotEmpty(dest_ino));
                    }
                }
                // Directory replacing file/symlink - not allowed
                (Some(FileType::Dir), FileType::File)
                | (Some(FileType::Dir), FileType::Symlink) => {
                    return Err(MetaError::NotDirectory(dest_ino));
                }
                // File/symlink replacing directory - not allowed
                (Some(FileType::File), FileType::Dir)
                | (Some(FileType::Symlink), FileType::Dir) => {
                    return Err(MetaError::Io(std::io::Error::from(
                        std::io::ErrorKind::IsADirectory,
                    )));
                }
                // File/symlink replacing file/symlink - allowed
                _ => {}
            }
        }

        Ok(())
    }

    async fn rename_with_flags(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
        flags: crate::vfs::fs::RenameFlags,
    ) -> Result<(), MetaError> {
        self.ensure_writable()?;

        if flags.exchange {
            // Delegate to the atomic rename_exchange implementation (backed by
            // Lua script in Redis; transactional in SQL backends).
            self.rename_exchange(old_parent, old_name, new_parent, &new_name)
                .await
        } else if flags.noreplace {
            // Check if destination exists
            if self.cached_lookup(new_parent, &new_name).await?.is_some() {
                return Err(MetaError::AlreadyExists {
                    parent: new_parent,
                    name: new_name,
                });
            }
            self.rename(old_parent, old_name, new_parent, new_name)
                .await
        } else {
            // Default behavior
            self.rename(old_parent, old_name, new_parent, new_name)
                .await
        }
    }
    #[tracing::instrument(level = "trace", skip(self), fields(ino, size))]
    async fn set_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError> {
        self.ensure_writable()?;
        let inode = self.check_root(ino);
        self.store.set_file_size(inode, size).await?;

        // Update cached attribute
        if let Some(node) = self.inode_cache.get_node(inode).await {
            let mut attr = node.attr.write().await;
            attr.size = size;
        }
        self.invalidate_open_file_cache_inode(inode).await;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino, size))]
    async fn extend_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError> {
        self.ensure_writable()?;
        let inode = self.check_root(ino);
        self.store.extend_file_size(inode, size).await?;

        if let Some(node) = self.inode_cache.get_node(inode).await {
            let mut attr = node.attr.write().await;
            if size > attr.size {
                attr.size = size;
            }
        }
        self.invalidate_open_file_cache_inode(inode).await;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino, size, chunk_size))]
    async fn truncate(&self, ino: i64, size: u64, chunk_size: u64) -> Result<(), MetaError> {
        self.ensure_writable()?;
        let inode = self.check_root(ino);
        self.store.truncate(inode, size, chunk_size).await?;
        self.inode_cache.invalidate_inode(inode).await;
        self.invalidate_open_file_cache_inode(inode).await;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn get_names(&self, ino: i64) -> Result<Vec<(Option<i64>, String)>, MetaError> {
        let inode = self.check_root(ino);
        if inode == self.root() {
            return Ok(vec![(None, "/".to_string())]);
        }

        self.store.get_names(inode).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn get_dentries(&self, ino: i64) -> Result<Vec<(i64, String)>, MetaError> {
        let inode = self.check_root(ino);
        if inode == self.root() {
            return Ok(vec![(self.root(), "/".to_string())]);
        }

        self.store.get_dentries(inode).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(dir_ino))]
    async fn get_dir_parent(&self, dir_ino: i64) -> Result<Option<i64>, MetaError> {
        let inode = self.check_root(dir_ino);
        if inode == self.root() {
            return Ok(None);
        }

        self.store.get_dir_parent(inode).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn get_paths(&self, ino: i64) -> Result<Vec<String>, MetaError> {
        let inode = self.check_root(ino);
        if inode == self.root() {
            return Ok(vec!["/".to_string()]);
        }

        self.store.get_paths(inode).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn read_symlink(&self, ino: i64) -> Result<String, MetaError> {
        let inode = self.check_root(ino);
        info!("MetaClient: read_symlink request for inode {}", inode);
        self.store.read_symlink(inode).await
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, req),
        fields(ino, size = req.size, flags = ?flags)
    )]
    async fn set_attr(
        &self,
        ino: i64,
        req: &SetAttrRequest,
        flags: SetAttrFlags,
    ) -> Result<FileAttr, MetaError> {
        self.ensure_writable()?;
        let inode = self.check_root(ino);
        let attr = self.store.set_attr(inode, req, flags).await?;
        self.inode_cache
            .insert_node(inode, attr.clone(), None)
            .await;
        self.invalidate_open_file_cache_inode(inode).await;
        Ok(attr)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino, flags = ?flags))]
    async fn open(&self, ino: i64, flags: OpenFlags) -> Result<FileAttr, MetaError> {
        let inode = self.check_root(ino);
        self.store.open(inode, flags).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn close(&self, ino: i64) -> Result<(), MetaError> {
        let inode = self.check_root(ino);
        self.store.close(inode).await
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, slice),
        fields(
            ino,
            chunk_id,
            slice_id = slice.slice_id,
            offset = slice.offset,
            len = slice.length,
            new_size
        )
    )]
    async fn write(
        &self,
        ino: i64,
        chunk_id: u64,
        slice: SliceDesc,
        new_size: u64,
    ) -> Result<(), MetaError> {
        self.ensure_writable()?;
        let inode = self.check_root(ino);
        self.store.write(inode, chunk_id, slice, new_size).await?;

        let (inode_from_chunk, chunk_index) = extract_ino_and_chunk_index(chunk_id);
        self.inode_cache
            .append_slice(inode_from_chunk, chunk_index, slice)
            .await;
        self.invalidate_open_file_cache_inode(inode_from_chunk)
            .await;

        if let Some(node) = self.inode_cache.get_node(inode).await {
            let mut attr = node.attr.write().await;
            if new_size > attr.size {
                attr.size = new_size;
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_deleted_files(&self) -> Result<Vec<i64>, MetaError> {
        self.store.get_deleted_files().await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    async fn remove_file_metadata(&self, ino: i64) -> Result<(), MetaError> {
        self.ensure_writable()?;
        self.invalidate_open_file_cache_checked(ino).await;
        self.store.remove_file_metadata(ino).await
    }

    #[tracing::instrument(
        level = "trace",
        skip(self),
        fields(chunk_id, cache_hit = tracing::field::Empty, slice_count = tracing::field::Empty)
    )]
    async fn get_slices(&self, chunk_id: u64) -> Result<Vec<SliceDesc>, MetaError> {
        let (inode, chunk_index) = extract_ino_and_chunk_index(chunk_id);
        if let Some(slices) = self
            .inode_cache
            .get_slices(inode, chunk_index)
            .instrument(tracing::trace_span!(
                "get_slices.cache_lookup",
                inode,
                chunk_index
            ))
            .await
        {
            tracing::Span::current().record("cache_hit", true);
            tracing::Span::current().record("slice_count", slices.len());
            self.metrics.record_get_slices_cache_hit();
            return Ok(slices);
        }
        tracing::Span::current().record("cache_hit", false);
        self.metrics.record_get_slices_cache_miss();
        let slices = self
            .store
            .get_slices(chunk_id)
            .instrument(tracing::trace_span!("get_slices.store", chunk_id))
            .await?;
        let cached = self
            .inode_cache
            .cache_slices_if_absent(inode, chunk_index, &slices)
            .await
            .unwrap_or(slices);
        tracing::Span::current().record("slice_count", cached.len());
        Ok(cached)
    }

    async fn invalidate_chunk_slices(&self, ino: i64, chunk_index: u64) -> Result<(), MetaError> {
        self.inode_cache.invalidate_slices(ino, chunk_index).await;
        self.invalidate_open_file_cache_checked(ino).await;
        Ok(())
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, slice),
        fields(chunk_id, slice_id = slice.slice_id, offset = slice.offset, len = slice.length)
    )]
    async fn append_slice(&self, chunk_id: u64, slice: SliceDesc) -> Result<(), MetaError> {
        self.ensure_writable()?;

        let (inode, chunk_index) = extract_ino_and_chunk_index(chunk_id);
        self.store.append_slice(chunk_id, slice).await?;
        self.inode_cache
            .append_slice(inode, chunk_index, slice)
            .await;
        self.invalidate_open_file_cache_inode(inode).await;
        Ok(())
    }

    async fn next_id(&self, key: &str) -> Result<i64, MetaError> {
        self.ensure_writable()?;
        self.store.next_id(key).await
    }

    #[tracing::instrument(level = "trace", skip(self), fields(pid = session_info.process_id))]
    async fn start_session(&self, session_info: SessionInfo) -> Result<(), MetaError> {
        MetaClient::start_session(self, session_info).await
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn shutdown_session(&self) -> Result<(), MetaError> {
        MetaClient::shutdown_session(self).await;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, query), fields(inode, owner = query.owner))]
    async fn get_plock(
        &self,
        inode: i64,
        query: &FileLockQuery,
    ) -> Result<FileLockInfo, MetaError> {
        self.store.get_plock(inode, query).await
    }

    #[tracing::instrument(
        level = "trace",
        skip(self),
        fields(inode, owner, block, lock_type = ?lock_type, pid)
    )]
    async fn set_plock(
        &self,
        inode: i64,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
        range: FileLockRange,
        pid: u32,
    ) -> Result<(), MetaError> {
        debug!(
            "MetaClient: set_plock inode={}, owner={}, block={}, type={:?}, range=[{}, {}), pid={}",
            inode, owner, block, lock_type, range.start, range.end, pid
        );
        let res = self
            .store
            .set_plock(inode, owner, block, lock_type, range, pid)
            .await;
        match &res {
            Ok(()) => debug!("MetaClient: set_plock OK inode={}", inode),
            Err(e) => warn!("MetaClient: set_plock ERR inode={}: {:?}", inode, e),
        }
        res
    }

    async fn get_flock(&self, inode: i64, owner: i64) -> Result<FileLockType, MetaError> {
        self.store.get_flock(inode, owner).await
    }

    async fn set_flock(
        &self,
        inode: i64,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
    ) -> Result<(), MetaError> {
        self.store.set_flock(inode, owner, block, lock_type).await
    }

    async fn set_xattr(
        &self,
        inode: i64,
        name: &str,
        value: &[u8],
        flags: u32,
    ) -> Result<(), MetaError> {
        self.ensure_writable()?;
        self.store.set_xattr(inode, name, value, flags).await
    }

    async fn get_xattr(&self, inode: i64, name: &str) -> Result<Option<Vec<u8>>, MetaError> {
        self.store.get_xattr(inode, name).await
    }

    async fn list_xattr(&self, inode: i64) -> Result<Vec<String>, MetaError> {
        self.store.list_xattr(inode).await
    }

    async fn remove_xattr(&self, inode: i64, name: &str) -> Result<(), MetaError> {
        self.ensure_writable()?;
        self.store.remove_xattr(inode, name).await
    }

    async fn set_acl(&self, inode: i64, rule: AclRule) -> Result<(), MetaError> {
        self.ensure_writable()?;
        self.store.set_acl(inode, rule).await
    }

    async fn get_acl(
        &self,
        inode: i64,
        acl_type: u8,
        acl_id: u32,
    ) -> Result<Option<AclRule>, MetaError> {
        self.store.get_acl(inode, acl_type, acl_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::config::{
        CacheConfig, ClientOptions, CompactConfig, Config, DatabaseConfig, DatabaseType,
    };
    use crate::meta::stores::database::DatabaseMetaStore;
    use crate::vfs::chunk_id_for;
    use std::time::Duration;

    async fn create_test_client() -> Arc<MetaClient<DatabaseMetaStore>> {
        create_test_client_with_capacity(100, 100).await
    }

    async fn create_test_client_with_options(
        options: MetaClientOptions,
    ) -> Arc<MetaClient<DatabaseMetaStore>> {
        let db_path = "sqlite::memory:".to_string();

        let config = Config {
            database: DatabaseConfig {
                db_config: DatabaseType::Sqlite { url: db_path },
            },
            cache: CacheConfig::default(),
            client: ClientOptions::default(),
            compact: CompactConfig::default(),
        };

        let store = Arc::new(DatabaseMetaStore::from_config(config).await.unwrap());

        let capacity = CacheCapacity {
            inode: 100,
            path: 100,
        };

        let ttl = CacheTtl {
            inode_ttl: Duration::from_secs(60),
            path_ttl: Duration::from_secs(60),
        };

        MetaClient::with_options(store, capacity, ttl, options)
    }

    async fn create_test_client_with_capacity(
        inode_capacity: usize,
        path_capacity: usize,
    ) -> Arc<MetaClient<DatabaseMetaStore>> {
        let db_path = "sqlite::memory:".to_string();

        let config = Config {
            database: DatabaseConfig {
                db_config: DatabaseType::Sqlite { url: db_path },
            },
            cache: CacheConfig::default(),
            client: ClientOptions::default(),
            compact: CompactConfig::default(),
        };

        let store = Arc::new(DatabaseMetaStore::from_config(config).await.unwrap());

        let capacity = CacheCapacity {
            inode: inode_capacity,
            path: path_capacity,
        };

        let ttl = CacheTtl {
            inode_ttl: Duration::from_secs(60),
            path_ttl: Duration::from_secs(60),
        };

        MetaClient::new(store, capacity, ttl)
    }

    #[tokio::test]
    async fn test_rename_operations() {
        let client = create_test_client().await;

        // Create test structure
        let dir1 = client.mkdir(1, "dir1".to_string()).await.unwrap();
        let dir2 = client.mkdir(1, "dir2".to_string()).await.unwrap();
        let file1 = client
            .create_file(dir1, "old_name.txt".to_string())
            .await
            .unwrap();

        // Scenario 1: Rename within same directory
        client
            .rename(dir1, "old_name.txt", dir1, "new_name.txt".to_string())
            .await
            .unwrap();

        // Verify old name doesn't exist
        let old_lookup = client.lookup(dir1, "old_name.txt").await.unwrap();
        assert_eq!(old_lookup, None, "Old name should not exist");

        // Verify new name exists
        let new_lookup = client.lookup(dir1, "new_name.txt").await.unwrap();
        assert_eq!(
            new_lookup,
            Some(file1),
            "New name should point to same inode"
        );

        // readdir should show new name
        let entries = client.readdir(dir1).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "new_name.txt");

        // Scenario 2: Move across directories
        client
            .rename(dir1, "new_name.txt", dir2, "moved_file.txt".to_string())
            .await
            .unwrap();

        // dir1 should be empty
        let dir1_entries = client.readdir(dir1).await.unwrap();
        assert_eq!(dir1_entries.len(), 0, "dir1 should be empty after move");

        // dir2 should contain moved file
        let dir2_entries = client.readdir(dir2).await.unwrap();
        assert_eq!(dir2_entries.len(), 1, "dir2 should have 1 file");
        assert_eq!(dir2_entries[0].name, "moved_file.txt");

        // Verify path resolution
        let resolved = client.resolve_path("/dir2/moved_file.txt").await.unwrap();
        assert_eq!(resolved, file1, "Path should resolve to correct inode");

        let path = client
            .get_paths(file1)
            .await
            .unwrap()
            .first()
            .cloned()
            .unwrap();
        assert_eq!(
            path, "/dir2/moved_file.txt",
            "get_path should return new path"
        );
    }

    #[tokio::test]
    async fn test_slice_operations() {
        let client = create_test_client().await;

        let ino = client.create_file(1, "text".to_string()).await.unwrap();
        let chunk_id = chunk_id_for(ino, 1).unwrap();

        let test_slices = (1..=10)
            .map(|e| crate::chunk::SliceDesc {
                slice_id: e,
                chunk_id,
                offset: 0,
                length: 100,
            })
            .collect::<Vec<_>>();

        for desc in test_slices.iter().copied() {
            client.append_slice(chunk_id, desc).await.unwrap();
        }

        let from_method = client.get_slices(chunk_id).await.unwrap();
        assert_eq!(test_slices, from_method);

        let (ino, chunk_index) = extract_ino_and_chunk_index(chunk_id);
        let from_cached = client
            .inode_cache
            .get_slices(ino, chunk_index)
            .await
            .unwrap();
        assert_eq!(test_slices, from_cached);

        let metrics = client.metrics().snapshot();
        assert_eq!(metrics.get_slices_cache_miss, 1);
        assert_eq!(metrics.get_slices_cache_hit, 0);

        let second = client.get_slices(chunk_id).await.unwrap();
        assert_eq!(test_slices, second);
        let metrics = client.metrics().snapshot();
        assert_eq!(metrics.get_slices_cache_hit, 1);
    }

    #[tokio::test]
    async fn test_meta_client_cache_metrics_track_stat_and_lookup() {
        let client = create_test_client().await;
        let ino = client
            .create_file(1, "metrics.txt".to_string())
            .await
            .unwrap();

        assert_eq!(client.lookup(1, "metrics.txt").await.unwrap(), Some(ino));
        assert!(client.stat(ino).await.unwrap().is_some());
        client.readdir(1).await.unwrap();
        assert_eq!(client.lookup(1, "metrics.txt").await.unwrap(), Some(ino));

        let metrics = client.metrics().snapshot();
        assert!(metrics.lookup_cache_miss >= 1);
        assert!(metrics.lookup_cache_hit >= 1);
        assert!(metrics.stat_cache_hit >= 1);
    }

    #[tokio::test]
    async fn test_meta_client_lookup_with_attr_populates_stat_cache_and_metrics() {
        let client = create_test_client().await;
        let ino = client
            .create_file(1, "lookup-with-attr.txt".to_string())
            .await
            .unwrap();

        client.inode_cache.invalidate_inode(1).await;
        client.inode_cache.invalidate_inode(ino).await;

        let before = client.metrics().snapshot();
        let (found, attr) = client
            .lookup_with_attr(1, "lookup-with-attr.txt")
            .await
            .unwrap()
            .expect("lookup_with_attr should find created file");
        assert_eq!(found, ino);
        assert_eq!(attr.ino, ino);

        let after_lookup = client.metrics().snapshot();
        assert_eq!(
            after_lookup.lookup_attr_fused_miss,
            before.lookup_attr_fused_miss + 1
        );
        assert_eq!(after_lookup.stat_cache_miss, before.stat_cache_miss);

        assert!(client.stat(ino).await.unwrap().is_some());
        let after_stat = client.metrics().snapshot();
        assert_eq!(
            after_stat.stat_cache_miss, after_lookup.stat_cache_miss,
            "lookup_with_attr should populate inode stat cache for later stat calls"
        );
        assert!(after_stat.stat_cache_hit > after_lookup.stat_cache_hit);
    }

    #[tokio::test]
    async fn test_meta_client_lookup_only_does_not_count_lookup_attr_fused_path() {
        let client = create_test_client().await;
        let ino = client
            .create_file(1, "lookup-only.txt".to_string())
            .await
            .unwrap();

        client.inode_cache.invalidate_inode(1).await;
        client.inode_cache.invalidate_inode(ino).await;

        let before = client.metrics().snapshot();
        assert_eq!(
            client.lookup(1, "lookup-only.txt").await.unwrap(),
            Some(ino)
        );
        let after = client.metrics().snapshot();

        assert_eq!(
            after.lookup_attr_fused_miss, before.lookup_attr_fused_miss,
            "lookup-only callers should stay on the lighter inode-only path"
        );
    }

    #[tokio::test]
    async fn test_open_file_cache_disabled_preserves_fresh_open_stat() {
        let client = create_test_client().await;
        let ino = client
            .create_file(1, "open-cache-disabled.txt".to_string())
            .await
            .unwrap();

        let first = client
            .stat_for_open(ino, true, false, false)
            .await
            .unwrap()
            .unwrap();
        client
            .record_open(ino, first, true, false, false)
            .await
            .unwrap();
        client.record_close(ino).await.unwrap();

        let second = client
            .stat_for_open(ino, true, false, false)
            .await
            .unwrap()
            .unwrap();
        client
            .record_open(ino, second, true, false, false)
            .await
            .unwrap();
        client.record_close(ino).await.unwrap();

        let metrics = client.metrics().snapshot();
        assert_eq!(metrics.open_fresh_stat, 2);
        assert_eq!(metrics.open_file_cache_hit, 0);
        assert_eq!(metrics.open_file_cache_miss, 0);
    }

    #[tokio::test]
    async fn test_open_file_cache_hits_readonly_open_and_invalidates_on_mutation() {
        let options = MetaClientOptions {
            open_file_cache: OpenFileCacheConfig {
                ttl: Duration::from_secs(60),
                capacity: 128,
            },
            ..Default::default()
        };
        let client = create_test_client_with_options(options).await;
        let ino = client
            .create_file(1, "open-cache-enabled.txt".to_string())
            .await
            .unwrap();

        let first = client
            .stat_for_open(ino, true, false, false)
            .await
            .unwrap()
            .unwrap();
        client
            .record_open(ino, first.clone(), true, false, false)
            .await
            .unwrap();
        client.record_close(ino).await.unwrap();

        let cached = client
            .stat_for_open(ino, true, false, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cached.size, first.size);

        let metrics = client.metrics().snapshot();
        assert_eq!(metrics.open_fresh_stat, 1);
        assert_eq!(metrics.open_file_cache_miss, 1);
        assert_eq!(metrics.open_file_cache_hit, 1);

        let rdwr_cached = client
            .stat_for_open(ino, true, true, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rdwr_cached.size, first.size);
        let metrics = client.metrics().snapshot();
        assert_eq!(metrics.open_fresh_stat, 1);
        assert_eq!(metrics.open_file_cache_hit, 2);

        client
            .set_attr(
                ino,
                &SetAttrRequest {
                    size: Some(first.size + 4096),
                    ..Default::default()
                },
                SetAttrFlags::empty(),
            )
            .await
            .unwrap();

        let refreshed = client
            .stat_for_open(ino, true, false, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(refreshed.size, first.size + 4096);

        let metrics = client.metrics().snapshot();
        assert_eq!(metrics.open_fresh_stat, 2);
        assert_eq!(metrics.open_file_cache_hit, 2);
        assert_eq!(metrics.open_file_cache_miss, 2);
    }

    #[tokio::test]
    async fn test_stat_fresh_refreshes_cached_file_entry_in_place() {
        let client = create_test_client().await;

        let ino = client
            .create_file(1, "fresh.txt".to_string())
            .await
            .unwrap();
        let attr = client.stat(ino).await.unwrap().unwrap();
        let cached_before = client.inode_cache.get_node(ino).await.unwrap();

        let chunk_id = chunk_id_for(ino, 1).unwrap();
        let (slice_ino, chunk_index) = extract_ino_and_chunk_index(chunk_id);
        assert_eq!(slice_ino, ino);
        let cached_slices = [SliceDesc {
            slice_id: 1,
            chunk_id,
            offset: 0,
            length: 128,
        }];
        client
            .inode_cache
            .cache_slices_if_absent(ino, chunk_index, &cached_slices)
            .await;
        assert!(
            client
                .inode_cache
                .get_slices(ino, chunk_index)
                .await
                .is_some()
        );

        client
            .store
            .set_file_size(ino, attr.size + 4096)
            .await
            .unwrap();

        let fresh = client.stat_fresh(ino).await.unwrap().unwrap();
        let cached_after = client.inode_cache.get_node(ino).await.unwrap();

        assert_eq!(fresh.size, attr.size + 4096);
        assert!(
            Arc::ptr_eq(&cached_before, &cached_after),
            "fresh stat should update cached file metadata without reallocating the inode entry"
        );
        assert!(
            client
                .inode_cache
                .get_slices(ino, chunk_index)
                .await
                .is_none(),
            "fresh stat must drop potentially stale cached slice metadata"
        );
    }

    #[tokio::test]
    async fn test_control_plane_registers_and_serves_gc_jobs() {
        let runtime_dir = tempfile::tempdir().unwrap();
        let options = MetaClientOptions {
            mount_point: Some("/mnt/test".to_string()),
            control_runtime_dir: Some(runtime_dir.path().to_path_buf()),
            ..Default::default()
        };

        let client = create_test_client_with_options(options).await;
        client.start_control_plane().await.unwrap();

        let registry =
            crate::control::runtime::RuntimeRegistry::new(runtime_dir.path().to_path_buf());
        let record = registry.select_instance(Some("/mnt/test")).await.unwrap();

        let pong = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::Ping,
        )
        .await
        .unwrap();
        assert_eq!(pong, crate::control::protocol::ControlResponse::Pong);

        let accepted = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::RunGc { dry_run: true },
        )
        .await
        .unwrap();

        let crate::control::protocol::ControlResponse::Accepted { job_id } = accepted else {
            panic!("expected accepted response");
        };

        let status = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::GetJob { job_id },
        )
        .await
        .unwrap();

        match status {
            crate::control::protocol::ControlResponse::JobStatus { .. } => {}
            other => panic!("unexpected status response: {other:?}"),
        }

        client.shutdown_runtime().await;
    }

    #[tokio::test]
    async fn test_control_plane_lists_directory_metadata() {
        let runtime_dir = tempfile::tempdir().unwrap();
        let options = MetaClientOptions {
            mount_point: Some("/mnt/list".to_string()),
            control_runtime_dir: Some(runtime_dir.path().to_path_buf()),
            ..Default::default()
        };

        let client = create_test_client_with_options(options).await;
        let docs_ino = client.mkdir(1, "docs".to_string()).await.unwrap();
        let readme_ino = client
            .create_file(docs_ino, "readme.md".to_string())
            .await
            .unwrap();
        client.start_control_plane().await.unwrap();

        let registry =
            crate::control::runtime::RuntimeRegistry::new(runtime_dir.path().to_path_buf());
        let record = registry.select_instance(Some("/mnt/list")).await.unwrap();

        let acl = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::PutAcl {
                path: "/docs/readme.md".to_string(),
                entries: vec![crate::control::protocol::ControlAclEntry {
                    scope: "access".to_string(),
                    tag: "user".to_string(),
                    id: Some(1001),
                    perm: "rw-".to_string(),
                }],
            },
        )
        .await
        .unwrap();
        match acl {
            crate::control::protocol::ControlResponse::Acl { path, entries } => {
                assert_eq!(path, "/docs/readme.md");
                assert_eq!(entries.len(), 1);
            }
            other => panic!("unexpected acl response: {other:?}"),
        }

        let response = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::ListDirectory {
                path: "/docs".to_string(),
            },
        )
        .await
        .unwrap();

        match response {
            crate::control::protocol::ControlResponse::DirectoryListing { path, entries } => {
                assert_eq!(path, "/docs");
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].name, "readme.md");
                assert_eq!(entries[0].inode, readme_ino);
                assert_eq!(
                    entries[0].kind,
                    crate::control::protocol::ControlFileKind::File
                );
                assert!(entries[0].has_acl);
            }
            other => panic!("unexpected directory listing response: {other:?}"),
        }

        client.shutdown_runtime().await;
    }

    #[tokio::test]
    async fn test_control_plane_stats_paths_and_reads_symlinks() {
        let runtime_dir = tempfile::tempdir().unwrap();
        let options = MetaClientOptions {
            mount_point: Some("/mnt/stat".to_string()),
            control_runtime_dir: Some(runtime_dir.path().to_path_buf()),
            ..Default::default()
        };

        let client = create_test_client_with_options(options).await;
        let docs_ino = client.mkdir(1, "docs".to_string()).await.unwrap();
        let readme_ino = client
            .create_file(docs_ino, "readme.md".to_string())
            .await
            .unwrap();
        client
            .symlink(1, "latest", "/docs/readme.md")
            .await
            .unwrap();
        client.start_control_plane().await.unwrap();

        let registry =
            crate::control::runtime::RuntimeRegistry::new(runtime_dir.path().to_path_buf());
        let record = registry.select_instance(Some("/mnt/stat")).await.unwrap();

        let metadata = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::StatPath {
                path: "/docs/readme.md".to_string(),
            },
        )
        .await
        .unwrap();

        match metadata {
            crate::control::protocol::ControlResponse::PathMetadata { path, metadata } => {
                assert_eq!(path, "/docs/readme.md");
                assert_eq!(metadata.inode, readme_ino);
                assert_eq!(
                    metadata.kind,
                    crate::control::protocol::ControlFileKind::File
                );
            }
            other => panic!("unexpected path metadata response: {other:?}"),
        }

        let target = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::ReadLink {
                path: "/latest".to_string(),
            },
        )
        .await
        .unwrap();

        match target {
            crate::control::protocol::ControlResponse::SymlinkTarget { path, target } => {
                assert_eq!(path, "/latest");
                assert_eq!(target, "/docs/readme.md");
            }
            other => panic!("unexpected symlink target response: {other:?}"),
        }

        client.shutdown_runtime().await;
    }

    #[tokio::test]
    async fn test_control_plane_acl_requests_persist_entries() {
        let runtime_dir = tempfile::tempdir().unwrap();
        let options = MetaClientOptions {
            mount_point: Some("/mnt/acl".to_string()),
            control_runtime_dir: Some(runtime_dir.path().to_path_buf()),
            ..Default::default()
        };

        let client = create_test_client_with_options(options).await;
        client.mkdir(1, "docs".to_string()).await.unwrap();
        client.start_control_plane().await.unwrap();

        let registry =
            crate::control::runtime::RuntimeRegistry::new(runtime_dir.path().to_path_buf());
        let record = registry.select_instance(Some("/mnt/acl")).await.unwrap();

        let missing = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::GetAcl {
                path: "/missing".to_string(),
            },
        )
        .await
        .unwrap();
        assert_control_error_code(missing, "not_found");

        let initial = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::GetAcl {
                path: "/docs".to_string(),
            },
        )
        .await
        .unwrap();
        match initial {
            crate::control::protocol::ControlResponse::Acl { path, entries } => {
                assert_eq!(path, "/docs");
                assert!(entries.is_empty());
            }
            other => panic!("unexpected initial ACL response: {other:?}"),
        }

        let entries = vec![
            crate::control::protocol::ControlAclEntry {
                scope: "access".to_string(),
                tag: "user_obj".to_string(),
                id: None,
                perm: "rwx".to_string(),
            },
            crate::control::protocol::ControlAclEntry {
                scope: "default".to_string(),
                tag: "group".to_string(),
                id: Some(1000),
                perm: "r-x".to_string(),
            },
        ];

        let put = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::PutAcl {
                path: "/docs".to_string(),
                entries: entries.clone(),
            },
        )
        .await
        .unwrap();
        match put {
            crate::control::protocol::ControlResponse::Acl {
                path,
                entries: put_entries,
            } => {
                assert_eq!(path, "/docs");
                assert_eq!(put_entries, entries);
            }
            other => panic!("unexpected put ACL response: {other:?}"),
        }

        let get = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::GetAcl {
                path: "/docs".to_string(),
            },
        )
        .await
        .unwrap();
        match get {
            crate::control::protocol::ControlResponse::Acl {
                path,
                entries: get_entries,
            } => {
                assert_eq!(path, "/docs");
                assert_eq!(get_entries, entries);
            }
            other => panic!("unexpected get ACL response: {other:?}"),
        }

        let deleted = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::DeleteAcl {
                path: "/docs".to_string(),
            },
        )
        .await
        .unwrap();
        match deleted {
            crate::control::protocol::ControlResponse::AclDeleted { path } => {
                assert_eq!(path, "/docs");
            }
            other => panic!("unexpected delete ACL response: {other:?}"),
        }

        let after_delete = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::GetAcl {
                path: "/docs".to_string(),
            },
        )
        .await
        .unwrap();
        match after_delete {
            crate::control::protocol::ControlResponse::Acl { path, entries } => {
                assert_eq!(path, "/docs");
                assert!(entries.is_empty());
            }
            other => panic!("unexpected ACL response after delete: {other:?}"),
        }

        client.shutdown_runtime().await;
    }

    #[tokio::test]
    async fn test_control_plane_acl_rejects_invalid_entries() {
        let runtime_dir = tempfile::tempdir().unwrap();
        let options = MetaClientOptions {
            mount_point: Some("/mnt/acl-invalid".to_string()),
            control_runtime_dir: Some(runtime_dir.path().to_path_buf()),
            ..Default::default()
        };

        let client = create_test_client_with_options(options).await;
        client.mkdir(1, "docs".to_string()).await.unwrap();
        client.start_control_plane().await.unwrap();

        let registry =
            crate::control::runtime::RuntimeRegistry::new(runtime_dir.path().to_path_buf());
        let record = registry
            .select_instance(Some("/mnt/acl-invalid"))
            .await
            .unwrap();

        let invalid = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::PutAcl {
                path: "/docs".to_string(),
                entries: vec![crate::control::protocol::ControlAclEntry {
                    scope: "access".to_string(),
                    tag: "group_obj".to_string(),
                    id: None,
                    perm: "read".to_string(),
                }],
            },
        )
        .await
        .unwrap();
        assert_control_error_code(invalid, "invalid_request");

        let get = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::GetAcl {
                path: "/docs".to_string(),
            },
        )
        .await
        .unwrap();
        match get {
            crate::control::protocol::ControlResponse::Acl { entries, .. } => {
                assert!(entries.is_empty());
            }
            other => panic!("unexpected ACL response after invalid put: {other:?}"),
        }

        client.shutdown_runtime().await;
    }

    #[tokio::test]
    async fn test_database_acl_capability_matches_rule_storage() {
        let client = create_test_client().await;
        let capabilities = client.store.capabilities();
        assert!(capabilities.xattr);
        assert!(capabilities.acl);

        let rule = AclRule {
            acl_type: 1,
            qualifier: 1000,
            permissions: 0o7,
        };
        client.set_acl(1, rule.clone()).await.unwrap();

        let stored = client.get_acl(1, 1, 1000).await.unwrap().unwrap();
        assert_eq!(stored, rule);

        let replacement = AclRule {
            permissions: 0o5,
            ..rule
        };
        client.set_acl(1, replacement.clone()).await.unwrap();
        let stored = client.get_acl(1, 1, 1000).await.unwrap().unwrap();
        assert_eq!(stored, replacement);
        assert!(client.get_acl(1, 1, 2000).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_control_plane_list_trash_returns_unlinked_files() {
        let runtime_dir = tempfile::tempdir().unwrap();
        let options = MetaClientOptions {
            mount_point: Some("/mnt/trash".to_string()),
            control_runtime_dir: Some(runtime_dir.path().to_path_buf()),
            ..Default::default()
        };

        let client = create_test_client_with_options(options).await;
        let docs_ino = client.mkdir(1, "docs".to_string()).await.unwrap();
        let report_ino = client
            .create_file(docs_ino, "report.txt".to_string())
            .await
            .unwrap();
        client.unlink(docs_ino, "report.txt").await.unwrap();
        client.start_control_plane().await.unwrap();

        let registry =
            crate::control::runtime::RuntimeRegistry::new(runtime_dir.path().to_path_buf());
        let record = registry.select_instance(Some("/mnt/trash")).await.unwrap();

        let response = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::ListTrash,
        )
        .await
        .unwrap();

        match response {
            crate::control::protocol::ControlResponse::Trash { entries } => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].id, report_ino.to_string());
                assert_eq!(entries[0].original_path, "/docs/report.txt");
                assert_eq!(entries[0].size, Some(0));
                assert!(entries[0].deleted_at.is_some());
            }
            other => panic!("unexpected trash response: {other:?}"),
        }

        let restore = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::RestoreTrashEntry {
                entry_id: report_ino.to_string(),
            },
        )
        .await
        .unwrap();
        match restore {
            crate::control::protocol::ControlResponse::TrashRestored { entry_id } => {
                assert_eq!(entry_id, report_ino.to_string());
            }
            other => panic!("unexpected trash restore response: {other:?}"),
        }
        assert_eq!(
            client.resolve_path("/docs/report.txt").await.unwrap(),
            report_ino
        );

        let after_restore = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::ListTrash,
        )
        .await
        .unwrap();
        match after_restore {
            crate::control::protocol::ControlResponse::Trash { entries } => {
                assert!(entries.is_empty());
            }
            other => panic!("unexpected trash response after restore: {other:?}"),
        }

        let delete = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::DeleteTrashEntry {
                entry_id: report_ino.to_string(),
            },
        )
        .await
        .unwrap();
        assert_control_error_code(delete, "not_found");

        client.shutdown_runtime().await;
    }

    #[tokio::test]
    async fn test_control_plane_delete_trash_permanently_removes_entry() {
        let runtime_dir = tempfile::tempdir().unwrap();
        let options = MetaClientOptions {
            mount_point: Some("/mnt/trash-delete".to_string()),
            control_runtime_dir: Some(runtime_dir.path().to_path_buf()),
            ..Default::default()
        };

        let client = create_test_client_with_options(options).await;
        let docs_ino = client.mkdir(1, "docs".to_string()).await.unwrap();
        let report_ino = client
            .create_file(docs_ino, "report.txt".to_string())
            .await
            .unwrap();
        client.unlink(docs_ino, "report.txt").await.unwrap();
        client.start_control_plane().await.unwrap();

        let registry =
            crate::control::runtime::RuntimeRegistry::new(runtime_dir.path().to_path_buf());
        let record = registry
            .select_instance(Some("/mnt/trash-delete"))
            .await
            .unwrap();

        let delete = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::DeleteTrashEntry {
                entry_id: report_ino.to_string(),
            },
        )
        .await
        .unwrap();
        match delete {
            crate::control::protocol::ControlResponse::TrashDeleted { entry_id } => {
                assert_eq!(entry_id, report_ino.to_string());
            }
            other => panic!("unexpected trash delete response: {other:?}"),
        }

        let trash = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::ListTrash,
        )
        .await
        .unwrap();
        match trash {
            crate::control::protocol::ControlResponse::Trash { entries } => {
                assert!(entries.is_empty());
            }
            other => panic!("unexpected trash response after delete: {other:?}"),
        }

        assert!(client.store.stat(report_ino).await.unwrap().is_none());
        let restore = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::RestoreTrashEntry {
                entry_id: report_ino.to_string(),
            },
        )
        .await
        .unwrap();
        assert_control_error_code(restore, "not_found");

        client.shutdown_runtime().await;
    }

    #[tokio::test]
    async fn test_control_plane_trash_actions_reject_live_inodes_with_trash_metadata() {
        let runtime_dir = tempfile::tempdir().unwrap();
        let options = MetaClientOptions {
            mount_point: Some("/mnt/trash-live".to_string()),
            control_runtime_dir: Some(runtime_dir.path().to_path_buf()),
            ..Default::default()
        };

        let client = create_test_client_with_options(options).await;
        let docs_ino = client.mkdir(1, "docs".to_string()).await.unwrap();
        let report_ino = client
            .create_file(docs_ino, "report.txt".to_string())
            .await
            .unwrap();
        let forged_metadata = ControlTrashMetadata {
            original_path: "/docs/report.txt".to_string(),
            deleted_at: Utc::now().to_rfc3339(),
        };
        let raw = serde_json::to_vec(&forged_metadata).unwrap();
        client
            .store
            .set_xattr(report_ino, CONTROL_TRASH_XATTR_NAME, &raw, 0)
            .await
            .unwrap();
        client.start_control_plane().await.unwrap();

        let registry =
            crate::control::runtime::RuntimeRegistry::new(runtime_dir.path().to_path_buf());
        let record = registry
            .select_instance(Some("/mnt/trash-live"))
            .await
            .unwrap();

        let restore = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::RestoreTrashEntry {
                entry_id: report_ino.to_string(),
            },
        )
        .await
        .unwrap();
        assert_control_error_code(restore, "not_found");

        let delete = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::DeleteTrashEntry {
                entry_id: report_ino.to_string(),
            },
        )
        .await
        .unwrap();
        assert_control_error_code(delete, "not_found");

        assert!(client.store.stat(report_ino).await.unwrap().is_some());

        client.shutdown_runtime().await;
    }

    #[tokio::test]
    async fn test_control_plane_restore_trash_preserves_entry_on_name_conflict() {
        let runtime_dir = tempfile::tempdir().unwrap();
        let options = MetaClientOptions {
            mount_point: Some("/mnt/trash-conflict".to_string()),
            control_runtime_dir: Some(runtime_dir.path().to_path_buf()),
            ..Default::default()
        };

        let client = create_test_client_with_options(options).await;
        let docs_ino = client.mkdir(1, "docs".to_string()).await.unwrap();
        let deleted_ino = client
            .create_file(docs_ino, "report.txt".to_string())
            .await
            .unwrap();
        client.unlink(docs_ino, "report.txt").await.unwrap();
        let replacement_ino = client
            .create_file(docs_ino, "report.txt".to_string())
            .await
            .unwrap();
        client.start_control_plane().await.unwrap();

        let registry =
            crate::control::runtime::RuntimeRegistry::new(runtime_dir.path().to_path_buf());
        let record = registry
            .select_instance(Some("/mnt/trash-conflict"))
            .await
            .unwrap();

        let restore = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::RestoreTrashEntry {
                entry_id: deleted_ino.to_string(),
            },
        )
        .await
        .unwrap();
        assert_control_error_code(restore, "already_exists");
        assert_eq!(
            client.resolve_path("/docs/report.txt").await.unwrap(),
            replacement_ino
        );

        let trash = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::ListTrash,
        )
        .await
        .unwrap();
        match trash {
            crate::control::protocol::ControlResponse::Trash { entries } => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].id, deleted_ino.to_string());
            }
            other => panic!("unexpected trash response after conflict: {other:?}"),
        }

        client.shutdown_runtime().await;
    }

    fn assert_control_error_code(
        response: crate::control::protocol::ControlResponse,
        expected: &str,
    ) {
        match response {
            crate::control::protocol::ControlResponse::Error { code, .. } => {
                assert_eq!(code, expected);
            }
            other => panic!("expected control error {expected}, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_control_plane_get_info_returns_mount_metadata() {
        let runtime_dir = tempfile::tempdir().unwrap();
        let options = MetaClientOptions {
            mount_point: Some("/mnt/info".to_string()),
            control_runtime_dir: Some(runtime_dir.path().to_path_buf()),
            ..Default::default()
        };

        let client = create_test_client_with_options(options).await;
        client.start_control_plane().await.unwrap();

        let registry =
            crate::control::runtime::RuntimeRegistry::new(runtime_dir.path().to_path_buf());
        let record = registry.select_instance(Some("/mnt/info")).await.unwrap();

        let response = crate::control::client::send_request(
            &record.socket_path,
            &crate::control::protocol::ControlRequest::GetInfo,
        )
        .await
        .unwrap();

        match response {
            crate::control::protocol::ControlResponse::Info {
                pid,
                mount_point,
                version,
                meta_backend,
                capabilities,
                ..
            } => {
                assert_eq!(pid, std::process::id());
                assert_eq!(mount_point, "/mnt/info");
                assert_eq!(version, env!("CARGO_PKG_VERSION"));
                assert_eq!(meta_backend, "database");
                assert!(capabilities.namespace);
                assert!(capabilities.xattr);
            }
            other => panic!("unexpected info response: {other:?}"),
        }

        client.shutdown_runtime().await;
    }

    #[tokio::test]
    async fn test_control_plane_shutdown_cleans_runtime_record() {
        let runtime_dir = tempfile::tempdir().unwrap();
        let options = MetaClientOptions {
            mount_point: Some("/mnt/test-cleanup".to_string()),
            control_runtime_dir: Some(runtime_dir.path().to_path_buf()),
            ..Default::default()
        };

        let client = create_test_client_with_options(options).await;
        client.start_control_plane().await.unwrap();
        client.shutdown_runtime().await;

        let registry =
            crate::control::runtime::RuntimeRegistry::new(runtime_dir.path().to_path_buf());
        let err = registry
            .select_instance(Some("/mnt/test-cleanup"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no brewfs instance"));
    }

    /// Test scenario: Complex sequence of mixed operations
    ///
    #[tokio::test]
    async fn test_complex_mixed_operations() {
        let client = create_test_client().await;

        // Create initial structure
        let dir1 = client.mkdir(1, "dir1".to_string()).await.unwrap();
        let file1 = client
            .create_file(dir1, "file1.txt".to_string())
            .await
            .unwrap();
        let file2 = client
            .create_file(dir1, "file2.txt".to_string())
            .await
            .unwrap();

        // Verify initial state
        let dir1_entries = client.readdir(dir1).await.unwrap();
        assert_eq!(dir1_entries.len(), 2);

        // Rename file1 within the same directory
        client
            .rename(dir1, "file1.txt", dir1, "renamed1.txt".to_string())
            .await
            .unwrap();

        // Verify rename worked
        let dir1_entries = client.readdir(dir1).await.unwrap();
        assert_eq!(dir1_entries.len(), 2);
        let names: std::collections::HashSet<String> =
            dir1_entries.iter().map(|e| e.name.clone()).collect();
        assert!(names.contains("renamed1.txt"));
        assert!(names.contains("file2.txt"));
        assert!(!names.contains("file1.txt"));

        // Create a subdirectory
        let subdir = client.mkdir(dir1, "subdir".to_string()).await.unwrap();

        // Move file2 into subdirectory
        client
            .rename(dir1, "file2.txt", subdir, "moved2.txt".to_string())
            .await
            .unwrap();

        // Verify move worked
        let dir1_entries = client.readdir(dir1).await.unwrap();
        assert_eq!(dir1_entries.len(), 2); // renamed1.txt + subdir
        let subdir_entries = client.readdir(subdir).await.unwrap();
        assert_eq!(subdir_entries.len(), 1);
        assert_eq!(subdir_entries[0].name, "moved2.txt");

        // Verify path resolution still works
        let path1 = client
            .get_paths(file1)
            .await
            .unwrap()
            .first()
            .cloned()
            .unwrap();
        assert_eq!(path1, "/dir1/renamed1.txt");

        let path2 = client
            .get_paths(file2)
            .await
            .unwrap()
            .first()
            .cloned()
            .unwrap();
        assert_eq!(path2, "/dir1/subdir/moved2.txt");

        // Create hard link and test rename behavior
        let _link_attr = client.link(file1, 1, "link1.txt").await.unwrap();

        // Verify both paths exist
        let paths = client.get_paths(file1).await.unwrap();
        assert_eq!(paths.len(), 2);
        let path_set: std::collections::HashSet<String> = paths.into_iter().collect();
        assert!(path_set.contains("/dir1/renamed1.txt"));
        assert!(path_set.contains("/link1.txt"));

        // Rename one of the links
        client
            .rename(1, "link1.txt", 1, "renamed_link.txt".to_string())
            .await
            .unwrap();

        // Verify paths updated correctly
        let paths = client.get_paths(file1).await.unwrap();
        assert_eq!(paths.len(), 2);
        let path_set: std::collections::HashSet<String> = paths.into_iter().collect();
        assert!(path_set.contains("/dir1/renamed1.txt"));
        assert!(path_set.contains("/renamed_link.txt"));
        assert!(!path_set.contains("/link1.txt"));
    }

    #[test]
    fn test_normalize_path() {
        // Basic absolute paths
        assert_eq!(MetaClient::<DatabaseMetaStore>::normalize_path("/"), "/");
        assert_eq!(
            MetaClient::<DatabaseMetaStore>::normalize_path("/home/user"),
            "/home/user"
        );

        // Handle .
        assert_eq!(MetaClient::<DatabaseMetaStore>::normalize_path("/./"), "/");
        assert_eq!(
            MetaClient::<DatabaseMetaStore>::normalize_path("/home/./user"),
            "/home/user"
        );

        // Handle ..
        assert_eq!(
            MetaClient::<DatabaseMetaStore>::normalize_path("/home/user/../"),
            "/home"
        );
        assert_eq!(
            MetaClient::<DatabaseMetaStore>::normalize_path("/home/../user"),
            "/user"
        );

        // Complex cases
        assert_eq!(
            MetaClient::<DatabaseMetaStore>::normalize_path("/a/b/../c/./d"),
            "/a/c/d"
        );
        assert_eq!(
            MetaClient::<DatabaseMetaStore>::normalize_path("/a/./b/../../c"),
            "/c"
        );

        // Relative paths
        assert_eq!(
            MetaClient::<DatabaseMetaStore>::normalize_path("file.txt"),
            "file.txt"
        );
        assert_eq!(
            MetaClient::<DatabaseMetaStore>::normalize_path("../file.txt"),
            "file.txt"
        );

        // Edge cases
        assert_eq!(MetaClient::<DatabaseMetaStore>::normalize_path(""), "");
        assert_eq!(MetaClient::<DatabaseMetaStore>::normalize_path("."), "");
        assert_eq!(
            MetaClient::<DatabaseMetaStore>::normalize_path("/../../../file.txt"),
            "/file.txt"
        );
    }

    /// Test scenario: Delete operations and cache invalidation
    ///
    /// Verify delete operations work correctly and cache is properly invalidated
    #[tokio::test]
    async fn test_delete_operations() {
        let client = create_test_client().await;

        let dir1 = client.mkdir(1, "dir1".to_string()).await.unwrap();

        // Create files
        let _f1 = client
            .create_file(dir1, "f1.txt".to_string())
            .await
            .unwrap();
        let _f2 = client
            .create_file(dir1, "f2.txt".to_string())
            .await
            .unwrap();
        let _f3 = client
            .create_file(dir1, "f3.txt".to_string())
            .await
            .unwrap();
        let _f4 = client
            .create_file(dir1, "f4.txt".to_string())
            .await
            .unwrap();

        // readdir should include new file
        let entries2 = client.readdir(dir1).await.unwrap();
        assert_eq!(entries2.len(), 4, "Should see all 4 files");

        // Delete a file
        client.unlink(dir1, "f2.txt").await.unwrap();

        // readdir should reflect deletion
        let entries3 = client.readdir(dir1).await.unwrap();
        assert_eq!(entries3.len(), 3, "Should have 3 files after deletion");

        let names3: Vec<String> = entries3.iter().map(|e| e.name.clone()).collect();
        assert!(
            !names3.contains(&"f2.txt".to_string()),
            "Deleted file should not appear"
        );
    }

    /// Test scenario: Empty directory handling
    ///
    /// Verify readdir behavior and cache handling for empty directories
    #[tokio::test]
    async fn test_empty_directory_handling() {
        let client = create_test_client().await;

        // Create empty directory
        let empty_dir = client.mkdir(1, "empty".to_string()).await.unwrap();

        // readdir on empty directory
        let entries = client.readdir(empty_dir).await.unwrap();
        assert_eq!(entries.len(), 0, "Empty directory should have no entries");

        // readdir again (should hit cache)
        let entries2 = client.readdir(empty_dir).await.unwrap();
        assert_eq!(entries2.len(), 0, "Cached result should also be empty");

        // Add file
        let file = client
            .create_file(empty_dir, "first.txt".to_string())
            .await
            .unwrap();

        // readdir should show new file
        let entries3 = client.readdir(empty_dir).await.unwrap();
        assert_eq!(entries3.len(), 1, "Should have 1 file");
        assert_eq!(entries3[0].name, "first.txt");
        assert_eq!(entries3[0].ino, file);

        // Delete file, restore to empty
        client.unlink(empty_dir, "first.txt").await.unwrap();

        // Should be empty again
        let entries4 = client.readdir(empty_dir).await.unwrap();
        assert_eq!(entries4.len(), 0, "Should be empty again");
    }

    #[tokio::test]
    async fn test_get_parent_and_name() {
        let client = create_test_client().await;

        let dir1 = client.mkdir(1, "parent_dir".to_string()).await.unwrap();
        let file1 = client
            .create_file(dir1, "child_file.txt".to_string())
            .await
            .unwrap();

        let root_parent = client.get_dir_parent(dir1).await.unwrap().unwrap();
        assert_eq!(root_parent, 1, "Parent of dir1 should be root");

        let file_links = client.get_dentries(file1).await.unwrap();
        assert!(
            file_links.contains(&(dir1, "child_file.txt".to_string())),
            "File should have expected (parent,name) link"
        );

        let dir_links = client.get_names(dir1).await.unwrap();
        assert!(
            dir_links.contains(&(Some(1), "parent_dir".to_string())),
            "Directory should have expected (parent,name) link"
        );

        let root_links = client.get_names(1).await.unwrap();
        assert_eq!(root_links, vec![(None, "/".to_string())]);
    }

    #[tokio::test]
    async fn test_hardlink_get_names_and_rename_one_link() {
        let client = create_test_client().await;

        let links = client.mkdir(1, "links".to_string()).await.unwrap();
        let file_ino = client
            .create_file(links, "a.txt".to_string())
            .await
            .unwrap();

        client.link(file_ino, links, "b.txt").await.unwrap();

        let names = client.get_names(file_ino).await.unwrap();
        assert!(names.contains(&(Some(links), "a.txt".to_string())));
        assert!(names.contains(&(Some(links), "b.txt".to_string())));

        client
            .rename(links, "b.txt", links, "c.txt".to_string())
            .await
            .unwrap();

        let names = client.get_names(file_ino).await.unwrap();
        assert!(names.contains(&(Some(links), "a.txt".to_string())));
        assert!(names.contains(&(Some(links), "c.txt".to_string())));
        assert!(!names.contains(&(Some(links), "b.txt".to_string())));

        client.unlink(links, "c.txt").await.unwrap();

        let names = client.get_names(file_ino).await.unwrap();
        assert_eq!(names, vec![(Some(links), "a.txt".to_string())]);
    }

    #[tokio::test]
    async fn test_hardlink_link_should_not_poison_cached_parent() {
        let client = create_test_client().await;

        let d1 = client.mkdir(1, "d1".to_string()).await.unwrap();
        let d2 = client.mkdir(1, "d2".to_string()).await.unwrap();

        let file_ino = client.create_file(d1, "a.txt".to_string()).await.unwrap();

        client.link(file_ino, d2, "b.txt").await.unwrap();

        let dentries = client.get_dentries(file_ino).await.unwrap();
        assert!(dentries.contains(&(d1, "a.txt".to_string())));
        assert!(dentries.contains(&(d2, "b.txt".to_string())));
    }

    /// Test scenario: Intelligent path cache invalidation
    ///
    /// Verify path cache invalidation strategy:
    /// 1. Modifying a directory only invalidates related paths
    /// 2. Unrelated paths should remain cached
    #[tokio::test]
    async fn test_intelligent_path_invalidation() {
        let client = create_test_client().await;

        // Create directory structure:
        // /dira/
        // /dira/file1.txt
        // /dirb/
        // /dirb/file2.txt
        let dira = client.mkdir(1, "dira".to_string()).await.unwrap();
        let dirb = client.mkdir(1, "dirb".to_string()).await.unwrap();

        let _file1 = client
            .create_file(dira, "file1.txt".to_string())
            .await
            .unwrap();
        let _file2 = client
            .create_file(dirb, "file2.txt".to_string())
            .await
            .unwrap();

        // Resolve all paths to populate cache
        let _ino_dira = client.resolve_path("/dira").await.unwrap();
        let _ino_file1 = client.resolve_path("/dira/file1.txt").await.unwrap();
        let _ino_dirb = client.resolve_path("/dirb").await.unwrap();
        let _ino_file2 = client.resolve_path("/dirb/file2.txt").await.unwrap();

        // Verify all paths are cached
        assert!(client.path_cache.get("/dira").await.is_some());
        assert!(client.path_cache.get("/dira/file1.txt").await.is_some());
        assert!(client.path_cache.get("/dirb").await.is_some());
        assert!(client.path_cache.get("/dirb/file2.txt").await.is_some());

        // Create new file in /dira (triggers invalidation)
        let _file3 = client
            .create_file(dira, "file3.txt".to_string())
            .await
            .unwrap();

        // Verify intelligent invalidation:
        // - /dira and its sub-paths should be invalidated
        // - /dirb and its sub-paths should remain cached (unrelated)

        // Re-resolve /dirb paths - should hit cache
        let ino_dirb_after = client.resolve_path("/dirb").await.unwrap();
        assert_eq!(ino_dirb_after, dirb, "/dirb should still be cached");

        let ino_file2_after = client.resolve_path("/dirb/file2.txt").await.unwrap();
        assert_eq!(
            ino_file2_after, _file2,
            "/dirb/file2.txt should still be cached"
        );

        // Verify new file is accessible
        let ino_file3 = client.resolve_path("/dira/file3.txt").await.unwrap();
        assert_eq!(ino_file3, _file3);
    }

    #[tokio::test]
    async fn test_rename_same_location_noop() {
        use std::sync::atomic::Ordering;

        let client = create_test_client().await;

        // Create test file
        let root = client.root.load(Ordering::Relaxed);
        let file_ino = client
            .create_file(root, "test.txt".to_string())
            .await
            .unwrap();

        // Get original attributes
        let original_attr = client.cached_stat(file_ino).await.unwrap().unwrap();

        // Test 1: Rename to same location should succeed as no-op
        client
            .rename(root, "test.txt", root, "test.txt".to_string())
            .await
            .unwrap();

        // Verify file still exists and unchanged
        let after_attr = client.cached_stat(file_ino).await.unwrap().unwrap();
        assert_eq!(original_attr.ino, after_attr.ino);

        // Test 2: Verify lookup still works
        let looked_up = client.lookup(root, "test.txt").await.unwrap().unwrap();
        assert_eq!(looked_up, file_ino);

        // Test 3: rename_exchange with same location should also succeed
        client
            .rename_exchange(root, "test.txt", root, "test.txt")
            .await
            .unwrap();

        // Verify still exists
        let final_attr = client.cached_stat(file_ino).await.unwrap().unwrap();
        assert_eq!(original_attr.ino, final_attr.ino);
    }
}
