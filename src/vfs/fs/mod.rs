//! FUSE/SDK-friendly VFS with path-based metadata ops and handle-based IO.

use crate::chunk::store::BlockStore;
use crate::chunk::{BlockGcConfig, ChunkLayout, CompactionWorker, CompactionWorkerConfig};
use crate::meta::MetaLayer;
use crate::meta::client::MetaClient;
use crate::meta::config::CompactConfig;
use crate::meta::config::MetaClientConfig;
use crate::meta::file_lock::{FileLockInfo, FileLockQuery, FileLockRange, FileLockType};
use crate::meta::store::{
    AclRule, MetaError, MetaStore, SetAttrFlags, SetAttrRequest, StatFsSnapshot,
};
use crate::posix::NAME_MAX;
use dashmap::{DashMap, Entry};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};

// Re-export types from meta::store for convenience
pub use crate::meta::store::{DirEntry, FileAttr, FileType};

const SPARSE_ZERO_MAX_SCAN_BYTES: usize = 128 * 1024;
const FUSE_STATFS_CACHE_TTL: Duration = Duration::from_secs(1);
const FALLOCATE_STATFS_CACHE_TTL: Duration = Duration::from_millis(100);
const FALLOCATE_ALLOCATION_UNIT: u64 = 4096;

/// Rename operation flags (similar to Linux renameat2 flags)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RenameFlags {
    /// Don't overwrite the destination if it exists (RENAME_NOREPLACE)
    pub noreplace: bool,
    /// Atomically exchange the source and destination (RENAME_EXCHANGE)
    pub exchange: bool,
    /// Remove the destination if it's a whiteout (RENAME_WHITEOUT)
    pub whiteout: bool,
}

/// Configuration for VFS background tasks
#[derive(Debug, Clone)]
pub struct VfsBackgroundConfig {
    pub compaction: CompactionWorkerConfig,
    pub gc: BlockGcConfig,
    pub compact_config: CompactConfig,
    pub enabled: bool,
}

impl VfsBackgroundConfig {
    pub fn from_compact_config(
        layout: &ChunkLayout,
        compact_config: CompactConfig,
        enabled: bool,
    ) -> Self {
        let compaction = CompactionWorkerConfig {
            scan_interval: compact_config.interval,
            max_chunks_per_run: compact_config.max_chunks_per_run,
            enabled,
        };

        let gc = BlockGcConfig {
            block_size: layout.block_size as u64,
            interval: compact_config.interval,
            ..Default::default()
        };

        Self {
            compaction,
            gc,
            compact_config,
            enabled,
        }
    }
}

impl Default for VfsBackgroundConfig {
    fn default() -> Self {
        Self {
            compaction: CompactionWorkerConfig::default(),
            gc: BlockGcConfig::default(),
            compact_config: CompactConfig::default(),
            enabled: true,
        }
    }
}

struct VfsBackgroundTasks {
    compaction_handle: tokio::task::JoinHandle<()>,
    gc_handle: tokio::task::JoinHandle<()>,
}

const RECENTLY_UNLINKED_ATTR_TTL: Duration = Duration::from_secs(5);
const RECENTLY_UNLINKED_ATTR_CLEANUP_THRESHOLD: usize = 4096;
const RECENTLY_UNLINKED_ATTR_CLEANUP_INTERVAL: u64 =
    RECENTLY_UNLINKED_ATTR_CLEANUP_THRESHOLD as u64;

fn vfs_timing_enabled_from_env() -> bool {
    std::env::var("BREWFS_VFS_TIMING")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

use crate::vfs::Inode;
use crate::vfs::backend::Backend;
use crate::vfs::cache::config::CacheConfig;
use crate::vfs::chunk_id_for;
use crate::vfs::config::VFSConfig;
use crate::vfs::error::{PathHint, VfsError};
use crate::vfs::handles::{DirHandle, FileHandle, HandleFlags};
use crate::vfs::io::{DataReader, DataWriter, split_chunk_spans};
use crate::vfs::memory::MemoryBudget;

struct HandleRegistry<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    handles: DashMap<u64, Arc<FileHandle<B, M>>>,
    inode_handles: DashMap<i64, Vec<u64>>,
    dir_handles: DashMap<u64, Arc<DirHandle>>,
    next_fh: AtomicU64,
}

impl<B, M> HandleRegistry<B, M>
where
    B: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    fn new() -> Self {
        Self {
            handles: DashMap::new(),
            inode_handles: DashMap::new(),
            dir_handles: DashMap::new(),
            next_fh: AtomicU64::new(1),
        }
    }

    fn allocate(&self, ino: i64, attr: FileAttr, flags: HandleFlags) -> Arc<FileHandle<B, M>> {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        let handle = Arc::new(FileHandle::new(fh, ino, attr, flags));
        self.handles.insert(fh, handle.clone());
        self.inode_handles.entry(ino).or_default().push(fh);
        handle
    }

    fn release(&self, fh: u64) -> Option<(Arc<FileHandle<B, M>>, bool)> {
        let handle = self.handles.remove(&fh)?.1;
        let ino = handle.ino;
        let mut last = false;
        if let Some(mut entry) = self.inode_handles.get_mut(&ino) {
            if let Some(idx) = entry.iter().position(|id| *id == fh) {
                entry.remove(idx);
            }
            let empty = entry.is_empty();
            drop(entry);
            if empty {
                self.inode_handles.remove(&ino);
                last = true;
            }
        }
        Some((handle, last))
    }

    fn get(&self, fh: u64) -> Option<Arc<FileHandle<B, M>>> {
        self.handles.get(&fh).map(|entry| Arc::clone(entry.value()))
    }

    fn mark_write_dirty(&self, fh: u64) -> bool {
        let Some(handle) = self.handles.get(&fh) else {
            return false;
        };
        handle.mark_write_dirty();
        true
    }

    fn take_write_dirty_for_inode(&self, ino: i64) -> bool {
        let mut dirty = false;
        for fh in self.handles_for(ino) {
            if let Some(handle) = self.handles.get(&fh) {
                dirty |= handle.take_write_dirty();
            }
        }
        dirty
    }

    fn take_write_dirty(&self, fh: u64) -> bool {
        self.handles
            .get(&fh)
            .map(|handle| handle.take_write_dirty())
            .unwrap_or(false)
    }

    fn handles_for(&self, ino: i64) -> Vec<u64> {
        self.inode_handles
            .get(&ino)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    fn attr_for(&self, fh: u64) -> Option<FileAttr> {
        self.handles.get(&fh).map(|entry| entry.attr()).or_else(|| {
            self.dir_handles
                .get(&fh)
                .and_then(|entry| entry.attr.clone())
        })
    }

    fn attr_for_inode(&self, ino: i64) -> Option<FileAttr> {
        let fhs = self.handles_for(ino);
        for fh in fhs {
            if let Some(handle) = self.handles.get(&fh) {
                return Some(handle.attr());
            }
        }
        for entry in self.dir_handles.iter() {
            if entry.ino == ino
                && let Some(attr) = entry.attr.clone()
            {
                return Some(attr);
            }
        }
        None
    }

    fn update_attr_for_inode(&self, ino: i64, attr: &FileAttr) {
        let fhs = self.handles_for(ino);
        for fh in fhs {
            if let Some(handle) = self.handles.get(&fh) {
                handle.update_attr(attr);
            }
        }
    }

    /// Check if any handle for this inode was opened for writing
    fn has_write_handle(&self, ino: i64) -> bool {
        let fhs = self.handles_for(ino);
        fhs.iter()
            .any(|fh| self.handles.get(fh).map(|h| h.flags.write).unwrap_or(false))
    }

    fn has_no_handle(&self, ino: i64) -> bool {
        self.handles_for(ino).is_empty()
    }

    fn allocate_dir(&self, handle: DirHandle) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.dir_handles.insert(fh, Arc::new(handle));
        fh
    }

    fn release_dir(&self, fh: u64) -> Option<Arc<DirHandle>> {
        self.dir_handles.remove(&fh).map(|(_, handle)| handle)
    }

    /// Replace the DirHandle at an existing fh with a fresh one.
    /// Used for rewinddir: keep the same fh but swap in new entries.
    fn replace_dir(&self, fh: u64, handle: DirHandle) -> bool {
        if self.dir_handles.contains_key(&fh) {
            self.dir_handles.insert(fh, Arc::new(handle));
            true
        } else {
            false
        }
    }

    fn get_dir(&self, fh: u64) -> Option<Arc<DirHandle>> {
        self.dir_handles
            .get(&fh)
            .map(|entry| Arc::clone(entry.value()))
    }
}

struct VfsState<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    handles: HandleRegistry<S, M>,
    inodes: DashMap<i64, Arc<Inode>>,
    recently_unlinked: DashMap<i64, (FileAttr, Instant)>,
    recently_unlinked_cleanup_tick: AtomicU64,
    reader: Arc<DataReader<S, M>>,
    writer: Arc<DataWriter<S, M>>,
    append_locks: DashMap<i64, Arc<Mutex<()>>>,
    fuse_cached_write_order: Arc<FuseCachedWriteOrder>,
    statfs_cache: StdMutex<Option<StatFsCache>>,
    fallocate_statfs_cache: StdMutex<Option<FallocateStatFsCache>>,
    posix_lock_owners: DashMap<(i64, i64), ()>,
    fuse_lock_owners_by_handle: DashMap<(i64, u64, i64), ()>,
    pub(crate) stats: Arc<crate::vfs::stats::FsStats>,
    memory_budget: Option<MemoryBudget>,
    vfs_timing_enabled: bool,
}

struct FallocateStatFsCache {
    snapshot: StatFsSnapshot,
    cached_at: Instant,
}

struct StatFsCache {
    snapshot: StatFsSnapshot,
    cached_at: Instant,
}

#[derive(Default)]
pub(crate) struct FuseCachedWriteOrder {
    pending: StdMutex<BTreeMap<i64, BTreeSet<u64>>>,
    notify: Notify,
}

pub(crate) struct FuseCachedWriteGuard {
    order: Arc<FuseCachedWriteOrder>,
    ino: i64,
    unique: u64,
}

impl FuseCachedWriteOrder {
    fn begin(self: &Arc<Self>, ino: i64, unique: u64) -> FuseCachedWriteGuard {
        self.pending
            .lock()
            .expect("fuse cached write order lock poisoned")
            .entry(ino)
            .or_default()
            .insert(unique);
        FuseCachedWriteGuard {
            order: Arc::clone(self),
            ino,
            unique,
        }
    }

    async fn wait_for_prior(&self, ino: i64, unique: u64) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if !self.has_prior(ino, unique) {
                return;
            }

            notified.await;
        }
    }

    fn has_prior(&self, ino: i64, unique: u64) -> bool {
        self.pending
            .lock()
            .expect("fuse cached write order lock poisoned")
            .get(&ino)
            .and_then(|pending| pending.iter().next().copied())
            .is_some_and(|first| first < unique)
    }

    fn finish(&self, ino: i64, unique: u64) {
        let removed = {
            let mut pending = self
                .pending
                .lock()
                .expect("fuse cached write order lock poisoned");
            let Some(writes) = pending.get_mut(&ino) else {
                return;
            };
            let removed = writes.remove(&unique);
            if writes.is_empty() {
                pending.remove(&ino);
            }
            removed
        };
        if removed {
            self.notify.notify_waiters();
        }
    }
}

impl Drop for FuseCachedWriteGuard {
    fn drop(&mut self) {
        self.order.finish(self.ino, self.unique);
    }
}

impl<S, M> VfsState<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    fn new(config: Arc<VFSConfig>, backend: Arc<Backend<S, M>>) -> Self {
        let memory_budget = (config.cache.memory_budget_bytes > 0)
            .then(|| MemoryBudget::new(config.cache.memory_budget_bytes));

        let prefetcher = if config.cache.prefetch_enabled {
            let prefetch_backend = backend.clone();
            let prefetch_layout = config.read.layout;
            let prefetch_concurrency = config.cache.prefetch_concurrency.max(1);
            let prefetch_queue_depth = prefetch_concurrency.saturating_mul(16).max(1024);
            Some(Arc::new(crate::vfs::cache::prefetch::GlobalPrefetcher::new(
                prefetch_concurrency,
                prefetch_queue_depth,
                move |ino, start, len| {
                    let backend = prefetch_backend.clone();
                    let layout = prefetch_layout;
                    async move {
                        use crate::chunk::reader::DataFetcher;
                        use crate::vfs::chunk_id_for;
                        use crate::vfs::io::split_chunk_spans;

                        let spans = split_chunk_spans(layout, start, len as usize);
                        // Issue all spans concurrently — each span fetches
                        // its blocks via SingleFlight so parallelism is bounded
                        // by the prefetch semaphore, not serialized here.
                        let mut tasks = Vec::with_capacity(spans.len());
                        for span in spans {
                            let cid = match chunk_id_for(ino, span.index) {
                                Ok(c) => c,
                                Err(_) => continue,
                            };
                            let backend = backend.clone();
                            tasks.push(tokio::spawn(async move {
                                let mut fetcher = DataFetcher::new(layout, cid, &*backend);
                                if fetcher.prepare_slices().await.is_err() {
                                    return;
                                }
                                let _ =
                                    fetcher.read_at(span.offset.into(), span.len as usize).await;
                            }));
                        }
                        for t in tasks {
                            let _ = t.await;
                        }
                    }
                },
            ))
                as Arc<dyn crate::vfs::cache::prefetch::Prefetcher>)
        } else {
            None
        };

        let mut reader_builder = DataReader::new(config.read.clone(), backend.clone());
        if let Some(memory_budget) = memory_budget.clone() {
            reader_builder = reader_builder.with_memory_budget(memory_budget);
        }
        if let Some(prefetcher) = prefetcher {
            reader_builder = reader_builder.with_prefetcher(prefetcher);
        }
        let reader = Arc::new(reader_builder);

        let write_back = if matches!(
            config.cache.writeback_mode,
            crate::vfs::cache::config::WriteBackMode::CommitBeforeUpload
        ) {
            let cache_root = config.cache.cache_root.join("writeback");
            let _ = std::fs::create_dir_all(&cache_root);
            let wb = Arc::new(
                crate::vfs::cache::write_back::FsWriteBackCache::new_with_sync(
                    cache_root,
                    config.cache.writeback_persist_sync,
                ),
            );

            // Crash recovery is only needed when metadata can be committed
            // before object upload completes.
            #[cfg(not(test))]
            {
                let wb_clone = wb.clone();
                let backend_clone = backend.clone();
                let layout = config.write.layout;
                tokio::spawn(async move {
                    Self::recover_dirty_slices(&wb_clone, &backend_clone, layout).await;
                });
            }

            Some(wb)
        } else {
            None
        };

        let mut writer_builder =
            DataWriter::new(config.write.clone(), backend, reader.clone(), write_back);
        if let Some(memory_budget) = memory_budget.clone() {
            writer_builder = writer_builder.with_memory_budget(memory_budget);
        }
        let writer = Arc::new(writer_builder);
        writer.start_flush_background();
        Self {
            handles: HandleRegistry::new(),
            inodes: DashMap::new(),
            recently_unlinked: DashMap::new(),
            recently_unlinked_cleanup_tick: AtomicU64::new(0),
            reader,
            writer,
            append_locks: DashMap::new(),
            fuse_cached_write_order: Arc::new(FuseCachedWriteOrder::default()),
            statfs_cache: StdMutex::new(None),
            fallocate_statfs_cache: StdMutex::new(None),
            posix_lock_owners: DashMap::new(),
            fuse_lock_owners_by_handle: DashMap::new(),
            stats: Arc::new(crate::vfs::stats::FsStats::new()),
            memory_budget,
            vfs_timing_enabled: vfs_timing_enabled_from_env(),
        }
    }

    /// Scan local SSD for dirty slices from a previous session.
    /// Re-uploads recoverable slices and cleans up stale records.
    async fn recover_dirty_slices(
        wb: &crate::vfs::cache::write_back::FsWriteBackCache,
        backend: &Arc<Backend<S, M>>,
        layout: crate::chunk::ChunkLayout,
    ) {
        use crate::vfs::cache::keys::DirtySliceState;
        use crate::vfs::cache::write_back::WriteBackCache;

        let records = match wb.recover().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = ?e, "write-back cache recovery scan failed");
                return;
            }
        };

        if records.is_empty() {
            return;
        }

        tracing::info!(
            count = records.len(),
            "recovered dirty slices from previous session"
        );

        for record in records {
            if !record.path.exists() {
                let _ = wb.remove(&record.key).await;
                continue;
            }

            match record.state {
                DirtySliceState::Sealed | DirtySliceState::Failed | DirtySliceState::Uploading => {
                    tracing::info!(
                        ino = record.ino,
                        chunk_id = record.chunk_id,
                        length = record.length,
                        state = ?record.state,
                        "re-uploading recovered slice"
                    );
                    Self::reupload_recovered_slice(wb, backend, layout, &record).await;
                }
                _ => {
                    let _ = wb.remove(&record.key).await;
                }
            }
        }
    }

    async fn reupload_recovered_slice(
        wb: &crate::vfs::cache::write_back::FsWriteBackCache,
        backend: &Arc<Backend<S, M>>,
        layout: crate::chunk::ChunkLayout,
        record: &crate::vfs::cache::write_back::DirtySliceRecord,
    ) {
        use crate::chunk::writer::DataUploader;
        use crate::vfs::cache::write_back::WriteBackCache;

        let data = match tokio::fs::read(&record.path).await {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(path = ?record.path, error = ?e, "cannot read recovered slice");
                return;
            }
        };

        let slice_id = match backend.meta().next_id(crate::meta::SLICE_ID_KEY).await {
            Ok(id) => id as u64,
            Err(e) => {
                tracing::warn!(error = ?e, "failed to allocate slice_id for recovery");
                return;
            }
        };

        let uploader = DataUploader::new(layout, backend);
        let chunks = vec![bytes::Bytes::from(data)];
        if let Err(e) = uploader
            .write_at_vectored(slice_id, 0u64.into(), &chunks)
            .await
        {
            tracing::warn!(slice_id, error = ?e, "recovery upload failed");
            return;
        }

        let desc = crate::chunk::SliceDesc {
            chunk_id: record.chunk_id,
            slice_id,
            offset: record.chunk_offset,
            length: record.length,
        };
        let (ino, chunk_index) = crate::vfs::extract_ino_and_chunk_index(record.chunk_id);
        let file_offset = chunk_index * layout.chunk_size + desc.offset;
        let new_size = file_offset + desc.length;

        // Check if the inode still exists before committing.  If the file was
        // deleted before the crash, the dirty record is orphaned and should be
        // cleaned up rather than entering an infinite recovery retry loop.
        match backend.meta().stat(ino).await {
            Ok(None) | Err(MetaError::NotFound(_)) => {
                tracing::warn!(
                    ino,
                    slice_id,
                    "recovery skipped: inode deleted, removing orphan dirty record"
                );
                let _ = wb.remove(&record.key).await;
                return;
            }
            Err(e) => {
                tracing::warn!(ino, slice_id, error = ?e, "recovery stat check failed, will retry commit");
            }
            Ok(Some(_)) => {}
        }

        if let Err(e) = backend
            .meta()
            .write(ino, record.chunk_id, desc, new_size)
            .await
        {
            // If the inode was deleted between stat and write, clean up and move on.
            if matches!(e, MetaError::NotFound(_)) {
                tracing::warn!(
                    ino,
                    slice_id,
                    "recovery metadata commit: inode gone, removing orphan dirty record"
                );
                let _ = wb.remove(&record.key).await;
                return;
            }
            tracing::warn!(ino, slice_id, error = ?e, "recovery metadata commit failed");
            return;
        }

        tracing::info!(
            ino,
            slice_id,
            length = record.length,
            "recovery commit success"
        );
        let _ = wb.remove(&record.key).await;
    }

    fn append_lock(&self, ino: i64) -> Arc<Mutex<()>> {
        self.append_locks
            .entry(ino)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

#[allow(dead_code)]
pub(crate) struct VfsCore<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    layout: ChunkLayout,
    pub(crate) backend: Arc<Backend<S, M>>,
    pub(crate) meta_layer: Arc<M>,
    root: i64,
}

impl<S, M> VfsCore<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    pub(crate) fn new(
        layout: ChunkLayout,
        backend: Arc<Backend<S, M>>,
        meta_layer: Arc<M>,
        root: i64,
    ) -> Self {
        Self {
            layout,
            backend,
            meta_layer,
            root,
        }
    }
}

#[allow(unused)]
#[allow(clippy::upper_case_acronyms)]
pub struct VFS<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    core: Arc<VfsCore<S, M>>,
    state: Arc<VfsState<S, M>>,
    /// Background tasks (compaction and gc) - only present when enabled
    #[allow(dead_code)]
    background_tasks: Option<VfsBackgroundTasks>,
}

#[derive(Clone, Debug)]
pub(crate) struct CreateFileAtResult {
    pub(crate) ino: i64,
    pub(crate) attr: Option<FileAttr>,
    pub(crate) created: bool,
    pub(crate) attrs_applied: bool,
}

impl<S, M> Clone for VFS<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            state: Arc::clone(&self.state),
            // Note: background tasks are not cloned as they should be unique per VFS instance
            background_tasks: None,
        }
    }
}

impl<S, R> VFS<S, MetaClient<R>>
where
    S: BlockStore + Send + Sync + 'static,
    R: MetaStore + Send + Sync + 'static,
{
    pub async fn new(layout: ChunkLayout, store: S, meta: R) -> Result<Self, VfsError> {
        Self::with_meta_client_config(layout, store, meta, MetaClientConfig::default()).await
    }

    pub(crate) async fn with_meta_client_config(
        layout: ChunkLayout,
        store: S,
        meta: R,
        config: MetaClientConfig,
    ) -> Result<Self, VfsError> {
        let store = Arc::new(store);
        let meta = Arc::new(meta);

        let ttl = config.effective_ttl();

        let meta_client = MetaClient::with_options(
            Arc::clone(&meta),
            config.capacity.clone(),
            ttl,
            config.options.clone(),
        );

        meta_client.initialize().await.map_err(VfsError::from)?;

        Self::with_meta_layer_with_compact_config(layout, store, meta_client, config.compact)
    }
}

impl<S, R> VFS<S, MetaClient<R>>
where
    S: BlockStore + Send + Sync + 'static,
    R: MetaStore + Send + Sync + ?Sized + 'static,
{
    pub(crate) fn with_meta_layer_with_compact_config(
        layout: ChunkLayout,
        store: Arc<S>,
        meta_layer: Arc<MetaClient<R>>,
        compact_config: CompactConfig,
    ) -> Result<Self, VfsError> {
        Self::with_meta_layer_with_cache_config(
            layout,
            store,
            meta_layer,
            compact_config,
            CacheConfig::default(),
        )
    }

    pub(crate) fn with_meta_layer_with_cache_config(
        layout: ChunkLayout,
        store: Arc<S>,
        meta_layer: Arc<MetaClient<R>>,
        compact_config: CompactConfig,
        cache_config: CacheConfig,
    ) -> Result<Self, VfsError> {
        let enabled = !meta_layer.options().no_background_jobs;
        let bg_config = VfsBackgroundConfig::from_compact_config(&layout, compact_config, enabled);
        let background_tasks =
            Self::start_background_tasks(&meta_layer, Arc::clone(&store), layout, bg_config);

        Self::from_components_with_background(
            VFSConfig::new_with_cache_config(layout, cache_config),
            store,
            meta_layer,
            background_tasks,
        )
    }

    pub(crate) fn with_meta_layer_with_default_background(
        layout: ChunkLayout,
        store: Arc<S>,
        meta_layer: Arc<MetaClient<R>>,
    ) -> Result<Self, VfsError> {
        Self::with_meta_layer_with_compact_config(
            layout,
            store,
            meta_layer,
            CompactConfig::default(),
        )
    }

    /// Start background compaction and gc tasks
    fn start_background_tasks(
        meta_client: &Arc<MetaClient<R>>,
        block_store: Arc<S>,
        layout: ChunkLayout,
        config: VfsBackgroundConfig,
    ) -> Option<VfsBackgroundTasks> {
        if !config.enabled {
            return None;
        }

        let meta_store = meta_client.store();
        let is_database_store = meta_store.name() == "database";

        let mut worker = CompactionWorker::with_config(
            meta_store,
            block_store,
            layout,
            config.compact_config.clone(),
            config.compact_config.lock_ttl.clone(),
        );

        if is_database_store {
            let client = Arc::clone(meta_client);
            worker = worker.with_compaction_hook(Arc::new(move |chunk_id| {
                let client = Arc::clone(&client);
                tokio::spawn(async move {
                    client.invalidate_chunk_slices(chunk_id).await;
                });
            }));
        }
        let (compaction_handle, gc_handle) = worker.start(config.compaction, config.gc);

        Some(VfsBackgroundTasks {
            compaction_handle,
            gc_handle,
        })
    }
}

#[allow(dead_code)]
impl<S, M> VFS<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    fn validate_entry_name(name: &str) -> Result<(), VfsError> {
        if name.is_empty() || name.contains('/') || name.contains('\0') {
            return Err(VfsError::InvalidFilename);
        }

        if name.len() > NAME_MAX {
            return Err(VfsError::FilenameTooLong {
                path: PathHint::none(),
            });
        }

        Ok(())
    }

    fn from_components_with_background(
        config: VFSConfig,
        store: Arc<S>,
        meta_layer: Arc<M>,
        background_tasks: Option<VfsBackgroundTasks>,
    ) -> Result<Self, VfsError> {
        let layout = config.write.layout;
        let root_ino = meta_layer.root_ino();
        let meta_metrics = meta_layer.metrics();
        let backend = Arc::new(Backend::new(store.clone(), meta_layer.clone()));
        let core = Arc::new(VfsCore::new(layout, backend.clone(), meta_layer, root_ino));
        let config = Arc::new(config);
        let state = Arc::new(VfsState::new(config, backend));

        // Background statistics logger — JuiceFS-stats equivalent.
        let fuse_stats = state.stats.clone();
        let (cache_hits, cache_misses) = store.cache_counters();
        let object_metrics = store.object_store_metrics();
        let memory_budget = state.memory_budget.clone();
        let writer = state.writer.clone();
        if cache_hits.is_some()
            || cache_misses.is_some()
            || object_metrics.is_some()
            || meta_metrics.is_some()
            || memory_budget.is_some()
        {
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(100));
                let mut prev_reads: u64 = 0;
                let mut prev_bytes: u64 = 0;
                let mut prev_lat_us: u64 = 0;
                loop {
                    interval.tick().await;
                    if let (Some(hits), Some(misses)) = (&cache_hits, &cache_misses) {
                        fuse_stats.sync_cache_counters(
                            hits.load(std::sync::atomic::Ordering::Relaxed),
                            misses.load(std::sync::atomic::Ordering::Relaxed),
                        );
                    }
                    if let Some(memory_budget) = &memory_budget {
                        fuse_stats.sync_buffer_bytes(
                            memory_budget.writer_bytes(),
                            memory_budget.reader_bytes(),
                        );
                    }
                    let dirty = writer.dirty_breakdown().await;
                    fuse_stats.sync_writeback_dirty_breakdown(
                        dirty.live_bytes,
                        dirty.live_slices,
                        dirty.recently_committed_pending_upload_bytes,
                        dirty.recently_committed_pending_upload_slices,
                        dirty.recently_committed_uploaded_bytes,
                        dirty.recently_committed_uploaded_slices,
                    );
                    fuse_stats.sync_writeback_live_origin_metrics(
                        dirty.live_normal_only_bytes,
                        dirty.live_normal_only_slices,
                        dirty.live_cached_only_bytes,
                        dirty.live_cached_only_slices,
                        dirty.live_mixed_origin_bytes,
                        dirty.live_mixed_origin_slices,
                        dirty.live_unknown_origin_bytes,
                        dirty.live_unknown_origin_slices,
                    );
                    fuse_stats.sync_writeback_backpressure_metrics(
                        dirty.backpressure_soft_sleep_ops,
                        dirty.backpressure_soft_sleep_us,
                        dirty.backpressure_hard_wait_ops,
                        dirty.backpressure_hard_wait_us,
                    );
                    fuse_stats.sync_writeback_buffer_backpressure_metrics(
                        dirty.buffer_soft_sleep_ops,
                        dirty.buffer_soft_sleep_us,
                        dirty.buffer_moderate_sleep_ops,
                        dirty.buffer_moderate_sleep_us,
                        dirty.buffer_hard_sleep_ops,
                        dirty.buffer_hard_sleep_us,
                    );
                    fuse_stats.sync_writeback_phase_metrics(
                        dirty.stage_inflight_bytes,
                        dirty.remote_upload_inflight_bytes,
                        dirty.stage_ops,
                        dirty.stage_bytes,
                        dirty.stage_us,
                        dirty.stage_failures,
                        dirty.commit_before_stage_ops,
                    );
                    fuse_stats.sync_writeback_commit_wait_metrics(
                        dirty.commit_wait_upload_ops,
                        dirty.commit_wait_upload_us,
                        dirty.commit_wait_retry_ops,
                        dirty.commit_wait_retry_us,
                    );
                    fuse_stats.sync_writeback_flush_wait_metrics(
                        dirty.flush_wait_ops,
                        dirty.flush_wait_us,
                        dirty.flush_wait_slices,
                    );
                    fuse_stats.sync_writeback_flush_fragmentation_metrics(
                        dirty.flush_fragmentation_ops,
                        dirty.flush_fragmentation_slices,
                        dirty.flush_fragmentation_bytes,
                        dirty.flush_fragmentation_cached_sub_block_slices,
                        dirty.flush_fragmentation_cached_sub_block_bytes,
                        dirty.flush_fragmentation_full_block_slices,
                        dirty.flush_fragmentation_full_block_bytes,
                    );
                    fuse_stats.sync_writeback_commit_wait_breakdown_metrics(
                        dirty.commit_wait_upload_size_ops,
                        dirty.commit_wait_upload_size_us,
                        dirty.commit_wait_upload_max_unflushed_ops,
                        dirty.commit_wait_upload_max_unflushed_us,
                        dirty.commit_wait_upload_explicit_flush_ops,
                        dirty.commit_wait_upload_explicit_flush_us,
                        dirty.commit_wait_upload_auto_ops,
                        dirty.commit_wait_upload_auto_us,
                        dirty.commit_wait_upload_commit_age_ops,
                        dirty.commit_wait_upload_commit_age_us,
                        dirty.commit_wait_upload_unknown_reason_ops,
                        dirty.commit_wait_upload_unknown_reason_us,
                        dirty.commit_wait_upload_normal_only_ops,
                        dirty.commit_wait_upload_normal_only_us,
                        dirty.commit_wait_upload_cached_only_ops,
                        dirty.commit_wait_upload_cached_only_us,
                        dirty.commit_wait_upload_mixed_origin_ops,
                        dirty.commit_wait_upload_mixed_origin_us,
                        dirty.commit_wait_upload_unknown_origin_ops,
                        dirty.commit_wait_upload_unknown_origin_us,
                    );
                    fuse_stats.sync_writeback_slice_selection_metrics(
                        dirty.slice_create_ops,
                        dirty.slice_reuse_ops,
                        dirty.slice_reject_older_unique_ops,
                        dirty.slice_reject_dispatched_prefix_ops,
                    );
                    fuse_stats.sync_writeback_freeze_metrics(
                        dirty.freeze_size_ops,
                        dirty.freeze_size_bytes,
                        dirty.freeze_max_unflushed_ops,
                        dirty.freeze_max_unflushed_bytes,
                        dirty.freeze_explicit_flush_ops,
                        dirty.freeze_explicit_flush_bytes,
                        dirty.freeze_auto_ops,
                        dirty.freeze_auto_bytes,
                        dirty.freeze_commit_age_ops,
                        dirty.freeze_commit_age_bytes,
                    );
                    fuse_stats.sync_writeback_upload_batch_metrics(
                        dirty.upload_batch_ops,
                        dirty.upload_batch_bytes,
                        dirty.upload_batch_blocks,
                        dirty.upload_partial_tail_ops,
                        dirty.upload_partial_tail_size_ops,
                        dirty.upload_partial_tail_max_unflushed_ops,
                        dirty.upload_partial_tail_explicit_flush_ops,
                        dirty.upload_partial_tail_auto_ops,
                        dirty.upload_partial_tail_auto_age_ops,
                        dirty.upload_partial_tail_auto_idle_ops,
                        dirty.upload_partial_tail_auto_pressure_ops,
                        dirty.upload_partial_tail_auto_too_many_ops,
                        dirty.upload_partial_tail_auto_buffer_high_ops,
                        dirty.upload_partial_tail_auto_flush_duration_ops,
                        dirty.upload_partial_tail_auto_unknown_ops,
                        dirty.upload_partial_tail_commit_age_ops,
                    );
                    fuse_stats.sync_writeback_upload_batch_shape_metrics(
                        dirty.upload_batch_single_block_ops,
                        dirty.upload_batch_multi_block_ops,
                    );
                    fuse_stats.sync_writeback_upload_origin_metrics(
                        dirty.upload_partial_tail_normal_only_ops,
                        dirty.upload_partial_tail_cached_only_ops,
                        dirty.upload_partial_tail_mixed_origin_ops,
                        dirty.upload_partial_tail_unknown_origin_ops,
                        dirty.upload_partial_tail_auto_normal_only_ops,
                        dirty.upload_partial_tail_auto_cached_only_ops,
                        dirty.upload_partial_tail_auto_mixed_origin_ops,
                        dirty.upload_partial_tail_auto_unknown_origin_ops,
                    );
                    if let Some(object_metrics) = &object_metrics {
                        let object = object_metrics.snapshot();
                        fuse_stats.sync_object_store_metrics(
                            object.get_ops,
                            object.get_bytes,
                            object.get_lat_us,
                            object.put_ops,
                            object.put_bytes,
                            object.put_lat_us,
                            object.put_prepare_lat_us,
                            object.put_cache_lat_us,
                            object.del_ops,
                        );
                        fuse_stats.sync_read_strategy_metrics(
                            object.read_block_cache_hits,
                            object.read_page_cache_hits,
                            object.read_page_cache_misses,
                            object.read_range_gets,
                            object.read_full_gets,
                            object.read_piggyback_full,
                            object.read_background_prefetches,
                            object.read_background_prefetch_dropped,
                        );
                    }
                    if let Some(meta_metrics) = &meta_metrics {
                        let meta = meta_metrics.snapshot();
                        fuse_stats.sync_meta_client_metrics(
                            meta.stat_cache_hit,
                            meta.stat_cache_miss,
                            meta.stat_fresh_store_hit,
                            meta.lookup_cache_hit,
                            meta.lookup_cache_miss,
                            meta.get_slices_cache_hit,
                            meta.get_slices_cache_miss,
                            meta.open_fresh_stat,
                            meta.open_file_cache_hit,
                            meta.open_file_cache_miss,
                            meta.lookup_attr_fused_hit,
                            meta.lookup_attr_fused_miss,
                            meta.lookup_attr_fused_error,
                        );
                    }

                    let snapshot = fuse_stats.snapshot();
                    let reads = snapshot.fuse_read_ops;
                    let bytes = snapshot.fuse_read_bytes;
                    let lat_us = snapshot.fuse_read_lat_us;
                    let reads_delta = reads.saturating_sub(prev_reads);
                    let bytes_delta = bytes.saturating_sub(prev_bytes);
                    let lat_delta = lat_us.saturating_sub(prev_lat_us);
                    let avg_sz = bytes_delta.checked_div(reads_delta).unwrap_or(0);
                    let avg_lat_us = lat_delta.checked_div(reads_delta).unwrap_or(0);
                    prev_reads = reads;
                    prev_bytes = bytes;
                    prev_lat_us = lat_us;
                    tracing::info!(
                        hits = snapshot.cache_hits,
                        misses = snapshot.cache_misses,
                        total = snapshot.cache_requests(),
                        hit_pct = snapshot.cache_hit_ratio() * 100.0,
                        dirty_bytes = snapshot.buf_dirty_bytes,
                        read_buffer_bytes = snapshot.buf_read_bytes,
                        s3_get_ops = snapshot.s3_get_ops,
                        s3_put_ops = snapshot.s3_put_ops,
                        s3_del_ops = snapshot.s3_del_ops,
                        fuse_reads = reads,
                        fuse_rd_bytes = bytes,
                        avg_read_sz = avg_sz,
                        avg_read_lat_us = avg_lat_us,
                        "stats"
                    );
                }
            });
        }

        Ok(Self {
            core,
            state,
            background_tasks,
        })
    }

    pub(crate) fn root_ino(&self) -> i64 {
        self.core.root
    }

    /// Access the shared statistics counters.
    pub fn stats(&self) -> &Arc<crate::vfs::stats::FsStats> {
        &self.state.stats
    }

    fn vfs_timing_timer<'a>(
        &'a self,
        ops_counter: &'a AtomicU64,
        lat_counter: &'a AtomicU64,
    ) -> crate::vfs::stats::MaybeOpTimer<'a> {
        crate::vfs::stats::MaybeOpTimer::new(
            self.state.vfs_timing_enabled,
            ops_counter,
            lat_counter,
        )
    }

    pub(crate) fn meta_layer(&self) -> &M {
        self.core.meta_layer.as_ref()
    }

    pub(crate) fn meta_layer_arc(&self) -> Arc<M> {
        Arc::clone(&self.core.meta_layer)
    }

    fn file_handle(&self, fh: u64) -> Option<Arc<FileHandle<S, M>>> {
        self.state.handles.get(fh)
    }

    fn file_handle_required(&self, fh: u64) -> Result<Arc<FileHandle<S, M>>, VfsError> {
        self.file_handle(fh).ok_or(VfsError::StaleNetworkFileHandle)
    }

    pub(crate) fn mark_handle_write_dirty(&self, fh: u64) -> bool {
        self.state.handles.mark_write_dirty(fh)
    }

    pub(crate) fn begin_fuse_cached_write(&self, ino: i64, unique: u64) -> FuseCachedWriteGuard {
        self.state.fuse_cached_write_order.begin(ino, unique)
    }

    pub(crate) async fn wait_for_prior_fuse_cached_writes(&self, ino: i64, unique: u64) {
        self.state
            .fuse_cached_write_order
            .wait_for_prior(ino, unique)
            .await;
    }

    pub(crate) fn handle_allows_write_for_inode(&self, fh: u64, ino: i64) -> bool {
        self.file_handle(fh)
            .map(|handle| handle.ino == ino && handle.flags.write)
            .unwrap_or(false)
    }

    fn file_handles_for_inode(&self, ino: i64) -> Vec<Arc<FileHandle<S, M>>> {
        self.state
            .handles
            .handles_for(ino)
            .into_iter()
            .filter_map(|fh| self.file_handle(fh))
            .collect()
    }

    fn dir_handle(&self, fh: u64) -> Option<Arc<DirHandle>> {
        self.state.handles.get_dir(fh)
    }

    fn release_dir_handle_required(&self, fh: u64) -> Result<Arc<DirHandle>, VfsError> {
        self.state
            .handles
            .release_dir(fh)
            .ok_or(VfsError::StaleNetworkFileHandle)
    }

    pub(crate) fn inode_size_cached(&self, ino: i64) -> Option<u64> {
        self.state.inodes.get(&ino).map(|inode| inode.file_size())
    }

    fn extend_local_file_size(&self, ino: i64, min_size: u64) {
        if let Some(inode) = self.state.inodes.get(&ino)
            && min_size > inode.file_size()
        {
            inode.extend_size(min_size);
        }

        for handle in self.file_handles_for_inode(ino) {
            handle.extend_size(min_size);
        }
    }

    pub(crate) async fn inode_size(&self, ino: i64) -> Result<u64, VfsError> {
        if let Some(size) = self.inode_size_cached(ino) {
            return Ok(size);
        }

        let attr = self.meta_stat_required(ino, PathHint::none()).await?;
        Ok(attr.size)
    }

    async fn try_sparse_zero_extend(
        &self,
        ino: i64,
        offset: u64,
        data: &[u8],
        visible_size: u64,
    ) -> Result<bool, VfsError> {
        // JuiceFS stores holes with slice id 0. BrewFS slice precedence is based
        // on monotonically increasing slice ids, so sparse-zero writes stay a
        // VFS fast path instead of materializing id-0 slice metadata.
        if data.is_empty()
            || data.len() > SPARSE_ZERO_MAX_SCAN_BYTES
            || data.iter().any(|&byte| byte != 0)
        {
            return Ok(false);
        }

        if self.state.writer.has_dirty_state(ino as u64).await {
            return Ok(false);
        }

        let new_end = offset
            .checked_add(data.len() as u64)
            .ok_or(VfsError::InvalidInput)?;

        if offset < visible_size
            && self
                .range_has_committed_slices(ino, offset, data.len())
                .await?
        {
            return Ok(false);
        }

        if new_end > visible_size {
            self.meta_extend_file_size(ino, new_end).await?;
        }

        if let Some(inode) = self.state.inodes.get(&ino) {
            inode.extend_size(new_end);
            inode.extend_committed_size(new_end);
        }
        self.extend_local_file_size(ino, new_end);

        let _ = self
            .state
            .reader
            .invalidate(ino as u64, offset, data.len())
            .await;

        Ok(true)
    }

    async fn range_has_committed_slices(
        &self,
        ino: i64,
        offset: u64,
        len: usize,
    ) -> Result<bool, VfsError> {
        let layout = self.core.layout;
        for span in split_chunk_spans(layout, offset, len) {
            let chunk_id = chunk_id_for(ino, span.index).map_err(VfsError::from)?;
            let span_start = span.offset;
            let span_end = span.offset + span.len;
            let slices = self
                .meta_layer()
                .get_slices(chunk_id)
                .await
                .map_err(|err| VfsError::from_meta(PathHint::none(), err))?;

            if slices.iter().any(|slice| {
                let slice_start = slice.offset;
                let slice_end = slice.offset + slice.length;
                slice_start < span_end && span_start < slice_end
            }) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// get the node's parent inode.
    pub async fn parent_of(&self, ino: i64) -> Option<i64> {
        self.meta_get_dir_parent(ino).await.ok().flatten()
    }

    /// get the node's fullpath.
    pub async fn path_of(&self, ino: i64) -> Option<String> {
        self.meta_get_paths(ino)
            .await
            .ok()
            .and_then(|paths| paths.into_iter().next())
    }

    /// get all known paths for a node.
    pub async fn paths_of(&self, ino: i64) -> Result<Vec<String>, VfsError> {
        self.meta_get_paths(ino).await
    }

    /// get the node's child inode by name.
    pub(crate) async fn child_of(&self, parent: i64, name: &str) -> Option<i64> {
        self.meta_lookup(parent, name).await.ok().flatten()
    }

    /// get the node's child inode and attributes by name.
    pub(crate) async fn child_attr_of(
        &self,
        parent: i64,
        name: &str,
    ) -> Result<Option<(i64, FileAttr)>, VfsError> {
        let Some((ino, mut attr)) = self.meta_lookup_with_attr(parent, name).await? else {
            return Ok(None);
        };

        // close-to-open semantics: if there is a local state, it should be considered as the newest state.
        if let Some(size) = self.inode_size_cached(ino) {
            attr.size = size;
        }

        tracing::debug!(ino, nlink = attr.nlink, kind = ?attr.kind, "child_attr_of");
        Ok(Some((ino, attr)))
    }

    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    pub(crate) async fn stat_ino(&self, ino: i64) -> Option<FileAttr> {
        let mut attr = self.meta_stat(ino).await.ok().flatten()?;

        // close-to-open semantics: if there is a local state, it should be considered as the newest state.
        if let Some(size) = self.inode_size_cached(ino) {
            attr.size = size;
        }

        tracing::debug!(ino, nlink = attr.nlink, kind = ?attr.kind, "stat_ino");
        Some(attr)
    }

    pub(crate) fn blocks_for_attr(&self, attr: &FileAttr) -> u64 {
        if let Some(inode) = self.state.inodes.get(&attr.ino)
            && let Some(blocks) = inode.allocated_blocks_512()
        {
            return blocks;
        }
        // Fall back to the metadata-provided value.  For backends that haven't
        // implemented accurate block tracking yet, this is `size.div_ceil(512)`.
        attr.blocks
    }

    /// Returns the current time as nanoseconds since UNIX_EPOCH.
    fn current_timestamp_nanos() -> Result<i64, VfsError> {
        use std::time::{SystemTime, UNIX_EPOCH};
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| VfsError::Other)?
            .as_nanos() as i64)
    }

    /// Update atime (access time) for an inode to current time
    pub(crate) async fn update_atime(&self, ino: i64) -> Result<(), VfsError> {
        let now = Self::current_timestamp_nanos()?;

        let req = SetAttrRequest {
            atime: Some(now),
            ..Default::default()
        };

        self.meta_set_attr(ino, &req, SetAttrFlags::empty()).await?;

        // Update handle cache if exists
        if let Some(mut attr) = self.state.handles.attr_for_inode(ino) {
            attr.atime = now;
            self.state.handles.update_attr_for_inode(ino, &attr);
        }

        Ok(())
    }

    /// Update mtime and ctime for an inode to current time
    /// This is called during flush/fsync to handle mmap writes where the kernel
    /// doesn't call the write() callback
    pub(crate) async fn update_mtime_ctime(&self, ino: i64) -> Result<(), VfsError> {
        let now = Self::current_timestamp_nanos()?;
        let local_size = self.inode_size_cached(ino);

        let req = SetAttrRequest {
            size: local_size,
            mtime: Some(now),
            ctime: Some(now),
            ..Default::default()
        };

        let mut attr = self.meta_set_attr(ino, &req, SetAttrFlags::empty()).await?;
        if let Some(size) = local_size {
            attr.size = attr.size.max(size);
            if let Some(inode) = self.state.inodes.get(&ino) {
                inode.extend_committed_size(size);
            }
        }

        // Update handle cache if exists
        if let Some(mut cached_attr) = self.state.handles.attr_for_inode(ino) {
            if let Some(size) = local_size {
                cached_attr.size = cached_attr.size.max(size);
            }
            cached_attr.size = cached_attr.size.max(attr.size);
            cached_attr.mtime = now;
            cached_attr.ctime = now;
            self.state.handles.update_attr_for_inode(ino, &cached_attr);
        }

        Ok(())
    }

    /// List directory entries by inode
    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    pub(crate) async fn readdir_ino(&self, ino: i64) -> Option<Vec<DirEntry>> {
        let meta_entries = self.meta_readdir(ino).await.ok()?;

        let entries: Vec<DirEntry> = meta_entries
            .into_iter()
            .map(|e| DirEntry {
                name: e.name,
                ino: e.ino,
                kind: e.kind,
            })
            .collect();
        Some(entries)
    }

    /// Normalize a path by stripping redundant separators and ensuring it starts with `/`.
    /// Does not resolve `.` or `..`.
    fn norm_path(p: &str) -> String {
        if p.is_empty() {
            return "/".into();
        }
        let parts: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
        let mut out = String::from("/");
        out.push_str(&parts.join("/"));
        if out.is_empty() { "/".into() } else { out }
    }

    /// Split a normalized path into parent directory and basename.
    fn split_dir_file(path: &str) -> (String, String) {
        let n = path.rfind('/').unwrap_or(0);
        if n == 0 {
            ("/".into(), path[1..].into())
        } else {
            (path[..n].into(), path[n + 1..].into())
        }
    }

    /// Recursively create directories (mkdir -p behavior).
    /// - If an intermediate component exists as a file, return "not a directory".
    /// - Idempotent: existing directories simply return their inode.
    /// - Returns the inode of the target directory.
    #[tracing::instrument(level = "trace", skip(self), fields(path))]
    pub async fn mkdir_p(&self, path: &str) -> Result<i64, VfsError> {
        let path = Self::norm_path(path);
        if &path == "/" {
            return Ok(self.core.root);
        }
        if let Some((ino, _attr)) = self.meta_lookup_path(&path).await? {
            return Ok(ino);
        }
        let mut cur_ino = self.core.root;
        for part in path.trim_start_matches('/').split('/') {
            if part.is_empty() {
                continue;
            }
            let child = self.meta_lookup(cur_ino, part).await?;
            match child {
                Some(ino) => {
                    let attr = self
                        .meta_stat_required(ino, PathHint::some(path.as_str()))
                        .await?;
                    if attr.kind != FileType::Dir {
                        return Err(VfsError::NotADirectory {
                            path: PathHint::some(path.as_str()),
                        });
                    }
                    cur_ino = ino;
                }
                None => {
                    let ino = self.meta_mkdir(cur_ino, part.to_string()).await?;
                    cur_ino = ino;
                }
            }
        }
        Ok(cur_ino)
    }

    /// Create a single directory (non-recursive).
    ///
    /// - Parent directory must exist.
    /// - If the target already exists as a directory, returns its inode.
    /// - If the target exists as a non-directory, returns `AlreadyExists`.
    /// - If parent does not exist, returns `NotFound`.
    pub async fn mkdir_err(&self, path: &str) -> Result<i64, VfsError> {
        let path = Self::norm_path(path);
        if path == "/" {
            return Ok(self.core.root);
        }

        let (dir, name) = Self::split_dir_file(&path);
        if name.is_empty() {
            return Err(VfsError::InvalidFilename);
        }

        let parent_ino = self.resolve_parent_inode(&dir).await?;

        self.mkdir_at(parent_ino, &name).await
    }

    /// Create a regular file in an existing parent directory (std-like behavior).
    ///
    /// - Does not create parent directories.
    /// - If the target exists and `create_new` is true, returns `AlreadyExists`.
    /// - If the target exists as a directory, returns `IsADirectory`.
    pub async fn create_file_in_existing_dir_err(
        &self,
        path: &str,
        create_new: bool,
    ) -> Result<i64, VfsError> {
        let path = Self::norm_path(path);
        if path == "/" {
            return Err(VfsError::IsADirectory { path: path.into() });
        }

        let (dir, name) = Self::split_dir_file(&path);
        if name.is_empty() {
            return Err(VfsError::InvalidFilename);
        }

        let parent_ino = self.resolve_parent_inode(&dir).await?;

        self.create_file_at(parent_ino, &name, create_new).await
    }

    /// Create a regular file (running `mkdir_p` on its parent if needed).
    /// - If a directory with the same name exists, returns "is a directory".
    /// - If the file already exists, returns its inode instead of creating a new one.
    #[tracing::instrument(level = "trace", skip(self), fields(path))]
    pub async fn create_file(&self, path: &str) -> Result<i64, VfsError> {
        let path = Self::norm_path(path);
        let (dir, name) = Self::split_dir_file(&path);
        let dir_ino = self.mkdir_p(&dir).await?;
        self.create_file_at(dir_ino, &name, false).await
    }

    /// Create a hard link using inode numbers directly, avoiding path reconstruction.
    /// This is the preferred path from FUSE which already has the inodes.
    #[tracing::instrument(level = "debug", skip(self), fields(src_ino, parent_ino, name))]
    pub(crate) async fn link_by_ino(
        &self,
        src_ino: i64,
        parent_ino: i64,
        name: &str,
    ) -> Result<FileAttr, VfsError> {
        Self::validate_entry_name(name)?;

        let attr = self.meta_link(src_ino, parent_ino, name).await?;

        Ok(attr)
    }

    /// Create a directory using a parent inode and entry name directly.
    #[tracing::instrument(level = "debug", skip(self), fields(parent_ino, name))]
    async fn mkdir_at_inner(
        &self,
        parent_ino: i64,
        name: &str,
        existing_dir_ok: bool,
    ) -> Result<i64, VfsError> {
        Self::validate_entry_name(name)?;

        match self.meta_mkdir(parent_ino, name.to_string()).await {
            Ok(ino) => Ok(ino),
            Err(VfsError::AlreadyExists { .. }) => {
                if !existing_dir_ok {
                    return Err(VfsError::AlreadyExists {
                        path: PathHint::none(),
                    });
                }

                let Some(existing) = self.meta_lookup(parent_ino, name).await? else {
                    return Err(VfsError::AlreadyExists {
                        path: PathHint::none(),
                    });
                };
                let attr = self.meta_stat_required(existing, PathHint::none()).await?;
                if attr.kind == FileType::Dir {
                    Ok(existing)
                } else {
                    Err(VfsError::AlreadyExists {
                        path: PathHint::none(),
                    })
                }
            }
            Err(VfsError::NotFound { path }) => {
                if let Some(parent_attr) = self.meta_stat(parent_ino).await?
                    && parent_attr.kind != FileType::Dir
                {
                    return Err(VfsError::NotADirectory {
                        path: PathHint::none(),
                    });
                }
                Err(VfsError::NotFound { path })
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn mkdir_at(&self, parent_ino: i64, name: &str) -> Result<i64, VfsError> {
        self.mkdir_at_inner(parent_ino, name, true).await
    }

    pub(crate) async fn mkdir_at_new(&self, parent_ino: i64, name: &str) -> Result<i64, VfsError> {
        self.mkdir_at_inner(parent_ino, name, false).await
    }

    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(parent_ino, name, create_new, has_attrs = create_attrs.is_some())
    )]
    async fn create_file_at_inner(
        &self,
        parent_ino: i64,
        name: &str,
        create_new: bool,
        create_attrs: Option<(u32, u32, u32)>,
    ) -> Result<CreateFileAtResult, VfsError> {
        let _total_timer = self.vfs_timing_timer(
            &self.stats().vfs_create_total_ops,
            &self.stats().vfs_create_total_lat_us,
        );
        Self::validate_entry_name(name)?;

        let create_result = {
            let _meta_timer = self.vfs_timing_timer(
                &self.stats().vfs_create_meta_ops,
                &self.stats().vfs_create_meta_lat_us,
            );
            if let Some((mode, uid, gid)) = create_attrs {
                match self
                    .meta_create_node_with_attr(
                        parent_ino,
                        name.to_string(),
                        FileType::File,
                        mode,
                        uid,
                        gid,
                        0,
                    )
                    .await
                {
                    Ok(created) => Ok((created.ino, created.attr, true)),
                    Err(VfsError::Unsupported) => self
                        .meta_create_file_with_attr(parent_ino, name.to_string())
                        .await
                        .map(|created| (created.ino, created.attr, false)),
                    Err(err) => Err(err),
                }
            } else {
                self.meta_create_file_with_attr(parent_ino, name.to_string())
                    .await
                    .map(|created| (created.ino, created.attr, false))
            }
        };

        match create_result {
            Ok((ino, attr, attrs_applied)) => Ok(CreateFileAtResult {
                ino,
                attr,
                created: true,
                attrs_applied,
            }),
            Err(VfsError::AlreadyExists { .. }) => {
                if create_new {
                    return Err(VfsError::AlreadyExists {
                        path: PathHint::none(),
                    });
                }
                let existing = self.meta_lookup(parent_ino, name).await?.ok_or_else(|| {
                    VfsError::AlreadyExists {
                        path: PathHint::none(),
                    }
                })?;
                let attr = self.meta_stat_required(existing, PathHint::none()).await?;
                if attr.kind == FileType::Dir {
                    Err(VfsError::IsADirectory {
                        path: PathHint::none(),
                    })
                } else {
                    Ok(CreateFileAtResult {
                        ino: existing,
                        attr: Some(attr),
                        created: false,
                        attrs_applied: false,
                    })
                }
            }
            Err(VfsError::NotFound { path }) => {
                if let Some(parent_attr) = self.meta_stat(parent_ino).await?
                    && parent_attr.kind != FileType::Dir
                {
                    return Err(VfsError::NotADirectory {
                        path: PathHint::none(),
                    });
                }
                Err(VfsError::NotFound { path })
            }
            Err(err) => Err(err),
        }
    }

    /// Create or open a regular file using a parent inode and entry name directly.
    #[tracing::instrument(level = "debug", skip(self), fields(parent_ino, name, create_new))]
    pub(crate) async fn create_file_at(
        &self,
        parent_ino: i64,
        name: &str,
        create_new: bool,
    ) -> Result<i64, VfsError> {
        Ok(self
            .create_file_at_inner(parent_ino, name, create_new, None)
            .await?
            .ino)
    }

    /// Create or open a regular file and ask capable metadata backends to apply
    /// mode/uid/gid in the same metadata mutation.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(parent_ino, name, create_new, mode, uid, gid)
    )]
    pub(crate) async fn create_file_at_with_attrs(
        &self,
        parent_ino: i64,
        name: &str,
        create_new: bool,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<CreateFileAtResult, VfsError> {
        self.create_file_at_inner(
            parent_ino,
            name,
            create_new,
            Some((mode & 0o7777, uid, gid)),
        )
        .await
    }

    /// Create a non-directory special node using a parent inode and entry name.
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(level = "debug", skip(self), fields(parent_ino, name, kind = ?kind, mode, rdev))]
    pub(crate) async fn create_special_node_at(
        &self,
        parent_ino: i64,
        name: &str,
        kind: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
    ) -> Result<i64, VfsError> {
        if name.is_empty() || name.contains('/') || name.contains('\0') {
            return Err(VfsError::InvalidFilename);
        }
        if !kind.is_special_node() {
            return Err(VfsError::InvalidInput);
        }

        self.meta_create_node(parent_ino, name.to_string(), kind, mode, uid, gid, rdev)
            .await
    }

    /// Create a symbolic link using a parent inode and entry name directly.
    #[tracing::instrument(level = "debug", skip(self), fields(parent_ino, name))]
    pub(crate) async fn create_symlink_at(
        &self,
        parent_ino: i64,
        name: &str,
        target: &str,
    ) -> Result<(i64, FileAttr), VfsError> {
        Self::validate_entry_name(name)?;

        let parent_attr = self
            .meta_stat_required(parent_ino, PathHint::none())
            .await?;
        if parent_attr.kind != FileType::Dir {
            return Err(VfsError::NotADirectory {
                path: PathHint::none(),
            });
        }

        if self.meta_lookup(parent_ino, name).await?.is_some() {
            return Err(VfsError::AlreadyExists {
                path: PathHint::none(),
            });
        }

        let result = self.meta_symlink(parent_ino, name, target).await?;
        Ok(result)
    }

    /// Remove a regular file or symlink using parent inode and name directly.
    #[tracing::instrument(level = "debug", skip(self), fields(parent_ino, name))]
    pub(crate) async fn unlink_at(&self, parent_ino: i64, name: &str) -> Result<(), VfsError> {
        let _total_timer = self.vfs_timing_timer(
            &self.stats().vfs_unlink_total_ops,
            &self.stats().vfs_unlink_total_lat_us,
        );
        Self::validate_entry_name(name)?;

        let ino = {
            let _lookup_timer = self.vfs_timing_timer(
                &self.stats().vfs_unlink_lookup_ops,
                &self.stats().vfs_unlink_lookup_lat_us,
            );
            self.meta_lookup_required(parent_ino, name, PathHint::none())
                .await?
        };
        let attr = {
            let _stat_timer = self.vfs_timing_timer(
                &self.stats().vfs_unlink_stat_ops,
                &self.stats().vfs_unlink_stat_lat_us,
            );
            self.meta_stat_required(ino, PathHint::none()).await?
        };
        if attr.kind == FileType::Dir {
            return Err(VfsError::IsADirectory {
                path: PathHint::none(),
            });
        }

        self.unlink_at_with_known_attr_inner(parent_ino, name, ino, attr)
            .await
    }

    /// Remove a regular file or symlink after the caller has already resolved
    /// the child inode and attributes. This avoids duplicate lookup/stat work
    /// on the FUSE unlink path while keeping store-level unlink checks.
    #[tracing::instrument(level = "debug", skip(self, attr), fields(parent_ino, name, ino))]
    pub(crate) async fn unlink_at_with_known_attr(
        &self,
        parent_ino: i64,
        name: &str,
        ino: i64,
        attr: FileAttr,
    ) -> Result<(), VfsError> {
        let _total_timer = self.vfs_timing_timer(
            &self.stats().vfs_unlink_total_ops,
            &self.stats().vfs_unlink_total_lat_us,
        );
        Self::validate_entry_name(name)?;

        self.unlink_at_with_known_attr_inner(parent_ino, name, ino, attr)
            .await
    }

    async fn unlink_at_with_known_attr_inner(
        &self,
        parent_ino: i64,
        name: &str,
        ino: i64,
        attr: FileAttr,
    ) -> Result<(), VfsError> {
        if attr.kind == FileType::Dir {
            return Err(VfsError::IsADirectory {
                path: PathHint::none(),
            });
        }

        {
            let _meta_timer = self.vfs_timing_timer(
                &self.stats().vfs_unlink_meta_ops,
                &self.stats().vfs_unlink_meta_lat_us,
            );
            self.meta_unlink(parent_ino, name).await?;
        }
        {
            let _recent_timer = self.vfs_timing_timer(
                &self.stats().vfs_unlink_recent_ops,
                &self.stats().vfs_unlink_recent_lat_us,
            );
            self.remember_recently_unlinked_attr(ino, attr);
        }
        self.discard_closed_unlinked_inode_state(ino).await;
        Ok(())
    }

    /// Remove an empty directory using parent inode and name directly.
    #[tracing::instrument(level = "debug", skip(self), fields(parent_ino, name))]
    pub(crate) async fn rmdir_at(&self, parent_ino: i64, name: &str) -> Result<(), VfsError> {
        Self::validate_entry_name(name)?;

        let ino = self
            .meta_lookup_required(parent_ino, name, PathHint::none())
            .await?;
        let attr = self.meta_stat_required(ino, PathHint::none()).await?;
        if attr.kind != FileType::Dir {
            return Err(VfsError::NotADirectory {
                path: PathHint::none(),
            });
        }
        if !self.meta_readdir(ino).await?.is_empty() {
            return Err(VfsError::DirectoryNotEmpty {
                path: PathHint::none(),
            });
        }

        self.meta_rmdir(parent_ino, name).await?;
        Ok(())
    }

    async fn parent_is_descendant_of(
        &self,
        mut parent_ino: i64,
        ancestor_ino: i64,
    ) -> Result<bool, VfsError> {
        while parent_ino != self.core.root {
            if parent_ino == ancestor_ino {
                return Ok(true);
            }
            match self.meta_get_dir_parent(parent_ino).await? {
                Some(next) if next != parent_ino => parent_ino = next,
                _ => return Ok(false),
            }
        }
        Ok(ancestor_ino == self.core.root)
    }

    /// Rename an entry using parent inodes and names directly.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(old_parent_ino, old_name, new_parent_ino, new_name)
    )]
    pub(crate) async fn rename_at(
        &self,
        old_parent_ino: i64,
        old_name: &str,
        new_parent_ino: i64,
        new_name: &str,
    ) -> Result<(), VfsError> {
        Self::validate_entry_name(old_name)?;
        Self::validate_entry_name(new_name)?;

        if old_parent_ino == new_parent_ino && old_name == new_name {
            return Ok(());
        }

        let src_ino = self
            .meta_lookup_required(old_parent_ino, old_name, PathHint::none())
            .await?;
        let src_attr = self.meta_stat_required(src_ino, PathHint::none()).await?;

        let new_parent_attr = self
            .meta_stat_required(new_parent_ino, PathHint::none())
            .await?;

        self.rename_at_with_known_attrs(
            old_parent_ino,
            old_name,
            new_parent_ino,
            new_name.to_string(),
            src_ino,
            &src_attr,
            &new_parent_attr,
            None,
        )
        .await
    }

    /// Rename after the caller has already resolved the source inode and
    /// destination parent attributes. This avoids duplicate lookup/stat work in
    /// the FUSE rename path while keeping VFS-level circular-directory checks.
    #[tracing::instrument(
        level = "debug",
        skip(self, src_attr, new_parent_attr),
        fields(old_parent_ino, old_name, new_parent_ino, new_name, src_ino)
    )]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn rename_at_with_known_attrs(
        &self,
        old_parent_ino: i64,
        old_name: &str,
        new_parent_ino: i64,
        new_name: String,
        src_ino: i64,
        src_attr: &FileAttr,
        new_parent_attr: &FileAttr,
        known_dest_ino: Option<Option<i64>>,
    ) -> Result<(), VfsError> {
        Self::validate_entry_name(old_name)?;
        Self::validate_entry_name(&new_name)?;

        if old_parent_ino == new_parent_ino && old_name == new_name {
            return Ok(());
        }

        if new_parent_attr.kind != FileType::Dir {
            return Err(VfsError::NotADirectory {
                path: PathHint::none(),
            });
        }

        if src_attr.kind == FileType::Dir
            && self
                .parent_is_descendant_of(new_parent_ino, src_ino)
                .await?
        {
            return Err(VfsError::CircularRename {
                path: PathHint::none(),
            });
        }

        let (dest_ino, destination_checked) = match known_dest_ino {
            Some(dest_ino) => (dest_ino, true),
            None => (None, false),
        };

        self.meta_rename_with_known_attrs(
            old_parent_ino,
            old_name,
            new_parent_ino,
            new_name,
            src_ino,
            src_attr.clone(),
            new_parent_attr.clone(),
            dest_ino,
            destination_checked,
        )
        .await?;

        Ok(())
    }

    /// Create a hard link at `link_path` that references `existing_path`.
    #[tracing::instrument(level = "debug", skip(self), fields(existing_path, link_path))]
    pub async fn link(&self, existing_path: &str, link_path: &str) -> Result<FileAttr, VfsError> {
        let existing_path = Self::norm_path(existing_path);
        let link_path = Self::norm_path(link_path);

        if existing_path == "/" {
            return Err(VfsError::IsADirectory {
                path: PathHint::some(existing_path.as_str()),
            });
        }
        if link_path == "/" {
            return Err(VfsError::InvalidFilename);
        }

        let (src_ino, src_kind) = self.meta_lookup_path_required(&existing_path).await?;

        if src_kind == FileType::Dir {
            return Err(VfsError::IsADirectory {
                path: PathHint::some(existing_path.as_str()),
            });
        }

        let (parent_path, name) = Self::split_dir_file(&link_path);
        if name.is_empty() {
            return Err(VfsError::InvalidFilename);
        }

        let parent_ino = self.resolve_parent_inode(&parent_path).await?;

        self.link_by_ino(src_ino, parent_ino, &name).await
    }

    /// Create a symbolic link at `link_path` pointing to `target`.
    #[tracing::instrument(level = "trace", skip(self), fields(link_path, target))]
    pub async fn create_symlink(
        &self,
        link_path: &str,
        target: &str,
    ) -> Result<(i64, FileAttr), VfsError> {
        let link_path = Self::norm_path(link_path);
        if link_path == "/" {
            return Err(VfsError::InvalidFilename);
        }
        let (dir, name) = Self::split_dir_file(&link_path);
        if name.is_empty() {
            return Err(VfsError::InvalidFilename);
        }

        let parent_ino = self.resolve_parent_inode(&dir).await?;

        self.create_symlink_at(parent_ino, &name, target).await
    }

    /// Fetch a file's attributes (kind/size come from the metadata layer).
    #[tracing::instrument(level = "trace", skip(self), fields(path))]
    pub async fn stat(&self, path: &str) -> Result<FileAttr, VfsError> {
        let path = Self::norm_path(path);

        let (ino, _) = self.meta_lookup_path_required(&path).await?;

        let mut meta_attr = self
            .meta_stat_required(ino, PathHint::some(path.as_str()))
            .await?;

        // close-to-open semantics: if there is a local state, it should be considered as the newest state.
        if let Some(size) = self.inode_size_cached(ino) {
            meta_attr.size = size;
        }

        Ok(meta_attr)
    }

    /// Fetch a file's attributes (kind/size come from the MetaStore), following symlinks.
    pub async fn stat_follow_err(&self, path: &str) -> Result<FileAttr, VfsError> {
        self.stat(path).await
    }

    /// Read a symlink target by inode.
    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    pub(crate) async fn readlink_ino(&self, ino: i64) -> Result<String, VfsError> {
        let attr = self.meta_stat_required(ino, PathHint::none()).await?;
        if attr.kind != FileType::Symlink {
            return Err(VfsError::InvalidInput);
        }

        self.meta_read_symlink(ino).await
    }

    /// Read a symlink target by path.
    #[tracing::instrument(level = "trace", skip(self), fields(path))]
    pub async fn readlink(&self, path: &str) -> Result<String, VfsError> {
        let path = Self::norm_path(path);

        let (ino, kind) = self.meta_lookup_path_required(&path).await?;
        if kind != FileType::Symlink {
            return Err(VfsError::InvalidInput);
        }

        self.readlink_ino(ino).await
    }

    /// Check whether a path exists.
    pub async fn exists(&self, path: &str) -> bool {
        let path = Self::norm_path(path);
        matches!(self.meta_lookup_path(&path).await, Ok(Some(_)))
    }

    /// Remove a regular file or symlink (directories are not supported here).
    #[tracing::instrument(level = "trace", skip(self), fields(path))]
    pub async fn unlink(&self, path: &str) -> Result<(), VfsError> {
        let path = Self::norm_path(path);
        let (dir, name) = Self::split_dir_file(&path);

        let parent_ino = self.resolve_parent_inode(&dir).await?;

        self.unlink_at(parent_ino, &name).await
    }

    /// Remove an empty directory (root cannot be removed; non-empty dirs error out).
    #[tracing::instrument(level = "trace", skip(self), fields(path))]
    pub async fn rmdir(&self, path: &str) -> Result<(), VfsError> {
        let path = Self::norm_path(path);
        if path == "/" {
            return Err(VfsError::PermissionDenied {
                path: PathHint::some(path),
            });
        }

        let (dir, name) = Self::split_dir_file(&path);

        let parent_ino = self.resolve_parent_inode(&dir).await?;

        self.rmdir_at(parent_ino, &name).await
    }

    /// Step 1: Resolve parent directory inode from path
    async fn resolve_parent_inode(&self, dir_path: &str) -> Result<i64, VfsError> {
        if dir_path == "/" {
            return Ok(self.core.root);
        }

        let (ino, kind) = self.meta_lookup_path_required(dir_path).await?;

        if kind != FileType::Dir {
            return Err(VfsError::NotADirectory {
                path: PathHint::some(dir_path),
            });
        }

        Ok(ino)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(old, new))]
    pub async fn rename(&self, old: &str, new: &str) -> Result<(), VfsError> {
        let old = Self::norm_path(old);
        let new = Self::norm_path(new);
        let (old_dir, old_name) = Self::split_dir_file(&old);
        let (new_dir, new_name) = Self::split_dir_file(&new);

        let old_parent_ino = self.resolve_parent_inode(&old_dir).await?;
        let new_parent_ino = self.resolve_parent_inode(&new_dir).await?;

        self.rename_at(old_parent_ino, &old_name, new_parent_ino, &new_name)
            .await?;

        Ok(())
    }

    /// Rename files or directories with extended flags support.
    /// This is similar to Linux renameat2 syscall with additional flags.
    pub async fn rename_with_flags(
        &self,
        old: &str,
        new: &str,
        flags: RenameFlags,
    ) -> Result<(), VfsError> {
        if flags.exchange {
            return self.rename_exchange(old, new).await;
        }

        if flags.noreplace {
            return self.rename_noreplace(old, new).await;
        }

        // Default behavior - allow replacement
        self.rename(old, new).await
    }

    /// Rename without replacing the destination (RENAME_NOREPLACE).
    /// Returns an error if the destination already exists.
    pub async fn rename_noreplace(&self, old: &str, new: &str) -> Result<(), VfsError> {
        let old = Self::norm_path(old);
        let new = Self::norm_path(new);

        // Check if destination exists
        if self.meta_lookup_path(&new).await?.is_some() {
            return Err(VfsError::AlreadyExists {
                path: PathHint::some(format!("destination '{}' already exists", new)),
            });
        }

        // Use standard rename
        self.rename(&old, &new).await
    }

    /// Atomically exchange the source and destination (RENAME_EXCHANGE).
    /// Both source and destination must exist.
    pub async fn rename_exchange(&self, old: &str, new: &str) -> Result<(), VfsError> {
        let old = Self::norm_path(old);
        let new = Self::norm_path(new);

        // Both source and destination must exist
        let (old_dir, old_name) = Self::split_dir_file(&old);
        let (new_dir, new_name) = Self::split_dir_file(&new);

        // Resolve parents
        let old_parent_ino = self.resolve_parent_inode(&old_dir).await?;
        let new_parent_ino = self.resolve_parent_inode(&new_dir).await?;

        // Both entries must exist
        let _old_ino = self
            .meta_lookup_required(old_parent_ino, &old_name, PathHint::some(old.as_str()))
            .await?;

        let _new_ino = self
            .meta_lookup_required(new_parent_ino, &new_name, PathHint::some(new.as_str()))
            .await?;

        // Perform atomic exchange via store layer
        self.meta_rename_exchange(old_parent_ino, &old_name, new_parent_ino, &new_name)
            .await?;

        Ok(())
    }

    /// Check if a rename operation would be allowed without actually performing it.
    pub async fn can_rename(&self, old: &str, new: &str) -> Result<(), VfsError> {
        let old = Self::norm_path(old);
        let new = Self::norm_path(new);
        let (old_dir, old_name) = Self::split_dir_file(&old);
        let (new_dir, new_name) = Self::split_dir_file(&new);

        // Validate basic parameters
        if old.is_empty() || new.is_empty() {
            return Err(VfsError::InvalidInput);
        }

        if new_name.is_empty() || new_name.contains('/') || new_name.contains('\0') {
            return Err(VfsError::InvalidFilename);
        }

        // Check source exists
        let old_parent_ino = self.resolve_parent_inode(&old_dir).await?;

        let src_ino = self
            .meta_lookup_required(old_parent_ino, &old_name, PathHint::some(old.as_str()))
            .await?;

        let src_attr = self
            .meta_stat_required(src_ino, PathHint::some(old.as_str()))
            .await?;

        // Check destination parent exists
        let _new_parent_ino = self.resolve_parent_inode(&new_dir).await?;

        // Check destination constraints
        if let Some((dest_ino, dest_kind)) = self.meta_lookup_path(&new).await? {
            let _dest_attr = self
                .meta_stat_required(dest_ino, PathHint::some(new.as_str()))
                .await?;

            match (src_attr.kind, dest_kind) {
                // Directory replacing directory
                (FileType::Dir, FileType::Dir) => {
                    let children = self.meta_readdir(dest_ino).await?;
                    if !children.is_empty() {
                        return Err(VfsError::DirectoryNotEmpty {
                            path: PathHint::some(new.as_str()),
                        });
                    }
                }
                // Directory replacing file/symlink
                (FileType::Dir, FileType::File) | (FileType::Dir, FileType::Symlink) => {
                    return Err(VfsError::NotADirectory {
                        path: PathHint::some(new.as_str()),
                    });
                }
                // File/symlink replacing directory
                (FileType::File, FileType::Dir) | (FileType::Symlink, FileType::Dir) => {
                    return Err(VfsError::IsADirectory {
                        path: PathHint::some(new.as_str()),
                    });
                }
                // File/symlink replacing file/symlink - allowed
                _ => {}
            }
        }

        Ok(())
    }

    /// Batch rename multiple files efficiently
    /// Returns a vector of results, one for each rename operation
    pub async fn rename_batch(
        &self,
        operations: Vec<(String, String)>,
    ) -> Vec<Result<(), VfsError>> {
        let mut results = Vec::with_capacity(operations.len());

        // Process operations sequentially for simplicity
        // In a more advanced implementation, we could parallelize non-conflicting operations
        for (old_path, new_path) in operations {
            let result = self.rename(&old_path, &new_path).await;
            results.push(result);
        }

        results
    }

    /// Truncate/extend file size (metadata only; holes are read as zeros).
    /// Shrinking does not eagerly reclaim block data.
    #[tracing::instrument(level = "trace", skip(self), fields(path, size))]
    pub async fn truncate(&self, path: &str, size: u64) -> Result<(), VfsError> {
        let path = Self::norm_path(path);

        let (ino, _) = self.meta_lookup_path_required(&path).await?;

        self.truncate_inode(ino, size).await
    }

    async fn flush_before_truncate(
        &self,
        ino: i64,
        size: u64,
        op: &'static str,
    ) -> Result<(), VfsError> {
        let start = Instant::now();
        tracing::debug!(ino, size, op, "truncate path: flush pending writes");
        self.state
            .writer
            .flush_required_for_truncate(ino as u64)
            .await
            .map_err(|err| {
                let message = err.to_string();
                let is_timeout = message.contains("flush timeout") || message.contains("timed out");
                tracing::error!(
                    ino,
                    size,
                    op,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    error = %message,
                    "truncate path: flush failed"
                );
                if is_timeout {
                    VfsError::TimedOut
                } else {
                    VfsError::from(err)
                }
            })?;
        tracing::debug!(
            ino,
            size,
            op,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "truncate path: flush complete"
        );
        Ok(())
    }

    /// Truncate/extend file size by inode (metadata only; holes are read as zeros).
    /// Shrinking does not eagerly reclaim block data.
    pub async fn truncate_inode(&self, ino: i64, size: u64) -> Result<(), VfsError> {
        // Flush dirty data BEFORE acquiring mutation_lock so that we do not hold the
        // lock across a potentially long upload wait (up to FLUSH_DEADLINE = 300 s).
        // Holding the lock during flush would cause all concurrent FUSE WRITEs for
        // this inode to queue at the mutex, eventually stalling kernel writeback and
        // blocking userspace pwrite(2) for the entire flush duration.
        //
        // After we take the lock we call writer.clear() to drop any newly-written
        // dirty slices that arrived between the pre-flush and the lock acquisition.
        // Those writes lose their data (truncate semantics: last-writer wins at the
        // inode level), and meta_truncate removes any slices committed in that window.
        self.flush_before_truncate(ino, size, "truncate_inode")
            .await?;

        let mutation_lock = self.state.append_lock(ino);
        tracing::debug!(ino, size, "truncate_inode: waiting for mutation lock");
        let _mutation_guard = mutation_lock.lock_owned().await;
        tracing::debug!(ino, size, "truncate_inode: mutation lock acquired");

        let handles = self.file_handles_for_inode(ino);
        let mut guards = Vec::with_capacity(handles.len());
        for handle in handles {
            guards.push(handle.lock_write().await);
        }

        self.meta_truncate(ino, size, self.core.layout.chunk_size)
            .await?;

        // POSIX semantic for `truncate`: `truncate` is immediately visible to old handles.
        self.state.reader.invalidate_all(ino as u64).await;
        // Discard dirty data written between the pre-flush and the lock acquisition.
        self.state.writer.clear(ino as u64).await;

        let guard = self
            .lock_inode(ino)
            .or_insert_with(|| Inode::new(ino, size));

        guard.set_size(size);
        // After truncate the allocated-bytes estimate is stale — we cannot
        // simply set it to `size` because extending truncates create holes
        // and shrinking truncates may or may not free blocks.  Mark it
        // unknown so st_blocks falls back to the metadata-provided value.
        guard.invalidate_allocated_blocks();
        guard.bump_data_epoch();

        if let Some(mut attr) = self.state.handles.attr_for_inode(ino) {
            attr.size = size;
            self.state.handles.update_attr_for_inode(ino, &attr);
        }

        drop(guards);
        Ok(())
    }

    /// Minimal fallocate support for buffered mmap tests.
    ///
    /// BrewFS does not reserve backend space ahead of time, but `mode=0`
    /// must still make the file logically extend to cover `offset + length`.
    /// Unsupported punch/collapse/zero-range modes are rejected by the FUSE
    /// adapter so callers get a clear error instead of falling back to slow
    /// userspace emulation.
    pub async fn fallocate_ino(&self, ino: i64, offset: u64, length: u64) -> Result<(), VfsError> {
        let attr = self.meta_stat_required(ino, PathHint::none()).await?;
        if matches!(attr.kind, FileType::Dir) {
            return Err(VfsError::IsADirectory {
                path: PathHint::none(),
            });
        }
        if attr.kind != FileType::File {
            return Err(VfsError::InvalidInput);
        }
        if length == 0 {
            return Ok(());
        }

        let end = offset.checked_add(length).ok_or(VfsError::FileTooLarge)?;
        let current_size = self.inode_size_cached(ino).unwrap_or(attr.size);

        if end > current_size {
            self.ensure_fallocate_space_available(current_size, end)
                .await?;
        }
        self.apply_fallocate_metadata(ino, offset, length).await?;
        Ok(())
    }

    pub async fn fallocate_handle(
        &self,
        fh: u64,
        ino: i64,
        offset: u64,
        length: u64,
    ) -> Result<(), VfsError> {
        let handle = self.file_handle_required(fh)?;
        if handle.ino != ino {
            return Err(VfsError::StaleNetworkFileHandle);
        }
        if !handle.flags.write {
            return Err(VfsError::PermissionDenied {
                path: PathHint::none(),
            });
        }

        let attr = handle.attr();
        if matches!(attr.kind, FileType::Dir) {
            return Err(VfsError::IsADirectory {
                path: PathHint::none(),
            });
        }
        if attr.kind != FileType::File {
            return Err(VfsError::InvalidInput);
        }
        if length == 0 {
            return Ok(());
        }

        let end = offset.checked_add(length).ok_or(VfsError::FileTooLarge)?;
        let current_size = self.inode_size_cached(ino).unwrap_or(attr.size);

        if end > current_size {
            self.ensure_fallocate_space_available(current_size, end)
                .await?;
        }

        // The handle path is used by FUSE fallocate while the file is open.
        // BrewFS represents the newly allocated range as a sparse hole, so the
        // check above is an early ENOSPC guard rather than a backend reservation.
        let _handle_guard = handle.lock_write().await;
        if end > current_size {
            let inode = self.ensure_inode_registered(ino).await?;
            let writer = self.state.writer.ensure_file(inode);
            writer
                .record_sparse_fallocate(current_size, end - current_size)
                .await
                .map_err(VfsError::from)?;
        }
        self.apply_fallocate_metadata(ino, offset, length).await?;
        Ok(())
    }

    async fn ensure_fallocate_space_available(
        &self,
        current_size: u64,
        new_size: u64,
    ) -> Result<(), VfsError> {
        let growth = new_size.saturating_sub(current_size);
        if growth == 0 {
            return Ok(());
        }

        let stat = self.stat_fs_for_fallocate().await?;
        if growth > stat.available_space {
            return Err(VfsError::StorageFull);
        }
        Ok(())
    }

    async fn stat_fs_for_fallocate(&self) -> Result<StatFsSnapshot, VfsError> {
        if let Some(snapshot) = {
            let cache = self
                .state
                .fallocate_statfs_cache
                .lock()
                .expect("fallocate statfs cache lock poisoned");
            cache
                .as_ref()
                .filter(|entry| entry.cached_at.elapsed() <= FALLOCATE_STATFS_CACHE_TTL)
                .map(|entry| entry.snapshot.clone())
        } {
            return Ok(snapshot);
        }

        let snapshot = self.meta_stat_fs().await?;
        let mut cache = self
            .state
            .fallocate_statfs_cache
            .lock()
            .expect("fallocate statfs cache lock poisoned");
        *cache = Some(FallocateStatFsCache {
            snapshot: snapshot.clone(),
            cached_at: Instant::now(),
        });
        Ok(snapshot)
    }

    async fn apply_fallocate_metadata(
        &self,
        ino: i64,
        offset: u64,
        length: u64,
    ) -> Result<FileAttr, VfsError> {
        let mut attr = self
            .meta_fallocate_file(ino, offset, length, FALLOCATE_ALLOCATION_UNIT)
            .await?;

        let local_size = self
            .inode_size_cached(ino)
            .into_iter()
            .chain(self.state.handles.attr_for_inode(ino).map(|attr| attr.size))
            .max()
            .unwrap_or(attr.size);
        attr.size = attr.size.max(local_size);

        self.extend_local_file_size(ino, attr.size);
        if let Some(inode) = self.state.inodes.get(&ino) {
            inode.set_exact_allocated_bytes(attr.blocks.saturating_mul(512));
        }
        self.state.handles.update_attr_for_inode(ino, &attr);
        Ok(attr)
    }

    #[tracing::instrument(level = "trace", skip(self, req), fields(ino, flags = ?flags))]
    pub async fn set_attr(
        &self,
        ino: i64,
        req: &SetAttrRequest,
        flags: SetAttrFlags,
    ) -> Result<FileAttr, VfsError> {
        if Self::deleted_inode_timestamp_only_setattr(req, &flags) {
            let remove_after = self.state.handles.has_no_handle(ino);
            let removed = if remove_after {
                let _remove_timer = self.vfs_timing_timer(
                    &self.stats().vfs_setattr_recent_remove_ops,
                    &self.stats().vfs_setattr_recent_remove_lat_us,
                );
                self.state.recently_unlinked.remove(&ino)
            } else {
                None
            };
            if let Some((_, (mut attr, inserted_at))) = removed {
                let original = attr.clone();
                if let Err(err) = Self::apply_timestamp_setattr_locally(&mut attr, req, &flags) {
                    self.state
                        .recently_unlinked
                        .insert(ino, (original, inserted_at));
                    return Err(err);
                }
                attr.nlink = 0;
                self.state.handles.update_attr_for_inode(ino, &attr);
                return Ok(attr);
            }

            let recent_entry = {
                let _get_mut_timer = self.vfs_timing_timer(
                    &self.stats().vfs_setattr_recent_get_mut_ops,
                    &self.stats().vfs_setattr_recent_get_mut_lat_us,
                );
                self.state.recently_unlinked.get_mut(&ino)
            };
            if let Some(mut entry) = recent_entry {
                let mut attr = entry.0.clone();
                Self::apply_timestamp_setattr_locally(&mut attr, req, &flags)?;
                attr.nlink = 0;
                *entry = (attr.clone(), Instant::now());
                self.state.handles.update_attr_for_inode(ino, &attr);
                drop(entry);
                if remove_after {
                    let _remove_timer = self.vfs_timing_timer(
                        &self.stats().vfs_setattr_recent_remove_ops,
                        &self.stats().vfs_setattr_recent_remove_lat_us,
                    );
                    self.state.recently_unlinked.remove(&ino);
                }
                return Ok(attr);
            }
        }

        // Hold handle write guards across the ENTIRE truncate + meta_set_attr
        // sequence so that no concurrent write_ino / FUSE_WRITE_CACHE can modify
        // the inode between the truncate and the attribute read-back.  Dropping
        // the guards too early allowed a race where meta_set_attr could read back
        // a size extended by a concurrent commit, causing the FUSE setattr
        // response to carry a wrong file size and confusing the kernel page cache.
        //
        // flush_required is called BEFORE acquiring mutation_lock to avoid holding
        // the lock during a potentially long upload wait (see truncate_inode for the
        // full rationale).  writer.clear() inside the lock discards any dirty slices
        // that arrived between the pre-flush and the lock acquisition.
        let _guards = if let Some(size) = req.size {
            self.flush_before_truncate(ino, size, "set_attr").await?;

            let mutation_lock = self.state.append_lock(ino);
            tracing::debug!(ino, size, "set_attr truncate: waiting for mutation lock");
            let _mutation_guard = mutation_lock.lock_owned().await;
            tracing::debug!(ino, size, "set_attr truncate: mutation lock acquired");

            let handles = self.file_handles_for_inode(ino);
            let mut guards = Vec::with_capacity(handles.len());
            for handle in handles {
                guards.push(handle.lock_write().await);
            }

            self.meta_truncate(ino, size, self.core.layout.chunk_size)
                .await?;
            self.state.reader.invalidate_all(ino as u64).await;
            self.state.writer.clear(ino as u64).await;

            let guard = self
                .lock_inode(ino)
                .or_insert_with(|| Inode::new(ino, size));
            guard.set_size(size);
            guard.invalidate_allocated_blocks();
            guard.bump_data_epoch();

            if let Some(mut attr) = self.state.handles.attr_for_inode(ino) {
                attr.size = size;
                self.state.handles.update_attr_for_inode(ino, &attr);
            }

            Some((_mutation_guard, guards))
        } else {
            None
        };

        let mut filtered = *req;
        filtered.size = None;

        let mut attr = self.meta_set_attr(ino, &filtered, flags).await?;

        // Ensure the returned attr carries exactly the requested truncation size.
        // The kernel trusts this value for truncate_pagecache decisions; a stale
        // or extended size here can cause it to keep or invalidate wrong pages.
        if let Some(size) = req.size {
            attr.size = size;
            if let Some(inode) = self.state.inodes.get(&ino) {
                inode.set_size(size);
            }
        } else if let Some(size) = self.inode_size_cached(ino) {
            // Non-size setattr requests (for example mtime/ctime updates emitted
            // during writeback-cache mmap traffic) must still report the current
            // local file size. Returning the stale meta-layer size here can make
            // the kernel believe the file shrank back to 0 and expose zero-filled
            // reads in xfstests generic/074 fstest.3.
            attr.size = size;
        }

        self.state.handles.update_attr_for_inode(ino, &attr);

        // _guards dropped here — after meta_set_attr has read the correct state
        Ok(attr)
    }

    fn deleted_inode_timestamp_only_setattr(req: &SetAttrRequest, flags: &SetAttrFlags) -> bool {
        let timestamp_flags = (SetAttrFlags::SET_ATIME_NOW | SetAttrFlags::SET_MTIME_NOW).bits();
        req.mode.is_none()
            && req.uid.is_none()
            && req.gid.is_none()
            && req.size.is_none()
            && req.flags.is_none()
            && (flags.bits() & !timestamp_flags) == 0
    }

    fn apply_timestamp_setattr_locally(
        attr: &mut FileAttr,
        req: &SetAttrRequest,
        flags: &SetAttrFlags,
    ) -> Result<(), VfsError> {
        let mut changed = false;
        let mut now = None;

        if flags.contains(SetAttrFlags::SET_ATIME_NOW) {
            let ts = *now.get_or_insert(Self::current_timestamp_nanos()?);
            attr.atime = ts;
            changed = true;
        } else if let Some(atime) = req.atime {
            attr.atime = atime;
            changed = true;
        }

        if flags.contains(SetAttrFlags::SET_MTIME_NOW) {
            let ts = *now.get_or_insert(Self::current_timestamp_nanos()?);
            attr.mtime = ts;
            changed = true;
        } else if let Some(mtime) = req.mtime {
            attr.mtime = mtime;
            changed = true;
        }

        if let Some(ctime) = req.ctime {
            attr.ctime = ctime;
        } else if changed {
            attr.ctime = *now.get_or_insert(Self::current_timestamp_nanos()?);
        }

        Ok(())
    }

    /// Change the permission bits of an inode (chmod).
    ///
    /// `new_mode` is masked to `0o7777`; setuid, setgid, and sticky bits are preserved.
    /// Returns `VfsError::NotFound` when the inode does not exist.
    #[tracing::instrument(level = "trace", skip(self), fields(ino, new_mode))]
    pub async fn chmod(&self, ino: i64, new_mode: u32) -> Result<FileAttr, VfsError> {
        let attr = self.meta_chmod(ino, new_mode).await?;

        self.state.handles.update_attr_for_inode(ino, &attr);

        Ok(attr)
    }

    /// Change the owner and/or group of an inode (chown).
    ///
    /// Either `uid` or `gid` may be `None` to leave that field unchanged.
    /// Returns `VfsError::NotFound` when the inode does not exist.
    #[tracing::instrument(level = "trace", skip(self), fields(ino, ?uid, ?gid))]
    pub async fn chown(
        &self,
        ino: i64,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Result<FileAttr, VfsError> {
        let attr = self.meta_chown(ino, uid, gid).await?;

        self.state.handles.update_attr_for_inode(ino, &attr);

        Ok(attr)
    }

    /// Read data by file handle and offset.
    #[tracing::instrument(
        name = "VFS.read",
        level = "trace",
        skip(self),
        fields(fh, offset, len)
    )]
    pub async fn read(&self, fh: u64, offset: u64, len: usize) -> Result<Vec<u8>, VfsError> {
        if len == 0 {
            return Ok(Vec::new());
        }

        let handle = self.file_handle_required(fh)?;
        // With writeback cache enabled, the kernel may issue reads on O_WRONLY
        // handles to fill partial pages before writing them back, so we only
        // reject reads when neither read nor write flags are set.
        if !handle.flags.read && !handle.flags.write {
            return Err(VfsError::PermissionDenied {
                path: PathHint::none(),
            });
        }

        let file_size = self
            .inode_size_cached(handle.ino)
            .unwrap_or_else(|| handle.attr().size);
        if offset >= file_size {
            return Ok(Vec::new());
        }
        let actual_len = len.min((file_size - offset) as usize);

        // Fast path for ranges fully covered by live dirty or recently committed
        // writeback-overlay data.  This also helps read-only handles opened after
        // a write handle: the slower reader path below would first fetch old
        // committed blocks and only then overlay the same data.
        let dirty_data = {
            let writer = self.state.writer.clone();
            let ino = handle.ino as u64;
            let _dirty_probe_timer = self.vfs_timing_timer(
                &self.state.stats.vfs_read_dirty_probe_ops,
                &self.state.stats.vfs_read_dirty_probe_lat_us,
            );
            handle
                .try_read_overlay(offset, actual_len, move |offset, len| {
                    let writer = writer.clone();
                    async move { writer.read_dirty_if_fully_covered(ino, offset, len).await }
                })
                .await
                .map_err(VfsError::from)?
        };
        if let Some(data) = dirty_data {
            return Ok(data);
        }

        // We intentionally do NOT call flush_if_exists here: blocking every
        // read on a full flush+commit cycle turns random-read-heavy workloads
        // into commit-bound traffic.  Still, writeback mode can make metadata
        // visible before the remote object upload completes.  Any reader, even
        // a read-only handle opened after close(), must wait before fetching an
        // overlapping range from the backend; otherwise mmap/read-after-close
        // can observe a zero-length or stale object.
        self.state
            .writer
            .wait_committed_uploads_for_range(handle.ino as u64, offset, actual_len)
            .await
            .map_err(VfsError::from)?;
        // Read committed data from the reader cache first, then overlay any
        // uncommitted dirty writes on top.
        // There is a narrow race where commit_chunk pops a just-committed
        // slice between handle.read() and overlay_dirty_if_exists().  In that
        // window the reader may serve a stale cached page that has already
        // been superseded.  The window is on the order of microseconds and a
        // subsequent read will see the correct data, so this is an acceptable
        // trade-off versus the 35+ ms read latency incurred by the
        // synchronous flush.
        let inode = self.ensure_inode_registered(handle.ino).await?;
        handle.ensure_reader_with(|| self.state.reader.open_for_handle(inode, fh));
        let mut data = {
            let _handle_read_timer = self.vfs_timing_timer(
                &self.state.stats.vfs_read_handle_ops,
                &self.state.stats.vfs_read_handle_lat_us,
            );
            handle
                .read(offset, actual_len)
                .await
                .map_err(VfsError::from)?
        };
        {
            let _overlay_timer = self.vfs_timing_timer(
                &self.state.stats.vfs_read_overlay_ops,
                &self.state.stats.vfs_read_overlay_lat_us,
            );
            self.state
                .writer
                .overlay_dirty_if_exists(handle.ino as u64, offset, &mut data)
                .await
                .map_err(VfsError::from)?;
        }

        self.state
            .reader
            .submit_prefetch(handle.ino, fh, offset, data.len() as u64);

        Ok(data)
    }

    /// Write data by file handle and offset.
    #[tracing::instrument(level = "trace", skip(self, data), fields(fh, offset, len = data.len()))]
    pub async fn write(&self, fh: u64, offset: u64, data: &[u8]) -> Result<usize, VfsError> {
        if data.is_empty() {
            return Ok(0);
        }
        let data_len = data.len() as u64;

        let handle = self.file_handle_required(fh)?;

        if !handle.flags.write {
            return Err(VfsError::PermissionDenied {
                path: PathHint::none(),
            });
        }

        tracing::trace!(fh, ino = handle.ino, offset, len = data.len(), "vfs.write");

        let (write_offset, written) = if handle.flags.append {
            let append_lock = self.state.append_lock(handle.ino);
            let _append_guard = append_lock.lock().await;
            let _handle_guard = handle.lock_write().await;

            let append_offset = self.inode_size(handle.ino).await?;
            let append_end = append_offset
                .checked_add(data_len)
                .ok_or(VfsError::FileTooLarge)?;
            if append_end >= i64::MAX as u64 {
                return Err(VfsError::FileTooLarge);
            }
            let written = if self
                .try_sparse_zero_extend(handle.ino, append_offset, data, append_offset)
                .await?
            {
                handle.update_offset(append_end);
                handle.extend_size(append_end);
                handle.mark_write_dirty();
                data.len()
            } else {
                handle.write_unlocked(append_offset, data).await?
            };
            tracing::debug!(
                fh,
                ino = handle.ino,
                append_offset,
                len = data.len(),
                written,
                "vfs.append_write"
            );
            (append_offset, written)
        } else {
            let _handle_guard = handle.lock_write().await;
            let write_end = offset.checked_add(data_len).ok_or(VfsError::FileTooLarge)?;
            if write_end >= i64::MAX as u64 {
                return Err(VfsError::FileTooLarge);
            }
            let visible_size = handle.attr().size;
            let written = if self
                .try_sparse_zero_extend(handle.ino, offset, data, visible_size)
                .await?
            {
                handle.update_offset(write_end);
                handle.extend_size(write_end);
                handle.mark_write_dirty();
                data.len()
            } else {
                handle.write_unlocked(offset, data).await?
            };
            (offset, written)
        };

        // Invalidate reader cache for the written range so subsequent reads
        // (including FUSE reads on kernel page-cache miss) see committed data
        // instead of a stale cached snapshot from before this write.
        let _ = self
            .state
            .reader
            .invalidate(handle.ino as u64, write_offset, data.len())
            .await;

        // Keep local inode and handle sizes in sync immediately.  Metadata size
        // is persisted by the writer commit/flush path; doing it here forces
        // every write through metadata and makes buffered writes serialize on
        // the store.
        let new_end = write_offset + written as u64;
        if new_end > handle.attr().size {
            self.extend_local_file_size(handle.ino, new_end);
        }

        tracing::trace!(
            fh,
            ino = handle.ino,
            offset = write_offset,
            written,
            new_end,
            "vfs.write_done"
        );
        Ok(written)
    }

    /// Write data by inode directly (used by FUSE to avoid path resolution).
    pub async fn write_ino(&self, ino: i64, offset: u64, data: &[u8]) -> Result<usize, VfsError> {
        if data.is_empty() {
            return Ok(0);
        }

        let mutation_lock = self.state.append_lock(ino);
        let _mutation_guard = mutation_lock.lock_owned().await;

        let attr = self.meta_stat_required(ino, PathHint::none()).await?;
        if attr.kind == FileType::Dir {
            return Err(VfsError::IsADirectory {
                path: PathHint::none(),
            });
        }
        if attr.kind != FileType::File {
            return Err(VfsError::InvalidInput);
        }

        let inode = self.ensure_inode_registered(ino).await?;
        if self
            .try_sparse_zero_extend(ino, offset, data, inode.file_size())
            .await?
        {
            return Ok(data.len());
        }

        let writer = self.state.writer.ensure_file(inode);
        let written = writer
            .write_at(offset, data)
            .await
            .map_err(VfsError::from)?;

        // Invalidate reader cache for the written range so any subsequent
        // read path flushes pending writer data instead of serving a stale
        // cached zero-fill from a prior truncate.
        let _ = self
            .state
            .reader
            .invalidate(ino as u64, offset, data.len())
            .await;

        // Keep local size visible immediately; metadata is extended when the
        // writer commits dirty slices.
        let new_end = offset + written as u64;
        if new_end > attr.size {
            self.extend_local_file_size(ino, new_end);
        }

        Ok(written)
    }

    /// Write back a kernel-cached page by inode. This is the hot path for
    /// FUSE_WRITE_CACHE (mmap writeback, kernel page cache flush).
    ///
    /// Unlike normal writes, cached writeback does NOT acquire the per-inode
    /// mutation lock.  The writer's internal slice-level locking is sufficient
    /// to handle concurrent cached pages.  Truncate correctness is preserved
    /// because truncate_inode / set_attr first drain all pending writes via
    /// flush_before_truncate, then acquire the mutation lock and call
    /// writer.clear().
    ///
    /// We also skip meta_stat_required (the kernel only sends WRITE_CACHE for
    /// inodes that are already open files) and reader.invalidate (commit_chunk
    /// invalidates the reader at commit time; doing it on every page write
    /// adds measurable latency under heavy mmap traffic).
    pub async fn write_cached_ino(
        &self,
        ino: i64,
        offset: u64,
        data: &[u8],
        creation_unique: u64,
    ) -> Result<usize, VfsError> {
        if data.is_empty() {
            return Ok(0);
        }

        let inode = self.ensure_inode_registered(ino).await?;
        if data.len() <= SPARSE_ZERO_MAX_SCAN_BYTES {
            let mutation_lock = self.state.append_lock(ino);
            let _mutation_guard = mutation_lock.lock_owned().await;
            if self
                .try_sparse_zero_extend(ino, offset, data, inode.file_size())
                .await?
            {
                return Ok(data.len());
            }
        }

        let writer = self.state.writer.ensure_file(inode.clone());
        let written = writer
            .write_at_cached(offset, data, creation_unique)
            .await
            .map_err(VfsError::from)?;

        Ok(written)
    }

    /// Copy a byte range between two opened file handles.
    ///
    /// This keeps the copy inside BrewFS so we can serialize it with the
    /// inode write path instead of falling back to kernel/user-space emulation.
    pub async fn copy_file_range(
        &self,
        fh_in: u64,
        off_in: u64,
        fh_out: u64,
        off_out: u64,
        length: u64,
    ) -> Result<usize, VfsError> {
        if length == 0 {
            return Ok(0);
        }

        let src = self.file_handle_required(fh_in)?;
        let dst = self.file_handle_required(fh_out)?;

        let mut mutation_guards = Vec::new();
        let mut mutation_locks = BTreeMap::new();
        mutation_locks.insert(src.ino, self.state.append_lock(src.ino));
        mutation_locks.insert(dst.ino, self.state.append_lock(dst.ino));
        for lock in mutation_locks.into_values() {
            mutation_guards.push(lock.lock_owned().await);
        }

        if !src.flags.read {
            return Err(VfsError::PermissionDenied {
                path: PathHint::none(),
            });
        }
        if !dst.flags.write {
            return Err(VfsError::PermissionDenied {
                path: PathHint::none(),
            });
        }

        let mut locked = Vec::new();
        let mut unique = BTreeMap::new();
        for handle in self.file_handles_for_inode(src.ino) {
            unique.insert(handle.fh, handle);
        }
        for handle in self.file_handles_for_inode(dst.ino) {
            unique.insert(handle.fh, handle);
        }
        for handle in unique.into_values() {
            locked.push(handle.lock_write().await);
        }

        self.state
            .writer
            .flush_required(src.ino as u64)
            .await
            .map_err(VfsError::from)?;
        if dst.ino != src.ino {
            self.state
                .writer
                .flush_required(dst.ino as u64)
                .await
                .map_err(VfsError::from)?;
        }

        let src_attr = self.meta_stat_required(src.ino, PathHint::none()).await?;
        let dst_attr = self.meta_stat_required(dst.ino, PathHint::none()).await?;
        let available = src_attr.size.saturating_sub(off_in);
        let copy_len = length.min(available);
        if copy_len == 0 {
            return Ok(0);
        }
        let dst_end = off_out
            .checked_add(copy_len)
            .ok_or(VfsError::FileTooLarge)?;
        if dst_end >= i64::MAX as u64 {
            return Err(VfsError::FileTooLarge);
        }
        let len = usize::try_from(copy_len).map_err(|_| VfsError::InvalidInput)?;

        self.state
            .writer
            .wait_committed_uploads_for_range(src.ino as u64, off_in, len)
            .await
            .map_err(VfsError::from)?;
        let _ = self
            .state
            .reader
            .invalidate(src.ino as u64, off_in, len)
            .await;
        let src_guard = self.open_guard(src.ino, src_attr, true, false).await?;
        let dst_guard = self.open_guard(dst.ino, dst_attr, false, true).await?;

        // Read the full source snapshot before writing so same-file overlap keeps
        // copy_file_range semantics close to a memmove-style copy.
        let data = src_guard.read(off_in, len).await?;
        let written = dst_guard.write(off_out, &data).await?;

        // Close guards to flush and commit before releasing locks.
        dst_guard.close().await?;
        src_guard.close().await?;
        self.state.reader.invalidate_all(dst.ino as u64).await;
        drop(locked);
        drop(mutation_guards);

        Ok(written)
    }

    /// Copy a byte range between two inodes by opening temporary handles.
    pub async fn copy_file_range_inodes(
        &self,
        src_ino: i64,
        off_in: u64,
        dst_ino: i64,
        off_out: u64,
        length: u64,
    ) -> Result<usize, VfsError> {
        let src_attr = self.meta_stat_required(src_ino, PathHint::none()).await?;
        let dst_attr = self.meta_stat_required(dst_ino, PathHint::none()).await?;
        let src_guard = self.open_guard(src_ino, src_attr, true, false).await?;
        let dst_guard = self.open_guard(dst_ino, dst_attr, false, true).await?;
        let fh_in = src_guard.fh();
        let fh_out = dst_guard.fh();

        let result = self
            .copy_file_range(fh_in, off_in, fh_out, off_out, length)
            .await;

        // Close guards to ensure handles are released cleanly.
        dst_guard.close().await?;
        src_guard.close().await?;

        result
    }

    /// Allocate a per-file handle, returning the opaque fh id.
    #[tracing::instrument(level = "trace", skip(self), fields(ino, read, write))]
    pub async fn open(
        &self,
        ino: i64,
        attr: FileAttr,
        read: bool,
        write: bool,
        append: bool,
    ) -> Result<u64, VfsError> {
        self.open_with_attr_refresh(ino, attr, read, write, append, true)
            .await
    }

    pub(crate) async fn open_with_cached_attr(
        &self,
        ino: i64,
        attr: FileAttr,
        read: bool,
        write: bool,
        append: bool,
    ) -> Result<u64, VfsError> {
        let fh = self
            .open_with_attr_refresh(ino, attr.clone(), read, write, append, false)
            .await?;
        self.meta_record_open(ino, attr, read, write, append)
            .await?;
        Ok(fh)
    }

    pub(crate) async fn open_fresh_ino(
        &self,
        ino: i64,
        read: bool,
        write: bool,
        append: bool,
    ) -> Result<u64, VfsError> {
        let attr = match self.meta_stat_for_open(ino, read, write, append).await {
            Ok(Some(attr)) => attr,
            Ok(None) => {
                return Err(VfsError::NotFound {
                    path: PathHint::none(),
                });
            }
            Err(err) => {
                tracing::warn!("open: stat_fresh failed for ino {}: {}", ino, err);
                return Err(VfsError::StaleNetworkFileHandle);
            }
        };

        if attr.kind == FileType::Dir {
            return Err(VfsError::IsADirectory {
                path: PathHint::none(),
            });
        }

        let fh = self
            .open_with_attr_refresh(ino, attr.clone(), read, write, append, false)
            .await?;
        self.meta_record_open(ino, attr, read, write, append)
            .await?;
        Ok(fh)
    }

    async fn open_with_attr_refresh(
        &self,
        ino: i64,
        attr: FileAttr,
        read: bool,
        write: bool,
        append: bool,
        refresh_attr: bool,
    ) -> Result<u64, VfsError> {
        let mut latest_attr = attr;
        let mut record_open = false;

        // Retrieve the latest attr for close-to-open semantics.
        if refresh_attr {
            match self.meta_stat_for_open(ino, read, write, append).await {
                Ok(Some(fresh)) => {
                    latest_attr = fresh;
                    record_open = true;
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!("open: stat_fresh failed for ino {}: {}", ino, err);
                    return Err(VfsError::StaleNetworkFileHandle);
                }
            }
        }

        let guard = self
            .lock_inode(ino)
            .or_insert_with(|| Inode::new(ino, latest_attr.size));
        if latest_attr.size > guard.file_size() {
            guard.extend_size(latest_attr.size);
        } else if guard.file_size() > latest_attr.size {
            latest_attr.size = guard.file_size();
        }

        let inode = guard.clone();
        let record_attr = record_open.then(|| latest_attr.clone());
        let handle =
            self.state
                .handles
                .allocate(ino, latest_attr, HandleFlags::new(read, write, append));
        if write {
            let writer = self.state.writer.ensure_file(inode.clone());
            handle.writer(writer);
        }
        if let Some(attr) = record_attr {
            self.meta_record_open(ino, attr, read, write, append)
                .await?;
        }
        Ok(handle.fh)
    }

    /// Allocate a file handle and return a guard that auto-closes on drop.
    pub async fn open_guard(
        &self,
        ino: i64,
        attr: FileAttr,
        read: bool,
        write: bool,
    ) -> Result<FileGuard<S, M>, VfsError> {
        let fh = self.open(ino, attr, read, write, false).await?;
        Ok(FileGuard::new(self.clone(), fh))
    }

    /// Release a previously allocated file handle.
    pub async fn close(&self, fh: u64) -> Result<(), VfsError> {
        // Note that we cannot hold the lock during the entire function, because `handle.flush()` is a I/O operation.
        let handle = self.file_handle_required(fh)?;

        tracing::trace!(
            fh,
            ino = handle.ino,
            write = handle.flags.write,
            "vfs.close"
        );
        if handle.flags.write {
            let _handle_guard = handle.lock_write().await;

            let had_write = self.state.handles.take_write_dirty(fh);
            let flushed_pending = if had_write {
                self.state
                    .writer
                    .flush_for_close(handle.ino as u64)
                    .await
                    .map_err(VfsError::from)?
            } else {
                false
            };
            if (had_write || flushed_pending)
                && let Err(err) = self.update_mtime_ctime(handle.ino).await
            {
                if had_write {
                    handle.mark_write_dirty();
                }
                return Err(err);
            }
        }

        // Prevent us from TOC-TOU (time of check to time of use) error.
        // If we release the handle and remove the inode directly, there is
        // a time windows between checking and releasing. It causes the inode and writer
        // to be deleted mistakenly.
        let (release_writer, discard_unlinked) = match self.lock_inode(handle.ino) {
            Entry::Occupied(entry) => {
                self.state.handles.release(fh);
                let release_writer =
                    handle.flags.write && !self.state.handles.has_write_handle(handle.ino);
                let no_handles = self.state.handles.has_no_handle(handle.ino);
                let discard_unlinked =
                    no_handles && self.state.recently_unlinked.contains_key(&handle.ino);

                if no_handles && !discard_unlinked {
                    entry.remove();
                }
                (release_writer, discard_unlinked)
            }
            Entry::Vacant(_) => {
                // This is weird/impossible?
                // It means the inode was deleted while we held a handle to it.
                unreachable!("Try closing a file that has never been opened");
            }
        };

        self.state
            .reader
            .close_for_handle(handle.ino as u64, fh)
            .await;

        if discard_unlinked {
            self.discard_closed_unlinked_inode_state(handle.ino).await;
        } else if release_writer {
            self.state.writer.release(handle.ino as u64).await;
        }

        if discard_unlinked {
            self.meta_record_close(handle.ino).await?;
        } else {
            self.meta_record_close_with_attr(
                handle.ino,
                handle.attr(),
                handle.flags.read,
                handle.flags.write,
                handle.flags.append,
            )
            .await?;
        }

        tracing::trace!(fh, ino = handle.ino, "vfs.close_done");
        Ok(())
    }

    pub(crate) async fn flush_dirty_handle_snapshot(&self, fh: u64) -> Result<(), VfsError> {
        let handle = self.file_handle_required(fh)?;

        tracing::trace!(
            fh,
            ino = handle.ino,
            write = handle.flags.write,
            "vfs.flush_dirty_handle_snapshot"
        );

        let had_write = self.state.handles.take_write_dirty(fh);
        if !had_write {
            return Ok(());
        }

        let flushed_pending = self
            .state
            .writer
            .flush_required_snapshot(handle.ino as u64)
            .await
            .map_err(VfsError::from)?;

        if (had_write || flushed_pending)
            && let Err(err) = self.update_mtime_ctime(handle.ino).await
        {
            handle.mark_write_dirty();
            return Err(err);
        }
        Ok(())
    }

    /// Shared implementation for flush and fsync: flushes pending writes for the
    /// inode and updates timestamps. Returns the inode number for logging.
    ///
    /// Always flushes the shared writer regardless of the handle's open flags:
    /// mmap writes via FUSE writeback (write_ino) deposit data in the shared
    /// writer and a subsequent fsync on a read-only handle must commit them.
    async fn flush_and_sync_handle(&self, fh: u64) -> Result<i64, VfsError> {
        let handle = self.file_handle_required(fh)?;

        tracing::info!(fh, ino = handle.ino, "vfs.flush_handle_start");
        let had_write = self.state.handles.take_write_dirty_for_inode(handle.ino);
        let flushed_pending = self
            .state
            .writer
            .flush_required(handle.ino as u64)
            .await
            .map_err(VfsError::from)?;
        tracing::trace!(fh, ino = handle.ino, "vfs.flush_handle_done");

        if (had_write || flushed_pending)
            && let Err(err) = self.update_mtime_ctime(handle.ino).await
        {
            if had_write {
                handle.mark_write_dirty();
            }
            return Err(err);
        }
        Ok(handle.ino)
    }

    /// Flush pending writes for a file handle.
    pub async fn flush(&self, fh: u64) -> Result<(), VfsError> {
        let handle = self.file_handle_required(fh)?;

        tracing::trace!(
            fh,
            ino = handle.ino,
            write = handle.flags.write,
            "vfs.flush"
        );
        let ino = self.flush_and_sync_handle(fh).await?;
        tracing::trace!(fh, ino, "vfs.flush_done");
        Ok(())
    }

    /// Flush pending writes for an inode (best-effort, without a file handle).
    /// Used by rename and other metadata operations that need write-back
    /// convergence before modifying directory entries.
    pub async fn flush_inode(&self, ino: u64) {
        let _ = self.state.writer.flush_if_exists(ino).await;
    }

    /// Sync file content (fsync): flush pending writes.
    pub async fn fsync(&self, fh: u64, _datasync: bool) -> Result<(), VfsError> {
        let handle = self.file_handle_required(fh)?;

        tracing::trace!(
            fh,
            ino = handle.ino,
            write = handle.flags.write,
            "vfs.fsync"
        );

        let ino = self.flush_and_sync_handle(fh).await?;
        tracing::trace!(fh, ino, "vfs.fsync_done");
        Ok(())
    }

    /// Sync file content for a FUSE fsync request after the adapter has waited
    /// for earlier cached-write requests.  This flushes the current dirty
    /// snapshot without chasing writes that arrive after this fsync started.
    pub(crate) async fn fsync_snapshot(&self, fh: u64, _datasync: bool) -> Result<(), VfsError> {
        let handle = self.file_handle_required(fh)?;

        tracing::trace!(
            fh,
            ino = handle.ino,
            write = handle.flags.write,
            "vfs.fsync_snapshot"
        );

        tracing::info!(fh, ino = handle.ino, "vfs.flush_handle_start");
        let had_write = self.state.handles.take_write_dirty_for_inode(handle.ino);
        let flushed_pending = self
            .state
            .writer
            .flush_required_snapshot(handle.ino as u64)
            .await
            .map_err(VfsError::from)?;
        tracing::trace!(fh, ino = handle.ino, "vfs.flush_handle_done");

        if (had_write || flushed_pending)
            && let Err(err) = self.update_mtime_ctime(handle.ino).await
        {
            if had_write {
                handle.mark_write_dirty();
            }
            return Err(err);
        }

        tracing::trace!(fh, ino = handle.ino, "vfs.fsync_snapshot_done");
        Ok(())
    }

    /// Open a directory handle for reading. Returns the file handle ID.
    /// This pre-loads all directory entries and starts background batch prefetch for attributes.
    #[tracing::instrument(level = "trace", skip(self), fields(ino))]
    pub async fn opendir(&self, ino: i64) -> Result<u64, VfsError> {
        let attr = self.meta_stat_required(ino, PathHint::none()).await?;
        let handle = self.meta_opendir(ino).await?.with_attr(attr);
        let fh = self.state.handles.allocate_dir(handle);

        Ok(fh)
    }

    /// Refresh a directory handle by re-reading entries from the meta layer.
    /// Keeps the same fh — the old handle is replaced in-place.
    /// Used for rewinddir(3): files created after opendir(3) must become
    /// visible after rewinddir(3) + readdir(3).
    pub async fn refresh_dir_handle(&self, fh: u64) -> Result<(), VfsError> {
        let ino = self
            .dir_handle(fh)
            .ok_or(VfsError::StaleNetworkFileHandle)?
            .ino;
        let fresh = self.meta_opendir(ino).await?;
        self.state.handles.replace_dir(fh, fresh);
        Ok(())
    }

    /// Close a directory handle
    pub fn closedir(&self, fh: u64) -> Result<(), VfsError> {
        let handle = self.release_dir_handle_required(fh)?;

        tracing::info!(
            "release dir handle: fh={}, ino={}, entries={}",
            fh,
            handle.ino,
            handle.entries.len()
        );

        // Check if prefetch task is still running
        let is_done = handle.prefetch_done.load(Ordering::Acquire);
        if !is_done {
            tracing::debug!(
                "Dir handle fh={}, ino={} released while prefetch still running - task will be aborted on drop",
                fh,
                handle.ino
            );
        }
        // When handle is dropped (Arc refcount reaches 0), DirHandle::drop() will abort the task

        Ok(())
    }

    /// Read directory entries by handle with pagination
    pub fn readdir(&self, fh: u64, offset: u64) -> Option<Vec<DirEntry>> {
        let handle = self.dir_handle(fh)?;

        Some(handle.get_entries(offset))
    }

    /// Update cached information about a handle (e.g. last observed offset).
    pub(crate) fn touch_handle_offset(&self, fh: u64, offset: u64) -> Result<(), VfsError> {
        let handle = self.file_handle_required(fh)?;
        handle.update_offset(offset);

        Ok(())
    }

    /// List all open handles for an inode.
    pub(crate) fn handles_for(&self, ino: i64) -> Vec<u64> {
        self.state.handles.handles_for(ino)
    }

    pub(crate) fn handle_attr(&self, fh: u64) -> Option<FileAttr> {
        self.state.handles.attr_for(fh)
    }

    pub(crate) fn handle_attr_by_ino(&self, ino: i64) -> Option<FileAttr> {
        self.state.handles.attr_for_inode(ino)
    }

    pub(crate) fn forget_recently_unlinked_attr(&self, ino: i64) {
        self.state.recently_unlinked.remove(&ino);
    }

    async fn discard_closed_unlinked_inode_state(&self, ino: i64) {
        if !self.state.handles.has_no_handle(ino) {
            return;
        }
        if let Some((_, inode)) = self.state.inodes.remove(&ino) {
            inode.bump_data_epoch();
        }
        self.state.reader.invalidate_all(ino as u64).await;
        self.state.writer.discard(ino as u64).await;
    }

    fn remember_recently_unlinked_attr(&self, ino: i64, mut attr: FileAttr) {
        if self.state.recently_unlinked.len() >= RECENTLY_UNLINKED_ATTR_CLEANUP_THRESHOLD
            && self
                .state
                .recently_unlinked_cleanup_tick
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(RECENTLY_UNLINKED_ATTR_CLEANUP_INTERVAL)
        {
            self.cleanup_recently_unlinked_attrs();
        }
        attr.nlink = 0;
        self.state
            .recently_unlinked
            .insert(ino, (attr, Instant::now()));
    }

    fn cleanup_recently_unlinked_attrs(&self) {
        let now = Instant::now();
        self.state.recently_unlinked.retain(|_, (_, inserted_at)| {
            now.duration_since(*inserted_at) <= RECENTLY_UNLINKED_ATTR_TTL
        });
    }

    /// Get file lock information for a given inode and query.
    pub(crate) async fn get_plock_ino(
        &self,
        inode: i64,
        query: &FileLockQuery,
    ) -> Result<FileLockInfo, VfsError> {
        self.meta_get_plock(inode, query).await
    }

    /// Set file lock for a given inode.
    pub(crate) async fn set_plock_ino(
        &self,
        inode: i64,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
        range: FileLockRange,
        pid: u32,
    ) -> Result<(), VfsError> {
        self.meta_set_plock(inode, owner, block, lock_type, range, pid)
            .await
    }

    pub(crate) fn remember_posix_lock_owner(
        &self,
        inode: i64,
        owner: i64,
        lock_type: FileLockType,
    ) {
        if lock_type != FileLockType::UnLock {
            self.state.posix_lock_owners.insert((inode, owner), ());
        }
    }

    pub(crate) fn remember_fuse_lock_owner(
        &self,
        inode: i64,
        fh: u64,
        owner: i64,
        lock_type: FileLockType,
    ) {
        if fh != 0 && lock_type != FileLockType::UnLock {
            self.state
                .fuse_lock_owners_by_handle
                .insert((inode, fh, owner), ());
        }
    }

    pub(crate) fn forget_fuse_lock_owner(&self, inode: i64, owner: i64) {
        let keys: Vec<_> = self
            .state
            .fuse_lock_owners_by_handle
            .iter()
            .filter_map(|entry| {
                let key = *entry.key();
                (key.0 == inode && key.2 == owner).then_some(key)
            })
            .collect();

        for key in keys {
            self.state.fuse_lock_owners_by_handle.remove(&key);
        }
    }

    pub(crate) fn take_fuse_lock_owners_for_handle(&self, inode: i64, fh: u64) -> Vec<i64> {
        let keys: Vec<_> = self
            .state
            .fuse_lock_owners_by_handle
            .iter()
            .filter_map(|entry| {
                let key = *entry.key();
                (key.0 == inode && key.1 == fh).then_some(key)
            })
            .collect();

        let mut owners = Vec::with_capacity(keys.len());
        for key in keys {
            if self.state.fuse_lock_owners_by_handle.remove(&key).is_some() {
                self.state.posix_lock_owners.remove(&(inode, key.2));
                owners.push(key.2);
            }
        }
        owners
    }

    pub(crate) fn take_posix_lock_owner(&self, inode: i64, owner: i64) -> bool {
        self.state
            .posix_lock_owners
            .remove(&(inode, owner))
            .is_some()
    }

    /// Set xattr for a given inode.
    pub async fn set_xattr_ino(
        &self,
        inode: i64,
        name: &str,
        value: &[u8],
        flags: u32,
    ) -> Result<(), VfsError> {
        self.meta_set_xattr(inode, name, value, flags).await
    }

    /// Get xattr for a given inode.
    pub async fn get_xattr_ino(&self, inode: i64, name: &str) -> Result<Option<Vec<u8>>, VfsError> {
        self.meta_get_xattr(inode, name).await
    }

    /// List xattr names for a given inode.
    pub async fn list_xattr_ino(&self, inode: i64) -> Result<Vec<String>, VfsError> {
        self.meta_list_xattr(inode).await
    }

    /// Remove xattr for a given inode.
    pub async fn remove_xattr_ino(&self, inode: i64, name: &str) -> Result<(), VfsError> {
        self.meta_remove_xattr(inode, name).await
    }

    /// Set ACL rule for a given inode.
    pub async fn set_acl_ino(&self, inode: i64, rule: AclRule) -> Result<(), VfsError> {
        self.meta_set_acl(inode, rule).await
    }

    /// Get ACL rule for a given inode.
    pub async fn get_acl_ino(
        &self,
        inode: i64,
        acl_type: u8,
        acl_id: u32,
    ) -> Result<Option<AclRule>, VfsError> {
        self.meta_get_acl(inode, acl_type, acl_id).await
    }

    /// Resolves a normalized path to its inode number, returning NotFound if absent.
    pub(crate) async fn lookup_path_to_ino(&self, path: &str) -> Result<i64, VfsError> {
        let (inode, _) = self.meta_lookup_path_required(path).await?;
        Ok(inode)
    }

    /// Get file lock information by path.
    pub async fn get_plock(
        &self,
        path: &str,
        query: &FileLockQuery,
    ) -> Result<FileLockInfo, VfsError> {
        let path = Self::norm_path(path);
        let inode = self.lookup_path_to_ino(&path).await?;
        self.meta_get_plock(inode, query).await
    }

    /// Set file lock by path.
    pub async fn set_plock(
        &self,
        path: &str,
        owner: i64,
        block: bool,
        lock_type: FileLockType,
        range: FileLockRange,
        pid: u32,
    ) -> Result<(), VfsError> {
        let path = Self::norm_path(path);
        let inode = self.lookup_path_to_ino(&path).await?;
        self.meta_set_plock(inode, owner, block, lock_type, range, pid)
            .await
    }

    /// Get file system statistics (total/available space and inodes).
    pub async fn stat_fs(&self) -> Result<StatFsSnapshot, VfsError> {
        self.meta_stat_fs().await
    }

    pub(crate) async fn stat_fs_cached_for_fuse(&self) -> Result<StatFsSnapshot, VfsError> {
        if let Some(snapshot) = {
            let cache = self
                .state
                .statfs_cache
                .lock()
                .expect("statfs cache lock poisoned");
            cache
                .as_ref()
                .filter(|entry| entry.cached_at.elapsed() <= FUSE_STATFS_CACHE_TTL)
                .map(|entry| entry.snapshot.clone())
        } {
            return Ok(snapshot);
        }

        let snapshot = self.meta_stat_fs().await?;
        let mut cache = self
            .state
            .statfs_cache
            .lock()
            .expect("statfs cache lock poisoned");
        *cache = Some(StatFsCache {
            snapshot: snapshot.clone(),
            cached_at: Instant::now(),
        });
        Ok(snapshot)
    }

    async fn ensure_inode_registered(&self, ino: i64) -> Result<Arc<Inode>, VfsError> {
        // Fast path to check whether there is an existing inode.
        if let Some(inode) = self.state.inodes.get(&ino) {
            return Ok(Arc::clone(inode.value()));
        }

        match self.lock_inode(ino) {
            Entry::Occupied(entry) => Ok(Arc::clone(entry.get())),
            Entry::Vacant(entry) => {
                let attr = self.meta_stat_required(ino, PathHint::none()).await?;
                if attr.kind != FileType::File {
                    let err = match attr.kind {
                        FileType::Dir => VfsError::IsADirectory {
                            path: PathHint::none(),
                        },
                        _ => VfsError::InvalidInput,
                    };
                    return Err(err);
                }

                let inode = Inode::new(ino, attr.size);
                entry.insert(inode.clone());
                Ok(inode)
            }
        }
    }

    fn lock_inode(&self, ino: i64) -> Entry<'_, i64, Arc<Inode>> {
        self.state.inodes.entry(ino)
    }
}

/// RAII guard for file handles that ensures close on drop.
pub struct FileGuard<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    vfs: VFS<S, M>,
    fh: u64,
    closed: bool,
}

impl<S, M> FileGuard<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    fn new(vfs: VFS<S, M>, fh: u64) -> Self {
        Self {
            vfs,
            fh,
            closed: false,
        }
    }

    pub fn fh(&self) -> u64 {
        self.fh
    }

    pub async fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, VfsError> {
        self.vfs.read(self.fh, offset, len).await
    }

    pub async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, VfsError> {
        self.vfs.write(self.fh, offset, data).await
    }

    pub async fn close(mut self) -> Result<(), VfsError> {
        self.closed = true;
        self.vfs.close(self.fh).await
    }
}

impl<S, M> Drop for FileGuard<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        close_handle_best_effort(self.vfs.clone(), self.fh);
    }
}

fn close_handle_best_effort<S, M>(vfs: VFS<S, M>, fh: u64)
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaLayer + Send + Sync + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            let _ = vfs.close(fh).await;
        });
        return;
    }

    let _ = std::thread::Builder::new()
        .name("brewfs-vfs-close".to_string())
        .spawn(move || {
            if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                let _ = rt.block_on(vfs.close(fh));
            }
        });
}

#[cfg(test)]
mod tests;
