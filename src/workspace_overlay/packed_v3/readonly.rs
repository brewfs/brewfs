//! Read-only VFS adapters for the packed-v3 snapshot.
//!
//! The authenticated current snapshot owns namespace and frame resolution. These adapters
//! deliberately keep the existing VFS contract: metadata exposes immutable
//! `SliceDesc` rows and the block store translates those synthetic rows back
//! into a bounded packed frame read.  No Redis/TiKV metadata client or loose
//! block cache is involved.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;

use crate::cadapter::client::ObjectBackend;
use crate::chunk::{BlockKey, BlockStore, SliceDesc};
use crate::meta::client::MetaClientMetrics;
use crate::meta::client::session::SessionInfo;
use crate::meta::file_lock::{FileLockInfo, FileLockQuery, FileLockRange, FileLockType};
use crate::meta::layer::MetaLayer;
use crate::meta::store::{
    AclRule, CreateEntryResult, DirEntry, FileAttr, FileType, MetaError, OpenFlags, SetAttrFlags,
    SetAttrRequest, StatFsSnapshot, stat_fs_snapshot_from_usage,
};
use crate::vfs::handles::{DirHandle, DirectoryPageSource, RawDirEntry};
use crate::vfs::{chunk_id_for, extract_ino_and_chunk_index};

use super::catalog::directory_key;
use super::index::PackedInodeIndexEntry;
use super::meta::GroupMetaEntry;
use super::wire::PackedWireError;

const READ_ONLY_ERROR: &str = "packed metadata v3 snapshot is read-only";
// The legacy Vec-returning MetaLayer API is a compatibility surface. Keep it
// bounded so a large immutable directory cannot turn a caller metadata
// request into an unbounded allocation. Callers that need larger directories
// must use `opendir`/the paged DirectoryPageSource API.
const MAX_LEGACY_READDIR_ENTRIES: usize = 4096;
const MAX_LEGACY_READDIR_BYTES: usize = 256 << 10;

mod transport;

#[derive(Debug, Default)]
pub(crate) struct V3ReadonlyMetrics {
    range_gets: std::sync::atomic::AtomicU64,
    requested_bytes: std::sync::atomic::AtomicU64,
    received_bytes: std::sync::atomic::AtomicU64,
    failures: std::sync::atomic::AtomicU64,
    logical_bytes: std::sync::atomic::AtomicU64,
    pub(crate) transport: Arc<transport::ReadonlyTransport>,
}
impl V3ReadonlyMetrics {
    pub(crate) fn logical_success(&self, bytes: u64) {
        self.logical_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub(crate) struct V3ReadonlyBackend<B: ObjectBackend + Clone + 'static> {
    client: crate::cadapter::client::ObjectClient<B>,
    metrics: Arc<V3ReadonlyMetrics>,
    budget: Arc<super::wire005::V3MountBudget>,
}

#[async_trait]
impl<B: ObjectBackend + Clone + 'static> ObjectBackend for V3ReadonlyBackend<B> {
    async fn put_object(&self, _key: &str, _bytes: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!(READ_ONLY_ERROR)
    }
    async fn get_object(&self, _key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        anyhow::bail!("wire 005 readonly path forbids whole-object GET")
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        bytes: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.metrics.range_gets.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .requested_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let length = bytes.len();
        let result = self
            .metrics
            .transport
            .run(&self.budget, key.len(), length as u64, || {
                let client = self.client.clone();
                let key = key.to_owned();
                async move {
                    let mut output = vec![0; length];
                    let actual = client.get_object_range(&key, offset, &mut output).await?;
                    anyhow::ensure!(actual <= output.len(), "readonly backend range overflow");
                    output.truncate(actual);
                    Ok(output)
                }
            })
            .await;
        match result {
            Ok(output) => {
                let length = output.value.len();
                bytes[..length].copy_from_slice(&output.value);
                self.metrics
                    .received_bytes
                    .fetch_add(length as u64, Ordering::Relaxed);
                Ok(length)
            }
            Err(error) => {
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<crate::cadapter::client::ObjectByteStream> {
        use futures_util::StreamExt;
        self.metrics.range_gets.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .requested_bytes
            .fetch_add(length, Ordering::Relaxed);
        let stream = match self
            .metrics
            .transport
            .open_body(&self.budget, key.len(), || {
                let client = self.client.clone();
                let key = key.to_owned();
                async move { client.get_object_range_stream(&key, offset, length).await }
            })
            .await
        {
            Ok(stream) => stream,
            Err(error) => {
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        let metrics = Arc::clone(&self.metrics);
        Ok(Box::pin(stream.map(move |result| {
            match &result {
                Ok(bytes) => {
                    metrics
                        .received_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                }
                Err(_) => {
                    metrics.failures.fetch_add(1, Ordering::Relaxed);
                }
            }
            result
        })))
    }
    async fn get_object_range_stream_observed(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        context: crate::cadapter::read_observer::ReadContext,
        observer: Arc<crate::cadapter::read_observer::ReadObserver>,
    ) -> anyhow::Result<crate::cadapter::client::ObjectByteStream> {
        use futures_util::StreamExt;
        self.metrics.range_gets.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .requested_bytes
            .fetch_add(length, Ordering::Relaxed);
        let stream = match self
            .metrics
            .transport
            .open_body(&self.budget, key.len(), || {
                let client = self.client.clone();
                let key = key.to_owned();
                async move {
                    client
                        .backend_range_stream_observed(&key, offset, length, context, observer)
                        .await
                }
            })
            .await
        {
            Ok(stream) => stream,
            Err(error) => {
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        let metrics = Arc::clone(&self.metrics);
        Ok(Box::pin(stream.map(move |result| {
            match &result {
                Ok(bytes) => {
                    metrics
                        .received_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                }
                Err(_) => {
                    metrics.failures.fetch_add(1, Ordering::Relaxed);
                }
            }
            result
        })))
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        let result = self
            .metrics
            .transport
            .run(&self.budget, key.len(), 0, || {
                let client = self.client.clone();
                let key = key.to_owned();
                async move { client.get_etag(&key).await }
            })
            .await?;
        Ok(result.value.clone())
    }

    async fn get_etag_observed(
        &self,
        key: &str,
        context: crate::cadapter::read_observer::ReadContext,
        observer: Arc<crate::cadapter::read_observer::ReadObserver>,
    ) -> anyhow::Result<String> {
        let result = self
            .metrics
            .transport
            .run(&self.budget, key.len(), 0, || {
                let client = self.client.clone();
                let key = key.to_owned();
                async move { client.backend_etag_observed(&key, context, observer).await }
            })
            .await?;
        Ok(result.value.clone())
    }
    async fn delete_object(&self, _key: &str) -> anyhow::Result<()> {
        anyhow::bail!(READ_ONLY_ERROR)
    }
}

pub(crate) fn shared_v3_readonly_client<B: ObjectBackend + Clone + 'static>(
    client: crate::cadapter::client::ObjectClient<B>,
    observer: Arc<crate::cadapter::read_observer::ReadObserver>,
    engine: crate::cadapter::read_observer::Engine,
    phase: crate::cadapter::read_observer::Phase,
    budget: Arc<super::wire005::V3MountBudget>,
) -> (
    crate::cadapter::client::ObjectClient<V3ReadonlyBackend<B>>,
    Arc<V3ReadonlyMetrics>,
) {
    let metrics = Arc::new(V3ReadonlyMetrics::default());
    let client = crate::cadapter::client::ObjectClient::new(V3ReadonlyBackend {
        client: client.without_read_observer(),
        metrics: metrics.clone(),
        budget,
    })
    .with_read_observer(
        observer,
        engine,
        phase,
        crate::cadapter::read_observer::Origin::Demand,
    );
    (client, metrics)
}

#[derive(Debug)]
struct V3StatsExtension {
    metrics: Arc<V3ReadonlyMetrics>,
    budget: Arc<super::wire005::V3MountBudget>,
    observer: Arc<crate::cadapter::read_observer::ReadObserver>,
    index: super::wire005::V3IndexCacheStats,
}
impl crate::vfs::stats::FsStatsExtension for V3StatsExtension {
    fn render_max_bytes(&self) -> usize {
        super::wire005::V3MountBudget::STATS_EXTENSION_RENDER_MAX_BYTES
            .saturating_add(self.observer.render_max_bytes())
    }
    fn begin_stats_observation(&self) -> Option<crate::cadapter::read_observer::TerminalGuard> {
        Some(self.observer.start(
            crate::cadapter::read_observer::Ledger::LogicalOperation,
            crate::cadapter::read_observer::ReadContext {
                engine: crate::cadapter::read_observer::Engine::PackedV3,
                phase: crate::cadapter::read_observer::Phase::Runtime,
                class: crate::cadapter::read_observer::ReadClass::StatsSnapshot,
                origin: crate::cadapter::read_observer::Origin::StatsObserver,
            },
            0,
        ))
    }
    fn render_into(&self, output: &mut dyn std::fmt::Write) {
        // Runtime backend calls/response-body bytes, including metadata and
        // payload. Initial manifest/probe and SDK-internal retries are excluded.
        for (name, counter) in [
            ("runtime_backend_range_gets_total", &self.metrics.range_gets),
            (
                "runtime_backend_requested_bytes_total",
                &self.metrics.requested_bytes,
            ),
            (
                "runtime_backend_received_bytes_total",
                &self.metrics.received_bytes,
            ),
            ("runtime_backend_failures_total", &self.metrics.failures),
            ("logical_bytes_total", &self.metrics.logical_bytes),
        ] {
            let _ = writeln!(
                output,
                "brewfs_packed_v3_{name} {}",
                counter.load(Ordering::Relaxed)
            );
        }
        self.budget.render_into(output);
        self.index.render_into(output);
        self.observer.render_into(output);
    }
}

struct V3ReadonlyContext<B: ObjectBackend + Clone + 'static> {
    client: crate::cadapter::client::ObjectClient<V3ReadonlyBackend<B>>,
    snapshot: super::wire005::AuthenticatedV3Snapshot,
    reader: super::wire005::V3IndexReader<V3ReadonlyBackend<B>>,
    metrics: Arc<V3ReadonlyMetrics>,
    budget: Arc<super::wire005::V3MountBudget>,
    _roots: super::wire005::V3OwnedPermit,
}

// The context retains the actual authenticated snapshot, index reader and
// Roots admission independently of the adapter and its final path consumer.
struct ReadonlyPathsOwner<B: ObjectBackend + Clone + 'static> {
    _permit: super::wire005::V3OwnedPermit,
    _context: Arc<V3ReadonlyContext<B>>,
}
impl<B: ObjectBackend + Clone + 'static> std::fmt::Debug for ReadonlyPathsOwner<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadonlyPathsOwner").finish_non_exhaustive()
    }
}

fn map_error(error: PackedWireError) -> MetaError {
    if matches!(error, PackedWireError::ReadViewChanged) {
        return MetaError::Anyhow(anyhow::Error::new(crate::chunk::read_plan::ReadViewChanged));
    }
    if matches!(error, PackedWireError::LimitExceeded(_)) {
        return MetaError::Io(std::io::Error::from_raw_os_error(libc::ENOMEM));
    }
    match &error {
        PackedWireError::Backend(message) => {
            if message.starts_with("S3 range service failure:")
                || message.starts_with("S3 range request failure:")
                || message.starts_with("S3 range stream failure:")
            {
                tracing::warn!(error = %message, "packed readonly metadata backend read failed");
            } else {
                tracing::warn!("packed readonly metadata backend read failed");
            }
        }
        _ => tracing::warn!(error = %error, "packed readonly metadata validation failed"),
    }
    MetaError::Internal(error.to_string())
}

fn packed_error_to_anyhow(error: PackedWireError) -> anyhow::Error {
    match error {
        PackedWireError::ReadViewChanged => {
            anyhow::Error::new(crate::chunk::read_plan::ReadViewChanged)
        }
        error => anyhow::Error::new(error),
    }
}

fn file_type(kind: u8, mode: u32) -> FileType {
    match kind {
        1 => FileType::File,
        2 => FileType::Dir,
        3 => FileType::Symlink,
        4 => FileType::Fifo,
        5 => FileType::Socket,
        6 => FileType::CharDevice,
        7 => FileType::BlockDevice,
        _ => FileType::from_mode(mode),
    }
}

fn attr(inode: u64, entry: &GroupMetaEntry) -> FileAttr {
    FileAttr {
        ino: inode as i64,
        size: entry.size,
        blocks: entry.size.div_ceil(512),
        kind: file_type(entry.kind, entry.mode),
        mode: entry.mode,
        rdev: entry.rdev.min(u64::from(u32::MAX)) as u32,
        uid: entry.uid,
        gid: entry.gid,
        atime: entry.atime_ns,
        mtime: entry.mtime_ns,
        ctime: entry.ctime_ns,
        nlink: entry.nlink,
    }
}

fn attr_from_inode_index(index: &PackedInodeIndexEntry) -> FileAttr {
    FileAttr {
        ino: index.inode as i64,
        size: index.size,
        blocks: index.size.div_ceil(512),
        kind: file_type(index.kind, index.mode),
        mode: index.mode,
        rdev: index.rdev.min(u64::from(u32::MAX)) as u32,
        uid: index.uid,
        gid: index.gid,
        atime: index.atime_ns,
        mtime: index.mtime_ns,
        ctime: index.ctime_ns,
        nlink: index.nlink,
    }
}

fn attr_from_root(root: &super::wire005::V3RootAttributes) -> FileAttr {
    FileAttr {
        ino: root.inode as i64,
        size: root.size,
        blocks: root.blocks,
        kind: FileType::Dir,
        mode: root.mode,
        rdev: 0,
        uid: root.uid,
        gid: root.gid,
        atime: root.atime_ns,
        mtime: root.mtime_ns,
        ctime: root.ctime_ns,
        nlink: root.nlink,
    }
}

/// A block-store facade over immutable packed frames.
///
/// Legacy catalogs cannot create a readonly block store:
/// ```compile_fail
/// use brewfs::cadapter::localfs::LocalFsBackend;
/// use brewfs::workspace_overlay::packed_v3::PackedV3BlockStore;
/// let _ = PackedV3BlockStore::<LocalFsBackend>::new;
/// ```
#[derive(Clone)]
pub struct PackedV3BlockStore<B: ObjectBackend + Clone + 'static> {
    chunk_size: u64,
    block_size: u64,
    context: Arc<V3ReadonlyContext<B>>,
}

impl<B: ObjectBackend + Clone + 'static> PackedV3BlockStore<B> {
    fn from_context(
        context: Arc<V3ReadonlyContext<B>>,
        chunk_size: u64,
        block_size: u32,
    ) -> Result<Self, MetaError> {
        if chunk_size == 0 || block_size == 0 || chunk_size < u64::from(block_size) {
            return Err(MetaError::Internal("invalid packed v3 block layout".into()));
        }
        Ok(Self {
            context,
            chunk_size,
            block_size: u64::from(block_size),
        })
    }

    fn range_for(
        &self,
        key: BlockKey,
        offset: u64,
        len: usize,
    ) -> Result<(u64, u64), anyhow::Error> {
        let (ino, chunk_index) = extract_ino_and_chunk_index(key.0);
        if ino <= 0 || chunk_id_for(ino, chunk_index)? != key.0 {
            anyhow::bail!("invalid packed v3 chunk id {}", key.0);
        }
        let absolute = chunk_index
            .checked_mul(self.chunk_size)
            .and_then(|value| value.checked_add(u64::from(key.1).checked_mul(self.block_size)?))
            .and_then(|value| value.checked_add(offset))
            .ok_or_else(|| anyhow::anyhow!("packed v3 read offset overflows"))?;
        let length = u64::try_from(len).map_err(|_| anyhow::anyhow!("read length exceeds u64"))?;
        Ok((
            u64::try_from(ino).unwrap(),
            absolute
                .checked_add(length)
                .ok_or_else(|| anyhow::anyhow!("packed v3 read end overflows"))?,
        ))
    }
}

#[async_trait]
impl<B> BlockStore for PackedV3BlockStore<B>
where
    B: ObjectBackend + Clone + Send + Sync + 'static,
{
    async fn write_fresh_range(
        &self,
        _key: BlockKey,
        _offset: u64,
        _data: &[u8],
    ) -> anyhow::Result<u64> {
        anyhow::bail!(READ_ONLY_ERROR)
    }

    async fn read_range(&self, key: BlockKey, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let (inode, end) = self.range_for(key, offset, buf.len())?;
        let start = end - u64::try_from(buf.len())?;
        let context = &self.context;
        context
            .snapshot
            .read_inode_range(
                &context.client,
                &context.reader,
                inode,
                start,
                buf,
                32 * 1024 * 1024,
            )
            .await
            .map_err(packed_error_to_anyhow)?;
        context
            .metrics
            .logical_bytes
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    async fn delete_range(&self, _key: BlockKey, _block_count: u64) -> anyhow::Result<()> {
        anyhow::bail!(READ_ONLY_ERROR)
    }
}

struct PackedDirectoryPageSource<B: ObjectBackend + Clone + 'static> {
    context: Arc<V3ReadonlyContext<B>>,
}

#[async_trait]
impl<B> DirectoryPageSource for PackedDirectoryPageSource<B>
where
    B: ObjectBackend + Clone + Send + Sync + 'static,
{
    async fn read_page_owned(
        &self,
        ino: i64,
        offset: u64,
        max_entries: usize,
    ) -> Result<crate::vfs::handles::OwnedDirectoryPage, MetaError> {
        let guard = self
            .context
            .budget
            .admit(&[
                (super::wire005::V3BudgetPool::Output, 1 << 20),
                (super::wire005::V3BudgetPool::Control, 4096),
            ])
            .map_err(map_error)?;
        let entries = self.read_page(ino, offset, max_entries).await?;
        Ok(crate::vfs::handles::OwnedDirectoryPage {
            entries,
            guard: Some(Arc::new(guard)),
        })
    }
    async fn read_page(
        &self,
        ino: i64,
        child_offset: u64,
        max_entries: usize,
    ) -> Result<Vec<RawDirEntry>, MetaError> {
        let parent = u64::try_from(ino).map_err(|_| MetaError::NotFound(ino))?;
        let context = &self.context;
        let manifest = context.snapshot.manifest();
        let key = if parent == manifest.root_inode {
            manifest.root_dir_key
        } else {
            directory_key(manifest.snapshot_id, parent)
        };
        let entries = context
            .snapshot
            .readdir_page(
                &context.client,
                &context.reader,
                key,
                child_offset,
                max_entries,
                512 * 1024,
            )
            .await
            .map_err(map_error)?;
        entries
            .into_iter()
            .map(|entry| {
                Ok(RawDirEntry {
                    name: entry.name,
                    ino: i64::try_from(entry.inode)
                        .map_err(|_| MetaError::Internal("packed inode exceeds i64".into()))?,
                    kind: file_type(entry.kind, entry.mode),
                })
            })
            .collect()
    }
}

/// Immutable metadata facade for a packed-v3 manifest.
///
/// Only authenticated current v3 snapshots can construct this adapter:
/// ```compile_fail
/// use brewfs::cadapter::localfs::LocalFsBackend;
/// use brewfs::workspace_overlay::packed_v3::PackedV3ReadonlyMeta;
/// let _ = PackedV3ReadonlyMeta::<LocalFsBackend>::new;
/// ```
/// ```compile_fail
/// use brewfs::cadapter::localfs::LocalFsBackend;
/// use brewfs::workspace_overlay::packed_v3::PackedV3ReadonlyMeta;
/// let _ = PackedV3ReadonlyMeta::<LocalFsBackend>::catalog;
/// ```
pub struct PackedV3ReadonlyMeta<B: ObjectBackend + Clone + 'static> {
    root: AtomicI64,
    chunk_size: u64,
    context: Arc<V3ReadonlyContext<B>>,
}

/// One authenticated reverse-index page and its memory owner.
pub(crate) struct PackedLowerReverseNames {
    pub rows: Vec<(i64, Vec<u8>)>,
    pub after: Option<Vec<u8>>,
    _permit: super::wire005::V3OwnedPermit,
}

/// Metadata-only template with its actual authenticated index/root owner.
pub(crate) struct PackedLowerInodeMetadata {
    pub attr: FileAttr,
    pub parent_hint: Option<i64>,
    _permit: super::wire005::V3OwnedPermit,
}

impl<B: ObjectBackend + Clone + 'static> PackedV3ReadonlyMeta<B> {
    pub fn from_v3(
        client: crate::cadapter::client::ObjectClient<B>,
        snapshot: super::wire005::AuthenticatedV3Snapshot,
        chunk_size: u64,
        metadata_bytes: u64,
    ) -> Self {
        Self::from_v3_budget(
            client,
            snapshot,
            chunk_size,
            metadata_bytes,
            super::wire005::V3MountBudget::defaults(),
        )
        .expect("default budgets admit a valid profile frame")
    }

    pub fn from_v3_budget(
        client: crate::cadapter::client::ObjectClient<B>,
        snapshot: super::wire005::AuthenticatedV3Snapshot,
        chunk_size: u64,
        metadata_bytes: u64,
        budget: Arc<super::wire005::V3MountBudget>,
    ) -> Result<Self, PackedWireError> {
        let source = snapshot.manifest();
        budget.validate_frame_capability(
            source
                .size_classes
                .max_random_frame_raw_bytes
                .max(source.size_classes.max_sequential_frame_raw_bytes) as usize,
        )?;
        let roots = budget.admit(&[(super::wire005::V3BudgetPool::Roots, 128 << 10)])?;
        let root = AtomicI64::new(source.root_inode as i64);
        let observer = budget.read_observer(client.read_observer())?;
        let (client, metrics) = shared_v3_readonly_client(
            client,
            observer,
            crate::cadapter::read_observer::Engine::PackedV3,
            crate::cadapter::read_observer::Phase::Runtime,
            budget.clone(),
        );
        let context = Arc::new(V3ReadonlyContext {
            reader: super::wire005::V3IndexReader::with_budget(
                client.clone(),
                metadata_bytes,
                budget.clone(),
            ),
            client,
            snapshot,
            metrics,
            budget,
            _roots: roots,
        });
        Ok(Self {
            root,
            chunk_size,
            context,
        })
    }

    pub fn install_v3_stats(&self, stats: &crate::vfs::stats::FsStats) -> bool {
        stats.set_extension(Arc::new(V3StatsExtension {
            metrics: Arc::clone(&self.context.metrics),
            budget: Arc::clone(&self.context.budget),
            index: self.context.reader.cache_stats(),
            observer: self
                .context
                .client
                .read_observer()
                .expect("v3 context carries its observer"),
        }))
    }

    pub fn block_store(&self, block_size: u32) -> Result<PackedV3BlockStore<B>, MetaError> {
        PackedV3BlockStore::from_context(self.context.clone(), self.chunk_size, block_size)
    }

    pub fn manifest_reference(&self) -> &super::wire005::V3ObjectRef {
        self.context.snapshot.manifest_reference()
    }

    pub fn mount_budget(&self) -> Arc<super::wire005::V3MountBudget> {
        Arc::clone(&self.context.budget)
    }

    pub(crate) async fn frozen_cold_attributes_owned(
        &self,
        inode: i64,
    ) -> Result<Option<super::wire005::V3Owned<super::wire005::V3ColdAttributes>>, MetaError> {
        self.context
            .snapshot
            .cold_attributes_owned(
                &self.context.client,
                &self.context.reader,
                Self::inode(inode)?,
            )
            .await
            .map_err(map_error)
    }

    pub(crate) async fn frozen_reverse_names_page_owned(
        &self,
        inode: i64,
        after: Option<&[u8]>,
    ) -> Result<PackedLowerReverseNames, MetaError> {
        let permit = self
            .context
            .budget
            .admit(&[(super::wire005::V3BudgetPool::Metadata, 128 << 10)])
            .map_err(map_error)?;
        let page = self
            .context
            .snapshot
            .reverse_names_page(&self.context.reader, Self::inode(inode)?, after, 32)
            .await
            .map_err(map_error)?;
        let mut rows = Vec::with_capacity(page.len());
        let mut cursor = after.map(ToOwned::to_owned);
        for location in page {
            let key = location.reverse_key();
            let parent = i64::try_from(location.hot.parent_inode)
                .map_err(|_| MetaError::Internal("packed reverse parent exceeds i64".into()))?;
            let name = location.hot.name;
            if parent <= 0
                || name.is_empty()
                || name.len() > 255
                || name == b"."
                || name == b".."
                || name.contains(&0)
                || name.contains(&b'/')
                || cursor.as_ref().is_some_and(|previous| key <= *previous)
            {
                return Err(MetaError::Internal(
                    "invalid authenticated reverse-name page".into(),
                ));
            }
            cursor = Some(key);
            rows.push((parent, name));
        }
        Ok(PackedLowerReverseNames {
            rows,
            after: cursor,
            _permit: permit,
        })
    }

    pub(crate) async fn frozen_inode_metadata_owned(
        &self,
        inode: i64,
    ) -> Result<Option<PackedLowerInodeMetadata>, MetaError> {
        let permit = self
            .context
            .budget
            .admit(&[(super::wire005::V3BudgetPool::Metadata, 4096)])
            .map_err(map_error)?;
        let number = Self::inode(inode)?;
        if number == self.context.snapshot.manifest().root_inode {
            return Ok(Some(PackedLowerInodeMetadata {
                attr: attr_from_root(
                    &self
                        .context
                        .snapshot
                        .manifest()
                        .source
                        .as_ref()
                        .ok_or_else(|| {
                            MetaError::Internal("packed root attributes missing".into())
                        })?
                        .root,
                ),
                parent_hint: None,
                _permit: permit,
            }));
        }
        let Some(hot) = self.index_entry(number).await? else {
            return Ok(None);
        };
        if hot.rdev > u64::from(u32::MAX) {
            return Err(MetaError::Internal(
                "packed inode rdev exceeds native metadata".into(),
            ));
        }
        let parent = i64::try_from(hot.parent_inode).map_err(|_| {
            MetaError::Internal("packed inode parent exceeds native identity".into())
        })?;
        Ok(Some(PackedLowerInodeMetadata {
            attr: attr_from_inode_index(&hot),
            parent_hint: Some(parent),
            _permit: permit,
        }))
    }

    /// Stop backend admission, join the real frame worker and wait every
    /// started response driver/body. The caller owns pin/budget retirement.
    pub(crate) async fn drain_packed_transport(&self) -> Result<(), MetaError> {
        self.context.metrics.transport.stop_admission();
        self.context.reader.close().await;
        self.context
            .metrics
            .transport
            .drain()
            .await
            .map_err(MetaError::Anyhow)
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub(crate) fn bind_reader_session(
        &self,
        reader: Arc<dyn crate::workspace_overlay::packed_reader_lifecycle::PackedReaderSession>,
    ) -> Result<(), MetaError> {
        if !Arc::ptr_eq(&self.context.budget, &reader.mount_budget())
            || &reader.binding().manifest != self.manifest_reference()
        {
            return Err(MetaError::Internal(
                "packed lower transport reader identity mismatch".into(),
            ));
        }
        self.context
            .metrics
            .transport
            .bind_reader(reader)
            .map_err(MetaError::Anyhow)
    }

    async fn index_entry(&self, inode: u64) -> Result<Option<PackedInodeIndexEntry>, MetaError> {
        let context = &self.context;
        context
            .snapshot
            .lookup_inode(&context.reader, inode)
            .await
            .map(|value| value.map(|location| location.hot))
            .map_err(map_error)
    }

    async fn with_source_blocks(&self, mut attributes: FileAttr) -> Result<FileAttr, MetaError> {
        let context = &self.context;
        if let Some(blocks) = context
            .snapshot
            .source_blocks(&context.reader, attributes.ino as u64)
            .await
            .map_err(map_error)?
        {
            attributes.blocks = blocks;
        }
        Ok(attributes)
    }

    async fn lookup_entry(
        &self,
        parent: [u8; 32],
        name: &[u8],
    ) -> Result<Option<GroupMetaEntry>, MetaError> {
        let context = &self.context;
        context
            .snapshot
            .lookup_dentry(&context.client, &context.reader, parent, name, 512 * 1024)
            .await
            .map_err(map_error)
    }

    fn inode(ino: i64) -> Result<u64, MetaError> {
        u64::try_from(ino).map_err(|_| MetaError::NotFound(ino))
    }

    fn parent_key(&self, inode: u64) -> [u8; 32] {
        let manifest = self.context.snapshot.manifest();
        if inode == manifest.root_inode {
            manifest.root_dir_key
        } else {
            directory_key(manifest.snapshot_id, inode)
        }
    }

    async fn entry(&self, inode: u64) -> Result<Option<GroupMetaEntry>, MetaError> {
        let context = &self.context;
        context
            .snapshot
            .inode_entry(&context.client, &context.reader, inode, 512 * 1024)
            .await
            .map_err(map_error)
    }

    async fn readonly<T>() -> Result<T, MetaError> {
        Err(MetaError::Io(std::io::Error::from_raw_os_error(
            libc::EROFS,
        )))
    }

    fn chunk_slices(
        &self,
        chunk_id: u64,
        entry: &GroupMetaEntry,
        chunk_index: u64,
    ) -> Result<Vec<SliceDesc>, MetaError> {
        let chunk_start = chunk_index
            .checked_mul(self.chunk_size)
            .ok_or_else(|| MetaError::Internal("packed chunk offset overflows".into()))?;
        let chunk_end = chunk_start.saturating_add(self.chunk_size);

        // Inline bytes live in GroupMeta rather than in a frame extent.  The
        // DataFetcher still asks MetaLayer for visible slices, so expose
        // the inline range as a logical slice.  PackedV3BlockStore resolves
        // the resulting block read back to the inode and serves it from the
        // already-decoded GroupMeta payload without issuing a frame request.
        if !entry.inline_data.is_empty() {
            let start = chunk_start.min(entry.size);
            let end = chunk_end.min(entry.size);
            if start < end {
                return Ok(vec![SliceDesc {
                    slice_id: chunk_id,
                    chunk_id,
                    offset: start - chunk_start,
                    length: end - start,
                }]);
            }
            return Ok(Vec::new());
        }

        let mut slices = Vec::new();
        for extent in &entry.extents {
            let extent_end = extent
                .file_offset
                .saturating_add(u64::from(extent.logical_len));
            let start = extent.file_offset.max(chunk_start);
            let end = extent_end.min(chunk_end).min(entry.size);
            if start < end {
                slices.push(SliceDesc {
                    slice_id: chunk_id,
                    chunk_id,
                    offset: start - chunk_start,
                    length: end - start,
                });
            }
        }
        Ok(slices)
    }
}

#[cfg(test)]
#[path = "readonly/tests.rs"]
mod tests;

#[async_trait]
impl<B> MetaLayer for PackedV3ReadonlyMeta<B>
where
    B: ObjectBackend + Clone + Send + Sync + 'static,
{
    fn reserve_inline_roots(
        &self,
        bytes: u64,
    ) -> Result<Option<asyncfuse::raw::reply::InlineRootPermit>, MetaError> {
        let permit = self
            .context
            .budget
            .admit(&[(super::wire005::V3BudgetPool::Roots, bytes)])
            .map_err(map_error)?;
        let permit = asyncfuse::raw::reply::InlineRootPermit::try_new(permit).map_err(|_| {
            map_error(PackedWireError::LimitExceeded(
                "inline Roots permit layout".into(),
            ))
        })?;
        Ok(Some(permit))
    }
    fn reserve_memory(
        &self,
        kind: crate::meta::layer::MetadataMemoryKind,
        bytes: u64,
    ) -> Result<Option<crate::meta::layer::MetadataMemoryGuard>, MetaError> {
        use super::wire005::V3BudgetPool;
        use crate::meta::layer::MetadataMemoryKind;
        let context = &self.context;
        let checked = |extra| {
            bytes.checked_add(extra).ok_or_else(|| {
                map_error(PackedWireError::LimitExceeded(
                    "FUSE memory size overflow".into(),
                ))
            })
        };
        if matches!(kind, MetadataMemoryKind::Roots) {
            let permit = context
                .budget
                .admit(&[(V3BudgetPool::Roots, bytes)])
                .map_err(map_error)?;
            return Ok(Some(Arc::new(permit)));
        }
        let charges = match kind {
            MetadataMemoryKind::Roots => vec![(V3BudgetPool::Roots, bytes)],
            MetadataMemoryKind::Request => vec![
                (V3BudgetPool::Control, 8192),
                (V3BudgetPool::Metadata, checked(2 << 20)?),
            ],
            MetadataMemoryKind::Reply => vec![
                (
                    V3BudgetPool::Output,
                    checked(super::wire005::V3MountBudget::REPLY_ALLOCATION_ALLOWANCE_BYTES)?,
                ),
                (V3BudgetPool::Control, 4096),
            ],
            // Attributes, reader state and immutable stats handles persist
            // beyond a request. Preserve their full bound and final guard,
            // independently of transient request/queue/cancellation controls.
            MetadataMemoryKind::Handle => vec![(V3BudgetPool::Metadata, bytes.max(8192))],
            MetadataMemoryKind::Control => vec![(V3BudgetPool::Control, bytes)],
        };
        Ok(Some(Arc::new(
            context.budget.admit(&charges).map_err(map_error)?,
        )))
    }
    fn name(&self) -> &'static str {
        "packed-metadata-v3-readonly"
    }
    fn supports_fuse_read_cancellation(&self) -> bool {
        true
    }
    fn posix_acl_capability(&self) -> crate::meta::layer::PosixAclCapability {
        crate::meta::layer::PosixAclCapability::ReadOnly
    }
    fn metrics(&self) -> Option<Arc<MetaClientMetrics>> {
        None
    }
    fn root_ino(&self) -> i64 {
        self.root.load(Ordering::Acquire)
    }
    fn chroot(&self, inode: i64) {
        self.root.store(inode, Ordering::Release);
    }
    async fn initialize(&self) -> Result<(), MetaError> {
        Ok(())
    }

    async fn stat_fs(&self) -> Result<StatFsSnapshot, MetaError> {
        // PM10 keeps aggregate usage out of the hot manifest so opening a
        // mount never requires scanning every inode page. Report the stable
        // default capacity until a future manifest adds authenticated totals.
        Ok(stat_fs_snapshot_from_usage(0, 0))
    }

    async fn stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        self.stat_fresh(ino).await
    }
    async fn stat_fresh(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        let inode = Self::inode(ino)?;
        let manifest = self.context.snapshot.manifest();
        if inode == manifest.root_inode {
            if let Some(source) = &manifest.source {
                return Ok(Some(attr_from_root(&source.root)));
            }
            return Ok(Some(FileAttr {
                ino,
                size: 0,
                blocks: 0,
                kind: FileType::Dir,
                mode: 0o040755,
                rdev: 0,
                uid: 0,
                gid: 0,
                atime: 0,
                mtime: 0,
                ctime: 0,
                nlink: 2,
            }));
        }
        // II05 carries the complete hot attribute set.  Use it directly for
        // getattr/open so a metadata-only operation does not fetch or clone a
        // GroupMeta page merely to reconstruct FileAttr.
        if let Some(index) = self.index_entry(inode).await? {
            return Ok(Some(
                self.with_source_blocks(attr_from_inode_index(&index))
                    .await?,
            ));
        }
        match self.entry(inode).await? {
            Some(entry) => Ok(Some(self.with_source_blocks(attr(inode, &entry)).await?)),
            None => Ok(None),
        }
    }
    async fn lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        let parent = Self::inode(parent)?;
        let entry = self
            .lookup_entry(self.parent_key(parent), name.as_bytes())
            .await?;
        entry
            .map(|entry| {
                i64::try_from(entry.inode)
                    .map_err(|_| MetaError::Internal("packed inode exceeds i64".into()))
            })
            .transpose()
    }
    async fn lookup_with_attr(
        &self,
        parent: i64,
        name: &str,
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        let parent = Self::inode(parent)?;
        let Some(entry) = self
            .lookup_entry(self.parent_key(parent), name.as_bytes())
            .await?
        else {
            return Ok(None);
        };
        let inode = i64::try_from(entry.inode)
            .map_err(|_| MetaError::Internal("packed inode exceeds i64".into()))?;
        Ok(Some((
            inode,
            self.with_source_blocks(attr(entry.inode, &entry)).await?,
        )))
    }
    async fn lookup_with_attr_bytes(
        &self,
        parent: i64,
        name: &[u8],
    ) -> Result<Option<(i64, FileAttr)>, MetaError> {
        let parent = Self::inode(parent)?;
        let Some(entry) = self.lookup_entry(self.parent_key(parent), name).await? else {
            return Ok(None);
        };
        let inode = i64::try_from(entry.inode)
            .map_err(|_| MetaError::Internal("packed inode exceeds i64".into()))?;
        Ok(Some((
            inode,
            self.with_source_blocks(attr(entry.inode, &entry)).await?,
        )))
    }
    async fn lookup_path(&self, path: &str) -> Result<Option<(i64, FileType)>, MetaError> {
        if path.is_empty() || !path.starts_with('/') {
            return Err(MetaError::InvalidPath(path.into()));
        }
        let mut inode = self.root_ino();
        for component in path.split('/').filter(|component| !component.is_empty()) {
            if component == "." {
                continue;
            }
            if component == ".." {
                inode = self.get_dir_parent(inode).await?.unwrap_or(self.root_ino());
                continue;
            }
            let Some(next) = self.lookup(inode, component).await? else {
                return Ok(None);
            };
            inode = next;
        }
        let Some(file) = self.stat(inode).await? else {
            return Ok(None);
        };
        Ok(Some((inode, file.kind)))
    }
    async fn readdir(&self, ino: i64) -> Result<Vec<DirEntry>, MetaError> {
        let mut offset = 0usize;
        let mut result = Vec::new();
        let mut bytes = 0usize;
        loop {
            let page = self.readdir_page_raw(ino, offset, 256).await?;
            if page.is_empty() {
                break;
            }
            offset += page.len();
            for entry in page {
                let entry_bytes = entry.name.len().saturating_add(32);
                if result.len() >= MAX_LEGACY_READDIR_ENTRIES
                    || bytes.saturating_add(entry_bytes) > MAX_LEGACY_READDIR_BYTES
                {
                    return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                        libc::E2BIG,
                    )));
                }
                bytes = bytes.saturating_add(entry_bytes);
                result.push(DirEntry {
                    name: String::from_utf8(entry.name).map_err(|_| MetaError::InvalidFilename)?,
                    ino: i64::try_from(entry.inode)
                        .map_err(|_| MetaError::Internal("packed inode exceeds i64".into()))?,
                    kind: file_type(entry.kind, entry.mode),
                });
            }
        }
        Ok(result)
    }
    async fn opendir(&self, ino: i64) -> Result<DirHandle, MetaError> {
        let inode = Self::inode(ino)?;
        let is_dir = if inode == self.context.snapshot.manifest().root_inode {
            true
        } else {
            self.entry(inode)
                .await?
                .map(|entry| file_type(entry.kind, entry.mode).is_dir())
                .ok_or(MetaError::NotFound(ino))?
        };
        if !is_dir {
            return Err(MetaError::NotDirectory(ino));
        }
        Ok(DirHandle::new_paged(
            ino,
            Arc::new(PackedDirectoryPageSource {
                context: Arc::clone(&self.context),
            }),
        ))
    }
    async fn mkdir(&self, _parent: i64, _name: String) -> Result<i64, MetaError> {
        Self::readonly().await
    }
    async fn rmdir(&self, _parent: i64, _name: &str) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn create_file(&self, _parent: i64, _name: String) -> Result<i64, MetaError> {
        Self::readonly().await
    }
    async fn create_file_with_attr(
        &self,
        _parent: i64,
        _name: String,
    ) -> Result<CreateEntryResult, MetaError> {
        Self::readonly().await
    }
    async fn create_node(
        &self,
        _parent: i64,
        _name: String,
        _kind: FileType,
        _mode: u32,
        _uid: u32,
        _gid: u32,
        _rdev: u32,
    ) -> Result<i64, MetaError> {
        Self::readonly().await
    }
    async fn link(&self, _ino: i64, _parent: i64, _name: &str) -> Result<FileAttr, MetaError> {
        Self::readonly().await
    }
    async fn symlink(
        &self,
        _parent: i64,
        _name: &str,
        _target: &str,
    ) -> Result<(i64, FileAttr), MetaError> {
        Self::readonly().await
    }
    async fn unlink(&self, _parent: i64, _name: &str) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn rename(
        &self,
        _old_parent: i64,
        _old_name: &str,
        _new_parent: i64,
        _new_name: String,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn rename_noreplace(
        &self,
        _old_parent: i64,
        _old_name: &str,
        _new_parent: i64,
        _new_name: String,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn rename_exchange(
        &self,
        _old_parent: i64,
        _old_name: &str,
        _new_parent: i64,
        _new_name: &str,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn set_file_size(&self, _ino: i64, _size: u64) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn extend_file_size(&self, _ino: i64, _size: u64) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn truncate(&self, _ino: i64, _size: u64, _chunk_size: u64) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn get_names(&self, ino: i64) -> Result<Vec<(Option<i64>, String)>, MetaError> {
        let inode = Self::inode(ino)?;
        let context = &self.context;
        let mut result = Vec::new();
        let mut after = None;
        let mut bytes = 0usize;
        loop {
            let page = context
                .snapshot
                .reverse_names_page(&context.reader, inode, after.as_deref(), 256)
                .await
                .map_err(map_error)?;
            if page.is_empty() {
                break;
            }
            for location in page {
                after = Some(location.reverse_key());
                bytes = bytes.saturating_add(location.hot.name.len() + 32);
                if result.len() >= 4096 || bytes > 256 * 1024 {
                    return Err(MetaError::Internal(
                        "use paged reverse index for large hardlink sets".into(),
                    ));
                }
                result.push((
                    Some(location.hot.parent_inode as i64),
                    String::from_utf8(location.hot.name).map_err(|_| MetaError::InvalidFilename)?,
                ));
            }
        }
        Ok(result)
    }
    async fn get_dentries(&self, ino: i64) -> Result<Vec<(i64, String)>, MetaError> {
        Ok(self
            .get_names(ino)
            .await?
            .into_iter()
            .filter_map(|(parent, name)| parent.map(|parent| (parent, name)))
            .collect())
    }
    async fn get_dir_parent(&self, dir_ino: i64) -> Result<Option<i64>, MetaError> {
        let inode = Self::inode(dir_ino)?;
        Ok(self
            .index_entry(inode)
            .await?
            .map(|entry| entry.parent_inode as i64))
    }
    async fn get_paths(&self, ino: i64) -> Result<Vec<String>, MetaError> {
        self.get_paths_bytes(ino)
            .await?
            .into_iter()
            .map(|path| String::from_utf8(path).map_err(|_| MetaError::InvalidFilename))
            .collect()
    }
    async fn get_paths_bytes(&self, ino: i64) -> Result<Vec<Vec<u8>>, MetaError> {
        Ok(self.get_paths_bytes_owned(ino).await?.paths)
    }
    async fn get_paths_bytes_owned(
        &self,
        ino: i64,
    ) -> Result<crate::meta::layer::OwnedPaths, MetaError> {
        let context = &self.context;
        // <=256 KiB path bytes, <=4096 Vec headers and the consumer's ancestor
        // component vector remain charged until the returned guard is dropped.
        let permit = context
            .budget
            .admit(&[(super::wire005::V3BudgetPool::Output, 1 << 20)])
            .map_err(map_error)?;
        let guard: crate::meta::layer::MetadataMemoryGuard = Arc::new(ReadonlyPathsOwner {
            _permit: permit,
            _context: self.context.clone(),
        });
        let inode = Self::inode(ino)?;
        let root_inode = context.snapshot.manifest().root_inode;
        if inode == root_inode {
            context.budget.admit(&[]).map_err(map_error)?;
            return Ok(crate::meta::layer::OwnedPaths {
                paths: vec![b"/".to_vec()],
                guard: Some(guard),
            });
        }
        let mut paths = Vec::new();
        let mut bytes = 0usize;
        let mut after = None;
        loop {
            let page = context
                .snapshot
                .reverse_names_page(&context.reader, inode, after.as_deref(), 256)
                .await
                .map_err(map_error)?;
            if page.is_empty() {
                break;
            }
            for location in page {
                after = Some(location.reverse_key());
                if paths.len() >= 4096 {
                    return Err(MetaError::Internal(
                        "use paged reverse index for large hardlink sets".into(),
                    ));
                }
                let mut components = vec![location.hot.name];
                let mut path_bytes = components[0].len() + 1;
                let mut current = location.hot.parent_inode;
                let mut depth = 0;
                while current != root_inode {
                    depth += 1;
                    if depth > 1024 {
                        return Err(MetaError::Internal(
                            "packed inode ancestor chain is cyclic/overlong".into(),
                        ));
                    }
                    let entry = self.index_entry(current).await?.ok_or_else(|| {
                        MetaError::Internal("packed ancestor inode is missing".into())
                    })?;
                    path_bytes = path_bytes.saturating_add(entry.name.len() + 1);
                    if bytes.saturating_add(path_bytes) > 256 * 1024 {
                        return Err(MetaError::Internal(
                            "packed reverse paths exceed bounded compatibility output".into(),
                        ));
                    }
                    components.push(entry.name);
                    current = entry.parent_inode;
                }
                bytes = bytes.saturating_add(path_bytes);
                if bytes > 256 * 1024 {
                    return Err(MetaError::Internal(
                        "packed reverse paths exceed bounded compatibility output".into(),
                    ));
                }
                let mut path = Vec::with_capacity(path_bytes);
                for component in components.into_iter().rev() {
                    path.push(b'/');
                    path.extend_from_slice(&component);
                }
                paths.push(path);
            }
        }
        context.budget.admit(&[]).map_err(map_error)?;
        Ok(crate::meta::layer::OwnedPaths {
            paths,
            guard: Some(guard),
        })
    }
    async fn read_symlink(&self, ino: i64) -> Result<String, MetaError> {
        String::from_utf8(self.read_symlink_bytes(ino).await?)
            .map_err(|_| MetaError::InvalidFilename)
    }
    async fn read_symlink_bytes(&self, ino: i64) -> Result<Vec<u8>, MetaError> {
        let context = &self.context;
        let inode = Self::inode(ino)?;
        let location = context
            .snapshot
            .lookup_inode(&context.reader, inode)
            .await
            .map_err(map_error)?
            .ok_or(MetaError::NotFound(ino))?;
        if location.hot.kind != 3 {
            return Err(MetaError::NotSupported(
                "readlink requires a symlink".into(),
            ));
        }
        let attrs = context
            .snapshot
            .cold_attributes_owned(&context.client, &context.reader, inode)
            .await
            .map_err(map_error)?
            .ok_or_else(|| {
                MetaError::Internal("symlink has no authenticated cold attributes".into())
            })?;
        let target = attrs
            .symlink_target
            .as_ref()
            .ok_or_else(|| MetaError::Internal("symlink cold object has no target".into()))?;
        if target.len() as u64 != location.hot.size {
            return Err(MetaError::Internal(
                "symlink cold target length disagrees with inode".into(),
            ));
        }
        Ok(target.clone())
    }
    async fn set_attr(
        &self,
        _ino: i64,
        _req: &SetAttrRequest,
        _flags: SetAttrFlags,
    ) -> Result<FileAttr, MetaError> {
        Self::readonly().await
    }
    async fn open(&self, ino: i64, flags: OpenFlags) -> Result<FileAttr, MetaError> {
        if flags.intersects(
            OpenFlags::WRONLY
                | OpenFlags::RDWR
                | OpenFlags::APPEND
                | OpenFlags::TRUNC
                | OpenFlags::CREATE,
        ) {
            return Self::readonly().await;
        }
        self.stat_fresh(ino).await?.ok_or(MetaError::NotFound(ino))
    }
    async fn close(&self, _ino: i64) -> Result<(), MetaError> {
        Ok(())
    }
    async fn record_open(
        &self,
        _ino: i64,
        _attr: FileAttr,
        _read: bool,
        write: bool,
        append: bool,
    ) -> Result<(), MetaError> {
        // VFS fresh/cached opens bypass MetaLayer::open. Reject them before
        // allocating a writable handle or activating writeback state.
        if write || append {
            return Self::readonly().await;
        }
        Ok(())
    }
    async fn write(
        &self,
        _ino: i64,
        _chunk_id: u64,
        _slice: SliceDesc,
        _new_size: u64,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn get_deleted_files(&self) -> Result<Vec<i64>, MetaError> {
        Ok(Vec::new())
    }
    async fn remove_file_metadata(&self, _ino: i64) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn get_slices(&self, chunk_id: u64) -> Result<Vec<SliceDesc>, MetaError> {
        let (ino, chunk_index) = extract_ino_and_chunk_index(chunk_id);
        if ino <= 0
            || chunk_id_for(ino, chunk_index)
                .map_err(|error| MetaError::Internal(error.to_string()))?
                != chunk_id
        {
            return Err(MetaError::Internal("invalid packed v3 chunk id".into()));
        }
        let Some(entry) = self.entry(ino as u64).await? else {
            return Err(MetaError::NotFound(ino));
        };
        let context = &self.context;
        if matches!(
            context
                .snapshot
                .placement(&context.reader, ino as u64, entry.size)
                .await
                .map_err(map_error)?,
            Some(super::wire005::V3Placement::External { .. })
        ) {
            return Err(MetaError::Internal(
                "external packed data requires the unified read provider".into(),
            ));
        }
        self.chunk_slices(chunk_id, &entry, chunk_index)
    }
    async fn append_slice(&self, _chunk_id: u64, _slice: SliceDesc) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn next_id(&self, _key: &str) -> Result<i64, MetaError> {
        Self::readonly().await
    }
    async fn start_session(&self, _session_info: SessionInfo) -> Result<(), MetaError> {
        Ok(())
    }
    async fn shutdown_session(&self) -> Result<(), MetaError> {
        self.drain_packed_transport().await?;
        self.context.budget.close();
        Ok(())
    }
    async fn get_plock(
        &self,
        _inode: i64,
        _query: &FileLockQuery,
    ) -> Result<FileLockInfo, MetaError> {
        Self::readonly().await
    }
    async fn set_plock(
        &self,
        _inode: i64,
        _owner: i64,
        _block: bool,
        _lock_type: FileLockType,
        _range: FileLockRange,
        _pid: u32,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn get_flock(&self, _inode: i64, _owner: i64) -> Result<FileLockType, MetaError> {
        Self::readonly().await
    }
    async fn set_flock(
        &self,
        _inode: i64,
        _owner: i64,
        _block: bool,
        _lock_type: FileLockType,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn set_xattr(
        &self,
        _inode: i64,
        _name: &str,
        _value: &[u8],
        _flags: u32,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn get_xattr(&self, inode: i64, name: &str) -> Result<Option<Vec<u8>>, MetaError> {
        self.get_xattr_bytes(inode, name.as_bytes()).await
    }
    async fn get_xattr_bytes(&self, inode: i64, name: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        let context = &self.context;
        let Some(attrs) = context
            .snapshot
            .cold_attributes_owned(&context.client, &context.reader, Self::inode(inode)?)
            .await
            .map_err(map_error)?
        else {
            return Ok(None);
        };
        Ok(attrs
            .xattrs
            .iter()
            .find(|attr| attr.name == name)
            .map(|attr| attr.value.clone()))
    }
    async fn list_xattr(&self, inode: i64) -> Result<Vec<String>, MetaError> {
        self.list_xattr_bytes(inode)
            .await?
            .into_iter()
            .map(|name| String::from_utf8(name).map_err(|_| MetaError::InvalidFilename))
            .collect()
    }
    async fn list_xattr_bytes(&self, inode: i64) -> Result<Vec<Vec<u8>>, MetaError> {
        Ok(self.list_xattr_bytes_owned(inode).await?.names)
    }
    async fn list_xattr_bytes_owned(
        &self,
        inode: i64,
    ) -> Result<crate::meta::layer::OwnedXattrNames, MetaError> {
        let context = &self.context;
        let permit = context
            .budget
            .admit(&[(super::wire005::V3BudgetPool::Output, 2 << 20)])
            .map_err(map_error)?;
        let Some(attrs) = context
            .snapshot
            .cold_attributes_owned(&context.client, &context.reader, Self::inode(inode)?)
            .await
            .map_err(map_error)?
        else {
            return Ok(crate::meta::layer::OwnedXattrNames {
                names: Vec::new(),
                guard: Some(Arc::new(permit)),
            });
        };
        if attrs
            .xattrs
            .iter()
            .map(|attr| attr.name.len() + 1)
            .sum::<usize>()
            > 65536
        {
            return Err(MetaError::Io(std::io::Error::from_raw_os_error(
                libc::E2BIG,
            )));
        }
        Ok(crate::meta::layer::OwnedXattrNames {
            names: attrs.xattrs.iter().map(|attr| attr.name.clone()).collect(),
            guard: Some(Arc::new(permit)),
        })
    }
    async fn remove_xattr(&self, _inode: i64, _name: &str) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn set_xattr_bytes(
        &self,
        _inode: i64,
        _name: &[u8],
        _value: &[u8],
        _flags: u32,
    ) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn remove_xattr_bytes(&self, _inode: i64, _name: &[u8]) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn set_acl(&self, _inode: i64, _rule: AclRule) -> Result<(), MetaError> {
        Self::readonly().await
    }
    async fn get_acl(
        &self,
        inode: i64,
        acl_type: u8,
        acl_id: u32,
    ) -> Result<Option<AclRule>, MetaError> {
        let context = &self.context;
        let Some(attrs) = context
            .snapshot
            .cold_attributes_owned(&context.client, &context.reader, Self::inode(inode)?)
            .await
            .map_err(map_error)?
        else {
            return Ok(None);
        };
        Ok(attrs
            .acl
            .iter()
            .find(|rule| rule.acl_type == acl_type && rule.qualifier == acl_id)
            .cloned())
    }
}

impl<B: ObjectBackend + Clone + 'static> PackedV3ReadonlyMeta<B> {
    async fn readdir_page_raw(
        &self,
        ino: i64,
        child_offset: usize,
        limit: usize,
    ) -> Result<Vec<GroupMetaEntry>, MetaError> {
        let parent = Self::inode(ino)?;
        let context = &self.context;
        context
            .snapshot
            .readdir_page(
                &context.client,
                &context.reader,
                self.parent_key(parent),
                child_offset as u64,
                limit,
                512 * 1024,
            )
            .await
            .map_err(map_error)
    }
}

#[async_trait]
impl<B: ObjectBackend + Clone + 'static> crate::chunk::read_plan::WorkspaceReadPlanProvider
    for PackedV3ReadonlyMeta<B>
{
    fn max_read_bytes(&self) -> Option<usize> {
        Some(self.context.budget.max_read_bytes())
    }
    fn reserve_read_output(
        &self,
        length: usize,
    ) -> Result<Option<Box<dyn Send + Sync>>, MetaError> {
        self.context
            .budget
            .output(length)
            .map(|permit| Some(Box::new(permit) as Box<dyn Send + Sync>))
            .map_err(map_error)
    }
    async fn read_plan(
        &self,
        _ino: i64,
        _chunk_index: u64,
        _offset: u64,
        _len: u64,
    ) -> Result<crate::chunk::read_plan::ResolvedReadPlan, MetaError> {
        Err(MetaError::NotSupported(
            "packed readonly uses prepared unified plans, not synthetic slices".into(),
        ))
    }
    fn supports_prepared_unified_read(&self) -> bool {
        true
    }
    async fn prepare_unified_read(
        &self,
        ino: i64,
        chunk_index: u64,
        offset: u64,
        len: u64,
    ) -> Result<Option<crate::chunk::read_plan::PreparedUnifiedRead>, MetaError> {
        self.prepare_unified_read_observed(ino, chunk_index, offset, len, None)
            .await
    }

    async fn prepare_unified_read_observed(
        &self,
        ino: i64,
        chunk_index: u64,
        offset: u64,
        len: u64,
        delivery: Option<Arc<crate::cadapter::read_observer::OperationDelivery>>,
    ) -> Result<Option<crate::chunk::read_plan::PreparedUnifiedRead>, MetaError> {
        let context = &self.context;
        let base = chunk_index
            .checked_mul(self.chunk_size)
            .ok_or_else(|| MetaError::Internal("packed chunk base overflows".into()))?;
        let absolute = base
            .checked_add(offset)
            .ok_or_else(|| MetaError::Internal("packed read offset overflows".into()))?;
        let length = usize::try_from(len)
            .map_err(|_| MetaError::Internal("packed read length exceeds usize".into()))?;
        let mut prepared = context
            .snapshot
            .prepare_inode_read_observed(
                &context.client,
                &context.reader,
                Self::inode(ino)?,
                super::wire005::V3ReadRange {
                    offset: absolute,
                    length,
                },
                32 * 1024 * 1024,
                delivery,
            )
            .await
            .map_err(map_error)?;
        for segment in &mut prepared.plan.segments {
            segment.logical_offset = segment
                .logical_offset
                .checked_sub(base)
                .ok_or_else(|| MetaError::Internal("packed plan precedes chunk base".into()))?;
        }
        prepared.plan.logical_size = prepared
            .plan
            .logical_size
            .saturating_sub(base)
            .min(self.chunk_size);
        prepared
            .plan
            .validate(offset, len)
            .map_err(|error| MetaError::Internal(error.to_string()))?;
        Ok(Some(prepared))
    }
    fn record_unified_read_success(&self, bytes: u64) {
        self.context
            .metrics
            .logical_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }
    fn begin_unified_read_operation(
        &self,
        requested: u64,
    ) -> Option<crate::cadapter::read_observer::TerminalGuard> {
        let context = &self.context;
        let observer = context.client.read_observer()?;
        Some(
            observer.start(
                crate::cadapter::read_observer::Ledger::LogicalOperation,
                context
                    .client
                    .read_context(crate::cadapter::read_observer::ReadClass::LogicalRead)?,
                requested,
            ),
        )
    }
    async fn range_has_data(&self, ino: i64, offset: u64, len: u64) -> Result<bool, MetaError> {
        let Some(entry) = self.entry(Self::inode(ino)?).await? else {
            return Err(MetaError::NotFound(ino));
        };
        let end = offset
            .checked_add(len)
            .ok_or_else(|| MetaError::Internal("packed range end overflows".into()))?;
        if !entry.inline_data.is_empty() {
            return Ok(len > 0 && offset < entry.size);
        }
        Ok(entry.extents.iter().any(|extent| {
            extent.file_offset < end && extent.file_offset + u64::from(extent.logical_len) > offset
        }))
    }
}
