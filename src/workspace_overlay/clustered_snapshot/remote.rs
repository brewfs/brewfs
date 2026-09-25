//! Async, range-oriented reader for one immutable `.brfc` cluster object.
//!
//! Opening a cluster transfers only its fixed superblock.  Merkle index pages
//! and namespace batches are fetched on demand, authenticated independently,
//! and retained in bounded caches.  A per-key async lock prevents concurrent
//! misses from issuing duplicate object-store requests without serializing
//! unrelated lookups.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use dashmap::DashMap;
use tokio::sync::Mutex;

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::native_base::wire::error::{WireError, WireResult};

use super::attribute::{AttributeBatch, AttributeGroup, attribute_index_key};
use super::batch::{BATCH_HEADER_LEN, BatchKind, EncodedBatch, NamespaceBatch, NamespaceEntry};
use super::cluster_format::{
    BatchLocator, ClusterSuperblock, IndexRootRef, SUPERBLOCK_LEN, namespace_index_key,
};
use super::extent::ExtentSpan;
use super::merkle_index::{ChildRef, DEFAULT_INDEX_NODE_SIZE, IndexEntry, IndexNode};
use super::range_reader::{MAX_RANGE_BYTES, MemoryRangeReader, read_batch};
use super::snapshot_manifest::ClusterDescriptor;

/// Default number of decoded index pages retained per cluster handle.
pub const DEFAULT_INDEX_CACHE_ENTRIES: usize = 1024;
/// Default number of decoded metadata batches retained per cluster handle.
pub const DEFAULT_BATCH_CACHE_ENTRIES: usize = 512;
/// Default decoded index-cache byte budget (including key/vector overhead).
pub const DEFAULT_INDEX_CACHE_BYTES: usize = 64 * 1024 * 1024;
/// Default decoded metadata-batch byte budget (stored plus raw payload).
pub const DEFAULT_BATCH_CACHE_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteClusterOptions {
    pub max_index_cache_entries: usize,
    pub max_batch_cache_entries: usize,
    pub max_index_cache_bytes: usize,
    pub max_batch_cache_bytes: usize,
}

impl Default for RemoteClusterOptions {
    fn default() -> Self {
        Self {
            max_index_cache_entries: DEFAULT_INDEX_CACHE_ENTRIES,
            max_batch_cache_entries: DEFAULT_BATCH_CACHE_ENTRIES,
            max_index_cache_bytes: DEFAULT_INDEX_CACHE_BYTES,
            max_batch_cache_bytes: DEFAULT_BATCH_CACHE_BYTES,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct IndexCacheKey {
    object_offset: u64,
    stored_len: u32,
    digest: [u8; 32],
}

impl IndexCacheKey {
    fn root(root: &IndexRootRef) -> Self {
        Self {
            object_offset: root.object_offset,
            stored_len: root.stored_len,
            digest: root.digest,
        }
    }

    fn child(child: &ChildRef) -> Self {
        Self {
            object_offset: child.object_offset,
            stored_len: child.stored_len,
            digest: child.digest,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BatchCacheKey {
    object_offset: u64,
    stored_len: u32,
    digest: [u8; 32],
}

impl BatchCacheKey {
    fn from_locator(locator: &BatchLocator) -> Self {
        Self {
            object_offset: locator.object_offset,
            stored_len: locator.total_stored_len,
            digest: locator.digest,
        }
    }
}

/// A mounted immutable cluster whose variable metadata is remote.
pub struct RemoteCluster<B: ObjectBackend + Clone> {
    client: ObjectClient<B>,
    object_key: String,
    descriptor: ClusterDescriptor,
    superblock: ClusterSuperblock,
    options: RemoteClusterOptions,
    index_cache: DashMap<IndexCacheKey, Arc<IndexNode>>,
    batch_cache: DashMap<BatchCacheKey, Arc<EncodedBatch>>,
    index_access: DashMap<IndexCacheKey, u64>,
    batch_access: DashMap<BatchCacheKey, u64>,
    index_cache_bytes: AtomicUsize,
    batch_cache_bytes: AtomicUsize,
    cache_clock: AtomicU64,
    cache_admission: StdMutex<()>,
    index_inflight: DashMap<IndexCacheKey, Arc<Mutex<()>>>,
    batch_inflight: DashMap<BatchCacheKey, Arc<Mutex<()>>>,
}

impl<B: ObjectBackend + Clone> RemoteCluster<B> {
    /// Open a cluster with the default bounded cache sizes.
    pub async fn open(client: &ObjectClient<B>, descriptor: ClusterDescriptor) -> WireResult<Self> {
        Self::open_with_options(client, descriptor, RemoteClusterOptions::default()).await
    }

    /// Open a cluster and fetch exactly its fixed 4 KiB superblock.
    pub async fn open_with_options(
        client: &ObjectClient<B>,
        descriptor: ClusterDescriptor,
        options: RemoteClusterOptions,
    ) -> WireResult<Self> {
        let object_key = std::str::from_utf8(&descriptor.metadata_ref.key)
            .map_err(|_| WireError::invalid("cluster object", "object key is not UTF-8"))?
            .to_owned();
        if descriptor.metadata_ref.object_len < SUPERBLOCK_LEN as u64 {
            return Err(WireError::Truncated {
                what: "cluster object",
                need: SUPERBLOCK_LEN,
                have: descriptor.metadata_ref.object_len as usize,
            });
        }

        let cluster = Self {
            client: client.clone(),
            object_key,
            descriptor,
            // Filled immediately below; keeping construction in one place
            // makes all later methods share the same object-length boundary.
            superblock: ClusterSuperblock {
                format_major: 0,
                format_minor: 0,
                cluster_id: [0; 16],
                volume_id: [0; 16],
                mount_dir_key: super::identity::DirKey::new([0; 16]),
                metadata_semantic_hash: [0; 32],
                build_options_digest: [0; 32],
                node_count: 0,
                directory_contribution_count: 0,
                dentry_count: 0,
                extent_count: 0,
                slice_count: 0,
                namespace_batch_count: 0,
                extent_batch_count: 0,
                attribute_batch_count: 0,
                chunk_size: 0,
                index_roots: [empty_root(), empty_root(), empty_root(), empty_root()],
            },
            options,
            index_cache: DashMap::new(),
            batch_cache: DashMap::new(),
            index_access: DashMap::new(),
            batch_access: DashMap::new(),
            index_cache_bytes: AtomicUsize::new(0),
            batch_cache_bytes: AtomicUsize::new(0),
            cache_clock: AtomicU64::new(0),
            cache_admission: StdMutex::new(()),
            index_inflight: DashMap::new(),
            batch_inflight: DashMap::new(),
        };
        let bytes = cluster.fetch_range(0, SUPERBLOCK_LEN as u32).await?;
        let superblock = ClusterSuperblock::decode(&bytes)?;
        if superblock.cluster_id != cluster.descriptor.cluster_id {
            return Err(WireError::invalid(
                "cluster superblock",
                "cluster id does not match manifest descriptor",
            ));
        }
        if superblock.namespace_batch_count == 0 || superblock.dentry_count == 0 {
            return Err(WireError::invalid(
                "cluster superblock",
                "namespace index must contain at least one batch and dentry",
            ));
        }
        validate_index_root(
            &superblock.index_roots[0],
            BatchKind::Namespace,
            true,
            superblock.namespace_batch_count,
            cluster.descriptor.metadata_ref.object_len,
        )?;
        validate_index_root(
            &superblock.index_roots[1],
            BatchKind::Extent,
            superblock.extent_batch_count != 0,
            superblock.extent_batch_count,
            cluster.descriptor.metadata_ref.object_len,
        )?;
        validate_index_root(
            &superblock.index_roots[2],
            BatchKind::Attribute,
            superblock.attribute_batch_count != 0,
            superblock.attribute_batch_count,
            cluster.descriptor.metadata_ref.object_len,
        )?;
        Ok(Self {
            superblock,
            ..cluster
        })
    }

    pub fn descriptor(&self) -> &ClusterDescriptor {
        &self.descriptor
    }

    pub fn superblock(&self) -> &ClusterSuperblock {
        &self.superblock
    }

    pub fn metadata_cache_entries(&self) -> (usize, usize) {
        (self.index_cache.len(), self.batch_cache.len())
    }

    /// Decoded cache residency, separated by index and batch class.
    pub fn metadata_cache_bytes(&self) -> (usize, usize) {
        (
            self.index_cache_bytes.load(Ordering::Acquire),
            self.batch_cache_bytes.load(Ordering::Acquire),
        )
    }

    /// Drop all decoded pages and batches.  The immutable object remains
    /// mounted and the next lookup repopulates only the ranges it needs.
    pub fn clear_cache(&self) {
        let _guard = self.cache_admission.lock().expect("cache admission lock");
        self.index_cache.clear();
        self.batch_cache.clear();
        self.index_access.clear();
        self.batch_access.clear();
        self.index_cache_bytes.store(0, Ordering::Release);
        self.batch_cache_bytes.store(0, Ordering::Release);
    }

    /// Evict one decoded index page by its object offset.
    pub fn evict_index(&self, object_offset: u64) -> bool {
        let _guard = self.cache_admission.lock().expect("cache admission lock");
        let key = self
            .index_cache
            .iter()
            .find(|entry| entry.key().object_offset == object_offset)
            .map(|entry| entry.key().clone());
        key.and_then(|key| self.remove_index_cache(&key)).is_some()
    }

    /// Evict all cached representations of a batch id.
    pub fn evict_batch(&self, batch_id: u32) -> bool {
        let _guard = self.cache_admission.lock().expect("cache admission lock");
        let keys = self
            .batch_cache
            .iter()
            .filter_map(|entry| {
                let encoded = entry.value();
                (encoded.header.batch_id == batch_id).then(|| entry.key().clone())
            })
            .collect::<Vec<_>>();
        let mut removed = false;
        for key in keys {
            removed |= self.remove_batch_cache(&key).is_some();
        }
        removed
    }

    /// Fetch, authenticate, and decode an index root on demand.
    pub async fn read_index_root(&self, kind: BatchKind) -> WireResult<Arc<IndexNode>> {
        let root = self.root_for_kind(kind)?;
        let key = IndexCacheKey::root(root);
        let node = self
            .load_index(
                key,
                root.object_offset,
                root.stored_len,
                root.digest,
                root.level,
                kind,
                (root.level == 0).then_some(root.entry_count),
            )
            .await?;
        if root.level > 0 {
            let subtree_count = node
                .entries
                .iter()
                .map(|entry| match entry {
                    IndexEntry::Internal(child) => child.visible_subtree_count,
                    IndexEntry::Leaf { .. } => None,
                })
                .try_fold(0u64, |sum, count| {
                    count.and_then(|count| sum.checked_add(count))
                })
                .ok_or_else(|| {
                    WireError::invalid("cluster index root", "internal root is missing rank counts")
                })?;
            if subtree_count != u64::from(root.entry_count) {
                return Err(WireError::invalid(
                    "cluster index root",
                    "internal root subtree count does not match reference",
                ));
            }
        }
        Ok(node)
    }

    /// Fetch, authenticate, and decode one internal index child on demand.
    pub async fn read_index_child(
        &self,
        child: &ChildRef,
        expected_kind: BatchKind,
        expected_level: u8,
    ) -> WireResult<Arc<IndexNode>> {
        let key = IndexCacheKey::child(child);
        self.load_index(
            key,
            child.object_offset,
            child.stored_len,
            child.digest,
            expected_level,
            expected_kind,
            Some(child.entry_count),
        )
        .await
    }

    /// Fetch and validate one independently decodable metadata batch of any
    /// supported kind.  Namespace, extent, and attribute payload codecs are
    /// layered on top of this authenticated transport/cache unit.
    pub async fn read_metadata_batch(
        &self,
        locator: &BatchLocator,
    ) -> WireResult<Arc<EncodedBatch>> {
        if locator.total_stored_len < BATCH_HEADER_LEN as u32 {
            return Err(WireError::Truncated {
                what: "metadata batch",
                need: BATCH_HEADER_LEN,
                have: locator.total_stored_len as usize,
            });
        }
        let key = BatchCacheKey::from_locator(locator);
        if let Some(cached) = self.batch_cache.get(&key) {
            self.touch_batch(&key);
            return Ok(cached.clone());
        }
        let lock = self
            .batch_inflight
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        if let Some(cached) = self.batch_cache.get(&key) {
            self.touch_batch(&key);
            return Ok(cached.clone());
        }
        validate_range_bounds(
            locator.object_offset,
            locator.total_stored_len,
            self.descriptor.metadata_ref.object_len,
        )?;
        if locator.total_stored_len as usize > MAX_RANGE_BYTES {
            return Err(WireError::LimitExceeded(format!(
                "metadata batch range exceeds {MAX_RANGE_BYTES} bytes"
            )));
        }
        let bytes = self
            .fetch_range(locator.object_offset, locator.total_stored_len)
            .await?;
        // `bytes` is the exact fetched slice, while the locator keeps the
        // absolute object offset.  Normalize the offset only for the
        // transport-neutral verifier; all other locator fields remain
        // authenticated against the batch header.
        let mut relative_locator = locator.clone();
        relative_locator.object_offset = 0;
        let reader = MemoryRangeReader::new(bytes);
        let decoded = read_batch(&reader, &relative_locator)?;
        if decoded.header.cluster_id != self.superblock.cluster_id {
            return Err(WireError::invalid(
                "metadata batch",
                "cluster id does not match superblock",
            ));
        }
        let decoded = Arc::new(decoded);
        self.insert_batch_cache(key, decoded.clone());
        Ok(decoded)
    }

    /// Fetch and validate one namespace batch.
    pub async fn read_namespace_batch(
        &self,
        locator: &BatchLocator,
    ) -> WireResult<Arc<EncodedBatch>> {
        if locator.kind != BatchKind::Namespace {
            return Err(WireError::invalid(
                "namespace batch",
                "locator kind is not namespace",
            ));
        }
        self.read_metadata_batch(locator).await
    }

    /// Fetch and validate one extent batch.  The returned bytes remain
    /// independently decodable through the extent payload codec.
    pub async fn read_extent_batch(&self, locator: &BatchLocator) -> WireResult<Arc<EncodedBatch>> {
        if locator.kind != BatchKind::Extent {
            return Err(WireError::invalid(
                "extent batch",
                "locator kind is not extent",
            ));
        }
        self.read_metadata_batch(locator).await
    }

    /// Resolve a bounded logical file range through the extent index.
    ///
    /// Only index pages and extent batches whose authenticated key ranges
    /// overlap the requested `(LocalNodeId, file offset)` interval are read.
    /// Holes are omitted and returned spans are clipped to `[file_offset,
    /// file_offset + length)`.
    pub async fn read_extent_range(
        &self,
        local_node_id: u32,
        file_offset: u64,
        length: u64,
    ) -> WireResult<Vec<ExtentSpan>> {
        if local_node_id == 0 {
            return Err(WireError::invalid(
                "extent range",
                "local node id must be non-zero",
            ));
        }
        if length == 0 {
            return Ok(Vec::new());
        }
        let end = file_offset
            .checked_add(length)
            .ok_or_else(|| WireError::LimitExceeded("extent read range overflows u64".into()))?;
        if self.superblock.index_roots[1].stored_len == 0 {
            return Ok(Vec::new());
        }

        let lower = super::extent::extent_index_key(local_node_id, file_offset);
        // Extents beginning at `end` do not intersect the half-open request;
        // using end - 1 avoids fetching their batch when it is independently
        // indexed.
        let upper = super::extent::extent_index_key(local_node_id, end - 1);
        let locators = self
            .find_locators_in_range(BatchKind::Extent, &lower, &upper)
            .await?;
        let mut spans = Vec::new();
        for locator in locators {
            let batch = self.read_extent_batch(&locator).await?;
            let extent = super::extent::ExtentBatch::decode(&batch)?;
            spans.extend(extent.spans_in_range(local_node_id, file_offset, end)?);
        }
        spans.sort_by_key(|span| span.file_offset);
        let mut previous_end = None;
        for span in &spans {
            let span_end = span
                .file_offset
                .checked_add(span.logical_length)
                .ok_or_else(|| WireError::LimitExceeded("extent span overflows u64".into()))?;
            if previous_end.is_some_and(|end| span.file_offset < end) {
                return Err(WireError::invalid(
                    "extent range",
                    "overlapping extent batches are corrupt",
                ));
            }
            previous_end = Some(span_end);
        }
        Ok(spans)
    }

    /// Fetch and validate one cold-attribute batch.
    pub async fn read_attribute_batch(
        &self,
        locator: &BatchLocator,
    ) -> WireResult<Arc<EncodedBatch>> {
        if locator.kind != BatchKind::Attribute {
            return Err(WireError::invalid(
                "attribute batch",
                "locator kind is not attribute",
            ));
        }
        self.read_metadata_batch(locator).await
    }

    /// Resolve one cold-attribute group by LocalNodeId without consulting
    /// namespace or extent batches.
    pub async fn lookup_attribute(&self, local_node_id: u32) -> WireResult<Option<AttributeGroup>> {
        if local_node_id == 0 {
            return Err(WireError::invalid(
                "attribute lookup",
                "local node id must be non-zero",
            ));
        }
        if self.superblock.index_roots[2].stored_len == 0 {
            return Ok(None);
        }
        let key = attribute_index_key(local_node_id);
        let root = self.read_index_root(BatchKind::Attribute).await?;
        let Some(locator) = self.find_locator(root, &key).await? else {
            return Ok(None);
        };
        let batch = self.read_attribute_batch(&locator).await?;
        let attributes = AttributeBatch::decode(&batch)?;
        let mut found = None;
        for group in attributes.groups {
            if group.local_node_id != local_node_id {
                continue;
            }
            if found.is_some() {
                return Err(WireError::invalid(
                    "attribute lookup",
                    "local node id appears in multiple groups",
                ));
            }
            found = Some(group);
        }
        Ok(found)
    }

    /// Resolve one `(parent local node id, raw name)` lookup without loading
    /// any unrelated batches.
    pub async fn lookup_namespace(
        &self,
        parent_local_node_id: u32,
        name: &[u8],
    ) -> WireResult<Option<NamespaceEntry>> {
        let key = namespace_index_key(parent_local_node_id, name);
        let root = self.read_index_root(BatchKind::Namespace).await?;
        let Some(locator) = self.find_locator(root, &key).await? else {
            return Ok(None);
        };
        let batch = self.read_namespace_batch(&locator).await?;
        let namespace = NamespaceBatch::decode(&batch)?;
        for segment in namespace.segments {
            if segment.parent_local_node_id != parent_local_node_id {
                continue;
            }
            if let Ok(index) = segment
                .entries
                .binary_search_by(|entry| namespace_entry_name(entry).as_bytes().cmp(name))
            {
                return Ok(Some(segment.entries[index].clone()));
            }
        }
        Ok(None)
    }

    /// Read one bounded physical source range for a merged-directory window.
    ///
    /// The route carries the first and last raw names owned by a source. The
    /// reader descends only index children whose authenticated key ranges
    /// overlap that interval, then fetches the matching batches and filters
    /// the parent segment. It never materializes a complete directory.
    pub async fn read_namespace_range(
        &self,
        parent_local_node_id: u32,
        first_name: &[u8],
        last_name: &[u8],
    ) -> WireResult<Vec<NamespaceEntry>> {
        if first_name.is_empty() || last_name.is_empty() || first_name > last_name {
            return Err(WireError::invalid(
                "namespace range",
                "raw-name bounds are invalid",
            ));
        }
        let lower = namespace_index_key(parent_local_node_id, first_name);
        let upper = namespace_index_key(parent_local_node_id, last_name);
        let root = self.read_index_root(BatchKind::Namespace).await?;
        let mut pending = vec![root];
        let mut located = Vec::<(Vec<u8>, BatchLocator)>::new();

        while let Some(node) = pending.pop() {
            if node.level == 0 {
                for entry in &node.entries {
                    let IndexEntry::Leaf {
                        first_key,
                        last_key,
                        locator,
                    } = entry
                    else {
                        return Err(WireError::invalid(
                            "namespace index",
                            "leaf node contains an internal entry",
                        ));
                    };
                    if last_key.as_slice() >= lower.as_slice()
                        && first_key.as_slice() <= upper.as_slice()
                    {
                        located.push((first_key.clone(), locator.clone()));
                    }
                }
                continue;
            }

            let mut children = Vec::new();
            for entry in &node.entries {
                let IndexEntry::Internal(child) = entry else {
                    return Err(WireError::invalid(
                        "namespace index",
                        "internal node contains a leaf entry",
                    ));
                };
                if child.last_key.as_slice() < lower.as_slice()
                    || child.first_key.as_slice() > upper.as_slice()
                {
                    continue;
                }
                children.push(
                    self.read_index_child(child, BatchKind::Namespace, node.level - 1)
                        .await?,
                );
            }
            // The stack is LIFO; reverse keeps range traversal deterministic.
            pending.extend(children.into_iter().rev());
        }

        located.sort_by(|left, right| left.0.cmp(&right.0));
        let mut seen = HashSet::new();
        let mut entries = Vec::new();
        for (_, locator) in located {
            let key = (
                locator.object_offset,
                locator.total_stored_len,
                locator.digest,
            );
            if !seen.insert(key) {
                continue;
            }
            let batch = self.read_namespace_batch(&locator).await?;
            let namespace = NamespaceBatch::decode(&batch)?;
            for segment in namespace.segments {
                if segment.parent_local_node_id != parent_local_node_id {
                    continue;
                }
                for entry in segment.entries {
                    let name = namespace_entry_name(&entry).as_bytes();
                    if name >= first_name && name <= last_name {
                        entries.push(entry);
                        if entries.len() > super::directory::MAX_WINDOW_ENTRIES {
                            return Err(WireError::LimitExceeded(
                                "namespace range exceeds the v2 window bound".into(),
                            ));
                        }
                    }
                }
            }
        }

        for pair in entries.windows(2) {
            if namespace_entry_name(&pair[0]) >= namespace_entry_name(&pair[1]) {
                return Err(WireError::invalid(
                    "namespace range",
                    "source entries are not strictly ordered",
                ));
            }
        }
        Ok(entries)
    }

    async fn find_locator(
        &self,
        mut node: Arc<IndexNode>,
        key: &[u8],
    ) -> WireResult<Option<BatchLocator>> {
        // Use an iterative descent so the future has a fixed size regardless
        // of how many internal index levels a very large directory needs.
        loop {
            if node.level == 0 {
                for entry in &node.entries {
                    let IndexEntry::Leaf {
                        first_key,
                        last_key,
                        locator,
                    } = entry
                    else {
                        return Err(WireError::invalid(
                            "namespace index",
                            "leaf node contains internal entry",
                        ));
                    };
                    if key >= first_key.as_slice() && key <= last_key.as_slice() {
                        return Ok(Some(locator.clone()));
                    }
                }
                return Ok(None);
            }

            let mut selected = None;
            for entry in &node.entries {
                let IndexEntry::Internal(child) = entry else {
                    return Err(WireError::invalid(
                        "namespace index",
                        "internal node contains leaf entry",
                    ));
                };
                if key < child.first_key.as_slice() {
                    break;
                }
                if key <= child.last_key.as_slice() {
                    selected = Some(child.clone());
                    break;
                }
            }
            let Some(child) = selected else {
                return Ok(None);
            };
            node = self
                .read_index_child(&child, node.kind, node.level - 1)
                .await?;
        }
    }

    /// Return every leaf locator whose authenticated key range intersects a
    /// half-open extent/name range.  The traversal is iterative so a deeply
    /// nested index cannot create an oversized async future.
    async fn find_locators_in_range(
        &self,
        kind: BatchKind,
        lower: &[u8],
        upper: &[u8],
    ) -> WireResult<Vec<BatchLocator>> {
        if lower > upper {
            return Err(WireError::invalid(
                "metadata range",
                "range bounds are not ordered",
            ));
        }
        let root = self.read_index_root(kind).await?;
        let mut pending = vec![root];
        let mut locators = Vec::new();
        while let Some(node) = pending.pop() {
            if node.level == 0 {
                for entry in &node.entries {
                    let IndexEntry::Leaf {
                        first_key,
                        last_key,
                        locator,
                    } = entry
                    else {
                        return Err(WireError::invalid(
                            "metadata index",
                            "leaf node contains an internal entry",
                        ));
                    };
                    if last_key.as_slice() >= lower && first_key.as_slice() <= upper {
                        if locator.kind != kind {
                            return Err(WireError::invalid(
                                "metadata index",
                                "leaf locator kind does not match index",
                            ));
                        }
                        locators.push(locator.clone());
                    }
                }
                continue;
            }

            let mut children = Vec::new();
            for entry in &node.entries {
                let IndexEntry::Internal(child) = entry else {
                    return Err(WireError::invalid(
                        "metadata index",
                        "internal node contains a leaf entry",
                    ));
                };
                if child.last_key.as_slice() < lower || child.first_key.as_slice() > upper {
                    continue;
                }
                children.push(self.read_index_child(child, kind, node.level - 1).await?);
            }
            pending.extend(children.into_iter().rev());
        }

        locators.sort_by_key(|locator| {
            (
                locator.object_offset,
                locator.batch_id,
                locator.stream_ordinal,
            )
        });
        locators.dedup_by(|left, right| {
            left.object_offset == right.object_offset
                && left.total_stored_len == right.total_stored_len
                && left.digest == right.digest
        });
        Ok(locators)
    }

    async fn load_index(
        &self,
        key: IndexCacheKey,
        object_offset: u64,
        stored_len: u32,
        digest: [u8; 32],
        expected_level: u8,
        expected_kind: BatchKind,
        expected_entry_count: Option<u32>,
    ) -> WireResult<Arc<IndexNode>> {
        if let Some(cached) = self.index_cache.get(&key) {
            self.touch_index(&key);
            return Ok(cached.clone());
        }
        let lock = self
            .index_inflight
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        if let Some(cached) = self.index_cache.get(&key) {
            self.touch_index(&key);
            return Ok(cached.clone());
        }
        if stored_len as usize != DEFAULT_INDEX_NODE_SIZE {
            return Err(WireError::invalid(
                "index page",
                "index pages must use the fixed v2 node size",
            ));
        }
        validate_range_bounds(
            object_offset,
            stored_len,
            self.descriptor.metadata_ref.object_len,
        )?;
        let bytes = self.fetch_range(object_offset, stored_len).await?;
        verify_digest("remote index page", &bytes, &digest)?;
        let node = IndexNode::decode(&bytes)?;
        if node.level != expected_level || node.kind != expected_kind {
            return Err(WireError::invalid(
                "index page",
                "decoded level or kind does not match reference",
            ));
        }
        if expected_entry_count.is_some_and(|count| node.entries.len() != count as usize) {
            return Err(WireError::invalid(
                "index page",
                "decoded entry count does not match reference",
            ));
        }
        let node = Arc::new(node);
        self.insert_index_cache(key, node.clone());
        Ok(node)
    }

    fn root_for_kind(&self, kind: BatchKind) -> WireResult<&IndexRootRef> {
        let index = match kind {
            BatchKind::Namespace => 0,
            BatchKind::Extent => 1,
            BatchKind::Attribute => 2,
            BatchKind::DirectoryProjection => 3,
        };
        let root = &self.superblock.index_roots[index];
        if root.stored_len == 0 {
            return Err(WireError::invalid(
                "cluster index root",
                "requested index is absent",
            ));
        }
        if root.kind != kind as u8 || root.raw_len != root.stored_len {
            return Err(WireError::invalid(
                "cluster index root",
                "root kind or length is invalid",
            ));
        }
        Ok(root)
    }

    async fn fetch_range(&self, offset: u64, len: u32) -> WireResult<Vec<u8>> {
        let len_usize = usize::try_from(len)
            .map_err(|_| WireError::LimitExceeded("remote range length exceeds usize".into()))?;
        if len_usize > MAX_RANGE_BYTES {
            return Err(WireError::LimitExceeded(format!(
                "remote range length {len_usize} exceeds {MAX_RANGE_BYTES}"
            )));
        }
        validate_range_bounds(offset, len, self.descriptor.metadata_ref.object_len)?;
        let mut bytes = vec![0u8; len_usize];
        let actual = self
            .client
            .get_object_range(&self.object_key, offset, &mut bytes)
            .await
            .map_err(|error| WireError::invalid("remote object range", error.to_string()))?;
        if actual != len_usize {
            return Err(WireError::Truncated {
                what: "remote object range",
                need: len_usize,
                have: actual,
            });
        }
        Ok(bytes)
    }

    fn insert_index_cache(&self, key: IndexCacheKey, value: Arc<IndexNode>) {
        let bytes = index_memory_bytes(&value);
        if self.options.max_index_cache_entries == 0
            || self.options.max_index_cache_bytes == 0
            || bytes > self.options.max_index_cache_bytes
        {
            return;
        }
        let _guard = self.cache_admission.lock().expect("cache admission lock");
        if self.index_cache.contains_key(&key) {
            self.touch_index(&key);
            return;
        }
        while self.index_cache.len() >= self.options.max_index_cache_entries
            || self
                .index_cache_bytes
                .load(Ordering::Acquire)
                .saturating_add(bytes)
                > self.options.max_index_cache_bytes
        {
            let Some(oldest) = self
                .index_access
                .iter()
                .min_by_key(|entry| *entry.value())
                .map(|entry| entry.key().clone())
            else {
                break;
            };
            self.remove_index_cache(&oldest);
        }
        if self.index_cache.len() >= self.options.max_index_cache_entries
            || self
                .index_cache_bytes
                .load(Ordering::Acquire)
                .saturating_add(bytes)
                > self.options.max_index_cache_bytes
        {
            return;
        }
        let access_key = key.clone();
        self.index_cache.insert(key, value);
        self.touch_index(&access_key);
        self.index_cache_bytes.fetch_add(bytes, Ordering::AcqRel);
    }

    fn insert_batch_cache(&self, key: BatchCacheKey, value: Arc<EncodedBatch>) {
        let bytes = batch_memory_bytes(&value);
        if self.options.max_batch_cache_entries == 0
            || self.options.max_batch_cache_bytes == 0
            || bytes > self.options.max_batch_cache_bytes
        {
            return;
        }
        let _guard = self.cache_admission.lock().expect("cache admission lock");
        if self.batch_cache.contains_key(&key) {
            self.touch_batch(&key);
            return;
        }
        while self.batch_cache.len() >= self.options.max_batch_cache_entries
            || self
                .batch_cache_bytes
                .load(Ordering::Acquire)
                .saturating_add(bytes)
                > self.options.max_batch_cache_bytes
        {
            let Some(oldest) = self
                .batch_access
                .iter()
                .min_by_key(|entry| *entry.value())
                .map(|entry| entry.key().clone())
            else {
                break;
            };
            self.remove_batch_cache(&oldest);
        }
        if self.batch_cache.len() >= self.options.max_batch_cache_entries
            || self
                .batch_cache_bytes
                .load(Ordering::Acquire)
                .saturating_add(bytes)
                > self.options.max_batch_cache_bytes
        {
            return;
        }
        let access_key = key.clone();
        self.batch_cache.insert(key, value);
        self.touch_batch(&access_key);
        self.batch_cache_bytes.fetch_add(bytes, Ordering::AcqRel);
    }

    fn touch_index(&self, key: &IndexCacheKey) {
        let tick = self.cache_clock.fetch_add(1, Ordering::AcqRel);
        self.index_access.insert(key.clone(), tick);
    }

    fn touch_batch(&self, key: &BatchCacheKey) {
        let tick = self.cache_clock.fetch_add(1, Ordering::AcqRel);
        self.batch_access.insert(key.clone(), tick);
    }

    fn remove_index_cache(&self, key: &IndexCacheKey) -> Option<(IndexCacheKey, Arc<IndexNode>)> {
        let removed = self.index_cache.remove(key);
        if let Some((removed_key, node)) = &removed {
            self.index_access.remove(removed_key);
            self.index_cache_bytes
                .fetch_sub(index_memory_bytes(node), Ordering::AcqRel);
        }
        removed
    }

    fn remove_batch_cache(
        &self,
        key: &BatchCacheKey,
    ) -> Option<(BatchCacheKey, Arc<EncodedBatch>)> {
        let removed = self.batch_cache.remove(key);
        if let Some((removed_key, batch)) = &removed {
            self.batch_access.remove(removed_key);
            self.batch_cache_bytes
                .fetch_sub(batch_memory_bytes(batch), Ordering::AcqRel);
        }
        removed
    }
}

fn index_memory_bytes(node: &IndexNode) -> usize {
    let mut bytes = std::mem::size_of::<IndexNode>().saturating_add(
        node.entries
            .capacity()
            .saturating_mul(std::mem::size_of::<IndexEntry>()),
    );
    for entry in &node.entries {
        match entry {
            IndexEntry::Leaf {
                first_key,
                last_key,
                ..
            } => {
                bytes = bytes
                    .saturating_add(first_key.capacity())
                    .saturating_add(last_key.capacity());
            }
            IndexEntry::Internal(child) => {
                bytes = bytes
                    .saturating_add(child.first_key.capacity())
                    .saturating_add(child.last_key.capacity());
            }
        }
    }
    bytes.max(1)
}

fn batch_memory_bytes(batch: &EncodedBatch) -> usize {
    std::mem::size_of::<EncodedBatch>()
        .saturating_add(batch.bytes.capacity())
        .saturating_add(batch.raw_payload.capacity())
        .max(1)
}

fn namespace_entry_name(entry: &NamespaceEntry) -> &super::name::NameBytes {
    match entry {
        NamespaceEntry::NewNode { name, .. } | NamespaceEntry::ExistingNode { name, .. } => name,
    }
}

fn validate_range_bounds(offset: u64, len: u32, object_len: u64) -> WireResult<()> {
    let end = offset
        .checked_add(u64::from(len))
        .ok_or_else(|| WireError::LimitExceeded("remote range offset overflows u64".into()))?;
    if end > object_len {
        return Err(WireError::Truncated {
            what: "remote object range",
            need: len as usize,
            have: object_len.saturating_sub(offset) as usize,
        });
    }
    Ok(())
}

fn validate_index_root(
    root: &IndexRootRef,
    expected_kind: BatchKind,
    required: bool,
    expected_batch_count: u32,
    object_len: u64,
) -> WireResult<()> {
    if !required {
        if root.stored_len != 0 || root.raw_len != 0 || root.entry_count != 0 {
            return Err(WireError::invalid(
                "cluster superblock",
                "absent index root carries non-zero fields",
            ));
        }
        return Ok(());
    }
    if root.kind != expected_kind as u8
        || root.stored_len as usize != DEFAULT_INDEX_NODE_SIZE
        || root.raw_len != root.stored_len
        || root.entry_count != expected_batch_count
        || root.entry_count == 0
    {
        return Err(WireError::invalid(
            "cluster superblock",
            "index root reference is invalid",
        ));
    }
    validate_range_bounds(root.object_offset, root.stored_len, object_len)
}

fn verify_digest(what: &'static str, bytes: &[u8], expected: &[u8; 32]) -> WireResult<()> {
    let computed = blake3::hash(bytes);
    if expected != computed.as_bytes() {
        return Err(WireError::HashMismatch {
            what,
            stored: hex::encode(expected),
            computed: hex::encode(computed.as_bytes()),
        });
    }
    Ok(())
}

fn empty_root() -> IndexRootRef {
    IndexRootRef {
        object_offset: 0,
        stored_len: 0,
        raw_len: 0,
        level: 0,
        kind: 0,
        entry_count: 0,
        digest: [0; 32],
        key_fingerprint: 0,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::cadapter::client::ObjectBackend;
    use crate::workspace_overlay::clustered_snapshot::directory::{DirectoryIdentity, NodeRef};
    use crate::workspace_overlay::clustered_snapshot::identity::DirKey;
    use crate::workspace_overlay::clustered_snapshot::merge::{
        ContributionEntry, DirectoryContribution, build_directory_plan,
    };
    use crate::workspace_overlay::clustered_snapshot::name::NameBytes;
    use crate::workspace_overlay::clustered_snapshot::{
        AttributeBatch, AttributeGroup, ClusterDescriptor, ExtentBatch, ExtentRecord,
        ExtentSegment, ManifestObjectRef, XattrRecord, build_single_directory_cluster,
        build_single_directory_cluster_with_metadata,
    };

    #[test]
    fn default_decoded_cache_budgets_cover_large_read_only_scans() {
        let options = RemoteClusterOptions::default();

        assert_eq!(options.max_index_cache_entries, DEFAULT_INDEX_CACHE_ENTRIES);
        assert_eq!(options.max_batch_cache_entries, DEFAULT_BATCH_CACHE_ENTRIES);
        assert_eq!(options.max_index_cache_bytes, DEFAULT_INDEX_CACHE_BYTES);
        assert_eq!(options.max_batch_cache_bytes, DEFAULT_BATCH_CACHE_BYTES);
        assert_eq!(options.max_index_cache_bytes, 64 * 1024 * 1024);
        assert_eq!(options.max_batch_cache_bytes, 256 * 1024 * 1024);
    }

    #[derive(Clone, Default)]
    struct CountingBackend {
        state: Arc<Mutex<BackendState>>,
    }

    #[derive(Default)]
    struct BackendState {
        objects: HashMap<String, Vec<u8>>,
        ranges: Vec<(String, u64, usize)>,
    }

    impl CountingBackend {
        fn insert(&self, key: &str, bytes: Vec<u8>) {
            self.state
                .lock()
                .expect("backend mutex")
                .objects
                .insert(key.to_string(), bytes);
        }

        fn ranges(&self) -> Vec<(String, u64, usize)> {
            self.state.lock().expect("backend mutex").ranges.clone()
        }

        fn tamper(&self, key: &str, offset: usize) {
            let mut state = self.state.lock().expect("backend mutex");
            state.objects.get_mut(key).expect("object exists")[offset] ^= 1;
        }
    }

    #[async_trait]
    impl ObjectBackend for CountingBackend {
        async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            self.insert(key, data.to_vec());
            Ok(())
        }

        async fn get_object(&self, _key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn get_object_range(
            &self,
            key: &str,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<usize> {
            let mut state = self.state.lock().expect("backend mutex");
            let Some(object) = state.objects.get(key) else {
                return Ok(0);
            };
            let start = usize::try_from(offset).unwrap();
            if start >= object.len() {
                return Ok(0);
            }
            let count = buf.len().min(object.len() - start);
            buf[..count].copy_from_slice(&object[start..start + count]);
            state.ranges.push((key.to_string(), offset, count));
            Ok(count)
        }

        async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
            Ok(String::new())
        }

        async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
            self.state
                .lock()
                .expect("backend mutex")
                .objects
                .remove(key);
            Ok(())
        }
    }

    fn contribution(dir_key: DirKey, names: &[&str]) -> DirectoryContribution {
        DirectoryContribution {
            dir_key,
            node: NodeRef {
                cluster_slot: 0,
                local_node_id: 1,
            },
            attributes_digest: [7; 32],
            entries: names
                .iter()
                .enumerate()
                .map(|(index, name)| ContributionEntry {
                    name: NameBytes::new(name.as_bytes().to_vec()).unwrap(),
                    inode: 10 + index as u64,
                    kind: 1,
                    child_dir_key: None,
                    attributes_digest: [0; 32],
                })
                .collect(),
        }
    }

    fn descriptor(cluster: &super::super::cluster_builder::BuiltCluster) -> ClusterDescriptor {
        let hash = *blake3::hash(&cluster.bytes).as_bytes();
        ClusterDescriptor {
            cluster_id: cluster.superblock.cluster_id,
            metadata_ref: ManifestObjectRef {
                object_id: [9; 16],
                kind: 1,
                object_len: cluster.bytes.len() as u64,
                full_hash: hash,
                key: b"clusters/test.brfc".to_vec(),
            },
            data_seal_ref: ManifestObjectRef {
                object_id: [10; 16],
                kind: 2,
                object_len: 1,
                full_hash: [0; 32],
                key: b"seals/test.brfds".to_vec(),
            },
            combined_semantic_hash: [0; 32],
            mount_dir_key: cluster.superblock.mount_dir_key,
            root_local_node_id: 1,
            flags: 0,
        }
    }

    #[tokio::test]
    async fn open_and_lookup_fetch_only_superblock_index_and_batch_once() {
        let dir_key = DirKey::new([3; 16]);
        let plan = build_directory_plan(
            DirectoryIdentity::from_dir_key([4; 32], dir_key),
            vec![contribution(dir_key, &["alpha", "target", "zulu"])],
        )
        .unwrap();
        let built =
            build_single_directory_cluster([5; 16], [6; 16], dir_key, &plan, 1, dir_key).unwrap();
        let backend = CountingBackend::default();
        backend.insert("clusters/test.brfc", built.bytes.clone());
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteCluster::open(&client, descriptor(&built))
            .await
            .unwrap();

        let found = remote.lookup_namespace(1, b"target").await.unwrap();
        assert!(matches!(found, Some(NamespaceEntry::NewNode { .. })));
        let first_count = backend.ranges().len();
        assert_eq!(first_count, 3, "superblock, index root, and one batch");

        let found_again = remote.lookup_namespace(1, b"target").await.unwrap();
        assert!(found_again.is_some());
        assert_eq!(backend.ranges().len(), first_count);
        assert_eq!(remote.metadata_cache_entries(), (1, 1));

        assert!(remote.evict_batch(0));
        remote.lookup_namespace(1, b"target").await.unwrap();
        assert_eq!(backend.ranges().len(), first_count + 1);
    }

    #[tokio::test]
    async fn cold_attribute_lookup_targets_only_attribute_index_and_batch() {
        let dir_key = DirKey::new([23; 16]);
        let plan = build_directory_plan(
            DirectoryIdentity::from_dir_key([24; 32], dir_key),
            vec![contribution(dir_key, &["file"])],
        )
        .unwrap();
        let attributes = AttributeBatch {
            cluster_id: [25; 16],
            batch_id: 0,
            stream_ordinal: 0,
            predecessor_ordinal:
                crate::workspace_overlay::clustered_snapshot::BatchHeader::NO_PREDECESSOR,
            groups: vec![AttributeGroup {
                local_node_id: 2,
                symlink_target: None,
                xattrs: vec![XattrRecord {
                    name: b"user.test".to_vec(),
                    value: b"packed".to_vec(),
                }],
                acl: None,
            }],
        };
        let built = build_single_directory_cluster_with_metadata(
            [25; 16],
            [26; 16],
            dir_key,
            &plan,
            1,
            dir_key,
            &[],
            &[attributes],
        )
        .unwrap();
        let backend = CountingBackend::default();
        backend.insert("clusters/test.brfc", built.bytes.clone());
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteCluster::open(&client, descriptor(&built))
            .await
            .unwrap();

        let group = remote.lookup_attribute(2).await.unwrap().unwrap();
        assert_eq!(group.xattrs[0].value, b"packed");
        assert_eq!(
            backend
                .ranges()
                .iter()
                .filter(|(key, _, _)| key == "clusters/test.brfc")
                .count(),
            3,
            "superblock, attribute index root, and one attribute batch"
        );
        assert!(remote.lookup_attribute(3).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn extent_range_targets_only_extent_index_and_batches_and_clips_spans() {
        let dir_key = DirKey::new([33; 16]);
        let plan = build_directory_plan(
            DirectoryIdentity::from_dir_key([34; 32], dir_key),
            vec![contribution(dir_key, &["file"])],
        )
        .unwrap();
        let extent_batch = ExtentBatch {
            cluster_id: [35; 16],
            batch_id: 0,
            stream_ordinal: 0,
            predecessor_ordinal:
                crate::workspace_overlay::clustered_snapshot::BatchHeader::NO_PREDECESSOR,
            segments: vec![ExtentSegment {
                local_node_id: 2,
                first_file_offset: 0,
                extents: vec![
                    ExtentRecord {
                        gap_from_previous_end: 0,
                        logical_length: 4,
                        slice_id: 9,
                        slice_offset: 10,
                    },
                    ExtentRecord {
                        gap_from_previous_end: 4,
                        logical_length: 8,
                        slice_id: 10,
                        slice_offset: 100,
                    },
                ],
            }],
        };
        let built = build_single_directory_cluster_with_metadata(
            [35; 16],
            [36; 16],
            dir_key,
            &plan,
            1,
            dir_key,
            &[extent_batch],
            &[],
        )
        .unwrap();
        let backend = CountingBackend::default();
        backend.insert("clusters/test.brfc", built.bytes.clone());
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteCluster::open(&client, descriptor(&built))
            .await
            .unwrap();

        let spans = remote.read_extent_range(2, 2, 8).await.unwrap();
        assert_eq!(
            spans,
            vec![
                ExtentSpan {
                    local_node_id: 2,
                    file_offset: 2,
                    logical_length: 2,
                    slice_id: 9,
                    slice_offset: 12,
                },
                ExtentSpan {
                    local_node_id: 2,
                    file_offset: 8,
                    logical_length: 2,
                    slice_id: 10,
                    slice_offset: 100,
                },
            ]
        );
        let ranges = backend.ranges();
        assert_eq!(
            ranges.len(),
            3,
            "superblock, extent index, and extent batch"
        );
        assert!(ranges.iter().all(|(key, _, _)| key == "clusters/test.brfc"));
        assert_eq!(remote.metadata_cache_entries(), (1, 1));
    }

    #[tokio::test]
    async fn decoded_cache_byte_caps_fail_closed_without_unbounded_residency() {
        let dir_key = DirKey::new([37; 16]);
        let plan = build_directory_plan(
            DirectoryIdentity::from_dir_key([38; 32], dir_key),
            vec![contribution(dir_key, &["file"])],
        )
        .unwrap();
        let built =
            build_single_directory_cluster([39; 16], [40; 16], dir_key, &plan, 1, dir_key).unwrap();
        let backend = CountingBackend::default();
        backend.insert("clusters/test.brfc", built.bytes.clone());
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteCluster::open_with_options(
            &client,
            descriptor(&built),
            RemoteClusterOptions {
                max_index_cache_entries: DEFAULT_INDEX_CACHE_ENTRIES,
                max_batch_cache_entries: DEFAULT_BATCH_CACHE_ENTRIES,
                max_index_cache_bytes: 1,
                max_batch_cache_bytes: 1,
            },
        )
        .await
        .unwrap();

        remote.lookup_namespace(1, b"file").await.unwrap();
        remote.lookup_namespace(1, b"file").await.unwrap();
        assert_eq!(remote.metadata_cache_entries(), (0, 0));
        assert_eq!(remote.metadata_cache_bytes(), (0, 0));
        assert_eq!(
            backend.ranges().len(),
            5,
            "superblock plus two uncached lookups"
        );
    }

    #[tokio::test]
    async fn remote_reader_fails_closed_on_tampered_index_page() {
        let dir_key = DirKey::new([13; 16]);
        let plan = build_directory_plan(
            DirectoryIdentity::from_dir_key([14; 32], dir_key),
            vec![contribution(dir_key, &["only"])],
        )
        .unwrap();
        let built =
            build_single_directory_cluster([15; 16], [16; 16], dir_key, &plan, 1, dir_key).unwrap();
        let backend = CountingBackend::default();
        backend.insert("clusters/test.brfc", built.bytes.clone());
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteCluster::open(&client, descriptor(&built))
            .await
            .unwrap();
        let root_offset = remote.superblock().index_roots[0].object_offset as usize;
        backend.tamper("clusters/test.brfc", root_offset + 40);
        assert!(matches!(
            remote.read_index_root(BatchKind::Namespace).await,
            Err(WireError::HashMismatch { .. })
        ));
    }
}
