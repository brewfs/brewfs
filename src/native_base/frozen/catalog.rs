//! Authenticated, read-only object catalog for a frozen metadata revision.
//!
//! `FixedRevisionReader` is intentionally synchronous because its lookup
//! contract is an already-open object source.  Production object backends are
//! asynchronous, so this adapter opens a manifest asynchronously, fetches the
//! complete authenticated page closure once, and then exposes the fixed
//! reader without a KV fallback.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;

use super::{
    BudgetReservation, DecodedPageKey, FixedRevisionReader, FrozenInodeRecord, FrozenReadError,
    MetadataBudget, MetadataBudgetSnapshot, ReaderPageCache, SnapshotManifest, decoded_page_key,
    dentry_prefix, inode_key,
};
use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::native_base::seal::source::{ObjectSource, ObjectSourceError};
use crate::native_base::wire::container::{
    Codec, ContainerHeader, FOOTER_LEN, HEADER_LEN, ObjectKind,
};
use crate::native_base::wire::error::WireError;
use crate::native_base::wire::page::{IndexPage, InternalEntry, LeafEntry, PageBody};
use crate::native_base::wire::refs::{
    ChildRef, MAX_INDEX_LEVEL, ObjectId, ObjectRef, PageAddress, PageKind, RootRef,
};

// Keep metadata read-ahead bounded.  A large contiguous range is cheaper than
// one request per BNPG page during readdir, but a narrow lookup must not turn
// into an object-sized download.
const MAX_COALESCED_METADATA_RANGE: u64 = 1024 * 1024;
/// Default process-local admission budget for a streaming frozen catalog.
/// Deployments with a tighter or shared metadata budget can use
/// `open_by_key_with_budget`/`open_with_budget`.
/// Default process-local metadata budget for a packed read-only mount.
///
/// Packed catalogs are immutable, so retaining decoded index pages and the
/// extent rows shared by read handles is safe.  512 MiB keeps a large
/// directory/workload from repeatedly re-reading metadata while remaining a
/// bounded per-mount allocation.
pub const DEFAULT_STREAMING_METADATA_BUDGET: usize = 512 * 1024 * 1024;

/// A directory listing row resolved entirely from one immutable snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenDirectoryEntry {
    pub name: Vec<u8>,
    pub inode: u64,
    pub attr: FrozenInodeRecord,
}

/// Read-only operations needed by the FUSE metadata facade.
///
/// The original in-memory catalog implements this with synchronous lookups;
/// the production packed mount uses the streaming implementation below.  A
/// small async trait keeps the facade independent of the storage strategy and
/// lets the latter issue S3 range requests without blocking a Tokio worker.
#[async_trait]
pub trait FrozenCatalog: Send + Sync {
    fn manifest(&self) -> &SnapshotManifest;

    async fn lookup_inode(&self, inode: u64) -> Result<Option<FrozenInodeRecord>, FrozenReadError>;

    async fn lookup_dentry(
        &self,
        parent: u64,
        name: &[u8],
    ) -> Result<Option<(u64, FrozenInodeRecord)>, FrozenReadError>;

    async fn readdir(&self, parent: u64) -> Result<Vec<FrozenDirectoryEntry>, FrozenReadError>;

    /// Return one bounded page of a directory without materializing the
    /// directory. `child_offset` is the stable ordinal among child entries;
    /// the FUSE adapter reserves ordinals 0 and 1 for `.` and `..`.
    ///
    /// Every packed catalog must implement this with an indexed/pageable
    /// implementation. There is intentionally no default that calls
    /// `readdir()`: doing so would turn a large-directory request back into a
    /// whole-directory materialization.
    async fn readdir_page(
        &self,
        parent: u64,
        child_offset: u64,
        limit: usize,
    ) -> Result<Vec<FrozenDirectoryEntry>, FrozenReadError>;

    async fn names_for_inode(&self, inode: u64) -> Result<Vec<(u64, Vec<u8>)>, FrozenReadError>;

    async fn readlink(&self, inode: u64) -> Result<Option<Vec<u8>>, FrozenReadError>;

    async fn query_extents(
        &self,
        inode: u64,
        chunk_index: u64,
        start: u64,
        end: u64,
    ) -> Result<Vec<super::FrozenExtent>, FrozenReadError>;
}

#[derive(Default)]
struct CachedObjectSource {
    objects: HashMap<ObjectId, Vec<u8>>,
}

impl CachedObjectSource {
    fn insert(&mut self, object: ObjectId, bytes: Vec<u8>) {
        self.objects.insert(object, bytes);
    }
}

impl ObjectSource for CachedObjectSource {
    fn get_range(
        &self,
        object_id: &ObjectId,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, ObjectSourceError> {
        let bytes = self
            .objects
            .get(object_id)
            .ok_or(ObjectSourceError::NotFound)?;
        let start = usize::try_from(start).map_err(|_| ObjectSourceError::ShortRead {
            requested: end.saturating_sub(start),
            received: 0,
        })?;
        let end = usize::try_from(end).map_err(|_| ObjectSourceError::ShortRead {
            requested: end.saturating_sub(start as u64),
            received: bytes.len().saturating_sub(start) as u64,
        })?;
        if start > end || end > bytes.len() {
            return Err(ObjectSourceError::ShortRead {
                requested: end.saturating_sub(start) as u64,
                received: bytes.len().saturating_sub(start) as u64,
            });
        }
        Ok(bytes[start..end].to_vec())
    }
}

/// A complete, authenticated page closure for one immutable manifest.
///
/// The catalog owns no metadata client and cannot issue writes, leases, or
/// mutable RPCs.  Missing or malformed closure members fail during `open`.
pub struct FrozenMetadataCatalog {
    source: CachedObjectSource,
    manifest: SnapshotManifest,
    page_cache: Arc<ReaderPageCache>,
    inode_cache: RwLock<HashMap<u64, FrozenInodeRecord>>,
}

impl FrozenMetadataCatalog {
    /// Open a manifest when the caller has the immutable object key but does
    /// not have a separately persisted `ObjectRef`. The complete-object hash
    /// is verified before the synthetic identity is used for page closure
    /// loading; the identity is only an in-memory source key.
    pub async fn open_by_key<B: ObjectBackend>(
        client: &ObjectClient<B>,
        key: &str,
    ) -> Result<Self, FrozenReadError> {
        let bytes = client
            .get_object(key)
            .await
            .map_err(|error| ObjectSourceError::Backend(error.to_string()))?
            .ok_or(ObjectSourceError::NotFound)?;
        let full_hash: ObjectId = Sha256::digest(&bytes)[..16]
            .try_into()
            .expect("sha256 prefix length");
        let object = ObjectRef {
            object_id: full_hash,
            kind: ObjectKind::SnapshotManifest.as_u8(),
            object_len: bytes.len() as u64,
            full_hash: Sha256::digest(&bytes).into(),
            key: key.as_bytes().to_vec(),
        };
        // Keep the manifest body fetched above. Calling `open` here used to
        // download and hash the same immutable object a second time before
        // the page-closure walk began.
        Self::open_with_manifest_bytes(client, &object, bytes).await
    }

    pub async fn open<B: ObjectBackend>(
        client: &ObjectClient<B>,
        manifest: &ObjectRef,
    ) -> Result<Self, FrozenReadError> {
        if manifest.kind != ObjectKind::SnapshotManifest.as_u8() {
            return Err(FrozenReadError::WrongObjectKind);
        }
        let manifest_bytes = fetch_object(client, manifest).await?;
        Self::open_with_manifest_bytes(client, manifest, manifest_bytes).await
    }

    async fn open_with_manifest_bytes<B: ObjectBackend>(
        client: &ObjectClient<B>,
        manifest: &ObjectRef,
        manifest_bytes: Vec<u8>,
    ) -> Result<Self, FrozenReadError> {
        let mut source = CachedObjectSource::default();
        source.insert(manifest.object_id, manifest_bytes.clone());
        let bootstrap = FixedRevisionReader::open_manifest(&source, &manifest_bytes)?;
        let decoded = bootstrap.manifest().clone();
        let page_cache = Arc::new(ReaderPageCache::default());

        let mut pages = VecDeque::new();
        pages.push_back((decoded.data_root.object.clone(), decoded.data_root.address));
        pages.push_back((
            decoded.inventory_root.object.clone(),
            decoded.inventory_root.address,
        ));
        if let Some(root) = decoded.namespace_root.clone() {
            pages.push_back((root.object, root.address));
        }
        let mut seen = HashMap::<(ObjectId, u64, u32), ()>::new();
        while let Some((object, address)) = pages.pop_front() {
            let key = (object.object_id, address.offset, address.stored_len);
            if seen.insert(key, ()).is_some() {
                continue;
            }
            if !source.objects.contains_key(&object.object_id) {
                let bytes = fetch_object(client, &object).await?;
                source.insert(object.object_id, bytes);
            }
            let bytes = source
                .objects
                .get(&object.object_id)
                .expect("inserted above");
            let raw = decode_page(bytes, &object, &address)?;
            let page = IndexPage::decode(&raw)?;
            page_cache.insert(decoded_page_key(&object, &address), page.clone());
            if let PageBody::Internal(entries) = page.body {
                for entry in entries {
                    match entry.child {
                        ChildRef::External(root) => pages.push_back((root.object, root.address)),
                        ChildRef::Local(child) => pages.push_back((object.clone(), child)),
                    }
                }
            }
        }

        Ok(Self {
            source,
            manifest: decoded,
            page_cache,
            inode_cache: RwLock::new(HashMap::new()),
        })
    }

    pub fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    fn reader(&self) -> Result<FixedRevisionReader<'_, CachedObjectSource>, FrozenReadError> {
        // The manifest and every referenced object were authenticated during
        // open. Reuse the decoded manifest and page cache instead of parsing
        // the manifest and decoding the same index pages for each API call.
        Ok(FixedRevisionReader::from_decoded_manifest(
            &self.source,
            self.manifest.clone(),
            self.page_cache.clone(),
        ))
    }

    pub fn object_count(&self) -> usize {
        self.source.objects.len()
    }

    pub fn lookup_namespace(&self, key: &[u8]) -> Result<Option<Vec<u8>>, FrozenReadError> {
        self.reader()?.lookup_namespace(key)
    }

    pub fn scan_namespace_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FrozenReadError> {
        self.reader()?.scan_namespace_prefix(prefix)
    }

    pub fn lookup_data(&self, key: &[u8]) -> Result<Option<Vec<u8>>, FrozenReadError> {
        self.reader()?.lookup_data(key)
    }

    pub fn lookup_inventory(&self, key: &[u8]) -> Result<Option<Vec<u8>>, FrozenReadError> {
        self.reader()?.lookup_inventory(key)
    }

    pub fn lookup_inode(
        &self,
        inode: u64,
    ) -> Result<Option<super::FrozenInodeRecord>, FrozenReadError> {
        if let Some(record) = self
            .inode_cache
            .read()
            .ok()
            .and_then(|cache| cache.get(&inode).cloned())
        {
            return Ok(Some(record));
        }
        let record = self.reader()?.lookup_inode(inode)?;
        if let Some(record) = &record
            && let Ok(mut cache) = self.inode_cache.write()
        {
            cache.insert(inode, record.clone());
        }
        Ok(record)
    }

    /// Resolve one dentry and its attributes without consulting a mutable
    /// metadata service.  Dentry values are deliberately required to be the
    /// canonical `inode_key` encoding; accepting an arbitrary integer here
    /// would make malformed snapshots ambiguous.
    pub fn lookup_dentry(
        &self,
        parent: u64,
        name: &[u8],
    ) -> Result<Option<(u64, FrozenInodeRecord)>, FrozenReadError> {
        let Some(value) = self.reader()?.lookup_dentry(parent, name)? else {
            return Ok(None);
        };
        let inode = decode_inode_ref(&value)?;
        let Some(attr) = self.lookup_inode(inode)? else {
            return Err(FrozenReadError::Wire(
                crate::native_base::wire::error::WireError::invalid(
                    "frozen dentry",
                    "dentry references a missing inode",
                ),
            ));
        };
        Ok(Some((inode, attr)))
    }

    /// Return a deterministic, fully resolved directory listing from the
    /// authenticated namespace prefix.  Every returned inode is checked
    /// before the listing is exposed to callers.
    pub fn readdir(&self, parent: u64) -> Result<Vec<FrozenDirectoryEntry>, FrozenReadError> {
        let prefix = dentry_prefix(parent);
        let rows = self.reader()?.scan_namespace_prefix(&prefix)?;
        let mut entries = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            if key.len() <= prefix.len() {
                return Err(FrozenReadError::Wire(
                    crate::native_base::wire::error::WireError::invalid(
                        "frozen dentry",
                        "dentry key has an empty name",
                    ),
                ));
            }
            let name = key[prefix.len()..].to_vec();
            let inode = decode_inode_ref(&value)?;
            let Some(attr) = self.lookup_inode(inode)? else {
                return Err(FrozenReadError::Wire(
                    crate::native_base::wire::error::WireError::invalid(
                        "frozen dentry",
                        "dentry references a missing inode",
                    ),
                ));
            };
            entries.push(FrozenDirectoryEntry { name, inode, attr });
        }
        Ok(entries)
    }

    /// Return every parent/name pair that links to an inode. This is a
    /// reverse namespace query used by `MetaLayer::get_names` and hardlink
    /// path reconstruction; it remains entirely inside the authenticated
    /// frozen namespace tree.
    pub fn names_for_inode(&self, inode: u64) -> Result<Vec<(u64, Vec<u8>)>, FrozenReadError> {
        let rows = self.reader()?.scan_namespace_prefix(b"d")?;
        let mut names = Vec::new();
        for (key, value) in rows {
            let prefix_len = super::dentry_prefix(0).len();
            if key.len() <= prefix_len || key[9] != 0 {
                return Err(FrozenReadError::Wire(
                    crate::native_base::wire::error::WireError::invalid(
                        "frozen dentry",
                        "invalid dentry key",
                    ),
                ));
            }
            if decode_inode_ref(&value)? != inode {
                continue;
            }
            let parent = u64::from_be_bytes(key[1..9].try_into().unwrap());
            names.push((parent, key[prefix_len..].to_vec()));
        }
        names.sort_by(|a, b| a.cmp(b));
        Ok(names)
    }

    pub fn readlink(&self, inode: u64) -> Result<Option<Vec<u8>>, FrozenReadError> {
        Ok(self
            .lookup_inode(inode)?
            .and_then(|attr| attr.symlink_target))
    }

    pub fn query_extents(
        &self,
        inode: u64,
        chunk_index: u64,
        start: u64,
        end: u64,
    ) -> Result<Vec<super::FrozenExtent>, FrozenReadError> {
        self.reader()?
            .query_extent_rows(inode, chunk_index, start, end)
    }
}

/// A page-at-a-time catalog for the production packed mount.
///
/// The eager catalog above is intentionally retained for synchronous protocol
/// tests and callers that already own a complete object source.  A real S3
/// mount must not download the data and inventory trees merely to serve the
/// namespace root, though.  This catalog keeps only the manifest, decoded
/// pages, and inode records that have actually been touched.  Each missing
/// page is fetched with one exact range GET and protected by a per-page
/// single-flight lock so concurrent FUSE requests share the same transfer.
pub struct StreamingFrozenMetadataCatalog<B: ObjectBackend + Clone> {
    client: ObjectClient<B>,
    manifest: SnapshotManifest,
    page_cache: Arc<ReaderPageCache>,
    page_reservations: RwLock<HashMap<DecodedPageKey, BudgetReservation>>,
    page_flights: DashMap<DecodedPageKey, Arc<AsyncMutex<()>>>,
    headers: RwLock<HashMap<ObjectId, ContainerHeader>>,
    object_refs: DashMap<ObjectId, ObjectRef>,
    inode_cache: RwLock<HashMap<u64, FrozenInodeRecord>>,
    inode_reservations: RwLock<HashMap<u64, BudgetReservation>>,
    /// Immutable extent rows are shared across all read handles. Without this
    /// cache every new handle would walk the data B-tree again even though the
    /// result is fixed for the manifest revision.
    extent_cache: DashMap<(u64, u64), Arc<Vec<super::FrozenExtent>>>,
    extent_reservations: RwLock<HashMap<(u64, u64), BudgetReservation>>,
    extent_flights: DashMap<(u64, u64), Arc<AsyncMutex<()>>>,
    budget: MetadataBudget,
}

impl<B> StreamingFrozenMetadataCatalog<B>
where
    B: ObjectBackend + Clone + 'static,
{
    pub async fn open_by_key(client: &ObjectClient<B>, key: &str) -> Result<Self, FrozenReadError> {
        Self::open_by_key_with_budget(
            client,
            key,
            MetadataBudget::new(DEFAULT_STREAMING_METADATA_BUDGET),
        )
        .await
    }

    pub async fn open_by_key_with_budget(
        client: &ObjectClient<B>,
        key: &str,
        budget: MetadataBudget,
    ) -> Result<Self, FrozenReadError> {
        let bytes = client
            .get_object(key)
            .await
            .map_err(|error| ObjectSourceError::Backend(error.to_string()))?
            .ok_or(ObjectSourceError::NotFound)?;
        let full_hash: ObjectId = Sha256::digest(&bytes)[..16]
            .try_into()
            .expect("sha256 prefix length");
        let object = ObjectRef {
            object_id: full_hash,
            kind: ObjectKind::SnapshotManifest.as_u8(),
            object_len: bytes.len() as u64,
            full_hash: Sha256::digest(&bytes).into(),
            key: key.as_bytes().to_vec(),
        };
        Self::from_manifest_bytes_with_budget(client.clone(), &object, bytes, budget)
    }

    pub async fn open(
        client: &ObjectClient<B>,
        manifest: &ObjectRef,
    ) -> Result<Self, FrozenReadError> {
        Self::open_with_budget(
            client,
            manifest,
            MetadataBudget::new(DEFAULT_STREAMING_METADATA_BUDGET),
        )
        .await
    }

    pub async fn open_with_budget(
        client: &ObjectClient<B>,
        manifest: &ObjectRef,
        budget: MetadataBudget,
    ) -> Result<Self, FrozenReadError> {
        if manifest.kind != ObjectKind::SnapshotManifest.as_u8() {
            return Err(FrozenReadError::WrongObjectKind);
        }
        let bytes = fetch_object(client, manifest).await?;
        Self::from_manifest_bytes_with_budget(client.clone(), manifest, bytes, budget)
    }

    fn from_manifest_bytes(
        client: ObjectClient<B>,
        manifest_object: &ObjectRef,
        manifest_bytes: Vec<u8>,
    ) -> Result<Self, FrozenReadError> {
        Self::from_manifest_bytes_with_budget(
            client,
            manifest_object,
            manifest_bytes,
            MetadataBudget::new(DEFAULT_STREAMING_METADATA_BUDGET),
        )
    }

    fn from_manifest_bytes_with_budget(
        client: ObjectClient<B>,
        manifest_object: &ObjectRef,
        manifest_bytes: Vec<u8>,
        budget: MetadataBudget,
    ) -> Result<Self, FrozenReadError> {
        let mut source = CachedObjectSource::default();
        source.insert(manifest_object.object_id, manifest_bytes.clone());
        let bootstrap = FixedRevisionReader::open_manifest(&source, &manifest_bytes)?;
        let manifest = bootstrap.manifest().clone();
        let object_refs = DashMap::new();
        object_refs.insert(
            manifest.data_root.object.object_id,
            manifest.data_root.object.clone(),
        );
        object_refs.insert(
            manifest.inventory_root.object.object_id,
            manifest.inventory_root.object.clone(),
        );
        if let Some(root) = &manifest.namespace_root {
            object_refs.insert(root.object.object_id, root.object.clone());
        }
        Ok(Self {
            client,
            manifest,
            page_cache: Arc::new(ReaderPageCache::default()),
            page_reservations: RwLock::new(HashMap::new()),
            page_flights: DashMap::new(),
            headers: RwLock::new(HashMap::new()),
            object_refs,
            inode_cache: RwLock::new(HashMap::new()),
            inode_reservations: RwLock::new(HashMap::new()),
            extent_cache: DashMap::new(),
            extent_reservations: RwLock::new(HashMap::new()),
            extent_flights: DashMap::new(),
            budget,
        })
    }

    pub fn metadata_budget_snapshot(&self) -> MetadataBudgetSnapshot {
        self.budget.snapshot()
    }

    /// Evict decoded index pages that are not needed by a caller. Stable
    /// object/page identities remain in the manifest, so the next lookup will
    /// authenticate and fetch the exact page again. The page reservation is
    /// released together with the decoded allocation.
    pub fn evict_metadata_pages(&self, max_pages: usize) -> usize {
        let mut evicted = 0;
        for key in self.page_cache.keys().into_iter().take(max_pages) {
            // Serialize page removal with reservation insertion. Removing the
            // cache entry first lets a concurrent refetch install a new page
            // and reservation, after which the old eviction could delete the
            // new reservation and leave the page unaccounted.
            let Ok(mut reservations) = self.page_reservations.write() else {
                continue;
            };
            if self.page_cache.remove(&key) {
                reservations.remove(&key);
                evicted += 1;
            }
        }
        let remaining = max_pages.saturating_sub(evicted);
        if remaining == 0 {
            return evicted;
        }
        let keys = self
            .extent_cache
            .iter()
            .take(remaining)
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for key in keys {
            let Ok(mut reservations) = self.extent_reservations.write() else {
                continue;
            };
            if self.extent_cache.remove(&key).is_some() {
                reservations.remove(&key);
                evicted += 1;
            }
        }
        evicted
    }

    fn budget_error(error: impl std::fmt::Display) -> FrozenReadError {
        FrozenReadError::Wire(WireError::LimitExceeded(format!(
            "metadata budget admission failed: {error}"
        )))
    }

    fn page_memory_bytes(page: &IndexPage) -> usize {
        let mut bytes = std::mem::size_of::<IndexPage>() + std::mem::size_of::<PageBody>();
        match &page.body {
            PageBody::Leaf(entries) => {
                bytes = bytes.saturating_add(
                    entries
                        .capacity()
                        .saturating_mul(std::mem::size_of::<LeafEntry>()),
                );
                for entry in entries {
                    bytes = bytes
                        .saturating_add(entry.key.len())
                        .saturating_add(entry.value.len());
                }
            }
            PageBody::Internal(entries) => {
                bytes = bytes.saturating_add(
                    entries
                        .capacity()
                        .saturating_mul(std::mem::size_of::<InternalEntry>()),
                );
                for entry in entries {
                    bytes = bytes
                        .saturating_add(entry.min_key.len())
                        .saturating_add(entry.max_key.len());
                }
            }
        }
        bytes.max(1)
    }

    fn cache_page(&self, key: DecodedPageKey, page: IndexPage) -> Result<(), FrozenReadError> {
        if self.page_cache.get_shared(&key).is_some() {
            return Ok(());
        }
        let reservation = self
            .budget
            .reserve_reclaimable_cache(Self::page_memory_bytes(&page))
            .map_err(Self::budget_error)?;
        let mut reservations = self
            .page_reservations
            .write()
            .map_err(|_| Self::budget_error("page reservation lock poisoned"))?;
        if reservations.contains_key(&key) {
            return Ok(());
        }
        reservations.insert(key, reservation);
        drop(reservations);
        self.page_cache.insert(key, page);
        Ok(())
    }

    fn cache_inode(&self, inode: u64, record: FrozenInodeRecord) -> Result<(), FrozenReadError> {
        let mut cache = self
            .inode_cache
            .write()
            .map_err(|_| Self::budget_error("inode cache lock poisoned"))?;
        if cache.contains_key(&inode) {
            return Ok(());
        }
        let bytes = std::mem::size_of::<FrozenInodeRecord>()
            .saturating_add(record.symlink_target.as_ref().map_or(0, Vec::len));
        let reservation = self
            .budget
            .reserve_handles_and_inodes(bytes.max(1))
            .map_err(Self::budget_error)?;
        let mut reservations = self
            .inode_reservations
            .write()
            .map_err(|_| Self::budget_error("inode reservation lock poisoned"))?;
        if reservations.contains_key(&inode) {
            return Ok(());
        }
        reservations.insert(inode, reservation);
        drop(reservations);
        cache.insert(inode, record);
        Ok(())
    }

    fn extent_rows_memory_bytes(rows: &[super::FrozenExtent]) -> usize {
        rows.iter()
            .fold(std::mem::size_of_val(rows).max(1), |total, row| {
                total
                    .saturating_add(std::mem::size_of::<super::FrozenExtent>())
                    .saturating_add(row.value.capacity())
            })
    }

    fn cache_extent_rows(
        &self,
        key: (u64, u64),
        rows: Arc<Vec<super::FrozenExtent>>,
    ) -> Result<(), FrozenReadError> {
        if self.extent_cache.contains_key(&key) {
            return Ok(());
        }
        let reservation = self
            .budget
            .reserve_reclaimable_cache(Self::extent_rows_memory_bytes(&rows))
            .map_err(Self::budget_error)?;
        let mut reservations = self
            .extent_reservations
            .write()
            .map_err(|_| Self::budget_error("extent reservation lock poisoned"))?;
        if reservations.contains_key(&key) {
            return Ok(());
        }
        reservations.insert(key, reservation);
        drop(reservations);
        self.extent_cache.insert(key, rows);
        Ok(())
    }

    async fn get_exact(
        &self,
        object: &ObjectRef,
        offset: u64,
        len: u32,
    ) -> Result<Vec<u8>, FrozenReadError> {
        let end = offset
            .checked_add(u64::from(len))
            .ok_or(FrozenReadError::PageOutOfBounds)?;
        if end > object.object_len {
            return Err(FrozenReadError::PageOutOfBounds);
        }
        let key = std::str::from_utf8(&object.key)
            .map_err(|_| ObjectSourceError::Backend("object key is not UTF-8".into()))?;
        let _compressed = self
            .budget
            .reserve_compressed_inflight(len as usize)
            .map_err(Self::budget_error)?;
        let mut bytes = vec![0u8; len as usize];
        let read = self
            .client
            .get_object_range(key, offset, &mut bytes)
            .await
            .map_err(|error| ObjectSourceError::Backend(error.to_string()))?;
        if read != bytes.len() {
            return Err(ObjectSourceError::ShortRead {
                requested: bytes.len() as u64,
                received: read as u64,
            }
            .into());
        }
        Ok(bytes)
    }

    async fn object_header(&self, object: &ObjectRef) -> Result<ContainerHeader, FrozenReadError> {
        if let Some(header) = self
            .headers
            .read()
            .ok()
            .and_then(|headers| headers.get(&object.object_id).cloned())
        {
            return Ok(header);
        }
        let bytes = self.get_exact(object, 0, HEADER_LEN as u32).await?;
        let header = ContainerHeader::parse(&bytes)?;
        header.ensure_supported_features()?;
        if header.object_len != object.object_len {
            return Err(WireError::invalid(
                "frozen metadata object",
                "header length disagrees with manifest",
            )
            .into());
        }
        if let Ok(mut headers) = self.headers.write() {
            headers.insert(object.object_id, header.clone());
        }
        Ok(header)
    }

    async fn read_page(
        &self,
        current_object: &ObjectRef,
        child: &ChildRef,
    ) -> Result<(Arc<IndexPage>, ObjectRef), FrozenReadError> {
        let (object, address) = match child {
            ChildRef::External(root) => (&root.object, root.address),
            ChildRef::Local(address) => (current_object, *address),
        };
        self.object_refs.insert(object.object_id, object.clone());
        let cache_key = decoded_page_key(object, &address);
        if let Some(page) = self.page_cache.get_shared(&cache_key) {
            return Ok((page, object.clone()));
        }

        let flight = self
            .page_flights
            .entry(cache_key)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone();
        let _guard = flight.lock().await;
        if let Some(page) = self.page_cache.get_shared(&cache_key) {
            return Ok((page, object.clone()));
        }

        let header = self.object_header(object).await?;
        if header.kind != ObjectKind::FrozenMetadata
            || object.kind != ObjectKind::FrozenMetadata.as_u8()
            || address.page_kind != PageKind::GenericKeyValue
        {
            return Err(FrozenReadError::WrongPageKind);
        }
        let stored = self
            .get_exact(object, address.offset, address.stored_len)
            .await?;
        let _workspace = self
            .budget
            .reserve_decompression_workspace(address.raw_len as usize)
            .map_err(Self::budget_error)?;
        let page = Self::decode_stored_page(&address, &stored)?;
        self.cache_page(cache_key, page.clone())?;
        Ok((Arc::new(page), object.clone()))
    }

    fn decode_stored_page(
        address: &PageAddress,
        stored: &[u8],
    ) -> Result<IndexPage, FrozenReadError> {
        if stored.len() != address.stored_len as usize {
            return Err(ObjectSourceError::ShortRead {
                requested: address.stored_len as u64,
                received: stored.len() as u64,
            }
            .into());
        }
        if <[u8; 32]>::from(Sha256::digest(stored)) != address.stored_digest {
            return Err(WireError::HashMismatch {
                what: "frozen metadata page",
                stored: hex::encode(address.stored_digest),
                computed: hex::encode(Sha256::digest(stored)),
            }
            .into());
        }
        let raw = match address.codec {
            Codec::None => {
                if address.raw_len != address.stored_len {
                    return Err(WireError::invalid(
                        "frozen metadata page",
                        "codec None length mismatch",
                    )
                    .into());
                }
                stored.to_vec()
            }
            Codec::Zstd => zstd::bulk::decompress(stored, address.raw_len as usize)
                .map_err(|error| WireError::Codec(error.to_string()))?,
        };
        let page = IndexPage::decode(&raw)?;
        if page.level != address.level || page.entry_count() != address.entry_count {
            return Err(WireError::invalid("frozen metadata page", "address mismatch").into());
        }
        Ok(page)
    }

    async fn prefetch_internal_children(
        &self,
        object: &ObjectRef,
        entries: &[InternalEntry],
    ) -> Result<(), FrozenReadError> {
        let mut addresses = entries
            .iter()
            .filter_map(|entry| match &entry.child {
                ChildRef::Local(address)
                    if address.page_kind == PageKind::GenericKeyValue
                        && address.offset <= object.object_len =>
                {
                    Some(*address)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if addresses.len() < 2 {
            return Ok(());
        }
        addresses.sort_by_key(|address| (address.offset, address.stored_len));
        addresses.dedup_by_key(|address| (address.offset, address.stored_len));

        let mut start = 0;
        while start < addresses.len() {
            let mut end = start + 1;
            while end < addresses.len() {
                let range_end = addresses[end]
                    .offset
                    .checked_add(u64::from(addresses[end].stored_len))
                    .ok_or(FrozenReadError::PageOutOfBounds)?;
                let range_len = range_end.saturating_sub(addresses[start].offset);
                if range_len > MAX_COALESCED_METADATA_RANGE {
                    break;
                }
                end += 1;
            }
            self.fetch_page_group(object, &addresses[start..end])
                .await?;
            start = end;
        }
        Ok(())
    }

    async fn fetch_page_group(
        &self,
        object: &ObjectRef,
        addresses: &[PageAddress],
    ) -> Result<(), FrozenReadError> {
        if addresses.is_empty() {
            return Ok(());
        }
        let header = self.object_header(object).await?;
        if header.kind != ObjectKind::FrozenMetadata
            || object.kind != ObjectKind::FrozenMetadata.as_u8()
            || addresses
                .iter()
                .any(|address| address.page_kind != PageKind::GenericKeyValue)
        {
            return Err(FrozenReadError::WrongPageKind);
        }

        let mut missing = Vec::new();
        let mut guards = Vec::new();
        for address in addresses {
            let cache_key = decoded_page_key(object, address);
            if self.page_cache.get_shared(&cache_key).is_some() {
                continue;
            }
            let flight = self
                .page_flights
                .entry(cache_key)
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone();
            let guard = flight.lock_owned().await;
            if self.page_cache.get_shared(&cache_key).is_none() {
                missing.push((cache_key, *address));
            }
            guards.push(guard);
        }
        if missing.is_empty() {
            return Ok(());
        }

        let first = missing
            .iter()
            .map(|(_, address)| address.offset)
            .min()
            .ok_or(FrozenReadError::PageOutOfBounds)?;
        let last = missing
            .iter()
            .map(|(_, address)| address.offset.checked_add(u64::from(address.stored_len)))
            .collect::<Option<Vec<_>>>()
            .ok_or(FrozenReadError::PageOutOfBounds)?
            .into_iter()
            .max()
            .ok_or(FrozenReadError::PageOutOfBounds)?;
        let range_len = last
            .checked_sub(first)
            .ok_or(FrozenReadError::PageOutOfBounds)?;
        let range_len_u32 =
            u32::try_from(range_len).map_err(|_| FrozenReadError::PageOutOfBounds)?;
        if range_len > MAX_COALESCED_METADATA_RANGE {
            return Err(FrozenReadError::PageOutOfBounds);
        }
        let stored = self.get_exact(object, first, range_len_u32).await?;
        for (cache_key, address) in missing {
            let start = usize::try_from(address.offset.saturating_sub(first))
                .map_err(|_| FrozenReadError::PageOutOfBounds)?;
            let end = start
                .checked_add(address.stored_len as usize)
                .ok_or(FrozenReadError::PageOutOfBounds)?;
            let _workspace = self
                .budget
                .reserve_decompression_workspace(address.raw_len as usize)
                .map_err(Self::budget_error)?;
            let page = Self::decode_stored_page(&address, &stored[start..end])?;
            self.cache_page(cache_key, page)?;
        }
        drop(guards);
        Ok(())
    }

    async fn lookup_root(
        &self,
        root: Option<&RootRef>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, FrozenReadError> {
        self.lookup_root_with_prefetch(root, key, false).await
    }

    async fn lookup_root_with_prefetch(
        &self,
        root: Option<&RootRef>,
        key: &[u8],
        prefetch_siblings: bool,
    ) -> Result<Option<Vec<u8>>, FrozenReadError> {
        let Some(root) = root else {
            return Ok(None);
        };
        let mut child = ChildRef::External(root.clone());
        let mut current_object = root.object.clone();
        for depth in 0..=MAX_INDEX_LEVEL {
            let (page, object) = self.read_page(&current_object, &child).await?;
            current_object = object;
            match &page.body {
                PageBody::Leaf(entries) => {
                    let at = entries.partition_point(|entry| entry.key.as_slice() < key);
                    return Ok(entries
                        .get(at)
                        .filter(|entry| entry.key.as_slice() == key)
                        .map(|entry| entry.value.clone()));
                }
                PageBody::Internal(entries) => {
                    if prefetch_siblings {
                        self.prefetch_internal_children(&current_object, &entries)
                            .await?;
                    }
                    child = entries
                        .into_iter()
                        .find(|entry| {
                            entry.min_key.as_slice() <= key && key <= entry.max_key.as_slice()
                        })
                        .map(|entry| entry.child.clone())
                        .unwrap_or_else(|| ChildRef::External(root.clone()));
                    if matches!(child, ChildRef::External(ref fallback) if fallback == root) {
                        return Ok(None);
                    }
                }
            }
            if depth == MAX_INDEX_LEVEL {
                return Err(FrozenReadError::IndexTooDeep(MAX_INDEX_LEVEL));
            }
        }
        unreachable!()
    }

    async fn scan_namespace_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FrozenReadError> {
        let Some(root) = self.manifest.namespace_root.as_ref() else {
            return Ok(Vec::new());
        };
        let mut rows = Vec::new();
        let mut current_object = root.object.clone();
        let mut child = ChildRef::External(root.clone());
        let mut stack = Vec::new();

        loop {
            let (page, object) = self.read_page(&current_object, &child).await?;
            match &page.body {
                PageBody::Internal(entries) => {
                    self.prefetch_internal_children(&object, entries).await?;
                    let selected = entries
                        .iter()
                        .position(|entry| entry.max_key.as_slice() >= prefix)
                        .unwrap_or(entries.len().saturating_sub(1));
                    let next_child = entries
                        .get(selected)
                        .ok_or(FrozenReadError::PageOutOfBounds)?
                        .child
                        .clone();
                    current_object = object.clone();
                    stack.push((object, entries.clone(), selected));
                    child = next_child;
                }
                PageBody::Leaf(entries) => {
                    for entry in entries
                        .iter()
                        .skip_while(|entry| entry.key.as_slice() < prefix)
                    {
                        if !entry.key.starts_with(prefix) {
                            return Ok(rows);
                        }
                        rows.push((entry.key.clone(), entry.value.clone()));
                    }

                    let mut next = None;
                    while let Some((container, entries, selected)) = stack.last_mut() {
                        if *selected + 1 < entries.len() {
                            *selected += 1;
                            next = Some((container.clone(), entries[*selected].child.clone()));
                            break;
                        }
                        stack.pop();
                    }
                    let Some((container, next_child)) = next else {
                        break;
                    };
                    current_object = container;
                    child = next_child;
                }
            }
        }
        Ok(rows)
    }

    fn sum_subtree_counts(entries: &[InternalEntry]) -> Result<u64, FrozenReadError> {
        entries.iter().try_fold(0u64, |total, entry| {
            total.checked_add(entry.subtree_count).ok_or_else(|| {
                FrozenReadError::Wire(WireError::invalid(
                    "frozen namespace index",
                    "subtree count overflows",
                ))
            })
        })
    }

    /// Return the number of namespace records whose key is strictly below
    /// `key`, using authenticated subtree counts. `None` means this is a
    /// legacy index without rank counts and the caller should use the
    /// compatibility cursor walk.
    async fn namespace_rank_lower_bound(
        &self,
        root: &RootRef,
        key: &[u8],
    ) -> Result<Option<u64>, FrozenReadError> {
        let mut current_object = root.object.clone();
        let mut child = ChildRef::External(root.clone());
        let mut rank = 0u64;

        for depth in 0..=MAX_INDEX_LEVEL {
            let (page, object) = self.read_page(&current_object, &child).await?;
            match &page.body {
                PageBody::Leaf(entries) => {
                    let local = entries.partition_point(|entry| entry.key.as_slice() < key);
                    return rank.checked_add(local as u64).map(Some).ok_or_else(|| {
                        FrozenReadError::Wire(WireError::invalid(
                            "frozen namespace index",
                            "rank overflows",
                        ))
                    });
                }
                PageBody::Internal(entries) => {
                    if entries.is_empty() {
                        return Err(FrozenReadError::PageOutOfBounds);
                    }
                    if entries.iter().any(|entry| entry.subtree_count == 0) {
                        return Ok(None);
                    }
                    let selected = entries
                        .iter()
                        .position(|entry| entry.max_key.as_slice() >= key);
                    let Some(selected) = selected else {
                        rank = rank
                            .checked_add(Self::sum_subtree_counts(entries)?)
                            .ok_or_else(|| {
                                FrozenReadError::Wire(WireError::invalid(
                                    "frozen namespace index",
                                    "rank overflows",
                                ))
                            })?;
                        return Ok(Some(rank));
                    };
                    rank = rank
                        .checked_add(Self::sum_subtree_counts(&entries[..selected])?)
                        .ok_or_else(|| {
                            FrozenReadError::Wire(WireError::invalid(
                                "frozen namespace index",
                                "rank overflows",
                            ))
                        })?;
                    child = entries[selected].child.clone();
                    current_object = object;
                }
            }
            if depth == MAX_INDEX_LEVEL {
                return Err(FrozenReadError::IndexTooDeep(MAX_INDEX_LEVEL));
            }
        }
        unreachable!()
    }

    /// Advance an authenticated internal-page stack to the next leaf. The
    /// stack contains the path to the current leaf; when a sibling is itself
    /// an internal subtree, descend its left edge while retaining the path so
    /// later pages remain bounded and restartable.
    async fn next_namespace_leaf(
        &self,
        stack: &mut Vec<(ObjectRef, Vec<InternalEntry>, usize)>,
    ) -> Result<Option<(Vec<LeafEntry>, ObjectRef)>, FrozenReadError> {
        loop {
            let next = match stack.last_mut() {
                Some((object, entries, selected)) if *selected + 1 < entries.len() => {
                    *selected += 1;
                    Some((object.clone(), entries[*selected].child.clone()))
                }
                Some(_) => {
                    stack.pop();
                    None
                }
                None => return Ok(None),
            };
            let Some((mut current_object, mut child)) = next else {
                continue;
            };
            loop {
                let (page, object) = self.read_page(&current_object, &child).await?;
                match &page.body {
                    PageBody::Leaf(entries) => return Ok(Some((entries.clone(), object))),
                    PageBody::Internal(entries) => {
                        if entries.is_empty()
                            || entries.iter().any(|entry| entry.subtree_count == 0)
                        {
                            return Err(FrozenReadError::Wire(WireError::invalid(
                                "frozen namespace index",
                                "ranked page has an uncounted internal subtree",
                            )));
                        }
                        child = entries[0].child.clone();
                        current_object = object.clone();
                        stack.push((object, entries.clone(), 0));
                    }
                }
            }
        }
    }

    /// Scan a bounded namespace page using rank/select when the immutable
    /// index carries subtree counts. This path visits only the root-to-leaf
    /// route plus enough adjacent leaves to fill the requested page.
    async fn scan_namespace_page_ranked(
        &self,
        prefix: &[u8],
        child_offset: u64,
        limit: usize,
    ) -> Result<Option<Vec<(Vec<u8>, Vec<u8>)>>, FrozenReadError> {
        if limit == 0 {
            return Ok(Some(Vec::new()));
        }
        let Some(root) = self.manifest.namespace_root.as_ref() else {
            return Ok(Some(Vec::new()));
        };
        let Some(prefix_rank) = self.namespace_rank_lower_bound(root, prefix).await? else {
            return Ok(None);
        };
        let target_rank = prefix_rank.checked_add(child_offset).ok_or_else(|| {
            FrozenReadError::Wire(WireError::invalid(
                "frozen namespace index",
                "directory cursor overflows",
            ))
        })?;

        let mut current_object = root.object.clone();
        let mut child = ChildRef::External(root.clone());
        let mut remaining = target_rank;
        let mut stack = Vec::new();
        let (mut leaf, mut leaf_offset) = loop {
            let (page, object) = self.read_page(&current_object, &child).await?;
            match &page.body {
                PageBody::Leaf(entries) => {
                    let offset =
                        usize::try_from(remaining).map_err(|_| FrozenReadError::PageOutOfBounds)?;
                    if offset > entries.len() {
                        return Ok(Some(Vec::new()));
                    }
                    break (entries.clone(), offset);
                }
                PageBody::Internal(entries) => {
                    if entries.is_empty() || entries.iter().any(|entry| entry.subtree_count == 0) {
                        return Ok(None);
                    }
                    let mut selected = None;
                    for (index, entry) in entries.iter().enumerate() {
                        if remaining < entry.subtree_count {
                            selected = Some(index);
                            break;
                        }
                        remaining = remaining.saturating_sub(entry.subtree_count);
                    }
                    let Some(index) = selected else {
                        return Ok(Some(Vec::new()));
                    };
                    child = entries[index].child.clone();
                    current_object = object.clone();
                    stack.push((object, entries.clone(), index));
                }
            }
        };

        let mut rows = Vec::with_capacity(limit);
        loop {
            for entry in leaf.drain(leaf_offset..) {
                if !entry.key.starts_with(prefix) {
                    return Ok(Some(rows));
                }
                rows.push((entry.key, entry.value));
                if rows.len() == limit {
                    return Ok(Some(rows));
                }
            }
            let Some((next, _object)) = self.next_namespace_leaf(&mut stack).await? else {
                return Ok(Some(rows));
            };
            leaf = next;
            leaf_offset = 0;
        }
    }

    /// Scan a bounded namespace page. Counted v2 indexes use rank/select;
    /// legacy indexes retain the old prefix walk for compatibility.
    async fn scan_namespace_page(
        &self,
        prefix: &[u8],
        child_offset: u64,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FrozenReadError> {
        if let Some(rows) = self
            .scan_namespace_page_ranked(prefix, child_offset, limit)
            .await?
        {
            return Ok(rows);
        }
        self.scan_namespace_page_legacy(prefix, child_offset, limit)
            .await
    }

    /// Compatibility scan for v1/legacy indexes without subtree counts.
    async fn scan_namespace_page_legacy(
        &self,
        prefix: &[u8],
        child_offset: u64,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FrozenReadError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let Some(root) = self.manifest.namespace_root.as_ref() else {
            return Ok(Vec::new());
        };
        let mut rows = Vec::with_capacity(limit);
        let mut skipped = 0u64;
        let mut current_object = root.object.clone();
        let mut child = ChildRef::External(root.clone());
        let mut stack = Vec::new();

        loop {
            let (page, object) = self.read_page(&current_object, &child).await?;
            match &page.body {
                PageBody::Internal(entries) => {
                    let selected = entries
                        .iter()
                        .position(|entry| entry.max_key.as_slice() >= prefix)
                        .unwrap_or(entries.len().saturating_sub(1));
                    let next_child = entries
                        .get(selected)
                        .ok_or(FrozenReadError::PageOutOfBounds)?
                        .child
                        .clone();
                    current_object = object.clone();
                    stack.push((object, entries.clone(), selected));
                    child = next_child;
                }
                PageBody::Leaf(entries) => {
                    for entry in entries
                        .iter()
                        .skip_while(|entry| entry.key.as_slice() < prefix)
                    {
                        if !entry.key.starts_with(prefix) {
                            return Ok(rows);
                        }
                        if skipped < child_offset {
                            skipped += 1;
                            continue;
                        }
                        rows.push((entry.key.clone(), entry.value.clone()));
                        if rows.len() == limit {
                            return Ok(rows);
                        }
                    }

                    let mut next = None;
                    while let Some((container, entries, selected)) = stack.last_mut() {
                        if *selected + 1 < entries.len() {
                            *selected += 1;
                            next = Some((container.clone(), entries[*selected].child.clone()));
                            break;
                        }
                        stack.pop();
                    }
                    let Some((container, next_child)) = next else {
                        break;
                    };
                    current_object = container;
                    child = next_child;
                }
            }
        }
        Ok(rows)
    }

    /// Scan one contiguous data-table prefix in key order. Extent queries are
    /// normally for one inode/chunk, so walking the leaf chain once is much
    /// cheaper than performing a new root-to-leaf lower-bound lookup for every
    /// extent row.
    async fn scan_data_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FrozenReadError> {
        let mut rows = Vec::new();
        let mut current_object = self.manifest.data_root.object.clone();
        let mut child = ChildRef::External(self.manifest.data_root.clone());
        let mut stack = Vec::new();

        loop {
            let (page, object) = self.read_page(&current_object, &child).await?;
            match &page.body {
                PageBody::Internal(entries) => {
                    self.prefetch_internal_children(&object, entries).await?;
                    let selected = entries
                        .iter()
                        .position(|entry| entry.max_key.as_slice() >= prefix)
                        .unwrap_or(entries.len().saturating_sub(1));
                    let next_child = entries
                        .get(selected)
                        .ok_or(FrozenReadError::PageOutOfBounds)?
                        .child
                        .clone();
                    current_object = object.clone();
                    stack.push((object, entries.clone(), selected));
                    child = next_child;
                }
                PageBody::Leaf(entries) => {
                    for entry in entries
                        .iter()
                        .skip_while(|entry| entry.key.as_slice() < prefix)
                    {
                        if !entry.key.starts_with(prefix) {
                            return Ok(rows);
                        }
                        rows.push((entry.key.clone(), entry.value.clone()));
                    }

                    let mut next = None;
                    while let Some((container, entries, selected)) = stack.last_mut() {
                        if *selected + 1 < entries.len() {
                            *selected += 1;
                            next = Some((container.clone(), entries[*selected].child.clone()));
                            break;
                        }
                        stack.pop();
                    }
                    let Some((container, next_child)) = next else {
                        break;
                    };
                    current_object = container;
                    child = next_child;
                }
            }
        }
        Ok(rows)
    }

    async fn resolve_inode_attributes(
        &self,
        inode: u64,
        prefetch_siblings: bool,
    ) -> Result<Option<FrozenInodeRecord>, FrozenReadError> {
        let Some(root) = self.manifest.namespace_root.as_ref() else {
            return Err(FrozenReadError::Wire(WireError::invalid(
                "frozen namespace",
                "namespace root is missing",
            )));
        };
        let Some(bytes) = self
            .lookup_root_with_prefetch(Some(root), &inode_key(inode), prefetch_siblings)
            .await?
        else {
            return Ok(None);
        };
        match super::AttributePlacement::decode_row(&bytes)? {
            super::AttributePlacement::Inline(bytes) => {
                Ok(Some(super::decode_canonical_attributes(&bytes)?))
            }
            super::AttributePlacement::External(reference) => {
                let object = self
                    .object_refs
                    .get(&reference.object_id)
                    .map(|entry| entry.value().clone())
                    .ok_or(ObjectSourceError::NotFound)?;
                let bytes = self
                    .get_exact(&object, reference.offset, reference.stored_len)
                    .await?;
                Ok(Some(super::decode_canonical_attributes(&bytes)?))
            }
        }
    }

    async fn lookup_inode_for_readdir(
        &self,
        inode: u64,
    ) -> Result<Option<FrozenInodeRecord>, FrozenReadError> {
        if let Some(record) = self
            .inode_cache
            .read()
            .ok()
            .and_then(|cache| cache.get(&inode).cloned())
        {
            return Ok(Some(record));
        }
        // Readdir resolves many consecutive inode rows.  Walking every
        // internal sibling list for each row repeats the same cache probes
        // after the first lookup and can dominate a large-directory scan.
        // Namespace scanning already coalesces its sequential pages, so keep
        // attribute lookups targeted here.
        let record = self.resolve_inode_attributes(inode, false).await?;
        if let Some(record) = &record {
            self.cache_inode(inode, record.clone())?;
        }
        Ok(record)
    }

    async fn query_extent_rows(
        &self,
        inode: u64,
        chunk_index: u64,
        start: u64,
        end: u64,
    ) -> Result<Vec<super::FrozenExtent>, FrozenReadError> {
        let cache_key = (inode, chunk_index);
        if let Some(rows) = self.extent_cache.get(&cache_key) {
            return Ok(super::query_extents(
                rows.as_slice(),
                super::ExtentQuery {
                    inode,
                    chunk_index,
                    start,
                    end,
                },
            ));
        }

        let flight = self
            .extent_flights
            .entry(cache_key)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone();
        let _guard = flight.lock().await;
        if let Some(rows) = self.extent_cache.get(&cache_key) {
            return Ok(super::query_extents(
                rows.as_slice(),
                super::ExtentQuery {
                    inode,
                    chunk_index,
                    start,
                    end,
                },
            ));
        }

        // Scan the prefix once. The previous implementation called
        // lower_bound_data for every extent, repeating the B-tree root-to-leaf
        // traversal and the associated page probes.
        let prefix = super::extent_prefix(inode, chunk_index);
        let rows = self.scan_data_prefix(&prefix).await?;
        let mut extents = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            if key.len() != prefix.len() + 8 {
                return Err(WireError::invalid("frozen extent key", "invalid key length").into());
            }
            let offset = u64::from_be_bytes(key[prefix.len()..].try_into().unwrap());
            let length = super::decode_extent_slice_value(&value)?.0;
            extents.push(super::FrozenExtent {
                inode,
                chunk_index,
                offset,
                length,
                value,
            });
        }
        let extents = Arc::new(extents);
        self.cache_extent_rows(cache_key, extents.clone())?;
        Ok(super::query_extents(
            extents.as_slice(),
            super::ExtentQuery {
                inode,
                chunk_index,
                start,
                end,
            },
        ))
    }
}

#[async_trait]
impl<B> FrozenCatalog for StreamingFrozenMetadataCatalog<B>
where
    B: ObjectBackend + Clone + 'static,
{
    fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    async fn lookup_inode(&self, inode: u64) -> Result<Option<FrozenInodeRecord>, FrozenReadError> {
        if let Some(record) = self
            .inode_cache
            .read()
            .ok()
            .and_then(|cache| cache.get(&inode).cloned())
        {
            return Ok(Some(record));
        }
        let record = self.resolve_inode_attributes(inode, false).await?;
        if let Some(record) = &record {
            self.cache_inode(inode, record.clone())?;
        }
        Ok(record)
    }

    async fn lookup_dentry(
        &self,
        parent: u64,
        name: &[u8],
    ) -> Result<Option<(u64, FrozenInodeRecord)>, FrozenReadError> {
        let Some(value) = self
            .lookup_root(
                self.manifest.namespace_root.as_ref(),
                &super::dentry_key(parent, name),
            )
            .await?
        else {
            return Ok(None);
        };
        let inode = decode_inode_ref(&value)?;
        let Some(attr) = self.lookup_inode(inode).await? else {
            return Err(FrozenReadError::Wire(WireError::invalid(
                "frozen dentry",
                "dentry references a missing inode",
            )));
        };
        Ok(Some((inode, attr)))
    }

    async fn readdir(&self, parent: u64) -> Result<Vec<FrozenDirectoryEntry>, FrozenReadError> {
        let prefix = super::dentry_prefix(parent);
        let rows = self.scan_namespace_prefix(&prefix).await?;
        let mut entries = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            if key.len() <= prefix.len() {
                return Err(FrozenReadError::Wire(WireError::invalid(
                    "frozen dentry",
                    "dentry key has an empty name",
                )));
            }
            let inode = decode_inode_ref(&value)?;
            let Some(attr) = self.lookup_inode_for_readdir(inode).await? else {
                return Err(FrozenReadError::Wire(WireError::invalid(
                    "frozen dentry",
                    "dentry references a missing inode",
                )));
            };
            entries.push(FrozenDirectoryEntry {
                name: key[prefix.len()..].to_vec(),
                inode,
                attr,
            });
        }
        Ok(entries)
    }

    async fn readdir_page(
        &self,
        parent: u64,
        child_offset: u64,
        limit: usize,
    ) -> Result<Vec<FrozenDirectoryEntry>, FrozenReadError> {
        if limit > 4096 {
            return Err(FrozenReadError::Wire(WireError::invalid(
                "frozen readdir",
                "page limit exceeds packed profile",
            )));
        }
        let prefix = super::dentry_prefix(parent);
        let rows = self
            .scan_namespace_page(&prefix, child_offset, limit)
            .await?;
        let output_bytes = rows.iter().fold(0usize, |total, (key, value)| {
            total
                .saturating_add(std::mem::size_of::<FrozenDirectoryEntry>())
                .saturating_add(key.len().saturating_sub(prefix.len()))
                .saturating_add(value.len())
        });
        let _output = self
            .budget
            .reserve_output_page(output_bytes.max(1))
            .map_err(Self::budget_error)?;
        let mut entries = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            if key.len() <= prefix.len() {
                return Err(FrozenReadError::Wire(WireError::invalid(
                    "frozen dentry",
                    "dentry key has an empty name",
                )));
            }
            let inode = decode_inode_ref(&value)?;
            let Some(attr) = self.lookup_inode_for_readdir(inode).await? else {
                return Err(FrozenReadError::Wire(WireError::invalid(
                    "frozen dentry",
                    "dentry references a missing inode",
                )));
            };
            entries.push(FrozenDirectoryEntry {
                name: key[prefix.len()..].to_vec(),
                inode,
                attr,
            });
        }
        Ok(entries)
    }

    async fn names_for_inode(&self, inode: u64) -> Result<Vec<(u64, Vec<u8>)>, FrozenReadError> {
        let rows = self.scan_namespace_prefix(b"d").await?;
        let mut names = Vec::new();
        let prefix_len = super::dentry_prefix(0).len();
        for (key, value) in rows {
            if key.len() <= prefix_len || key[9] != 0 {
                return Err(FrozenReadError::Wire(WireError::invalid(
                    "frozen dentry",
                    "invalid dentry key",
                )));
            }
            if decode_inode_ref(&value)? == inode {
                names.push((
                    u64::from_be_bytes(key[1..9].try_into().unwrap()),
                    key[prefix_len..].to_vec(),
                ));
            }
        }
        names.sort();
        Ok(names)
    }

    async fn readlink(&self, inode: u64) -> Result<Option<Vec<u8>>, FrozenReadError> {
        Ok(self
            .lookup_inode(inode)
            .await?
            .and_then(|attr| attr.symlink_target))
    }

    async fn query_extents(
        &self,
        inode: u64,
        chunk_index: u64,
        start: u64,
        end: u64,
    ) -> Result<Vec<super::FrozenExtent>, FrozenReadError> {
        self.query_extent_rows(inode, chunk_index, start, end).await
    }
}

#[async_trait]
impl FrozenCatalog for FrozenMetadataCatalog {
    fn manifest(&self) -> &SnapshotManifest {
        self.manifest()
    }

    async fn lookup_inode(&self, inode: u64) -> Result<Option<FrozenInodeRecord>, FrozenReadError> {
        self.lookup_inode(inode)
    }

    async fn lookup_dentry(
        &self,
        parent: u64,
        name: &[u8],
    ) -> Result<Option<(u64, FrozenInodeRecord)>, FrozenReadError> {
        self.lookup_dentry(parent, name)
    }

    async fn readdir(&self, parent: u64) -> Result<Vec<FrozenDirectoryEntry>, FrozenReadError> {
        self.readdir(parent)
    }

    async fn readdir_page(
        &self,
        parent: u64,
        child_offset: u64,
        limit: usize,
    ) -> Result<Vec<FrozenDirectoryEntry>, FrozenReadError> {
        if limit > 4096 {
            return Err(FrozenReadError::Wire(WireError::invalid(
                "frozen readdir",
                "page limit exceeds packed profile",
            )));
        }
        let entries = self.readdir(parent)?;
        let start = usize::try_from(child_offset).unwrap_or(usize::MAX);
        Ok(entries.into_iter().skip(start).take(limit).collect())
    }

    async fn names_for_inode(&self, inode: u64) -> Result<Vec<(u64, Vec<u8>)>, FrozenReadError> {
        self.names_for_inode(inode)
    }

    async fn readlink(&self, inode: u64) -> Result<Option<Vec<u8>>, FrozenReadError> {
        self.readlink(inode)
    }

    async fn query_extents(
        &self,
        inode: u64,
        chunk_index: u64,
        start: u64,
        end: u64,
    ) -> Result<Vec<super::FrozenExtent>, FrozenReadError> {
        self.query_extents(inode, chunk_index, start, end)
    }
}

fn decode_inode_ref(bytes: &[u8]) -> Result<u64, FrozenReadError> {
    let expected_len = inode_key(0).len();
    if bytes.len() != expected_len || bytes.first().copied() != Some(b'i') {
        return Err(FrozenReadError::Wire(
            crate::native_base::wire::error::WireError::invalid(
                "frozen dentry",
                "value is not a canonical inode reference",
            ),
        ));
    }
    Ok(u64::from_be_bytes(bytes[1..].try_into().unwrap()))
}

async fn fetch_object<B: ObjectBackend>(
    client: &ObjectClient<B>,
    object: &ObjectRef,
) -> Result<Vec<u8>, FrozenReadError> {
    let key = std::str::from_utf8(&object.key)
        .map_err(|_| ObjectSourceError::Backend("object key is not UTF-8".into()))?;
    let bytes = client
        .get_object(key)
        .await
        .map_err(|error| ObjectSourceError::Backend(error.to_string()))?
        .ok_or(ObjectSourceError::NotFound)?;
    verify_object_bytes(object, &bytes)?;
    Ok(bytes)
}

fn verify_object_bytes(object: &ObjectRef, bytes: &[u8]) -> Result<(), FrozenReadError> {
    if bytes.len() as u64 != object.object_len {
        return Err(ObjectSourceError::ShortRead {
            requested: object.object_len,
            received: bytes.len() as u64,
        }
        .into());
    }
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    if digest != object.full_hash {
        return Err(FrozenReadError::Wire(
            crate::native_base::wire::error::WireError::HashMismatch {
                what: "frozen object",
                stored: hex::encode(object.full_hash),
                computed: hex::encode(digest),
            },
        ));
    }
    Ok(())
}

fn decode_page(
    bytes: &[u8],
    object: &ObjectRef,
    address: &PageAddress,
) -> Result<Vec<u8>, FrozenReadError> {
    let header = ContainerHeader::parse(bytes)?;
    header.ensure_supported_features()?;
    if header.kind != ObjectKind::FrozenMetadata
        || object.kind != ObjectKind::FrozenMetadata.as_u8()
        || address.page_kind != PageKind::GenericKeyValue
    {
        return Err(FrozenReadError::WrongPageKind);
    }
    let start = usize::try_from(address.offset).map_err(|_| FrozenReadError::PageOutOfBounds)?;
    let end = start
        .checked_add(address.stored_len as usize)
        .ok_or(FrozenReadError::PageOutOfBounds)?;
    if start < HEADER_LEN || end > bytes.len().saturating_sub(FOOTER_LEN) {
        return Err(FrozenReadError::PageOutOfBounds);
    }
    let stored = &bytes[start..end];
    if <[u8; 32]>::from(Sha256::digest(stored)) != address.stored_digest {
        return Err(crate::native_base::wire::error::WireError::HashMismatch {
            what: "frozen metadata page",
            stored: hex::encode(address.stored_digest),
            computed: hex::encode(Sha256::digest(stored)),
        }
        .into());
    }
    match address.codec {
        Codec::None => {
            if address.raw_len != address.stored_len {
                return Err(FrozenReadError::PageOutOfBounds);
            }
            Ok(stored.to_vec())
        }
        Codec::Zstd => Ok(
            zstd::bulk::decompress(stored, address.raw_len as usize).map_err(|error| {
                crate::native_base::wire::error::WireError::Codec(error.to_string())
            })?,
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::cadapter::client::ObjectBackend;
    use crate::native_base::frozen::producer::{FrozenSnapshotInput, build_snapshot};
    use crate::native_base::frozen::{FrozenInodeRecord, FrozenRow, dentry_key, inode_key};

    #[derive(Clone, Default)]
    struct CountingBackend {
        state: Arc<Mutex<CountingBackendState>>,
    }

    #[derive(Default)]
    struct CountingBackendState {
        objects: HashMap<String, Vec<u8>>,
        full_gets: Vec<String>,
        ranges: Vec<(String, u64, usize)>,
    }

    impl CountingBackend {
        fn insert(&self, key: String, bytes: Vec<u8>) {
            self.state.lock().unwrap().objects.insert(key, bytes);
        }

        fn stats(&self) -> (Vec<String>, Vec<(String, u64, usize)>) {
            let state = self.state.lock().unwrap();
            (state.full_gets.clone(), state.ranges.clone())
        }
    }

    #[async_trait]
    impl ObjectBackend for CountingBackend {
        async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            self.insert(key.to_string(), data.to_vec());
            Ok(())
        }

        async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            let mut state = self.state.lock().unwrap();
            state.full_gets.push(key.to_string());
            Ok(state.objects.get(key).cloned())
        }

        async fn get_object_range(
            &self,
            key: &str,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<usize> {
            let bytes = self.state.lock().unwrap().objects.get(key).cloned();
            let Some(bytes) = bytes else {
                return Ok(0);
            };
            let start = usize::try_from(offset).unwrap();
            if start >= bytes.len() {
                return Ok(0);
            }
            let count = buf.len().min(bytes.len() - start);
            self.state
                .lock()
                .unwrap()
                .ranges
                .push((key.to_string(), offset, count));
            tokio::task::yield_now().await;
            buf[..count].copy_from_slice(&bytes[start..start + count]);
            Ok(count)
        }

        async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
            Ok(String::new())
        }

        async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
            self.state.lock().unwrap().objects.remove(key);
            Ok(())
        }
    }

    fn inode(kind: u8, mode: u32, size: u64, parent_hint: Option<u64>) -> Vec<u8> {
        FrozenInodeRecord {
            kind,
            mode,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            nlink: 1,
            size,
            atime_ns: 1,
            mtime_ns: 2,
            ctime_ns: 3,
            parent_hint,
            symlink_target: None,
        }
        .encode()
    }

    fn sample_snapshot() -> crate::native_base::frozen::producer::BuiltFrozenSnapshot {
        let snapshot = FrozenSnapshotInput {
            volume_id: [1; 16],
            storage_namespace_id: [2; 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [3; 32],
            namespace_rows: vec![
                FrozenRow {
                    key: dentry_key(1, b"file"),
                    value: inode_key(2),
                },
                FrozenRow {
                    key: inode_key(1),
                    value: inode(2, 0o040755, 0, None),
                },
                FrozenRow {
                    key: inode_key(2),
                    value: inode(1, 0o100644, 7, Some(1)),
                },
            ],
            data_rows: vec![FrozenRow {
                key: crate::native_base::frozen::extent_key(2, 0, 0),
                value: crate::native_base::frozen::encode_extent_slice_value(
                    7,
                    9,
                    crate::vfs::chunk_id_for(2, 0).unwrap(),
                    0,
                ),
            }],
            inventory_rows: vec![FrozenRow {
                key: b"i".to_vec(),
                value: b"object".to_vec(),
            }],
            file_count: 1,
            directory_count: 1,
            total_logical_bytes: 7,
            created_at_ns: 4,
            namespace_object_id: [4; 16],
            data_object_id: [5; 16],
            inventory_object_id: [6; 16],
            manifest_object_id: [7; 16],
        };
        build_snapshot(snapshot).unwrap()
    }

    fn load_snapshot(backend: &CountingBackend) -> String {
        let snapshot = sample_snapshot();
        for (object, bytes) in &snapshot.metadata {
            backend.insert(
                String::from_utf8(object.key.clone()).unwrap(),
                bytes.clone(),
            );
        }
        let manifest_key = String::from_utf8(snapshot.manifest_object.key.clone()).unwrap();
        backend.insert(manifest_key.clone(), snapshot.manifest_bytes);
        manifest_key
    }

    #[test]
    fn cached_source_returns_exact_ranges_and_rejects_short_reads() {
        let object = [7u8; 16];
        let mut source = CachedObjectSource::default();
        source.insert(object, b"0123456789".to_vec());
        assert_eq!(source.get_range(&object, 2, 7).unwrap(), b"23456");
        assert!(matches!(
            source.get_range(&object, 7, 11),
            Err(ObjectSourceError::ShortRead { .. })
        ));
        assert!(matches!(
            source.get_range(&[8u8; 16], 0, 1),
            Err(ObjectSourceError::NotFound)
        ));
    }

    #[test]
    fn dentry_values_must_use_the_canonical_inode_reference() {
        assert_eq!(decode_inode_ref(&inode_key(42)).unwrap(), 42);
        assert!(decode_inode_ref(b"x\0\0\0\0\0\0\0\0").is_err());
        assert!(decode_inode_ref(b"i\0\0").is_err());
    }

    #[tokio::test]
    async fn streaming_catalog_fetches_manifest_then_page_ranges_only() {
        let backend = CountingBackend::default();
        let manifest_key = load_snapshot(&backend);
        let client = ObjectClient::new(backend.clone());
        let catalog = StreamingFrozenMetadataCatalog::open_by_key(&client, &manifest_key)
            .await
            .unwrap();

        let (full_gets, ranges) = backend.stats();
        assert_eq!(full_gets, vec![manifest_key.clone()]);
        assert!(ranges.is_empty());

        assert_eq!(
            catalog.lookup_dentry(1, b"file").await.unwrap().unwrap().0,
            2
        );
        let (full_gets, ranges) = backend.stats();
        assert_eq!(full_gets, vec![manifest_key]);
        assert_eq!(ranges.len(), 2, "metadata header plus one page range");
        assert!(ranges.iter().all(|(key, _, _)| key.contains("frozen")));
        assert!(ranges.iter().any(|(_, offset, _)| *offset == 0));
        assert!(ranges.iter().any(|(_, offset, _)| *offset > 0));
        let budget = catalog.metadata_budget_snapshot();
        assert!(budget.reclaimable_cache > 0);
        assert!(budget.handles_and_inodes > 0);
        assert_eq!(budget.compressed_inflight, 0);
        assert_eq!(budget.decompression_workspace, 0);

        let evicted = catalog.evict_metadata_pages(usize::MAX);
        assert!(evicted > 0);
        assert_eq!(catalog.metadata_budget_snapshot().reclaimable_cache, 0);
        assert_eq!(
            catalog.lookup_dentry(1, b"file").await.unwrap().unwrap().0,
            2
        );
        let (_, refetched_ranges) = backend.stats();
        assert!(refetched_ranges.len() > ranges.len());
    }

    #[tokio::test]
    async fn streaming_catalog_single_flights_a_shared_page() {
        let backend = CountingBackend::default();
        let manifest_key = load_snapshot(&backend);
        let client = ObjectClient::new(backend.clone());
        let catalog = Arc::new(
            StreamingFrozenMetadataCatalog::open_by_key(&client, &manifest_key)
                .await
                .unwrap(),
        );

        let first = catalog.lookup_inode(1);
        let second = catalog.lookup_inode(2);
        let (first, second) = tokio::join!(first, second);
        assert!(first.unwrap().is_some());
        assert!(second.unwrap().is_some());

        let (_, ranges) = backend.stats();
        assert_eq!(ranges.len(), 2, "one shared header and page fetch");
    }

    #[tokio::test]
    async fn streaming_catalog_reuses_extent_rows_across_queries() {
        let backend = CountingBackend::default();
        let manifest_key = load_snapshot(&backend);
        let client = ObjectClient::new(backend.clone());
        let catalog = StreamingFrozenMetadataCatalog::open_by_key(&client, &manifest_key)
            .await
            .unwrap();

        let first = catalog.query_extents(2, 0, 0, u64::MAX).await.unwrap();
        assert_eq!(first.len(), 1);
        let (_, ranges_after_first) = backend.stats();

        let second = catalog.query_extents(2, 0, 0, u64::MAX).await.unwrap();
        assert_eq!(second, first);
        let (_, ranges_after_second) = backend.stats();
        assert_eq!(ranges_after_second.len(), ranges_after_first.len());
        assert!(catalog.metadata_budget_snapshot().reclaimable_cache > 0);
    }

    #[tokio::test]
    async fn streaming_readdir_coalesces_adjacent_metadata_pages() {
        const FILES: u64 = 5_000;
        let mut namespace = vec![
            FrozenRow {
                key: inode_key(1),
                value: inode(2, 0o040755, 0, None),
            },
            FrozenRow {
                key: dentry_key(1, b"dir"),
                value: inode_key(2),
            },
            FrozenRow {
                key: inode_key(2),
                value: inode(2, 0o040755, 0, Some(1)),
            },
        ];
        for index in 0..FILES {
            let file_inode = index + 3;
            namespace.push(FrozenRow {
                key: dentry_key(2, format!("f{index:05}").as_bytes()),
                value: inode_key(file_inode),
            });
            namespace.push(FrozenRow {
                key: inode_key(file_inode),
                value: inode(1, 0o100644, 1, Some(2)),
            });
        }
        namespace.sort_by(|left, right| left.key.cmp(&right.key));
        let snapshot = build_snapshot(FrozenSnapshotInput {
            volume_id: [11; 16],
            storage_namespace_id: [12; 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [13; 32],
            namespace_rows: namespace,
            data_rows: vec![FrozenRow {
                key: crate::native_base::frozen::extent_key(3, 0, 0),
                value: crate::native_base::frozen::encode_extent_slice_value(
                    1,
                    9,
                    crate::vfs::chunk_id_for(3, 0).unwrap(),
                    0,
                ),
            }],
            inventory_rows: vec![FrozenRow {
                key: b"i".to_vec(),
                value: b"object".to_vec(),
            }],
            file_count: FILES,
            directory_count: 2,
            total_logical_bytes: FILES,
            created_at_ns: 0,
            namespace_object_id: [14; 16],
            data_object_id: [15; 16],
            inventory_object_id: [16; 16],
            manifest_object_id: [17; 16],
        })
        .unwrap();
        let backend = CountingBackend::default();
        for (object, bytes) in &snapshot.metadata {
            backend.insert(
                String::from_utf8(object.key.clone()).unwrap(),
                bytes.clone(),
            );
        }
        let manifest_key = String::from_utf8(snapshot.manifest_object.key.clone()).unwrap();
        backend.insert(manifest_key.clone(), snapshot.manifest_bytes);
        let catalog = StreamingFrozenMetadataCatalog::open_by_key(
            &ObjectClient::new(backend.clone()),
            &manifest_key,
        )
        .await
        .unwrap();

        let mid_page = FrozenCatalog::readdir_page(&catalog, 2, 1_234, 37)
            .await
            .unwrap();
        assert_eq!(mid_page.len(), 37);
        assert_eq!(mid_page.first().unwrap().name, b"f01234");
        assert_eq!(mid_page.last().unwrap().name, b"f01270");
        let tail_page = FrozenCatalog::readdir_page(&catalog, 2, FILES - 1, 37)
            .await
            .unwrap();
        assert_eq!(tail_page.len(), 1);
        assert_eq!(tail_page[0].name, b"f04999");

        let entries = FrozenCatalog::readdir(&catalog, 2).await.unwrap();
        assert_eq!(entries.len(), FILES as usize);
        let (_, ranges) = backend.stats();
        let namespace_ranges = ranges
            .iter()
            .filter(|(key, _, _)| key.contains("frozen"))
            .collect::<Vec<_>>();
        assert!(
            namespace_ranges.iter().any(|(_, _, len)| *len > 16 * 1024),
            "readdir should fetch at least one coalesced metadata range: {namespace_ranges:?}"
        );
        assert!(
            namespace_ranges.len() < FILES as usize / 10,
            "metadata pages should not require one GET per inode: {}",
            namespace_ranges.len()
        );
    }
}
