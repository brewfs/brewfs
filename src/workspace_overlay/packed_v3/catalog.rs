//! Read-only catalog facade for a pinned packed v3 manifest.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use moka::future::Cache;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::chunk::cache::ChunksCache;
use crate::chunk::read_plan::{
    LogicalSegment, ReadGeneration, ReadSource, UnifiedReadPlan, execute_unified_into,
};

use super::coordinator::{CoordinatorLimits, FrameReadRequest, SharedGroupReadCoordinator};
use super::group::{FRAME_RECORD_LEN, PackedFrameDescriptor};
use super::index::{PackedGroupIndexPage, PackedInodeIndexPage};
use super::layout::SizeClass;
use super::meta::{GroupMeta, GroupMetaEntry, MAX_GROUP_META_BYTES};
use super::metrics::{PackedRuntimeMetrics, PackedRuntimeMetricsSnapshot};
use super::remote::{
    MAX_PACKED_STREAM_RANGE_BYTES, PackedWindowCache, RemotePackedObject, read_exact_range,
};
use super::wire::{
    PackedGroupRef, PackedObjectKind, PackedResult, PackedSnapshotManifest, PackedWireError,
};

const MAX_GROUPS_PER_PAGE: usize = 4096;
const MAX_OPEN_CONTAINERS: u64 = 1024;
// These caches retain only immutable decoded indexes. They are deliberately
// separate from the read/payload cache so a cold payload benchmark still
// performs object reads while avoiding repeated decoding of the same routing
// page for every FUSE read request.
const INDEX_PAGE_CACHE_BYTES: u64 = 4 * 1024 * 1024;
const INDEX_PAGE_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_METADATA_CACHE_BYTES: u64 = 256 * 1024 * 1024;
const FILE_LOCATOR_CACHE_BYTES: u64 = 32 * 1024 * 1024;
const INODE_ENTRY_CACHE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_FRAME_DESCRIPTORS: u64 = 65_536;
const MAX_FRAME_DESCRIPTOR_CACHE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_FRAME_DIRECTORY_CACHE_BYTES: u64 = 8 * 1024 * 1024;
/// Keep metadata read-ahead bounded independently from payload windows. A
/// small gap is cheaper than another OSS RTT, while a large gap usually
/// means that the groups are not part of the same contiguous metadata run.
const MAX_GROUP_META_BATCH_GAP_BYTES: u64 = 64 * 1024;

fn projected_metadata_page_bytes(encoded_bytes: u64) -> u64 {
    // Decoding owns Vec/Arc/name storage in addition to the authenticated
    // wire bytes.  Keep the warm-up estimate conservative so Moka does not
    // immediately evict the pages that were just fetched.
    encoded_bytes.saturating_mul(2).max(1)
}

fn select_metadata_pages(lengths: impl IntoIterator<Item = u64>, budget: u64) -> Vec<usize> {
    if budget == 0 {
        return Vec::new();
    }
    let mut remaining = budget;
    let mut selected = Vec::new();
    for (ordinal, encoded_bytes) in lengths.into_iter().enumerate() {
        let projected = projected_metadata_page_bytes(encoded_bytes);
        if projected > remaining {
            // Keep the selection a stable prefix. This matters for directory
            // scans: a scanner should warm the same early pages on every
            // mount instead of warming a later page after a large outlier.
            break;
        }
        remaining = remaining.saturating_sub(projected);
        selected.push(ordinal);
    }
    selected
}

/// Return the number of bounded object ranges and descriptor bytes needed by
/// `RemotePackedObject::read_frame_descriptors`.  The method always performs
/// one 24-byte prefix probe and then merges only contiguous requested ordinal
/// runs. Keeping this accounting here makes partial descriptor reads visible
/// separately from an intentional full frame-directory warm-up.
fn frame_descriptor_range_stats(ordinals: &[u32]) -> (u64, u64) {
    if ordinals.is_empty() {
        return (0, 0);
    }
    let max_records =
        (super::remote::MAX_PACKED_STREAM_RANGE_BYTES / FRAME_RECORD_LEN as u64).max(1);
    let mut ranges = 1u64; // frame-count/table-offset prefix
    let mut bytes = 24u64;
    let mut run_len = 1u64;
    let mut previous = ordinals[0];

    let finish_run = |run_len: u64, ranges: &mut u64, bytes: &mut u64| {
        *ranges = ranges.saturating_add(run_len.div_ceil(max_records));
        *bytes = bytes.saturating_add(run_len.saturating_mul(FRAME_RECORD_LEN as u64));
    };
    for ordinal in ordinals.iter().copied().skip(1) {
        if ordinal == previous.saturating_add(1) {
            run_len = run_len.saturating_add(1);
        } else {
            finish_run(run_len, &mut ranges, &mut bytes);
            run_len = 1;
        }
        previous = ordinal;
    }
    finish_run(run_len, &mut ranges, &mut bytes);
    (ranges, bytes)
}

#[derive(Clone, Debug)]
pub(crate) struct PackedFileLocator {
    pub(crate) group: PackedGroupRef,
    pub(crate) entry: GroupMetaEntry,
}

impl PackedFileLocator {
    fn decoded_weight_for(group: &PackedGroupRef, entry: &GroupMetaEntry) -> u64 {
        let bytes = 160usize
            .saturating_add(group.first_name.len())
            .saturating_add(group.last_name.len())
            .saturating_add(entry.name.len())
            .saturating_add(entry.inline_data.len())
            .saturating_add(entry.extents.len().saturating_mul(28));
        u64::try_from(bytes).unwrap_or(u64::MAX)
    }

    fn decoded_weight(&self) -> u32 {
        u32::try_from(Self::decoded_weight_for(&self.group, &self.entry).min(u64::from(u32::MAX)))
            .unwrap_or(u32::MAX)
    }
}

/// Counters and current retained sizes for the immutable metadata caches.
/// These are deliberately separate from payload/frame-window statistics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PackedMetadataCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub index_hits: u64,
    pub index_misses: u64,
    pub inode_entry_hits: u64,
    pub inode_entry_misses: u64,
    pub group_meta_hits: u64,
    pub group_meta_misses: u64,
    pub locator_hits: u64,
    pub locator_misses: u64,
    pub group_index_remote_gets: u64,
    pub group_index_remote_bytes: u64,
    pub inode_index_remote_gets: u64,
    pub inode_index_remote_bytes: u64,
    pub group_meta_remote_gets: u64,
    pub group_meta_remote_bytes: u64,
    pub frame_directory_remote_gets: u64,
    pub frame_directory_remote_bytes: u64,
    /// Ranges/bytes for demand-loaded descriptor runs. Full directory
    /// prefetch is reported by the fields above; keeping these separate shows
    /// whether a cold read paid for only the referenced frame records.
    pub frame_descriptor_remote_gets: u64,
    pub frame_descriptor_remote_bytes: u64,
    pub group_index_entries: u64,
    pub group_index_bytes: u64,
    pub inode_index_entries: u64,
    pub inode_index_bytes: u64,
    pub inode_entry_entries: u64,
    pub inode_entry_bytes: u64,
    pub group_meta_entries: u64,
    pub group_meta_bytes: u64,
    pub frame_directory_entries: u64,
    pub frame_directory_bytes: u64,
    pub frame_descriptor_entries: u64,
    pub frame_descriptor_bytes: u64,
    pub file_locator_entries: u64,
    pub file_locator_bytes: u64,
}

/// Work performed by the optional mount-time metadata warm-up.  These counts
/// describe decoded pages admitted to the in-process caches; they never imply
/// that payload frames or the persistent data cache were touched.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PackedMetadataWarmupStats {
    pub inode_index_pages: u64,
    pub inode_entries: u64,
    pub inode_entry_bytes: u64,
    pub inode_entries_skipped: u64,
    pub group_index_pages: u64,
    pub group_meta_pages: u64,
    pub frame_directory_pages: u64,
    pub index_budget_bytes: u64,
    pub group_meta_budget_bytes: u64,
    pub frame_directory_budget_bytes: u64,
    pub index_bytes_estimated: u64,
    pub inode_index_pages_skipped: u64,
    pub group_index_pages_skipped: u64,
    /// Encoded GroupMeta bytes observed while deciding whether an adaptive
    /// warm-up can fit the decoded metadata budget.
    pub group_meta_bytes_estimated: u64,
    /// GroupMeta pages intentionally left to demand loading because the
    /// complete set could not fit in the configured budget.
    pub group_meta_pages_skipped: u64,
    pub frame_directory_pages_skipped: u64,
    pub frame_directory_bytes_estimated: u64,
    /// Locator entries admitted while warming decoded GroupMeta. These are
    /// bounded by the locator share of the metadata byte budget and avoid a
    /// second inode-page/name lookup on the first read of a warmed file.
    pub locator_entries: u64,
    pub locator_bytes: u64,
    pub locator_entries_skipped: u64,
}

/// One bounded same-container range used by metadata warm-up. The individual
/// GroupMeta blocks remain independently authenticated and are decoded from
/// their exact slices after the merged range arrives.
struct GroupMetaBatchRange {
    container_ordinal: u32,
    offset: u64,
    length: u64,
    groups: Vec<PackedGroupRef>,
}

fn group_meta_cache(max_bytes: u64) -> Cache<u64, Arc<GroupMeta>> {
    Cache::builder()
        .max_capacity(max_bytes.max(1))
        .weigher(|_key: &u64, value: &Arc<GroupMeta>| value.decoded_weight())
        .build()
}

fn file_locator_cache(max_bytes: u64) -> Cache<u64, Arc<PackedFileLocator>> {
    Cache::builder()
        .max_capacity(max_bytes.max(1))
        .weigher(|_key: &u64, value: &Arc<PackedFileLocator>| value.decoded_weight())
        .build()
}

fn inode_entry_cache(max_bytes: u64) -> Cache<u64, Arc<super::index::PackedInodeIndexEntry>> {
    Cache::builder()
        .max_capacity(max_bytes.max(1))
        .weigher(
            |_key: &u64, value: &Arc<super::index::PackedInodeIndexEntry>| {
                u32::try_from(inode_entry_decoded_weight(value).min(u64::from(u32::MAX)))
                    .unwrap_or(u32::MAX)
            },
        )
        .build()
}

fn inode_entry_decoded_weight(entry: &super::index::PackedInodeIndexEntry) -> u64 {
    // II05 entries contain fixed-size attributes plus one owned filename. Keep
    // the same conservative estimate for cache admission and warm-up planning
    // so a warm mount cannot immediately evict the records it just fetched.
    128u64.saturating_add(entry.name.len() as u64)
}

fn group_index_page_cache(max_bytes: u64) -> Cache<usize, Arc<PackedGroupIndexPage>> {
    Cache::builder()
        .max_capacity(max_bytes.max(1))
        .weigher(|_key: &usize, value: &Arc<PackedGroupIndexPage>| {
            let bytes = value.groups.iter().fold(64usize, |total, group| {
                total
                    .saturating_add(160)
                    .saturating_add(group.first_name.len())
                    .saturating_add(group.last_name.len())
            });
            u32::try_from(bytes.min(u32::MAX as usize)).unwrap_or(u32::MAX)
        })
        .build()
}

fn inode_index_page_cache(max_bytes: u64) -> Cache<usize, Arc<PackedInodeIndexPage>> {
    Cache::builder()
        .max_capacity(max_bytes.max(1))
        .weigher(|_key: &usize, value: &Arc<PackedInodeIndexPage>| {
            let bytes = value.entries.iter().fold(64usize, |total, entry| {
                total.saturating_add(128).saturating_add(entry.name.len())
            });
            u32::try_from(bytes.min(u32::MAX as usize)).unwrap_or(u32::MAX)
        })
        .build()
}

fn frame_directory_cache(max_bytes: u64) -> Cache<u32, Arc<Vec<PackedFrameDescriptor>>> {
    Cache::builder()
        .max_capacity(max_bytes.max(1))
        .weigher(|_key: &u32, value: &Arc<Vec<PackedFrameDescriptor>>| {
            value
                .len()
                .saturating_mul(std::mem::size_of::<PackedFrameDescriptor>())
                .min(u32::MAX as usize) as u32
        })
        .build()
}

fn frame_descriptor_cache(max_bytes: u64) -> Cache<(u32, u32), PackedFrameDescriptor> {
    Cache::builder()
        .max_capacity(max_bytes.max(1))
        .weigher(|_key: &(u32, u32), _value: &PackedFrameDescriptor| {
            u32::try_from(std::mem::size_of::<PackedFrameDescriptor>()).unwrap_or(u32::MAX)
        })
        .build()
}

fn decoded_frame_cache(
    max_bytes: u64,
    metrics: Arc<PackedRuntimeMetrics>,
) -> Cache<(u32, u32), Bytes> {
    Cache::builder()
        .max_capacity(max_bytes.max(1))
        .weigher(|_key: &(u32, u32), value: &Bytes| value.len().min(u32::MAX as usize) as u32)
        .eviction_listener(move |_key, _value, cause| {
            if cause.was_evicted() {
                metrics.record_decoded_frame_cache_eviction();
            }
        })
        .build()
}

/// Derive the stable namespace key used by a child directory group.  The
/// root key is stored explicitly in the manifest; every other directory key
/// is derived from the immutable snapshot and inode, so a dentry lookup never
/// needs a second mutable namespace table.
pub fn directory_key(snapshot_id: [u8; 32], inode: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"BrewFS-packed-v3-directory\0");
    hasher.update(snapshot_id);
    hasher.update(inode.to_le_bytes());
    hasher.finalize().into()
}

/// A catalog keeps immutable decoded metadata in a byte-budgeted cache.  The
/// internal read path shares each cached page through `Arc`, so a cache hit
/// does not clone every inline payload before a single entry is selected.
#[derive(Clone)]
pub struct RemoteGroupCatalog<B: ObjectBackend + Clone> {
    client: ObjectClient<B>,
    manifest: Arc<PackedSnapshotManifest>,
    group_indexes: Arc<HashMap<u64, usize>>,
    parent_indexes: Arc<HashMap<[u8; 32], Vec<usize>>>,
    /// Immutable container headers are safe to share across requests. Keeping
    /// the opened object here avoids a header range GET for each GroupMeta or
    /// frame-descriptor lookup without retaining decoded metadata or payload.
    containers: Cache<u32, Arc<RemotePackedObject<B>>>,
    group_index_pages: Cache<usize, Arc<PackedGroupIndexPage>>,
    inode_index_pages: Cache<usize, Arc<PackedInodeIndexPage>>,
    /// Hot II05 records avoid repeatedly scanning a decoded 4096-entry page
    /// for stat/open/get_names calls. This is a separate bounded view so the
    /// page cache can still serve cold range lookups without pinning every
    /// inode record indefinitely.
    inode_entries: Cache<u64, Arc<super::index::PackedInodeIndexEntry>>,
    group_meta_pages: Cache<u64, Arc<GroupMeta>>,
    /// Resolved inode locators avoid cloning a GroupMeta entry on every
    /// get_slices/read/stat sequence.  The group metadata cache remains the
    /// canonical page cache; this is a bounded hot-entry view over it.
    file_locators: Cache<u64, Arc<PackedFileLocator>>,
    frame_descriptors: Cache<(u32, u32), PackedFrameDescriptor>,
    /// Optional exact decoded-frame cache for the explicitly named warm-frame
    /// profile. A zero budget disables it and preserves strict-cold semantics.
    decoded_frames: Option<Cache<(u32, u32), Bytes>>,
    decoded_frame_cache_bytes: u64,
    /// A container's frame table is tiny compared with its payload. Cache it
    /// as one immutable directory so concurrent small-file reads do not each
    /// issue a prefix probe plus a one-record range request.
    frame_directories: Cache<u32, Arc<Vec<PackedFrameDescriptor>>>,
    /// Optional whole-container cache. The cache key includes the snapshot
    /// and manifest digest, so immutable payloads can safely survive a
    /// process restart without colliding with another snapshot.
    payload_cache: Option<Arc<ChunksCache>>,
    /// Ephemeral aligned payload-window budget. This cache starts empty for
    /// every catalog and is only used to share adjacent frame reads within a
    /// mount; it is independent from the persistent whole-container cache.
    frame_window_cache_bytes: u64,
    frame_window_cache: Option<Arc<PackedWindowCache>>,
    /// Bounded read-ahead admission. Tasks are best effort and never block a
    /// foreground read once the small concurrency budget is occupied.
    frame_window_prefetch_limit: Arc<Semaphore>,
    frame_window_prefetch_enabled: bool,
    metadata_cache_enabled: bool,
    metadata_index_budget_bytes: u64,
    metadata_group_index_budget_bytes: u64,
    metadata_inode_index_budget_bytes: u64,
    metadata_group_budget_bytes: u64,
    metadata_frame_directory_budget_bytes: u64,
    metadata_inode_prefetch_remaining: Arc<AtomicU64>,
    metadata_locator_prefetch_remaining: Arc<AtomicU64>,
    metadata_cache_hits: Arc<AtomicU64>,
    metadata_cache_misses: Arc<AtomicU64>,
    index_cache_hits: Arc<AtomicU64>,
    index_cache_misses: Arc<AtomicU64>,
    inode_entry_cache_hits: Arc<AtomicU64>,
    inode_entry_cache_misses: Arc<AtomicU64>,
    group_meta_cache_hits: Arc<AtomicU64>,
    group_meta_cache_misses: Arc<AtomicU64>,
    locator_cache_hits: Arc<AtomicU64>,
    locator_cache_misses: Arc<AtomicU64>,
    group_index_remote_gets: Arc<AtomicU64>,
    group_index_remote_bytes: Arc<AtomicU64>,
    inode_index_remote_gets: Arc<AtomicU64>,
    inode_index_remote_bytes: Arc<AtomicU64>,
    group_meta_remote_gets: Arc<AtomicU64>,
    group_meta_remote_bytes: Arc<AtomicU64>,
    frame_directory_remote_gets: Arc<AtomicU64>,
    frame_directory_remote_bytes: Arc<AtomicU64>,
    frame_descriptor_remote_gets: Arc<AtomicU64>,
    frame_descriptor_remote_bytes: Arc<AtomicU64>,
    /// Mount-scoped demand coalescing. The coordinator only retains pending
    /// requests for its short collection tick; decoded payloads are returned
    /// directly to the waiting FUSE calls and are not cached here.
    read_coordinator: Arc<SharedGroupReadCoordinator<B>>,
    runtime_metrics: Arc<PackedRuntimeMetrics>,
}

impl<B: ObjectBackend + Clone + 'static> RemoteGroupCatalog<B> {
    pub fn new(client: ObjectClient<B>, manifest: PackedSnapshotManifest) -> Self {
        let mut group_indexes = HashMap::with_capacity(manifest.groups.len());
        let mut parent_indexes: HashMap<[u8; 32], Vec<usize>> = HashMap::new();
        for (index, group) in manifest.groups.iter().enumerate() {
            group_indexes.entry(group.group_id).or_insert(index);
            parent_indexes
                .entry(group.parent_dir_key)
                .or_default()
                .push(index);
        }
        for indexes in parent_indexes.values_mut() {
            indexes.sort_by(|left, right| {
                let left = &manifest.groups[*left];
                let right = &manifest.groups[*right];
                left.first_name
                    .cmp(&right.first_name)
                    .then_with(|| left.group_id.cmp(&right.group_id))
            });
        }
        let runtime_metrics = Arc::new(PackedRuntimeMetrics::default());
        let catalog = Self {
            client,
            manifest: Arc::new(manifest),
            group_indexes: Arc::new(group_indexes),
            parent_indexes: Arc::new(parent_indexes),
            containers: Cache::new(MAX_OPEN_CONTAINERS),
            group_index_pages: group_index_page_cache(INDEX_PAGE_CACHE_BYTES),
            inode_index_pages: inode_index_page_cache(INDEX_PAGE_CACHE_BYTES),
            inode_entries: inode_entry_cache(INODE_ENTRY_CACHE_BYTES),
            group_meta_pages: group_meta_cache(
                DEFAULT_METADATA_CACHE_BYTES
                    .saturating_sub(FILE_LOCATOR_CACHE_BYTES)
                    .saturating_sub(INODE_ENTRY_CACHE_BYTES),
            ),
            file_locators: file_locator_cache(FILE_LOCATOR_CACHE_BYTES),
            frame_descriptors: frame_descriptor_cache(
                MAX_FRAME_DESCRIPTORS
                    .saturating_mul(std::mem::size_of::<PackedFrameDescriptor>() as u64),
            ),
            decoded_frames: None,
            decoded_frame_cache_bytes: 0,
            frame_directories: frame_directory_cache(MAX_FRAME_DIRECTORY_CACHE_BYTES),
            payload_cache: None,
            frame_window_cache_bytes: 0,
            frame_window_cache: None,
            frame_window_prefetch_limit: Arc::new(Semaphore::new(4)),
            frame_window_prefetch_enabled: false,
            metadata_cache_enabled: true,
            metadata_index_budget_bytes: 0,
            metadata_group_index_budget_bytes: 0,
            metadata_inode_index_budget_bytes: 0,
            metadata_group_budget_bytes: 0,
            metadata_frame_directory_budget_bytes: 0,
            metadata_inode_prefetch_remaining: Arc::new(AtomicU64::new(0)),
            metadata_locator_prefetch_remaining: Arc::new(AtomicU64::new(0)),
            metadata_cache_hits: Arc::new(AtomicU64::new(0)),
            metadata_cache_misses: Arc::new(AtomicU64::new(0)),
            index_cache_hits: Arc::new(AtomicU64::new(0)),
            index_cache_misses: Arc::new(AtomicU64::new(0)),
            inode_entry_cache_hits: Arc::new(AtomicU64::new(0)),
            inode_entry_cache_misses: Arc::new(AtomicU64::new(0)),
            group_meta_cache_hits: Arc::new(AtomicU64::new(0)),
            group_meta_cache_misses: Arc::new(AtomicU64::new(0)),
            locator_cache_hits: Arc::new(AtomicU64::new(0)),
            locator_cache_misses: Arc::new(AtomicU64::new(0)),
            group_index_remote_gets: Arc::new(AtomicU64::new(0)),
            group_index_remote_bytes: Arc::new(AtomicU64::new(0)),
            inode_index_remote_gets: Arc::new(AtomicU64::new(0)),
            inode_index_remote_bytes: Arc::new(AtomicU64::new(0)),
            group_meta_remote_gets: Arc::new(AtomicU64::new(0)),
            group_meta_remote_bytes: Arc::new(AtomicU64::new(0)),
            frame_directory_remote_gets: Arc::new(AtomicU64::new(0)),
            frame_directory_remote_bytes: Arc::new(AtomicU64::new(0)),
            frame_descriptor_remote_gets: Arc::new(AtomicU64::new(0)),
            frame_descriptor_remote_bytes: Arc::new(AtomicU64::new(0)),
            read_coordinator: Arc::new(
                SharedGroupReadCoordinator::new_with_metrics(
                    CoordinatorLimits::default(),
                    Arc::clone(&runtime_metrics),
                )
                .expect("default packed coordinator limits are valid"),
            ),
            runtime_metrics,
        };
        catalog.with_metadata_cache_bytes(DEFAULT_METADATA_CACHE_BYTES)
    }

    pub fn with_payload_cache(
        client: ObjectClient<B>,
        manifest: PackedSnapshotManifest,
        payload_cache: Arc<ChunksCache>,
    ) -> Self {
        let mut catalog = Self::new(client, manifest);
        catalog.payload_cache = Some(payload_cache);
        catalog
    }

    /// Set the total decoded metadata budget for this mount.  The budget is
    /// split between pageable index pages, complete GroupMeta pages, and hot
    /// inode/locator entries. Every decoded metadata cache is included in the
    /// caller-visible budget; payload/frame caches remain separate.
    pub fn with_metadata_cache_bytes(mut self, max_bytes: u64) -> Self {
        self.metadata_cache_enabled = max_bytes > 0;
        // Keep all decoded metadata under one caller-visible budget. Index
        // pages are useful for random stat/open workloads, while GroupMeta
        // remains the dominant cache, so cap the index pool at 64 MiB and
        // reserve bounded hot-entry views before giving the remainder to
        // complete GroupMeta pages.
        let locator_bytes = max_bytes.saturating_div(8).min(FILE_LOCATOR_CACHE_BYTES);
        let inode_bytes = max_bytes.saturating_div(16).min(INODE_ENTRY_CACHE_BYTES);
        let index_pool = max_bytes.saturating_div(4).min(INDEX_PAGE_CACHE_MAX_BYTES);
        let frame_descriptor_bytes = max_bytes
            .saturating_div(16)
            .min(MAX_FRAME_DESCRIPTOR_CACHE_BYTES);
        // A frame directory is immutable metadata too. Reserve a small
        // bounded tier for it so the first read from every container does not
        // pay an otherwise invisible prefix/table RTT. The cache remains
        // capped independently to keep a large snapshot pageable.
        let frame_directory_bytes = if max_bytes >= 1024 * 1024 {
            max_bytes
                .saturating_div(8)
                .min(MAX_FRAME_DIRECTORY_CACHE_BYTES)
        } else {
            // Tiny test/diagnostic budgets should still be able to retain at
            // least one GroupMeta page. A frame-directory tier is useful
            // only once the mount has a meaningful metadata budget.
            0
        };
        let remaining = max_bytes
            .saturating_sub(locator_bytes)
            .saturating_sub(inode_bytes)
            .saturating_sub(index_pool)
            .saturating_sub(frame_descriptor_bytes);
        let remaining = remaining.saturating_sub(frame_directory_bytes);
        let index_each = index_pool / 2;
        let group_bytes = remaining;
        self.group_meta_pages = group_meta_cache(group_bytes);
        self.file_locators = file_locator_cache(locator_bytes);
        self.inode_entries = inode_entry_cache(inode_bytes);
        self.group_index_pages = group_index_page_cache(index_each);
        self.inode_index_pages = inode_index_page_cache(index_pool - index_each);
        self.frame_descriptors = frame_descriptor_cache(frame_descriptor_bytes);
        self.frame_directories = frame_directory_cache(frame_directory_bytes);
        self.metadata_index_budget_bytes = index_pool;
        self.metadata_group_index_budget_bytes = index_each;
        self.metadata_inode_index_budget_bytes = index_pool - index_each;
        self.metadata_group_budget_bytes = group_bytes;
        self.metadata_frame_directory_budget_bytes = frame_directory_bytes;
        self.metadata_inode_prefetch_remaining
            .store(inode_bytes, Ordering::Relaxed);
        self.metadata_locator_prefetch_remaining
            .store(locator_bytes, Ordering::Relaxed);
        self
    }

    /// Set the exact decoded-frame budget used only by the explicit
    /// warm-frame-cache profile. A zero budget preserves strict-cold behavior.
    pub fn with_decoded_frame_cache_bytes(mut self, max_bytes: u64) -> Self {
        self.decoded_frame_cache_bytes = max_bytes;
        self.decoded_frames = (max_bytes > 0)
            .then(|| decoded_frame_cache(max_bytes, Arc::clone(&self.runtime_metrics)));
        self
    }

    /// Set the bounded in-process group-window budget used by packed payload
    /// reads. A zero budget preserves strict per-range cold-read behavior.
    pub fn with_frame_window_cache_bytes(mut self, max_bytes: u64) -> Self {
        self.frame_window_cache_bytes = max_bytes;
        self.frame_window_cache =
            (max_bytes > 0).then(|| Arc::new(PackedWindowCache::new(max_bytes)));
        self
    }

    /// Enable bounded sequential read-ahead for the ephemeral window cache.
    /// The cache budget must still be non-zero; keeping this opt-in preserves
    /// strict cold-read semantics for existing mounts and benchmarks.
    pub fn with_frame_window_prefetch(mut self, enabled: bool) -> Self {
        self.frame_window_prefetch_enabled = enabled;
        self
    }

    pub fn metadata_cache_stats(&self) -> PackedMetadataCacheStats {
        PackedMetadataCacheStats {
            hits: self.metadata_cache_hits.load(Ordering::Relaxed),
            misses: self.metadata_cache_misses.load(Ordering::Relaxed),
            index_hits: self.index_cache_hits.load(Ordering::Relaxed),
            index_misses: self.index_cache_misses.load(Ordering::Relaxed),
            inode_entry_hits: self.inode_entry_cache_hits.load(Ordering::Relaxed),
            inode_entry_misses: self.inode_entry_cache_misses.load(Ordering::Relaxed),
            group_meta_hits: self.group_meta_cache_hits.load(Ordering::Relaxed),
            group_meta_misses: self.group_meta_cache_misses.load(Ordering::Relaxed),
            locator_hits: self.locator_cache_hits.load(Ordering::Relaxed),
            locator_misses: self.locator_cache_misses.load(Ordering::Relaxed),
            group_index_remote_gets: self.group_index_remote_gets.load(Ordering::Relaxed),
            group_index_remote_bytes: self.group_index_remote_bytes.load(Ordering::Relaxed),
            inode_index_remote_gets: self.inode_index_remote_gets.load(Ordering::Relaxed),
            inode_index_remote_bytes: self.inode_index_remote_bytes.load(Ordering::Relaxed),
            group_meta_remote_gets: self.group_meta_remote_gets.load(Ordering::Relaxed),
            group_meta_remote_bytes: self.group_meta_remote_bytes.load(Ordering::Relaxed),
            frame_directory_remote_gets: self.frame_directory_remote_gets.load(Ordering::Relaxed),
            frame_directory_remote_bytes: self.frame_directory_remote_bytes.load(Ordering::Relaxed),
            frame_descriptor_remote_gets: self.frame_descriptor_remote_gets.load(Ordering::Relaxed),
            frame_descriptor_remote_bytes: self
                .frame_descriptor_remote_bytes
                .load(Ordering::Relaxed),
            group_index_entries: self.group_index_pages.entry_count(),
            group_index_bytes: self.group_index_pages.weighted_size(),
            inode_index_entries: self.inode_index_pages.entry_count(),
            inode_index_bytes: self.inode_index_pages.weighted_size(),
            inode_entry_entries: self.inode_entries.entry_count(),
            inode_entry_bytes: self.inode_entries.weighted_size(),
            group_meta_entries: self.group_meta_pages.entry_count(),
            group_meta_bytes: self.group_meta_pages.weighted_size(),
            frame_directory_entries: self.frame_directories.entry_count(),
            frame_directory_bytes: self.frame_directories.weighted_size(),
            frame_descriptor_entries: self.frame_descriptors.entry_count(),
            frame_descriptor_bytes: self.frame_descriptors.weighted_size(),
            file_locator_entries: self.file_locators.entry_count(),
            file_locator_bytes: self.file_locators.weighted_size(),
        }
    }

    pub fn packed_runtime_metrics(&self) -> PackedRuntimeMetricsSnapshot {
        let mut snapshot = self.runtime_metrics.snapshot();
        snapshot.decoded_frame_cache_configured_bytes = self.decoded_frame_cache_bytes;
        if let Some(cache) = &self.decoded_frames {
            snapshot.decoded_frame_cache_entries = cache.entry_count();
            snapshot.decoded_frame_cache_resident_bytes = cache.weighted_size();
        }
        snapshot
    }

    /// Warm immutable routing and group metadata before the FUSE mount starts
    /// serving requests. The work is bounded by the caller's concurrency and
    /// the catalog's byte-weighted cache budgets; Moka may evict older pages
    /// when a snapshot is larger than the configured budget. A zero
    /// `max_groups` skips GroupMeta while still warming both pageable indexes.
    pub async fn prefetch_metadata(
        &self,
        concurrency: usize,
        max_groups: Option<usize>,
    ) -> PackedResult<PackedMetadataWarmupStats> {
        let concurrency = concurrency.clamp(1, 64);
        let mut stats = PackedMetadataWarmupStats {
            index_budget_bytes: self.metadata_index_budget_bytes,
            group_meta_budget_bytes: self.metadata_group_budget_bytes,
            frame_directory_budget_bytes: self.metadata_frame_directory_budget_bytes,
            ..Default::default()
        };
        if !self.metadata_cache_enabled {
            return Ok(stats);
        }

        let inode_page_count = self.manifest.inode_index_pages.len();
        let mut inode_pages = stream::iter(0..inode_page_count)
            .map(|page_ordinal| {
                let catalog = self.clone();
                async move { catalog.load_inode_index_page_shared(page_ordinal).await }
            })
            .buffered(concurrency);
        while let Some(result) = inode_pages.next().await {
            let page = result?;
            stats.inode_index_pages = stats.inode_index_pages.saturating_add(1);
            let (admitted, bytes, skipped) = self.prefetch_inode_entries(&page).await;
            stats.inode_entries = stats.inode_entries.saturating_add(admitted);
            stats.inode_entry_bytes = stats.inode_entry_bytes.saturating_add(bytes);
            stats.inode_entries_skipped = stats.inode_entries_skipped.saturating_add(skipped);
        }

        let mut groups_seen = 0usize;
        if self.manifest.group_index_pages.is_empty() {
            let group_limit = max_groups.unwrap_or(self.manifest.groups.len());
            let groups = self
                .manifest
                .groups
                .iter()
                .take(group_limit)
                .cloned()
                .collect::<Vec<_>>();
            self.warm_group_meta_batch(groups, concurrency, &mut stats)
                .await?;
        } else {
            let group_page_count = self.manifest.group_index_pages.len();
            let mut group_pages = stream::iter(0..group_page_count)
                .map(|page_ordinal| {
                    let catalog = self.clone();
                    async move { catalog.load_group_index_page_shared(page_ordinal).await }
                })
                .buffered(concurrency);
            while let Some(result) = group_pages.next().await {
                let page = result?;
                stats.group_index_pages = stats.group_index_pages.saturating_add(1);
                if max_groups.is_some_and(|limit| groups_seen >= limit) {
                    continue;
                }
                let remaining =
                    max_groups.map_or(usize::MAX, |limit| limit.saturating_sub(groups_seen));
                let groups = page
                    .groups
                    .iter()
                    .take(remaining)
                    .cloned()
                    .collect::<Vec<_>>();
                groups_seen = groups_seen.saturating_add(groups.len());
                self.warm_group_meta_batch(groups, concurrency, &mut stats)
                    .await?;
            }
        }

        self.settle_metadata_caches().await;
        Ok(stats)
    }

    /// Adaptive warm-up for large snapshots. Each pageable index is admitted
    /// as a stable prefix under its own byte sub-budget, then GroupMeta pages
    /// from the admitted group-index prefix are filled into the GroupMeta
    /// budget. A large snapshot therefore gets useful early pages even when
    /// the complete index set cannot fit in memory.
    pub async fn prefetch_metadata_adaptive(
        &self,
        concurrency: usize,
    ) -> PackedResult<PackedMetadataWarmupStats> {
        let concurrency = concurrency.clamp(1, 64);
        let mut stats = PackedMetadataWarmupStats {
            index_budget_bytes: self.metadata_index_budget_bytes,
            group_meta_budget_bytes: self.metadata_group_budget_bytes,
            frame_directory_budget_bytes: self.metadata_frame_directory_budget_bytes,
            ..Default::default()
        };
        if !self.metadata_cache_enabled {
            return Ok(stats);
        }

        let inode_index_bytes: u64 = self
            .manifest
            .inode_index_pages
            .iter()
            .map(|page| page.object.object_len)
            .sum();
        let group_index_bytes: u64 = self
            .manifest
            .group_index_pages
            .iter()
            .map(|page| page.object.object_len)
            .sum();
        stats.index_bytes_estimated = inode_index_bytes.saturating_add(group_index_bytes);
        let inode_page_ordinals = select_metadata_pages(
            self.manifest
                .inode_index_pages
                .iter()
                .map(|page| page.object.object_len),
            self.metadata_inode_index_budget_bytes,
        );
        stats.inode_index_pages_skipped =
            self.manifest
                .inode_index_pages
                .len()
                .saturating_sub(inode_page_ordinals.len()) as u64;
        let mut inode_pages = stream::iter(inode_page_ordinals)
            .map(|page_ordinal| {
                let catalog = self.clone();
                async move { catalog.load_inode_index_page_shared(page_ordinal).await }
            })
            .buffered(concurrency);
        while let Some(result) = inode_pages.next().await {
            let page = result?;
            stats.inode_index_pages = stats.inode_index_pages.saturating_add(1);
            let (admitted, bytes, skipped) = self.prefetch_inode_entries(&page).await;
            stats.inode_entries = stats.inode_entries.saturating_add(admitted);
            stats.inode_entry_bytes = stats.inode_entry_bytes.saturating_add(bytes);
            stats.inode_entries_skipped = stats.inode_entries_skipped.saturating_add(skipped);
        }

        let group_page_ordinals = select_metadata_pages(
            self.manifest
                .group_index_pages
                .iter()
                .map(|page| page.object.object_len),
            self.metadata_group_index_budget_bytes,
        );
        stats.group_index_pages_skipped =
            self.manifest
                .group_index_pages
                .len()
                .saturating_sub(group_page_ordinals.len()) as u64;

        // Keep the loaded pages so their authenticated descriptors can be
        // consumed immediately. This avoids a second range GET after Moka
        // evicts an index page while GroupMeta warm-up is running.
        let mut loaded_group_pages = Vec::with_capacity(group_page_ordinals.len());
        let mut group_pages = stream::iter(group_page_ordinals)
            .map(|page_ordinal| {
                let catalog = self.clone();
                async move {
                    let page = catalog.load_group_index_page_shared(page_ordinal).await?;
                    Ok::<_, PackedWireError>((page_ordinal, page))
                }
            })
            .buffer_unordered(concurrency);
        while let Some(result) = group_pages.next().await {
            loaded_group_pages.push(result?);
            stats.group_index_pages = stats.group_index_pages.saturating_add(1);
        }
        loaded_group_pages.sort_unstable_by_key(|(ordinal, _)| *ordinal);

        let mut groups = Vec::new();
        if self.manifest.group_index_pages.is_empty() {
            groups.extend(self.manifest.groups.iter().cloned());
        } else {
            for (_, page) in &loaded_group_pages {
                stats.group_meta_bytes_estimated = stats.group_meta_bytes_estimated.saturating_add(
                    page.groups
                        .iter()
                        .map(|group| u64::from(group.meta_len))
                        .sum(),
                );
                groups.extend(page.groups.iter().cloned());
            }
            // We cannot know the exact number of group descriptors in index
            // pages that did not fit the index budget. Report the bounded
            // upper estimate so operators can see that their tail was left
            // demand-loaded rather than mistaking it for a complete warm-up.
            stats.group_meta_pages_skipped = stats.group_meta_pages_skipped.saturating_add(
                stats
                    .group_index_pages_skipped
                    .saturating_mul(MAX_GROUPS_PER_PAGE as u64),
            );
        }
        // Frame directories are metadata, not payload. Record the maximum
        // table size advertised by each selected group container and warm a
        // stable container prefix under its own byte budget. A container can
        // hold many groups, so this often removes one metadata RTT for a
        // large batch of files without downloading any data frame.
        let mut container_directory_bytes = BTreeMap::<u32, u64>::new();
        for group in &groups {
            let table_bytes = u64::from(group.frame_count)
                .saturating_mul(FRAME_RECORD_LEN as u64)
                .saturating_add(24);
            container_directory_bytes
                .entry(group.container_ordinal)
                .and_modify(|size| *size = (*size).saturating_add(table_bytes.saturating_sub(24)))
                .or_insert(table_bytes);
        }
        stats.frame_directory_bytes_estimated = container_directory_bytes
            .values()
            .copied()
            .fold(0, u64::saturating_add);
        let mut remaining_directory_budget = self.metadata_frame_directory_budget_bytes;
        let mut directory_ordinals = Vec::new();
        let directory_count = container_directory_bytes.len();
        for (container_ordinal, encoded_bytes) in container_directory_bytes {
            let projected = projected_metadata_page_bytes(encoded_bytes);
            if projected > remaining_directory_budget {
                // Keep the same stable-prefix rule used by index and
                // GroupMeta warm-up. The tail is demand-loaded and still
                // protected by the single-flight directory cache.
                break;
            }
            remaining_directory_budget = remaining_directory_budget.saturating_sub(projected);
            directory_ordinals.push(container_ordinal);
        }
        stats.frame_directory_pages_skipped =
            directory_count.saturating_sub(directory_ordinals.len()) as u64;
        let mut directories = stream::iter(directory_ordinals)
            .map(|container_ordinal| {
                let catalog = self.clone();
                async move {
                    catalog
                        .load_frame_directory_shared(container_ordinal)
                        .await
                        .map(|_| ())
                }
            })
            .buffer_unordered(concurrency);
        while let Some(result) = directories.next().await {
            result?;
            stats.frame_directory_pages = stats.frame_directory_pages.saturating_add(1);
        }

        // Do not make an all-or-nothing decision when a large snapshot's
        // GroupMeta set is bigger than the budget.  A sequential scanner
        // benefits from a stable prefix of decoded groups even when the
        // complete snapshot cannot fit.  Reserve a conservative 2x estimate
        // for Vec/Arc/extent overhead so the admitted set remains below the
        // byte-weighted cache budget instead of being immediately evicted.
        let mut remaining_group_budget = self.metadata_group_budget_bytes;
        let mut warm_groups = Vec::new();
        for group in groups {
            let projected = projected_metadata_page_bytes(u64::from(group.meta_len));
            if projected > remaining_group_budget {
                stats.group_meta_pages_skipped = stats.group_meta_pages_skipped.saturating_add(1);
                continue;
            }
            remaining_group_budget = remaining_group_budget.saturating_sub(projected);
            warm_groups.push(group);
        }
        // Plan all admitted groups together. Splitting this list into small
        // concurrency-sized batches would also split adjacent metadata in one
        // container and reintroduce one OSS RTT per batch. The planner below
        // still bounds each range and the number of in-flight range reads.
        self.warm_group_meta_batch(warm_groups, concurrency, &mut stats)
            .await?;
        self.settle_metadata_caches().await;
        Ok(stats)
    }

    async fn settle_metadata_caches(&self) {
        let _ = tokio::join!(
            self.group_index_pages.run_pending_tasks(),
            self.inode_index_pages.run_pending_tasks(),
            self.inode_entries.run_pending_tasks(),
            self.group_meta_pages.run_pending_tasks(),
            self.file_locators.run_pending_tasks(),
            self.frame_descriptors.run_pending_tasks(),
            self.frame_directories.run_pending_tasks(),
        );
    }

    fn reserve_locator_prefetch_bytes(&self, requested: u64) -> bool {
        if requested == 0 {
            return false;
        }
        let mut remaining = self
            .metadata_locator_prefetch_remaining
            .load(Ordering::Relaxed);
        loop {
            if remaining < requested {
                return false;
            }
            let next = remaining - requested;
            match self
                .metadata_locator_prefetch_remaining
                .compare_exchange_weak(remaining, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(observed) => remaining = observed,
            }
        }
    }

    fn reserve_inode_prefetch_bytes(&self, requested: u64) -> bool {
        if requested == 0 {
            return false;
        }
        let mut remaining = self
            .metadata_inode_prefetch_remaining
            .load(Ordering::Relaxed);
        loop {
            if remaining < requested {
                return false;
            }
            let next = remaining - requested;
            match self
                .metadata_inode_prefetch_remaining
                .compare_exchange_weak(remaining, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(observed) => remaining = observed,
            }
        }
    }

    async fn prefetch_inode_entries(&self, page: &PackedInodeIndexPage) -> (u64, u64, u64) {
        if !self.metadata_cache_enabled || page.entries.is_empty() {
            return (0, 0, page.entries.len() as u64);
        }
        // Keep admission ordered by the authenticated II05 page while allowing
        // Moka's async maintenance to overlap with the next record. The page
        // remains canonical; this tier is only a hot view for stat/path calls.
        let mut admissions = stream::iter(page.entries.iter())
            .map(|entry| {
                let catalog = self.clone();
                let entry = entry.clone();
                async move {
                    let weight = inode_entry_decoded_weight(&entry);
                    if catalog.inode_entries.get(&entry.inode).await.is_some() {
                        return (1, weight, 0);
                    }
                    if !catalog.reserve_inode_prefetch_bytes(weight) {
                        return (0, 0, 1);
                    }
                    catalog
                        .inode_entries
                        .insert(entry.inode, Arc::new(entry))
                        .await;
                    (1, weight, 0)
                }
            })
            .buffered(32);
        let mut admitted = 0u64;
        let mut bytes = 0u64;
        let mut skipped = 0u64;
        while let Some((entries, weight, rejected)) = admissions.next().await {
            admitted = admitted.saturating_add(entries);
            bytes = bytes.saturating_add(weight);
            skipped = skipped.saturating_add(rejected);
        }
        (admitted, bytes, skipped)
    }

    /// Admit only the bounded hot locator view while GroupMeta is being
    /// decoded.  The immutable page remains the source of truth; this small
    /// cache removes the first-read inode-page lookup and name binary search
    /// without duplicating an unbounded number of entries in memory.
    async fn prefetch_locator_entries(
        &self,
        group: &PackedGroupRef,
        entries: &[GroupMetaEntry],
    ) -> (u64, u64, u64) {
        if !self.metadata_cache_enabled || entries.is_empty() {
            return (0, 0, entries.len() as u64);
        }
        let group = Arc::new(group.clone());
        let mut admissions = stream::iter(entries.iter())
            .map(|entry| {
                let catalog = self.clone();
                let group = Arc::clone(&group);
                async move {
                    let weight = PackedFileLocator::decoded_weight_for(&group, entry);
                    if !catalog.reserve_locator_prefetch_bytes(weight) {
                        return (0, 0, 1);
                    }
                    let inode = entry.inode;
                    let locator = Arc::new(PackedFileLocator {
                        group: (*group).clone(),
                        entry: entry.clone(),
                    });
                    catalog.file_locators.insert(inode, locator).await;
                    (1, weight, 0)
                }
            })
            .buffered(32);
        let mut admitted = 0u64;
        let mut bytes = 0u64;
        let mut skipped = 0u64;
        while let Some((entries, weight, rejected)) = admissions.next().await {
            admitted = admitted.saturating_add(entries);
            bytes = bytes.saturating_add(weight);
            skipped = skipped.saturating_add(rejected);
        }
        (admitted, bytes, skipped)
    }

    async fn warm_group_meta_batch(
        &self,
        groups: Vec<PackedGroupRef>,
        concurrency: usize,
        stats: &mut PackedMetadataWarmupStats,
    ) -> PackedResult<()> {
        // GroupMeta blocks are laid out contiguously in a GroupContainer. A
        // per-group warm-up would therefore pay one object-store RTT for
        // every directory shard even though the bytes are already adjacent.
        // Merge only bounded same-container runs here; demand reads keep the
        // exact one-group range path below this helper.
        let mut by_container = BTreeMap::<u32, Vec<PackedGroupRef>>::new();
        let mut already_warm = 0u64;
        for group in groups {
            if let Some(metadata) = self.group_meta_pages.get(&group.group_id).await {
                already_warm = already_warm.saturating_add(1);
                let (admitted, bytes, skipped) = self
                    .prefetch_locator_entries(&group, metadata.entries())
                    .await;
                stats.locator_entries = stats.locator_entries.saturating_add(admitted);
                stats.locator_bytes = stats.locator_bytes.saturating_add(bytes);
                stats.locator_entries_skipped =
                    stats.locator_entries_skipped.saturating_add(skipped);
            } else {
                by_container
                    .entry(group.container_ordinal)
                    .or_default()
                    .push(group);
            }
        }
        stats.group_meta_pages = stats.group_meta_pages.saturating_add(already_warm);

        let mut ranges = Vec::<GroupMetaBatchRange>::new();
        for (container_ordinal, mut groups) in by_container {
            groups.sort_unstable_by_key(|group| group.meta_offset);
            let mut current = Vec::new();
            let mut current_end = 0u64;
            for group in groups {
                let group_end = group
                    .meta_offset
                    .checked_add(u64::from(group.meta_len))
                    .ok_or_else(|| {
                        PackedWireError::LimitExceeded(
                            "packed group metadata range overflows".into(),
                        )
                    })?;
                if current.is_empty() {
                    current_end = group_end;
                    current.push(group);
                    continue;
                }
                if group.meta_offset < current_end {
                    return Err(PackedWireError::Invalid(
                        "packed group metadata ranges overlap".into(),
                    ));
                }
                let gap = group.meta_offset - current_end;
                let merged_len = group_end.saturating_sub(current[0].meta_offset);
                if gap <= MAX_GROUP_META_BATCH_GAP_BYTES
                    && merged_len <= MAX_PACKED_STREAM_RANGE_BYTES
                {
                    current_end = group_end;
                    current.push(group);
                } else {
                    ranges.push(GroupMetaBatchRange {
                        container_ordinal,
                        offset: current[0].meta_offset,
                        length: current_end.saturating_sub(current[0].meta_offset),
                        groups: std::mem::take(&mut current),
                    });
                    current_end = group_end;
                    current.push(group);
                }
            }
            if !current.is_empty() {
                ranges.push(GroupMetaBatchRange {
                    container_ordinal,
                    offset: current[0].meta_offset,
                    length: current_end.saturating_sub(current[0].meta_offset),
                    groups: current,
                });
            }
        }

        let mut group_meta = stream::iter(ranges)
            .map(|range| {
                let catalog = self.clone();
                async move { catalog.warm_group_meta_range(range).await }
            })
            // Preserve the planner's stable container/offset order while
            // still keeping up to `concurrency` range requests in flight.
            // This makes the bounded locator admission deterministic across
            // mounts instead of letting completion order choose the hot set.
            .buffered(concurrency);
        while let Some(result) = group_meta.next().await {
            let (groups, locators, bytes, skipped) = result?;
            stats.group_meta_pages = stats.group_meta_pages.saturating_add(groups);
            stats.locator_entries = stats.locator_entries.saturating_add(locators);
            stats.locator_bytes = stats.locator_bytes.saturating_add(bytes);
            stats.locator_entries_skipped = stats.locator_entries_skipped.saturating_add(skipped);
        }
        Ok(())
    }

    async fn warm_group_meta_range(
        &self,
        range: GroupMetaBatchRange,
    ) -> PackedResult<(u64, u64, u64, u64)> {
        if range.length == 0 || range.length > MAX_PACKED_STREAM_RANGE_BYTES {
            return Err(PackedWireError::LimitExceeded(
                "packed group metadata batch exceeds range budget".into(),
            ));
        }
        let remote = self.open_container(range.container_ordinal).await?;
        self.group_meta_remote_gets.fetch_add(1, Ordering::Relaxed);
        self.group_meta_remote_bytes
            .fetch_add(range.length, Ordering::Relaxed);
        let bytes = remote.read_range(range.offset, range.length).await?;
        let mut decoded = Vec::with_capacity(range.groups.len());
        for group in &range.groups {
            let relative = group.meta_offset.checked_sub(range.offset).ok_or_else(|| {
                PackedWireError::Invalid("packed metadata batch starts after group".into())
            })?;
            let start = usize::try_from(relative).map_err(|_| {
                PackedWireError::LimitExceeded("packed metadata batch offset exceeds usize".into())
            })?;
            let end = start
                .checked_add(usize::try_from(group.meta_len).map_err(|_| {
                    PackedWireError::LimitExceeded(
                        "packed group metadata length exceeds usize".into(),
                    )
                })?)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("packed metadata batch slice overflows".into())
                })?;
            let metadata = bytes.get(start..end).ok_or(PackedWireError::Truncated {
                what: "packed group metadata batch",
                need: end,
                have: bytes.len(),
            })?;
            let digest: [u8; 32] = Sha256::digest(metadata).into();
            if digest != group.metadata_digest {
                return Err(PackedWireError::Invalid(
                    "packed group metadata digest mismatch".into(),
                ));
            }
            let metadata = GroupMeta::decode(metadata)?;
            if metadata.len() != usize::try_from(group.entry_count).unwrap_or(usize::MAX) {
                return Err(PackedWireError::Invalid(
                    "packed group metadata entry count disagrees with group descriptor".into(),
                ));
            }
            decoded.push((group.clone(), Arc::new(metadata)));
        }
        let mut locator_entries = 0u64;
        let mut locator_bytes = 0u64;
        let mut locator_entries_skipped = 0u64;
        for (group, metadata) in decoded {
            self.group_meta_pages
                .insert(group.group_id, Arc::clone(&metadata))
                .await;
            let (admitted, bytes, skipped) = self
                .prefetch_locator_entries(&group, metadata.entries())
                .await;
            locator_entries = locator_entries.saturating_add(admitted);
            locator_bytes = locator_bytes.saturating_add(bytes);
            locator_entries_skipped = locator_entries_skipped.saturating_add(skipped);
        }
        Ok((
            u64::try_from(range.groups.len()).unwrap_or(u64::MAX),
            locator_entries,
            locator_bytes,
            locator_entries_skipped,
        ))
    }

    pub fn manifest(&self) -> &PackedSnapshotManifest {
        self.manifest.as_ref()
    }

    pub fn client(&self) -> &ObjectClient<B> {
        &self.client
    }

    pub fn group_ref(&self, group_id: u64) -> PackedResult<&PackedGroupRef> {
        self.group_indexes
            .get(&group_id)
            .and_then(|index| self.manifest.groups.get(*index))
            .ok_or_else(|| PackedWireError::Invalid("packed group id is missing".into()))
    }

    /// Return one bounded page of groups belonging to a directory.  The
    /// per-parent order is built once when the catalog is created, so paging
    /// never scans or clones the complete directory.
    pub fn groups_for_parent(
        &self,
        parent_dir_key: [u8; 32],
        start: usize,
        limit: usize,
    ) -> Vec<PackedGroupRef> {
        if limit == 0 {
            return Vec::new();
        }
        self.parent_indexes
            .get(&parent_dir_key)
            .into_iter()
            .flat_map(|indexes| {
                indexes
                    .iter()
                    .skip(start)
                    .take(limit.min(MAX_GROUPS_PER_PAGE))
            })
            .filter_map(|index| self.manifest.groups.get(*index).cloned())
            .collect()
    }

    /// Locate the only group whose name range can contain `name`.
    pub fn group_for_name(&self, parent_dir_key: [u8; 32], name: &[u8]) -> Option<&PackedGroupRef> {
        let indexes = self.parent_indexes.get(&parent_dir_key)?;
        let candidate = indexes
            .partition_point(|index| self.manifest.groups[*index].last_name.as_slice() < name);
        let group = self.manifest.groups.get(*indexes.get(candidate)?)?;
        (group.first_name.as_slice() <= name && group.last_name.as_slice() >= name).then_some(group)
    }

    /// Resolve a group from one remote index page without materializing the
    /// manifest's complete group table.  Large snapshots leave
    /// `manifest.groups` empty and publish these pages instead; callers that
    /// already know the page ordinal (for example from a directory cursor)
    /// can keep the lookup strictly bounded to one page.
    pub async fn group_ref_from_index_page(
        &self,
        page_ordinal: usize,
        group_id: u64,
    ) -> PackedResult<Option<PackedGroupRef>> {
        if self.manifest.group_index_pages.is_empty() {
            return Ok(self
                .group_indexes
                .get(&group_id)
                .and_then(|index| self.manifest.groups.get(*index).cloned()));
        }
        Ok(self
            .load_group_index_page_shared(page_ordinal)
            .await?
            .groups
            .iter()
            .find(|group| group.group_id == group_id)
            .cloned())
    }

    /// Resolve a name range from one remote index page.  The page is decoded
    /// and dropped before the returned group is used, so memory remains
    /// bounded by one index page plus the cloned descriptor.
    pub async fn group_for_name_from_index_page(
        &self,
        page_ordinal: usize,
        parent_dir_key: [u8; 32],
        name: &[u8],
    ) -> PackedResult<Option<PackedGroupRef>> {
        if self.manifest.group_index_pages.is_empty() {
            return Ok(self.group_for_name(parent_dir_key, name).cloned());
        }
        Ok(self
            .load_group_index_page_shared(page_ordinal)
            .await?
            .groups
            .iter()
            .find(|group| {
                group.parent_dir_key == parent_dir_key
                    && group.first_name.as_slice() <= name
                    && group.last_name.as_slice() >= name
            })
            .cloned())
    }

    /// Resolve a dentry by the manifest's authenticated page fence. Large
    /// snapshots therefore need one binary search in the manifest and one
    /// bounded group-index page read; callers do not guess a page ordinal.
    pub async fn group_for_name_paged(
        &self,
        parent_dir_key: [u8; 32],
        name: &[u8],
    ) -> PackedResult<Option<PackedGroupRef>> {
        if self.manifest.group_index_pages.is_empty() {
            return Ok(self.group_for_name(parent_dir_key, name).cloned());
        }
        let Some(page_ordinal) = self
            .manifest
            .group_index_page_for_name(parent_dir_key, name)
        else {
            return Ok(None);
        };
        self.group_for_name_from_index_page(page_ordinal, parent_dir_key, name)
            .await
    }

    /// Perform a dentry lookup using one pageable group-index page.  This is
    /// the page-backed counterpart to [`Self::lookup_entry`]; it is useful for
    /// million-file snapshots where keeping all group descriptors in RAM
    /// would defeat the format's paging contract.
    pub async fn lookup_entry_from_index_page(
        &self,
        page_ordinal: usize,
        parent_dir_key: [u8; 32],
        name: &[u8],
    ) -> PackedResult<Option<GroupMetaEntry>> {
        let Some(group) = self
            .group_for_name_from_index_page(page_ordinal, parent_dir_key, name)
            .await?
        else {
            return Ok(None);
        };
        let meta = self.load_group_meta_for_ref(&group).await?;
        let Some(entry) = meta.lookup(name) else {
            return Ok(None);
        };
        let entry = entry.clone();
        self.file_locators
            .insert(
                entry.inode,
                Arc::new(PackedFileLocator {
                    group,
                    entry: entry.clone(),
                }),
            )
            .await;
        Ok(Some(entry))
    }

    pub async fn lookup_entry_paged(
        &self,
        parent_dir_key: [u8; 32],
        name: &[u8],
    ) -> PackedResult<Option<GroupMetaEntry>> {
        let Some(group) = self.group_for_name_paged(parent_dir_key, name).await? else {
            return Ok(None);
        };
        let meta = self.load_group_meta_for_ref(&group).await?;
        let Some(entry) = meta.lookup(name) else {
            return Ok(None);
        };
        let entry = entry.clone();
        self.file_locators
            .insert(
                entry.inode,
                Arc::new(PackedFileLocator {
                    group,
                    entry: entry.clone(),
                }),
            )
            .await;
        Ok(Some(entry))
    }

    /// Resolve an inode to its group and canonical directory entry.  The
    /// pageable inode index keeps the lookup bounded to one inode page and
    /// one group page; small inline manifests fall back to scanning their
    /// bounded group table.
    pub async fn lookup_inode_entry(
        &self,
        inode: u64,
    ) -> PackedResult<Option<(PackedGroupRef, GroupMetaEntry)>> {
        Ok(self
            .lookup_inode_locator(inode)
            .await?
            .map(|locator| (locator.group.clone(), locator.entry.clone())))
    }

    /// Resolve and retain one immutable inode locator.  The returned Arc is
    /// shared by stat/get_slices/read paths, so a hot file no longer clones
    /// its full GroupMeta entry for every FUSE operation.
    pub(crate) async fn lookup_inode_locator(
        &self,
        inode: u64,
    ) -> PackedResult<Option<Arc<PackedFileLocator>>> {
        if let Some(locator) = self.file_locators.get(&inode).await {
            self.metadata_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.locator_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(locator));
        }
        self.metadata_cache_misses.fetch_add(1, Ordering::Relaxed);
        self.locator_cache_misses.fetch_add(1, Ordering::Relaxed);
        let Some(locator) = self.resolve_inode_locator_uncached(inode).await? else {
            return Ok(None);
        };
        let locator = Arc::new(locator);
        self.file_locators.insert(inode, Arc::clone(&locator)).await;
        Ok(Some(locator))
    }

    async fn resolve_inode_locator_uncached(
        &self,
        inode: u64,
    ) -> PackedResult<Option<PackedFileLocator>> {
        if let Some(index) = self.inode_paged(inode).await? {
            let group = if self.manifest.group_index_pages.is_empty() {
                self.group_for_name(index.parent_dir_key, &index.name)
                    .cloned()
            } else if !self.manifest.groups.is_empty() {
                Some(self.group_ref(index.group_id)?.clone())
            } else {
                let Some(page) = self
                    .manifest
                    .group_index_page_for_name(index.parent_dir_key, &index.name)
                else {
                    return Ok(None);
                };
                self.group_for_name_from_index_page(page, index.parent_dir_key, &index.name)
                    .await?
            };
            let Some(group) = group else {
                return Ok(None);
            };
            let metadata = self.load_group_meta_for_ref(&group).await?;
            // II05 authenticates the ordinal within the sorted GroupMeta
            // page. Use it directly instead of doing another name binary
            // search for every first read of an inode. Keep the name check as
            // a cross-index integrity guard.
            let entry_ordinal = usize::try_from(index.entry_ordinal).map_err(|_| {
                PackedWireError::LimitExceeded("packed inode entry ordinal exceeds usize".into())
            })?;
            let entry = metadata
                .entries()
                .get(entry_ordinal)
                .cloned()
                .ok_or_else(|| {
                    PackedWireError::Invalid(
                        "packed inode index entry ordinal is missing from group".into(),
                    )
                })?;
            if entry.name != index.name {
                return Err(PackedWireError::Invalid(
                    "packed inode index and group metadata names disagree".into(),
                ));
            }
            if entry.inode != inode {
                return Err(PackedWireError::Invalid(
                    "packed inode index and group metadata disagree".into(),
                ));
            }
            return Ok(Some(PackedFileLocator { group, entry }));
        }

        for group in &self.manifest.groups {
            let meta = self.load_group_meta_for_ref(group).await?;
            if let Some(entry) = meta.entries().iter().find(|entry| entry.inode == inode) {
                return Ok(Some(PackedFileLocator {
                    group: group.clone(),
                    entry: entry.clone(),
                }));
            }
        }
        Ok(None)
    }

    /// Resolve and execute a packed read for an inode.  The caller supplies a
    /// file range; only the containing group's metadata, selected frame
    /// descriptors, and requested frame payloads are fetched.
    pub async fn read_inode_range(
        &self,
        inode: u64,
        offset: u64,
        output: &mut [u8],
    ) -> PackedResult<()> {
        let Some(locator) = self.lookup_inode_locator(inode).await? else {
            return Err(PackedWireError::Invalid("packed inode is missing".into()));
        };
        let group = &locator.group;
        let entry = &locator.entry;
        let length = u64::try_from(output.len())
            .map_err(|_| PackedWireError::LimitExceeded("packed read length exceeds u64".into()))?;
        let plan = self
            .read_unified_plan_for_entry(group, entry, offset, length)
            .await?;
        let container_ordinal = plan.segments.iter().find_map(|segment| {
            if let ReadSource::PackedFrame {
                container_ordinal, ..
            } = segment.source
            {
                Some(container_ordinal)
            } else {
                None
            }
        });
        let Some(container_ordinal) = container_ordinal else {
            let fetcher: super::remote::PackedFrameSourceFetcher<B> =
                super::remote::PackedFrameSourceFetcher::new(HashMap::new())
                    .with_generation(plan.generation);
            let result = execute_unified_into(&fetcher, offset, &plan, output)
                .await
                .map_err(|error| PackedWireError::Backend(error.to_string()));
            if result.is_ok() {
                self.runtime_metrics.record_logical_bytes(length);
            }
            return result;
        };
        let requests = plan
            .segments
            .iter()
            .filter_map(|segment| match &segment.source {
                ReadSource::PackedFrame {
                    container_ordinal: source_container,
                    frame_ordinal,
                    object_offset,
                    stored_len,
                    raw_len,
                    size_class,
                    codec,
                    frame_digest,
                    ..
                } => {
                    if *source_container != container_ordinal {
                        return Some(Err(PackedWireError::Invalid(
                            "packed read plan spans multiple containers".into(),
                        )));
                    }
                    let size_class = match SizeClass::from_u8(*size_class) {
                        Ok(size_class) => size_class,
                        Err(error) => {
                            return Some(Err(PackedWireError::Invalid(error.to_string())));
                        }
                    };
                    Some(Ok(FrameReadRequest {
                        descriptor: PackedFrameDescriptor {
                            frame_ordinal: *frame_ordinal,
                            object_offset: *object_offset,
                            stored_len: *stored_len,
                            raw_len: *raw_len,
                            first_file_slot: 0,
                            last_file_slot: 0,
                            size_class,
                            codec: *codec,
                            frame_digest: *frame_digest,
                        },
                        logical_len: segment.length,
                    }))
                }
                ReadSource::Hole => None,
                _ => Some(Err(PackedWireError::Invalid(
                    "packed read plan contains a non-packed source".into(),
                ))),
            })
            .collect::<PackedResult<Vec<_>>>()?;
        let mut frames = BTreeMap::new();
        let mut pending_requests = Vec::with_capacity(requests.len());
        if let Some(cache) = &self.decoded_frames {
            for request in requests {
                let key = (container_ordinal, request.descriptor.frame_ordinal);
                if let Some(frame) = cache.get(&key).await {
                    self.runtime_metrics.record_decoded_frame_cache_hit();
                    self.runtime_metrics.record_data_cache_hit();
                    frames.insert(request.descriptor.frame_ordinal, frame);
                } else {
                    self.runtime_metrics.record_decoded_frame_cache_miss();
                    pending_requests.push(request);
                }
            }
        } else {
            pending_requests = requests;
        }
        let object = if pending_requests.is_empty() {
            None
        } else {
            Some(self.open_container(container_ordinal).await?)
        };
        let prefetch_anchor = pending_requests
            .iter()
            .map(|request| {
                (
                    request.descriptor.object_offset,
                    u64::from(request.descriptor.stored_len),
                )
            })
            .max_by_key(|(offset, _)| *offset);
        // Start the next-window fetch before waiting for the foreground
        // range. The window cache and bounded semaphore keep this
        // speculative work explicit; strict/random profiles never enter this
        // branch. Starting here lets the next sequential files overlap OSS
        // latency with the current frame read instead of adding a serialized
        // RTT between adjacent windows.
        if self.frame_window_prefetch_enabled
            && self.frame_window_cache_bytes > 0
            && matches!(
                group.layout_profile,
                super::layout::AccessProfile::SequentialSmallFile
            )
            && let Some((offset, length)) = prefetch_anchor
            && let Ok(permit) = self.frame_window_prefetch_limit.clone().try_acquire_owned()
        {
            let object = Arc::clone(object.as_ref().expect("pending requests opened container"));
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(error) = object.prefetch_next_payload_window(offset, length).await {
                    tracing::debug!(error = ?error, "packed sequential window prefetch failed");
                }
            });
        }
        if !pending_requests.is_empty() {
            let fetched = self
                .read_coordinator
                .submit(
                    Arc::clone(object.as_ref().expect("pending requests opened container")),
                    group.layout_profile,
                    pending_requests,
                )
                .await?;
            if let Some(cache) = &self.decoded_frames {
                for (ordinal, frame) in &fetched {
                    if frame.len() as u64 <= self.decoded_frame_cache_bytes {
                        cache
                            .insert((container_ordinal, *ordinal), frame.clone())
                            .await;
                    }
                }
            }
            frames.extend(fetched);
        }
        let fetcher = if let Some(object) = object {
            super::remote::PackedFrameSourceFetcher::with_prefetched_frames(
                (*object).clone(),
                container_ordinal,
                frames,
            )
        } else {
            super::remote::PackedFrameSourceFetcher::with_prefetched_frame_map(
                container_ordinal,
                frames,
            )
        }
        .with_generation(plan.generation);
        let result = execute_unified_into(&fetcher, offset, &plan, output)
            .await
            .map_err(|error| PackedWireError::Backend(error.to_string()));
        if result.is_ok() {
            self.runtime_metrics.record_logical_bytes(length);
        }
        result
    }

    /// Return a bounded directory page. Group metadata remains the unit of
    /// remote IO; the method only retains the groups needed to fill this
    /// page, so a million-entry directory never becomes one allocation.
    pub async fn readdir_page(
        &self,
        parent_dir_key: [u8; 32],
        child_offset: usize,
        limit: usize,
    ) -> PackedResult<Vec<GroupMetaEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut skip = child_offset;
        let mut output = Vec::with_capacity(limit);
        if self.manifest.group_index_pages.is_empty() {
            let groups = self.groups_for_parent(parent_dir_key, 0, usize::MAX);
            for group in groups {
                let entry_count = usize::try_from(group.entry_count).map_err(|_| {
                    PackedWireError::LimitExceeded("packed group entry count exceeds usize".into())
                })?;
                if skip >= entry_count {
                    skip -= entry_count;
                    continue;
                }
                let meta = self.load_group_meta_for_ref(&group).await?;
                let page = meta.page(skip, limit - output.len()).to_vec();
                // A directory page already contains the canonical immutable
                // locator for every returned inode. Admit those locators
                // before handing the page to FUSE so the subsequent getattr,
                // get_slices and read calls do not repeat inode-index lookup
                // or GroupMeta entry cloning. The cache itself remains
                // byte-budgeted, so large directories cannot grow memory
                // without bound.
                self.admit_locator_entries(&group, &page).await;
                output.extend(page);
                if output.len() == limit {
                    break;
                }
                skip = 0;
            }
            return Ok(output);
        }

        // Page fences are sorted by (parent key, first name). Start at the
        // first page that can contain this parent, then continue until the
        // fence moves past it. This bounds cold reads for a nested directory
        // while preserving a stable ordinal cursor.
        let start_page = self
            .manifest
            .group_index_pages
            .iter()
            .position(|page| page.last_parent_dir_key >= parent_dir_key)
            .unwrap_or(0);
        for page_ordinal in start_page..self.manifest.group_index_pages.len() {
            let page = self.load_group_index_page_shared(page_ordinal).await?;
            if page
                .groups
                .first()
                .is_some_and(|group| group.parent_dir_key > parent_dir_key)
            {
                break;
            }
            for group in page
                .groups
                .iter()
                .filter(|group| group.parent_dir_key == parent_dir_key)
                .cloned()
            {
                let entry_count = usize::try_from(group.entry_count).map_err(|_| {
                    PackedWireError::LimitExceeded("packed group entry count exceeds usize".into())
                })?;
                if skip >= entry_count {
                    skip -= entry_count;
                    continue;
                }
                let meta = self.load_group_meta_for_ref(&group).await?;
                let entries = meta.page(skip, limit - output.len()).to_vec();
                self.admit_locator_entries(&group, &entries).await;
                output.extend(entries);
                if output.len() == limit {
                    return Ok(output);
                }
                skip = 0;
            }
        }
        Ok(output)
    }

    async fn admit_locator_entries(&self, group: &PackedGroupRef, entries: &[GroupMetaEntry]) {
        if entries.is_empty() || !self.metadata_cache_enabled {
            return;
        }
        // Moka's future cache admission is asynchronous. Keep a small
        // bounded fan-out so a 256-entry FUSE directory page does not turn
        // into a serial metadata tail, while still completing admission
        // before the page is returned to the caller.
        let group = group.clone();
        let mut admissions = stream::iter(entries.iter().cloned())
            .map(|entry| {
                let catalog = self.clone();
                let group = group.clone();
                async move {
                    catalog
                        .file_locators
                        .insert(entry.inode, Arc::new(PackedFileLocator { group, entry }))
                        .await;
                }
            })
            .buffer_unordered(32);
        while admissions.next().await.is_some() {}
    }

    /// Resolve one directory entry without a per-entry namespace lookup.
    pub async fn lookup_entry(
        &self,
        parent_dir_key: [u8; 32],
        name: &[u8],
    ) -> PackedResult<Option<GroupMetaEntry>> {
        let Some(group_id) = self
            .group_for_name(parent_dir_key, name)
            .map(|group| group.group_id)
        else {
            return Ok(None);
        };
        let group = self.group_ref(group_id)?.clone();
        let meta = self.load_group_meta_for_ref(&group).await?;
        let Some(entry) = meta.lookup(name) else {
            return Ok(None);
        };
        let entry = entry.clone();
        self.file_locators
            .insert(
                entry.inode,
                Arc::new(PackedFileLocator {
                    group,
                    entry: entry.clone(),
                }),
            )
            .await;
        Ok(Some(entry))
    }

    pub async fn load_group_meta(&self, group_id: u64) -> PackedResult<GroupMeta> {
        let group = self.group_ref(group_id)?.clone();
        Ok((*self.load_group_meta_for_ref(&group).await?).clone())
    }

    async fn load_group_meta_for_ref(
        &self,
        group: &PackedGroupRef,
    ) -> PackedResult<Arc<GroupMeta>> {
        if let Some(meta) = self.group_meta_pages.get(&group.group_id).await {
            self.metadata_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.group_meta_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(meta);
        }
        self.metadata_cache_misses.fetch_add(1, Ordering::Relaxed);
        self.group_meta_cache_misses.fetch_add(1, Ordering::Relaxed);
        // Moka's try_get_with makes the immutable page load a single flight.
        // Without it, a parallel scanner can observe the same cold miss in
        // many workers and issue duplicate GroupMeta range requests.
        let catalog = self.clone();
        let group = group.clone();
        self.group_meta_pages
            .try_get_with(group.group_id, async move {
                if usize::try_from(group.meta_len).is_ok_and(|len| len > MAX_GROUP_META_BYTES) {
                    return Err(PackedWireError::LimitExceeded(
                        "packed group metadata exceeds 256 KiB".into(),
                    ));
                }
                catalog
                    .group_meta_remote_gets
                    .fetch_add(1, Ordering::Relaxed);
                catalog
                    .group_meta_remote_bytes
                    .fetch_add(u64::from(group.meta_len), Ordering::Relaxed);
                let remote = catalog.open_container(group.container_ordinal).await?;
                let bytes = remote
                    .read_range(group.meta_offset, u64::from(group.meta_len))
                    .await?;
                let digest: [u8; 32] = Sha256::digest(&bytes).into();
                if digest != group.metadata_digest {
                    return Err(PackedWireError::Invalid(
                        "packed group metadata digest mismatch".into(),
                    ));
                }
                let metadata = GroupMeta::decode(&bytes)?;
                if metadata.len() != usize::try_from(group.entry_count).unwrap_or(usize::MAX) {
                    return Err(PackedWireError::Invalid(
                        "packed group metadata entry count disagrees with group descriptor".into(),
                    ));
                }
                Ok(Arc::new(metadata))
            })
            .await
            .map_err(|error| (*error).clone())
    }

    pub async fn load_group_meta_page(
        &self,
        group_id: u64,
        start: usize,
        limit: usize,
    ) -> PackedResult<Vec<super::meta::GroupMetaEntry>> {
        let group = self.group_ref(group_id)?.clone();
        Ok(self
            .load_group_meta_for_ref(&group)
            .await?
            .page(start, limit.min(MAX_GROUPS_PER_PAGE))
            .to_vec())
    }

    /// Resolve one file range directly from GroupMeta into the common read
    /// plan.  The plan contains only immutable packed sources and explicit
    /// holes; no namespace or KV lookup is needed after this call.  Frame
    /// descriptors are fetched from the bounded container directory and the
    /// payload itself is left to `PackedFrameSourceFetcher`.
    pub async fn read_unified_plan(
        &self,
        group_id: u64,
        name: &[u8],
        offset: u64,
        length: u64,
    ) -> PackedResult<UnifiedReadPlan> {
        let group = self.group_ref(group_id)?.clone();
        self.read_unified_plan_for_ref(&group, name, offset, length)
            .await
    }

    /// Generate a read plan from one pageable group-index page.  This is the
    /// data-path counterpart to [`Self::lookup_entry_from_index_page`]: large
    /// snapshots can keep `manifest.groups` empty and still resolve metadata,
    /// frame descriptors, and data ranges without loading the full index.
    pub async fn read_unified_plan_from_index_page(
        &self,
        page_ordinal: usize,
        group_id: u64,
        name: &[u8],
        offset: u64,
        length: u64,
    ) -> PackedResult<UnifiedReadPlan> {
        let Some(group) = self
            .group_ref_from_index_page(page_ordinal, group_id)
            .await?
        else {
            return Err(PackedWireError::Invalid(
                "packed group id is missing from the index page".into(),
            ));
        };
        self.read_unified_plan_for_ref(&group, name, offset, length)
            .await
    }

    pub async fn read_unified_plan_paged(
        &self,
        parent_dir_key: [u8; 32],
        name: &[u8],
        offset: u64,
        length: u64,
    ) -> PackedResult<UnifiedReadPlan> {
        if self.manifest.group_index_pages.is_empty() {
            let group = self
                .group_for_name(parent_dir_key, name)
                .cloned()
                .ok_or_else(|| PackedWireError::Invalid("packed dentry is missing".into()))?;
            return self
                .read_unified_plan_for_ref(&group, name, offset, length)
                .await;
        }
        let page_ordinal = self
            .manifest
            .group_index_page_for_name(parent_dir_key, name)
            .ok_or_else(|| PackedWireError::Invalid("packed dentry is missing".into()))?;
        let group = self
            .group_for_name_from_index_page(page_ordinal, parent_dir_key, name)
            .await?
            .ok_or_else(|| PackedWireError::Invalid("packed dentry is missing".into()))?;
        self.read_unified_plan_for_ref(&group, name, offset, length)
            .await
    }

    async fn read_unified_plan_for_ref(
        &self,
        group: &PackedGroupRef,
        name: &[u8],
        offset: u64,
        length: u64,
    ) -> PackedResult<UnifiedReadPlan> {
        let entry = self
            .load_group_meta_for_ref(group)
            .await?
            .lookup(name)
            .cloned()
            .ok_or_else(|| PackedWireError::Invalid("packed group entry is missing".into()))?;
        self.read_unified_plan_for_entry(group, &entry, offset, length)
            .await
    }

    async fn read_unified_plan_for_entry(
        &self,
        group: &PackedGroupRef,
        entry: &GroupMetaEntry,
        offset: u64,
        length: u64,
    ) -> PackedResult<UnifiedReadPlan> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| PackedWireError::LimitExceeded("packed read range overflows".into()))?;
        if end > entry.size {
            return Err(PackedWireError::Invalid(
                "packed read range exceeds file size".into(),
            ));
        }
        let generation = ReadGeneration::readonly(self.manifest.snapshot_id);
        if length == 0 {
            return Ok(UnifiedReadPlan {
                generation,
                logical_size: entry.size,
                segments: Vec::new(),
            });
        }

        // The GroupMeta range already contains the complete bytes for this
        // small file.  Keep the source in the unified plan so the executor
        // can copy it without opening the container or reading a frame table.
        if !entry.inline_data.is_empty() {
            let raw_offset = u32::try_from(offset).map_err(|_| {
                PackedWireError::LimitExceeded("packed inline offset exceeds u32".into())
            })?;
            let plan = UnifiedReadPlan {
                generation,
                logical_size: entry.size,
                segments: vec![LogicalSegment {
                    logical_offset: offset,
                    length,
                    source: ReadSource::PackedInline {
                        data: entry.inline_data.clone(),
                        raw_offset,
                    },
                }],
            };
            plan.validate(offset, length)
                .map_err(|error| PackedWireError::Invalid(error.to_string()))?;
            return Ok(plan);
        }

        let frame_ordinals = entry.extents.iter().map(|extent| extent.frame_ordinal);
        let frames = self
            .load_frame_descriptors(group.container_ordinal, frame_ordinals)
            .await?
            .into_iter()
            .map(|frame| (frame.frame_ordinal, frame))
            .collect::<HashMap<_, _>>();
        let mut segments = Vec::new();
        for extent in &entry.extents {
            let extent_end = extent
                .file_offset
                .checked_add(u64::from(extent.logical_len))
                .ok_or_else(|| PackedWireError::LimitExceeded("packed extent overflows".into()))?;
            let start = offset.max(extent.file_offset);
            let segment_end = end.min(extent_end);
            if start >= segment_end {
                continue;
            }
            let frame = frames
                .get(&extent.frame_ordinal)
                .ok_or_else(|| PackedWireError::Invalid("packed extent frame is missing".into()))?;
            if frame.raw_len != extent.raw_len {
                return Err(PackedWireError::Invalid(
                    "packed extent and frame lengths disagree".into(),
                ));
            }
            let raw_offset = u64::from(extent.raw_offset)
                .checked_add(start - extent.file_offset)
                .ok_or_else(|| {
                    PackedWireError::LimitExceeded("packed raw offset overflows".into())
                })?;
            let segment_len = segment_end - start;
            if raw_offset
                .checked_add(segment_len)
                .is_none_or(|raw_end| raw_end > u64::from(frame.raw_len))
            {
                return Err(PackedWireError::Invalid(
                    "packed extent exceeds frame raw length".into(),
                ));
            }
            segments.push(LogicalSegment {
                logical_offset: start,
                length: segment_len,
                source: ReadSource::PackedFrame {
                    group_id: group.group_id,
                    container_ordinal: group.container_ordinal,
                    frame_ordinal: frame.frame_ordinal,
                    object_offset: frame.object_offset,
                    stored_len: frame.stored_len,
                    raw_offset: u32::try_from(raw_offset).map_err(|_| {
                        PackedWireError::LimitExceeded("packed raw offset exceeds u32".into())
                    })?,
                    raw_len: frame.raw_len,
                    size_class: frame.size_class as u8,
                    codec: frame.codec,
                    frame_digest: frame.frame_digest,
                },
            });
        }
        segments.sort_by_key(|segment| segment.logical_offset);
        let plan = UnifiedReadPlan {
            generation,
            logical_size: entry.size,
            segments,
        };
        plan.validate(offset, length)
            .map_err(|error| PackedWireError::Invalid(error.to_string()))?;
        Ok(plan)
    }

    pub async fn load_group_index_page(
        &self,
        page_ordinal: usize,
    ) -> PackedResult<PackedGroupIndexPage> {
        Ok((*self.load_group_index_page_shared(page_ordinal).await?).clone())
    }

    async fn load_group_index_page_shared(
        &self,
        page_ordinal: usize,
    ) -> PackedResult<Arc<PackedGroupIndexPage>> {
        if let Some(page) = self.group_index_pages.get(&page_ordinal).await {
            self.metadata_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.index_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(page);
        }
        self.metadata_cache_misses.fetch_add(1, Ordering::Relaxed);
        self.index_cache_misses.fetch_add(1, Ordering::Relaxed);
        let reference = self
            .manifest
            .group_index_pages
            .get(page_ordinal)
            .cloned()
            .ok_or_else(|| PackedWireError::Invalid("packed group index page is missing".into()))?;
        let catalog = self.clone();
        let total_pages = catalog.manifest.group_index_pages.len() as u32;
        let snapshot_id = catalog.manifest.snapshot_id;
        self.group_index_pages
            .try_get_with(page_ordinal, async move {
                catalog
                    .group_index_remote_gets
                    .fetch_add(1, Ordering::Relaxed);
                catalog
                    .group_index_remote_bytes
                    .fetch_add(reference.object.object_len, Ordering::Relaxed);
                let object = catalog
                    .read_index_object(&reference.object, PackedObjectKind::GroupIndex)
                    .await?;
                let page = PackedGroupIndexPage::decode(object)?;
                if page.snapshot_id != snapshot_id
                    || page.page_ordinal != page_ordinal as u32
                    || page.total_pages != total_pages
                {
                    return Err(PackedWireError::Invalid(
                        "packed group index page does not match the manifest".into(),
                    ));
                }
                page.validate_reference(&reference)?;
                Ok(Arc::new(page))
            })
            .await
            .map_err(|error| (*error).clone())
    }

    pub async fn load_inode_index_page(
        &self,
        page_ordinal: usize,
    ) -> PackedResult<PackedInodeIndexPage> {
        Ok((*self.load_inode_index_page_shared(page_ordinal).await?).clone())
    }

    async fn load_inode_index_page_shared(
        &self,
        page_ordinal: usize,
    ) -> PackedResult<Arc<PackedInodeIndexPage>> {
        if let Some(page) = self.inode_index_pages.get(&page_ordinal).await {
            self.metadata_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.index_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(page);
        }
        self.metadata_cache_misses.fetch_add(1, Ordering::Relaxed);
        self.index_cache_misses.fetch_add(1, Ordering::Relaxed);
        let reference = self
            .manifest
            .inode_index_pages
            .get(page_ordinal)
            .cloned()
            .ok_or_else(|| PackedWireError::Invalid("packed inode index page is missing".into()))?;
        let catalog = self.clone();
        let total_pages = catalog.manifest.inode_index_pages.len() as u32;
        let snapshot_id = catalog.manifest.snapshot_id;
        self.inode_index_pages
            .try_get_with(page_ordinal, async move {
                catalog
                    .inode_index_remote_gets
                    .fetch_add(1, Ordering::Relaxed);
                catalog
                    .inode_index_remote_bytes
                    .fetch_add(reference.object.object_len, Ordering::Relaxed);
                let object = catalog
                    .read_index_object(&reference.object, PackedObjectKind::InodeIndex)
                    .await?;
                let page = PackedInodeIndexPage::decode(object)?;
                if page.snapshot_id != snapshot_id
                    || page.page_ordinal != page_ordinal as u32
                    || page.total_pages != total_pages
                {
                    return Err(PackedWireError::Invalid(
                        "packed inode index page does not match the manifest".into(),
                    ));
                }
                page.validate_reference(&reference)?;
                Ok(Arc::new(page))
            })
            .await
            .map_err(|error| (*error).clone())
    }

    /// Look up one inode in a single remote inode-index page.  The caller
    /// supplies the page ordinal from its inode cursor/routing layer; only
    /// that bounded page is downloaded and decoded.
    pub async fn inode_from_index_page(
        &self,
        page_ordinal: usize,
        inode: u64,
    ) -> PackedResult<Option<super::index::PackedInodeIndexEntry>> {
        if self.manifest.inode_index_pages.is_empty() {
            return Ok(None);
        }
        if let Some(entry) = self.inode_entries.get(&inode).await {
            self.metadata_cache_hits.fetch_add(1, Ordering::Relaxed);
            self.inode_entry_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some((*entry).clone()));
        }
        self.metadata_cache_misses.fetch_add(1, Ordering::Relaxed);
        self.inode_entry_cache_misses
            .fetch_add(1, Ordering::Relaxed);
        let page = self.load_inode_index_page_shared(page_ordinal).await?;
        let Some(index) = page
            .entries
            .binary_search_by_key(&inode, |entry| entry.inode)
            .ok()
        else {
            return Ok(None);
        };
        let entry = Arc::new(page.entries[index].clone());
        self.inode_entries.insert(inode, Arc::clone(&entry)).await;
        Ok(Some((*entry).clone()))
    }

    pub async fn inode_paged(
        &self,
        inode: u64,
    ) -> PackedResult<Option<super::index::PackedInodeIndexEntry>> {
        if self.manifest.inode_index_pages.is_empty() {
            return Ok(None);
        }
        let Some(page_ordinal) = self.manifest.inode_index_page_for_inode(inode) else {
            return Ok(None);
        };
        self.inode_from_index_page(page_ordinal, inode).await
    }

    async fn read_index_object(
        &self,
        reference: &super::wire::PackedContainerRef,
        kind: PackedObjectKind,
    ) -> PackedResult<Vec<u8>> {
        let key = std::str::from_utf8(&reference.object_key)
            .map_err(|_| PackedWireError::Invalid("packed index object key is not UTF-8".into()))?;
        // Index pages are independently bounded (4096 records) and are
        // already authenticated as a whole object. One bounded range for the
        // complete page avoids the header/body/footer RTT triplet used by a
        // payload container while still consuming the response as a stream.
        if reference.object_len > super::remote::MAX_PACKED_STREAM_RANGE_BYTES {
            return Err(PackedWireError::LimitExceeded(
                "packed index object exceeds the bounded page read limit".into(),
            ));
        }
        let object = read_exact_range(&self.client, key, 0, reference.object_len).await?;
        if object.len() as u64 != reference.object_len {
            return Err(PackedWireError::Invalid(
                "packed index object length disagrees with manifest".into(),
            ));
        }
        let envelope = super::wire::PackedEnvelope::parse(object.clone())?;
        if envelope.header.kind != kind {
            return Err(PackedWireError::Invalid(
                "packed index object kind does not match the requested reader".into(),
            ));
        }
        let digest: [u8; 32] = Sha256::digest(&object).into();
        if digest != reference.object_digest {
            return Err(PackedWireError::Invalid(
                "packed index object digest mismatch".into(),
            ));
        }
        Ok(object)
    }

    async fn load_frame_descriptors(
        &self,
        container_ordinal: u32,
        ordinals: impl IntoIterator<Item = u32>,
    ) -> PackedResult<Vec<PackedFrameDescriptor>> {
        let mut requested: Vec<u32> = ordinals.into_iter().collect();
        requested.sort_unstable();
        requested.dedup();
        if requested.is_empty() {
            return Ok(Vec::new());
        }
        let mut descriptors = HashMap::with_capacity(requested.len());
        let mut missing = Vec::new();
        for ordinal in &requested {
            if let Some(descriptor) = self
                .frame_descriptors
                .get(&(container_ordinal, *ordinal))
                .await
            {
                descriptors.insert(*ordinal, descriptor);
            } else {
                missing.push(*ordinal);
            }
        }
        if !missing.is_empty() {
            // Demand reads must remain bounded to the referenced descriptor
            // runs. A full frame directory is reserved for mount-time
            // metadata warm-up; loading it here would make one random file
            // pay for every frame in its container.
            if let Some(directory) = self.frame_directories.get(&container_ordinal).await {
                for ordinal in missing {
                    let descriptor = directory
                        .iter()
                        .find(|descriptor| descriptor.frame_ordinal == ordinal)
                        .cloned()
                        .ok_or_else(|| {
                            PackedWireError::Invalid("packed frame descriptor is missing".into())
                        })?;
                    self.frame_descriptors
                        .insert((container_ordinal, ordinal), descriptor.clone())
                        .await;
                    descriptors.insert(ordinal, descriptor);
                }
            } else {
                let (remote_gets, remote_bytes) = frame_descriptor_range_stats(&missing);
                let remote = self.open_container(container_ordinal).await?;
                let fetched = remote
                    .read_frame_descriptors(missing.iter().copied())
                    .await?;
                self.frame_descriptor_remote_gets
                    .fetch_add(remote_gets, Ordering::Relaxed);
                self.frame_descriptor_remote_bytes
                    .fetch_add(remote_bytes, Ordering::Relaxed);
                for descriptor in fetched {
                    let ordinal = descriptor.frame_ordinal;
                    self.frame_descriptors
                        .insert((container_ordinal, ordinal), descriptor.clone())
                        .await;
                    descriptors.insert(ordinal, descriptor);
                }
            }
        }
        requested
            .into_iter()
            .map(|ordinal| {
                descriptors.remove(&ordinal).ok_or_else(|| {
                    PackedWireError::Invalid("packed frame descriptor is missing".into())
                })
            })
            .collect()
    }

    async fn load_frame_directory_shared(
        &self,
        container_ordinal: u32,
    ) -> PackedResult<Arc<Vec<PackedFrameDescriptor>>> {
        let catalog = self.clone();
        self.frame_directories
            .try_get_with(container_ordinal, async move {
                let remote = catalog.open_container(container_ordinal).await?;
                let frames = remote.read_frame_directory().await?;
                if frames.len() > MAX_FRAME_DESCRIPTORS as usize {
                    return Err(PackedWireError::LimitExceeded(
                        "packed frame directory exceeds cache limit".into(),
                    ));
                }
                // read_frame_directory uses one prefix range and one bounded
                // record range for ordinary containers. Count the actual
                // logical range calls, including paged large frame tables.
                let count = u64::try_from(frames.len()).unwrap_or(u64::MAX);
                let records = count.saturating_mul(FRAME_RECORD_LEN as u64);
                let records_per_range =
                    super::remote::MAX_PACKED_STREAM_RANGE_BYTES / FRAME_RECORD_LEN as u64;
                let ranges = 1 + count.div_ceil(records_per_range);
                catalog
                    .frame_directory_remote_gets
                    .fetch_add(ranges, Ordering::Relaxed);
                catalog
                    .frame_directory_remote_bytes
                    .fetch_add(24u64.saturating_add(records), Ordering::Relaxed);
                Ok(Arc::new(frames))
            })
            .await
            .map_err(|error| (*error).clone())
    }

    async fn open_container(
        &self,
        container_ordinal: u32,
    ) -> PackedResult<Arc<RemotePackedObject<B>>> {
        // Validate before cache admission. Failed opens are never cached, and
        // concurrent requests for the same ordinal share one initialization.
        let container = self
            .manifest
            .containers
            .get(container_ordinal as usize)
            .ok_or_else(|| PackedWireError::Invalid("packed group container is missing".into()))?;
        let object_key = std::str::from_utf8(&container.object_key)
            .map_err(|_| {
                PackedWireError::Invalid("packed container object key is not UTF-8".into())
            })?
            .to_owned();
        let object_len = container.object_len;
        let client = self.client.clone();
        let payload_cache = self.payload_cache.clone();
        let frame_window_cache = self.frame_window_cache.clone();
        let runtime_metrics = Arc::clone(&self.runtime_metrics);
        let snapshot_id = self.manifest.snapshot_id;
        let object_digest = container.object_digest;
        self.containers
            .try_get_with(container_ordinal, async move {
                let object = if let Some(payload_cache) = payload_cache {
                    let cache_key = format!(
                        "packed-v3/{}/container-{container_ordinal:08x}-{}",
                        hex::encode(snapshot_id),
                        hex::encode(object_digest)
                    );
                    RemotePackedObject::open_with_payload_cache(
                        &client,
                        &object_key,
                        object_len,
                        PackedObjectKind::GroupContainer,
                        payload_cache,
                        cache_key,
                    )
                    .await?
                } else {
                    RemotePackedObject::open(
                        &client,
                        &object_key,
                        object_len,
                        PackedObjectKind::GroupContainer,
                    )
                    .await?
                };
                Ok::<_, PackedWireError>(Arc::new(
                    object
                        .with_runtime_metrics(runtime_metrics)
                        .with_shared_frame_window_cache(frame_window_cache),
                ))
            })
            .await
            .map_err(|error| (*error).clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::client::ObjectBackend;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::workspace_overlay::packed_v3::{
        AccessProfile, GroupMetaEntry, GroupMetaExtent, PackedContainerRef, PackedFrameInput,
        PackedGroupContainer, PackedGroupInput, PackedSnapshotManifest, SizeClass,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use tempfile::tempdir;

    #[derive(Clone)]
    struct RecordingBackend {
        inner: LocalFsBackend,
        ranges: Arc<Mutex<Vec<(u64, u64)>>>,
    }

    #[async_trait]
    impl ObjectBackend for RecordingBackend {
        async fn put_object(&self, key: &str, data: &[u8]) -> Result<()> {
            self.inner.put_object(key, data).await
        }

        async fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.inner.get_object(key).await
        }

        async fn get_object_range(&self, key: &str, offset: u64, buf: &mut [u8]) -> Result<usize> {
            self.ranges
                .lock()
                .expect("range recorder mutex poisoned")
                .push((offset, buf.len() as u64));
            self.inner.get_object_range(key, offset, buf).await
        }

        async fn get_etag(&self, key: &str) -> Result<String> {
            self.inner.get_etag(key).await
        }

        async fn delete_object(&self, key: &str) -> Result<()> {
            self.inner.delete_object(key).await
        }
    }

    fn meta_bytes() -> Vec<u8> {
        super::super::meta::GroupMeta::new(vec![GroupMetaEntry {
            name: b"sample.bin".to_vec(),
            inode: 7,
            kind: 1,
            mode: 0o100644,
            uid: 0,
            gid: 0,
            rdev: 0,
            nlink: 1,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            size: 7,
            flags: 0,
            inline_data: Arc::from([]),
            extents: vec![GroupMetaExtent {
                file_offset: 0,
                logical_len: 7,
                frame_ordinal: 0,
                raw_offset: 0,
                raw_len: 7,
            }],
        }])
        .unwrap()
        .encode()
        .unwrap()
    }

    fn inline_meta_bytes() -> Vec<u8> {
        super::super::meta::GroupMeta::new(vec![GroupMetaEntry {
            name: b"tiny.bin".to_vec(),
            inode: 8,
            kind: 1,
            mode: 0o100644,
            uid: 0,
            gid: 0,
            rdev: 0,
            nlink: 1,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            size: 7,
            flags: super::super::meta::INLINE_DATA_FLAG,
            inline_data: Arc::from(b"payload".as_slice()),
            extents: Vec::new(),
        }])
        .unwrap()
        .encode()
        .unwrap()
    }

    fn two_file_meta_bytes() -> Vec<u8> {
        super::super::meta::GroupMeta::new(vec![
            GroupMetaEntry {
                name: b"a.bin".to_vec(),
                inode: 7,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                size: 7,
                flags: 0,
                inline_data: Arc::from([]),
                extents: vec![GroupMetaExtent {
                    file_offset: 0,
                    logical_len: 7,
                    frame_ordinal: 0,
                    raw_offset: 0,
                    raw_len: 7,
                }],
            },
            GroupMetaEntry {
                name: b"b.bin".to_vec(),
                inode: 8,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                size: 7,
                flags: 0,
                inline_data: Arc::from([]),
                extents: vec![GroupMetaExtent {
                    file_offset: 0,
                    logical_len: 7,
                    frame_ordinal: 1,
                    raw_offset: 0,
                    raw_len: 7,
                }],
            },
        ])
        .unwrap()
        .encode()
        .unwrap()
    }

    #[tokio::test]
    async fn metadata_warmup_admits_inode_entries_for_hot_lookup() {
        let temp = tempdir().unwrap();
        let backend = LocalFsBackend::new(temp.path());
        let client = ObjectClient::new(backend.clone());
        let page = PackedInodeIndexPage {
            snapshot_id: [41; 32],
            page_ordinal: 0,
            total_pages: 1,
            entries: vec![
                super::super::index::PackedInodeIndexEntry {
                    inode: 7,
                    parent_inode: 1,
                    parent_dir_key: [3; 32],
                    group_id: 1,
                    entry_ordinal: 0,
                    name: b"a.bin".to_vec(),
                    kind: 1,
                    mode: 0o100644,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    nlink: 1,
                    atime_ns: 0,
                    mtime_ns: 0,
                    ctime_ns: 0,
                    size: 7,
                },
                super::super::index::PackedInodeIndexEntry {
                    inode: 8,
                    parent_inode: 1,
                    parent_dir_key: [3; 32],
                    group_id: 1,
                    entry_ordinal: 1,
                    name: b"b.bin".to_vec(),
                    kind: 1,
                    mode: 0o100644,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    nlink: 1,
                    atime_ns: 0,
                    mtime_ns: 0,
                    ctime_ns: 0,
                    size: 7,
                },
            ],
        }
        .encode()
        .unwrap();
        client.put_object("inode-index", &page).await.unwrap();
        let catalog = RemoteGroupCatalog::new(
            client,
            PackedSnapshotManifest {
                snapshot_id: [41; 32],
                root_dir_key: [3; 32],
                root_inode: 1,
                layout_profile: AccessProfile::RandomSmallFile,
                size_classes: Default::default(),
                groups: Vec::new(),
                containers: Vec::new(),
                group_index_pages: Vec::new(),
                inode_index_pages: vec![super::super::wire::PackedInodeIndexPageRef {
                    object: PackedContainerRef {
                        object_key: b"inode-index".to_vec(),
                        object_len: page.len() as u64,
                        object_digest: Sha256::digest(&page).into(),
                    },
                    first_inode: 7,
                    last_inode: 8,
                }],
            },
        )
        .with_metadata_cache_bytes(1024 * 1024);

        let warmup = catalog.prefetch_metadata_adaptive(2).await.unwrap();
        assert_eq!(warmup.inode_index_pages, 1);
        assert_eq!(warmup.inode_entries, 2);
        assert_eq!(warmup.inode_entries_skipped, 0);
        assert!(warmup.inode_entry_bytes >= 256);
        catalog.inode_entries.run_pending_tasks().await;
        let before = catalog.metadata_cache_stats();
        let entry = catalog.inode_from_index_page(0, 8).await.unwrap().unwrap();
        let after = catalog.metadata_cache_stats();
        assert_eq!(entry.name, b"b.bin");
        assert_eq!(after.inode_entry_hits, before.inode_entry_hits + 1);
        assert_eq!(after.inode_index_remote_gets, 1);
        assert!(after.inode_entry_entries >= 2);
    }

    #[tokio::test]
    async fn catalog_fetches_only_the_group_metadata_range() {
        let temp = tempdir().unwrap();
        let backend = LocalFsBackend::new(temp.path());
        let client = ObjectClient::new(backend.clone());
        let metadata = meta_bytes();
        let object = PackedGroupContainer::build(
            11,
            AccessProfile::RandomSmallFile,
            vec![PackedGroupInput {
                group_id: 1,
                parent_dir_key: [3; 32],
                metadata: metadata.clone(),
                frame_ordinals: vec![0],
                entry_count: 1,
                file_count: 1,
                layout_profile: AccessProfile::RandomSmallFile,
            }],
            vec![PackedFrameInput {
                raw: b"payload".to_vec(),
                size_class: SizeClass::Tiny,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 0,
            }],
        )
        .unwrap();
        let opened = PackedGroupContainer::open(object.clone()).unwrap();
        client.put_object("container", &object).await.unwrap();
        let manifest = PackedSnapshotManifest {
            snapshot_id: [9; 32],
            root_dir_key: [8; 32],
            root_inode: 1,
            layout_profile: AccessProfile::RandomSmallFile,
            size_classes: Default::default(),
            groups: vec![super::super::wire::PackedGroupRef {
                group_id: 1,
                container_ordinal: 0,
                parent_dir_key: [3; 32],
                first_name: b"sample.bin".to_vec(),
                last_name: b"sample.bin".to_vec(),
                meta_offset: opened.groups()[0].metadata_offset,
                meta_len: opened.groups()[0].metadata_len,
                data_offset: opened.groups()[0].data_offset,
                data_len: opened.groups()[0].data_len,
                entry_count: 1,
                file_count: 1,
                frame_count: 1,
                layout_profile: AccessProfile::RandomSmallFile,
                metadata_digest: Sha256::digest(&metadata).into(),
                data_digest: opened.groups()[0].data_digest,
            }],
            containers: vec![PackedContainerRef {
                object_key: b"container".to_vec(),
                object_len: opened.object_len(),
                object_digest: opened.object_digest(),
            }],
            group_index_pages: Vec::new(),
            inode_index_pages: Vec::new(),
        };
        let catalog = RemoteGroupCatalog::new(client, manifest);
        let page = catalog.load_group_meta_page(1, 0, 1).await.unwrap();
        assert_eq!(page[0].name, b"sample.bin");
        let directory_page = catalog.readdir_page([3; 32], 0, 1).await.unwrap();
        assert_eq!(directory_page[0].inode, 7);
        catalog.file_locators.run_pending_tasks().await;
        let locator_hits_before = catalog.metadata_cache_stats().locator_hits;
        assert!(catalog.lookup_inode_locator(7).await.unwrap().is_some());
        let locator_stats = catalog.metadata_cache_stats();
        assert!(locator_stats.file_locator_entries >= 1);
        assert!(locator_stats.locator_hits > locator_hits_before);
        let entry = catalog
            .lookup_entry([3; 32], b"sample.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.inode, 7);
        let plan = catalog
            .read_unified_plan(1, b"sample.bin", 1, 4)
            .await
            .unwrap();
        assert_eq!(
            plan.generation,
            crate::chunk::read_plan::ReadGeneration::readonly([9; 32])
        );
        assert_eq!(plan.segments.len(), 1);
        let remote = super::super::remote::RemotePackedObject::open(
            catalog.client(),
            "container",
            opened.object_len(),
            PackedObjectKind::GroupContainer,
        )
        .await
        .unwrap();
        let fetcher = super::super::remote::PackedFrameSourceFetcher::from_object(remote, 0);
        let mut bytes = [0u8; 4];
        crate::chunk::read_plan::execute_unified_into(&fetcher, 1, &plan, &mut bytes)
            .await
            .unwrap();
        assert_eq!(&bytes, b"aylo");
        assert!(
            catalog
                .lookup_entry([3; 32], b"missing.bin")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn adaptive_metadata_warmup_keeps_a_budgeted_group_prefix() {
        let temp = tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        let metadata = inline_meta_bytes();
        let object = PackedGroupContainer::build(
            14,
            AccessProfile::RandomSmallFile,
            vec![
                PackedGroupInput {
                    group_id: 1,
                    parent_dir_key: [3; 32],
                    metadata: metadata.clone(),
                    frame_ordinals: Vec::new(),
                    entry_count: 1,
                    file_count: 1,
                    layout_profile: AccessProfile::RandomSmallFile,
                },
                PackedGroupInput {
                    group_id: 2,
                    parent_dir_key: [3; 32],
                    metadata: metadata.clone(),
                    frame_ordinals: Vec::new(),
                    entry_count: 1,
                    file_count: 1,
                    layout_profile: AccessProfile::RandomSmallFile,
                },
            ],
            Vec::new(),
        )
        .unwrap();
        let opened = PackedGroupContainer::open(object.clone()).unwrap();
        client.put_object("container", &object).await.unwrap();
        let groups = opened
            .groups()
            .iter()
            .enumerate()
            .map(|(index, descriptor)| PackedGroupRef {
                group_id: index as u64 + 1,
                container_ordinal: 0,
                parent_dir_key: [3; 32],
                first_name: b"tiny.bin".to_vec(),
                last_name: b"tiny.bin".to_vec(),
                meta_offset: descriptor.metadata_offset,
                meta_len: descriptor.metadata_len,
                data_offset: descriptor.data_offset,
                data_len: descriptor.data_len,
                entry_count: 1,
                file_count: 1,
                frame_count: 0,
                layout_profile: AccessProfile::RandomSmallFile,
                metadata_digest: Sha256::digest(&metadata).into(),
                data_digest: descriptor.data_digest,
            })
            .collect();
        let metadata_len = u64::try_from(metadata.len()).unwrap();
        let catalog = RemoteGroupCatalog::new(
            client,
            PackedSnapshotManifest {
                snapshot_id: [14; 32],
                root_dir_key: [3; 32],
                root_inode: 1,
                layout_profile: AccessProfile::RandomSmallFile,
                size_classes: Default::default(),
                groups,
                containers: vec![PackedContainerRef {
                    object_key: b"container".to_vec(),
                    object_len: opened.object_len(),
                    object_digest: opened.object_digest(),
                }],
                group_index_pages: Vec::new(),
                inode_index_pages: Vec::new(),
            },
        )
        .with_metadata_cache_bytes(metadata_len.saturating_mul(4));
        let warmup = catalog.prefetch_metadata_adaptive(2).await.unwrap();
        assert_eq!(warmup.group_meta_pages, 1);
        assert_eq!(warmup.group_meta_pages_skipped, 1);
        catalog.group_meta_pages.run_pending_tasks().await;
        assert_eq!(catalog.metadata_cache_stats().group_meta_entries, 1);
    }

    #[tokio::test]
    async fn concurrent_inode_reads_share_one_payload_range() {
        let temp = tempdir().unwrap();
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let backend = RecordingBackend {
            inner: LocalFsBackend::new(temp.path()),
            ranges: Arc::clone(&ranges),
        };
        let client = ObjectClient::new(backend);
        let metadata = two_file_meta_bytes();
        let object = PackedGroupContainer::build(
            12,
            AccessProfile::RandomSmallFile,
            vec![PackedGroupInput {
                group_id: 1,
                parent_dir_key: [4; 32],
                metadata: metadata.clone(),
                frame_ordinals: vec![0, 1],
                entry_count: 2,
                file_count: 2,
                layout_profile: AccessProfile::RandomSmallFile,
            }],
            vec![
                PackedFrameInput {
                    raw: b"aaaaaaa".to_vec(),
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 0,
                    last_file_slot: 0,
                },
                PackedFrameInput {
                    raw: b"bbbbbbb".to_vec(),
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 1,
                    last_file_slot: 1,
                },
            ],
        )
        .unwrap();
        let opened = PackedGroupContainer::open(object.clone()).unwrap();
        client.put_object("container", &object).await.unwrap();
        let manifest = PackedSnapshotManifest {
            snapshot_id: [10; 32],
            root_dir_key: [11; 32],
            root_inode: 1,
            layout_profile: AccessProfile::RandomSmallFile,
            size_classes: Default::default(),
            groups: vec![super::super::wire::PackedGroupRef {
                group_id: 1,
                container_ordinal: 0,
                parent_dir_key: [4; 32],
                first_name: b"a.bin".to_vec(),
                last_name: b"b.bin".to_vec(),
                meta_offset: opened.groups()[0].metadata_offset,
                meta_len: opened.groups()[0].metadata_len,
                data_offset: opened.groups()[0].data_offset,
                data_len: opened.groups()[0].data_len,
                entry_count: 2,
                file_count: 2,
                frame_count: 2,
                layout_profile: AccessProfile::RandomSmallFile,
                metadata_digest: Sha256::digest(&metadata).into(),
                data_digest: opened.groups()[0].data_digest,
            }],
            containers: vec![PackedContainerRef {
                object_key: b"container".to_vec(),
                object_len: opened.object_len(),
                object_digest: opened.object_digest(),
            }],
            group_index_pages: Vec::new(),
            inode_index_pages: Vec::new(),
        };
        let catalog = Arc::new(RemoteGroupCatalog::new(client, manifest));
        let warmup = catalog.prefetch_metadata_adaptive(2).await.unwrap();
        assert_eq!(warmup.locator_entries, 2);
        assert_eq!(warmup.locator_entries_skipped, 0);
        let first = Arc::clone(&catalog);
        let second = Arc::clone(&catalog);
        let (first, second) = tokio::join!(
            async move {
                let mut output = [0u8; 7];
                first.read_inode_range(7, 0, &mut output).await.unwrap();
                output
            },
            async move {
                let mut output = [0u8; 7];
                second.read_inode_range(8, 0, &mut output).await.unwrap();
                output
            }
        );
        assert_eq!(&first, b"aaaaaaa");
        assert_eq!(&second, b"bbbbbbb");

        let first_offset = opened.frames()[0].object_offset;
        let last_end = opened.frames()[1].object_offset + u64::from(opened.frames()[1].stored_len);
        let payload_ranges = ranges
            .lock()
            .unwrap()
            .iter()
            .filter(|(offset, _length)| *offset >= first_offset && *offset <= last_end)
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(
            payload_ranges.len(),
            1,
            "payload ranges: {payload_ranges:?}"
        );
        assert_eq!(payload_ranges[0].0, first_offset);
        assert_eq!(payload_ranges[0].1, last_end - first_offset);

        let first_locator = catalog.lookup_inode_locator(7).await.unwrap().unwrap();
        let second_locator = catalog.lookup_inode_locator(7).await.unwrap().unwrap();
        assert!(Arc::ptr_eq(&first_locator, &second_locator));
        catalog.file_locators.run_pending_tasks().await;
        let stats = catalog.metadata_cache_stats();
        assert!(stats.hits >= 2, "metadata cache stats: {stats:?}");
        assert!(stats.file_locator_entries >= 2);
        let runtime = catalog.packed_runtime_metrics();
        assert_eq!(runtime.data_range_gets, 1);
        assert_eq!(runtime.logical_bytes, 14);
        assert_eq!(runtime.frames_decoded, 2);
        assert_eq!(runtime.coalesced_ranges, 1);
        assert!(runtime.pipeline_bytes_peak > 0);
    }

    #[tokio::test]
    async fn dynamic_size_class_corpus_reads_inline_and_split_frames_and_rejects_unproven_p90() {
        use super::super::{
            PackedFileInput, SizeClassTable, choose_frame_layout, pack_group_files,
        };

        for profile in [
            AccessProfile::RandomSmallFile,
            AccessProfile::SequentialSmallFile,
        ] {
            for (size, hint) in [
                (200 * 1024, None),
                (512 * 1024, None),
                (1024 * 1024, None),
                (10 * 1024 * 1024, None),
                (32 * 1024 * 1024, None),
                (10 * 1024 * 1024, Some(200 * 1024)),
            ] {
                let temp = tempdir().unwrap();
                let ranges = Arc::new(Mutex::new(Vec::new()));
                let client = ObjectClient::new(RecordingBackend {
                    inner: LocalFsBackend::new(temp.path()),
                    ranges: Arc::clone(&ranges),
                });
                let payload = |inode: u64| -> Vec<u8> {
                    (0..size)
                        .map(|offset| {
                            let mixed = (offset as u64)
                                .wrapping_mul(6_364_136_223_846_793_005)
                                .wrapping_add(inode.wrapping_mul(1_442_695_040_888_963_407));
                            (mixed ^ (mixed >> 32) ^ (mixed >> 56)) as u8
                        })
                        .collect()
                };
                let file = |name: &[u8], inode| PackedFileInput {
                    name: name.to_vec(),
                    inode,
                    kind: 1,
                    mode: 0o100644,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    nlink: 1,
                    atime_ns: 0,
                    mtime_ns: 0,
                    ctime_ns: 0,
                    flags: 0,
                    data: payload(inode),
                };
                let mut files = vec![file(b"a", 7)];
                // The second tiny file exceeds the group inline budget and
                // exercises the Tiny frame path on the same published layout.
                if size == 200 * 1024 {
                    files.push(file(b"b", 8));
                }
                let table = SizeClassTable::default();
                let decision = choose_frame_layout(size as u64, hint, profile, table).unwrap();
                let packed = pack_group_files(1, [4; 32], files, profile, table, hint);
                if hint.is_some() {
                    // A caller-supplied hint has no authenticated histogram
                    // provenance and must fail before publishing any object.
                    assert!(matches!(
                        packed,
                        Err(PackedWireError::UnsupportedFormat(message))
                            if message.contains("authenticated histogram provenance")
                    ));
                    assert!(ranges.lock().unwrap().is_empty());
                    continue;
                }
                let (input, frames) = packed.unwrap();
                let meta = GroupMeta::decode(&input.metadata).unwrap();
                let object = PackedGroupContainer::build(1, profile, vec![input], frames).unwrap();
                let opened = PackedGroupContainer::open(object.clone()).unwrap();
                for frame in opened.frames() {
                    assert_eq!(frame.size_class, decision.size_class);
                    assert_eq!(frame.codec, 0);
                    assert_eq!(frame.raw_len, frame.stored_len);
                    assert!(u64::from(frame.raw_len) <= decision.frame_raw_bytes);
                }
                if size >= 256 * 1024 {
                    assert_eq!(opened.frames().len() as u64, decision.frame_count);
                    assert_eq!(meta.entries()[0].extents.len() as u64, decision.frame_count);
                } else {
                    assert_eq!(meta.entries()[0].inline_data.len(), size);
                    assert_eq!(meta.entries()[1].extents.len(), 1);
                }
                client
                    .put_object("size-class-container", &object)
                    .await
                    .unwrap();
                let descriptor = &opened.groups()[0];
                let manifest = PackedSnapshotManifest {
                    snapshot_id: [3; 32],
                    root_dir_key: [4; 32],
                    root_inode: 1,
                    layout_profile: profile,
                    size_classes: table,
                    groups: vec![PackedGroupRef {
                        group_id: 1,
                        container_ordinal: 0,
                        parent_dir_key: [4; 32],
                        first_name: b"a".to_vec(),
                        last_name: meta.entries().last().unwrap().name.clone(),
                        meta_offset: descriptor.metadata_offset,
                        meta_len: descriptor.metadata_len,
                        data_offset: descriptor.data_offset,
                        data_len: descriptor.data_len,
                        entry_count: descriptor.entry_count,
                        file_count: descriptor.file_count,
                        frame_count: descriptor.frame_ordinals.len() as u32,
                        layout_profile: profile,
                        metadata_digest: descriptor.metadata_digest,
                        data_digest: descriptor.data_digest,
                    }],
                    containers: vec![PackedContainerRef {
                        object_key: b"size-class-container".to_vec(),
                        object_len: opened.object_len(),
                        object_digest: opened.object_digest(),
                    }],
                    group_index_pages: Vec::new(),
                    inode_index_pages: Vec::new(),
                };
                let manifest = PackedSnapshotManifest::decode(manifest.encode().unwrap()).unwrap();
                let catalog = RemoteGroupCatalog::new(client, manifest);
                catalog.prefetch_metadata_adaptive(2).await.unwrap();
                for entry in meta.entries() {
                    let expected = payload(entry.inode);
                    let mut offsets = vec![0, size - 257, size / 2];
                    if decision.frame_count > 1 {
                        offsets.push(decision.frame_raw_bytes as usize - 128);
                    }
                    for offset in offsets {
                        let mut output = vec![0; 257];
                        catalog
                            .read_inode_range(entry.inode, offset as u64, &mut output)
                            .await
                            .unwrap();
                        assert_eq!(output, expected[offset..offset + 257]);
                    }
                    if !entry.inline_data.is_empty() {
                        assert_eq!(catalog.packed_runtime_metrics().data_range_gets, 0);
                    }
                }
                assert!(!ranges.lock().unwrap().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn decoded_frame_cache_reuses_a_shared_frame_for_adjacent_files() {
        let temp = tempdir().unwrap();
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let client = ObjectClient::new(RecordingBackend {
            inner: LocalFsBackend::new(temp.path()),
            ranges: Arc::clone(&ranges),
        });
        let metadata = super::super::meta::GroupMeta::new(vec![
            GroupMetaEntry {
                name: b"a.bin".to_vec(),
                inode: 7,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                size: 7,
                flags: 0,
                inline_data: Arc::from([]),
                extents: vec![GroupMetaExtent {
                    file_offset: 0,
                    logical_len: 7,
                    frame_ordinal: 0,
                    raw_offset: 0,
                    raw_len: 14,
                }],
            },
            GroupMetaEntry {
                name: b"b.bin".to_vec(),
                inode: 8,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                size: 7,
                flags: 0,
                inline_data: Arc::from([]),
                extents: vec![GroupMetaExtent {
                    file_offset: 0,
                    logical_len: 7,
                    frame_ordinal: 0,
                    raw_offset: 7,
                    raw_len: 14,
                }],
            },
        ])
        .unwrap()
        .encode()
        .unwrap();
        let object = PackedGroupContainer::build(
            91,
            AccessProfile::RandomSmallFile,
            vec![PackedGroupInput {
                group_id: 1,
                parent_dir_key: [4; 32],
                metadata: metadata.clone(),
                frame_ordinals: vec![0],
                entry_count: 2,
                file_count: 2,
                layout_profile: AccessProfile::RandomSmallFile,
            }],
            vec![PackedFrameInput {
                raw: b"aaaaaaabbbbbbb".to_vec(),
                size_class: SizeClass::Tiny,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 1,
            }],
        )
        .unwrap();
        let opened = PackedGroupContainer::open(object.clone()).unwrap();
        client
            .put_object("shared-container", &object)
            .await
            .unwrap();
        let manifest = PackedSnapshotManifest {
            snapshot_id: [90; 32],
            root_dir_key: [11; 32],
            root_inode: 1,
            layout_profile: AccessProfile::RandomSmallFile,
            size_classes: Default::default(),
            groups: vec![super::super::wire::PackedGroupRef {
                group_id: 1,
                container_ordinal: 0,
                parent_dir_key: [4; 32],
                first_name: b"a.bin".to_vec(),
                last_name: b"b.bin".to_vec(),
                meta_offset: opened.groups()[0].metadata_offset,
                meta_len: opened.groups()[0].metadata_len,
                data_offset: opened.groups()[0].data_offset,
                data_len: opened.groups()[0].data_len,
                entry_count: 2,
                file_count: 2,
                frame_count: 1,
                layout_profile: AccessProfile::RandomSmallFile,
                metadata_digest: Sha256::digest(&metadata).into(),
                data_digest: opened.groups()[0].data_digest,
            }],
            containers: vec![PackedContainerRef {
                object_key: b"shared-container".to_vec(),
                object_len: opened.object_len(),
                object_digest: opened.object_digest(),
            }],
            group_index_pages: Vec::new(),
            inode_index_pages: Vec::new(),
        };
        let catalog =
            RemoteGroupCatalog::new(client, manifest).with_decoded_frame_cache_bytes(1024);
        catalog.prefetch_metadata_adaptive(2).await.unwrap();
        ranges.lock().unwrap().clear();

        let mut oversized = [0u8; 8];
        assert!(
            catalog
                .read_inode_range(7, 0, &mut oversized)
                .await
                .is_err()
        );
        assert_eq!(catalog.packed_runtime_metrics().logical_bytes, 0);

        let mut first = [0u8; 7];
        catalog.read_inode_range(7, 0, &mut first).await.unwrap();
        catalog
            .decoded_frames
            .as_ref()
            .unwrap()
            .run_pending_tasks()
            .await;
        let frame = &opened.frames()[0];
        assert_eq!(
            ranges
                .lock()
                .unwrap()
                .iter()
                .filter(|(offset, _)| *offset == frame.object_offset)
                .count(),
            1
        );
        catalog.containers.invalidate_all();
        catalog.containers.run_pending_tasks().await;
        ranges.lock().unwrap().clear();

        let mut second = [0u8; 7];
        catalog.read_inode_range(8, 0, &mut second).await.unwrap();

        assert_eq!(&first, b"aaaaaaa");
        assert_eq!(&second, b"bbbbbbb");
        assert!(
            ranges.lock().unwrap().is_empty(),
            "a decoded-frame hit must not reopen the container"
        );
        let runtime = catalog.packed_runtime_metrics();
        assert_eq!(runtime.data_range_gets, 1);
        assert_eq!(runtime.logical_bytes, 14);
        assert_eq!(runtime.decoded_frame_cache_configured_bytes, 1024);
        assert_eq!(runtime.decoded_frame_cache_entries, 1);
        assert_eq!(runtime.decoded_frame_cache_resident_bytes, 14);
        assert_eq!(runtime.decoded_frame_cache_hits, 1);
        assert_eq!(runtime.decoded_frame_cache_misses, 1);
    }

    #[tokio::test]
    async fn catalog_reads_a_container_frame_directory_once_for_multiple_files() {
        let temp = tempdir().unwrap();
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let backend = RecordingBackend {
            inner: LocalFsBackend::new(temp.path()),
            ranges: Arc::clone(&ranges),
        };
        let client = ObjectClient::new(backend);
        let metadata = super::super::meta::GroupMeta::new(vec![
            GroupMetaEntry {
                name: b"a.bin".to_vec(),
                inode: 7,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                size: 3,
                flags: 0,
                inline_data: Arc::from([]),
                extents: vec![GroupMetaExtent {
                    file_offset: 0,
                    logical_len: 3,
                    frame_ordinal: 0,
                    raw_offset: 0,
                    raw_len: 3,
                }],
            },
            GroupMetaEntry {
                name: b"b.bin".to_vec(),
                inode: 8,
                kind: 1,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                size: 3,
                flags: 0,
                inline_data: Arc::from([]),
                extents: vec![GroupMetaExtent {
                    file_offset: 0,
                    logical_len: 3,
                    frame_ordinal: 1,
                    raw_offset: 0,
                    raw_len: 3,
                }],
            },
        ])
        .unwrap()
        .encode()
        .unwrap();
        let object = PackedGroupContainer::build(
            19,
            AccessProfile::RandomSmallFile,
            vec![PackedGroupInput {
                group_id: 3,
                parent_dir_key: [4; 32],
                metadata: metadata.clone(),
                frame_ordinals: vec![0, 1],
                entry_count: 2,
                file_count: 2,
                layout_profile: AccessProfile::RandomSmallFile,
            }],
            vec![
                PackedFrameInput {
                    raw: b"one".to_vec(),
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 0,
                    last_file_slot: 0,
                },
                PackedFrameInput {
                    raw: b"two".to_vec(),
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 1,
                    last_file_slot: 1,
                },
            ],
        )
        .unwrap();
        let opened = PackedGroupContainer::open(object.clone()).unwrap();
        client.put_object("container", &object).await.unwrap();
        let descriptor = &opened.groups()[0];
        let manifest = PackedSnapshotManifest {
            snapshot_id: [9; 32],
            root_dir_key: [8; 32],
            root_inode: 1,
            layout_profile: AccessProfile::RandomSmallFile,
            size_classes: Default::default(),
            groups: vec![super::super::wire::PackedGroupRef {
                group_id: 3,
                container_ordinal: 0,
                parent_dir_key: [4; 32],
                first_name: b"a.bin".to_vec(),
                last_name: b"b.bin".to_vec(),
                meta_offset: descriptor.metadata_offset,
                meta_len: descriptor.metadata_len,
                data_offset: descriptor.data_offset,
                data_len: descriptor.data_len,
                entry_count: 2,
                file_count: 2,
                frame_count: 2,
                layout_profile: AccessProfile::RandomSmallFile,
                metadata_digest: Sha256::digest(&metadata).into(),
                data_digest: descriptor.data_digest,
            }],
            containers: vec![PackedContainerRef {
                object_key: b"container".to_vec(),
                object_len: opened.object_len(),
                object_digest: opened.object_digest(),
            }],
            group_index_pages: Vec::new(),
            inode_index_pages: Vec::new(),
        };
        let catalog = RemoteGroupCatalog::new(client, manifest);
        // A single range worker must still plan all selected groups together;
        // concurrency limits in-flight requests, not metadata coalescing.
        let warmup = catalog.prefetch_metadata_adaptive(1).await.unwrap();
        assert_eq!(warmup.frame_directory_pages, 1);
        assert_eq!(warmup.frame_directory_pages_skipped, 0);
        let metadata_stats = catalog.metadata_cache_stats();
        assert_eq!(metadata_stats.frame_directory_remote_gets, 2);
        catalog.read_unified_plan(3, b"a.bin", 0, 3).await.unwrap();
        catalog.read_unified_plan(3, b"b.bin", 0, 3).await.unwrap();

        // One header range, one GroupMeta range, then the two bounded frame
        // directory ranges (prefix and records), regardless of file count.
        assert_eq!(ranges.lock().unwrap().len(), 4);
    }

    #[tokio::test]
    async fn metadata_warmup_coalesces_adjacent_groups_in_one_container() {
        let temp = tempdir().unwrap();
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let backend = RecordingBackend {
            inner: LocalFsBackend::new(temp.path()),
            ranges: Arc::clone(&ranges),
        };
        let client = ObjectClient::new(backend);
        let metadata = meta_bytes();
        let mut second_meta = GroupMeta::decode(&metadata).unwrap();
        for entry in second_meta.entries_mut() {
            for extent in &mut entry.extents {
                extent.frame_ordinal = 1;
            }
        }
        let second_metadata = second_meta.encode().unwrap();
        let object = PackedGroupContainer::build(
            29,
            AccessProfile::SequentialSmallFile,
            vec![
                PackedGroupInput {
                    group_id: 3,
                    parent_dir_key: [4; 32],
                    metadata: metadata.clone(),
                    frame_ordinals: vec![0],
                    entry_count: 1,
                    file_count: 1,
                    layout_profile: AccessProfile::SequentialSmallFile,
                },
                PackedGroupInput {
                    group_id: 4,
                    parent_dir_key: [5; 32],
                    metadata: second_metadata,
                    frame_ordinals: vec![1],
                    entry_count: 1,
                    file_count: 1,
                    layout_profile: AccessProfile::SequentialSmallFile,
                },
            ],
            vec![
                PackedFrameInput {
                    raw: b"oneoneo".to_vec(),
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 0,
                    last_file_slot: 0,
                },
                PackedFrameInput {
                    raw: b"twotwot".to_vec(),
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 0,
                    last_file_slot: 0,
                },
            ],
        )
        .unwrap();
        let opened = PackedGroupContainer::open(object.clone()).unwrap();
        client.put_object("container", &object).await.unwrap();
        let groups = opened
            .groups()
            .iter()
            .map(|descriptor| super::super::wire::PackedGroupRef {
                group_id: descriptor.group_id,
                container_ordinal: 0,
                parent_dir_key: descriptor.parent_dir_key,
                first_name: b"sample.bin".to_vec(),
                last_name: b"sample.bin".to_vec(),
                meta_offset: descriptor.metadata_offset,
                meta_len: descriptor.metadata_len,
                data_offset: descriptor.data_offset,
                data_len: descriptor.data_len,
                entry_count: descriptor.entry_count,
                file_count: descriptor.file_count,
                frame_count: 1,
                layout_profile: descriptor.layout_profile,
                metadata_digest: descriptor.metadata_digest,
                data_digest: descriptor.data_digest,
            })
            .collect::<Vec<_>>();
        let metadata_start = groups.iter().map(|group| group.meta_offset).min().unwrap();
        let metadata_end = groups
            .iter()
            .map(|group| group.meta_offset + u64::from(group.meta_len))
            .max()
            .unwrap();
        let catalog = RemoteGroupCatalog::new(
            client,
            PackedSnapshotManifest {
                snapshot_id: [29; 32],
                root_dir_key: [8; 32],
                root_inode: 1,
                layout_profile: AccessProfile::SequentialSmallFile,
                size_classes: Default::default(),
                groups,
                containers: vec![PackedContainerRef {
                    object_key: b"container".to_vec(),
                    object_len: opened.object_len(),
                    object_digest: opened.object_digest(),
                }],
                group_index_pages: Vec::new(),
                inode_index_pages: Vec::new(),
            },
        );

        let warmup = catalog.prefetch_metadata_adaptive(2).await.unwrap();
        assert_eq!(warmup.group_meta_pages, 2);
        let stats = catalog.metadata_cache_stats();
        assert_eq!(stats.group_meta_remote_gets, 1);
        assert_eq!(stats.group_meta_remote_bytes, metadata_end - metadata_start);
        let metadata_ranges = ranges
            .lock()
            .unwrap()
            .iter()
            .filter(|(offset, length)| {
                *offset == metadata_start && *length == metadata_end - metadata_start
            })
            .count();
        assert_eq!(metadata_ranges, 1);
    }

    #[tokio::test]
    async fn readdir_skips_preceding_groups_without_fetching_their_metadata() {
        let temp = tempdir().unwrap();
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let client = ObjectClient::new(RecordingBackend {
            inner: LocalFsBackend::new(temp.path()),
            ranges: Arc::clone(&ranges),
        });
        let mut first = GroupMeta::decode(&meta_bytes()).unwrap();
        first.entries_mut()[0].name = b"a.bin".to_vec();
        let first_metadata = first.encode().unwrap();
        let mut second = GroupMeta::decode(&meta_bytes()).unwrap();
        second.entries_mut()[0].name = b"b.bin".to_vec();
        second.entries_mut()[0].inode = 8;
        second.entries_mut()[0].extents[0].frame_ordinal = 1;
        let second_metadata = second.encode().unwrap();
        let object = PackedGroupContainer::build(
            30,
            AccessProfile::RandomSmallFile,
            vec![
                PackedGroupInput {
                    group_id: 3,
                    parent_dir_key: [4; 32],
                    metadata: first_metadata.clone(),
                    frame_ordinals: vec![0],
                    entry_count: 1,
                    file_count: 1,
                    layout_profile: AccessProfile::RandomSmallFile,
                },
                PackedGroupInput {
                    group_id: 4,
                    parent_dir_key: [4; 32],
                    metadata: second_metadata.clone(),
                    frame_ordinals: vec![1],
                    entry_count: 1,
                    file_count: 1,
                    layout_profile: AccessProfile::RandomSmallFile,
                },
            ],
            vec![
                PackedFrameInput {
                    raw: b"oneoneo".to_vec(),
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 0,
                    last_file_slot: 0,
                },
                PackedFrameInput {
                    raw: b"twotwot".to_vec(),
                    size_class: SizeClass::Tiny,
                    codec: 0,
                    first_file_slot: 0,
                    last_file_slot: 0,
                },
            ],
        )
        .unwrap();
        let opened = PackedGroupContainer::open(object.clone()).unwrap();
        client.put_object("container", &object).await.unwrap();
        let groups = opened
            .groups()
            .iter()
            .enumerate()
            .map(|(index, descriptor)| super::super::wire::PackedGroupRef {
                group_id: descriptor.group_id,
                container_ordinal: 0,
                parent_dir_key: descriptor.parent_dir_key,
                first_name: if index == 0 { b"a.bin" } else { b"b.bin" }.to_vec(),
                last_name: if index == 0 { b"a.bin" } else { b"b.bin" }.to_vec(),
                meta_offset: descriptor.metadata_offset,
                meta_len: descriptor.metadata_len,
                data_offset: descriptor.data_offset,
                data_len: descriptor.data_len,
                entry_count: descriptor.entry_count,
                file_count: descriptor.file_count,
                frame_count: 1,
                layout_profile: descriptor.layout_profile,
                metadata_digest: descriptor.metadata_digest,
                data_digest: descriptor.data_digest,
            })
            .collect::<Vec<_>>();
        let skipped_meta_range = (groups[0].meta_offset, u64::from(groups[0].meta_len));
        let requested_meta_range = (groups[1].meta_offset, u64::from(groups[1].meta_len));
        let catalog = RemoteGroupCatalog::new(
            client,
            PackedSnapshotManifest {
                snapshot_id: [30; 32],
                root_dir_key: [4; 32],
                root_inode: 1,
                layout_profile: AccessProfile::RandomSmallFile,
                size_classes: Default::default(),
                groups,
                containers: vec![PackedContainerRef {
                    object_key: b"container".to_vec(),
                    object_len: opened.object_len(),
                    object_digest: opened.object_digest(),
                }],
                group_index_pages: Vec::new(),
                inode_index_pages: Vec::new(),
            },
        );

        let page = catalog.readdir_page([4; 32], 1, 1).await.unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].name, b"b.bin");
        let fetched = ranges.lock().unwrap();
        assert!(fetched.contains(&requested_meta_range));
        assert!(!fetched.contains(&skipped_meta_range));
    }

    #[tokio::test]
    async fn demand_read_fetches_only_referenced_frame_descriptors() {
        let temp = tempdir().unwrap();
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let backend = RecordingBackend {
            inner: LocalFsBackend::new(temp.path()),
            ranges: Arc::clone(&ranges),
        };
        let client = ObjectClient::new(backend);
        let metadata = meta_bytes();
        let frames = (0..4)
            .map(|ordinal| PackedFrameInput {
                raw: vec![b'a' + ordinal as u8; 7],
                size_class: SizeClass::Tiny,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 0,
            })
            .collect::<Vec<_>>();
        let object = PackedGroupContainer::build(
            21,
            AccessProfile::RandomSmallFile,
            vec![PackedGroupInput {
                group_id: 1,
                parent_dir_key: [3; 32],
                metadata: metadata.clone(),
                frame_ordinals: vec![0, 1, 2, 3],
                entry_count: 1,
                file_count: 1,
                layout_profile: AccessProfile::RandomSmallFile,
            }],
            frames,
        )
        .unwrap();
        let opened = PackedGroupContainer::open(object.clone()).unwrap();
        client.put_object("container", &object).await.unwrap();
        let descriptor = &opened.groups()[0];
        let catalog = RemoteGroupCatalog::new(
            client,
            PackedSnapshotManifest {
                snapshot_id: [21; 32],
                root_dir_key: [22; 32],
                root_inode: 1,
                layout_profile: AccessProfile::RandomSmallFile,
                size_classes: Default::default(),
                groups: vec![super::super::wire::PackedGroupRef {
                    group_id: 1,
                    container_ordinal: 0,
                    parent_dir_key: [3; 32],
                    first_name: b"sample.bin".to_vec(),
                    last_name: b"sample.bin".to_vec(),
                    meta_offset: descriptor.metadata_offset,
                    meta_len: descriptor.metadata_len,
                    data_offset: descriptor.data_offset,
                    data_len: descriptor.data_len,
                    entry_count: 1,
                    file_count: 1,
                    frame_count: 4,
                    layout_profile: AccessProfile::RandomSmallFile,
                    metadata_digest: Sha256::digest(&metadata).into(),
                    data_digest: descriptor.data_digest,
                }],
                containers: vec![PackedContainerRef {
                    object_key: b"container".to_vec(),
                    object_len: opened.object_len(),
                    object_digest: opened.object_digest(),
                }],
                group_index_pages: Vec::new(),
                inode_index_pages: Vec::new(),
            },
        );

        let plan = catalog
            .read_unified_plan(1, b"sample.bin", 0, 7)
            .await
            .unwrap();
        assert_eq!(plan.segments.len(), 1);
        let stats = catalog.metadata_cache_stats();
        assert_eq!(stats.frame_directory_remote_gets, 0);
        assert_eq!(stats.frame_directory_remote_bytes, 0);
        assert_eq!(stats.frame_descriptor_remote_gets, 2);
        assert_eq!(
            stats.frame_descriptor_remote_bytes,
            24 + FRAME_RECORD_LEN as u64
        );

        // The descriptor path probes the prefix and one record, not all four
        // records in the container's frame table.
        let recorded = ranges.lock().unwrap();
        assert_eq!(recorded.len(), 4); // header, GroupMeta, prefix, one record
        assert_eq!(recorded[2].1, 24);
        assert_eq!(recorded[3].1, FRAME_RECORD_LEN as u64);
    }

    #[tokio::test]
    async fn frame_window_budget_is_shared_across_containers() {
        let temp = tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        let frame_bytes = 3 * 1024 * 1024;
        let mut containers = Vec::new();
        for ordinal in 0..2u64 {
            let object = PackedGroupContainer::build(
                40 + ordinal,
                AccessProfile::SequentialSmallFile,
                vec![PackedGroupInput {
                    group_id: ordinal + 1,
                    parent_dir_key: [ordinal as u8; 32],
                    metadata: super::super::meta::GroupMeta::new(Vec::new())
                        .unwrap()
                        .encode()
                        .unwrap(),
                    frame_ordinals: vec![0],
                    entry_count: 0,
                    file_count: 0,
                    layout_profile: AccessProfile::SequentialSmallFile,
                }],
                vec![PackedFrameInput {
                    raw: vec![ordinal as u8; frame_bytes],
                    size_class: SizeClass::Medium,
                    codec: 0,
                    first_file_slot: 0,
                    last_file_slot: 0,
                }],
            )
            .unwrap();
            let key = format!("container-{ordinal}");
            client.put_object(&key, &object).await.unwrap();
            let opened = PackedGroupContainer::open(object).unwrap();
            containers.push((key, opened));
        }

        let manifest = PackedSnapshotManifest {
            snapshot_id: [7; 32],
            root_dir_key: [8; 32],
            root_inode: 1,
            layout_profile: AccessProfile::SequentialSmallFile,
            size_classes: Default::default(),
            groups: Vec::new(),
            containers: containers
                .iter()
                .map(|(key, object)| PackedContainerRef {
                    object_key: key.as_bytes().to_vec(),
                    object_len: object.object_len(),
                    object_digest: object.object_digest(),
                })
                .collect(),
            group_index_pages: Vec::new(),
            inode_index_pages: Vec::new(),
        };
        let budget = 4 * 1024 * 1024;
        let catalog =
            RemoteGroupCatalog::new(client, manifest).with_frame_window_cache_bytes(budget);

        for (ordinal, (_, opened)) in containers.iter().enumerate() {
            let remote = catalog.open_container(ordinal as u32).await.unwrap();
            let frame = &opened.frames()[0];
            let bytes = remote
                .read_windowed_payload_range(frame.object_offset, u64::from(frame.stored_len))
                .await
                .unwrap();
            assert_eq!(bytes.len(), frame_bytes);
            remote.run_window_cache_maintenance().await;
        }

        let cache = catalog.frame_window_cache.as_ref().unwrap();
        cache.run_pending_tasks().await;
        assert!(cache.weighted_size() <= budget);
        assert_eq!(cache.entry_count(), 1);
    }

    #[tokio::test]
    async fn catalog_reads_inline_file_without_a_frame_range() {
        let temp = tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(temp.path()));
        let metadata = inline_meta_bytes();
        let object = PackedGroupContainer::build(
            13,
            AccessProfile::RandomSmallFile,
            vec![PackedGroupInput {
                group_id: 2,
                parent_dir_key: [4; 32],
                metadata: metadata.clone(),
                frame_ordinals: Vec::new(),
                entry_count: 1,
                file_count: 1,
                layout_profile: AccessProfile::RandomSmallFile,
            }],
            Vec::new(),
        )
        .unwrap();
        let opened = PackedGroupContainer::open(object.clone()).unwrap();
        client.put_object("inline", &object).await.unwrap();
        let descriptor = &opened.groups()[0];
        let manifest = PackedSnapshotManifest {
            snapshot_id: [6; 32],
            root_dir_key: [5; 32],
            root_inode: 1,
            layout_profile: AccessProfile::RandomSmallFile,
            size_classes: Default::default(),
            groups: vec![super::super::wire::PackedGroupRef {
                group_id: 2,
                container_ordinal: 0,
                parent_dir_key: [4; 32],
                first_name: b"tiny.bin".to_vec(),
                last_name: b"tiny.bin".to_vec(),
                meta_offset: descriptor.metadata_offset,
                meta_len: descriptor.metadata_len,
                data_offset: descriptor.data_offset,
                data_len: descriptor.data_len,
                entry_count: 1,
                file_count: 1,
                frame_count: 0,
                layout_profile: AccessProfile::RandomSmallFile,
                metadata_digest: Sha256::digest(&metadata).into(),
                data_digest: descriptor.data_digest,
            }],
            containers: vec![PackedContainerRef {
                object_key: b"inline".to_vec(),
                object_len: opened.object_len(),
                object_digest: opened.object_digest(),
            }],
            group_index_pages: Vec::new(),
            inode_index_pages: Vec::new(),
        };
        let catalog = RemoteGroupCatalog::new(client, manifest);
        let plan = catalog
            .read_unified_plan(2, b"tiny.bin", 1, 4)
            .await
            .unwrap();
        assert!(matches!(
            plan.segments[0].source,
            ReadSource::PackedInline { .. }
        ));
        let fetcher =
            super::super::remote::PackedFrameSourceFetcher::<LocalFsBackend>::new(HashMap::new());
        let mut bytes = [0u8; 4];
        crate::chunk::read_plan::execute_unified_into(&fetcher, 1, &plan, &mut bytes)
            .await
            .unwrap();
        assert_eq!(&bytes, b"aylo");
    }

    #[tokio::test]
    async fn catalog_resolves_entries_from_a_pageable_group_index() {
        let temp = tempdir().unwrap();
        let backend = LocalFsBackend::new(temp.path());
        let client = ObjectClient::new(backend.clone());
        let metadata = meta_bytes();
        let object = PackedGroupContainer::build(
            12,
            AccessProfile::RandomSmallFile,
            vec![PackedGroupInput {
                group_id: 9,
                parent_dir_key: [8; 32],
                metadata: metadata.clone(),
                frame_ordinals: vec![0],
                entry_count: 1,
                file_count: 1,
                layout_profile: AccessProfile::RandomSmallFile,
            }],
            vec![PackedFrameInput {
                raw: b"payload".to_vec(),
                size_class: SizeClass::Tiny,
                codec: 0,
                first_file_slot: 0,
                last_file_slot: 0,
            }],
        )
        .unwrap();
        let opened = PackedGroupContainer::open(object.clone()).unwrap();
        client.put_object("container", &object).await.unwrap();

        let group = super::super::wire::PackedGroupRef {
            group_id: 9,
            container_ordinal: 0,
            parent_dir_key: [8; 32],
            first_name: b"sample.bin".to_vec(),
            last_name: b"sample.bin".to_vec(),
            meta_offset: opened.groups()[0].metadata_offset,
            meta_len: opened.groups()[0].metadata_len,
            data_offset: opened.groups()[0].data_offset,
            data_len: opened.groups()[0].data_len,
            entry_count: 1,
            file_count: 1,
            frame_count: 1,
            layout_profile: AccessProfile::RandomSmallFile,
            metadata_digest: Sha256::digest(&metadata).into(),
            data_digest: opened.groups()[0].data_digest,
        };
        let page = PackedGroupIndexPage {
            snapshot_id: [7; 32],
            page_ordinal: 0,
            total_pages: 1,
            groups: vec![group],
        }
        .encode()
        .unwrap();
        client.put_object("group-index", &page).await.unwrap();
        let manifest = PackedSnapshotManifest {
            snapshot_id: [7; 32],
            root_dir_key: [8; 32],
            root_inode: 1,
            layout_profile: AccessProfile::RandomSmallFile,
            size_classes: Default::default(),
            // The complete group table is intentionally omitted.  This is
            // the shape used by large snapshots to keep manifest memory
            // independent of file count.
            groups: Vec::new(),
            containers: vec![PackedContainerRef {
                object_key: b"container".to_vec(),
                object_len: opened.object_len(),
                object_digest: opened.object_digest(),
            }],
            group_index_pages: vec![super::super::wire::PackedGroupIndexPageRef {
                object: PackedContainerRef {
                    object_key: b"group-index".to_vec(),
                    object_len: page.len() as u64,
                    object_digest: Sha256::digest(&page).into(),
                },
                first_parent_dir_key: [8; 32],
                first_name: b"sample.bin".to_vec(),
                last_parent_dir_key: [8; 32],
                last_name: b"sample.bin".to_vec(),
            }],
            inode_index_pages: Vec::new(),
        };
        let catalog = RemoteGroupCatalog::new(client, manifest);
        let adaptive = catalog
            .clone()
            .with_metadata_cache_bytes(1)
            .prefetch_metadata_adaptive(2)
            .await
            .unwrap();
        assert_eq!(adaptive.group_index_pages, 0);
        assert_eq!(adaptive.group_index_pages_skipped, 1);
        assert_eq!(adaptive.group_meta_pages, 0);
        assert!(adaptive.group_meta_pages_skipped >= 4096);
        assert_eq!(adaptive.group_meta_bytes_estimated, 0);
        let group = catalog
            .group_for_name_from_index_page(0, [8; 32], b"sample.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(group.group_id, 9);
        assert_eq!(
            catalog
                .group_for_name_paged([8; 32], b"sample.bin")
                .await
                .unwrap()
                .unwrap()
                .group_id,
            9
        );
        let entry = catalog
            .lookup_entry_from_index_page(0, [8; 32], b"sample.bin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.inode, 7);
        assert_eq!(
            catalog
                .lookup_entry_paged([8; 32], b"sample.bin")
                .await
                .unwrap()
                .unwrap()
                .inode,
            7
        );
        let warmup = catalog.prefetch_metadata(2, None).await.unwrap();
        assert_eq!(warmup.inode_index_pages, 0);
        assert_eq!(warmup.group_index_pages, 1);
        assert_eq!(warmup.group_meta_pages, 1);
        catalog.group_index_pages.run_pending_tasks().await;
        catalog.group_meta_pages.run_pending_tasks().await;
        let stats = catalog.metadata_cache_stats();
        assert!(stats.group_index_entries >= 1);
        assert!(stats.group_meta_entries >= 1);
        let plan = catalog
            .read_unified_plan_from_index_page(0, 9, b"sample.bin", 1, 4)
            .await
            .unwrap();
        let paged_plan = catalog
            .read_unified_plan_paged([8; 32], b"sample.bin", 1, 4)
            .await
            .unwrap();
        assert_eq!(paged_plan, plan);
        let remote = super::super::remote::RemotePackedObject::open(
            catalog.client(),
            "container",
            opened.object_len(),
            PackedObjectKind::GroupContainer,
        )
        .await
        .unwrap();
        let fetcher = super::super::remote::PackedFrameSourceFetcher::from_object(remote, 0);
        let mut bytes = [0u8; 4];
        crate::chunk::read_plan::execute_unified_into(&fetcher, 1, &plan, &mut bytes)
            .await
            .unwrap();
        assert_eq!(&bytes, b"aylo");
        assert!(
            catalog
                .group_ref_from_index_page(0, 99)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn catalog_paginates_parent_groups_in_name_order() {
        let client = ObjectClient::new(LocalFsBackend::new(tempdir().unwrap().path()));
        let group = |group_id: u64, name: &[u8]| super::super::wire::PackedGroupRef {
            group_id,
            container_ordinal: 0,
            parent_dir_key: [4; 32],
            first_name: name.to_vec(),
            last_name: name.to_vec(),
            meta_offset: 0,
            meta_len: 0,
            data_offset: 0,
            data_len: 0,
            entry_count: 0,
            file_count: 0,
            frame_count: 0,
            layout_profile: AccessProfile::RandomSmallFile,
            metadata_digest: [0; 32],
            data_digest: [0; 32],
        };
        let catalog = RemoteGroupCatalog::new(
            client,
            PackedSnapshotManifest {
                snapshot_id: [0; 32],
                root_dir_key: [8; 32],
                root_inode: 1,
                layout_profile: AccessProfile::RandomSmallFile,
                size_classes: Default::default(),
                groups: vec![group(2, b"z"), group(1, b"a")],
                containers: vec![PackedContainerRef {
                    object_key: b"container".to_vec(),
                    object_len: 128,
                    object_digest: [0; 32],
                }],
                group_index_pages: Vec::new(),
                inode_index_pages: Vec::new(),
            },
        );
        assert_eq!(catalog.groups_for_parent([4; 32], 0, 1)[0].group_id, 1);
        assert_eq!(catalog.groups_for_parent([4; 32], 1, 1)[0].group_id, 2);
    }

    #[test]
    fn adaptive_metadata_page_selection_keeps_a_stable_budgeted_prefix() {
        // Each encoded page is projected at 2x while selecting. A later page
        // must not be chosen after the first page no longer fits: this makes
        // repeated mounts warm the same routing prefix and leaves the tail to
        // demand loading.
        assert_eq!(select_metadata_pages([10, 10, 10, 10], 45), vec![0, 1]);
        assert_eq!(select_metadata_pages([30, 10, 10], 70), vec![0]);
        assert!(select_metadata_pages([30, 10], 0).is_empty());
    }
}
