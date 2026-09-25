//! Contracts shared by the clustered frozen-metadata v2 reader and producer.
//!
//! This module deliberately contains no object-store or FUSE integration.  It
//! defines the bounded pieces that both sides must agree on: raw POSIX names,
//! range windows, stable snapshot cursors, and allocation admission.  The
//! legacy packed-metadata-v1 reader does not use these types.

mod attribute;
mod batch;
mod budget;
mod cluster_builder;
mod cluster_format;
mod data_pack;
mod data_seal;
mod data_seal_remote;
mod directory;
mod extent;
mod head_store;
mod identity;
mod index_builder;
mod ingest;
mod manifest_remote;
mod merge;
mod merge_route;
mod merkle_index;
mod mount_trie;
mod name;
mod publication;
mod range_reader;
mod remote;
mod remote_union;
mod snapshot_manifest;

pub use attribute::{AttributeBatch, AttributeGroup, XattrRecord, attribute_index_key};
pub use batch::{
    BATCH_HEADER_LEN, BATCH_MAGIC, BATCH_VERSION, BatchCodec, BatchHeader, BatchKind, EncodedBatch,
    MAX_BATCH_RAW, MAX_BATCH_STORED, NAME_RESTART_INTERVAL, NamespaceBatch, NamespaceEntry,
    NamespaceSegment, NodeRecord, SEGMENT_CONTINUATION, SEGMENT_END, SEGMENT_START, encode_batch,
};
pub use budget::{BudgetError, BudgetReservation, MetadataBudget, MetadataBudgetSnapshot};
pub use cluster_builder::{
    BuiltCluster, NamespaceSegmentInput, build_namespace_segments_cluster,
    build_namespace_segments_cluster_with_metadata, build_single_directory_cluster,
    build_single_directory_cluster_with_metadata,
};
pub use cluster_format::{
    BatchLocator, CLUSTER_MAGIC, ClusterSuperblock, FORMAT_MAJOR, FORMAT_MINOR, IndexRootRef,
    SNAPSHOT_MAGIC, SUPERBLOCK_LEN, SnapshotSuperblock, namespace_index_key,
};
pub use data_pack::{DATA_PACK_MAGIC, DataPackBuilder, DataPackSnapshot, RemoteDataPack};
pub use data_seal::{
    DATA_SEAL_FOOTER_LEN, DATA_SEAL_HEADER_LEN, DATA_SEAL_MAGIC, DataObjectDescriptor,
    DataSealBuilder, DataSealSnapshot, DataSpan, FrameDescriptor as DataFrameDescriptor,
    SliceDescriptor, frame_descriptors_from_pack,
};
pub use data_seal_remote::{MAX_DATA_SEAL_RANGE_BYTES, RemoteDataSeal};
pub use directory::{
    DirectoryEntry, DirectoryIdentity, DirectoryPage, DirectoryView, MAX_RANGE_SOURCES,
    MAX_WINDOW_ENTRIES, NodeRef, RangeRoute, RangeRouteError, RangeSource, RangeWindow,
    ReadDirCursor, ReadDirLimit, WindowSource, WindowSourceKind,
};
pub use extent::{ExtentBatch, ExtentRecord, ExtentSegment, ExtentSpan, extent_index_key};
pub use head_store::KvWorkspaceHeadStore;
pub use identity::{
    DirKey, DirKeyError, derive_child_dir_key, derive_root_dir_key, validate_child_dir_key,
};
pub use index_builder::{BuiltIndexTree, IndexLeafEntry, build_index_tree};
pub use ingest::{
    BuiltSourceCluster, BuiltSourceData, build_local_directory_cluster,
    build_local_directory_namespace_cluster, build_source_cluster, build_source_namespace_cluster,
};
pub use manifest_remote::{MAX_MANIFEST_RANGE_BYTES, RemoteSnapshotManifest};
pub use merge::{
    CanonicalDirectory, CanonicalEntry, ContributionEntry, DirectoryContribution, MergeError,
    PlannedDirectory, SourceEntries, build_directory_plan, fold_directory_contributions,
};
pub use merge_route::{
    DirectoryViewRecord, MERGE_ROUTE_HEADER_LEN, MERGE_ROUTE_MAGIC, MergeRouteIndex,
    MergeRoutePageRef, MergeRouteRecord, MergeRouteSource, MergeRouteWindow,
};
pub use merkle_index::{
    ChildRef, DEFAULT_INDEX_NODE_SIZE, FLAG_RANK_COUNTS, INDEX_HEADER_LEN, INDEX_MAGIC,
    INDEX_RESTART_INTERVAL, IndexEntry, IndexNode, MAX_INDEX_NODE_SIZE,
};
pub use mount_trie::{
    MOUNT_TRIE_HEADER_LEN, MOUNT_TRIE_MAGIC, MountTrie, MountTrieChild, MountTrieIndex,
    MountTrieNode, MountTriePageRef,
};
pub use name::{NameBytes, NameError};
pub use publication::{
    BuiltSingleClusterManifest, MemoryWorkspaceHeadStore, PublishedSnapshot, SnapshotPublisher,
    SnapshotRef, WorkspaceHead, WorkspaceHeadStore, build_single_cluster_manifest,
};
pub use range_reader::{
    MAX_RANGE_BYTES, MemoryRangeReader, RangeReader, read_batch, read_index_child, read_index_root,
};
pub use remote::{
    DEFAULT_BATCH_CACHE_BYTES, DEFAULT_BATCH_CACHE_ENTRIES, DEFAULT_INDEX_CACHE_BYTES,
    DEFAULT_INDEX_CACHE_ENTRIES, RemoteCluster, RemoteClusterOptions,
};
pub use remote_union::{
    REMOTE_PAGE_MAX_OWNED_BYTES, RemoteClusterUnion, RemoteDirectoryPageSource, RemoteSnapshot,
};
pub use snapshot_manifest::{
    CLUSTER_TABLE_ROOT, ClusterDescriptor, MOUNT_INDEX_ROOT, ManifestIndexPayload,
    ManifestObjectRef, ROUTE_INDEX_ROOT, SnapshotManifest,
};
