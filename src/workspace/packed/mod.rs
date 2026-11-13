//! Packed metadata building blocks.
//!
//! This module is deliberately separate from the native-base container kinds.
//! The packed manifest/container objects use their own magic values and are
//! consumed by the workspace lower-layer resolver.

mod catalog;
mod codec;
mod coordinator;
mod group;
mod index;
mod layout;
mod meta;
mod metrics;
mod readonly;
pub(crate) use readonly::{PackedLowerInodeMetadata, PackedLowerReverseNames};
mod remote;
mod wire;
pub(crate) use wire::{Reader, Writer};
pub mod snapshot;

pub use catalog::{
    PackedMetadataCacheStats, PackedMetadataWarmupStats, RemoteGroupCatalog, directory_key,
};
pub use coordinator::{
    CoalescedRange, CoordinatorLimits, FrameReadRequest, GroupReadCoordinator,
    coalesce_frame_ranges, read_coalesced_frames,
};
pub use group::{
    ContainerPackingLimits, GroupPackingLimits, PackedContainerInput, PackedFileInput,
    PackedFrameDescriptor, PackedFrameInput, PackedGroupContainer, PackedGroupDescriptor,
    PackedGroupInput, frame_directory_body_len, frame_table_body_offset, group_container_counts,
    pack_group_file_shards, pack_group_files, pack_group_files_with_policy,
    pack_group_shard_containers, parse_frame_descriptor_range, parse_frame_directory,
};
pub use index::{PackedGroupIndexPage, PackedInodeIndexEntry, PackedInodeIndexPage};
pub use layout::{
    AccessProfile, FrameLayoutDecision, LayoutError, SizeClass, SizeClassTable, choose_frame_layout,
};
pub use meta::{
    GroupMeta, GroupMetaEntry, GroupMetaExtent, INLINE_DATA_FLAG, INLINE_FILE_MAX_BYTES,
};
pub use metrics::{PackedRuntimeMetrics, PackedRuntimeMetricsSnapshot, PackedStatsExtension};
pub use readonly::{PackedBlockStore, PackedReadonlyMeta};
pub use remote::{
    MAX_PACKED_STREAM_RANGE_BYTES, PackedFrameSourceFetcher, PackedWindowCacheStats,
    RemotePackedObject, read_exact_range,
};
pub use wire::{
    COLD_ATTRIBUTE_MAGIC, GROUP_CONTAINER_MAGIC, GROUP_INDEX_MAGIC, INODE_INDEX_MAGIC,
    MANIFEST_MAGIC, PACKED_FOOTER_LEN, PACKED_FOOTER_MAGIC, PACKED_HEADER_LEN, PackedContainerRef,
    PackedEnvelope, PackedGroupIndexPageRef, PackedGroupRef, PackedHeader, PackedInodeIndexPageRef,
    PackedObjectKind, PackedResult, PackedSnapshotManifest, PackedWireError,
};

pub use codec::{PackedCodec, decode_block, encode_block};
