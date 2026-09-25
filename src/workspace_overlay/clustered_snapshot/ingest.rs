//! Deterministic bridge from the source inventory to a v2 namespace cluster.
//!
//! Source adapters deliberately stop at an inventory of paths and reopen
//! tokens.  This module is the producer-side boundary that assigns stable
//! cluster-local node ids, derives directory identities, preserves hardlink
//! identity, and feeds parent-first namespace segments to the authenticated
//! `.brfc` builder.  File data and Data Seal planning remain separate: a
//! namespace cluster produced here is not publishable until those objects
//! have also been built and verified.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use blake3::Hasher;
use sha2::{Digest, Sha256};

use crate::native_base::ingest::{
    ConsistencyPolicy, EntryKind, IngestError, IngestSource, LocalDirSource, SourceEntry,
};
use crate::native_base::wire::datapack::PackFrame;
use crate::native_base::wire::error::{WireError, WireResult};

use super::attribute::{AttributeBatch, AttributeGroup};
use super::batch::{BatchHeader, NamespaceEntry, NodeRecord};
use super::cluster_builder::{
    BuiltCluster, NamespaceSegmentInput, build_namespace_segments_cluster,
    build_namespace_segments_cluster_with_metadata,
};
use super::data_pack::{DataPackBuilder, DataPackSnapshot};
use super::data_seal::{
    DataObjectDescriptor, DataSealBuilder, DataSpan, SliceDescriptor, frame_descriptors_from_pack,
};
use super::directory::{DirectoryIdentity, NodeRef};
use super::extent::{ExtentBatch, ExtentRecord, ExtentSegment};
use super::identity::{DirKey, derive_child_dir_key, derive_root_dir_key};
use super::merge::{ContributionEntry, DirectoryContribution, build_directory_plan};
use super::name::NameBytes;

/// Result of converting one complete source inventory into a namespace
/// cluster.  The maps are producer bookkeeping and are intentionally not
/// consulted by the remote reader.
#[derive(Clone, Debug)]
pub struct BuiltSourceCluster {
    pub cluster: BuiltCluster,
    pub root_dir_key: DirKey,
    pub local_node_ids: BTreeMap<Vec<u8>, u32>,
    pub directory_keys: BTreeMap<Vec<u8>, DirKey>,
    /// The source's parent-first namespace streams, retained so the same
    /// deterministic node assignment can be assembled with extent and cold
    /// attribute batches after source bytes have been read.
    namespace_segments: Vec<NamespaceSegmentInput>,
    /// Empty files, directories, and all-hole sparse files are represented by
    /// an empty DataPack plus an empty Data Seal rather than by a missing
    /// seal.  This keeps every complete source on the sealed-publication
    /// path.
    pub data: Option<BuiltSourceData>,
}

/// The producer-side pair of objects referenced by a complete source
/// cluster.  Upload coordinators can put `data_pack` under `object_key`,
/// verify its checksum, and then upload `data_seal` before publication.
#[derive(Clone, Debug)]
pub struct BuiltSourceData {
    pub object_key: String,
    pub data_pack: Vec<u8>,
    pub data_seal: Vec<u8>,
    pub slice_count: u64,
    pub frame_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EntryInfo {
    path: Vec<u8>,
    name: NameBytes,
    parent_path: Vec<u8>,
    kind: u8,
    dir_key: Option<DirKey>,
    attributes_digest: [u8; 32],
}

/// Build the namespace portion of one v2 cluster from a sorted source
/// inventory.  Parent directories must be present in the inventory; silently
/// synthesizing archive directories here would make source equivalence and
/// hardlink validation ambiguous.
pub fn build_source_namespace_cluster(
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
    entries: &[SourceEntry],
) -> WireResult<BuiltSourceCluster> {
    if entries.is_empty() {
        return Err(WireError::invalid(
            "cluster ingest",
            "an empty source has no namespace segment",
        ));
    }

    let mut sorted = entries.to_vec();
    sorted.sort_by(|left, right| left.path.cmp(&right.path));
    for pair in sorted.windows(2) {
        if pair[0].path == pair[1].path {
            return Err(WireError::invalid(
                "cluster ingest",
                "source inventory contains duplicate paths",
            ));
        }
    }

    let root_dir_key = derive_root_dir_key(volume_id);
    let mut directory_keys = BTreeMap::from([(Vec::new(), root_dir_key)]);
    let mut directory_paths = BTreeSet::from([Vec::new()]);
    let mut directory_attribute_digests =
        BTreeMap::<Vec<u8>, [u8; 32]>::from([(Vec::new(), root_attribute_digest(volume_id))]);
    let mut entry_infos = Vec::with_capacity(sorted.len());

    // Parent-first assignment is independent of the source walk order.  The
    // second sort key keeps local ids stable for the same source snapshot.
    let mut parent_first = sorted.clone();
    parent_first.sort_by_key(|entry| (path_depth(&entry.path), entry.path.clone()));
    let mut local_node_ids = BTreeMap::new();
    let mut node_records = BTreeMap::<u32, NodeRecord>::new();
    let mut hardlinks = BTreeMap::<u64, (u32, [u8; 32])>::new();
    let mut next_node_id = 2u32;

    for entry in &parent_first {
        validate_path(&entry.path)?;
        let (parent_path, raw_name) = split_parent_name(&entry.path)?;
        let parent_dir_key = directory_keys.get(&parent_path).copied().ok_or_else(|| {
            WireError::invalid(
                "cluster ingest",
                format!(
                    "entry {:?} has no inventoried parent directory",
                    String::from_utf8_lossy(&entry.path)
                ),
            )
        })?;
        let name = NameBytes::new(raw_name.to_vec())
            .map_err(|error| WireError::invalid("cluster ingest name", error.to_string()))?;

        let (kind, size, dir_key) = match &entry.kind {
            EntryKind::Directory => {
                let child = derive_child_dir_key(volume_id, parent_dir_key, name.as_bytes())
                    .map_err(|error| {
                        WireError::invalid("cluster ingest directory", error.to_string())
                    })?;
                (2, 0, Some(child))
            }
            EntryKind::File { size } => (1, *size, None),
            EntryKind::Symlink { target } => (3, target.len() as u64, None),
        };
        let attributes_digest = attribute_digest(entry, kind, size, dir_key);
        let local_node_id = if let Some(group) = entry.hardlink_group {
            if kind != 1 {
                return Err(WireError::invalid(
                    "cluster ingest",
                    "hardlink groups may only contain regular files",
                ));
            }
            if let Some((existing, digest)) = hardlinks.get(&group) {
                if *digest != attributes_digest {
                    return Err(WireError::invalid(
                        "cluster ingest",
                        "hardlink members disagree on file attributes",
                    ));
                }
                *existing
            } else {
                let assigned = allocate_node(&mut next_node_id)?;
                hardlinks.insert(group, (assigned, attributes_digest));
                assigned
            }
        } else {
            allocate_node(&mut next_node_id)?
        };

        if kind == 2 {
            let child_dir_key = dir_key.ok_or_else(|| {
                WireError::invalid("cluster ingest", "directory is missing its DirKey")
            })?;
            directory_paths.insert(entry.path.clone());
            directory_keys.insert(entry.path.clone(), child_dir_key);
            directory_attribute_digests.insert(entry.path.clone(), attributes_digest);
        }
        local_node_ids.insert(entry.path.clone(), local_node_id);
        node_records
            .entry(local_node_id)
            .or_insert_with(|| NodeRecord {
                local_node_id,
                kind,
                mode: entry.attributes.mode,
                size,
                dir_key,
            });
        entry_infos.push(EntryInfo {
            path: entry.path.clone(),
            name,
            parent_path,
            kind,
            dir_key,
            attributes_digest,
        });
    }

    // Every directory was assigned before its children because of the
    // parent-first pass.  Check this explicitly before building any bytes.
    for info in &entry_infos {
        if !directory_paths.contains(&info.parent_path) {
            return Err(WireError::invalid(
                "cluster ingest",
                "namespace parent is not a directory",
            ));
        }
    }

    // Run the reference fold/planner for every non-empty logical directory
    // before materializing bytes. This keeps the producer and merged-reader
    // collision rules identical, including raw-name ordering and child
    // directory identity checks.
    let mut planner_entries = BTreeMap::<Vec<u8>, Vec<ContributionEntry>>::new();
    for info in &entry_infos {
        planner_entries
            .entry(info.parent_path.clone())
            .or_default()
            .push(ContributionEntry {
                name: info.name.clone(),
                inode: local_node_ids[&info.path] as u64,
                kind: info.kind,
                child_dir_key: info.dir_key,
                attributes_digest: info.attributes_digest,
            });
    }
    for (parent_path, mut contribution_entries) in planner_entries {
        contribution_entries.sort_by(|left, right| left.name.cmp(&right.name));
        let parent_local_node_id = if parent_path.is_empty() {
            1
        } else {
            local_node_ids[&parent_path]
        };
        let parent_dir_key = directory_keys[&parent_path];
        let attributes_digest = directory_attribute_digests
            .get(&parent_path)
            .copied()
            .ok_or_else(|| WireError::invalid("cluster ingest", "directory attributes missing"))?;
        build_directory_plan(
            DirectoryIdentity::from_dir_key([0; 32], parent_dir_key),
            vec![DirectoryContribution {
                dir_key: parent_dir_key,
                node: NodeRef {
                    cluster_slot: 0,
                    local_node_id: parent_local_node_id,
                },
                attributes_digest,
                entries: contribution_entries,
            }],
        )
        .map_err(|error| WireError::invalid("cluster ingest planner", error.to_string()))?;
    }

    let mut by_parent = BTreeMap::<Vec<u8>, Vec<NamespaceEntry>>::new();
    let mut emitted_nodes = BTreeSet::new();
    let mut infos = entry_infos;
    infos.sort_by(|left, right| left.path.cmp(&right.path));
    for info in infos {
        let node_id = local_node_ids
            .get(&info.path)
            .copied()
            .ok_or_else(|| WireError::invalid("cluster ingest", "node id disappeared"))?;
        let entry = if emitted_nodes.insert(node_id) {
            NamespaceEntry::NewNode {
                name: info.name,
                node: node_records
                    .get(&node_id)
                    .cloned()
                    .ok_or_else(|| WireError::invalid("cluster ingest", "node record missing"))?,
            }
        } else {
            NamespaceEntry::ExistingNode {
                name: info.name,
                local_node_id: node_id,
            }
        };
        by_parent.entry(info.parent_path).or_default().push(entry);
    }

    let mut segments = Vec::with_capacity(by_parent.len());
    for (parent_path, mut entries) in by_parent {
        entries.sort_by(|left, right| namespace_name(left).cmp(namespace_name(right)));
        let parent_local_node_id = if parent_path.is_empty() {
            1
        } else {
            local_node_ids
                .get(&parent_path)
                .copied()
                .ok_or_else(|| WireError::invalid("cluster ingest", "parent node is missing"))?
        };
        let parent_dir_key = directory_keys
            .get(&parent_path)
            .copied()
            .ok_or_else(|| WireError::invalid("cluster ingest", "parent directory key missing"))?;
        segments.push(NamespaceSegmentInput {
            parent_local_node_id,
            parent_dir_key,
            entries,
        });
    }
    segments.sort_by_key(|segment| segment.parent_local_node_id);

    let node_count = next_node_id
        .checked_sub(1)
        .ok_or_else(|| WireError::invalid("cluster ingest", "local node count underflow"))?;
    let directory_contribution_count = u32::try_from(directory_paths.len())
        .map_err(|_| WireError::LimitExceeded("directory count exceeds u32".into()))?;
    let cluster = build_namespace_segments_cluster(
        cluster_id,
        volume_id,
        root_dir_key,
        node_count,
        directory_contribution_count,
        &segments,
    )?;
    Ok(BuiltSourceCluster {
        cluster,
        root_dir_key,
        local_node_ids,
        directory_keys,
        namespace_segments: segments,
        data: None,
    })
}

/// Inventory a local directory with the existing source consistency contract
/// and immediately build its v2 namespace object. DataPack/Data Seal
/// production is intentionally a later stage; callers must keep the returned
/// object unpublished until those references are verified.
pub fn build_local_directory_namespace_cluster(
    root: &Path,
    policy: ConsistencyPolicy,
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
) -> Result<BuiltSourceCluster, IngestError> {
    let mut source = LocalDirSource::new(root, policy);
    let entries = source.inventory()?;
    build_source_namespace_cluster(cluster_id, volume_id, &entries)
        .map_err(|error| IngestError::Backend(error.to_string()))
}

/// Build a complete v2 cluster from an inventoried source.
///
/// The source is revalidated before and after data reads.  Metadata is
/// assembled only after all source bytes have been read and the DataPack and
/// Data Seal have been scrubbed/closed, so a caller can safely keep the
/// returned objects private until its upload coordinator verifies them.
pub fn build_source_cluster<S: IngestSource>(
    source: &mut S,
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
) -> Result<BuiltSourceCluster, IngestError> {
    let entries = source.inventory()?;
    source.revalidate(&entries)?;

    let mut built = build_source_namespace_cluster(cluster_id, volume_id, &entries)
        .map_err(|error| IngestError::Backend(error.to_string()))?;
    let (extent_batches, attribute_batches, data) = build_source_payload(
        source,
        cluster_id,
        volume_id,
        &entries,
        &built.local_node_ids,
    )
    .map_err(|error| IngestError::Backend(error.to_string()))?;

    // A source changing while the DataPack is being built must never be
    // turned into a publishable immutable cluster.
    source.revalidate(&entries)?;

    built.cluster = build_namespace_segments_cluster_with_metadata(
        cluster_id,
        volume_id,
        built.root_dir_key,
        built.cluster.superblock.node_count,
        built.cluster.superblock.directory_contribution_count,
        &built.namespace_segments,
        &extent_batches,
        &attribute_batches,
    )
    .map_err(|error| IngestError::Backend(error.to_string()))?;
    built.data = data;
    Ok(built)
}

/// Convenience complete producer for a local directory.  The existing
/// `*_namespace_cluster` function remains namespace-only for callers that do
/// not want to read file payloads.
pub fn build_local_directory_cluster(
    root: &Path,
    policy: ConsistencyPolicy,
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
) -> Result<BuiltSourceCluster, IngestError> {
    let mut source = LocalDirSource::new(root, policy);
    build_source_cluster(&mut source, cluster_id, volume_id)
}

const DATA_FRAME_RAW_BYTES: usize = 1 << 20;
const MAX_EXTENTS_PER_BATCH: usize = 4096;
const MAX_ATTRIBUTE_GROUPS_PER_BATCH: usize = 4096;

#[derive(Clone, Debug)]
struct PlannedExtent {
    file_offset: u64,
    logical_length: u64,
    slice_id: u64,
    frame_ordinal: u32,
}

fn build_source_payload<S: IngestSource>(
    source: &S,
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
    entries: &[SourceEntry],
    local_node_ids: &BTreeMap<Vec<u8>, u32>,
) -> WireResult<(
    Vec<ExtentBatch>,
    Vec<AttributeBatch>,
    Option<BuiltSourceData>,
)> {
    let mut sorted = entries.to_vec();
    sorted.sort_by(|left, right| left.path.cmp(&right.path));

    let attribute_batches = build_attribute_batches(cluster_id, &sorted, local_node_ids)?;
    let mut pack = DataPackBuilder::new();
    let mut next_slice_id = 1u64;
    let mut placements = BTreeMap::<u32, Vec<PlannedExtent>>::new();
    let mut files_by_node = BTreeMap::<u32, &SourceEntry>::new();
    for entry in &sorted {
        if matches!(entry.kind, EntryKind::File { .. }) {
            let local_node_id = local_node_ids
                .get(&entry.path)
                .copied()
                .ok_or_else(|| WireError::invalid("cluster ingest", "file node id is missing"))?;
            files_by_node.entry(local_node_id).or_insert(entry);
        }
    }

    for (local_node_id, entry) in files_by_node {
        let EntryKind::File { size } = entry.kind else {
            unreachable!("files_by_node only contains regular files")
        };
        let data_ranges = complement_ranges(size, &entry.sparse_ranges)?;
        let mut file_extents = Vec::new();
        for (range_start, range_len) in data_ranges {
            let mut offset = range_start;
            let range_end = range_start
                .checked_add(range_len)
                .ok_or_else(|| WireError::LimitExceeded("source range overflows u64".into()))?;
            while offset < range_end {
                let remaining = range_end - offset;
                let chunk_len = remaining.min(DATA_FRAME_RAW_BYTES as u64);
                let chunk_len_usize = usize::try_from(chunk_len).map_err(|_| {
                    WireError::LimitExceeded("source chunk length exceeds usize".into())
                })?;
                let mut raw = vec![0u8; chunk_len_usize];
                source
                    .read_range(&entry.token, offset, &mut raw)
                    .map_err(|error| WireError::invalid("source read", error.to_string()))?;
                let frame = PackFrame::plain_bytes_zstd(&raw, 3)?;
                let frame_ordinal = u32::try_from(pack.frame_count()).map_err(|_| {
                    WireError::LimitExceeded("data pack frame count exceeds u32".into())
                })?;
                pack.push(frame);
                file_extents.push(PlannedExtent {
                    file_offset: offset,
                    logical_length: chunk_len,
                    slice_id: next_slice_id,
                    frame_ordinal,
                });
                next_slice_id = next_slice_id
                    .checked_add(1)
                    .ok_or_else(|| WireError::LimitExceeded("slice id overflows u64".into()))?;
                offset = offset.checked_add(chunk_len).ok_or_else(|| {
                    WireError::LimitExceeded("source offset overflows u64".into())
                })?;
            }
        }
        if !file_extents.is_empty() {
            placements.insert(local_node_id, file_extents);
        }
    }

    let extent_batches = build_extent_batches(cluster_id, &placements)?;
    let data_pack = pack.build()?;
    let snapshot = DataPackSnapshot::open(data_pack.clone())?;
    let object_key = format!("clusters/{}/data/0.brfdp", hex::encode(cluster_id));
    let mut seal = DataSealBuilder::new(cluster_id, volume_id);
    seal.add_object(DataObjectDescriptor {
        object_ordinal: 0,
        object_key: object_key.as_bytes().to_vec(),
        object_len: data_pack.len() as u64,
        object_checksum: Sha256::digest(&data_pack).into(),
        etag: Vec::new(),
    })?;
    for descriptor in frame_descriptors_from_pack(0, &snapshot)? {
        seal.add_frame(descriptor)?;
    }
    for extent_list in placements.values() {
        for extent in extent_list {
            seal.add_slice(SliceDescriptor {
                slice_id: extent.slice_id,
                logical_len: extent.logical_length,
                spans: vec![DataSpan {
                    frame_ordinal: extent.frame_ordinal,
                    raw_offset_in_frame: 0,
                    raw_len: u32::try_from(extent.logical_length)
                        .map_err(|_| WireError::LimitExceeded("slice length exceeds u32".into()))?,
                }],
            })?;
        }
    }
    let data_seal = seal.build()?;
    Ok((
        extent_batches,
        attribute_batches,
        Some(BuiltSourceData {
            object_key,
            data_pack,
            data_seal,
            slice_count: next_slice_id - 1,
            frame_count: snapshot.frames().len() as u32,
        }),
    ))
}

fn build_attribute_batches(
    cluster_id: [u8; 16],
    entries: &[SourceEntry],
    local_node_ids: &BTreeMap<Vec<u8>, u32>,
) -> WireResult<Vec<AttributeBatch>> {
    let mut groups = BTreeMap::<u32, AttributeGroup>::new();
    for entry in entries {
        let EntryKind::Symlink { target } = &entry.kind else {
            continue;
        };
        let local_node_id = local_node_ids
            .get(&entry.path)
            .copied()
            .ok_or_else(|| WireError::invalid("cluster ingest", "symlink node id is missing"))?;
        groups
            .entry(local_node_id)
            .or_insert_with(|| AttributeGroup {
                local_node_id,
                symlink_target: Some(target.clone()),
                xattrs: Vec::new(),
                acl: None,
            });
    }
    let groups = groups.into_values().collect::<Vec<_>>();
    let mut batches = Vec::new();
    for (batch_index, chunk) in groups.chunks(MAX_ATTRIBUTE_GROUPS_PER_BATCH).enumerate() {
        let batch_id = u32::try_from(batch_index)
            .map_err(|_| WireError::LimitExceeded("attribute batch id exceeds u32".into()))?;
        batches.push(AttributeBatch {
            cluster_id,
            batch_id,
            stream_ordinal: batch_id,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            groups: chunk.to_vec(),
        });
    }
    Ok(batches)
}

fn build_extent_batches(
    cluster_id: [u8; 16],
    placements: &BTreeMap<u32, Vec<PlannedExtent>>,
) -> WireResult<Vec<ExtentBatch>> {
    let mut batches = Vec::new();
    let mut batch_id = 0u32;
    for (local_node_id, extents) in placements {
        for chunk in extents.chunks(MAX_EXTENTS_PER_BATCH) {
            let first_file_offset = chunk
                .first()
                .map(|extent| extent.file_offset)
                .ok_or_else(|| WireError::invalid("extent planner", "empty extent chunk"))?;
            let mut records = Vec::with_capacity(chunk.len());
            let mut previous_end = first_file_offset;
            for (index, extent) in chunk.iter().enumerate() {
                let gap = if index == 0 {
                    0
                } else {
                    extent
                        .file_offset
                        .checked_sub(previous_end)
                        .ok_or_else(|| {
                            WireError::invalid("extent planner", "overlapping source extents")
                        })?
                };
                records.push(ExtentRecord {
                    gap_from_previous_end: gap,
                    logical_length: extent.logical_length,
                    slice_id: extent.slice_id,
                    slice_offset: 0,
                });
                previous_end = extent
                    .file_offset
                    .checked_add(extent.logical_length)
                    .ok_or_else(|| WireError::LimitExceeded("extent end overflows u64".into()))?;
            }
            batches.push(ExtentBatch {
                cluster_id,
                batch_id,
                stream_ordinal: batch_id,
                predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
                segments: vec![ExtentSegment {
                    local_node_id: *local_node_id,
                    first_file_offset,
                    extents: records,
                }],
            });
            batch_id = batch_id
                .checked_add(1)
                .ok_or_else(|| WireError::LimitExceeded("extent batch id exceeds u32".into()))?;
        }
    }
    Ok(batches)
}

fn complement_ranges(size: u64, holes: &[(u64, u64)]) -> WireResult<Vec<(u64, u64)>> {
    let mut ranges = Vec::new();
    let mut cursor = 0u64;
    let mut sorted = holes.to_vec();
    sorted.sort_unstable();
    for (hole_start, hole_len) in sorted {
        if hole_len == 0 {
            return Err(WireError::invalid(
                "source sparse ranges",
                "hole length is zero",
            ));
        }
        let hole_end = hole_start
            .checked_add(hole_len)
            .ok_or_else(|| WireError::LimitExceeded("sparse range overflows u64".into()))?;
        if hole_start < cursor || hole_end > size {
            return Err(WireError::invalid(
                "source sparse ranges",
                "sparse ranges overlap or exceed file size",
            ));
        }
        if hole_start > cursor {
            ranges.push((cursor, hole_start - cursor));
        }
        cursor = hole_end;
    }
    if cursor < size {
        ranges.push((cursor, size - cursor));
    }
    Ok(ranges)
}

fn allocate_node(next: &mut u32) -> WireResult<u32> {
    let id = *next;
    *next = next
        .checked_add(1)
        .ok_or_else(|| WireError::LimitExceeded("cluster local node id overflow".into()))?;
    Ok(id)
}

fn validate_path(path: &[u8]) -> WireResult<()> {
    if path.is_empty() || path[0] == b'/' || path.contains(&0) {
        return Err(WireError::invalid("cluster ingest path", "invalid path"));
    }
    for component in path.split(|byte| *byte == b'/') {
        if component.is_empty() || component == b"." || component == b".." {
            return Err(WireError::invalid(
                "cluster ingest path",
                "path contains an invalid component",
            ));
        }
    }
    Ok(())
}

fn split_parent_name(path: &[u8]) -> WireResult<(Vec<u8>, &[u8])> {
    let Some(index) = path.iter().rposition(|byte| *byte == b'/') else {
        return Ok((Vec::new(), path));
    };
    Ok((path[..index].to_vec(), &path[index + 1..]))
}

fn path_depth(path: &[u8]) -> usize {
    path.split(|byte| *byte == b'/').count()
}

fn namespace_name(entry: &NamespaceEntry) -> &NameBytes {
    match entry {
        NamespaceEntry::NewNode { name, .. } | NamespaceEntry::ExistingNode { name, .. } => name,
    }
}

fn attribute_digest(entry: &SourceEntry, kind: u8, size: u64, dir_key: Option<DirKey>) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(b"BrewFS.BRFCL002.source-attributes.v2");
    hasher.update(&kind.to_le_bytes());
    hasher.update(&entry.attributes.mode.to_le_bytes());
    hasher.update(&entry.attributes.mtime_ns.to_le_bytes());
    hasher.update(&size.to_le_bytes());
    if let Some(dir_key) = dir_key {
        hasher.update(dir_key.as_ref());
    }
    if let EntryKind::Symlink { target } = &entry.kind {
        hasher.update(target);
    }
    *hasher.finalize().as_bytes()
}

fn root_attribute_digest(volume_id: [u8; 16]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(b"BrewFS.BRFCL002.root-attributes.v2");
    hasher.update(&volume_id);
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_base::ingest::source::{SourceAttributes, SourceToken};
    use crate::workspace_overlay::clustered_snapshot::batch::{
        BatchKind, EncodedBatch, NamespaceBatch,
    };
    use crate::workspace_overlay::clustered_snapshot::cluster_format::ClusterSuperblock;
    use crate::workspace_overlay::clustered_snapshot::merkle_index::{IndexEntry, IndexNode};

    fn entry(path: &[u8], kind: EntryKind, group: Option<u64>) -> SourceEntry {
        SourceEntry {
            raw_name: path.to_vec(),
            path: path.to_vec(),
            kind,
            attributes: SourceAttributes {
                mode: 0o100644,
                mtime_ns: 7,
            },
            hardlink_group: group,
            sparse_ranges: Vec::new(),
            token: SourceToken {
                location: path.to_vec(),
                identity: [1; 32],
            },
        }
    }

    #[test]
    fn source_inventory_builds_parent_first_cluster_and_preserves_hardlinks() {
        let entries = vec![
            entry(b"data", EntryKind::Directory, None),
            entry(b"data/a", EntryKind::File { size: 4 }, Some(7)),
            entry(b"data/b", EntryKind::File { size: 4 }, Some(7)),
            entry(
                b"link",
                EntryKind::Symlink {
                    target: b"data/a".to_vec(),
                },
                None,
            ),
        ];
        let built = build_source_namespace_cluster([3; 16], [4; 16], &entries).unwrap();
        assert_eq!(built.cluster.superblock.node_count, 4);
        assert_eq!(built.cluster.superblock.directory_contribution_count, 2);
        assert_eq!(built.cluster.superblock.dentry_count, 4);
        assert_eq!(
            built.local_node_ids[&b"data/a"[..]],
            built.local_node_ids[&b"data/b"[..]]
        );

        let superblock = ClusterSuperblock::decode(&built.cluster.bytes).unwrap();
        let root = superblock.index_roots[0];
        assert_eq!(root.kind, BatchKind::Namespace as u8);
        let index_start = root.object_offset as usize;
        let index = IndexNode::decode(
            &built.cluster.bytes[index_start..index_start + root.stored_len as usize],
        )
        .unwrap();
        let mut parents = Vec::new();
        let mut hardlink_entries = 0;
        for entry in index.entries {
            let IndexEntry::Leaf { locator, .. } = entry else {
                panic!("small fixture should have a leaf root")
            };
            let start = locator.object_offset as usize;
            let encoded = EncodedBatch::decode(
                &built.cluster.bytes[start..start + locator.total_stored_len as usize],
            )
            .unwrap();
            let namespace = NamespaceBatch::decode(&encoded).unwrap();
            parents.push(namespace.segments[0].parent_local_node_id);
            hardlink_entries += namespace.segments[0]
                .entries
                .iter()
                .filter(|entry| matches!(entry, NamespaceEntry::ExistingNode { .. }))
                .count();
        }
        assert_eq!(parents, vec![1, 2]);
        assert_eq!(hardlink_entries, 1);
    }

    #[test]
    fn source_inventory_rejects_missing_parent_and_conflicting_hardlink() {
        let missing_parent = entry(b"missing/file", EntryKind::File { size: 1 }, None);
        assert!(build_source_namespace_cluster([1; 16], [2; 16], &[missing_parent]).is_err());

        let mut left = entry(b"a", EntryKind::File { size: 1 }, Some(3));
        let mut right = entry(b"b", EntryKind::File { size: 2 }, Some(3));
        left.attributes.mode = 0o100600;
        right.attributes.mode = 0o100600;
        assert!(build_source_namespace_cluster([1; 16], [2; 16], &[left, right]).is_err());
    }

    #[test]
    fn sparse_ranges_are_complemented_without_reading_holes() {
        assert_eq!(
            complement_ranges(100, &[(10, 20), (70, 10)]).unwrap(),
            vec![(0, 10), (30, 40), (80, 20)]
        );
        assert!(complement_ranges(100, &[(20, 10), (25, 2)]).is_err());
        assert!(complement_ranges(100, &[(90, 11)]).is_err());
        assert!(complement_ranges(100, &[(0, 0)]).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn local_directory_source_is_connected_to_v2_builder() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("shards")).unwrap();
        std::fs::write(root.path().join("shards/part-000"), b"alpha").unwrap();
        std::fs::write(root.path().join("README"), b"root").unwrap();

        let built = build_local_directory_namespace_cluster(
            root.path(),
            ConsistencyPolicy::SnapshotBacked,
            [8; 16],
            [9; 16],
        )
        .unwrap();
        assert_eq!(built.cluster.superblock.dentry_count, 3);
        assert_eq!(built.cluster.superblock.directory_contribution_count, 2);
        assert_eq!(built.root_dir_key, derive_root_dir_key([9; 16]));
        assert!(
            built
                .local_node_ids
                .contains_key(b"shards/part-000".as_slice())
        );
    }

    #[cfg(unix)]
    #[test]
    fn local_directory_complete_builder_closes_metadata_and_data_graph() {
        use crate::workspace_overlay::clustered_snapshot::{DataPackSnapshot, DataSealSnapshot};

        let root = tempfile::tempdir().unwrap();
        let payload = vec![b'x'; (1 << 20) + 33];
        std::fs::write(root.path().join("payload"), &payload).unwrap();
        std::os::unix::fs::symlink("payload", root.path().join("latest")).unwrap();

        let built = build_local_directory_cluster(
            root.path(),
            ConsistencyPolicy::SnapshotBacked,
            [10; 16],
            [11; 16],
        )
        .unwrap();
        assert!(built.cluster.superblock.extent_count >= 2);
        assert!(built.cluster.superblock.slice_count >= 2);
        assert!(built.cluster.superblock.extent_batch_count >= 1);
        assert_eq!(built.cluster.superblock.attribute_batch_count, 1);

        let data = built.data.as_ref().expect("regular file data artifact");
        assert_eq!(data.frame_count, 2);
        assert_eq!(data.slice_count, 2);
        assert!(data.object_key.ends_with("/0.brfdp"));
        let pack = DataPackSnapshot::open(data.data_pack.clone()).unwrap();
        assert_eq!(pack.frames().len(), 2);
        let seal = DataSealSnapshot::open(data.data_seal.clone()).unwrap();
        assert_eq!(seal.slices().len(), 2);
        assert_eq!(seal.frames().len(), 2);
        assert_eq!(seal.objects().len(), 1);
        assert_eq!(seal.lookup_slice(1).unwrap().logical_len, 1 << 20);
        assert_eq!(seal.lookup_slice(2).unwrap().logical_len, 33);
    }

    #[cfg(unix)]
    #[test]
    fn complete_builder_emits_empty_seal_for_empty_file_only_source() {
        use crate::workspace_overlay::clustered_snapshot::{DataPackSnapshot, DataSealSnapshot};

        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("empty"), []).unwrap();
        let built = build_local_directory_cluster(
            root.path(),
            ConsistencyPolicy::SnapshotBacked,
            [12; 16],
            [13; 16],
        )
        .unwrap();
        assert_eq!(built.cluster.superblock.extent_count, 0);
        let data = built.data.as_ref().expect("empty data artifact");
        assert_eq!(data.frame_count, 0);
        assert!(
            DataPackSnapshot::open(data.data_pack.clone())
                .unwrap()
                .frames()
                .is_empty()
        );
        let seal = DataSealSnapshot::open(data.data_seal.clone()).unwrap();
        assert!(seal.slices().is_empty());
        assert!(seal.frames().is_empty());
        assert_eq!(seal.objects().len(), 1);
    }
}
