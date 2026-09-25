//! Compile a P3 reference directory plan into a v2 cluster object.
//!
//! This is the bridge between the in-memory reference planner
//! ([PlannedDirectory]) and the wire-format cluster object (superblock +
//! Merkle index + namespace batches).  The builder treats the planner as the
//! source of truth for canonical entries and window boundaries, and
//! materializes them into independently authenticated on-disk pieces.

use crate::native_base::wire::error::{WireError, WireResult};

use super::attribute::{AttributeBatch, attribute_index_key};
use super::batch::{
    BatchHeader, BatchKind, EncodedBatch, NamespaceBatch, NamespaceEntry, NamespaceSegment,
    NodeRecord, SEGMENT_CONTINUATION, SEGMENT_END, SEGMENT_START,
};
use super::cluster_format::{
    BatchLocator, ClusterSuperblock, FORMAT_MAJOR, FORMAT_MINOR, IndexRootRef, SUPERBLOCK_LEN,
    namespace_index_key,
};
use super::extent::{ExtentBatch, extent_index_key};
use super::identity::DirKey;
use super::index_builder::{IndexLeafEntry, build_index_tree};
use super::merge::{PlannedDirectory, SourceEntries};
use super::name::NameBytes;

/// Assembled cluster object with its decoded parts for verification.
#[derive(Clone, Debug)]
pub struct BuiltCluster {
    /// Raw bytes of the complete .brfc object.
    pub bytes: Vec<u8>,
    /// Decoded superblock.
    pub superblock: ClusterSuperblock,
    /// Number of namespace batches.
    pub namespace_batch_count: u32,
    /// Total dentry count across all batches.
    pub dentry_count: u64,
}

/// One parent-directory namespace segment supplied by an offline producer.
///
/// The segment already carries stable local node ids.  Keeping this input
/// separate from [`PlannedDirectory`] lets a producer assemble a complete
/// tree (rather than only one directory) while the reference planner remains
/// useful for merged-directory routing tests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceSegmentInput {
    pub parent_local_node_id: u32,
    pub parent_dir_key: DirKey,
    pub entries: Vec<NamespaceEntry>,
}

/// Assemble a namespace tree from parent-first, independently sorted
/// segments.  Each segment is split at the v2 4096-entry boundary and every
/// resulting batch remains independently authenticated and indexable.
pub fn build_namespace_segments_cluster(
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
    mount_dir_key: DirKey,
    node_count: u32,
    directory_contribution_count: u32,
    segments: &[NamespaceSegmentInput],
) -> WireResult<BuiltCluster> {
    let namespace_batches = encode_namespace_segment_batches(cluster_id, segments)?;
    let dentry_count = namespace_batches
        .iter()
        .map(|batch| u64::from(batch.header.record_count))
        .try_fold(0u64, |sum, count| sum.checked_add(count))
        .ok_or_else(|| WireError::LimitExceeded("cluster dentry count overflows u64".into()))?;
    assemble_cluster(
        cluster_id,
        volume_id,
        mount_dir_key,
        node_count,
        directory_contribution_count,
        dentry_count,
        0,
        0,
        namespace_batches,
        Vec::new(),
        Vec::new(),
    )
}

/// Assemble a complete multi-directory cluster from independently prepared
/// namespace, extent, and cold-attribute batches.
///
/// Namespace segments are still the source of node identity and directory
/// ordering.  Extent and attribute batches are placed in their own sections
/// and indexed independently, so a reader can fetch file ranges or cold
/// attributes without loading namespace predecessors.
pub fn build_namespace_segments_cluster_with_metadata(
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
    mount_dir_key: DirKey,
    node_count: u32,
    directory_contribution_count: u32,
    segments: &[NamespaceSegmentInput],
    extent_batches: &[ExtentBatch],
    attribute_batches: &[AttributeBatch],
) -> WireResult<BuiltCluster> {
    let namespace_batches = encode_namespace_segment_batches(cluster_id, segments)?;
    let extent_batches = extent_batches
        .iter()
        .map(|batch| {
            if batch.cluster_id != cluster_id {
                return Err(WireError::invalid(
                    "cluster builder",
                    "extent batch cluster id does not match cluster",
                ));
            }
            batch.encode()
        })
        .collect::<WireResult<Vec<_>>>()?;
    let attribute_batches = attribute_batches
        .iter()
        .map(|batch| {
            if batch.cluster_id != cluster_id {
                return Err(WireError::invalid(
                    "cluster builder",
                    "attribute batch cluster id does not match cluster",
                ));
            }
            batch.encode()
        })
        .collect::<WireResult<Vec<_>>>()?;
    let dentry_count = namespace_batches
        .iter()
        .map(|batch| u64::from(batch.header.record_count))
        .try_fold(0u64, |sum, count| sum.checked_add(count))
        .ok_or_else(|| WireError::LimitExceeded("cluster dentry count overflows u64".into()))?;
    let extent_count = extent_batches
        .iter()
        .map(|batch| u64::from(batch.header.record_count))
        .try_fold(0u64, |sum, count| sum.checked_add(count))
        .ok_or_else(|| WireError::LimitExceeded("extent count overflows u64".into()))?;
    let mut slice_ids = std::collections::BTreeSet::new();
    for batch in &extent_batches {
        let decoded = ExtentBatch::decode(batch)?;
        for segment in decoded.segments {
            for extent in segment.extents {
                slice_ids.insert(extent.slice_id);
            }
        }
    }
    assemble_cluster(
        cluster_id,
        volume_id,
        mount_dir_key,
        node_count,
        directory_contribution_count,
        dentry_count,
        extent_count,
        slice_ids.len() as u64,
        namespace_batches,
        extent_batches,
        attribute_batches,
    )
}

/// Compile a [PlannedDirectory] into a v2 cluster object.
///
/// The builder creates one namespace batch per source stream (since each
/// source in the plan is already a sorted, deduplicated range).  It then
/// builds a capacity-bounded multi-level Merkle index over those batches.
pub fn build_single_directory_cluster(
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
    mount_dir_key: DirKey,
    plan: &PlannedDirectory,
    parent_local_node_id: u32,
    parent_dir_key: DirKey,
) -> WireResult<BuiltCluster> {
    let (batches, node_id_counter, total_entries) =
        encode_namespace_batches(cluster_id, plan, parent_local_node_id, parent_dir_key)?;

    // Step 2: Describe the sorted leaf entries.  The index builder computes
    // the complete tree shape first, then patches final batch offsets after
    // all fixed-size index pages have been accounted for.
    let mut leaf_entries = Vec::with_capacity(batches.len());
    for batch in &batches {
        // Extract first and last name from the batch segments.
        let first_name = namespace_index_key(parent_local_node_id, &first_name_of_batch(batch)?);
        let last_name = namespace_index_key(parent_local_node_id, &last_name_of_batch(batch)?);

        let loc = BatchLocator {
            kind: BatchKind::Namespace,
            batch_id: batch.header.batch_id,
            stream_ordinal: batch.header.stream_ordinal,
            object_offset: 0, // filled in after layout
            total_stored_len: batch.bytes.len() as u32,
            raw_payload_len: batch.raw_payload.len() as u32,
            first_new_node_id: batch.header.first_new_node_id,
            new_node_count: batch.header.new_node_count,
            digest: batch.header.digest,
        };
        leaf_entries.push(IndexLeafEntry {
            first_key: first_name,
            last_key: last_name,
            locator: loc,
        });
    }

    let batch_lengths: Vec<usize> = batches.iter().map(|batch| batch.bytes.len()).collect();
    let index_tree = build_index_tree(
        BatchKind::Namespace,
        leaf_entries,
        &batch_lengths,
        SUPERBLOCK_LEN as u64,
        super::merkle_index::DEFAULT_INDEX_NODE_SIZE,
    )?;

    // Step 3: Build the cluster superblock.
    let index_root = index_tree.root;

    let metadata_semantic_hash = {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"BrewFS.BRFCL002.semantic.v2");
        hasher.update(mount_dir_key.as_ref());
        for batch in &batches {
            hasher.update(&batch.raw_payload);
        }
        *hasher.finalize().as_bytes()
    };
    let build_options_digest =
        *blake3::hash(b"BrewFS.BRFCL002.options.v2;index=4096;batch-raw=4194304;chunk=1048576")
            .as_bytes();

    let superblock = ClusterSuperblock {
        format_major: FORMAT_MAJOR,
        format_minor: FORMAT_MINOR,
        cluster_id,
        volume_id,
        mount_dir_key,
        metadata_semantic_hash,
        build_options_digest,
        node_count: node_id_counter.saturating_sub(1), // allocated children + root
        directory_contribution_count: plan.sources.len() as u32,
        dentry_count: total_entries,
        extent_count: 0,
        slice_count: 0,
        namespace_batch_count: batches.len() as u32,
        extent_batch_count: 0,
        attribute_batch_count: 0,
        chunk_size: 1 << 20,
        index_roots: [index_root, empty_root(), empty_root(), empty_root()],
    };

    // Step 4: Assemble the complete object.
    let total_size = usize::try_from(
        index_tree
            .batch_base_offset
            .checked_add(
                batches
                    .iter()
                    .try_fold(0u64, |sum, batch| sum.checked_add(batch.bytes.len() as u64))
                    .ok_or_else(|| {
                        WireError::LimitExceeded("cluster batch bytes overflow u64".into())
                    })?,
            )
            .ok_or_else(|| WireError::LimitExceeded("cluster size overflows u64".into()))?,
    )
    .map_err(|_| WireError::LimitExceeded("cluster size exceeds usize".into()))?;
    let mut bytes = Vec::with_capacity(total_size);
    bytes.extend_from_slice(&superblock.encode());
    for node in &index_tree.nodes {
        bytes.extend_from_slice(node);
    }
    for batch in &batches {
        bytes.extend_from_slice(&batch.bytes);
    }

    Ok(BuiltCluster {
        bytes,
        superblock,
        namespace_batch_count: batches.len() as u32,
        dentry_count: total_entries,
    })
}

/// Compile a directory plan together with independently encoded extent and
/// cold-attribute batches. Each batch kind gets its own Merkle index section;
/// no kind requires replaying or loading a preceding kind.
pub fn build_single_directory_cluster_with_metadata(
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
    mount_dir_key: DirKey,
    plan: &PlannedDirectory,
    parent_local_node_id: u32,
    parent_dir_key: DirKey,
    extent_batches: &[ExtentBatch],
    attribute_batches: &[AttributeBatch],
) -> WireResult<BuiltCluster> {
    let (namespace_batches, node_id_counter, dentry_count) =
        encode_namespace_batches(cluster_id, plan, parent_local_node_id, parent_dir_key)?;
    let extent_batches = extent_batches
        .iter()
        .map(|batch| {
            if batch.cluster_id != cluster_id {
                return Err(WireError::invalid(
                    "cluster builder",
                    "extent batch cluster id does not match cluster",
                ));
            }
            batch.encode()
        })
        .collect::<WireResult<Vec<_>>>()?;
    let attribute_batches = attribute_batches
        .iter()
        .map(|batch| {
            if batch.cluster_id != cluster_id {
                return Err(WireError::invalid(
                    "cluster builder",
                    "attribute batch cluster id does not match cluster",
                ));
            }
            batch.encode()
        })
        .collect::<WireResult<Vec<_>>>()?;
    let extent_count = extent_batches
        .iter()
        .map(|batch| u64::from(batch.header.record_count))
        .try_fold(0u64, |sum, count| sum.checked_add(count))
        .ok_or_else(|| WireError::LimitExceeded("extent count overflows u64".into()))?;
    let mut slice_ids = std::collections::BTreeSet::new();
    for batch in &extent_batches {
        let decoded = ExtentBatch::decode(batch)?;
        for segment in decoded.segments {
            for extent in segment.extents {
                slice_ids.insert(extent.slice_id);
            }
        }
    }
    assemble_cluster(
        cluster_id,
        volume_id,
        mount_dir_key,
        node_id_counter.saturating_sub(1),
        plan.sources.len() as u32,
        dentry_count,
        extent_count,
        slice_ids.len() as u64,
        namespace_batches,
        extent_batches,
        attribute_batches,
    )
}

fn assemble_cluster(
    cluster_id: [u8; 16],
    volume_id: [u8; 16],
    mount_dir_key: DirKey,
    node_count: u32,
    directory_contribution_count: u32,
    dentry_count: u64,
    extent_count: u64,
    slice_count: u64,
    namespace_batches: Vec<EncodedBatch>,
    extent_batches: Vec<EncodedBatch>,
    attribute_batches: Vec<EncodedBatch>,
) -> WireResult<BuiltCluster> {
    let mut index_roots = [empty_root(), empty_root(), empty_root(), empty_root()];
    let mut sections = Vec::new();
    let mut next_offset = SUPERBLOCK_LEN as u64;
    let mut semantic = blake3::Hasher::new();
    semantic.update(b"BrewFS.BRFCL002.semantic.v2");
    semantic.update(mount_dir_key.as_ref());

    for (kind, batches, root_index) in [
        (BatchKind::Namespace, namespace_batches, 0usize),
        (BatchKind::Extent, extent_batches, 1usize),
        (BatchKind::Attribute, attribute_batches, 2usize),
    ] {
        if batches.is_empty() {
            continue;
        }
        let leaves = leaf_entries_for_kind(kind, &batches)?;
        let lengths = batches
            .iter()
            .map(|batch| batch.bytes.len())
            .collect::<Vec<_>>();
        let tree = build_index_tree(
            kind,
            leaves,
            &lengths,
            next_offset,
            super::merkle_index::DEFAULT_INDEX_NODE_SIZE,
        )?;
        index_roots[root_index] = tree.root;
        let section_len = tree
            .nodes
            .iter()
            .map(Vec::len)
            .try_fold(0usize, |sum, len| sum.checked_add(len))
            .and_then(|sum| {
                batches
                    .iter()
                    .map(|batch| batch.bytes.len())
                    .try_fold(sum, |sum, len| sum.checked_add(len))
            })
            .ok_or_else(|| WireError::LimitExceeded("cluster section length overflows".into()))?;
        let mut section = Vec::with_capacity(section_len);
        for node in &tree.nodes {
            section.extend_from_slice(node);
        }
        for batch in &batches {
            semantic.update(&(kind as u16).to_le_bytes());
            semantic.update(&batch.raw_payload);
            section.extend_from_slice(&batch.bytes);
        }
        next_offset = next_offset
            .checked_add(section.len() as u64)
            .ok_or_else(|| WireError::LimitExceeded("cluster size overflows u64".into()))?;
        sections.push(section);
    }

    if index_roots[0].stored_len == 0 {
        return Err(WireError::invalid(
            "cluster builder",
            "namespace index must contain at least one batch",
        ));
    }
    let metadata_semantic_hash = *semantic.finalize().as_bytes();
    let build_options_digest =
        *blake3::hash(b"BrewFS.BRFCL002.options.v2;index=4096;batch-raw=4194304;chunk=1048576")
            .as_bytes();
    let superblock = ClusterSuperblock {
        format_major: FORMAT_MAJOR,
        format_minor: FORMAT_MINOR,
        cluster_id,
        volume_id,
        mount_dir_key,
        metadata_semantic_hash,
        build_options_digest,
        node_count,
        directory_contribution_count,
        dentry_count,
        extent_count,
        slice_count,
        namespace_batch_count: index_roots[0].entry_count,
        extent_batch_count: index_roots[1].entry_count,
        attribute_batch_count: index_roots[2].entry_count,
        chunk_size: 1 << 20,
        index_roots,
    };
    let total_size = usize::try_from(next_offset)
        .map_err(|_| WireError::LimitExceeded("cluster size exceeds usize".into()))?;
    let mut bytes = Vec::with_capacity(total_size);
    bytes.extend_from_slice(&superblock.encode());
    for section in sections {
        bytes.extend_from_slice(&section);
    }
    debug_assert_eq!(bytes.len(), total_size);
    Ok(BuiltCluster {
        bytes,
        superblock,
        namespace_batch_count: index_roots[0].entry_count,
        dentry_count,
    })
}

fn leaf_entries_for_kind(
    kind: BatchKind,
    batches: &[EncodedBatch],
) -> WireResult<Vec<IndexLeafEntry>> {
    batches
        .iter()
        .map(|batch| {
            if batch.header.kind != kind {
                return Err(WireError::invalid(
                    "cluster builder",
                    "batch kind does not match index kind",
                ));
            }
            let (first_key, last_key) = match kind {
                BatchKind::Namespace => {
                    let namespace = NamespaceBatch::decode(batch)?;
                    let first = namespace
                        .segments
                        .first()
                        .and_then(|segment| segment.entries.first())
                        .ok_or_else(|| WireError::invalid("namespace batch", "no entries"))?;
                    let last = namespace
                        .segments
                        .last()
                        .and_then(|segment| segment.entries.last())
                        .ok_or_else(|| WireError::invalid("namespace batch", "no entries"))?;
                    (
                        namespace_index_key(
                            namespace.segments[0].parent_local_node_id,
                            name_of(first).as_bytes(),
                        ),
                        namespace_index_key(
                            namespace.segments.last().unwrap().parent_local_node_id,
                            name_of(last).as_bytes(),
                        ),
                    )
                }
                BatchKind::Extent => {
                    let extent = ExtentBatch::decode(batch)?;
                    let first = extent.segments.first().unwrap();
                    let last = extent.segments.last().unwrap();
                    let mut last_offset = last.first_file_offset;
                    for record in &last.extents {
                        last_offset = last_offset
                            .checked_add(record.gap_from_previous_end)
                            .and_then(|offset| offset.checked_add(record.logical_length))
                            .ok_or_else(|| {
                                WireError::LimitExceeded("extent index key overflows".into())
                            })?;
                    }
                    (
                        extent_index_key(first.local_node_id, first.first_file_offset).to_vec(),
                        extent_index_key(last.local_node_id, last_offset).to_vec(),
                    )
                }
                BatchKind::Attribute => {
                    let attributes = AttributeBatch::decode(batch)?;
                    let first = attributes.groups.first().unwrap().local_node_id;
                    let last = attributes.groups.last().unwrap().local_node_id;
                    (
                        attribute_index_key(first).to_vec(),
                        attribute_index_key(last).to_vec(),
                    )
                }
                BatchKind::DirectoryProjection => {
                    return Err(WireError::UnsupportedFormat(
                        "directory projection assembly is not implemented".into(),
                    ));
                }
            };
            Ok(IndexLeafEntry {
                first_key,
                last_key,
                locator: BatchLocator {
                    kind,
                    batch_id: batch.header.batch_id,
                    stream_ordinal: batch.header.stream_ordinal,
                    object_offset: 0,
                    total_stored_len: u32::try_from(batch.bytes.len()).map_err(|_| {
                        WireError::LimitExceeded("metadata batch exceeds u32".into())
                    })?,
                    raw_payload_len: u32::try_from(batch.raw_payload.len()).map_err(|_| {
                        WireError::LimitExceeded("metadata batch payload exceeds u32".into())
                    })?,
                    first_new_node_id: batch.header.first_new_node_id,
                    new_node_count: batch.header.new_node_count,
                    digest: batch.header.digest,
                },
            })
        })
        .collect()
}

fn encode_namespace_batches(
    cluster_id: [u8; 16],
    plan: &PlannedDirectory,
    parent_local_node_id: u32,
    parent_dir_key: DirKey,
) -> WireResult<(Vec<EncodedBatch>, u32, u64)> {
    let mut batches = Vec::new();
    let mut total_entries = 0u64;
    let mut node_id_counter = 2u32;
    let mut next_batch_id = 0u32;
    let mut predecessor_ordinal = BatchHeader::NO_PREDECESSOR;

    for source in &plan.sources {
        let entries = source_entries_to_namespace_entries(
            source,
            parent_local_node_id,
            &mut node_id_counter,
        )?;
        total_entries = total_entries
            .checked_add(entries.len() as u64)
            .ok_or_else(|| WireError::LimitExceeded("cluster dentry count overflows u64".into()))?;
        let mut cursor = 0usize;
        while cursor < entries.len() {
            let remaining = entries.len() - cursor;
            let mut take = remaining.min(4096);
            let batch_id = next_batch_id;
            let stream_ordinal = next_batch_id;
            let is_first = cursor == 0;
            let encoded = loop {
                let end = cursor + take;
                let is_last = end == entries.len();
                let mut flags = if is_first {
                    SEGMENT_START
                } else {
                    super::batch::SEGMENT_CONTINUATION
                };
                if is_last {
                    flags |= SEGMENT_END;
                }
                let segment = NamespaceSegment {
                    parent_local_node_id,
                    parent_dir_key,
                    flags,
                    total_entry_count: is_first.then_some(entries.len() as u64),
                    entries: entries[cursor..end].to_vec(),
                };
                let batch = NamespaceBatch {
                    cluster_id,
                    batch_id,
                    stream_ordinal,
                    predecessor_ordinal,
                    segments: vec![segment],
                };
                match batch.encode() {
                    Ok(encoded) => break encoded,
                    Err(WireError::LimitExceeded(_)) if take > 1 => take = take.div_ceil(2),
                    Err(error) => return Err(error),
                }
            };
            batches.push(encoded);
            cursor += take;
            predecessor_ordinal = stream_ordinal;
            next_batch_id = next_batch_id.checked_add(1).ok_or_else(|| {
                WireError::LimitExceeded("namespace batch id overflows u32".into())
            })?;
        }
    }
    if batches.is_empty() {
        return Err(WireError::invalid("cluster builder", "plan has no sources"));
    }
    Ok((batches, node_id_counter, total_entries))
}

fn encode_namespace_segment_batches(
    cluster_id: [u8; 16],
    segments: &[NamespaceSegmentInput],
) -> WireResult<Vec<EncodedBatch>> {
    if segments.is_empty() {
        return Err(WireError::invalid(
            "cluster builder",
            "namespace segment list must not be empty",
        ));
    }
    let mut batches = Vec::new();
    let mut next_batch_id = 0u32;
    let mut predecessor_ordinal = BatchHeader::NO_PREDECESSOR;
    for segment in segments {
        if segment.parent_local_node_id == 0 || segment.entries.is_empty() {
            return Err(WireError::invalid(
                "cluster builder",
                "namespace segment has no parent or entries",
            ));
        }
        for pair in segment.entries.windows(2) {
            if name_of(&pair[0]) >= name_of(&pair[1]) {
                return Err(WireError::invalid(
                    "cluster builder",
                    "namespace segment names are not strictly sorted",
                ));
            }
        }
        let mut cursor = 0usize;
        while cursor < segment.entries.len() {
            let remaining = segment.entries.len() - cursor;
            let mut take = remaining.min(4096);
            let encoded = loop {
                let end = cursor + take;
                let flags = (cursor == 0)
                    .then_some(SEGMENT_START)
                    .unwrap_or(SEGMENT_CONTINUATION)
                    | if end == segment.entries.len() {
                        SEGMENT_END
                    } else {
                        0
                    };
                let namespace = NamespaceBatch {
                    cluster_id,
                    batch_id: next_batch_id,
                    stream_ordinal: next_batch_id,
                    predecessor_ordinal,
                    segments: vec![NamespaceSegment {
                        parent_local_node_id: segment.parent_local_node_id,
                        parent_dir_key: segment.parent_dir_key,
                        flags,
                        total_entry_count: (cursor == 0).then_some(segment.entries.len() as u64),
                        entries: segment.entries[cursor..end].to_vec(),
                    }],
                };
                match namespace.encode() {
                    Ok(encoded) => break encoded,
                    Err(WireError::LimitExceeded(_)) if take > 1 => take = take.div_ceil(2),
                    Err(error) => return Err(error),
                }
            };
            batches.push(encoded);
            cursor += take;
            predecessor_ordinal = next_batch_id;
            next_batch_id = next_batch_id.checked_add(1).ok_or_else(|| {
                WireError::LimitExceeded("namespace batch id overflows u32".into())
            })?;
        }
    }
    Ok(batches)
}

fn source_entries_to_namespace_entries(
    source: &SourceEntries,
    _parent_local_node_id: u32,
    node_id_counter: &mut u32,
) -> WireResult<Vec<NamespaceEntry>> {
    let mut out = Vec::with_capacity(source.entries.len());
    for entry in &source.entries {
        let name = entry.entry.name.clone();
        let kind = entry.entry.kind;
        let node_id = *node_id_counter;
        *node_id_counter = node_id_counter
            .checked_add(1)
            .ok_or_else(|| WireError::LimitExceeded("cluster builder: node id overflow".into()))?;

        let node = NodeRecord {
            local_node_id: node_id,
            kind,
            mode: mode_for_kind(kind),
            size: 0,
            dir_key: entry.child_dir_key,
        };
        out.push(NamespaceEntry::NewNode { name, node });
    }
    Ok(out)
}

fn mode_for_kind(kind: u8) -> u32 {
    match kind {
        2 => 0o040755, // directory
        1 => 0o100644, // regular file
        3 => 0o120777, // symlink
        _ => 0,
    }
}

fn first_name_of_batch(batch: &EncodedBatch) -> WireResult<Vec<u8>> {
    // We need to decode the batch to get the first name.
    // Since the batch was just encoded and is in our hands, this is cheap.
    let ns_batch = NamespaceBatch::decode(batch)?;
    let first = ns_batch
        .segments
        .first()
        .and_then(|seg| seg.entries.first())
        .ok_or_else(|| WireError::invalid("batch", "no entries"))?;
    Ok(name_of(first).as_bytes().to_vec())
}

fn last_name_of_batch(batch: &EncodedBatch) -> WireResult<Vec<u8>> {
    let ns_batch = NamespaceBatch::decode(batch)?;
    let last = ns_batch
        .segments
        .last()
        .and_then(|seg| seg.entries.last())
        .ok_or_else(|| WireError::invalid("batch", "no entries"))?;
    Ok(name_of(last).as_bytes().to_vec())
}

fn name_of(entry: &NamespaceEntry) -> &NameBytes {
    match entry {
        NamespaceEntry::NewNode { name, .. } => name,
        NamespaceEntry::ExistingNode { name, .. } => name,
    }
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
    use super::*;
    use crate::workspace_overlay::clustered_snapshot::attribute::AttributeGroup;
    use crate::workspace_overlay::clustered_snapshot::directory::{DirectoryIdentity, NodeRef};
    use crate::workspace_overlay::clustered_snapshot::extent::{ExtentRecord, ExtentSegment};
    use crate::workspace_overlay::clustered_snapshot::merge::{
        DirectoryContribution, build_directory_plan,
    };
    use crate::workspace_overlay::clustered_snapshot::merkle_index::DEFAULT_INDEX_NODE_SIZE;
    use crate::workspace_overlay::clustered_snapshot::merkle_index::{IndexEntry, IndexNode};
    use crate::workspace_overlay::clustered_snapshot::name::NameBytes;

    fn make_contribution(
        dir_key: DirKey,
        cluster_slot: u32,
        node_id: u32,
        entries: &[&str],
    ) -> DirectoryContribution {
        let mut contrib = DirectoryContribution {
            dir_key,
            node: NodeRef {
                cluster_slot,
                local_node_id: node_id,
            },
            attributes_digest: [0xAB; 32],
            entries: Vec::new(),
        };
        for (i, name) in entries.iter().enumerate() {
            contrib.entries.push(
                crate::workspace_overlay::clustered_snapshot::merge::ContributionEntry {
                    name: NameBytes::new(name.as_bytes().to_vec()).unwrap(),
                    inode: 100 + i as u64,
                    kind: 1,
                    child_dir_key: None,
                    attributes_digest: [0; 32],
                },
            );
        }
        contrib
    }

    #[test]
    fn single_contribution_builds_valid_cluster() {
        let dir_key = DirKey::new([7; 16]);
        let identity = DirectoryIdentity::from_dir_key([0; 32], dir_key);

        let entries: Vec<String> = (0..50).map(|i| format!("file_{:03}.txt", i)).collect();
        let entry_refs: Vec<&str> = entries.iter().map(|s| s.as_str()).collect();

        let contrib = make_contribution(dir_key, 0, 1, &entry_refs);
        let plan = build_directory_plan(identity, vec![contrib]).unwrap();
        assert_eq!(plan.visible_entry_count, 50);

        let cluster = build_single_directory_cluster(
            [0xCD; 16],
            [0xAB; 16],
            DirKey::new([0xFF; 16]),
            &plan,
            1,
            dir_key,
        )
        .unwrap();

        // Verify superblock round-trip
        let decoded_sb = ClusterSuperblock::decode(&cluster.bytes).unwrap();
        assert_eq!(decoded_sb.cluster_id, [0xCD; 16]);
        assert_eq!(decoded_sb.namespace_batch_count, 1);
        assert_eq!(decoded_sb.dentry_count, 50);

        // Verify index root
        let root = &decoded_sb.index_roots[0];
        assert_eq!(root.kind, BatchKind::Namespace as u8);
        assert_eq!(root.entry_count, 1);

        // Verify the index leaf
        let idx_start = root.object_offset as usize;
        let idx_end = idx_start + root.stored_len as usize;
        let idx_hash = blake3::hash(&cluster.bytes[idx_start..idx_end]);
        assert_eq!(idx_hash.as_bytes(), &root.digest);

        let leaf = IndexNode::decode(&cluster.bytes[idx_start..idx_end]).unwrap();
        assert_eq!(leaf.entries.len(), 1);

        // Verify the batch via the index
        match &leaf.entries[0] {
            IndexEntry::Leaf { locator, .. } => {
                let batch_start = locator.object_offset as usize;
                let batch_end = batch_start + locator.total_stored_len as usize;
                let decoded = EncodedBatch::decode(&cluster.bytes[batch_start..batch_end]).unwrap();
                assert_eq!(decoded.header.digest, locator.digest);
                let ns = NamespaceBatch::decode(&decoded).unwrap();
                assert_eq!(ns.segments[0].entries.len(), 50);
            }
            _ => panic!("expected leaf entry"),
        }
    }

    #[test]
    fn two_contributions_merge_into_two_source_batches() {
        // Two contributions to the same directory, each with different
        // non-overlapping names, produce two source batches.
        let dir_key = DirKey::new([7; 16]);
        let identity = DirectoryIdentity::from_dir_key([0; 32], dir_key);

        let entries_a: Vec<String> = (0..20).map(|i| format!("a_{:03}.txt", i)).collect();
        let entries_z: Vec<String> = (0..20).map(|i| format!("z_{:03}.txt", i)).collect();
        let refs_a: Vec<&str> = entries_a.iter().map(|s| s.as_str()).collect();
        let refs_z: Vec<&str> = entries_z.iter().map(|s| s.as_str()).collect();

        let contrib1 = make_contribution(dir_key, 0, 1, &refs_a);
        let contrib2 = make_contribution(dir_key, 1, 2, &refs_z);

        let plan = build_directory_plan(identity, vec![contrib1, contrib2]).unwrap();
        assert_eq!(plan.visible_entry_count, 40);
        // Two sources, each from a separate contributor.
        assert_eq!(plan.sources.len(), 2);

        let cluster = build_single_directory_cluster(
            [0xCD; 16],
            [0xAB; 16],
            DirKey::new([0xFF; 16]),
            &plan,
            1,
            dir_key,
        )
        .unwrap();

        let decoded_sb = ClusterSuperblock::decode(&cluster.bytes).unwrap();
        assert_eq!(decoded_sb.namespace_batch_count, 2);
        assert_eq!(decoded_sb.dentry_count, 40);

        // Verify we can read through the index to find both batches
        let root = &decoded_sb.index_roots[0];
        let idx_start = root.object_offset as usize;
        let idx_end = idx_start + root.stored_len as usize;
        let leaf = IndexNode::decode(&cluster.bytes[idx_start..idx_end]).unwrap();
        assert_eq!(leaf.entries.len(), 2);

        // First batch should start with "a_"
        match &leaf.entries[0] {
            IndexEntry::Leaf { first_key, .. } => {
                assert_eq!(&first_key[4..], b"a_000.txt");
            }
            _ => panic!(),
        }
        // Second batch should start with "z_"
        match &leaf.entries[1] {
            IndexEntry::Leaf { first_key, .. } => {
                assert_eq!(&first_key[4..], b"z_000.txt");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn planner_output_matches_cluster_content() {
        // Verify that the entries we read back from the cluster are the same
        // as what the planner produced as canonical entries.
        let dir_key = DirKey::new([7; 16]);
        let identity = DirectoryIdentity::from_dir_key([0; 32], dir_key);

        let names: Vec<String> = (0..30).map(|i| format!("entry_{:03}", i)).collect();
        let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let contrib = make_contribution(dir_key, 0, 1, &name_refs);
        let plan = build_directory_plan(identity, vec![contrib]).unwrap();
        let cluster = build_single_directory_cluster(
            [0xCD; 16],
            [0xAB; 16],
            DirKey::new([0xFF; 16]),
            &plan,
            1,
            dir_key,
        )
        .unwrap();

        let decoded_sb = ClusterSuperblock::decode(&cluster.bytes).unwrap();
        let root = &decoded_sb.index_roots[0];
        let idx_start = root.object_offset as usize;
        let idx_end = idx_start + root.stored_len as usize;
        let leaf = IndexNode::decode(&cluster.bytes[idx_start..idx_end]).unwrap();

        let mut all_names = Vec::new();
        for entry in &leaf.entries {
            if let IndexEntry::Leaf { locator, .. } = entry {
                let batch_start = locator.object_offset as usize;
                let batch_end = batch_start + locator.total_stored_len as usize;
                let decoded = EncodedBatch::decode(&cluster.bytes[batch_start..batch_end]).unwrap();
                let ns = NamespaceBatch::decode(&decoded).unwrap();
                for seg in &ns.segments {
                    for e in &seg.entries {
                        match e {
                            NamespaceEntry::NewNode { name, .. } => {
                                all_names.push(name.as_bytes().to_vec());
                            }
                            NamespaceEntry::ExistingNode { name, .. } => {
                                all_names.push(name.as_bytes().to_vec());
                            }
                        }
                    }
                }
            }
        }

        assert_eq!(all_names.len(), 30);
        for (i, name) in all_names.iter().enumerate() {
            assert_eq!(name, format!("entry_{:03}", i).as_bytes());
        }

        // Planner's canonical entry count matches
        assert_eq!(plan.visible_entry_count, all_names.len() as u64);
    }

    #[test]
    fn large_directory_cluster_reads_multilevel_index_and_all_batches() {
        const SOURCE_COUNT: usize = 240;

        let dir_key = DirKey::new([0x17; 16]);
        let identity = DirectoryIdentity::from_dir_key([0x22; 32], dir_key);
        let names: Vec<String> = (0..SOURCE_COUNT)
            .map(|index| format!("training_shard_{index:04}.bin"))
            .collect();
        let contributions: Vec<DirectoryContribution> = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                make_contribution(dir_key, index as u32, index as u32 + 1, &[name.as_str()])
            })
            .collect();

        let plan = build_directory_plan(identity, contributions).unwrap();
        assert_eq!(plan.sources.len(), SOURCE_COUNT);
        assert_eq!(plan.visible_entry_count, SOURCE_COUNT as u64);
        assert!(
            plan.windows.len() > 1,
            "source fan-out must be range-partitioned"
        );

        let cluster = build_single_directory_cluster(
            [0x31; 16],
            [0x41; 16],
            DirKey::new([0x51; 16]),
            &plan,
            1,
            dir_key,
        )
        .unwrap();
        let superblock = ClusterSuperblock::decode(&cluster.bytes).unwrap();
        let root = superblock.index_roots[0];
        assert!(root.level >= 1, "240 batches must require an internal root");
        assert_eq!(root.entry_count as usize, SOURCE_COUNT);

        let locators = read_index_tree(&cluster.bytes, &root, BatchKind::Namespace);
        assert_eq!(locators.len(), SOURCE_COUNT);

        let mut batch_ids: Vec<u32> = locators.iter().map(|locator| locator.batch_id).collect();
        batch_ids.sort_unstable();
        assert_eq!(batch_ids, (0..SOURCE_COUNT as u32).collect::<Vec<_>>());

        for &index in &[0usize, 1, 79, 160, SOURCE_COUNT - 1] {
            let locator = locators
                .iter()
                .find(|locator| locator.batch_id == index as u32)
                .expect("every batch must be reachable from the root");
            let start = usize::try_from(locator.object_offset).unwrap();
            let end = start + locator.total_stored_len as usize;
            let encoded = EncodedBatch::decode(&cluster.bytes[start..end]).unwrap();
            assert_eq!(encoded.header.digest, locator.digest);
            assert_eq!(encoded.header.batch_id, index as u32);
            let namespace = NamespaceBatch::decode(&encoded).unwrap();
            assert_eq!(namespace.segments.len(), 1);
            assert_eq!(namespace.segments[0].entries.len(), 1);
            let actual_name = match &namespace.segments[0].entries[0] {
                NamespaceEntry::NewNode { name, .. }
                | NamespaceEntry::ExistingNode { name, .. } => name.as_bytes(),
            };
            assert_eq!(actual_name, names[index].as_bytes());
        }
    }

    #[test]
    fn large_source_is_split_into_independently_decodable_batches() {
        let dir_key = DirKey::new([0x61; 16]);
        let identity = DirectoryIdentity::from_dir_key([0x62; 32], dir_key);
        let names: Vec<String> = (0..9_000)
            .map(|index| format!("training_sample_{index:05}.bin"))
            .collect();
        let name_refs = names.iter().map(String::as_str).collect::<Vec<_>>();
        let plan =
            build_directory_plan(identity, vec![make_contribution(dir_key, 0, 1, &name_refs)])
                .unwrap();

        let cluster =
            build_single_directory_cluster([0x63; 16], [0x64; 16], dir_key, &plan, 1, dir_key)
                .unwrap();
        assert!(cluster.namespace_batch_count >= 3);
        assert_eq!(cluster.dentry_count, names.len() as u64);

        let root = ClusterSuperblock::decode(&cluster.bytes)
            .unwrap()
            .index_roots[0];
        let locators = read_index_tree(&cluster.bytes, &root, BatchKind::Namespace);
        assert_eq!(locators.len(), cluster.namespace_batch_count as usize);
        for locator in locators {
            let start = locator.object_offset as usize;
            let end = start + locator.total_stored_len as usize;
            let encoded = EncodedBatch::decode(&cluster.bytes[start..end]).unwrap();
            let decoded = NamespaceBatch::decode(&encoded).unwrap();
            assert_eq!(decoded.segments.len(), 1);
            assert!(decoded.segments[0].entries.len() <= 4096);
        }
    }

    #[test]
    fn extended_builder_assembles_extent_and_attribute_indexes() {
        let dir_key = DirKey::new([0x71; 16]);
        let identity = DirectoryIdentity::from_dir_key([0x72; 32], dir_key);
        let contribution = make_contribution(dir_key, 0, 1, &["sample"]);
        let plan = build_directory_plan(identity, vec![contribution]).unwrap();
        let cluster_id = [0x73; 16];
        let extent = ExtentBatch {
            cluster_id,
            batch_id: 0,
            stream_ordinal: 0,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![ExtentSegment {
                local_node_id: 2,
                first_file_offset: 0,
                extents: vec![ExtentRecord {
                    gap_from_previous_end: 0,
                    logical_length: 4096,
                    slice_id: 9,
                    slice_offset: 0,
                }],
            }],
        };
        let attribute = AttributeBatch {
            cluster_id,
            batch_id: 0,
            stream_ordinal: 0,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            groups: vec![AttributeGroup {
                local_node_id: 2,
                symlink_target: None,
                xattrs: Vec::new(),
                acl: None,
            }],
        };
        let built = build_single_directory_cluster_with_metadata(
            cluster_id,
            [0x74; 16],
            dir_key,
            &plan,
            1,
            dir_key,
            &[extent],
            &[attribute],
        )
        .unwrap();
        let superblock = ClusterSuperblock::decode(&built.bytes).unwrap();
        assert_eq!(superblock.extent_count, 1);
        assert_eq!(superblock.slice_count, 1);
        assert_eq!(superblock.extent_batch_count, 1);
        assert_eq!(superblock.attribute_batch_count, 1);
        for (index, kind) in [(1usize, BatchKind::Extent), (2usize, BatchKind::Attribute)] {
            let root = superblock.index_roots[index];
            assert_eq!(root.kind, kind as u8);
            let start = root.object_offset as usize;
            let node =
                IndexNode::decode(&built.bytes[start..start + root.stored_len as usize]).unwrap();
            assert_eq!(node.entries.len(), 1);
            let IndexEntry::Leaf { locator, .. } = &node.entries[0] else {
                panic!("expected metadata leaf")
            };
            let batch_start = locator.object_offset as usize;
            let batch_end = batch_start + locator.total_stored_len as usize;
            let encoded = EncodedBatch::decode(&built.bytes[batch_start..batch_end]).unwrap();
            assert_eq!(encoded.header.kind, kind);
        }
    }

    fn read_index_tree(
        cluster: &[u8],
        root: &IndexRootRef,
        expected_kind: BatchKind,
    ) -> Vec<BatchLocator> {
        let start = usize::try_from(root.object_offset).unwrap();
        let end = start + root.stored_len as usize;
        let root_bytes = &cluster[start..end];
        assert_eq!(blake3::hash(root_bytes).as_bytes(), &root.digest);
        let node = IndexNode::decode(root_bytes).unwrap();
        assert_eq!(node.level, root.level);
        assert_eq!(node.kind, expected_kind);
        read_index_node(cluster, node, root.level, expected_kind)
    }

    fn read_index_node(
        cluster: &[u8],
        node: IndexNode,
        expected_level: u8,
        expected_kind: BatchKind,
    ) -> Vec<BatchLocator> {
        assert_eq!(node.level, expected_level);
        assert_eq!(node.kind, expected_kind);
        let mut locators = Vec::new();
        for entry in node.entries {
            match entry {
                IndexEntry::Leaf { locator, .. } => {
                    assert_eq!(expected_level, 0);
                    locators.push(locator);
                }
                IndexEntry::Internal(child) => {
                    assert!(expected_level > 0);
                    assert_eq!(child.stored_len as usize, DEFAULT_INDEX_NODE_SIZE);
                    let start = usize::try_from(child.object_offset).unwrap();
                    let end = start + child.stored_len as usize;
                    let child_bytes = &cluster[start..end];
                    assert_eq!(blake3::hash(child_bytes).as_bytes(), &child.digest);
                    let child_node = IndexNode::decode(child_bytes).unwrap();
                    assert_eq!(child_node.level, expected_level - 1);
                    assert_eq!(
                        child.visible_subtree_count,
                        Some(child_node_leaf_count(
                            &child_node,
                            cluster,
                            expected_level - 1
                        )),
                    );
                    locators.extend(read_index_node(
                        cluster,
                        child_node,
                        expected_level - 1,
                        expected_kind,
                    ));
                }
            }
        }
        locators
    }

    fn child_node_leaf_count(node: &IndexNode, cluster: &[u8], level: u8) -> u64 {
        if level == 0 {
            return node.entries.len() as u64;
        }
        node.entries
            .iter()
            .map(|entry| match entry {
                IndexEntry::Internal(child) => {
                    let start = usize::try_from(child.object_offset).unwrap();
                    let end = start + child.stored_len as usize;
                    let child_node = IndexNode::decode(&cluster[start..end]).unwrap();
                    child_node_leaf_count(&child_node, cluster, level - 1)
                }
                IndexEntry::Leaf { .. } => panic!("internal node contains a leaf entry"),
            })
            .sum()
    }
}
