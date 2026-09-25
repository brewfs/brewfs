//! Deterministic multi-level Merkle index layout for a clustered metadata
//! object.
//!
//! The wire codec in [`super::merkle_index`] deliberately knows nothing about
//! object layout.  This module supplies the missing producer-side step: split
//! sorted leaf entries into capacity-bounded nodes, build parent levels until
//! one root remains, assign offsets, and then encode the nodes with the final
//! child digests and batch locators.

use crate::native_base::wire::error::{WireError, WireResult};

use super::batch::BatchKind;
use super::cluster_format::{BatchLocator, IndexRootRef};
use super::merkle_index::{ChildRef, DEFAULT_INDEX_NODE_SIZE, IndexNode};

/// Input for one leaf index entry.  Entries must be sorted by raw name and
/// must not overlap their predecessor's range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexLeafEntry {
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub locator: BatchLocator,
}

/// A fully materialized index tree.  Nodes are ordered by level, from leaves
/// to the root.  The caller can append them directly after the cluster
/// superblock; batches are expected to follow the returned `batch_base_offset`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuiltIndexTree {
    pub nodes: Vec<Vec<u8>>,
    pub root: IndexRootRef,
    pub batch_base_offset: u64,
    pub node_size: usize,
}

#[derive(Clone, Debug)]
struct NodeShape {
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    entry_count: u32,
    subtree_count: u64,
    child_start: usize,
    child_len: usize,
}

/// Build a capacity-bounded, multi-level index tree.
///
/// `object_start` is the first byte available for index nodes (normally the
/// fixed cluster superblock length).  `batch_lengths` is in the same order as
/// `entries`; the builder places those batches immediately after all index
/// nodes and patches every leaf locator with the resulting absolute offset.
pub fn build_index_tree(
    kind: BatchKind,
    entries: Vec<IndexLeafEntry>,
    batch_lengths: &[usize],
    object_start: u64,
    node_size: usize,
) -> WireResult<BuiltIndexTree> {
    if entries.is_empty() {
        return Err(WireError::invalid("index builder", "no leaf entries"));
    }
    if entries.len() != batch_lengths.len() {
        return Err(WireError::invalid(
            "index builder",
            "batch length count does not match leaf count",
        ));
    }
    if node_size != DEFAULT_INDEX_NODE_SIZE {
        // The current cluster layout has fixed-size index pages.  Keeping the
        // check explicit prevents a caller from producing roots that a reader
        // cannot range-fetch using the fixed page contract.
        return Err(WireError::invalid(
            "index builder",
            "only the default index node size is supported",
        ));
    }

    validate_leaf_entries(kind, &entries)?;

    // First determine the complete tree shape using zero offsets/digests.
    // Those fields are fixed-width, so replacing them later cannot change
    // which entries fit in a node.
    let mut levels = vec![plan_leaf_level(kind, &entries, node_size)?];
    while levels.last().is_some_and(|level| level.len() > 1) {
        let previous = levels.last().expect("level exists");
        let next_level = plan_internal_level(kind, previous, levels.len() as u8, node_size)?;
        levels.push(next_level);
    }

    let node_count = levels.iter().map(Vec::len).sum::<usize>();
    let node_bytes = node_count
        .checked_mul(node_size)
        .ok_or_else(|| WireError::LimitExceeded("index node layout overflows usize".into()))?;
    let batch_base_offset = object_start
        .checked_add(node_bytes as u64)
        .ok_or_else(|| WireError::LimitExceeded("index layout offset overflows u64".into()))?;

    // Physical offsets are stable because every index node is exactly
    // `node_size` bytes and levels are laid out leaves-first.
    let mut level_offsets = Vec::with_capacity(levels.len());
    let mut next_offset = object_start;
    for level in &levels {
        let mut offsets = Vec::with_capacity(level.len());
        for _ in level {
            offsets.push(next_offset);
            next_offset = next_offset.checked_add(node_size as u64).ok_or_else(|| {
                WireError::LimitExceeded("index node offset overflows u64".into())
            })?;
        }
        level_offsets.push(offsets);
    }

    let mut batch_offsets = Vec::with_capacity(batch_lengths.len());
    let mut next_batch_offset = batch_base_offset;
    for &length in batch_lengths {
        let length_u64 = u64::try_from(length)
            .map_err(|_| WireError::LimitExceeded("batch length exceeds u64".into()))?;
        batch_offsets.push(next_batch_offset);
        next_batch_offset = next_batch_offset
            .checked_add(length_u64)
            .ok_or_else(|| WireError::LimitExceeded("batch offset overflows u64".into()))?;
    }

    // Materialize leaves with their final batch offsets, then parents with
    // authenticated child offsets and digests.
    let mut encoded_levels: Vec<Vec<Vec<u8>>> = Vec::with_capacity(levels.len());
    let leaf_shapes = &levels[0];
    let mut encoded_leaves = Vec::with_capacity(leaf_shapes.len());
    for shape in leaf_shapes {
        let mut leaf_entries = Vec::with_capacity(shape.child_len);
        for index in shape.child_start..shape.child_start + shape.child_len {
            let mut entry = entries[index].clone();
            entry.locator.object_offset = batch_offsets[index];
            let locator = entry.locator;
            leaf_entries.push((entry.first_key, entry.last_key, locator));
        }
        let node = IndexNode::leaf(kind, leaf_entries);
        encoded_leaves.push(node.encode(node_size)?);
    }
    encoded_levels.push(encoded_leaves);

    for level_index in 1..levels.len() {
        let shapes = &levels[level_index];
        let previous_shapes = &levels[level_index - 1];
        let previous_bytes = &encoded_levels[level_index - 1];
        let mut encoded_nodes = Vec::with_capacity(shapes.len());
        for shape in shapes {
            let mut children = Vec::with_capacity(shape.child_len);
            for child_index in shape.child_start..shape.child_start + shape.child_len {
                let child_shape = &previous_shapes[child_index];
                let child_digest = blake3::hash(&previous_bytes[child_index]);
                children.push(ChildRef {
                    object_offset: level_offsets[level_index - 1][child_index],
                    stored_len: node_size as u32,
                    raw_len: node_size as u32,
                    entry_count: child_shape.entry_count,
                    digest: *child_digest.as_bytes(),
                    first_key: child_shape.first_key.clone(),
                    last_key: child_shape.last_key.clone(),
                    visible_subtree_count: Some(child_shape.subtree_count),
                });
            }
            let node = IndexNode::internal_at_level(level_index as u8, kind, children, true);
            encoded_nodes.push(node.encode(node_size)?);
        }
        encoded_levels.push(encoded_nodes);
    }

    let mut nodes = Vec::with_capacity(node_count);
    for level in encoded_levels {
        nodes.extend(level);
    }
    let root_level = (levels.len() - 1) as u8;
    let root_index = levels[root_level as usize].len() - 1;
    let root_offset = level_offsets[root_level as usize][root_index];
    let root_bytes = &nodes[levels[..root_level as usize]
        .iter()
        .map(Vec::len)
        .sum::<usize>()
        + root_index];
    let root_shape = &levels[root_level as usize][root_index];
    let root_digest = blake3::hash(root_bytes);
    let key_fingerprint = fingerprint(&root_shape.first_key);
    let entry_count = u32::try_from(root_shape.subtree_count)
        .map_err(|_| WireError::LimitExceeded("index root entry count exceeds u32".into()))?;

    Ok(BuiltIndexTree {
        nodes,
        root: IndexRootRef {
            object_offset: root_offset,
            stored_len: node_size as u32,
            raw_len: node_size as u32,
            level: root_level,
            kind: kind as u8,
            entry_count,
            digest: *root_digest.as_bytes(),
            key_fingerprint,
        },
        batch_base_offset,
        node_size,
    })
}

fn validate_leaf_entries(kind: BatchKind, entries: &[IndexLeafEntry]) -> WireResult<()> {
    let mut previous_last: Option<&[u8]> = None;
    for entry in entries {
        if entry.first_key.is_empty() || entry.last_key.is_empty() {
            return Err(WireError::invalid("index builder", "empty key"));
        }
        if entry.first_key > entry.last_key {
            return Err(WireError::invalid(
                "index builder",
                "leaf first key exceeds last key",
            ));
        }
        if let Some(last) = previous_last {
            if entry.first_key.as_slice() <= last {
                return Err(WireError::invalid(
                    "index builder",
                    "leaf ranges are not strictly ordered",
                ));
            }
        }
        if entry.locator.kind != kind {
            return Err(WireError::invalid(
                "index builder",
                "leaf locator kind does not match index kind",
            ));
        }
        previous_last = Some(&entry.last_key);
    }
    Ok(())
}

fn plan_leaf_level(
    kind: BatchKind,
    entries: &[IndexLeafEntry],
    node_size: usize,
) -> WireResult<Vec<NodeShape>> {
    let mut shapes = Vec::new();
    let mut start = 0usize;
    while start < entries.len() {
        let mut end = start + 1;
        let mut last_fit = start;
        while end <= entries.len() {
            let candidate = entries[start..end]
                .iter()
                .map(|entry| {
                    (
                        entry.first_key.clone(),
                        entry.last_key.clone(),
                        entry.locator.clone(),
                    )
                })
                .collect();
            match IndexNode::leaf(kind, candidate).encode(node_size) {
                Ok(_) => {
                    last_fit = end;
                    end += 1;
                }
                Err(WireError::LimitExceeded(_)) if last_fit > start => break,
                Err(error) => return Err(error),
            }
        }
        if last_fit == start {
            return Err(WireError::LimitExceeded(
                "one leaf index entry exceeds node capacity".into(),
            ));
        }
        let end = last_fit;
        let entry_count = u32::try_from(end - start)
            .map_err(|_| WireError::LimitExceeded("leaf entry count exceeds u32".into()))?;
        shapes.push(NodeShape {
            first_key: entries[start].first_key.clone(),
            last_key: entries[end - 1].last_key.clone(),
            entry_count,
            subtree_count: (end - start) as u64,
            child_start: start,
            child_len: end - start,
        });
        start = end;
    }
    Ok(shapes)
}

fn plan_internal_level(
    kind: BatchKind,
    children: &[NodeShape],
    level: u8,
    node_size: usize,
) -> WireResult<Vec<NodeShape>> {
    let mut shapes = Vec::new();
    let mut start = 0usize;
    while start < children.len() {
        let mut end = start + 1;
        let mut last_fit = start;
        while end <= children.len() {
            let candidate = children[start..end]
                .iter()
                .map(|child| ChildRef {
                    object_offset: 0,
                    stored_len: node_size as u32,
                    raw_len: node_size as u32,
                    entry_count: child.entry_count,
                    digest: [0; 32],
                    first_key: child.first_key.clone(),
                    last_key: child.last_key.clone(),
                    visible_subtree_count: Some(child.subtree_count),
                })
                .collect();
            match IndexNode::internal_at_level(level, kind, candidate, true).encode(node_size) {
                Ok(_) => {
                    last_fit = end;
                    end += 1;
                }
                Err(WireError::LimitExceeded(_)) if last_fit > start => break,
                Err(error) => return Err(error),
            }
        }
        if last_fit == start {
            return Err(WireError::LimitExceeded(
                "one internal index entry exceeds node capacity".into(),
            ));
        }
        let end = last_fit;
        let entry_count = u32::try_from(end - start)
            .map_err(|_| WireError::LimitExceeded("internal entry count exceeds u32".into()))?;
        let subtree_count = children[start..end]
            .iter()
            .try_fold(0u64, |sum, child| sum.checked_add(child.subtree_count))
            .ok_or_else(|| WireError::LimitExceeded("index subtree count overflows u64".into()))?;
        shapes.push(NodeShape {
            first_key: children[start].first_key.clone(),
            last_key: children[end - 1].last_key.clone(),
            entry_count,
            subtree_count,
            child_start: start,
            child_len: end - start,
        });
        start = end;
    }
    Ok(shapes)
}

fn fingerprint(key: &[u8]) -> u64 {
    let digest = blake3::hash(key);
    u64::from_le_bytes(digest.as_bytes()[..8].try_into().expect("eight bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_overlay::clustered_snapshot::batch::BatchKind;
    use crate::workspace_overlay::clustered_snapshot::merkle_index::IndexEntry;

    fn entry(index: usize) -> IndexLeafEntry {
        IndexLeafEntry {
            first_key: format!("file_{index:06}_a").into_bytes(),
            last_key: format!("file_{index:06}_z").into_bytes(),
            locator: BatchLocator {
                kind: BatchKind::Namespace,
                batch_id: index as u32,
                stream_ordinal: index as u32,
                object_offset: 0,
                total_stored_len: 200,
                raw_payload_len: 100,
                first_new_node_id: index as u32 + 1,
                new_node_count: 1,
                digest: [index as u8; 32],
            },
        }
    }

    #[test]
    fn small_tree_keeps_single_leaf_root() {
        let tree = build_index_tree(
            BatchKind::Namespace,
            (0..8).map(entry).collect(),
            &[200; 8],
            4096,
            DEFAULT_INDEX_NODE_SIZE,
        )
        .unwrap();
        assert_eq!(tree.nodes.len(), 1);
        assert_eq!(tree.root.level, 0);
        assert_eq!(tree.root.entry_count, 8);
        assert_eq!(
            tree.batch_base_offset,
            4096 + DEFAULT_INDEX_NODE_SIZE as u64
        );
    }

    #[test]
    fn large_tree_builds_multiple_levels_and_resolves_children() {
        let count = 240usize;
        let tree = build_index_tree(
            BatchKind::Namespace,
            (0..count).map(entry).collect(),
            &vec![200; count],
            4096,
            DEFAULT_INDEX_NODE_SIZE,
        )
        .unwrap();
        assert!(tree.root.level >= 1);
        assert!(tree.nodes.len() > 1);
        assert_eq!(tree.root.entry_count, count as u32);

        let root_offset = tree.root.object_offset as usize - 4096;
        let root_index = root_offset / DEFAULT_INDEX_NODE_SIZE;
        let root = IndexNode::decode(&tree.nodes[root_index]).unwrap();
        assert_eq!(root.level, tree.root.level);
        assert!(root.entries.len() > 1);
        for child in root.entries {
            let IndexEntry::Internal(child) = child else {
                panic!("root must contain internal references")
            };
            let index = (child.object_offset as usize - 4096) / DEFAULT_INDEX_NODE_SIZE;
            assert_eq!(child.digest, *blake3::hash(&tree.nodes[index]).as_bytes());
            assert!(child.visible_subtree_count.unwrap_or(0) > 0);
        }
    }
}
