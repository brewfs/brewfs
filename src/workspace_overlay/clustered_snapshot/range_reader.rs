//! Bounded range reads for clustered snapshot objects.
//!
//! The memory implementation is used by producer/read-path tests and as the
//! reference contract for a later S3 range reader.  It deliberately performs
//! no prefetching or caching: each call returns exactly one authenticated byte
//! range and all higher-level decoders remain independent of the transport.

use super::batch::{BATCH_HEADER_LEN, BatchKind, EncodedBatch, MAX_BATCH_STORED};
use super::cluster_format::{BatchLocator, IndexRootRef};
use super::merkle_index::{ChildRef, IndexNode};
use crate::native_base::wire::error::{WireError, WireResult};

/// The largest range a metadata reader may request in one operation.
pub const MAX_RANGE_BYTES: usize = BATCH_HEADER_LEN + MAX_BATCH_STORED;

/// Transport-neutral bounded range reader.
pub trait RangeReader {
    fn object_len(&self) -> u64;
    fn read_range(&self, offset: u64, len: u32) -> WireResult<Vec<u8>>;
}

/// In-memory reference reader for a complete immutable object.
#[derive(Clone, Debug)]
pub struct MemoryRangeReader {
    bytes: Vec<u8>,
}

impl MemoryRangeReader {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

impl RangeReader for MemoryRangeReader {
    fn object_len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn read_range(&self, offset: u64, len: u32) -> WireResult<Vec<u8>> {
        let len = usize::try_from(len)
            .map_err(|_| WireError::LimitExceeded("range length exceeds usize".into()))?;
        if len > MAX_RANGE_BYTES {
            return Err(WireError::LimitExceeded(format!(
                "range length {len} exceeds {MAX_RANGE_BYTES}"
            )));
        }
        let start = usize::try_from(offset)
            .map_err(|_| WireError::LimitExceeded("range offset exceeds usize".into()))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| WireError::LimitExceeded("range end overflows usize".into()))?;
        if end > self.bytes.len() {
            return Err(WireError::Truncated {
                what: "object range",
                need: len,
                have: self.bytes.len().saturating_sub(start),
            });
        }
        Ok(self.bytes[start..end].to_vec())
    }
}

/// Fetch, authenticate, and decode an index root.
pub fn read_index_root<R: RangeReader>(reader: &R, root: &IndexRootRef) -> WireResult<IndexNode> {
    let bytes = reader.read_range(root.object_offset, root.stored_len)?;
    verify_digest("index root", &bytes, &root.digest)?;
    let node = IndexNode::decode(&bytes)?;
    if node.level != root.level || node.kind as u8 != root.kind {
        return Err(WireError::invalid(
            "index root",
            "decoded node does not match root reference",
        ));
    }
    if node.entries.len() != root.entry_count as usize && root.level == 0 {
        return Err(WireError::invalid(
            "index root",
            "leaf entry count does not match root reference",
        ));
    }
    Ok(node)
}

/// Fetch, authenticate, and decode one internal index child.
pub fn read_index_child<R: RangeReader>(
    reader: &R,
    child: &ChildRef,
    expected_kind: BatchKind,
    expected_level: u8,
) -> WireResult<IndexNode> {
    let bytes = reader.read_range(child.object_offset, child.stored_len)?;
    verify_digest("index child", &bytes, &child.digest)?;
    let node = IndexNode::decode(&bytes)?;
    if node.level != expected_level || node.kind != expected_kind {
        return Err(WireError::invalid(
            "index child",
            "decoded node does not match child reference",
        ));
    }
    if node.entries.len() != child.entry_count as usize {
        return Err(WireError::invalid(
            "index child",
            "entry count does not match child reference",
        ));
    }
    Ok(node)
}

/// Fetch, authenticate, and decode one independently decodable metadata batch.
pub fn read_batch<R: RangeReader>(reader: &R, locator: &BatchLocator) -> WireResult<EncodedBatch> {
    let bytes = reader.read_range(locator.object_offset, locator.total_stored_len)?;
    let batch = EncodedBatch::decode(&bytes)?;
    if batch.header.kind != locator.kind
        || batch.header.batch_id != locator.batch_id
        || batch.header.stream_ordinal != locator.stream_ordinal
        || BATCH_HEADER_LEN as u64 + batch.header.stored_payload_len
            != u64::from(locator.total_stored_len)
        || batch.header.raw_payload_len != u64::from(locator.raw_payload_len)
        || batch.header.first_new_node_id != locator.first_new_node_id
        || batch.header.new_node_count != locator.new_node_count
        || batch.header.digest != locator.digest
    {
        return Err(WireError::invalid(
            "metadata batch",
            "decoded header does not match locator",
        ));
    }
    Ok(batch)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace_overlay::clustered_snapshot::batch::{
        BatchHeader, NamespaceBatch, NamespaceEntry, NamespaceSegment, NodeRecord, SEGMENT_END,
        SEGMENT_START,
    };
    use crate::workspace_overlay::clustered_snapshot::identity::DirKey;
    use crate::workspace_overlay::clustered_snapshot::merkle_index::DEFAULT_INDEX_NODE_SIZE;
    use crate::workspace_overlay::clustered_snapshot::name::NameBytes;

    #[test]
    fn memory_reader_returns_only_bounded_ranges() {
        let reader = MemoryRangeReader::new((0..=255).collect());
        assert_eq!(reader.object_len(), 256);
        assert_eq!(reader.read_range(10, 3).unwrap(), vec![10, 11, 12]);
        assert!(matches!(
            reader.read_range(255, 2),
            Err(WireError::Truncated { .. })
        ));
        assert!(matches!(
            reader.read_range(0, (MAX_RANGE_BYTES as u32).saturating_add(1)),
            Err(WireError::LimitExceeded(_))
        ));
    }

    #[test]
    fn index_and_batch_helpers_authenticate_referenced_ranges() {
        let index = IndexNode::leaf(
            BatchKind::Namespace,
            vec![(
                b"file_a".to_vec(),
                b"file_a".to_vec(),
                BatchLocator {
                    kind: BatchKind::Namespace,
                    batch_id: 0,
                    stream_ordinal: 0,
                    object_offset: 0,
                    total_stored_len: 0,
                    raw_payload_len: 0,
                    first_new_node_id: 1,
                    new_node_count: 1,
                    digest: [0; 32],
                },
            )],
        );
        let index_bytes = index.encode(DEFAULT_INDEX_NODE_SIZE).unwrap();
        let root = IndexRootRef {
            object_offset: 0,
            stored_len: index_bytes.len() as u32,
            raw_len: index_bytes.len() as u32,
            level: 0,
            kind: BatchKind::Namespace as u8,
            entry_count: 1,
            digest: *blake3::hash(&index_bytes).as_bytes(),
            key_fingerprint: 0,
        };
        let reader = MemoryRangeReader::new(index_bytes);
        let decoded = read_index_root(&reader, &root).unwrap();
        assert_eq!(decoded.entries.len(), 1);

        let batch = NamespaceBatch {
            cluster_id: [1; 16],
            batch_id: 4,
            stream_ordinal: 9,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![NamespaceSegment {
                parent_local_node_id: 1,
                parent_dir_key: DirKey::new([2; 16]),
                flags: SEGMENT_START | SEGMENT_END,
                total_entry_count: Some(1),
                entries: vec![NamespaceEntry::NewNode {
                    name: NameBytes::new(b"file_a".to_vec()).unwrap(),
                    node: NodeRecord {
                        local_node_id: 1,
                        kind: 1,
                        mode: 0o100644,
                        size: 0,
                        dir_key: None,
                    },
                }],
            }],
        }
        .encode()
        .unwrap();
        let locator = BatchLocator {
            kind: BatchKind::Namespace,
            batch_id: batch.header.batch_id,
            stream_ordinal: batch.header.stream_ordinal,
            object_offset: 0,
            total_stored_len: batch.bytes.len() as u32,
            raw_payload_len: batch.raw_payload.len() as u32,
            first_new_node_id: batch.header.first_new_node_id,
            new_node_count: batch.header.new_node_count,
            digest: batch.header.digest,
        };
        let reader = MemoryRangeReader::new(batch.bytes);
        let decoded = read_batch(&reader, &locator).unwrap();
        assert_eq!(decoded.header.batch_id, 4);
    }
}
