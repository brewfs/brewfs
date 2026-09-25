//! Merkle index nodes for clustered frozen metadata v2.
//!
//! Every index is an immutable B+ tree of fixed-size nodes.  Leaf nodes carry
//! [BatchLocator] entries; internal nodes carry child-node references.  Each
//! node is independently authenticated with CRC32C and BLAKE3, and namespace
//! keys use restart-point prefix compression so a node decodes without its
//! predecessor.

use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::uvarint::{Reader, Writer};

use super::batch::BatchKind;
use super::cluster_format::BatchLocator;

pub const INDEX_MAGIC: &[u8; 8] = b"BRFIDX02";
pub const INDEX_HEADER_LEN: usize = 32;
pub const DEFAULT_INDEX_NODE_SIZE: usize = 4096;
pub const MAX_INDEX_NODE_SIZE: usize = 64 * 1024;
pub const INDEX_RESTART_INTERVAL: usize = 16;

pub const FLAG_RANK_COUNTS: u16 = 1 << 0;

/// A child reference in an internal index node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildRef {
    pub object_offset: u64,
    pub stored_len: u32,
    pub raw_len: u32,
    pub entry_count: u32,
    pub digest: [u8; 32],
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub visible_subtree_count: Option<u64>,
}

/// A decoded index node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexNode {
    pub level: u8,
    pub kind: BatchKind,
    pub flags: u16,
    pub entries: Vec<IndexEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexEntry {
    Leaf {
        first_key: Vec<u8>,
        last_key: Vec<u8>,
        locator: BatchLocator,
    },
    Internal(ChildRef),
}

impl IndexNode {
    pub fn leaf(kind: BatchKind, entries: Vec<(Vec<u8>, Vec<u8>, BatchLocator)>) -> Self {
        Self {
            level: 0,
            kind,
            flags: 0,
            entries: entries
                .into_iter()
                .map(|(first_key, last_key, locator)| IndexEntry::Leaf {
                    first_key,
                    last_key,
                    locator,
                })
                .collect(),
        }
    }

    pub fn internal(kind: BatchKind, entries: Vec<ChildRef>, has_rank: bool) -> Self {
        Self::internal_at_level(1, kind, entries, has_rank)
    }

    /// Construct an internal node at an arbitrary tree level.  Level 1 is
    /// the parent of leaves; larger values are used by the producer when a
    /// directory has enough batches to require more than one internal tier.
    pub fn internal_at_level(
        level: u8,
        kind: BatchKind,
        entries: Vec<ChildRef>,
        has_rank: bool,
    ) -> Self {
        debug_assert!(level > 0);
        let flags = if has_rank { FLAG_RANK_COUNTS } else { 0 };
        Self {
            level,
            kind,
            flags,
            entries: entries.into_iter().map(IndexEntry::Internal).collect(),
        }
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn encode(&self, node_size: usize) -> WireResult<Vec<u8>> {
        if !(INDEX_HEADER_LEN..=MAX_INDEX_NODE_SIZE).contains(&node_size) {
            return Err(WireError::invalid("index node", "node size out of range"));
        }
        if self.entries.is_empty() {
            return Err(WireError::invalid("index node", "no entries"));
        }
        let mut payload = Writer::new();
        payload.u8(self.level);
        payload.u8(self.kind as u8);
        payload.u16(self.flags);
        payload.uvarint(self.entries.len() as u64);

        let has_rank = self.flags & FLAG_RANK_COUNTS != 0;
        let mut previous_key = Vec::new();
        for (index, entry) in self.entries.iter().enumerate() {
            let (first_key, last_key) = match entry {
                IndexEntry::Leaf {
                    first_key,
                    last_key,
                    ..
                } => (&first_key[..], &last_key[..]),
                IndexEntry::Internal(child) => (&child.first_key[..], &child.last_key[..]),
            };
            if index.is_multiple_of(INDEX_RESTART_INTERVAL) {
                payload.bytes(first_key);
                previous_key.clear();
                previous_key.extend_from_slice(first_key);
            } else {
                let shared = common_prefix_len(&previous_key, first_key);
                payload.uvarint(shared as u64);
                payload.bytes(&first_key[shared..]);
                previous_key.clear();
                previous_key.extend_from_slice(first_key);
            }
            // last key is stored fully for range verification
            payload.bytes(last_key);

            match entry {
                IndexEntry::Leaf { locator: loc, .. } => {
                    if self.level != 0 {
                        return Err(WireError::invalid(
                            "index node",
                            "leaf entry in non-leaf node",
                        ));
                    }
                    payload.put(&loc.encode());
                }
                IndexEntry::Internal(child) => {
                    if self.level == 0 {
                        return Err(WireError::invalid(
                            "index node",
                            "internal entry in leaf node",
                        ));
                    }
                    payload.u64(child.object_offset);
                    payload.u32(child.stored_len);
                    payload.u32(child.raw_len);
                    payload.u32(child.entry_count);
                    payload.put(&child.digest);
                    if has_rank {
                        payload.u64(child.visible_subtree_count.unwrap_or(0));
                    }
                }
            }
        }

        let payload_bytes = payload.as_slice();
        if INDEX_HEADER_LEN + payload_bytes.len() > node_size {
            return Err(WireError::LimitExceeded(format!(
                "index node payload {} exceeds capacity {}",
                payload_bytes.len(),
                node_size - INDEX_HEADER_LEN
            )));
        }

        let mut out = vec![0u8; node_size];
        out[0..8].copy_from_slice(INDEX_MAGIC);
        out[8..10].copy_from_slice(&0u16.to_le_bytes()); // version
        out[10..12].copy_from_slice(&0u16.to_le_bytes()); // reserved
        out[12..16].copy_from_slice(&(payload_bytes.len() as u32).to_le_bytes());
        let header_crc = crc32c::crc32c(&out[..16]);
        out[16..20].copy_from_slice(&header_crc.to_le_bytes());
        // 20..32 reserved for digest offset or future use
        out[32..32 + payload_bytes.len()].copy_from_slice(payload_bytes);

        // BLAKE3 over header[..20] + payload
        let mut hasher = blake3::Hasher::new();
        hasher.update(&out[..20]);
        hasher.update(payload_bytes);
        let digest = hasher.finalize();
        out[20..32].copy_from_slice(&digest.as_bytes()[..12]);

        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> WireResult<Self> {
        if bytes.len() < INDEX_HEADER_LEN {
            return Err(WireError::Truncated {
                what: "index node header",
                need: INDEX_HEADER_LEN,
                have: bytes.len(),
            });
        }
        if &bytes[0..8] != INDEX_MAGIC {
            return Err(WireError::UnsupportedFormat(
                "index node magic mismatch".into(),
            ));
        }
        let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        if version != 0 {
            return Err(WireError::UnsupportedFormat(format!(
                "index node version {version}"
            )));
        }
        if bytes[10..12].iter().any(|b| *b != 0) {
            return Err(WireError::invalid(
                "index node header",
                "reserved bytes are non-zero",
            ));
        }
        let payload_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
        let header_crc = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        let computed_header_crc = crc32c::crc32c(&bytes[..16]);
        if header_crc != computed_header_crc {
            return Err(WireError::CrcMismatch {
                what: "index node header",
                stored: header_crc,
                computed: computed_header_crc,
            });
        }
        let stored_digest_tail = &bytes[20..32];

        if INDEX_HEADER_LEN + payload_len > bytes.len() {
            return Err(WireError::Truncated {
                what: "index node payload",
                need: INDEX_HEADER_LEN + payload_len,
                have: bytes.len(),
            });
        }
        let payload = &bytes[INDEX_HEADER_LEN..INDEX_HEADER_LEN + payload_len];

        // Verify BLAKE3 digest
        let mut hasher = blake3::Hasher::new();
        hasher.update(&bytes[..20]);
        hasher.update(payload);
        let computed_digest = hasher.finalize();
        if &computed_digest.as_bytes()[..12] != stored_digest_tail {
            return Err(WireError::HashMismatch {
                what: "index node",
                stored: hex::encode(stored_digest_tail),
                computed: hex::encode(&computed_digest.as_bytes()[..12]),
            });
        }

        let mut reader = Reader::new(payload);
        let level = reader.u8("index node")?;
        let kind = BatchKind::from_u16(u16::from(reader.u8("index node")?))?;
        let flags = reader.u16("index node")?;
        let entry_count = reader.uvarint("index node")?;
        let entry_count = usize::try_from(entry_count)
            .map_err(|_| WireError::LimitExceeded("index entry count exceeds usize".into()))?;
        if entry_count == 0 || entry_count > 1024 {
            return Err(WireError::LimitExceeded(
                "index entry count must be in 1..=1024".into(),
            ));
        }

        let has_rank = flags & FLAG_RANK_COUNTS != 0;
        let mut entries = Vec::with_capacity(entry_count);
        let mut previous_key = Vec::new();
        let mut previous_last_key: Option<Vec<u8>> = None;

        for index in 0..entry_count {
            let first_key = if index.is_multiple_of(INDEX_RESTART_INTERVAL) {
                let key = reader.bytes("index entry")?.to_vec();
                previous_key.clear();
                previous_key.extend_from_slice(&key);
                key
            } else {
                let shared = usize::try_from(reader.uvarint("index entry")?)
                    .map_err(|_| WireError::LimitExceeded("key prefix exceeds usize".into()))?;
                if shared > previous_key.len() {
                    return Err(WireError::invalid(
                        "index entry",
                        "shared prefix exceeds previous key",
                    ));
                }
                let suffix = reader.bytes("index entry")?;
                let mut key = previous_key[..shared].to_vec();
                key.extend_from_slice(suffix);
                previous_key.clear();
                previous_key.extend_from_slice(&key);
                key
            };
            let last_key = reader.bytes("index entry")?.to_vec();

            // Range ordering check
            if let Some(prev_last) = &previous_last_key {
                if &first_key[..] <= &prev_last[..] {
                    return Err(WireError::invalid(
                        "index node",
                        "entries are not strictly sorted",
                    ));
                }
            }
            if first_key > last_key {
                return Err(WireError::invalid(
                    "index entry",
                    "first key exceeds last key",
                ));
            }
            previous_last_key = Some(last_key.clone());

            if level == 0 {
                let loc_bytes = reader.take(BatchLocator::ENCODED_LEN, "index entry")?;
                let loc = BatchLocator::decode(loc_bytes)?;
                if loc.kind != kind {
                    return Err(WireError::invalid(
                        "index entry",
                        "batch locator kind does not match node kind",
                    ));
                }
                entries.push(IndexEntry::Leaf {
                    first_key,
                    last_key,
                    locator: loc,
                });
            } else {
                let object_offset = reader.u64("index entry")?;
                let stored_len = reader.u32("index entry")?;
                let raw_len = reader.u32("index entry")?;
                let entry_count = reader.u32("index entry")?;
                let digest = reader.take(32, "index entry")?.try_into().unwrap();
                let visible_subtree_count = if has_rank {
                    Some(reader.u64("index entry")?)
                } else {
                    None
                };
                entries.push(IndexEntry::Internal(ChildRef {
                    object_offset,
                    stored_len,
                    raw_len,
                    entry_count,
                    digest,
                    first_key,
                    last_key,
                    visible_subtree_count,
                }));
            }
        }

        if !reader.is_empty() {
            return Err(WireError::invalid(
                "index node",
                "trailing bytes after entries",
            ));
        }

        // Index keys are binary routing keys, not standalone POSIX names. In
        // particular, namespace keys carry a big-endian parent node id before
        // the raw name and may therefore contain zero bytes. The batch codec
        // validates the actual POSIX component; the index only requires a
        // non-empty, ordered key range.
        if first_key_of(&entries[0]).is_empty() {
            return Err(WireError::invalid("index key", "empty routing key"));
        }

        Ok(Self {
            level,
            kind,
            flags,
            entries,
        })
    }
}

fn first_key_of(entry: &IndexEntry) -> &[u8] {
    match entry {
        IndexEntry::Leaf { first_key, .. } => first_key,
        IndexEntry::Internal(child) => &child.first_key,
    }
}

fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter().zip(right).take_while(|(l, r)| l == r).count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_loc(id: u32, first_name: &[u8]) -> BatchLocator {
        // We can't easily encode a name into a BatchLocator fingerprint,
        // so for testing we use the batch_id as the ordering identifier
        // and verify structural round-trip.
        let _ = first_name;
        BatchLocator {
            kind: BatchKind::Namespace,
            batch_id: id,
            stream_ordinal: id,
            object_offset: 4096 + u64::from(id) * 256,
            total_stored_len: 256,
            raw_payload_len: 200,
            first_new_node_id: id * 10,
            new_node_count: 5,
            digest: [id as u8; 32],
        }
    }

    #[test]
    fn leaf_node_round_trips_with_batch_locators() {
        let entries: Vec<(Vec<u8>, Vec<u8>, BatchLocator)> = (0..8)
            .map(|i| {
                let first = format!("f_{:04}_a", i).into_bytes();
                let last = format!("f_{:04}_z", i).into_bytes();
                (first, last, make_loc(i as u32, &[]))
            })
            .collect();
        let node = IndexNode::leaf(BatchKind::Namespace, entries);
        let encoded = node.encode(DEFAULT_INDEX_NODE_SIZE).unwrap();
        assert_eq!(encoded.len(), DEFAULT_INDEX_NODE_SIZE);
        let decoded = IndexNode::decode(&encoded).unwrap();
        assert_eq!(decoded.level, 0);
        assert_eq!(decoded.kind, BatchKind::Namespace);
        assert_eq!(decoded.entries.len(), 8);
        for (i, entry) in decoded.entries.iter().enumerate() {
            match entry {
                IndexEntry::Leaf {
                    locator: loc,
                    first_key,
                    last_key,
                } => {
                    assert_eq!(loc.batch_id, i as u32);
                    assert_eq!(first_key, &format!("f_{:04}_a", i).into_bytes());
                    assert_eq!(last_key, &format!("f_{:04}_z", i).into_bytes());
                }
                _ => panic!("expected leaf entry"),
            }
        }
    }

    #[test]
    fn leaf_node_rejects_corrupt_header_crc() {
        let entries: Vec<_> = (0..4)
            .map(|i| {
                let first = format!("a_{:04}", i).into_bytes();
                let last = format!("z_{:04}", i).into_bytes();
                (first, last, make_loc(i as u32, &[]))
            })
            .collect();
        let node = IndexNode::leaf(BatchKind::Namespace, entries);
        let mut encoded = node.encode(DEFAULT_INDEX_NODE_SIZE).unwrap();
        encoded[12] ^= 1; // tamper with payload_len (inside header CRC range)
        assert!(matches!(
            IndexNode::decode(&encoded),
            Err(WireError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn leaf_node_rejects_digest_tampering() {
        let entries: Vec<_> = (0..4)
            .map(|i| {
                let first = format!("a_{:04}", i).into_bytes();
                let last = format!("z_{:04}", i).into_bytes();
                (first, last, make_loc(i as u32, &[]))
            })
            .collect();
        let node = IndexNode::leaf(BatchKind::Namespace, entries);
        let mut encoded = node.encode(DEFAULT_INDEX_NODE_SIZE).unwrap();
        encoded[20] ^= 1; // tamper with digest
        assert!(matches!(
            IndexNode::decode(&encoded),
            Err(WireError::HashMismatch { .. })
        ));
    }

    #[test]
    fn leaf_node_rejects_unknown_version() {
        let entries: Vec<_> = (0..4)
            .map(|i| {
                let first = format!("a_{:04}", i).into_bytes();
                let last = format!("z_{:04}", i).into_bytes();
                (first, last, make_loc(i as u32, &[]))
            })
            .collect();
        let node = IndexNode::leaf(BatchKind::Namespace, entries);
        let mut encoded = node.encode(DEFAULT_INDEX_NODE_SIZE).unwrap();
        encoded[8..10].copy_from_slice(&1u16.to_le_bytes());
        // Recompute header CRC since version is inside [..16]
        let new_crc = crc32c::crc32c(&encoded[..16]);
        encoded[16..20].copy_from_slice(&new_crc.to_le_bytes());
        // Recompute digest
        let mut hasher = blake3::Hasher::new();
        hasher.update(&encoded[..20]);
        let payload_len = u32::from_le_bytes(encoded[12..16].try_into().unwrap()) as usize;
        hasher.update(&encoded[INDEX_HEADER_LEN..INDEX_HEADER_LEN + payload_len]);
        let digest = hasher.finalize();
        encoded[20..32].copy_from_slice(&digest.as_bytes()[..12]);
        assert!(matches!(
            IndexNode::decode(&encoded),
            Err(WireError::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn internal_node_round_trips_with_children() {
        let children: Vec<ChildRef> = (0..4)
            .map(|i| ChildRef {
                object_offset: 4096 + (i as u64) * 4096,
                stored_len: 4096,
                raw_len: 2048,
                entry_count: 32,
                digest: [i as u8; 32],
                first_key: format!("{:04}_a", i * 2).into_bytes(),
                last_key: format!("{:04}_z", i * 2 + 1).into_bytes(),
                visible_subtree_count: Some(1000 + i as u64),
            })
            .collect();
        let node = IndexNode::internal(BatchKind::Namespace, children, true);
        let encoded = node.encode(DEFAULT_INDEX_NODE_SIZE).unwrap();
        let decoded = IndexNode::decode(&encoded).unwrap();
        assert_eq!(decoded.level, 1);
        assert_eq!(decoded.kind, BatchKind::Namespace);
        assert_eq!(decoded.flags & FLAG_RANK_COUNTS, FLAG_RANK_COUNTS);
        assert_eq!(decoded.entries.len(), 4);
        for (i, entry) in decoded.entries.iter().enumerate() {
            match entry {
                IndexEntry::Internal(child) => {
                    assert_eq!(child.entry_count, 32);
                    assert_eq!(child.visible_subtree_count, Some(1000 + i as u64));
                }
                _ => panic!("expected internal entry"),
            }
        }
    }

    #[test]
    fn node_rejects_empty_entries() {
        let node = IndexNode {
            level: 0,
            kind: BatchKind::Namespace,
            flags: 0,
            entries: vec![],
        };
        assert!(node.encode(DEFAULT_INDEX_NODE_SIZE).is_err());
    }

    #[test]
    fn node_rejects_empty_entries_size_too_small() {
        let entries = vec![(b"a".to_vec(), b"z".to_vec(), make_loc(0, &[]))];
        let node = IndexNode::leaf(BatchKind::Namespace, entries);
        assert!(node.encode(16).is_err());
    }
}
