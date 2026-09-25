//! Fixed-size roots for the clustered v2 objects.
//!
//! The superblocks are intentionally boring.  They are small enough to pin
//! for the lifetime of a cluster handle, and every field has a fixed offset so
//! opening a cluster never requires decoding a variable-sized manifest first.
//! Variable metadata lives in independently authenticated batch/index objects.

use crate::native_base::wire::error::{WireError, WireResult};

use super::batch::BatchKind;
use super::identity::DirKey;

pub const CLUSTER_MAGIC: &[u8; 8] = b"BRFCL002";
pub const SNAPSHOT_MAGIC: &[u8; 8] = b"BRFSM002";
pub const SUPERBLOCK_LEN: usize = 4096;
pub const FORMAT_MAJOR: u16 = 2;
pub const FORMAT_MINOR: u16 = 0;

/// Build the canonical key used by the namespace Merkle index.
///
/// The parent local-node id is encoded before the raw POSIX component in
/// big-endian order.  This makes the byte ordering equivalent to
/// `(parent_local_node_id, name)` ordering while keeping the name lossless,
/// including non-UTF-8 bytes.  A name is still validated by the namespace
/// batch codec; this helper intentionally only defines the index key shape.
pub fn namespace_index_key(parent_local_node_id: u32, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + name.len());
    key.extend_from_slice(&parent_local_node_id.to_be_bytes());
    key.extend_from_slice(name);
    key
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexRootRef {
    pub object_offset: u64,
    pub stored_len: u32,
    pub raw_len: u32,
    pub level: u8,
    pub kind: u8,
    pub entry_count: u32,
    pub digest: [u8; 32],
    pub key_fingerprint: u64,
}

impl IndexRootRef {
    pub const ENCODED_LEN: usize = 64;

    fn encode_into(&self, out: &mut [u8]) -> WireResult<()> {
        if out.len() != Self::ENCODED_LEN {
            return Err(WireError::invalid("index root", "invalid output length"));
        }
        out[0..8].copy_from_slice(&self.object_offset.to_le_bytes());
        out[8..12].copy_from_slice(&self.stored_len.to_le_bytes());
        out[12..16].copy_from_slice(&self.raw_len.to_le_bytes());
        out[16] = self.level;
        out[17] = self.kind;
        out[18..20].fill(0);
        out[20..24].copy_from_slice(&self.entry_count.to_le_bytes());
        out[24..56].copy_from_slice(&self.digest);
        out[56..64].copy_from_slice(&self.key_fingerprint.to_le_bytes());
        Ok(())
    }

    fn decode(bytes: &[u8]) -> WireResult<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return Err(WireError::invalid("index root", "invalid input length"));
        }
        if bytes[18..20].iter().any(|byte| *byte != 0) {
            return Err(WireError::invalid(
                "index root",
                "reserved bytes are non-zero",
            ));
        }
        Ok(Self {
            object_offset: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            stored_len: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            raw_len: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            level: bytes[16],
            kind: bytes[17],
            entry_count: u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            digest: bytes[24..56].try_into().unwrap(),
            key_fingerprint: u64::from_le_bytes(bytes[56..64].try_into().unwrap()),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchLocator {
    pub kind: BatchKind,
    pub batch_id: u32,
    pub stream_ordinal: u32,
    pub object_offset: u64,
    /// Includes the fixed batch header and stored payload.
    pub total_stored_len: u32,
    pub raw_payload_len: u32,
    pub first_new_node_id: u32,
    pub new_node_count: u32,
    pub digest: [u8; 32],
}

impl BatchLocator {
    pub const ENCODED_LEN: usize = 68;

    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[0..2].copy_from_slice(&(self.kind as u16).to_le_bytes());
        out[2..4].copy_from_slice(&0u16.to_le_bytes());
        out[4..8].copy_from_slice(&self.batch_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.stream_ordinal.to_le_bytes());
        out[12..20].copy_from_slice(&self.object_offset.to_le_bytes());
        out[20..24].copy_from_slice(&self.total_stored_len.to_le_bytes());
        out[24..28].copy_from_slice(&self.raw_payload_len.to_le_bytes());
        out[28..32].copy_from_slice(&self.first_new_node_id.to_le_bytes());
        out[32..36].copy_from_slice(&self.new_node_count.to_le_bytes());
        out[36..68].copy_from_slice(&self.digest);
        out
    }

    pub fn decode(bytes: &[u8]) -> WireResult<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return Err(WireError::invalid("batch locator", "invalid length"));
        }
        if bytes[2..4].iter().any(|byte| *byte != 0) {
            return Err(WireError::invalid(
                "batch locator",
                "reserved bytes are non-zero",
            ));
        }
        Ok(Self {
            kind: BatchKind::from_u16(u16::from_le_bytes(bytes[0..2].try_into().unwrap()))?,
            batch_id: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            stream_ordinal: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            object_offset: u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
            total_stored_len: u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            raw_payload_len: u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
            first_new_node_id: u32::from_le_bytes(bytes[28..32].try_into().unwrap()),
            new_node_count: u32::from_le_bytes(bytes[32..36].try_into().unwrap()),
            digest: bytes[36..68].try_into().unwrap(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterSuperblock {
    pub format_major: u16,
    pub format_minor: u16,
    pub cluster_id: [u8; 16],
    pub volume_id: [u8; 16],
    pub mount_dir_key: DirKey,
    pub metadata_semantic_hash: [u8; 32],
    pub build_options_digest: [u8; 32],
    pub node_count: u32,
    pub directory_contribution_count: u32,
    pub dentry_count: u64,
    pub extent_count: u64,
    pub slice_count: u64,
    pub namespace_batch_count: u32,
    pub extent_batch_count: u32,
    pub attribute_batch_count: u32,
    pub chunk_size: u64,
    pub index_roots: [IndexRootRef; 4],
}

impl ClusterSuperblock {
    pub fn encode(&self) -> [u8; SUPERBLOCK_LEN] {
        let mut out = [0u8; SUPERBLOCK_LEN];
        out[0..8].copy_from_slice(CLUSTER_MAGIC);
        out[8..10].copy_from_slice(&self.format_major.to_le_bytes());
        out[10..12].copy_from_slice(&self.format_minor.to_le_bytes());
        out[16..32].copy_from_slice(&self.cluster_id);
        out[32..48].copy_from_slice(&self.volume_id);
        out[48..64].copy_from_slice(self.mount_dir_key.as_ref());
        out[64..96].copy_from_slice(&self.metadata_semantic_hash);
        out[96..128].copy_from_slice(&self.build_options_digest);
        out[128..132].copy_from_slice(&self.node_count.to_le_bytes());
        out[132..136].copy_from_slice(&self.directory_contribution_count.to_le_bytes());
        out[136..144].copy_from_slice(&self.dentry_count.to_le_bytes());
        out[144..152].copy_from_slice(&self.extent_count.to_le_bytes());
        out[152..160].copy_from_slice(&self.slice_count.to_le_bytes());
        out[160..164].copy_from_slice(&self.namespace_batch_count.to_le_bytes());
        out[164..168].copy_from_slice(&self.extent_batch_count.to_le_bytes());
        out[168..172].copy_from_slice(&self.attribute_batch_count.to_le_bytes());
        out[176..184].copy_from_slice(&self.chunk_size.to_le_bytes());
        for (index, root) in self.index_roots.iter().enumerate() {
            root.encode_into(
                &mut out[184 + index * IndexRootRef::ENCODED_LEN..][..IndexRootRef::ENCODED_LEN],
            )
            .expect("fixed root slice");
        }
        let crc = crc32c::crc32c(&out[..SUPERBLOCK_LEN - 4]);
        out[SUPERBLOCK_LEN - 4..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> WireResult<Self> {
        if bytes.len() < SUPERBLOCK_LEN {
            return Err(WireError::Truncated {
                what: "cluster superblock",
                need: SUPERBLOCK_LEN,
                have: bytes.len(),
            });
        }
        let bytes = &bytes[..SUPERBLOCK_LEN];
        if &bytes[..8] != CLUSTER_MAGIC {
            return Err(WireError::UnsupportedFormat(
                "not a BRFCL002 superblock".into(),
            ));
        }
        let major = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        let minor = u16::from_le_bytes(bytes[10..12].try_into().unwrap());
        if major != FORMAT_MAJOR || minor != FORMAT_MINOR {
            return Err(WireError::UnsupportedFormat(format!(
                "cluster format {major}.{minor}"
            )));
        }
        if bytes[12..16].iter().any(|byte| *byte != 0)
            || bytes[172..176].iter().any(|byte| *byte != 0)
        {
            return Err(WireError::invalid(
                "cluster superblock",
                "reserved bytes are non-zero",
            ));
        }
        let stored = u32::from_le_bytes(bytes[SUPERBLOCK_LEN - 4..].try_into().unwrap());
        let computed = crc32c::crc32c(&bytes[..SUPERBLOCK_LEN - 4]);
        if stored != computed {
            return Err(WireError::CrcMismatch {
                what: "cluster superblock",
                stored,
                computed,
            });
        }
        let mut index_roots = [IndexRootRef {
            object_offset: 0,
            stored_len: 0,
            raw_len: 0,
            level: 0,
            kind: 0,
            entry_count: 0,
            digest: [0; 32],
            key_fingerprint: 0,
        }; 4];
        for (index, root) in index_roots.iter_mut().enumerate() {
            *root = IndexRootRef::decode(
                &bytes[184 + index * IndexRootRef::ENCODED_LEN..][..IndexRootRef::ENCODED_LEN],
            )?;
        }
        Ok(Self {
            format_major: major,
            format_minor: minor,
            cluster_id: bytes[16..32].try_into().unwrap(),
            volume_id: bytes[32..48].try_into().unwrap(),
            mount_dir_key: DirKey::new(bytes[48..64].try_into().unwrap()),
            metadata_semantic_hash: bytes[64..96].try_into().unwrap(),
            build_options_digest: bytes[96..128].try_into().unwrap(),
            node_count: u32::from_le_bytes(bytes[128..132].try_into().unwrap()),
            directory_contribution_count: u32::from_le_bytes(bytes[132..136].try_into().unwrap()),
            dentry_count: u64::from_le_bytes(bytes[136..144].try_into().unwrap()),
            extent_count: u64::from_le_bytes(bytes[144..152].try_into().unwrap()),
            slice_count: u64::from_le_bytes(bytes[152..160].try_into().unwrap()),
            namespace_batch_count: u32::from_le_bytes(bytes[160..164].try_into().unwrap()),
            extent_batch_count: u32::from_le_bytes(bytes[164..168].try_into().unwrap()),
            attribute_batch_count: u32::from_le_bytes(bytes[168..172].try_into().unwrap()),
            chunk_size: u64::from_le_bytes(bytes[176..184].try_into().unwrap()),
            index_roots,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotSuperblock {
    pub volume_id: [u8; 16],
    pub snapshot_id: [u8; 16],
    pub semantic_hash: [u8; 32],
    pub route_seed: [u8; 16],
    pub cluster_count: u32,
    pub mount_count: u32,
    pub merged_directory_count: u64,
    pub root_dir_key: DirKey,
    pub index_roots: [IndexRootRef; 3],
}

impl SnapshotSuperblock {
    pub fn encode(&self) -> [u8; SUPERBLOCK_LEN] {
        let mut out = [0u8; SUPERBLOCK_LEN];
        out[0..8].copy_from_slice(SNAPSHOT_MAGIC);
        out[8..10].copy_from_slice(&FORMAT_MAJOR.to_le_bytes());
        out[10..12].copy_from_slice(&FORMAT_MINOR.to_le_bytes());
        out[16..32].copy_from_slice(&self.volume_id);
        out[32..48].copy_from_slice(&self.snapshot_id);
        out[48..80].copy_from_slice(&self.semantic_hash);
        out[80..96].copy_from_slice(&self.route_seed);
        out[96..100].copy_from_slice(&self.cluster_count.to_le_bytes());
        out[100..104].copy_from_slice(&self.mount_count.to_le_bytes());
        out[104..112].copy_from_slice(&self.merged_directory_count.to_le_bytes());
        out[112..128].copy_from_slice(self.root_dir_key.as_ref());
        for (index, root) in self.index_roots.iter().enumerate() {
            root.encode_into(
                &mut out[128 + index * IndexRootRef::ENCODED_LEN..][..IndexRootRef::ENCODED_LEN],
            )
            .expect("fixed root slice");
        }
        let crc = crc32c::crc32c(&out[..SUPERBLOCK_LEN - 4]);
        out[SUPERBLOCK_LEN - 4..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> WireResult<Self> {
        if bytes.len() < SUPERBLOCK_LEN {
            return Err(WireError::Truncated {
                what: "snapshot superblock",
                need: SUPERBLOCK_LEN,
                have: bytes.len(),
            });
        }
        let bytes = &bytes[..SUPERBLOCK_LEN];
        if &bytes[..8] != SNAPSHOT_MAGIC {
            return Err(WireError::UnsupportedFormat(
                "not a BRFSM002 superblock".into(),
            ));
        }
        if u16::from_le_bytes(bytes[8..10].try_into().unwrap()) != FORMAT_MAJOR
            || u16::from_le_bytes(bytes[10..12].try_into().unwrap()) != FORMAT_MINOR
        {
            return Err(WireError::UnsupportedFormat(
                "unsupported snapshot format".into(),
            ));
        }
        if bytes[12..16].iter().any(|byte| *byte != 0) {
            return Err(WireError::invalid(
                "snapshot superblock",
                "reserved bytes are non-zero",
            ));
        }
        let stored = u32::from_le_bytes(bytes[SUPERBLOCK_LEN - 4..].try_into().unwrap());
        let computed = crc32c::crc32c(&bytes[..SUPERBLOCK_LEN - 4]);
        if stored != computed {
            return Err(WireError::CrcMismatch {
                what: "snapshot superblock",
                stored,
                computed,
            });
        }
        let mut index_roots = [IndexRootRef {
            object_offset: 0,
            stored_len: 0,
            raw_len: 0,
            level: 0,
            kind: 0,
            entry_count: 0,
            digest: [0; 32],
            key_fingerprint: 0,
        }; 3];
        for (index, root) in index_roots.iter_mut().enumerate() {
            *root = IndexRootRef::decode(
                &bytes[128 + index * IndexRootRef::ENCODED_LEN..][..IndexRootRef::ENCODED_LEN],
            )?;
        }
        Ok(Self {
            volume_id: bytes[16..32].try_into().unwrap(),
            snapshot_id: bytes[32..48].try_into().unwrap(),
            semantic_hash: bytes[48..80].try_into().unwrap(),
            route_seed: bytes[80..96].try_into().unwrap(),
            cluster_count: u32::from_le_bytes(bytes[96..100].try_into().unwrap()),
            mount_count: u32::from_le_bytes(bytes[100..104].try_into().unwrap()),
            merged_directory_count: u64::from_le_bytes(bytes[104..112].try_into().unwrap()),
            root_dir_key: DirKey::new(bytes[112..128].try_into().unwrap()),
            index_roots,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(seed: u8) -> IndexRootRef {
        IndexRootRef {
            object_offset: 4096,
            stored_len: 512,
            raw_len: 1024,
            level: 2,
            kind: seed,
            entry_count: 10,
            digest: [seed; 32],
            key_fingerprint: u64::from(seed),
        }
    }

    #[test]
    fn cluster_superblock_is_fixed_size_and_crc_protected() {
        let block = ClusterSuperblock {
            format_major: FORMAT_MAJOR,
            format_minor: FORMAT_MINOR,
            cluster_id: [1; 16],
            volume_id: [2; 16],
            mount_dir_key: DirKey::new([3; 16]),
            metadata_semantic_hash: [4; 32],
            build_options_digest: [5; 32],
            node_count: 7,
            directory_contribution_count: 8,
            dentry_count: 9,
            extent_count: 10,
            slice_count: 11,
            namespace_batch_count: 12,
            extent_batch_count: 13,
            attribute_batch_count: 14,
            chunk_size: 1 << 20,
            index_roots: [root(1), root(2), root(3), root(4)],
        };
        let encoded = block.encode();
        assert_eq!(encoded.len(), SUPERBLOCK_LEN);
        assert_eq!(ClusterSuperblock::decode(&encoded).unwrap(), block);
        let mut corrupt = encoded;
        corrupt[100] ^= 1;
        assert!(matches!(
            ClusterSuperblock::decode(&corrupt),
            Err(WireError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn snapshot_superblock_rejects_wrong_magic_and_roundtrips() {
        let block = SnapshotSuperblock {
            volume_id: [1; 16],
            snapshot_id: [2; 16],
            semantic_hash: [3; 32],
            route_seed: [4; 16],
            cluster_count: 5,
            mount_count: 6,
            merged_directory_count: 7,
            root_dir_key: DirKey::new([8; 16]),
            index_roots: [root(1), root(2), root(3)],
        };
        let encoded = block.encode();
        assert_eq!(SnapshotSuperblock::decode(&encoded).unwrap(), block);
        let mut wrong = encoded;
        wrong[0] = b'X';
        assert!(matches!(
            SnapshotSuperblock::decode(&wrong),
            Err(WireError::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn locator_has_explicit_reserved_field() {
        let locator = BatchLocator {
            kind: BatchKind::Namespace,
            batch_id: 1,
            stream_ordinal: 2,
            object_offset: 4096,
            total_stored_len: 100,
            raw_payload_len: 80,
            first_new_node_id: 3,
            new_node_count: 4,
            digest: [9; 32],
        };
        let encoded = locator.encode();
        assert_eq!(BatchLocator::decode(&encoded).unwrap().digest, [9; 32]);
        let mut corrupt = encoded;
        corrupt[2] = 1;
        assert!(BatchLocator::decode(&corrupt).is_err());
    }

    #[test]
    fn cluster_object_round_trip_with_namespace_batch() {
        use super::super::batch::{
            BatchHeader, BatchKind, EncodedBatch, NamespaceBatch, NamespaceEntry, NamespaceSegment,
            NodeRecord, SEGMENT_END, SEGMENT_START,
        };
        use super::super::identity::DirKey;
        use super::super::name::NameBytes;

        // Step 1: Build a namespace batch.
        let entries: Vec<NamespaceEntry> = (0..32)
            .map(|i| NamespaceEntry::NewNode {
                name: NameBytes::new(format!("f_{:04}.txt", i).into_bytes()).unwrap(),
                node: NodeRecord {
                    local_node_id: 100 + i,
                    kind: 1,
                    mode: 0o100644,
                    size: (i as u64) * 1024,
                    dir_key: None,
                },
            })
            .collect();
        let batch = NamespaceBatch {
            cluster_id: [0xAB; 16],
            batch_id: 0,
            stream_ordinal: 0,
            predecessor_ordinal: BatchHeader::NO_PREDECESSOR,
            segments: vec![NamespaceSegment {
                parent_local_node_id: 1,
                parent_dir_key: DirKey::new([7; 16]),
                flags: SEGMENT_START | SEGMENT_END,
                total_entry_count: Some(32),
                entries,
            }],
        };
        let encoded_batch = batch.encode().unwrap();
        let batch_offset = SUPERBLOCK_LEN as u64;

        // Step 2: Build a cluster superblock with an index root that points
        // directly at the batch (level 0 = leaf, one batch total).
        let first_key_digest = blake3::hash(b"f_0000.txt");
        let first_key_fingerprint =
            u64::from_le_bytes(first_key_digest.as_bytes()[..8].try_into().unwrap());
        let root = IndexRootRef {
            object_offset: batch_offset,
            stored_len: encoded_batch.bytes.len() as u32,
            raw_len: encoded_batch.raw_payload.len() as u32,
            level: 0,
            kind: BatchKind::Namespace as u8,
            entry_count: 32,
            digest: encoded_batch.header.digest,
            key_fingerprint: first_key_fingerprint,
        };
        let cluster = ClusterSuperblock {
            format_major: FORMAT_MAJOR,
            format_minor: FORMAT_MINOR,
            cluster_id: [0xAB; 16],
            volume_id: [2; 16],
            mount_dir_key: DirKey::new([3; 16]),
            metadata_semantic_hash: [4; 32],
            build_options_digest: [5; 32],
            node_count: 33, // 32 files + 1 dir
            directory_contribution_count: 1,
            dentry_count: 32,
            extent_count: 0,
            slice_count: 0,
            namespace_batch_count: 1,
            extent_batch_count: 0,
            attribute_batch_count: 0,
            chunk_size: 1 << 20,
            index_roots: [
                root,
                IndexRootRef {
                    object_offset: 0,
                    stored_len: 0,
                    raw_len: 0,
                    level: 0,
                    kind: 0,
                    entry_count: 0,
                    digest: [0; 32],
                    key_fingerprint: 0,
                },
                IndexRootRef {
                    object_offset: 0,
                    stored_len: 0,
                    raw_len: 0,
                    level: 0,
                    kind: 0,
                    entry_count: 0,
                    digest: [0; 32],
                    key_fingerprint: 0,
                },
                IndexRootRef {
                    object_offset: 0,
                    stored_len: 0,
                    raw_len: 0,
                    level: 0,
                    kind: 0,
                    entry_count: 0,
                    digest: [0; 32],
                    key_fingerprint: 0,
                },
            ],
        };

        // Step 3: Assemble the full cluster object.
        let superblock_bytes = cluster.encode();
        assert_eq!(superblock_bytes.len(), SUPERBLOCK_LEN);
        let mut cluster_object = Vec::with_capacity(SUPERBLOCK_LEN + encoded_batch.bytes.len());
        cluster_object.extend_from_slice(&superblock_bytes);
        cluster_object.extend_from_slice(&encoded_batch.bytes);

        // Step 4: Decode the superblock from the cluster object.
        let decoded_cluster = ClusterSuperblock::decode(&cluster_object).unwrap();
        assert_eq!(decoded_cluster.cluster_id, [0xAB; 16]);
        assert_eq!(decoded_cluster.namespace_batch_count, 1);
        assert_eq!(
            decoded_cluster.index_roots[0].kind,
            BatchKind::Namespace as u8
        );
        assert_eq!(decoded_cluster.index_roots[0].entry_count, 32);
        assert_eq!(decoded_cluster.index_roots[0].level, 0);

        // Step 5: Locate and decode the batch using the index root.
        let root = &decoded_cluster.index_roots[0];
        let batch_start = root.object_offset as usize;
        let batch_end = batch_start + root.stored_len as usize;
        assert!(batch_end <= cluster_object.len());
        let decoded = EncodedBatch::decode(&cluster_object[batch_start..batch_end]).unwrap();
        assert_eq!(decoded.header.kind, BatchKind::Namespace);
        assert_eq!(decoded.header.record_count, 32);
        assert_eq!(decoded.header.digest, root.digest);

        // Step 6: Full namespace decode matches the original.
        let decoded_batch = NamespaceBatch::decode(&decoded).unwrap();
        assert_eq!(decoded_batch.segments.len(), 1);
        assert_eq!(decoded_batch.segments[0].entries.len(), 32);
        match &decoded_batch.segments[0].entries[0] {
            NamespaceEntry::NewNode { name, node } => {
                assert_eq!(name.as_bytes(), b"f_0000.txt");
                assert_eq!(node.local_node_id, 100);
                assert_eq!(node.size, 0);
            }
            _ => panic!("expected NewNode"),
        }
        match &decoded_batch.segments[0].entries[31] {
            NamespaceEntry::NewNode { name, node } => {
                assert_eq!(name.as_bytes(), b"f_0031.txt");
                assert_eq!(node.local_node_id, 131);
                assert_eq!(node.size, 31 * 1024);
            }
            _ => panic!("expected NewNode"),
        }
    }

    #[test]
    fn full_cluster_read_path_with_merkle_index() {
        use super::super::batch::{
            BatchHeader, BatchKind, EncodedBatch, NamespaceBatch, NamespaceEntry, NamespaceSegment,
            NodeRecord, SEGMENT_END, SEGMENT_START,
        };
        use super::super::identity::DirKey;
        use super::super::merkle_index::{DEFAULT_INDEX_NODE_SIZE, IndexEntry, IndexNode};
        use super::super::name::NameBytes;

        // Step 1: Build 3 namespace batches covering disjoint name ranges.
        let batch_specs = [
            ("a", "g", 0u32, 0u32, 1u32..21u32),   // 20 files, a..f
            ("h", "n", 1u32, 21u32, 21u32..41u32), // 20 files, h..m
            ("o", "z", 2u32, 41u32, 41u32..61u32), // 20 files, o..y
        ];

        let cluster_id = [0xCD; 16];
        let mut batches: Vec<EncodedBatch> = Vec::new();
        let mut first_names: Vec<Vec<u8>> = Vec::new();
        let mut last_names: Vec<Vec<u8>> = Vec::new();
        let mut total_nodes = 0u32;

        for (idx, (first_char, last_char, _fc, _fn, range)) in batch_specs.iter().enumerate() {
            let batch_id = idx as u32;
            let entries: Vec<NamespaceEntry> = range
                .clone()
                .map(|i| NamespaceEntry::NewNode {
                    name: NameBytes::new(format!("file_{:04}.txt", i).into_bytes()).unwrap(),
                    node: NodeRecord {
                        local_node_id: i + 100,
                        kind: 1,
                        mode: 0o100644,
                        size: (i as u64) * 512,
                        dir_key: None,
                    },
                })
                .collect();
            total_nodes += entries.len() as u32;
            let batch = NamespaceBatch {
                cluster_id,
                batch_id,
                stream_ordinal: batch_id,
                predecessor_ordinal: if batch_id == 0 {
                    BatchHeader::NO_PREDECESSOR
                } else {
                    batch_id - 1
                },
                segments: vec![NamespaceSegment {
                    parent_local_node_id: 1,
                    parent_dir_key: DirKey::new([7; 16]),
                    flags: SEGMENT_START | SEGMENT_END,
                    total_entry_count: Some(entries.len() as u64),
                    entries,
                }],
            };
            let encoded = batch.encode().unwrap();

            // Record first and last names for the index
            if let NamespaceEntry::NewNode { name, .. } = &batch.segments[0].entries[0] {
                first_names.push(name.as_bytes().to_vec());
            }
            if let NamespaceEntry::NewNode { name, .. } =
                &batch.segments[0].entries[batch.segments[0].entries.len() - 1]
            {
                last_names.push(name.as_bytes().to_vec());
            }
            batches.push(encoded);
        }

        // Step 2: Build a Merkle index leaf node pointing to all 3 batches.
        let mut leaf_entries = Vec::new();
        for (i, batch) in batches.iter().enumerate() {
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
            leaf_entries.push((first_names[i].clone(), last_names[i].clone(), loc));
        }
        let leaf_node = IndexNode::leaf(BatchKind::Namespace, leaf_entries);
        let leaf_bytes = leaf_node.encode(DEFAULT_INDEX_NODE_SIZE).unwrap();

        // Step 3: Lay out the full cluster object.
        // [0..4096] superblock
        // [4096..4096+leaf_size] index leaf node
        // [leaf_end..] batches
        let superblock_end = SUPERBLOCK_LEN;
        let leaf_start = superblock_end;
        let leaf_end = leaf_start + leaf_bytes.len();
        let mut offsets = Vec::new();
        let mut cursor = leaf_end;
        for batch in &batches {
            offsets.push(cursor as u64);
            cursor += batch.bytes.len();
        }
        let total_size = cursor;

        // Step 4: Re-encode leaf node with correct object_offsets.
        let mut leaf_entries_with_offsets = Vec::new();
        for (i, batch) in batches.iter().enumerate() {
            let loc = BatchLocator {
                kind: BatchKind::Namespace,
                batch_id: batch.header.batch_id,
                stream_ordinal: batch.header.stream_ordinal,
                object_offset: offsets[i],
                total_stored_len: batch.bytes.len() as u32,
                raw_payload_len: batch.raw_payload.len() as u32,
                first_new_node_id: batch.header.first_new_node_id,
                new_node_count: batch.header.new_node_count,
                digest: batch.header.digest,
            };
            leaf_entries_with_offsets.push((first_names[i].clone(), last_names[i].clone(), loc));
        }
        let leaf_node = IndexNode::leaf(BatchKind::Namespace, leaf_entries_with_offsets);
        let leaf_bytes = leaf_node.encode(DEFAULT_INDEX_NODE_SIZE).unwrap();

        // Step 5: Build the cluster superblock with index root.
        let leaf_digest = blake3::hash(&leaf_bytes);
        let first_fingerprint = {
            let digest = blake3::hash(&first_names[0]);
            u64::from_le_bytes(digest.as_bytes()[..8].try_into().unwrap())
        };
        let root = IndexRootRef {
            object_offset: leaf_start as u64,
            stored_len: leaf_bytes.len() as u32,
            raw_len: leaf_bytes.len() as u32, // uncompressed for now
            level: 0,
            kind: BatchKind::Namespace as u8,
            entry_count: 3,
            digest: leaf_digest.as_bytes().clone(),
            key_fingerprint: first_fingerprint,
        };

        let cluster = ClusterSuperblock {
            format_major: FORMAT_MAJOR,
            format_minor: FORMAT_MINOR,
            cluster_id,
            volume_id: [2; 16],
            mount_dir_key: DirKey::new([3; 16]),
            metadata_semantic_hash: [4; 32],
            build_options_digest: [5; 32],
            node_count: total_nodes + 1, // +1 for root dir
            directory_contribution_count: 1,
            dentry_count: total_nodes as u64,
            extent_count: 0,
            slice_count: 0,
            namespace_batch_count: 3,
            extent_batch_count: 0,
            attribute_batch_count: 0,
            chunk_size: 1 << 20,
            index_roots: [
                root,
                IndexRootRef {
                    object_offset: 0,
                    stored_len: 0,
                    raw_len: 0,
                    level: 0,
                    kind: 0,
                    entry_count: 0,
                    digest: [0; 32],
                    key_fingerprint: 0,
                },
                IndexRootRef {
                    object_offset: 0,
                    stored_len: 0,
                    raw_len: 0,
                    level: 0,
                    kind: 0,
                    entry_count: 0,
                    digest: [0; 32],
                    key_fingerprint: 0,
                },
                IndexRootRef {
                    object_offset: 0,
                    stored_len: 0,
                    raw_len: 0,
                    level: 0,
                    kind: 0,
                    entry_count: 0,
                    digest: [0; 32],
                    key_fingerprint: 0,
                },
            ],
        };

        // Step 6: Assemble the full cluster object.
        let mut cluster_obj = Vec::with_capacity(total_size);
        cluster_obj.extend_from_slice(&cluster.encode());
        cluster_obj.extend_from_slice(&leaf_bytes);
        for batch in &batches {
            cluster_obj.extend_from_slice(&batch.bytes);
        }
        assert_eq!(cluster_obj.len(), total_size);

        // Step 7: Read path: decode superblock → index root → leaf node → batch.
        let decoded_cluster = ClusterSuperblock::decode(&cluster_obj).unwrap();
        assert_eq!(decoded_cluster.namespace_batch_count, 3);
        assert_eq!(decoded_cluster.node_count, total_nodes + 1);

        let ns_root = &decoded_cluster.index_roots[0];
        assert_eq!(ns_root.kind, BatchKind::Namespace as u8);
        assert_eq!(ns_root.entry_count, 3);

        // Verify index node digest
        let idx_start = ns_root.object_offset as usize;
        let idx_end = idx_start + ns_root.stored_len as usize;
        assert!(idx_end <= cluster_obj.len());
        let idx_bytes = &cluster_obj[idx_start..idx_end];
        let idx_hash = blake3::hash(idx_bytes);
        assert_eq!(idx_hash.as_bytes(), &ns_root.digest);

        // Decode the leaf index node
        let leaf = IndexNode::decode(idx_bytes).unwrap();
        assert_eq!(leaf.level, 0);
        assert_eq!(leaf.kind, BatchKind::Namespace);
        assert_eq!(leaf.entries.len(), 3);

        // Step 8: Look up each batch and verify its content.
        for (i, entry) in leaf.entries.iter().enumerate() {
            match entry {
                IndexEntry::Leaf {
                    locator,
                    first_key,
                    last_key,
                } => {
                    assert_eq!(locator.batch_id, i as u32);
                    assert_eq!(first_key, &first_names[i]);
                    assert_eq!(last_key, &last_names[i]);

                    let batch_start = locator.object_offset as usize;
                    let batch_end = batch_start + locator.total_stored_len as usize;
                    assert!(batch_end <= cluster_obj.len());

                    let decoded =
                        EncodedBatch::decode(&cluster_obj[batch_start..batch_end]).unwrap();
                    assert_eq!(decoded.header.digest, locator.digest);
                    assert_eq!(decoded.header.batch_id, i as u32);

                    let ns_batch = NamespaceBatch::decode(&decoded).unwrap();
                    assert_eq!(ns_batch.segments.len(), 1);
                    assert_eq!(ns_batch.segments[0].entries.len(), 20);
                }
                _ => panic!("expected leaf entry"),
            }
        }
    }
}
