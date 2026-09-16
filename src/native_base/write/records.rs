//! Compact row encodings for the head-KV side of the write pipeline.
//!
//! The cross-component control records (domain, registrations, mutation
//! results) are stored as BNCT v2 envelopes; these are the *row* formats the
//! head store itself keeps, encoded with the same canonical uvarint writer
//! as the wire layer (explicit sequential encodings, never a naturally
//! aligned Rust struct — spec 02 §10).

use crate::native_base::wire::bnct::HeadRef;
use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::refs::ObjectId;
use crate::native_base::wire::uvarint::{Reader, Writer};

/// The workspace head token guarding every commit (spec 18 §2: the
/// authoritative `HeadCommitToken` — head id, epoch, commit sequence and
/// writer generation). `write_domain_id` pins the origin domain of the head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadState {
    pub head: HeadRef,
    pub writer_generation: u64,
    pub write_domain_id: [u8; 16],
}

impl HeadState {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        self.head.encode_into(&mut w);
        w.u64(self.writer_generation);
        w.put(&self.write_domain_id);
        w.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> WireResult<HeadState> {
        let mut r = Reader::new(bytes);
        let head = HeadRef::decode(&mut r)?;
        let writer_generation = r.u64("head state")?;
        let write_domain_id: [u8; 16] = r.take(16, "head state")?.try_into().unwrap();
        if !r.is_empty() {
            return Err(WireError::invalid("head state", "trailing bytes"));
        }
        Ok(HeadState {
            head,
            writer_generation,
            write_domain_id,
        })
    }

    /// The next head state after one commit: same identity and epoch, one
    /// higher commit sequence. Callers must hold the same writer
    /// generation — a changed generation is a lease loss, not a commit.
    pub fn next_commit(&self) -> HeadState {
        HeadState {
            head: HeadRef {
                head_id: self.head.head_id,
                epoch: self.head.epoch,
                commit_seq: self.head.commit_seq + 1,
            },
            writer_generation: self.writer_generation,
            write_domain_id: self.write_domain_id,
        }
    }
}

/// Per-inode committed state. `committed_order` is the durable ordering
/// watermark: the `mutation_order` of the last commit applied to this inode
/// (spec 18 §10). A mutation with `mutation_order != committed_order + 1`
/// cannot commit — this is what makes reordered uploads impossible to land
/// out of order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InodeData {
    pub size: u64,
    pub data_version: u64,
    pub committed_order: u64,
}

impl InodeData {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u64(self.size);
        w.u64(self.data_version);
        w.u64(self.committed_order);
        w.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> WireResult<InodeData> {
        let mut r = Reader::new(bytes);
        let size = r.u64("inode data")?;
        let data_version = r.u64("inode data")?;
        let committed_order = r.u64("inode data")?;
        if !r.is_empty() {
            return Err(WireError::invalid("inode data", "trailing bytes"));
        }
        Ok(InodeData {
            size,
            data_version,
            committed_order,
        })
    }
}

/// Extent kind. Data extents reference an uploaded slice; Hole extents are
/// logical zeros recorded in the head KV (READ-001's hole semantics live
/// here, PR04+).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtentKind {
    Data,
    Hole,
}

impl ExtentKind {
    pub fn as_u8(self) -> u8 {
        match self {
            ExtentKind::Data => 0,
            ExtentKind::Hole => 1,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<ExtentKind> {
        match v {
            0 => Ok(ExtentKind::Data),
            1 => Ok(ExtentKind::Hole),
            other => Err(WireError::UnsupportedFormat(format!(
                "extent kind {other} (expected 0=Data or 1=Hole)"
            ))),
        }
    }
}

/// One committed extent: `logical_len` bytes starting at the row's key
/// offset. Data extents name the slice and the block range inside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeExtent {
    pub kind: ExtentKind,
    pub logical_len: u64,
    /// All-zero for Hole extents.
    pub slice_id: [u8; 16],
    /// First block of the extent inside the slice.
    pub first_block: u64,
    pub block_count: u64,
}

impl NativeExtent {
    pub fn hole(logical_len: u64) -> NativeExtent {
        NativeExtent {
            kind: ExtentKind::Hole,
            logical_len,
            slice_id: [0u8; 16],
            first_block: 0,
            block_count: 0,
        }
    }

    pub fn data(logical_len: u64, slice_id: [u8; 16], first_block: u64, block_count: u64) -> Self {
        NativeExtent {
            kind: ExtentKind::Data,
            logical_len,
            slice_id,
            first_block,
            block_count,
        }
    }

    pub fn end(&self, offset: u64) -> u64 {
        offset + self.logical_len
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u8(self.kind.as_u8());
        w.u64(self.logical_len);
        w.put(&self.slice_id);
        w.u64(self.first_block);
        w.u64(self.block_count);
        w.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> WireResult<NativeExtent> {
        let mut r = Reader::new(bytes);
        let kind = ExtentKind::from_u8(r.u8("extent")?)?;
        let logical_len = r.u64("extent")?;
        let slice_id: [u8; 16] = r.take(16, "extent")?.try_into().unwrap();
        let first_block = r.u64("extent")?;
        let block_count = r.u64("extent")?;
        if !r.is_empty() {
            return Err(WireError::invalid("extent", "trailing bytes"));
        }
        if logical_len == 0 {
            return Err(WireError::invalid("extent", "logical_len must be > 0"));
        }
        if kind == ExtentKind::Hole
            && (slice_id != [0u8; 16] || first_block != 0 || block_count != 0)
        {
            return Err(WireError::invalid(
                "extent",
                "hole extent must not reference a slice",
            ));
        }
        if kind == ExtentKind::Data && block_count == 0 {
            return Err(WireError::invalid(
                "extent",
                "data extent must reference at least one block",
            ));
        }
        Ok(NativeExtent {
            kind,
            logical_len,
            slice_id,
            first_block,
            block_count,
        })
    }
}

/// Head placement of one block (spec 07 §2: the commit writes the "head
/// Loose placement"). Loose placement identifies the object globally by
/// `ObjectId` plus the native layout version — unlike the seal-scoped
/// placements of PR03, which address objects by table ordinal. Packed head
/// placements arrive with the PR05/PR06A seal integration; only tag 0
/// exists today and decode fails closed on anything else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeadPlacement {
    Loose {
        object_id: ObjectId,
        native_layout: u8,
    },
}

impl HeadPlacement {
    pub fn loose(object_id: ObjectId) -> HeadPlacement {
        HeadPlacement::Loose {
            object_id,
            native_layout: crate::native_base::seal::placement::NATIVE_LAYOUT_VERSIONED_FRAMED,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        match self {
            HeadPlacement::Loose {
                object_id,
                native_layout,
            } => {
                w.u8(0);
                w.put(object_id);
                w.u8(*native_layout);
            }
        }
        w.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> WireResult<HeadPlacement> {
        let mut r = Reader::new(bytes);
        let what = "head placement";
        let placement = match r.u8(what)? {
            0 => {
                let object_id: ObjectId = r.take(16, what)?.try_into().unwrap();
                let native_layout = r.u8(what)?;
                HeadPlacement::Loose {
                    object_id,
                    native_layout,
                }
            }
            other => {
                return Err(WireError::UnsupportedFormat(format!(
                    "head placement tag {other} (only 0=Loose exists in this version)"
                )));
            }
        };
        if !r.is_empty() {
            return Err(WireError::invalid(what, "trailing bytes"));
        }
        Ok(placement)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head_state() -> HeadState {
        HeadState {
            head: HeadRef {
                head_id: [7u8; 16],
                epoch: 3,
                commit_seq: 41,
            },
            writer_generation: 2,
            write_domain_id: [9u8; 16],
        }
    }

    #[test]
    fn head_state_roundtrip_and_next_commit() {
        let h = head_state();
        let decoded = HeadState::decode(&h.encode()).unwrap();
        assert_eq!(decoded, h);
        let next = h.next_commit();
        assert_eq!(next.head.commit_seq, 42);
        assert_eq!(next.head.epoch, 3);
        assert_eq!(next.writer_generation, 2);
        // next_commit never changes identity — a new generation is a new head.
        assert_eq!(next.head.head_id, h.head.head_id);
    }

    #[test]
    fn inode_data_roundtrip() {
        let d = InodeData {
            size: 12345,
            data_version: 6,
            committed_order: 9,
        };
        assert_eq!(InodeData::decode(&d.encode()).unwrap(), d);
        assert_eq!(
            InodeData::decode(&InodeData::default().encode()).unwrap(),
            InodeData::default()
        );
    }

    #[test]
    fn extent_roundtrip_and_invariants() {
        let e = NativeExtent::data(4096, [5u8; 16], 3, 4);
        assert_eq!(NativeExtent::decode(&e.encode()).unwrap(), e);
        let h = NativeExtent::hole(512);
        assert_eq!(NativeExtent::decode(&h.encode()).unwrap(), h);
        // A hole that references a slice is rejected.
        let mut bad = NativeExtent::hole(10);
        bad.slice_id = [1u8; 16];
        assert!(NativeExtent::decode(&bad.encode()).is_err());
        // Zero-length extents are rejected.
        let mut zero = NativeExtent::hole(0);
        zero.logical_len = 0;
        assert!(NativeExtent::decode(&zero.encode()).is_err());
        // Unknown kind rejected.
        let mut w = Writer::new();
        w.u8(9);
        w.u64(1);
        w.put(&[0u8; 16]);
        w.u64(0);
        w.u64(1);
        assert!(NativeExtent::decode(&w.into_bytes()).is_err());
    }

    #[test]
    fn head_placement_roundtrip_and_closed_tags() {
        let p = HeadPlacement::loose([0xab; 16]);
        let decoded = HeadPlacement::decode(&p.encode()).unwrap();
        assert_eq!(decoded, p);
        assert_eq!(
            decoded,
            HeadPlacement::Loose {
                object_id: [0xab; 16],
                native_layout: 1,
            }
        );
        let mut w = Writer::new();
        w.u8(1); // unknown tag: packed placements arrive with PR05/06A
        w.put(&[0u8; 16]);
        assert!(HeadPlacement::decode(&w.into_bytes()).is_err());
    }
}
