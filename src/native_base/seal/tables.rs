//! Data Seal table directory (spec 04 §2) and the per-table key encodings.
//!
//! The `.brfds` root payload is a kind-2 (TableRootDirectory) page: `BNSD`,
//! `u16 version=1`, `u16 table_count=4`, then one `u16 table_id +
//! Option<ChildRef>` per table in strictly ascending table-id order. Empty
//! tables use `None`. Table keys are the only big-endian fields in the
//! format, so that numeric order equals byte order inside an index.

use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::refs::ChildRef;
use crate::native_base::wire::uvarint::{Reader, Writer};

pub const SEAL_ROOT_MAGIC: &[u8; 4] = b"BNSD";
pub const SEAL_ROOT_VERSION: u16 = 1;
pub const SEAL_TABLE_COUNT: u16 = 4;

/// The four Data Seal tables (spec 04 §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TableId {
    Bindings = 1,
    Placements = 2,
    Objects = 3,
    Frames = 4,
}

impl TableId {
    pub const ALL: [TableId; 4] = [
        TableId::Bindings,
        TableId::Placements,
        TableId::Objects,
        TableId::Frames,
    ];

    pub fn as_u16(self) -> u16 {
        self as u16
    }

    pub fn from_u16(v: u16) -> WireResult<TableId> {
        match v {
            1 => Ok(TableId::Bindings),
            2 => Ok(TableId::Placements),
            3 => Ok(TableId::Objects),
            4 => Ok(TableId::Frames),
            other => Err(WireError::invalid(
                "seal table id",
                format!("table id {other} (expected 1..=4)"),
            )),
        }
    }

    /// The sealed-at-rest key domain of this table.
    pub fn key_len(self) -> usize {
        match self {
            TableId::Bindings | TableId::Placements => 12,
            TableId::Objects => 4,
            TableId::Frames => 8,
        }
    }
}

/// Bindings/Placements key: `BE64(slice_id) || BE32(block_index)`.
///
/// Big-endian so that `(slice_id, block_index)` numeric order equals the
/// lexicographic byte order the BNPG pages rely on.
pub fn binding_key(slice_id: u64, block_index: u32) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..8].copy_from_slice(&slice_id.to_be_bytes());
    key[8..].copy_from_slice(&block_index.to_be_bytes());
    key
}

/// Objects key: `BE32(object_ordinal)`.
pub fn object_key(object_ordinal: u32) -> [u8; 4] {
    object_ordinal.to_be_bytes()
}

/// Frames key: `BE64(frame_slot)`. The frame slot is a Seal-scoped number,
/// distinct from the pack-internal frame ordinal (spec 04 §2).
pub fn frame_key(frame_slot: u64) -> [u8; 8] {
    frame_slot.to_be_bytes()
}

/// The decoded root directory: one optional child per table.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SealRoot {
    /// Indexed by `TableId` position in [`TableId::ALL`].
    pub tables: [Option<ChildRef>; 4],
}

impl SealRoot {
    pub fn table(&self, id: TableId) -> Option<&ChildRef> {
        self.tables[Self::index(id)].as_ref()
    }

    pub fn set_table(&mut self, id: TableId, child: Option<ChildRef>) {
        self.tables[Self::index(id)] = child;
    }

    fn index(id: TableId) -> usize {
        Self::index_of(id.as_u16())
    }

    fn index_of(id: u16) -> usize {
        (id - 1) as usize
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.put(SEAL_ROOT_MAGIC);
        w.u16(SEAL_ROOT_VERSION);
        w.u16(SEAL_TABLE_COUNT);
        // Strictly ascending table ids 1..=4.
        for (i, table) in self.tables.iter().enumerate() {
            w.u16((i as u16) + 1);
            match table {
                None => w.option_tag(false),
                Some(child) => {
                    w.option_tag(true);
                    child.encode_into(&mut w);
                }
            }
        }
        w.into_bytes()
    }

    pub fn decode(payload: &[u8]) -> WireResult<SealRoot> {
        let what = "seal root directory";
        let mut r = Reader::new(payload);
        let magic = r.take(4, what)?;
        if magic != SEAL_ROOT_MAGIC {
            return Err(WireError::invalid(
                what,
                format!("magic {magic:02x?} is not {SEAL_ROOT_MAGIC:02x?}"),
            ));
        }
        let version = r.u16(what)?;
        if version != SEAL_ROOT_VERSION {
            return Err(WireError::UnsupportedFormat(format!(
                "seal root version {version} (expected {SEAL_ROOT_VERSION})"
            )));
        }
        let count = r.u16(what)?;
        if count != SEAL_TABLE_COUNT {
            return Err(WireError::invalid(
                what,
                format!("table_count {count} (expected {SEAL_TABLE_COUNT})"),
            ));
        }
        let mut tables: [Option<ChildRef>; 4] = [None, None, None, None];
        let mut prev_id: Option<u16> = None;
        for _ in 0..count {
            let id = r.u16(what)?;
            TableId::from_u16(id)?;
            if let Some(prev) = prev_id
                && id <= prev
            {
                return Err(WireError::invalid(
                    what,
                    format!("table ids must strictly ascend, got {prev} then {id}"),
                ));
            }
            if id == 0 || id > SEAL_TABLE_COUNT {
                return Err(WireError::invalid(
                    what,
                    format!("table id {id} outside 1..={SEAL_TABLE_COUNT}"),
                ));
            }
            prev_id = Some(id);
            let present = r.option_tag(what)?;
            tables[Self::index_of(id)] = if present {
                Some(ChildRef::decode(&mut r)?)
            } else {
                None
            };
        }
        if !r.is_empty() {
            return Err(WireError::invalid(
                what,
                format!("{} trailing bytes after last table entry", r.remaining()),
            ));
        }
        Ok(SealRoot { tables })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_base::wire::container::Codec;
    use crate::native_base::wire::refs::{ObjectRef, PageAddress, PageKind};

    fn sample_child() -> ChildRef {
        ChildRef::Local(PageAddress {
            offset: 64,
            stored_len: 128,
            raw_len: 128,
            codec: Codec::None,
            page_kind: PageKind::GenericKeyValue,
            level: 0,
            entry_count: 3,
            stored_digest: [1u8; 32],
        })
    }

    fn sample_object() -> ObjectRef {
        ObjectRef {
            object_id: [7u8; 16],
            kind: 2,
            object_len: 512,
            full_hash: [3u8; 32],
            key: b"seals/base".to_vec(),
        }
    }

    #[test]
    fn root_roundtrip_with_empty_and_present_tables() {
        let mut root = SealRoot::default();
        root.set_table(TableId::Bindings, Some(sample_child()));
        root.set_table(
            TableId::Frames,
            Some(ChildRef::External(
                crate::native_base::wire::refs::RootRef {
                    object: sample_object(),
                    address: PageAddress {
                        level: 2,
                        ..match sample_child() {
                            ChildRef::Local(addr) => addr,
                            _ => unreachable!(),
                        }
                    },
                },
            )),
        );
        let enc = root.encode();
        assert_eq!(SealRoot::decode(&enc).unwrap(), root);
        // Four tables always encoded, ascending.
        assert_eq!(&enc[..4], SEAL_ROOT_MAGIC);
    }

    #[test]
    fn root_rejects_wrong_version_count_unsorted_ids_and_trailing_bytes() {
        let root = SealRoot::default();
        let mut enc = root.encode();
        enc[4] = 2; // version
        assert!(SealRoot::decode(&enc).is_err());
        let mut enc = root.encode();
        enc[6] = 3; // table_count
        assert!(SealRoot::decode(&enc).is_err());
        // Unknown table id 5.
        let mut w = Writer::new();
        w.put(SEAL_ROOT_MAGIC);
        w.u16(1);
        w.u16(1);
        w.u16(5);
        w.option_tag(false);
        assert!(SealRoot::decode(w.as_slice()).is_err());
        // Trailing byte.
        let mut enc = root.encode();
        enc.push(0);
        assert!(SealRoot::decode(&enc).is_err());
    }

    #[test]
    fn keys_are_big_endian_so_numeric_order_equals_byte_order() {
        // (7, 0) must sort before (7, 1) and before (8, 0) in byte order.
        assert!(binding_key(7, 0) < binding_key(7, 1));
        assert!(binding_key(7, u32::MAX) < binding_key(8, 0));
        assert!(binding_key(0, 0) < binding_key(1, 0));
        assert_eq!(binding_key(0x0102, 0x0304)[..8], [0, 0, 0, 0, 0, 0, 1, 2]);
        assert_eq!(binding_key(0x0102, 0x0304)[8..], [0, 0, 3, 4]);
        assert_eq!(object_key(0x0506), [0, 0, 5, 6]);
        assert_eq!(frame_key(0x0708), [0, 0, 0, 0, 0, 0, 7, 8]);
        assert!(frame_key(1) < frame_key(2));
        assert!(object_key(1) < object_key(256));
    }
}
