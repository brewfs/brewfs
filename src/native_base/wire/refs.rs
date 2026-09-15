//! PageAddress, ObjectRef, RootRef, ChildRef (spec 02 §5).
//!
//! These are explicit sequential encodings — never a naturally aligned Rust
//! struct persisted directly (spec 02 §10).

use super::container::{Codec, ObjectKind};
use super::error::{WireError, WireResult};
use super::uvarint::{Reader, Writer};

/// Maximum physical key length (spec 02 §5).
pub const MAX_KEY_LEN: usize = 1024;

pub type ObjectId = [u8; 16];
pub type Hash32 = [u8; 32];

/// PageAddress: exactly 56 bytes (spec 02 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageAddress {
    pub offset: u64,
    pub stored_len: u32,
    pub raw_len: u32,
    pub codec: Codec,
    pub page_kind: PageKind,
    /// Leaf pages are level 0; internal pages descend by one per level.
    pub level: u8,
    pub entry_count: u32,
    /// SHA-256 of the stored (possibly compressed) payload.
    pub stored_digest: Hash32,
}

/// page_kind (spec 02 §6). Kinds 2/3 do not use the BNPG prefix; their
/// payload layouts are defined by specs 04/05.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
    GenericKeyValue,
    TableRootDirectory,
    ManifestPayload,
    InventoryIndex,
}

impl PageKind {
    pub fn as_u8(self) -> u8 {
        match self {
            PageKind::GenericKeyValue => 1,
            PageKind::TableRootDirectory => 2,
            PageKind::ManifestPayload => 3,
            PageKind::InventoryIndex => 4,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<PageKind> {
        match v {
            1 => Ok(PageKind::GenericKeyValue),
            2 => Ok(PageKind::TableRootDirectory),
            3 => Ok(PageKind::ManifestPayload),
            4 => Ok(PageKind::InventoryIndex),
            other => Err(WireError::UnsupportedFormat(format!(
                "page kind {other} (expected 1..=4)"
            ))),
        }
    }
}

/// Maximum index height (spec 02 §8).
pub const MAX_INDEX_LEVEL: u8 = 12;

impl PageAddress {
    pub const ENCODED_LEN: usize = 56;

    pub fn encode_into(&self, w: &mut Writer) {
        w.u64(self.offset);
        w.u32(self.stored_len);
        w.u32(self.raw_len);
        w.u8(self.codec.as_u8());
        w.u8(self.page_kind.as_u8());
        w.u8(self.level);
        w.u8(0); // reserved
        w.u32(self.entry_count);
        w.put(&self.stored_digest);
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        self.encode_into(&mut w);
        debug_assert_eq!(w.len(), Self::ENCODED_LEN);
        w.into_bytes()
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<PageAddress> {
        let what = "page address";
        let offset = r.u64(what)?;
        let stored_len = r.u32(what)?;
        let raw_len = r.u32(what)?;
        let codec = Codec::from_u8(r.u8(what)?)?;
        let page_kind = PageKind::from_u8(r.u8(what)?)?;
        let level = r.u8(what)?;
        let reserved = r.u8(what)?;
        if reserved != 0 {
            return Err(WireError::invalid(what, "reserved byte not zero"));
        }
        if level > MAX_INDEX_LEVEL {
            return Err(WireError::invalid(
                what,
                format!("level {level} exceeds max index height {MAX_INDEX_LEVEL}"),
            ));
        }
        let entry_count = r.u32(what)?;
        let stored_digest: Hash32 = r.take(32, what)?.try_into().unwrap();
        Ok(PageAddress {
            offset,
            stored_len,
            raw_len,
            codec,
            page_kind,
            level,
            entry_count,
            stored_digest,
        })
    }
}

/// ObjectRef (spec 02 §5): sequential encoding, key is UTF-8 without NUL,
/// at most [`MAX_KEY_LEN`] bytes, and is relative to the key prefix allowed
/// by the outer snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRef {
    pub object_id: ObjectId,
    pub kind: u8,
    pub object_len: u64,
    pub full_hash: Hash32,
    pub key: Vec<u8>,
}

impl ObjectRef {
    pub fn encode_into(&self, w: &mut Writer) {
        w.put(&self.object_id);
        w.u8(self.kind);
        w.u64(self.object_len);
        w.put(&self.full_hash);
        w.bytes(&self.key);
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<ObjectRef> {
        let what = "object ref";
        let object_id: ObjectId = r.take(16, what)?.try_into().unwrap();
        let kind = r.u8(what)?;
        let object_len = r.u64(what)?;
        let full_hash: Hash32 = r.take(32, what)?.try_into().unwrap();
        let key = r.bytes(what)?.to_vec();
        validate_object_key(&key)?;
        Ok(ObjectRef {
            object_id,
            kind,
            object_len,
            full_hash,
            key,
        })
    }
}

pub fn validate_object_key(key: &[u8]) -> WireResult<()> {
    if key.len() > MAX_KEY_LEN {
        return Err(WireError::LimitExceeded(format!(
            "object key {} bytes exceeds {MAX_KEY_LEN}",
            key.len()
        )));
    }
    if key.contains(&0) {
        return Err(WireError::invalid("object key", "contains NUL"));
    }
    if std::str::from_utf8(key).is_err() {
        return Err(WireError::invalid("object key", "not valid UTF-8"));
    }
    Ok(())
}

/// RootRef = ObjectRef followed by PageAddress (spec 02 §5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootRef {
    pub object: ObjectRef,
    pub address: PageAddress,
}

impl RootRef {
    pub fn encode_into(&self, w: &mut Writer) {
        self.object.encode_into(w);
        self.address.encode_into(w);
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<RootRef> {
        Ok(RootRef {
            object: ObjectRef::decode(r)?,
            address: PageAddress::decode(r)?,
        })
    }
}

/// ChildRef (spec 02 §5): tag 0 = local PageAddress of the current
/// container; tag 1 = external RootRef of an already-sealed object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildRef {
    Local(PageAddress),
    External(RootRef),
}

impl ChildRef {
    pub fn encode_into(&self, w: &mut Writer) {
        match self {
            ChildRef::Local(addr) => {
                w.u8(0);
                addr.encode_into(w);
            }
            ChildRef::External(root) => {
                w.u8(1);
                root.encode_into(w);
            }
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> WireResult<ChildRef> {
        match r.u8("child ref tag")? {
            0 => Ok(ChildRef::Local(PageAddress::decode(r)?)),
            1 => Ok(ChildRef::External(RootRef::decode(r)?)),
            other => Err(WireError::invalid(
                "child ref tag",
                format!("tag {other} is not 0 (local) or 1 (external)"),
            )),
        }
    }
}

/// Structural check for a child of an internal index page: the child level
/// must be exactly one below the parent's (spec 02 §5). The table root
/// directory is not an internal index page and is exempt.
pub fn ensure_child_descends(parent_level: u8, child_level: u8) -> WireResult<()> {
    if parent_level == 0 {
        return Err(WireError::invalid(
            "internal page level",
            "level 0 cannot have children (leaf level is 0)",
        ));
    }
    let expected = parent_level - 1;
    if child_level != expected {
        return Err(WireError::invalid(
            "internal page level",
            format!(
                "child level {child_level} does not equal parent level {parent_level} minus one"
            ),
        ));
    }
    Ok(())
}

/// Structural check that a RootRef's object kind may carry the referenced
/// page kind (spec 02 §10: RootRef validation must check the real object
/// type allows the kind).
pub fn ensure_object_kind_allows_page(
    object_kind: ObjectKind,
    page_kind: PageKind,
) -> WireResult<()> {
    let ok = matches!(
        (object_kind, page_kind),
        (
            ObjectKind::FrozenMetadata | ObjectKind::SnapshotManifest | ObjectKind::PagedInventory,
            PageKind::TableRootDirectory
                | PageKind::ManifestPayload
                | PageKind::InventoryIndex
                | PageKind::GenericKeyValue
        ) | (
            ObjectKind::DataSeal,
            PageKind::TableRootDirectory | PageKind::GenericKeyValue
        )
    );
    if ok {
        Ok(())
    } else {
        Err(WireError::invalid(
            "root ref",
            format!(
                "object kind {:?} cannot be referenced as page kind {:?}",
                object_kind, page_kind
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_object_ref() -> ObjectRef {
        ObjectRef {
            object_id: [1u8; 16],
            kind: ObjectKind::DataPack.as_u8(),
            object_len: 312,
            full_hash: [2u8; 32],
            key: b"packs/abc".to_vec(),
        }
    }

    fn sample_page_address() -> PageAddress {
        PageAddress {
            offset: 64,
            stored_len: 128,
            raw_len: 130,
            codec: Codec::Zstd,
            page_kind: PageKind::GenericKeyValue,
            level: 3,
            entry_count: 7,
            stored_digest: [3u8; 32],
        }
    }

    #[test]
    fn page_address_is_exactly_56_bytes_and_roundtrips() {
        let addr = sample_page_address();
        assert_eq!(addr.encode().len(), PageAddress::ENCODED_LEN);
        let enc = addr.encode();
        let mut r = Reader::new(&enc);
        assert_eq!(PageAddress::decode(&mut r).unwrap(), addr);
        assert!(r.is_empty());
    }

    #[test]
    fn page_address_rejects_unknown_codec_kind_and_reserved() {
        let addr = sample_page_address();
        let mut enc = addr.encode();
        enc[16] = 1; // codec 1
        assert!(matches!(
            PageAddress::decode(&mut Reader::new(&enc)),
            Err(WireError::UnsupportedFormat(_))
        ));
        let mut enc = addr.encode();
        enc[17] = 5; // page_kind 5
        assert!(matches!(
            PageAddress::decode(&mut Reader::new(&enc)),
            Err(WireError::UnsupportedFormat(_))
        ));
        let mut enc = addr.encode();
        enc[19] = 1; // reserved
        assert!(PageAddress::decode(&mut Reader::new(&enc)).is_err());
        let mut enc = addr.encode();
        enc[18] = 13; // level > 12
        assert!(PageAddress::decode(&mut Reader::new(&enc)).is_err());
    }

    #[test]
    fn object_ref_roundtrip_and_key_validation() {
        let r = sample_object_ref();
        let mut w = Writer::new();
        r.encode_into(&mut w);
        let mut rd = Reader::new(w.as_slice());
        assert_eq!(ObjectRef::decode(&mut rd).unwrap(), r);
        assert!(rd.is_empty());

        for bad in [
            vec![0u8; MAX_KEY_LEN + 1], // too long
            b"a\x00b".to_vec(),         // NUL
            vec![0xffu8, 0xfe],         // not UTF-8
        ] {
            let mut obj = r.clone();
            obj.key = bad;
            let mut w = Writer::new();
            obj.encode_into(&mut w);
            assert!(ObjectRef::decode(&mut Reader::new(w.as_slice())).is_err());
        }
    }

    #[test]
    fn root_ref_roundtrip() {
        let root = RootRef {
            object: sample_object_ref(),
            address: sample_page_address(),
        };
        let mut w = Writer::new();
        root.encode_into(&mut w);
        let mut r = Reader::new(w.as_slice());
        assert_eq!(RootRef::decode(&mut r).unwrap(), root);
        assert!(r.is_empty());
    }

    #[test]
    fn child_ref_both_tags_roundtrip_and_rejects_unknown() {
        for child in [
            ChildRef::Local(sample_page_address()),
            ChildRef::External(RootRef {
                object: sample_object_ref(),
                address: sample_page_address(),
            }),
        ] {
            let mut w = Writer::new();
            child.encode_into(&mut w);
            let mut r = Reader::new(w.as_slice());
            assert_eq!(ChildRef::decode(&mut r).unwrap(), child);
            assert!(r.is_empty());
        }
        let mut w = Writer::new();
        w.u8(2);
        assert!(ChildRef::decode(&mut Reader::new(w.as_slice())).is_err());
    }

    #[test]
    fn child_must_descend_exactly_one_level() {
        assert!(ensure_child_descends(1, 0).is_ok());
        assert!(ensure_child_descends(4, 3).is_ok());
        assert!(ensure_child_descends(1, 1).is_err()); // same level
        assert!(ensure_child_descends(2, 0).is_err()); // skipped a level
        assert!(ensure_child_descends(0, 0).is_err()); // leaf cannot parent
    }

    #[test]
    fn object_kind_page_kind_matrix() {
        assert!(
            ensure_object_kind_allows_page(
                ObjectKind::FrozenMetadata,
                PageKind::TableRootDirectory
            )
            .is_ok()
        );
        assert!(
            ensure_object_kind_allows_page(ObjectKind::SnapshotManifest, PageKind::ManifestPayload)
                .is_ok()
        );
        assert!(
            ensure_object_kind_allows_page(ObjectKind::PagedInventory, PageKind::InventoryIndex)
                .is_ok()
        );
        assert!(
            ensure_object_kind_allows_page(ObjectKind::DataPack, PageKind::GenericKeyValue)
                .is_err()
        );
    }
}
