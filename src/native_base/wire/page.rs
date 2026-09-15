//! BNPG index pages (spec 02 §6): the raw (decompressed) page layout used
//! by generic key/value indexes.
//!
//! Raw page prefix is 16 bytes: `"BNPG"[4] | kind:u8 | level:u8 |
//! flags:u16=0 | entry_count:u32 | reserved:u32=0`. Leaf records are
//! `shared_prefix:uvarint + suffix:bytes + value:bytes`; internal records
//! are `min_key:bytes + max_key:bytes + ChildRef`. Keys are compared as
//! unsigned byte strings (lexicographic, spec 02 §10).
//!
//! Page kinds 2/3 (table root directory, manifest payload) do not use the
//! BNPG prefix; their codecs live in later PRs (04/05 formats).

use super::error::{WireError, WireResult};
use super::refs::{ChildRef, ensure_child_descends};
use super::uvarint::{Reader, Writer};

pub const PAGE_PREFIX_LEN: usize = 16;
pub const PAGE_MAGIC: &[u8; 4] = b"BNPG";

/// Hard limits (spec 02 §8).
pub const MAX_RAW_PAGE: usize = 1024 * 1024;
pub const MAX_ENCODED_PAGE: usize = MAX_RAW_PAGE + 64 * 1024;
pub const MAX_PAGE_ENTRIES: u32 = 65_535;
pub const MAX_INDEX_KEY: usize = 4096;
pub const MAX_INDEX_VALUE: usize = 256 * 1024;

/// A BNPG page kind (subset: kind 1 only; 2/3/4 pages do not carry the BNPG
/// prefix and are handled by their own container codecs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BnpgKind {
    GenericKeyValue,
}

impl BnpgKind {
    pub fn as_u8(self) -> u8 {
        match self {
            BnpgKind::GenericKeyValue => 1,
        }
    }

    pub fn from_u8(v: u8) -> WireResult<BnpgKind> {
        match v {
            1 => Ok(BnpgKind::GenericKeyValue),
            other => Err(WireError::UnsupportedFormat(format!(
                "BNPG page kind {other} (expected 1=generic key/value)"
            ))),
        }
    }
}

/// One leaf record with its fully reconstructed key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// One internal record: a closed key range routing to a child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalEntry {
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub child: ChildRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageBody {
    Leaf(Vec<LeafEntry>),
    Internal(Vec<InternalEntry>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPage {
    pub kind: BnpgKind,
    /// Leaf pages are level 0.
    pub level: u8,
    pub body: PageBody,
}

impl IndexPage {
    pub fn entry_count(&self) -> u32 {
        match &self.body {
            PageBody::Leaf(entries) => entries.len() as u32,
            PageBody::Internal(entries) => entries.len() as u32,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.put(PAGE_MAGIC);
        w.u8(self.kind.as_u8());
        w.u8(self.level);
        w.u16(0); // flags
        w.u32(self.entry_count());
        w.u32(0); // reserved
        match &self.body {
            PageBody::Leaf(entries) => {
                let mut prev: &[u8] = &[];
                for entry in entries {
                    let shared = common_prefix_len(prev, &entry.key);
                    w.uvarint(shared as u64);
                    w.bytes(&entry.key[shared..]);
                    w.bytes(&entry.value);
                    prev = &entry.key;
                }
            }
            PageBody::Internal(entries) => {
                for entry in entries {
                    w.bytes(&entry.min_key);
                    w.bytes(&entry.max_key);
                    entry.child.encode_into(&mut w);
                }
            }
        }
        w.into_bytes()
    }

    /// Decode and structurally validate a raw (already decompressed) page.
    /// Enforces the prefix fields, entry count agreement, exact consumption
    /// (no trailing bytes), leaf key ordering/strict increase, prefix rules,
    /// internal range ordering/non-overlap, and per-level child descent.
    /// Limits are checked against the input length before any allocation
    /// beyond the page itself.
    pub fn decode(raw: &[u8]) -> WireResult<IndexPage> {
        if raw.len() > MAX_RAW_PAGE {
            return Err(WireError::LimitExceeded(format!(
                "raw page {} bytes exceeds {MAX_RAW_PAGE}",
                raw.len()
            )));
        }
        let what = "index page";
        let mut r = Reader::new(raw);
        let magic = r.take(4, what)?;
        if magic != PAGE_MAGIC {
            return Err(WireError::invalid(
                what,
                format!("magic {magic:02x?} is not {PAGE_MAGIC:02x?}"),
            ));
        }
        let kind = BnpgKind::from_u8(r.u8(what)?)?;
        let level = r.u8(what)?;
        let flags = r.u16(what)?;
        if flags != 0 {
            return Err(WireError::invalid(what, "flags must be zero"));
        }
        let entry_count = r.u32(what)?;
        let reserved = r.u32(what)?;
        if reserved != 0 {
            return Err(WireError::invalid(what, "reserved must be zero"));
        }
        if entry_count > MAX_PAGE_ENTRIES {
            return Err(WireError::LimitExceeded(format!(
                "entry_count {entry_count} exceeds {MAX_PAGE_ENTRIES}"
            )));
        }

        let body = match level {
            0 => {
                let mut entries = Vec::with_capacity(entry_count as usize);
                let mut prev: Option<Vec<u8>> = None;
                for i in 0..entry_count {
                    let shared = r.uvarint(what)? as usize;
                    let suffix = r.bytes(what)?;
                    if let Some(p) = &prev {
                        if shared > p.len() {
                            return Err(WireError::invalid(
                                what,
                                format!(
                                    "entry {i}: shared prefix {shared} exceeds previous key length {}",
                                    p.len()
                                ),
                            ));
                        }
                    } else if shared != 0 {
                        return Err(WireError::invalid(
                            what,
                            format!("first entry shared prefix {shared} must be 0"),
                        ));
                    }
                    let value = r.bytes(what)?;
                    let mut key = match &prev {
                        Some(p) => p[..shared].to_vec(),
                        None => Vec::new(),
                    };
                    key.extend_from_slice(suffix);
                    if key.len() > MAX_INDEX_KEY {
                        return Err(WireError::LimitExceeded(format!(
                            "index key {} bytes exceeds {MAX_INDEX_KEY}",
                            key.len()
                        )));
                    }
                    if value.len() > MAX_INDEX_VALUE {
                        return Err(WireError::LimitExceeded(format!(
                            "index value {} bytes exceeds {MAX_INDEX_VALUE}",
                            value.len()
                        )));
                    }
                    if let Some(p) = &prev
                        && key.as_slice() <= p.as_slice()
                    {
                        return Err(WireError::invalid(
                            what,
                            format!("entry {i}: key not strictly greater than previous"),
                        ));
                    }
                    prev = Some(key.clone());
                    entries.push(LeafEntry {
                        key,
                        value: value.to_vec(),
                    });
                }
                PageBody::Leaf(entries)
            }
            _ => {
                let mut entries = Vec::with_capacity(entry_count as usize);
                let mut last_max: Option<Vec<u8>> = None;
                for i in 0..entry_count {
                    let min_key = r.bytes(what)?.to_vec();
                    let max_key = r.bytes(what)?.to_vec();
                    if min_key.len() > MAX_INDEX_KEY || max_key.len() > MAX_INDEX_KEY {
                        return Err(WireError::LimitExceeded(format!(
                            "internal key exceeds {MAX_INDEX_KEY} bytes"
                        )));
                    }
                    let child = ChildRef::decode(&mut r)?;
                    let child_level = match &child {
                        ChildRef::Local(addr) => addr.level,
                        ChildRef::External(root) => root.address.level,
                    };
                    // WIRE-011: local and external children must both
                    // descend exactly one level (spec 02 §5).
                    ensure_child_descends(level, child_level)?;
                    if min_key > max_key {
                        return Err(WireError::invalid(
                            what,
                            format!("entry {i}: min_key > max_key"),
                        ));
                    }
                    if let Some(lm) = &last_max
                        && min_key.as_slice() <= lm.as_slice()
                    {
                        return Err(WireError::invalid(
                            what,
                            format!("entry {i}: range overlaps or is unordered vs previous"),
                        ));
                    }
                    last_max = Some(max_key.clone());
                    entries.push(InternalEntry {
                        min_key,
                        max_key,
                        child,
                    });
                }
                PageBody::Internal(entries)
            }
        };

        if !r.is_empty() {
            return Err(WireError::invalid(
                what,
                format!("{} trailing bytes after last record", r.remaining()),
            ));
        }
        Ok(IndexPage { kind, level, body })
    }
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let mut n = 0;
    while n < a.len() && n < b.len() && a[n] == b[n] {
        n += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_base::wire::container::Codec;
    use crate::native_base::wire::refs::PageAddress;

    fn addr(level: u8) -> PageAddress {
        PageAddress {
            offset: 64,
            stored_len: 32,
            raw_len: 32,
            codec: Codec::None,
            page_kind: crate::native_base::wire::refs::PageKind::GenericKeyValue,
            level,
            entry_count: 1,
            stored_digest: [9u8; 32],
        }
    }

    #[test]
    fn leaf_page_roundtrip_with_prefix_compression() {
        let page = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 0,
            body: PageBody::Leaf(vec![
                LeafEntry {
                    key: b"apple".to_vec(),
                    value: b"v1".to_vec(),
                },
                LeafEntry {
                    key: b"application".to_vec(),
                    value: vec![0u8; 64],
                },
                LeafEntry {
                    key: b"zebra".to_vec(),
                    value: b"v3".to_vec(),
                },
            ]),
        };
        let enc = page.encode();
        assert_eq!(IndexPage::decode(&enc).unwrap(), page);
        // The middle record is prefix-compressed: shared=4, suffix="ication".
        let needle = [4u8, 7u8, b'i', b'c', b'a', b't', b'i', b'o', b'n'];
        assert!(enc.windows(needle.len()).any(|w| w == needle));
    }

    #[test]
    fn leaf_rejects_duplicate_and_unsorted_keys() {
        // WIRE-010: full verification rejects duplicates/ordering violations.
        let page = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 0,
            body: PageBody::Leaf(vec![
                LeafEntry {
                    key: b"b".to_vec(),
                    value: b"v".to_vec(),
                },
                LeafEntry {
                    key: b"a".to_vec(),
                    value: b"v".to_vec(),
                },
            ]),
        };
        assert!(IndexPage::decode(&page.encode()).is_err());
        let page = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 0,
            body: PageBody::Leaf(vec![
                LeafEntry {
                    key: b"a".to_vec(),
                    value: b"v".to_vec(),
                },
                LeafEntry {
                    key: b"a".to_vec(),
                    value: b"v".to_vec(),
                },
            ]),
        };
        assert!(IndexPage::decode(&page.encode()).is_err());
    }

    #[test]
    fn leaf_rejects_nonzero_first_shared_prefix_and_overlong_shared() {
        // Hand-build a page whose second record's shared prefix exceeds the
        // first key's length.
        let mut w = Writer::new();
        w.put(PAGE_MAGIC);
        w.u8(1);
        w.u8(0);
        w.u16(0);
        w.u32(2);
        w.u32(0);
        w.uvarint(0);
        w.bytes(b"ab");
        w.bytes(b"v");
        w.uvarint(5); // longer than "ab"
        w.bytes(b"c");
        w.bytes(b"v");
        assert!(IndexPage::decode(w.as_slice()).is_err());
    }

    #[test]
    fn internal_page_roundtrip_and_range_validation() {
        let page = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 2,
            body: PageBody::Internal(vec![
                InternalEntry {
                    min_key: b"a".to_vec(),
                    max_key: b"m".to_vec(),
                    child: ChildRef::Local(addr(1)),
                },
                InternalEntry {
                    min_key: b"n".to_vec(),
                    max_key: b"z".to_vec(),
                    child: ChildRef::Local(addr(1)),
                },
            ]),
        };
        assert_eq!(IndexPage::decode(&page.encode()).unwrap(), page);

        // Overlapping ranges rejected.
        let bad = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 2,
            body: PageBody::Internal(vec![
                InternalEntry {
                    min_key: b"a".to_vec(),
                    max_key: b"n".to_vec(),
                    child: ChildRef::Local(addr(1)),
                },
                InternalEntry {
                    min_key: b"b".to_vec(),
                    max_key: b"z".to_vec(),
                    child: ChildRef::Local(addr(1)),
                },
            ]),
        };
        assert!(IndexPage::decode(&bad.encode()).is_err());

        // min > max rejected.
        let bad = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 2,
            body: PageBody::Internal(vec![InternalEntry {
                min_key: b"z".to_vec(),
                max_key: b"a".to_vec(),
                child: ChildRef::Local(addr(1)),
            }]),
        };
        assert!(IndexPage::decode(&bad.encode()).is_err());
    }

    #[test]
    fn internal_rejects_child_level_not_descending() {
        // WIRE-011: local child at the same level as the parent.
        let bad = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 2,
            body: PageBody::Internal(vec![InternalEntry {
                min_key: b"a".to_vec(),
                max_key: b"z".to_vec(),
                child: ChildRef::Local(addr(2)),
            }]),
        };
        assert!(IndexPage::decode(&bad.encode()).is_err());

        // External child that does not descend either.
        let bad = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 1,
            body: PageBody::Internal(vec![InternalEntry {
                min_key: b"a".to_vec(),
                max_key: b"z".to_vec(),
                child: ChildRef::External(crate::native_base::wire::refs::RootRef {
                    object: crate::native_base::wire::refs::ObjectRef {
                        object_id: [1u8; 16],
                        kind: 3,
                        object_len: 128,
                        full_hash: [2u8; 32],
                        key: b"old-container".to_vec(),
                    },
                    address: addr(1), // must be 0 under a level-1 parent
                }),
            }]),
        };
        assert!(IndexPage::decode(&bad.encode()).is_err());
    }

    #[test]
    fn page_rejects_trailing_bytes_and_count_mismatch() {
        let page = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 0,
            body: PageBody::Leaf(vec![LeafEntry {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }]),
        };
        let mut enc = page.encode();
        enc.push(0); // trailing byte
        assert!(IndexPage::decode(&enc).is_err());

        let mut enc = page.encode();
        // Announce two entries but provide one record (entry_count at [8..12]).
        enc[8] = 0;
        enc[9] = 0;
        enc[10] = 0;
        enc[11] = 2;
        assert!(IndexPage::decode(&enc).is_err());
    }

    #[test]
    fn page_rejects_unknown_kind_and_flags() {
        let page = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 0,
            body: PageBody::Leaf(vec![]),
        };
        let mut enc = page.encode();
        enc[4] = 9;
        assert!(matches!(
            IndexPage::decode(&enc),
            Err(WireError::UnsupportedFormat(_))
        ));
        let mut enc = page.encode();
        enc[6] = 1; // flags
        assert!(IndexPage::decode(&enc).is_err());
    }

    #[test]
    fn page_rejects_oversized_raw_page_and_entries() {
        // A >1 MiB raw page is rejected before parsing (WIRE-009).
        let big = vec![0u8; MAX_RAW_PAGE + 1];
        assert!(matches!(
            IndexPage::decode(&big),
            Err(WireError::LimitExceeded(_))
        ));

        let page = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 0,
            body: PageBody::Leaf(vec![LeafEntry {
                key: vec![0u8; MAX_INDEX_KEY + 1],
                value: b"v".to_vec(),
            }]),
        };
        assert!(IndexPage::decode(&page.encode()).is_err());

        let page = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 0,
            body: PageBody::Leaf(vec![LeafEntry {
                key: b"k".to_vec(),
                value: vec![0u8; MAX_INDEX_VALUE + 1],
            }]),
        };
        assert!(IndexPage::decode(&page.encode()).is_err());
    }

    #[test]
    fn empty_leaf_page_is_valid() {
        let page = IndexPage {
            kind: BnpgKind::GenericKeyValue,
            level: 0,
            body: PageBody::Leaf(vec![]),
        };
        assert_eq!(IndexPage::decode(&page.encode()).unwrap(), page);
        assert_eq!(page.encode().len(), PAGE_PREFIX_LEN);
    }
}
