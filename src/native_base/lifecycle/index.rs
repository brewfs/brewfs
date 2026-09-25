//! Authenticated object inventories used by manifests and RetainBatch roots.
//!
//! Both are type-5 `.brfin` containers whose BNPG leaves map raw ObjectId
//! bytes to canonical ObjectRef encodings.  The container itself is not an
//! entry in its own tree; its RootRef in an authority record is the
//! non-cyclic retention edge.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::native_base::wire::container::{
    Codec, ContainerFooter, ContainerHeader, FOOTER_LEN, HEADER_LEN, ObjectKind, parse_footer,
};
use crate::native_base::wire::index_build::{IndexTreeParams, build_index_tree};
use crate::native_base::wire::page::{BnpgKind, IndexPage, LeafEntry, PageBody};
use crate::native_base::wire::refs::{
    ChildRef, Hash32, ObjectRef, PageAddress, PageKind, RootRef, ensure_child_descends,
};
use crate::native_base::wire::uvarint::{Reader, Writer};

use super::{LifecycleError, LifecycleResult};

pub const DEFAULT_INVENTORY_LEAF_TARGET: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltObjectIndex {
    pub bytes: Vec<u8>,
    pub root: RootRef,
    pub entries: Vec<ObjectRef>,
}

fn encode_object(object: &ObjectRef) -> Vec<u8> {
    let mut w = Writer::new();
    object.encode_into(&mut w);
    w.into_bytes()
}

fn canonical_entries(objects: &[ObjectRef], self_id: &[u8; 16]) -> LifecycleResult<Vec<ObjectRef>> {
    let mut by_id = BTreeMap::new();
    for object in objects {
        if !(1..=6).contains(&object.kind) {
            return Err(LifecycleError::Retention(format!(
                "ObjectId {:02x?} has unsupported object kind {}",
                object.object_id, object.kind
            )));
        }
        crate::native_base::wire::refs::validate_object_key(&object.key)?;
        if &object.object_id == self_id {
            return Err(LifecycleError::Retention(
                "an inventory/RetainBatch index cannot contain itself".into(),
            ));
        }
        match by_id.insert(object.object_id, object.clone()) {
            Some(previous) if previous != *object => {
                return Err(LifecycleError::Retention(format!(
                    "ObjectId {:02x?} has two different ObjectRefs",
                    object.object_id
                )));
            }
            _ => {}
        }
    }
    if by_id.is_empty() {
        return Err(LifecycleError::Retention(
            "object index must contain at least one object".into(),
        ));
    }
    Ok(by_id.into_values().collect())
}

/// Build a deterministic type-5 authenticated ObjectId -> ObjectRef index.
pub fn build_object_index(
    objects: &[ObjectRef],
    object_id: [u8; 16],
    key: Vec<u8>,
    leaf_target: usize,
) -> LifecycleResult<BuiltObjectIndex> {
    let entries = canonical_entries(objects, &object_id)?;
    let encoded: Vec<(Vec<u8>, Vec<u8>)> = entries
        .iter()
        .map(|object| (object.object_id.to_vec(), encode_object(object)))
        .collect();
    let params = IndexTreeParams {
        leaf_target: leaf_target.max(64),
        leaf_kind: BnpgKind::GenericKeyValue,
        page_kind: PageKind::InventoryIndex,
        page_codec: Codec::None,
    };
    let mut body = Vec::new();
    let child = build_index_tree(&encoded, &params, &mut body)?;
    let ChildRef::Local(address) = child else {
        unreachable!("the local builder never creates an external root")
    };
    while !body.len().is_multiple_of(8) {
        body.push(0);
    }
    let object_len = HEADER_LEN + body.len() + FOOTER_LEN;
    let header = ContainerHeader {
        kind: ObjectKind::PagedInventory,
        required_features: 0,
        object_len: object_len as u64,
        root_offset: address.offset,
        root_stored_len: address.stored_len,
        root_raw_len: address.raw_len,
        hash_id: 1,
        root_codec: address.codec,
    };
    let footer = ContainerFooter {
        object_len: object_len as u64,
        root_stored_digest: address.stored_digest,
    };
    let mut bytes = Vec::with_capacity(object_len);
    bytes.extend_from_slice(&header.encode());
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&footer.encode());
    let object = ObjectRef {
        object_id,
        kind: ObjectKind::PagedInventory.as_u8(),
        object_len: object_len as u64,
        full_hash: Sha256::digest(&bytes).into(),
        key,
    };
    crate::native_base::wire::refs::validate_object_key(&object.key)?;
    Ok(BuiltObjectIndex {
        bytes,
        root: RootRef { object, address },
        entries,
    })
}

/// A built type-3 single-value control record container: the complete object
/// bytes plus the [`RootRef`] that an authority record must reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltSingleValue {
    pub bytes: Vec<u8>,
    pub root: RootRef,
}

/// Build the canonical type-3 single-value container for one control record.
///
/// Layout: 64-byte header, one level-0 `GenericKeyValue` leaf holding exactly
/// one entry (fixed empty key, `record_bytes` as the value) at offset 64, then
/// the 64-byte footer.  The encoding is a pure function of
/// `(object_id, key, record_bytes)`, so any party can recompute the expected
/// `full_hash`/`stored_digest` and compare a published copy against the
/// authoritative record instead of trusting the copy.
pub fn build_single_value_record(
    object_id: [u8; 16],
    key: Vec<u8>,
    record_bytes: Vec<u8>,
) -> LifecycleResult<BuiltSingleValue> {
    let page = IndexPage {
        kind: BnpgKind::GenericKeyValue,
        level: 0,
        body: PageBody::Leaf(vec![LeafEntry {
            key: Vec::new(),
            value: record_bytes,
        }]),
    };
    let raw = page.encode();
    let stored = raw.clone();
    let root_offset = HEADER_LEN as u64;
    let object_len = HEADER_LEN + stored.len() + FOOTER_LEN;
    let header = ContainerHeader {
        kind: ObjectKind::FrozenMetadata,
        required_features: 0,
        object_len: object_len as u64,
        root_offset,
        root_stored_len: stored.len() as u32,
        root_raw_len: raw.len() as u32,
        hash_id: 1,
        root_codec: Codec::None,
    };
    let address = PageAddress {
        offset: root_offset,
        stored_len: stored.len() as u32,
        raw_len: raw.len() as u32,
        codec: Codec::None,
        page_kind: PageKind::GenericKeyValue,
        level: 0,
        entry_count: 1,
        stored_digest: Sha256::digest(&stored).into(),
    };
    let footer = ContainerFooter {
        object_len: object_len as u64,
        root_stored_digest: address.stored_digest,
    };
    let mut bytes = Vec::with_capacity(object_len);
    bytes.extend_from_slice(&header.encode());
    bytes.extend_from_slice(&stored);
    bytes.extend_from_slice(&footer.encode());
    let object = ObjectRef {
        object_id,
        kind: ObjectKind::FrozenMetadata.as_u8(),
        object_len: object_len as u64,
        full_hash: Sha256::digest(&bytes).into(),
        key,
    };
    crate::native_base::wire::refs::validate_object_key(&object.key)?;
    Ok(BuiltSingleValue {
        bytes,
        root: RootRef { object, address },
    })
}

fn page_bytes<'a>(bytes: &'a [u8], address: &PageAddress) -> LifecycleResult<&'a [u8]> {
    if address.page_kind != PageKind::InventoryIndex || address.codec != Codec::None {
        return Err(LifecycleError::Record(
            "inventory page must be uncompressed page_kind=4".into(),
        ));
    }
    if address.offset < HEADER_LEN as u64 || !address.offset.is_multiple_of(8) {
        return Err(LifecycleError::Record(
            "inventory page offset is outside/aligned before the header".into(),
        ));
    }
    if address.raw_len != address.stored_len {
        return Err(LifecycleError::Record(
            "uncompressed inventory page raw/stored lengths differ".into(),
        ));
    }
    let start = usize::try_from(address.offset)
        .map_err(|_| LifecycleError::Record("inventory page offset overflow".into()))?;
    let end = start
        .checked_add(address.stored_len as usize)
        .ok_or_else(|| LifecycleError::Record("inventory page range overflow".into()))?;
    if end > bytes.len().saturating_sub(FOOTER_LEN) {
        return Err(LifecycleError::Record(
            "inventory page crosses the footer".into(),
        ));
    }
    let raw = &bytes[start..end];
    let digest: Hash32 = Sha256::digest(raw).into();
    if digest != address.stored_digest {
        return Err(LifecycleError::Record(
            "inventory page digest mismatch".into(),
        ));
    }
    Ok(raw)
}

type WalkResult = (Vec<u8>, Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>);

fn walk(
    bytes: &[u8],
    address: &PageAddress,
    seen: &mut BTreeSet<(u64, u64)>,
) -> LifecycleResult<WalkResult> {
    let end = address
        .offset
        .checked_add(u64::from(address.stored_len))
        .ok_or_else(|| LifecycleError::Record("inventory page range overflow".into()))?;
    if seen
        .iter()
        .any(|(other_start, other_end)| address.offset < *other_end && *other_start < end)
        || !seen.insert((address.offset, end))
    {
        return Err(LifecycleError::Record(
            "inventory tree reuses or overlaps a physical page".into(),
        ));
    }
    let page = IndexPage::decode(page_bytes(bytes, address)?)?;
    if page.kind != BnpgKind::GenericKeyValue
        || page.level != address.level
        || page.entry_count() != address.entry_count
    {
        return Err(LifecycleError::Record(
            "inventory PageAddress disagrees with BNPG prefix".into(),
        ));
    }
    match page.body {
        PageBody::Leaf(entries) => {
            let first = entries
                .first()
                .ok_or_else(|| LifecycleError::Record("empty inventory leaf".into()))?
                .key
                .clone();
            let last = entries.last().unwrap().key.clone();
            Ok((
                first,
                last,
                entries.into_iter().map(|e| (e.key, e.value)).collect(),
            ))
        }
        PageBody::Internal(entries) => {
            let first = entries
                .first()
                .ok_or_else(|| LifecycleError::Record("empty inventory internal page".into()))?
                .min_key
                .clone();
            let last = entries.last().unwrap().max_key.clone();
            let mut all = Vec::new();
            for entry in entries {
                let ChildRef::Local(child) = entry.child else {
                    return Err(LifecycleError::Record(
                        "object inventory cannot escape to an external container".into(),
                    ));
                };
                ensure_child_descends(address.level, child.level)?;
                if child.offset >= address.offset {
                    return Err(LifecycleError::Record(
                        "inventory child was not written before its parent".into(),
                    ));
                }
                let (actual_min, actual_max, mut child_entries) = walk(bytes, &child, seen)?;
                if actual_min != entry.min_key || actual_max != entry.max_key {
                    return Err(LifecycleError::Record(
                        "inventory child range does not match parent declaration".into(),
                    ));
                }
                all.append(&mut child_entries);
            }
            Ok((first, last, all))
        }
    }
}

/// Verify the complete object, every reachable page and every ObjectRef.
pub fn open_object_index(root: &RootRef, bytes: &[u8]) -> LifecycleResult<Vec<ObjectRef>> {
    if root.object.kind != ObjectKind::PagedInventory.as_u8()
        || root.object.object_len != bytes.len() as u64
    {
        return Err(LifecycleError::Record(
            "object index ObjectRef kind/length mismatch".into(),
        ));
    }
    let full_hash: Hash32 = Sha256::digest(bytes).into();
    if full_hash != root.object.full_hash {
        return Err(LifecycleError::Record(
            "object index full-object hash mismatch".into(),
        ));
    }
    let header = ContainerHeader::parse(bytes)?;
    header.ensure_supported_features()?;
    let footer = parse_footer(bytes)?;
    if header.kind != ObjectKind::PagedInventory
        || header.required_features != 0
        || header.object_len != bytes.len() as u64
        || header.root_offset != root.address.offset
        || header.root_stored_len != root.address.stored_len
        || header.root_raw_len != root.address.raw_len
        || header.root_codec != root.address.codec
        || footer.root_stored_digest != root.address.stored_digest
    {
        return Err(LifecycleError::Record(
            "object index header/footer/root disagreement".into(),
        ));
    }
    let (_, _, raw_entries) = walk(bytes, &root.address, &mut BTreeSet::new())?;
    let mut objects = Vec::with_capacity(raw_entries.len());
    let mut previous: Option<Vec<u8>> = None;
    for (key, value) in raw_entries {
        if key.len() != 16 || previous.as_ref().is_some_and(|p| p >= &key) {
            return Err(LifecycleError::Record(
                "object index keys are not unique sorted 16-byte ObjectIds".into(),
            ));
        }
        let mut reader = Reader::new(&value);
        let object = ObjectRef::decode(&mut reader)?;
        if !reader.is_empty() || object.object_id.as_slice() != key.as_slice() {
            return Err(LifecycleError::Record(
                "object index key/value identity mismatch".into(),
            ));
        }
        if object.object_id == root.object.object_id {
            return Err(LifecycleError::Retention(
                "object index contains its own ObjectRef".into(),
            ));
        }
        previous = Some(key);
        objects.push(object);
    }
    Ok(objects)
}
