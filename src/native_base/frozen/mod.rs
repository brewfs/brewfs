//! Frozen metadata primitives (PR08/PR09).
//!
//! The P2 format is deliberately separate from the P1 KV head.  This module
//! owns the canonical inode/dentry/extent rows, authenticated snapshot
//! manifest, bounded directory cursor, and the prefix-scoped extent lookup
//! rule.  Mount admission does not enable it unless the persisted volume
//! header explicitly requires `frozen-base-metadata`.

pub mod budget;
pub mod catalog;
pub mod producer;
pub mod readonly;

pub use budget::{BudgetError, BudgetReservation, MetadataBudget, MetadataBudgetSnapshot};

use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use sha2::{Digest, Sha256};

use crate::native_base::seal::source::{ObjectSource, ObjectSourceError};
use crate::native_base::wire::container::{
    Codec, ContainerHeader, FOOTER_LEN, HEADER_LEN, ObjectKind, parse_footer,
};
use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::page::{IndexPage, PageBody};
use crate::native_base::wire::refs::{
    ChildRef, MAX_INDEX_LEVEL, ObjectId, PageKind, RootRef, ensure_object_kind_allows_page,
};
use crate::native_base::wire::uvarint::{Reader, Writer};

pub const MANIFEST_MAGIC: &[u8; 4] = b"BNSM";
pub const MANIFEST_VERSION: u16 = 1;
pub const MAX_MANIFEST_RAW: usize = 1024 * 1024;
pub const MAX_DIRECTORY_COOKIE_SPOOL: usize = 64 * 1024 * 1024;
/// Hard cap on the number of live directory continuation cookies.  The cookie
/// table is the only directory state that must stay resident while the entry
/// spool is evictable, so it is bounded on its own (IDX-005).
pub const MAX_DIRECTORY_COOKIES: usize = 1 << 20;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenInodeRecord {
    pub kind: u8,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u64,
    pub nlink: u64,
    pub size: u64,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub parent_hint: Option<u64>,
    pub symlink_target: Option<Vec<u8>>,
}

impl FrozenInodeRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u8(self.kind);
        w.u32(self.mode);
        w.u32(self.uid);
        w.u32(self.gid);
        w.u64(self.rdev);
        w.u64(self.nlink);
        w.u64(self.size);
        w.i64(self.atime_ns);
        w.i64(self.mtime_ns);
        w.i64(self.ctime_ns);
        match self.parent_hint {
            Some(parent) => {
                w.u8(1);
                w.u64(parent);
            }
            None => w.u8(0),
        }
        match &self.symlink_target {
            Some(target) => {
                w.u8(1);
                w.bytes(target);
            }
            None => w.u8(0),
        }
        w.into_bytes()
    }

    pub fn decode(bytes: &[u8]) -> WireResult<Self> {
        let mut r = Reader::new(bytes);
        let kind = r.u8("frozen inode")?;
        if !(1..=7).contains(&kind) {
            return Err(WireError::invalid("frozen inode", "unknown inode kind"));
        }
        let record = Self {
            kind,
            mode: r.u32("frozen inode")?,
            uid: r.u32("frozen inode")?,
            gid: r.u32("frozen inode")?,
            rdev: r.u64("frozen inode")?,
            nlink: r.u64("frozen inode")?,
            size: r.u64("frozen inode")?,
            atime_ns: r.i64("frozen inode")?,
            mtime_ns: r.i64("frozen inode")?,
            ctime_ns: r.i64("frozen inode")?,
            parent_hint: match r.u8("frozen inode")? {
                0 => None,
                1 => Some(r.u64("frozen inode")?),
                _ => return Err(WireError::invalid("frozen inode", "invalid parent tag")),
            },
            symlink_target: match r.u8("frozen inode")? {
                0 => None,
                1 => Some(r.bytes("frozen inode")?.to_vec()),
                _ => return Err(WireError::invalid("frozen inode", "invalid target tag")),
            },
        };
        if !r.is_empty() {
            return Err(WireError::invalid("frozen inode", "trailing bytes"));
        }
        if record.kind != 3 && record.symlink_target.is_some() {
            return Err(WireError::invalid(
                "frozen inode",
                "only symlinks may carry a target",
            ));
        }
        let expected_mode_type = match record.kind {
            1 => 0o100000,
            2 => 0o040000,
            3 => 0o120000,
            4 => 0o010000,
            5 => 0o140000,
            6 => 0o020000,
            7 => 0o060000,
            _ => unreachable!(),
        };
        if record.mode & 0o170000 != expected_mode_type {
            return Err(WireError::invalid(
                "frozen inode",
                "inode kind and mode type disagree",
            ));
        }
        Ok(record)
    }
}

/// Canonical attributes of an inode live either inline in the namespace row or
/// in an external `FrozenMetadata` object addressed by a [`ValueRef`].
/// Relocating them is only allowed to be a *move*: the bytes stay exactly
/// canonical, so `logical_revision` cannot change for a pure relocation.
///
/// The tag cannot collide with an inline record because a `FrozenInodeRecord`
/// starts with an inode kind in `1..=7` (IDX-003).
pub const EXTERNAL_ATTRIBUTE_TAG: u8 = 0xff;

/// A reference to `stored_len` canonical attribute bytes of an object at
/// `[offset, offset + stored_len)`.  The bytes are authenticated by the
/// enclosing page chain; this reference only names them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValueRef {
    pub object_id: ObjectId,
    pub offset: u64,
    pub stored_len: u32,
}

impl ValueRef {
    pub const ENCODED_LEN: usize = 1 + 16 + 8 + 4;

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u8(EXTERNAL_ATTRIBUTE_TAG);
        w.put(&self.object_id);
        w.u64(self.offset);
        w.u32(self.stored_len);
        w.into_bytes()
    }

    /// Decode a namespace row value that carries an external reference.
    /// Untagged canonical attribute bytes are deliberately *not* accepted
    /// here: the caller has to decide which shape it is looking at.
    pub fn decode(bytes: &[u8]) -> WireResult<Self> {
        let what = "frozen inode value ref";
        let mut r = Reader::new(bytes);
        if r.u8(what)? != EXTERNAL_ATTRIBUTE_TAG {
            return Err(WireError::invalid(what, "missing external attribute tag"));
        }
        let object_id: ObjectId = r.take(16, what)?.try_into().unwrap();
        let offset = r.u64(what)?;
        let stored_len = r.u32(what)?;
        if !r.is_empty() {
            return Err(WireError::invalid(what, "trailing bytes"));
        }
        Ok(Self {
            object_id,
            offset,
            stored_len,
        })
    }

    pub fn end(&self) -> u64 {
        self.offset.saturating_add(u64::from(self.stored_len))
    }
}

/// The canonical inline encoding of an inode record.  This is also the shape
/// namespace rows already use, so untouched revisions stay byte-identical.
pub fn encode_inline_attributes(record: &FrozenInodeRecord) -> Vec<u8> {
    record.encode()
}

/// Decode *exactly* canonical attribute bytes.  A payload that decodes to a
/// record but does not re-encode to the same bytes is refused instead of being
/// normalized, because normalizing it would silently move the revision the
/// caller claimed to be reading.
pub fn decode_canonical_attributes(bytes: &[u8]) -> WireResult<FrozenInodeRecord> {
    let record = FrozenInodeRecord::decode(bytes)?;
    if record.encode() != bytes {
        return Err(WireError::invalid(
            "frozen inode",
            "attribute bytes are not canonical",
        ));
    }
    Ok(record)
}

/// A namespace row value: the canonical inline bytes, or a `ValueRef` to them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttributePlacement {
    Inline(Vec<u8>),
    External(ValueRef),
}

impl AttributePlacement {
    pub fn encode_row(&self) -> Vec<u8> {
        match self {
            Self::Inline(bytes) => bytes.clone(),
            Self::External(reference) => reference.encode(),
        }
    }

    pub fn decode_row(bytes: &[u8]) -> WireResult<Self> {
        if bytes.first().copied() == Some(EXTERNAL_ATTRIBUTE_TAG) {
            return Ok(Self::External(ValueRef::decode(bytes)?));
        }
        decode_canonical_attributes(bytes)?;
        Ok(Self::Inline(bytes.to_vec()))
    }
}

/// Move canonical inline attribute bytes into an external object without
/// changing a single byte.  The caller stores the returned payload verbatim.
pub fn relocate_attributes(
    record: &FrozenInodeRecord,
    object_id: ObjectId,
    offset: u64,
) -> (AttributePlacement, Vec<u8>) {
    let canonical = encode_inline_attributes(record);
    let reference = ValueRef {
        object_id,
        offset,
        stored_len: canonical.len() as u32,
    };
    (AttributePlacement::External(reference), canonical)
}

/// Fail-closed proof that a relocation kept the revision: byte-identical
/// payloads mean the revision digest cannot move.
pub fn ensure_relocation_keeps_revision(inline: &[u8], external: &[u8]) -> WireResult<()> {
    if inline != external {
        return Err(WireError::invalid(
            "frozen inode",
            "relocated attributes are not byte-identical to the inline canonical encoding",
        ));
    }
    Ok(())
}

/// Read the canonical attributes of an external placement.
pub fn resolve_external_attributes<S: ObjectSource>(
    source: &S,
    reference: &ValueRef,
) -> Result<FrozenInodeRecord, FrozenReadError> {
    let bytes = source.get_range(&reference.object_id, reference.offset, reference.end())?;
    if bytes.len() != reference.stored_len as usize {
        return Err(WireError::invalid(
            "frozen inode value ref",
            "external attribute length mismatch",
        )
        .into());
    }
    Ok(decode_canonical_attributes(&bytes)?)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenRow {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl FrozenRow {
    fn encode_into(&self, w: &mut Writer) {
        w.bytes(&self.key);
        w.bytes(&self.value);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotManifest {
    pub volume_id: [u8; 16],
    pub storage_namespace_id: [u8; 16],
    pub chunk_size: u64,
    pub block_size: u32,
    pub required_features: u64,
    pub logical_revision: [u8; 32],
    pub namespace_digest: [u8; 32],
    pub binding_digest: [u8; 32],
    pub namespace_mode: u8,
    pub kv_layer_id: Option<[u8; 16]>,
    pub kv_sealed_version: Option<u64>,
    pub namespace_root: Option<RootRef>,
    pub data_root: RootRef,
    pub inventory_root: RootRef,
    pub file_count: u64,
    pub directory_count: u64,
    pub total_logical_bytes: u64,
    pub created_at_ns: i64,
}

impl SnapshotManifest {
    pub fn encode(&self) -> WireResult<Vec<u8>> {
        if self.namespace_mode != 1 && self.namespace_mode != 2 {
            return Err(WireError::invalid(
                "snapshot manifest",
                "unknown namespace mode",
            ));
        }
        if self.namespace_mode == 1
            && (self.kv_layer_id.is_none()
                || self.kv_sealed_version.is_none()
                || self.namespace_root.is_some())
        {
            return Err(WireError::invalid(
                "snapshot manifest",
                "invalid KV namespace refs",
            ));
        }
        if self.namespace_mode == 2
            && (self.namespace_root.is_none()
                || self.kv_layer_id.is_some()
                || self.kv_sealed_version.is_some())
        {
            return Err(WireError::invalid(
                "snapshot manifest",
                "invalid frozen namespace refs",
            ));
        }
        let mut w = Writer::new();
        w.put(MANIFEST_MAGIC);
        w.u16(MANIFEST_VERSION);
        w.u16(0);
        w.put(&self.volume_id);
        w.put(&self.storage_namespace_id);
        w.u64(self.chunk_size);
        w.u32(self.block_size);
        w.u64(self.required_features);
        w.put(&self.logical_revision);
        w.put(&self.namespace_digest);
        w.put(&self.binding_digest);
        w.u8(self.namespace_mode);
        match self.namespace_mode {
            1 => {
                w.put(&self.kv_layer_id.unwrap());
                w.u64(self.kv_sealed_version.unwrap());
            }
            2 => self.namespace_root.as_ref().unwrap().encode_into(&mut w),
            _ => unreachable!(),
        }
        self.data_root.encode_into(&mut w);
        self.inventory_root.encode_into(&mut w);
        w.u64(self.file_count);
        w.u64(self.directory_count);
        w.u64(self.total_logical_bytes);
        w.i64(self.created_at_ns);
        let bytes = w.into_bytes();
        if bytes.len() > MAX_MANIFEST_RAW {
            return Err(WireError::LimitExceeded(
                "snapshot manifest exceeds 1 MiB".into(),
            ));
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> WireResult<Self> {
        if bytes.len() > MAX_MANIFEST_RAW {
            return Err(WireError::LimitExceeded(
                "snapshot manifest exceeds 1 MiB".into(),
            ));
        }
        let mut r = Reader::new(bytes);
        if r.take(4, "snapshot manifest")? != MANIFEST_MAGIC {
            return Err(WireError::invalid("snapshot manifest", "magic mismatch"));
        }
        if r.u16("snapshot manifest")? != MANIFEST_VERSION {
            return Err(WireError::invalid(
                "snapshot manifest",
                "unsupported version",
            ));
        }
        if r.u16("snapshot manifest")? != 0 {
            return Err(WireError::invalid(
                "snapshot manifest",
                "reserved field not zero",
            ));
        }
        let volume_id = r.take(16, "snapshot manifest")?.try_into().unwrap();
        let storage_namespace_id = r.take(16, "snapshot manifest")?.try_into().unwrap();
        let chunk_size = r.u64("snapshot manifest")?;
        let block_size = r.u32("snapshot manifest")?;
        let required_features = r.u64("snapshot manifest")?;
        let logical_revision = r.take(32, "snapshot manifest")?.try_into().unwrap();
        let namespace_digest = r.take(32, "snapshot manifest")?.try_into().unwrap();
        let binding_digest = r.take(32, "snapshot manifest")?.try_into().unwrap();
        let namespace_mode = r.u8("snapshot manifest")?;
        let (kv_layer_id, kv_sealed_version, namespace_root) = match namespace_mode {
            1 => (
                Some(r.take(16, "snapshot manifest")?.try_into().unwrap()),
                Some(r.u64("snapshot manifest")?),
                None,
            ),
            2 => (None, None, Some(RootRef::decode(&mut r)?)),
            _ => {
                return Err(WireError::invalid(
                    "snapshot manifest",
                    "unknown namespace mode",
                ));
            }
        };
        let data_root = RootRef::decode(&mut r)?;
        let inventory_root = RootRef::decode(&mut r)?;
        let result = Self {
            volume_id,
            storage_namespace_id,
            chunk_size,
            block_size,
            required_features,
            logical_revision,
            namespace_digest,
            binding_digest,
            namespace_mode,
            kv_layer_id,
            kv_sealed_version,
            namespace_root,
            data_root,
            inventory_root,
            file_count: r.u64("snapshot manifest")?,
            directory_count: r.u64("snapshot manifest")?,
            total_logical_bytes: r.u64("snapshot manifest")?,
            created_at_ns: r.i64("snapshot manifest")?,
        };
        if !r.is_empty() {
            return Err(WireError::invalid("snapshot manifest", "trailing bytes"));
        }
        Ok(result)
    }
}

pub fn canonical_table_digest(table_id: u16, rows: &[FrozenRow]) -> [u8; 32] {
    let mut sorted = rows.to_vec();
    sorted.sort_by(|a, b| a.key.cmp(&b.key));
    let mut w = Writer::new();
    w.u16(table_id);
    w.u64(sorted.len() as u64);
    for row in sorted {
        row.encode_into(&mut w);
    }
    Sha256::digest(w.as_slice()).into()
}

/// Stable key prefixes used by the P2 namespace/data tables.  Prefixes are
/// part of the table contract so a lower-bound lookup cannot cross from one
/// inode, chunk, or dentry family into another.
pub fn inode_key(inode: u64) -> Vec<u8> {
    let mut key = b"i".to_vec();
    key.extend_from_slice(&inode.to_be_bytes());
    key
}

pub fn dentry_prefix(parent: u64) -> Vec<u8> {
    let mut key = b"d".to_vec();
    key.extend_from_slice(&parent.to_be_bytes());
    key.push(0);
    key
}

pub fn dentry_key(parent: u64, name: &[u8]) -> Vec<u8> {
    let mut key = dentry_prefix(parent);
    key.extend_from_slice(name);
    key
}

pub fn extent_prefix(inode: u64, chunk_index: u64) -> Vec<u8> {
    let mut key = b"e".to_vec();
    key.extend_from_slice(&inode.to_be_bytes());
    key.extend_from_slice(&chunk_index.to_be_bytes());
    key
}

pub fn extent_key(inode: u64, chunk_index: u64, offset: u64) -> Vec<u8> {
    let mut key = extent_prefix(inode, chunk_index);
    key.extend_from_slice(&offset.to_be_bytes());
    key
}

/// Canonical data-table value for an extent that can be served by the normal
/// block reader. The first field preserves the extent length decoder's
/// predecessor lookup contract; the remaining fields identify the immutable
/// loose slice referenced by the data path.
pub fn encode_extent_slice_value(
    length: u64,
    slice_id: u64,
    chunk_id: u64,
    offset: u64,
) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.u64(length);
    writer.u64(slice_id);
    writer.u64(chunk_id);
    writer.u64(offset);
    writer.into_bytes()
}

/// Decode the canonical extent value used by the packed data table.
///
/// All scalar fields use the wire module's little-endian encoding. Keeping
/// this decoder strict prevents a snapshot from silently turning a malformed
/// row into a different block address during a read-only mount.
pub fn decode_extent_slice_value(value: &[u8]) -> WireResult<(u64, u64, u64, u64)> {
    let mut reader = Reader::new(value);
    let decoded = (
        reader.u64("frozen extent")?,
        reader.u64("frozen extent")?,
        reader.u64("frozen extent")?,
        reader.u64("frozen extent")?,
    );
    if !reader.is_empty() {
        return Err(WireError::invalid("frozen extent", "trailing bytes"));
    }
    Ok(decoded)
}

/// `BE32(ordinal)` key of the frozen inventory ordinal map.  Big-endian so
/// numeric ordinal order equals the byte order of the inventory pages.
pub fn inventory_ordinal_key(ordinal: u32) -> [u8; 4] {
    ordinal.to_be_bytes()
}

/// One decoded inventory row: which pack object an ordinal resolves into.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InventoryOrdinal {
    pub ordinal: u32,
    pub pack: ObjectId,
    pub offset: u64,
    pub stored_len: u32,
}

/// Outcome of a repack-time ordinal map prune (IDX-004).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InventoryPrunePlan {
    /// Rows of the repacked pack that stay verbatim.  A pinned row is kept even
    /// when it is not live: dropping it would have to reinterpret its slot.
    pub retained: Vec<InventoryOrdinal>,
    /// `BE32(ordinal)` keys of this pack whose mapping is dead and must be
    /// removed from the inventory page.  Dead mappings are deleted, never
    /// re-pointed at a new slot.
    pub pruned_keys: Vec<[u8; 4]>,
    /// Rows owned by another pack.  They are neither pruned nor reported as
    /// this pack's own mappings, so a repack cannot absorb a foreign slot.
    pub foreign: Vec<InventoryOrdinal>,
}

/// Prune a sparse ordinal map after a repack.
///
/// `old` is the map as it exists *before* the repack, in strictly ascending
/// ordinal order; `pinned` ordinals must survive untouched and `live` ordinals
/// are the ones the repacked layout still references.  Both sets are validated
/// against the old map first: an ordinal that is not mapped inside `pack`
/// cannot be retained, because accepting it would either reinterpret an
/// existing slot or steal another pack's row.
pub fn plan_inventory_prune(
    old: &[InventoryOrdinal],
    pack: &ObjectId,
    pinned: &BTreeSet<u32>,
    live: &BTreeSet<u32>,
) -> WireResult<InventoryPrunePlan> {
    for pair in old.windows(2) {
        if pair[0].ordinal >= pair[1].ordinal {
            return Err(WireError::invalid(
                "frozen inventory ordinal map",
                format!(
                    "ordinals must strictly ascend, got {} then {}",
                    pair[0].ordinal, pair[1].ordinal
                ),
            ));
        }
    }
    for ordinal in pinned.iter().chain(live.iter()) {
        let Some(row) = old.iter().find(|row| row.ordinal == *ordinal) else {
            return Err(WireError::invalid(
                "frozen inventory ordinal map",
                format!("ordinal {ordinal} is not mapped and cannot be retained"),
            ));
        };
        if &row.pack != pack {
            return Err(WireError::invalid(
                "frozen inventory ordinal map",
                format!("ordinal {ordinal} maps into another pack"),
            ));
        }
    }
    let mut plan = InventoryPrunePlan::default();
    for row in old {
        if &row.pack != pack {
            plan.foreign.push(*row);
        } else if pinned.contains(&row.ordinal) || live.contains(&row.ordinal) {
            plan.retained.push(*row);
        } else {
            plan.pruned_keys.push(inventory_ordinal_key(row.ordinal));
        }
    }
    Ok(plan)
}

#[derive(Debug, thiserror::Error)]
pub enum FrozenReadError {
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(transparent)]
    Source(#[from] ObjectSourceError),
    #[error("frozen metadata object kind is not FrozenMetadata")]
    WrongObjectKind,
    #[error("frozen metadata page is outside its object")]
    PageOutOfBounds,
    #[error("frozen metadata index walk exceeded {0} levels")]
    IndexTooDeep(u8),
    #[error("frozen metadata page kind is not a generic key/value page")]
    WrongPageKind,
    #[error("fixed revision attempted a mutable metadata RPC")]
    MutableRpc,
    #[error("copy-on-write page reuse rejected: {0}")]
    CopyOnWrite(String),
}

type DecodedPageKey = (ObjectId, u64, u32, u32, u8, u8, u8, u32, [u8; 32]);

pub(super) fn decoded_page_key(
    object: &crate::native_base::wire::refs::ObjectRef,
    address: &crate::native_base::wire::refs::PageAddress,
) -> DecodedPageKey {
    (
        object.object_id,
        address.offset,
        address.stored_len,
        address.raw_len,
        address.codec.as_u8(),
        address.page_kind.as_u8(),
        address.level,
        address.entry_count,
        address.stored_digest,
    )
}

/// Shared decoded-page state for one immutable catalog.
///
/// The object bytes are already authenticated and resident at mount time, but
/// rebuilding a reader used to repeat header/range/hash/decode work for every
/// lookup.  Catalog readers share this cache; standalone fixed readers keep it
/// disabled so their deterministic read-count tests remain a true cold path.
#[derive(Default)]
pub(super) struct ReaderPageCache {
    pages: RwLock<HashMap<DecodedPageKey, Arc<IndexPage>>>,
}

impl ReaderPageCache {
    fn get(&self, key: &DecodedPageKey) -> Option<IndexPage> {
        self.pages
            .read()
            .ok()?
            .get(key)
            .cloned()
            .map(|page| (*page).clone())
    }

    /// Borrow the immutable decoded page without cloning its entry vectors.
    /// Streaming packed lookups usually need one leaf value or one child;
    /// cloning the whole page on every FUSE lookup defeats the page cache.
    pub(super) fn get_shared(&self, key: &DecodedPageKey) -> Option<Arc<IndexPage>> {
        self.pages.read().ok()?.get(key).cloned()
    }

    fn insert(&self, key: DecodedPageKey, page: IndexPage) {
        if let Ok(mut pages) = self.pages.write() {
            pages.insert(key, Arc::new(page));
        }
    }

    pub(super) fn keys(&self) -> Vec<DecodedPageKey> {
        self.pages
            .read()
            .map(|pages| pages.keys().copied().collect())
            .unwrap_or_default()
    }

    pub(super) fn remove(&self, key: &DecodedPageKey) -> bool {
        self.pages
            .write()
            .map(|mut pages| pages.remove(key).is_some())
            .unwrap_or(false)
    }
}

/// A fixed revision reader owns no mutable KV client.  Every lookup is served
/// from the authenticated manifest and its external pages; the counter is
/// intentionally exposed for tests and observability to prove that inode
/// lookups do not silently fall back to a KV RPC.
pub struct FixedRevisionReader<'a, S: ObjectSource> {
    source: &'a S,
    manifest: SnapshotManifest,
    page_cache: Option<Arc<ReaderPageCache>>,
    metadata_rpc_count: std::sync::atomic::AtomicU64,
    object_reads: std::sync::atomic::AtomicU64,
}

impl<'a, S: ObjectSource> FixedRevisionReader<'a, S> {
    pub fn open_manifest(source: &'a S, object: &[u8]) -> Result<Self, FrozenReadError> {
        let header = ContainerHeader::parse(object)?;
        if header.kind != ObjectKind::SnapshotManifest {
            return Err(FrozenReadError::WrongObjectKind);
        }
        header.ensure_supported_features()?;
        if header.object_len as usize != object.len() {
            return Err(WireError::invalid("snapshot manifest", "object length mismatch").into());
        }
        let footer = parse_footer(object)?;
        let start = usize::try_from(header.root_offset)
            .map_err(|_| WireError::invalid("snapshot manifest", "root offset overflow"))?;
        let end = start
            .checked_add(header.root_stored_len as usize)
            .ok_or_else(|| WireError::invalid("snapshot manifest", "root range overflow"))?;
        if start < HEADER_LEN || end > object.len().saturating_sub(FOOTER_LEN) {
            return Err(FrozenReadError::PageOutOfBounds);
        }
        let stored = &object[start..end];
        if <[u8; 32]>::from(Sha256::digest(stored)) != footer.root_stored_digest {
            return Err(WireError::HashMismatch {
                what: "snapshot manifest root",
                stored: hex::encode(footer.root_stored_digest),
                computed: hex::encode(Sha256::digest(stored)),
            }
            .into());
        }
        let raw = match header.root_codec {
            Codec::None => {
                if header.root_raw_len != header.root_stored_len {
                    return Err(WireError::invalid(
                        "snapshot manifest",
                        "codec None length mismatch",
                    )
                    .into());
                }
                stored.to_vec()
            }
            Codec::Zstd => zstd::bulk::decompress(stored, header.root_raw_len as usize)
                .map_err(|error| WireError::Codec(error.to_string()))?,
        };
        Ok(Self {
            source,
            manifest: SnapshotManifest::decode(&raw)?,
            page_cache: None,
            metadata_rpc_count: std::sync::atomic::AtomicU64::new(0),
            object_reads: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub(super) fn from_decoded_manifest(
        source: &'a S,
        manifest: SnapshotManifest,
        page_cache: Arc<ReaderPageCache>,
    ) -> Self {
        Self {
            source,
            manifest,
            page_cache: Some(page_cache),
            metadata_rpc_count: std::sync::atomic::AtomicU64::new(0),
            object_reads: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    pub fn metadata_rpc_count(&self) -> u64 {
        self.metadata_rpc_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Authenticated pages this reader has consumed so far (RET-011).
    ///
    /// A fixed readonly baseline is *fixed*: the same logical lookups always
    /// load the same pages, so this counter is reproducible across runs and
    /// independent of source latency or cache warmth.  The reader has no KV
    /// and no GC/retention-lease client, so a baseline can never issue a
    /// write or a lease RPC.
    pub fn object_read_count(&self) -> u64 {
        self.object_reads.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn lookup_namespace(&self, key: &[u8]) -> Result<Option<Vec<u8>>, FrozenReadError> {
        self.lookup_root(self.manifest.namespace_root.as_ref(), key)
    }

    /// Return all namespace rows whose keys start with `prefix` in canonical
    /// byte order.  The walk stays inside the authenticated namespace tree;
    /// it never asks a mutable metadata service to enumerate the directory.
    ///
    /// This is the producer/read-only-facade bridge for directory enumeration.
    /// Callers must still apply the semantic prefix they own (for example
    /// [`dentry_prefix`]) because the index itself is a general key/value tree.
    pub fn scan_namespace_prefix(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FrozenReadError> {
        let Some(root) = self.manifest.namespace_root.as_ref() else {
            return Ok(Vec::new());
        };
        let mut rows = Vec::new();
        let mut current_object = root.object.clone();
        let mut child = ChildRef::External(root.clone());
        // Keep the internal path so that after exhausting one leaf we can
        // descend into the next sibling without another root lower-bound walk.
        let mut stack = Vec::new();

        loop {
            let (page, object) = self.read_page(&current_object, &child)?;
            match page.body {
                PageBody::Internal(entries) => {
                    let selected = entries
                        .iter()
                        .position(|entry| entry.max_key.as_slice() >= prefix)
                        .unwrap_or(entries.len().saturating_sub(1));
                    let next_child = entries
                        .get(selected)
                        .ok_or(FrozenReadError::PageOutOfBounds)?
                        .child
                        .clone();
                    current_object = object.clone();
                    stack.push((object, entries, selected));
                    child = next_child;
                }
                PageBody::Leaf(entries) => {
                    for entry in entries
                        .into_iter()
                        .skip_while(|entry| entry.key.as_slice() < prefix)
                    {
                        if !entry.key.starts_with(prefix) {
                            return Ok(rows);
                        }
                        rows.push((entry.key, entry.value));
                    }

                    let mut next = None;
                    while let Some((container, entries, selected)) = stack.last_mut() {
                        if *selected + 1 < entries.len() {
                            *selected += 1;
                            next = Some((container.clone(), entries[*selected].child.clone()));
                            break;
                        }
                        stack.pop();
                    }
                    let Some((container, next_child)) = next else {
                        break;
                    };
                    current_object = container;
                    child = next_child;
                }
            }
        }
        Ok(rows)
    }

    pub fn lookup_data(&self, key: &[u8]) -> Result<Option<Vec<u8>>, FrozenReadError> {
        self.lookup_root(Some(&self.manifest.data_root), key)
    }

    pub fn lookup_inventory(&self, key: &[u8]) -> Result<Option<Vec<u8>>, FrozenReadError> {
        self.lookup_root(Some(&self.manifest.inventory_root), key)
    }

    pub fn lookup_inode(&self, inode: u64) -> Result<Option<FrozenInodeRecord>, FrozenReadError> {
        self.resolve_inode_attributes(inode)
    }

    /// Resolve an inode's attributes from the namespace tree, accepting both
    /// the inline canonical bytes and an external [`ValueRef`] (IDX-003).  The
    /// external form is reached through the same authenticated page chain and
    /// must still decode to exactly canonical bytes, so moving attributes out
    /// of the row is invisible to the revision.
    pub fn resolve_inode_attributes(
        &self,
        inode: u64,
    ) -> Result<Option<FrozenInodeRecord>, FrozenReadError> {
        let Some(bytes) = self.lookup_namespace(&inode_key(inode))? else {
            return Ok(None);
        };
        match AttributePlacement::decode_row(&bytes)? {
            AttributePlacement::Inline(bytes) => Ok(Some(decode_canonical_attributes(&bytes)?)),
            AttributePlacement::External(reference) => {
                Ok(Some(resolve_external_attributes(self.source, &reference)?))
            }
        }
    }

    pub fn lookup_dentry(
        &self,
        parent: u64,
        name: &[u8],
    ) -> Result<Option<Vec<u8>>, FrozenReadError> {
        self.lookup_namespace(&dentry_key(parent, name))
    }

    /// Return the predecessor plus successors in one inode/chunk prefix.  A
    /// page boundary never changes the prefix check, so a neighboring record
    /// from another inode cannot become a false extent predecessor.
    pub fn query_extent_rows(
        &self,
        inode: u64,
        chunk_index: u64,
        start: u64,
        end: u64,
    ) -> Result<Vec<FrozenExtent>, FrozenReadError> {
        let prefix = extent_prefix(inode, chunk_index);
        let mut rows = Vec::new();
        let mut cursor = prefix.clone();
        while let Some((key, value)) = self.lower_bound_data(&cursor)? {
            if !key.starts_with(&prefix) {
                break;
            }
            if key.len() != prefix.len() + 8 {
                return Err(WireError::invalid("frozen extent key", "invalid key length").into());
            }
            let offset = u64::from_be_bytes(key[prefix.len()..].try_into().unwrap());
            let (length, _, _, _) = decode_extent_slice_value(&value)?;
            if length != 0 && offset < end && offset.saturating_add(length) > start {
                rows.push(FrozenExtent {
                    inode,
                    chunk_index,
                    offset,
                    length,
                    value,
                });
            }
            cursor = increment_key(&key)?;
        }
        Ok(rows)
    }

    fn lookup_root(
        &self,
        root: Option<&RootRef>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, FrozenReadError> {
        let Some(root) = root else {
            return Ok(None);
        };
        let mut child = ChildRef::External(root.clone());
        let mut current_object = root.object.clone();
        for depth in 0..=MAX_INDEX_LEVEL {
            let (page, object) = self.read_page(&current_object, &child)?;
            current_object = object;
            match page.body {
                PageBody::Leaf(entries) => {
                    let at = entries.partition_point(|entry| entry.key.as_slice() < key);
                    return Ok(entries
                        .get(at)
                        .filter(|entry| entry.key.as_slice() == key)
                        .map(|entry| entry.value.clone()));
                }
                PageBody::Internal(entries) => {
                    child = entries
                        .into_iter()
                        .find(|entry| {
                            entry.min_key.as_slice() <= key && key <= entry.max_key.as_slice()
                        })
                        .map(|entry| entry.child)
                        .unwrap_or_else(|| ChildRef::External(root.clone()));
                    if matches!(child, ChildRef::External(ref fallback) if fallback == root) {
                        return Ok(None);
                    }
                }
            }
            if depth == MAX_INDEX_LEVEL {
                return Err(FrozenReadError::IndexTooDeep(MAX_INDEX_LEVEL));
            }
        }
        unreachable!()
    }

    fn read_page(
        &self,
        current_object: &crate::native_base::wire::refs::ObjectRef,
        child: &ChildRef,
    ) -> Result<(IndexPage, crate::native_base::wire::refs::ObjectRef), FrozenReadError> {
        let (object, address) = match child {
            ChildRef::External(root) => (&root.object, root.address),
            ChildRef::Local(address) => (current_object, *address),
        };
        let cache_key = decoded_page_key(object, &address);
        if let Some(page) = self
            .page_cache
            .as_ref()
            .and_then(|cache| cache.get(&cache_key))
        {
            self.object_reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok((page, object.clone()));
        }
        let header_bytes = self
            .source
            .get_range(&object.object_id, 0, HEADER_LEN as u64)?;
        let kind = ObjectKind::from_magic(&header_bytes[..8])?;
        ensure_object_kind_allows_page(kind, address.page_kind)?;
        if kind != ObjectKind::FrozenMetadata || address.page_kind != PageKind::GenericKeyValue {
            return Err(FrozenReadError::WrongPageKind);
        }
        let end = address
            .offset
            .checked_add(address.stored_len as u64)
            .ok_or(FrozenReadError::PageOutOfBounds)?;
        if end > object.object_len {
            return Err(FrozenReadError::PageOutOfBounds);
        }
        let stored = self
            .source
            .get_range(&object.object_id, address.offset, end)?;
        if stored.len() != address.stored_len as usize
            || <[u8; 32]>::from(Sha256::digest(&stored)) != address.stored_digest
        {
            return Err(WireError::HashMismatch {
                what: "frozen metadata page",
                stored: hex::encode(address.stored_digest),
                computed: hex::encode(Sha256::digest(&stored)),
            }
            .into());
        }
        let raw = match address.codec {
            Codec::None => stored,
            Codec::Zstd => zstd::bulk::decompress(&stored, address.raw_len as usize)
                .map_err(|error| WireError::Codec(error.to_string()))?,
        };
        if address.page_kind != PageKind::GenericKeyValue {
            return Err(FrozenReadError::WrongPageKind);
        }
        let page = IndexPage::decode(&raw)?;
        if page.level != address.level || page.entry_count() != address.entry_count {
            return Err(WireError::invalid("frozen metadata page", "address mismatch").into());
        }
        if let Some(cache) = &self.page_cache {
            cache.insert(cache_key, page.clone());
        }
        self.object_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok((page, object.clone()))
    }

    fn lower_bound_data(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>, FrozenReadError> {
        self.lower_bound_root(&self.manifest.data_root, key)
    }

    fn lower_bound_root(
        &self,
        root: &RootRef,
        key: &[u8],
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, FrozenReadError> {
        let mut child = ChildRef::External(root.clone());
        let mut current_object = root.object.clone();
        for _ in 0..=MAX_INDEX_LEVEL {
            let (page, object) = self.read_page(&current_object, &child)?;
            current_object = object;
            match page.body {
                PageBody::Leaf(entries) => {
                    let at = entries.partition_point(|entry| entry.key.as_slice() < key);
                    return Ok(entries
                        .get(at)
                        .map(|entry| (entry.key.clone(), entry.value.clone())));
                }
                PageBody::Internal(entries) => {
                    let mut selected = entries
                        .iter()
                        .find(|entry| entry.max_key.as_slice() >= key)
                        .map(|entry| entry.child.clone());
                    if selected.is_none() {
                        selected = entries.last().map(|entry| entry.child.clone());
                    }
                    child = selected.ok_or(FrozenReadError::PageOutOfBounds)?;
                }
            }
        }
        Err(FrozenReadError::IndexTooDeep(MAX_INDEX_LEVEL))
    }
}

fn increment_key(key: &[u8]) -> Result<Vec<u8>, FrozenReadError> {
    let mut result = key.to_vec();
    for byte in result.iter_mut().rev() {
        if *byte != 0xff {
            *byte += 1;
            return Ok(result);
        }
        *byte = 0;
    }
    Err(FrozenReadError::PageOutOfBounds)
}

fn decode_extent_length(value: &[u8]) -> Result<u64, FrozenReadError> {
    if value.len() != 8 {
        return Err(WireError::invalid("frozen extent", "length probe requires 8 bytes").into());
    }
    let mut reader = Reader::new(value);
    let length = reader.u64("frozen extent")?;
    if !reader.is_empty() {
        return Err(WireError::invalid("frozen extent", "trailing bytes").into());
    }
    Ok(length)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtentQuery {
    pub inode: u64,
    pub chunk_index: u64,
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenExtent {
    pub inode: u64,
    pub chunk_index: u64,
    pub offset: u64,
    pub length: u64,
    pub value: Vec<u8>,
}

/// Resolve the predecessor plus all successors, restricted to one inode/chunk
/// prefix.  A lower-bound-only lookup is deliberately not sufficient: an
/// extent beginning before the requested range may cover its first bytes.
pub fn query_extents(rows: &[FrozenExtent], query: ExtentQuery) -> Vec<FrozenExtent> {
    let mut selected = rows
        .iter()
        .filter(|row| {
            row.inode == query.inode
                && row.chunk_index == query.chunk_index
                && row.length != 0
                && row.offset < query.end
                && row.offset.saturating_add(row.length) > query.start
        })
        .cloned()
        .collect::<Vec<_>>();
    selected.sort_by_key(|row| row.offset);
    selected
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryEntry {
    pub name: Vec<u8>,
    pub inode: u64,
    pub kind: u8,
}

/// The resume point of a continuation cookie: the last entry that was handed
/// to the caller.  Anchoring a cookie on an entry instead of on a RAM index is
/// what lets the entry spool be evicted and replayed without changing the
/// continuation the cookie promises (IDX-005).
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct DirectoryAnchor {
    pub name: Vec<u8>,
    pub inode: u64,
}

#[derive(Debug)]
pub struct DirectoryCursor {
    entries: Vec<DirectoryEntry>,
    anchors: BTreeMap<u64, DirectoryAnchor>,
    cookie_by_anchor: BTreeMap<DirectoryAnchor, u64>,
    next_cookie: u64,
    max_cookies: usize,
}

impl DirectoryCursor {
    pub fn new(entries: Vec<DirectoryEntry>, max_spool_bytes: usize) -> WireResult<Self> {
        Self::with_cookie_budget(entries, max_spool_bytes, MAX_DIRECTORY_COOKIES)
    }

    /// A cursor whose cookie table is bounded independently of the spool.  A
    /// page that would have to issue a *new* cookie beyond the budget fails
    /// before any cookie is returned, so a caller can never be handed a cookie
    /// the cursor cannot replay.
    pub fn with_cookie_budget(
        entries: Vec<DirectoryEntry>,
        max_spool_bytes: usize,
        max_cookies: usize,
    ) -> WireResult<Self> {
        let mut cursor = Self {
            entries: Vec::new(),
            anchors: BTreeMap::new(),
            cookie_by_anchor: BTreeMap::new(),
            next_cookie: 1,
            max_cookies,
        };
        cursor.respool(entries, max_spool_bytes)?;
        Ok(cursor)
    }

    /// Whether the entry payloads are still resident in RAM.
    pub fn entries_resident(&self) -> bool {
        !self.entries.is_empty()
    }

    pub fn resident_entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Live continuation cookies.  The anchor table is the only directory
    /// state that survives eviction.
    pub fn cookie_count(&self) -> usize {
        self.anchors.len()
    }

    /// Drop the entry payloads and return the bytes that were freed.  Issued
    /// cookies stay resolvable against a later [`Self::respool`] of the same
    /// directory.
    pub fn evict_entries(&mut self) -> usize {
        let freed: usize = self.entries.iter().map(|entry| entry.name.len() + 24).sum();
        self.entries = Vec::new();
        freed
    }

    /// Replace the entry spool.  Cookie numbering and issued anchors survive,
    /// so a cookie issued before an eviction resumes at the same successor
    /// entry after the replay; if its anchor entry is gone it fails instead of
    /// resuming at a guessed position.
    pub fn respool(
        &mut self,
        mut entries: Vec<DirectoryEntry>,
        max_spool_bytes: usize,
    ) -> WireResult<()> {
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        if entries.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err(WireError::invalid("directory", "duplicate names"));
        }
        let spool_bytes: usize = entries.iter().map(|entry| entry.name.len() + 24).sum();
        if spool_bytes > max_spool_bytes || spool_bytes > MAX_DIRECTORY_COOKIE_SPOOL {
            return Err(WireError::LimitExceeded(
                "directory cookie spool budget".into(),
            ));
        }
        self.entries = entries;
        Ok(())
    }

    pub fn page(&mut self, after: u64, limit: usize) -> WireResult<(u64, Vec<DirectoryEntry>)> {
        if limit == 0 {
            return Err(WireError::invalid(
                "directory cursor",
                "page limit must be non-zero",
            ));
        }
        let start = if after == 0 {
            0
        } else {
            let anchor = self
                .anchors
                .get(&after)
                .ok_or_else(|| WireError::invalid("directory cursor", "unknown cookie"))?
                .clone();
            let found = self
                .entries
                .binary_search_by(|entry| entry.name.as_slice().cmp(anchor.name.as_slice()))
                .map_err(|_| {
                    WireError::invalid("directory cursor", "cookie anchor is not in the spool")
                })?;
            if self.entries[found].inode != anchor.inode {
                return Err(WireError::invalid(
                    "directory cursor",
                    "cookie anchor was replaced by another inode",
                ));
            }
            found + 1
        };
        let end = start.saturating_add(limit).min(self.entries.len());
        let page = self.entries[start..end].to_vec();
        if end == self.entries.len() {
            return Ok((0, page));
        }
        let anchor = DirectoryAnchor {
            name: self.entries[end - 1].name.clone(),
            inode: self.entries[end - 1].inode,
        };
        let cookie = match self.cookie_by_anchor.get(&anchor).copied() {
            Some(cookie) => cookie,
            None => {
                if self.anchors.len() >= self.max_cookies {
                    return Err(WireError::LimitExceeded(
                        "directory cookie table budget".into(),
                    ));
                }
                let cookie = self.next_cookie;
                self.next_cookie = self
                    .next_cookie
                    .checked_add(1)
                    .ok_or_else(|| WireError::LimitExceeded("directory cookie overflow".into()))?;
                self.anchors.insert(cookie, anchor.clone());
                self.cookie_by_anchor.insert(anchor, cookie);
                cookie
            }
        };
        Ok((cookie, page))
    }
}

/// A bounded attribute cache in front of a fixed revision (FROZEN-005).
///
/// Attributes are the cheapest thing to recompute and the first thing a
/// memory budget evicts, so eviction is explicit and observable: a cold inode
/// is re-read from the authenticated pages and must come back identical,
/// including `mode`/`uid`/`gid` and therefore its permissions.
pub struct FrozenAttributeCache {
    capacity: usize,
    entries: BTreeMap<u64, FrozenInodeRecord>,
    /// Least-recently-used order; the front is the coldest entry.
    order: Vec<u64>,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl FrozenAttributeCache {
    /// `capacity == 0` disables caching without changing any answer.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: BTreeMap::new(),
            order: Vec::new(),
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn hits(&self) -> u64 {
        self.hits
    }

    pub fn misses(&self) -> u64 {
        self.misses
    }

    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    pub fn is_resident(&self, inode: u64) -> bool {
        self.entries.contains_key(&inode)
    }

    /// Serve one inode's attributes from RAM, reloading and re-inserting them
    /// when they were evicted.  A missing inode stays missing: the reload path
    /// can never invent an attribute record.
    pub fn attributes_of<S: ObjectSource>(
        &mut self,
        reader: &FixedRevisionReader<'_, S>,
        inode: u64,
    ) -> Result<Option<FrozenInodeRecord>, FrozenReadError> {
        if let Some(record) = self.entries.get(&inode).cloned() {
            self.hits += 1;
            self.touch(inode);
            return Ok(Some(record));
        }
        self.misses += 1;
        let Some(record) = reader.lookup_inode(inode)? else {
            return Ok(None);
        };
        self.insert(inode, record.clone());
        Ok(Some(record))
    }

    fn touch(&mut self, inode: u64) {
        if let Some(position) = self.order.iter().position(|resident| *resident == inode) {
            self.order.remove(position);
        }
        self.order.push(inode);
    }

    fn insert(&mut self, inode: u64, record: FrozenInodeRecord) {
        if self.capacity == 0 {
            return;
        }
        self.entries.insert(inode, record);
        self.touch(inode);
        while self.entries.len() > self.capacity {
            let cold = self.order.remove(0);
            self.entries.remove(&cold);
            self.evictions += 1;
        }
    }
}

/// One page a snapshot addresses: the container object plus the byte range
/// inside it (FROZEN-006).
///
/// Two snapshots can share a slot only when both the container and the range
/// are identical, which is exactly what makes a copy-on-write metadata rewrite
/// safe for the older snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageSlot {
    pub container: ObjectId,
    pub offset: u64,
    pub stored_len: u32,
}

impl PageSlot {
    pub fn end(&self) -> u64 {
        self.offset.saturating_add(u64::from(self.stored_len))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PageReusePlan {
    /// Slots the new snapshot addresses that already exist untouched.
    pub reused: Vec<PageSlot>,
    /// Slots the new snapshot needs that must be written into fresh ranges.
    pub must_write: Vec<PageSlot>,
    /// Slots only the old snapshot still addresses.  They stay readable — and
    /// therefore must not be overwritten — while the old revision is
    /// referenced.
    pub unreferenced: Vec<PageSlot>,
}

impl PageReusePlan {
    /// Every slot the new snapshot addresses is either reused or written, so
    /// the new page closure is complete.
    pub fn covers_new_snapshot(&self, new_pages: &BTreeSet<PageSlot>) -> bool {
        let covered: BTreeSet<PageSlot> = self
            .reused
            .iter()
            .chain(self.must_write.iter())
            .copied()
            .collect();
        covered == *new_pages
    }
}

/// Split the pages a copy-on-write rewrite reuses from the pages it must write
/// (FROZEN-006).  Nothing here mutates the old snapshot: its pages are only
/// reported as still-shared or unreferenced.
pub fn plan_page_reuse(
    old_pages: &BTreeSet<PageSlot>,
    new_pages: &BTreeSet<PageSlot>,
) -> PageReusePlan {
    PageReusePlan {
        reused: new_pages.intersection(old_pages).copied().collect(),
        must_write: new_pages.difference(old_pages).copied().collect(),
        unreferenced: old_pages.difference(new_pages).copied().collect(),
    }
}

/// Refuse an in-place page overwrite (FROZEN-006).
///
/// A page the old snapshot still addresses may not be rewritten at the same
/// slot: the new content would silently change what the old revision reads.
/// The rewrite must go to a fresh range instead.
pub fn ensure_page_overwrite_is_safe(
    old_pages: &BTreeSet<PageSlot>,
    target: &PageSlot,
) -> Result<(), FrozenReadError> {
    if old_pages.contains(target) {
        return Err(FrozenReadError::CopyOnWrite(format!(
            "page {}..{} of container {:02x?} is still referenced by the old snapshot",
            target.offset,
            target.end(),
            target.container
        )));
    }
    Ok(())
}

/// Separate accounting for a summary metadata scan and the part it rewrote
/// (FROZEN-008).  Scan volume and rewrite volume are reported independently so
/// a rewrite can never hide inside the scan total.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetaScanCounters {
    pub scanned_pages: u64,
    pub scanned_bytes: u64,
    pub rewritten_pages: u64,
    pub rewritten_bytes: u64,
}

/// Account a summary metadata scan that rewrites part of what it read.
///
/// Every rewritten page must have been scanned first, and each page
/// contributes its stored length to exactly one of the two byte counters.
pub fn account_meta_scan(
    scanned: &[PageSlot],
    rewritten: &[PageSlot],
) -> Result<MetaScanCounters, FrozenReadError> {
    let scanned_set: BTreeSet<PageSlot> = scanned.iter().copied().collect();
    let rewritten_set: BTreeSet<PageSlot> = rewritten.iter().copied().collect();
    if let Some(unscanned) = rewritten_set.difference(&scanned_set).next() {
        return Err(FrozenReadError::CopyOnWrite(format!(
            "rewrite of page {}..{} was never scanned",
            unscanned.offset,
            unscanned.end()
        )));
    }
    let sum = |pages: &BTreeSet<PageSlot>| -> Result<u64, FrozenReadError> {
        pages.iter().try_fold(0u64, |total, page| {
            total
                .checked_add(u64::from(page.stored_len))
                .ok_or_else(|| FrozenReadError::CopyOnWrite("meta scan byte sum overflow".into()))
        })
    };
    Ok(MetaScanCounters {
        scanned_pages: scanned_set.len() as u64,
        scanned_bytes: sum(&scanned_set)?,
        rewritten_pages: rewritten_set.len() as u64,
        rewritten_bytes: sum(&rewritten_set)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_base::seal::source::{ObjectSource, ObjectSourceError};
    use crate::native_base::wire::container::{Codec, ObjectKind};
    use crate::native_base::wire::container::{ContainerFooter, FOOTER_LEN, HEADER_LEN};
    use crate::native_base::wire::index_build::{IndexTreeParams, build_index_tree};
    use crate::native_base::wire::refs::{ObjectRef, PageAddress, PageKind};
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;

    /// Simple in-memory object source for frozen reader tests.
    struct MemoryObjectSource {
        objects: HashMap<[u8; 16], Vec<u8>>,
    }

    impl MemoryObjectSource {
        fn new() -> Self {
            Self {
                objects: HashMap::new(),
            }
        }
        fn insert(&mut self, id: [u8; 16], data: Vec<u8>) {
            self.objects.insert(id, data);
        }
    }

    impl ObjectSource for MemoryObjectSource {
        fn get_range(
            &self,
            object_id: &[u8; 16],
            start: u64,
            end: u64,
        ) -> Result<Vec<u8>, ObjectSourceError> {
            let bytes = self
                .objects
                .get(object_id)
                .ok_or(ObjectSourceError::NotFound)?;
            let len = bytes.len() as u64;
            if end > len || start > end {
                return Err(ObjectSourceError::ShortRead {
                    requested: end - start,
                    received: len.saturating_sub(start),
                });
            }
            Ok(bytes[start as usize..end as usize].to_vec())
        }
    }

    fn root(seed: u8) -> RootRef {
        RootRef {
            object: ObjectRef {
                object_id: [seed; 16],
                kind: ObjectKind::FrozenMetadata.as_u8(),
                object_len: 128,
                full_hash: [seed; 32],
                key: format!("frozen/{seed}").into_bytes(),
            },
            address: PageAddress {
                offset: 0,
                stored_len: 1,
                raw_len: 1,
                codec: Codec::None,
                page_kind: PageKind::TableRootDirectory,
                level: 0,
                entry_count: 1,
                stored_digest: [seed; 32],
            },
        }
    }

    #[test]
    fn manifest_roundtrip_and_mode_shape_are_fail_closed() {
        let manifest = SnapshotManifest {
            volume_id: [1; 16],
            storage_namespace_id: [2; 16],
            chunk_size: 64,
            block_size: 16,
            required_features: 0,
            logical_revision: [3; 32],
            namespace_digest: [4; 32],
            binding_digest: [5; 32],
            namespace_mode: 1,
            kv_layer_id: Some([6; 16]),
            kv_sealed_version: Some(7),
            namespace_root: None,
            data_root: root(8),
            inventory_root: root(9),
            file_count: 10,
            directory_count: 11,
            total_logical_bytes: 12,
            created_at_ns: 13,
        };
        assert_eq!(
            SnapshotManifest::decode(&manifest.encode().unwrap()).unwrap(),
            manifest
        );
        let mut bad = manifest.clone();
        bad.namespace_mode = 2;
        assert!(bad.encode().is_err());
    }

    #[test]
    fn predecessor_query_is_prefix_scoped() {
        let rows = vec![
            FrozenExtent {
                inode: 1,
                chunk_index: 0,
                offset: 0,
                length: 8192,
                value: b"a".to_vec(),
            },
            FrozenExtent {
                inode: 1,
                chunk_index: 0,
                offset: 8192,
                length: 8192,
                value: b"b".to_vec(),
            },
            FrozenExtent {
                inode: 1,
                chunk_index: 1,
                offset: 0,
                length: 8192,
                value: b"wrong-chunk".to_vec(),
            },
            FrozenExtent {
                inode: 2,
                chunk_index: 0,
                offset: 0,
                length: 8192,
                value: b"wrong-inode".to_vec(),
            },
        ];
        let found = query_extents(
            &rows,
            ExtentQuery {
                inode: 1,
                chunk_index: 0,
                start: 4096,
                end: 4100,
            },
        );
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].value, b"a");
    }

    #[test]
    fn extent_slice_value_uses_canonical_wire_endianness() {
        let value = encode_extent_slice_value(
            0x0102_0304_0506_0708,
            0x1112_1314_1516_1718,
            0x2122_2324_2526_2728,
            0x3132_3334_3536_3738,
        );
        assert_eq!(value.len(), 32);
        assert_eq!(value[..8], 0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(value[8..16], 0x1112_1314_1516_1718u64.to_le_bytes());
        assert_eq!(
            decode_extent_length(&value[..8]).unwrap(),
            0x0102_0304_0506_0708
        );
        assert!(decode_extent_length(&value).is_err());
    }

    #[test]
    fn directory_cookie_survives_page_replay() {
        let mut cursor = DirectoryCursor::new(
            vec![
                DirectoryEntry {
                    name: b"b".to_vec(),
                    inode: 2,
                    kind: 1,
                },
                DirectoryEntry {
                    name: b"a".to_vec(),
                    inode: 1,
                    kind: 1,
                },
                DirectoryEntry {
                    name: b"c".to_vec(),
                    inode: 1,
                    kind: 1,
                },
            ],
            1024,
        )
        .unwrap();
        assert!(cursor.page(0, 0).is_err());
        let (cookie, first) = cursor.page(0, 2).unwrap();
        assert_eq!(first[0].name, b"a");
        for _ in 0..1024 {
            assert_eq!(cursor.page(0, 2).unwrap().0, cookie);
        }
        assert_eq!(cursor.next_cookie, cookie + 1);
        let (last, second) = cursor.page(cookie, 2).unwrap();
        assert_eq!(last, 0);
        assert_eq!(second[0].name, b"c");
        assert!(cursor.page(cookie + 100, 1).is_err());
    }

    #[test]
    fn one_million_directory_entries_page_in_order() {
        let entries: Vec<DirectoryEntry> = (0..1_000_000u32)
            .map(|entry_index| DirectoryEntry {
                name: entry_index.to_be_bytes().to_vec(),
                inode: u64::from(entry_index) + 1,
                kind: 1,
            })
            .collect();
        let mut cursor = DirectoryCursor::new(entries, MAX_DIRECTORY_COOKIE_SPOOL).unwrap();
        let mut first_name = Vec::new();
        let mut last_name = Vec::new();
        let mut observed_count = 0usize;
        let mut cookie = 0u64;
        loop {
            let (next_cookie, page) = cursor.page(cookie, 16_384).unwrap();
            if cookie == 0
                && let Some(first) = page.first()
            {
                first_name = first.name.clone();
            }
            if let Some(last) = page.last() {
                last_name = last.name.clone();
            }
            observed_count += page.len();
            cookie = next_cookie;
            if cookie == 0 {
                break;
            }
        }

        assert_eq!(observed_count, 1_000_000);
        assert_eq!(first_name, 0u32.to_be_bytes());
        assert_eq!(last_name, 999_999u32.to_be_bytes());
        // Cookie zero means "restart from the beginning"; only an unknown
        // non-zero cookie must be rejected after the final page.
        assert!(cursor.page(u64::MAX, 1).is_err());
    }

    #[test]
    fn non_utf8_dentry_names_preserve_original_bytes() {
        let mut cursor = DirectoryCursor::new(
            vec![
                DirectoryEntry {
                    name: vec![b'a', 0xff, b'b'],
                    inode: 1,
                    kind: 1,
                },
                DirectoryEntry {
                    name: vec![b'a', 0xfe, b'c'],
                    inode: 2,
                    kind: 1,
                },
                DirectoryEntry {
                    name: b"plain".to_vec(),
                    inode: 3,
                    kind: 1,
                },
            ],
            1024,
        )
        .unwrap();
        let (cookie, first) = cursor.page(0, 2).unwrap();
        assert_eq!(first[0].name, vec![b'a', 0xfe, b'c']);
        assert_eq!(first[1].name, vec![b'a', 0xff, b'b']);
        let (_, second) = cursor.page(cookie, 2).unwrap();
        assert_eq!(second[0].name, b"plain");
    }

    #[test]
    fn hardlinked_names_across_pages_share_one_inode_record() {
        let inode_number = 777u64;
        let inode_record = FrozenInodeRecord {
            kind: 1,
            mode: 0o100_644,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            nlink: 2,
            size: 4096,
            atime_ns: 1,
            mtime_ns: 2,
            ctime_ns: 3,
            parent_hint: Some(1),
            symlink_target: None,
        };
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = (0..151u32)
            .map(|entry_index| {
                let name = format!("hardlink-{entry_index:03}");
                (dentry_key(1, name.as_bytes()), inode_key(inode_number))
            })
            .collect();
        entries.push((inode_key(inode_number), inode_record.encode()));

        let namespace_id = [13u8; 16];
        let (namespace_obj, namespace_root) =
            build_frozen_metadata_object(namespace_id, b"frozen/hardlinks", &entries);
        let inv_id = [14u8; 16];
        let (inv_obj, inv_root) = build_frozen_metadata_object(
            inv_id,
            b"frozen/inventory",
            &[(b"__inv_sentinel".to_vec(), vec![])],
        );
        let manifest = SnapshotManifest {
            volume_id: [1; 16],
            storage_namespace_id: [2; 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [15; 32],
            namespace_digest: [16; 32],
            binding_digest: [17; 32],
            namespace_mode: 2,
            kv_layer_id: None,
            kv_sealed_version: None,
            namespace_root: Some(namespace_root.clone()),
            data_root: inv_root.clone(),
            inventory_root: inv_root,
            file_count: 1,
            directory_count: 1,
            total_logical_bytes: 4096,
            created_at_ns: 0,
        };
        let manifest_id = [19u8; 16];
        let manifest_obj = build_manifest_object(manifest_id, b"frozen/manifest", &manifest);

        let mut source = MemoryObjectSource::new();
        source.insert(namespace_id, namespace_obj);
        source.insert(inv_id, inv_obj);
        source.insert(manifest_id, manifest_obj.clone());
        let reader = FixedRevisionReader::open_manifest(&source, &manifest_obj).unwrap();

        let loaded = reader.lookup_inode(inode_number).unwrap().unwrap();
        assert_eq!(loaded, inode_record);
        for entry_index in [0u32, 75, 150] {
            let name = format!("hardlink-{entry_index:03}");
            let got = reader.lookup_dentry(1, name.as_bytes()).unwrap().unwrap();
            assert_eq!(got, inode_key(inode_number));
        }
        assert!(reader.lookup_dentry(1, b"hardlink-151").unwrap().is_none());
    }

    #[test]
    fn directory_rename_keeps_inode_and_descendant_identity() {
        let directory_inode = 42u64;
        let child_inode = 43u64;
        let directory_record = FrozenInodeRecord {
            kind: 2,
            mode: 0o040_755,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            nlink: 2,
            size: 0,
            atime_ns: 1,
            mtime_ns: 2,
            ctime_ns: 3,
            parent_hint: Some(1),
            symlink_target: None,
        };
        let child_record = FrozenInodeRecord {
            kind: 1,
            mode: 0o100_644,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            nlink: 1,
            size: 128,
            atime_ns: 4,
            mtime_ns: 5,
            ctime_ns: 6,
            parent_hint: Some(directory_inode),
            symlink_target: None,
        };
        // This is the post-rename snapshot: the old dentry is absent, while
        // the replacement dentry and the descendant still use stable inode
        // keys and records.
        let entries = vec![
            (dentry_key(1, b"after"), inode_key(directory_inode)),
            (dentry_key(directory_inode, b"leaf"), inode_key(child_inode)),
            (inode_key(directory_inode), directory_record.encode()),
            (inode_key(child_inode), child_record.encode()),
        ];

        let namespace_id = [20u8; 16];
        let (namespace_obj, namespace_root) =
            build_frozen_metadata_object(namespace_id, b"frozen/rename", &entries);
        let inventory_id = [21u8; 16];
        let (inventory_obj, inventory_root) = build_frozen_metadata_object(
            inventory_id,
            b"frozen/inventory-rename",
            &[(b"__inv_sentinel".to_vec(), vec![])],
        );
        let manifest = SnapshotManifest {
            volume_id: [1; 16],
            storage_namespace_id: [2; 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [22; 32],
            namespace_digest: [23; 32],
            binding_digest: [24; 32],
            namespace_mode: 2,
            kv_layer_id: None,
            kv_sealed_version: None,
            namespace_root: Some(namespace_root),
            data_root: inventory_root.clone(),
            inventory_root,
            file_count: 1,
            directory_count: 2,
            total_logical_bytes: 128,
            created_at_ns: 0,
        };
        let manifest_id = [25u8; 16];
        let manifest_obj = build_manifest_object(manifest_id, b"frozen/manifest-rename", &manifest);

        let mut source = MemoryObjectSource::new();
        source.insert(namespace_id, namespace_obj);
        source.insert(inventory_id, inventory_obj);
        source.insert(manifest_id, manifest_obj.clone());
        let reader = FixedRevisionReader::open_manifest(&source, &manifest_obj).unwrap();

        assert!(reader.lookup_dentry(1, b"before").unwrap().is_none());
        assert_eq!(
            reader.lookup_dentry(1, b"after").unwrap(),
            Some(inode_key(directory_inode))
        );
        assert_eq!(
            reader.lookup_inode(directory_inode).unwrap(),
            Some(directory_record)
        );
        assert_eq!(
            reader.lookup_dentry(directory_inode, b"leaf").unwrap(),
            Some(inode_key(child_inode))
        );
        assert_eq!(
            reader.lookup_inode(child_inode).unwrap(),
            Some(child_record)
        );
    }

    #[test]
    fn partial_namespace_arrival_fails_closed() {
        let namespace_id = [26u8; 16];
        let (namespace_object, namespace_root) = build_frozen_metadata_object(
            namespace_id,
            b"frozen/partial-namespace",
            &[(dentry_key(1, b"entry"), inode_key(99))],
        );
        let inventory_id = [27u8; 16];
        let (inventory_object, inventory_root) = build_frozen_metadata_object(
            inventory_id,
            b"frozen/partial-inventory",
            &[(b"__inv_sentinel".to_vec(), vec![])],
        );
        let manifest = SnapshotManifest {
            volume_id: [1; 16],
            storage_namespace_id: [2; 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [28; 32],
            namespace_digest: [29; 32],
            binding_digest: [30; 32],
            namespace_mode: 2,
            kv_layer_id: None,
            kv_sealed_version: None,
            namespace_root: Some(namespace_root),
            data_root: inventory_root.clone(),
            inventory_root,
            file_count: 0,
            directory_count: 1,
            total_logical_bytes: 0,
            created_at_ns: 0,
        };
        let manifest_id = [31u8; 16];
        let manifest_obj =
            build_manifest_object(manifest_id, b"frozen/manifest-partial", &manifest);

        let mut source = MemoryObjectSource::new();
        // The namespace object is intentionally omitted. A manifest without
        // all referenced namespace bytes must never degrade to an empty tree.
        let _ = namespace_object;
        source.insert(inventory_id, inventory_object);
        source.insert(manifest_id, manifest_obj.clone());
        let reader = FixedRevisionReader::open_manifest(&source, &manifest_obj).unwrap();

        assert!(matches!(
            reader.lookup_dentry(1, b"entry"),
            Err(FrozenReadError::Source(ObjectSourceError::NotFound))
        ));
    }

    /// Build a complete FrozenMetadata container with a BNPG index over the
    /// provided entries.  Returns the raw object bytes and a RootRef pointing
    /// at the index root.
    fn build_frozen_metadata_object(
        object_id: [u8; 16],
        key: &[u8],
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> (Vec<u8>, RootRef) {
        let params = IndexTreeParams::generic(256); // small target → multi-page
        let mut body = Vec::new();
        let root_child = build_index_tree(entries, &params, &mut body).unwrap();
        let root_addr = match root_child {
            ChildRef::Local(addr) => addr,
            _ => panic!("expected local root"),
        };
        let root_stored =
            &body[root_addr.offset as usize - HEADER_LEN..][..root_addr.stored_len as usize];
        let root_digest: [u8; 32] = Sha256::digest(root_stored).into();
        let total_len = (HEADER_LEN + body.len() + FOOTER_LEN) as u64;
        let header = ContainerHeader {
            kind: ObjectKind::FrozenMetadata,
            required_features: 0,
            object_len: total_len,
            root_offset: root_addr.offset,
            root_stored_len: root_addr.stored_len,
            root_raw_len: root_addr.raw_len,
            hash_id: 1,
            root_codec: Codec::None,
        };
        let footer = ContainerFooter {
            object_len: total_len,
            root_stored_digest: root_digest,
        };
        let mut object = Vec::with_capacity(total_len as usize);
        object.extend_from_slice(&header.encode());
        object.extend_from_slice(&body);
        object.extend_from_slice(&footer.encode());
        let object_ref = ObjectRef {
            object_id,
            kind: ObjectKind::FrozenMetadata.as_u8(),
            object_len: object.len() as u64,
            full_hash: Sha256::digest(&object).into(),
            key: key.to_vec(),
        };
        (
            object,
            RootRef {
                object: object_ref,
                address: root_addr,
            },
        )
    }

    /// Build a SnapshotManifest container object.
    fn build_manifest_object(
        object_id: [u8; 16],
        key: &[u8],
        manifest: &SnapshotManifest,
    ) -> Vec<u8> {
        let raw = manifest.encode().unwrap();
        let stored_digest: [u8; 32] = Sha256::digest(&raw).into();
        let total_len = (HEADER_LEN + raw.len() + FOOTER_LEN) as u64;
        let header = ContainerHeader {
            kind: ObjectKind::SnapshotManifest,
            required_features: 0,
            object_len: total_len,
            root_offset: HEADER_LEN as u64,
            root_stored_len: raw.len() as u32,
            root_raw_len: raw.len() as u32,
            hash_id: 1,
            root_codec: Codec::None,
        };
        let footer = ContainerFooter {
            object_len: total_len,
            root_stored_digest: stored_digest,
        };
        let mut object = Vec::with_capacity(total_len as usize);
        object.extend_from_slice(&header.encode());
        object.extend_from_slice(&raw);
        object.extend_from_slice(&footer.encode());
        object
    }

    #[test]
    fn fixed_revision_reader_lookup_and_negative_proof() {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..64u32)
            .map(|i| {
                let mut key = b"key-".to_vec();
                key.extend_from_slice(&format!("{:04}", i).into_bytes());
                let mut val = b"val-".to_vec();
                val.extend_from_slice(&format!("{:04}", i).into_bytes());
                (key, val)
            })
            .collect();

        let data_id = [3u8; 16];
        let (data_obj, data_root) =
            build_frozen_metadata_object(data_id, b"frozen/data-001", &entries);

        let inv_id = [4u8; 16];
        let (inv_obj, inv_root) = build_frozen_metadata_object(
            inv_id,
            b"frozen/inventory",
            &[(b"__inv_sentinel".to_vec(), vec![])],
        );

        let manifest = SnapshotManifest {
            volume_id: [1; 16],
            storage_namespace_id: [2; 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [5; 32],
            namespace_digest: [6; 32],
            binding_digest: [7; 32],
            namespace_mode: 1,
            kv_layer_id: Some([8; 16]),
            kv_sealed_version: Some(42),
            namespace_root: None,
            data_root,
            inventory_root: inv_root,
            file_count: 64,
            directory_count: 1,
            total_logical_bytes: 64 * 64,
            created_at_ns: 1_000_000,
        };

        let manifest_id = [9u8; 16];
        let manifest_obj = build_manifest_object(manifest_id, b"frozen/manifest", &manifest);

        let mut source = MemoryObjectSource::new();
        source.insert(data_id, data_obj.clone());
        source.insert(inv_id, inv_obj.clone());
        source.insert(manifest_id, manifest_obj.clone());

        let reader = FixedRevisionReader::open_manifest(&source, &manifest_obj).unwrap();
        assert_eq!(reader.metadata_rpc_count(), 0);

        // Positive lookups.
        for i in [0, 1, 31, 32, 63] {
            let key = format!("key-{:04}", i);
            let expected = format!("val-{:04}", i);
            let got = reader.lookup_data(key.as_bytes()).unwrap();
            assert_eq!(got.as_deref(), Some(expected.as_bytes()));
        }
        assert_eq!(
            reader.metadata_rpc_count(),
            0,
            "zero KV RPC on fixed revision"
        );

        // Negative lookups must return None, not panic.
        assert!(reader.lookup_data(b"key-9999").unwrap().is_none());
        assert!(reader.lookup_data(b"zebra").unwrap().is_none());
        assert!(reader.lookup_data(b"").unwrap().is_none());
        assert_eq!(reader.metadata_rpc_count(), 0);
    }

    #[test]
    fn fixed_revision_reader_scans_only_authenticated_namespace_prefix() {
        let inode = FrozenInodeRecord {
            kind: 1,
            mode: 0o100_644,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            nlink: 1,
            size: 7,
            atime_ns: 1,
            mtime_ns: 2,
            ctime_ns: 3,
            parent_hint: Some(1),
            symlink_target: None,
        };
        let entries = vec![
            (dentry_key(1, b"a"), inode_key(7)),
            (dentry_key(1, b"b"), inode_key(8)),
            (dentry_key(2, b"hidden"), inode_key(9)),
            (inode_key(7), inode.encode()),
        ];
        let (namespace_obj, namespace_root) =
            build_frozen_metadata_object([31; 16], b"frozen/ns-scan", &entries);
        let (inventory_obj, inventory_root) = build_frozen_metadata_object(
            [32; 16],
            b"frozen/inventory-scan",
            &[(b"__inv_sentinel".to_vec(), vec![])],
        );
        let manifest = SnapshotManifest {
            volume_id: [33; 16],
            storage_namespace_id: [34; 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [35; 32],
            namespace_digest: [36; 32],
            binding_digest: [37; 32],
            namespace_mode: 2,
            kv_layer_id: None,
            kv_sealed_version: None,
            namespace_root: Some(namespace_root),
            data_root: inventory_root.clone(),
            inventory_root,
            file_count: 1,
            directory_count: 1,
            total_logical_bytes: 7,
            created_at_ns: 0,
        };
        let manifest_obj = build_manifest_object([38; 16], b"frozen/manifest-scan", &manifest);
        let mut source = MemoryObjectSource::new();
        source.insert([31; 16], namespace_obj);
        source.insert([32; 16], inventory_obj);
        source.insert([38; 16], manifest_obj.clone());
        let reader = FixedRevisionReader::open_manifest(&source, &manifest_obj).unwrap();

        let rows = reader.scan_namespace_prefix(&dentry_prefix(1)).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, dentry_key(1, b"a"));
        assert_eq!(rows[1].0, dentry_key(1, b"b"));
        assert!(
            reader
                .scan_namespace_prefix(&dentry_prefix(3))
                .unwrap()
                .is_empty()
        );
        assert_eq!(reader.metadata_rpc_count(), 0);
    }

    #[test]
    fn fixed_revision_reader_traverses_multi_level_index() {
        // 150 entries with a tiny leaf target forces ≥ 2 internal levels.
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..150u32)
            .map(|i| {
                let mut k = i.to_be_bytes().to_vec();
                let mut v = vec![0u8; 32];
                v[0..4].copy_from_slice(&i.to_be_bytes());
                (k, v)
            })
            .collect();

        let data_id = [5u8; 16];
        let (data_obj, data_root) =
            build_frozen_metadata_object(data_id, b"frozen/data-big", &entries);

        // Verify the tree actually has multiple levels.
        match &data_root.address {
            addr => assert!(
                addr.level >= 2,
                "expected multi-level tree, got level {}",
                addr.level
            ),
        }

        let inv_id = [6u8; 16];
        let (inv_obj, inv_root) = build_frozen_metadata_object(
            inv_id,
            b"frozen/inv",
            &[(b"__inv_sentinel".to_vec(), vec![])],
        );

        let manifest = SnapshotManifest {
            volume_id: [1; 16],
            storage_namespace_id: [2; 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [3; 32],
            namespace_digest: [4; 32],
            binding_digest: [5; 32],
            namespace_mode: 1,
            kv_layer_id: Some([6; 16]),
            kv_sealed_version: Some(1),
            namespace_root: None,
            data_root,
            inventory_root: inv_root,
            file_count: 150,
            directory_count: 0,
            total_logical_bytes: 150 * 32,
            created_at_ns: 0,
        };

        let manifest_id = [7u8; 16];
        let manifest_obj = build_manifest_object(manifest_id, b"frozen/manifest2", &manifest);

        let mut source = MemoryObjectSource::new();
        source.insert(data_id, data_obj.clone());
        source.insert(inv_id, inv_obj.clone());
        source.insert(manifest_id, manifest_obj.clone());

        let reader = FixedRevisionReader::open_manifest(&source, &manifest_obj).unwrap();

        // Every entry must be reachable through a multi-level walk.
        for i in [0u32, 1, 50, 75, 99, 100, 149] {
            let key = i.to_be_bytes().to_vec();
            let got = reader.lookup_data(&key).unwrap().unwrap();
            assert_eq!(&got[0..4], &i.to_be_bytes());
        }

        // Boundary keys just outside the range must be missing.
        assert!(
            reader
                .lookup_data(&u32::MAX.to_be_bytes())
                .unwrap()
                .is_none()
        );
        assert!(reader.lookup_data(&[0xFF; 8]).unwrap().is_none());
    }

    #[test]
    fn corrupted_frozen_page_fails_closed() {
        let entries = vec![(b"hello".to_vec(), b"world".to_vec())];
        let data_id = [10u8; 16];
        let (mut data_obj, data_root) =
            build_frozen_metadata_object(data_id, b"frozen/corrupt", &entries);
        let inv_id = [11u8; 16];
        let (inv_obj, inv_root) = build_frozen_metadata_object(
            inv_id,
            b"frozen/inv",
            &[(b"__inv_sentinel".to_vec(), vec![])],
        );
        let manifest = SnapshotManifest {
            volume_id: [1; 16],
            storage_namespace_id: [2; 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [3; 32],
            namespace_digest: [4; 32],
            binding_digest: [5; 32],
            namespace_mode: 1,
            kv_layer_id: Some([6; 16]),
            kv_sealed_version: Some(1),
            namespace_root: None,
            data_root: data_root.clone(),
            inventory_root: inv_root.clone(),
            file_count: 1,
            directory_count: 0,
            total_logical_bytes: 5,
            created_at_ns: 0,
        };
        let manifest_id = [12u8; 16];
        let manifest_obj = build_manifest_object(manifest_id, b"frozen/mf", &manifest);

        // Corrupt the first byte of the leaf page body.
        let leaf_offset = data_root.address.offset as usize;
        data_obj[leaf_offset + 20] ^= 0xFF;

        let mut source = MemoryObjectSource::new();
        source.insert(data_id, data_obj);
        source.insert(inv_id, inv_obj);
        source.insert(manifest_id, manifest_obj.clone());

        // The reader still opens (manifest is fine) but the lookup must fail.
        let reader = FixedRevisionReader::open_manifest(&source, &manifest_obj).unwrap();
        let result = reader.lookup_data(b"hello");
        assert!(
            result.is_err(),
            "corrupted page must fail, got {:?}",
            result
        );
    }

    /// RET-011 / INV-11, INV-18: a fixed readonly baseline loads exactly one
    /// authenticated page per index level and nothing else.  The counters are
    /// fixed across readers and cache states, and the reader owns no KV and no
    /// GC/retention-lease client, so a baseline can never write or lease.
    #[test]
    fn fixed_readonly_baseline_page_counts_are_fixed_and_lease_free() {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..64u32)
            .map(|i| {
                let mut key = b"key-".to_vec();
                key.extend_from_slice(&format!("{:04}", i).into_bytes());
                let mut val = b"val-".to_vec();
                val.extend_from_slice(&format!("{:04}", i).into_bytes());
                (key, val)
            })
            .collect();
        let data_id = [31u8; 16];
        let (data_obj, data_root) =
            build_frozen_metadata_object(data_id, b"frozen/data-031", &entries);
        let inv_id = [32u8; 16];
        let (inv_obj, inv_root) = build_frozen_metadata_object(
            inv_id,
            b"frozen/inventory",
            &[(b"__inv_sentinel".to_vec(), Vec::new())],
        );
        let manifest = SnapshotManifest {
            volume_id: [30; 16],
            storage_namespace_id: [29; 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [28; 32],
            namespace_digest: [27; 32],
            binding_digest: [26; 32],
            namespace_mode: 1,
            kv_layer_id: Some([25; 16]),
            kv_sealed_version: Some(3),
            namespace_root: None,
            data_root,
            inventory_root: inv_root,
            file_count: 64,
            directory_count: 1,
            total_logical_bytes: 64 * 64,
            created_at_ns: 7,
        };
        let manifest_id = [33u8; 16];
        let manifest_obj = build_manifest_object(manifest_id, b"frozen/manifest", &manifest);
        let mut counted = CountingSource {
            inner: MemoryObjectSource::new(),
            reads: std::cell::Cell::new(0),
        };
        counted.inner.insert(data_id, data_obj);
        counted.inner.insert(inv_id, inv_obj);
        counted.inner.insert(manifest_id, manifest_obj.clone());

        // A root-to-leaf walk crosses one page per index level, and every page
        // costs exactly one header range plus one stored-page range.
        let levels = u64::from(manifest.data_root.address.level) + 1;
        let keys = [
            b"key-0000".to_vec(),
            b"key-0031".to_vec(),
            b"key-0063".to_vec(),
        ];
        let mut counts = Vec::new();
        for _ in 0..3 {
            counted.reads.set(0);
            let reader = FixedRevisionReader::open_manifest(&counted, &manifest_obj).unwrap();
            for key in &keys {
                assert!(reader.lookup_data(key).unwrap().is_some());
            }
            assert_eq!(reader.metadata_rpc_count(), 0, "no KV or lease RPC");
            counts.push((reader.object_read_count(), counted.reads.get()));
        }
        assert_eq!(counts[0], counts[1], "the read counters are fixed");
        assert_eq!(counts[1], counts[2], "the read counters are fixed");
        assert_eq!(counts[0].0, levels * keys.len() as u64);
        assert_eq!(
            counts[0].1,
            2 * counts[0].0,
            "one header and one page range per level, nothing else"
        );
    }

    /// Wraps the in-memory source so the fixed baseline's range reads are
    /// observable.  `ObjectSource` only exposes `get_range`, so a readonly
    /// baseline cannot write even in principle.
    struct CountingSource {
        inner: MemoryObjectSource,
        reads: std::cell::Cell<u64>,
    }

    impl ObjectSource for CountingSource {
        fn get_range(
            &self,
            object_id: &[u8; 16],
            start: u64,
            end: u64,
        ) -> Result<Vec<u8>, ObjectSourceError> {
            self.reads.set(self.reads.get() + 1);
            self.inner.get_range(object_id, start, end)
        }
    }

    /// Every page a container object addresses, walked from its root.
    fn page_slots_of(bytes: &[u8], root: &RootRef) -> BTreeSet<PageSlot> {
        let mut slots = BTreeSet::new();
        let mut stack = vec![root.address];
        while let Some(address) = stack.pop() {
            slots.insert(PageSlot {
                container: root.object.object_id,
                offset: address.offset,
                stored_len: address.stored_len,
            });
            let page = IndexPage::decode(&page_bytes_of(bytes, &address)).unwrap();
            if let PageBody::Internal(entries) = page.body {
                for entry in entries {
                    if let ChildRef::Local(child) = entry.child {
                        stack.push(child);
                    }
                }
            }
        }
        slots
    }

    fn page_bytes_of(bytes: &[u8], address: &PageAddress) -> Vec<u8> {
        let start = address.offset as usize;
        let end = start + address.stored_len as usize;
        let stored = &bytes[start..end];
        match address.codec {
            Codec::None => stored.to_vec(),
            Codec::Zstd => zstd::bulk::decompress(stored, address.raw_len as usize).unwrap(),
        }
    }

    /// A frozen-namespace (mode 2) manifest whose three trees are explicit.
    fn frozen_manifest(
        namespace_root: RootRef,
        data_root: RootRef,
        inventory_root: RootRef,
        seed: u8,
    ) -> (Vec<u8>, [u8; 16]) {
        let manifest = SnapshotManifest {
            volume_id: [seed; 16],
            storage_namespace_id: [seed.wrapping_add(1); 16],
            chunk_size: 4096,
            block_size: 512,
            required_features: 0,
            logical_revision: [seed.wrapping_add(2); 32],
            namespace_digest: [seed.wrapping_add(3); 32],
            binding_digest: [seed.wrapping_add(4); 32],
            namespace_mode: 2,
            kv_layer_id: None,
            kv_sealed_version: None,
            namespace_root: Some(namespace_root),
            data_root,
            inventory_root,
            file_count: 1,
            directory_count: 1,
            total_logical_bytes: 4096,
            created_at_ns: 0,
        };
        let manifest_id = [seed.wrapping_add(5); 16];
        (
            build_manifest_object(manifest_id, b"frozen/manifest", &manifest),
            manifest_id,
        )
    }

    /// FROZEN-005 / INV-05: attributes evicted from RAM are re-read from the
    /// authenticated pages and come back identical, including mode/uid/gid and
    /// therefore permissions.  Eviction is observable and never changes an
    /// answer, and a negative lookup is never cached.
    #[test]
    fn cold_attribute_eviction_reloads_identical_attributes_and_permissions() {
        let records: Vec<(u64, FrozenInodeRecord)> = (1..=4u64)
            .map(|inode| {
                (
                    inode,
                    FrozenInodeRecord {
                        kind: 1,
                        mode: 0o100_600 + (inode as u32 % 7),
                        uid: 1000 + inode as u32,
                        gid: 2000 + inode as u32,
                        rdev: 0,
                        nlink: 1,
                        size: 4096 * inode,
                        atime_ns: inode as i64,
                        mtime_ns: inode as i64 + 1,
                        ctime_ns: inode as i64 + 2,
                        parent_hint: Some(1),
                        symlink_target: None,
                    },
                )
            })
            .collect();
        let entries: Vec<(Vec<u8>, Vec<u8>)> = records
            .iter()
            .map(|(inode, record)| (inode_key(*inode), record.encode()))
            .collect();
        let namespace_id = [41u8; 16];
        let (namespace_obj, namespace_root) =
            build_frozen_metadata_object(namespace_id, b"frozen/attributes", &entries);
        let inventory_id = [42u8; 16];
        let (inventory_obj, inventory_root) = build_frozen_metadata_object(
            inventory_id,
            b"frozen/inventory",
            &[(b"__inv_sentinel".to_vec(), Vec::new())],
        );
        let (manifest_obj, manifest_id) =
            frozen_manifest(namespace_root, inventory_root.clone(), inventory_root, 47);
        let mut source = MemoryObjectSource::new();
        source.insert(namespace_id, namespace_obj);
        source.insert(inventory_id, inventory_obj);
        source.insert(manifest_id, manifest_obj.clone());
        let reader = FixedRevisionReader::open_manifest(&source, &manifest_obj).unwrap();

        let mut cache = FrozenAttributeCache::new(2);
        assert!(cache.is_empty());
        let first = cache.attributes_of(&reader, 1).unwrap().unwrap();
        assert_eq!(first, records[0].1);
        assert_eq!(
            cache.attributes_of(&reader, 2).unwrap().unwrap(),
            records[1].1
        );
        assert_eq!(
            cache.attributes_of(&reader, 3).unwrap().unwrap(),
            records[2].1
        );
        assert_eq!(
            cache.attributes_of(&reader, 4).unwrap().unwrap(),
            records[3].1
        );
        assert!(!cache.is_resident(1), "inode 1 was the coldest entry");
        assert!(!cache.is_resident(2));
        assert!(cache.is_resident(3) && cache.is_resident(4));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.misses(), 4);
        assert_eq!(cache.hits(), 0);
        assert_eq!(cache.evictions(), 2);

        let reloaded = cache.attributes_of(&reader, 1).unwrap().unwrap();
        assert_eq!(reloaded, first, "a cold reload is byte-identical");
        assert_eq!(reloaded.mode, first.mode);
        assert_eq!(reloaded.mode & 0o7777, first.mode & 0o7777);
        assert_eq!(reloaded.uid, first.uid);
        assert_eq!(reloaded.gid, first.gid);
        assert_eq!(reloaded.kind, first.kind);
        assert_eq!(reloaded.size, first.size);
        assert_eq!(reloaded.nlink, first.nlink);
        assert_eq!(
            FixedRevisionReader::open_manifest(&source, &manifest_obj)
                .unwrap()
                .lookup_inode(1)
                .unwrap()
                .unwrap(),
            first,
            "a fresh reader agrees with the reloaded cache"
        );
        assert_eq!(cache.attributes_of(&reader, 1).unwrap().unwrap(), first);
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 5);

        assert!(cache.attributes_of(&reader, 999).unwrap().is_none());
        assert!(!cache.is_resident(999), "negatives are never cached");

        let mut uncached = FrozenAttributeCache::new(0);
        assert_eq!(uncached.attributes_of(&reader, 1).unwrap().unwrap(), first);
        assert!(uncached.is_empty());
        assert_eq!(uncached.evictions(), 0);
    }

    /// FROZEN-006 / INV-11: a copy-on-write metadata rewrite reuses every page
    /// it can, writes the changed pages into fresh ranges, and leaves the old
    /// snapshot readable.  An in-place overwrite of a page the old snapshot
    /// still references is refused.
    #[test]
    fn cow_page_reuse_keeps_the_old_snapshot_readable_and_the_new_closure_complete() {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..64u32)
            .map(|i| {
                let mut key = b"key-".to_vec();
                key.extend_from_slice(&format!("{:04}", i).into_bytes());
                let mut value = b"val-".to_vec();
                value.extend_from_slice(&format!("{:04}", i).into_bytes());
                (key, value)
            })
            .collect();
        let old_id = [51u8; 16];
        let (old_bytes, old_root) = build_frozen_metadata_object(old_id, b"frozen/old", &entries);
        let old_slots = page_slots_of(&old_bytes, &old_root);
        assert!(old_slots.len() >= 2, "the fixture must span several pages");

        // The rewrite reuses everything except one leaf, which moves into a
        // fresh container so the old page bytes are never touched.
        let replaced = old_slots.iter().next_back().copied().unwrap();
        let fresh = PageSlot {
            container: [52u8; 16],
            offset: 128,
            stored_len: replaced.stored_len,
        };
        let mut new_slots = old_slots.clone();
        new_slots.remove(&replaced);
        new_slots.insert(fresh);

        let plan = plan_page_reuse(&old_slots, &new_slots);
        assert_eq!(plan.reused.len(), old_slots.len() - 1);
        assert_eq!(plan.must_write, vec![fresh]);
        assert_eq!(plan.unreferenced, vec![replaced]);
        assert!(
            plan.covers_new_snapshot(&new_slots),
            "every page of the new snapshot is reused or written"
        );
        assert!(
            plan.reused.iter().all(|slot| old_slots.contains(slot)),
            "reused pages are untouched old pages"
        );

        // The old snapshot still reads its own bytes: nothing was overwritten.
        let inventory_id = [53u8; 16];
        let (inventory_obj, inventory_root) = build_frozen_metadata_object(
            inventory_id,
            b"frozen/inventory",
            &[(b"__inv_sentinel".to_vec(), Vec::new())],
        );
        let (manifest_obj, manifest_id) =
            frozen_manifest(inventory_root.clone(), old_root.clone(), inventory_root, 54);
        let mut source = MemoryObjectSource::new();
        source.insert(old_id, old_bytes.clone());
        source.insert(inventory_id, inventory_obj);
        source.insert(manifest_id, manifest_obj.clone());
        let reader = FixedRevisionReader::open_manifest(&source, &manifest_obj).unwrap();
        assert_eq!(
            reader.lookup_data(b"key-0000").unwrap().as_deref(),
            Some(b"val-0000".as_slice())
        );
        assert!(old_slots.contains(&replaced));

        // An in-place overwrite of a still-referenced page is refused; the same
        // page written into a fresh container is accepted.
        let error = ensure_page_overwrite_is_safe(&old_slots, &replaced).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("still referenced by the old snapshot"),
            "unexpected error: {error}"
        );
        ensure_page_overwrite_is_safe(&old_slots, &fresh).unwrap();
    }

    /// FROZEN-008 / INV-09: a summary metadata scan reports the volume it
    /// scanned and the volume it rewrote separately, and a rewrite that was
    /// never scanned is refused instead of being counted.
    #[test]
    fn summary_meta_scan_counts_scanned_and_rewritten_volume_separately() {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..64u32)
            .map(|i| {
                let mut key = b"key-".to_vec();
                key.extend_from_slice(&format!("{:04}", i).into_bytes());
                let mut value = b"val-".to_vec();
                value.extend_from_slice(&format!("{:04}", i).into_bytes());
                (key, value)
            })
            .collect();
        let old_id = [61u8; 16];
        let (old_bytes, old_root) =
            build_frozen_metadata_object(old_id, b"frozen/scan-old", &entries);
        let old_slots = page_slots_of(&old_bytes, &old_root);

        let replaced = old_slots.iter().next_back().copied().unwrap();
        let fresh = PageSlot {
            container: [62u8; 16],
            offset: 128,
            stored_len: replaced.stored_len,
        };
        let mut new_slots = old_slots.clone();
        new_slots.remove(&replaced);
        new_slots.insert(fresh);
        let plan = plan_page_reuse(&old_slots, &new_slots);

        // The scan read every old page and rewrote the ones it could not reuse.
        let scanned: Vec<PageSlot> = old_slots.iter().copied().collect();
        let counters = account_meta_scan(&scanned, &plan.unreferenced).unwrap();
        let unreferenced_bytes: u64 = plan
            .unreferenced
            .iter()
            .map(|slot| u64::from(slot.stored_len))
            .sum();
        assert_eq!(counters.scanned_pages, old_slots.len() as u64);
        assert_eq!(counters.rewritten_pages, plan.unreferenced.len() as u64);
        assert_eq!(counters.rewritten_bytes, unreferenced_bytes);
        assert!(counters.scanned_bytes > counters.rewritten_bytes);
        assert_eq!(
            counters.scanned_pages,
            counters.rewritten_pages + (old_slots.len() - plan.unreferenced.len()) as u64
        );

        let unscanned = PageSlot {
            container: [63u8; 16],
            offset: 4096,
            stored_len: 512,
        };
        let error = account_meta_scan(&scanned, &[unscanned]).unwrap_err();
        assert!(
            error.to_string().contains("was never scanned"),
            "unexpected error: {error}"
        );
        // A scan with no rewrite still reports its own volume.
        let read_only = account_meta_scan(&scanned, &[]).unwrap();
        assert_eq!(read_only.rewritten_pages, 0);
        assert_eq!(read_only.scanned_pages, old_slots.len() as u64);
    }

    /// IDX-003 / INV-10: attributes may move from the namespace row into an
    /// external object, but the move is byte-exact, so the resolved canonical
    /// bytes -- and therefore the revision digest -- cannot change.
    /// Non-canonical payloads, wrong lengths and foreign objects are refused
    /// instead of being repaired or silently resolved elsewhere.
    #[test]
    fn attribute_relocation_from_inline_to_external_keeps_the_canonical_bytes() {
        let inode = 7u64;
        let record = FrozenInodeRecord {
            kind: 1,
            mode: 0o100_644,
            uid: 1000,
            gid: 1001,
            rdev: 0,
            nlink: 3,
            size: 4096,
            atime_ns: 11,
            mtime_ns: 12,
            ctime_ns: 13,
            parent_hint: Some(1),
            symlink_target: None,
        };
        let inline = encode_inline_attributes(&record);
        assert_eq!(inline, record.encode());
        let inline_row = AttributePlacement::Inline(inline.clone());
        assert_eq!(inline_row.encode_row(), inline);

        // The external payload is stored verbatim in a FrozenMetadata object;
        // the reference names the offset the encoder chose for it.
        let external_id = [77u8; 16];
        let (object, _root) = build_frozen_metadata_object(
            external_id,
            b"frozen/inline-external",
            &[(inode_key(inode), inline.clone())],
        );
        let offset = object
            .windows(inline.len())
            .position(|window| window == inline.as_slice())
            .expect("the canonical payload is stored verbatim") as u64;
        let (placement, payload) = relocate_attributes(&record, external_id, offset);
        let reference = match &placement {
            AttributePlacement::External(reference) => *reference,
            AttributePlacement::Inline(_) => unreachable!(),
        };
        assert_ne!(
            placement.encode_row(),
            inline,
            "the namespace row is a reference now"
        );
        assert_eq!(reference.offset, offset);
        assert_eq!(reference.stored_len as usize, inline.len());
        assert_eq!(reference.end(), offset + inline.len() as u64);
        assert_eq!(payload, inline);
        ensure_relocation_keeps_revision(&inline, &payload).unwrap();

        let mut source = MemoryObjectSource::new();
        source.insert(external_id, object);
        let resolved = resolve_external_attributes(&source, &reference).unwrap();
        assert_eq!(resolved, record);
        assert_eq!(resolved.encode(), inline, "canonical bytes survived");
        assert_eq!(
            AttributePlacement::decode_row(&placement.encode_row()).unwrap(),
            placement
        );

        // Revision proof: the digest is taken over the resolved canonical
        // bytes and both placements produce exactly the same bytes.
        let resolved_inline =
            match AttributePlacement::decode_row(&inline_row.encode_row()).unwrap() {
                AttributePlacement::Inline(bytes) => {
                    decode_canonical_attributes(&bytes).unwrap().encode()
                }
                AttributePlacement::External(_) => unreachable!(),
            };
        let resolved_external =
            match AttributePlacement::decode_row(&placement.encode_row()).unwrap() {
                AttributePlacement::External(reference) => {
                    resolve_external_attributes(&source, &reference)
                        .unwrap()
                        .encode()
                }
                AttributePlacement::Inline(_) => unreachable!(),
            };
        let rows = |value: Vec<u8>| {
            vec![FrozenRow {
                key: inode_key(inode),
                value,
            }]
        };
        assert_eq!(
            canonical_table_digest(1, &rows(resolved_inline)),
            canonical_table_digest(1, &rows(resolved_external)),
        );

        // Negatives: a shortened payload, a non-canonical payload and a
        // foreign object are refused rather than reinterpreted.
        assert!(ensure_relocation_keeps_revision(&inline, &payload[..payload.len() - 1]).is_err());
        let mut non_canonical = payload.clone();
        non_canonical.push(0);
        assert!(decode_canonical_attributes(&non_canonical).is_err());
        let mut short = reference;
        short.stored_len -= 1;
        assert!(resolve_external_attributes(&source, &short).is_err());
        let foreign = ValueRef {
            object_id: [99u8; 16],
            ..reference
        };
        assert!(resolve_external_attributes(&source, &foreign).is_err());
    }

    /// IDX-004 / INV-01 / INV-11: a repack leaves gaps in the ordinal map and
    /// historical Objects rows behind.  Pruning removes exactly this pack's
    /// dead mappings, keeps pinned rows, and never touches or claims another
    /// pack's rows.  An ordinal that is not already mapped inside the pack
    /// cannot be retained, because accepting it would reinterpret the slot.
    #[test]
    fn inventory_prune_removes_only_dead_slots_of_the_repacked_pack() {
        let pack = [0xAAu8; 16];
        let other = [0xBBu8; 16];
        let row = |ordinal: u32, pack: ObjectId, offset: u64| InventoryOrdinal {
            ordinal,
            pack,
            offset,
            stored_len: 4096,
        };
        let old = vec![
            row(0, pack, 0),
            row(1, pack, 4096),
            row(2, pack, 8192),
            row(5, pack, 20480),
            row(7, other, 28672),
        ];
        let pinned: BTreeSet<u32> = [1].into_iter().collect();
        let live: BTreeSet<u32> = [0, 5].into_iter().collect();
        let plan = plan_inventory_prune(&old, &pack, &pinned, &live).unwrap();
        assert_eq!(
            plan.retained
                .iter()
                .map(|row| row.ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1, 5],
            "pinned survives even though it is not live yet"
        );
        assert_eq!(plan.pruned_keys, vec![inventory_ordinal_key(2)]);
        assert_eq!(plan.foreign, vec![row(7, other, 28672)]);
        assert!(!plan.pruned_keys.contains(&inventory_ordinal_key(1)));
        assert!(
            !plan.pruned_keys.contains(&inventory_ordinal_key(7)),
            "a foreign row is never deleted by this pack"
        );
        assert!(
            plan.retained.iter().all(|row| row.pack == pack),
            "a foreign row is never retained as this pack's mapping"
        );

        let unmapped: BTreeSet<u32> = [3].into_iter().collect();
        let error = plan_inventory_prune(&old, &pack, &unmapped, &BTreeSet::new()).unwrap_err();
        assert!(error.to_string().contains("is not mapped"), "{error}");
        let stolen: BTreeSet<u32> = [7].into_iter().collect();
        let error = plan_inventory_prune(&old, &pack, &BTreeSet::new(), &stolen).unwrap_err();
        assert!(
            error.to_string().contains("maps into another pack"),
            "{error}"
        );
        let reinterpreted = vec![row(2, pack, 0), row(2, pack, 4096)];
        let error = plan_inventory_prune(&reinterpreted, &pack, &BTreeSet::new(), &BTreeSet::new())
            .unwrap_err();
        assert!(error.to_string().contains("strictly ascend"), "{error}");
    }

    /// IDX-005 / INV-05 / INV-13: an issued readdir cookie survives RAM
    /// eviction of the entry spool because it names its anchor entry instead of
    /// a spool index.  A full cookie table fails *before* a cookie is handed
    /// out, so a caller can never receive a cookie the cursor cannot replay,
    /// and a cookie whose anchor is gone or was replaced fails instead of
    /// resuming at a guessed position.
    #[test]
    fn issued_directory_cookies_survive_spool_eviction_and_the_table_fails_first() {
        let entries: Vec<DirectoryEntry> = (0..8u32)
            .map(|index| DirectoryEntry {
                name: format!("name-{index}").into_bytes(),
                inode: u64::from(index) + 1,
                kind: 1,
            })
            .collect();
        let names = |page: &[DirectoryEntry]| {
            page.iter()
                .map(|entry| entry.name.clone())
                .collect::<Vec<_>>()
        };
        let mut cursor = DirectoryCursor::with_cookie_budget(entries.clone(), 4096, 2).unwrap();
        let (first_cookie, first_page) = cursor.page(0, 2).unwrap();
        assert_eq!(
            names(&first_page),
            vec![b"name-0".to_vec(), b"name-1".to_vec()]
        );
        assert!(cursor.entries_resident());
        assert_eq!(cursor.cookie_count(), 1);

        // The spool is evicted; the issued cookie still resolves after the
        // replay and resumes at exactly the same successor entry.
        let freed = cursor.evict_entries();
        assert!(freed > 0);
        assert!(!cursor.entries_resident());
        assert_eq!(cursor.resident_entry_count(), 0);
        let error = cursor.page(first_cookie, 2).unwrap_err();
        assert!(error.to_string().contains("not in the spool"), "{error}");
        cursor.respool(entries.clone(), 4096).unwrap();
        let (second_cookie, second_page) = cursor.page(first_cookie, 2).unwrap();
        assert_eq!(
            names(&second_page),
            vec![b"name-2".to_vec(), b"name-3".to_vec()]
        );
        assert_eq!(cursor.page(first_cookie, 2).unwrap().0, second_cookie);
        assert_eq!(cursor.cookie_count(), 2);

        // The cookie table is full: the next new cookie fails closed and the
        // caller gets no page at all, but replaying a known anchor still works.
        let error = cursor.page(second_cookie, 2).unwrap_err();
        assert!(error.to_string().contains("cookie table budget"), "{error}");
        assert_eq!(cursor.page(first_cookie, 2).unwrap().0, second_cookie);
        assert_eq!(cursor.cookie_count(), 2);

        // A stale anchor is refused instead of resuming at a guessed position.
        cursor
            .respool(
                vec![DirectoryEntry {
                    name: b"name-1".to_vec(),
                    inode: 999,
                    kind: 1,
                }],
                4096,
            )
            .unwrap();
        let error = cursor.page(first_cookie, 2).unwrap_err();
        assert!(
            error.to_string().contains("replaced by another inode"),
            "{error}"
        );
        cursor
            .respool(
                vec![DirectoryEntry {
                    name: b"other".to_vec(),
                    inode: 1,
                    kind: 1,
                }],
                4096,
            )
            .unwrap();
        let error = cursor.page(first_cookie, 2).unwrap_err();
        assert!(error.to_string().contains("not in the spool"), "{error}");
        assert!(cursor.page(u64::MAX, 1).is_err());
    }
}
