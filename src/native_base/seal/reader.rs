//! SealSnapshot and SealReader: the pinned-view read path over a Data Seal
//! (spec 04 §7).
//!
//! The lookup contract enforced here:
//! - Bindings and Placements must both resolve for a block; one without the
//!   other is an error, never a partial result.
//! - Exact-key lookup descends the BNPG tree page by page; an absent key is
//!   `None`. A page that fails to read or parse is an *error* — absence is
//!   never cached or inferred from a parse failure.
//! - Missing structures (frame descriptor, object, source 404) are errors;
//!   nothing is ever zero-filled.
//!
//! PR03 scope: the head KV table (checked first for a pinned ReadView) is
//! PR04; the read executor here is the synchronous, per-call plan in
//! `plan.rs`. The async singleflight executor and Range GET coalescing are
//! PR10.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use sha2::{Digest, Sha256};

use super::binding::BlockBinding;
use super::builder::NATIVE_LOOSE_KIND;
use super::descriptor::FrameDescriptor;
use super::error::SealError;
use super::placement::BlockPlacement;
use super::source::ObjectSource;
use super::tables::{SealRoot, TableId, binding_key, frame_key, object_key};
use crate::native_base::wire::container::{
    Codec, ContainerHeader, FOOTER_LEN, FeatureClosure, HEADER_LEN, ObjectKind, features,
};
use crate::native_base::wire::error::WireError;
use crate::native_base::wire::page::{IndexPage, PageBody};
use crate::native_base::wire::refs::{
    ChildRef, Hash32, MAX_INDEX_LEVEL, ObjectId, ObjectRef, PageAddress, PageKind, RootRef,
    ensure_object_kind_allows_page,
};
use crate::native_base::wire::uvarint::Reader;

/// A parsed, digest-verified `.brfds` object. Immutable once opened; all
/// lookups against it are view-bound (a pinned ReadView keeps exactly one
/// of these alive, spec 04 §1).
#[derive(Debug, Clone)]
pub struct SealSnapshot {
    object: Vec<u8>,
    header: ContainerHeader,
    root: SealRoot,
    /// Capability closure of the seal's own container bytes (root codec,
    /// external index children), spec 02 §3 / INV-20.
    feature_closure: FeatureClosure,
}

impl SealSnapshot {
    /// Parse and authenticate a complete seal object: header kind/features,
    /// footer, root region bounds, root stored digest, root codec, and the
    /// BNSD table directory.
    pub fn open(object: Vec<u8>) -> Result<SealSnapshot, SealError> {
        let header = ContainerHeader::parse(&object)?;
        if header.kind != ObjectKind::DataSeal {
            return Err(WireError::invalid(
                "data seal",
                format!("container kind {:?} is not DataSeal", header.kind),
            )
            .into());
        }
        header.ensure_supported_features()?;
        if header.object_len as usize != object.len() {
            return Err(WireError::invalid(
                "data seal",
                format!(
                    "header object_len {} != actual {}",
                    header.object_len,
                    object.len()
                ),
            )
            .into());
        }
        let footer = crate::native_base::wire::container::parse_footer(&object)?;
        let root_start = header.root_offset as usize;
        let root_end = root_start
            .checked_add(header.root_stored_len as usize)
            .ok_or(WireError::invalid("data seal", "root region end overflow"))?;
        if root_start < HEADER_LEN || root_end > object.len() - FOOTER_LEN {
            return Err(WireError::invalid(
                "data seal",
                "root region overlaps the header or footer",
            )
            .into());
        }
        let stored = &object[root_start..root_end];
        let digest: [u8; 32] = Sha256::digest(stored).into();
        if digest != footer.root_stored_digest {
            return Err(WireError::HashMismatch {
                what: "seal root payload",
                stored: hex::encode(footer.root_stored_digest),
                computed: hex::encode(digest),
            }
            .into());
        }
        let raw = match header.root_codec {
            Codec::None => {
                if header.root_raw_len != header.root_stored_len {
                    return Err(WireError::invalid(
                        "seal root",
                        "codec None requires root_raw_len == root_stored_len",
                    )
                    .into());
                }
                stored.to_vec()
            }
            Codec::Zstd => {
                let expected = header.root_raw_len as usize;
                let out = zstd::bulk::decompress(stored, expected)
                    .map_err(|e| WireError::Codec(format!("zstd: {e}")))?;
                if out.len() != expected {
                    return Err(WireError::invalid(
                        "seal root",
                        format!("zstd output {} != announced raw_len {expected}", out.len()),
                    )
                    .into());
                }
                out
            }
        };
        let root = SealRoot::decode(&raw)?;
        // The seal's own content: a Zstd root payload needs the Zstd
        // capability, and an external table root needs the external-index
        // capability, whatever the header claims (spec 02 §3, INV-20).
        let mut content_features = 0u64;
        if header.root_codec == Codec::Zstd {
            content_features |= features::ZSTD_FRAME_OR_PAGE;
        }
        if TableId::ALL
            .iter()
            .any(|id| matches!(root.table(*id), Some(ChildRef::External(_))))
        {
            content_features |= features::EXTERNAL_INDEX_CHILDREN;
        }
        let feature_closure = FeatureClosure::of(header.required_features, content_features);
        feature_closure.ensure_declared()?;
        Ok(SealSnapshot {
            object,
            header,
            root,
            feature_closure,
        })
    }

    pub fn header(&self) -> &ContainerHeader {
        &self.header
    }

    pub fn root(&self) -> &SealRoot {
        &self.root
    }

    /// The capability closure of this seal container: the declared bits
    /// unioned with what its own bytes require (spec 02 §3, INV-20).
    pub fn feature_closure(&self) -> FeatureClosure {
        self.feature_closure
    }

    pub fn table(&self, id: TableId) -> Option<&ChildRef> {
        self.root.table(id)
    }

    pub fn object_bytes(&self) -> &[u8] {
        &self.object
    }
}

/// A block resolved from the Bindings and Placements tables.
#[derive(Debug, Clone)]
pub struct ResolvedBlock {
    pub slice_id: u64,
    pub block_index: u32,
    pub binding: BlockBinding,
    pub placement: BlockPlacement,
}

/// Digest-keyed identity of one authenticated index page.
///
/// `object` is `None` for a page inside the reader's own pinned seal and
/// `Some` for a page located in an external container, either directly
/// (`ChildRef::External`) or as a local child addressed inside one.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PageKey {
    object: Option<ObjectId>,
    offset: u64,
    stored_len: u32,
    stored_digest: Hash32,
}

fn page_key(object: Option<ObjectId>, addr: &PageAddress) -> PageKey {
    PageKey {
        object,
        offset: addr.offset,
        stored_len: addr.stored_len,
        stored_digest: addr.stored_digest,
    }
}

/// Reads a pinned seal snapshot through an object backend.
pub struct SealReader<'a> {
    snapshot: &'a SealSnapshot,
    pub(super) source: &'a dyn ObjectSource,
    /// Index pages already authenticated by this reader, keyed by the exact
    /// stored bytes they were authenticated against.  A reader is created per
    /// read, so this is a read-scoped cache: one fetch per distinct page per
    /// read.  Because the key carries the stored digest, a container whose
    /// bytes change underneath the reader can never contribute a second
    /// revision to the same read ? every lookup either resolves to the page
    /// the pinned view was opened with or fails authentication.
    pages: Mutex<BTreeMap<PageKey, (IndexPage, Option<ObjectRef>)>>,
}

impl<'a> SealReader<'a> {
    pub fn new(snapshot: &'a SealSnapshot, source: &'a dyn ObjectSource) -> SealReader<'a> {
        SealReader {
            snapshot,
            source,
            pages: Mutex::new(BTreeMap::new()),
        }
    }

    fn pages(&self) -> MutexGuard<'_, BTreeMap<PageKey, (IndexPage, Option<ObjectRef>)>> {
        self.pages
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn snapshot(&self) -> &SealSnapshot {
        self.snapshot
    }

    /// Verify one page's stored digest, undo the page codec, parse and
    /// cross-check the PageAddress against the decoded page (kind, level,
    /// entry count, raw length).
    fn decode_page_payload(
        &self,
        addr: &PageAddress,
        stored: &[u8],
    ) -> Result<IndexPage, SealError> {
        let digest: [u8; 32] = Sha256::digest(stored).into();
        if digest != addr.stored_digest {
            return Err(WireError::HashMismatch {
                what: "index page",
                stored: hex::encode(addr.stored_digest),
                computed: hex::encode(digest),
            }
            .into());
        }
        let raw = match addr.codec {
            Codec::None => {
                if addr.raw_len != addr.stored_len {
                    return Err(WireError::invalid(
                        "index page",
                        "codec None requires raw_len == stored_len",
                    )
                    .into());
                }
                stored.to_vec()
            }
            Codec::Zstd => {
                let expected = addr.raw_len as usize;
                let out = zstd::bulk::decompress(stored, expected)
                    .map_err(|e| WireError::Codec(format!("zstd: {e}")))?;
                if out.len() != expected {
                    return Err(WireError::invalid(
                        "index page",
                        format!("zstd output {} != announced {expected}", out.len()),
                    )
                    .into());
                }
                out
            }
        };
        if addr.page_kind != PageKind::GenericKeyValue {
            return Err(WireError::invalid(
                "index page",
                format!("page kind {:?} is not GenericKeyValue", addr.page_kind),
            )
            .into());
        }
        let page = IndexPage::decode(&raw)?;
        if page.level != addr.level {
            return Err(WireError::invalid(
                "index page",
                format!(
                    "page level {} disagrees with address level {}",
                    page.level, addr.level
                ),
            )
            .into());
        }
        if page.entry_count() != addr.entry_count {
            return Err(WireError::invalid(
                "index page",
                format!(
                    "page entry count {} disagrees with address count {}",
                    page.entry_count(),
                    addr.entry_count
                ),
            )
            .into());
        }
        Ok(page)
    }

    /// Fetch, authenticate and parse one index page, local or external.
    ///
    /// Returns the page plus the container its own local children are
    /// addressed in: `None` for the reader's pinned seal, `Some` for the
    /// external container the page was located in.  `ChildRef::Local` is
    /// therefore resolved against the *current* container ? the pinned seal
    /// or the external object a parent page pointed at ? never against
    /// whichever container happens to belong to the reader.  A page that is
    /// not in memory yet is fetched through its locator; a page that cannot
    /// be read or authenticated stays an error.
    ///
    /// The location is resolved (and bounds-checked) before the cache is
    /// consulted, so an already-authenticated page is never fetched twice
    /// inside one view.
    fn read_page(
        &self,
        child: &ChildRef,
        container: Option<&ObjectRef>,
    ) -> Result<(IndexPage, Option<ObjectRef>), SealError> {
        let (addr, owner) = match child {
            ChildRef::Local(addr) => match container {
                None => {
                    let end = addr
                        .offset
                        .checked_add(addr.stored_len as u64)
                        .ok_or_else(|| WireError::invalid("index page", "page end overflow"))?;
                    if addr.offset < HEADER_LEN as u64
                        || end > (self.snapshot.object.len() - FOOTER_LEN) as u64
                    {
                        return Err(WireError::invalid(
                            "index page",
                            "local page outside the container's page region",
                        )
                        .into());
                    }
                    (addr, None)
                }
                Some(object) => {
                    let end = addr
                        .offset
                        .checked_add(addr.stored_len as u64)
                        .ok_or_else(|| WireError::invalid("index page", "page end overflow"))?;
                    if addr.offset < HEADER_LEN as u64 || end > object.object_len {
                        return Err(WireError::invalid(
                            "index page",
                            "local page outside the external container's page region",
                        )
                        .into());
                    }
                    (addr, Some(object.clone()))
                }
            },
            ChildRef::External(root_ref) => {
                let addr = &root_ref.address;
                let kind = object_kind_from_u8(root_ref.object.kind).ok_or_else(|| {
                    WireError::invalid(
                        "external child",
                        format!(
                            "object kind {} is not a container kind",
                            root_ref.object.kind
                        ),
                    )
                })?;
                ensure_object_kind_allows_page(kind, addr.page_kind)?;
                let end = addr
                    .offset
                    .checked_add(addr.stored_len as u64)
                    .ok_or_else(|| WireError::invalid("index page", "page end overflow"))?;
                if end > root_ref.object.object_len {
                    return Err(WireError::invalid(
                        "external child",
                        "page range exceeds the referenced object length",
                    )
                    .into());
                }
                (addr, Some(root_ref.object.clone()))
            }
        };
        let key = page_key(owner.as_ref().map(|object| object.object_id), addr);
        if let Some(cached) = self.pages().get(&key).cloned() {
            return Ok(cached);
        }
        let stored = match &owner {
            None => {
                let start = addr.offset as usize;
                self.snapshot.object[start..start + addr.stored_len as usize].to_vec()
            }
            Some(object) => {
                let end = addr.offset + addr.stored_len as u64;
                let stored = self.source.get_range(&object.object_id, addr.offset, end)?;
                if stored.len() as u64 != addr.stored_len as u64 {
                    return Err(SealError::Source(
                        super::source::ObjectSourceError::ShortRead {
                            requested: addr.stored_len as u64,
                            received: stored.len() as u64,
                        },
                    ));
                }
                stored
            }
        };
        let page = self.decode_page_payload(addr, &stored)?;
        let entry = (page, owner);
        self.pages().insert(key, entry.clone());
        Ok(entry)
    }

    /// Exact-key lookup in one table, descending the index page by page.
    /// Returns `Ok(None)` only when the owning page was actually loaded and
    /// the key was not in it: a page still absent from memory is fetched
    /// through its locator first, and a page read or parse failure is an
    /// error ? never ENOENT, never a hole, and never cached as absence
    /// (spec 04 ?7).
    pub fn lookup(&self, table: TableId, key: &[u8]) -> Result<Option<Vec<u8>>, SealError> {
        let child = self
            .snapshot
            .root
            .table(table)
            .ok_or(SealError::MissingTable(table))?;
        let mut current = child.clone();
        let mut container: Option<ObjectRef> = None;
        let mut depth = 0usize;
        loop {
            depth += 1;
            if depth > MAX_INDEX_LEVEL as usize + 2 {
                return Err(SealError::integrity(
                    "index walk exceeded the maximum height",
                ));
            }
            let (page, next_container) = self.read_page(&current, container.as_ref())?;
            container = next_container;
            match page.body {
                PageBody::Leaf(entries) => {
                    let at = entries.partition_point(|e| e.key.as_slice() < key);
                    return Ok(if at < entries.len() && entries[at].key == key {
                        Some(entries[at].value.clone())
                    } else {
                        None
                    });
                }
                PageBody::Internal(entries) => {
                    let mut next = None;
                    for entry in &entries {
                        if entry.min_key.as_slice() <= key && key <= entry.max_key.as_slice() {
                            next = Some(entry.child.clone());
                            break;
                        }
                    }
                    match next {
                        Some(child) => current = child,
                        // The key falls in a gap between child ranges: absent.
                        None => return Ok(None),
                    }
                }
            }
        }
    }

    /// Iterate every entry of a table in key order (verification path; the
    /// read path never scans).
    pub fn for_each_entry(
        &self,
        table: TableId,
        mut f: impl FnMut(&[u8], &[u8]) -> Result<(), SealError>,
    ) -> Result<(), SealError> {
        let child = self
            .snapshot
            .root
            .table(table)
            .ok_or(SealError::MissingTable(table))?;
        let mut stack: Vec<(ChildRef, Option<ObjectRef>)> = vec![(child.clone(), None)];
        while let Some((child, container)) = stack.pop() {
            let (page, next_container) = self.read_page(&child, container.as_ref())?;
            match page.body {
                PageBody::Leaf(entries) => {
                    for entry in entries {
                        f(&entry.key, &entry.value)?;
                    }
                }
                PageBody::Internal(entries) => {
                    for entry in entries.iter().rev() {
                        stack.push((entry.child.clone(), next_container.clone()));
                    }
                }
            }
        }
        Ok(())
    }

    /// Verify the Bindings and Placements key sets are identical (spec 04
    /// §3). The reader enforces per-block consistency during reads; this is
    /// the full-scan audit. An absent table is the empty set (spec 04 §2:
    /// empty tables use `None`), so a binding without any placements table
    /// at all still reports as a key-set asymmetry.
    pub fn verify_key_sets(&self) -> Result<(), SealError> {
        let collect = |table: TableId| -> Result<Vec<Vec<u8>>, SealError> {
            let mut keys = Vec::new();
            if self.snapshot.root.table(table).is_none() {
                return Ok(keys);
            }
            self.for_each_entry(table, |key, _| {
                keys.push(key.to_vec());
                Ok(())
            })?;
            Ok(keys)
        };
        let bindings = collect(TableId::Bindings)?;
        let placements = collect(TableId::Placements)?;
        if bindings == placements {
            return Ok(());
        }
        let only_bindings = bindings.iter().filter(|k| !placements.contains(*k)).count();
        let only_placements = placements.iter().filter(|k| !bindings.contains(*k)).count();
        Err(SealError::KeySetMismatch(format!(
            "{} binding keys vs {} placement keys ({} binding-only, {} placement-only)",
            bindings.len(),
            placements.len(),
            only_bindings,
            only_placements
        )))
    }

    /// Resolve one block's binding and placement. Both must exist
    /// (spec 04 §7); a binding without a placement is an error.
    pub fn resolve_block(
        &self,
        slice_id: u64,
        block_index: u32,
    ) -> Result<ResolvedBlock, SealError> {
        let key = binding_key(slice_id, block_index);
        let binding_value =
            self.lookup(TableId::Bindings, &key)?
                .ok_or_else(|| SealError::KeyNotFound {
                    table: TableId::Bindings,
                    key: hex::encode(key),
                })?;
        let binding =
            BlockBinding::decode(&mut Reader::new(&binding_value)).map_err(SealError::Wire)?;
        let placement_value = match self.lookup(TableId::Placements, &key) {
            Ok(Some(value)) => value,
            // An absent placements table is the empty set: a binding with
            // no placement is a contract violation, not a missing-table
            // technicality.
            Ok(None) | Err(SealError::MissingTable(TableId::Placements)) => {
                return Err(SealError::placement(format!(
                    "block {} has a binding but no placement",
                    hex::encode(key)
                )));
            }
            Err(e) => return Err(e),
        };
        let placement =
            BlockPlacement::decode(&mut Reader::new(&placement_value)).map_err(SealError::Wire)?;
        Ok(ResolvedBlock {
            slice_id,
            block_index,
            binding,
            placement,
        })
    }

    /// Look up an object reference by ordinal.
    pub fn object_for(&self, object_ordinal: u32) -> Result<ObjectRef, SealError> {
        let key = object_key(object_ordinal);
        let value = self
            .lookup(TableId::Objects, &key)?
            .ok_or_else(|| SealError::KeyNotFound {
                table: TableId::Objects,
                key: hex::encode(key),
            })?;
        ObjectRef::decode(&mut Reader::new(&value)).map_err(SealError::Wire)
    }

    /// Look up a frame descriptor by seal-scoped slot.
    pub fn frame_descriptor(&self, frame_slot: u64) -> Result<FrameDescriptor, SealError> {
        let key = frame_key(frame_slot);
        let value = self
            .lookup(TableId::Frames, &key)?
            .ok_or_else(|| SealError::KeyNotFound {
                table: TableId::Frames,
                key: hex::encode(key),
            })?;
        FrameDescriptor::decode(&mut Reader::new(&value)).map_err(SealError::Wire)
    }

    /// The object kind byte a placement tag requires (spec 04 §5):
    /// Loose -> NativeLoose (6), Packed -> DataPack (1).
    pub(crate) fn required_object_kind(placement: &BlockPlacement) -> u8 {
        match placement {
            BlockPlacement::Loose { .. } => NATIVE_LOOSE_KIND,
            BlockPlacement::Packed { .. } => ObjectKind::DataPack.as_u8(),
        }
    }

    /// Validate a RootRef external child shape (used by tests and PR06A).
    pub fn validate_external_child(root_ref: &RootRef) -> Result<(), SealError> {
        let kind = object_kind_from_u8(root_ref.object.kind).ok_or_else(|| {
            WireError::invalid(
                "external child",
                format!(
                    "object kind {} is not a container kind",
                    root_ref.object.kind
                ),
            )
        })?;
        ensure_object_kind_allows_page(kind, root_ref.address.page_kind).map_err(SealError::Wire)
    }
}

/// Map an object kind byte to a container kind. Byte 6 (native loose) has no
/// container and is excluded — it can never carry index pages.
fn object_kind_from_u8(v: u8) -> Option<ObjectKind> {
    match v {
        1 => Some(ObjectKind::DataPack),
        2 => Some(ObjectKind::DataSeal),
        3 => Some(ObjectKind::FrozenMetadata),
        4 => Some(ObjectKind::SnapshotManifest),
        5 => Some(ObjectKind::PagedInventory),
        _ => None,
    }
}
