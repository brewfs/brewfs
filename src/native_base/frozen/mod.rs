//! Frozen metadata primitives (PR08/PR09).
//!
//! The P2 format is deliberately separate from the P1 KV head.  This module
//! owns the canonical inode/dentry/extent rows, authenticated snapshot
//! manifest, bounded directory cursor, and the prefix-scoped extent lookup
//! rule.  Mount admission does not enable it unless the persisted volume
//! header explicitly requires `frozen-base-metadata`.

use std::collections::{BTreeMap, BTreeSet};

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
        Ok(record)
    }
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

/// A fixed revision reader owns no mutable KV client.  Every lookup is served
/// from the authenticated manifest and its external pages; the counter is
/// intentionally exposed for tests and observability to prove that inode
/// lookups do not silently fall back to a KV RPC.
pub struct FixedRevisionReader<'a, S: ObjectSource> {
    source: &'a S,
    manifest: SnapshotManifest,
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
            metadata_rpc_count: std::sync::atomic::AtomicU64::new(0),
            object_reads: std::sync::atomic::AtomicU64::new(0),
        })
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

    pub fn lookup_data(&self, key: &[u8]) -> Result<Option<Vec<u8>>, FrozenReadError> {
        self.lookup_root(Some(&self.manifest.data_root), key)
    }

    pub fn lookup_inventory(&self, key: &[u8]) -> Result<Option<Vec<u8>>, FrozenReadError> {
        self.lookup_root(Some(&self.manifest.inventory_root), key)
    }

    pub fn lookup_inode(&self, inode: u64) -> Result<Option<FrozenInodeRecord>, FrozenReadError> {
        let Some(bytes) = self.lookup_namespace(&inode_key(inode))? else {
            return Ok(None);
        };
        Ok(Some(FrozenInodeRecord::decode(&bytes)?))
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
            let length = decode_extent_length(&value)?;
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
        self.object_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok((page, object.clone()))
    }

    fn lower_bound_data(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>, FrozenReadError> {
        let Some(root) = Some(&self.manifest.data_root) else {
            unreachable!()
        };
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

#[derive(Debug)]
pub struct DirectoryCursor {
    entries: Vec<DirectoryEntry>,
    next_cookie: u64,
    cookies: BTreeMap<u64, usize>,
    cookie_by_index: BTreeMap<usize, u64>,
}

impl DirectoryCursor {
    pub fn new(mut entries: Vec<DirectoryEntry>, max_spool_bytes: usize) -> WireResult<Self> {
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
        Ok(Self {
            entries,
            next_cookie: 1,
            cookies: BTreeMap::new(),
            cookie_by_index: BTreeMap::new(),
        })
    }

    pub fn page(&mut self, after: u64, limit: usize) -> WireResult<(u64, Vec<DirectoryEntry>)> {
        if limit == 0 {
            return Err(WireError::invalid(
                "directory cursor",
                "page limit must be non-zero",
            ));
        }
        if after != 0 && !self.cookies.contains_key(&after) {
            return Err(WireError::invalid("directory cursor", "unknown cookie"));
        }
        let index = self.cookies.get(&after).copied().unwrap_or(0);
        let end = index.saturating_add(limit).min(self.entries.len());
        let page = self.entries[index..end].to_vec();
        let cookie = if end == self.entries.len() {
            0
        } else if let Some(cookie) = self.cookie_by_index.get(&end).copied() {
            cookie
        } else {
            let cookie = self.next_cookie;
            self.next_cookie = self
                .next_cookie
                .checked_add(1)
                .ok_or_else(|| WireError::LimitExceeded("directory cookie overflow".into()))?;
            self.cookies.insert(cookie, end);
            self.cookie_by_index.insert(end, cookie);
            cookie
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
}
