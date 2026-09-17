//! Frozen metadata primitives (PR08/PR09).
//!
//! The P2 format is deliberately separate from the P1 KV head.  This module
//! owns the canonical inode/dentry/extent rows, authenticated snapshot
//! manifest, bounded directory cursor, and the prefix-scoped extent lookup
//! rule.  Mount admission does not enable it unless the persisted volume
//! header explicitly requires `frozen-base-metadata`.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::native_base::seal::source::{ObjectSource, ObjectSourceError};
use crate::native_base::wire::container::{
    Codec, ContainerHeader, FOOTER_LEN, HEADER_LEN, ObjectKind, parse_footer,
};
use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::page::{IndexPage, PageBody};
use crate::native_base::wire::refs::{
    ChildRef, MAX_INDEX_LEVEL, PageKind, RootRef, ensure_object_kind_allows_page,
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
}

/// A fixed revision reader owns no mutable KV client.  Every lookup is served
/// from the authenticated manifest and its external pages; the counter is
/// intentionally exposed for tests and observability to prove that inode
/// lookups do not silently fall back to a KV RPC.
pub struct FixedRevisionReader<'a, S: ObjectSource> {
    source: &'a S,
    manifest: SnapshotManifest,
    metadata_rpc_count: std::sync::atomic::AtomicU64,
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
        })
    }

    pub fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    pub fn metadata_rpc_count(&self) -> u64 {
        self.metadata_rpc_count
            .load(std::sync::atomic::Ordering::Relaxed)
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
    spool_bytes: usize,
    max_spool_bytes: usize,
}

impl DirectoryCursor {
    pub fn new(mut entries: Vec<DirectoryEntry>, max_spool_bytes: usize) -> WireResult<Self> {
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        if entries.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err(WireError::invalid("directory", "duplicate names"));
        }
        let spool_bytes = entries.iter().map(|entry| entry.name.len() + 24).sum();
        if spool_bytes > max_spool_bytes || spool_bytes > MAX_DIRECTORY_COOKIE_SPOOL {
            return Err(WireError::LimitExceeded(
                "directory cookie spool budget".into(),
            ));
        }
        Ok(Self {
            entries,
            next_cookie: 1,
            cookies: BTreeMap::new(),
            spool_bytes,
            max_spool_bytes,
        })
    }

    pub fn page(&mut self, after: u64, limit: usize) -> WireResult<(u64, Vec<DirectoryEntry>)> {
        if after != 0 && !self.cookies.contains_key(&after) {
            return Err(WireError::invalid("directory cursor", "unknown cookie"));
        }
        let start = after.checked_add(1).unwrap_or(u64::MAX);
        let index = self.cookies.get(&after).copied().unwrap_or(0);
        let end = index.saturating_add(limit).min(self.entries.len());
        let page = self.entries[index..end].to_vec();
        let cookie = if end == self.entries.len() {
            0
        } else {
            let cookie = self.next_cookie;
            self.next_cookie = self
                .next_cookie
                .checked_add(1)
                .ok_or_else(|| WireError::LimitExceeded("directory cookie overflow".into()))?;
            self.cookies.insert(cookie, end);
            cookie
        };
        let _ = (start, self.spool_bytes, self.max_spool_bytes);
        Ok((cookie, page))
    }
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
        let (cookie, first) = cursor.page(0, 2).unwrap();
        assert_eq!(first[0].name, b"a");
        let (last, second) = cursor.page(cookie, 2).unwrap();
        assert_eq!(last, 0);
        assert_eq!(second[0].name, b"c");
        assert!(cursor.page(cookie + 100, 1).is_err());
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
}
