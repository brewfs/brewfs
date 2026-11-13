//! Deterministic `.brfsm` manifest assembly for clustered metadata v2.
//!
//! This module owns the immutable wire envelope only.  The mount and merge
//! route payloads are opaque, independently authenticated sections for now;
//! their paged index codecs and remote range reader are separate milestones.

use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::uvarint::{Reader, Writer};

use super::cluster_format::{
    FORMAT_MAJOR, FORMAT_MINOR, IndexRootRef, SUPERBLOCK_LEN, SnapshotSuperblock,
};
use super::identity::DirKey;
use super::merge_route::MergeRouteIndex;
use super::mount_trie::MountTrie;

pub(crate) const SECTION_MAGIC: &[u8; 8] = b"BRFMSEC2";
pub(crate) const FOOTER_MAGIC: &[u8; 8] = b"BRFMEND2";
pub(crate) const SECTION_HEADER_LEN: usize = 48;
pub(crate) const FOOTER_LEN: usize = 64;
pub(crate) const MAX_MANIFEST_OBJECT_LEN: usize = 256 * 1024 * 1024;
pub(crate) const MAX_MANIFEST_SECTION_LEN: usize = 64 * 1024 * 1024;
pub(crate) const MAX_MANIFEST_CLUSTERS: usize = 1 << 20;
const MAX_OBJECT_KEY_LEN: usize = 1024;

/// Root order in [`SnapshotSuperblock::index_roots`].
pub const MOUNT_INDEX_ROOT: usize = 0;
pub const CLUSTER_TABLE_ROOT: usize = 1;
pub const ROUTE_INDEX_ROOT: usize = 2;

pub(crate) const SECTION_MOUNT: u8 = 1;
pub(crate) const SECTION_CLUSTER_TABLE: u8 = 2;
pub(crate) const SECTION_ROUTE: u8 = 3;

/// A manifest object reference.  Keys are UTF-8 object-store keys and hashes
/// authenticate the complete referenced object, not its diagnostic ETag.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestObjectRef {
    pub object_id: [u8; 16],
    pub kind: u8,
    pub object_len: u64,
    pub full_hash: [u8; 32],
    pub key: Vec<u8>,
}

/// One immutable cluster selected by a snapshot manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterDescriptor {
    pub cluster_id: [u8; 16],
    pub metadata_ref: ManifestObjectRef,
    pub data_seal_ref: ManifestObjectRef,
    pub combined_semantic_hash: [u8; 32],
    pub mount_dir_key: DirKey,
    pub root_local_node_id: u32,
    pub flags: u32,
}

/// An opaque but authenticated manifest index section.
///
/// The payload is deliberately not decoded here.  This lets the fixed
/// manifest envelope land before the paged mount-trie and route-index codecs,
/// while preserving their exact bytes and entry counts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestIndexPayload {
    pub entry_count: u64,
    pub bytes: Vec<u8>,
}

/// Complete deterministic `.brfsm` object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotManifest {
    pub superblock: SnapshotSuperblock,
    pub clusters: Vec<ClusterDescriptor>,
    pub mount_index: ManifestIndexPayload,
    pub route_index: ManifestIndexPayload,
}

impl ManifestIndexPayload {
    /// Decode a typed v2 mount trie and verify that its authenticated record
    /// count agrees with the manifest section root.
    pub fn decode_mount_trie(&self) -> WireResult<MountTrie> {
        let index = MountTrie::index(&self.bytes)?;
        if index.node_count != self.entry_count {
            return Err(WireError::invalid(
                "mount trie",
                "node count does not match manifest section",
            ));
        }
        MountTrie::decode(&self.bytes)
    }

    /// Decode a typed v2 merge-route index and verify its record count.
    pub fn decode_merge_routes(&self) -> WireResult<Vec<super::merge_route::MergeRouteRecord>> {
        let index = MergeRouteIndex::index(&self.bytes)?;
        if index.directory_count != self.entry_count {
            return Err(WireError::invalid(
                "merge route",
                "directory count does not match manifest section",
            ));
        }
        MergeRouteIndex::decode(&self.bytes)
    }
}

impl ManifestObjectRef {
    fn encode_into(&self, writer: &mut Writer) -> WireResult<()> {
        validate_object_ref(self)?;
        writer.put(&self.object_id);
        writer.u8(self.kind);
        writer.put(&[0; 3]);
        writer.u64(self.object_len);
        writer.put(&self.full_hash);
        writer.bytes(&self.key);
        Ok(())
    }

    fn decode(reader: &mut Reader<'_>) -> WireResult<Self> {
        let object_id = reader.take(16, "manifest object ref")?.try_into().unwrap();
        let kind = reader.u8("manifest object ref")?;
        let reserved = reader.take(3, "manifest object ref")?;
        if reserved.iter().any(|byte| *byte != 0) {
            return Err(WireError::invalid(
                "manifest object ref",
                "reserved bytes are non-zero",
            ));
        }
        let object_len = reader.u64("manifest object ref")?;
        let full_hash = reader.take(32, "manifest object ref")?.try_into().unwrap();
        let key_bytes = reader.bytes("manifest object key")?;
        if key_bytes.len() > MAX_OBJECT_KEY_LEN {
            return Err(WireError::LimitExceeded(format!(
                "manifest object key exceeds {MAX_OBJECT_KEY_LEN} bytes"
            )));
        }
        let key = key_bytes.to_vec();
        let object = Self {
            object_id,
            kind,
            object_len,
            full_hash,
            key,
        };
        validate_object_ref(&object)?;
        Ok(object)
    }
}

impl ClusterDescriptor {
    fn encode_into(&self, writer: &mut Writer) -> WireResult<()> {
        if self.root_local_node_id != 1 {
            return Err(WireError::invalid(
                "cluster descriptor",
                "root_local_node_id must be 1",
            ));
        }
        writer.put(&self.cluster_id);
        self.metadata_ref.encode_into(writer)?;
        self.data_seal_ref.encode_into(writer)?;
        writer.put(&self.combined_semantic_hash);
        writer.put(self.mount_dir_key.as_ref());
        writer.u32(self.root_local_node_id);
        writer.u32(self.flags);
        Ok(())
    }

    fn decode(reader: &mut Reader<'_>) -> WireResult<Self> {
        let cluster_id = reader.take(16, "cluster descriptor")?.try_into().unwrap();
        let metadata_ref = ManifestObjectRef::decode(reader)?;
        let data_seal_ref = ManifestObjectRef::decode(reader)?;
        let combined_semantic_hash = reader.take(32, "cluster descriptor")?.try_into().unwrap();
        let mount_dir_key = DirKey::new(reader.take(16, "cluster descriptor")?.try_into().unwrap());
        let root_local_node_id = reader.u32("cluster descriptor")?;
        let flags = reader.u32("cluster descriptor")?;
        let descriptor = Self {
            cluster_id,
            metadata_ref,
            data_seal_ref,
            combined_semantic_hash,
            mount_dir_key,
            root_local_node_id,
            flags,
        };
        if descriptor.root_local_node_id != 1 {
            return Err(WireError::invalid(
                "cluster descriptor",
                "root_local_node_id must be 1",
            ));
        }
        Ok(descriptor)
    }
}

impl SnapshotManifest {
    /// Encode the complete manifest.  The section roots in the superblock are
    /// recomputed from the encoded bytes, so repeated builds are byte-identical
    /// for identical inputs.
    pub fn encode(&self) -> WireResult<Vec<u8>> {
        self.validate_counts()?;
        validate_index_payload("mount index", &self.mount_index)?;
        validate_index_payload("route index", &self.route_index)?;

        let cluster_payload = encode_cluster_table(&self.clusters)?;
        let mount_section = encode_section(SECTION_MOUNT, &self.mount_index.bytes)?;
        let cluster_section = encode_section(SECTION_CLUSTER_TABLE, &cluster_payload)?;
        let route_section = encode_section(SECTION_ROUTE, &self.route_index.bytes)?;

        let mut offset = SUPERBLOCK_LEN as u64;
        let mount_root = section_root(
            offset,
            &mount_section,
            self.mount_index.entry_count,
            SECTION_MOUNT,
        )?;
        offset = checked_section_end(offset, mount_section.len())?;
        let cluster_root = section_root(
            offset,
            &cluster_section,
            self.clusters.len() as u64,
            SECTION_CLUSTER_TABLE,
        )?;
        offset = checked_section_end(offset, cluster_section.len())?;
        let route_root = section_root(
            offset,
            &route_section,
            self.route_index.entry_count,
            SECTION_ROUTE,
        )?;
        offset = checked_section_end(offset, route_section.len())?;
        let footer_offset = usize::try_from(offset)
            .map_err(|_| WireError::LimitExceeded("manifest length exceeds usize".into()))?;
        let total_len = footer_offset
            .checked_add(FOOTER_LEN)
            .ok_or_else(|| WireError::LimitExceeded("manifest length overflows usize".into()))?;
        if total_len > MAX_MANIFEST_OBJECT_LEN {
            return Err(WireError::LimitExceeded(format!(
                "manifest exceeds {MAX_MANIFEST_OBJECT_LEN} bytes"
            )));
        }

        let mut superblock = self.superblock.clone();
        superblock.index_roots = [mount_root, cluster_root, route_root];
        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&superblock.encode());
        bytes.extend_from_slice(&mount_section);
        bytes.extend_from_slice(&cluster_section);
        bytes.extend_from_slice(&route_section);
        debug_assert_eq!(bytes.len(), footer_offset);
        bytes.extend_from_slice(&encode_footer(&bytes, total_len));
        Ok(bytes)
    }

    /// Decode and authenticate a complete `.brfsm` object.
    pub fn decode(bytes: &[u8]) -> WireResult<Self> {
        if bytes.len() < SUPERBLOCK_LEN + FOOTER_LEN {
            return Err(WireError::Truncated {
                what: "snapshot manifest",
                need: SUPERBLOCK_LEN + FOOTER_LEN,
                have: bytes.len(),
            });
        }
        if bytes.len() > MAX_MANIFEST_OBJECT_LEN {
            return Err(WireError::LimitExceeded(format!(
                "manifest exceeds {MAX_MANIFEST_OBJECT_LEN} bytes"
            )));
        }
        let footer_offset = bytes.len() - FOOTER_LEN;
        decode_footer(&bytes[footer_offset..], bytes, footer_offset)?;
        let superblock = SnapshotSuperblock::decode(bytes)?;
        let roots = &superblock.index_roots;

        let mut offset = SUPERBLOCK_LEN as u64;
        let (mount_payload, mount_count) = decode_section(
            bytes,
            &roots[MOUNT_INDEX_ROOT],
            offset,
            SECTION_MOUNT,
            footer_offset,
        )?;
        offset = checked_section_end(offset, roots[MOUNT_INDEX_ROOT].stored_len as usize)?;
        let (cluster_payload, cluster_count) = decode_section(
            bytes,
            &roots[CLUSTER_TABLE_ROOT],
            offset,
            SECTION_CLUSTER_TABLE,
            footer_offset,
        )?;
        offset = checked_section_end(offset, roots[CLUSTER_TABLE_ROOT].stored_len as usize)?;
        let (route_payload, route_count) = decode_section(
            bytes,
            &roots[ROUTE_INDEX_ROOT],
            offset,
            SECTION_ROUTE,
            footer_offset,
        )?;
        offset = checked_section_end(offset, roots[ROUTE_INDEX_ROOT].stored_len as usize)?;
        if usize::try_from(offset).unwrap() != footer_offset {
            return Err(WireError::invalid(
                "snapshot manifest",
                "unreferenced bytes before footer",
            ));
        }

        let clusters = decode_cluster_table(&cluster_payload, cluster_count)?;
        if superblock.cluster_count as usize != clusters.len() {
            return Err(WireError::invalid(
                "snapshot manifest",
                "cluster count does not match superblock",
            ));
        }
        if u64::from(superblock.mount_count) != mount_count {
            return Err(WireError::invalid(
                "snapshot manifest",
                "mount count does not match superblock",
            ));
        }
        if superblock.merged_directory_count != route_count {
            return Err(WireError::invalid(
                "snapshot manifest",
                "route count does not match superblock",
            ));
        }

        Ok(Self {
            superblock,
            clusters,
            mount_index: ManifestIndexPayload {
                entry_count: mount_count,
                bytes: mount_payload,
            },
            route_index: ManifestIndexPayload {
                entry_count: route_count,
                bytes: route_payload,
            },
        })
    }

    fn validate_counts(&self) -> WireResult<()> {
        if self.superblock.cluster_count as usize != self.clusters.len() {
            return Err(WireError::invalid(
                "snapshot manifest",
                "cluster count does not match superblock",
            ));
        }
        if u64::from(self.superblock.mount_count) != self.mount_index.entry_count {
            return Err(WireError::invalid(
                "snapshot manifest",
                "mount count does not match superblock",
            ));
        }
        if self.superblock.merged_directory_count != self.route_index.entry_count {
            return Err(WireError::invalid(
                "snapshot manifest",
                "route count does not match superblock",
            ));
        }
        if self.clusters.len() > MAX_MANIFEST_CLUSTERS {
            return Err(WireError::LimitExceeded(format!(
                "cluster count exceeds {MAX_MANIFEST_CLUSTERS}"
            )));
        }
        let mut ids = self
            .clusters
            .iter()
            .map(|cluster| cluster.cluster_id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(WireError::invalid(
                "snapshot manifest",
                "duplicate cluster id",
            ));
        }
        Ok(())
    }
}

fn validate_object_ref(object: &ManifestObjectRef) -> WireResult<()> {
    if object.kind == 0 {
        return Err(WireError::invalid(
            "manifest object ref",
            "object kind must be non-zero",
        ));
    }
    if object.key.is_empty() || object.key.len() > MAX_OBJECT_KEY_LEN {
        return Err(WireError::LimitExceeded(format!(
            "manifest object key length must be in 1..={MAX_OBJECT_KEY_LEN}"
        )));
    }
    if object.key.contains(&0) || std::str::from_utf8(&object.key).is_err() {
        return Err(WireError::invalid(
            "manifest object key",
            "key must be UTF-8 without NUL",
        ));
    }
    Ok(())
}

fn validate_index_payload(what: &'static str, payload: &ManifestIndexPayload) -> WireResult<()> {
    if payload.bytes.len() > MAX_MANIFEST_SECTION_LEN {
        return Err(WireError::LimitExceeded(format!(
            "{what} exceeds {MAX_MANIFEST_SECTION_LEN} bytes"
        )));
    }
    Ok(())
}

fn encode_cluster_table(clusters: &[ClusterDescriptor]) -> WireResult<Vec<u8>> {
    if clusters.len() > MAX_MANIFEST_CLUSTERS {
        return Err(WireError::LimitExceeded(format!(
            "cluster count exceeds {MAX_MANIFEST_CLUSTERS}"
        )));
    }
    let mut writer = Writer::new();
    writer.uvarint(clusters.len() as u64);
    for cluster in clusters {
        cluster.encode_into(&mut writer)?;
    }
    Ok(writer.into_bytes())
}

pub(crate) fn decode_cluster_table(
    payload: &[u8],
    expected_count: u64,
) -> WireResult<Vec<ClusterDescriptor>> {
    let mut reader = Reader::new(payload);
    let count = reader.uvarint("cluster table")?;
    if count != expected_count {
        return Err(WireError::invalid(
            "cluster table",
            "entry count does not match section root",
        ));
    }
    let count = usize::try_from(count)
        .map_err(|_| WireError::LimitExceeded("cluster count exceeds usize".into()))?;
    if count > MAX_MANIFEST_CLUSTERS {
        return Err(WireError::LimitExceeded(format!(
            "cluster count exceeds {MAX_MANIFEST_CLUSTERS}"
        )));
    }
    let mut clusters = Vec::with_capacity(count);
    for _ in 0..count {
        clusters.push(ClusterDescriptor::decode(&mut reader)?);
    }
    if !reader.is_empty() {
        return Err(WireError::invalid(
            "cluster table",
            "trailing bytes after descriptors",
        ));
    }
    let mut ids = clusters
        .iter()
        .map(|cluster| cluster.cluster_id)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(WireError::invalid("cluster table", "duplicate cluster id"));
    }
    Ok(clusters)
}

fn encode_section(kind: u8, payload: &[u8]) -> WireResult<Vec<u8>> {
    if payload.len() > MAX_MANIFEST_SECTION_LEN || payload.len() > u32::MAX as usize {
        return Err(WireError::LimitExceeded(format!(
            "manifest section exceeds {MAX_MANIFEST_SECTION_LEN} bytes"
        )));
    }
    let digest = blake3::hash(payload);
    let mut section = vec![0u8; SECTION_HEADER_LEN + payload.len()];
    section[..8].copy_from_slice(SECTION_MAGIC);
    section[8] = kind;
    section[12..16].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    section[16..48].copy_from_slice(digest.as_bytes());
    section[SECTION_HEADER_LEN..].copy_from_slice(payload);
    Ok(section)
}

fn section_root(
    offset: u64,
    section: &[u8],
    entry_count: u64,
    kind: u8,
) -> WireResult<IndexRootRef> {
    let stored_len = u32::try_from(section.len())
        .map_err(|_| WireError::LimitExceeded("manifest section length exceeds u32".into()))?;
    let raw_len = u32::try_from(section.len() - SECTION_HEADER_LEN)
        .map_err(|_| WireError::LimitExceeded("manifest payload length exceeds u32".into()))?;
    let entry_count = u32::try_from(entry_count)
        .map_err(|_| WireError::LimitExceeded("manifest entry count exceeds u32".into()))?;
    Ok(IndexRootRef {
        object_offset: offset,
        stored_len,
        raw_len,
        level: 0,
        kind,
        entry_count,
        digest: *blake3::hash(section).as_bytes(),
        key_fingerprint: 0,
    })
}

fn checked_section_end(offset: u64, len: usize) -> WireResult<u64> {
    offset
        .checked_add(
            u64::try_from(len).map_err(|_| {
                WireError::LimitExceeded("manifest section length exceeds u64".into())
            })?,
        )
        .ok_or_else(|| WireError::LimitExceeded("manifest section offset overflows u64".into()))
}

fn decode_section(
    bytes: &[u8],
    root: &IndexRootRef,
    expected_offset: u64,
    expected_kind: u8,
    footer_offset: usize,
) -> WireResult<(Vec<u8>, u64)> {
    if root.object_offset != expected_offset || root.kind != expected_kind {
        return Err(WireError::invalid(
            "manifest section root",
            "section order or kind is invalid",
        ));
    }
    let stored_len = usize::try_from(root.stored_len)
        .map_err(|_| WireError::LimitExceeded("manifest section length exceeds usize".into()))?;
    if !(SECTION_HEADER_LEN..=MAX_MANIFEST_SECTION_LEN + SECTION_HEADER_LEN).contains(&stored_len) {
        return Err(WireError::LimitExceeded(
            "manifest section length is outside bounds".into(),
        ));
    }
    let start = usize::try_from(root.object_offset)
        .map_err(|_| WireError::LimitExceeded("manifest section offset exceeds usize".into()))?;
    let end = start
        .checked_add(stored_len)
        .ok_or_else(|| WireError::LimitExceeded("manifest section end overflows usize".into()))?;
    if end > footer_offset || end > bytes.len() {
        return Err(WireError::Truncated {
            what: "manifest section",
            need: stored_len,
            have: footer_offset.saturating_sub(start),
        });
    }
    let section = &bytes[start..end];
    decode_section_range(section, root, expected_kind)
}

/// Decode and authenticate one complete section fetched independently from a
/// manifest object.  The caller is responsible for checking the absolute
/// object range before issuing the fetch; this helper only validates bytes
/// covered by the section root itself.
pub(crate) fn decode_section_range(
    section: &[u8],
    root: &IndexRootRef,
    expected_kind: u8,
) -> WireResult<(Vec<u8>, u64)> {
    let stored_len = usize::try_from(root.stored_len)
        .map_err(|_| WireError::LimitExceeded("manifest section length exceeds usize".into()))?;
    if !(SECTION_HEADER_LEN..=MAX_MANIFEST_SECTION_LEN + SECTION_HEADER_LEN).contains(&stored_len) {
        return Err(WireError::LimitExceeded(
            "manifest section length is outside bounds".into(),
        ));
    }
    if section.len() != stored_len {
        return Err(WireError::Truncated {
            what: "manifest section",
            need: stored_len,
            have: section.len(),
        });
    }
    if root.kind != expected_kind {
        return Err(WireError::invalid(
            "manifest section root",
            "section kind is invalid",
        ));
    }
    if &section[..8] != SECTION_MAGIC || section[8] != expected_kind {
        return Err(WireError::UnsupportedFormat(
            "manifest section magic or kind mismatch".into(),
        ));
    }
    if section[9..12].iter().any(|byte| *byte != 0) {
        return Err(WireError::invalid(
            "manifest section",
            "reserved bytes are non-zero",
        ));
    }
    let payload_len = u32::from_le_bytes(section[12..16].try_into().unwrap()) as usize;
    let expected_stored_len = payload_len.checked_add(SECTION_HEADER_LEN).ok_or_else(|| {
        WireError::LimitExceeded("manifest section length overflows usize".into())
    })?;
    if expected_stored_len != section.len() || payload_len != root.raw_len as usize {
        return Err(WireError::invalid(
            "manifest section",
            "payload length does not match root",
        ));
    }
    let payload = &section[SECTION_HEADER_LEN..];
    let stored_payload_digest = &section[16..48];
    let computed_payload_digest = blake3::hash(payload);
    if stored_payload_digest != computed_payload_digest.as_bytes() {
        return Err(WireError::HashMismatch {
            what: "manifest section payload",
            stored: hex::encode(stored_payload_digest),
            computed: hex::encode(computed_payload_digest.as_bytes()),
        });
    }
    let computed_section_digest = blake3::hash(section);
    if root.digest != *computed_section_digest.as_bytes() {
        return Err(WireError::HashMismatch {
            what: "manifest section",
            stored: hex::encode(root.digest),
            computed: hex::encode(computed_section_digest.as_bytes()),
        });
    }
    Ok((payload.to_vec(), u64::from(root.entry_count)))
}

fn encode_footer(bytes_without_footer: &[u8], total_len: usize) -> [u8; FOOTER_LEN] {
    let mut footer = [0u8; FOOTER_LEN];
    footer[..8].copy_from_slice(FOOTER_MAGIC);
    footer[8..10].copy_from_slice(&FORMAT_MAJOR.to_le_bytes());
    footer[10..12].copy_from_slice(&FORMAT_MINOR.to_le_bytes());
    footer[16..24].copy_from_slice(&(total_len as u64).to_le_bytes());
    footer[24..56].copy_from_slice(blake3::hash(bytes_without_footer).as_bytes());
    let crc = crc32c::crc32c(&footer[..56]);
    footer[56..60].copy_from_slice(&crc.to_le_bytes());
    footer
}

/// Validate the fixed footer fields without reading the content hash input.
/// A remote reader can use this before streaming the bytes covered by the
/// footer hash; the complete in-memory decoder calls this helper as part of
/// its full-object verification.
pub(crate) fn decode_footer_header(footer: &[u8], object_len: u64) -> WireResult<[u8; 32]> {
    if footer.len() != FOOTER_LEN {
        return Err(WireError::Truncated {
            what: "manifest footer",
            need: FOOTER_LEN,
            have: footer.len(),
        });
    }
    if &footer[..8] != FOOTER_MAGIC {
        return Err(WireError::UnsupportedFormat(
            "manifest footer magic mismatch".into(),
        ));
    }
    if u16::from_le_bytes(footer[8..10].try_into().unwrap()) != FORMAT_MAJOR
        || u16::from_le_bytes(footer[10..12].try_into().unwrap()) != FORMAT_MINOR
    {
        return Err(WireError::UnsupportedFormat(
            "unsupported manifest footer version".into(),
        ));
    }
    if footer[12..16].iter().any(|byte| *byte != 0) || footer[60..].iter().any(|byte| *byte != 0) {
        return Err(WireError::invalid(
            "manifest footer",
            "reserved bytes are non-zero",
        ));
    }
    let stored_crc = u32::from_le_bytes(footer[56..60].try_into().unwrap());
    let computed_crc = crc32c::crc32c(&footer[..56]);
    if stored_crc != computed_crc {
        return Err(WireError::CrcMismatch {
            what: "manifest footer",
            stored: stored_crc,
            computed: computed_crc,
        });
    }
    if u64::from_le_bytes(footer[16..24].try_into().unwrap()) != object_len {
        return Err(WireError::invalid(
            "manifest footer",
            "object length does not match input",
        ));
    }
    Ok(footer[24..56].try_into().unwrap())
}

fn decode_footer(footer: &[u8], complete: &[u8], footer_offset: usize) -> WireResult<()> {
    let stored_hash = decode_footer_header(footer, complete.len() as u64)?;
    if footer_offset != complete.len() - FOOTER_LEN {
        return Err(WireError::invalid(
            "manifest footer",
            "object length does not match input",
        ));
    }
    let computed_hash = blake3::hash(&complete[..footer_offset]);
    if stored_hash != *computed_hash.as_bytes() {
        return Err(WireError::HashMismatch {
            what: "snapshot manifest",
            stored: hex::encode(stored_hash),
            computed: hex::encode(computed_hash.as_bytes()),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(seed: u8, kind: u8, key: &str) -> ManifestObjectRef {
        ManifestObjectRef {
            object_id: [seed; 16],
            kind,
            object_len: 4096 + u64::from(seed),
            full_hash: [seed.wrapping_add(1); 32],
            key: key.as_bytes().to_vec(),
        }
    }

    fn manifest() -> SnapshotManifest {
        let clusters = vec![ClusterDescriptor {
            cluster_id: [0x11; 16],
            metadata_ref: object(1, 1, "meta/11.brfc"),
            data_seal_ref: object(2, 2, "seal/11.brfds"),
            combined_semantic_hash: [0x33; 32],
            mount_dir_key: DirKey::new([0x44; 16]),
            root_local_node_id: 1,
            flags: 0,
        }];
        SnapshotManifest {
            superblock: SnapshotSuperblock {
                volume_id: [0x55; 16],
                snapshot_id: [0x66; 16],
                semantic_hash: [0x77; 32],
                route_seed: [0x88; 16],
                cluster_count: 1,
                mount_count: 2,
                merged_directory_count: 3,
                root_dir_key: DirKey::new([0x99; 16]),
                index_roots: [empty_root(), empty_root(), empty_root()],
            },
            clusters,
            mount_index: ManifestIndexPayload {
                entry_count: 2,
                bytes: b"mount-index".to_vec(),
            },
            route_index: ManifestIndexPayload {
                entry_count: 3,
                bytes: b"route-index".to_vec(),
            },
        }
    }

    fn empty_root() -> IndexRootRef {
        IndexRootRef {
            object_offset: 0,
            stored_len: 0,
            raw_len: 0,
            level: 0,
            kind: 0,
            entry_count: 0,
            digest: [0; 32],
            key_fingerprint: 0,
        }
    }

    #[test]
    fn manifest_round_trips_deterministically() {
        let manifest = manifest();
        let first = manifest.encode().unwrap();
        let second = manifest.encode().unwrap();
        assert_eq!(first, second);
        assert_eq!(
            SnapshotManifest::decode(&first).unwrap(),
            manifest_with_roots(&manifest)
        );
    }

    #[test]
    fn manifest_rejects_section_tampering() {
        let manifest = manifest();
        let mut encoded = manifest.encode().unwrap();
        encoded[SUPERBLOCK_LEN + SECTION_HEADER_LEN] ^= 1;
        assert!(matches!(
            SnapshotManifest::decode(&encoded),
            Err(WireError::HashMismatch { .. })
        ));
    }

    #[test]
    fn manifest_rejects_duplicate_cluster_ids() {
        let mut manifest = manifest();
        manifest.clusters.push(manifest.clusters[0].clone());
        manifest.superblock.cluster_count = 2;
        assert!(matches!(
            manifest.encode(),
            Err(WireError::Invalid {
                what: "snapshot manifest",
                ..
            })
        ));
    }

    #[test]
    fn typed_mount_and_route_sections_round_trip_through_manifest() {
        let mut manifest = manifest();
        let mount = super::super::mount_trie::MountTrie {
            nodes: vec![super::super::mount_trie::MountTrieNode {
                node_id: 1,
                parent_id: 0,
                component: None,
                cluster_slots: vec![0],
                children: Vec::new(),
            }],
        };
        let route = super::super::merge_route::MergeRouteIndex::encode(&[]).unwrap();
        manifest.mount_index.bytes = mount.encode().unwrap();
        manifest.mount_index.entry_count = 1;
        manifest.route_index.bytes = route;
        manifest.route_index.entry_count = 0;
        manifest.superblock.mount_count = 1;
        manifest.superblock.merged_directory_count = 0;
        let decoded = SnapshotManifest::decode(&manifest.encode().unwrap()).unwrap();
        assert_eq!(decoded.mount_index.decode_mount_trie().unwrap(), mount);
        assert!(
            decoded
                .route_index
                .decode_merge_routes()
                .unwrap()
                .is_empty()
        );
    }

    fn manifest_with_roots(input: &SnapshotManifest) -> SnapshotManifest {
        let bytes = input.encode().unwrap();
        let superblock = SnapshotSuperblock::decode(&bytes).unwrap();
        SnapshotManifest {
            superblock,
            clusters: input.clusters.clone(),
            mount_index: input.mount_index.clone(),
            route_index: input.route_index.clone(),
        }
    }
}
