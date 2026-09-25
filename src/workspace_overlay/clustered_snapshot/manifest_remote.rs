//! Range-oriented reader for immutable `BRFSM002` snapshot manifests.
//!
//! A manifest is the publication boundary for a snapshot, but opening it
//! must not materialize every mount-trie and merge-route page.  This reader
//! fetches the fixed superblock first, then the small cluster descriptor
//! section.  Mount and route sections are fetched only when their accessors
//! are called and are single-flight cached after authentication.

use std::sync::Arc;

use tokio::sync::OnceCell;

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::native_base::wire::error::{WireError, WireResult};

use super::cluster_format::{IndexRootRef, SUPERBLOCK_LEN, SnapshotSuperblock};
use super::merge_route::{MergeRouteIndex, MergeRouteRecord};
use super::mount_trie::{MountTrie, MountTrieIndex};
use super::snapshot_manifest::{
    CLUSTER_TABLE_ROOT, ClusterDescriptor, FOOTER_LEN, MAX_MANIFEST_OBJECT_LEN,
    MAX_MANIFEST_SECTION_LEN, MOUNT_INDEX_ROOT, ManifestIndexPayload, ManifestObjectRef,
    ROUTE_INDEX_ROOT, SECTION_CLUSTER_TABLE, SECTION_HEADER_LEN, SECTION_MOUNT, SECTION_ROUTE,
    SnapshotManifest, decode_cluster_table, decode_footer_header, decode_section_range,
};

/// A section range is bounded by the on-disk manifest section limit plus its
/// fixed header.  The object backend never receives an unbounded allocation
/// request from this reader.
pub const MAX_MANIFEST_RANGE_BYTES: usize = MAX_MANIFEST_SECTION_LEN + SECTION_HEADER_LEN;

/// A snapshot manifest with variable sections loaded on demand.
pub struct RemoteSnapshotManifest<B: ObjectBackend + Clone> {
    client: ObjectClient<B>,
    object_key: String,
    object_len: u64,
    object_ref: ManifestObjectRef,
    superblock: SnapshotSuperblock,
    clusters: Arc<[ClusterDescriptor]>,
    mount_index: OnceCell<ManifestIndexPayload>,
    route_index: OnceCell<ManifestIndexPayload>,
    mount_trie_index: OnceCell<MountTrieIndex>,
    merge_route_index: OnceCell<MergeRouteIndex>,
}

impl<B: ObjectBackend + Clone> RemoteSnapshotManifest<B> {
    /// Open a manifest with exactly two range reads: its fixed superblock and
    /// its cluster descriptor section.  Mount and route bytes stay remote.
    pub async fn open(client: &ObjectClient<B>, object_ref: ManifestObjectRef) -> WireResult<Self> {
        if object_ref.kind == 0 {
            return Err(WireError::invalid(
                "snapshot manifest",
                "manifest object kind must be non-zero",
            ));
        }
        if object_ref.key.is_empty() || object_ref.key.len() > 1024 || object_ref.key.contains(&0) {
            return Err(WireError::invalid(
                "snapshot manifest",
                "manifest object key must be non-empty and contain no NUL",
            ));
        }
        let object_key = std::str::from_utf8(&object_ref.key)
            .map_err(|_| WireError::invalid("snapshot manifest", "object key is not UTF-8"))?
            .to_owned();
        if object_ref.object_len < (SUPERBLOCK_LEN + FOOTER_LEN) as u64 {
            return Err(WireError::Truncated {
                what: "snapshot manifest",
                need: SUPERBLOCK_LEN + FOOTER_LEN,
                have: object_ref.object_len as usize,
            });
        }
        if object_ref.object_len > MAX_MANIFEST_OBJECT_LEN as u64 {
            return Err(WireError::LimitExceeded(format!(
                "manifest exceeds {MAX_MANIFEST_OBJECT_LEN} bytes"
            )));
        }

        let reader = Self {
            client: client.clone(),
            object_key,
            object_len: object_ref.object_len,
            object_ref,
            superblock: SnapshotSuperblock {
                volume_id: [0; 16],
                snapshot_id: [0; 16],
                semantic_hash: [0; 32],
                route_seed: [0; 16],
                cluster_count: 0,
                mount_count: 0,
                merged_directory_count: 0,
                root_dir_key: super::identity::DirKey::new([0; 16]),
                index_roots: [empty_root(), empty_root(), empty_root()],
            },
            clusters: Arc::from([]),
            mount_index: OnceCell::new(),
            route_index: OnceCell::new(),
            mount_trie_index: OnceCell::new(),
            merge_route_index: OnceCell::new(),
        };
        let superblock_bytes = reader.fetch_range(0, SUPERBLOCK_LEN as u32).await?;
        let superblock = SnapshotSuperblock::decode(&superblock_bytes)?;
        reader.validate_roots(&superblock)?;

        let cluster_root = &superblock.index_roots[CLUSTER_TABLE_ROOT];
        let cluster_bytes = reader
            .fetch_range(cluster_root.object_offset, cluster_root.stored_len)
            .await?;
        let (cluster_payload, cluster_count) =
            decode_section_range(&cluster_bytes, cluster_root, SECTION_CLUSTER_TABLE)?;
        let clusters = decode_cluster_table(&cluster_payload, cluster_count)?;
        if clusters.len() != superblock.cluster_count as usize {
            return Err(WireError::invalid(
                "snapshot manifest",
                "cluster count does not match superblock",
            ));
        }

        Ok(Self {
            superblock,
            clusters: Arc::from(clusters),
            ..reader
        })
    }

    pub fn object_ref(&self) -> &ManifestObjectRef {
        &self.object_ref
    }

    pub fn object_len(&self) -> u64 {
        self.object_len
    }

    pub fn superblock(&self) -> &SnapshotSuperblock {
        &self.superblock
    }

    /// Cluster descriptors are authenticated during [`Self::open`].
    pub fn clusters(&self) -> &[ClusterDescriptor] {
        &self.clusters
    }

    /// Fetch and authenticate the mount-trie section on first use.
    pub async fn read_mount_index(&self) -> WireResult<ManifestIndexPayload> {
        self.mount_index
            .get_or_try_init(|| async {
                self.read_index_section(
                    MOUNT_INDEX_ROOT,
                    SECTION_MOUNT,
                    u64::from(self.superblock.mount_count),
                    "mount index",
                )
                .await
            })
            .await
            .cloned()
    }

    /// Decode the typed v2 mount trie after its section has been fetched.
    pub async fn read_mount_trie(&self) -> WireResult<MountTrie> {
        let index = self.read_mount_trie_index().await?;
        let mut nodes = Vec::with_capacity(index.node_count as usize);
        for page_index in 0..index.pages.len() {
            nodes.extend(self.read_mount_trie_page(page_index).await?);
        }
        Ok(MountTrie { nodes })
    }

    /// Read the typed mount-trie page directory without decoding every node.
    pub async fn read_mount_trie_index(&self) -> WireResult<MountTrieIndex> {
        self.mount_trie_index
            .get_or_try_init(|| async { self.read_mount_trie_index_uncached().await })
            .await
            .cloned()
    }

    /// Fetch one authenticated mount-trie page. The request contains only the
    /// page body; its offset and digest come from the separately authenticated
    /// header+directory range.
    pub async fn read_mount_trie_page(
        &self,
        page_index: usize,
    ) -> WireResult<Vec<super::mount_trie::MountTrieNode>> {
        let index = self.read_mount_trie_index().await?;
        let page = index.pages.get(page_index).ok_or_else(|| {
            WireError::invalid("remote mount trie", "page index is out of bounds")
        })?;
        let root = &self.superblock.index_roots[MOUNT_INDEX_ROOT];
        let offset = root
            .object_offset
            .checked_add(SECTION_HEADER_LEN as u64)
            .and_then(|offset| offset.checked_add(page.offset))
            .ok_or_else(|| WireError::LimitExceeded("mount trie page offset overflows".into()))?;
        let bytes = self.fetch_range(offset, page.len).await?;
        MountTrie::decode_page_bytes(&bytes, page)
    }

    /// Resolve a mount-trie node by fetching only its containing page.
    pub async fn lookup_mount_node(
        &self,
        node_id: u32,
    ) -> WireResult<Option<super::mount_trie::MountTrieNode>> {
        if node_id == 0 {
            return Ok(None);
        }
        let index = self.read_mount_trie_index().await?;
        let Some(page_index) = index
            .pages
            .iter()
            .position(|page| page.first_node_id <= node_id && node_id <= page.last_node_id)
        else {
            return Ok(None);
        };
        Ok(self
            .read_mount_trie_page(page_index)
            .await?
            .into_iter()
            .find(|node| node.node_id == node_id))
    }

    /// Fetch and authenticate the merge-route section on first use.
    pub async fn read_route_index(&self) -> WireResult<ManifestIndexPayload> {
        self.route_index
            .get_or_try_init(|| async {
                self.read_index_section(
                    ROUTE_INDEX_ROOT,
                    SECTION_ROUTE,
                    self.superblock.merged_directory_count,
                    "route index",
                )
                .await
            })
            .await
            .cloned()
    }

    /// Decode the typed v2 merge-route index after its section has been fetched.
    pub async fn read_merge_routes(&self) -> WireResult<Vec<MergeRouteRecord>> {
        let index = self.read_merge_route_index().await?;
        let mut records = Vec::with_capacity(index.directory_count as usize);
        for page_index in 0..index.pages.len() {
            records.extend(self.read_merge_route_page(page_index).await?);
        }
        Ok(records)
    }

    /// Read the typed merge-route page directory without fetching page bodies.
    pub async fn read_merge_route_index(&self) -> WireResult<MergeRouteIndex> {
        self.merge_route_index
            .get_or_try_init(|| async { self.read_merge_route_index_uncached().await })
            .await
            .cloned()
    }

    /// Fetch one authenticated merge-route page.
    pub async fn read_merge_route_page(
        &self,
        page_index: usize,
    ) -> WireResult<Vec<MergeRouteRecord>> {
        let index = self.read_merge_route_index().await?;
        let page = index.pages.get(page_index).ok_or_else(|| {
            WireError::invalid("remote merge route", "page index is out of bounds")
        })?;
        let root = &self.superblock.index_roots[ROUTE_INDEX_ROOT];
        let offset = root
            .object_offset
            .checked_add(SECTION_HEADER_LEN as u64)
            .and_then(|offset| offset.checked_add(page.offset))
            .ok_or_else(|| WireError::LimitExceeded("merge route page offset overflows".into()))?;
        let bytes = self.fetch_range(offset, page.len).await?;
        MergeRouteIndex::decode_page_bytes(&bytes, page)
    }

    /// Resolve one merged directory route by fetching only the containing
    /// route page.
    pub async fn lookup_merge_route(
        &self,
        dir_key: super::identity::DirKey,
    ) -> WireResult<Option<MergeRouteRecord>> {
        let index = self.read_merge_route_index().await?;
        let Some(page_index) = index
            .pages
            .iter()
            .position(|page| page.first_dir_key <= dir_key && dir_key <= page.last_dir_key)
        else {
            return Ok(None);
        };
        Ok(self
            .read_merge_route_page(page_index)
            .await?
            .into_iter()
            .find(|record| record.view.dir_key == dir_key))
    }

    /// Materialize a complete [`SnapshotManifest`] only when a caller truly
    /// needs both deferred sections.  The common mount path should use the
    /// individual section accessors instead.
    pub async fn load_complete(&self) -> WireResult<SnapshotManifest> {
        self.verify_footer().await?;
        let mount_index = self.read_mount_index().await?;
        let route_index = self.read_route_index().await?;
        Ok(SnapshotManifest {
            superblock: self.superblock.clone(),
            clusters: self.clusters.to_vec(),
            mount_index,
            route_index,
        })
    }

    /// Stream the bytes covered by the manifest footer hash in bounded range
    /// requests. Ordinary lookup does not call this expensive full-object
    /// verification path.
    pub async fn verify_footer(&self) -> WireResult<()> {
        let footer_offset =
            self.object_len
                .checked_sub(FOOTER_LEN as u64)
                .ok_or(WireError::Truncated {
                    what: "snapshot manifest footer",
                    need: FOOTER_LEN,
                    have: self.object_len as usize,
                })?;
        let footer = self.fetch_range(footer_offset, FOOTER_LEN as u32).await?;
        let expected = decode_footer_header(&footer, self.object_len)?;
        let mut hasher = blake3::Hasher::new();
        let mut offset = 0u64;
        while offset < footer_offset {
            let remaining = footer_offset - offset;
            let length = remaining.min(MAX_MANIFEST_RANGE_BYTES as u64) as u32;
            let bytes = self.fetch_range(offset, length).await?;
            hasher.update(&bytes);
            offset += u64::from(length);
        }
        let computed = hasher.finalize();
        if expected != *computed.as_bytes() {
            return Err(WireError::HashMismatch {
                what: "snapshot manifest",
                stored: hex::encode(expected),
                computed: hex::encode(computed.as_bytes()),
            });
        }
        Ok(())
    }

    async fn read_mount_trie_index_uncached(&self) -> WireResult<MountTrieIndex> {
        let root = &self.superblock.index_roots[MOUNT_INDEX_ROOT];
        let (prefix, payload_len) = self
            .read_typed_directory_prefix(
                root,
                SECTION_MOUNT,
                super::mount_trie::MOUNT_TRIE_MAGIC,
                super::mount_trie::MOUNT_TRIE_HEADER_LEN,
                super::mount_trie::MOUNT_TRIE_PAGE_REF_LEN,
                "mount trie",
            )
            .await?;
        match MountTrie::index_from_directory(&prefix, payload_len) {
            Ok(index) if index.node_count == u64::from(self.superblock.mount_count) => Ok(index),
            Ok(_) => Err(WireError::invalid(
                "remote mount trie",
                "node count does not match manifest section",
            )),
            Err(WireError::UnsupportedFormat(_)) => {
                let payload = self.read_mount_index().await?;
                let index = MountTrie::index(&payload.bytes)?;
                if index.node_count != payload.entry_count {
                    return Err(WireError::invalid(
                        "remote mount trie",
                        "node count does not match manifest section",
                    ));
                }
                Ok(index)
            }
            Err(error) => Err(error),
        }
    }

    async fn read_merge_route_index_uncached(&self) -> WireResult<MergeRouteIndex> {
        let root = &self.superblock.index_roots[ROUTE_INDEX_ROOT];
        let (prefix, payload_len) = self
            .read_typed_directory_prefix(
                root,
                SECTION_ROUTE,
                super::merge_route::MERGE_ROUTE_MAGIC,
                super::merge_route::MERGE_ROUTE_HEADER_LEN,
                super::merge_route::MERGE_ROUTE_PAGE_REF_LEN,
                "merge route",
            )
            .await?;
        match MergeRouteIndex::index_from_directory(&prefix, payload_len) {
            Ok(index) if index.directory_count == self.superblock.merged_directory_count => {
                Ok(index)
            }
            Ok(_) => Err(WireError::invalid(
                "remote merge route",
                "directory count does not match manifest section",
            )),
            Err(WireError::UnsupportedFormat(_)) => {
                let payload = self.read_route_index().await?;
                let index = MergeRouteIndex::index(&payload.bytes)?;
                if index.directory_count != payload.entry_count {
                    return Err(WireError::invalid(
                        "remote merge route",
                        "directory count does not match manifest section",
                    ));
                }
                Ok(index)
            }
            Err(error) => Err(error),
        }
    }

    /// Fetch a typed section wrapper plus its fixed payload header and page
    /// directory. The returned bytes are relative to the typed payload, and
    /// `payload_len` is the complete typed payload length.
    async fn read_typed_directory_prefix(
        &self,
        root: &IndexRootRef,
        expected_kind: u8,
        magic: &[u8; 8],
        typed_header_len: usize,
        page_ref_len: usize,
        what: &'static str,
    ) -> WireResult<(Vec<u8>, usize)> {
        let root_len = usize::try_from(root.stored_len)
            .map_err(|_| WireError::LimitExceeded("manifest section exceeds usize".into()))?;
        if root_len < SECTION_HEADER_LEN + typed_header_len {
            return Err(WireError::Truncated {
                what: "manifest typed section header",
                need: SECTION_HEADER_LEN + typed_header_len,
                have: root_len,
            });
        }
        let initial_len = SECTION_HEADER_LEN + typed_header_len;
        let initial = self
            .fetch_range(
                root.object_offset,
                u32::try_from(initial_len).map_err(|_| {
                    WireError::LimitExceeded("manifest typed header exceeds u32".into())
                })?,
            )
            .await?;
        let payload_len = validate_typed_section_prefix(
            &initial[..SECTION_HEADER_LEN],
            root,
            expected_kind,
            what,
        )?;
        let typed_header = &initial[SECTION_HEADER_LEN..];
        if &typed_header[..8] != magic {
            return Ok((initial[SECTION_HEADER_LEN..].to_vec(), payload_len));
        }
        let page_count = u32::from_le_bytes(typed_header[12..16].try_into().unwrap()) as usize;
        let directory_len = u32::from_le_bytes(typed_header[24..28].try_into().unwrap()) as usize;
        let expected_directory_len = page_count
            .checked_mul(page_ref_len)
            .ok_or_else(|| WireError::LimitExceeded("manifest page directory overflows".into()))?;
        if directory_len != expected_directory_len
            || typed_header_len
                .checked_add(directory_len)
                .is_none_or(|length| length > payload_len)
        {
            return Err(WireError::invalid(
                what,
                "typed page directory length is not canonical",
            ));
        }
        let prefix_len = typed_header_len + directory_len;
        if prefix_len == typed_header_len {
            return Ok((typed_header.to_vec(), payload_len));
        }
        let directory = self
            .fetch_range(
                root.object_offset
                    .checked_add(SECTION_HEADER_LEN as u64)
                    .and_then(|offset| offset.checked_add(typed_header_len as u64))
                    .ok_or_else(|| {
                        WireError::LimitExceeded("manifest page directory offset overflows".into())
                    })?,
                u32::try_from(directory_len).map_err(|_| {
                    WireError::LimitExceeded("manifest page directory exceeds u32".into())
                })?,
            )
            .await?;
        let mut prefix = Vec::with_capacity(prefix_len);
        prefix.extend_from_slice(typed_header);
        prefix.extend_from_slice(&directory);
        Ok((prefix, payload_len))
    }

    async fn read_index_section(
        &self,
        root_index: usize,
        expected_kind: u8,
        expected_count: u64,
        what: &'static str,
    ) -> WireResult<ManifestIndexPayload> {
        let root = &self.superblock.index_roots[root_index];
        let bytes = self
            .fetch_range(root.object_offset, root.stored_len)
            .await?;
        let (payload, count) = decode_section_range(&bytes, root, expected_kind)?;
        if count != expected_count {
            return Err(WireError::invalid(
                what,
                "entry count does not match snapshot superblock",
            ));
        }
        Ok(ManifestIndexPayload {
            entry_count: count,
            bytes: payload,
        })
    }

    fn validate_roots(&self, superblock: &SnapshotSuperblock) -> WireResult<()> {
        if superblock.cluster_count as usize > super::snapshot_manifest::MAX_MANIFEST_CLUSTERS {
            return Err(WireError::LimitExceeded(format!(
                "cluster count exceeds {}",
                super::snapshot_manifest::MAX_MANIFEST_CLUSTERS
            )));
        }
        let expected_counts = [
            u64::from(superblock.mount_count),
            u64::from(superblock.cluster_count),
            superblock.merged_directory_count,
        ];
        let expected_kinds = [SECTION_MOUNT, SECTION_CLUSTER_TABLE, SECTION_ROUTE];
        let mut previous_end = SUPERBLOCK_LEN as u64;
        for (index, root) in superblock.index_roots.iter().enumerate() {
            validate_section_root(
                root,
                expected_kinds[index],
                expected_counts[index],
                self.object_len,
            )?;
            if root.object_offset != previous_end {
                return Err(WireError::invalid(
                    "snapshot manifest",
                    "sections are not contiguous and ordered",
                ));
            }
            previous_end = root
                .object_offset
                .checked_add(u64::from(root.stored_len))
                .ok_or_else(|| {
                    WireError::LimitExceeded("manifest section end overflows u64".into())
                })?;
        }
        let footer_offset =
            self.object_len
                .checked_sub(FOOTER_LEN as u64)
                .ok_or(WireError::Truncated {
                    what: "snapshot manifest footer",
                    need: FOOTER_LEN,
                    have: self.object_len as usize,
                })?;
        if previous_end != footer_offset {
            return Err(WireError::invalid(
                "snapshot manifest",
                "sections do not end immediately before footer",
            ));
        }
        Ok(())
    }

    async fn fetch_range(&self, offset: u64, len: u32) -> WireResult<Vec<u8>> {
        let length = usize::try_from(len)
            .map_err(|_| WireError::LimitExceeded("manifest range length exceeds usize".into()))?;
        if length > MAX_MANIFEST_RANGE_BYTES {
            return Err(WireError::LimitExceeded(format!(
                "manifest range length {length} exceeds {MAX_MANIFEST_RANGE_BYTES}"
            )));
        }
        validate_range_bounds(offset, len, self.object_len)?;
        let mut bytes = vec![0u8; length];
        let actual = self
            .client
            .get_object_range(&self.object_key, offset, &mut bytes)
            .await
            .map_err(|error| WireError::invalid("remote manifest range", error.to_string()))?;
        if actual != length {
            return Err(WireError::Truncated {
                what: "remote manifest range",
                need: length,
                have: actual,
            });
        }
        Ok(bytes)
    }
}

fn validate_section_root(
    root: &IndexRootRef,
    expected_kind: u8,
    expected_count: u64,
    object_len: u64,
) -> WireResult<()> {
    if root.level != 0 || root.kind != expected_kind {
        return Err(WireError::invalid(
            "manifest section root",
            "section level or kind is invalid",
        ));
    }
    if u64::from(root.entry_count) != expected_count {
        return Err(WireError::invalid(
            "manifest section root",
            "entry count does not match superblock",
        ));
    }
    let stored_len = usize::try_from(root.stored_len)
        .map_err(|_| WireError::LimitExceeded("manifest section length exceeds usize".into()))?;
    if !(SECTION_HEADER_LEN..=MAX_MANIFEST_RANGE_BYTES).contains(&stored_len) {
        return Err(WireError::LimitExceeded(
            "manifest section length is outside bounds".into(),
        ));
    }
    let expected_stored_len = usize::try_from(root.raw_len)
        .ok()
        .and_then(|length| length.checked_add(SECTION_HEADER_LEN))
        .ok_or_else(|| {
            WireError::LimitExceeded("manifest section length overflows usize".into())
        })?;
    if expected_stored_len != stored_len {
        return Err(WireError::invalid(
            "manifest section root",
            "raw and stored lengths do not match",
        ));
    }
    let end = root
        .object_offset
        .checked_add(u64::from(root.stored_len))
        .ok_or_else(|| WireError::LimitExceeded("manifest section end overflows u64".into()))?;
    let data_limit = object_len
        .checked_sub(FOOTER_LEN as u64)
        .ok_or(WireError::Truncated {
            what: "snapshot manifest footer",
            need: FOOTER_LEN,
            have: object_len as usize,
        })?;
    if root.object_offset < SUPERBLOCK_LEN as u64 || end > data_limit {
        return Err(WireError::Truncated {
            what: "manifest section",
            need: root.stored_len as usize,
            have: object_len.saturating_sub(root.object_offset) as usize,
        });
    }
    Ok(())
}

fn validate_typed_section_prefix(
    section_header: &[u8],
    root: &IndexRootRef,
    expected_kind: u8,
    what: &'static str,
) -> WireResult<usize> {
    if section_header.len() != SECTION_HEADER_LEN {
        return Err(WireError::Truncated {
            what: "manifest section header",
            need: SECTION_HEADER_LEN,
            have: section_header.len(),
        });
    }
    if &section_header[..8] != super::snapshot_manifest::SECTION_MAGIC
        || section_header[8] != expected_kind
    {
        return Err(WireError::UnsupportedFormat(
            "manifest section magic or kind mismatch".into(),
        ));
    }
    if section_header[9..12].iter().any(|byte| *byte != 0) {
        return Err(WireError::invalid(
            "manifest section",
            "reserved bytes are non-zero",
        ));
    }
    let payload_len = u32::from_le_bytes(section_header[12..16].try_into().unwrap()) as usize;
    if payload_len != root.raw_len as usize
        || payload_len
            .checked_add(SECTION_HEADER_LEN)
            .is_none_or(|stored_len| stored_len != root.stored_len as usize)
    {
        return Err(WireError::invalid(
            what,
            "typed section length does not match manifest root",
        ));
    }
    Ok(payload_len)
}

fn validate_range_bounds(offset: u64, len: u32, object_len: u64) -> WireResult<()> {
    let end = offset
        .checked_add(u64::from(len))
        .ok_or_else(|| WireError::LimitExceeded("manifest range offset overflows u64".into()))?;
    if end > object_len {
        return Err(WireError::Truncated {
            what: "remote manifest range",
            need: len as usize,
            have: object_len.saturating_sub(offset) as usize,
        });
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::cadapter::client::ObjectBackend;
    use crate::workspace_overlay::clustered_snapshot::directory::NodeRef;
    use crate::workspace_overlay::clustered_snapshot::identity::DirKey;
    use crate::workspace_overlay::clustered_snapshot::merge_route::DirectoryViewRecord;
    use crate::workspace_overlay::clustered_snapshot::mount_trie::{MountTrieChild, MountTrieNode};
    use crate::workspace_overlay::clustered_snapshot::name::NameBytes;

    #[derive(Clone, Default)]
    struct CountingBackend {
        state: Arc<Mutex<State>>,
    }

    #[derive(Default)]
    struct State {
        objects: HashMap<String, Vec<u8>>,
        ranges: Vec<(u64, usize)>,
    }

    impl CountingBackend {
        fn insert(&self, key: &str, object: Vec<u8>) {
            self.state
                .lock()
                .expect("backend mutex")
                .objects
                .insert(key.to_string(), object);
        }

        fn ranges(&self) -> Vec<(u64, usize)> {
            self.state.lock().expect("backend mutex").ranges.clone()
        }

        fn tamper(&self, key: &str, offset: usize) {
            self.state
                .lock()
                .expect("backend mutex")
                .objects
                .get_mut(key)
                .expect("object exists")[offset] ^= 1;
        }
    }

    #[async_trait]
    impl ObjectBackend for CountingBackend {
        async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            self.insert(key, data.to_vec());
            Ok(())
        }

        async fn get_object(&self, _key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn get_object_range(
            &self,
            key: &str,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<usize> {
            let mut state = self.state.lock().expect("backend mutex");
            let Some(object) = state.objects.get(key) else {
                return Ok(0);
            };
            let start = usize::try_from(offset)?;
            if start >= object.len() {
                return Ok(0);
            }
            let count = buf.len().min(object.len() - start);
            buf[..count].copy_from_slice(&object[start..start + count]);
            state.ranges.push((offset, count));
            Ok(count)
        }

        async fn get_etag(&self, _key: &str) -> anyhow::Result<String> {
            Ok(String::new())
        }

        async fn delete_object(&self, _key: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn manifest() -> SnapshotManifest {
        let object_ref = |seed: u8, kind: u8, key: &str| ManifestObjectRef {
            object_id: [seed; 16],
            kind,
            object_len: 1,
            full_hash: [seed; 32],
            key: key.as_bytes().to_vec(),
        };
        SnapshotManifest {
            superblock: SnapshotSuperblock {
                volume_id: [1; 16],
                snapshot_id: [2; 16],
                semantic_hash: [3; 32],
                route_seed: [4; 16],
                cluster_count: 1,
                mount_count: 2,
                merged_directory_count: 3,
                root_dir_key: DirKey::new([5; 16]),
                index_roots: [empty_root(), empty_root(), empty_root()],
            },
            clusters: vec![ClusterDescriptor {
                cluster_id: [6; 16],
                metadata_ref: object_ref(7, 1, "clusters/6.brfc"),
                data_seal_ref: object_ref(8, 2, "seals/6.brfds"),
                combined_semantic_hash: [9; 32],
                mount_dir_key: DirKey::new([10; 16]),
                root_local_node_id: 1,
                flags: 0,
            }],
            mount_index: ManifestIndexPayload {
                entry_count: 2,
                bytes: b"mount trie bytes".to_vec(),
            },
            route_index: ManifestIndexPayload {
                entry_count: 3,
                bytes: b"route index bytes".to_vec(),
            },
        }
    }

    fn manifest_ref(object_len: usize) -> ManifestObjectRef {
        ManifestObjectRef {
            object_id: [11; 16],
            kind: 3,
            object_len: object_len as u64,
            full_hash: [12; 32],
            key: b"snapshots/test.brfsm".to_vec(),
        }
    }

    #[tokio::test]
    async fn opens_with_superblock_and_cluster_table_only() {
        let bytes = manifest().encode().unwrap();
        let backend = CountingBackend::default();
        backend.insert("snapshots/test.brfsm", bytes.clone());
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteSnapshotManifest::open(&client, manifest_ref(bytes.len()))
            .await
            .unwrap();
        assert_eq!(remote.clusters().len(), 1);
        assert_eq!(backend.ranges().len(), 2);

        let mount = remote.read_mount_index().await.unwrap();
        assert_eq!(mount.bytes, b"mount trie bytes");
        assert_eq!(backend.ranges().len(), 3);
        let route = remote.read_route_index().await.unwrap();
        assert_eq!(route.bytes, b"route index bytes");
        assert_eq!(backend.ranges().len(), 4);
        remote.read_mount_index().await.unwrap();
        remote.read_route_index().await.unwrap();
        assert_eq!(backend.ranges().len(), 4);
    }

    #[tokio::test]
    async fn rejects_tampered_cluster_section_before_open_returns() {
        let bytes = manifest().encode().unwrap();
        let backend = CountingBackend::default();
        backend.insert("snapshots/test.brfsm", bytes.clone());
        let cluster_offset = SnapshotSuperblock::decode(&bytes).unwrap().index_roots
            [CLUSTER_TABLE_ROOT]
            .object_offset as usize;
        backend.tamper("snapshots/test.brfsm", cluster_offset + SECTION_HEADER_LEN);
        let client = ObjectClient::new(backend);
        assert!(matches!(
            RemoteSnapshotManifest::open(&client, manifest_ref(bytes.len())).await,
            Err(WireError::HashMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn rejects_manifest_object_length_that_hides_footer_or_section() {
        let bytes = manifest().encode().unwrap();
        let backend = CountingBackend::default();
        backend.insert("snapshots/test.brfsm", bytes.clone());
        let client = ObjectClient::new(backend);
        let mut object_ref = manifest_ref(bytes.len() - 1);
        object_ref.object_len = (bytes.len() - 1) as u64;
        assert!(
            RemoteSnapshotManifest::open(&client, object_ref)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn full_manifest_verification_streams_footer_hash() {
        let bytes = manifest().encode().unwrap();
        let backend = CountingBackend::default();
        backend.insert("snapshots/test.brfsm", bytes.clone());
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteSnapshotManifest::open(&client, manifest_ref(bytes.len()))
            .await
            .unwrap();
        remote.verify_footer().await.unwrap();
        backend.tamper("snapshots/test.brfsm", SUPERBLOCK_LEN);
        assert!(matches!(
            remote.verify_footer().await,
            Err(WireError::HashMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn typed_page_access_fetches_directory_then_one_page() {
        let mount = MountTrie {
            nodes: vec![
                MountTrieNode {
                    node_id: 1,
                    parent_id: 0,
                    component: None,
                    cluster_slots: vec![0],
                    children: vec![MountTrieChild {
                        name: NameBytes::new(b"dataset".to_vec()).unwrap(),
                        node_id: 2,
                    }],
                },
                MountTrieNode {
                    node_id: 2,
                    parent_id: 1,
                    component: Some(NameBytes::new(b"dataset".to_vec()).unwrap()),
                    cluster_slots: vec![0],
                    children: Vec::new(),
                },
            ],
        };
        let dir_key = DirKey::new([17; 16]);
        let route = MergeRouteIndex::encode(&[MergeRouteRecord {
            view: DirectoryViewRecord {
                dir_key,
                canonical_node: NodeRef {
                    cluster_slot: 0,
                    local_node_id: 1,
                },
                visible_entry_count: 0,
                entry_index_root: [18; 32],
            },
            windows: Vec::new(),
        }])
        .unwrap();
        let manifest = SnapshotManifest {
            superblock: SnapshotSuperblock {
                volume_id: [1; 16],
                snapshot_id: [2; 16],
                semantic_hash: [3; 32],
                route_seed: [4; 16],
                cluster_count: 0,
                mount_count: 2,
                merged_directory_count: 1,
                root_dir_key: DirKey::new([5; 16]),
                index_roots: [empty_root(), empty_root(), empty_root()],
            },
            clusters: Vec::new(),
            mount_index: ManifestIndexPayload {
                entry_count: 2,
                bytes: mount.encode().unwrap(),
            },
            route_index: ManifestIndexPayload {
                entry_count: 1,
                bytes: route,
            },
        };
        let bytes = manifest.encode().unwrap();
        let backend = CountingBackend::default();
        backend.insert("snapshots/typed.brfsm", bytes.clone());
        let client = ObjectClient::new(backend.clone());
        let remote = RemoteSnapshotManifest::open(
            &client,
            ManifestObjectRef {
                object_id: [11; 16],
                kind: 3,
                object_len: bytes.len() as u64,
                full_hash: [12; 32],
                key: b"snapshots/typed.brfsm".to_vec(),
            },
        )
        .await
        .unwrap();
        assert_eq!(backend.ranges().len(), 2);

        let mount_index = remote.read_mount_trie_index().await.unwrap();
        assert_eq!(mount_index.node_count, 2);
        assert_eq!(backend.ranges().len(), 4);
        let page = remote.read_mount_trie_page(0).await.unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(backend.ranges().len(), 5);
        assert_eq!(
            remote.lookup_mount_node(2).await.unwrap().unwrap().node_id,
            2
        );
        assert_eq!(backend.ranges().len(), 6);

        let route_index = remote.read_merge_route_index().await.unwrap();
        assert_eq!(route_index.directory_count, 1);
        assert_eq!(backend.ranges().len(), 8);
        let route_page = remote.read_merge_route_page(0).await.unwrap();
        assert_eq!(route_page.len(), 1);
        assert_eq!(backend.ranges().len(), 9);
        assert_eq!(
            remote
                .lookup_merge_route(dir_key)
                .await
                .unwrap()
                .unwrap()
                .view
                .dir_key,
            dir_key
        );
        assert_eq!(backend.ranges().len(), 10);
        // The old full-section API remains explicit and is not used by the
        // typed page path above.
        assert_eq!(remote.read_mount_index().await.unwrap().entry_count, 2);
        assert_eq!(remote.read_route_index().await.unwrap().entry_count, 1);
    }
}
