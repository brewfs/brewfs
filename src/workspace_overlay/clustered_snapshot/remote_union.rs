//! Multi-cluster remote union helpers for clustered snapshot v2.
//!
//! A cluster is immutable, but one logical directory can have contributors in
//! several clusters. The union layer keeps the physical parent NodeRef
//! explicit; it never guesses a local node id from a global DirKey.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::try_join_all;

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::meta::store::{FileType, MetaError};
use crate::native_base::wire::error::{WireError, WireResult};
use crate::vfs::handles::{DirectoryPageSource, RawDirEntry};

use super::attribute::AttributeGroup;
use super::batch::NamespaceEntry;
use super::directory::{
    DirectoryEntry, DirectoryIdentity, DirectoryPage, MAX_RANGE_SOURCES, MAX_WINDOW_ENTRIES,
    NodeRef, ReadDirLimit, WindowSourceKind,
};
use super::extent::ExtentSpan;
use super::merge_route::MergeRouteRecord;
use super::name::NameBytes;
use super::remote::{RemoteCluster, RemoteClusterOptions};
use super::snapshot_manifest::{ClusterDescriptor, ManifestObjectRef};

/// The existing FUSE page source asks for an entry count rather than a byte
/// budget. Keep a conservative fixed output cap at this compatibility
/// boundary; the route reader still enforces its own per-request count and
/// name-byte accounting before this allocation is returned.
pub const REMOTE_PAGE_MAX_OWNED_BYTES: usize = 1024 * 1024;

/// Adapter from the clustered v2 page protocol to the generic FUSE directory
/// handle. It stores only immutable snapshot identity and a `DirKey`; decoded
/// namespace batches remain owned by the remote reader's bounded caches.
pub struct RemoteDirectoryPageSource<B: ObjectBackend + Clone> {
    snapshot: Arc<RemoteSnapshot<B>>,
    dir_key: super::identity::DirKey,
}

impl<B: ObjectBackend + Clone> RemoteDirectoryPageSource<B> {
    pub fn new(snapshot: Arc<RemoteSnapshot<B>>, dir_key: super::identity::DirKey) -> Self {
        Self { snapshot, dir_key }
    }
}

#[async_trait]
impl<B: ObjectBackend + Clone + 'static> DirectoryPageSource for RemoteDirectoryPageSource<B> {
    async fn read_page(
        &self,
        ino: i64,
        child_offset: u64,
        max_entries: usize,
    ) -> Result<Vec<RawDirEntry>, MetaError> {
        let max_entries = max_entries.min(MAX_WINDOW_ENTRIES);
        if max_entries == 0 {
            return Ok(Vec::new());
        }
        let page = self
            .snapshot
            .read_directory_page(
                self.dir_key,
                child_offset,
                ReadDirLimit {
                    max_entries,
                    max_owned_bytes: REMOTE_PAGE_MAX_OWNED_BYTES,
                },
            )
            .await
            .map_err(|error| MetaError::Internal(error.to_string()))?
            .ok_or(MetaError::NotFound(ino))?;
        page.entries
            .into_iter()
            .map(|entry| {
                Ok(RawDirEntry {
                    name: entry.name.into_bytes(),
                    ino: i64::try_from(entry.inode)
                        .map_err(|_| MetaError::Internal("packed inode exceeds i64".into()))?,
                    kind: remote_file_type(entry.kind),
                })
            })
            .collect()
    }
}

fn remote_file_type(kind: u8) -> FileType {
    match kind {
        1 => FileType::File,
        2 => FileType::Dir,
        3 => FileType::Symlink,
        4 => FileType::Fifo,
        5 => FileType::Socket,
        6 => FileType::CharDevice,
        7 => FileType::BlockDevice,
        _ => FileType::File,
    }
}

/// A bounded set of remote cluster handles belonging to one immutable
/// manifest. Opening this set reads one superblock per selected cluster; all
/// index pages and batches remain on demand inside each handle.
pub struct RemoteClusterUnion<B: ObjectBackend + Clone> {
    clusters: Arc<[Arc<RemoteCluster<B>>]>,
}

impl<B: ObjectBackend + Clone> RemoteClusterUnion<B> {
    pub async fn open(
        client: &ObjectClient<B>,
        descriptors: &[ClusterDescriptor],
        options: RemoteClusterOptions,
    ) -> WireResult<Self> {
        let mut clusters = Vec::with_capacity(descriptors.len());
        for descriptor in descriptors {
            clusters.push(Arc::new(
                RemoteCluster::open_with_options(client, descriptor.clone(), options.clone())
                    .await?,
            ));
        }
        Ok(Self {
            clusters: Arc::from(clusters),
        })
    }

    pub fn clusters(&self) -> &[Arc<RemoteCluster<B>>] {
        &self.clusters
    }

    pub fn cluster(&self, cluster_slot: u32) -> WireResult<&Arc<RemoteCluster<B>>> {
        self.clusters.get(cluster_slot as usize).ok_or_else(|| {
            WireError::invalid(
                "cluster union",
                format!("cluster slot {cluster_slot} is outside the manifest"),
            )
        })
    }

    /// Resolve one raw name from an explicit bounded contributor list. A
    /// duplicate is valid only when the authenticated namespace entry is
    /// byte-for-byte identical; conflicts fail closed.
    pub async fn lookup_namespace(
        &self,
        contributors: &[NodeRef],
        name: &[u8],
    ) -> WireResult<Option<super::batch::NamespaceEntry>> {
        if contributors.is_empty() || contributors.len() > super::directory::MAX_RANGE_SOURCES {
            return Err(WireError::invalid(
                "cluster union",
                "contributor count is outside the v2 window bound",
            ));
        }
        let mut found = None;
        for contributor in contributors {
            let entry = self
                .cluster(contributor.cluster_slot)?
                .lookup_namespace(contributor.local_node_id, name)
                .await?;
            let Some(entry) = entry else {
                continue;
            };
            if let Some(previous) = &found
                && previous != &entry
            {
                return Err(WireError::invalid(
                    "cluster union",
                    "contributors disagree on one raw name",
                ));
            }
            found = Some(entry);
        }
        Ok(found)
    }

    /// Load one contributor's cold attributes by its authenticated physical
    /// NodeRef. Attributes are not merged by cluster slot or source id.
    pub async fn lookup_attribute(
        &self,
        contributor: NodeRef,
        local_node_id: u32,
    ) -> WireResult<Option<AttributeGroup>> {
        if contributor.local_node_id != local_node_id {
            return Err(WireError::invalid(
                "cluster union",
                "attribute contributor and local node id disagree",
            ));
        }
        self.cluster(contributor.cluster_slot)?
            .lookup_attribute(local_node_id)
            .await
    }

    /// Resolve a physical file range from the contributor authenticated by
    /// the merge route.  File collisions are rejected during route loading,
    /// so a committed file has one unambiguous extent owner.
    pub async fn read_extent_range(
        &self,
        contributor: NodeRef,
        file_offset: u64,
        length: u64,
    ) -> WireResult<Vec<ExtentSpan>> {
        self.cluster(contributor.cluster_slot)?
            .read_extent_range(contributor.local_node_id, file_offset, length)
            .await
    }

    /// Resolve a route record using the authenticated physical contributor
    /// carried by each route source. The planner-local source id remains only
    /// a grouping key and is never interpreted as a cluster slot.
    pub async fn lookup_route(
        &self,
        route: &MergeRouteRecord,
        name: &[u8],
    ) -> WireResult<Option<super::batch::NamespaceEntry>> {
        let Some(window) = route.windows.iter().find(|window| {
            window
                .lower
                .as_ref()
                .is_none_or(|lower| name >= lower.as_bytes())
                && window
                    .upper
                    .as_ref()
                    .is_none_or(|upper| name < upper.as_bytes())
        }) else {
            return Ok(None);
        };
        let mut contributors = Vec::with_capacity(window.sources.len());
        for source in &window.sources {
            match source.kind {
                WindowSourceKind::CanonicalDirectoryEntries => {
                    contributors.push(source.contributor);
                }
                WindowSourceKind::FileEntries => {
                    contributors.push(source.contributor);
                }
            }
        }
        self.lookup_namespace(&contributors, name).await
    }

    /// Read one bounded page from a persisted merged-directory route.
    ///
    /// Only the route windows intersecting the requested ordinal are fetched.
    /// Each physical source is range-read by raw name, merged by the
    /// authenticated duplicate rules, and converted to stable snapshot-local
    /// inode numbers derived from its pair of cluster slot and local node id.
    pub async fn read_route_page(
        &self,
        route: &MergeRouteRecord,
        identity: DirectoryIdentity,
        next_ordinal: u64,
        limit: ReadDirLimit,
    ) -> WireResult<DirectoryPage> {
        if identity.dir_key != route.view.dir_key.into_bytes() {
            return Err(WireError::invalid(
                "cluster union",
                "directory identity does not match route",
            ));
        }
        let limit = limit
            .validate()
            .map_err(|error| WireError::invalid("cluster union readdir", error.to_string()))?;
        if next_ordinal > route.view.visible_entry_count {
            return Err(WireError::invalid(
                "cluster union readdir",
                "ordinal is outside the directory",
            ));
        }
        if next_ordinal == route.view.visible_entry_count {
            return Ok(DirectoryPage {
                identity,
                next_ordinal,
                entries: Vec::new(),
                end: true,
            });
        }

        let mut before = 0u64;
        let mut output = Vec::with_capacity(limit.max_entries.min(MAX_WINDOW_ENTRIES));
        let mut owned_bytes = 0usize;
        for window in &route.windows {
            let count = u64::from(window.visible_entry_count);
            if next_ordinal >= before.saturating_add(count) {
                before = before.saturating_add(count);
                continue;
            }
            let skip = usize::try_from(next_ordinal.saturating_sub(before)).map_err(|_| {
                WireError::invalid("cluster union readdir", "ordinal does not fit usize")
            })?;
            let remaining = limit.max_entries.saturating_sub(output.len());
            if remaining == 0 || owned_bytes >= limit.max_owned_bytes {
                break;
            }
            let merged = self.read_and_merge_window(window).await?;
            if merged.len() != window.visible_entry_count as usize {
                return Err(WireError::invalid(
                    "cluster union readdir",
                    "route visible count does not match merged source names",
                ));
            }
            for (contributor, entry) in merged.into_iter().skip(skip) {
                if output.len() >= remaining {
                    break;
                }
                let entry = namespace_to_directory_entry(contributor, entry);
                let entry_bytes = limit.owned_bytes(&entry);
                if entry_bytes > limit.max_owned_bytes.saturating_sub(owned_bytes) {
                    if output.is_empty() {
                        return Err(WireError::invalid(
                            "cluster union readdir",
                            "single directory entry exceeds the page budget",
                        ));
                    }
                    break;
                }
                owned_bytes = owned_bytes.saturating_add(entry_bytes);
                output.push(entry);
            }
            before = before.saturating_add(count);
            if output.len() >= limit.max_entries || owned_bytes >= limit.max_owned_bytes {
                break;
            }
        }

        let next = next_ordinal.saturating_add(output.len() as u64);
        Ok(DirectoryPage {
            identity,
            next_ordinal: next,
            entries: output,
            end: next >= route.view.visible_entry_count,
        })
    }

    async fn read_and_merge_window(
        &self,
        window: &super::merge_route::MergeRouteWindow,
    ) -> WireResult<Vec<(NodeRef, NamespaceEntry)>> {
        if window.sources.is_empty() || window.sources.len() > MAX_RANGE_SOURCES {
            return Err(WireError::invalid(
                "cluster union readdir",
                "route source count is outside the v2 bound",
            ));
        }
        let mut futures = Vec::with_capacity(window.sources.len());
        for source in &window.sources {
            let cluster = self.cluster(source.contributor.cluster_slot)?.clone();
            let source = source.clone();
            futures.push(async move {
                let entries = cluster
                    .read_namespace_range(
                        source.contributor.local_node_id,
                        source.first_name.as_bytes(),
                        source.last_name.as_bytes(),
                    )
                    .await?;
                if entries.len() != source.entry_count as usize {
                    return Err(WireError::invalid(
                        "cluster union readdir",
                        format!(
                            "source {} returned {} names, route records {}",
                            source.source_id,
                            entries.len(),
                            source.entry_count
                        ),
                    ));
                }
                Ok((source.contributor, entries))
            });
        }
        let sources = try_join_all(futures).await?;
        let physical_count = sources
            .iter()
            .map(|(_, entries)| entries.len())
            .sum::<usize>();
        if physical_count > MAX_RANGE_SOURCES * MAX_WINDOW_ENTRIES {
            return Err(WireError::LimitExceeded(
                "physical route source entries exceed the v2 window bound".into(),
            ));
        }

        let mut by_name = BTreeMap::<NameBytes, Vec<(NodeRef, NamespaceEntry)>>::new();
        for (contributor, entries) in sources {
            for entry in entries {
                by_name
                    .entry(namespace_entry_name(&entry).clone())
                    .or_default()
                    .push((contributor, entry));
            }
        }
        by_name
            .into_iter()
            .map(|(name, candidates)| merge_namespace_candidates(&name, candidates))
            .collect()
    }

    pub fn clear_caches(&self) {
        for cluster in self.clusters.iter() {
            cluster.clear_cache();
        }
    }
}

fn namespace_entry_name(entry: &NamespaceEntry) -> &NameBytes {
    match entry {
        NamespaceEntry::NewNode { name, .. } | NamespaceEntry::ExistingNode { name, .. } => name,
    }
}

fn merge_namespace_candidates(
    name: &NameBytes,
    mut candidates: Vec<(NodeRef, NamespaceEntry)>,
) -> WireResult<(NodeRef, NamespaceEntry)> {
    if candidates.len() == 1 {
        return Ok(candidates.pop().expect("one candidate"));
    }
    let is_directory = candidates.iter().all(|(_, entry)| {
        matches!(
            entry,
            NamespaceEntry::NewNode {
                node: super::batch::NodeRecord {
                    kind: 2,
                    dir_key: Some(_),
                    ..
                },
                ..
            }
        )
    });
    if is_directory {
        let first_key = match &candidates[0].1 {
            NamespaceEntry::NewNode { node, .. } => (node.dir_key, node.mode, node.size),
            NamespaceEntry::ExistingNode { .. } => unreachable!(),
        };
        if candidates.iter().any(|(_, entry)| match entry {
            NamespaceEntry::NewNode { node, .. } => {
                (node.dir_key, node.mode, node.size) != first_key
            }
            NamespaceEntry::ExistingNode { .. } => true,
        }) {
            return Err(WireError::invalid(
                "cluster union readdir",
                format!("directory contributors disagree for {:?}", name.as_bytes()),
            ));
        }
        candidates.sort_by_key(|(contributor, _)| *contributor);
        return Ok(candidates.remove(0));
    }
    Err(WireError::invalid(
        "cluster union readdir",
        format!("committed name collision for {:?}", name.as_bytes()),
    ))
}

fn namespace_to_directory_entry(contributor: NodeRef, entry: NamespaceEntry) -> DirectoryEntry {
    match entry {
        NamespaceEntry::NewNode { name, node } => DirectoryEntry {
            name,
            inode: stable_inode(contributor, node.local_node_id),
            kind: node.kind,
        },
        // ExistingNode currently represents a hardlink to a regular inode in
        // the namespace codec. Keep the identity stable; cold attributes can
        // refine the kind when the attribute path is added.
        NamespaceEntry::ExistingNode {
            name,
            local_node_id,
        } => DirectoryEntry {
            name,
            inode: stable_inode(contributor, local_node_id),
            kind: 1,
        },
    }
}

fn stable_inode(contributor: NodeRef, local_node_id: u32) -> u64 {
    (u64::from(contributor.cluster_slot) << 32) | u64::from(local_node_id)
}

/// A manifest plus its lazily decoded cluster handles. This is the narrow
/// runtime boundary used by callers that need to mount a complete snapshot;
/// mutable agent overlays remain outside this type.
pub struct RemoteSnapshot<B: ObjectBackend + Clone> {
    manifest: super::manifest_remote::RemoteSnapshotManifest<B>,
    clusters: RemoteClusterUnion<B>,
}

impl<B: ObjectBackend + Clone> RemoteSnapshot<B> {
    pub async fn open(
        client: &ObjectClient<B>,
        manifest_ref: ManifestObjectRef,
        options: RemoteClusterOptions,
    ) -> WireResult<Self> {
        let manifest =
            super::manifest_remote::RemoteSnapshotManifest::open(client, manifest_ref).await?;
        let clusters = RemoteClusterUnion::open(client, manifest.clusters(), options).await?;
        Ok(Self { manifest, clusters })
    }

    pub fn manifest(&self) -> &super::manifest_remote::RemoteSnapshotManifest<B> {
        &self.manifest
    }

    pub fn clusters(&self) -> &RemoteClusterUnion<B> {
        &self.clusters
    }

    /// Route lookup uses the physical contributor identities authenticated in
    /// the manifest route pages; no cluster-wide contributor scan is needed.
    pub async fn lookup_route(
        &self,
        dir_key: super::identity::DirKey,
        name: &[u8],
    ) -> WireResult<Option<super::batch::NamespaceEntry>> {
        let Some(route) = self.manifest.lookup_merge_route(dir_key).await? else {
            return Ok(None);
        };
        self.clusters.lookup_route(&route, name).await
    }

    /// Read one bounded page from a merged directory in this immutable
    /// snapshot. The manifest content hash is the cursor identity, so a page
    /// cannot be replayed against a different published snapshot.
    pub async fn read_directory_page(
        &self,
        dir_key: super::identity::DirKey,
        next_ordinal: u64,
        limit: ReadDirLimit,
    ) -> WireResult<Option<DirectoryPage>> {
        let Some(route) = self.manifest.lookup_merge_route(dir_key).await? else {
            return Ok(None);
        };
        let identity = DirectoryIdentity {
            snapshot: self.manifest.object_ref().full_hash,
            dir_key: dir_key.into_bytes(),
        };
        self.clusters
            .read_route_page(&route, identity, next_ordinal, limit)
            .await
            .map(Some)
    }

    pub async fn lookup_attribute(
        &self,
        contributor: NodeRef,
    ) -> WireResult<Option<AttributeGroup>> {
        self.clusters
            .lookup_attribute(contributor, contributor.local_node_id)
            .await
    }

    pub async fn read_extent_range(
        &self,
        contributor: NodeRef,
        file_offset: u64,
        length: u64,
    ) -> WireResult<Vec<ExtentSpan>> {
        self.clusters
            .read_extent_range(contributor, file_offset, length)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::client::ObjectBackend;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::workspace_overlay::clustered_snapshot::cluster_builder::build_single_directory_cluster;
    use crate::workspace_overlay::clustered_snapshot::directory::DirectoryIdentity;
    use crate::workspace_overlay::clustered_snapshot::identity::DirKey;
    use crate::workspace_overlay::clustered_snapshot::merge::{
        ContributionEntry, DirectoryContribution, build_directory_plan,
    };
    use crate::workspace_overlay::clustered_snapshot::name::NameBytes;
    use crate::workspace_overlay::clustered_snapshot::snapshot_manifest::{
        ClusterDescriptor, ManifestObjectRef,
    };
    use crate::workspace_overlay::clustered_snapshot::{
        DirectoryViewRecord, MergeRouteIndex, MergeRouteRecord, MergeRouteSource, MergeRouteWindow,
        NodeRef, WindowSourceKind,
    };

    fn descriptor(
        built: &super::super::cluster_builder::BuiltCluster,
        key: &str,
        object_id: u8,
    ) -> ClusterDescriptor {
        ClusterDescriptor {
            cluster_id: built.superblock.cluster_id,
            metadata_ref: ManifestObjectRef {
                object_id: [object_id; 16],
                kind: 1,
                object_len: built.bytes.len() as u64,
                full_hash: *blake3::hash(&built.bytes).as_bytes(),
                key: key.as_bytes().to_vec(),
            },
            data_seal_ref: ManifestObjectRef {
                object_id: [object_id.wrapping_add(1); 16],
                kind: 2,
                object_len: 1,
                full_hash: [0; 32],
                key: format!("seals/{object_id}.brfds").into_bytes(),
            },
            combined_semantic_hash: [0; 32],
            mount_dir_key: built.superblock.mount_dir_key,
            root_local_node_id: 1,
            flags: 0,
        }
    }

    #[tokio::test]
    async fn route_lookup_uses_persisted_contributor_not_source_id() {
        let root = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(root.path());
        let client = ObjectClient::new(backend.clone());
        let dir_key = DirKey::new([4; 16]);
        let plan = build_directory_plan(
            DirectoryIdentity::from_dir_key([5; 32], dir_key),
            vec![DirectoryContribution {
                dir_key,
                node: NodeRef {
                    cluster_slot: 0,
                    local_node_id: 1,
                },
                attributes_digest: [6; 32],
                entries: vec![ContributionEntry {
                    name: NameBytes::new(b"target".to_vec()).unwrap(),
                    inode: 9,
                    kind: 1,
                    child_dir_key: None,
                    attributes_digest: [0; 32],
                }],
            }],
        )
        .unwrap();
        let built =
            build_single_directory_cluster([7; 16], [8; 16], dir_key, &plan, 1, dir_key).unwrap();
        backend
            .put_object("clusters/test.brfc", &built.bytes)
            .await
            .unwrap();
        let descriptor = ClusterDescriptor {
            cluster_id: built.superblock.cluster_id,
            metadata_ref: ManifestObjectRef {
                object_id: [1; 16],
                kind: 1,
                object_len: built.bytes.len() as u64,
                full_hash: *blake3::hash(&built.bytes).as_bytes(),
                key: b"clusters/test.brfc".to_vec(),
            },
            data_seal_ref: ManifestObjectRef {
                object_id: [2; 16],
                kind: 2,
                object_len: 1,
                full_hash: [0; 32],
                key: b"seals/test.brfds".to_vec(),
            },
            combined_semantic_hash: [0; 32],
            mount_dir_key: dir_key,
            root_local_node_id: 1,
            flags: 0,
        };
        let union =
            RemoteClusterUnion::open(&client, &[descriptor], RemoteClusterOptions::default())
                .await
                .unwrap();
        let route = MergeRouteRecord {
            view: DirectoryViewRecord {
                dir_key,
                canonical_node: NodeRef {
                    cluster_slot: 0,
                    local_node_id: 1,
                },
                visible_entry_count: 1,
                entry_index_root: [0; 32],
            },
            windows: vec![MergeRouteWindow {
                lower: None,
                upper: None,
                visible_entry_count: 1,
                sources: vec![MergeRouteSource {
                    // Deliberately unrelated to the physical cluster slot.
                    source_id: 99,
                    kind: WindowSourceKind::FileEntries,
                    contributor: NodeRef {
                        cluster_slot: 0,
                        local_node_id: 1,
                    },
                    first_name: NameBytes::new(b"target".to_vec()).unwrap(),
                    last_name: NameBytes::new(b"target".to_vec()).unwrap(),
                    entry_count: 1,
                }],
            }],
        };
        let encoded = MergeRouteIndex::encode(&[route.clone()]).unwrap();
        let decoded = MergeRouteIndex::decode(&encoded).unwrap().remove(0);
        let found = union.lookup_route(&decoded, b"target").await.unwrap();
        assert!(matches!(
            found,
            Some(super::super::batch::NamespaceEntry::NewNode { .. })
        ));

        let page = union
            .read_route_page(
                &decoded,
                DirectoryIdentity {
                    snapshot: [1; 32],
                    dir_key: dir_key.into_bytes(),
                },
                0,
                ReadDirLimit::DEFAULT,
            )
            .await
            .unwrap();
        assert!(page.end);
        assert_eq!(page.next_ordinal, 1);
        assert_eq!(page.entries[0].name.as_bytes(), b"target");
        assert_eq!(page.entries[0].kind, 1);
    }

    #[tokio::test]
    async fn bounded_remote_readdir_folds_directory_duplicates_and_rejects_file_collisions() {
        let root = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(root.path());
        let client = ObjectClient::new(backend.clone());
        let dir_key = DirKey::new([41; 16]);
        let child_key = DirKey::new([42; 16]);
        let build = |cluster_slot: u32, kind: u8| {
            build_directory_plan(
                DirectoryIdentity::from_dir_key([43; 32], dir_key),
                vec![DirectoryContribution {
                    dir_key,
                    node: NodeRef {
                        cluster_slot,
                        local_node_id: 1,
                    },
                    attributes_digest: [44; 32],
                    entries: vec![ContributionEntry {
                        name: NameBytes::new(b"shared".to_vec()).unwrap(),
                        inode: 100 + u64::from(cluster_slot),
                        kind,
                        child_dir_key: (kind == 2).then_some(child_key),
                        attributes_digest: [45; 32],
                    }],
                }],
            )
            .unwrap()
        };
        let first = build(0, 2);
        let second = build(1, 2);
        let first_cluster =
            build_single_directory_cluster([46; 16], [47; 16], dir_key, &first, 1, dir_key)
                .unwrap();
        let second_cluster =
            build_single_directory_cluster([48; 16], [47; 16], dir_key, &second, 1, dir_key)
                .unwrap();
        backend
            .put_object("clusters/0.brfc", &first_cluster.bytes)
            .await
            .unwrap();
        backend
            .put_object("clusters/1.brfc", &second_cluster.bytes)
            .await
            .unwrap();
        let union = RemoteClusterUnion::open(
            &client,
            &[
                descriptor(&first_cluster, "clusters/0.brfc", 50),
                descriptor(&second_cluster, "clusters/1.brfc", 52),
            ],
            RemoteClusterOptions::default(),
        )
        .await
        .unwrap();
        let route = MergeRouteRecord {
            view: DirectoryViewRecord {
                dir_key,
                canonical_node: NodeRef {
                    cluster_slot: 0,
                    local_node_id: 1,
                },
                visible_entry_count: 1,
                entry_index_root: [0; 32],
            },
            windows: vec![MergeRouteWindow {
                lower: None,
                upper: None,
                visible_entry_count: 1,
                sources: vec![
                    MergeRouteSource {
                        source_id: 1,
                        kind: WindowSourceKind::CanonicalDirectoryEntries,
                        contributor: NodeRef {
                            cluster_slot: 0,
                            local_node_id: 1,
                        },
                        first_name: NameBytes::new(b"shared".to_vec()).unwrap(),
                        last_name: NameBytes::new(b"shared".to_vec()).unwrap(),
                        entry_count: 1,
                    },
                    MergeRouteSource {
                        source_id: 2,
                        kind: WindowSourceKind::CanonicalDirectoryEntries,
                        contributor: NodeRef {
                            cluster_slot: 1,
                            local_node_id: 1,
                        },
                        first_name: NameBytes::new(b"shared".to_vec()).unwrap(),
                        last_name: NameBytes::new(b"shared".to_vec()).unwrap(),
                        entry_count: 1,
                    },
                ],
            }],
        };
        let identity = DirectoryIdentity {
            snapshot: [53; 32],
            dir_key: dir_key.into_bytes(),
        };
        let page = union
            .read_route_page(&route, identity, 0, ReadDirLimit::DEFAULT)
            .await
            .unwrap();
        assert!(page.end);
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].kind, 2);

        let file_first = build(0, 1);
        let file_second = build(1, 1);
        let file_first_cluster =
            build_single_directory_cluster([54; 16], [47; 16], dir_key, &file_first, 1, dir_key)
                .unwrap();
        let file_second_cluster =
            build_single_directory_cluster([55; 16], [47; 16], dir_key, &file_second, 1, dir_key)
                .unwrap();
        backend
            .put_object("clusters/file-0.brfc", &file_first_cluster.bytes)
            .await
            .unwrap();
        backend
            .put_object("clusters/file-1.brfc", &file_second_cluster.bytes)
            .await
            .unwrap();
        let file_union = RemoteClusterUnion::open(
            &client,
            &[
                descriptor(&file_first_cluster, "clusters/file-0.brfc", 56),
                descriptor(&file_second_cluster, "clusters/file-1.brfc", 58),
            ],
            RemoteClusterOptions::default(),
        )
        .await
        .unwrap();
        assert!(
            file_union
                .read_route_page(&route, identity, 0, ReadDirLimit::DEFAULT)
                .await
                .is_err()
        );
    }
}
