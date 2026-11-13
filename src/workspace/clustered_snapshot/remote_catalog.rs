//! Read-only frozen-catalog adapter for a remote clustered v2 snapshot.
//!
//! The existing [`FrozenReadonlyMeta`] already contains the POSIX-facing
//! read-only semantics and the paged directory handle implementation. This
//! adapter supplies its narrow `FrozenCatalog` contract from BRFSM/BRFCL
//! range reads, retaining only inode records observed by the current mount.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;

use crate::cadapter::client::ObjectBackend;
use crate::native_base::frozen::catalog::{FrozenCatalog, FrozenDirectoryEntry};
use crate::native_base::frozen::readonly::FrozenReadonlyMeta;
use crate::native_base::frozen::{FrozenExtent, FrozenInodeRecord, SnapshotManifest};
use crate::native_base::wire::container::{Codec, ObjectKind};
use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::refs::{ObjectRef, PageAddress, PageKind, RootRef};
use crate::vfs::chunk_id_for;

use super::batch::{NamespaceEntry, NodeRecord};
use super::directory::{NodeRef, ReadDirLimit};
use super::identity::DirKey;
use super::remote_union::RemoteSnapshot;

#[derive(Clone, Debug)]
struct CachedNode {
    contributor: NodeRef,
    dir_key: Option<DirKey>,
    attr: FrozenInodeRecord,
}

/// A lazily populated immutable catalog backed by one `RemoteSnapshot`.
pub struct RemoteFrozenCatalog<B: ObjectBackend + Clone> {
    snapshot: Arc<RemoteSnapshot<B>>,
    manifest: SnapshotManifest,
    nodes: DashMap<u64, CachedNode>,
    parents: DashMap<u64, Vec<(u64, Vec<u8>)>>,
    root_dir_key: DirKey,
    chunk_size: u64,
}

impl<B: ObjectBackend + Clone + 'static> RemoteFrozenCatalog<B> {
    pub fn new(snapshot: Arc<RemoteSnapshot<B>>) -> Self {
        let root_dir_key = snapshot.manifest().superblock().root_dir_key;
        let chunk_size = snapshot
            .clusters()
            .clusters()
            .first()
            .map(|cluster| cluster.superblock().chunk_size)
            .filter(|size| *size != 0)
            .unwrap_or(1 << 20);
        let (node_count, directory_count) = snapshot.clusters().clusters().iter().fold(
            (0u64, 0u64),
            |(nodes, directories), cluster| {
                (
                    nodes.saturating_add(u64::from(cluster.superblock().node_count)),
                    directories.saturating_add(u64::from(
                        cluster.superblock().directory_contribution_count,
                    )),
                )
            },
        );
        let manifest = SnapshotManifest {
            volume_id: snapshot.manifest().superblock().volume_id,
            storage_namespace_id: [0; 16],
            chunk_size,
            block_size: u32::try_from(chunk_size).unwrap_or(u32::MAX),
            required_features: 0,
            logical_revision: snapshot.manifest().superblock().semantic_hash,
            namespace_digest: snapshot.manifest().superblock().semantic_hash,
            binding_digest: [0; 32],
            namespace_mode: 2,
            kv_layer_id: None,
            kv_sealed_version: None,
            namespace_root: Some(placeholder_root()),
            data_root: placeholder_root(),
            inventory_root: placeholder_root(),
            file_count: node_count.saturating_sub(directory_count),
            directory_count,
            total_logical_bytes: 0,
            created_at_ns: 0,
        };
        let catalog = Self {
            snapshot,
            manifest,
            nodes: DashMap::new(),
            parents: DashMap::new(),
            root_dir_key,
            chunk_size,
        };
        catalog.nodes.insert(
            1,
            CachedNode {
                contributor: NodeRef {
                    cluster_slot: 0,
                    local_node_id: 1,
                },
                dir_key: Some(root_dir_key),
                attr: FrozenInodeRecord {
                    kind: 2,
                    mode: 0o040755,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    nlink: 1,
                    size: 0,
                    atime_ns: 0,
                    mtime_ns: 0,
                    ctime_ns: 0,
                    parent_hint: None,
                    symlink_target: None,
                },
            },
        );
        catalog
    }

    /// Construct the existing POSIX read-only facade over this v2 catalog.
    pub fn readonly_meta(self: &Arc<Self>) -> FrozenReadonlyMeta {
        FrozenReadonlyMeta::new(Arc::clone(self), 1)
    }

    fn stable_inode(node: NodeRef) -> u64 {
        (u64::from(node.cluster_slot) << 32) | u64::from(node.local_node_id)
    }

    fn parent_dir_key(&self, parent: u64) -> Option<DirKey> {
        if parent == 1 {
            return Some(self.root_dir_key);
        }
        self.nodes.get(&parent).and_then(|node| node.dir_key)
    }

    async fn attr_for_node(
        &self,
        contributor: NodeRef,
        node: &NodeRecord,
        parent: Option<u64>,
    ) -> Result<FrozenInodeRecord, WireError> {
        let symlink_target = if node.kind == 3 {
            self.snapshot
                .clusters()
                .lookup_attribute(contributor, node.local_node_id)
                .await?
                .and_then(|group| group.symlink_target)
        } else {
            None
        };
        let mode_type = match node.kind {
            1 => 0o100000,
            2 => 0o040000,
            3 => 0o120000,
            _ => 0,
        };
        Ok(FrozenInodeRecord {
            kind: node.kind,
            mode: if node.mode & 0o170000 == 0 {
                node.mode | mode_type
            } else {
                node.mode
            },
            uid: 0,
            gid: 0,
            rdev: 0,
            nlink: 1,
            size: node.size,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            parent_hint: parent,
            symlink_target,
        })
    }

    async fn remember_entry(
        &self,
        parent: u64,
        name: &[u8],
        contributor: NodeRef,
        entry: NamespaceEntry,
    ) -> Result<(u64, FrozenInodeRecord), WireError> {
        let (local_node_id, node, dir_key) = match entry {
            NamespaceEntry::NewNode { node, .. } => {
                let dir_key = node.dir_key;
                (node.local_node_id, Some(node), dir_key)
            }
            NamespaceEntry::ExistingNode { local_node_id, .. } => (local_node_id, None, None),
        };
        // The route contributor identifies the physical parent directory.
        // The child node has the same cluster slot but its own local id.
        let physical_contributor = NodeRef {
            cluster_slot: contributor.cluster_slot,
            local_node_id,
        };
        let inode = Self::stable_inode(physical_contributor);
        let attr = if let Some(node) = node {
            self.attr_for_node(physical_contributor, &node, Some(parent))
                .await?
        } else {
            self.nodes
                .get(&inode)
                .map(|cached| cached.attr.clone())
                .ok_or_else(|| {
                    WireError::invalid(
                        "v2 namespace",
                        "hardlink target was observed before its inode record",
                    )
                })?
        };
        self.nodes.insert(
            inode,
            CachedNode {
                contributor: physical_contributor,
                dir_key,
                attr: attr.clone(),
            },
        );
        let mut links = self.parents.entry(inode).or_default();
        if !links.iter().any(|(known_parent, known_name)| {
            *known_parent == parent && known_name.as_slice() == name
        }) {
            links.push((parent, name.to_vec()));
        }
        Ok((inode, attr))
    }

    async fn page_entries(
        &self,
        parent: u64,
        child_offset: u64,
        limit: usize,
    ) -> Result<Vec<FrozenDirectoryEntry>, WireError> {
        let dir_key = self
            .parent_dir_key(parent)
            .ok_or_else(|| WireError::invalid("v2 readdir", "directory identity is unknown"))?;
        let page = self
            .snapshot
            .read_directory_page(
                dir_key,
                child_offset,
                ReadDirLimit {
                    max_entries: limit,
                    max_owned_bytes: 1024 * 1024,
                },
            )
            .await?
            .ok_or_else(|| WireError::invalid("v2 readdir", "directory route is missing"))?;
        let mut entries = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let contributor = entry.contributor.ok_or_else(|| {
                WireError::invalid("v2 readdir", "remote page entry has no contributor")
            })?;
            let namespace = self
                .snapshot
                .clusters()
                .cluster(contributor.cluster_slot)?
                .lookup_namespace(contributor.local_node_id, entry.name.as_bytes())
                .await?
                .ok_or_else(|| WireError::invalid("v2 readdir", "route entry disappeared"))?;
            let (inode, attr) = self
                .remember_entry(parent, entry.name.as_bytes(), contributor, namespace)
                .await?;
            entries.push(FrozenDirectoryEntry {
                name: entry.name.into_bytes(),
                inode,
                attr,
            });
        }
        Ok(entries)
    }
}

#[async_trait]
impl<B: ObjectBackend + Clone + 'static> FrozenCatalog for RemoteFrozenCatalog<B> {
    fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    async fn lookup_inode(
        &self,
        inode: u64,
    ) -> Result<Option<FrozenInodeRecord>, crate::native_base::frozen::FrozenReadError> {
        if let Some(node) = self.nodes.get(&inode) {
            return Ok(Some(node.attr.clone()));
        }
        if inode == 1 {
            return Ok(self.nodes.get(&inode).map(|node| node.attr.clone()));
        }
        let contributor = NodeRef {
            cluster_slot: u32::try_from(inode >> 32)
                .map_err(|_| WireError::invalid("v2 inode", "cluster slot exceeds u32"))?,
            local_node_id: u32::try_from(inode & u64::from(u32::MAX))
                .map_err(|_| WireError::invalid("v2 inode", "local node id exceeds u32"))?,
        };
        if contributor.local_node_id == 0 {
            return Ok(None);
        }
        let Some(node) = self
            .snapshot
            .clusters()
            .cluster(contributor.cluster_slot)?
            .lookup_node(contributor.local_node_id)
            .await?
        else {
            return Ok(None);
        };
        let attr = self.attr_for_node(contributor, &node, None).await?;
        self.nodes.insert(
            inode,
            CachedNode {
                contributor,
                dir_key: node.dir_key,
                attr: attr.clone(),
            },
        );
        Ok(Some(attr))
    }

    async fn lookup_dentry(
        &self,
        parent: u64,
        name: &[u8],
    ) -> Result<Option<(u64, FrozenInodeRecord)>, crate::native_base::frozen::FrozenReadError> {
        let Some(dir_key) = self.parent_dir_key(parent) else {
            return Ok(None);
        };
        let Some((contributor, entry)) = self
            .snapshot
            .lookup_route_with_contributor(dir_key, name)
            .await?
        else {
            return Ok(None);
        };
        self.remember_entry(parent, name, contributor, entry)
            .await
            .map(Some)
            .map_err(Into::into)
    }

    async fn readdir(
        &self,
        parent: u64,
    ) -> Result<Vec<FrozenDirectoryEntry>, crate::native_base::frozen::FrozenReadError> {
        let mut offset = 0u64;
        let mut entries = Vec::new();
        loop {
            let page = self.page_entries(parent, offset, 4096).await?;
            if page.is_empty() {
                break;
            }
            offset = offset.saturating_add(page.len() as u64);
            let end = page.len() < 4096;
            entries.extend(page);
            if end {
                break;
            }
        }
        Ok(entries)
    }

    async fn readdir_page(
        &self,
        parent: u64,
        child_offset: u64,
        limit: usize,
    ) -> Result<Vec<FrozenDirectoryEntry>, crate::native_base::frozen::FrozenReadError> {
        if limit == 0 || limit > 4096 {
            return Err(WireError::invalid("v2 readdir", "page limit is outside bounds").into());
        }
        self.page_entries(parent, child_offset, limit)
            .await
            .map_err(Into::into)
    }

    async fn names_for_inode(
        &self,
        inode: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, crate::native_base::frozen::FrozenReadError> {
        Ok(self
            .parents
            .get(&inode)
            .map(|links| links.clone())
            .unwrap_or_default())
    }

    async fn readlink(
        &self,
        inode: u64,
    ) -> Result<Option<Vec<u8>>, crate::native_base::frozen::FrozenReadError> {
        Ok(self
            .lookup_inode(inode)
            .await?
            .and_then(|node| node.symlink_target))
    }

    async fn query_extents(
        &self,
        inode: u64,
        chunk_index: u64,
        start: u64,
        end: u64,
    ) -> Result<Vec<FrozenExtent>, crate::native_base::frozen::FrozenReadError> {
        // A direct open/getattr may arrive without a preceding dentry lookup;
        // populate the physical contributor before resolving extents.
        self.lookup_inode(inode).await?;
        let Some(node) = self.nodes.get(&inode) else {
            return Ok(Vec::new());
        };
        if node.attr.kind != 1 || end <= start {
            return Ok(Vec::new());
        }
        let base = chunk_index
            .checked_mul(self.chunk_size)
            .ok_or_else(|| WireError::LimitExceeded("chunk offset overflows u64".into()))?;
        let global_start = base
            .checked_add(start)
            .ok_or_else(|| WireError::LimitExceeded("extent query overflows u64".into()))?;
        // `FrozenReadonlyMeta::get_slices` uses `u64::MAX` as an open-ended
        // query within the selected chunk. Keep that sentinel local to the
        // chunk; treating it as a global end would either overflow for a
        // non-zero chunk or return extents belonging to later chunks.
        let relative_end = if end == u64::MAX {
            self.chunk_size
        } else {
            end
        };
        let global_end = base
            .checked_add(relative_end)
            .ok_or_else(|| WireError::LimitExceeded("extent query overflows u64".into()))?;
        let spans = self
            .snapshot
            .read_extent_range(
                node.contributor,
                global_start,
                global_end.saturating_sub(global_start),
            )
            .await?;
        let chunk_id = chunk_id_for(inode as i64, chunk_index)
            .map_err(|error| WireError::invalid("v2 extent", error.to_string()))?;
        spans
            .into_iter()
            .map(|span| {
                let locator = self
                    .snapshot
                    .register_data_slice(node.contributor, span.slice_id)?;
                let offset = span
                    .file_offset
                    .checked_sub(base)
                    .ok_or_else(|| WireError::invalid("v2 extent", "span precedes chunk"))?;
                Ok(FrozenExtent {
                    inode,
                    chunk_index,
                    offset,
                    length: span.logical_length,
                    value: crate::native_base::frozen::encode_extent_slice_value(
                        span.logical_length,
                        locator,
                        chunk_id,
                        offset,
                    ),
                })
            })
            .collect::<WireResult<Vec<_>>>()
            .map_err(Into::into)
    }
}

fn placeholder_root() -> RootRef {
    RootRef {
        object: ObjectRef {
            object_id: [0; 16],
            kind: ObjectKind::FrozenMetadata.as_u8(),
            object_len: 0,
            full_hash: [0; 32],
            key: b"v2-placeholder".to_vec(),
        },
        address: PageAddress {
            offset: 0,
            stored_len: 0,
            raw_len: 0,
            codec: Codec::None,
            page_kind: PageKind::GenericKeyValue,
            level: 0,
            entry_count: 0,
            stored_digest: [0; 32],
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::client::ObjectClient;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::chunk::{BlockStore, ChunkLayout, DEFAULT_BLOCK_SIZE};
    use crate::meta::layer::MetaLayer;
    use crate::native_base::ingest::ConsistencyPolicy;
    use crate::vfs::config::VFSConfig;
    use crate::vfs::fs::VFS;
    use crate::workspace::clustered_snapshot::publication::build_single_cluster_manifest;
    use crate::workspace::clustered_snapshot::{
        RemoteClusterOptions, RemoteDataBlockStore, RemoteSnapshot,
    };

    #[cfg(unix)]
    #[tokio::test]
    async fn v2_remote_catalog_resolves_root_lookup_and_extent() {
        let root = tempfile::tempdir().unwrap();
        let payload = vec![0x5a; 2 * 1024 * 1024];
        std::fs::write(root.path().join("sample"), &payload).unwrap();
        let built = super::super::ingest::build_local_directory_cluster(
            root.path(),
            ConsistencyPolicy::SnapshotBacked,
            [0x51; 16],
            [0x52; 16],
        )
        .unwrap();
        let data = built.data.as_ref().unwrap().clone();
        let bundle = build_single_cluster_manifest(&built, [0x53; 16], [0x54; 16]).unwrap();
        let object_root = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(object_root.path());
        let client = ObjectClient::new(backend.clone());
        let descriptor = &bundle.manifest.clusters[0];
        client
            .put_object(
                std::str::from_utf8(&descriptor.metadata_ref.key).unwrap(),
                &built.cluster.bytes,
            )
            .await
            .unwrap();
        client
            .put_object(
                std::str::from_utf8(&descriptor.data_seal_ref.key).unwrap(),
                &data.data_seal,
            )
            .await
            .unwrap();
        client
            .put_object(&data.object_key, &data.data_pack)
            .await
            .unwrap();
        client
            .put_object(
                std::str::from_utf8(&bundle.manifest_ref.key).unwrap(),
                &bundle.bytes,
            )
            .await
            .unwrap();
        let snapshot = Arc::new(
            RemoteSnapshot::open_by_key(
                &client,
                std::str::from_utf8(&bundle.manifest_ref.key).unwrap(),
                RemoteClusterOptions::default(),
            )
            .await
            .unwrap(),
        );
        let catalog = Arc::new(RemoteFrozenCatalog::new(snapshot));
        let found = catalog.lookup_dentry(1, b"sample").await.unwrap().unwrap();
        assert_eq!(found.1.size, payload.len() as u64);
        // Drop the dentry-populated record and verify a direct inode lookup
        // can recover it from the namespace index's node-id range metadata.
        catalog.nodes.remove(&found.0);
        let cold_attr = catalog.lookup_inode(found.0).await.unwrap().unwrap();
        assert_eq!(cold_attr.size, payload.len() as u64);
        let extents = catalog
            .query_extents(found.0 as u64, 0, 0, u64::MAX)
            .await
            .unwrap();
        assert_eq!(extents.len(), 1);
        assert_eq!(
            extents.iter().map(|extent| extent.length).sum::<u64>(),
            catalog.chunk_size
        );

        let second_extents = catalog
            .query_extents(found.0 as u64, 1, 0, u64::MAX)
            .await
            .unwrap();
        assert_eq!(second_extents.len(), 1);
        assert_eq!(
            second_extents
                .iter()
                .map(|extent| extent.length)
                .sum::<u64>(),
            payload.len() as u64 - catalog.chunk_size
        );

        let readonly = catalog.readonly_meta();
        let chunk_id = chunk_id_for(found.0 as i64, 0).unwrap();
        let slices = readonly.get_slices(chunk_id).await.unwrap();
        assert_eq!(slices.len(), 1);
        let second_chunk_id = chunk_id_for(found.0 as i64, 1).unwrap();
        let second_chunk_slices = readonly.get_slices(second_chunk_id).await.unwrap();
        assert_eq!(second_chunk_slices.len(), 1);
        let store = RemoteDataBlockStore::new(Arc::clone(&catalog.snapshot), DEFAULT_BLOCK_SIZE);
        let mut read_back = vec![0u8; slices[0].length as usize];
        store
            .read_range((slices[0].slice_id, 0), 0, &mut read_back)
            .await
            .unwrap();
        assert_eq!(read_back, payload[..read_back.len()]);

        let layout = ChunkLayout {
            chunk_size: catalog.manifest().chunk_size,
            block_size: catalog.manifest().block_size,
        };
        let fs = VFS::from_readonly_components(
            VFSConfig::new_with_cache_config(layout, Default::default()),
            Arc::new(store),
            Arc::new(catalog.readonly_meta()),
        )
        .unwrap();
        let attr = fs.stat_ino(found.0 as i64).await.unwrap();
        let handle = fs.open(attr.ino, attr, true, false, false).await.unwrap();
        let vfs_bytes = fs.read(handle, 4096, 8192).await.unwrap();
        assert_eq!(vfs_bytes, payload[4096..12288]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn v2_remote_catalog_pages_large_nested_directory_with_independent_files() {
        let root = tempfile::tempdir().unwrap();
        let flat = root.path().join("flat");
        let nested = root.path().join("nested").join("level-1");
        std::fs::create_dir_all(&flat).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        for index in 0..4_200u32 {
            let name = format!("file-{index:05}");
            let mut payload = vec![0u8; 256];
            payload[..4].copy_from_slice(&index.to_le_bytes());
            payload[4..].fill((index as u8).wrapping_mul(17).wrapping_add(3));
            std::fs::write(flat.join(name), payload).unwrap();
        }
        std::fs::write(nested.join("marker-a"), b"nested-a").unwrap();
        std::fs::write(nested.join("marker-b"), b"nested-b").unwrap();

        let built = super::super::ingest::build_local_directory_cluster(
            root.path(),
            ConsistencyPolicy::SnapshotBacked,
            [0x61; 16],
            [0x62; 16],
        )
        .unwrap();
        let data = built.data.as_ref().unwrap().clone();
        let bundle = build_single_cluster_manifest(&built, [0x63; 16], [0x64; 16]).unwrap();
        let object_root = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::new(object_root.path());
        let client = ObjectClient::new(backend.clone());
        let descriptor = &bundle.manifest.clusters[0];
        client
            .put_object(
                std::str::from_utf8(&descriptor.metadata_ref.key).unwrap(),
                &built.cluster.bytes,
            )
            .await
            .unwrap();
        client
            .put_object(
                std::str::from_utf8(&descriptor.data_seal_ref.key).unwrap(),
                &data.data_seal,
            )
            .await
            .unwrap();
        client
            .put_object(&data.object_key, &data.data_pack)
            .await
            .unwrap();
        client
            .put_object(
                std::str::from_utf8(&bundle.manifest_ref.key).unwrap(),
                &bundle.bytes,
            )
            .await
            .unwrap();

        let snapshot = Arc::new(
            RemoteSnapshot::open_by_key(
                &client,
                std::str::from_utf8(&bundle.manifest_ref.key).unwrap(),
                RemoteClusterOptions::default(),
            )
            .await
            .unwrap(),
        );
        let catalog = Arc::new(RemoteFrozenCatalog::new(snapshot));
        let (flat_inode, flat_attr) = catalog.lookup_dentry(1, b"flat").await.unwrap().unwrap();
        assert_eq!(flat_attr.kind, 2);

        let mut offset = 0u64;
        let mut names = Vec::new();
        loop {
            let page = catalog.readdir_page(flat_inode, offset, 127).await.unwrap();
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= 127);
            names.extend(page.iter().map(|entry| entry.name.clone()));
            offset = offset.saturating_add(page.len() as u64);
        }
        assert_eq!(names.len(), 4_200);
        assert!(names.windows(2).all(|pair| pair[0] < pair[1]));

        let (nested_inode, nested_attr) =
            catalog.lookup_dentry(1, b"nested").await.unwrap().unwrap();
        assert_eq!(nested_attr.kind, 2);
        let (level_inode, level_attr) = catalog
            .lookup_dentry(nested_inode, b"level-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(level_attr.kind, 2);
        let nested_page = catalog.readdir_page(level_inode, 0, 16).await.unwrap();
        assert_eq!(
            nested_page
                .iter()
                .map(|entry| entry.name.as_slice())
                .collect::<Vec<_>>(),
            vec![b"marker-a".as_slice(), b"marker-b".as_slice()]
        );

        let first = catalog
            .lookup_dentry(flat_inode, b"file-00000")
            .await
            .unwrap()
            .unwrap();
        let second = catalog
            .lookup_dentry(flat_inode, b"file-00001")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(first.1.size, 0);
        assert_ne!(first.0, second.0);
    }
}
