//! Immutable v2 snapshot publication boundary.
//!
//! A manifest is not mountable merely because its bytes exist in object
//! storage.  Before the workspace head can reference it, every selected
//! cluster must have an authenticated metadata object and a closed Data Seal.
//! This module performs that verification and then exposes one small CAS
//! trait for the concrete workspace-control backend.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::cadapter::client::{ObjectBackend, ObjectClient};
use crate::native_base::wire::container::{ContainerFooter, FOOTER_LEN};
use crate::native_base::wire::error::{WireError, WireResult};
use crate::native_base::wire::frame::{
    ALIGNMENT, FRAME_HEADER_LEN, FrameHeader, MAX_ENCODED_FRAME_PAYLOAD,
};

use super::attribute::AttributeBatch;
use super::batch::{BatchKind, EncodedBatch, NamespaceBatch};
use super::cluster_format::SnapshotSuperblock;
use super::cluster_format::{ClusterSuperblock, IndexRootRef, SUPERBLOCK_LEN};
use super::data_pack::RemoteDataPack;
use super::data_seal::{DataObjectDescriptor, DataSealSnapshot, FrameDescriptor, SliceDescriptor};
use super::extent::ExtentBatch;
use super::ingest::BuiltSourceCluster;
use super::merge_route::MergeRouteIndex;
use super::merkle_index::IndexEntry;
use super::mount_trie::{MountTrie, MountTrieNode};
use super::range_reader::{MemoryRangeReader, read_batch, read_index_child, read_index_root};
use super::snapshot_manifest::{
    ClusterDescriptor, ManifestIndexPayload, ManifestObjectRef, SnapshotManifest,
};

/// Immutable identity placed in the mutable workspace head.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotRef {
    pub manifest_hash: [u8; 32],
    pub superblock_digest: [u8; 32],
    pub object_len: u64,
}

impl SnapshotRef {
    fn from_manifest(bytes: &[u8]) -> WireResult<Self> {
        if bytes.len() < SUPERBLOCK_LEN {
            return Err(WireError::Truncated {
                what: "snapshot manifest",
                need: SUPERBLOCK_LEN,
                have: bytes.len(),
            });
        }
        Ok(Self {
            manifest_hash: *blake3::hash(bytes).as_bytes(),
            superblock_digest: *blake3::hash(&bytes[..SUPERBLOCK_LEN]).as_bytes(),
            object_len: bytes.len() as u64,
        })
    }
}

/// The value stored by the workspace head CAS record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkspaceHead {
    pub epoch: u64,
    pub snapshot: SnapshotRef,
}

/// Backend-neutral conditional head update.  Implementations must perform the
/// expected-value check and update atomically; a false result means another
/// writer won the race.
#[async_trait]
pub trait WorkspaceHeadStore: Send + Sync {
    async fn load(&self) -> WireResult<Option<WorkspaceHead>>;

    async fn compare_and_swap(
        &self,
        expected: Option<WorkspaceHead>,
        desired: WorkspaceHead,
    ) -> WireResult<bool>;
}

/// Small in-memory implementation used by integration tests and local tools.
/// Production control stores can implement [`WorkspaceHeadStore`] without
/// changing the object verification path.
#[derive(Clone, Default)]
pub struct MemoryWorkspaceHeadStore {
    head: Arc<Mutex<Option<WorkspaceHead>>>,
}

impl MemoryWorkspaceHeadStore {
    pub async fn get(&self) -> Option<WorkspaceHead> {
        *self.head.lock().await
    }
}

#[async_trait]
impl WorkspaceHeadStore for MemoryWorkspaceHeadStore {
    async fn load(&self) -> WireResult<Option<WorkspaceHead>> {
        Ok(*self.head.lock().await)
    }

    async fn compare_and_swap(
        &self,
        expected: Option<WorkspaceHead>,
        desired: WorkspaceHead,
    ) -> WireResult<bool> {
        let mut head = self.head.lock().await;
        if *head == Some(desired) {
            return Ok(true);
        }
        if *head != expected {
            return Ok(false);
        }
        if let Some(current) = *head {
            if desired.epoch <= current.epoch {
                return Err(WireError::invalid(
                    "workspace head",
                    "new epoch is not greater than the current epoch",
                ));
            }
        } else if desired.epoch != 1 {
            return Err(WireError::invalid(
                "workspace head",
                "the first published epoch must be one",
            ));
        }
        *head = Some(desired);
        Ok(true)
    }
}

/// Result of a successful publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublishedSnapshot {
    pub snapshot: SnapshotRef,
    pub head: WorkspaceHead,
}

/// Locally assembled single-cluster publication bundle. The bytes remain
/// caller-owned: upload metadata and the Data Seal/DataPack objects first,
/// then pass `manifest_ref` to [`SnapshotPublisher::publish`].
#[derive(Clone, Debug)]
pub struct BuiltSingleClusterManifest {
    pub manifest: SnapshotManifest,
    pub bytes: Vec<u8>,
    pub manifest_ref: ManifestObjectRef,
}

/// Assemble a complete one-cluster v2 manifest from a source producer result.
///
/// This is intentionally limited to one cluster/root mount. It supplies the
/// canonical root mount trie and an empty merge-route index; multi-cluster
/// route planning remains a separate producer phase.
pub fn build_single_cluster_manifest(
    built: &BuiltSourceCluster,
    snapshot_id: [u8; 16],
    route_seed: [u8; 16],
) -> WireResult<BuiltSingleClusterManifest> {
    let data = built.data.as_ref().ok_or_else(|| {
        WireError::invalid(
            "snapshot manifest",
            "source cluster has no DataPack/Data Seal payload",
        )
    })?;
    let cluster = &built.cluster;
    let volume_id = cluster.superblock.volume_id;
    let cluster_id = cluster.superblock.cluster_id;
    let metadata_key = format!("clusters/{}/metadata.brfc", hex::encode(cluster_id));
    let seal_key = format!("clusters/{}/data/seal.brfds", hex::encode(cluster_id));
    let manifest_key = format!(
        "snapshots/{}/{}.brfsm",
        hex::encode(volume_id),
        hex::encode(snapshot_id)
    );
    let seal = DataSealSnapshot::open(data.data_seal.clone())?;
    if seal.cluster_id() != cluster_id || seal.volume_id() != volume_id {
        return Err(WireError::invalid(
            "snapshot manifest",
            "source Data Seal identity does not match cluster metadata",
        ));
    }
    let metadata_ref = ManifestObjectRef {
        object_id: derive_object_id(1, &cluster.bytes),
        kind: 1,
        object_len: cluster.bytes.len() as u64,
        full_hash: *blake3::hash(&cluster.bytes).as_bytes(),
        key: metadata_key.into_bytes(),
    };
    let data_seal_ref = ManifestObjectRef {
        object_id: derive_object_id(2, &data.data_seal),
        kind: 2,
        object_len: data.data_seal.len() as u64,
        full_hash: *blake3::hash(&data.data_seal).as_bytes(),
        key: seal_key.into_bytes(),
    };
    let mut combined = blake3::Hasher::new();
    combined.update(b"BrewFS.SealedCluster.v2");
    combined.update(&cluster.superblock.metadata_semantic_hash);
    combined.update(&seal.semantic_hash());
    let descriptor = ClusterDescriptor {
        cluster_id,
        metadata_ref,
        data_seal_ref,
        combined_semantic_hash: *combined.finalize().as_bytes(),
        mount_dir_key: built.root_dir_key,
        root_local_node_id: 1,
        flags: 0,
    };
    let mount_bytes = MountTrie {
        nodes: vec![MountTrieNode {
            node_id: 1,
            parent_id: 0,
            component: None,
            cluster_slots: vec![0],
            children: Vec::new(),
        }],
    }
    .encode()?;
    let route_bytes = MergeRouteIndex::encode(&[])?;
    let mut snapshot_semantic = blake3::Hasher::new();
    snapshot_semantic.update(b"BrewFS.Snapshot.v2");
    snapshot_semantic.update(&volume_id);
    snapshot_semantic.update(&snapshot_id);
    snapshot_semantic.update(&descriptor.combined_semantic_hash);
    let manifest = SnapshotManifest {
        superblock: SnapshotSuperblock {
            volume_id,
            snapshot_id,
            semantic_hash: *snapshot_semantic.finalize().as_bytes(),
            route_seed,
            cluster_count: 1,
            mount_count: 1,
            merged_directory_count: 0,
            root_dir_key: built.root_dir_key,
            index_roots: [
                empty_manifest_root(),
                empty_manifest_root(),
                empty_manifest_root(),
            ],
        },
        clusters: vec![descriptor],
        mount_index: ManifestIndexPayload {
            entry_count: 1,
            bytes: mount_bytes,
        },
        route_index: ManifestIndexPayload {
            entry_count: 0,
            bytes: route_bytes,
        },
    };
    let bytes = manifest.encode()?;
    let manifest = SnapshotManifest::decode(&bytes)?;
    let manifest_ref = ManifestObjectRef {
        object_id: derive_object_id(3, &bytes),
        kind: 3,
        object_len: bytes.len() as u64,
        full_hash: *blake3::hash(&bytes).as_bytes(),
        key: manifest_key.into_bytes(),
    };
    Ok(BuiltSingleClusterManifest {
        manifest,
        bytes,
        manifest_ref,
    })
}

fn derive_object_id(kind: u8, bytes: &[u8]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"BrewFS.ManifestObjectId.v2");
    hasher.update(&[kind]);
    hasher.update(bytes);
    hasher.finalize().as_bytes()[..16].try_into().unwrap()
}

fn empty_manifest_root() -> IndexRootRef {
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

/// Verifies a complete immutable manifest graph before publishing its head.
pub struct SnapshotPublisher<B, H>
where
    B: ObjectBackend + Clone,
    H: WorkspaceHeadStore,
{
    client: ObjectClient<B>,
    head: H,
}

const DATA_OBJECT_VERIFY_CHUNK_BYTES: usize = 64 * 1024;
const DATA_PACK_HEADER_LEN: u64 = 64;

struct VerifiedMetadata {
    semantic_hash: [u8; 32],
    extent_slice_refs: Vec<(u64, u64, u64)>,
    slice_count: u64,
}

impl<B, H> SnapshotPublisher<B, H>
where
    B: ObjectBackend + Clone,
    H: WorkspaceHeadStore,
{
    pub fn new(client: ObjectClient<B>, head: H) -> Self {
        Self { client, head }
    }

    pub fn head_store(&self) -> &H {
        &self.head
    }

    /// Verify all objects selected by `manifest_ref`, then CAS the workspace
    /// head.  No head write is attempted if any referenced object is missing,
    /// corrupt, or semantically inconsistent with its descriptor.
    pub async fn publish(
        &self,
        manifest_ref: &ManifestObjectRef,
        expected_head: Option<WorkspaceHead>,
    ) -> WireResult<PublishedSnapshot> {
        let manifest_bytes = self
            .fetch_verified(manifest_ref, "snapshot manifest")
            .await?;
        let manifest = SnapshotManifest::decode(&manifest_bytes)?;
        for descriptor in &manifest.clusters {
            self.verify_cluster(descriptor, manifest.superblock.volume_id)
                .await?;
        }

        let snapshot = SnapshotRef::from_manifest(&manifest_bytes)?;
        let current = self.head.load().await?;
        if current != expected_head {
            return Err(WireError::invalid(
                "workspace head",
                "expected head is stale before publication",
            ));
        }
        let epoch = expected_head
            .map(|head| {
                head.epoch
                    .checked_add(1)
                    .ok_or_else(|| WireError::LimitExceeded("workspace head epoch overflow".into()))
            })
            .transpose()?
            .unwrap_or(1);
        let desired = WorkspaceHead { epoch, snapshot };
        if !self.head.compare_and_swap(expected_head, desired).await? {
            return Err(WireError::invalid(
                "workspace head",
                "compare-and-swap lost a concurrent publication",
            ));
        }
        Ok(PublishedSnapshot {
            snapshot,
            head: desired,
        })
    }

    async fn verify_cluster(
        &self,
        descriptor: &ClusterDescriptor,
        volume_id: [u8; 16],
    ) -> WireResult<()> {
        let metadata = self
            .fetch_verified(&descriptor.metadata_ref, "cluster metadata")
            .await?;
        let metadata = verify_metadata(&metadata, descriptor, volume_id)?;

        let seal_bytes = self
            .fetch_verified(&descriptor.data_seal_ref, "data seal")
            .await?;
        let seal = DataSealSnapshot::open(seal_bytes)?;
        if seal.cluster_id() != descriptor.cluster_id || seal.volume_id() != volume_id {
            return Err(WireError::invalid(
                "data seal",
                "cluster or volume id does not match the manifest",
            ));
        }
        verify_extent_slice_refs(
            &metadata.extent_slice_refs,
            seal.slices(),
            metadata.slice_count,
        )?;
        for object in seal.objects() {
            let object_frames = seal
                .frames()
                .iter()
                .filter(|frame| frame.object_ordinal == object.object_ordinal)
                .collect::<Vec<_>>();
            self.verify_data_object(object, &object_frames).await?;
        }
        let mut combined = blake3::Hasher::new();
        combined.update(b"BrewFS.SealedCluster.v2");
        combined.update(&metadata.semantic_hash);
        combined.update(&seal.semantic_hash());
        let computed = *combined.finalize().as_bytes();
        if descriptor.combined_semantic_hash != computed {
            return Err(WireError::HashMismatch {
                what: "sealed cluster semantic hash",
                stored: hex::encode(descriptor.combined_semantic_hash),
                computed: hex::encode(computed),
            });
        }
        Ok(())
    }

    async fn verify_data_object(
        &self,
        object: &DataObjectDescriptor,
        frames: &[&FrameDescriptor],
    ) -> WireResult<()> {
        let key = std::str::from_utf8(&object.object_key)
            .map_err(|_| WireError::invalid("data object", "object key is not UTF-8"))?;
        let pack = RemoteDataPack::open(&self.client, key, object.object_len).await?;
        let footer_offset = object
            .object_len
            .checked_sub(FOOTER_LEN as u64)
            .ok_or_else(|| {
                WireError::invalid("data object", "object is shorter than its footer")
            })?;

        let mut ordered_frames = frames.to_vec();
        ordered_frames.sort_unstable_by_key(|frame| frame.object_offset);
        let mut expected_offset = DATA_PACK_HEADER_LEN;
        for frame in &ordered_frames {
            if frame.stored_len > MAX_ENCODED_FRAME_PAYLOAD
                || frame.object_offset != expected_offset
            {
                return Err(WireError::invalid(
                    "data object",
                    "frame descriptors do not form a bounded contiguous pack",
                ));
            }
            expected_offset = frame
                .object_offset
                .checked_add(
                    (FRAME_HEADER_LEN as u64)
                        .checked_add(u64::from(frame.stored_len))
                        .ok_or_else(|| {
                            WireError::LimitExceeded("data frame length overflows u64".into())
                        })?
                        .div_ceil(ALIGNMENT)
                        .checked_mul(ALIGNMENT)
                        .ok_or_else(|| {
                            WireError::LimitExceeded("data frame span overflows u64".into())
                        })?,
                )
                .ok_or_else(|| WireError::LimitExceeded("data frame end overflows u64".into()))?;
            if expected_offset > footer_offset {
                return Err(WireError::invalid(
                    "data object",
                    "frame extends into the DataPack footer",
                ));
            }
        }
        if expected_offset != footer_offset {
            return Err(WireError::invalid(
                "data object",
                "frame descriptors do not cover the complete DataPack frame region",
            ));
        }

        let mut object_hash = Sha256::new();
        let mut frame_index = 0usize;
        let mut current_frame = Vec::new();
        let mut offset = 0u64;
        while offset < object.object_len {
            let request_len = usize::try_from(
                (object.object_len - offset).min(DATA_OBJECT_VERIFY_CHUNK_BYTES as u64),
            )
            .expect("bounded data-object read length");
            let mut chunk = vec![0u8; request_len];
            let read = self
                .client
                .get_object_range(key, offset, &mut chunk)
                .await
                .map_err(|error| WireError::invalid("data object", error.to_string()))?;
            if read == 0 || read > request_len {
                return Err(WireError::Truncated {
                    what: "data object",
                    need: request_len,
                    have: read.min(request_len),
                });
            }
            chunk.truncate(read);
            object_hash.update(&chunk);
            let chunk_end = offset
                .checked_add(read as u64)
                .ok_or_else(|| WireError::LimitExceeded("data object offset overflows".into()))?;

            while let Some(frame) = ordered_frames.get(frame_index) {
                let frame_data_end = frame
                    .object_offset
                    .checked_add(FRAME_HEADER_LEN as u64)
                    .and_then(|end| end.checked_add(u64::from(frame.stored_len)))
                    .ok_or_else(|| {
                        WireError::LimitExceeded("data frame payload end overflows".into())
                    })?;
                if chunk_end <= frame.object_offset {
                    break;
                }
                let overlap_start = offset.max(frame.object_offset);
                let overlap_end = chunk_end.min(frame_data_end);
                if overlap_start >= overlap_end {
                    break;
                }
                if current_frame.is_empty() {
                    if overlap_start != frame.object_offset {
                        return Err(WireError::invalid(
                            "data object",
                            "remote frame stream starts after its authenticated offset",
                        ));
                    }
                    current_frame =
                        Vec::with_capacity(FRAME_HEADER_LEN + frame.stored_len as usize);
                }
                let expected_captured = usize::try_from(overlap_start - frame.object_offset)
                    .map_err(|_| {
                        WireError::LimitExceeded("data frame offset exceeds usize".into())
                    })?;
                if current_frame.len() != expected_captured {
                    return Err(WireError::invalid(
                        "data object",
                        "remote frame bytes contain a gap or overlap",
                    ));
                }
                let local_start = usize::try_from(overlap_start - offset).map_err(|_| {
                    WireError::LimitExceeded("data range offset exceeds usize".into())
                })?;
                let local_end = usize::try_from(overlap_end - offset)
                    .map_err(|_| WireError::LimitExceeded("data range end exceeds usize".into()))?;
                current_frame.extend_from_slice(&chunk[local_start..local_end]);
                let expected_frame_bytes = FRAME_HEADER_LEN + frame.stored_len as usize;
                if current_frame.len() == expected_frame_bytes {
                    verify_frame_bytes(&current_frame, frame)?;
                    current_frame.clear();
                    frame_index += 1;
                    continue;
                }
                break;
            }
            offset = chunk_end;
        }
        if frame_index != ordered_frames.len() || !current_frame.is_empty() {
            return Err(WireError::Truncated {
                what: "data object frames",
                need: ordered_frames.len(),
                have: frame_index,
            });
        }

        let digest: [u8; 32] = object_hash.finalize().into();
        if digest != object.object_checksum {
            return Err(WireError::HashMismatch {
                what: "data object SHA-256",
                stored: hex::encode(object.object_checksum),
                computed: hex::encode(digest),
            });
        }
        let mut trailing = [0u8; 1];
        let trailing_len = self
            .client
            .get_object_range(key, object.object_len, &mut trailing)
            .await
            .map_err(|error| WireError::invalid("data object", error.to_string()))?;
        if trailing_len != 0 {
            return Err(WireError::invalid(
                "data object",
                "remote object is longer than its sealed length",
            ));
        }
        let mut footer_bytes = [0u8; FOOTER_LEN];
        self.read_object_range_exact(key, footer_offset, &mut footer_bytes)
            .await?;
        let footer = ContainerFooter::parse(&footer_bytes)?;
        if footer.object_len != object.object_len || footer.root_stored_digest != [0; 32] {
            return Err(WireError::invalid(
                "data object footer",
                "footer length or DataPack root digest is invalid",
            ));
        }
        if pack.object_len() != object.object_len {
            return Err(WireError::invalid(
                "data object",
                "DataPack header length does not match its seal descriptor",
            ));
        }
        Ok(())
    }

    async fn read_object_range_exact(
        &self,
        key: &str,
        offset: u64,
        bytes: &mut [u8],
    ) -> WireResult<()> {
        let mut filled = 0usize;
        while filled < bytes.len() {
            let range_offset = offset
                .checked_add(filled as u64)
                .ok_or_else(|| WireError::LimitExceeded("object range offset overflows".into()))?;
            let read = self
                .client
                .get_object_range(key, range_offset, &mut bytes[filled..])
                .await
                .map_err(|error| WireError::invalid("data object range", error.to_string()))?;
            if read == 0 || read > bytes.len() - filled {
                return Err(WireError::Truncated {
                    what: "data object range",
                    need: bytes.len(),
                    have: filled + read.min(bytes.len() - filled),
                });
            }
            filled += read;
        }
        Ok(())
    }

    async fn fetch_verified(
        &self,
        object: &ManifestObjectRef,
        what: &'static str,
    ) -> WireResult<Vec<u8>> {
        let key = std::str::from_utf8(&object.key)
            .map_err(|_| WireError::invalid("publication object", "object key is not UTF-8"))?;
        let bytes = self
            .client
            .get_object(key)
            .await
            .map_err(|error| WireError::invalid("publication object", format!("{what}: {error}")))?
            .ok_or_else(|| {
                WireError::invalid("publication object", format!("{what} is missing"))
            })?;
        if bytes.len() as u64 != object.object_len {
            return Err(WireError::invalid(
                "publication object",
                format!("{what} length does not match its manifest reference"),
            ));
        }
        let digest = blake3::hash(&bytes);
        if object.full_hash != *digest.as_bytes() {
            return Err(WireError::HashMismatch {
                what,
                stored: hex::encode(object.full_hash),
                computed: hex::encode(digest.as_bytes()),
            });
        }
        Ok(bytes)
    }
}

fn verify_metadata(
    bytes: &[u8],
    descriptor: &ClusterDescriptor,
    volume_id: [u8; 16],
) -> WireResult<VerifiedMetadata> {
    let superblock = ClusterSuperblock::decode(bytes)?;
    if superblock.cluster_id != descriptor.cluster_id
        || superblock.volume_id != volume_id
        || superblock.mount_dir_key != descriptor.mount_dir_key
    {
        return Err(WireError::invalid(
            "cluster metadata",
            "superblock identity does not match the manifest descriptor",
        ));
    }
    if descriptor.root_local_node_id != 1 {
        return Err(WireError::invalid(
            "cluster metadata",
            "manifest root_local_node_id must be one",
        ));
    }

    let reader = MemoryRangeReader::new(bytes.to_vec());
    let mut batches_by_kind = Vec::new();
    let mut extent_slice_refs = Vec::new();
    for (index, (kind, root, expected_count)) in [
        (
            BatchKind::Namespace,
            &superblock.index_roots[0],
            superblock.namespace_batch_count,
        ),
        (
            BatchKind::Extent,
            &superblock.index_roots[1],
            superblock.extent_batch_count,
        ),
        (
            BatchKind::Attribute,
            &superblock.index_roots[2],
            superblock.attribute_batch_count,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        if expected_count == 0 {
            if root.stored_len != 0 {
                return Err(WireError::invalid(
                    "cluster metadata",
                    "empty index count has a non-empty root",
                ));
            }
            batches_by_kind.push(Vec::new());
            continue;
        }
        if root.stored_len == 0 || root.kind != kind as u8 {
            return Err(WireError::invalid(
                "cluster metadata",
                "index root kind or presence does not match the superblock",
            ));
        }
        let batches = collect_batches(&reader, root, kind, bytes.len() as u64)?;
        if batches.len() != expected_count as usize {
            return Err(WireError::invalid(
                "cluster metadata",
                format!("index {index} batch count does not match the superblock"),
            ));
        }
        for batch in &batches {
            if batch.header.cluster_id != descriptor.cluster_id {
                return Err(WireError::invalid(
                    "cluster metadata",
                    "batch cluster id does not match the superblock",
                ));
            }
            match kind {
                BatchKind::Namespace => {
                    NamespaceBatch::decode(batch)?;
                }
                BatchKind::Extent => {
                    let decoded = ExtentBatch::decode(batch)?;
                    for segment in decoded.segments {
                        for extent in segment.extents {
                            extent_slice_refs.push((
                                extent.slice_id,
                                extent.slice_offset,
                                extent.logical_length,
                            ));
                        }
                    }
                }
                BatchKind::Attribute => {
                    AttributeBatch::decode(batch)?;
                }
                BatchKind::DirectoryProjection => {
                    return Err(WireError::UnsupportedFormat(
                        "directory projection batches are not publishable yet".into(),
                    ));
                }
            }
        }
        batches_by_kind.push(batches);
    }

    // The current extended builder includes the batch kind in the semantic
    // stream.  Accept the earlier namespace-only encoding for immutable
    // clusters produced before extent/attribute assembly landed.
    let mut semantic = blake3::Hasher::new();
    semantic.update(b"BrewFS.BRFCL002.semantic.v2");
    semantic.update(descriptor.mount_dir_key.as_ref());
    let mut legacy = blake3::Hasher::new();
    legacy.update(b"BrewFS.BRFCL002.semantic.v2");
    legacy.update(descriptor.mount_dir_key.as_ref());
    for (kind, batches) in [
        (BatchKind::Namespace, &batches_by_kind[0]),
        (BatchKind::Extent, &batches_by_kind[1]),
        (BatchKind::Attribute, &batches_by_kind[2]),
    ] {
        for batch in batches {
            semantic.update(&(kind as u16).to_le_bytes());
            semantic.update(&batch.raw_payload);
            if kind == BatchKind::Namespace {
                legacy.update(&batch.raw_payload);
            }
        }
    }
    let semantic_hash = *semantic.finalize().as_bytes();
    let legacy_hash = *legacy.finalize().as_bytes();
    if superblock.metadata_semantic_hash != semantic_hash
        && superblock.metadata_semantic_hash != legacy_hash
    {
        return Err(WireError::HashMismatch {
            what: "cluster metadata semantic hash",
            stored: hex::encode(superblock.metadata_semantic_hash),
            computed: hex::encode(semantic_hash),
        });
    }
    let mut referenced_slice_ids = extent_slice_refs
        .iter()
        .map(|(slice_id, _, _)| *slice_id)
        .collect::<Vec<_>>();
    referenced_slice_ids.sort_unstable();
    referenced_slice_ids.dedup();
    if superblock.extent_count != extent_slice_refs.len() as u64
        || superblock.slice_count != referenced_slice_ids.len() as u64
    {
        return Err(WireError::invalid(
            "cluster metadata",
            "extent or slice count does not match decoded batches",
        ));
    }
    Ok(VerifiedMetadata {
        semantic_hash: superblock.metadata_semantic_hash,
        extent_slice_refs,
        slice_count: superblock.slice_count,
    })
}

fn verify_extent_slice_refs(
    extent_refs: &[(u64, u64, u64)],
    slices: &[SliceDescriptor],
    expected_slice_count: u64,
) -> WireResult<()> {
    if expected_slice_count != slices.len() as u64 {
        return Err(WireError::invalid(
            "cluster data seal",
            "slice count does not match cluster metadata",
        ));
    }
    let mut referenced = Vec::with_capacity(extent_refs.len());
    for &(slice_id, slice_offset, logical_length) in extent_refs {
        let slice = slices
            .binary_search_by_key(&slice_id, |slice| slice.slice_id)
            .ok()
            .map(|index| &slices[index])
            .ok_or_else(|| {
                WireError::invalid(
                    "cluster data seal",
                    format!("metadata references missing SliceId {slice_id}"),
                )
            })?;
        let end = slice_offset
            .checked_add(logical_length)
            .ok_or_else(|| WireError::LimitExceeded("metadata slice range overflows u64".into()))?;
        if logical_length == 0 || end > slice.logical_len {
            return Err(WireError::invalid(
                "cluster data seal",
                format!("metadata range exceeds SliceId {slice_id}"),
            ));
        }
        referenced.push(slice_id);
    }
    referenced.sort_unstable();
    referenced.dedup();
    if referenced.len() != slices.len()
        || referenced
            .iter()
            .zip(slices)
            .any(|(slice_id, slice)| *slice_id != slice.slice_id)
    {
        return Err(WireError::invalid(
            "cluster data seal",
            "metadata and Data Seal SliceId sets do not match",
        ));
    }
    Ok(())
}

fn verify_frame_bytes(bytes: &[u8], descriptor: &FrameDescriptor) -> WireResult<()> {
    let header = FrameHeader::parse(bytes)?;
    if header.ordinal != u64::from(descriptor.frame_ordinal)
        || header.stored_len != descriptor.stored_len
        || header.raw_len != descriptor.raw_len
        || header.payload_format.as_u8() != descriptor.payload_format
        || header.codec.as_u8() != descriptor.codec
    {
        return Err(WireError::invalid(
            "data object frame",
            "frame header disagrees with its Data Seal descriptor",
        ));
    }
    let stored = &bytes[FRAME_HEADER_LEN..];
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    if digest[..16] != descriptor.frame_checksum {
        return Err(WireError::HashMismatch {
            what: "data object frame",
            stored: hex::encode(descriptor.frame_checksum),
            computed: hex::encode(&digest[..16]),
        });
    }
    header.decode_payload(stored)?;
    Ok(())
}

fn collect_batches(
    reader: &MemoryRangeReader,
    root: &IndexRootRef,
    kind: BatchKind,
    object_len: u64,
) -> WireResult<Vec<EncodedBatch>> {
    const MAX_INDEX_NODES: usize = 1_000_000;
    let root_node = read_index_root(reader, root)?;
    let mut pending = vec![(root_node, root.level, root.object_offset, root.stored_len)];
    let mut visited = HashSet::new();
    let mut batches = Vec::new();
    while let Some((node, level, offset, stored_len)) = pending.pop() {
        if !visited.insert((offset, stored_len)) {
            return Err(WireError::invalid(
                "cluster metadata",
                "index tree contains a duplicate or cycle",
            ));
        }
        if visited.len() > MAX_INDEX_NODES {
            return Err(WireError::LimitExceeded(
                "metadata index contains too many nodes".into(),
            ));
        }
        if node.level != level || node.kind != kind {
            return Err(WireError::invalid(
                "cluster metadata",
                "index node level or kind is inconsistent",
            ));
        }
        if level == 0 {
            for entry in node.entries {
                let IndexEntry::Leaf { locator, .. } = entry else {
                    return Err(WireError::invalid(
                        "cluster metadata",
                        "leaf index contains an internal entry",
                    ));
                };
                batches.push(read_batch(reader, &locator)?);
            }
            continue;
        }
        let next_level = level
            .checked_sub(1)
            .ok_or_else(|| WireError::invalid("cluster metadata", "index level underflow"))?;
        let mut children = Vec::with_capacity(node.entries.len());
        for entry in node.entries {
            let IndexEntry::Internal(child) = entry else {
                return Err(WireError::invalid(
                    "cluster metadata",
                    "internal index contains a leaf entry",
                ));
            };
            let end = child
                .object_offset
                .checked_add(u64::from(child.stored_len))
                .ok_or_else(|| WireError::LimitExceeded("index child range overflows".into()))?;
            if end > object_len {
                return Err(WireError::Truncated {
                    what: "cluster metadata index child",
                    need: child.stored_len as usize,
                    have: object_len.saturating_sub(child.object_offset) as usize,
                });
            }
            let child_node = read_index_child(reader, &child, kind, next_level)?;
            children.push((
                child_node,
                next_level,
                child.object_offset,
                child.stored_len,
            ));
        }
        // The stack is LIFO; reverse the children to preserve the authenticated
        // left-to-right batch order used by the semantic hash.
        pending.extend(children.into_iter().rev());
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_base::ingest::ConsistencyPolicy;
    use crate::native_base::wire::datapack::PackFrame;
    use crate::workspace_overlay::clustered_snapshot::cluster_builder::build_single_directory_cluster_with_metadata;
    use crate::workspace_overlay::clustered_snapshot::cluster_format::{
        IndexRootRef, SnapshotSuperblock,
    };
    use crate::workspace_overlay::clustered_snapshot::data_pack::{
        DataPackBuilder, DataPackSnapshot,
    };
    use crate::workspace_overlay::clustered_snapshot::data_seal::{
        DataObjectDescriptor, DataSealBuilder, DataSpan, SliceDescriptor,
        frame_descriptors_from_pack,
    };
    use crate::workspace_overlay::clustered_snapshot::directory::{DirectoryIdentity, NodeRef};
    use crate::workspace_overlay::clustered_snapshot::extent::{
        ExtentBatch, ExtentRecord, ExtentSegment,
    };
    use crate::workspace_overlay::clustered_snapshot::identity::DirKey;
    use crate::workspace_overlay::clustered_snapshot::merge::{
        DirectoryContribution, build_directory_plan,
    };
    use crate::workspace_overlay::clustered_snapshot::merge_route::MergeRouteIndex;
    use crate::workspace_overlay::clustered_snapshot::mount_trie::{MountTrie, MountTrieNode};
    use crate::workspace_overlay::clustered_snapshot::name::NameBytes;
    use crate::workspace_overlay::clustered_snapshot::snapshot_manifest::{
        ClusterDescriptor, ManifestIndexPayload, SnapshotManifest,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::collections::BTreeMap;
    use std::sync::Mutex as StdMutex;

    #[derive(Clone, Default)]
    struct MemoryObjects {
        objects: Arc<StdMutex<BTreeMap<String, Vec<u8>>>>,
    }

    #[async_trait]
    impl ObjectBackend for MemoryObjects {
        async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_owned(), data.to_vec());
            Ok(())
        }

        async fn put_object_vectored(&self, key: &str, chunks: Vec<Bytes>) -> anyhow::Result<()> {
            let data = chunks
                .into_iter()
                .flat_map(|chunk| chunk.to_vec())
                .collect::<Vec<_>>();
            self.put_object(key, &data).await
        }

        async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(self.objects.lock().unwrap().get(key).cloned())
        }

        async fn get_object_range(
            &self,
            key: &str,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<usize> {
            let Some(bytes) = self.objects.lock().unwrap().get(key).cloned() else {
                return Ok(0);
            };
            let start = usize::try_from(offset).unwrap_or(usize::MAX);
            if start >= bytes.len() {
                return Ok(0);
            }
            let count = buf.len().min(bytes.len() - start);
            buf[..count].copy_from_slice(&bytes[start..start + count]);
            Ok(count)
        }

        async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .map(|bytes| hex::encode(blake3::hash(bytes).as_bytes()))
                .unwrap_or_default())
        }

        async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
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

    fn fixture() -> (MemoryObjects, ManifestObjectRef) {
        let volume_id = [7; 16];
        let cluster_id = [8; 16];
        let dir_key = DirKey::new([9; 16]);
        let plan = build_directory_plan(
            DirectoryIdentity::from_dir_key([10; 32], dir_key),
            vec![DirectoryContribution {
                dir_key,
                node: NodeRef {
                    cluster_slot: 0,
                    local_node_id: 1,
                },
                attributes_digest: [11; 32],
                entries: vec![
                    crate::workspace_overlay::clustered_snapshot::merge::ContributionEntry {
                        name: NameBytes::new(b"file".to_vec()).unwrap(),
                        inode: 2,
                        kind: 1,
                        child_dir_key: None,
                        attributes_digest: [0; 32],
                    },
                ],
            }],
        )
        .unwrap();
        let mut random_state = 0x7f4a_7c15u32;
        let file_data = (0..128 * 1024)
            .map(|_| {
                random_state ^= random_state << 13;
                random_state ^= random_state >> 17;
                random_state ^= random_state << 5;
                random_state as u8
            })
            .collect::<Vec<_>>();
        let mut pack_builder = DataPackBuilder::new();
        pack_builder.push(PackFrame::plain_bytes_zstd(&file_data, 3).unwrap());
        let data_pack = pack_builder.build().unwrap();
        assert!(data_pack.len() > DATA_OBJECT_VERIFY_CHUNK_BYTES);
        let pack_snapshot = DataPackSnapshot::open(data_pack.clone()).unwrap();
        let data_key = b"data/cluster.brfdp";
        let extent_batch = ExtentBatch {
            cluster_id,
            batch_id: 0,
            stream_ordinal: 0,
            predecessor_ordinal:
                crate::workspace_overlay::clustered_snapshot::batch::BatchHeader::NO_PREDECESSOR,
            segments: vec![ExtentSegment {
                local_node_id: 2,
                first_file_offset: 0,
                extents: vec![ExtentRecord {
                    gap_from_previous_end: 0,
                    logical_length: file_data.len() as u64,
                    slice_id: 1,
                    slice_offset: 0,
                }],
            }],
        };
        let cluster = build_single_directory_cluster_with_metadata(
            cluster_id,
            volume_id,
            dir_key,
            &plan,
            1,
            dir_key,
            &[extent_batch],
            &[],
        )
        .unwrap();
        let mut seal_builder = DataSealBuilder::new(cluster_id, volume_id);
        seal_builder
            .add_object(DataObjectDescriptor {
                object_ordinal: 0,
                object_key: data_key.to_vec(),
                object_len: data_pack.len() as u64,
                object_checksum: Sha256::digest(&data_pack).into(),
                etag: Vec::new(),
            })
            .unwrap();
        for frame in frame_descriptors_from_pack(0, &pack_snapshot).unwrap() {
            seal_builder.add_frame(frame).unwrap();
        }
        seal_builder
            .add_slice(SliceDescriptor {
                slice_id: 1,
                logical_len: file_data.len() as u64,
                spans: vec![DataSpan {
                    frame_ordinal: 0,
                    raw_offset_in_frame: 0,
                    raw_len: file_data.len() as u32,
                }],
            })
            .unwrap();
        let seal_bytes = seal_builder.build().unwrap();
        let seal = DataSealSnapshot::open(seal_bytes.clone()).unwrap();
        let metadata_hash = cluster.superblock.metadata_semantic_hash;
        let mut combined = blake3::Hasher::new();
        combined.update(b"BrewFS.SealedCluster.v2");
        combined.update(&metadata_hash);
        combined.update(&seal.semantic_hash());
        let descriptor = ClusterDescriptor {
            cluster_id,
            metadata_ref: ManifestObjectRef {
                object_id: [1; 16],
                kind: 1,
                object_len: cluster.bytes.len() as u64,
                full_hash: *blake3::hash(&cluster.bytes).as_bytes(),
                key: b"meta/cluster.brfc".to_vec(),
            },
            data_seal_ref: ManifestObjectRef {
                object_id: [2; 16],
                kind: 2,
                object_len: seal_bytes.len() as u64,
                full_hash: *blake3::hash(&seal_bytes).as_bytes(),
                key: b"seal/cluster.brfds".to_vec(),
            },
            combined_semantic_hash: *combined.finalize().as_bytes(),
            mount_dir_key: dir_key,
            root_local_node_id: 1,
            flags: 0,
        };
        let mount = MountTrie {
            nodes: vec![MountTrieNode {
                node_id: 1,
                parent_id: 0,
                component: None,
                cluster_slots: vec![0],
                children: Vec::new(),
            }],
        }
        .encode()
        .unwrap();
        let route = MergeRouteIndex::encode(&[]).unwrap();
        let manifest = SnapshotManifest {
            superblock: SnapshotSuperblock {
                volume_id,
                snapshot_id: [12; 16],
                semantic_hash: [13; 32],
                route_seed: [14; 16],
                cluster_count: 1,
                mount_count: 1,
                merged_directory_count: 0,
                root_dir_key: dir_key,
                index_roots: [empty_root(), empty_root(), empty_root()],
            },
            clusters: vec![descriptor],
            mount_index: ManifestIndexPayload {
                entry_count: 1,
                bytes: mount,
            },
            route_index: ManifestIndexPayload {
                entry_count: 0,
                bytes: route,
            },
        };
        let manifest_bytes = manifest.encode().unwrap();
        let objects = MemoryObjects::default();
        let mut map = objects.objects.lock().unwrap();
        map.insert("meta/cluster.brfc".into(), cluster.bytes);
        map.insert("seal/cluster.brfds".into(), seal_bytes);
        map.insert(String::from_utf8(data_key.to_vec()).unwrap(), data_pack);
        map.insert("manifest.brfsm".into(), manifest_bytes.clone());
        drop(map);
        (
            objects,
            ManifestObjectRef {
                object_id: [3; 16],
                kind: 3,
                object_len: manifest_bytes.len() as u64,
                full_hash: *blake3::hash(&manifest_bytes).as_bytes(),
                key: b"manifest.brfsm".to_vec(),
            },
        )
    }

    #[tokio::test]
    async fn publication_verifies_graph_before_head_cas() {
        let (backend, manifest_ref) = fixture();
        let head = MemoryWorkspaceHeadStore::default();
        let publisher = SnapshotPublisher::new(ObjectClient::new(backend), head.clone());
        let published = publisher.publish(&manifest_ref, None).await.unwrap();
        assert_eq!(published.head.epoch, 1);
        assert_eq!(head.get().await, Some(published.head));

        let stale = publisher.publish(&manifest_ref, None).await.unwrap_err();
        assert!(matches!(
            stale,
            WireError::Invalid {
                what: "workspace head",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn publication_rejects_tampered_metadata_before_head_change() {
        let (backend, manifest_ref) = fixture();
        backend
            .objects
            .lock()
            .unwrap()
            .get_mut("meta/cluster.brfc")
            .unwrap()[SUPERBLOCK_LEN] ^= 1;
        let head = MemoryWorkspaceHeadStore::default();
        let publisher = SnapshotPublisher::new(ObjectClient::new(backend), head.clone());
        assert!(publisher.publish(&manifest_ref, None).await.is_err());
        assert_eq!(head.get().await, None);
    }

    #[tokio::test]
    async fn publication_rejects_missing_or_corrupt_data_before_head_change() {
        let (backend, manifest_ref) = fixture();
        backend.objects.lock().unwrap().remove("data/cluster.brfdp");
        let head = MemoryWorkspaceHeadStore::default();
        let publisher = SnapshotPublisher::new(ObjectClient::new(backend), head.clone());
        assert!(publisher.publish(&manifest_ref, None).await.is_err());
        assert_eq!(head.get().await, None);

        let (backend, manifest_ref) = fixture();
        backend
            .objects
            .lock()
            .unwrap()
            .get_mut("data/cluster.brfdp")
            .unwrap()[64 + FRAME_HEADER_LEN + 5] ^= 1;
        let head = MemoryWorkspaceHeadStore::default();
        let publisher = SnapshotPublisher::new(ObjectClient::new(backend), head.clone());
        assert!(publisher.publish(&manifest_ref, None).await.is_err());
        assert_eq!(head.get().await, None);

        let (backend, manifest_ref) = fixture();
        let mut objects = backend.objects.lock().unwrap();
        let data = objects.get_mut("data/cluster.brfdp").unwrap();
        let footer_byte = data.len() - FOOTER_LEN + 16;
        data[footer_byte] ^= 1;
        drop(objects);
        let head = MemoryWorkspaceHeadStore::default();
        let publisher = SnapshotPublisher::new(ObjectClient::new(backend), head.clone());
        assert!(publisher.publish(&manifest_ref, None).await.is_err());
        assert_eq!(head.get().await, None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_source_builds_manifest_and_publishes_as_one_graph() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("sample"), vec![0x5a; 128 * 1024]).unwrap();
        let built = super::super::ingest::build_local_directory_cluster(
            root.path(),
            ConsistencyPolicy::SnapshotBacked,
            [0x31; 16],
            [0x32; 16],
        )
        .unwrap();
        let data = built.data.as_ref().unwrap().clone();
        let bundle = build_single_cluster_manifest(&built, [0x33; 16], [0x34; 16]).unwrap();
        let backend = MemoryObjects::default();
        let descriptor = &bundle.manifest.clusters[0];
        let metadata_key = String::from_utf8(descriptor.metadata_ref.key.clone()).unwrap();
        let seal_key = String::from_utf8(descriptor.data_seal_ref.key.clone()).unwrap();
        let manifest_key = String::from_utf8(bundle.manifest_ref.key.clone()).unwrap();
        let mut objects = backend.objects.lock().unwrap();
        objects.insert(metadata_key, built.cluster.bytes.clone());
        objects.insert(seal_key, data.data_seal);
        objects.insert(data.object_key, data.data_pack);
        objects.insert(manifest_key, bundle.bytes.clone());
        drop(objects);

        let head = MemoryWorkspaceHeadStore::default();
        let publisher = SnapshotPublisher::new(ObjectClient::new(backend), head.clone());
        let published = publisher.publish(&bundle.manifest_ref, None).await.unwrap();
        assert_eq!(published.head.epoch, 1);
        assert_eq!(published.snapshot.object_len, bundle.bytes.len() as u64);
    }

    #[test]
    fn metadata_extent_slice_set_must_match_seal() {
        let slice = SliceDescriptor {
            slice_id: 2,
            logical_len: 8,
            spans: vec![DataSpan {
                frame_ordinal: 0,
                raw_offset_in_frame: 0,
                raw_len: 8,
            }],
        };
        assert!(verify_extent_slice_refs(&[(1, 0, 8)], &[slice.clone()], 1).is_err());
        assert!(verify_extent_slice_refs(&[(2, 4, 8)], &[slice.clone()], 1).is_err());
        assert!(verify_extent_slice_refs(&[(2, 0, 8)], &[slice], 1).is_ok());
    }
}
