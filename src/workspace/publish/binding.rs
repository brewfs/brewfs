//! v3 packed binding foundation. This does not certify durable publication.

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::cadapter::client::ObjectBackend;
use crate::workspace::catalog::{HeadGuard, PackedLowerBinding};
use crate::workspace::error::WorkspaceError;
use crate::workspace::ids::{LayerId, WorkspaceId};
use crate::workspace::model::{BaseRevision, InodeDelta, InodeState, LayerRecord, LayerState};
use crate::workspace::packed::snapshot::{
    AuthenticatedSnapshot, SnapshotIndexReader, SnapshotObjectKind, SnapshotObjectRef,
    SnapshotRootKind,
};

const MAX_RECORD_BYTES: usize = 8192;

/// Opaque proof of the authenticated PM11/IP06 inode namespace boundary.
/// It proves neither uploaded dependency closure nor source-view consistency.
/// Only the authenticated snapshot and bounded index route can construct it.
#[derive(Clone, Debug)]
pub struct VerifiedPackedLower {
    manifest: SnapshotObjectRef,
    highest_inode: i64,
}

impl VerifiedPackedLower {
    /// A genuine staged full graph supplies only the authenticated namespace
    /// boundary. Native source/phase and the atomic publication CAS separately
    /// authorize installing this lower; raw highest-inode values cannot.
    #[cfg(target_os = "linux")]
    pub(crate) fn from_staged_graph(
        graph: &crate::workspace::packed::snapshot::SnapshotIndexContextAudit,
    ) -> Result<Self, WorkspaceError> {
        let (_, root, highest) = graph.publication_facts();
        if root != 1 || highest < root || highest >= i64::MAX as u64 - 1 {
            return Err(corrupt("invalid packed graph namespace boundary"));
        }
        Ok(Self {
            manifest: graph.manifest_reference().clone(),
            highest_inode: highest as i64,
        })
    }

    pub async fn from_authenticated_snapshot<B: ObjectBackend + Clone + 'static>(
        snapshot: &AuthenticatedSnapshot,
        reader: &SnapshotIndexReader<B>,
    ) -> Result<Self, WorkspaceError> {
        if snapshot.manifest().root_inode != 1 {
            return Err(WorkspaceError::UnsupportedCapability(
                "packed binding foundation requires root inode 1",
            ));
        }
        let last = reader
            .maximum_inode(snapshot.root(SnapshotRootKind::Inodes))
            .await
            .map_err(corrupt)?;
        let highest = last.unwrap_or(1).max(snapshot.manifest().root_inode);
        if highest >= i64::MAX as u64 - 1 {
            return Err(corrupt("packed namespace exhausts mutable inode allocator"));
        }
        Ok(Self {
            manifest: snapshot.manifest_reference().clone(),
            highest_inode: highest as i64,
        })
    }

    pub fn manifest_reference(&self) -> &SnapshotObjectRef {
        &self.manifest
    }

    pub fn highest_inode(&self) -> i64 {
        self.highest_inode
    }
}

/// Independent, explicitly encoded sidecar. Existing native bincode records
/// and BaseRevision retain their exact schema and native root-hash meaning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackedLowerBindingRecord {
    pub workspace_id: WorkspaceId,
    pub head_layer_id: LayerId,
    pub head_epoch: u64,
    pub base_revision: BaseRevision,
    pub highest_inode: i64,
    pub binding: PackedLowerBinding,
}

#[derive(Clone, Debug)]
pub struct InstallPackedLowerBinding {
    pub guard: HeadGuard,
    pub expected_layers: [LayerRecord; 2],
    pub expected_base: BaseRevision,
    pub expected_binding: Option<PackedLowerBindingRecord>,
    pub lower: VerifiedPackedLower,
}

/// Same-head replacement of an already-installed packed lower binding.
///
/// Publication advances the independent PWB3 history version and the
/// workspace head epoch in one backend transaction.  It intentionally keeps
/// the current writable head and sealed base fixed; seal/head-layer rotation
/// remains the responsibility of the native seal journal.
#[derive(Clone, Debug)]
pub struct PublishPackedLowerBinding {
    pub guard: HeadGuard,
    pub expected_layers: [LayerRecord; 2],
    pub expected_base: BaseRevision,
    pub expected_binding: PackedLowerBindingRecord,
    pub lower: VerifiedPackedLower,
}

impl PublishPackedLowerBinding {
    /// Reserve an expanded lower namespace before mutable IDs have been issued
    /// from that range. The authenticated maximum alone cannot prove that a
    /// replacement avoids individual IDs already handed to native writers.
    pub(crate) fn validate_first_publication_allocator(
        &self,
        next_inode: i64,
    ) -> Result<(), WorkspaceError> {
        let previous_floor = self
            .expected_binding
            .highest_inode
            .checked_add(1)
            .ok_or_else(|| corrupt("packed inode floor overflow"))?
            .max(2);
        if next_inode < previous_floor {
            return Err(corrupt("inode allocator is below packed namespace floor"));
        }
        if self.lower.highest_inode > self.expected_binding.highest_inode
            && next_inode != previous_floor
        {
            return Err(WorkspaceError::UnsupportedCapability(
                "packed lower expansion overlaps issued native inode IDs",
            ));
        }
        Ok(())
    }

    /// Recognize only the exact state produced by this publication. A retry
    /// must still validate the returned target guard against a live lease;
    /// this proof does not authorize any other use of the request's old epoch.
    pub(crate) fn validate_committed_state(
        &self,
        record: &PackedLowerBindingRecord,
        layers: &[LayerRecord; 2],
        base_revision: &BaseRevision,
        next_inode: i64,
    ) -> Result<HeadGuard, WorkspaceError> {
        let mut committed_layers = self.expected_layers.clone();
        committed_layers[0].next_sequence = committed_layers[0]
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| corrupt("packed publication sequence overflows"))?;
        let floor = record
            .highest_inode
            .checked_add(1)
            .ok_or_else(|| corrupt("packed inode floor overflow"))?
            .max(2);
        if *record != self.record()?
            || layers != &committed_layers
            || base_revision != &self.expected_base
            || next_inode < floor
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(HeadGuard {
            expected_head_epoch: record.head_epoch,
            ..self.guard.clone()
        })
    }

    pub(crate) fn record(&self) -> Result<PackedLowerBindingRecord, WorkspaceError> {
        crate::workspace::resolver::validate_layer_chain(
            self.guard.expected_head_layer_id,
            &self.expected_layers,
        )?;
        let [head, base] = &self.expected_layers;
        if head.layer_id != self.guard.expected_head_layer_id
            || head.state != LayerState::Writable
            || head.owner_workspace_id != Some(self.guard.workspace_id)
            || head.parent_layer_id != Some(base.layer_id)
            || self.expected_base.layer_id != base.layer_id
            || Some(self.expected_base.sealed_version) != base.sealed_version
            || Some(self.expected_base.root_hash) != base.root_hash
        {
            return Err(WorkspaceError::Fenced);
        }
        self.expected_binding
            .validate_for_guard(&self.guard, base)?;
        if self.expected_binding.binding.manifest == *self.lower.manifest_reference() {
            return Err(WorkspaceError::UnsupportedCapability(
                "packed publication must advance the manifest",
            ));
        }
        let binding_version = self
            .expected_binding
            .binding
            .binding_version
            .checked_add(1)
            .ok_or_else(|| corrupt("packed binding version overflows"))?;
        let head_epoch = self
            .guard
            .expected_head_epoch
            .checked_add(1)
            .ok_or_else(|| corrupt("packed binding head epoch overflows"))?;
        let record = PackedLowerBindingRecord {
            workspace_id: self.guard.workspace_id,
            head_layer_id: head.layer_id,
            head_epoch,
            base_revision: self.expected_base.clone(),
            highest_inode: self
                .lower
                .highest_inode
                .max(self.expected_binding.highest_inode),
            binding: PackedLowerBinding {
                binding_version,
                base_layer_id: base.layer_id,
                manifest: self.lower.manifest.clone(),
            },
        };
        record.encode()?;
        Ok(record)
    }
}

impl InstallPackedLowerBinding {
    pub(crate) fn validate_native_root(&self, root: &InodeDelta) -> Result<(), WorkspaceError> {
        use crate::workspace::digest::{CanonicalLayerDelta, delta_digest, root_hash};
        if root.layer_id != self.expected_base.layer_id
            || root.ino != 1
            || root.state != InodeState::Present
            || root.kind != 1
            || root.sequence != 1
            || root.parent_hint != Some(1)
            || root.symlink_target.is_some()
            || self.expected_layers[1].owned_slice_count != 0
            || self.expected_layers[1].owned_bytes != 0
        {
            return Err(corrupt("initial native base root identity changed"));
        }
        let digest = delta_digest(&CanonicalLayerDelta {
            inodes: vec![root.clone()],
            ..Default::default()
        })?;
        if self.expected_layers[1].delta_digest != Some(digest)
            || self.expected_base.root_hash != root_hash([0; 32], digest)
        {
            return Err(WorkspaceError::UnsupportedCapability(
                "first packed binding requires root-only native base",
            ));
        }
        Ok(())
    }

    pub(crate) fn record(&self) -> Result<PackedLowerBindingRecord, WorkspaceError> {
        crate::workspace::resolver::validate_layer_chain(
            self.guard.expected_head_layer_id,
            &self.expected_layers,
        )?;
        let [head, base] = &self.expected_layers;
        if head.layer_id != self.guard.expected_head_layer_id
            || head.state != LayerState::Writable
            || head.owner_workspace_id != Some(self.guard.workspace_id)
            || head.depth != 2
            || base.depth != 1
            || base.state != LayerState::Sealed
            || base.parent_layer_id.is_some()
            || head.parent_layer_id != Some(base.layer_id)
            || self.expected_base.layer_id != base.layer_id
            || Some(self.expected_base.sealed_version) != base.sealed_version
            || Some(self.expected_base.root_hash) != base.root_hash
        {
            return Err(WorkspaceError::Fenced);
        }
        if self.expected_binding.is_some() {
            return Err(WorkspaceError::UnsupportedCapability(
                "packed lower replacement requires publication lifecycle",
            ));
        }
        if head.next_sequence != 1 || base.sealed_version != Some(1) || base.next_sequence != 2 {
            return Err(WorkspaceError::UnsupportedCapability(
                "first packed binding requires initial empty workspace",
            ));
        }
        let record = PackedLowerBindingRecord {
            workspace_id: self.guard.workspace_id,
            head_layer_id: head.layer_id,
            head_epoch: self
                .guard
                .expected_head_epoch
                .checked_add(1)
                .ok_or_else(|| corrupt("packed binding head epoch overflows"))?,
            base_revision: self.expected_base.clone(),
            highest_inode: self.lower.highest_inode,
            binding: PackedLowerBinding {
                binding_version: 1,
                base_layer_id: base.layer_id,
                manifest: self.lower.manifest.clone(),
            },
        };
        record.encode()?;
        Ok(record)
    }
}

impl PackedLowerBindingRecord {
    pub fn validate_for_guard(
        &self,
        guard: &HeadGuard,
        base: &LayerRecord,
    ) -> Result<(), WorkspaceError> {
        self.validate()?;
        if self.workspace_id != guard.workspace_id
            || self.head_layer_id != guard.expected_head_layer_id
            || self.head_epoch != guard.expected_head_epoch
            || self.base_revision.layer_id != base.layer_id
            || base.state != LayerState::Sealed
            || Some(self.base_revision.sealed_version) != base.sealed_version
            || Some(self.base_revision.root_hash) != base.root_hash
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), WorkspaceError> {
        if self.binding.binding_version == 0
            || self.head_epoch == 0
            || self.highest_inode < 1
            || self.highest_inode >= i64::MAX - 1
            || self.binding.base_layer_id != self.base_revision.layer_id
            || self.base_revision.sealed_version == 0
            || self.binding.manifest.kind != SnapshotObjectKind::Manifest
        {
            return Err(corrupt("invalid PWB3 binding identity"));
        }
        self.binding.manifest.encode_value().map_err(corrupt)?;
        Ok(())
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        self.validate()?;
        let reference = self.binding.manifest.encode_value().map_err(corrupt)?;
        if reference.len() > MAX_RECORD_BYTES - 192 {
            return Err(corrupt("PWB3 reference exceeds record bound"));
        }
        let mut bytes = Vec::with_capacity(192 + reference.len());
        bytes.extend_from_slice(b"PWB3");
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(self.workspace_id.as_bytes());
        bytes.extend_from_slice(self.head_layer_id.as_bytes());
        bytes.extend_from_slice(self.base_revision.layer_id.as_bytes());
        for value in [
            self.head_epoch,
            self.binding.binding_version,
            self.base_revision.sealed_version,
            self.highest_inode as u64,
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.extend_from_slice(&self.base_revision.root_hash);
        bytes.extend_from_slice(&(reference.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&reference);
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, WorkspaceError> {
        if bytes.len() < 156 || bytes.len() > MAX_RECORD_BYTES {
            return Err(corrupt("PWB3 record length exceeds bound"));
        }
        let (body, digest) = bytes.split_at(bytes.len() - 32);
        let computed: [u8; 32] = Sha256::digest(body).into();
        if computed.as_slice() != digest {
            return Err(corrupt("PWB3 record digest mismatch"));
        }
        let mut cursor = Cursor {
            bytes: body,
            position: 0,
        };
        if cursor.take::<4>()? != *b"PWB3" || u32::from_le_bytes(cursor.take()?) != 1 {
            return Err(WorkspaceError::UnsupportedVolumeFormat(
                "PWB3 codec version".into(),
            ));
        }
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_bytes(cursor.take()?));
        let head_layer_id = LayerId::from_uuid(Uuid::from_bytes(cursor.take()?));
        let base_layer_id = LayerId::from_uuid(Uuid::from_bytes(cursor.take()?));
        let head_epoch = u64::from_le_bytes(cursor.take()?);
        let binding_version = u64::from_le_bytes(cursor.take()?);
        let sealed_version = u64::from_le_bytes(cursor.take()?);
        let highest_inode = i64::try_from(u64::from_le_bytes(cursor.take()?)).map_err(corrupt)?;
        let root_hash = cursor.take()?;
        let reference_len = u32::from_le_bytes(cursor.take()?) as usize;
        if body.len().checked_sub(cursor.position) != Some(reference_len) {
            return Err(corrupt("PWB3 reference length/trailing fields disagree"));
        }
        let manifest =
            SnapshotObjectRef::decode_value(&body[cursor.position..]).map_err(corrupt)?;
        let record = Self {
            workspace_id,
            head_layer_id,
            head_epoch,
            base_revision: BaseRevision {
                layer_id: base_layer_id,
                sealed_version,
                root_hash,
            },
            highest_inode,
            binding: PackedLowerBinding {
                binding_version,
                base_layer_id,
                manifest,
            },
        };
        record.validate()?;
        Ok(record)
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl Cursor<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], WorkspaceError> {
        let end = self
            .position
            .checked_add(N)
            .ok_or_else(|| corrupt("PWB3 cursor overflow"))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| corrupt("truncated PWB3 record"))?;
        self.position = end;
        bytes.try_into().map_err(corrupt)
    }
}

fn corrupt(error: impl std::fmt::Display) -> WorkspaceError {
    WorkspaceError::CorruptMetadata(format!("packed binding: {error}"))
}
