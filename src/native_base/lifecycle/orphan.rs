//! Open-unlinked (nameless) inode carry across seal and fork (ORD-009/ORD-010).
//!
//! An inode whose last name is removed while a handle is still open keeps its
//! data in the private write domain.  The carry names those inodes so a seal
//! can move the head without losing them, while the fork-visible baseline and
//! any immediate fork must not see them at all: they have no name and no
//! published extent.  A write through the surviving handle stays in the same
//! private head and is never copied up into the visible base.
//!
//! Recovery after a lost publication reply is keyed on the carry's
//! OperationId and may only perform the single head switch that the recorded
//! operation describes; a later fast-forward is refused instead of guessed.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::native_base::frozen::decode_canonical_attributes;
use crate::native_base::wire::bnct::{Id16, NativeWorkspaceHead, WorkspaceHeadState};
use crate::native_base::wire::refs::{Hash32, ObjectId};
use crate::native_base::wire::uvarint::{Reader, Writer};
use crate::native_base::write::keys::Keys;
use crate::native_base::write::records::HeadState;
use crate::native_base::write::store::{ControlStore, Txn};

use super::seal::{decode_workspace_view, next_head_ref};
use super::{LifecycleError, LifecycleResult, map_conflict};

pub const ORPHAN_CARRY_MAGIC: &[u8] = b"BrewFS.OrphanCarry.v1\0";
pub const MAX_ORPHAN_INODES: usize = 4096;
pub const MAX_ORPHAN_EXTENTS: usize = 1 << 20;
pub const MAX_ORPHAN_ATTRIBUTES: usize = 64 * 1024;

/// One extent of a nameless inode.  The carry records the placement the
/// private head already owns plus the payload hash, so a read through the
/// surviving handle can be checked without a copy-up.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct OrphanExtent {
    pub chunk_index: u64,
    pub offset: u64,
    pub length: u64,
    pub digest: Hash32,
}

impl OrphanExtent {
    /// A placement covering `payload`, hashed by the caller-side content.
    pub fn covering(chunk_index: u64, offset: u64, payload: &[u8]) -> LifecycleResult<Self> {
        if payload.is_empty() {
            return Err(LifecycleError::InvalidState(
                "an orphan extent cannot be empty".into(),
            ));
        }
        Ok(Self {
            chunk_index,
            offset,
            length: payload.len() as u64,
            digest: Sha256::digest(payload).into(),
        })
    }
}

/// An inode whose last name was removed while a handle stayed open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrphanInode {
    /// Private inode number of the write domain, never a published inode.
    pub private_inode: u64,
    /// Canonical `FrozenInodeRecord` bytes, byte-exact and never re-encoded.
    pub attributes: Vec<u8>,
    pub extents: Vec<OrphanExtent>,
}

impl OrphanInode {
    pub fn new(private_inode: u64, attributes: Vec<u8>) -> LifecycleResult<Self> {
        let inode = Self {
            private_inode,
            attributes,
            extents: Vec::new(),
        };
        inode.validate()?;
        Ok(inode)
    }

    pub fn with_extent(mut self, extent: OrphanExtent) -> LifecycleResult<Self> {
        self.extents.push(extent);
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> LifecycleResult<()> {
        if self.private_inode == 0 {
            return Err(LifecycleError::InvalidState(
                "orphan private inode must be non-zero".into(),
            ));
        }
        if self.attributes.is_empty() || self.attributes.len() > MAX_ORPHAN_ATTRIBUTES {
            return Err(LifecycleError::LimitExceeded(
                "orphan attributes are empty or too large".into(),
            ));
        }
        // The attributes must be exactly canonical: a carry that reproduced
        // them lossily would change the inode identity it claims to preserve.
        decode_canonical_attributes(&self.attributes)?;
        if self.extents.len() > MAX_ORPHAN_EXTENTS {
            return Err(LifecycleError::LimitExceeded(
                "orphan extent count exceeds the limit".into(),
            ));
        }
        for pair in self.extents.windows(2) {
            if (pair[0].chunk_index, pair[0].offset) >= (pair[1].chunk_index, pair[1].offset) {
                return Err(LifecycleError::InvalidState(
                    "orphan extents must strictly ascend by (chunk, offset)".into(),
                ));
            }
        }
        for extent in &self.extents {
            if extent.length == 0 {
                return Err(LifecycleError::InvalidState(
                    "orphan extent length must be non-zero".into(),
                ));
            }
        }
        Ok(())
    }
}

/// The nameless inodes a seal moves onto the new private head.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrphanCarry {
    pub workspace_id: Id16,
    pub domain_id: Id16,
    pub operation_id: Id16,
    pub inodes: Vec<OrphanInode>,
}

impl OrphanCarry {
    pub fn encode(&self) -> LifecycleResult<Vec<u8>> {
        self.validate()?;
        let mut w = Writer::new();
        w.put(ORPHAN_CARRY_MAGIC);
        w.put(&self.workspace_id);
        w.put(&self.domain_id);
        w.put(&self.operation_id);
        w.u32(self.inodes.len() as u32);
        for inode in &self.inodes {
            w.u64(inode.private_inode);
            w.bytes(&inode.attributes);
            w.u32(inode.extents.len() as u32);
            for extent in &inode.extents {
                w.u64(extent.chunk_index);
                w.u64(extent.offset);
                w.u64(extent.length);
                w.put(&extent.digest);
            }
        }
        Ok(w.into_bytes())
    }

    pub fn decode(bytes: &[u8]) -> LifecycleResult<Self> {
        let what = "orphan carry";
        let mut r = Reader::new(bytes);
        if r.take(ORPHAN_CARRY_MAGIC.len(), what)? != ORPHAN_CARRY_MAGIC {
            return Err(LifecycleError::Record("orphan carry magic mismatch".into()));
        }
        let workspace_id: Id16 = r.take(16, what)?.try_into().unwrap();
        let domain_id: Id16 = r.take(16, what)?.try_into().unwrap();
        let operation_id: Id16 = r.take(16, what)?.try_into().unwrap();
        let inode_count = r.u32(what)? as usize;
        if inode_count > MAX_ORPHAN_INODES {
            return Err(LifecycleError::LimitExceeded(
                "orphan inode count exceeds the limit".into(),
            ));
        }
        let mut inodes = Vec::with_capacity(inode_count);
        for _ in 0..inode_count {
            let private_inode = r.u64(what)?;
            let attributes = r.bytes(what)?.to_vec();
            let extent_count = r.u32(what)? as usize;
            if extent_count > MAX_ORPHAN_EXTENTS {
                return Err(LifecycleError::LimitExceeded(
                    "orphan extent count exceeds the limit".into(),
                ));
            }
            let mut extents = Vec::with_capacity(extent_count);
            for _ in 0..extent_count {
                extents.push(OrphanExtent {
                    chunk_index: r.u64(what)?,
                    offset: r.u64(what)?,
                    length: r.u64(what)?,
                    digest: r.take(32, what)?.try_into().unwrap(),
                });
            }
            inodes.push(OrphanInode {
                private_inode,
                attributes,
                extents,
            });
        }
        if !r.is_empty() {
            return Err(LifecycleError::Record(
                "orphan carry has trailing bytes".into(),
            ));
        }
        let carry = Self {
            workspace_id,
            domain_id,
            operation_id,
            inodes,
        };
        carry.validate()?;
        Ok(carry)
    }

    /// Digest of the canonical encoding; this is what the workspace head
    /// records, so a head can never claim a carry it does not hold.
    pub fn digest(&self) -> LifecycleResult<Hash32> {
        Ok(Sha256::digest(self.encode()?).into())
    }

    pub fn validate(&self) -> LifecycleResult<()> {
        if self.workspace_id == [0u8; 16] || self.domain_id == [0u8; 16] {
            return Err(LifecycleError::InvalidState(
                "orphan carry workspace and domain must be non-zero".into(),
            ));
        }
        if self.operation_id == [0u8; 16] {
            return Err(LifecycleError::InvalidState(
                "orphan carry OperationId must be non-zero".into(),
            ));
        }
        if self.inodes.is_empty() {
            return Err(LifecycleError::InvalidState(
                "orphan carry is empty; a seal without nameless inodes writes none".into(),
            ));
        }
        if self.inodes.len() > MAX_ORPHAN_INODES {
            return Err(LifecycleError::LimitExceeded(
                "orphan inode count exceeds the limit".into(),
            ));
        }
        for pair in self.inodes.windows(2) {
            if pair[0].private_inode >= pair[1].private_inode {
                return Err(LifecycleError::InvalidState(
                    "orphan inodes must strictly ascend by private inode".into(),
                ));
            }
        }
        for inode in &self.inodes {
            inode.validate()?;
        }
        Ok(())
    }
}

/// One closed nameless inode: its placements are private garbage now.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrphanClose {
    pub private_inode: u64,
    pub released_extents: Vec<OrphanExtent>,
    pub open_orphan_count: u64,
}

/// The nameless-inode side of one write domain.
#[derive(Debug)]
pub struct OrphanLedger {
    workspace_id: Id16,
    domain_id: Id16,
    open: BTreeMap<u64, OrphanInode>,
    closed: BTreeSet<u64>,
}

impl OrphanLedger {
    pub fn new(workspace_id: Id16, domain_id: Id16) -> Self {
        Self {
            workspace_id,
            domain_id,
            open: BTreeMap::new(),
            closed: BTreeSet::new(),
        }
    }

    pub fn workspace_id(&self) -> Id16 {
        self.workspace_id
    }

    pub fn domain_id(&self) -> Id16 {
        self.domain_id
    }

    pub fn open_orphan_count(&self) -> u64 {
        self.open.len() as u64
    }

    pub fn is_open(&self, private_inode: u64) -> bool {
        self.open.contains_key(&private_inode)
    }

    /// Register an open handle whose last name was just removed.  A private
    /// inode number that is already open, or was already closed, is refused:
    /// the number is not reusable while the carry exists.
    pub fn open_unlinked(&mut self, inode: OrphanInode) -> LifecycleResult<()> {
        inode.validate()?;
        if self.closed.contains(&inode.private_inode) {
            return Err(LifecycleError::InvalidState(format!(
                "private inode {} was already closed",
                inode.private_inode
            )));
        }
        if self.open.insert(inode.private_inode, inode).is_some() {
            return Err(LifecycleError::InvalidState(
                "private inode is already carried".into(),
            ));
        }
        Ok(())
    }

    pub fn inode(&self, private_inode: u64) -> LifecycleResult<&OrphanInode> {
        self.open.get(&private_inode).ok_or_else(|| {
            LifecycleError::InvalidState(format!("private inode {private_inode} is not open"))
        })
    }

    /// Read through the surviving handle: the payload must match a carried
    /// placement and its hash, so content is preserved without a copy-up.
    pub fn verify_read(
        &self,
        private_inode: u64,
        chunk_index: u64,
        offset: u64,
        payload: &[u8],
    ) -> LifecycleResult<()> {
        let inode = self.inode(private_inode)?;
        let found = inode.extents.iter().find(|extent| {
            extent.chunk_index == chunk_index
                && extent.offset == offset
                && extent.length == payload.len() as u64
        });
        let Some(extent) = found else {
            return Err(LifecycleError::InvalidState(format!(
                "no carried placement of private inode {private_inode} matches this read"
            )));
        };
        let computed: Hash32 = Sha256::digest(payload).into();
        if extent.digest != computed {
            return Err(LifecycleError::InvalidState(
                "orphan read content does not match the carried placement".into(),
            ));
        }
        Ok(())
    }

    /// A write through the surviving handle appends a placement inside the
    /// same private head.  The inode stays nameless: nothing is copied up and
    /// no visible delta is produced.
    pub fn append_write(
        &mut self,
        private_inode: u64,
        extent: OrphanExtent,
    ) -> LifecycleResult<()> {
        let inode = self.open.get_mut(&private_inode).ok_or_else(|| {
            LifecycleError::InvalidState(format!("private inode {private_inode} is not open"))
        })?;
        let end = extent.offset.saturating_add(extent.length);
        if inode.extents.iter().any(|existing| {
            existing.chunk_index == extent.chunk_index
                && existing.offset < end
                && existing.offset + existing.length > extent.offset
        }) {
            return Err(LifecycleError::Conflict(
                "orphan write overlaps a carried placement".into(),
            ));
        }
        inode.extents.push(extent);
        inode
            .extents
            .sort_by_key(|extent| (extent.chunk_index, extent.offset));
        inode.validate()?;
        Ok(())
    }

    /// Close the last handle.  The inode leaves the carry and its placements
    /// become private garbage for the domain cleaner.
    pub fn close(&mut self, private_inode: u64) -> LifecycleResult<OrphanClose> {
        let inode = self.open.remove(&private_inode).ok_or_else(|| {
            LifecycleError::InvalidState(format!("private inode {private_inode} is not open"))
        })?;
        self.closed.insert(private_inode);
        Ok(OrphanClose {
            private_inode,
            released_extents: inode.extents,
            open_orphan_count: self.open_orphan_count(),
        })
    }

    /// The carry a seal moves onto the new private head.  Sealing with no open
    /// nameless inode writes no carry at all.
    pub fn carry(&self, operation_id: Id16) -> LifecycleResult<OrphanCarry> {
        let carry = OrphanCarry {
            workspace_id: self.workspace_id,
            domain_id: self.domain_id,
            operation_id,
            inodes: self.open.values().cloned().collect(),
        };
        carry.validate()?;
        Ok(carry)
    }

    /// A fork listing must not contain a nameless inode: it has no name and no
    /// published extent, so finding one there is a leak, not a warning.
    pub fn ensure_fork_invisible(
        &self,
        fork_visible: &BTreeMap<Vec<u8>, u64>,
    ) -> LifecycleResult<()> {
        for inode in self.open.values() {
            if fork_visible
                .values()
                .any(|visible| *visible == inode.private_inode)
            {
                return Err(LifecycleError::InvalidState(format!(
                    "nameless inode {} is visible in the fork listing",
                    inode.private_inode
                )));
            }
        }
        Ok(())
    }
}

/// Publish the carry onto a new private head.
///
/// The switch keeps the write domain, keeps the base, and records
/// `open_orphan_count`/`orphan_carry_digest`.  A seal never hides visible
/// deltas: a non-empty `visible_delta_count` must be published first.
pub fn seal_head_with_orphans(
    previous: &NativeWorkspaceHead,
    new_head_id: Id16,
    carry: &OrphanCarry,
) -> LifecycleResult<NativeWorkspaceHead> {
    carry.validate()?;
    if previous.state != WorkspaceHeadState::Running {
        return Err(LifecycleError::InvalidState(
            "only a running workspace head can be sealed".into(),
        ));
    }
    if previous.visible_delta_count != 0 {
        return Err(LifecycleError::Conflict(
            "seal cannot hide visible deltas; publish them first".into(),
        ));
    }
    if previous.workspace_id != carry.workspace_id || previous.write_domain_id != carry.domain_id {
        return Err(LifecycleError::Conflict(
            "orphan carry does not belong to this workspace write domain".into(),
        ));
    }
    if new_head_id == [0u8; 16] {
        return Err(LifecycleError::InvalidState(
            "sealed head id must be non-zero".into(),
        ));
    }
    let mut sealed = previous.clone();
    sealed.head = next_head_ref(&previous.head, new_head_id)?;
    sealed.open_orphan_count = carry.inodes.len() as u64;
    sealed.orphan_carry_digest = Some(carry.digest()?);
    sealed.entity_version = sealed
        .entity_version
        .checked_add(1)
        .ok_or_else(|| LifecycleError::LimitExceeded("workspace entity version overflow".into()))?;
    Ok(sealed)
}

/// A fork starts from the published base and therefore inherits no private
/// nameless inode: a fork view that still reports orphan state is refused.
pub fn ensure_fork_view_has_no_carry(fork_view: &NativeWorkspaceHead) -> LifecycleResult<()> {
    if fork_view.open_orphan_count != 0 || fork_view.orphan_carry_digest.is_some() {
        return Err(LifecycleError::InvalidState(
            "fork view carries private orphan state".into(),
        ));
    }
    Ok(())
}

/// The readable object set of a base that an immediate fork must be able to
/// read completely: every Loose object plus every Packed object of the sealed
/// revision, exactly once each, manifest included.
pub fn plan_fork_readable_base(
    manifest: &ObjectId,
    loose: &BTreeSet<ObjectId>,
    packed: &BTreeSet<ObjectId>,
) -> LifecycleResult<BTreeSet<ObjectId>> {
    if let Some(shared) = loose.intersection(packed).next() {
        return Err(LifecycleError::InvalidState(format!(
            "object {} is claimed as both Loose and Packed",
            hex::encode(shared)
        )));
    }
    if !loose.contains(manifest) && !packed.contains(manifest) {
        return Err(LifecycleError::InvalidState(
            "sealed base manifest object is missing".into(),
        ));
    }
    Ok(loose.union(packed).copied().collect())
}

#[derive(Debug, Clone)]
pub struct OrphanCarryRequest {
    pub carry: OrphanCarry,
    pub expected_head: HeadState,
    pub expected_view: NativeWorkspaceHead,
    pub new_head_id: Id16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanCarryOutcome {
    pub workspace_id: Id16,
    pub head: HeadState,
    pub view: NativeWorkspaceHead,
}

/// Finish a seal whose publication reply was lost.
///
/// Recovery is keyed on the carry's OperationId.  A replay must reproduce the
/// *single* head switch the recorded operation describes; if the workspace has
/// moved on (another head, base or orphan accounting) the replay is refused so
/// it can never fast-forward over the lost reply.
pub async fn resume_orphan_carry(
    store: &dyn ControlStore,
    keys: &Keys,
    request: &OrphanCarryRequest,
) -> LifecycleResult<OrphanCarryOutcome> {
    let carry = &request.carry;
    let carry_bytes = carry.encode()?;
    let expected_sealed =
        seal_head_with_orphans(&request.expected_view, request.new_head_id, carry)?;
    let head_key = keys.head(&carry.workspace_id);
    let view_key = keys.workspace_view(&carry.workspace_id);
    let operation_key = keys.orphan_carry(&carry.operation_id);

    if let Some(existing) = store.get(&operation_key).await? {
        if existing != carry_bytes {
            return Err(LifecycleError::OperationIdMismatch(
                "orphan carry OperationId was used with another payload".into(),
            ));
        }
        let head =
            HeadState::decode(&store.get(&head_key).await?.ok_or_else(|| {
                LifecycleError::Record("orphan carry result head missing".into())
            })?)?;
        let view =
            decode_workspace_view(&store.get(&view_key).await?.ok_or_else(|| {
                LifecycleError::Record("orphan carry result view missing".into())
            })?)?;
        let expected_head = HeadState {
            head: expected_sealed.head.clone(),
            writer_generation: request.expected_head.writer_generation,
            write_domain_id: request.expected_head.write_domain_id,
        };
        if head != expected_head || view != expected_sealed {
            return Err(LifecycleError::Conflict(
                "orphan carry replay would fast-forward over a different head switch".into(),
            ));
        }
        return Ok(OrphanCarryOutcome {
            workspace_id: carry.workspace_id,
            head,
            view,
        });
    }

    let head_bytes = store
        .get(&head_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("orphan carry target head missing".into()))?;
    let view_bytes = store
        .get(&view_key)
        .await?
        .ok_or_else(|| LifecycleError::InvalidState("orphan carry target view missing".into()))?;
    let current_head = HeadState::decode(&head_bytes)?;
    let current_view = decode_workspace_view(&view_bytes)?;
    if current_head != request.expected_head || current_view != request.expected_view {
        return Err(LifecycleError::Conflict(
            "orphan carry target head/view is stale".into(),
        ));
    }
    let current_digest = current_view.orphan_carry_digest;
    if let Some(recorded) = current_digest
        && recorded != carry.digest()?
    {
        return Err(LifecycleError::Conflict(
            "workspace already records a different orphan carry".into(),
        ));
    }
    let result = OrphanCarryOutcome {
        workspace_id: carry.workspace_id,
        head: HeadState {
            head: expected_sealed.head.clone(),
            writer_generation: current_head.writer_generation,
            write_domain_id: current_head.write_domain_id,
        },
        view: expected_sealed,
    };
    store
        .run(
            Txn::new()
                .check_absent(operation_key.clone())
                .check_bytes(head_key.clone(), head_bytes)
                .check_bytes(view_key.clone(), view_bytes)
                .put(head_key, result.head.encode())
                .put(
                    view_key,
                    crate::native_base::wire::bnct::ControlRecord::NativeWorkspaceHead(
                        result.view.clone(),
                    )
                    .encode(),
                )
                .put(operation_key, carry_bytes),
        )
        .await
        .map_err(|e| map_conflict(e, "orphan carry target changed during the head switch"))?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_base::frozen::FrozenInodeRecord;
    use crate::native_base::wire::bnct::{ControlRecord, HeadRef, SnapshotRef};
    use crate::native_base::wire::container::ObjectKind;
    use crate::native_base::wire::refs::ObjectRef;
    use crate::native_base::write::memory::MemoryControlStore;

    const WORKSPACE: Id16 = [1u8; 16];
    const DOMAIN: Id16 = [4u8; 16];
    const VOLUME: Id16 = [7u8; 16];

    fn object(id: u8, kind: u8, key: &str) -> ObjectRef {
        ObjectRef {
            object_id: [id; 16],
            kind,
            object_len: 4096,
            full_hash: [id; 32],
            key: key.as_bytes().to_vec(),
        }
    }

    fn snapshot(volume_id: Id16, id: u8) -> SnapshotRef {
        SnapshotRef {
            volume_id,
            logical_revision: [id; 32],
            manifest: object(id, ObjectKind::SnapshotManifest.as_u8(), "manifest/1.brfsm"),
        }
    }

    /// Canonical attributes of an inode whose last name was just removed.
    fn attributes(inode: u64, mode: u32) -> Vec<u8> {
        FrozenInodeRecord {
            kind: 1,
            mode,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            nlink: 0,
            size: 8192,
            atime_ns: inode as i64,
            mtime_ns: inode as i64,
            ctime_ns: inode as i64,
            parent_hint: None,
            symlink_target: None,
        }
        .encode()
    }

    fn running_view() -> NativeWorkspaceHead {
        NativeWorkspaceHead {
            workspace_id: WORKSPACE,
            head: HeadRef {
                head_id: [2u8; 16],
                epoch: 3,
                commit_seq: 7,
            },
            base: snapshot(VOLUME, 9),
            writer_generation: 5,
            write_domain_id: DOMAIN,
            visible_delta_count: 0,
            open_orphan_count: 0,
            orphan_carry_digest: None,
            state: WorkspaceHeadState::Running,
            entity_version: 11,
        }
    }

    fn head_state() -> HeadState {
        HeadState {
            head: HeadRef {
                head_id: [2u8; 16],
                epoch: 3,
                commit_seq: 7,
            },
            writer_generation: 5,
            write_domain_id: DOMAIN,
        }
    }

    async fn put(store: &dyn ControlStore, key: Vec<u8>, value: Vec<u8>) {
        store
            .run(Txn::new().check_absent(key.clone()).put(key, value))
            .await
            .unwrap();
    }

    async fn put_over(store: &dyn ControlStore, key: Vec<u8>, value: Vec<u8>) {
        store.run(Txn::new().put(key, value)).await.unwrap();
    }

    /// ORD-009 / WRITE-011 / INV-02 / INV-03 / INV-09 / INV-11: an inode whose
    /// last name is removed while a handle stays open survives the seal inside
    /// the same write domain, keeps its placement (no copy-up), is visible to
    /// neither the baseline nor a fork listing, and leaves the carry when the
    /// last handle closes.
    #[test]
    fn open_unlinked_inode_survives_seal_and_never_enters_the_visible_baseline() {
        let payload = b"orphan payload with bytes".to_vec();
        let inode = OrphanInode::new(41, attributes(41, 0o100_600))
            .unwrap()
            .with_extent(OrphanExtent::covering(0, 0, &payload).unwrap())
            .unwrap();
        let mut ledger = OrphanLedger::new(WORKSPACE, DOMAIN);
        ledger.open_unlinked(inode.clone()).unwrap();
        assert_eq!(ledger.open_orphan_count(), 1);
        assert!(ledger.is_open(41));
        assert!(
            ledger.open_unlinked(inode.clone()).is_err(),
            "double open refused"
        );

        let operation_id = [21u8; 16];
        let carry = ledger.carry(operation_id).unwrap();
        assert_eq!(carry.inodes, vec![inode.clone()]);
        let sealed = seal_head_with_orphans(&running_view(), [22u8; 16], &carry).unwrap();
        assert_eq!(sealed.write_domain_id, DOMAIN, "the write domain is kept");
        assert_eq!(sealed.base, running_view().base);
        assert_eq!(sealed.open_orphan_count, 1);
        assert_eq!(sealed.orphan_carry_digest, Some(carry.digest().unwrap()));
        assert_eq!(sealed.head.head_id, [22u8; 16]);
        assert_eq!(sealed.head.epoch, 4);
        assert_eq!(sealed.entity_version, 12);

        // The fork listing of the sealed base never contains the nameless
        // inode; a leaked private inode is refused, not tolerated.
        let mut fork_visible = BTreeMap::new();
        fork_visible.insert(b"kept".to_vec(), 7u64);
        ledger.ensure_fork_invisible(&fork_visible).unwrap();
        let mut leaked = fork_visible.clone();
        leaked.insert(b"leak".to_vec(), 41u64);
        assert!(ledger.ensure_fork_invisible(&leaked).is_err());

        // Reads keep the carried placement and content; writes append a new
        // placement in the same private head instead of copying up.
        ledger.verify_read(41, 0, 0, &payload).unwrap();
        assert!(ledger.verify_read(41, 0, 0, b"other").is_err());
        assert!(ledger.verify_read(41, 1, 0, &payload).is_err());
        let appended = OrphanExtent::covering(1, 0, b"extra").unwrap();
        ledger.append_write(41, appended).unwrap();
        assert_eq!(ledger.inode(41).unwrap().extents.len(), 2);
        assert!(
            ledger
                .append_write(41, OrphanExtent::covering(0, 0, b"overlap").unwrap())
                .is_err(),
            "an overlapping placement is refused"
        );
        assert!(
            ledger.append_write(999, appended).is_err(),
            "unknown handle"
        );
        assert_eq!(ledger.open_orphan_count(), 1, "no copy-up: still nameless");

        // The carry round-trips byte-exactly and re-validates.
        let encoded = carry.encode().unwrap();
        assert_eq!(OrphanCarry::decode(&encoded).unwrap(), carry);
        let mut corrupted = encoded.clone();
        corrupted[0] ^= 1;
        assert!(OrphanCarry::decode(&corrupted).is_err());

        // Closing the last handle releases the placements and ends the carry.
        let closed = ledger.close(41).unwrap();
        assert_eq!(closed.private_inode, 41);
        assert_eq!(closed.released_extents.len(), 2);
        assert_eq!(closed.open_orphan_count, 0);
        assert!(ledger.carry(operation_id).is_err(), "no empty carry");
        assert!(
            ledger.open_unlinked(inode).is_err(),
            "a closed private inode number is not reused"
        );
    }

    /// IDX-006 / LIFE-006 / INV-02 / INV-06 / INV-11: an immediate fork after a
    /// seal reads every Loose and Packed object of the sealed base completely,
    /// claims each object exactly once, and never inherits the private carry.
    #[test]
    fn a_fork_after_seal_reads_loose_and_packed_and_never_sees_the_carry() {
        let payload = b"packed frame".to_vec();
        let inode = OrphanInode::new(51, attributes(51, 0o100_644))
            .unwrap()
            .with_extent(OrphanExtent::covering(0, 0, &payload).unwrap())
            .unwrap();
        let mut ledger = OrphanLedger::new(WORKSPACE, DOMAIN);
        ledger.open_unlinked(inode).unwrap();
        let carry = ledger.carry([31u8; 16]).unwrap();
        let sealed = seal_head_with_orphans(&running_view(), [32u8; 16], &carry).unwrap();
        assert_eq!(sealed.open_orphan_count, 1);

        // The fork view starts from the published base: no orphan accounting.
        let mut fork_view = sealed.clone();
        fork_view.head = HeadRef {
            head_id: [33u8; 16],
            epoch: 1,
            commit_seq: 0,
        };
        fork_view.write_domain_id = [34u8; 16];
        fork_view.base = snapshot(VOLUME, 35);
        fork_view.open_orphan_count = 0;
        fork_view.orphan_carry_digest = None;
        fork_view.entity_version = 1;
        ensure_fork_view_has_no_carry(&fork_view).unwrap();
        let mut leaked_fork = fork_view.clone();
        leaked_fork.orphan_carry_digest = sealed.orphan_carry_digest;
        leaked_fork.open_orphan_count = 1;
        assert!(ensure_fork_view_has_no_carry(&leaked_fork).is_err());

        // Both object classes of the sealed base are readable immediately.
        let loose: BTreeSet<ObjectId> = [[41u8; 16], [42u8; 16]].into_iter().collect();
        let packed: BTreeSet<ObjectId> = [[43u8; 16], [44u8; 16]].into_iter().collect();
        let manifest = sealed.base.manifest.object_id;
        assert!(
            plan_fork_readable_base(&manifest, &loose, &packed).is_err(),
            "the manifest object must be part of the sealed base"
        );
        let mut with_manifest = loose.clone();
        with_manifest.insert(manifest);
        let readable = plan_fork_readable_base(&manifest, &with_manifest, &packed).unwrap();
        assert_eq!(readable.len(), 5);
        for object in with_manifest.iter().chain(packed.iter()) {
            assert!(readable.contains(object), "every object stays readable");
        }
        let mut overlapping = packed.clone();
        overlapping.insert([41u8; 16]);
        assert!(
            plan_fork_readable_base(&manifest, &with_manifest, &overlapping).is_err(),
            "an object cannot be loose and packed at once"
        );
    }

    /// ORD-010 / INV-08 / INV-16 / INV-18: after a lost publication reply the
    /// same OperationId recovers exactly one head switch.  A replay that would
    /// land on another head, another orphan accounting or another payload is
    /// refused instead of fast-forwarding, and visible deltas block the seal.
    #[tokio::test]
    async fn a_lost_seal_reply_is_recovered_by_operation_id_without_wrong_fast_forward() {
        let payload = b"carried".to_vec();
        let inode = OrphanInode::new(61, attributes(61, 0o100_600))
            .unwrap()
            .with_extent(OrphanExtent::covering(0, 0, &payload).unwrap())
            .unwrap();
        let mut ledger = OrphanLedger::new(WORKSPACE, DOMAIN);
        ledger.open_unlinked(inode).unwrap();
        let carry = ledger.carry([41u8; 16]).unwrap();
        let view = running_view();
        let head = head_state();
        let keys = Keys::new(&VOLUME);
        let store = MemoryControlStore::new();
        put(&store, keys.head(&WORKSPACE), head.encode()).await;
        put(
            &store,
            keys.workspace_view(&WORKSPACE),
            ControlRecord::NativeWorkspaceHead(view.clone()).encode(),
        )
        .await;
        let request = OrphanCarryRequest {
            carry: carry.clone(),
            expected_head: head.clone(),
            expected_view: view.clone(),
            new_head_id: [42u8; 16],
        };

        let first = resume_orphan_carry(&store, &keys, &request).await.unwrap();
        assert_eq!(first.view.open_orphan_count, 1);
        assert_eq!(
            first.view.orphan_carry_digest,
            Some(carry.digest().unwrap())
        );
        assert_eq!(first.view.head.head_id, [42u8; 16]);
        assert_eq!(first.head.head.head_id, [42u8; 16]);
        assert_eq!(first.head.head.epoch, 4);

        // Replaying the same OperationId with the same payload is idempotent.
        let replay = resume_orphan_carry(&store, &keys, &request).await.unwrap();
        assert_eq!(replay, first);

        // The same OperationId may not be replayed onto another head switch.
        let mut other_head = request.clone();
        other_head.new_head_id = [43u8; 16];
        let error = resume_orphan_carry(&store, &keys, &other_head)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("fast-forward"), "{error}");

        // The same OperationId with a different payload is refused outright.
        let mut other_payload = request.clone();
        other_payload.carry.inodes[0].attributes = attributes(61, 0o100_640);
        let error = resume_orphan_carry(&store, &keys, &other_payload)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("another payload"), "{error}");

        // A stale pre-state (the head already moved) is refused.
        let mut fresh_operation = request.clone();
        fresh_operation.carry.operation_id = [44u8; 16];
        let error = resume_orphan_carry(&store, &keys, &fresh_operation)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("stale"), "{error}");

        // A seal never hides visible deltas: the switch must be refused before
        // anything is written, so a later `visible_delta_count` cannot be
        // fast-forwarded away.
        let mut dirty_view = request.clone();
        dirty_view.expected_view.visible_delta_count = 1;
        let error = resume_orphan_carry(&store, &keys, &dirty_view)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("visible deltas"), "{error}");

        // Reply lost, then the workspace moved on: the recorded operation can
        // no longer be reproduced, so the replay must not fast-forward over it.
        let store = MemoryControlStore::new();
        put(&store, keys.head(&WORKSPACE), head.encode()).await;
        put(
            &store,
            keys.workspace_view(&WORKSPACE),
            ControlRecord::NativeWorkspaceHead(view.clone()).encode(),
        )
        .await;
        put(
            &store,
            keys.orphan_carry(&carry.operation_id),
            carry.encode().unwrap(),
        )
        .await;
        let mut moved_on = view.clone();
        moved_on.head = HeadRef {
            head_id: [77u8; 16],
            epoch: 9,
            commit_seq: 0,
        };
        moved_on.entity_version = 40;
        moved_on.open_orphan_count = 3;
        moved_on.orphan_carry_digest = Some([78u8; 32]);
        put_over(
            &store,
            keys.head(&WORKSPACE),
            HeadState {
                head: moved_on.head.clone(),
                writer_generation: head.writer_generation,
                write_domain_id: head.write_domain_id,
            }
            .encode(),
        )
        .await;
        put_over(
            &store,
            keys.workspace_view(&WORKSPACE),
            ControlRecord::NativeWorkspaceHead(moved_on).encode(),
        )
        .await;
        let error = resume_orphan_carry(&store, &keys, &request)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("fast-forward"), "{error}");
    }
}
