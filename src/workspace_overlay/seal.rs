//! Offline workspace sealing into an immutable fork base.
//!
//! A seal is only valid while no writer lease is live. It materializes the
//! current two-layer view into one flat sealed layer, then atomically installs
//! that layer plus a fresh empty writable head. The durable journal allows a
//! later invocation to finish the exact same operation after a crash.

use std::collections::BTreeSet;
use std::sync::Arc;

use uuid::Uuid;

use super::catalog::{AbortWorkspaceSeal, BeginWorkspaceSeal, CommitWorkspaceSeal, WorkspaceStore};
use super::digest::CanonicalLayerDelta;
use super::error::WorkspaceError;
use super::ids::{JournalId, LayerId, WorkspaceId};
use super::model::{
    AclDelta, DataExtentDelta, DentryDelta, ExtentKind, InodeState, LayerRecord, LayerState,
    SealJournal, SealPhase, SealResult, ValueOp, WorkspaceState, XattrDelta,
};
use super::resolver::{resolve_acl, resolve_dentry, resolve_extents, resolve_inode, resolve_xattr};

pub struct WorkspaceSealer<W> {
    store: Arc<W>,
}

impl<W: WorkspaceStore + 'static> WorkspaceSealer<W> {
    pub fn new(store: Arc<W>) -> Self {
        Self { store }
    }

    /// Seal `workspace_id` if its writable head contains any changes.
    ///
    /// The operation is idempotent: a workspace whose head is already empty
    /// returns its current fork base without allocating a new sealed version.
    pub async fn seal(&self, workspace_id: WorkspaceId) -> Result<SealResult, WorkspaceError> {
        let workspace = self.store.load_workspace(workspace_id).await?;
        if workspace.active_lease.is_some() {
            return Err(WorkspaceError::Busy);
        }
        if workspace.state == WorkspaceState::Sealing {
            return self.resume(workspace_id).await;
        }
        if workspace.state != WorkspaceState::Active {
            return Err(WorkspaceError::InvalidStateTransition {
                from: format!("{:?}", workspace.state),
                to: "Sealing".into(),
            });
        }
        if self.latest_incomplete(workspace_id).await?.is_some() {
            return Err(WorkspaceError::CorruptMetadata(
                "active workspace has an incomplete seal journal".into(),
            ));
        }
        let head = self.store.load_layer(workspace.head_layer_id).await?;
        if head.state != LayerState::Writable
            || head.depth != 2
            || head.owner_workspace_id != Some(workspace_id)
        {
            return Err(WorkspaceError::CorruptMetadata(
                "seal requires one writable workspace head".into(),
            ));
        }
        if let Some(revision) = &workspace.fork_base
            && head.next_sequence == 1
            && head.parent_layer_id == Some(revision.layer_id)
        {
            self.store.load_layer_chain(workspace.head_layer_id).await?;
            return Ok(SealResult {
                revision: revision.clone(),
                new_head_layer_id: workspace.head_layer_id,
                head_epoch: workspace.head_epoch,
            });
        }
        let journal = self
            .store
            .begin_workspace_seal(BeginWorkspaceSeal {
                workspace_id,
                journal_id: JournalId::new(),
                new_head_layer_id: LayerId::new(),
            })
            .await?;
        self.finish(journal).await
    }

    async fn resume(&self, workspace_id: WorkspaceId) -> Result<SealResult, WorkspaceError> {
        let journal = self.latest_incomplete(workspace_id).await?.ok_or_else(|| {
            WorkspaceError::CorruptMetadata(
                "sealing workspace has no recoverable seal journal".into(),
            )
        })?;
        if journal.phase != SealPhase::Prepare {
            return Err(WorkspaceError::InvalidStateTransition {
                from: format!("{:?}", journal.phase),
                to: "Completed".into(),
            });
        }
        let workspace = self.store.load_workspace(workspace_id).await?;
        if workspace.head_layer_id != journal.old_head_layer_id
            || workspace.head_epoch != journal.expected_head_epoch
            || workspace.state != WorkspaceState::Sealing
            || workspace.active_lease.is_some()
        {
            return Err(WorkspaceError::Fenced);
        }
        self.finish(journal).await
    }

    async fn latest_incomplete(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<SealJournal>, WorkspaceError> {
        Ok(self
            .store
            .list_seal_journals(workspace_id)
            .await?
            .into_iter()
            .filter(|journal| !matches!(journal.phase, SealPhase::Completed | SealPhase::Aborted))
            .max_by_key(|journal| (journal.created_at_ns, journal.journal_id)))
    }

    async fn finish(&self, journal: SealJournal) -> Result<SealResult, WorkspaceError> {
        let chain = self
            .store
            .load_layer_chain(journal.old_head_layer_id)
            .await?;
        let sealed_layer_id = sealed_layer_id_for(journal.journal_id);
        let delta =
            materialize_resolved_chain(self.store.as_ref(), sealed_layer_id, &chain).await?;
        let result = self
            .store
            .commit_workspace_seal(CommitWorkspaceSeal {
                journal_id: journal.journal_id,
                sealed_layer_id,
                delta,
            })
            .await;
        match result {
            Ok(result) => Ok(result),
            Err(error) => {
                if let Some(completed) = self.completed_result(&journal).await? {
                    return Ok(completed);
                }
                let current = self.store.load_workspace(journal.workspace_id).await?;
                if current.state == WorkspaceState::Sealing {
                    let _ = self
                        .store
                        .abort_workspace_seal(AbortWorkspaceSeal {
                            journal_id: journal.journal_id,
                            reason: error.to_string(),
                        })
                        .await;
                }
                Err(error)
            }
        }
    }

    async fn completed_result(
        &self,
        journal: &SealJournal,
    ) -> Result<Option<SealResult>, WorkspaceError> {
        let current = self.store.load_workspace(journal.workspace_id).await?;
        if current.state != WorkspaceState::Active {
            return Ok(None);
        }
        let durable = self.store.load_seal_journal(journal.journal_id).await?;
        if durable.phase != SealPhase::Completed {
            return Ok(None);
        }
        let revision = current.fork_base.ok_or_else(|| {
            WorkspaceError::CorruptMetadata("completed seal has no fork base".into())
        })?;
        Ok(Some(SealResult {
            revision,
            new_head_layer_id: current.head_layer_id,
            head_epoch: current.head_epoch,
        }))
    }
}

/// Stable layer identity for the output of one journal, so a resumed seal
/// cannot allocate a second immutable layer for the same operation.
fn sealed_layer_id_for(journal_id: JournalId) -> LayerId {
    let digest = blake3::hash(journal_id.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    LayerId::from_uuid(Uuid::from_bytes(bytes))
}

pub(crate) async fn materialize_resolved_chain<W: WorkspaceStore + ?Sized>(
    store: &W,
    output_layer: LayerId,
    chain: &[LayerRecord],
) -> Result<CanonicalLayerDelta, WorkspaceError> {
    let [head, base] = chain else {
        return Err(WorkspaceError::CorruptMetadata(
            "seal requires exactly one writable head and one sealed base".into(),
        ));
    };
    if head.state != LayerState::Writable
        || head.parent_layer_id != Some(base.layer_id)
        || base.state != LayerState::Sealed
    {
        return Err(WorkspaceError::CorruptMetadata(
            "seal source violates the fixed two-layer invariant".into(),
        ));
    }

    let mut input = CanonicalLayerDelta::default();
    for layer in chain {
        let delta = store.load_layer_delta(layer.layer_id).await?;
        input.dentries.extend(delta.dentries);
        input.inodes.extend(delta.inodes);
        input.xattrs.extend(delta.xattrs);
        input.acls.extend(delta.acls);
        input.extents.extend(delta.extents);
    }

    let mut output = CanonicalLayerDelta::default();
    let dentry_keys = input
        .dentries
        .iter()
        .map(|row| (row.parent_ino, row.name.clone()))
        .collect::<BTreeSet<_>>();
    for (parent, name) in dentry_keys {
        if let Some(entry) = resolve_dentry(chain, &input.dentries, parent, &name)? {
            output.dentries.push(DentryDelta::put(
                output_layer,
                parent,
                name,
                entry.ino,
                entry.entry_type,
                0,
            ));
        }
    }
    let inode_keys = input
        .inodes
        .iter()
        .map(|row| row.ino)
        .collect::<BTreeSet<_>>();
    for ino in inode_keys {
        if let Some(mut inode) = resolve_inode(chain, &input.inodes, ino)?.map(|value| value.inode)
        {
            inode.layer_id = output_layer;
            inode.state = InodeState::Present;
            inode.sequence = 0;
            output.inodes.push(inode);
        }
    }
    let xattr_keys = input
        .xattrs
        .iter()
        .map(|row| (row.ino, row.name.clone()))
        .collect::<BTreeSet<_>>();
    for (ino, name) in xattr_keys {
        if let Some(value) = resolve_xattr(chain, &input.xattrs, ino, &name)? {
            output.xattrs.push(XattrDelta {
                layer_id: output_layer,
                ino,
                name,
                op: ValueOp::Put,
                value: Some(value.value),
                sequence: 0,
            });
        }
    }
    let acl_keys = input
        .acls
        .iter()
        .map(|row| (row.ino, row.acl_type, row.acl_id))
        .collect::<BTreeSet<_>>();
    for (ino, acl_type, acl_id) in acl_keys {
        if let Some(value) = resolve_acl(chain, &input.acls, ino, acl_type, acl_id)? {
            output.acls.push(AclDelta {
                layer_id: output_layer,
                ino,
                acl_type,
                acl_id,
                op: ValueOp::Put,
                value: Some(value.value),
                sequence: 0,
            });
        }
    }
    let extent_keys = input
        .extents
        .iter()
        .map(|row| (row.ino, row.chunk_index))
        .collect::<BTreeSet<_>>();
    for (ino, chunk_index) in extent_keys {
        let range_end = input
            .extents
            .iter()
            .filter(|row| row.ino == ino && row.chunk_index == chunk_index)
            .try_fold(0_u64, |end, row| {
                row.logical_offset
                    .checked_add(row.length)
                    .map(|row_end| end.max(row_end))
                    .ok_or_else(|| {
                        WorkspaceError::CorruptMetadata(
                            "extent range overflows during seal materialization".into(),
                        )
                    })
            })?;
        for extent in resolve_extents(chain, &input.extents, ino, chunk_index, 0..range_end)? {
            if let ExtentKind::Data {
                slice_id,
                slice_offset,
            } = extent.kind
            {
                output.extents.push(DataExtentDelta::data(
                    output_layer,
                    ino,
                    chunk_index,
                    extent.logical_offset,
                    extent.length,
                    slice_id,
                    slice_offset,
                    0,
                ));
            }
        }
    }

    let mut sequence = 1_u64;
    for row in &mut output.dentries {
        row.sequence = sequence;
        sequence = next_sequence(sequence)?;
    }
    for row in &mut output.inodes {
        row.sequence = sequence;
        sequence = next_sequence(sequence)?;
    }
    for row in &mut output.xattrs {
        row.sequence = sequence;
        sequence = next_sequence(sequence)?;
    }
    for row in &mut output.acls {
        row.sequence = sequence;
        sequence = next_sequence(sequence)?;
    }
    for row in &mut output.extents {
        row.sequence = sequence;
        sequence = next_sequence(sequence)?;
    }
    Ok(output)
}

fn next_sequence(sequence: u64) -> Result<u64, WorkspaceError> {
    sequence
        .checked_add(1)
        .ok_or_else(|| WorkspaceError::CorruptMetadata("seal sequence overflows".into()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use uuid::Uuid;

    use super::*;
    use crate::meta::layer::MetaLayer;
    use crate::workspace_overlay::catalog::{CreateVolumeRoot, CreateWorkspace};
    use crate::workspace_overlay::lifecycle::{
        DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_LEASE_TTL, WorkspaceMountSession,
    };
    use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
    use crate::workspace_overlay::model::{WORKSPACE_SCHEMA_VERSION, WorkspaceRecord};
    use crate::workspace_overlay::stores::database::SqliteWorkspaceStore;

    async fn root() -> (Arc<SqliteWorkspaceStore>, WorkspaceRecord) {
        let store = Arc::new(
            SqliteWorkspaceStore::connect("sqlite::memory:")
                .await
                .unwrap(),
        );
        store.initialize_workspace_schema().await.unwrap();
        let workspace = store
            .create_volume_root(CreateVolumeRoot {
                upper_bucket: None,
                volume_format: "workspace-v1".into(),
                schema_version: WORKSPACE_SCHEMA_VERSION,
                volume_id: Uuid::new_v4(),
                workspace_id: WorkspaceId::new(),
                root_layer_id: LayerId::new(),
                writable_layer_id: LayerId::new(),
                owner_id: None,
            })
            .await
            .unwrap();
        (store, workspace)
    }

    async fn create_root_file(store: &Arc<SqliteWorkspaceStore>, workspace: &WorkspaceRecord) {
        let session = WorkspaceMountSession::acquire(
            store.clone(),
            workspace.workspace_id,
            1,
            DEFAULT_LEASE_TTL,
            DEFAULT_HEARTBEAT_INTERVAL,
        )
        .await
        .unwrap();
        let meta = WorkspaceMetaLayer::new(store.clone(), session.view.clone());
        meta.create_file(meta.root_ino(), "sealed-file".to_owned())
            .await
            .unwrap();
        session.release().await.unwrap();
    }

    #[tokio::test]
    async fn seal_revision_is_accepted_by_fork() {
        let (store, root) = root().await;
        create_root_file(&store, &root).await;

        let sealed = WorkspaceSealer::new(store.clone())
            .seal(root.workspace_id)
            .await
            .unwrap();
        assert!(sealed.revision.sealed_version > 1);

        let forked = store
            .create_workspace(CreateWorkspace {
                workspace_id: WorkspaceId::new(),
                head_layer_id: LayerId::new(),
                base_revision: sealed.revision.clone(),
                owner_id: None,
                upper_bucket: None,
            })
            .await
            .unwrap();
        let session = WorkspaceMountSession::acquire(
            store.clone(),
            forked.workspace_id,
            2,
            DEFAULT_LEASE_TTL,
            DEFAULT_HEARTBEAT_INTERVAL,
        )
        .await
        .unwrap();
        let meta = WorkspaceMetaLayer::new(store.clone(), session.view.clone());
        assert!(
            meta.lookup(meta.root_ino(), "sealed-file")
                .await
                .unwrap()
                .is_some()
        );
        session.release().await.unwrap();
    }

    #[tokio::test]
    async fn repeated_seal_returns_the_same_revision_without_bumping_version() {
        let (store, root) = root().await;
        create_root_file(&store, &root).await;
        let sealer = WorkspaceSealer::new(store.clone());

        let first = sealer.seal(root.workspace_id).await.unwrap();
        let second = sealer.seal(root.workspace_id).await.unwrap();

        assert_eq!(first.revision, second.revision);
        let listed = store.load_workspace(root.workspace_id).await.unwrap();
        assert_eq!(second.new_head_layer_id, listed.head_layer_id);
        assert_eq!(listed.fork_base, Some(first.revision));
    }

    #[tokio::test]
    async fn seal_updates_workspace_listing_head_epoch_and_sealed_version() {
        let (store, root) = root().await;
        create_root_file(&store, &root).await;
        let before = store
            .list_workspaces()
            .await
            .unwrap()
            .into_iter()
            .find(|workspace| workspace.workspace_id == root.workspace_id)
            .unwrap();

        let sealed = WorkspaceSealer::new(store.clone())
            .seal(root.workspace_id)
            .await
            .unwrap();

        let after = store
            .list_workspaces()
            .await
            .unwrap()
            .into_iter()
            .find(|workspace| workspace.workspace_id == root.workspace_id)
            .unwrap();
        let before_revision = before.fork_base.unwrap();
        let after_revision = after.fork_base.unwrap();
        assert_eq!(before_revision.sealed_version, 1);
        assert_eq!(after_revision, sealed.revision);
        assert_eq!(
            after_revision.sealed_version,
            before_revision.sealed_version + 1
        );
        assert_eq!(after.head_epoch, before.head_epoch + 1);
        assert_eq!(after.head_epoch, sealed.head_epoch);
        assert_eq!(after.head_layer_id, sealed.new_head_layer_id);
        assert_ne!(after.head_layer_id, before.head_layer_id);
        assert_eq!(after.state, WorkspaceState::Active);
    }

    #[tokio::test]
    async fn seal_resumes_persisted_prepare_journal() {
        let (store, root) = root().await;
        create_root_file(&store, &root).await;
        let journal = store
            .begin_workspace_seal(BeginWorkspaceSeal {
                workspace_id: root.workspace_id,
                journal_id: JournalId::new(),
                new_head_layer_id: LayerId::new(),
            })
            .await
            .unwrap();

        let sealed = WorkspaceSealer::new(store.clone())
            .seal(root.workspace_id)
            .await
            .unwrap();

        assert_eq!(sealed.revision.sealed_version, 2);
        assert_eq!(
            store
                .load_seal_journal(journal.journal_id)
                .await
                .unwrap()
                .phase,
            SealPhase::Completed
        );
    }

    #[tokio::test]
    async fn seal_unknown_workspace_fails_closed() {
        let (store, _root) = root().await;
        let missing = WorkspaceId::new();
        let error = WorkspaceSealer::new(store).seal(missing).await.unwrap_err();
        assert!(matches!(error, WorkspaceError::WorkspaceNotFound(id) if id == missing));
    }

    #[tokio::test]
    async fn seal_waits_for_writer_release() {
        let (store, root) = root().await;
        let session = WorkspaceMountSession::acquire(
            store.clone(),
            root.workspace_id,
            3,
            DEFAULT_LEASE_TTL,
            DEFAULT_HEARTBEAT_INTERVAL,
        )
        .await
        .unwrap();
        let error = WorkspaceSealer::new(store.clone())
            .seal(root.workspace_id)
            .await
            .unwrap_err();
        assert!(matches!(error, WorkspaceError::Busy));
        session.release().await.unwrap();
    }
}
