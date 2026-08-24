//! Offline, lease-aware sealed-chain compaction.

use std::collections::BTreeSet;
use std::sync::Arc;

use super::catalog::{CompactionResult, InstallCompaction, WorkspaceStore};
use super::digest::CanonicalLayerDelta;
use super::error::WorkspaceError;
use super::ids::{LayerId, WorkspaceId};
use super::model::{
    AclDelta, DataExtentDelta, DentryDelta, ExtentKind, InodeState, LayerState, ValueOp, XattrDelta,
};
use super::resolver::{resolve_acl, resolve_dentry, resolve_extents, resolve_inode, resolve_xattr};

pub const SOFT_LAYER_DEPTH: u32 = 8;

pub struct WorkspaceCompactor<W> {
    store: Arc<W>,
    chunk_size: u64,
}

impl<W: WorkspaceStore + 'static> WorkspaceCompactor<W> {
    pub fn new(store: Arc<W>, chunk_size: u64) -> Self {
        Self { store, chunk_size }
    }

    pub async fn compact(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<CompactionResult, WorkspaceError> {
        let workspace = self.store.load_workspace(workspace_id).await?;
        let head = self.store.load_layer(workspace.head_layer_id).await?;
        if head.state != LayerState::Writable || head.next_sequence != 1 {
            return Err(WorkspaceError::Busy);
        }
        let parent = head.parent_layer_id.ok_or_else(|| {
            WorkspaceError::CorruptMetadata("writable head lacks sealed parent".into())
        })?;
        let chain = self.store.load_layer_chain(parent).await?;
        if chain.iter().any(|layer| layer.state != LayerState::Sealed) {
            return Err(WorkspaceError::CorruptMetadata(
                "compaction source contains a non-sealed layer".into(),
            ));
        }
        let compacted_layer_id = LayerId::new();
        let delta = self.materialize(compacted_layer_id, &chain).await?;
        let compacted_bytes = delta
            .extents
            .iter()
            .map(|extent| extent.length)
            .fold(0u64, u64::saturating_add);
        let result = self
            .store
            .install_compaction(InstallCompaction {
                workspace_id,
                expected_head_layer_id: workspace.head_layer_id,
                expected_head_epoch: workspace.head_epoch,
                expected_parent_layer_id: parent,
                compacted_layer_id,
                replacement_head_layer_id: LayerId::new(),
                delta,
            })
            .await;
        if result.is_ok() {
            super::metrics::global_workspace_metrics().add_compaction_bytes(compacted_bytes);
        }
        result
    }

    async fn materialize(
        &self,
        output_layer: LayerId,
        chain: &[super::model::LayerRecord],
    ) -> Result<CanonicalLayerDelta, WorkspaceError> {
        let mut input = CanonicalLayerDelta::default();
        for layer in chain {
            let delta = self.store.load_layer_delta(layer.layer_id).await?;
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
            if let Some(mut inode) =
                resolve_inode(chain, &input.inodes, ino)?.map(|value| value.inode)
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
            for extent in
                resolve_extents(chain, &input.extents, ino, chunk_index, 0..self.chunk_size)?
            {
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

        let mut sequence = 1u64;
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
}

fn next_sequence(sequence: u64) -> Result<u64, WorkspaceError> {
    sequence
        .checked_add(1)
        .ok_or_else(|| WorkspaceError::CorruptMetadata("compaction sequence overflows".into()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use super::*;
    use crate::chunk::SliceDesc;
    use crate::chunk::read_plan::WorkspaceReadPlanProvider;
    use crate::meta::MetaLayer;
    use crate::workspace_overlay::catalog::{CreateVolumeRoot, WorkspaceStore};
    use crate::workspace_overlay::ids::{LayerId, WorkspaceId};
    use crate::workspace_overlay::lifecycle::{
        DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_LEASE_TTL, NoopDurableRemoteBarrier,
        WorkspaceLifecycle, WorkspaceMountSession,
    };
    use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
    use crate::workspace_overlay::model::ViewContext;
    use crate::workspace_overlay::stores::database::SqliteWorkspaceStore;

    #[tokio::test]
    async fn compaction_preserves_namespace_attributes_and_extent_plan() {
        let store = Arc::new(
            SqliteWorkspaceStore::connect("sqlite::memory:")
                .await
                .unwrap(),
        );
        store.initialize_workspace_schema().await.unwrap();
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(901));
        store
            .create_volume_root(CreateVolumeRoot {
                volume_id: Uuid::from_u128(902),
                workspace_id,
                root_layer_id: LayerId::from_uuid(Uuid::from_u128(903)),
                writable_layer_id: LayerId::from_uuid(Uuid::from_u128(904)),
                owner_id: None,
            })
            .await
            .unwrap();
        let session = WorkspaceMountSession::acquire(
            store.clone(),
            workspace_id,
            1,
            DEFAULT_LEASE_TTL,
            DEFAULT_HEARTBEAT_INTERVAL,
        )
        .await
        .unwrap();
        let meta = WorkspaceMetaLayer::new(store.clone(), session.view.clone());
        let dir = meta.mkdir(meta.root_ino(), "work".into()).await.unwrap();
        let ino = meta.create_file(dir, "old".into()).await.unwrap();
        let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();
        meta.write(
            ino,
            chunk_id,
            SliceDesc {
                slice_id: 41,
                chunk_id,
                offset: 0,
                length: 8,
            },
            8,
        )
        .await
        .unwrap();
        let lifecycle = WorkspaceLifecycle::new(store.clone());
        let mut view = session.view.clone();
        for step in 0..3 {
            let sealed = lifecycle
                .seal(&view, &NoopDurableRemoteBarrier)
                .await
                .unwrap();
            view = ViewContext {
                head_layer_id: sealed.new_head_layer_id,
                head_epoch: sealed.head_epoch,
                ..view
            };
            meta.replace_view_context(view.clone()).await;
            match step {
                0 => {
                    meta.rename(dir, "old", dir, "new".into()).await.unwrap();
                }
                1 => {
                    meta.set_xattr(ino, "user.compacted", b"yes", 0)
                        .await
                        .unwrap();
                }
                _ => {}
            }
        }
        let expected_plan = meta.read_plan(ino, 0, 0, 8).await.unwrap();
        session.release().await.unwrap();

        let result = WorkspaceCompactor::new(store.clone(), crate::chunk::DEFAULT_CHUNK_SIZE)
            .compact(workspace_id)
            .await
            .unwrap();
        assert_eq!(
            store
                .load_layer_chain(result.replacement_head_layer_id)
                .await
                .unwrap()
                .len(),
            2
        );
        let mounted = WorkspaceMountSession::acquire(
            store.clone(),
            workspace_id,
            2,
            DEFAULT_LEASE_TTL,
            DEFAULT_HEARTBEAT_INTERVAL,
        )
        .await
        .unwrap();
        let compacted = WorkspaceMetaLayer::new(store, mounted.view.clone());
        assert_eq!(compacted.lookup(dir, "new").await.unwrap(), Some(ino));
        assert_eq!(
            compacted.get_xattr(ino, "user.compacted").await.unwrap(),
            Some(b"yes".to_vec())
        );
        assert_eq!(
            compacted.read_plan(ino, 0, 0, 8).await.unwrap(),
            expected_plan
        );
        mounted.release().await.unwrap();
    }
}
