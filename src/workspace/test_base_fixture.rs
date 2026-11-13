//! Test-only fixture: materialize a workspace's resolved layer chain
//! (including its writable head) as a new flat immutable lower plus an empty
//! writable overlay. Production lowers come only from packed publication;
//! nothing outside tests can rotate a workspace head.

use std::collections::BTreeSet;
use std::sync::Arc;

use super::catalog::{InstallTestBase, TestBaseResult, WorkspaceStore};
use super::digest::CanonicalLayerDelta;
use super::error::WorkspaceError;
use super::ids::{LayerId, WorkspaceId};
use super::model::{
    AclDelta, DataExtentDelta, DentryDelta, ExtentKind, InodeState, ValueOp, XattrDelta,
};
use super::resolver::{resolve_acl, resolve_dentry, resolve_extents, resolve_inode, resolve_xattr};

/// Flatten `workspace_id`'s current view into a fresh immutable lower.
pub(crate) async fn install_test_base<W: WorkspaceStore + 'static>(
    store: &Arc<W>,
    workspace_id: WorkspaceId,
) -> TestBaseResult {
    let workspace = store.load_workspace(workspace_id).await.unwrap();
    let chain = store
        .load_layer_chain(workspace.head_layer_id)
        .await
        .unwrap();
    let head = &chain[0];
    let compacted_layer_id = LayerId::new();
    let delta = TestBaseBuilder {
        store: store.clone(),
    }
    .materialize(compacted_layer_id, &chain)
    .await
    .unwrap();
    store
        .install_test_base(InstallTestBase {
            workspace_id,
            expected_head_layer_id: workspace.head_layer_id,
            expected_head_epoch: workspace.head_epoch,
            expected_parent_layer_id: head.parent_layer_id.unwrap(),
            compacted_layer_id,
            replacement_head_layer_id: LayerId::new(),
            delta,
        })
        .await
        .unwrap()
}

struct TestBaseBuilder<W> {
    store: Arc<W>,
}

impl<W: WorkspaceStore + 'static> TestBaseBuilder<W> {
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
                                "extent range overflows during test base materialization".into(),
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
