use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

use serde::Serialize;

use super::super::catalog::WorkspaceStore;
use super::super::error::WorkspaceError;
use super::super::ids::LayerId;
use super::super::metrics::{WorkspaceMetrics, global_workspace_metrics};
use super::super::model::{BaseRevision, LayerRecord};
use super::super::resolver::{resolve_dentry, resolve_inode};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum PathChangeKind {
    Added,
    Modified,
    Deleted,
    Renamed,
    MetadataOnly,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PathChange {
    pub path: Vec<u8>,
    pub kind: PathChangeKind,
    pub old_path: Option<Vec<u8>>,
    pub ino: Option<i64>,
    pub changed_ranges: Vec<Range<u64>>,
}

pub struct WorkspaceDiff<W> {
    store: Arc<W>,
    chunk_size: u64,
    metrics: Arc<WorkspaceMetrics>,
}

impl<W: WorkspaceStore + 'static> WorkspaceDiff<W> {
    pub fn new(store: Arc<W>, chunk_size: u64) -> Self {
        Self {
            store,
            chunk_size,
            metrics: global_workspace_metrics(),
        }
    }

    pub async fn diff(
        &self,
        base: &BaseRevision,
        current_head: LayerId,
    ) -> Result<Vec<PathChange>, WorkspaceError> {
        let base_layer = self.store.load_layer(base.layer_id).await?;
        if base_layer.sealed_version != Some(base.sealed_version)
            || base_layer.root_hash != Some(base.root_hash)
        {
            return Err(WorkspaceError::Conflict(
                super::super::error::ConflictDetail {
                    path: Vec::new(),
                    reason: "diff base revision changed".into(),
                },
            ));
        }
        let current_chain = self.store.load_layer_chain(current_head).await?;
        let base_index = current_chain
            .iter()
            .position(|layer| layer.layer_id == base.layer_id)
            .ok_or_else(|| {
                WorkspaceError::Conflict(super::super::error::ConflictDetail {
                    path: Vec::new(),
                    reason: "base revision is not an ancestor of current head".into(),
                })
            })?;
        let base_chain = self.store.load_layer_chain(base.layer_id).await?;
        let current_deltas = self.load_deltas(&current_chain).await?;
        let base_deltas = self.load_deltas(&base_chain).await?;
        let old_paths = materialize_paths(&base_chain, &base_deltas)?;
        let new_paths = materialize_paths(&current_chain, &current_deltas)?;

        let changed_layers: BTreeSet<_> = current_chain[..base_index]
            .iter()
            .map(|layer| layer.layer_id)
            .collect();
        let mut changed_inodes = BTreeSet::new();
        let mut data_changed = BTreeSet::new();
        let mut changed_ranges: BTreeMap<i64, Vec<Range<u64>>> = BTreeMap::new();
        for delta in current_deltas.values() {
            for inode in &delta.inodes {
                if changed_layers.contains(&inode.layer_id) {
                    changed_inodes.insert(inode.ino);
                }
            }
            for xattr in &delta.xattrs {
                if changed_layers.contains(&xattr.layer_id) {
                    changed_inodes.insert(xattr.ino);
                }
            }
            for acl in &delta.acls {
                if changed_layers.contains(&acl.layer_id) {
                    changed_inodes.insert(acl.ino);
                }
            }
            for extent in &delta.extents {
                if changed_layers.contains(&extent.layer_id) {
                    changed_inodes.insert(extent.ino);
                    data_changed.insert(extent.ino);
                    let start = extent
                        .chunk_index
                        .checked_mul(self.chunk_size)
                        .and_then(|value| value.checked_add(extent.logical_offset))
                        .ok_or_else(|| {
                            WorkspaceError::CorruptMetadata("diff range overflows".into())
                        })?;
                    let end = start.checked_add(extent.length).ok_or_else(|| {
                        WorkspaceError::CorruptMetadata("diff range overflows".into())
                    })?;
                    changed_ranges
                        .entry(extent.ino)
                        .or_default()
                        .push(start..end);
                }
            }
            for dentry in &delta.dentries {
                if changed_layers.contains(&dentry.layer_id)
                    && let Some(ino) = dentry.ino
                {
                    changed_inodes.insert(ino);
                }
            }
        }
        for ranges in changed_ranges.values_mut() {
            *ranges = merge_ranges(std::mem::take(ranges));
        }

        let mut changes = BTreeMap::<Vec<u8>, PathChange>::new();
        for ino in changed_inodes {
            let old = old_paths.get(&ino).cloned().unwrap_or_default();
            let new = new_paths.get(&ino).cloned().unwrap_or_default();
            let removed = old.difference(&new).cloned().collect::<Vec<_>>();
            let added = new.difference(&old).cloned().collect::<Vec<_>>();
            let ranges = changed_ranges.get(&ino).cloned().unwrap_or_default();
            if removed.len() == 1 && added.len() == 1 {
                changes.insert(
                    added[0].clone(),
                    PathChange {
                        path: added[0].clone(),
                        kind: PathChangeKind::Renamed,
                        old_path: Some(removed[0].clone()),
                        ino: Some(ino),
                        changed_ranges: ranges.clone(),
                    },
                );
            } else {
                for path in removed {
                    changes.insert(
                        path.clone(),
                        PathChange {
                            path,
                            kind: PathChangeKind::Deleted,
                            old_path: None,
                            ino: Some(ino),
                            changed_ranges: Vec::new(),
                        },
                    );
                }
                for path in added {
                    changes.insert(
                        path.clone(),
                        PathChange {
                            path,
                            kind: PathChangeKind::Added,
                            old_path: None,
                            ino: Some(ino),
                            changed_ranges: ranges.clone(),
                        },
                    );
                }
            }
            for path in old.intersection(&new) {
                changes.entry(path.clone()).or_insert_with(|| PathChange {
                    path: path.clone(),
                    kind: if data_changed.contains(&ino) {
                        PathChangeKind::Modified
                    } else {
                        PathChangeKind::MetadataOnly
                    },
                    old_path: None,
                    ino: Some(ino),
                    changed_ranges: ranges.clone(),
                });
            }
        }
        let changes = changes.into_values().collect::<Vec<_>>();
        self.metrics.add_publish_changed_paths(changes.len() as u64);
        Ok(changes)
    }

    async fn load_deltas(
        &self,
        chain: &[LayerRecord],
    ) -> Result<BTreeMap<LayerId, super::super::digest::CanonicalLayerDelta>, WorkspaceError> {
        let mut deltas = BTreeMap::new();
        for layer in chain {
            deltas.insert(
                layer.layer_id,
                self.store.load_layer_delta(layer.layer_id).await?,
            );
        }
        Ok(deltas)
    }
}

fn materialize_paths(
    chain: &[LayerRecord],
    deltas: &BTreeMap<LayerId, super::super::digest::CanonicalLayerDelta>,
) -> Result<BTreeMap<i64, BTreeSet<Vec<u8>>>, WorkspaceError> {
    let all_dentries = deltas
        .values()
        .flat_map(|delta| delta.dentries.iter().cloned())
        .collect::<Vec<_>>();
    let all_inodes = deltas
        .values()
        .flat_map(|delta| delta.inodes.iter().cloned())
        .collect::<Vec<_>>();
    let candidates = all_dentries
        .iter()
        .map(|dentry| (dentry.parent_ino, dentry.name.clone()))
        .collect::<BTreeSet<_>>();
    let mut edges = Vec::new();
    for (parent, name) in candidates {
        if let Some(entry) = resolve_dentry(chain, &all_dentries, parent, &name)? {
            edges.push(entry);
        }
    }
    let mut paths: BTreeMap<i64, BTreeSet<Vec<u8>>> = BTreeMap::new();
    paths.entry(1).or_default().insert(b"/".to_vec());
    let mut unresolved = edges;
    for _ in 0..=unresolved.len() {
        let mut progress = false;
        let mut next = Vec::new();
        for edge in unresolved {
            let Some(parent_paths) = paths.get(&edge.parent_ino).cloned() else {
                next.push(edge);
                continue;
            };
            if resolve_inode(chain, &all_inodes, edge.ino)?.is_none() {
                continue;
            }
            for parent_path in parent_paths {
                let mut path = parent_path;
                if path.as_slice() != b"/" {
                    path.push(b'/');
                }
                path.extend_from_slice(&edge.name);
                progress |= paths.entry(edge.ino).or_default().insert(path);
            }
        }
        unresolved = next;
        if !progress {
            break;
        }
    }
    Ok(paths)
}

fn merge_ranges(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<Range<u64>> = Vec::new();
    for range in ranges {
        if let Some(last) = merged.last_mut()
            && range.start <= last.end
        {
            last.end = last.end.max(range.end);
            continue;
        }
        merged.push(range);
    }
    merged
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use super::*;
    use crate::chunk::SliceDesc;
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
    async fn diff_detects_rename_metadata_and_changed_data_ranges() {
        let store = Arc::new(
            SqliteWorkspaceStore::connect("sqlite::memory:")
                .await
                .unwrap(),
        );
        store.initialize_workspace_schema().await.unwrap();
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(701));
        store
            .create_volume_root(CreateVolumeRoot {
                volume_id: Uuid::from_u128(702),
                workspace_id,
                root_layer_id: LayerId::from_uuid(Uuid::from_u128(703)),
                writable_layer_id: LayerId::from_uuid(Uuid::from_u128(704)),
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
        let ino = meta
            .create_file(meta.root_ino(), "old".into())
            .await
            .unwrap();
        let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();
        meta.write(
            ino,
            chunk_id,
            SliceDesc {
                slice_id: 1,
                chunk_id,
                offset: 0,
                length: 8,
            },
            8,
        )
        .await
        .unwrap();
        let lifecycle = WorkspaceLifecycle::new(store.clone());
        let sealed = lifecycle
            .seal(&session.view, &NoopDurableRemoteBarrier)
            .await
            .unwrap();
        meta.replace_view_context(ViewContext {
            head_layer_id: sealed.new_head_layer_id,
            head_epoch: sealed.head_epoch,
            ..session.view.clone()
        })
        .await;
        meta.rename(meta.root_ino(), "old", meta.root_ino(), "new".into())
            .await
            .unwrap();
        meta.set_xattr(ino, "user.agent", b"changed", 0)
            .await
            .unwrap();
        meta.write(
            ino,
            chunk_id,
            SliceDesc {
                slice_id: 2,
                chunk_id,
                offset: 4,
                length: 2,
            },
            8,
        )
        .await
        .unwrap();

        let changes = WorkspaceDiff::new(store.clone(), crate::chunk::DEFAULT_CHUNK_SIZE)
            .diff(&sealed.revision, sealed.new_head_layer_id)
            .await
            .unwrap();
        let renamed = changes
            .iter()
            .find(|change| change.kind == PathChangeKind::Renamed)
            .unwrap();
        assert_eq!(renamed.path, b"/new");
        assert_eq!(renamed.old_path.as_deref(), Some(b"/old".as_slice()));
        assert_eq!(renamed.changed_ranges, vec![4..6]);
        session.release().await.unwrap();
    }
}
