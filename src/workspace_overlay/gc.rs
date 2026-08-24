//! Lease-aware layer and immutable-slice mark-and-sweep.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::chunk::{BlockStore, ChunkLayout};

use super::catalog::{DeleteLayerMetadata, WorkspaceStore};
use super::error::WorkspaceError;
use super::ids::LayerId;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GcReport {
    pub reachable_layers: usize,
    pub deleted_layers: Vec<LayerId>,
    pub deleted_slices: Vec<u64>,
    pub orphan_bytes: u64,
}

pub struct WorkspaceGc<W, B> {
    store: Arc<W>,
    blocks: Arc<B>,
    layout: ChunkLayout,
    layer_grace: Duration,
    lease_grace: Duration,
}

impl<W, B> WorkspaceGc<W, B>
where
    W: WorkspaceStore + 'static,
    B: BlockStore + Send + Sync + 'static,
{
    pub fn new(
        store: Arc<W>,
        blocks: Arc<B>,
        layout: ChunkLayout,
        layer_grace: Duration,
        lease_grace: Duration,
    ) -> Self {
        Self {
            store,
            blocks,
            layout,
            layer_grace,
            lease_grace,
        }
    }

    pub async fn run_once(&self) -> Result<GcReport, WorkspaceError> {
        self.run_at(now_ns()?).await
    }

    pub async fn run_at(&self, now_ns: i64) -> Result<GcReport, WorkspaceError> {
        let lease_grace_ns = duration_ns(self.lease_grace)?;
        let snapshot = self.store.gc_snapshot(now_ns, lease_grace_ns).await?;
        let parents = snapshot
            .layers
            .iter()
            .map(|layer| (layer.layer_id, layer.parent_layer_id))
            .collect::<BTreeMap<_, _>>();
        let mut reachable = BTreeSet::new();
        let mut pending = snapshot.root_layers.clone();
        while let Some(layer_id) = pending.pop() {
            if !reachable.insert(layer_id) {
                continue;
            }
            if let Some(Some(parent)) = parents.get(&layer_id) {
                pending.push(*parent);
            }
        }

        let layer_cutoff = now_ns.saturating_sub(to_i64(duration_ns(self.layer_grace)?)?);
        let deletable = snapshot
            .layers
            .iter()
            .filter(|layer| {
                !reachable.contains(&layer.layer_id) && layer.created_at_ns <= layer_cutoff
            })
            .map(|layer| layer.layer_id)
            .collect::<BTreeSet<_>>();
        let reachable_slices = snapshot
            .slice_references
            .iter()
            .filter(|reference| reachable.contains(&reference.layer_id))
            .map(|reference| reference.slice_id)
            .collect::<BTreeSet<_>>();
        let mut orphan_slices = BTreeMap::<u64, u64>::new();
        for reference in &snapshot.slice_references {
            if deletable.contains(&reference.layer_id)
                && !reachable_slices.contains(&reference.slice_id)
            {
                orphan_slices
                    .entry(reference.slice_id)
                    .and_modify(|end| *end = (*end).max(reference.slice_end))
                    .or_insert(reference.slice_end);
            }
        }

        let deleted_layers = deletable.into_iter().collect::<Vec<_>>();
        self.store
            .delete_layer_metadata(DeleteLayerMetadata {
                layer_ids: deleted_layers.clone(),
                now_ns,
                lease_grace_ns,
            })
            .await?;

        let mut deleted_slices = Vec::with_capacity(orphan_slices.len());
        let mut orphan_bytes = 0u64;
        for (slice_id, slice_end) in orphan_slices {
            let blocks = slice_end.div_ceil(u64::from(self.layout.block_size));
            if blocks != 0 {
                self.blocks
                    .delete_range((slice_id, 0), blocks)
                    .await
                    .map_err(|error| WorkspaceError::Backend(error.to_string()))?;
            }
            orphan_bytes = orphan_bytes.saturating_add(slice_end);
            deleted_slices.push(slice_id);
        }
        self.store
            .finalize_layer_metadata_deletion(deleted_layers.clone())
            .await?;
        let report = GcReport {
            reachable_layers: reachable.len(),
            deleted_layers,
            deleted_slices,
            orphan_bytes,
        };
        super::metrics::global_workspace_metrics()
            .set_gc(report.reachable_layers as u64, report.orphan_bytes);
        Ok(report)
    }
}

fn now_ns() -> Result<i64, WorkspaceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| WorkspaceError::Backend(error.to_string()))?
        .as_nanos();
    i64::try_from(nanos)
        .map_err(|_| WorkspaceError::CorruptMetadata("timestamp exceeds i64".into()))
}

fn duration_ns(duration: Duration) -> Result<u64, WorkspaceError> {
    u64::try_from(duration.as_nanos())
        .map_err(|_| WorkspaceError::CorruptMetadata("duration exceeds u64 nanoseconds".into()))
}

fn to_i64(value: u64) -> Result<i64, WorkspaceError> {
    i64::try_from(value)
        .map_err(|_| WorkspaceError::CorruptMetadata("duration exceeds i64 nanoseconds".into()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use super::*;
    use crate::chunk::{InMemoryBlockStore, SliceDesc};
    use crate::meta::MetaLayer;
    use crate::workspace_overlay::catalog::{CreateVolumeRoot, RecordOrphanSlice, WorkspaceStore};
    use crate::workspace_overlay::ids::{LayerId, WorkspaceId};
    use crate::workspace_overlay::lifecycle::{
        DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_LEASE_TTL, NoopDurableRemoteBarrier,
        WorkspaceLifecycle, WorkspaceMountSession,
    };
    use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
    use crate::workspace_overlay::stores::database::SqliteWorkspaceStore;

    async fn setup() -> (
        Arc<SqliteWorkspaceStore>,
        WorkspaceMountSession<SqliteWorkspaceStore>,
    ) {
        let store = Arc::new(
            SqliteWorkspaceStore::connect("sqlite::memory:")
                .await
                .unwrap(),
        );
        store.initialize_workspace_schema().await.unwrap();
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(801));
        store
            .create_volume_root(CreateVolumeRoot {
                volume_id: Uuid::from_u128(802),
                workspace_id,
                root_layer_id: LayerId::from_uuid(Uuid::from_u128(803)),
                writable_layer_id: LayerId::from_uuid(Uuid::from_u128(804)),
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
        (store, session)
    }

    #[tokio::test]
    async fn deleting_ninety_nine_children_keeps_the_shared_base() {
        let (store, session) = setup().await;
        let lifecycle = WorkspaceLifecycle::new(store.clone());
        let base = store
            .load_workspace(session.view.workspace_id)
            .await
            .unwrap()
            .fork_base
            .unwrap();
        let base_layer = base.layer_id;
        let children = lifecycle.fork_revision(base, 100, None).await.unwrap();
        for child in &children[..99] {
            lifecycle.discard(child.workspace_id, false).await.unwrap();
        }
        let gc = WorkspaceGc::new(
            store.clone(),
            Arc::new(InMemoryBlockStore::new()),
            ChunkLayout::default(),
            Duration::ZERO,
            Duration::from_secs(60),
        );
        let report = gc.run_at(i64::MAX / 2).await.unwrap();
        assert_eq!(report.deleted_layers.len(), 99);
        assert!(store.load_layer(base_layer).await.is_ok());
        assert!(
            store
                .load_workspace(children[99].workspace_id)
                .await
                .is_ok()
        );
        session.release().await.unwrap();
    }

    #[tokio::test]
    async fn reachable_child_prevents_shared_slice_collection() {
        let (store, session) = setup().await;
        let lifecycle = WorkspaceLifecycle::new(store.clone());
        let meta = WorkspaceMetaLayer::new(store.clone(), session.view.clone());
        let ino = meta
            .create_file(meta.root_ino(), "shared".into())
            .await
            .unwrap();
        let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();
        meta.write(
            ino,
            chunk_id,
            SliceDesc {
                slice_id: 77,
                chunk_id,
                offset: 0,
                length: 4096,
            },
            4096,
        )
        .await
        .unwrap();
        let sealed = lifecycle
            .seal(&session.view, &NoopDurableRemoteBarrier)
            .await
            .unwrap();
        lifecycle
            .fork_revision(sealed.revision, 1, None)
            .await
            .unwrap();
        let source_workspace_id = session.view.workspace_id;
        session.release().await.unwrap();
        lifecycle.discard(source_workspace_id, false).await.unwrap();
        let gc = WorkspaceGc::new(
            store,
            Arc::new(InMemoryBlockStore::new()),
            ChunkLayout::default(),
            Duration::ZERO,
            Duration::from_secs(60),
        );
        let report = gc.run_at(i64::MAX / 2).await.unwrap();
        assert!(!report.deleted_slices.contains(&77));
    }

    #[tokio::test]
    async fn persisted_upload_orphan_is_deleted_end_to_end() {
        let (store, session) = setup().await;
        let blocks = Arc::new(InMemoryBlockStore::new());
        blocks
            .write_fresh_range((88, 0), 0, b"orphan")
            .await
            .unwrap();
        let mut before = [0; 6];
        blocks.read_range((88, 0), 0, &mut before).await.unwrap();
        assert_eq!(&before, b"orphan");
        let orphan_layer_id = LayerId::from_uuid(Uuid::from_u128(805));
        store
            .record_orphan_slice(RecordOrphanSlice {
                orphan_layer_id,
                slice_id: 88,
                slice_end: 6,
            })
            .await
            .unwrap();

        let gc = WorkspaceGc::new(
            store.clone(),
            blocks.clone(),
            ChunkLayout::default(),
            Duration::ZERO,
            Duration::from_secs(60),
        );
        let report = gc.run_at(i64::MAX / 2).await.unwrap();
        assert_eq!(report.deleted_slices, vec![88]);
        assert!(report.deleted_layers.contains(&orphan_layer_id));
        assert!(store.load_layer(orphan_layer_id).await.is_err());
        let mut output = [9; 6];
        blocks.read_range((88, 0), 0, &mut output).await.unwrap();
        assert_eq!(output, [9; 6]);
        session.release().await.unwrap();
    }
}
