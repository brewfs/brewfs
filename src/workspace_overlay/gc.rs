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
    volume_format: Option<String>,
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
            volume_format: None,
        }
    }

    /// Bind this legacy collector to a volume format. Native-v2 volumes must
    /// use `native_base::lifecycle::cleanup::apply_private_cleanup`; allowing
    /// the old mark-and-sweep would not distinguish permanent objects.
    pub fn with_volume_format(mut self, volume_format: impl Into<String>) -> Self {
        self.volume_format = Some(volume_format.into());
        self
    }

    pub async fn run_once(&self) -> Result<GcReport, WorkspaceError> {
        self.run_at(now_ns()?).await
    }

    pub async fn run_at(&self, now_ns: i64) -> Result<GcReport, WorkspaceError> {
        self.run_selected_at(now_ns, None).await
    }

    /// Collect one layer while retaining the complete catalog's root and
    /// shared-slice evidence. Selection bounds the deletion set, not reads.
    pub async fn run_layer_at(
        &self,
        now_ns: i64,
        layer_id: LayerId,
    ) -> Result<GcReport, WorkspaceError> {
        self.run_selected_at(now_ns, Some(layer_id)).await
    }

    async fn run_selected_at(
        &self,
        now_ns: i64,
        selected_layer: Option<LayerId>,
    ) -> Result<GcReport, WorkspaceError> {
        if self.volume_format.as_deref() == Some("workspace-native-v2") {
            return Err(WorkspaceError::UnsupportedVolumeFormat(
                "legacy WorkspaceGc is disabled for workspace-native-v2; use the private-domain cleaner".into(),
            ));
        }
        self.store.reap_packed_reader_sessions().await?;
        let lease_grace_ns = duration_ns(self.lease_grace)?;
        self.store.reap_expired_leases().await?;
        // 宽限期结束后清理终止状态的租约与 journal，再扫描 GC 根集合。
        self.store
            .prune_terminal_records(now_ns, lease_grace_ns)
            .await?;
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
        let candidates = snapshot
            .layers
            .iter()
            .filter(|layer| {
                selected_layer.is_none_or(|selected| selected == layer.layer_id)
                    && !reachable.contains(&layer.layer_id)
                    && layer.created_at_ns <= layer_cutoff
            })
            .map(|layer| layer.layer_id)
            .collect::<BTreeSet<_>>();
        // A bounded native finalizer authenticates every target incarnation in
        // one durable reservation. Select one deterministic batch before
        // computing slice reachability so candidates left for a later tick
        // protect their shared slices from physical deletion.
        let deleted_layers = select_gc_targets(candidates, self.store.gc_target_limit());
        let deleted_layer_set = deleted_layers.iter().copied().collect::<BTreeSet<_>>();
        let protected_slices = snapshot
            .slice_references
            .iter()
            .filter(|reference| {
                reachable.contains(&reference.layer_id)
                    || !deleted_layer_set.contains(&reference.layer_id)
            })
            .map(|reference| reference.slice_id)
            .collect::<BTreeSet<_>>();
        let mut orphan_slices = BTreeMap::<u64, u64>::new();
        let mut selected_slice_bounds = BTreeMap::<u64, u64>::new();
        for reference in &snapshot.slice_references {
            if deleted_layer_set.contains(&reference.layer_id) {
                selected_slice_bounds
                    .entry(reference.slice_id)
                    .and_modify(|end| *end = (*end).max(reference.slice_end))
                    .or_insert(reference.slice_end);
            }
            if deleted_layer_set.contains(&reference.layer_id)
                && !protected_slices.contains(&reference.slice_id)
            {
                orphan_slices
                    .entry(reference.slice_id)
                    .and_modify(|end| *end = (*end).max(reference.slice_end))
                    .or_insert(reference.slice_end);
            }
        }

        // 候选集合允许过期；存储后端在标记 Deleting 时复核根引用并执行 CAS。
        self.store
            .delete_layer_metadata(DeleteLayerMetadata {
                layer_ids: deleted_layers.clone(),
                now_ns,
                lease_grace_ns,
            })
            .await?;

        // A shared slice survives this layer's metadata deletion. Persist its
        // largest observed range first so a later, shorter reference cannot
        // cause the tail to be forgotten. Errors retain the Deleting layer.
        for (slice_id, slice_end) in selected_slice_bounds {
            self.blocks
                .retain_gc_slice_upper_bound(slice_id, slice_end)
                .await
                .map_err(|error| WorkspaceError::Backend(error.to_string()))?;
        }

        let mut deleted_slices = Vec::with_capacity(orphan_slices.len());
        let mut pending_slices = Vec::with_capacity(orphan_slices.len());
        let mut orphan_bytes = 0u64;
        for (slice_id, slice_end) in orphan_slices {
            let slice_end = self
                .blocks
                .gc_slice_upper_bound(slice_id, slice_end)
                .await
                .map_err(|error| WorkspaceError::Backend(error.to_string()))?
                .max(slice_end);
            let blocks = slice_end.div_ceil(u64::from(self.layout.block_size));
            // A completed range is an authenticated durable progress marker.
            // The snapshot and retained bound above are still rebuilt every
            // tick, so a newly shared SID cannot be skipped accidentally.
            if self
                .blocks
                .gc_slice_deletion_complete(slice_id, blocks)
                .await
                .map_err(|error| WorkspaceError::Backend(error.to_string()))?
                && self
                    .store
                    .gc_slice_deletion_reservation_complete(slice_id, slice_end, &deleted_layers)
                    .await?
            {
                orphan_bytes = orphan_bytes.saturating_add(slice_end);
                deleted_slices.push(slice_id);
                continue;
            }
            pending_slices.push((slice_id, slice_end, blocks));
        }
        // Native KV backends amortize their extent census across this bounded
        // batch. Other stores use the trait's single-SID compatibility path.
        self.store
            .reserve_gc_slice_deletions(
                &pending_slices
                    .iter()
                    .map(|(slice_id, slice_end, _)| (*slice_id, *slice_end))
                    .collect::<Vec<_>>(),
                &deleted_layers,
            )
            .await?;
        for (slice_id, slice_end, blocks) in pending_slices {
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

fn select_gc_targets(candidates: BTreeSet<LayerId>, limit: Option<usize>) -> Vec<LayerId> {
    match limit.filter(|limit| *limit > 0) {
        Some(limit) => candidates.into_iter().take(limit).collect(),
        None => candidates.into_iter().collect(),
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
    use crate::chunk::{BlockKey, InMemoryBlockStore, IncompleteBlockRead, SliceDesc};
    use crate::meta::MetaLayer;
    use crate::workspace_overlay::catalog::{CreateVolumeRoot, RecordOrphanSlice, WorkspaceStore};
    use crate::workspace_overlay::ids::{LayerId, WorkspaceId};
    use crate::workspace_overlay::lifecycle::{
        DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_LEASE_TTL, WorkspaceLifecycle, WorkspaceMountSession,
    };
    use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
    use crate::workspace_overlay::stores::database::SqliteWorkspaceStore;

    // The same test store retains both bytes and monotonic GC range bounds.
    // Bounds survive deletion; they do not authorize reachability or deletion.
    #[derive(Default)]
    struct RetainedRangeBlockStore {
        inner: InMemoryBlockStore,
        bounds: tokio::sync::RwLock<BTreeMap<u64, u64>>,
    }

    #[async_trait::async_trait]
    impl BlockStore for RetainedRangeBlockStore {
        async fn write_fresh_range(
            &self,
            key: BlockKey,
            offset: u64,
            data: &[u8],
        ) -> anyhow::Result<u64> {
            self.inner.write_fresh_range(key, offset, data).await
        }

        async fn read_range(
            &self,
            key: BlockKey,
            offset: u64,
            buf: &mut [u8],
        ) -> anyhow::Result<()> {
            self.inner.read_range(key, offset, buf).await
        }

        async fn delete_range(&self, key: BlockKey, count: u64) -> anyhow::Result<()> {
            self.inner.delete_range(key, count).await
        }

        async fn retain_gc_slice_upper_bound(&self, slice: u64, end: u64) -> anyhow::Result<()> {
            anyhow::ensure!(slice != 0 && end != 0, "fixture GC bound must be positive");
            self.bounds
                .write()
                .await
                .entry(slice)
                .and_modify(|bound| *bound = (*bound).max(end))
                .or_insert(end);
            Ok(())
        }

        async fn gc_slice_upper_bound(&self, slice: u64, observed: u64) -> anyhow::Result<u64> {
            self.bounds
                .read()
                .await
                .get(&slice)
                .copied()
                .filter(|bound| *bound >= observed)
                .ok_or_else(|| anyhow::anyhow!("fixture GC range was not retained"))
        }
    }

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
                upper_bucket: None,
                volume_format: "workspace-v1".into(),
                schema_version: crate::workspace_overlay::model::WORKSPACE_SCHEMA_VERSION,
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
        let source_workspace_id = session.view.workspace_id;
        session.release().await.unwrap();
        let sealed = crate::workspace_overlay::test_base_fixture::install_test_base(
            &store,
            source_workspace_id,
        )
        .await;
        lifecycle
            .fork_revision(sealed.revision, 1, None)
            .await
            .unwrap();
        lifecycle.discard(source_workspace_id, false).await.unwrap();
        let gc = WorkspaceGc::new(
            store,
            Arc::new(RetainedRangeBlockStore::default()),
            ChunkLayout::default(),
            Duration::ZERO,
            Duration::from_secs(60),
        );
        let report = gc.run_at(i64::MAX / 2).await.unwrap();
        assert!(!report.deleted_slices.contains(&77));
    }

    #[tokio::test]
    async fn gc_prunes_all_old_released_leases() {
        let (store, first) = setup().await;
        let workspace_id = first.view.workspace_id;
        first.release().await.unwrap();
        let second = WorkspaceMountSession::acquire(
            store.clone(),
            workspace_id,
            2,
            DEFAULT_LEASE_TTL,
            DEFAULT_HEARTBEAT_INTERVAL,
        )
        .await
        .unwrap();
        second.release().await.unwrap();
        assert_eq!(store.list_leases(workspace_id).await.unwrap().len(), 2);
        let gc = WorkspaceGc::new(
            store.clone(),
            Arc::new(InMemoryBlockStore::new()),
            ChunkLayout::default(),
            Duration::ZERO,
            Duration::ZERO,
        );
        gc.run_at(i64::MAX / 2).await.unwrap();
        assert!(store.list_leases(workspace_id).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn persisted_upload_orphan_is_deleted_end_to_end() {
        let (store, session) = setup().await;
        let blocks = Arc::new(RetainedRangeBlockStore::default());
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
        let error = blocks
            .read_range((88, 0), 0, &mut output)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<IncompleteBlockRead>().is_some());
        assert_eq!(output, [9; 6]);
        session.release().await.unwrap();
    }

    #[test]
    fn bounded_gc_targets_leave_later_candidates_for_the_next_tick() {
        let candidates = (0..99)
            .map(|index| LayerId::from_uuid(Uuid::from_u128(10_000 + index)))
            .collect::<BTreeSet<_>>();
        let batch = select_gc_targets(candidates.clone(), Some(4));
        assert_eq!(batch.len(), 4);
        assert_eq!(
            batch,
            candidates.iter().copied().take(4).collect::<Vec<_>>()
        );
        assert_eq!(select_gc_targets(candidates, None).len(), 99);
    }

    #[tokio::test]
    async fn legacy_gc_rejects_native_v2_volumes() {
        let (store, session) = setup().await;
        let gc = WorkspaceGc::new(
            store,
            Arc::new(InMemoryBlockStore::new()),
            ChunkLayout::default(),
            Duration::ZERO,
            Duration::from_secs(60),
        )
        .with_volume_format("workspace-native-v2");
        let error = gc.run_at(i64::MAX / 2).await.unwrap_err();
        assert!(matches!(error, WorkspaceError::UnsupportedVolumeFormat(_)));
        session.release().await.unwrap();
    }
}
