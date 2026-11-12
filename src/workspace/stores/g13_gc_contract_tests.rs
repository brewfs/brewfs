//! Candidate helpers call production catalog, resolver, collector and blocks.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::chunk::read_plan::{WorkspaceReadPlanProvider, execute_into};
use crate::chunk::{BlockKey, BlockStore, ChunkLayout, InMemoryBlockStore, SliceDesc};
use crate::meta::MetaLayer;
use crate::workspace::catalog::*;
use crate::workspace::error::WorkspaceError;
use crate::workspace::gc::WorkspaceGc;
use crate::workspace::ids::{LeaseId, WorkspaceId};
use crate::workspace::meta_layer::WorkspaceMetaLayer;
use crate::workspace::model::*;

/// Only this fixture supports an in-memory retained range ledger. The same
/// Arc owns bytes and monotonic bounds across fresh collector instances. It
/// never grants reachability/deletion authority and deletion keeps the bound.
#[derive(Default)]
pub(super) struct RetainedRangeBlockStore {
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

    async fn read_range(&self, key: BlockKey, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.inner.read_range(key, offset, buf).await
    }

    async fn delete_range(&self, key: BlockKey, count: u64) -> anyhow::Result<()> {
        self.inner.delete_range(key, count).await
    }

    async fn retain_gc_slice_upper_bound(&self, slice: u64, end: u64) -> anyhow::Result<()> {
        anyhow::ensure!(
            slice != 0 && end != 0,
            "fixture GC slice bound must be positive"
        );
        self.bounds
            .write()
            .await
            .entry(slice)
            .and_modify(|bound| *bound = (*bound).max(end))
            .or_insert(end);
        Ok(())
    }

    async fn gc_slice_upper_bound(&self, slice: u64, observed: u64) -> anyhow::Result<u64> {
        anyhow::ensure!(
            slice != 0 && observed != 0,
            "fixture GC observed bound must be positive"
        );
        self.bounds
            .read()
            .await
            .get(&slice)
            .copied()
            .filter(|bound| *bound >= observed)
            .ok_or_else(|| anyhow::anyhow!("fixture GC range was not retained"))
    }
}

pub(super) const GRACE_NS: u64 = 200;
pub(super) const BOUNDARIES: [i64; 5] = [-1, 0, 199, 200, 201];
pub(super) const SHARED_SLICE: u64 = 77;
pub(super) const SHARED_BYTES: &[u8; 6] = b"shared";

pub(super) async fn assert_mark<W: WorkspaceStore>(store: &W, lease: &SnapshotLease, elapsed: i64) {
    let now = lease.expires_at_ns.checked_add(elapsed).unwrap();
    let snapshot = store.gc_snapshot(now, GRACE_NS).await.unwrap();
    assert_eq!(
        snapshot.root_layers.contains(&lease.base_revision.layer_id),
        elapsed < GRACE_NS as i64,
        "actual GC mark disagrees with recovery grace: state={:?}, elapsed={elapsed}",
        lease.state,
    );
}

pub(super) async fn assert_delete<W: WorkspaceStore>(
    deleting: &W,
    peer: &W,
    lease: &SnapshotLease,
    elapsed: i64,
) {
    let base = lease.base_revision.layer_id;
    let before = peer.load_layer(base).await.unwrap();
    assert_eq!(before.state, LayerState::Sealed);
    let result = deleting
        .delete_layer_metadata(DeleteLayerMetadata {
            layer_ids: vec![base],
            now_ns: lease.expires_at_ns.checked_add(elapsed).unwrap(),
            lease_grace_ns: GRACE_NS,
        })
        .await;
    if elapsed < GRACE_NS as i64 {
        assert!(
            matches!(result, Err(WorkspaceError::Busy)),
            "delete revalidation must preserve the only lease root: state={:?}, elapsed={elapsed}, result={result:?}",
            lease.state,
        );
        assert_eq!(peer.load_layer(base).await.unwrap(), before);
    } else {
        result.unwrap();
        assert_eq!(
            peer.load_layer(base).await.unwrap().state,
            LayerState::Deleting
        );
        deleting
            .finalize_layer_metadata_deletion(vec![base])
            .await
            .unwrap();
        assert!(matches!(
            peer.load_layer(base).await,
            Err(WorkspaceError::LayerNotFound(found)) if found == base,
        ));
    }
}

pub(super) fn collector<W: WorkspaceStore + 'static>(
    store: Arc<W>,
    blocks: Arc<RetainedRangeBlockStore>,
    grace: u64,
) -> WorkspaceGc<W, RetainedRangeBlockStore> {
    WorkspaceGc::new(
        store,
        blocks,
        ChunkLayout::default(),
        Duration::ZERO,
        Duration::from_nanos(grace),
    )
}

/// Construct nonempty sealed bytes through the actual native metadata path.
/// The fixture already owns the bytes before the test-only noop barrier.
pub(super) async fn build_shared_base<W: WorkspaceStore + 'static>(
    store: Arc<W>,
    request: CreateVolumeRoot,
) -> (BaseRevision, i64, Arc<RetainedRangeBlockStore>) {
    store.initialize_workspace_schema().await.unwrap();
    let workspace = store.create_volume_root(request).await.unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::new(),
            holder_generation: 1,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    let view = ViewContext {
        workspace_id: workspace.workspace_id,
        head_layer_id: workspace.head_layer_id,
        head_epoch: workspace.head_epoch,
        lease_id: lease.lease_id,
        holder_generation: lease.holder_generation,
    };
    let meta = WorkspaceMetaLayer::new(store.clone(), view.clone());
    let ino = meta
        .create_file(meta.root_ino(), "shared".into())
        .await
        .unwrap();
    let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();
    let blocks = Arc::new(RetainedRangeBlockStore::default());
    blocks
        .write_fresh_range((SHARED_SLICE, 0), 0, SHARED_BYTES)
        .await
        .unwrap();
    meta.write(
        ino,
        chunk_id,
        SliceDesc {
            slice_id: SHARED_SLICE,
            chunk_id,
            offset: 0,
            length: SHARED_BYTES.len() as u64,
        },
        SHARED_BYTES.len() as u64,
    )
    .await
    .unwrap();
    store
        .release_lease(ReleaseLease {
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        })
        .await
        .unwrap();
    let sealed =
        crate::workspace::test_base_fixture::install_test_base(&store, workspace.workspace_id)
            .await;
    store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: workspace.workspace_id,
            force_fence_lease: false,
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .load_layer(sealed.revision.layer_id)
            .await
            .unwrap()
            .state,
        LayerState::Sealed
    );
    let snapshot = store.gc_snapshot(i64::MAX / 2, 0).await.unwrap();
    assert!(
        snapshot.root_layers.is_empty(),
        "fixture has an unexpected live GC root"
    );
    assert!(snapshot.slice_references.iter().any(|r| {
        r.layer_id == sealed.revision.layer_id
            && r.slice_id == SHARED_SLICE
            && r.slice_end == SHARED_BYTES.len() as u64
    }));
    (sealed.revision, ino, blocks)
}

pub(super) async fn assert_child_reads_shared<W: WorkspaceStore + 'static, B: BlockStore + Sync>(
    store: Arc<W>,
    child: WorkspaceId,
    base: &BaseRevision,
    ino: i64,
    blocks: &B,
) {
    let workspace = store.load_workspace(child).await.unwrap();
    assert_eq!(workspace.fork_base.as_ref(), Some(base));
    let chain = store
        .load_layer_chain(workspace.head_layer_id)
        .await
        .unwrap();
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[1].layer_id, base.layer_id);
    assert_eq!(chain[1].state, LayerState::Sealed);
    assert_eq!(chain[1].sealed_version, Some(base.sealed_version));
    assert_eq!(chain[1].root_hash, Some(base.root_hash));
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: child,
            lease_id: LeaseId::new(),
            holder_generation: 7,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    let meta = WorkspaceMetaLayer::new(
        store.clone(),
        ViewContext {
            workspace_id: child,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        },
    );
    assert_eq!(
        meta.lookup(meta.root_ino(), "shared").await.unwrap(),
        Some(ino)
    );
    assert_eq!(
        meta.stat(ino).await.unwrap().unwrap().size,
        SHARED_BYTES.len() as u64
    );
    let plan = meta
        .read_plan(ino, 0, 0, SHARED_BYTES.len() as u64)
        .await
        .unwrap();
    let mut output = [0; SHARED_BYTES.len()];
    execute_into(blocks, ChunkLayout::default(), 0, &plan, &mut output)
        .await
        .unwrap();
    assert_eq!(&output, SHARED_BYTES);
    store
        .release_lease(ReleaseLease {
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        })
        .await
        .unwrap();
}
