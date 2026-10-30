//! Target selection uses the production Redis/TiKV catalog implementation.
//! The memory backend supplies the atomic KV substrate, not SQLite behavior.

use super::tests::JournalMemoryBackend;
use crate::chunk::{BlockKey, BlockStore, ChunkLayout, InMemoryBlockStore};
use crate::meta::MetaLayer;
use crate::workspace_overlay::catalog::{
    AcquireLease, AppendDataExtent, CreateVolumeRoot, ExtentQuery, HeadGuard, RecordOrphanSlice,
    WorkspaceStore,
};
use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::gc::WorkspaceGc;
use crate::workspace_overlay::ids::{LayerId, LeaseId, WorkspaceId};
use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
use crate::workspace_overlay::model::{
    DataExtentDelta, LayerState, ViewContext, WORKSPACE_SCHEMA_VERSION,
};
use crate::workspace_overlay::stores::kv_store::{KvWorkspaceStore, VOLUME_FORMAT};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use uuid::Uuid;

#[derive(Default)]
struct ObservedBlocks {
    inner: InMemoryBlockStore,
    deletions: Mutex<Vec<(BlockKey, u64)>>,
    retained_bounds: Mutex<BTreeMap<u64, u64>>,
    fail_retain: AtomicBool,
    retain_pause: std::sync::Mutex<Option<Arc<RetainedBoundPause>>>,
}

struct RetainedBoundPause {
    slice: u64,
    armed: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
}

impl RetainedBoundPause {
    fn new(slice: u64) -> Self {
        Self {
            slice,
            armed: AtomicBool::new(true),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }
}

struct ReleaseRetainedBoundOnDrop(Arc<RetainedBoundPause>);

impl Drop for ReleaseRetainedBoundOnDrop {
    fn drop(&mut self) {
        self.0.release.add_permits(1);
    }
}

#[async_trait]
impl BlockStore for ObservedBlocks {
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

    async fn delete_range(&self, key: BlockKey, block_count: u64) -> anyhow::Result<()> {
        self.deletions.lock().await.push((key, block_count));
        self.inner.delete_range(key, block_count).await
    }

    async fn retain_gc_slice_upper_bound(
        &self,
        slice_id: u64,
        slice_end: u64,
    ) -> anyhow::Result<()> {
        if self.fail_retain.load(Ordering::SeqCst) {
            anyhow::bail!("injected persistent slice bound failure");
        }
        self.retained_bounds
            .lock()
            .await
            .entry(slice_id)
            .and_modify(|end| *end = (*end).max(slice_end))
            .or_insert(slice_end);
        let pause = self.retain_pause.lock().unwrap().clone();
        if let Some(pause) = pause
            && pause.slice == slice_id
            && pause.armed.swap(false, Ordering::SeqCst)
        {
            // The actual collector has marked the target and durably retained
            // its bound, but has not dispatched the first physical DELETE.
            pause.entered.add_permits(1);
            pause.release.acquire().await?.forget();
        }
        Ok(())
    }

    async fn gc_slice_upper_bound(&self, slice_id: u64, observed_end: u64) -> anyhow::Result<u64> {
        Ok(self
            .retained_bounds
            .lock()
            .await
            .get(&slice_id)
            .copied()
            .unwrap_or(0)
            .max(observed_end))
    }
}

fn layer(value: u128) -> LayerId {
    LayerId::from_uuid(Uuid::from_u128(value))
}

async fn catalog() -> Arc<KvWorkspaceStore<JournalMemoryBackend>> {
    let store = Arc::new(
        KvWorkspaceStore::from_arc(Arc::new(JournalMemoryBackend::at_time(100)))
            .with_packed_reader_pin_budget(
                crate::workspace_overlay::packed_v3::wire005::V3MountBudget::defaults(),
            ),
    );
    store.initialize_workspace_schema().await.unwrap();
    store
}

async fn orphan(
    store: &KvWorkspaceStore<JournalMemoryBackend>,
    layer_id: LayerId,
    slice_id: u64,
    slice_end: u64,
) {
    // Real deletion reservations require a real authenticated volume header.
    // The dedicated reachable/race fixtures already install their own volume.
    if store.load_volume_header().await.unwrap().is_none() {
        store
            .create_volume_root(CreateVolumeRoot {
                upper_bucket: None,
                volume_format: "workspace-v1".into(),
                schema_version: WORKSPACE_SCHEMA_VERSION,
                volume_id: Uuid::from_u128(901),
                workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(902)),
                root_layer_id: layer(903),
                writable_layer_id: layer(904),
                owner_id: None,
            })
            .await
            .unwrap();
    }
    store
        .record_orphan_slice(RecordOrphanSlice {
            orphan_layer_id: layer_id,
            slice_id,
            slice_end,
        })
        .await
        .unwrap();
}

fn collector(
    store: Arc<KvWorkspaceStore<JournalMemoryBackend>>,
    blocks: Arc<ObservedBlocks>,
) -> WorkspaceGc<KvWorkspaceStore<JournalMemoryBackend>, ObservedBlocks> {
    WorkspaceGc::new(
        store,
        blocks,
        ChunkLayout {
            chunk_size: 64,
            block_size: 4,
        },
        Duration::ZERO,
        Duration::ZERO,
    )
}

#[tokio::test]
async fn targeted_native_gc_removes_only_the_selected_orphan() {
    let store = catalog().await;
    let blocks = Arc::new(ObservedBlocks::default());
    orphan(&store, layer(41), 141, 6).await;
    orphan(&store, layer(42), 142, 6).await;
    blocks.write_fresh_range((141, 0), 0, b"one").await.unwrap();
    blocks.write_fresh_range((142, 0), 0, b"two").await.unwrap();

    let report = collector(store.clone(), blocks.clone())
        .run_layer_at(101, layer(41))
        .await
        .unwrap();

    assert_eq!(report.deleted_layers, vec![layer(41)]);
    assert_eq!(report.deleted_slices, vec![141]);
    assert_eq!(report.orphan_bytes, 6);
    assert_eq!(*blocks.deletions.lock().await, vec![((141, 0), 2)]);
    assert!(store.load_layer(layer(41)).await.is_err());
    assert!(store.load_layer(layer(42)).await.is_ok());
    let mut retained = [0; 3];
    blocks.read_range((142, 0), 0, &mut retained).await.unwrap();
    assert_eq!(&retained, b"two");
}

#[tokio::test]
async fn targeted_native_gc_keeps_a_reachable_selected_layer_and_issues_no_delete() {
    let store = catalog().await;
    let blocks = Arc::new(ObservedBlocks::default());
    store
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::from_u128(151),
            workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(152)),
            root_layer_id: layer(153),
            writable_layer_id: layer(154),
            owner_id: None,
        })
        .await
        .unwrap();
    orphan(&store, layer(155), 156, 6).await;

    let report = collector(store.clone(), blocks.clone())
        .run_layer_at(101, layer(153))
        .await
        .unwrap();

    assert_eq!(report.reachable_layers, 2);
    assert!(report.deleted_layers.is_empty());
    assert!(report.deleted_slices.is_empty());
    assert_eq!(report.orphan_bytes, 0);
    assert!(blocks.deletions.lock().await.is_empty());
    assert!(store.load_layer(layer(153)).await.is_ok());
    assert!(store.load_layer(layer(155)).await.is_ok());
}

#[tokio::test]
async fn targeted_native_gc_preserves_shared_slice_range_until_the_last_orphan() {
    let store = catalog().await;
    let blocks = Arc::new(ObservedBlocks::default());
    orphan(&store, layer(161), 163, 9).await;
    orphan(&store, layer(162), 163, 5).await;
    blocks
        .write_fresh_range((163, 0), 0, b"same")
        .await
        .unwrap();
    let first = collector(store.clone(), blocks.clone())
        .run_layer_at(101, layer(161))
        .await
        .unwrap();
    assert_eq!(first.deleted_layers, vec![layer(161)]);
    assert!(first.deleted_slices.is_empty());
    assert!(blocks.deletions.lock().await.is_empty());
    assert!(store.load_layer(layer(161)).await.is_err());
    assert!(store.load_layer(layer(162)).await.is_ok());
    assert_eq!(blocks.retained_bounds.lock().await.get(&163), Some(&9));
    let mut retained = [0; 4];
    blocks.read_range((163, 0), 0, &mut retained).await.unwrap();
    assert_eq!(&retained, b"same");

    let last = collector(store.clone(), blocks.clone())
        .run_layer_at(101, layer(162))
        .await
        .unwrap();
    assert_eq!(last.deleted_layers, vec![layer(162)]);
    assert_eq!(last.deleted_slices, vec![163]);
    assert_eq!(last.orphan_bytes, 9);
    assert_eq!(*blocks.deletions.lock().await, vec![((163, 0), 3)]);
    assert!(store.load_layer(layer(162)).await.is_err());
}

#[tokio::test]
async fn full_native_gc_still_collects_both_orphans_in_one_run() {
    let store = catalog().await;
    let blocks = Arc::new(ObservedBlocks::default());
    orphan(&store, layer(171), 173, 9).await;
    orphan(&store, layer(172), 173, 5).await;

    let report = collector(store.clone(), blocks.clone())
        .run_at(101)
        .await
        .unwrap();

    assert_eq!(report.deleted_layers, vec![layer(171), layer(172)]);
    assert_eq!(report.deleted_slices, vec![173]);
    assert_eq!(report.orphan_bytes, 9);
    assert_eq!(*blocks.deletions.lock().await, vec![((173, 0), 3)]);
    assert!(store.load_layer(layer(171)).await.is_err());
    assert!(store.load_layer(layer(172)).await.is_err());
}

#[tokio::test]
async fn targeted_native_gc_does_not_delete_or_finalize_without_a_retained_slice_bound() {
    let store = catalog().await;
    let blocks = Arc::new(ObservedBlocks::default());
    orphan(&store, layer(181), 182, 9).await;
    blocks.fail_retain.store(true, Ordering::SeqCst);

    let error = collector(store.clone(), blocks.clone())
        .run_layer_at(101, layer(181))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        crate::workspace_overlay::error::WorkspaceError::Backend(_)
    ));
    assert!(blocks.deletions.lock().await.is_empty());
    assert_eq!(
        store.load_layer(layer(181)).await.unwrap().state,
        LayerState::Deleting
    );
    assert!(blocks.retained_bounds.lock().await.is_empty());
}

#[tokio::test]
async fn targeted_native_gc_never_deletes_slice_published_after_bound_retention() {
    const WATCHDOG: Duration = Duration::from_secs(10);
    let store = catalog().await;
    // Independent catalog instances share only the real backend/CAS records.
    let writer = Arc::new(
        KvWorkspaceStore::from_arc(store.backend.clone()).with_packed_reader_pin_budget(
            crate::workspace_overlay::packed_v3::wire005::V3MountBudget::defaults(),
        ),
    );
    let workspace = writer
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: VOLUME_FORMAT.into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::from_u128(191),
            workspace_id: WorkspaceId::from_uuid(Uuid::from_u128(192)),
            root_layer_id: layer(193),
            writable_layer_id: layer(194),
            owner_id: None,
        })
        .await
        .unwrap();
    let lease = writer
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::from_uuid(Uuid::from_u128(195)),
            holder_generation: 1,
            ttl_ns: 1_000_000,
        })
        .await
        .unwrap();
    let meta = WorkspaceMetaLayer::with_chunk_size(
        writer.clone(),
        ViewContext {
            workspace_id: workspace.workspace_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        },
        64,
    );
    let ino = meta.create_file(1, "live-reference".into()).await.unwrap();
    // Allocate exactly once. The later birth reuses this existing immutable ID.
    let slice = u64::try_from(writer.allocate_id("slice").await.unwrap()).unwrap();
    let blocks = Arc::new(ObservedBlocks::default());
    blocks
        .write_fresh_range((slice, 0), 0, b"live")
        .await
        .unwrap();
    orphan(&store, layer(196), slice, 4).await;
    let pause = Arc::new(RetainedBoundPause::new(slice));
    *blocks.retain_pause.lock().unwrap() = Some(pause.clone());
    let release_on_drop = ReleaseRetainedBoundOnDrop(pause.clone());
    let gc = collector(store.clone(), blocks.clone());
    let collecting = tokio::spawn(async move { gc.run_layer_at(101, layer(196)).await });
    tokio::time::timeout(WATCHDOG, pause.entered.acquire())
        .await
        .expect("collector did not reach retained-bound barrier")
        .unwrap()
        .forget();
    assert_eq!(
        store.load_layer(layer(196)).await.unwrap().state,
        LayerState::Deleting
    );
    assert_eq!(blocks.retained_bounds.lock().await.get(&slice), Some(&4));
    assert!(blocks.deletions.lock().await.is_empty());

    let publication = tokio::time::timeout(
        WATCHDOG,
        writer.append_data_extent(AppendDataExtent {
            guard: HeadGuard {
                workspace_id: workspace.workspace_id,
                expected_head_layer_id: workspace.head_layer_id,
                expected_head_epoch: workspace.head_epoch,
                lease_id: lease.lease_id,
                holder_generation: lease.holder_generation,
            },
            extent: DataExtentDelta::data(workspace.head_layer_id, ino, 0, 0, 4, slice, 0, 0),
            chunk_size: 64,
        }),
    )
    .await
    .expect("existing SID publication did not resolve while GC was paused");
    drop(release_on_drop);
    let collected = tokio::time::timeout(WATCHDOG, collecting)
        .await
        .expect("collector did not finish after releasing retained bound")
        .expect("collector task panicked");
    assert!(
        matches!(
            &collected,
            Ok(_) | Err(WorkspaceError::Busy | WorkspaceError::Fenced)
        ),
        "unexpected collector outcome: {collected:?}"
    );

    match publication {
        Ok(published) => {
            let rows = writer
                .get_extent_deltas(ExtentQuery {
                    layer_ids: vec![workspace.head_layer_id],
                    ino,
                    chunk_index: 0,
                    range_start: 0,
                    range_end: 4,
                })
                .await
                .unwrap();
            assert_eq!(rows, vec![published], "live publication disappeared");
            let deletes = blocks.deletions.lock().await.clone();
            assert!(
                deletes.is_empty(),
                "existing SID {slice} was published into a live head after bound retention but physically deleted: {deletes:?}; GC={collected:?}"
            );
            let mut retained = [0; 4];
            blocks
                .read_range((slice, 0), 0, &mut retained)
                .await
                .unwrap();
            assert_eq!(&retained, b"live");
            if let Ok(report) = collected {
                assert!(!report.deleted_slices.contains(&slice));
            }
        }
        Err(error) => {
            assert!(
                matches!(&error, WorkspaceError::Busy | WorkspaceError::Fenced),
                "publication must be explicitly deferred or fenced: {error:?}"
            );
            let rows = writer
                .get_extent_deltas(ExtentQuery {
                    layer_ids: vec![workspace.head_layer_id],
                    ino,
                    chunk_index: 0,
                    range_start: 0,
                    range_end: 4,
                })
                .await
                .unwrap();
            assert!(rows.is_empty(), "fenced publication left an extent behind");
        }
    }
}
