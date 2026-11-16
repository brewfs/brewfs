use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Notify;
use uuid::Uuid;

use super::*;
use crate::chunk::store::{BlockKey, InMemoryBlockStore};
use crate::chunk::{BlockStore, SliceDesc};
use crate::vfs::cache::config::CacheConfig;
use crate::vfs::config::VFSConfig;
use crate::workspace::catalog::{AcquireLease, CreateVolumeRoot, WorkspaceStore};
use crate::workspace::ids::{LayerId, LeaseId, WorkspaceId};
use crate::workspace::meta_layer::WorkspaceMetaLayer;
use crate::workspace::model::{ViewContext, WORKSPACE_SCHEMA_VERSION};
use crate::workspace::stores::database::SqliteWorkspaceStore;

#[derive(Default)]
struct PrefetchBlockStore {
    inner: InMemoryBlockStore,
    read_calls: AtomicUsize,
    warm_calls: AtomicUsize,
    warmed: Notify,
}

#[async_trait]
impl BlockStore for PrefetchBlockStore {
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
        output: &mut [u8],
    ) -> anyhow::Result<()> {
        self.read_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.read_range(key, offset, output).await
    }

    async fn warm_range(&self, _key: BlockKey, _offset: u64, _len: usize) -> anyhow::Result<()> {
        self.warm_calls.fetch_add(1, Ordering::SeqCst);
        self.warmed.notify_one();
        Ok(())
    }

    async fn delete_range(&self, key: BlockKey, block_count: u64) -> anyhow::Result<()> {
        self.inner.delete_range(key, block_count).await
    }
}

async fn fixture() -> (
    VFS<PrefetchBlockStore, WorkspaceMetaLayer<SqliteWorkspaceStore>>,
    Arc<PrefetchBlockStore>,
    i64,
) {
    let workspace_store = Arc::new(
        SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    );
    workspace_store.initialize_workspace_schema().await.unwrap();
    let workspace_id = WorkspaceId::new();
    let workspace = workspace_store
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::new_v4(),
            workspace_id,
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: None,
        })
        .await
        .unwrap();
    let lease = workspace_store
        .acquire_lease(AcquireLease {
            workspace_id,
            lease_id: LeaseId::new(),
            holder_generation: 1,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    let meta = Arc::new(WorkspaceMetaLayer::with_chunk_size(
        workspace_store,
        ViewContext {
            workspace_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        },
        64,
    ));
    meta.initialize().await.unwrap();

    let layout = ChunkLayout {
        chunk_size: 64,
        block_size: 8,
    };
    let ino = meta
        .create_file(meta.root_ino(), "prefetch".into())
        .await
        .unwrap();
    let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();
    let payload = b"abcdefghijklmnopqrstuvwx";
    meta.write(
        ino,
        chunk_id,
        SliceDesc {
            slice_id: 42,
            chunk_id,
            offset: 0,
            length: payload.len() as u64,
        },
        payload.len() as u64,
    )
    .await
    .unwrap();

    let mut cache = CacheConfig::default();
    cache.prefetch_enabled = true;
    cache.prefetch_initial_bytes = 8;
    cache.prefetch_max_bytes = 16;
    cache.prefetch_concurrency = 1;
    cache.memory_budget_bytes = 0;
    let store = Arc::new(PrefetchBlockStore::default());
    for (index, start) in [0usize, 8, 16].into_iter().enumerate() {
        store
            .inner
            .write_fresh_range((42, index as u32), 0, &payload[start..start + 8])
            .await
            .unwrap();
    }
    let fs = VFS::from_workspace_components(
        VFSConfig::new_with_cache_config(layout, cache),
        store.clone(),
        meta,
    )
    .unwrap();
    (fs, store, ino)
}

#[tokio::test]
async fn workspace_native_prefetch_uses_warm_range_without_an_extra_read_buffer() {
    let (fs, store, ino) = fixture().await;
    let attr = fs.stat_ino(ino).await.unwrap();
    let fh = fs.open(ino, attr, true, false, false).await.unwrap();
    let payload = b"abcdefghijklmnopqrstuvwx";

    let first_block = fs.read(fh, 0, 8).await.unwrap();
    assert_eq!(first_block.as_slice(), &payload[..8]);
    let reads_after_foreground = store.read_calls.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), store.warmed.notified())
        .await
        .expect("native prefetch should call warm_range");

    assert!(store.warm_calls.load(Ordering::SeqCst) > 0);
    assert_eq!(
        store.read_calls.load(Ordering::SeqCst),
        reads_after_foreground
    );
}
