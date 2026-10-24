use super::*;
use crate::vfs::config::VFSConfig;
use crate::vfs::error::VfsError;
use crate::workspace_overlay::catalog::{AcquireLease, CreateVolumeRoot, WorkspaceStore};
use crate::workspace_overlay::ids::{LayerId, LeaseId, WorkspaceId};
use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
use crate::workspace_overlay::model::{ViewContext, WORKSPACE_SCHEMA_VERSION};
use crate::workspace_overlay::stores::database::{SqliteWorkspaceStore, StoreFailpoint};
use uuid::Uuid;

async fn fixture() -> (
    VFS<InMemoryBlockStore, WorkspaceMetaLayer<SqliteWorkspaceStore>>,
    Arc<WorkspaceMetaLayer<SqliteWorkspaceStore>>,
) {
    let store = Arc::new(
        SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    );
    store.initialize_workspace_schema().await.unwrap();
    let workspace_id = WorkspaceId::new();
    let workspace = store
        .create_volume_root(CreateVolumeRoot {
            volume_format: "workspace-v1".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::new_v4(),
            workspace_id,
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: None,
        })
        .await
        .unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id,
            lease_id: LeaseId::new(),
            holder_generation: 1,
            ttl_ns: 60_000_000_000,
        })
        .await
        .unwrap();
    let meta = Arc::new(WorkspaceMetaLayer::new(
        store,
        ViewContext {
            workspace_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        },
    ));
    let fs = VFS::from_workspace_components(
        VFSConfig::new(ChunkLayout::default()),
        Arc::new(InMemoryBlockStore::new()),
        meta.clone(),
    )
    .unwrap();
    (fs, meta)
}

enum CloseMode {
    Consume,
    Drop,
    Concurrent,
}

async fn failed_close_retires_overlay_handle(timestamp_failure: bool, mode: CloseMode) {
    let (fs, meta) = fixture().await;
    let ino = fs.create_file("/failed-close").await.unwrap();
    let guard = fs
        .open_guard(ino, fs.stat("/failed-close").await.unwrap(), true, true)
        .await
        .unwrap();
    let fh = guard.fh();
    guard.write(0, b"original").await.unwrap();
    fs.flush(fh).await.unwrap();
    assert_eq!(guard.read(0, 8).await.unwrap(), b"original");
    guard.write(0, b"modified").await.unwrap();
    if timestamp_failure {
        // Finish the data flush while preserving the handle's pending timestamp
        // publication, so the injected error occurs after a successful flush.
        fs.state.writer.flush_required(ino as u64).await.unwrap();
    }
    meta.store().set_failpoint(StoreFailpoint::BeforeCommit);
    let weak_handle = Arc::downgrade(&fs.file_handle(fh).unwrap());

    if matches!(mode, CloseMode::Drop) {
        drop(guard);
        tokio::time::timeout(Duration::from_secs(10), async {
            while weak_handle.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("best-effort close must retire the handle after an I/O error");
    } else {
        let result = if matches!(mode, CloseMode::Concurrent) {
            // Both closes must acquire the same handle before either retires it.
            let handle = fs.file_handle(fh).unwrap();
            let lock = handle.lock_write().await;
            let closing = guard.close();
            let duplicate = fs.close(fh);
            tokio::pin!(closing, duplicate);
            assert!(futures::poll!(&mut closing).is_pending());
            assert!(futures::poll!(&mut duplicate).is_pending());
            drop(lock);
            let (result, duplicate) = tokio::join!(closing, duplicate);
            assert!(matches!(duplicate, Err(VfsError::StaleNetworkFileHandle)));
            result
        } else {
            guard.close().await
        };
        let error = result.expect_err("injected close failure");
        assert!(error.to_string().contains("injected failure"), "{error}");
        eprintln!("timestamp_failure={timestamp_failure}: {error}");
        let read = fs.read(fh, 0, 1).await;
        assert!(
            matches!(read, Err(VfsError::StaleNetworkFileHandle)),
            "consumed handle is still readable: {read:?}"
        );
        assert!(
            weak_handle.upgrade().is_none(),
            "handle allocation must be released"
        );
    }
    assert!(fs.handles_for(ino).is_empty());
    assert!(!fs.state.inodes.contains_key(&ino));
    if !timestamp_failure {
        assert!(fs.state.writer.has_file(ino as u64));
        assert!(
            fs.state.writer.has_dirty_state(ino as u64).await,
            "failed dirty data must remain owned by the writer pool"
        );
    }

    meta.store().set_failpoint(StoreFailpoint::Disabled);
    // This public operation leaves an unlinked inode alive if record_close was
    // skipped. Also prove a repeated close cannot consume another open's count.
    let survivor = fs
        .open_guard(ino, fs.stat("/failed-close").await.unwrap(), true, false)
        .await
        .unwrap();
    assert!(matches!(
        fs.close(fh).await,
        Err(VfsError::StaleNetworkFileHandle)
    ));
    fs.unlink("/failed-close").await.unwrap();
    assert!(meta.stat(ino).await.unwrap().is_some());
    survivor.close().await.unwrap();
    assert!(
        meta.stat(ino).await.unwrap().is_none(),
        "metadata open/close accounting must balance"
    );
}

#[tokio::test]
async fn consuming_close_retires_overlay_handle_after_flush_failure() {
    failed_close_retires_overlay_handle(false, CloseMode::Consume).await;
}

#[tokio::test]
async fn consuming_close_retires_overlay_handle_after_timestamp_failure() {
    failed_close_retires_overlay_handle(true, CloseMode::Consume).await;
}

#[tokio::test]
async fn dropped_guard_retires_overlay_handle_after_timestamp_failure() {
    failed_close_retires_overlay_handle(true, CloseMode::Drop).await;
}

#[tokio::test]
async fn concurrent_close_retires_overlay_handle_exactly_once_after_timestamp_failure() {
    failed_close_retires_overlay_handle(true, CloseMode::Concurrent).await;
}
