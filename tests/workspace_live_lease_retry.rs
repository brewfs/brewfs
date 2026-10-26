//! Delayed live-file mutations must refresh authority, never poison writeback.
#![cfg(feature = "workspace-overlay")]

use brewfs::workspace_overlay::catalog::*;
use brewfs::workspace_overlay::error::WorkspaceError;
use brewfs::workspace_overlay::ids::{LayerId, LeaseId, WorkspaceId};
use brewfs::workspace_overlay::model::*;
use brewfs::workspace_overlay::packed_v3::wire005::V3MountBudget;
use brewfs::workspace_overlay::stores::kv_store::KvWorkspaceStore;
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;
use uuid::Uuid;
#[path = "support/lease_backend.rs"]
mod lease_backend;
use brewfs::chunk::{ChunkLayout, SliceDesc, store::InMemoryBlockStore};
use brewfs::meta::MetaLayer;
use brewfs::meta::store::{SetAttrFlags, SetAttrRequest};
use brewfs::workspace_overlay::{gc::WorkspaceGc, meta_layer::WorkspaceMetaLayer};
use lease_backend::MemoryBackend;

struct Fixture {
    store: Arc<KvWorkspaceStore<MemoryBackend>>,
    backend: MemoryBackend,
    workspace: WorkspaceRecord,
    lease: SnapshotLease,
    meta: Arc<WorkspaceMetaLayer<KvWorkspaceStore<MemoryBackend>>>,
    gc: WorkspaceGc<KvWorkspaceStore<MemoryBackend>, InMemoryBlockStore>,
    ino: i64,
}

async fn fixture() -> Fixture {
    let backend = MemoryBackend::default();
    backend.clock.store(1_000_000_000, Ordering::SeqCst);
    let store = Arc::new(
        KvWorkspaceStore::new(backend.clone())
            .with_packed_reader_pin_budget(V3MountBudget::defaults()),
    );
    store.initialize_workspace_schema().await.unwrap();
    let workspace = store
        .create_volume_root(CreateVolumeRoot {
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
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace.workspace_id,
            lease_id: LeaseId::new(),
            holder_generation: 1,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap();
    let meta = Arc::new(WorkspaceMetaLayer::new(
        store.clone(),
        ViewContext {
            workspace_id: workspace.workspace_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        },
    ));
    let blocks = Arc::new(InMemoryBlockStore::new());
    let layout = ChunkLayout::default();
    let ino = meta
        .create_file(meta.root_ino(), "live".into())
        .await
        .unwrap();
    let gc = WorkspaceGc::new(
        store.clone(),
        blocks,
        layout,
        Duration::ZERO,
        Duration::ZERO,
    );
    Fixture {
        store,
        backend,
        workspace,
        lease,
        meta,
        gc,
        ino,
    }
}

async fn arrived(f: &Fixture) {
    tokio::time::timeout(
        Duration::from_secs(5),
        f.backend.mutation_arrived.notified(),
    )
    .await
    .expect("mutation reached its actual metadata CAS");
}

async fn renew(f: &Fixture) -> Result<SnapshotLease, WorkspaceError> {
    f.store
        .renew_lease(RenewLease {
            lease_id: f.lease.lease_id,
            holder_generation: f.lease.holder_generation,
            ttl_ns: 30_000_000_000,
        })
        .await
}

// This is the metadata entry point called by writer::commit_chunk after upload.
async fn commit_chunk_metadata(f: &Fixture) -> Result<(), brewfs::meta::store::MetaError> {
    let chunk_id = brewfs::vfs::chunk_id_for(f.ino, 0).unwrap();
    f.meta
        .write(
            f.ino,
            chunk_id,
            SliceDesc {
                slice_id: 1,
                chunk_id,
                offset: 0,
                length: 10,
            },
            10,
        )
        .await
}

async fn delayed_mutation(chunk_first: bool, renewal_busy: bool) {
    let f = fixture().await;
    // Pause the metadata transaction while a concurrent operation queues on
    // the same mutation gate used by the mounted writer and setattr paths.
    f.backend.pause_mutation.store(true, Ordering::SeqCst);
    // Exercise both permitted CAS orders. In particular, the renewal-conflict
    // case uses Redis's value-first order and does not depend on stale-packet
    // deadline precedence to reproduce the lost live-file authority.
    f.backend
        .deadline_first
        .store(!renewal_busy, Ordering::SeqCst);
    let request = SetAttrRequest {
        mtime: Some(123),
        ..Default::default()
    };
    let setattr = f.meta.set_attr(f.ino, &request, SetAttrFlags::empty());
    let flush = commit_chunk_metadata(&f);
    tokio::pin!(setattr, flush);
    // Poll only the selected leader until it reaches the backend pause.
    if chunk_first {
        tokio::select! {
            result = &mut flush => panic!("flush completed before pause: {result:?}"),
            _ = arrived(&f) => {}
        }
        assert!(futures::poll!(&mut setattr).is_pending());
    } else {
        tokio::select! {
            result = &mut setattr => panic!("setattr completed before pause: {result:?}"),
            _ = arrived(&f) => {}
        }
        assert!(futures::poll!(&mut flush).is_pending());
    }
    // Advance backend time, not wall time: renewal is timely, but the stalled
    // request's original 30-second packet expires while it holds the gate.
    f.backend.clock.store(21_000_000_000, Ordering::SeqCst);
    f.backend
        .fail_bounded_read_busy_once
        .store(renewal_busy, Ordering::SeqCst);
    let renewed = renew(&f).await;
    f.backend.clock.store(40_000_000_000, Ordering::SeqCst);
    let gc = f.gc.run_at(40_000_000_000).await.unwrap();
    assert!(!gc.deleted_layers.contains(&f.workspace.head_layer_id));
    f.backend.mutation_continue.notify_one();
    let (attr, committed) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(setattr, flush)
    })
    .await
    .expect("live mutations must complete");
    // ESTALE from this entry point is terminal to the writer and becomes EIO.
    assert!(
        attr.is_ok() && committed.is_ok(),
        "live inode: setattr={attr:?}, commit_chunk={committed:?}, renewal={renewed:?}"
    );
    assert!(renewed.is_ok());
    assert_eq!(
        f.meta.lookup(f.meta.root_ino(), "live").await.unwrap(),
        Some(f.ino)
    );
    assert_eq!(f.meta.stat(f.ino).await.unwrap().unwrap().size, 10);
    let extents = f
        .store
        .get_extent_deltas(ExtentQuery {
            layer_ids: vec![f.workspace.head_layer_id],
            ino: f.ino,
            chunk_index: 0,
            range_start: 0,
            range_end: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        extents.len(),
        1,
        "refresh must publish the extent exactly once"
    );
    assert_eq!(extents[0].length, 10);
    assert_eq!(
        f.store.list_leases(f.workspace.workspace_id).await.unwrap()[0].state,
        LeaseState::Active
    );
}

#[tokio::test]
async fn delayed_setattr_refreshes_renewed_lease_with_chunk_commit_and_gc() {
    delayed_mutation(false, false).await;
}

#[tokio::test]
async fn delayed_commit_chunk_refreshes_renewed_lease_with_setattr_and_gc() {
    delayed_mutation(true, false).await;
}

#[tokio::test]
async fn renewal_read_conflict_does_not_strand_live_setattr_and_commit_chunk() {
    delayed_mutation(true, true).await;
}

#[tokio::test]
async fn delayed_commit_cannot_refresh_a_lost_lease() {
    for loss in ["released", "replaced", "expired"] {
        let f = fixture().await;
        f.backend.pause_mutation.store(true, Ordering::SeqCst);
        let flush = commit_chunk_metadata(&f);
        tokio::pin!(flush);
        tokio::select! {
            result = &mut flush => panic!("unexpected early flush: {result:?}"),
            _ = arrived(&f) => {}
        }
        if loss == "expired" {
            f.backend.clock.store(40_000_000_000, Ordering::SeqCst);
        } else {
            f.store
                .release_lease(ReleaseLease {
                    lease_id: f.lease.lease_id,
                    holder_generation: f.lease.holder_generation,
                })
                .await
                .unwrap();
        }
        if loss == "replaced" {
            f.store
                .acquire_lease(AcquireLease {
                    workspace_id: f.workspace.workspace_id,
                    lease_id: LeaseId::new(),
                    holder_generation: 2,
                    ttl_ns: 30_000_000_000,
                })
                .await
                .unwrap();
        }
        f.backend.mutation_continue.notify_one();
        let error = tokio::time::timeout(Duration::from_secs(5), flush)
            .await
            .unwrap()
            .expect_err("a lost lease must stay fenced");
        assert!(
            matches!(&error, brewfs::meta::store::MetaError::Io(error)
                if error.kind() == std::io::ErrorKind::StaleNetworkFileHandle),
            "{loss} owner must receive the ownership fence: {error:?}"
        );
        assert!(
            f.store
                .get_extent_deltas(ExtentQuery {
                    layer_ids: vec![f.workspace.head_layer_id],
                    ino: f.ino,
                    chunk_index: 0,
                    range_start: 0,
                    range_end: ChunkLayout::default().chunk_size,
                })
                .await
                .unwrap()
                .is_empty(),
            "old owner must publish no extent"
        );
    }
}
