use std::sync::Arc;

use uuid::Uuid;

use crate::meta::MetaLayer;
use crate::workspace_overlay::catalog::{CreateVolumeRoot, WorkspaceStore};
use crate::workspace_overlay::ids::{LayerId, WorkspaceId};
use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
use crate::workspace_overlay::model::{LayerState, ViewContext, WorkspaceState};
use crate::workspace_overlay::stores::database::SqliteWorkspaceStore;

use super::*;

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
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(501));
    store
        .create_volume_root(CreateVolumeRoot {
            volume_id: Uuid::from_u128(502),
            workspace_id,
            root_layer_id: LayerId::from_uuid(Uuid::from_u128(503)),
            writable_layer_id: LayerId::from_uuid(Uuid::from_u128(504)),
            owner_id: None,
        })
        .await
        .unwrap();
    let session = WorkspaceMountSession::acquire(
        store.clone(),
        workspace_id,
        1,
        DEFAULT_LEASE_TTL,
        Duration::from_secs(3600),
    )
    .await
    .unwrap();
    (store, session)
}

#[tokio::test]
async fn seal_installs_a_flat_base_and_fences_the_old_view() {
    let (store, session) = setup().await;
    let meta = WorkspaceMetaLayer::new(store.clone(), session.view.clone());
    meta.create_file(meta.root_ino(), "before-seal".into())
        .await
        .unwrap();
    let lifecycle = WorkspaceLifecycle::new(store.clone());
    let sealed = lifecycle
        .seal(&session.view, &NoopDurableRemoteBarrier)
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
    assert_eq!(
        store
            .load_layer(sealed.new_head_layer_id)
            .await
            .unwrap()
            .state,
        LayerState::Writable
    );
    let pair = store
        .load_layer_chain(sealed.new_head_layer_id)
        .await
        .unwrap();
    assert_eq!(pair.len(), 2);
    assert_eq!(pair[0].state, LayerState::Writable);
    assert_eq!(pair[0].depth, 2);
    assert_eq!(pair[1].state, LayerState::Sealed);
    assert_eq!(pair[1].depth, 1);
    assert!(pair[1].parent_layer_id.is_none());
    assert!(
        meta.create_file(meta.root_ino(), "stale".into())
            .await
            .is_err()
    );
    let current_view = ViewContext {
        head_layer_id: sealed.new_head_layer_id,
        head_epoch: sealed.head_epoch,
        ..session.view.clone()
    };
    meta.replace_view_context(current_view.clone()).await;
    meta.create_file(meta.root_ino(), "after-seal".into())
        .await
        .unwrap();
    let resealed = lifecycle
        .seal(&current_view, &NoopDurableRemoteBarrier)
        .await
        .unwrap();
    let resealed_pair = store
        .load_layer_chain(resealed.new_head_layer_id)
        .await
        .unwrap();
    assert_eq!(resealed_pair.len(), 2);
    assert_eq!(resealed_pair[0].depth, 2);
    assert_eq!(resealed_pair[1].depth, 1);
    assert!(resealed_pair[1].parent_layer_id.is_none());
    session.release().await.unwrap();
}

#[tokio::test]
async fn fork_snapshot_discard_and_fast_forward_are_revision_checked() {
    let (store, session) = setup().await;
    let lifecycle = WorkspaceLifecycle::new(store.clone());
    let source_meta = WorkspaceMetaLayer::new(store.clone(), session.view.clone());
    source_meta
        .create_file(source_meta.root_ino(), "source-change".into())
        .await
        .unwrap();
    let sealed = lifecycle
        .seal(&session.view, &NoopDurableRemoteBarrier)
        .await
        .unwrap();
    let fork_base = store
        .load_workspace(session.view.workspace_id)
        .await
        .unwrap()
        .fork_base
        .unwrap();
    let snapshot = lifecycle
        .snapshot_revision(sealed.revision.clone(), Some("release".into()), None)
        .await
        .unwrap();
    assert_eq!(
        store.load_snapshot(snapshot.snapshot_id).await.unwrap(),
        snapshot
    );

    let children = lifecycle
        .fork_revision(fork_base.clone(), 2, Some("agent".into()))
        .await
        .unwrap();
    let target = children[0].clone();
    let committed = lifecycle
        .fast_forward(sealed.revision.clone(), fork_base, &target)
        .await
        .unwrap();
    assert_eq!(committed.revision, sealed.revision);
    let stale = lifecycle
        .fast_forward(
            committed.revision.clone(),
            target.fork_base.clone().unwrap(),
            &target,
        )
        .await;
    assert!(stale.is_err());

    lifecycle
        .discard(children[1].workspace_id, false)
        .await
        .unwrap();
    assert_eq!(
        store
            .load_workspace(children[1].workspace_id)
            .await
            .unwrap()
            .state,
        WorkspaceState::Deleting
    );
    session.release().await.unwrap();
}

#[tokio::test]
async fn recovery_aborts_pre_hash_journal_to_the_old_view() {
    let (store, session) = setup().await;
    store
        .begin_seal(BeginSeal {
            guard: HeadGuard {
                workspace_id: session.view.workspace_id,
                expected_head_layer_id: session.view.head_layer_id,
                expected_head_epoch: session.view.head_epoch,
                lease_id: session.view.lease_id,
                holder_generation: session.view.holder_generation,
            },
            journal_id: JournalId::new(),
            new_head_layer_id: LayerId::new(),
        })
        .await
        .unwrap();
    let lifecycle = WorkspaceLifecycle::new(store.clone());
    assert!(
        lifecycle
            .recover_incomplete_seals()
            .await
            .unwrap()
            .is_empty()
    );
    let workspace = store
        .load_workspace(session.view.workspace_id)
        .await
        .unwrap();
    assert_eq!(workspace.state, WorkspaceState::Active);
    assert_eq!(workspace.head_layer_id, session.view.head_layer_id);
    session.release().await.unwrap();
}

#[tokio::test]
async fn recovery_completes_data_drained_and_hashed_journals_exactly_once() {
    for stop_phase in [SealPhase::DataDrained, SealPhase::Hashed] {
        let (store, session) = setup().await;
        let journal_id = JournalId::new();
        store
            .begin_seal(BeginSeal {
                guard: HeadGuard {
                    workspace_id: session.view.workspace_id,
                    expected_head_layer_id: session.view.head_layer_id,
                    expected_head_epoch: session.view.head_epoch,
                    lease_id: session.view.lease_id,
                    holder_generation: session.view.holder_generation,
                },
                journal_id,
                new_head_layer_id: LayerId::new(),
            })
            .await
            .unwrap();
        store
            .advance_seal(AdvanceSeal {
                journal_id,
                expected_phase: SealPhase::Prepare,
                next_phase: SealPhase::Quiesced,
                pending_bytes: None,
                last_error: None,
            })
            .await
            .unwrap();
        store
            .advance_seal(AdvanceSeal {
                journal_id,
                expected_phase: SealPhase::Quiesced,
                next_phase: SealPhase::DataDrained,
                pending_bytes: Some(0),
                last_error: None,
            })
            .await
            .unwrap();
        if stop_phase == SealPhase::Hashed {
            store.hash_seal(journal_id).await.unwrap();
        }

        let completed = WorkspaceLifecycle::new(store.clone())
            .recover_incomplete_seals()
            .await
            .unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(
            store.load_seal_journal(journal_id).await.unwrap().phase,
            SealPhase::Completed
        );
        assert!(
            WorkspaceLifecycle::new(store.clone())
                .recover_incomplete_seals()
                .await
                .unwrap()
                .is_empty()
        );
        session.release().await.unwrap();
    }
}

struct FailingBarrier;

#[async_trait::async_trait]
impl DurableRemoteBarrier for FailingBarrier {
    async fn drain(
        &self,
        _workspace_id: WorkspaceId,
        _head_epoch: u64,
    ) -> Result<(), WorkspaceError> {
        Err(WorkspaceError::Backend("injected drain failure".into()))
    }
}

#[tokio::test]
async fn failed_durable_barrier_restores_active_old_view() {
    let (store, session) = setup().await;
    let old = store
        .load_workspace(session.view.workspace_id)
        .await
        .unwrap();
    let error = WorkspaceLifecycle::new(store.clone())
        .seal(&session.view, &FailingBarrier)
        .await
        .unwrap_err();
    assert!(matches!(error, WorkspaceError::Backend(_)));
    let restored = store
        .load_workspace(session.view.workspace_id)
        .await
        .unwrap();
    assert_eq!(restored.state, WorkspaceState::Active);
    assert_eq!(restored.head_layer_id, old.head_layer_id);
    assert_eq!(restored.head_epoch, old.head_epoch);
    session.release().await.unwrap();
}
