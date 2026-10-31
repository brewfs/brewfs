use std::sync::Arc;

use uuid::Uuid;

use crate::meta::MetaLayer;
use crate::workspace_overlay::catalog::{CreateVolumeRoot, WorkspaceStore};
use crate::workspace_overlay::ids::{LayerId, WorkspaceId};
use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
use crate::workspace_overlay::model::{LayerState, WorkspaceState};
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
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: crate::workspace_overlay::model::WORKSPACE_SCHEMA_VERSION,
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
async fn fork_from_revision_and_discard_never_touch_the_mounted_workspace() {
    let (store, session) = setup().await;
    let lifecycle = WorkspaceLifecycle::new(store.clone());
    let meta = WorkspaceMetaLayer::new(store.clone(), session.view.clone());
    meta.create_file(meta.root_ino(), "mounted-change".into())
        .await
        .unwrap();
    let before = store
        .load_workspace(session.view.workspace_id)
        .await
        .unwrap();
    let fork_base = before.fork_base.clone().unwrap();

    let children = lifecycle
        .fork_revision(fork_base.clone(), 2, Some("agent".into()))
        .await
        .unwrap();
    assert_eq!(children.len(), 2);
    for child in &children {
        assert_eq!(child.fork_base.as_ref(), Some(&fork_base));
        let pair = store.load_layer_chain(child.head_layer_id).await.unwrap();
        assert_eq!(pair.len(), 2);
        assert_eq!(pair[0].state, LayerState::Writable);
        assert_eq!(pair[1].layer_id, fork_base.layer_id);
    }
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

    // The live workspace's head never moves; its mount keeps committing.
    let after = store
        .load_workspace(session.view.workspace_id)
        .await
        .unwrap();
    assert_eq!(after.head_layer_id, before.head_layer_id);
    assert_eq!(after.head_epoch, before.head_epoch);
    meta.create_file(meta.root_ino(), "still-mounted".into())
        .await
        .unwrap();
    session.release().await.unwrap();
}

#[tokio::test]
async fn session_lease_validity_follows_real_renewals_and_lease_loss() {
    let store = Arc::new(
        SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    );
    store.initialize_workspace_schema().await.unwrap();
    let workspace_id = WorkspaceId::from_uuid(Uuid::from_u128(601));
    store
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: crate::workspace_overlay::model::WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::from_u128(602),
            workspace_id,
            root_layer_id: LayerId::from_uuid(Uuid::from_u128(603)),
            writable_layer_id: LayerId::from_uuid(Uuid::from_u128(604)),
            owner_id: None,
        })
        .await
        .unwrap();
    // TTL 300 ms, renewed every 50 ms: the proof must outlive the initial
    // TTL only because real renewals extend it.
    let session = WorkspaceMountSession::acquire(
        store.clone(),
        workspace_id,
        1,
        Duration::from_millis(300),
        Duration::from_millis(50),
    )
    .await
    .unwrap();
    let validity = session.lease_validity();
    assert!(validity.authoritative());
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(validity.authoritative(), "renewals keep the proof alive");
    // Lose the lease underneath the session: the next renewal fails and the
    // proof is revoked permanently.
    store
        .release_lease(crate::workspace_overlay::catalog::ReleaseLease {
            lease_id: session.view.lease_id,
            holder_generation: session.view.holder_generation,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !validity.authoritative(),
        "a failed renewal revokes the proof"
    );
}
