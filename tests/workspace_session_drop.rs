#![cfg(feature = "workspace-overlay")]

use std::sync::Arc;
use std::time::Duration;

use brewfs::workspace_overlay::catalog::{CreateVolumeRoot, WorkspaceStore};
use brewfs::workspace_overlay::ids::{LayerId, WorkspaceId};
use brewfs::workspace_overlay::lifecycle::WorkspaceMountSession;
use brewfs::workspace_overlay::model::WORKSPACE_SCHEMA_VERSION;
use brewfs::workspace_overlay::stores::database::SqliteWorkspaceStore;
use uuid::Uuid;

async fn session() -> (
    Arc<SqliteWorkspaceStore>,
    WorkspaceMountSession<SqliteWorkspaceStore>,
) {
    let store = Arc::new(
        SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap(),
    );
    store.initialize_workspace_schema().await.unwrap();
    let workspace_id = WorkspaceId::new();
    store
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
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
    let session = WorkspaceMountSession::acquire(
        store.clone(),
        workspace_id,
        1,
        Duration::from_millis(300),
        Duration::from_millis(50),
    )
    .await
    .unwrap();
    (store, session)
}

#[tokio::test]
async fn dropped_mount_session_stops_renewing_the_writer_lease() {
    let (store, session) = session().await;
    let workspace = session.view.workspace_id;
    let validity = session.lease_validity();
    drop(session);
    // Three TTLs allow the last in-flight renewal to expire; the runtime
    // remains alive so a detached renewal loop cannot hide at test teardown.
    tokio::time::sleep(Duration::from_millis(900)).await;
    let replacement = WorkspaceMountSession::acquire(
        store,
        workspace,
        2,
        Duration::from_secs(30),
        Duration::from_secs(10),
    )
    .await;
    assert!(
        replacement.is_ok(),
        "dropped mount kept renewing its exclusive writer lease: {:?}",
        replacement.as_ref().err()
    );
    assert!(
        !validity.authoritative(),
        "dropped session retained a usable local lease proof"
    );
    replacement.unwrap().release().await.unwrap();
}

#[tokio::test]
async fn explicit_mount_release_allows_an_immediate_replacement() {
    let (store, session) = session().await;
    let workspace = session.view.workspace_id;
    session.release().await.unwrap();
    let replacement = WorkspaceMountSession::acquire(
        store,
        workspace,
        2,
        Duration::from_secs(30),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    replacement.release().await.unwrap();
}
