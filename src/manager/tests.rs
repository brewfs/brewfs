use super::*;
use crate::workspace_overlay::catalog::{AcquireLease, WorkspaceStore};
use crate::workspace_overlay::ids::LeaseId;

async fn fixture() -> (tempfile::TempDir, Manager) {
    let dir = tempfile::tempdir().unwrap();
    let config = serde_json::json!({"catalogs": {
        "local": {"backend":"sqlite", "path":dir.path().join("catalog.sqlite")}
    }});
    std::fs::write(dir.path().join("manager-catalogs.json"), config.to_string()).unwrap();
    let manager = Manager::open(dir.path()).await.unwrap();
    (dir, manager)
}

fn initialize() -> VolumeRequest {
    VolumeRequest {
        mode: VolumeMode::Initialize,
        catalog_ref: "local".into(),
        owner: Some("alice".into()),
        expected_volume_id: None,
        confirm_empty: true,
        volume_format: "workspace-v1".into(),
    }
}

async fn completed(manager: &Manager, operation: Operation) -> Operation {
    manager.execute(operation.id).await.unwrap();
    manager.operation(operation.id).await.unwrap()
}

async fn initialized(manager: &Manager) -> VolumeView {
    let op = completed(
        manager,
        manager
            .create_volume("initialize", initialize())
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(op.state, OperationState::Succeeded, "{op:?}");
    manager.inspect_volume(op.volume_id).await.unwrap()
}

#[tokio::test]
async fn initialize_creates_real_catalog_and_retries_without_duplicate_resources() {
    let (_dir, manager) = fixture().await;
    let volume = initialized(&manager).await;
    let store = manager.catalog("local", false).await.unwrap();
    assert_eq!(
        store.load_volume_header().await.unwrap().unwrap().volume_id,
        volume.id
    );
    assert_eq!(store.list_workspaces().await.unwrap().len(), 1);
    let first = manager
        .create_volume("initialize", initialize())
        .await
        .unwrap();
    let again = manager
        .create_volume("initialize", initialize())
        .await
        .unwrap();
    assert_eq!(first.id, again.id);
    assert_eq!(again.volume_id, volume.id);
    let mut changed = initialize();
    changed.owner = Some("bob".into());
    assert_eq!(
        manager
            .create_volume("initialize", changed)
            .await
            .unwrap_err()
            .code,
        "idempotency_conflict"
    );
}

#[tokio::test]
async fn initialization_refuses_existing_data_and_registration_requires_real_header() {
    let (dir, manager) = fixture().await;
    std::fs::write(
        dir.path().join("catalog.sqlite"),
        b"existing non-catalog data",
    )
    .unwrap();
    let op = completed(
        &manager,
        manager.create_volume("init", initialize()).await.unwrap(),
    )
    .await;
    assert_ne!(op.state, OperationState::Succeeded);
    assert_eq!(
        std::fs::read(dir.path().join("catalog.sqlite")).unwrap(),
        b"existing non-catalog data"
    );
    assert!(manager.list_volumes().await.unwrap().is_empty());
}

#[tokio::test]
async fn exact_fork_and_batch_recovery_keep_preallocated_ids_after_lost_reply() {
    let (dir, manager) = fixture().await;
    let volume = initialized(&manager).await;
    let revision = volume.root_revision.unwrap();
    manager
        .inner
        .fail_after_effect
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let request = ForkRequest {
        revision: revision.clone(),
        count: 3,
        owner: Some("bob".into()),
    };
    let op = manager
        .submit_fork(volume.id, "fork", request.clone(), false)
        .await
        .unwrap();
    manager.execute(op.id).await.unwrap();
    let partial = manager.operation(op.id).await.unwrap();
    assert_eq!(partial.state, OperationState::RecoveryRequired);
    assert_eq!(partial.items[0].state, ItemState::Succeeded);
    assert_eq!(manager.list_workspaces(volume.id).await.unwrap().len(), 3);
    let targets: Vec<_> = partial.items.iter().map(|i| i.workspace_id).collect();
    manager.inner.pool.close().await;
    drop(manager);
    let reopened = Manager::open(dir.path()).await.unwrap();
    let recovered = reopened.operation(op.id).await.unwrap();
    assert_eq!(recovered.state, OperationState::Succeeded, "{recovered:?}");
    assert_eq!(
        recovered
            .items
            .iter()
            .map(|i| i.workspace_id)
            .collect::<Vec<_>>(),
        targets
    );
    assert_eq!(reopened.list_workspaces(volume.id).await.unwrap().len(), 4);
    assert_eq!(
        reopened.fork(volume.id, "fork", request).await.unwrap().id,
        op.id
    );
    for id in targets.into_iter().flatten() {
        let view = reopened.inspect_workspace(volume.id, id).await.unwrap();
        assert_eq!(view.base_revision, revision);
        assert_eq!(view.record.owner_id.as_deref(), Some("bob"));
        assert_ne!(view.record.head_layer_id, revision.layer_id);
    }
}

#[tokio::test]
async fn lease_blocks_delete_and_success_only_marks_metadata() {
    let (_dir, manager) = fixture().await;
    let volume = initialized(&manager).await;
    let id = volume.root_workspace_id.unwrap();
    let store = manager.catalog("local", false).await.unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: id,
            lease_id: LeaseId::new(),
            holder_generation: 1,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap();
    let op = completed(
        &manager,
        manager
            .delete_workspace(volume.id, id, "delete")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(op.state, OperationState::Failed);
    assert_eq!(op.error.as_ref().unwrap().code, "conflict");
    assert_eq!(
        store.load_workspace(id).await.unwrap().state,
        WorkspaceState::Active
    );
    assert!(!op.items[0].physical_reclaimed);
    store
        .release_lease(crate::workspace_overlay::catalog::ReleaseLease {
            lease_id: lease.lease_id,
            holder_generation: 1,
        })
        .await
        .unwrap();
    let retried = completed(
        &manager,
        manager
            .delete_workspace(volume.id, id, "delete")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(retried.id, op.id);
    assert_eq!(retried.state, OperationState::Succeeded);
    assert!(retried.items[0].deletion_marked);
}

#[tokio::test]
async fn singleton_lock_scope_validation_and_unknown_request_fields_fail_closed() {
    let (dir, manager) = fixture().await;
    assert_eq!(
        Manager::open(dir.path()).await.err().unwrap().code,
        "unavailable"
    );
    assert_eq!(
        manager
            .inspect_volume(Uuid::new_v4())
            .await
            .unwrap_err()
            .code,
        "not_found"
    );
    let mut request = initialize();
    request.catalog_ref = "../secret".into();
    assert_eq!(
        manager
            .create_volume("bad", request)
            .await
            .unwrap_err()
            .code,
        "invalid_request"
    );
    assert!(
        serde_json::from_value::<VolumeRequest>(
            serde_json::json!({"mode":"initialize","catalog_ref":"local","force":true})
        )
        .is_err()
    );
}

#[tokio::test]
async fn register_validates_existing_catalog_identity_without_initializing() {
    let (dir, manager) = fixture().await;
    let store = manager.catalog("local", true).await.unwrap();
    store.initialize_workspace_schema().await.unwrap();
    let volume = Uuid::new_v4();
    store
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: volume,
            workspace_id: WorkspaceId::new(),
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: Some("external".into()),
        })
        .await
        .unwrap();
    let before = store.list_workspaces().await.unwrap();
    let request = VolumeRequest {
        mode: VolumeMode::Register,
        catalog_ref: "local".into(),
        owner: None,
        expected_volume_id: Some(volume),
        confirm_empty: false,
        volume_format: "workspace-v1".into(),
    };
    let op = completed(
        &manager,
        manager.create_volume("register", request).await.unwrap(),
    )
    .await;
    assert_eq!(op.state, OperationState::Succeeded, "{op:?}");
    assert_eq!(manager.inspect_volume(volume).await.unwrap().id, volume);
    assert_eq!(store.list_workspaces().await.unwrap(), before);
    assert!(dir.path().join("catalog.sqlite").exists());
}

#[tokio::test]
async fn initialize_rejects_an_already_initialized_catalog() {
    let (_dir, manager) = fixture().await;
    let store = manager.catalog("local", true).await.unwrap();
    store.initialize_workspace_schema().await.unwrap();
    let id = Uuid::new_v4();
    store
        .create_volume_root(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: id,
            workspace_id: WorkspaceId::new(),
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: None,
        })
        .await
        .unwrap();
    let before = store.list_workspaces().await.unwrap();
    let op = completed(
        &manager,
        manager.create_volume("refuse", initialize()).await.unwrap(),
    )
    .await;
    assert_eq!(op.state, OperationState::Failed);
    assert_eq!(
        store.load_volume_header().await.unwrap().unwrap().volume_id,
        id
    );
    assert_eq!(store.list_workspaces().await.unwrap(), before);
    let request = VolumeRequest {
        mode: VolumeMode::Register,
        catalog_ref: "local".into(),
        owner: None,
        expected_volume_id: Some(id),
        confirm_empty: false,
        volume_format: "workspace-v1".into(),
    };
    let registered = completed(
        &manager,
        manager
            .create_volume("correct-registration", request)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(registered.state, OperationState::Succeeded);
    // The original failed initializer must not acquire the registered scope.
    let retry = completed(
        &manager,
        manager.create_volume("refuse", initialize()).await.unwrap(),
    )
    .await;
    assert_eq!(retry.state, OperationState::Failed);
    assert_eq!(manager.inspect_volume(id).await.unwrap().id, id);
}

#[tokio::test]
async fn invalid_revision_cannot_fork_and_deleting_workspace_is_inspectable() {
    let (_dir, manager) = fixture().await;
    let volume = initialized(&manager).await;
    let mut revision = volume.root_revision.unwrap();
    revision.root_hash[0] ^= 1;
    let op = completed(
        &manager,
        manager
            .fork(
                volume.id,
                "wrong",
                ForkRequest {
                    revision,
                    count: 2,
                    owner: None,
                },
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(op.state, OperationState::Failed);
    assert_eq!(manager.list_workspaces(volume.id).await.unwrap().len(), 1);
    let id = volume.root_workspace_id.unwrap();
    let deleted = completed(
        &manager,
        manager
            .delete_workspace(volume.id, id, "delete")
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(deleted.state, OperationState::Succeeded);
    assert!(deleted.items[0].deletion_marked);
    assert!(!deleted.items[0].physical_reclaimed);
    assert_eq!(
        manager
            .inspect_workspace(volume.id, id)
            .await
            .unwrap()
            .record
            .state,
        WorkspaceState::Deleting
    );
    assert_eq!(
        manager
            .delete_workspace(volume.id, id, "delete")
            .await
            .unwrap()
            .id,
        deleted.id
    );
}

#[tokio::test]
async fn concurrent_normalized_retries_share_one_operation() {
    let (_dir, manager) = fixture().await;
    let mut padded = initialize();
    padded.owner = Some(" alice ".into());
    padded.catalog_ref = " local ".into();
    let (a, b) = tokio::join!(
        manager.create_volume("same", initialize()),
        manager.create_volume("same", padded)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.id, b.id);
    assert_eq!(
        completed(&manager, a).await.state,
        OperationState::Succeeded
    );
    assert_eq!(manager.list_volumes().await.unwrap().len(), 1);
}

#[tokio::test]
async fn registering_missing_file_does_not_create_a_catalog() {
    let (dir, manager) = fixture().await;
    let request = VolumeRequest {
        mode: VolumeMode::Register,
        catalog_ref: "local".into(),
        owner: None,
        expected_volume_id: Some(Uuid::new_v4()),
        confirm_empty: false,
        volume_format: "workspace-v1".into(),
    };
    let op = completed(
        &manager,
        manager.create_volume("missing", request).await.unwrap(),
    )
    .await;
    assert_eq!(op.state, OperationState::Failed);
    assert!(!dir.path().join("catalog.sqlite").exists());
}

#[tokio::test]
async fn partial_batch_failure_resumes_unfinished_items_under_the_original_ids() {
    let (_dir, manager) = fixture().await;
    let volume = initialized(&manager).await;
    let request = ForkRequest {
        revision: volume.root_revision.unwrap(),
        count: 3,
        owner: None,
    };
    manager
        .inner
        .fail_before_effect
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let op = manager
        .submit_fork(volume.id, "partial", request.clone(), false)
        .await
        .unwrap();
    manager.execute(op.id).await.unwrap();
    let failed = manager.operation(op.id).await.unwrap();
    assert_eq!(failed.state, OperationState::RecoveryRequired);
    assert_eq!(
        failed.items.iter().map(|i| i.state).collect::<Vec<_>>(),
        vec![
            ItemState::Succeeded,
            ItemState::RecoveryRequired,
            ItemState::Pending
        ]
    );
    assert_eq!(manager.list_workspaces(volume.id).await.unwrap().len(), 2);
    let retried = manager
        .submit_fork(volume.id, "partial", request, false)
        .await
        .unwrap();
    assert_eq!(retried.id, op.id);
    let finished = completed(&manager, retried).await;
    assert_eq!(finished.state, OperationState::Succeeded);
    assert_eq!(
        finished
            .items
            .iter()
            .map(|i| i.workspace_id)
            .collect::<Vec<_>>(),
        failed
            .items
            .iter()
            .map(|i| i.workspace_id)
            .collect::<Vec<_>>()
    );
    assert_eq!(manager.list_workspaces(volume.id).await.unwrap().len(), 4);
}

#[tokio::test]
async fn reopen_recovers_volume_commit_before_registration_or_success_checkpoint() {
    let (dir, manager) = fixture().await;
    let root = WorkspaceId::new();
    let lower = LayerId::new();
    let head = LayerId::new();
    let request = initialize();
    let mut record = Record {
        operation: Operation::new(
            Uuid::new_v4(),
            "volume",
            vec![OperationItem::new(Some(root), Some(head))],
        ),
        catalog_ref: "local".into(),
        fingerprint: manager.fingerprint("local").unwrap(),
        transport_fingerprint: None,
        claim_established: false,
        task: Task::Volume {
            request: request.clone(),
            root,
            lower,
            head,
            empty_checked: false,
        },
    };
    manager
        .prepare(
            "volumes",
            "crash",
            digest(&request).unwrap(),
            record.clone(),
            true,
        )
        .await
        .unwrap();
    if let Task::Volume { empty_checked, .. } = &mut record.task {
        *empty_checked = true;
    }
    manager.save(&record).await.unwrap();
    let store = manager.catalog("local", true).await.unwrap();
    store.initialize_workspace_schema().await.unwrap();
    store
        .create_volume_root_in_empty_namespace(CreateVolumeRoot {
            upper_bucket: None,
            volume_format: "workspace-v1".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: record.operation.volume_id,
            workspace_id: root,
            root_layer_id: lower,
            writable_layer_id: head,
            owner_id: request.owner,
        })
        .await
        .unwrap();
    assert!(manager.list_volumes().await.unwrap().is_empty());
    drop(store);
    manager.inner.pool.close().await;
    drop(manager);
    let reopened = Manager::open(dir.path()).await.unwrap();
    assert_eq!(
        reopened.operation(record.operation.id).await.unwrap().state,
        OperationState::Succeeded
    );
    assert_eq!(
        reopened
            .inspect_volume(record.operation.volume_id)
            .await
            .unwrap()
            .root_workspace_id,
        Some(root)
    );
    assert_eq!(
        reopened
            .list_workspaces(record.operation.volume_id)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn initialization_recovery_does_not_recreate_a_missing_original_catalog() {
    let (dir, manager) = fixture().await;
    let root = WorkspaceId::new();
    let lower = LayerId::new();
    let head = LayerId::new();
    let request = initialize();
    let mut record = Record {
        operation: Operation::new(
            Uuid::new_v4(),
            "initialize_volume",
            vec![OperationItem::new(Some(root), Some(head))],
        ),
        catalog_ref: "local".into(),
        fingerprint: manager.fingerprint("local").unwrap(),
        transport_fingerprint: None,
        claim_established: false,
        task: Task::Volume {
            request: request.clone(),
            root,
            lower,
            head,
            empty_checked: false,
        },
    };
    manager
        .prepare(
            "volumes",
            "missing-original",
            digest(&request).unwrap(),
            record.clone(),
            true,
        )
        .await
        .unwrap();
    let path = dir.path().join("catalog.sqlite");
    let pool = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true),
        )
        .await
        .unwrap();
    pool.close().await;
    if let Task::Volume { empty_checked, .. } = &mut record.task {
        *empty_checked = true;
    }
    manager.save(&record).await.unwrap();
    manager.inner.pool.close().await;
    drop(manager);
    let offline = dir.path().join("catalog.offline");
    std::fs::rename(&path, &offline).unwrap();

    let reopened = Manager::open(dir.path()).await.unwrap();
    let recovered = reopened.operation(record.operation.id).await.unwrap();
    assert_eq!(recovered.state, OperationState::RecoveryRequired);
    assert_eq!(recovered.error.unwrap().code, "recovery_required");
    assert!(
        !path.exists(),
        "recovery must not create a replacement catalog"
    );
    assert!(reopened.list_volumes().await.unwrap().is_empty());
    let retried = completed(
        &reopened,
        reopened
            .create_volume("missing-original", request.clone())
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(retried.id, record.operation.id);
    assert_eq!(retried.state, OperationState::RecoveryRequired);
    assert!(!path.exists());
    assert!(
        reopened
            .create_volume("other-init", request.clone())
            .await
            .is_err()
    );

    std::fs::rename(&offline, &path).unwrap();
    let restored = completed(
        &reopened,
        reopened
            .create_volume("missing-original", request)
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(restored.id, record.operation.id);
    assert_eq!(restored.state, OperationState::Succeeded);
    assert_eq!(restored.items[0].workspace_id, Some(root));
    assert_eq!(restored.items[0].head_layer_id, Some(head));
}

#[tokio::test]
async fn retry_returns_original_result_even_when_catalog_directory_is_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("catalogs")).unwrap();
    let config = serde_json::json!({"catalogs":{"local":{"backend":"sqlite","path":dir.path().join("catalogs/catalog.sqlite")}}});
    std::fs::write(dir.path().join("manager-catalogs.json"), config.to_string()).unwrap();
    let manager = Manager::open(dir.path()).await.unwrap();
    let volume = initialized(&manager).await;
    let request = ForkRequest {
        revision: volume.root_revision.unwrap(),
        count: 1,
        owner: None,
    };
    let fork = completed(
        &manager,
        manager
            .fork(volume.id, "fork", request.clone())
            .await
            .unwrap(),
    )
    .await;
    let workspace = fork.items[0].workspace_id.unwrap();
    let delete = completed(
        &manager,
        manager
            .delete_workspace(volume.id, workspace, "delete")
            .await
            .unwrap(),
    )
    .await;
    std::fs::rename(dir.path().join("catalogs"), dir.path().join("offline")).unwrap();
    assert_eq!(
        manager.list_volumes().await.unwrap_err().code,
        "recovery_required"
    );
    assert_eq!(
        manager
            .create_volume("initialize", initialize())
            .await
            .unwrap()
            .volume_id,
        volume.id
    );
    assert_eq!(
        manager
            .fork(volume.id, "fork", request.clone())
            .await
            .unwrap()
            .id,
        fork.id
    );
    assert_eq!(
        manager
            .delete_workspace(volume.id, workspace, "delete")
            .await
            .unwrap()
            .id,
        delete.id
    );
    let mut changed = request;
    changed.count = 2;
    assert_eq!(
        manager
            .fork(volume.id, "fork", changed)
            .await
            .unwrap_err()
            .code,
        "idempotency_conflict"
    );
}

#[tokio::test]
async fn registration_rejects_a_header_without_a_complete_catalog() {
    let (dir, manager) = fixture().await;
    let store = manager.catalog("local", true).await.unwrap();
    store.initialize_workspace_schema().await.unwrap();
    let volume = Uuid::new_v4();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(SqliteConnectOptions::new().filename(dir.path().join("catalog.sqlite")))
        .await
        .unwrap();
    sqlx::query("INSERT INTO ws_v1_volume_header VALUES(1,'workspace-v1',1,?,0)")
        .bind(volume.as_bytes().as_slice())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP TABLE ws_v1_layers")
        .execute(&pool)
        .await
        .unwrap();
    let request = VolumeRequest {
        mode: VolumeMode::Register,
        catalog_ref: "local".into(),
        owner: None,
        expected_volume_id: Some(volume),
        confirm_empty: false,
        volume_format: "workspace-v1".into(),
    };
    let op = completed(
        &manager,
        manager.create_volume("ghost", request).await.unwrap(),
    )
    .await;
    assert_eq!(op.state, OperationState::Failed);
    assert_eq!(op.error.unwrap().code, "unsupported");
    assert!(manager.list_volumes().await.unwrap().is_empty());
    let tables = store.schema_table_names().await.unwrap();
    assert!(
        !tables.iter().any(|name| name == "ws_v1_layers"),
        "register must not recreate missing schema"
    );
    assert_eq!(
        store.load_volume_header().await.unwrap().unwrap().volume_id,
        volume
    );
    pool.close().await;
}

#[tokio::test]
async fn accepted_retry_survives_restart_before_worker_starts() {
    let (dir, manager) = fixture().await;
    let volume = initialized(&manager).await;
    let workspace = volume.root_workspace_id.unwrap();
    let store = manager.catalog("local", false).await.unwrap();
    let lease = store
        .acquire_lease(AcquireLease {
            workspace_id: workspace,
            lease_id: LeaseId::new(),
            holder_generation: 1,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap();
    let binding = manager.binding(volume.id).await.unwrap();
    let record = Record {
        operation: Operation::new(
            volume.id,
            "delete_workspace",
            vec![OperationItem::new(Some(workspace), None)],
        ),
        catalog_ref: binding.view.catalog_ref,
        fingerprint: binding.fingerprint,
        transport_fingerprint: None,
        claim_established: false,
        task: Task::Delete { workspace },
    };
    let scope = format!("volumes/{}/workspaces/{workspace}/delete", volume.id);
    let request_digest = digest(&workspace).unwrap();
    manager
        .prepare(
            &scope,
            "retry",
            request_digest.clone(),
            record.clone(),
            false,
        )
        .await
        .unwrap();
    manager.execute(record.operation.id).await.unwrap();
    assert_eq!(
        manager.operation(record.operation.id).await.unwrap().state,
        OperationState::Failed
    );
    store
        .release_lease(crate::workspace_overlay::catalog::ReleaseLease {
            lease_id: lease.lease_id,
            holder_generation: 1,
        })
        .await
        .unwrap();
    // Admit an explicit retry without scheduling a worker, then reopen storage.
    let queued = manager
        .replay(&scope, "retry", &request_digest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(queued.id, record.operation.id);
    assert_eq!(queued.state, OperationState::Accepted);
    assert_eq!(
        manager.operation(queued.id).await.unwrap().state,
        OperationState::Accepted
    );
    drop(store);
    manager.inner.pool.close().await;
    drop(manager);
    let reopened = Manager::open(dir.path()).await.unwrap();
    assert_eq!(
        reopened.operation(queued.id).await.unwrap().state,
        OperationState::Succeeded
    );
    assert_eq!(
        reopened
            .inspect_workspace(volume.id, workspace)
            .await
            .unwrap()
            .record
            .state,
        WorkspaceState::Deleting
    );
}

#[tokio::test]
async fn registration_rejects_orphan_packed_history_without_native_fallback() {
    let (dir, manager) = fixture().await;
    let store = manager.catalog("local", true).await.unwrap();
    store.initialize_workspace_schema().await.unwrap();
    let volume = Uuid::new_v4();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(SqliteConnectOptions::new().filename(dir.path().join("catalog.sqlite")))
        .await
        .unwrap();
    sqlx::query("INSERT INTO ws_v1_volume_header VALUES(1,'workspace-v1',1,?,0)")
        .bind(volume.as_bytes().as_slice())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO ws_v3_packed_bindings(workspace_id,binding_version,record) VALUES(?,2,?)",
    )
    .bind(WorkspaceId::new().as_bytes().as_slice())
    .bind(vec![1u8])
    .execute(&pool)
    .await
    .unwrap();
    let request = VolumeRequest {
        mode: VolumeMode::Register,
        catalog_ref: "local".into(),
        owner: None,
        expected_volume_id: Some(volume),
        confirm_empty: false,
        volume_format: "workspace-v1".into(),
    };
    let op = completed(
        &manager,
        manager.create_volume("packed", request).await.unwrap(),
    )
    .await;
    assert_eq!(op.state, OperationState::Failed);
    assert_eq!(op.error.unwrap().code, "unsupported");
    assert!(manager.list_volumes().await.unwrap().is_empty());
    pool.close().await;
}

#[tokio::test]
async fn shutdown_stops_admission_and_recovers_queued_work_under_original_ids() {
    let (dir, manager) = fixture().await;
    let volume = initialized(&manager).await;
    let request = ForkRequest {
        revision: volume.root_revision.clone().unwrap(),
        count: 1,
        owner: Some("shutdown-recovery".into()),
    };
    let accepted = manager
        .submit_fork(volume.id, "queued-at-shutdown", request.clone(), false)
        .await
        .unwrap();
    let workspace = accepted.items[0].workspace_id.unwrap();
    let head = accepted.items[0].head_layer_id.unwrap();
    manager.schedule(accepted.id);
    tokio::time::timeout(std::time::Duration::from_secs(5), manager.shutdown())
        .await
        .expect("shutdown must wait for queued workers to release their handles")
        .unwrap();
    assert!(manager.inner.scheduled.lock().unwrap().is_empty());
    assert_eq!(
        manager
            .create_volume("after-shutdown", initialize())
            .await
            .unwrap_err()
            .code,
        "unavailable"
    );
    // The worker may have finished before shutdown won the gate, or may still
    // be durably queued. Neither ordering may lose the accepted identities.
    let stopped = manager.operation(accepted.id).await.unwrap();
    assert!(matches!(
        stopped.state,
        OperationState::Accepted | OperationState::Succeeded
    ));
    assert_eq!(stopped.items[0].workspace_id, Some(workspace));
    assert_eq!(stopped.items[0].head_layer_id, Some(head));
    manager.inner.pool.close().await;
    drop(manager);

    let reopened = Manager::open(dir.path()).await.unwrap();
    let finished = reopened.operation(accepted.id).await.unwrap();
    assert_eq!(finished.state, OperationState::Succeeded, "{finished:?}");
    assert_eq!(finished.items[0].workspace_id, Some(workspace));
    assert_eq!(finished.items[0].head_layer_id, Some(head));
    let view = reopened
        .inspect_workspace(volume.id, workspace)
        .await
        .unwrap();
    assert_eq!(view.record.head_layer_id, head);
    assert_eq!(view.base_revision, request.revision);
    assert_eq!(view.record.owner_id.as_deref(), Some("shutdown-recovery"));
    assert_eq!(reopened.list_workspaces(volume.id).await.unwrap().len(), 2);
    let replay = reopened
        .fork(volume.id, "queued-at-shutdown", request)
        .await
        .unwrap();
    assert_eq!(replay.id, accepted.id);
    assert_eq!(replay.items[0].workspace_id, Some(workspace));
    assert_eq!(replay.items[0].head_layer_id, Some(head));
    reopened.shutdown().await.unwrap();
}
