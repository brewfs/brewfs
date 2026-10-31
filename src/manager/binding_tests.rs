use super::*;

async fn two_catalogs() -> (tempfile::TempDir, Manager, CreateVolumeRoot) {
    let dir = tempfile::tempdir().unwrap();
    let config = serde_json::json!({"catalogs": {
        "a": {"backend": "sqlite", "path": dir.path().join("a.sqlite")},
        "b": {"backend": "sqlite", "path": dir.path().join("b.sqlite")}
    }});
    std::fs::write(dir.path().join("manager-catalogs.json"), config.to_string()).unwrap();
    let manager = Manager::open(dir.path()).await.unwrap();
    let root = CreateVolumeRoot {
        upper_bucket: None,
        volume_format: "workspace-v1".into(),
        schema_version: WORKSPACE_SCHEMA_VERSION,
        volume_id: Uuid::new_v4(),
        workspace_id: WorkspaceId::new(),
        root_layer_id: LayerId::new(),
        writable_layer_id: LayerId::new(),
        owner_id: Some("external".into()),
    };
    // Two independent physical catalogs with identical logical IDs model a
    // copied catalog without sharing SQLite database or WAL files.
    for name in ["a", "b"] {
        let store = manager.catalog(name, true).await.unwrap();
        store.initialize_workspace_schema().await.unwrap();
        store.create_volume_root(root.clone()).await.unwrap();
    }
    (dir, manager, root)
}

fn register_request(catalog_ref: &str, volume: Uuid) -> VolumeRequest {
    VolumeRequest {
        mode: VolumeMode::Register,
        catalog_ref: catalog_ref.into(),
        owner: None,
        expected_volume_id: Some(volume),
        confirm_empty: false,
        volume_format: "workspace-v1".into(),
    }
}

async fn accepted_registration(manager: &Manager, volume: Uuid) -> Record {
    let request = register_request("a", volume);
    let record = Record {
        operation: Operation::new(volume, "volume", vec![OperationItem::new(None, None)]),
        catalog_ref: "a".into(),
        fingerprint: manager.fingerprint("a").unwrap(),
        task: Task::Volume {
            request: request.clone(),
            root: WorkspaceId::new(),
            lower: LayerId::new(),
            head: LayerId::new(),
            empty_checked: false,
        },
    };
    manager
        .prepare(
            "volumes",
            "register-a",
            digest(&request).unwrap(),
            record.clone(),
            true,
        )
        .await
        .unwrap();
    record
}

#[tokio::test]
async fn lost_registration_checkpoint_cannot_release_a_committed_binding() {
    let (dir, manager, root) = two_catalogs().await;
    let mut record = accepted_registration(&manager, root.volume_id).await;
    manager.effect(&mut record, 0).await.unwrap();
    assert_eq!(
        manager.operation(record.operation.id).await.unwrap().state,
        OperationState::Accepted
    );
    assert_eq!(
        manager
            .binding(root.volume_id)
            .await
            .unwrap()
            .view
            .catalog_ref,
        "a"
    );

    // The binding committed, but the item-success checkpoint did not. A later
    // deterministic catalog error must not make the same volume UUID reusable.
    std::fs::rename(dir.path().join("a.sqlite"), dir.path().join("a.offline")).unwrap();
    manager.execute(record.operation.id).await.unwrap();
    let failed = manager.operation(record.operation.id).await.unwrap();
    assert_eq!(failed.state, OperationState::Failed);
    assert_eq!(failed.error.unwrap().code, "not_found");
    let owner: String =
        sqlx::query_scalar("SELECT operation_id FROM manager_reservations WHERE volume_id=?")
            .bind(root.volume_id.to_string())
            .fetch_one(&manager.inner.pool)
            .await
            .unwrap();
    assert_eq!(owner, record.operation.id.to_string());
    let error = manager
        .create_volume("register-b", register_request("b", root.volume_id))
        .await
        .unwrap_err();
    assert_eq!(error.code, "conflict");
    assert_eq!(
        manager
            .binding(root.volume_id)
            .await
            .unwrap()
            .view
            .catalog_ref,
        "a"
    );
}

#[tokio::test]
async fn pending_fork_and_delete_cannot_follow_a_rebound_volume() {
    let (_dir, manager, root) = two_catalogs().await;
    let registration = accepted_registration(&manager, root.volume_id).await;
    manager.execute(registration.operation.id).await.unwrap();
    let original = manager
        .inspect_workspace(root.volume_id, root.workspace_id)
        .await
        .unwrap();
    let fork = manager
        .submit_fork(
            root.volume_id,
            "pending-fork",
            ForkRequest {
                revision: original.base_revision,
                count: 1,
                owner: Some("child".into()),
            },
            false,
        )
        .await
        .unwrap();
    let binding = manager.binding(root.volume_id).await.unwrap();
    let deletion = Record {
        operation: Operation::new(
            root.volume_id,
            "delete_workspace",
            vec![OperationItem::new(Some(root.workspace_id), None)],
        ),
        catalog_ref: binding.view.catalog_ref,
        fingerprint: binding.fingerprint,
        task: Task::Delete {
            workspace: root.workspace_id,
        },
    };
    manager
        .prepare(
            &format!(
                "volumes/{}/workspaces/{}/delete",
                root.volume_id, root.workspace_id
            ),
            "pending-delete",
            digest(&root.workspace_id).unwrap(),
            deletion.clone(),
            false,
        )
        .await
        .unwrap();

    // Simulate a stale/restored journal binding. Even an identical cloned
    // catalog must not become the target of previously accepted operations.
    let mut redirected = manager.binding(root.volume_id).await.unwrap();
    redirected.view.catalog_ref = "b".into();
    redirected.fingerprint = manager.fingerprint("b").unwrap();
    sqlx::query("UPDATE manager_volumes SET binding=? WHERE id=?")
        .bind(serde_json::to_string(&redirected).unwrap())
        .bind(root.volume_id.to_string())
        .execute(&manager.inner.pool)
        .await
        .unwrap();
    for id in [fork.id, deletion.operation.id] {
        manager.execute(id).await.unwrap();
        let operation = manager.operation(id).await.unwrap();
        assert_eq!(
            operation.state,
            OperationState::RecoveryRequired,
            "{operation:?}"
        );
        assert_eq!(operation.items[0].state, ItemState::RecoveryRequired);
    }
    for name in ["a", "b"] {
        let store = manager.catalog(name, false).await.unwrap();
        let records = store.list_workspaces().await.unwrap();
        assert_eq!(
            records.len(),
            1,
            "pending fork must not create in catalog {name}"
        );
        assert_eq!(
            records[0].state,
            WorkspaceState::Active,
            "pending delete must not mark catalog {name}"
        );
    }
}
