// PR #141 entity-level CAS regression tests.
///
/// These tests intentionally observe the backend CAS packets instead of
/// assuming a fixed set of derived packed-v3 authority keys.  The packed
/// native preparation may add more exact checks over time; the invariant from
/// PR #141 is that unrelated entity transitions do not lock CONTROL and that
/// every topology write has a prior read condition.
#[tokio::test]
async fn topology_transaction_rejects_writes_without_prior_reads() {
    let store = budgeted_kv_store(MemoryBackend::default());
    let mut transaction = TopologyTxn::new(&store);
    let row = WorkspaceRecord {
        upper_bucket: None,
        workspace_id: WorkspaceId::from_uuid(id(1200)),
        head_layer_id: LayerId::from_uuid(id(1201)),
        head_epoch: 0,
        fork_base: None,
        owner_id: None,
        state: WorkspaceState::Active,
        active_lease: None,
        created_at_ns: 0,
        updated_at_ns: 0,
    };
    assert!(matches!(
        transaction.put_workspace(&row),
        Err(WorkspaceError::CorruptMetadata(_))
    ));
    assert!(matches!(
        transaction.put_checked(KvWrite::Delete {
            key: layer_key(row.head_layer_id),
        }),
        Err(WorkspaceError::CorruptMetadata(_))
    ));
    assert!(store.backend.cas_checks.lock().await.is_empty());
}

#[tokio::test]
async fn topology_transaction_repeated_reads_keep_the_original_condition() {
    let store = budgeted_kv_store(MemoryBackend::default());
    store.initialize_workspace_schema().await.unwrap();
    let workspace = store
        .create_volume_root(create_request(1500))
        .await
        .unwrap();

    let mut transaction = TopologyTxn::new(&store);
    assert_eq!(
        transaction
            .read_workspace(workspace.workspace_id)
            .await
            .unwrap(),
        Some(workspace.clone())
    );

    store
        .create_workspace(CreateWorkspace {
            upper_bucket: None,
            workspace_id: WorkspaceId::from_uuid(id(1600)),
            head_layer_id: LayerId::from_uuid(id(1601)),
            base_revision: workspace.fork_base.clone().unwrap(),
            owner_id: None,
        })
        .await
        .unwrap();

    let key = workspace_key(workspace.workspace_id);
    let (raw, _) = store
        .load_hot::<WorkspaceRecord>(key.clone())
        .await
        .unwrap();
    let mut updated = workspace.clone();
    updated.updated_at_ns += 1;
    assert!(
        store
            .backend
            .compare_and_swap(
                &[KvCheck {
                    key: key.clone(),
                    expected: raw,
                }],
                &[put(key, &updated).unwrap()],
            )
            .await
            .unwrap()
    );

    // A repeated read must not replace the original condition.
    assert_eq!(
        transaction
            .read_workspace(workspace.workspace_id)
            .await
            .unwrap(),
        Some(workspace.clone())
    );
    transaction.put_workspace(&workspace).unwrap();
    assert!(!transaction.commit().await.unwrap());
    assert_eq!(
        store.load_workspace(workspace.workspace_id).await.unwrap(),
        updated
    );
}

#[tokio::test]
async fn topology_transaction_rejects_changes_to_a_read_entity_atomically() {
    let store = budgeted_kv_store(MemoryBackend::default());
    store.initialize_workspace_schema().await.unwrap();
    let root = store
        .create_volume_root(create_request(1300))
        .await
        .unwrap();
    let chain = store.load_layer_chain(root.head_layer_id).await.unwrap();
    let revision = BaseRevision {
        layer_id: chain[1].layer_id,
        sealed_version: chain[1].sealed_version.unwrap(),
        root_hash: chain[1].root_hash.unwrap(),
    };
    let workspace_id = root.workspace_id;
    let head_id = root.head_layer_id;

    let mut transaction = TopologyTxn::new(&store);
    transaction.read_layer(revision.layer_id).await.unwrap();
    transaction.read_workspace(workspace_id).await.unwrap();
    transaction.read_layer(head_id).await.unwrap();

    let key = workspace_key(workspace_id);
    let raw = store.backend.get(&key).await.unwrap();
    let mut replacement = root.clone();
    replacement.updated_at_ns += 1;
    assert!(
        store
            .backend
            .compare_and_swap(
                &[KvCheck {
                    key: key.clone(),
                    expected: raw,
                }],
                &[put(key, &replacement).unwrap()],
            )
            .await
            .unwrap()
    );
    transaction.put_workspace(&root).unwrap();

    assert!(!transaction.commit().await.unwrap());
    assert_eq!(store.load_workspace(workspace_id).await.unwrap(), replacement);
}

#[tokio::test]
async fn topology_transaction_checks_only_declared_entities() {
    let store = budgeted_kv_store(MemoryBackend::default());
    store.initialize_workspace_schema().await.unwrap();
    let root = store.create_volume_root(create_request(300)).await.unwrap();
    let chain = store.load_layer_chain(root.head_layer_id).await.unwrap();
    let revision = BaseRevision {
        layer_id: chain[1].layer_id,
        sealed_version: chain[1].sealed_version.unwrap(),
        root_hash: chain[1].root_hash.unwrap(),
    };

    store.backend.cas_checks.lock().await.clear();
    for index in 0..64 {
        store
            .create_workspace(CreateWorkspace {
                upper_bucket: None,
                workspace_id: WorkspaceId::from_uuid(id(400 + index)),
                head_layer_id: LayerId::from_uuid(id(500 + index)),
                base_revision: revision.clone(),
                owner_id: None,
            })
            .await
            .unwrap();
    }
    assert_eq!(store.list_workspaces().await.unwrap().len(), 65);

    let checks = store.backend.cas_checks.lock().await.clone();
    assert!(!checks.is_empty());
    assert!(
        checks
            .iter()
            .all(|keys| keys.iter().all(|key| key.as_slice() != CONTROL_KEY)),
        "ordinary entity CAS must not include the global CONTROL key"
    );
    assert!(
        checks
            .iter()
            .any(|keys| { keys.iter().any(|key| key.starts_with(HOT_WORKSPACE_PREFIX)) }),
        "at least one final packet must authenticate a workspace entity"
    );
}

#[tokio::test]
async fn fork_retries_busy_from_initial_bounded_carrier_probe() {
    let backend = MemoryBackend::default();
    let store = budgeted_kv_store(backend.clone());
    store.initialize_workspace_schema().await.unwrap();
    let root = store
        .create_volume_root(create_request(1700))
        .await
        .unwrap();
    let chain = store.load_layer_chain(root.head_layer_id).await.unwrap();
    let base = &chain[1];
    let revision = BaseRevision {
        layer_id: base.layer_id,
        sealed_version: base.sealed_version.unwrap(),
        root_hash: base.root_hash.unwrap(),
    };

    backend
        .fail_bounded_read_busy_once
        .store(true, Ordering::SeqCst);
    let workspace_id = WorkspaceId::from_uuid(id(1701));
    let head_layer_id = LayerId::from_uuid(id(1702));
    let workspace = store
        .create_workspace(CreateWorkspace {
            workspace_id,
            head_layer_id,
            base_revision: revision,
            owner_id: None,
            upper_bucket: None,
        })
        .await
        .expect("a transient bounded carrier probe Busy should be retried");

    assert_eq!(store.load_workspace(workspace_id).await.unwrap(), workspace);
    assert_eq!(
        store
            .load_layer(head_layer_id)
            .await
            .unwrap()
            .owner_workspace_id,
        Some(workspace_id)
    );
}

#[tokio::test]
async fn concurrent_forks_from_one_revision_have_distinct_heads() {
    let store = Arc::new(budgeted_kv_store(MemoryBackend::default()));
    store.initialize_workspace_schema().await.unwrap();
    let root = store.create_volume_root(create_request(700)).await.unwrap();
    let chain = store.load_layer_chain(root.head_layer_id).await.unwrap();
    let revision = BaseRevision {
        layer_id: chain[1].layer_id,
        sealed_version: chain[1].sealed_version.unwrap(),
        root_hash: chain[1].root_hash.unwrap(),
    };

    let barrier = Arc::new(Barrier::new(64));
    let mut forks = Vec::new();
    for index in 0..64 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let revision = revision.clone();
        forks.push(tokio::spawn(async move {
            barrier.wait().await;
            let workspace_id = WorkspaceId::from_uuid(id(800 + index));
            let head_layer_id = LayerId::from_uuid(id(900 + index));
            let workspace = store
                .create_workspace(CreateWorkspace {
                    workspace_id,
                    head_layer_id,
                    base_revision: revision,
                    owner_id: None,
                    upper_bucket: None,
                })
                .await?;
            assert_eq!(store.load_workspace(workspace_id).await?, workspace);
            assert_eq!(
                store.load_layer(head_layer_id).await?.owner_workspace_id,
                Some(workspace_id)
            );
            Ok::<(), WorkspaceError>(())
        }));
    }
    for fork in forks {
        fork.await.unwrap().unwrap();
    }
    assert_eq!(store.list_workspaces().await.unwrap().len(), 65);
}
