mod public_sidecar_migration_tests {
    use super::*;

    async fn public_sidecar_operation(
        store: &KvWorkspaceStore<MemoryBackend>,
        workspace: WorkspaceId,
        token: Option<&SnapshotOpenToken>,
        operation: usize,
    ) -> Result<(), WorkspaceError> {
        match operation {
            0 | 4 => store
                .open_workspace_snapshot(workspace, "owner", Duration::from_secs(30))
                .await
                .map(|_| ()),
            1 => store
                .mark_workspace_snapshot_ready(token.unwrap())
                .await
                .map(|_| ()),
            2 => store
                .renew_workspace_snapshot(token.unwrap(), Duration::from_secs(30))
                .await
                .map(|_| ()),
            3 => store.close_workspace_snapshot(token.unwrap()).await,
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn public_sidecars_reject_migration_marker_without_cas_or_row_changes() {
        // Initial open, ready, renew, close, and same-owner reopen all reject
        // the unpublished catalog before attempting a sidecar CAS.
        for operation in 0..5 {
            let (store, workspace, _, _) = initialized().await;
            store.backend.clock.store(1_000_000_000, Ordering::SeqCst);
            let token = if operation == 0 {
                None
            } else {
                Some(
                    store
                        .open_workspace_snapshot(workspace.workspace_id, "owner", Duration::from_secs(30))
                        .await
                        .unwrap(),
                )
            };
            store
                .backend
                .clock
                .fetch_add(1_000_000_000, Ordering::SeqCst);
            store.backend.cas_checks.lock().await.clear();
            store.backend.cas_write_keys.lock().await.clear();
            store
                .backend
                .records
                .lock()
                .await
                .insert(CONTROL_KEY.to_vec(), b"BWSMG002incomplete".to_vec());
            let before = store.backend.records.lock().await.clone();

            let result =
                public_sidecar_operation(&store, workspace.workspace_id, token.as_ref(), operation)
                    .await;
            assert!(
                matches!(result, Err(WorkspaceError::CorruptMetadata(ref message))
                    if message == "invalid control marker"),
                "operation {operation}: {result:?}"
            );
            assert_eq!(*store.backend.records.lock().await, before);
            assert!(store.backend.cas_checks.lock().await.is_empty());
            assert!(store.backend.cas_write_keys.lock().await.is_empty());
        }
    }

    #[tokio::test]
    async fn public_sidecars_do_not_require_catalog_header_in_actual_cas() {
        for operation in 0..5 {
            let (store, workspace, _, _) = initialized().await;
            store.backend.clock.store(1_000_000_000, Ordering::SeqCst);
            let token = if operation == 0 {
                None
            } else {
                Some(
                    store
                        .open_workspace_snapshot(workspace.workspace_id, "owner", Duration::from_secs(30))
                        .await
                        .unwrap(),
                )
            };
            store
                .backend
                .clock
                .fetch_add(1_000_000_000, Ordering::SeqCst);
            store.backend.cas_checks.lock().await.clear();
            store.backend.cas_write_keys.lock().await.clear();
            let sidecar_key = open_snapshot_key(workspace.workspace_id);
            let marker = b"BWSMG002incomplete".to_vec();
            let mut expected = store.backend.records.lock().await.clone();
            expected.insert(CONTROL_KEY.to_vec(), marker.clone());
            store.backend.mutate_on_cas.lock().await.push(KvWrite::Put {
                key: CONTROL_KEY.to_vec(),
                value: marker,
            });

            let result =
                public_sidecar_operation(&store, workspace.workspace_id, token.as_ref(), operation)
                    .await;
            assert!(result.is_ok(), "operation {operation}: {result:?}");
            let sidecar_after = store.backend.get(&sidecar_key).await.unwrap();
            assert!(sidecar_after.is_some(), "operation {operation}");
            expected.insert(sidecar_key.clone(), sidecar_after.unwrap());
            assert_eq!(*store.backend.records.lock().await, expected);
            assert!(store.backend.mutate_on_cas.lock().await.is_empty());
            let checks = store.backend.cas_checks.lock().await;
            assert_eq!(checks.len(), 1, "operation {operation}");
            assert!(checks[0].iter().all(|key| key.as_slice() != CONTROL_KEY));
            let writes = store.backend.cas_write_keys.lock().await;
            assert_eq!(writes.len(), 1, "operation {operation}");
            assert!(writes[0].contains(&sidecar_key));
            assert!(writes[0].iter().all(|key| key.as_slice() != CONTROL_KEY));
        }
    }
}
