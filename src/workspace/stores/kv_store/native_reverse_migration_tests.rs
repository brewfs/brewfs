mod native_reverse_migration_tests {
    use super::*;
    use crate::workspace::packed::snapshot::SnapshotMountBudget;

    async fn fixture(
        operation: usize,
    ) -> (KvWorkspaceStore<MemoryBackend>, LayerId, Arc<SnapshotMountBudget>) {
        let (store, workspace, _, _) = initialized().await;
        let budget = store.packed_reader_pin_budget.get().unwrap().clone();
        let layer = if operation == 2 {
            // A valid historical Deleting entity without its reverse identity.
            let mut orphan = store.load_layer(workspace.head_layer_id).await.unwrap();
            orphan.layer_id = LayerId::new();
            orphan.state = LayerState::Deleting;
            orphan.owner_workspace_id = None;
            store
                .backend
                .records
                .lock()
                .await
                .insert(hot_layer_key(orphan.layer_id), encode(&orphan).unwrap());
            orphan.layer_id
        } else {
            workspace.head_layer_id
        };
        if operation == 1 || operation == 3 {
            store
                .start_native_reverse_index(layer, budget.clone())
                .await
                .unwrap();
        }
        if operation == 3 {
            assert!(
                store
                    .advance_native_reverse_index(layer, budget.clone())
                    .await
                    .unwrap()
            );
        }
        store.backend.cas_checks.lock().await.clear();
        store.backend.cas_write_keys.lock().await.clear();
        (store, layer, budget)
    }

    async fn maintain(
        store: &KvWorkspaceStore<MemoryBackend>,
        layer: LayerId,
        budget: Arc<SnapshotMountBudget>,
        operation: usize,
    ) -> Result<(), WorkspaceError> {
        match operation {
            0 => store.start_native_reverse_index(layer, budget).await,
            1 | 3 => store
                .advance_native_reverse_index(layer, budget)
                .await
                .map(|_| ()),
            2 => {
                store
                    .initialize_native_reverse_deleting_identity(layer, budget)
                    .await
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn reverse_maintenance_rejects_migration_before_mutation_or_ready_claim() {
        for operation in 0..4 {
            let (store, layer, budget) = fixture(operation).await;
            store
                .backend
                .records
                .lock()
                .await
                .insert(CONTROL_KEY.to_vec(), b"BWSMG002incomplete".to_vec());
            let before = store.backend.records.lock().await.clone();
            let used = budget.state().used;
            let result = maintain(&store, layer, budget.clone(), operation).await;
            assert!(
                matches!(result, Err(WorkspaceError::CorruptMetadata(ref message))
                    if message == "invalid control marker"),
                "operation {operation}: {result:?}"
            );
            assert_eq!(*store.backend.records.lock().await, before);
            assert!(store.backend.cas_checks.lock().await.is_empty());
            assert!(store.backend.cas_write_keys.lock().await.is_empty());
            assert_eq!(budget.state().used, used);
        }
    }

    #[tokio::test]
    async fn reverse_maintenance_authenticates_migration_in_its_actual_cas() {
        for operation in 0..3 {
            let (store, layer, budget) = fixture(operation).await;
            let marker = b"BWSMG002incomplete".to_vec();
            let mut expected = store.backend.records.lock().await.clone();
            expected.insert(CONTROL_KEY.to_vec(), marker.clone());
            store.backend.mutate_on_cas.lock().await.push(KvWrite::Put {
                key: CONTROL_KEY.to_vec(),
                value: marker,
            });
            let used = budget.state().used;
            assert!(matches!(
                maintain(&store, layer, budget.clone(), operation).await,
                Err(WorkspaceError::Busy)
            ));
            assert_eq!(*store.backend.records.lock().await, expected);
            assert!(store.backend.mutate_on_cas.lock().await.is_empty());
            let checks = store.backend.cas_checks.lock().await;
            assert_eq!(checks.len(), 1);
            assert!(checks[0].iter().any(|key| key.as_slice() == CONTROL_KEY));
            assert!(checks[0].contains(&hot_layer_key(layer)));
            let writes = store.backend.cas_write_keys.lock().await;
            assert_eq!(writes.len(), 1);
            assert!(writes[0].contains(&native_reverse::state_key(layer)));
            assert!(writes[0].iter().all(|key| key.as_slice() != CONTROL_KEY));
            assert_eq!(budget.state().used, used);
        }
    }
}
