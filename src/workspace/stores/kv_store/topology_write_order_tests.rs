mod topology_write_order_tests {
    use super::*;

    #[tokio::test]
    async fn namespace_duplicate_put_then_whiteout_keeps_forward_and_reverse_final_state() {
        let (store, workspace, _, guard) = initialized().await;
        let layer = workspace.head_layer_id;
        let name = b"ordered-\xff".to_vec();
        store
            .apply_namespace_mutation(NamespaceMutation {
                guard: guard.clone(),
                dentries: vec![DentryDelta::put(layer, 1, name.clone(), 2, 0, 0)],
                inodes: vec![],
            })
            .await
            .unwrap();
        let result = store
            .apply_namespace_mutation(NamespaceMutation {
                guard,
                dentries: vec![
                    DentryDelta::put(layer, 1, name.clone(), 3, 0, 0),
                    DentryDelta::whiteout(layer, 1, name.clone(), 0),
                ],
                inodes: vec![],
            })
            .await
            .unwrap();
        let final_sequence = result.last_sequence.unwrap();
        assert_eq!(result.first_sequence, Some(final_sequence - 1));
        let forward: DentryDelta = decode(
            &store
                .backend
                .get(&dentry_identity_key(layer, 1, &name))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            forward,
            DentryDelta::whiteout(layer, 1, name, final_sequence)
        );
        assert_eq!(
            store.load_layer(layer).await.unwrap().next_sequence,
            final_sequence + 1
        );
        let budget = store.packed_reader_pin_budget.get().unwrap().clone();
        let layers = store.load_layer_chain(layer).await.unwrap();
        let proof = store
            .get_native_reverse_authority(&layers, budget.clone())
            .await
            .unwrap();
        for inode in [2, 3] {
            assert!(
                store
                    .get_native_reverse_dentry_page(&proof, layer, inode, None, budget.clone())
                    .await
                    .unwrap()
                    .rows
                    .is_empty()
            );
        }
        store
            .confirm_native_reverse_authority(&proof, budget)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn namespace_duplicate_whiteout_then_put_keeps_forward_and_reverse_final_state() {
        let (store, workspace, _, guard) = initialized().await;
        let layer = workspace.head_layer_id;
        let name = b"ordered-\xff".to_vec();
        store
            .apply_namespace_mutation(NamespaceMutation {
                guard: guard.clone(),
                dentries: vec![DentryDelta::put(layer, 1, name.clone(), 2, 0, 0)],
                inodes: vec![],
            })
            .await
            .unwrap();
        let result = store
            .apply_namespace_mutation(NamespaceMutation {
                guard,
                dentries: vec![
                    DentryDelta::whiteout(layer, 1, name.clone(), 0),
                    DentryDelta::put(layer, 1, name.clone(), 3, 0, 0),
                ],
                inodes: vec![],
            })
            .await
            .unwrap();
        let final_sequence = result.last_sequence.unwrap();
        assert_eq!(result.first_sequence, Some(final_sequence - 1));
        let forward: DentryDelta = decode(
            &store
                .backend
                .get(&dentry_identity_key(layer, 1, &name))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            forward,
            DentryDelta::put(layer, 1, name, 3, 0, final_sequence)
        );
        assert_eq!(
            store.load_layer(layer).await.unwrap().next_sequence,
            final_sequence + 1
        );
        let budget = store.packed_reader_pin_budget.get().unwrap().clone();
        let layers = store.load_layer_chain(layer).await.unwrap();
        let proof = store
            .get_native_reverse_authority(&layers, budget.clone())
            .await
            .unwrap();
        assert!(
            store
                .get_native_reverse_dentry_page(&proof, layer, 2, None, budget.clone())
                .await
                .unwrap()
                .rows
                .is_empty()
        );
        let page = store
            .get_native_reverse_dentry_page(&proof, layer, 3, None, budget.clone())
            .await
            .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0], forward);
        store
            .confirm_native_reverse_authority(&proof, budget)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn topology_packet_conflicting_non_dentry_writes_are_fenced_without_cas() {
        let (store, _, _, _) = initialized().await;
        let key = hot_allocator_key("inode");
        let raw = store.backend.get(&key).await.unwrap().unwrap();
        let current: i64 = decode(&raw).unwrap();
        let records_before = store.backend.records.lock().await.clone();
        let checks_before = store.backend.cas_checks.lock().await.clone();
        let writes_before = store.backend.cas_write_keys.lock().await.clone();
        let checks = vec![KvCheck {
            key: key.clone(),
            expected: Some(raw),
        }];
        for writes in [
            vec![
                put(key.clone(), &(current + 1)).unwrap(),
                put(key.clone(), &(current + 2)).unwrap(),
            ],
            vec![
                put(key.clone(), &(current + 1)).unwrap(),
                KvWrite::Delete { key: key.clone() },
            ],
        ] {
            assert!(matches!(
                store
                    .prepare_topology_packet(checks.clone(), writes, None)
                    .await,
                Err(WorkspaceError::Fenced)
            ));
            assert_eq!(*store.backend.records.lock().await, records_before);
            assert_eq!(*store.backend.cas_checks.lock().await, checks_before);
            assert_eq!(*store.backend.cas_write_keys.lock().await, writes_before);
        }
    }
}
