mod packed_permission_migration_tests {
    use super::*;
    use crate::workspace_overlay::catalog::PackedLowerBinding;
    use crate::workspace_overlay::packed_v3::wire005::V3MountBudget;

    async fn fixture() -> (
        tempfile::TempDir,
        KvWorkspaceStore<MemoryBackend>,
        VersionedMutation,
        PackedLowerBinding,
        Arc<V3MountBudget>,
    ) {
        let backend = MemoryBackend::default();
        backend.clock.store(1_000_000_000, Ordering::SeqCst);
        let budget = V3MountBudget::defaults();
        let store = KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(budget.clone());
        let (directory, _client, _snapshot, lower, _payload) =
            crate::workspace_overlay::stores::binding_tests::packed().await;
        let install = crate::workspace_overlay::stores::binding_tests::request(&store, lower).await;
        let record = store
            .install_packed_lower_binding(install.clone())
            .await
            .unwrap();
        let guard = HeadGuard {
            expected_head_epoch: record.head_epoch,
            ..install.guard
        };
        let layers = store
            .load_layer_chain(guard.expected_head_layer_id)
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let mut mutation = VersionedMutation::empty(guard.clone(), layers, 64);
        mutation.inodes.push(InodeDelta {
            layer_id: guard.expected_head_layer_id,
            ino: 400,
            state: InodeState::Present,
            kind: 0,
            size: 6,
            mode: 0o100644,
            uid: 1,
            gid: 2,
            rdev: 0,
            nlink: 1,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            symlink_target: None,
            parent_hint: Some(1),
            data_version: 1,
            sequence: 1,
        });
        store.backend.cas_checks.lock().await.clear();
        store.backend.cas_write_keys.lock().await.clear();
        (directory, store, mutation, record.binding, budget)
    }

    #[tokio::test]
    async fn packed_hole_compaction_keeps_cached_commits_bounded() {
        let (_directory, store, mut mutation, binding, budget) = fixture().await;
        let head = mutation.guard.expected_head_layer_id;
        mutation.extents = vec![DataExtentDelta::hole(head, 400, 0, 1, 63, 0)];
        let used = budget.state().used;
        for _ in 0..1025 {
            mutation.expected_layers[0] = store.load_layer(head).await.unwrap();
            store
                .apply_packed_versioned_mutation(mutation.clone(), binding.clone())
                .await
                .unwrap();
        }
        let rows = store
            .get_extent_deltas_bounded(
                ExtentQuery {
                    layer_ids: mutation
                        .expected_layers
                        .iter()
                        .map(|layer| layer.layer_id)
                        .collect(),
                    ino: 400,
                    chunk_index: 0,
                    range_start: 0,
                    range_end: 64,
                },
                1,
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, ExtentKind::Hole);
        assert_eq!(budget.state().used, used);
    }

    #[tokio::test]
    async fn packed_adjacent_shrinks_coalesce_before_the_1024_row_limit() {
        let (_directory, store, mut mutation, binding, budget) = fixture().await;
        let head = mutation.guard.expected_head_layer_id;
        mutation.chunk_size = 8192;
        let used = budget.state().used;
        for size in (4095..8192).rev() {
            mutation.expected_layers[0] = store.load_layer(head).await.unwrap();
            mutation.inodes[0].size = size;
            mutation.extents = vec![DataExtentDelta::hole(head, 400, 0, size, 1, 0)];
            store
                .apply_packed_versioned_mutation(mutation.clone(), binding.clone())
                .await
                .unwrap();
        }
        let reopened = budgeted_kv_store(store.backend.as_ref().clone());
        let query = ExtentQuery {
            layer_ids: mutation
                .expected_layers
                .iter()
                .map(|layer| layer.layer_id)
                .collect(),
            ino: 400,
            chunk_index: 0,
            range_start: 0,
            range_end: 8192,
        };
        let rows = reopened
            .get_extent_deltas_bounded(query, 1024)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].logical_offset, rows[0].length), (4095, 4097));
        assert_eq!(budget.state().used, used);
    }

    #[tokio::test]
    async fn packed_data_only_write_recovers_oversized_history_and_keeps_gc_ownership() {
        let (_directory, store, mut mutation, binding, budget) = fixture().await;
        let head = mutation.guard.expected_head_layer_id;
        {
            let mut records = store.backend.records.lock().await;
            for sequence in 1..=4097 {
                let row = DataExtentDelta::data(head, 400, 0, 0, 32, 777, sequence, sequence);
                records.insert(extent_key(&row), encode(&row).unwrap());
            }
            let key = hot_layer_key(head);
            let mut layer: LayerRecord = decode(&records[&key]).unwrap();
            layer.next_sequence = 4098;
            layer.owned_slice_count = 4097;
            layer.owned_bytes = 4097 * 32;
            records.insert(key, encode(&layer).unwrap());
        }
        mutation.expected_layers[0] = store.load_layer(head).await.unwrap();
        mutation.inodes[0].size = 32;
        mutation.extents = vec![DataExtentDelta::data(head, 400, 0, 0, 32, 888, 0, 0)];
        let before = store.backend.records.lock().await.clone();
        let used = budget.state().used;
        store.backend.fail_cas_once.store(true, Ordering::SeqCst);
        assert!(matches!(
            store
                .apply_packed_versioned_mutation(mutation.clone(), binding.clone())
                .await,
            Err(WorkspaceError::Backend(_))
        ));
        assert_eq!(
            *store.backend.records.lock().await,
            before,
            "ownership and data deletes are one CAS"
        );
        store
            .apply_packed_versioned_mutation(mutation.clone(), binding)
            .await
            .unwrap();
        let reopened = budgeted_kv_store(store.backend.as_ref().clone());
        let rows = reopened
            .get_extent_deltas_bounded(
                ExtentQuery {
                    layer_ids: mutation
                        .expected_layers
                        .iter()
                        .map(|layer| layer.layer_id)
                        .collect(),
                    ino: 400,
                    chunk_index: 0,
                    range_start: 0,
                    range_end: 64,
                },
                1024,
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].kind,
            ExtentKind::Data {
                slice_id: 888,
                slice_offset: 0
            }
        );
        let census = reopened.gc_snapshot(0, 0).await.unwrap();
        assert!(census.slice_references.contains(&SliceReference {
            layer_id: head,
            slice_id: 777,
            slice_end: 4129
        }));
        assert!(census.slice_references.contains(&SliceReference {
            layer_id: head,
            slice_id: 888,
            slice_end: 32
        }));
        assert_eq!(budget.state().used, used);
    }

    #[tokio::test]
    async fn packed_admission_counts_incoming_rows_and_rows_beyond_eof_in_both_layers() {
        let (_directory, store, mut mutation, binding, _) = fixture().await;
        let head = mutation.guard.expected_head_layer_id;
        let base = mutation.expected_layers[1].layer_id;
        mutation.chunk_size = 8192;
        {
            let mut records = store.backend.records.lock().await;
            for sequence in 1..=1023 {
                let row =
                    DataExtentDelta::data(head, 400, 0, sequence * 2, 1, sequence, 0, sequence);
                records.insert(extent_key(&row), encode(&row).unwrap());
            }
            let lower = DataExtentDelta::data(base, 400, 0, 4096, 1, 9999, 0, 1);
            records.insert(extent_key(&lower), encode(&lower).unwrap());
            let key = hot_layer_key(head);
            let mut layer: LayerRecord = decode(&records[&key]).unwrap();
            layer.next_sequence = 1024;
            layer.owned_slice_count = 1023;
            layer.owned_bytes = 1023;
            records.insert(key, encode(&layer).unwrap());
        }
        mutation.expected_layers[0] = store.load_layer(head).await.unwrap();
        mutation.extents = vec![DataExtentDelta::hole(head, 400, 0, 0, 1, 0)];
        // Every head row is outside this read. They still consume the raw
        // budget before the reader scans the sealed base's one extra row.
        assert!(matches!(
            store
                .get_extent_deltas_bounded(
                    ExtentQuery {
                        layer_ids: vec![head, base],
                        ino: 400,
                        chunk_index: 0,
                        range_start: 0,
                        range_end: 1,
                    },
                    1023,
                )
                .await,
            Err(WorkspaceError::InvalidReadPlan(_))
        ));
        let before = store.backend.records.lock().await.clone();
        store.backend.cas_checks.lock().await.clear();
        let result = store
            .apply_packed_versioned_mutation(mutation.clone(), binding.clone())
            .await;
        assert!(
            matches!(result, Err(WorkspaceError::InvalidReadPlan(_))),
            "{result:?}"
        );
        assert_eq!(*store.backend.records.lock().await, before);
        assert!(store.backend.cas_checks.lock().await.is_empty());
        // At exactly the same boundary, an overwrite can make room before
        // admission. The masked data slice must still belong to the layer.
        mutation.extents[0].logical_offset = 2;
        store
            .apply_packed_versioned_mutation(mutation.clone(), binding)
            .await
            .unwrap();
        let rows = store
            .get_extent_deltas_bounded(
                ExtentQuery {
                    layer_ids: vec![head, base],
                    ino: 400,
                    chunk_index: 0,
                    range_start: 0,
                    range_end: 8192,
                },
                1024,
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1024);
        assert!(
            store
                .gc_snapshot(0, 0)
                .await
                .unwrap()
                .slice_references
                .contains(&SliceReference {
                    layer_id: head,
                    slice_id: 1,
                    slice_end: 1
                })
        );
    }

    #[tokio::test]
    async fn packed_permission_mutation_rejects_migration_marker_before_any_cas() {
        let (_directory, store, mutation, binding, budget) = fixture().await;
        store
            .backend
            .records
            .lock()
            .await
            .insert(CONTROL_KEY.to_vec(), b"BWSMG002incomplete".to_vec());
        let before = store.backend.records.lock().await.clone();
        let used = budget.state().used;

        let result = store
            .apply_packed_versioned_mutation(mutation, binding)
            .await;
        assert!(
            matches!(result, Err(WorkspaceError::CorruptMetadata(ref message))
                if message == "invalid control marker"),
            "{result:?}"
        );
        assert_eq!(*store.backend.records.lock().await, before);
        assert!(store.backend.cas_checks.lock().await.is_empty());
        assert!(store.backend.cas_write_keys.lock().await.is_empty());
        assert_eq!(budget.state().used, used);
    }

    #[tokio::test]
    async fn packed_permission_mutation_authenticates_migration_marker_in_actual_row_cas() {
        let (_directory, store, mutation, binding, budget) = fixture().await;
        let head_key = hot_layer_key(mutation.guard.expected_head_layer_id);
        let inode_key = inode_key(&mutation.inodes[0]);
        let head_before = store.backend.get(&head_key).await.unwrap();
        let marker = b"BWSMG002incomplete".to_vec();
        let mut expected = store.backend.records.lock().await.clone();
        expected.insert(CONTROL_KEY.to_vec(), marker.clone());
        store.backend.mutate_on_cas.lock().await.push(KvWrite::Put {
            key: CONTROL_KEY.to_vec(),
            value: marker,
        });
        let used = budget.state().used;

        let result = store
            .apply_packed_versioned_mutation(mutation, binding)
            .await;
        assert!(
            matches!(result, Err(WorkspaceError::CorruptMetadata(ref message))
                if message == "invalid control marker"),
            "{result:?}"
        );
        assert_eq!(*store.backend.records.lock().await, expected);
        assert_eq!(store.backend.get(&head_key).await.unwrap(), head_before);
        assert!(store.backend.get(&inode_key).await.unwrap().is_none());
        assert!(store.backend.mutate_on_cas.lock().await.is_empty());
        let checks = store.backend.cas_checks.lock().await;
        assert_eq!(checks.len(), 1);
        assert!(checks[0].iter().any(|key| key.as_slice() == CONTROL_KEY));
        assert!(checks[0].contains(&head_key));
        let writes = store.backend.cas_write_keys.lock().await;
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].len(), 2);
        assert!(writes[0].contains(&head_key));
        assert!(writes[0].contains(&inode_key));
        assert!(writes[0].iter().all(|key| key.as_slice() != CONTROL_KEY));
        assert_eq!(budget.state().used, used);
    }
}
