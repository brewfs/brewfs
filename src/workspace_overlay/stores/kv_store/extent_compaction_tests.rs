mod extent_compaction_tests {
    use super::*;
    use crate::workspace_overlay::resolver::resolve_extent_coverage;

    fn hole(head: LayerId, offset: u64, length: u64) -> DataExtentDelta {
        DataExtentDelta::hole(head, 2, 0, offset, length, 0)
    }

    async fn append(
        store: &KvWorkspaceStore<MemoryBackend>,
        guard: &HeadGuard,
        extent: DataExtentDelta,
    ) -> Result<DataExtentDelta, WorkspaceError> {
        store
            .append_data_extent(AppendDataExtent {
                guard: guard.clone(),
                extent,
                chunk_size: 64,
            })
            .await
    }

    async fn rows(store: &KvWorkspaceStore<MemoryBackend>, head: LayerId) -> Vec<DataExtentDelta> {
        store
            .get_extent_deltas(ExtentQuery {
                layer_ids: vec![head],
                ino: 2,
                chunk_index: 0,
                range_start: 0,
                range_end: 64,
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn hole_compaction_preserves_partial_overlap_data_and_lower_masks() {
        let (store, workspace, _lease, guard) = initialized().await;
        let head = workspace.head_layer_id;
        let mut history = Vec::new();
        let mut seed = 0x1414_u64;
        for index in 0..256_u64 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let start = (seed >> 32) % 63;
            let length = 1 + (seed >> 16) % (64 - start);
            let row = if index.is_multiple_of(3) {
                DataExtentDelta::data(head, 2, 0, start, length, 100 + index, 7, 0)
            } else {
                hole(head, start, length)
            };
            history.push(append(&store, &guard, row).await.unwrap());
            let chain = store.load_layer_chain(head).await.unwrap();
            let compacted = rows(&store, head).await;
            // Coverage compares holes AND absent lower intervals, as well as
            // slice identity/offset. It catches accidentally exposing a lower
            // extent or advancing an older mask over a later data write.
            for range in [0..64, 3..17, 40..64] {
                assert_eq!(
                    resolve_extent_coverage(&chain, &compacted, 2, 0, range.clone()).unwrap(),
                    resolve_extent_coverage(&chain, &history, 2, 0, range).unwrap(),
                    "operation {index}"
                );
            }
            assert_eq!(
                compacted
                    .iter()
                    .filter(|row| matches!(row.kind, ExtentKind::Data { .. }))
                    .count(),
                history
                    .iter()
                    .filter(|row| matches!(row.kind, ExtentKind::Data { .. }))
                    .count(),
                "GC must retain every data slice reference"
            );
        }
        assert!(rows(&store, head).await.len() < history.len());
    }

    #[tokio::test]
    async fn hole_compaction_failure_is_atomic_and_reopen_preserves_the_winner() {
        let (store, workspace, _lease, guard) = initialized().await;
        let head = workspace.head_layer_id;
        let old = append(&store, &guard, hole(head, 8, 40)).await.unwrap();
        let before = store.backend.records.lock().await.clone();
        store.backend.fail_cas_once.store(true, Ordering::SeqCst);
        assert!(matches!(
            append(&store, &guard, hole(head, 0, 64)).await,
            Err(WorkspaceError::Backend(_))
        ));
        assert_eq!(*store.backend.records.lock().await, before);
        let reopened = budgeted_kv_store(store.backend.as_ref().clone());
        assert_eq!(rows(&reopened, head).await, vec![old]);
        let new = append(&reopened, &guard, hole(head, 0, 64)).await.unwrap();
        let reopened = budgeted_kv_store(store.backend.as_ref().clone());
        assert_eq!(rows(&reopened, head).await, vec![new]);
    }

    #[tokio::test]
    async fn hole_compaction_retries_a_concurrent_write_under_the_head_cas() {
        let (store, workspace, _lease, guard) = initialized().await;
        let head = workspace.head_layer_id;
        let old = append(&store, &guard, hole(head, 32, 32)).await.unwrap();
        let mut concurrent_head = store.load_layer(head).await.unwrap();
        let concurrent =
            DataExtentDelta::data(head, 2, 0, 16, 8, 777, 5, concurrent_head.next_sequence);
        concurrent_head.next_sequence += 1;
        concurrent_head.owned_slice_count += 1;
        concurrent_head.owned_bytes += concurrent.length;
        store.backend.mutate_on_cas.lock().await.extend([
            put(extent_key(&concurrent), &concurrent).unwrap(),
            put(hot_layer_key(head), &concurrent_head).unwrap(),
        ]);
        let new = append(&store, &guard, hole(head, 32, 32)).await.unwrap();
        assert!(new.sequence > concurrent.sequence);
        assert!(
            store
                .backend
                .get(&extent_key(&old))
                .await
                .unwrap()
                .is_none()
        );
        let chain = store.load_layer_chain(head).await.unwrap();
        assert_eq!(
            resolve_extent_coverage(&chain, &rows(&store, head).await, 2, 0, 0..64).unwrap(),
            resolve_extent_coverage(&chain, &[old, concurrent, new], 2, 0, 0..64).unwrap()
        );
    }

    #[tokio::test]
    async fn hole_compaction_rejects_oversized_or_corrupt_history_without_commit() {
        for corrupt in [false, true] {
            let (store, workspace, _lease, guard) = initialized().await;
            let head = workspace.head_layer_id;
            {
                let mut records = store.backend.records.lock().await;
                let count = if corrupt { 1 } else { DELTA_SCAN_MAX_ROWS + 1 };
                for sequence in 1..=count as u64 {
                    let mut row = hole(head, 0, 64);
                    row.sequence = sequence;
                    let key = extent_key(&row);
                    if corrupt {
                        row.ino = 3;
                    }
                    records.insert(key, encode(&row).unwrap());
                }
                let key = hot_layer_key(head);
                let mut layer: LayerRecord = decode(&records[&key]).unwrap();
                layer.next_sequence = count as u64 + 1;
                records.insert(key, encode(&layer).unwrap());
            }
            let before = store.backend.records.lock().await.clone();
            store.backend.cas_checks.lock().await.clear();
            let result = append(&store, &guard, hole(head, 0, 64)).await;
            if corrupt {
                assert!(matches!(result, Err(WorkspaceError::CorruptMetadata(_))));
            } else {
                assert!(matches!(result, Err(WorkspaceError::InvalidReadPlan(_))));
            }
            assert_eq!(*store.backend.records.lock().await, before);
            assert!(store.backend.cas_checks.lock().await.is_empty());
        }
    }
}
