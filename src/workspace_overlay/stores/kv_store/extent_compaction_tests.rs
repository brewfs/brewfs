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
    async fn adjacent_4097_shrinks_remain_readable_after_reopen() {
        use crate::chunk::read_plan::{ReadPlanSegment, WorkspaceReadPlanProvider};
        use crate::meta::MetaLayer;
        use crate::meta::store::{SetAttrFlags, SetAttrRequest};
        use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;

        let (store, workspace, lease, _) = initialized().await;
        // The old quadratic history scan can outlive a real test lease on
        // slow hosts. Keep time fixed so only read admission is under test.
        store
            .backend
            .clock
            .store(store.backend.clock_ns().unwrap(), Ordering::SeqCst);
        let store = Arc::new(store);
        let view = ViewContext {
            workspace_id: workspace.workspace_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        };
        let meta = WorkspaceMetaLayer::with_chunk_size(store.clone(), view.clone(), 8192);
        let ino = meta
            .create_file(meta.root_ino(), "shrinks".into())
            .await
            .unwrap();
        // ftruncate's SETATTR path: one writable head, no seal/snapshot and
        // no data writes, starting with a sparse 8192-byte file.
        for size in std::iter::once(8192).chain((4095..8192).rev()) {
            meta.set_attr(
                ino,
                &SetAttrRequest {
                    size: Some(size),
                    ..Default::default()
                },
                SetAttrFlags::empty(),
            )
            .await
            .unwrap();
        }
        let count = store
            .backend
            .scan_prefix(&extent_chunk_prefix(view.head_layer_id, ino, 0))
            .await
            .unwrap()
            .len();
        eprintln!("4097 adjacent shrinks: {count} persisted extent rows");
        drop(meta);
        let reopened = Arc::new(budgeted_kv_store(store.backend.as_ref().clone()));
        let query = ExtentQuery {
            layer_ids: vec![view.head_layer_id],
            ino,
            chunk_index: 0,
            range_start: 0,
            range_end: 4095,
        };
        let uncached = reopened.get_extent_deltas(query).await;
        assert!(
            uncached.is_ok(),
            "uncached read after 4097 shrinks: {uncached:?}"
        );
        let meta = WorkspaceMetaLayer::with_chunk_size(reopened, view, 8192);
        assert_eq!(meta.stat(ino).await.unwrap().unwrap().size, 4095);
        assert_eq!(
            meta.read_plan(ino, 0, 0, 4095).await.unwrap().segments,
            vec![ReadPlanSegment::Zero {
                logical_offset: 0,
                length: 4095
            }]
        );
        assert_eq!(count, 1, "adjacent shrink masks should coalesce");
    }

    #[tokio::test]
    async fn adjacent_overlapping_and_nested_holes_coalesce_without_resequencing() {
        for (first, second, expected) in [
            ((0, 16), (16, 16), (0, 32)),
            ((0, 24), (16, 24), (0, 40)),
            ((0, 64), (16, 8), (0, 64)),
        ] {
            let (store, workspace, _, guard) = initialized().await;
            let head = workspace.head_layer_id;
            let old = append(&store, &guard, hole(head, first.0, first.1))
                .await
                .unwrap();
            let new = append(&store, &guard, hole(head, second.0, second.1))
                .await
                .unwrap();
            let compacted = rows(&store, head).await;
            assert_eq!(compacted.len(), 1);
            assert_eq!((compacted[0].logical_offset, compacted[0].length), expected);
            assert_eq!(
                compacted[0].sequence, old.sequence,
                "never promote an older mask"
            );
            let chain = store.load_layer_chain(head).await.unwrap();
            assert_eq!(
                resolve_extent_coverage(&chain, &compacted, 2, 0, 0..64).unwrap(),
                resolve_extent_coverage(&chain, &[old, new], 2, 0, 0..64).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn coalescing_keeps_intervening_data_visible_and_newer_masks_effective() {
        for offset in [4, 8, 16, 24, 40] {
            let (store, workspace, _, guard) = initialized().await;
            let head = workspace.head_layer_id;
            let old = append(&store, &guard, hole(head, 0, 16)).await.unwrap();
            let data = append(
                &store,
                &guard,
                DataExtentDelta::data(head, 2, 0, offset, 16, 777, 7, 0),
            )
            .await
            .unwrap();
            let new = append(&store, &guard, hole(head, 16, 16)).await.unwrap();
            let chain = store.load_layer_chain(head).await.unwrap();
            let compacted = rows(&store, head).await;
            for start in 0..64 {
                assert_eq!(
                    resolve_extent_coverage(&chain, &compacted, 2, 0, start..start + 1).unwrap(),
                    resolve_extent_coverage(
                        &chain,
                        &[old.clone(), data.clone(), new.clone()],
                        2,
                        0,
                        start..start + 1
                    )
                    .unwrap(),
                    "data offset {offset}, byte {start}"
                );
            }
            let census = store.gc_snapshot(0, 0).await.unwrap();
            assert!(
                census
                    .slice_references
                    .iter()
                    .any(|reference| reference.layer_id == head
                        && reference.slice_id == 777
                        && reference.slice_end == 23)
            );
        }
    }

    #[tokio::test]
    async fn oversized_adjacent_history_recovers_under_the_next_head_cas() {
        let (store, workspace, _, guard) = initialized().await;
        let head = workspace.head_layer_id;
        {
            let mut records = store.backend.records.lock().await;
            for sequence in 1..=4097 {
                let row = DataExtentDelta::hole(head, 2, 0, 8192 - sequence, 1, sequence);
                records.insert(extent_key(&row), encode(&row).unwrap());
            }
            let key = hot_layer_key(head);
            let mut layer: LayerRecord = decode(&records[&key]).unwrap();
            layer.next_sequence = 4098;
            records.insert(key, encode(&layer).unwrap());
        }
        let query = ExtentQuery {
            layer_ids: vec![head],
            ino: 2,
            chunk_index: 0,
            range_start: 0,
            range_end: 8192,
        };
        assert!(matches!(
            store.get_extent_deltas(query.clone()).await,
            Err(WorkspaceError::InvalidReadPlan(_))
        ));
        store
            .append_data_extent(AppendDataExtent {
                guard,
                extent: DataExtentDelta::hole(head, 2, 0, 4094, 1, 0),
                chunk_size: 8192,
            })
            .await
            .unwrap();
        let reopened = budgeted_kv_store(store.backend.as_ref().clone());
        let compacted = reopened.get_extent_deltas(query).await.unwrap();
        assert_eq!(compacted.len(), 1);
        assert_eq!(
            (compacted[0].logical_offset, compacted[0].length),
            (4094, 4098)
        );
    }

    #[tokio::test]
    async fn paged_extent_read_retries_compaction_that_moves_masks_behind_cursor() {
        let (store, workspace, _, guard) = initialized().await;
        let head = workspace.head_layer_id;
        let mut history = vec![
            DataExtentDelta::hole(head, 2, 0, 0, 1, 1),
            DataExtentDelta::data(head, 2, 0, 1, 1, 777, 7, 2),
        ];
        // Keep 30 unrelated Data rows so the mask for byte 1 is on page two.
        history.extend((3..=32).map(|sequence| {
            DataExtentDelta::data(head, 2, 0, sequence + 1, 1, 100 + sequence, 0, sequence)
        }));
        history.push(DataExtentDelta::hole(head, 2, 0, 1, 1, 33));
        {
            let mut records = store.backend.records.lock().await;
            for row in &history {
                records.insert(extent_key(row), encode(row).unwrap());
            }
            let key = hot_layer_key(head);
            let mut layer: LayerRecord = decode(&records[&key]).unwrap();
            layer.next_sequence = 34;
            layer.owned_slice_count = 31;
            layer.owned_bytes = 31;
            records.insert(key, encode(&layer).unwrap());
        }
        let before = store.backend.records.lock().await.clone();
        let chain = store.load_layer_chain(head).await.unwrap();
        let expected = resolve_extent_coverage(&chain, &history, 2, 0, 1..2).unwrap();
        assert_eq!(expected.covered[0].kind, ExtentKind::Hole);

        // Capture a real mutation, including its ownership and generation
        // writes, then replay its atomic commit between the reader's pages.
        append(&store, &guard, hole(head, 2, 1)).await.unwrap();
        let after = store.backend.records.lock().await.clone();
        let mut commit = Vec::new();
        for key in before.keys().filter(|key| !after.contains_key(*key)) {
            commit.push(KvWrite::Delete { key: key.clone() });
        }
        for (key, value) in &after {
            if before.get(key) != Some(value) {
                commit.push(KvWrite::Put {
                    key: key.clone(),
                    value: value.clone(),
                });
            }
        }
        let compacted = rows(&store, head).await;
        assert_eq!(compacted.len(), 31);
        assert_eq!(
            resolve_extent_coverage(&chain, &compacted, 2, 0, 1..2).unwrap(),
            expected
        );

        // Establish that this interleaving exposes masked Data without a
        // fence: old page one contains it, new page two has lost its mask.
        *store.backend.records.lock().await = before.clone();
        *store.backend.mutate_after_extent_page.lock().await = commit.clone();
        let prefix = extent_chunk_prefix(head, 2, 0);
        let page = store
            .backend
            .scan_prefix_page_with_byte_limits(&prefix, None, delta_scan_limits())
            .await
            .unwrap();
        assert_eq!(page.len(), 32);
        assert!(
            store
                .backend
                .scan_prefix_page_with_byte_limits(
                    &prefix,
                    Some(&page.last().unwrap().key),
                    delta_scan_limits(),
                )
                .await
                .unwrap()
                .is_empty()
        );
        let mixed: Vec<DataExtentDelta> = page
            .iter()
            .map(|entry| decode(&entry.value).unwrap())
            .collect();
        assert!(matches!(
            resolve_extent_coverage(&chain, &mixed, 2, 0, 1..2)
                .unwrap()
                .covered[0]
                .kind,
            ExtentKind::Data { slice_id: 777, .. }
        ));

        *store.backend.records.lock().await = before;
        *store.backend.mutate_after_extent_page.lock().await = commit;
        let observed = rows(&store, head).await;
        assert_eq!(observed, compacted, "discard the mixed pages and retry");
        assert_eq!(
            resolve_extent_coverage(&chain, &observed, 2, 0, 1..2).unwrap(),
            expected
        );
        assert!(
            store
                .backend
                .mutate_after_extent_page
                .lock()
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn compacted_slice_ownership_retains_the_largest_end_across_reopen() {
        let (store, workspace, _, guard) = initialized().await;
        let head = workspace.head_layer_id;
        for (offset, expected_end) in [(7, 15), (1, 15), (50, 58)] {
            let reopened = budgeted_kv_store(store.backend.as_ref().clone());
            append(
                &reopened,
                &guard,
                DataExtentDelta::data(head, 2, 0, 8, 8, 777, offset, 0),
            )
            .await
            .unwrap();
            append(&reopened, &guard, hole(head, 0, 64)).await.unwrap();
            let census = reopened.gc_snapshot(0, 0).await.unwrap();
            assert_eq!(
                census.slice_references,
                vec![SliceReference {
                    layer_id: head,
                    slice_id: 777,
                    slice_end: expected_end,
                }]
            );
            assert_eq!(rows(&reopened, head).await.len(), 1);
        }
    }

    #[test]
    fn compaction_preserves_each_byte_for_all_three_record_overlap_shapes() {
        fn winner(rows: &[DataExtentDelta], offset: u64) -> Option<ExtentKind> {
            rows.iter()
                .filter(|row| {
                    row.logical_offset <= offset && offset < row.logical_offset + row.length
                })
                .max_by_key(|row| row.sequence)
                .map(|row| match row.kind {
                    ExtentKind::Hole => ExtentKind::Hole,
                    ExtentKind::Data {
                        slice_id,
                        slice_offset,
                    } => ExtentKind::Data {
                        slice_id,
                        slice_offset: slice_offset + offset - row.logical_offset,
                    },
                })
        }
        let head = LayerId::from_uuid(id(4));
        let ranges: Vec<_> = (0..6)
            .flat_map(|start| (start + 1..=6).map(move |end| (start, end - start)))
            .collect();
        // 21^3 ranges * 8 Hole/Data combinations. The oracle selects the
        // original byte winner directly, independently of interval merging.
        for &a in &ranges {
            for &b in &ranges {
                for &c in &ranges {
                    for kinds in 0..8 {
                        let history: Vec<_> = [a, b, c]
                            .into_iter()
                            .enumerate()
                            .map(|(index, (start, length))| {
                                let sequence = index as u64 + 1;
                                if kinds & (1 << index) == 0 {
                                    DataExtentDelta::hole(head, 2, 0, start, length, sequence)
                                } else {
                                    DataExtentDelta::data(
                                        head,
                                        2,
                                        0,
                                        start,
                                        length,
                                        100 + sequence,
                                        7,
                                        sequence,
                                    )
                                }
                            })
                            .collect();
                        let compacted = extent_compaction::coalesce_extent_rows(&history);
                        for offset in 0..7 {
                            assert_eq!(
                                winner(&compacted, offset),
                                winner(&history, offset),
                                "history {history:?}, compacted {compacted:?}, byte {offset}"
                            );
                        }
                    }
                }
            }
        }
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
            let expected = history
                .iter()
                .filter_map(|row| match row.kind {
                    ExtentKind::Hole => None,
                    ExtentKind::Data {
                        slice_id,
                        slice_offset,
                    } => Some((slice_id, slice_offset + row.length)),
                })
                .collect::<BTreeMap<_, _>>();
            let actual = store
                .gc_snapshot(0, 0)
                .await
                .unwrap()
                .slice_references
                .into_iter()
                .filter(|reference| reference.layer_id == head)
                .map(|reference| (reference.slice_id, reference.slice_end))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(
                actual, expected,
                "GC must retain every original slice and its full end"
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
    async fn hole_compaction_rejects_history_beyond_recovery_bound_or_corruption_without_commit() {
        for corrupt in [false, true] {
            let (store, workspace, _lease, guard) = initialized().await;
            let head = workspace.head_layer_id;
            {
                let mut records = store.backend.records.lock().await;
                let count = if corrupt {
                    1
                } else {
                    extent_compaction::RECOVERY_MAX_ROWS + 1
                };
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
