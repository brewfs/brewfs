mod extent_compaction_batch_tests {
    use super::*;
    use crate::workspace::packed::snapshot::{
        SnapshotBudgetLimits, SnapshotBudgetPool, SnapshotMountBudget,
    };

    const ROWS_PER_CHUNK: u64 = 300;

    #[test]
    fn extent_compaction_pages_keep_encoding_order_and_cursor_bounds() {
        let layer = LayerId::new();
        let data = DataExtentDelta::data(
            layer,
            i64::MAX,
            u64::MAX,
            u64::MAX - 1,
            1,
            u64::MAX,
            u64::MAX - 1,
            u64::MAX,
        );
        let hole = DataExtentDelta::hole(layer, i64::MAX, u64::MAX, u64::MAX - 1, 1, u64::MAX);
        for row in [data, hole] {
            row.validate().unwrap();
            assert!(extent_key(&row).len() <= 128);
            let value = encode(&row).unwrap();
            assert!(value.len() <= 128);
            assert_eq!(
                decode_open_value::<DataExtentDelta>(&value, 128).unwrap(),
                row
            );
        }
        let prefix = extent_chunk_prefix(layer, 2, 0);
        let mut page = (1..=128)
            .map(|sequence| {
                let row = DataExtentDelta::hole(layer, 2, 0, 0, 1, sequence);
                KvEntry {
                    key: extent_key(&row),
                    value: encode(&row).unwrap(),
                }
            })
            .collect::<Vec<_>>();
        assert!(validate_delta_scan_page_with_limit(&page, &prefix, None, 128).is_ok());
        assert!(validate_delta_scan_page(&page, &prefix, None).is_err());
        assert!(
            validate_delta_scan_page_with_limit(&page, &prefix, Some(&page[0].key), 128).is_err()
        );
        assert!(validate_delta_scan_page_with_limit(&page, b"foreign/", None, 128).is_err());
        page.swap(31, 32);
        assert!(validate_delta_scan_page_with_limit(&page, &prefix, None, 128).is_err());
    }

    async fn publication_batch() -> (KvWorkspaceStore<MemoryBackend>, VersionedMutation) {
        let (store, workspace, _, guard) = initialized().await;
        store
            .backend
            .clock
            .store(store.backend.clock_ns().unwrap(), Ordering::SeqCst);
        let head = workspace.head_layer_id;
        {
            let mut records = store.backend.records.lock().await;
            let mut sequence = 1;
            for ino in 2..6 {
                for chunk in 0..8 {
                    for block in 0..ROWS_PER_CHUNK {
                        let row = DataExtentDelta::data(
                            head,
                            ino,
                            chunk,
                            block * 4096,
                            4096,
                            1000 + sequence,
                            0,
                            sequence,
                        );
                        records.insert(extent_key(&row), encode(&row).unwrap());
                        sequence += 1;
                    }
                }
            }
            let key = hot_layer_key(head);
            let mut layer: LayerRecord = decode(&records[&key]).unwrap();
            layer.next_sequence = sequence;
            layer.owned_slice_count = sequence - 1;
            layer.owned_bytes = (sequence - 1) * 4096;
            records.insert(key, encode(&layer).unwrap());
        }
        let layers = store
            .load_layer_chain(head)
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let mut request = VersionedMutation::empty(guard, layers, 64 << 20);
        for ino in 2..6 {
            request.inodes.push(InodeDelta {
                layer_id: head,
                ino,
                state: InodeState::Present,
                kind: 0,
                size: 512 << 20,
                mode: 0o100644,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                symlink_target: None,
                parent_hint: Some(1),
                data_version: 1,
                sequence: 0,
            });
            for chunk in 0..8 {
                request.extents.push(DataExtentDelta::data(
                    head,
                    ino,
                    chunk,
                    0,
                    4096,
                    100_000 + (ino as u64 - 2) * 8 + chunk,
                    0,
                    0,
                ));
            }
        }
        (store, request)
    }

    #[tokio::test]
    async fn publication_batch_keeps_each_legal_chunk_readable_after_reopen() {
        let (store, request) = publication_batch().await;
        let expected = request.extents.clone();
        let result = store.apply_versioned_mutation(request).await;
        assert!(
            result.is_ok(),
            "32 legal chunk histories must publish: {result:?}"
        );

        let reopened = budgeted_kv_store(store.backend.as_ref().clone());
        for incoming in expected {
            let rows = reopened
                .get_extent_deltas(ExtentQuery {
                    layer_ids: vec![incoming.layer_id],
                    ino: incoming.ino,
                    chunk_index: incoming.chunk_index,
                    range_start: 0,
                    range_end: 64 << 20,
                })
                .await
                .unwrap();
            assert_eq!(rows.len(), ROWS_PER_CHUNK as usize);
            for block in 0..ROWS_PER_CHUNK {
                let row = rows
                    .iter()
                    .find(|row| row.logical_offset == block * 4096)
                    .unwrap();
                let ordinal = (incoming.ino as u64 - 2) * 8 + incoming.chunk_index;
                let expected_kind = if block == 0 {
                    incoming.kind
                } else {
                    ExtentKind::Data {
                        slice_id: 1001 + ordinal * ROWS_PER_CHUNK + block,
                        slice_offset: 0,
                    }
                };
                assert_eq!(row.kind, expected_kind);
                assert_eq!(row.length, 4096);
            }
            let retired = extent_ownership::ExtentSliceOwner {
                layer_id: incoming.layer_id,
                slice_id: 1001
                    + ((incoming.ino as u64 - 2) * 8 + incoming.chunk_index) * ROWS_PER_CHUNK,
                slice_end: 4096,
            };
            assert_eq!(
                reopened.backend.get(&retired.key()).await.unwrap(),
                Some(encode(&retired).unwrap()),
                "masked data remains owned through layer GC"
            );
        }
    }

    #[tokio::test]
    async fn publication_batch_releases_scan_scratch_but_keeps_cas_output_admitted() {
        let (original, request) = publication_batch().await;
        let mut limits = SnapshotBudgetLimits::default();
        limits.bytes[SnapshotBudgetPool::Metadata as usize] = 2 << 20;
        let budget = SnapshotMountBudget::new(limits).unwrap();
        let store = KvWorkspaceStore::new(original.backend.as_ref().clone())
            .with_packed_reader_pin_budget(budget.clone());
        let head = &request.expected_layers[0];
        let mut writes = request
            .extents
            .into_iter()
            .enumerate()
            .map(|(index, mut row)| {
                row.sequence = head.next_sequence + index as u64;
                put(extent_key(&row), &row).unwrap()
            })
            .collect::<Vec<_>>();
        let admissions = store
            .compact_extent_history(head, None, DELTA_SCAN_MAX_ROWS, &mut writes)
            .await
            .expect("completed chunk scans must not accumulate temporary admission");
        let owned = budget.state().used[SnapshotBudgetPool::Metadata as usize];
        let output_capacity = writes.capacity() * std::mem::size_of::<KvWrite>()
            + writes
                .iter()
                .map(|write| match write {
                    KvWrite::Put { key, value } => key.capacity() + value.capacity(),
                    KvWrite::Delete { key } => key.capacity(),
                })
                .sum::<usize>();
        assert!(
            owned >= output_capacity as u64,
            "CAS output must retain its owner"
        );
        assert!(
            owned < 512 << 10,
            "completed scan scratch is no longer live"
        );
        assert_eq!(budget.state().rejections, 0);
        drop(writes);
        drop(admissions);
        assert_eq!(budget.state().used, [0; 8]);
    }

    #[tokio::test]
    async fn recovery_bound_still_combines_all_layers_of_one_chunk() {
        let (store, workspace, _, guard) = initialized().await;
        let head = workspace.head_layer_id;
        let lower = store
            .load_layer(head)
            .await
            .unwrap()
            .parent_layer_id
            .unwrap();
        {
            let mut records = store.backend.records.lock().await;
            for (layer, count) in [(head, 4096), (lower, 4097)] {
                for sequence in 1..=count {
                    let row = DataExtentDelta::hole(layer, 2, 0, 0, 64, sequence);
                    records.insert(extent_key(&row), encode(&row).unwrap());
                }
            }
            let key = hot_layer_key(head);
            let mut layer: LayerRecord = decode(&records[&key]).unwrap();
            layer.next_sequence = 4097;
            records.insert(key, encode(&layer).unwrap());
        }
        let before = store.backend.records.lock().await.clone();
        let result = store
            .append_data_extent(AppendDataExtent {
                guard,
                extent: DataExtentDelta::hole(head, 2, 0, 0, 64, 0),
                chunk_size: 64,
            })
            .await;
        assert!(matches!(result,
            Err(WorkspaceError::InvalidReadPlan(message))
                if message.contains("bounded recovery budget")));
        assert_eq!(*store.backend.records.lock().await, before);
    }
}
