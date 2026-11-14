mod native_merge_tests {
    use super::*;
    use crate::workspace::publish::merge::MergeRequest;

    async fn fixture() -> (
        KvWorkspaceStore<MemoryBackend>,
        MergeRequest,
        LayerId,
        LayerId,
    ) {
        let backend = MemoryBackend::default();
        let store = budgeted_kv_store(backend.clone());
        let request = MergeRequest {
            operation: uuid::Uuid::new_v4(),
            child: WorkspaceId::new(),
            target: WorkspaceId::new(),
        };
        let base_id = LayerId::new();
        let child_head = LayerId::new();
        let target_head = LayerId::new();
        let mut state = ControlState {
            header: Some(VolumeHeader {
                volume_format: "workspace".into(),
                schema_version: WORKSPACE_SCHEMA_VERSION,
                volume_id: uuid::Uuid::new_v4(),
                created_at_ns: 1,
            }),
            ..ControlState::default()
        };
        let mut base = writable_layer(base_id, base_id, 1, request.target, 1);
        base.parent_layer_id = None;
        base.owner_workspace_id = None;
        base.state = LayerState::Sealed;
        base.sealed_version = Some(1);
        base.delta_digest = Some([0; 32]);
        base.root_hash = Some([0; 32]);
        base.sealed_at_ns = Some(1);
        state.layers.insert(base_id, base);
        for (id, head) in [(request.child, child_head), (request.target, target_head)] {
            state
                .layers
                .insert(head, writable_layer(head, base_id, 2, id, 1));
            state.workspaces.insert(
                id,
                WorkspaceRecord {
                    workspace_id: id,
                    head_layer_id: head,
                    head_epoch: 0,
                    fork_base: Some(BaseRevision {
                        layer_id: base_id,
                        sealed_version: 1,
                        root_hash: [0; 32],
                    }),
                    owner_id: None,
                    state: WorkspaceState::Active,
                    active_lease: None,
                    created_at_ns: 1,
                    updated_at_ns: 1,
                    upper_bucket: None,
                },
            );
        }
        let mut records = backend.records.lock().await;
        test_write_topology_rows(&mut records, &state);
        records.insert(
            VOLUME_HEADER_KEY.to_vec(),
            encode(state.header.as_ref().unwrap()).unwrap(),
        );
        drop(records);
        (store, request, child_head, target_head)
    }

    async fn file(store: &KvWorkspaceStore<MemoryBackend>, layer: LayerId, ino: i64, name: &[u8]) {
        let row = InodeDelta {
            layer_id: layer,
            ino,
            state: InodeState::Present,
            kind: 0,
            size: 4,
            mode: 0o644,
            uid: 0,
            gid: 0,
            rdev: 0,
            nlink: 1,
            atime_ns: 1,
            mtime_ns: 1,
            ctime_ns: 1,
            symlink_target: None,
            parent_hint: Some(1),
            data_version: 1,
            sequence: 1,
        };
        let dentry = DentryDelta {
            layer_id: layer,
            parent_ino: 1,
            name: name.to_vec(),
            op: DentryOp::Put,
            ino: Some(ino),
            entry_type: Some(0),
            sequence: 2,
        };
        let mut records = store.backend.records.lock().await;
        records.insert(inode_key(&row), encode(&row).unwrap());
        records.insert(dentry_key(&dentry), encode(&dentry).unwrap());
        let mut head: LayerRecord = decode(records.get(&hot_layer_key(layer)).unwrap()).unwrap();
        head.next_sequence = 3;
        records.insert(hot_layer_key(layer), encode(&head).unwrap());
    }

    #[tokio::test]
    async fn native_merge_conflict_preserves_all_records() {
        let (store, request, child, target) = fixture().await;
        file(&store, child, 2, b"file").await;
        file(&store, target, 3, b"file").await;
        let before = store.backend.records.lock().await.clone();
        let result = store.merge_native(request).await.unwrap();
        assert!(!result.committed);
        assert!(!result.conflicts.is_empty());
        assert_eq!(*store.backend.records.lock().await, before);
    }

    #[tokio::test]
    async fn native_merge_disjoint_retry_and_reopen() {
        let (store, request, child, target) = fixture().await;
        file(&store, child, 2, b"child").await;
        file(&store, target, 3, b"target").await;
        let result = store.merge_native(request.clone()).await.unwrap();
        assert!(result.committed);
        let committed = store.backend.records.lock().await.clone();
        assert_eq!(store.merge_native(request.clone()).await.unwrap(), result);
        assert_eq!(*store.backend.records.lock().await, committed);
        store
            .mark_workspace_deleting(MarkDeleting {
                workspace_id: request.child,
                force_fence_lease: false,
            })
            .await
            .unwrap();
        let reopened = budgeted_kv_store(store.backend.as_ref().clone());
        assert_eq!(reopened.merge_native(request).await.unwrap(), result);
        let delta = reopened.load_layer_delta(target).await.unwrap();
        assert!(delta.dentries.iter().any(|row| row.name == b"child"));
        assert!(delta.dentries.iter().any(|row| row.name == b"target"));
    }

    #[tokio::test]
    async fn native_merge_extent_survives_child_metadata_removal() {
        let (store, request, child, target) = fixture().await;
        file(&store, child, 2, b"sparse").await;
        let extent = DataExtentDelta::data(child, 2, 0, 2, 2, 700, 1, 3);
        let mut records = store.backend.records.lock().await;
        records.insert(extent_key(&extent), encode(&extent).unwrap());
        let mut head: LayerRecord = decode(records.get(&hot_layer_key(child)).unwrap()).unwrap();
        head.next_sequence = 4;
        records.insert(hot_layer_key(child), encode(&head).unwrap());
        drop(records);
        assert!(store.merge_native(request.clone()).await.unwrap().committed);
        store
            .mark_workspace_deleting(MarkDeleting {
                workspace_id: request.child,
                force_fence_lease: false,
            })
            .await
            .unwrap();
        let retained = store.gc_snapshot(i64::MAX / 2, 0).await.unwrap();
        assert!(
            retained
                .slice_references
                .iter()
                .any(|row| row.layer_id == target && row.slice_id == 700)
        );
        store
            .delete_layer_metadata(DeleteLayerMetadata {
                layer_ids: vec![child],
                now_ns: i64::MAX / 2,
                lease_grace_ns: 0,
            })
            .await
            .unwrap();
        store
            .finalize_layer_metadata_deletion(vec![child])
            .await
            .unwrap();
        assert!(store.load_layer(child).await.is_err());
        let reopened = budgeted_kv_store(store.backend.as_ref().clone());
        let chain = reopened.load_layer_chain(target).await.unwrap();
        let delta = reopened.load_layer_delta(target).await.unwrap();
        let plan = crate::workspace::resolver::resolve_extents(&chain, &delta.extents, 2, 0, 0..4)
            .unwrap();
        let object = b"abcd";
        let mut actual = vec![0; 4];
        for extent in plan {
            if let ExtentKind::Data {
                slice_id,
                slice_offset,
            } = extent.kind
            {
                assert_eq!(slice_id, 700);
                let source = slice_offset as usize;
                let start = extent.logical_offset as usize;
                let length = extent.length as usize;
                actual[start..start + length].copy_from_slice(&object[source..source + length]);
            }
        }
        assert_eq!(actual, b"\0\0bc");
    }

    #[tokio::test]
    async fn native_merge_precommit_failure_does_not_publish_receipt() {
        let (store, request, child, target) = fixture().await;
        file(&store, child, 2, b"child").await;
        store.backend.fail_cas_once.store(true, Ordering::SeqCst);
        let result = store.merge_native(request.clone()).await;
        assert!(matches!(result, Err(WorkspaceError::Backend(_))));
        assert!(
            store
                .load_layer_delta(target)
                .await
                .unwrap()
                .dentries
                .is_empty()
        );
        assert!(store.merge_native(request).await.unwrap().committed);
    }

    #[tokio::test]
    async fn native_merge_live_writer_is_rejected() {
        let (store, request, child, _) = fixture().await;
        file(&store, child, 2, b"child").await;
        let key = hot_workspace_key(request.target);
        let mut records = store.backend.records.lock().await;
        let mut workspace: WorkspaceRecord = decode(records.get(&key).unwrap()).unwrap();
        workspace.active_lease = Some(LeaseId::new());
        records.insert(key, encode(&workspace).unwrap());
        let before = records.clone();
        drop(records);
        assert!(store.merge_native(request).await.is_err());
        assert_eq!(*store.backend.records.lock().await, before);
    }

    #[tokio::test]
    async fn native_merge_directory_deletion_conflicts_with_descendant_changes() {
        let (store, request, child, target) = fixture().await;
        let base = store
            .load_layer(child)
            .await
            .unwrap()
            .parent_layer_id
            .unwrap();
        file(&store, base, 2, b"directory").await;
        file(&store, target, 3, b"nested").await;
        let mut records = store.backend.records.lock().await;
        let mut directory: InodeDelta =
            decode(records.get(&inode_identity_key(base, 2)).unwrap()).unwrap();
        directory.kind = 1;
        directory.nlink = 2;
        records.insert(inode_key(&directory), encode(&directory).unwrap());
        let base_name = DentryDelta {
            layer_id: base,
            parent_ino: 1,
            name: b"directory".to_vec(),
            op: DentryOp::Put,
            ino: Some(2),
            entry_type: Some(1),
            sequence: 2,
        };
        records.insert(dentry_key(&base_name), encode(&base_name).unwrap());
        directory.layer_id = child;
        directory.state = InodeState::Deleted;
        records.insert(inode_key(&directory), encode(&directory).unwrap());
        let removed = DentryDelta {
            layer_id: child,
            parent_ino: 1,
            name: b"directory".to_vec(),
            op: DentryOp::Whiteout,
            ino: None,
            entry_type: None,
            sequence: 2,
        };
        records.insert(dentry_key(&removed), encode(&removed).unwrap());
        let mut child_head: LayerRecord =
            decode(records.get(&hot_layer_key(child)).unwrap()).unwrap();
        child_head.next_sequence = 3;
        records.insert(hot_layer_key(child), encode(&child_head).unwrap());
        let mut nested: InodeDelta =
            decode(records.get(&inode_identity_key(target, 3)).unwrap()).unwrap();
        nested.parent_hint = Some(2);
        records.insert(inode_key(&nested), encode(&nested).unwrap());
        records.remove(&dentry_identity_key(target, 1, b"nested"));
        let name = DentryDelta {
            layer_id: target,
            parent_ino: 2,
            name: b"nested".to_vec(),
            op: DentryOp::Put,
            ino: Some(3),
            entry_type: Some(0),
            sequence: 2,
        };
        records.insert(dentry_key(&name), encode(&name).unwrap());
        let before = records.clone();
        drop(records);
        let result = store.merge_native(request.clone()).await.unwrap();
        assert!(!result.committed);
        assert!(result.conflicts.iter().any(|conflict| conflict.ino == 3));
        assert_eq!(*store.backend.records.lock().await, before);

        let mut records = store.backend.records.lock().await;
        records.remove(&inode_key(&nested));
        records.remove(&dentry_key(&name));
        nested.layer_id = base;
        records.insert(inode_key(&nested), encode(&nested).unwrap());
        let mut base_name = name;
        base_name.layer_id = base;
        records.insert(dentry_key(&base_name), encode(&base_name).unwrap());
        let attribute = XattrDelta {
            layer_id: target,
            ino: 3,
            name: b"user.note".to_vec(),
            op: ValueOp::Put,
            value: Some(b"note".to_vec()),
            sequence: 1,
        };
        records.insert(xattr_key(&attribute), encode(&attribute).unwrap());
        let before = records.clone();
        drop(records);
        let result = store.merge_native(request.clone()).await.unwrap();
        assert!(!result.committed);
        assert!(result.conflicts.iter().any(|conflict| conflict.ino == 3));
        assert_eq!(*store.backend.records.lock().await, before);

        let mut records = store.backend.records.lock().await;
        records.remove(&xattr_key(&attribute));
        let extent = DataExtentDelta::data(target, 3, 0, 0, 4, 700, 0, 1);
        records.insert(extent_key(&extent), encode(&extent).unwrap());
        let before = records.clone();
        drop(records);
        let result = store.merge_native(request).await.unwrap();
        assert!(!result.committed);
        assert!(result.conflicts.iter().any(|conflict| conflict.ino == 3));
        assert_eq!(*store.backend.records.lock().await, before);
    }

    #[tokio::test]
    async fn native_merge_operation_cannot_be_reused_for_another_child() {
        let (store, mut request, child, _) = fixture().await;
        file(&store, child, 2, b"child").await;
        assert!(store.merge_native(request.clone()).await.unwrap().committed);
        request.child = WorkspaceId::new();
        assert!(store.merge_native(request).await.is_err());
    }

    #[tokio::test]
    async fn native_merge_attribute_change_conflicts_with_file_deletion() {
        let (store, request, child, target) = fixture().await;
        let base = store
            .load_layer(child)
            .await
            .unwrap()
            .parent_layer_id
            .unwrap();
        file(&store, base, 2, b"file").await;
        file(&store, target, 2, b"file").await;
        let mut records = store.backend.records.lock().await;
        let mut inode: InodeDelta =
            decode(records.get(&inode_identity_key(target, 2)).unwrap()).unwrap();
        inode.state = InodeState::Deleted;
        records.insert(inode_key(&inode), encode(&inode).unwrap());
        let removed = DentryDelta {
            layer_id: target,
            parent_ino: 1,
            name: b"file".to_vec(),
            op: DentryOp::Whiteout,
            ino: None,
            entry_type: None,
            sequence: 2,
        };
        records.insert(dentry_key(&removed), encode(&removed).unwrap());
        let attribute = XattrDelta {
            layer_id: child,
            ino: 2,
            name: b"user.note".to_vec(),
            op: ValueOp::Put,
            value: Some(b"note".to_vec()),
            sequence: 1,
        };
        records.insert(xattr_key(&attribute), encode(&attribute).unwrap());
        let mut child_head: LayerRecord =
            decode(records.get(&hot_layer_key(child)).unwrap()).unwrap();
        child_head.next_sequence = 2;
        records.insert(hot_layer_key(child), encode(&child_head).unwrap());
        let before = records.clone();
        drop(records);
        assert!(!store.merge_native(request).await.unwrap().committed);
        assert_eq!(*store.backend.records.lock().await, before);
    }

    #[tokio::test]
    async fn native_merge_gc_bounds_survive_reopen_and_never_shrink() {
        let (store, _, _, _) = fixture().await;
        store.retain_gc_slice_bound(700, 8192).await.unwrap();
        store.retain_gc_slice_bound(700, 4096).await.unwrap();
        let reopened = budgeted_kv_store(store.backend.as_ref().clone());
        assert_eq!(reopened.read_gc_slice_bound(700, 1024).await.unwrap(), 8192);
        assert!(reopened.read_gc_slice_bound(700, 16384).await.is_err());
        reopened.retain_gc_slice_bound(700, 16384).await.unwrap();
        assert_eq!(store.read_gc_slice_bound(700, 8192).await.unwrap(), 16384);
    }

    #[tokio::test]
    #[ignore = "requires an isolated Redis and BREWFS_MERGE_FIXTURES output path"]
    async fn native_merge_seed_fuse() {
        let url = std::env::var("BREWFS_TEST_REDIS_URL").unwrap();
        let output = std::env::var("BREWFS_MERGE_FIXTURES").unwrap();
        let mut cases = Vec::new();
        for name in ["unchanged", "disjoint", "conflict"] {
            let (store, request, child, _) = fixture().await;
            let base = store
                .load_layer(child)
                .await
                .unwrap()
                .parent_layer_id
                .unwrap();
            file(&store, base, 10, b"baseline").await;
            file(&store, base, 11, b"deleted").await;
            let root = InodeDelta {
                layer_id: base,
                ino: 1,
                state: InodeState::Present,
                kind: 1,
                size: 0,
                mode: 0o755,
                uid: 0,
                gid: 0,
                rdev: 0,
                nlink: 2,
                atime_ns: 1,
                mtime_ns: 1,
                ctime_ns: 1,
                symlink_target: None,
                parent_hint: Some(1),
                data_version: 1,
                sequence: 1,
            };
            let mut records = store.backend.records.lock().await;
            records.insert(inode_key(&root), encode(&root).unwrap());
            records.insert(hot_allocator_key("inode"), encode(&12_i64).unwrap());
            records.insert(hot_allocator_key("slice"), encode(&1_i64).unwrap());
            records.insert(hot_allocator_key("sealed_version"), encode(&2_i64).unwrap());
            let namespace = format!("merge-test-{}", uuid::Uuid::new_v4());
            let backend = RedisWorkspaceBackend::connect(&url, &namespace)
                .await
                .unwrap();
            let writes = records
                .iter()
                .map(|(key, value)| KvWrite::Put {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect::<Vec<_>>();
            assert!(backend.compare_and_swap(&[], &writes).await.unwrap());
            backend.flush_metadata().await.unwrap();
            cases.push(serde_json::json!({ "name": name, "namespace": namespace,
                "child": request.child, "target": request.target, "operation": request.operation }));
        }
        std::fs::write(output, serde_json::to_vec_pretty(&cases).unwrap()).unwrap();
    }
}
