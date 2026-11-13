//! Real-producer index-context contracts. These tests construct no publication
//! proof and exercise neither Install nor Publish authority.

#[path = "namespace_relation_tests.rs"]
mod namespace_relation_tests;

#[path = "content_relation_tests.rs"]
mod content_relation_tests;

#[path = "container_occurrence_tests.rs"]
mod container_occurrence_tests;

use super::*;
use crate::workspace::packed::snapshot::{
    SnapshotIndexAuditLimits, SnapshotIndexContextAudit, SnapshotIndexRecord, SnapshotLargeExtent,
    SnapshotPlacement, audit_snapshot_index_contexts,
};

async fn checked_index_audit(fixture: &Fixture) -> PackedResult<SnapshotIndexContextAudit> {
    let scratch = scratch();
    let budget = SnapshotMountBudget::defaults();
    let result = audit_snapshot_index_contexts(
        &fixture.client,
        &fixture.reference,
        scratch.path(),
        budget.clone(),
        SnapshotIndexAuditLimits::default(),
        CancellationToken::new(),
    )
    .await;
    assert_scratch_clean(scratch.path());
    if result.is_err() {
        assert_eq!(budget.state().used, [0; 8]);
    }
    result
}

fn bounded_index_limits() -> SnapshotIndexAuditLimits {
    SnapshotIndexAuditLimits {
        max_objects: 256,
        max_authenticated_bytes: 4 << 20,
        max_requested_bytes: 4 << 20,
        max_decoded_bytes: 4 << 20,
        max_frame_validation_steps: 100_000,
        max_logical_hash_bytes: 4 << 20,
        max_contexts: 64,
        max_visits: 512,
        max_leaf_records: 512,
        max_page_records: 1024,
        max_disk_bytes: 256 << 10,
        sqlite_cache_bytes: 64 << 10,
        max_sql_operations: 100_000,
        max_sql_vm_steps: 10_000_000,
        chunk_bytes: 4096,
    }
}

async fn run_bounded_index_audit(
    fixture: &Fixture,
    limits: SnapshotIndexAuditLimits,
    budget: Arc<SnapshotMountBudget>,
    cancel: CancellationToken,
    scratch: &Path,
) -> PackedResult<SnapshotIndexContextAudit> {
    audit_snapshot_index_contexts(
        &fixture.client,
        &fixture.reference,
        scratch,
        budget,
        limits,
        cancel,
    )
    .await
}

#[tokio::test]
async fn index_context_audit_success_retains_only_the_real_result_roots_owner() {
    let fixture = fixture(PackedCodec::Raw, false, 4).await;
    let scratch = scratch();
    let budget = SnapshotMountBudget::defaults();
    let accepted = run_bounded_index_audit(
        &fixture,
        bounded_index_limits(),
        budget.clone(),
        CancellationToken::new(),
        scratch.path(),
    )
    .await
    .unwrap();
    assert_eq!(accepted.manifest_reference(), &fixture.reference);
    assert_eq!(accepted.counts().contexts, 8);
    assert_scratch_clean(scratch.path());
    assert_eq!(budget.state().used, [64 << 10, 0, 0, 0, 0, 0, 0, 0]);
    assert!(!budget.state().closed);
    drop(accepted);
    assert_released(scratch.path(), &budget);
}

#[tokio::test]
async fn index_context_audit_enforces_every_graph_and_sql_work_quota() {
    let mut fixture = fixture(PackedCodec::Raw, false, 4).await;
    rebuild_inode_root(&mut fixture, "index-contexts/quotas/inodes".into()).await;
    let control_scratch = scratch();
    let control_budget = SnapshotMountBudget::defaults();
    let accepted = run_bounded_index_audit(
        &fixture,
        bounded_index_limits(),
        control_budget.clone(),
        CancellationToken::new(),
        control_scratch.path(),
    )
    .await
    .unwrap();
    let actual = accepted.counts();
    drop(accepted);
    assert_released(control_scratch.path(), &control_budget);

    // A successful real graph supplies observed bounds rather than guessed
    // counts. Each failing run differs by just one quota, one below its bound.
    for quota in 0..12 {
        let mut limits = bounded_index_limits();
        match quota {
            0 => limits.max_objects = actual.objects - 1,
            1 => limits.max_authenticated_bytes = actual.authenticated_bytes - 1,
            2 => limits.max_requested_bytes = actual.requested_bytes - 1,
            3 => limits.max_contexts = actual.contexts - 1,
            4 => limits.max_visits = actual.visits - 1,
            5 => limits.max_leaf_records = actual.leaf_records - 1,
            6 => limits.max_page_records = actual.page_records - 1,
            7 => limits.max_sql_operations = actual.sql_operations - 1,
            8 => limits.max_sql_vm_steps = actual.sql_vm_steps - 1,
            9 => limits.max_decoded_bytes = actual.decoded_bytes - 1,
            10 => limits.max_logical_hash_bytes = actual.logical_hash_bytes - 1,
            _ => limits.max_frame_validation_steps = actual.frame_validation_steps - 1,
        }
        let scratch = scratch();
        let budget = SnapshotMountBudget::defaults();
        let error = run_bounded_index_audit(
            &fixture,
            limits,
            budget.clone(),
            CancellationToken::new(),
            scratch.path(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, PackedWireError::LimitExceeded(_)),
            "quota {quota}: {error}"
        );
        assert_released(scratch.path(), &budget);
        assert!(!budget.state().closed);
    }

    // Exact observed bounds must admit the same graph. Objects include PM11,
    // payloads, Cold and FD pages; pages count only IP06 objects.
    let mut exact = bounded_index_limits();
    exact.max_objects = actual.objects;
    exact.max_authenticated_bytes = actual.authenticated_bytes;
    exact.max_requested_bytes = actual.requested_bytes;
    exact.max_decoded_bytes = actual.decoded_bytes;
    exact.max_frame_validation_steps = actual.frame_validation_steps;
    exact.max_logical_hash_bytes = actual.logical_hash_bytes;
    exact.max_contexts = actual.contexts;
    exact.max_visits = actual.visits;
    exact.max_leaf_records = actual.leaf_records;
    exact.max_page_records = actual.page_records;
    exact.max_sql_operations = actual.sql_operations;
    exact.max_sql_vm_steps = actual.sql_vm_steps;
    let accepted = run_bounded_index_audit(
        &fixture,
        exact,
        control_budget.clone(),
        CancellationToken::new(),
        control_scratch.path(),
    )
    .await
    .unwrap();
    assert_eq!(accepted.counts(), actual);
    drop(accepted);
    assert_released(control_scratch.path(), &control_budget);
}

#[tokio::test]
async fn index_context_audit_releases_create_time_sql_and_disk_quota_failures() {
    let fixture = fixture(PackedCodec::Raw, false, 4).await;
    for quota in 0..5 {
        let mut limits = bounded_index_limits();
        match quota {
            0 => limits.max_sql_operations = 1,
            1 => limits.max_sql_vm_steps = 1,
            2 => limits.max_disk_bytes = 16 << 10,
            3 => limits.chunk_bytes = 0,
            _ => limits.chunk_bytes = (1 << 20) + 1,
        }
        let scratch = scratch();
        let budget = SnapshotMountBudget::defaults();
        let error = run_bounded_index_audit(
            &fixture,
            limits,
            budget.clone(),
            CancellationToken::new(),
            scratch.path(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, PackedWireError::LimitExceeded(_)),
            "create-time quota {quota}: {error}"
        );
        // Returning an error is an acknowledgment boundary too; it may not
        // race a detached worker's release of its directory or memory owners.
        assert_released(scratch.path(), &budget);
        assert!(!budget.state().closed);
    }
}

#[tokio::test]
async fn index_context_audit_checks_memory_admission_and_preexisting_termination() {
    use crate::workspace::packed::snapshot::SnapshotBudgetLimits;

    let fixture = fixture(PackedCodec::Raw, false, 4).await;
    for mode in 0..4 {
        let scratch = scratch();
        let mut limits = SnapshotBudgetLimits::default();
        if mode == 0 {
            limits.bytes[SnapshotBudgetPool::Roots as usize] = (64 << 10) - 1;
        } else if mode == 1 {
            limits.bytes[SnapshotBudgetPool::Metadata as usize] = 1024;
        }
        let budget = SnapshotMountBudget::new(limits).unwrap();
        let cancel = CancellationToken::new();
        if mode == 2 {
            cancel.cancel();
        } else if mode == 3 {
            budget.close();
        }
        let error = run_bounded_index_audit(
            &fixture,
            bounded_index_limits(),
            budget.clone(),
            cancel,
            scratch.path(),
        )
        .await
        .unwrap_err();
        if mode == 2 {
            assert!(
                matches!(error, PackedWireError::Backend(_)),
                "cancel: {error}"
            );
        } else {
            assert!(
                matches!(error, PackedWireError::LimitExceeded(_)),
                "memory/closed mode {mode}: {error}"
            );
        }
        assert_released(scratch.path(), &budget);
        assert_eq!(budget.state().closed, mode == 3);
    }
}

#[tokio::test]
async fn index_context_audit_cancellation_and_budget_close_release_inflight_owners() {
    let fixture = fixture(PackedCodec::Raw, false, 4).await;
    for close_budget in [false, true] {
        let scratch = scratch();
        let budget = SnapshotMountBudget::defaults();
        let pause = Arc::new(PauseRange::default());
        let mut backend = ReadOnlyLocal::new(fixture.objects.path());
        backend.pause = Some(pause.clone());
        let client = ObjectClient::new(backend);
        let cancel = CancellationToken::new();
        let task_reference = fixture.reference.clone();
        let task_scratch = scratch.path().to_owned();
        let task_budget = budget.clone();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            audit_snapshot_index_contexts(
                &client,
                &task_reference,
                &task_scratch,
                task_budget,
                bounded_index_limits(),
                task_cancel,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
            .await
            .unwrap();
        assert!(budget.state().used.iter().any(|used| *used > 0));
        assert!(std::fs::read_dir(scratch.path()).unwrap().count() > 1);
        if close_budget {
            budget.close();
        } else {
            cancel.cancel();
        }
        let error = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        if close_budget {
            assert!(
                matches!(error, PackedWireError::LimitExceeded(_)),
                "budget: {error}"
            );
        } else {
            assert!(
                matches!(error, PackedWireError::Backend(_)),
                "cancel: {error}"
            );
        }
        assert_released(scratch.path(), &budget);
        assert_eq!(budget.state().closed, close_budget);
    }
}

#[tokio::test]
async fn index_context_audit_waits_for_close_ack_and_rejects_a_terminated_real_result() {
    use futures_util::FutureExt;

    let fixture = fixture(PackedCodec::Raw, false, 4).await;
    for mode in 0..4 {
        let scratch = scratch();
        let budget = SnapshotMountBudget::defaults();
        let cancel = CancellationToken::new();
        let verified = run_bounded_index_audit(
            &fixture,
            bounded_index_limits(),
            budget.clone(),
            cancel.clone(),
            scratch.path(),
        )
        .await
        .unwrap();
        let counts = verified.counts();
        assert_eq!(verified.manifest_reference(), &fixture.reference);
        assert_eq!(counts.contexts, 8);
        assert_scratch_clean(scratch.path());
        assert_eq!(budget.state().used, [64 << 10, 0, 0, 0, 0, 0, 0, 0]);

        // This is the actual public traversal's result, not a constructed or
        // forged authority. Only the final close acknowledgment is delayed.
        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        let finishing = super::super::index_context::finish_after_close(
            Ok(verified),
            async {
                receiver.await.unwrap();
                if mode == 3 {
                    Err(PackedWireError::Backend(
                        "forced index close failure".into(),
                    ))
                } else {
                    Ok(())
                }
            },
            &budget,
            &cancel,
        );
        tokio::pin!(finishing);
        assert!(finishing.as_mut().now_or_never().is_none());
        assert_eq!(budget.state().used, [64 << 10, 0, 0, 0, 0, 0, 0, 0]);
        if mode == 1 {
            cancel.cancel();
        } else if mode == 2 {
            budget.close();
        }
        sender.send(()).unwrap();
        let result = finishing.await;
        if mode == 0 {
            let accepted = result.unwrap();
            assert_eq!(accepted.manifest_reference(), &fixture.reference);
            assert_eq!(accepted.counts(), counts);
            drop(accepted);
        } else {
            let error = result.unwrap_err();
            if mode == 2 {
                assert!(
                    matches!(error, PackedWireError::LimitExceeded(_)),
                    "close: {error}"
                );
            } else {
                assert!(
                    matches!(error, PackedWireError::Backend(_)),
                    "termination: {error}"
                );
            }
            if mode == 3 {
                assert!(error.to_string().contains("forced index close failure"));
            }
        }
        assert_released(scratch.path(), &budget);
        assert_eq!(budget.state().closed, mode == 2);
    }
}

async fn assert_physical_precondition(fixture: &Fixture) {
    // The existing public physical audit proves the complete reachable byte
    // graph still exists and authenticates. It does not prove index contexts.
    drop(audit(fixture, limits()).await.unwrap());
}

async fn upload_index_page(
    fixture: &Fixture,
    key: &str,
    value: &SnapshotIndexPage,
) -> SnapshotObjectRef {
    let bytes = value.encode().unwrap();
    let reference = SnapshotObjectRef::from_bytes(key.into(), value.kind, &bytes).unwrap();
    assert_eq!(
        SnapshotIndexPage::decode(&reference, &bytes).unwrap(),
        *value
    );
    fixture
        .client
        .put_object_create_only(key, &bytes)
        .await
        .unwrap();
    reference
}

async fn replace_root(
    fixture: &mut Fixture,
    kind: SnapshotRootKind,
    root: SnapshotObjectRef,
    manifest_key: &str,
) {
    let mut manifest = fixture.manifest.clone();
    manifest.roots[kind as usize] = root;
    replace_manifest(fixture, manifest, manifest_key).await;
    // Open the uploaded PM11 rather than relying on an in-memory descriptor.
    let opened = AuthenticatedSnapshot::open(&fixture.client, &fixture.reference)
        .await
        .unwrap();
    assert_eq!(opened.manifest(), &fixture.manifest);
}

fn assert_index_relation_error(result: PackedResult<SnapshotIndexContextAudit>) {
    let error = match result {
        Err(error) => error,
        Ok(audit) => panic!(
            "index-context audit accepted the SHA-correct malformed graph: {:?}",
            audit.counts()
        ),
    };
    assert!(
        matches!(
            error,
            PackedWireError::Invalid(_) | PackedWireError::UnsupportedFormat(_)
        ),
        "graph must fail a typed/index relation, not an unrelated resource/backend failure: {error}"
    );
}

#[tokio::test]
async fn index_context_audit_accepts_seven_roots_and_source_with_real_two_child_inode_tree() {
    let mut fixture = fixture(PackedCodec::Raw, false, 4).await;
    let root = rebuild_inode_root(&mut fixture, "index-contexts/correct/inodes".into()).await;
    let branch = page(&fixture, &root).await;
    assert_eq!(branch.height, 1);
    assert_eq!(branch.records.len(), 2);
    assert_physical_precondition(&fixture).await;
    let accepted = checked_index_audit(&fixture).await.unwrap();
    assert_eq!(accepted.manifest_reference(), &fixture.reference);
    assert_eq!(accepted.counts().contexts, 8);
    assert!(accepted.counts().visits >= accepted.counts().contexts);
    assert!(accepted.counts().leaf_records >= 4);
}

#[tokio::test]
async fn index_context_audit_rejects_offroute_weight_fence_and_height_with_valid_object_shas() {
    for mismatch in ["weight", "fence", "height"] {
        let mut fixture = fixture(PackedCodec::Raw, false, 4).await;
        let root =
            rebuild_inode_root(&mut fixture, format!("index-contexts/{mismatch}/inodes")).await;
        let mut branch = page(&fixture, &root).await;
        assert_eq!(branch.height, 1);
        assert_eq!(branch.records.len(), 2);
        let original_right = branch.records[1].clone();
        match mismatch {
            "weight" => {
                let SnapshotIndexValue::Child { subtree_weight, .. } = &mut branch.records[0].value
                else {
                    panic!("left child expected")
                };
                assert_eq!(*subtree_weight, 2);
                *subtree_weight = 3;
            }
            "fence" => {
                assert_eq!(branch.records[0].last_key, 3u64.to_be_bytes());
                branch.records[0].last_key = 2u64.to_be_bytes().to_vec();
            }
            "height" => {
                let SnapshotIndexValue::Child {
                    reference,
                    subtree_weight,
                } = &branch.records[0].value
                else {
                    panic!("left child expected")
                };
                let wrapper = SnapshotIndexPage {
                    kind: SnapshotObjectKind::InodeIndex,
                    height: 1,
                    records: vec![SnapshotIndexRecord {
                        first_key: branch.records[0].first_key.clone(),
                        last_key: branch.records[0].last_key.clone(),
                        value: SnapshotIndexValue::Child {
                            reference: reference.clone(),
                            subtree_weight: *subtree_weight,
                        },
                    }],
                };
                let wrapped =
                    upload_index_page(&fixture, "index-contexts/height/wrapped-left", &wrapper)
                        .await;
                let SnapshotIndexValue::Child { reference, .. } = &mut branch.records[0].value
                else {
                    unreachable!()
                };
                *reference = wrapped;
            }
            _ => unreachable!(),
        }
        assert_eq!(branch.records[1], original_right);
        let changed_root = upload_index_page(
            &fixture,
            &format!("index-contexts/{mismatch}/root"),
            &branch,
        )
        .await;
        assert_ne!(changed_root.digest, root.digest);
        replace_root(
            &mut fixture,
            SnapshotRootKind::Inodes,
            changed_root.clone(),
            &format!("index-contexts/{mismatch}/manifest"),
        )
        .await;
        assert_physical_precondition(&fixture).await;
        let ceiling_reader = SnapshotIndexReader::new(fixture.client.clone(), 0);
        assert_eq!(
            ceiling_reader.maximum_inode(&changed_root).await.unwrap(),
            Some(5)
        );
        ceiling_reader.close().await;
        assert_index_relation_error(checked_index_audit(&fixture).await);
    }
}

#[cfg(target_os = "linux")]
async fn current_selector(
    fixture: &Fixture,
    inode: u64,
) -> (SnapshotIndexPage, usize, SnapshotPlacement) {
    let selectors = page(
        fixture,
        &fixture.manifest.roots[SnapshotRootKind::LargePlacements as usize],
    )
    .await;
    let ordinal = selectors
        .records
        .iter()
        .position(|record| record.first_key == inode.to_be_bytes())
        .unwrap();
    let SnapshotIndexValue::Leaf(value) = &selectors.records[ordinal].value else {
        panic!("real selector leaf expected")
    };
    let placement = SnapshotPlacement::decode(value, inode, 8 << 20).unwrap();
    (selectors, ordinal, placement)
}

#[cfg(target_os = "linux")]
async fn replace_selector_extent_root(
    fixture: &mut Fixture,
    inode: u64,
    root: SnapshotObjectRef,
    key_prefix: &str,
) {
    let (mut selectors, ordinal, mut placement) = current_selector(fixture, inode).await;
    let SnapshotPlacement::External { extents, .. } = &mut placement else {
        panic!("external selector expected")
    };
    *extents = root;
    selectors.records[ordinal].value = SnapshotIndexValue::Leaf(placement.encode().unwrap());
    let selectors =
        upload_index_page(fixture, &format!("{key_prefix}/selector-root"), &selectors).await;
    replace_root(
        fixture,
        SnapshotRootKind::LargePlacements,
        selectors,
        &format!("{key_prefix}/manifest"),
    )
    .await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn index_context_audit_accepts_external_and_all_hole_selector_contexts() {
    for all_hole in [false, true] {
        let fixture = external_fixture(all_hole).await;
        assert_physical_precondition(&fixture).await;
        let accepted = checked_index_audit(&fixture).await.unwrap();
        assert_eq!(accepted.manifest_reference(), &fixture.reference);
        assert_eq!(accepted.counts().contexts, 9);
        assert!(accepted.counts().visits >= 9);
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn index_context_audit_rejects_namespace_le09_and_external_ps09_role_substitution() {
    let mut namespace = external_fixture(false).await;
    let extents = external_extent_root(&namespace).await;
    replace_root(
        &mut namespace,
        SnapshotRootKind::LargePlacements,
        extents,
        "index-contexts/namespace-le09/manifest",
    )
    .await;
    assert_physical_precondition(&namespace).await;
    assert_index_relation_error(checked_index_audit(&namespace).await);

    let mut external = external_fixture(false).await;
    // Point a newly encoded PS09 at the old immutable namespace root. The old
    // root in turn points at the original LE09, so there is no self-hash cycle.
    let old_namespace = external.manifest.roots[SnapshotRootKind::LargePlacements as usize].clone();
    replace_selector_extent_root(
        &mut external,
        2,
        old_namespace,
        "index-contexts/external-ps09",
    )
    .await;
    assert_physical_precondition(&external).await;
    assert_index_relation_error(checked_index_audit(&external).await);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn index_context_audit_rejects_le09_under_wrong_selector_inode_or_eof() {
    for mismatch in ["inode", "eof"] {
        let mut fixture = external_fixture(false).await;
        let root = external_extent_root(&fixture).await;
        let mut extents = page(&fixture, &root).await;
        assert_eq!(extents.height, 0);
        assert_eq!(extents.records.len(), 1);
        let mut extent =
            SnapshotLargeExtent::decode_record(&extents.records[0], 2, 8 << 20).unwrap();
        match mismatch {
            "inode" => extent.inode = 3,
            "eof" => extent.file_offset = (8 << 20) - 1,
            _ => unreachable!(),
        }
        extents.records[0] = extent.record().unwrap();
        let changed = upload_index_page(
            &fixture,
            &format!("index-contexts/wrong-{mismatch}/extent-root"),
            &extents,
        )
        .await;
        replace_selector_extent_root(
            &mut fixture,
            2,
            changed,
            &format!("index-contexts/wrong-{mismatch}"),
        )
        .await;
        assert_physical_precondition(&fixture).await;
        assert_index_relation_error(checked_index_audit(&fixture).await);
    }
}

#[cfg(target_os = "linux")]
async fn two_external_fixture() -> Fixture {
    use crate::workspace::packed::snapshot::CapturedSourceLayout;
    use crate::workspace::packed::{GroupMeta, PackedGroupInput};
    use std::io::Write;

    // The existing one-file external fixture cannot exercise two real inode
    // contexts sharing an extent ref. Keep its real producer/capture recipe,
    // with exactly two sparse files and only 8 KiB of physically written data.
    let source = tempfile::tempdir().unwrap();
    let capture_scratch = tempfile::tempdir().unwrap();
    let producer_scratch = tempfile::tempdir().unwrap();
    let objects = tempfile::tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
    let options = options(PackedCodec::Zstd, false);
    let mut producer = SnapshotProducer::new(
        client.clone(),
        producer_scratch.path(),
        "two-external-index-contexts".into(),
        options.clone(),
    )
    .await
    .unwrap();
    producer.set_root_attributes(root_attributes()).unwrap();
    let mut entries = Vec::with_capacity(2);
    let mut allocations = Vec::with_capacity(2);
    for (inode, name) in [(2, "external-a"), (3, "external-b")] {
        let path = source.path().join(name);
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(&[37; 4096]).unwrap();
        file.set_len(8 << 20).unwrap();
        file.sync_all().unwrap();
        let mut captured = CapturedSourceLayout::capture_with_policy(
            &path,
            capture_scratch.path(),
            inode,
            options.profile,
            options.size_classes,
            options.build_policy,
        )
        .await
        .unwrap();
        assert_eq!(captured.data_bytes(), 4096);
        entries.push(captured.entry().clone());
        allocations.push((inode, captured.source_blocks()));
        assert_eq!(
            producer.add_external_source(&mut captured).await.unwrap(),
            1
        );
        drop(captured);
    }
    let group = PackedGroupInput {
        group_id: 1,
        parent_dir_key: options.root_dir_key,
        metadata: GroupMeta::new(entries).unwrap().encode().unwrap(),
        frame_ordinals: vec![],
        entry_count: 2,
        file_count: 2,
        layout_profile: options.profile,
    };
    producer
        .add_container(1, &[group], &[], &[1])
        .await
        .unwrap();
    for (inode, blocks) in allocations {
        producer.set_inode_blocks(inode, blocks).await.unwrap();
    }
    let reference = producer.finish().await.unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &reference)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_dir(capture_scratch.path()).unwrap().count(),
        0
    );
    assert_eq!(
        std::fs::read_dir(producer_scratch.path()).unwrap().count(),
        0
    );
    Fixture {
        objects,
        client,
        reference,
        manifest: snapshot.manifest().clone(),
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn index_context_audit_revalidates_shared_physical_extent_root_for_every_inode_context() {
    let mut fixture = two_external_fixture().await;
    assert_physical_precondition(&fixture).await;
    let accepted = checked_index_audit(&fixture).await.unwrap();
    assert_eq!(accepted.counts().contexts, 10);
    drop(accepted);

    let (_, _, first) = current_selector(&fixture, 2).await;
    let SnapshotPlacement::External {
        extents: shared, ..
    } = first
    else {
        panic!("first real external context expected")
    };
    replace_selector_extent_root(
        &mut fixture,
        3,
        shared.clone(),
        "index-contexts/shared-physical-extent",
    )
    .await;
    for inode in [2, 3] {
        let (_, _, selector) = current_selector(&fixture, inode).await;
        let SnapshotPlacement::External { extents, .. } = selector else {
            panic!("two external contexts must remain present")
        };
        assert_eq!(extents, shared);
    }
    // Canonical inodes 2 and 3 and their GroupMeta entries came from the actual
    // producer. Only inode 3's selector root has changed. Physical dedup must
    // never suppress LE09 validation against its second owner inode of 3.
    assert_physical_precondition(&fixture).await;
    assert_index_relation_error(checked_index_audit(&fixture).await);
}
