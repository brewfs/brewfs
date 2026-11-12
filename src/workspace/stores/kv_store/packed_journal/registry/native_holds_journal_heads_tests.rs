use super::*;
use crate::workspace::stores::kv_store::packed_journal::tests::JournalMemoryBackend;

fn sealed() -> LayerRecord {
    LayerRecord {
        layer_id: LayerId::new(),
        parent_layer_id: None,
        state: LayerState::Sealed,
        schema_version: WORKSPACE_SCHEMA_VERSION,
        sealed_version: Some(1),
        delta_digest: Some([11; 32]),
        root_hash: Some([22; 32]),
        depth: 1,
        owner_workspace_id: None,
        next_sequence: 1,
        owned_slice_count: 0,
        owned_bytes: 0,
        created_at_ns: 1,
        sealed_at_ns: Some(2),
    }
}

fn journal(old: LayerId, new: LayerId) -> SealJournal {
    SealJournal {
        journal_id: JournalId::new(),
        workspace_id: WorkspaceId::new(),
        old_head_layer_id: old,
        expected_head_epoch: 1,
        phase: SealPhase::HeadSwitched,
        pending_bytes: 0,
        delta_digest: None,
        root_hash: None,
        new_head_layer_id: Some(new),
        last_error: None,
        created_at_ns: 1,
        updated_at_ns: 2,
    }
}

#[test]
fn unfinished_journal_has_two_distinct_holds_and_completion_releases_both() {
    let row = journal(LayerId::new(), LayerId::new());
    let old = NativeHold::journal(&row).unwrap();
    let new = NativeHold::journal_new_head(&row).unwrap();
    assert_ne!(old.key(), new.key());
    assert_eq!(NativeHold::decode(&old.encode().unwrap()).unwrap(), old);
    assert_eq!(NativeHold::decode(&new.encode().unwrap()).unwrap(), new);
    let mut before = ControlState::default();
    before.journals.insert(row.journal_id, row.clone());
    for phase in [SealPhase::Completed, SealPhase::Aborted] {
        let mut after = before.clone();
        after.journals.get_mut(&row.journal_id).unwrap().phase = phase;
        let (checks, writes) = test_entity_packet(&before, &after);
        let changes = owner_changes(&checks, &writes).unwrap();
        assert_eq!(changes.len(), 2);
        for expected in [&old, &new] {
            let change = changes.iter().find(|c| c.key == expected.key()).unwrap();
            assert_eq!(change.old.as_ref(), Some(expected));
            assert_eq!(change.new, None);
        }
    }
}

#[test]
fn publishing_or_replacing_new_journal_head_is_a_real_owner_change() {
    let mut row = journal(LayerId::new(), LayerId::new());
    let mut before = ControlState::default();
    before.journals.insert(row.journal_id, row.clone());
    for next in [Some(LayerId::new()), None] {
        let mut after = before.clone();
        row.new_head_layer_id = next;
        after.journals.insert(row.journal_id, row.clone());
        let (checks, writes) = test_entity_packet(&before, &after);
        let changes = owner_changes(&checks, &writes).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(
            changes[0].key,
            NativeHold::journal_new_head(before.journals.values().next().unwrap())
                .unwrap()
                .key()
        );
        assert_eq!(changes[0].new, NativeHold::journal_new_head(&row));
        before = after;
    }
}

#[tokio::test]
async fn migration_certifies_both_journal_heads_before_zero_owner_census() {
    let backend = Arc::new(JournalMemoryBackend::default());
    let store = Arc::new(KvWorkspaceStore::from_arc(backend.clone()));
    let budget = SnapshotMountBudget::defaults();
    let old = sealed();
    let new = sealed();
    let row = journal(old.layer_id, new.layer_id);
    let mut control = ControlState::default();
    control.journals.insert(row.journal_id, row.clone());
    {
        let mut rows = backend.rows.lock().await;
        test_write_topology_rows(&mut rows, &control);
        rows.insert(hot_layer_key(old.layer_id), encode(&old).unwrap());
        rows.insert(hot_layer_key(new.layer_id), encode(&new).unwrap());
    }
    assert_eq!(
        store
            .migrate_native_packed_holds(&budget, 8, CancellationToken::new())
            .await
            .unwrap(),
        1,
        "one authoritative journal row installs both distinct head holds"
    );
    {
        let rows = backend.rows.lock().await;
        assert_eq!(rows.get(HOLD_FEATURE), rows.get(HOLD_JOURNAL_HEADS_FEATURE));
        for hold in [
            NativeHold::journal(&row),
            NativeHold::journal_new_head(&row),
        ]
        .into_iter()
        .flatten()
        {
            assert_eq!(rows.get(&hold.key()), Some(&hold.encode().unwrap()));
        }
    }
    for target in [&old, &new] {
        let census = store
            .native_packed_hold_census(
                &revision_from_layer(target).unwrap(),
                8,
                &budget,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(census.matched(), 1);
    }
}

#[tokio::test]
async fn legacy_active_gate_does_not_certify_missing_new_head_projection() {
    let backend = Arc::new(JournalMemoryBackend::default());
    let store = Arc::new(KvWorkspaceStore::from_arc(backend.clone()));
    let budget = SnapshotMountBudget::defaults();
    let old = sealed();
    let new = sealed();
    let row = journal(old.layer_id, new.layer_id);
    let mut control = ControlState::default();
    control.journals.insert(row.journal_id, row.clone());
    {
        let mut rows = backend.rows.lock().await;
        test_write_topology_rows(&mut rows, &control);
        rows.insert(hot_layer_key(old.layer_id), encode(&old).unwrap());
        rows.insert(hot_layer_key(new.layer_id), encode(&new).unwrap());
        rows.insert(
            HOLD_FEATURE.to_vec(),
            encode(&NativeHoldGate {
                run: Uuid::new_v4(),
                active: true,
            })
            .unwrap(),
        );
        let hold = NativeHold::journal(&row).unwrap();
        rows.insert(hold.key(), hold.encode().unwrap());
    }
    let target = revision_from_layer(&new).unwrap();
    assert!(matches!(
        store
            .native_packed_hold_census(&target, 8, &budget, CancellationToken::new())
            .await,
        Err(WorkspaceError::Fenced)
    ));
    store
        .migrate_native_packed_holds(&budget, 8, CancellationToken::new())
        .await
        .unwrap();
    let census = store
        .native_packed_hold_census(&target, 8, &budget, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(census.matched(), 1);
}

fn snapshot(revision: BaseRevision) -> SnapshotRecord {
    SnapshotRecord {
        snapshot_id: SnapshotId::new(),
        name: Some("guarded-root".into()),
        revision,
        owner_id: None,
        created_at_ns: 1,
    }
}

#[tokio::test]
async fn native_root_birth_rejects_deleting_start_or_ancestor_without_active_gate() {
    for deleting_parent in [false, true] {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = KvWorkspaceStore::from_arc(backend.clone());
        store
            .configure_packed_reader_pin_budget(SnapshotMountBudget::defaults())
            .unwrap();
        let mut parent = sealed();
        let mut child = sealed();
        child.parent_layer_id = Some(parent.layer_id);
        child.depth = 2;
        let revision = revision_from_layer(&child).unwrap();
        if deleting_parent {
            parent.state = LayerState::Deleting;
        } else {
            child.state = LayerState::Deleting;
        }
        {
            let mut rows = backend.rows.lock().await;
            rows.insert(hot_layer_key(parent.layer_id), encode(&parent).unwrap());
            rows.insert(hot_layer_key(child.layer_id), encode(&child).unwrap());
        }
        let new = snapshot(revision);
        let before = backend.rows.lock().await.clone();
        let mut checks = vec![KvCheck {
            key: hot_snapshot_key(new.snapshot_id),
            expected: None,
        }];
        let mut writes = vec![put(hot_snapshot_key(new.snapshot_id), &new).unwrap()];
        assert!(matches!(
            store
                .prepare_native_owner_cas(&mut checks, &mut writes)
                .await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(*backend.rows.lock().await, before);
    }
}

#[tokio::test]
async fn native_root_birth_cas_loses_to_deleting_after_the_chain_read() {
    let backend = Arc::new(JournalMemoryBackend::default());
    let store = KvWorkspaceStore::from_arc(backend.clone());
    store
        .configure_packed_reader_pin_budget(SnapshotMountBudget::defaults())
        .unwrap();
    let mut layer = sealed();
    let new = snapshot(revision_from_layer(&layer).unwrap());
    backend
        .rows
        .lock()
        .await
        .insert(hot_layer_key(layer.layer_id), encode(&layer).unwrap());
    let mut checks = vec![KvCheck {
        key: hot_snapshot_key(new.snapshot_id),
        expected: None,
    }];
    let mut writes = vec![put(hot_snapshot_key(new.snapshot_id), &new).unwrap()];
    let _owner = store
        .prepare_native_owner_cas(&mut checks, &mut writes)
        .await
        .unwrap();
    let exact = checks
        .iter()
        .find(|check| check.key == hot_layer_key(layer.layer_id))
        .unwrap();
    assert_eq!(exact.expected, Some(encode(&layer).unwrap()));
    layer.state = LayerState::Deleting;
    backend
        .rows
        .lock()
        .await
        .insert(hot_layer_key(layer.layer_id), encode(&layer).unwrap());
    assert!(!backend.compare_and_swap(&checks, &writes).await.unwrap());
    assert!(
        !backend
            .rows
            .lock()
            .await
            .contains_key(&hot_snapshot_key(new.snapshot_id))
    );
}

#[tokio::test]
async fn planned_journal_head_authenticates_absence_and_rejects_deleting_reuse() {
    for deleting in [false, true] {
        let backend = Arc::new(JournalMemoryBackend::default());
        let store = KvWorkspaceStore::from_arc(backend.clone());
        store
            .configure_packed_reader_pin_budget(SnapshotMountBudget::defaults())
            .unwrap();
        let old = sealed();
        let mut planned = sealed();
        let row = journal(old.layer_id, planned.layer_id);
        let mut state = ControlState::default();
        state.journals.insert(row.journal_id, row);
        {
            let mut rows = backend.rows.lock().await;
            rows.insert(hot_layer_key(old.layer_id), encode(&old).unwrap());
            if deleting {
                planned.state = LayerState::Deleting;
                rows.insert(hot_layer_key(planned.layer_id), encode(&planned).unwrap());
            }
        }
        let (mut checks, mut writes) = test_entity_packet(&ControlState::default(), &state);
        let result = store
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await;
        if deleting {
            assert!(matches!(result, Err(WorkspaceError::Fenced)));
        } else {
            let _owner = result.unwrap();
            assert_eq!(
                checks
                    .iter()
                    .find(|check| check.key == hot_layer_key(planned.layer_id))
                    .unwrap()
                    .expected,
                None
            );
            assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
        }
    }
}

#[tokio::test]
async fn planned_head_born_in_same_packet_must_authenticate_its_parent_chain() {
    let backend = Arc::new(JournalMemoryBackend::default());
    let store = KvWorkspaceStore::from_arc(backend.clone());
    store
        .configure_packed_reader_pin_budget(SnapshotMountBudget::defaults())
        .unwrap();
    let old = sealed();
    let mut parent = sealed();
    parent.state = LayerState::Deleting;
    let mut planned = sealed();
    planned.parent_layer_id = Some(parent.layer_id);
    planned.depth = 2;
    let row = journal(old.layer_id, planned.layer_id);
    let mut state = ControlState::default();
    state.journals.insert(row.journal_id, row);
    {
        let mut rows = backend.rows.lock().await;
        rows.insert(hot_layer_key(old.layer_id), encode(&old).unwrap());
        rows.insert(hot_layer_key(parent.layer_id), encode(&parent).unwrap());
    }
    let (mut checks, mut writes) = test_entity_packet(&ControlState::default(), &state);
    checks.push(KvCheck {
        key: hot_layer_key(planned.layer_id),
        expected: None,
    });
    writes.push(put(hot_layer_key(planned.layer_id), &planned).unwrap());
    assert!(matches!(
        store
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert!(
        !backend
            .rows
            .lock()
            .await
            .contains_key(&hot_layer_key(planned.layer_id))
    );
}
