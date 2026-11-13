//! Complete raw reverse paths using the actual conditional catalog and reader.
use super::*;
use crate::workspace::packed::snapshot::SnapshotBudgetPool;
use futures::FutureExt;

#[tokio::test]
async fn bounded_raw_paths_owned_root_absent_and_normal_keep_output_until_last_owner_drop() {
    let f = raw_fixture().await;
    let pool = SnapshotBudgetPool::Output as usize;
    let baseline = f.budget.state().used[pool];
    for target in [1, 999_999, 400] {
        let paths = f.meta.get_paths_bytes_owned(target).await.unwrap();
        assert_eq!(f.budget.state().used[pool], baseline + (1 << 20));
        assert!(f.budget.state().used[SnapshotBudgetPool::Metadata as usize] < 16 << 20);
        let owner = paths.guard.clone().unwrap();
        match target {
            1 => assert_eq!(paths.paths, vec![b"/".to_vec()]),
            400 => assert_eq!(paths.paths, vec![b"/nonzero-\xff".to_vec()]),
            _ => assert!(paths.paths.is_empty()),
        }
        drop(paths);
        assert_eq!(f.budget.state().used[pool], baseline + (1 << 20));
        drop(owner);
        assert_eq!(f.budget.state().used[pool], baseline);
    }
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounded_raw_paths_vfs_owned_result_retains_reader_until_final_drop() {
    let f = raw_fixture().await;
    let pool = SnapshotBudgetPool::Output as usize;
    let baseline = f.budget.state().used[pool];
    let paths = f.vfs.paths_of_bytes_owned(400).await.unwrap();
    let reader = f.reader.clone();
    let mut shutdown = tokio::spawn(async move { reader.shutdown().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(paths.paths, vec![b"/nonzero-\xff".to_vec()]);
    assert_eq!(f.budget.state().used[pool], baseline + (1 << 20));
    drop(paths);
    assert_eq!(f.budget.state().used[pool], baseline);
    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn bounded_raw_paths_cancelled_construction_releases_work_output_and_reader() {
    let f = raw_fixture().await;
    let baseline = f.budget.state().used;
    // Hold the actual catalog before its first authority read can finish.
    let rows = f.backend.rows.lock().await;
    let mut pending = Box::pin(f.meta.get_paths_bytes_owned(400));
    assert!(pending.as_mut().now_or_never().is_none());
    assert_eq!(
        f.budget.state().used[SnapshotBudgetPool::Output as usize],
        baseline[SnapshotBudgetPool::Output as usize] + (1 << 20)
    );
    assert_eq!(
        f.budget.state().used[SnapshotBudgetPool::Metadata as usize],
        baseline[SnapshotBudgetPool::Metadata as usize] + (16 << 20)
    );
    drop(pending);
    assert_eq!(f.budget.state().used, baseline);
    drop(rows);
    tokio::time::timeout(Duration::from_secs(2), f.reader.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn bounded_raw_paths_merge_all_upper_hardlinks_and_lower_whiteouts() {
    let f = raw_fixture().await;
    assert_eq!(
        f.meta.get_paths_bytes(400).await.unwrap(),
        vec![b"/nonzero-\xff".to_vec()]
    );
    f.meta.link(400, 1, "alias").await.unwrap();
    super::super::add_raw_alias(&f).await;
    assert_eq!(
        f.meta.get_paths_bytes(400).await.unwrap(),
        vec![
            b"/alias".to_vec(),
            b"/nonzero-\xff".to_vec(),
            [b"/".as_slice(), RAW_ALIAS].concat()
        ]
    );
    let mut mutation = raw_mutation(&f).await;
    mutation.dentries.push(DentryDelta::whiteout(
        f.guard.expected_head_layer_id,
        1,
        b"nonzero-\xff".to_vec(),
        0,
    ));
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    assert_eq!(
        f.meta.get_paths_bytes(400).await.unwrap(),
        vec![b"/alias".to_vec(), [b"/".as_slice(), RAW_ALIAS].concat()]
    );
    assert!(f.meta.get_paths(400).await.is_err());
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounded_raw_paths_reconstruct_raw_ancestor_and_renamed_file_without_parent_hint() {
    let f = raw_fixture().await;
    let directory = f.meta.mkdir(1, "original-dir".into()).await.unwrap();
    f.meta.link(400, directory, "before").await.unwrap();
    f.meta
        .rename(directory, "before", directory, "after".into())
        .await
        .unwrap();
    let mut mutation = raw_mutation(&f).await;
    mutation.dentries.push(DentryDelta::whiteout(
        f.guard.expected_head_layer_id,
        1,
        b"original-dir".to_vec(),
        0,
    ));
    mutation.dentries.push(DentryDelta::put(
        f.guard.expected_head_layer_id,
        1,
        b"dir-\xfe".to_vec(),
        directory,
        1,
        0,
    ));
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    assert_eq!(
        f.meta.get_paths_bytes(400).await.unwrap(),
        vec![b"/dir-\xfe/after".to_vec(), b"/nonzero-\xff".to_vec()]
    );
    assert_eq!(
        f.meta.get_paths_bytes(directory).await.unwrap(),
        vec![b"/dir-\xfe".to_vec()]
    );
    assert_eq!(
        f.vfs.paths_of_bytes(400).await.unwrap(),
        f.meta.get_paths_bytes(400).await.unwrap()
    );
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounded_raw_paths_cycle_fails_without_returning_earlier_valid_path() {
    let f = raw_fixture().await;
    let directory = f.meta.mkdir(1, "directory".into()).await.unwrap();
    f.meta.link(400, directory, "leaf").await.unwrap();
    let mut mutation = raw_mutation(&f).await;
    mutation.dentries.push(DentryDelta::whiteout(
        f.guard.expected_head_layer_id,
        1,
        b"directory".to_vec(),
        0,
    ));
    mutation.dentries.push(DentryDelta::put(
        f.guard.expected_head_layer_id,
        directory,
        b"cycle-\xff".to_vec(),
        directory,
        1,
        0,
    ));
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    let error = f.meta.get_paths_bytes(400).await.unwrap_err();
    assert!(error.to_string().contains("cyclic/overlong"));
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounded_raw_paths_global_delta_pages_continue_short_page_until_real_empty() {
    let f = raw_fixture().await;
    let mut mutation = raw_mutation(&f).await;
    for index in 0..35 {
        mutation.dentries.push(DentryDelta::put(
            f.guard.expected_head_layer_id,
            1,
            format!("alias-{index:03}").into_bytes(),
            400,
            0,
            0,
        ));
    }
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    let page = f
        .store
        .get_layer_dentry_delta_page(f.guard.expected_head_layer_id, None, f.budget.clone())
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 32);
    let last = page.rows.last().unwrap();
    let page = f
        .store
        .get_layer_dentry_delta_page(
            f.guard.expected_head_layer_id,
            Some((last.parent_ino, &last.name)),
            f.budget.clone(),
        )
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 3);
    let last = page.rows.last().unwrap();
    let page = f
        .store
        .get_layer_dentry_delta_page(
            f.guard.expected_head_layer_id,
            Some((last.parent_ino, &last.name)),
            f.budget.clone(),
        )
        .await
        .unwrap();
    assert!(page.rows.is_empty());
    let paths = f.meta.get_paths_bytes(400).await.unwrap();
    assert_eq!(paths.len(), 36);
    assert!(paths.contains(&b"/alias-034".to_vec()));
    assert!(paths.contains(&b"/nonzero-\xff".to_vec()));
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounded_raw_paths_more_than_4096_native_rows_fail_closed_not_partial() {
    let f = raw_fixture().await;
    for start in (0..4097).step_by(64) {
        let mut mutation = raw_mutation(&f).await;
        for index in start..(start + 64).min(4097) {
            mutation.dentries.push(DentryDelta::put(
                f.guard.expected_head_layer_id,
                1,
                format!("extra-{index:04}").into_bytes(),
                400,
                0,
                0,
            ));
        }
        f.store.apply_versioned_mutation(mutation).await.unwrap();
    }
    assert!(
        matches!(f.meta.get_paths_bytes(400).await, Err(crate::meta::store::MetaError::Io(error)) if error.raw_os_error() == Some(libc::E2BIG))
    );
    // Root has a complete constant path and does not need a native census.
    assert_eq!(
        f.meta.get_paths_bytes(1).await.unwrap(),
        vec![b"/".to_vec()]
    );
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounded_raw_paths_stopped_reader_rejects_before_return_or_metadata_write() {
    let f = raw_fixture().await;
    f.reader.shutdown().await.unwrap();
    let before = f.backend.rows.lock().await.clone();
    assert!(f.meta.get_paths_bytes(400).await.is_err());
    assert!(f.meta.get_paths_bytes(1).await.is_err());
    assert_eq!(*f.backend.rows.lock().await, before);
}

#[tokio::test]
async fn native_reverse_more_than_4096_unrelated_rows_only_scans_requested_inode() {
    let f = raw_fixture().await;
    for start in (0..4097).step_by(64) {
        let mut mutation = raw_mutation(&f).await;
        for index in start..(start + 64).min(4097) {
            mutation.dentries.push(DentryDelta::put(
                f.guard.expected_head_layer_id,
                1,
                format!("unrelated-{index:04}").into_bytes(),
                5000 + index as i64,
                0,
                0,
            ));
        }
        f.store.apply_versioned_mutation(mutation).await.unwrap();
    }
    f.backend.page_prefixes.lock().await.clear();
    let before = f.budget.state().used;
    assert_eq!(
        f.meta.get_paths_bytes(400).await.unwrap(),
        vec![b"/nonzero-\xff".to_vec()]
    );
    let pages = f.backend.page_prefixes.lock().await.clone();
    assert_eq!(
        pages.len(),
        2,
        "head/base empty target prefixes only: {pages:?}"
    );
    assert!(
        pages
            .iter()
            .all(|prefix| prefix.starts_with(b"packed/native-reverse/rows/")
                && prefix.ends_with(b"8000000000000190/"))
    );
    assert_eq!(f.budget.state().used, before);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn reverse_path_walk_trusts_proven_short_page_and_stays_complete() {
    let f = raw_fixture().await;
    let mut mutation = raw_mutation(&f).await;
    // 35 head aliases: one full 32-row page and a 3-row short page.
    for index in 0..35 {
        mutation.dentries.push(DentryDelta::put(
            f.guard.expected_head_layer_id,
            1,
            format!("alias-{index:03}").into_bytes(),
            400,
            0,
            0,
        ));
    }
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    let reverse_pages = |pages: &[Vec<u8>]| {
        pages
            .iter()
            .filter(|prefix| prefix.starts_with(b"packed/native-reverse/rows/"))
            .count()
    };
    // Default contract: a short page is followed by an empty proof page per
    // layer (head: 32 + 3 + empty, base: empty).
    f.backend.page_prefixes.lock().await.clear();
    let paths = f.meta.get_paths_bytes(400).await.unwrap();
    assert_eq!(paths.len(), 36);
    assert_eq!(reverse_pages(&f.backend.page_prefixes.lock().await), 4);
    // Single-range backend: the short page itself ends each layer's walk
    // (head: 32 + 3, base: one empty page), with the identical result.
    f.backend.short_page_is_end.store(true, Ordering::SeqCst);
    f.backend.page_prefixes.lock().await.clear();
    assert_eq!(f.meta.get_paths_bytes(400).await.unwrap(), paths);
    assert_eq!(reverse_pages(&f.backend.page_prefixes.lock().await), 3);
    assert!(paths.contains(&b"/alias-034".to_vec()));
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_reverse_backfill_resumes_short_pages_and_dual_writes_behind_cursor() {
    let f = raw_fixture().await;
    let layer = f.guard.expected_head_layer_id;
    let mut mutation = raw_mutation(&f).await;
    for name in [b"a", b"b", b"c", b"d", b"e"] {
        mutation
            .dentries
            .push(DentryDelta::put(layer, 1, name.to_vec(), 400, 0, 0));
    }
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    // Model an old persisted layer with no reverse completeness authority.
    f.backend
        .rows
        .lock()
        .await
        .remove(&native_reverse::state_key(layer));
    assert!(f.meta.get_paths_bytes(400).await.is_err());
    f.store
        .start_native_reverse_index(layer, f.budget.clone())
        .await
        .unwrap();
    f.backend.page_size.store(2, Ordering::SeqCst);
    assert!(
        !f.store
            .advance_native_reverse_index(layer, f.budget.clone())
            .await
            .unwrap()
    );
    assert!(f.meta.get_paths_bytes(400).await.is_err());
    let mut mutation = raw_mutation(&f).await;
    mutation.dentries.extend([
        DentryDelta::put(layer, 1, b"!behind-\xff".to_vec(), 400, 0, 0),
        DentryDelta::put(layer, 1, b"a".to_vec(), 401, 3, 0),
        DentryDelta::whiteout(layer, 1, b"b".to_vec(), 0),
    ]);
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    // A new store/process resumes the exact durable cursor and build ID.
    let restarted = KvWorkspaceStore::from_arc(f.store.backend.clone())
        .with_packed_reader_pin_budget(f.budget.clone());
    assert!(
        !restarted
            .advance_native_reverse_index(layer, f.budget.clone())
            .await
            .unwrap()
    );
    assert!(
        !restarted
            .advance_native_reverse_index(layer, f.budget.clone())
            .await
            .unwrap(),
        "short nonempty final page cannot publish Ready"
    );
    assert!(f.meta.get_paths_bytes(400).await.is_err());
    assert!(
        restarted
            .advance_native_reverse_index(layer, f.budget.clone())
            .await
            .unwrap(),
        "only a real empty page publishes Ready"
    );
    let paths = f.meta.get_paths_bytes(400).await.unwrap();
    assert_eq!(paths.len(), 5);
    for name in [
        b"/!behind-\xff".as_slice(),
        b"/c",
        b"/d",
        b"/e",
        b"/nonzero-\xff",
    ] {
        assert!(paths.contains(&name.to_vec()), "{paths:?}");
    }
    assert!(!paths.contains(&b"/a".to_vec()));
    assert!(!paths.contains(&b"/b".to_vec()));
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_reverse_preinstall_writer_and_old_query_authority_lose_state_cas() {
    let f = raw_fixture().await;
    let layer = f.guard.expected_head_layer_id;
    f.backend
        .rows
        .lock()
        .await
        .remove(&native_reverse::state_key(layer));
    let old = f.store.load_layer(layer).await.unwrap();
    let mut next = old.clone();
    next.next_sequence += 1;
    let row = DentryDelta::put(layer, 1, b"preinstall".to_vec(), 400, 0, old.next_sequence);
    let mut checks = vec![KvCheck {
        key: hot_layer_key(layer),
        expected: Some(encode(&old).unwrap()),
    }];
    let mut writes = vec![
        put(dentry_key(&row), &row).unwrap(),
        put(hot_layer_key(layer), &next).unwrap(),
    ];
    let _owner = f
        .store
        .prepare_native_reverse_cas(&mut checks, &mut writes)
        .await
        .unwrap();
    f.store
        .start_native_reverse_index(layer, f.budget.clone())
        .await
        .unwrap();
    assert!(!f.backend.compare_and_swap(&checks, &writes).await.unwrap());
    assert_eq!(f.backend.get(&dentry_key(&row)).await.unwrap(), None);
    assert!(
        f.store
            .advance_native_reverse_index(layer, f.budget.clone())
            .await
            .unwrap()
    );
    let layers = f.store.load_layer_chain(layer).await.unwrap();
    let proof = f
        .store
        .get_native_reverse_authority(&layers, f.budget.clone())
        .await
        .unwrap();
    f.store
        .start_native_reverse_index(layer, f.budget.clone())
        .await
        .unwrap();
    assert!(matches!(
        f.store
            .confirm_native_reverse_authority(&proof, f.budget.clone())
            .await,
        Err(WorkspaceError::Busy)
    ));
    drop(proof);
    drop(_owner);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_reverse_rejects_corrupt_state_and_stale_forward_value() {
    let f = raw_fixture().await;
    let layer = f.guard.expected_head_layer_id;
    let mut mutation = raw_mutation(&f).await;
    mutation
        .dentries
        .push(DentryDelta::put(layer, 1, b"stale".to_vec(), 400, 0, 0));
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    let key = dentry_identity_key(layer, 1, b"stale");
    let row: DentryDelta = decode(&f.backend.get(&key).await.unwrap().unwrap()).unwrap();
    f.backend.rows.lock().await.insert(
        key,
        encode(&DentryDelta::whiteout(
            layer,
            1,
            b"stale".to_vec(),
            row.sequence,
        ))
        .unwrap(),
    );
    assert!(
        f.meta
            .get_paths_bytes(400)
            .await
            .unwrap_err()
            .to_string()
            .contains("stale native reverse")
    );
    f.backend
        .rows
        .lock()
        .await
        .insert(native_reverse::state_key(layer), b"corrupt".to_vec());
    assert!(f.meta.get_paths_bytes(400).await.is_err());
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_reverse_inventory_change_and_deleting_layer_cannot_finish_old_build() {
    let f = raw_fixture().await;
    let layer = f.guard.expected_head_layer_id;
    f.store
        .start_native_reverse_index(layer, f.budget.clone())
        .await
        .unwrap();
    let raw = f.backend.get(LAYER_INVENTORY_GENERATION_KEY).await.unwrap();
    f.backend.rows.lock().await.insert(
        LAYER_INVENTORY_GENERATION_KEY.to_vec(),
        encode(&next_layer_inventory_generation(&raw).unwrap()).unwrap(),
    );
    assert!(matches!(
        f.store
            .advance_native_reverse_index(layer, f.budget.clone())
            .await,
        Err(WorkspaceError::Busy)
    ));
    f.store
        .start_native_reverse_index(layer, f.budget.clone())
        .await
        .unwrap();
    let mut record = f.store.load_layer(layer).await.unwrap();
    record.state = LayerState::Deleting;
    f.backend
        .rows
        .lock()
        .await
        .insert(hot_layer_key(layer), encode(&record).unwrap());
    assert!(matches!(
        f.store
            .advance_native_reverse_index(layer, f.budget.clone())
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert!(
        f.store
            .start_native_reverse_index(layer, f.budget.clone())
            .await
            .is_err()
    );
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_reverse_maintenance_quota_and_cancel_leave_persisted_cursor_unchanged() {
    let f = raw_fixture().await;
    let layer = f.guard.expected_head_layer_id;
    let key = native_reverse::state_key(layer);
    let before = f.backend.get(&key).await.unwrap();
    let metadata = SnapshotBudgetPool::Metadata as usize;
    let remaining = crate::workspace::packed::snapshot::SnapshotBudgetLimits::default().bytes
        [metadata]
        - f.budget.state().used[metadata];
    let full = f
        .budget
        .admit(&[(SnapshotBudgetPool::Metadata, remaining)])
        .unwrap();
    assert!(
        f.store
            .start_native_reverse_index(layer, f.budget.clone())
            .await
            .is_err()
    );
    drop(full);
    assert_eq!(f.backend.get(&key).await.unwrap(), before);
    f.store
        .start_native_reverse_index(layer, f.budget.clone())
        .await
        .unwrap();
    let before = f.backend.get(&key).await.unwrap();
    let used = f.budget.state().used;
    let rows = f.backend.rows.lock().await;
    let mut step = Box::pin(
        f.store
            .advance_native_reverse_index(layer, f.budget.clone()),
    );
    assert!(step.as_mut().now_or_never().is_none());
    assert!(f.budget.state().used[metadata] > used[metadata]);
    drop(step);
    assert_eq!(f.budget.state().used, used);
    drop(rows);
    assert_eq!(f.backend.get(&key).await.unwrap(), before);
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_reverse_backfill_cas_loses_to_real_writer_after_page_snapshot() {
    let f = raw_fixture().await;
    let layer = f.guard.expected_head_layer_id;
    let mut mutation = raw_mutation(&f).await;
    mutation
        .dentries
        .push(DentryDelta::put(layer, 1, b"a".to_vec(), 400, 0, 0));
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    f.store
        .start_native_reverse_index(layer, f.budget.clone())
        .await
        .unwrap();
    let state_key = native_reverse::state_key(layer);
    let before = f.backend.get(&state_key).await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *f.backend.page_barrier.lock().await = Some((entered.clone(), resume.clone()));
    let store = f.store.clone();
    let budget = f.budget.clone();
    let builder =
        tokio::spawn(async move { store.advance_native_reverse_index(layer, budget).await });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    let mut mutation = raw_mutation(&f).await;
    mutation.dentries.extend([
        DentryDelta::put(layer, 1, b"a".to_vec(), 401, 3, 0),
        DentryDelta::put(layer, 1, b"!race-\xff".to_vec(), 400, 0, 0),
    ]);
    f.store.apply_versioned_mutation(mutation).await.unwrap();
    resume.notify_one();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), builder)
            .await
            .unwrap()
            .unwrap(),
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(
        f.backend.get(&state_key).await.unwrap(),
        before,
        "failed page CAS must not move the durable cursor"
    );
    assert!(
        !f.store
            .advance_native_reverse_index(layer, f.budget.clone())
            .await
            .unwrap()
    );
    assert!(
        f.store
            .advance_native_reverse_index(layer, f.budget.clone())
            .await
            .unwrap()
    );
    let paths = f.meta.get_paths_bytes(400).await.unwrap();
    assert_eq!(
        paths,
        vec![b"/!race-\xff".to_vec(), b"/nonzero-\xff".to_vec()]
    );
    f.reader.shutdown().await.unwrap();
}
