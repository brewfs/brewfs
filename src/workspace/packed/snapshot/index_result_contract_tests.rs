//! Existing APIs: result owners must survive reader/cache retirement.
use super::*;
use crate::cadapter::localfs::LocalFsBackend;

async fn fixture(
    cache: u64,
) -> (
    SnapshotObjectRef,
    Arc<SnapshotMountBudget>,
    SnapshotIndexReader<LocalFsBackend>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let client = ObjectClient::new(LocalFsBackend::new(dir.path()));
    let bytes = SnapshotIndexPage {
        kind: SnapshotObjectKind::InodeIndex,
        height: 0,
        records: vec![SnapshotIndexRecord {
            first_key: vec![1],
            last_key: vec![1],
            value: SnapshotIndexValue::Leaf(vec![9; 8192]),
        }],
    }
    .encode()
    .unwrap();
    let reference = SnapshotObjectRef::from_bytes(
        "result-contract".into(),
        SnapshotObjectKind::InodeIndex,
        &bytes,
    )
    .unwrap();
    client.put_object(&reference.key, &bytes).await.unwrap();
    let budget = SnapshotMountBudget::defaults();
    let reader = SnapshotIndexReader::with_budget(client, cache, budget.clone());
    (reference, budget, reader, dir)
}

#[tokio::test]
async fn existing_scan_result_and_cloned_record_keep_metadata_until_final_consumer() {
    let (reference, budget, reader, _dir) = fixture(0).await;
    let rows = reader
        .scan_page(&reference, &[1], &[2], None, 1)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].first_key, vec![1]);
    assert_eq!(rows[0].value, SnapshotIndexValue::Leaf(vec![9; 8192]));
    let held = rows[0].clone();
    reader.close().await;
    drop(reader);
    assert!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize] >= 8192,
        "scan result outlived its admitted page with no allocation owner"
    );
    drop(rows);
    assert!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize] >= 8192,
        "cloned record detached bytes from their final-consumer owner"
    );
    assert_eq!(held.first_key, vec![1]);
    drop(held);
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize],
        0
    );
}

#[tokio::test]
async fn existing_warm_scan_rejects_full_metadata_before_allocating_result_slots() {
    let (reference, budget, reader, _dir) = fixture(1 << 20).await;
    let rows = reader
        .scan_page(&reference, &[1], &[2], None, 1)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    drop(rows);
    let used = budget.state().used[SnapshotBudgetPool::Metadata as usize];
    assert!(used >= 8192, "warm retained-page precondition absent");
    let remainder = budget.capacity(SnapshotBudgetPool::Metadata) - used;
    let held = budget
        .admit(&[(SnapshotBudgetPool::Metadata, remainder)])
        .unwrap();
    let result = reader.scan_page(&reference, &[1], &[2], None, 1).await;
    assert!(
        matches!(result, Err(PackedWireError::LimitExceeded(_))),
        "warm scan created result allocations while Metadata was full"
    );
    drop(held);
    assert_eq!(
        reader
            .scan_page(&reference, &[1], &[2], None, 1)
            .await
            .unwrap()
            .len(),
        1
    );
    reader.close().await;
}

#[tokio::test]
async fn existing_overlap_scan_keeps_result_admitted_after_reader_retirement() {
    let (reference, budget, reader, _dir) = fixture(0).await;
    let rows = reader
        .scan_overlaps_page(&reference, &[1], &[2], None, 1)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].value, SnapshotIndexValue::Leaf(vec![9; 8192]));
    reader.close().await;
    drop(reader);
    assert!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize] >= 8192,
        "overlap output lost its admission owner after await"
    );
    drop(rows);
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize],
        0
    );
}

#[tokio::test]
async fn existing_lookup_clone_keeps_metadata_after_original_and_reader_drop() {
    let (reference, budget, reader, _dir) = fixture(0).await;
    let result = reader.lookup(&reference, &[1]).await.unwrap().unwrap();
    let held = result.clone();
    reader.close().await;
    drop(reader);
    drop(result);
    assert_eq!(held[8191], 9);
    assert!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize] >= 8192,
        "lookup clone detached the final consumer from allocation ownership"
    );
    drop(held);
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize],
        0
    );
}

#[tokio::test]
async fn existing_weighted_scan_result_keeps_metadata_after_reader_retirement() {
    let (reference, budget, reader, _dir) = fixture(0).await;
    let rows = reader
        .scan_weighted_page(&reference, &[1], &[2], 0, 1, 1)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, 0);
    assert_eq!(rows[0].0.value, SnapshotIndexValue::Leaf(vec![9; 8192]));
    reader.close().await;
    drop(reader);
    assert_eq!(budget.state().used[SnapshotBudgetPool::Control as usize], 0);
    assert!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize] >= 8192,
        "weighted scan output lost its last-consumer allocation owner"
    );
    drop(rows);
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize],
        0
    );
}
