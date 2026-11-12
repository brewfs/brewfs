//! Later-phase existing API red tests: raw Vec outputs and dormant runtime roots.
//! Phase02 does not satisfy these. No fake zeroing or short-lived owner is valid.
use super::*;
use crate::cadapter::localfs::LocalFsBackend;

#[tokio::test]
async fn existing_reader_close_retires_idle_coordinator_roots_without_dropping_reader() {
    let dir = tempfile::tempdir().unwrap();
    let budget = SnapshotMountBudget::defaults();
    let reader = SnapshotIndexReader::with_budget(
        ObjectClient::new(LocalFsBackend::new(dir.path())),
        0,
        budget.clone(),
    );
    let pipeline = reader.demand_pipeline().await.unwrap();
    assert!(budget.state().used[SnapshotBudgetPool::Roots as usize] > 0);
    drop(pipeline);
    tokio::time::timeout(std::time::Duration::from_secs(2), reader.close())
        .await
        .unwrap();
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Roots as usize],
        0,
        "closed reader still strongly owns coordinator channel/registry roots in OnceCell"
    );
}

#[tokio::test]
async fn existing_lookup_output_stays_admitted_after_reader_and_page_retire() {
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
        "owned-result".into(),
        SnapshotObjectKind::InodeIndex,
        &bytes,
    )
    .unwrap();
    client.put_object(&reference.key, &bytes).await.unwrap();
    let budget = SnapshotMountBudget::defaults();
    let reader = SnapshotIndexReader::with_budget(client, 1 << 20, budget.clone());
    let result = reader.lookup(&reference, &[1]).await.unwrap().unwrap();
    assert_eq!(result.as_ref(), &[9; 8192][..]);
    let retained = reader
        .pages
        .get(&(reference.digest, reference.kind, reference.object_len))
        .await
        .unwrap();
    let old_retained = Arc::downgrade(&retained);
    let old_page = Arc::downgrade(&retained.page);
    drop(retained);
    reader.close().await;
    drop(reader);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while old_retained.upgrade().is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "old retained cache wrapper/lease did not actually retire"
        );
        tokio::task::yield_now().await;
    }
    // The output remains reachable after the cache wrapper and reader retire.
    // Its owner may hold copied bytes or the admitted page; either must last
    // until the final result consumer drops.
    assert_eq!(result[8191], 9);
    assert!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize] >= 8192,
        "lookup returned allocated bytes with no owner after await/cache retirement"
    );
    drop(result);
    while old_page.upgrade().is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "last result consumer dropped but decoded page did not retire"
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize],
        0
    );
}
