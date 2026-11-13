//! Close uses actual coordinator shutdown+join before retiring strong roots.
use super::*;
use crate::cadapter::localfs::LocalFsBackend;

fn fixture() -> (
    Arc<SnapshotMountBudget>,
    Arc<SnapshotIndexReader<LocalFsBackend>>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let budget = SnapshotMountBudget::defaults();
    let reader = Arc::new(SnapshotIndexReader::with_budget(
        ObjectClient::new(LocalFsBackend::new(dir.path())),
        0,
        budget.clone(),
    ));
    (budget, reader, dir)
}

#[tokio::test]
async fn reader_close_is_an_admission_fence_before_mount_budget_close() {
    let (budget, reader, _dir) = fixture();
    let bytes = SnapshotIndexPage {
        kind: SnapshotObjectKind::InodeIndex,
        height: 0,
        records: vec![SnapshotIndexRecord {
            first_key: vec![1],
            last_key: vec![1],
            value: SnapshotIndexValue::Leaf(vec![9]),
        }],
    }
    .encode()
    .unwrap();
    let reference = SnapshotObjectRef::from_bytes(
        "closed-reader-index".into(),
        SnapshotObjectKind::InodeIndex,
        &bytes,
    )
    .unwrap();

    // The outer session closes the budget only after transport drain. The
    // reader itself must reject new index work in that interval.
    reader.close().await;
    let error = reader.lookup(&reference, &[1]).await.unwrap_err();
    assert!(matches!(
        error,
        PackedWireError::LimitExceeded(message) if message.contains("reader is closed")
    ));
    assert_eq!(budget.state().used, [0; 8]);
}

#[tokio::test]
async fn existing_reader_close_drops_actual_idle_coordinator_and_rejects_reinitialization() {
    let (budget, reader, _dir) = fixture();
    let pipeline = reader.demand_pipeline().await.unwrap();
    let actual = Arc::downgrade(&pipeline);
    assert!(budget.state().used[SnapshotBudgetPool::Roots as usize] > 0);
    drop(pipeline);
    tokio::time::timeout(std::time::Duration::from_secs(2), reader.close())
        .await
        .unwrap();
    assert!(
        actual.upgrade().is_none(),
        "closed reader still owns actual coordinator"
    );
    assert_eq!(budget.state().used[SnapshotBudgetPool::Roots as usize], 0);
    assert!(matches!(
        reader.demand_pipeline().await,
        Err(PackedWireError::LimitExceeded(_))
    ));
}

#[tokio::test]
async fn existing_two_concurrent_reader_closes_join_and_retire_idle_coordinator() {
    let (budget, reader, _dir) = fixture();
    drop(reader.demand_pipeline().await.unwrap());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(reader.close(), reader.close());
    })
    .await
    .unwrap();
    assert_eq!(budget.state().used[SnapshotBudgetPool::Roots as usize], 0);
    assert!(matches!(
        reader.demand_pipeline().await,
        Err(PackedWireError::LimitExceeded(_))
    ));
}

#[tokio::test]
async fn existing_lazy_runtime_initialization_race_cannot_reinstall_after_close() {
    let (budget, reader, _dir) = fixture();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let init_reader = reader.clone();
    let init_barrier = barrier.clone();
    let initialize = tokio::spawn(async move {
        init_barrier.wait().await;
        init_reader.demand_pipeline().await
    });
    let close_reader = reader.clone();
    let close = tokio::spawn(async move {
        barrier.wait().await;
        close_reader.close().await;
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let result = initialize.await.unwrap();
        close.await.unwrap();
        if let Err(error) = &result {
            assert!(matches!(error, PackedWireError::LimitExceeded(_)));
        }
        drop(result);
    })
    .await
    .unwrap();
    assert_eq!(budget.state().used[SnapshotBudgetPool::Roots as usize], 0);
    assert!(matches!(
        reader.demand_pipeline().await,
        Err(PackedWireError::LimitExceeded(_))
    ));
}

#[tokio::test]
async fn existing_reader_close_retires_runtime_even_when_mount_budget_already_closed() {
    let (budget, reader, _dir) = fixture();
    drop(reader.demand_pipeline().await.unwrap());
    budget.close();
    tokio::time::timeout(std::time::Duration::from_secs(2), reader.close())
        .await
        .unwrap();
    assert_eq!(budget.state().used[SnapshotBudgetPool::Roots as usize], 0);
}

#[tokio::test]
async fn existing_held_runtime_consumer_keeps_roots_until_its_actual_final_drop() {
    let (budget, reader, _dir) = fixture();
    let held = reader.demand_pipeline().await.unwrap();
    let actual = Arc::downgrade(&held);
    reader.close().await;
    assert!(actual.upgrade().is_some());
    assert!(budget.state().used[SnapshotBudgetPool::Roots as usize] > 0);
    drop(held);
    assert!(
        actual.upgrade().is_none(),
        "reader retained coordinator after final external consumer"
    );
    assert_eq!(budget.state().used[SnapshotBudgetPool::Roots as usize], 0);
}
