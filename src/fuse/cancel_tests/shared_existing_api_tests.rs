//! Actual adapter reads, using only the existing green03 fixture and public API.
use super::*;
use crate::cadapter::read_observer::ReadEvent;
use crate::workspace::packed::snapshot::SnapshotBudgetPool;

#[tokio::test]
async fn g09_existing_api_concurrent_same_frame_reads_share_one_real_body() {
    let f = fixture_with_budget(constrained_plans_budget()).await;
    assert_shared_real_body(f).await;
}

#[tokio::test]
async fn minimum_plans_existing_api_same_frame_reads_share_one_real_body() {
    let f = fixture_with_budget(constrained_plans_budget_with_capacity(2 << 20)).await;
    assert_shared_real_body(f).await;
}

async fn assert_shared_real_body(f: Fixture) {
    let opened = Filesystem::open(&f.fs, request(601), f.ino, libc::O_RDONLY as u32)
        .await
        .unwrap();
    f.gate.mode.store(3, Ordering::SeqCst);
    let first_fs = f.fs.clone();
    let ino = f.ino;
    let fh = opened.fh;
    let first =
        tokio::spawn(
            async move { Filesystem::read(&first_fs, request(602), ino, fh, 0, 8192).await },
        );
    tokio::time::timeout(Duration::from_secs(2), f.gate.entered.notified())
        .await
        .expect("the leader did not open its real LocalFS payload stream");
    let second_fs = f.fs.clone();
    let second =
        tokio::spawn(
            async move { Filesystem::read(&second_fs, request(603), ino, fh, 0, 8192).await },
        );

    // Hold the first real body open. Drive the second read until it either
    // joins that flight, opens a duplicate body, or actually returns an error.
    // This uses observable progress, without a sleep-based race assumption.
    let progress = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let shared = f
                .observer
                .snapshot()
                .events
                .iter()
                .filter(|((context, event), _)| {
                    context.class == ReadClass::PackedPayload
                        && *event == ReadEvent::SharedResultAfterMiss
                })
                .map(|(_, count)| *count)
                .sum::<u64>();
            if shared != 0
                || second.is_finished()
                || f.gate.payload_bodies.load(Ordering::SeqCst) > 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    f.gate.released.store(true, Ordering::SeqCst);
    f.gate.resumed.notify_waiters();
    let first = tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .expect("leader did not finish after its body was released")
        .unwrap()
        .expect("leader read failed");
    let second = tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .expect("follower did not finish after the shared body was released")
        .unwrap()
        .expect("same-frame follower was refused or failed instead of sharing");
    progress.expect("follower made no observable progress while the real body was held");
    assert_eq!(first.data.as_ref(), &[0x75; 8192]);
    assert_eq!(second.data.as_ref(), &[0x75; 8192]);
    assert_eq!(
        f.gate.payload_bodies.load(Ordering::SeqCst),
        1,
        "concurrent same-frame reads opened duplicate physical payload bodies"
    );
    assert!(
        f.observer
            .snapshot()
            .rows
            .values()
            .all(|row| row.conserved())
    );
    assert!(
        f.budget.state().peak[SnapshotBudgetPool::Plans as usize]
            <= f.budget.capacity(SnapshotBudgetPool::Plans)
    );
    assert_eq!(f.budget.state().rejections, 0);
    assert_eq!(f.budget.state().used[SnapshotBudgetPool::Raw as usize], 0);
    f.fs.close(fh).await.unwrap();
}

#[tokio::test]
async fn constrained_plans_existing_api_inline_read_keeps_index_admission_room() {
    let f = fixture_with_source_files(constrained_plans_budget(), &[("payload", 8192, 0x36)]).await;
    assert_inline_read(f).await;
}

#[tokio::test]
async fn minimum_plans_existing_api_inline_read_keeps_index_admission_room() {
    let f = fixture_with_source_files(
        constrained_plans_budget_with_capacity(2 << 20),
        &[("payload", 8192, 0x36)],
    )
    .await;
    assert_inline_read(f).await;
}

async fn assert_inline_read(f: Fixture) {
    let opened = Filesystem::open(&f.fs, request(1701), f.ino, libc::O_RDONLY as u32)
        .await
        .unwrap();
    let idle = f.budget.state().used;
    for unique in [1702, 1703] {
        let request_owner = Filesystem::reserve_request_memory(&f.fs, 40)
            .unwrap()
            .unwrap();
        let reply = Filesystem::read(&f.fs, request(unique), f.ino, opened.fh, 0, 8192)
            .await
            .expect("validated Plans cannot admit the inline read and its index recipes");
        assert_eq!(reply.data.as_ref(), &[0x36; 8192]);
        drop(reply);
        drop(request_owner);
        assert_eq!(f.budget.state().used, idle, "inline read leaked owners");
    }
    let snapshot = f.observer.snapshot();
    assert!(snapshot.rows.values().all(|row| row.conserved()));
    assert!(
        snapshot.rows.iter().all(|((ledger, context), row)| {
            *ledger != Ledger::BackendBody
                || !matches!(
                    context.class,
                    ReadClass::PackedPayload | ReadClass::ExternalPayload
                )
                || row.started == 0
        }),
        "inline read opened a separate payload body"
    );
    assert_eq!(f.budget.state().rejections, 0);
    f.fs.close(opened.fh).await.unwrap();
}
