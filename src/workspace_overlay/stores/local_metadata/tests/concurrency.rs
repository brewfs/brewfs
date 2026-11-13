use super::*;

#[tokio::test]
async fn untimed_reads_merge_local_values_without_removing_mutation_clock_checks() {
    for durable in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let remote = Arc::new(Memory::default());
        remote
            .rows
            .lock()
            .await
            .insert(b"unchanged".to_vec(), Some(vec![9]));
        let mut cfg = config(dir.path());
        cfg.sync_on_update = durable;
        let b = backend(remote.clone(), cfg);
        edit(&b, None, 1).await;
        let count = remote.timed_reads.load(Ordering::SeqCst);
        let keys = [b"inode/1".to_vec(), b"unchanged".to_vec()];
        assert_eq!(
            b.get_many(&keys).await.unwrap(),
            vec![Some(vec![1]), Some(vec![9])]
        );
        assert_eq!(
            b.get_many_consistent(&keys).await.unwrap(),
            vec![Some(vec![1]), Some(vec![9])]
        );
        assert_eq!(remote.timed_reads.load(Ordering::SeqCst), count);
        assert_eq!(remote.commits.load(Ordering::SeqCst), 0);
        b.compare_and_swap_metadata_before(
            &check(Some(1)),
            &[KvWrite::Delete {
                key: keys[0].clone(),
            }],
            10000,
        )
        .await
        .unwrap();
        assert_eq!(
            b.get_many_consistent(&keys).await.unwrap(),
            vec![None, Some(vec![9])]
        );
        let count = remote.timed_reads.load(Ordering::SeqCst);
        b.get_many_consistent_with_time(&keys).await.unwrap();
        assert_eq!(remote.timed_reads.load(Ordering::SeqCst), count + 1);
        remote.now.store(10000, Ordering::SeqCst);
        assert!(matches!(
            b.compare_and_swap_metadata_before(&check(None), &put(2), 10000)
                .await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(remote.timed_reads.load(Ordering::SeqCst), count + 2);
    }
}

/// The reference calls the old timed wrapper directly; no production switch.
#[tokio::test]
#[ignore = "requires BREWFS_LOCAL_SYNC_REDIS_URL pointing at disposable local Redis"]
async fn local_metadata_untimed_redis_pairs() {
    use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
    let url = std::env::var("BREWFS_LOCAL_SYNC_REDIS_URL").unwrap();
    assert!(url.starts_with("redis://127.0.0.1:") || url.starts_with("redis://localhost:"));
    let client = redis::Client::open(url.clone()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    for pair in 0..3 {
        for timed in if pair % 2 == 0 {
            [true, false]
        } else {
            [false, true]
        } {
            let namespace = format!("local-read-{}", uuid::Uuid::new_v4());
            let remote = RedisWorkspaceBackend::connect(&url, &namespace)
                .await
                .unwrap();
            assert!(
                remote
                    .compare_and_swap(&check(None), &put(9))
                    .await
                    .unwrap()
            );
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = config(dir.path());
            cfg.sync_on_update = false;
            let b = LocalMetadataBackend::new(remote, Some(cfg), namespace.as_bytes())
                .await
                .unwrap();
            let before: String = redis::cmd("INFO")
                .arg("commandstats")
                .query_async(&mut connection)
                .await
                .unwrap();
            let keys = [b"inode/1".to_vec()];
            let start = std::time::Instant::now();
            let mut samples = Vec::new();
            for _ in 0..100 {
                let operation = std::time::Instant::now();
                let values = if timed {
                    b.overlay_values(&keys, None).await.unwrap().0
                } else {
                    b.get_many_consistent(&keys).await.unwrap()
                };
                assert_eq!(values, vec![Some(vec![9])]);
                samples.push(operation.elapsed().as_nanos() as u64);
            }
            let elapsed_ns = start.elapsed().as_nanos() as u64;
            let after: String = redis::cmd("INFO")
                .arg("commandstats")
                .query_async(&mut connection)
                .await
                .unwrap();
            println!(
                "UNTIMED_READ_RESULT {}",
                serde_json::json!({"pair":pair,"timed_reference":timed,"operations":100,"elapsed_ns":elapsed_ns,"operation_ns":samples,"redis_before":before,"redis_after":after})
            );
        }
    }
}

#[tokio::test]
async fn buffered_journal_io_does_not_block_new_edits_even_if_waiter_is_cancelled() {
    for pause_ack in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let remote = Arc::new(Memory::default());
        let mut cfg = config(dir.path());
        cfg.sync_on_update = false;
        let b = Arc::new(backend(remote.clone(), cfg.clone()));
        edit(&b, None, 1).await;
        let (reached, wait) = tokio::sync::oneshot::channel();
        let (resume, pause) = std::sync::mpsc::channel();
        if pause_ack {
            remote.pause.store(true, Ordering::SeqCst);
        } else {
            *b.core.as_ref().unwrap().journal.pause.lock().unwrap() = Some((reached, pause));
        }
        let core = b.core.clone().unwrap();
        let first = tokio::spawn(async move { core.flush_to(1).await });
        // The ACK branch needs its own pause handles because the freeze branch
        // moved the first ones. Use the remote gate to install the ACK pause.
        let (wait, resume) = if pause_ack {
            remote.reached.acquire().await.unwrap().forget();
            let (reached, wait) = tokio::sync::oneshot::channel();
            let (resume, pause) = std::sync::mpsc::channel();
            *b.core.as_ref().unwrap().journal.pause.lock().unwrap() = Some((reached, pause));
            remote.resume.add_permits(1);
            (wait, resume)
        } else {
            (wait, resume)
        };
        wait.await.unwrap();
        let edited = tokio::time::timeout(Duration::from_secs(2), edit(&b, Some(1), 2)).await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        // Always release the blocking thread before asserting, including when
        // running the regression test against the old locking implementation.
        resume.send(()).unwrap();
        b.flush_metadata().await.unwrap();
        assert!(
            edited.is_ok(),
            "journal I/O blocked a buffered edit; ACK={pause_ack}"
        );
        assert_eq!(b.get(b"inode/1").await.unwrap(), Some(vec![2]));
        assert_eq!(remote.get(b"inode/1").await.unwrap(), Some(vec![2]));
        drop(b);
        let reopened = backend(remote.clone(), cfg);
        assert_eq!(reopened.get(b"inode/1").await.unwrap(), Some(vec![2]));
    }
}

#[tokio::test]
async fn buffered_failed_freeze_is_persisted_before_retrying_remote_commit() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(Memory::default());
    let mut cfg = config(dir.path());
    cfg.sync_on_update = false;
    let b = backend(remote.clone(), cfg);
    edit(&b, None, 1).await;
    let obstruction = dir.path().join("state.tmp");
    std::fs::create_dir(&obstruction).unwrap();
    assert!(b.flush_metadata().await.is_err());
    assert_eq!(remote.commits.load(Ordering::SeqCst), 0);
    assert_eq!(b.get(b"inode/1").await.unwrap(), Some(vec![1]));
    std::fs::remove_dir(obstruction).unwrap();
    b.flush_metadata().await.unwrap();
    assert_eq!(remote.commits.load(Ordering::SeqCst), 1);
    assert_eq!(remote.get(b"inode/1").await.unwrap(), Some(vec![1]));
}

#[tokio::test]
async fn buffered_failed_ack_checkpoint_retries_even_with_no_pending_edits() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(Memory::default());
    let mut cfg = config(dir.path());
    cfg.sync_on_update = false;
    let b = backend(remote.clone(), cfg.clone());
    edit(&b, None, 1).await;
    remote.pause.store(true, Ordering::SeqCst);
    let core = b.core.clone().unwrap();
    let flush = tokio::spawn(async move { core.flush_to(1).await });
    remote.reached.acquire().await.unwrap().forget();
    let obstruction = dir.path().join("state.tmp");
    std::fs::create_dir(&obstruction).unwrap();
    remote.resume.add_permits(1);
    assert!(flush.await.unwrap().is_err());
    assert_eq!(remote.commits.load(Ordering::SeqCst), 1);
    std::fs::remove_dir(obstruction).unwrap();
    b.flush_metadata().await.unwrap();
    assert_eq!(remote.commits.load(Ordering::SeqCst), 1);
    drop(b);
    let (_journal, state) = Journal::open(&cfg, b"test-workspace").unwrap();
    assert!(
        !state.dirty(),
        "remote confirmation must be durably checkpointed before the barrier returns"
    );
}
