use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::{Semaphore, oneshot};

#[derive(Default)]
pub(super) struct Control {
    pause: Mutex<Option<(oneshot::Sender<()>, Arc<Semaphore>)>>,
    allocation_pause: Mutex<Option<(oneshot::Sender<()>, Arc<Semaphore>)>>,
    lose_ack: AtomicBool,
    commits: AtomicUsize,
}

impl Control {
    fn pause_allocation(&self) -> (oneshot::Receiver<()>, Arc<Semaphore>) {
        let (tx, rx) = oneshot::channel();
        let resume = Arc::new(Semaphore::new(0));
        assert!(
            self.allocation_pause
                .lock()
                .unwrap()
                .replace((tx, resume.clone()))
                .is_none()
        );
        (rx, resume)
    }
    pub(super) async fn after_allocation_read(&self) {
        let pause = self.allocation_pause.lock().unwrap().take();
        if let Some((entered, resume)) = pause {
            let _ = entered.send(());
            tokio::time::timeout(Duration::from_secs(10), resume.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
        }
    }
    fn pause(&self) -> (oneshot::Receiver<()>, Arc<Semaphore>) {
        let (tx, rx) = oneshot::channel();
        let resume = Arc::new(Semaphore::new(0));
        assert!(
            self.pause
                .lock()
                .unwrap()
                .replace((tx, resume.clone()))
                .is_none()
        );
        (rx, resume)
    }
    pub(super) async fn before_clock(&self) {
        let pause = self.pause.lock().unwrap().take();
        if let Some((entered, resume)) = pause {
            let _ = entered.send(());
            tokio::time::timeout(Duration::from_secs(10), resume.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
        }
    }
    pub(super) fn submitting(&self) {
        self.commits.fetch_add(1, Ordering::SeqCst);
    }
    pub(super) fn after_commit(&self) -> Result<()> {
        if self.lose_ack.swap(false, Ordering::SeqCst) {
            return Err(backend(tikv_client::Error::UndeterminedError(Box::new(
                tikv_client::Error::StringError("injected lost commit acknowledgement".into()),
            ))));
        }
        Ok(())
    }
}

#[test]
fn tikv_entity_precommit_conflicts_do_not_classify_unknown_outcomes() {
    let conflict = || {
        tikv_client::Error::KeyError(Box::new(tikv_client::ProtoKeyError {
            conflict: Some(Default::default()),
            ..Default::default()
        }))
    };
    assert_eq!(precommit_error(conflict()), EntityError::Conflict);
    assert!(matches!(
        precommit_error(tikv_client::Error::UndeterminedError(Box::new(conflict()))),
        EntityError::Backend(_)
    ));
    assert!(matches!(
        precommit_error(tikv_client::Error::StringError("write conflict".into())),
        EntityError::Backend(_)
    ));
    assert!(matches!(
        precommit_error(tikv_client::Error::ExtractedErrors(vec![
            conflict(),
            tikv_client::Error::StringError("transport".into())
        ])),
        EntityError::Backend(_)
    ));
}

#[test]
fn tikv_entity_precommit_deadlocks_require_a_definite_precommit_error() {
    let deadlock = || tikv_client::ProtoKeyError {
        deadlock: Some(Default::default()),
        ..Default::default()
    };
    assert_eq!(
        precommit_error(tikv_client::Error::KeyError(Box::new(deadlock()))),
        EntityError::Conflict
    );
    assert!(matches!(
        precommit_error(tikv_client::Error::UndeterminedError(Box::new(
            tikv_client::Error::KeyError(Box::new(deadlock()))
        ))),
        EntityError::Backend(_)
    ));
    assert!(matches!(
        precommit_error(tikv_client::Error::StringError("deadlock".into())),
        EntityError::Backend(_)
    ));
    for other in [
        tikv_client::ProtoKeyError {
            conflict: Some(Default::default()),
            ..deadlock()
        },
        tikv_client::ProtoKeyError {
            abort: "another failure".into(),
            ..deadlock()
        },
        tikv_client::ProtoKeyError {
            locked: Some(Default::default()),
            ..deadlock()
        },
    ] {
        assert!(matches!(
            precommit_error(tikv_client::Error::KeyError(Box::new(other))),
            EntityError::Backend(_)
        ));
    }
    assert!(matches!(
        precommit_error(tikv_client::Error::ExtractedErrors(vec![
            tikv_client::Error::KeyError(Box::new(deadlock())),
            tikv_client::Error::StringError("transport".into())
        ])),
        EntityError::Backend(_)
    ));
}

#[test]
fn tikv_entity_clock_and_prefix_boundaries() {
    assert_eq!(time_ns(1234).unwrap(), 1_234_000_000);
    assert!(time_ns(-1).is_err());
    assert!(time_ns(i64::MAX).is_err());
    assert_eq!(prefix_end(b"a/\xff").unwrap(), b"a0");
    assert!(prefix_end(b"\xff").is_err());
}

fn endpoints() -> Vec<String> {
    std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .expect("isolated TiKV endpoints")
        .split(',')
        .map(str::to_owned)
        .collect()
}

async fn fixture() -> (TiKvEntityBackend, EntityKeys, TransactionAuthority) {
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let backend = TiKvEntityBackend::connect(endpoints(), keys.volume_id(), None)
        .await
        .unwrap();
    let initial = TransactionAuthority::from_validated(
        vec![RowCheck {
            key: keys.descriptor(),
            expected: None,
        }],
        None,
    )
    .unwrap();
    backend
        .commit(
            &initial,
            &EntityPlan {
                checks: vec![],
                writes: vec![RowWrite::Put {
                    key: keys.descriptor(),
                    value: b"active".to_vec(),
                }],
            },
        )
        .await
        .unwrap();
    let authority = TransactionAuthority::from_validated(
        vec![RowCheck {
            key: keys.descriptor(),
            expected: Some(b"active".to_vec()),
        }],
        None,
    )
    .unwrap();
    (backend, keys, authority)
}

fn plan(keys: &EntityKeys) -> EntityPlan {
    EntityPlan {
        checks: vec![],
        writes: vec![RowWrite::Put {
            key: keys.inode(2),
            value: b"once".to_vec(),
        }],
    }
}

// Observe an actual server-side wait instead of guessing when a spawned
// transaction has reached TiKV. The runners provide an isolated server and
// execute these integration tests with one test thread.
type MetricsClient = hyper::Client<hyper::client::HttpConnector>;

async fn lock_waiters(client: &MetricsClient, endpoint: &str) -> u64 {
    let metrics = tokio::time::timeout(Duration::from_secs(2), async {
        let response = client
            .get(format!("{endpoint}/metrics").parse().unwrap())
            .await
            .unwrap();
        assert!(response.status().is_success());
        hyper::body::to_bytes(response.into_body()).await.unwrap()
    })
    .await
    .unwrap();
    let metrics = std::str::from_utf8(&metrics).unwrap();
    metrics
        .lines()
        .find_map(|line| {
            line.strip_prefix("tikv_lock_wait_queue_entries_gauge_vec{type=\"waiters\"} ")
        })
        // TiKV registers this gauge lazily on the first lock wait. Absence
        // means zero before then; a waiting assertion still requires it to
        // appear with the expected nonzero value.
        .unwrap_or("0")
        .parse()
        .unwrap()
}

async fn wait_for_lock_waiters(client: &MetricsClient, endpoint: &str, expected: u64) {
    let mut observed = 0;
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            observed = lock_waiters(client, endpoint).await;
            if observed >= expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "expected {expected} server lock waiters, observed {observed}"
    );
}

#[tokio::test]
#[ignore = "requires isolated PD/TiKV and BREWFS_TEST_TIKV_STATUS_ENDPOINT"]
async fn tikv_entity_waiting_mutation_retains_its_ordered_lock_prefix() {
    let (backend, keys, authority) = fixture().await;
    let endpoint = std::env::var("BREWFS_TEST_TIKV_STATUS_ENDPOINT").unwrap();
    let metrics = MetricsClient::new();
    let first = keys.descriptor();
    let middle = keys.lease();
    let last = keys.lower_pin();
    assert!(first < middle && middle < last);
    let mut blocker = backend.inner.client.begin_pessimistic().await.unwrap();
    assert_eq!(blocker.get_for_update(middle.clone()).await.unwrap(), None);
    assert_eq!(lock_waiters(&metrics, &endpoint).await, 0);
    let mutation = {
        let backend = backend.clone();
        let last = last.clone();
        tokio::spawn(async move {
            backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![RowCheck {
                            key: middle,
                            expected: None,
                        }],
                        writes: vec![RowWrite::Put {
                            key: last,
                            value: b"published".to_vec(),
                        }],
                    },
                )
                .await
        })
    };
    wait_for_lock_waiters(&metrics, &endpoint, 1).await;
    let mut probe = {
        let backend = backend.clone();
        let first = first.clone();
        tokio::spawn(async move {
            // TiKV may wake a blocked locking read with PessimisticRetry.
            // A retry must close its transaction first; it cannot turn a
            // successfully acquired early key into evidence of blocking.
            for _ in 0..MAX_PRECOMMIT_ATTEMPTS {
                let mut transaction = backend.inner.client.begin_pessimistic().await.unwrap();
                match transaction.get_for_update(first.clone()).await {
                    Ok(value) => {
                        assert_eq!(value, Some(b"active".to_vec()));
                        return transaction;
                    }
                    Err(error) => {
                        transaction.rollback().await.unwrap();
                        assert_eq!(precommit_error(error), EntityError::Conflict);
                    }
                }
            }
            panic!("lock probe exhausted its precommit retry budget");
        })
    };
    let retained_prefix = tokio::select! {
        transaction = &mut probe => {
            // Even the failing implementation must release both raw test
            // transactions before the assertion or backend shutdown.
            transaction.unwrap().rollback().await.unwrap();
            false
        }
        () = wait_for_lock_waiters(&metrics, &endpoint, 2) => true,
    };
    blocker.rollback().await.unwrap();
    // A post-wakeup retry may let the probe acquire the first key before
    // the mutation reacquires it. Release the probe before joining mutation.
    if retained_prefix {
        tokio::time::timeout(Duration::from_secs(5), probe)
            .await
            .unwrap()
            .unwrap()
            .rollback()
            .await
            .unwrap();
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), mutation)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        CommitOutcome::Committed
    );
    assert_eq!(
        backend.read(&[first, last]).await.unwrap().rows,
        [Some(b"active".to_vec()), Some(b"published".to_vec())]
    );
    backend.shutdown().await.unwrap();
    assert!(
        retained_prefix,
        "waiting on a later key released the earlier lock"
    );
}

#[tokio::test]
#[ignore = "requires isolated PD/TiKV and BREWFS_TEST_TIKV_STATUS_ENDPOINT"]
async fn tikv_entity_allocation_retains_counter_until_object_reservation() {
    let (backend, keys, authority) = fixture().await;
    let counter = keys.counter("slice");
    assert_eq!(
        backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![RowCheck {
                        key: counter.clone(),
                        expected: None
                    }],
                    writes: vec![RowWrite::Put {
                        key: counter.clone(),
                        value: b"0".to_vec()
                    }],
                }
            )
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    let endpoint = std::env::var("BREWFS_TEST_TIKV_STATUS_ENDPOINT").unwrap();
    let metrics = MetricsClient::new();
    assert_eq!(lock_waiters(&metrics, &endpoint).await, 0);
    let (entered, resume) = backend.inner.control.pause_allocation();
    let allocation = {
        let backend = backend.clone();
        let keys = keys.clone();
        tokio::spawn(async move {
            backend
                .reserve_slices(
                    &authority,
                    &SliceAllocation {
                        keys,
                        sizes: vec![4096],
                        upload_id: Uuid::new_v4(),
                        lease: super::super::lifecycle::LeaseIdentity {
                            lease_id: Uuid::new_v4(),
                            holder_generation: 1,
                        },
                    },
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .unwrap()
        .unwrap();
    let mut probe = {
        let backend = backend.clone();
        let counter = counter.clone();
        tokio::spawn(async move {
            for _ in 0..MAX_PRECOMMIT_ATTEMPTS {
                let mut transaction = backend.inner.client.begin_pessimistic().await.unwrap();
                let result = transaction.get_for_update(counter.clone()).await;
                transaction.rollback().await.unwrap();
                match result {
                    Ok(value) => {
                        assert!(value.is_some());
                        return;
                    }
                    Err(error) => assert_eq!(precommit_error(error), EntityError::Conflict),
                }
            }
            panic!("allocator counter probe exhausted its precommit retry budget");
        })
    };
    let observed = tokio::select! {
        result = &mut probe => { result.unwrap(); Some(false) }
        result = tokio::time::timeout(Duration::from_secs(5), async {
            while lock_waiters(&metrics, &endpoint).await == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }) => result.ok().map(|()| true),
    };
    resume.add_permits(1);
    let reserved = tokio::time::timeout(Duration::from_secs(5), allocation)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    if observed != Some(false) {
        tokio::time::timeout(Duration::from_secs(5), probe)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(reserved, Some(vec![1]));
    let rows = backend.read(&[counter, keys.slice(1)]).await.unwrap().rows;
    assert_eq!(rows[0], Some(b"1".to_vec()));
    assert!(rows[1].is_some());
    backend.shutdown().await.unwrap();
    assert_eq!(
        observed,
        Some(true),
        "allocation exposed an unlocked counter read before reservation"
    );
}

#[tokio::test]
#[ignore = "requires isolated PD/TiKV"]
async fn tikv_entity_allocation_lost_commit_ack_is_not_replayed() {
    let (backend, keys, authority) = fixture().await;
    let counter = keys.counter("slice");
    backend
        .commit(
            &authority,
            &EntityPlan {
                checks: vec![RowCheck {
                    key: counter.clone(),
                    expected: None,
                }],
                writes: vec![RowWrite::Put {
                    key: counter.clone(),
                    value: b"0".to_vec(),
                }],
            },
        )
        .await
        .unwrap();
    let allocation = SliceAllocation {
        keys: keys.clone(),
        sizes: vec![4096],
        upload_id: Uuid::new_v4(),
        lease: super::super::lifecycle::LeaseIdentity {
            lease_id: Uuid::new_v4(),
            holder_generation: 1,
        },
    };
    let submitted = backend.inner.control.commits.load(Ordering::SeqCst);
    backend.inner.control.lose_ack.store(true, Ordering::SeqCst);
    assert!(matches!(
        backend.reserve_slices(&authority, &allocation).await,
        Err(EntityError::Backend(_))
    ));
    assert_eq!(
        backend.inner.control.commits.load(Ordering::SeqCst),
        submitted + 1
    );
    let rows = backend
        .read(&[counter, keys.slice(1), keys.slice(2)])
        .await
        .unwrap()
        .rows;
    assert_eq!(rows[0], Some(b"1".to_vec()));
    assert!(rows[1].is_some());
    assert!(rows[2].is_none());
    assert_eq!(
        backend
            .reserve_slices(&authority, &allocation)
            .await
            .unwrap(),
        Some(vec![2])
    );
    backend.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated PD/TiKV"]
async fn tikv_entity_expiry_is_checked_after_reads_and_before_success() {
    for read_only in [false, true] {
        let (backend, keys, authority) = fixture().await;
        let now = backend.read(&[]).await.unwrap().now_ns;
        let expired_after_pause =
            TransactionAuthority::from_validated(authority.checks, Some(now + 500_000_000))
                .unwrap();
        let (entered, resume) = backend.inner.control.pause();
        let request = {
            let backend = backend.clone();
            let plan = if read_only {
                EntityPlan::default()
            } else {
                plan(&keys)
            };
            tokio::spawn(async move { backend.commit(&expired_after_pause, &plan).await })
        };
        tokio::time::timeout(Duration::from_secs(5), entered)
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(600)).await;
        resume.add_permits(1);
        assert_eq!(request.await.unwrap(), Err(EntityError::Fenced));
        assert_eq!(backend.read(&[keys.inode(2)]).await.unwrap().rows, [None]);
        backend.shutdown().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires isolated PD/TiKV"]
async fn tikv_entity_lost_commit_ack_is_not_replayed() {
    let (backend, keys, authority) = fixture().await;
    let before = backend.inner.control.commits.load(Ordering::SeqCst);
    backend.inner.control.lose_ack.store(true, Ordering::SeqCst);
    assert!(matches!(
        backend.commit(&authority, &plan(&keys)).await,
        Err(EntityError::Backend(_))
    ));
    assert_eq!(
        backend.inner.control.commits.load(Ordering::SeqCst),
        before + 1
    );
    assert_eq!(
        backend.read(&[keys.inode(2)]).await.unwrap().rows,
        [Some(b"once".to_vec())]
    );
    backend.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated PD/TiKV"]
async fn tikv_entity_cancelled_caller_keeps_commit_and_shutdown_obligation() {
    let (backend, keys, authority) = fixture().await;
    let (entered, resume) = backend.inner.control.pause();
    let request = {
        let backend = backend.clone();
        let plan = plan(&keys);
        tokio::spawn(async move { backend.commit(&authority, &plan).await })
    };
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .unwrap()
        .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    let mut shutdown = {
        let backend = backend.clone();
        tokio::spawn(async move { backend.shutdown().await })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut shutdown)
            .await
            .is_err()
    );
    resume.add_permits(1);
    shutdown.await.unwrap().unwrap();
    let reopened = TiKvEntityBackend::connect(endpoints(), keys.volume_id(), None)
        .await
        .unwrap();
    assert_eq!(
        reopened.read(&[keys.inode(2)]).await.unwrap().rows,
        [Some(b"once".to_vec())]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated PD/TiKV"]
async fn tikv_entity_read_validation_does_not_lock_out_renewal() {
    let (backend, keys, authority) = fixture().await;
    let (entered, resume) = backend.inner.control.pause();
    let read = {
        let backend = backend.clone();
        let authority = authority.clone();
        let key = keys.lease();
        tokio::spawn(async move {
            backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![RowCheck {
                            key,
                            expected: None,
                        }],
                        writes: vec![],
                    },
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .unwrap()
        .unwrap();
    let mut renewal = {
        let backend = backend.clone();
        let key = keys.lease();
        tokio::spawn(async move {
            backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![RowCheck {
                            key: key.clone(),
                            expected: None,
                        }],
                        writes: vec![RowWrite::Put {
                            key,
                            value: b"renewed".to_vec(),
                        }],
                    },
                )
                .await
        })
    };
    // A paused read validation must not hold the authority/lease keys hostage.
    // Resume and join both tasks even on failure, so the owned backend closes.
    let renewed = tokio::time::timeout(Duration::from_secs(2), &mut renewal).await;
    let concurrent = renewed.is_ok();
    resume.add_permits(1);
    if renewed.is_err() {
        assert_eq!(renewal.await.unwrap().unwrap(), CommitOutcome::Committed);
    } else {
        assert_eq!(renewed.unwrap().unwrap().unwrap(), CommitOutcome::Committed);
    }
    // This read linearizes at its earlier snapshot, before the renewal.
    assert_eq!(read.await.unwrap().unwrap(), CommitOutcome::Committed);
    assert_eq!(
        backend.read(&[keys.lease()]).await.unwrap().rows,
        [Some(b"renewed".to_vec())]
    );
    backend.shutdown().await.unwrap();
    assert!(concurrent, "read validation blocked lease renewal");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires isolated PD/TiKV"]
async fn tikv_entity_eight_readers_leave_renewal_live() {
    let (backend, keys, authority) = fixture().await;
    let barrier = Arc::new(tokio::sync::Barrier::new(9));
    let mut readers = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let backend = backend.clone();
        let authority = authority.clone();
        let barrier = barrier.clone();
        readers.spawn(async move {
            barrier.wait().await;
            for _ in 0..64 {
                assert_eq!(
                    backend
                        .commit(&authority, &EntityPlan::default())
                        .await
                        .unwrap(),
                    CommitOutcome::Committed
                );
            }
        });
    }
    barrier.wait().await;
    for generation in 0..32 {
        assert_eq!(
            backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: vec![],
                        writes: vec![RowWrite::Put {
                            key: keys.lease(),
                            value: generation.to_string().into_bytes()
                        }],
                    }
                )
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
    }
    while let Some(result) = readers.join_next().await {
        result.unwrap();
    }
    backend.shutdown().await.unwrap();
}
