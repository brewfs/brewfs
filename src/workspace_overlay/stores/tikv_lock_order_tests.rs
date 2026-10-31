// Included inside tikv.rs::tests::commit_failure_candidates.
// These cases observe completed real SDK lock calls, never substitute values
// or synthesize CAS responses. Distinct first acquisitions are per start_ts;
// later put/delete relocks of an already owned key preserve write order.
const LOCK_ORDER_A: &[u8] = b"layer/lock-order-head";
const LOCK_ORDER_B: &[u8] = b"workspace/lock-order";
const LOCK_ORDER_A_VALUE: &[u8] = b"head-A-value";
const LOCK_ORDER_B_VALUE: &[u8] = b"workspace-B-value";

fn lock_order_checks(reverse: bool) -> Vec<KvCheck> {
    let mut checks = vec![
        KvCheck {
            key: LOCK_ORDER_A.to_vec(),
            expected: Some(LOCK_ORDER_A_VALUE.to_vec()),
        },
        KvCheck {
            key: LOCK_ORDER_B.to_vec(),
            expected: Some(LOCK_ORDER_B_VALUE.to_vec()),
        },
    ];
    if reverse {
        checks.reverse();
    }
    checks
}

fn lock_order_limits() -> KvReadLimits {
    KvReadLimits {
        max_records: 4,
        max_key_bytes: 256,
        max_value_bytes: 128,
        max_total_bytes: 64 << 10,
        max_response_bytes: 64 << 10,
        max_data_requests: 8,
    }
}

async fn seed_lock_order_pair(backend: &TiKvWorkspaceBackend) {
    assert!(
        backend
            .compare_and_swap(
                &[],
                &[
                    KvWrite::Put {
                        key: LOCK_ORDER_A.to_vec(),
                        value: LOCK_ORDER_A_VALUE.to_vec()
                    },
                    KvWrite::Put {
                        key: LOCK_ORDER_B.to_vec(),
                        value: LOCK_ORDER_B_VALUE.to_vec()
                    },
                ]
            )
            .await
            .unwrap()
    );
}

fn distinct_lock_order(events: &[LockObservation]) -> Vec<Vec<u8>> {
    assert!(
        !events.is_empty(),
        "real locking entry never completed a lock request"
    );
    let start_version = events[0].start_version;
    assert!(
        events
            .iter()
            .all(|event| event.start_version == start_version),
        "a new transaction must not be mixed into one acquisition trace: {events:?}"
    );
    let mut seen = std::collections::BTreeSet::new();
    events
        .iter()
        .filter(|event| seen.insert(event.key.clone()))
        .map(|event| event.key.clone())
        .collect()
}

fn assert_sorted_real_pair(backend: &TiKvWorkspaceBackend, events: &[LockObservation]) {
    assert_eq!(
        distinct_lock_order(events),
        vec![backend.scoped(LOCK_ORDER_A), backend.scoped(LOCK_ORDER_B)],
        "real TiKV first distinct lock acquisitions must use global byte order; events={events:?}"
    );
}

async fn run_lock_order_case<F, Fut>(run: F)
where
    F: FnOnce(TiKvWorkspaceBackend) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .expect("isolated BREWFS_TEST_TIKV_PD_ENDPOINTS is required")
        .split(',')
        .map(str::to_owned)
        .collect();
    // Reuse the exact scoped cleanup helper's existing UUID allowlist.
    let namespace = format!("commitred-{}", Uuid::new_v4().simple());
    let budget = V3MountBudget::defaults();
    let backend = TiKvWorkspaceBackend::connect_with_budget(endpoints, &namespace, budget.clone())
        .await
        .unwrap();
    establish_catalog_namespace(&backend).await;
    let mut task = tokio::spawn(run(backend.clone()));
    let (result, timed_out) = match tokio::time::timeout(CASE_TIMEOUT, &mut task).await {
        Ok(result) => (result, false),
        Err(_) => {
            task.abort();
            (task.await, true)
        }
    };
    // Clear observation before cleanup, including when the case panics.
    backend.test_control.active.store(false, Ordering::SeqCst);
    backend
        .test_control
        .observe_locks
        .store(false, Ordering::SeqCst);
    tokio::time::timeout(STEP_TIMEOUT, cleanup_namespace(&backend))
        .await
        .expect("lock-order namespace cleanup deadline exceeded");
    assert!(
        backend
            .scan_prefix_bounded(b"", 1)
            .await
            .unwrap()
            .is_empty()
    );
    backend.shutdown().await.unwrap();
    drop(backend);
    assert_eq!(
        budget.state().used,
        [0; 8],
        "lock-order case leaked a resident owner"
    );
    assert!(!timed_out, "real TiKV lock-order case timed out");
    assert!(
        result.is_ok(),
        "real TiKV lock-order assertion failed: {result:?}"
    );
}

#[tokio::test]
#[ignore = "requires isolated TiKV and existing exact scoped cleanup helper"]
async fn real_tikv_lock_order_exact_check_paths_are_caller_independent() {
    run_lock_order_case(|backend| async move {
        seed_lock_order_pair(&backend).await;
        // All three public CAS wrappers share the production CAS entry. The
        // fourth entry is the separate bounded authentication transaction.
        let mut all_traces = Vec::new();
        for path in 0..4 {
            let mut traces = Vec::new();
            for reverse in [false, true] {
                let checks = lock_order_checks(reverse);
                let expiry = backend.server_time_ns().await.unwrap() + 60_000_000_000;
                backend.test_control.start_lock_observation();
                let result = match path {
                    0 => backend.compare_and_swap(&checks, &[]).await,
                    1 => backend.compare_and_swap_before(&checks, &[], expiry).await,
                    2 => backend.compare_and_swap_in_time_window(&checks, &[], None, Some(expiry)).await,
                    3 => backend.authenticate_checks_before_bounded(&checks, expiry, lock_order_limits()).await,
                    _ => unreachable!(),
                };
                let (events, attempts) = backend.test_control.finish_lock_observation();
                eprintln!("real lock-order entry receipt: path={path}, reverse={reverse}, result={result:?}, attempts={attempts}, events={events:?}");
                assert!(result.unwrap());
                assert_eq!(attempts, 1);
                assert_eq!(events.len(), 2, "two distinct checks require two locking reads");
                assert!(events.iter().all(|event| event.returns_value));
                traces.push(events);
            }
            all_traces.push(traces);
        }
        // Finish all eight real operations, including bounded authentication,
        // before the RED ordering assertion can fail.
        for traces in all_traces {
            assert_eq!(distinct_lock_order(&traces[0]), distinct_lock_order(&traces[1]),
                "opposite check inputs changed the actual TiKV acquisition order");
            for events in &traces { assert_sorted_real_pair(&backend, events); }
        }
    }).await;
}

#[tokio::test]
#[ignore = "requires isolated TiKV and existing exact scoped cleanup helper"]
async fn real_tikv_lock_order_write_only_and_checked_union_are_sorted() {
    run_lock_order_case(|backend| async move {
        let mut traces = Vec::new();
        for checked in [false, true] {
            seed_lock_order_pair(&backend).await;
            let checks = if checked { vec![lock_order_checks(false).remove(1)] } else { Vec::new() };
            // In the checked case B is checked and A is write-only; sorting
            // the check list and write list separately leaves the inversion.
            let writes = if checked {
                vec![KvWrite::Put { key: LOCK_ORDER_A.to_vec(), value: b"new-A".to_vec() }]
            } else {
                vec![
                    KvWrite::Put { key: LOCK_ORDER_B.to_vec(), value: b"new-B".to_vec() },
                    KvWrite::Put { key: LOCK_ORDER_A.to_vec(), value: b"new-A".to_vec() },
                ]
            };
            backend.test_control.start_lock_observation();
            let result = backend.compare_and_swap(&checks, &writes).await;
            let (events, attempts) = backend.test_control.finish_lock_observation();
            eprintln!("real lock-order union receipt: checked={checked}, result={result:?}, attempts={attempts}, events={events:?}");
            assert!(result.unwrap());
            assert_eq!(attempts, 1);
            assert_eq!(backend.get_many_consistent(&[LOCK_ORDER_B.to_vec(), LOCK_ORDER_A.to_vec()]).await.unwrap(),
                vec![Some(if checked { LOCK_ORDER_B_VALUE.to_vec() } else { b"new-B".to_vec() }), Some(b"new-A".to_vec())]);
            traces.push(events);
        }
        // Both write-only and checked/write-only union cases terminate before
        // evaluating their actual acquisition order.
        for events in traces {
            assert_sorted_real_pair(&backend, &events);
        }
    }).await;
}

#[tokio::test]
#[ignore = "requires isolated TiKV and existing exact scoped cleanup helper"]
async fn real_tikv_lock_order_duplicate_checks_keep_all_expectations() {
    run_lock_order_case(|backend| async move {
        seed_lock_order_pair(&backend).await;
        let mut traces = Vec::new();
        for contradictory in [false, true] {
            let pair = lock_order_checks(false);
            let mut repeated = pair[1].clone();
            if contradictory { repeated.expected = Some(b"wrong-B".to_vec()); }
            let checks = vec![pair[1].clone(), pair[0].clone(), repeated];
            let writes = if contradictory {
                vec![KvWrite::Put { key: LOCK_ORDER_A.to_vec(), value: b"forbidden".to_vec() }]
            } else { Vec::new() };
            backend.test_control.start_lock_observation();
            let result = backend.compare_and_swap(&checks, &writes).await;
            let (events, attempts) = backend.test_control.finish_lock_observation();
            eprintln!("real lock-order duplicate-check receipt: contradictory={contradictory}, result={result:?}, attempts={attempts}, events={events:?}");
            assert_eq!(result.unwrap(), !contradictory);
            assert_eq!(attempts, 1);
            assert_eq!(backend.get_many_consistent(&[LOCK_ORDER_B.to_vec(), LOCK_ORDER_A.to_vec(), LOCK_ORDER_B.to_vec()]).await.unwrap(),
                vec![Some(LOCK_ORDER_B_VALUE.to_vec()), Some(LOCK_ORDER_A_VALUE.to_vec()), Some(LOCK_ORDER_B_VALUE.to_vec())],
                "contradictory checks must not write, and repeated result indices must retain key association");
            traces.push(events);
        }
        for events in traces {
            assert_sorted_real_pair(&backend, &events);
            assert_eq!(events.iter().filter(|event| event.returns_value).count(), 2,
                "duplicate checks must reuse their exact locked value, without a third locking read");
        }
    }).await;
}

#[tokio::test]
#[ignore = "requires isolated TiKV and existing exact scoped cleanup helper"]
async fn real_tikv_lock_order_duplicate_writes_keep_original_last_write() {
    run_lock_order_case(|backend| async move {
        seed_lock_order_pair(&backend).await;
        let writes = vec![
            KvWrite::Put { key: LOCK_ORDER_B.to_vec(), value: b"first-B".to_vec() },
            KvWrite::Delete { key: LOCK_ORDER_B.to_vec() },
            KvWrite::Put { key: LOCK_ORDER_B.to_vec(), value: b"final-B".to_vec() },
            KvWrite::Put { key: LOCK_ORDER_A.to_vec(), value: b"first-A".to_vec() },
            KvWrite::Put { key: LOCK_ORDER_A.to_vec(), value: b"final-A".to_vec() },
        ];
        backend.test_control.start_lock_observation();
        let result = backend.compare_and_swap(&[], &writes).await;
        let (events, attempts) = backend.test_control.finish_lock_observation();
        eprintln!("real lock-order duplicate-write receipt: result={result:?}, attempts={attempts}, events={events:?}");
        assert!(result.unwrap());
        assert_eq!(attempts, 1);
        assert!(events.iter().all(|event| !event.returns_value));
        let values = backend.get_many_consistent(&[
            LOCK_ORDER_B.to_vec(), LOCK_ORDER_A.to_vec(), LOCK_ORDER_B.to_vec(), b"missing".to_vec(),
        ]).await.unwrap();
        assert_eq!(values, vec![Some(b"final-B".to_vec()), Some(b"final-A".to_vec()), Some(b"final-B".to_vec()), None],
            "sorting the acquisition footprint must preserve put/delete/put last-write semantics and result order");
        assert_sorted_real_pair(&backend, &events);
        // The original five write operations remain observable SDK relocks.
        assert_eq!(&events[events.len() - writes.len()..].iter().map(|event| event.key.clone()).collect::<Vec<_>>(),
            &vec![backend.scoped(LOCK_ORDER_B), backend.scoped(LOCK_ORDER_B), backend.scoped(LOCK_ORDER_B), backend.scoped(LOCK_ORDER_A), backend.scoped(LOCK_ORDER_A)]);
    }).await;
}

#[tokio::test]
#[ignore = "requires isolated TiKV and existing exact scoped cleanup helper"]
async fn real_tikv_lock_order_bounded_duplicate_is_rejected_before_transaction() {
    run_lock_order_case(|backend| async move {
        seed_lock_order_pair(&backend).await;
        let pair = lock_order_checks(false);
        let checks = vec![pair[1].clone(), pair[0].clone(), pair[1].clone()];
        let expiry = backend.server_time_ns().await.unwrap() + 60_000_000_000;
        backend.test_control.start_lock_observation();
        let result = backend.authenticate_checks_before_bounded(&checks, expiry, lock_order_limits()).await;
        let (events, attempts) = backend.test_control.finish_lock_observation();
        eprintln!("real lock-order bounded-duplicate receipt: result={result:?}, attempts={attempts}, events={events:?}");
        assert!(matches!(result, Err(WorkspaceError::InvalidReadPlan(_))));
        assert_eq!(attempts, 0, "duplicate bounded plan must fail before beginning a transaction");
        assert!(events.is_empty(), "duplicate bounded plan must not send a locking request");
    }).await;
}

#[tokio::test]
#[ignore = "requires isolated TiKV advertised through the owned commit-reply proxy"]
async fn real_tikv_lock_order_unknown_auth_commit_reply_is_not_replayed() {
    // Reuse the existing verified successful upstream commit / suppressed
    // downstream reply case against the newly sorted authentication entry.
    run_whole_authentication_postsubmission_case(true).await;
}
