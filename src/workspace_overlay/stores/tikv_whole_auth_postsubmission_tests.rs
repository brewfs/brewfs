// Included after the scoped commit-failure and proxy test-only helpers.
// Both regressions call the specialized bounded authentication entry directly.
fn whole_auth_postsubmission_limits() -> KvReadLimits {
    KvReadLimits {
        max_records: 2,
        max_key_bytes: 1024,
        max_value_bytes: 64,
        max_total_bytes: 64 << 10,
        max_response_bytes: 64 << 10,
        // Leave ample slots: the forbidden replay must be rejected because
        // submission happened, independently of request-budget exhaustion.
        max_data_requests: 32,
    }
}

async fn seed_whole_authentication_pair(backend: &TiKvWorkspaceBackend) -> ([KvCheck; 2], i64) {
    let absent = [
        KvCheck {
            key: GUARD.to_vec(),
            expected: None,
        },
        KvCheck {
            key: TARGET.to_vec(),
            expected: None,
        },
    ];
    let writes = [
        KvWrite::Put {
            key: GUARD.to_vec(),
            value: b"old-guard".to_vec(),
        },
        KvWrite::Put {
            key: TARGET.to_vec(),
            value: b"old-target".to_vec(),
        },
    ];
    assert!(backend.compare_and_swap(&absent, &writes).await.unwrap());
    let checks = [
        KvCheck {
            key: GUARD.to_vec(),
            expected: Some(b"old-guard".to_vec()),
        },
        KvCheck {
            key: TARGET.to_vec(),
            expected: Some(b"old-target".to_vec()),
        },
    ];
    let expiry = backend
        .server_time_ns()
        .await
        .unwrap()
        .checked_add(60_000_000_000)
        .unwrap();
    (checks, expiry)
}

async fn revoked_whole_authentication_lock_case(
    a: TiKvWorkspaceBackend,
    b: TiKvWorkspaceBackend,
    inspector: TiKvWorkspaceBackend,
    jobs: Jobs,
) {
    let (checks, expiry) = seed_whole_authentication_pair(&b).await;
    let control = a.test_control.clone();
    let (entered, resume) = control.arm();
    let operation_checks = checks.clone();
    let mut operation = jobs.spawn(async move {
        a.authenticate_checks_before_bounded(
            &operation_checks,
            expiry,
            whole_auth_postsubmission_limits(),
        )
        .await
    });
    let timestamp = tokio::time::timeout(STEP_TIMEOUT, entered)
        .await
        .expect("specialized authentication never reached its actual commit boundary")
        .unwrap();
    assert_eq!(control.attempts.load(Ordering::SeqCst), 1);
    let locks = scoped_locks(&inspector, &timestamp).await;
    assert_eq!(locks.len(), 2, "both authentication checks must own real locks");
    assert!(locks.iter().all(|lock| {
        lock.lock_type == 5 && lock.lock_version == timestamp.version()
    }));
    let mut keys = locks.iter().map(|lock| lock.key.clone()).collect::<Vec<_>>();
    keys.sort();
    let mut expected = vec![inspector.scoped(GUARD), inspector.scoped(TARGET)];
    expected.sort();
    assert_eq!(keys, expected, "only the exact authentication keys may be revoked");
    revoke_test_pessimistic_locks(&inspector, &timestamp, &locks).await;
    assert!(scoped_locks(&inspector, &timestamp).await.is_empty());
    let peer = [
        KvWrite::Put {
            key: GUARD.to_vec(),
            value: b"peer-guard".to_vec(),
        },
        KvWrite::Put {
            key: TARGET.to_vec(),
            value: b"peer-target".to_vec(),
        },
    ];
    // The first authentication's real prewrite must now fail. A forbidden
    // whole-operation replay would instead observe a mismatch and return false.
    assert!(b.compare_and_swap(&checks, &peer).await.unwrap());
    drop(resume);
    let result = operation.finish().await;
    let attempts = control.attempts.load(Ordering::SeqCst);
    let errors = control.commit_errors.lock().unwrap().clone();
    let kinds = control.commit_failure_kinds.lock().unwrap().clone();
    control.active.store(false, Ordering::SeqCst);
    assert_eq!(
        b.get_many_consistent(&[GUARD.to_vec(), TARGET.to_vec()])
            .await
            .unwrap(),
        vec![Some(b"peer-guard".to_vec()), Some(b"peer-target".to_vec())]
    );
    eprintln!(
        "real specialized-auth revoked-lock receipt: attempts={attempts}, result={result:?}, commit_errors={errors:?}"
    );
    assert_eq!(errors.len(), 1, "the real authentication commit must fail once");
    assert_eq!(kinds, ["rpc-failed-precondition"]);
    // The bounded Prewrite codec deliberately rejects nested key/region
    // errors before prost can materialize them. The exact rollback receipts,
    // empty lock scan and peer update above establish the real server fault;
    // this path must retain the typed codec rejection, not expose a subtype.
    assert!(
        errors[0].contains("bounded authentication returned a key or region error"),
        "the real bounded prewrite did not reject the server error: {errors:?}"
    );
    assert!(
        matches!(result, Err(WorkspaceError::Backend(_))),
        "specialized-auth prewrite failure must retain Err: {result:?}"
    );
    assert_eq!(attempts, 1, "no authentication transaction may follow submission");
}

async fn lost_whole_authentication_commit_reply_case(
    a: TiKvWorkspaceBackend,
    b: TiKvWorkspaceBackend,
    inspector: TiKvWorkspaceBackend,
    jobs: Jobs,
    control_url: String,
    namespace: String,
) {
    let (checks, expiry) = seed_whole_authentication_pair(&b).await;
    let control = a.test_control.clone();
    let (entered, resume) = control.arm();
    let mut operation = jobs.spawn(async move {
        a.authenticate_checks_before_bounded(&checks, expiry, whole_auth_postsubmission_limits())
            .await
    });
    let timestamp = tokio::time::timeout(STEP_TIMEOUT, entered)
        .await
        .expect("specialized authentication never reached its actual proxy commit boundary")
        .unwrap();
    let version = timestamp.version();
    assert_eq!(control.attempts.load(Ordering::SeqCst), 1);
    let locks = scoped_locks(&inspector, &timestamp).await;
    assert_eq!(locks.len(), 2);
    assert!(locks.iter().all(|lock| lock.lock_type == 5 && lock.lock_version == version));
    let mut keys = locks.iter().map(|lock| lock.key.clone()).collect::<Vec<_>>();
    keys.sort();
    let mut expected = vec![inspector.scoped(GUARD), inspector.scoped(TARGET)];
    expected.sort();
    assert_eq!(keys, expected);
    let arm = proxy_control(
        &control_url,
        "/arm",
        Some(serde_json::json!({"namespace": namespace, "start_version": version})),
    )
    .await;
    assert_eq!(arm["armed"], true);
    assert_eq!(arm["start_version"].as_u64(), Some(version));
    drop(resume);
    let result = operation.finish().await;
    let attempts = control.attempts.load(Ordering::SeqCst);
    let errors = control.commit_errors.lock().unwrap().clone();
    let kinds = control.commit_failure_kinds.lock().unwrap().clone();
    control.active.store(false, Ordering::SeqCst);
    let receipt = proxy_control(&control_url, "/receipt", None).await;
    assert_eq!(receipt["namespace"].as_str(), Some(namespace.as_str()));
    assert_eq!(receipt["start_version"].as_u64(), Some(version));
    let events = receipt["events"].as_array().unwrap();
    assert!(!events.is_empty(), "no real successful auth KvCommit reply was observed");
    for event in events {
        assert_eq!(event["namespace"].as_str(), Some(namespace.as_str()));
        assert_eq!(event["start_version"].as_u64(), Some(version));
        assert_eq!(event["upstream_commit_response_verified"], true);
        assert_eq!(event["downstream_success_reply_suppressed"], true);
        assert_eq!(event["downstream_grpc_status"], 14);
    }
    // This is lock-only authentication: successful server commit changes no
    // stored values. Replaying the unchanged checks would spuriously succeed.
    assert_eq!(
        b.get_many_consistent(&[GUARD.to_vec(), TARGET.to_vec()])
            .await
            .unwrap(),
        vec![Some(b"old-guard".to_vec()), Some(b"old-target".to_vec())]
    );
    eprintln!(
        "real specialized-auth lost-commit-reply receipt: start_version={version}, attempts={attempts}, result={result:?}, commit_errors={errors:?}, verified_server_replies={}",
        events.len()
    );
    assert_eq!(errors.len(), 1, "the SDK must expose this authentication reply loss");
    assert_eq!(kinds, ["rpc-unavailable"]);
    assert!(
        matches!(result, Err(WorkspaceError::Backend(_))),
        "unknown auth commit must remain Err: {result:?}"
    );
    assert_eq!(attempts, 1, "unknown commit must not start another authentication");
    // A lost successful primary reply prevents the SDK from dispatching its
    // secondary phase. Preserve the unknown result above, then independently
    // ask the real server for that exact transaction's final status and retire
    // any remaining lock-only secondary. This never replays authentication or
    // supplies a fabricated commit timestamp, and does not change PD safepoint.
    let client = inspector.main_client().await.unwrap();
    let through = client.current_timestamp().await.unwrap();
    let remaining = scoped_locks(&inspector, &through).await;
    assert!(remaining.iter().all(|lock| {
        lock.lock_type == 2
            && lock.lock_version == version
            && lock.primary_lock == inspector.scoped(GUARD)
            && lock.key == inspector.scoped(TARGET)
    }), "only the exact committed transaction's secondary may remain");
    assert!(remaining.len() <= 1);
    let live = tokio::time::timeout(
        STEP_TIMEOUT,
        client.resolve_locks(remaining, through.clone(), tikv_client::Backoff::no_backoff()),
    )
    .await
    .expect("independent committed-secondary resolution exceeded its deadline")
    .unwrap();
    assert!(live.is_empty(), "the real primary status did not resolve its secondary");
    assert!(
        scoped_locks(&inspector, &through).await.is_empty(),
        "the committed authentication's independently resolved scope must be empty"
    );
    assert_eq!(control.attempts.load(Ordering::SeqCst), 1);
}

async fn run_whole_authentication_postsubmission_case(lost_reply: bool) {
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    let control_url = if lost_reply {
        let url = std::env::var("BREWFS_TEST_TIKV_COMMIT_PROXY_CONTROL_URL")
            .expect("BREWFS_TEST_TIKV_COMMIT_PROXY_CONTROL_URL is required");
        assert_eq!(proxy_control(&url, "/health", None).await["ready"], true);
        Some(url)
    } else {
        None
    };
    // Preserve the frozen fault helpers' strict UUID namespace allowlists.
    let namespace = format!(
        "{}-{}",
        if lost_reply { "commitunknown" } else { "commitred" },
        Uuid::new_v4().simple()
    );
    eprintln!("real specialized-auth postsubmission namespace: {namespace}");
    let a = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace).await.unwrap();
    let b = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace).await.unwrap();
    let inspector = TiKvWorkspaceBackend::connect(endpoints, &namespace).await.unwrap();
    let cleanup = inspector.clone();
    let jobs = Jobs::default();
    let case_jobs = jobs.clone();
    let case_control_url = control_url.clone();
    let mut task = tokio::spawn(async move {
        if let Some(url) = case_control_url {
            lost_whole_authentication_commit_reply_case(a, b, inspector, case_jobs, url, namespace)
                .await;
        } else {
            revoked_whole_authentication_lock_case(a, b, inspector, case_jobs).await;
        }
    });
    let (result, timed_out) = match tokio::time::timeout(CASE_TIMEOUT, &mut task).await {
        Ok(result) => (result, false),
        Err(_) => {
            task.abort();
            (task.await, true)
        }
    };
    jobs.stop_and_wait().await;
    if let Some(url) = control_url {
        proxy_control(&url, "/disarm", Some(serde_json::json!({}))).await;
    }
    tokio::time::timeout(STEP_TIMEOUT, cleanup_namespace(&cleanup))
        .await
        .expect("specialized-auth namespace cleanup exceeded its deadline");
    assert!(!timed_out, "real specialized-auth postsubmission case timed out");
    assert!(result.is_ok(), "real specialized-auth contract failed: {result:?}");
}

#[tokio::test]
#[ignore = "requires isolated TiKV and exact scoped rollback fault helper"]
async fn real_tikv_authentication_prewrite_failure_does_not_replay() {
    run_whole_authentication_postsubmission_case(false).await;
}

#[tokio::test]
#[ignore = "requires isolated TiKV advertised through the owned commit-reply proxy"]
async fn real_tikv_authentication_lost_successful_commit_reply_does_not_replay() {
    run_whole_authentication_postsubmission_case(true).await;
}
