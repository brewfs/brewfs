// Include after tikv_commit_failure_tests.rs inside the same test-only child.
async fn proxy_control(
    control_url: &str,
    route: &str,
    body: Option<serde_json::Value>,
) -> serde_json::Value {
    assert!(matches!(route, "/health" | "/arm" | "/receipt" | "/disarm"));
    let port = control_url
        .strip_prefix("http://127.0.0.1:")
        .expect("commit proxy control requires an isolated localhost endpoint")
        .parse::<u16>()
        .expect("commit proxy control port was invalid");
    let method = if body.is_some() { "POST" } else { "GET" };
    let payload = body
        .map(|body| serde_json::to_vec(&body).unwrap())
        .unwrap_or_default();
    assert!(payload.len() <= 4096);
    let request = format!(
        "{method} {route} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    tokio::time::timeout(STEP_TIMEOUT, async {
        let mut stream = tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
            .await
            .expect("commit proxy control connect failed");
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.write_all(&payload).await.unwrap();
        let mut bytes = Vec::new();
        stream
            .take((64 << 10) + 1)
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        assert!(
            bytes.len() <= 64 << 10,
            "commit proxy control response exceeded its bound"
        );
        assert!(
            bytes.starts_with(b"HTTP/1.1 200 "),
            "commit proxy control rejected the request"
        );
        let end = bytes
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap();
        serde_json::from_slice(&bytes[end + 4..])
            .expect("commit proxy control receipt was not JSON")
    })
    .await
    .expect("commit proxy control deadline exceeded")
}

async fn unknown_commit_case(
    a: TiKvWorkspaceBackend,
    b: TiKvWorkspaceBackend,
    jobs: Jobs,
    control_url: String,
    namespace: String,
) {
    establish_catalog_namespace(&b).await;
    let checks = [
        KvCheck {
            key: GUARD.to_vec(),
            expected: None,
        },
        KvCheck {
            key: TARGET.to_vec(),
            expected: None,
        },
    ];
    let initial = [
        KvWrite::Put {
            key: GUARD.to_vec(),
            value: b"old-guard".to_vec(),
        },
        KvWrite::Put {
            key: TARGET.to_vec(),
            value: b"old-target".to_vec(),
        },
    ];
    assert!(b.compare_and_swap(&checks, &initial).await.unwrap());
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
    let writes = [
        KvWrite::Put {
            key: GUARD.to_vec(),
            value: b"writer-guard".to_vec(),
        },
        KvWrite::Put {
            key: TARGET.to_vec(),
            value: b"writer-target".to_vec(),
        },
    ];
    let control = a.test_control.clone();
    let (entered, resume) = control.arm();
    let mut operation = jobs.spawn(async move { a.compare_and_swap(&checks, &writes).await });
    let timestamp = tokio::time::timeout(STEP_TIMEOUT, entered)
        .await
        .expect("actual proxy CAS pre-commit boundary was not reached")
        .unwrap();
    let version = timestamp.version();
    let arm = proxy_control(
        &control_url,
        "/arm",
        Some(serde_json::json!({
            "namespace": namespace,
            "start_version": version,
        })),
    )
    .await;
    assert_eq!(arm["armed"], true);
    assert_eq!(arm["start_version"].as_u64(), Some(version));
    drop(resume);
    let result = operation.finish().await;
    let attempts = control.attempts.load(Ordering::SeqCst);
    let errors = control.commit_errors.lock().unwrap().clone();
    control.active.store(false, Ordering::SeqCst);
    let receipt = proxy_control(&control_url, "/receipt", None).await;
    assert_eq!(receipt["namespace"].as_str(), Some(namespace.as_str()));
    assert_eq!(receipt["start_version"].as_u64(), Some(version));
    let events = receipt["events"]
        .as_array()
        .expect("proxy receipt omitted actual success events");
    assert!(
        !events.is_empty(),
        "proxy never observed a real successful TiKV KvCommit reply"
    );
    for event in events {
        assert_eq!(event["start_version"].as_u64(), Some(version));
        assert_eq!(event["upstream_commit_response_verified"], true);
        assert_eq!(event["downstream_success_reply_suppressed"], true);
        assert_eq!(event["downstream_grpc_status"], 14);
    }
    // Upstream commit success was actually observed. An independent snapshot
    // must therefore see the entire authentic new pair, never fabricated data.
    let values = b
        .get_many_consistent(&[GUARD.to_vec(), TARGET.to_vec()])
        .await
        .unwrap();
    assert_eq!(
        values,
        vec![
            Some(b"writer-guard".to_vec()),
            Some(b"writer-target".to_vec())
        ]
    );
    eprintln!(
        "real lost-commit-reply receipt: start_version={version}, attempts={attempts}, result={result:?}, commit_errors={errors:?}, verified_server_replies={}",
        events.len()
    );
    assert!(
        !errors.is_empty(),
        "the real SDK did not expose the reply-loss fault"
    );
    assert!(
        is_retryable(&errors[0]),
        "RED prerequisite: original string policy must classify the actual gRPC fault as retryable"
    );
    assert!(
        matches!(result, Err(WorkspaceError::Backend(_))),
        "unknown successful commit must remain Err, not become a replay mismatch: {result:?}"
    );
    assert_eq!(
        attempts, 1,
        "unknown commit must not start a new CAS transaction"
    );
}

#[tokio::test]
#[ignore = "requires isolated TiKV advertised through commit_reply_proxy.py and its control URL"]
async fn real_tikv_lost_successful_commit_reply_does_not_replay() {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .expect("isolated BREWFS_TEST_TIKV_PD_ENDPOINTS is required")
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let control_url = std::env::var("BREWFS_TEST_TIKV_COMMIT_PROXY_CONTROL_URL")
        .expect("BREWFS_TEST_TIKV_COMMIT_PROXY_CONTROL_URL is required");
    let health = proxy_control(&control_url, "/health", None).await;
    assert_eq!(health["ready"], true);
    let namespace = format!("commitunknown-{}", Uuid::new_v4().simple());
    let a = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
        .await
        .unwrap();
    let b = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
        .await
        .unwrap();
    let cleanup = TiKvWorkspaceBackend::connect(endpoints, &namespace)
        .await
        .unwrap();
    let jobs = Jobs::default();
    let case_jobs = jobs.clone();
    let case_control_url = control_url.clone();
    let mut task = tokio::spawn(async move {
        unknown_commit_case(a, b, case_jobs, case_control_url, namespace).await
    });
    let (result, timed_out) = match tokio::time::timeout(CASE_TIMEOUT, &mut task).await {
        Ok(result) => (result, false),
        Err(_) => {
            task.abort();
            (task.await, true)
        }
    };
    jobs.stop_and_wait().await;
    proxy_control(&control_url, "/disarm", Some(serde_json::json!({}))).await;
    tokio::time::timeout(STEP_TIMEOUT, cleanup_namespace(&cleanup))
        .await
        .expect("proxy case namespace cleanup exceeded its overall deadline");
    assert!(!timed_out, "unknown commit proxy case deadline exceeded");
    assert!(
        result.is_ok(),
        "real unknown commit contract failed: {result:?}"
    );
}
