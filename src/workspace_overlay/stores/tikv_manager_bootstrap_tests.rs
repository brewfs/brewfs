// Included beside the real CAS fault helpers to reuse their bounded task and
// exact UUID-namespace cleanup discipline.

async fn bootstrap_wait_case(
    writer: TiKvWorkspaceBackend,
    initializer: TiKvWorkspaceBackend,
    jobs: Jobs,
) {
    let (writer_entered, resume) = writer.test_control.arm();
    let inspect = writer.clone();
    let mut writing = jobs.spawn(async move {
        writer
            .compare_and_swap(
                &[],
                &[KvWrite::Put {
                    key: b"foreign".to_vec(),
                    value: b"before-bootstrap".to_vec(),
                }],
            )
            .await
    });
    let _writer_stamp = tokio::time::timeout(STEP_TIMEOUT, writer_entered)
        .await
        .unwrap()
        .unwrap();
    let (started, receiver) = oneshot::channel();
    *initializer.test_control.bootstrap_started.lock().unwrap() = Some(started);
    let before = initializer.server_time_ns().await.unwrap() + 60_000_000_000;
    let mut initializing = jobs.spawn(async move {
        initializer
            .compare_and_swap_in_empty_namespace(
                &[],
                &[KvWrite::Put {
                    key: b"control".to_vec(),
                    value: b"must-not-appear".to_vec(),
                }],
                before,
            )
            .await
    });
    // This transaction definitely began before the foreign write committed.
    let _old_snapshot = tokio::time::timeout(STEP_TIMEOUT, receiver)
        .await
        .unwrap()
        .unwrap();
    drop(resume);
    assert!(writing.finish().await.unwrap());
    assert!(
        !initializing.finish().await.unwrap(),
        "stale start snapshot incorrectly proved namespace empty"
    );
    assert_eq!(
        inspect.get(b"foreign").await.unwrap(),
        Some(b"before-bootstrap".to_vec())
    );
    assert!(inspect.get(b"control").await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_TIKV_PD_ENDPOINTS; actual pre-init barrier wait and fresh range snapshot"]
async fn real_tikv_manager_bootstrap_observes_writer_committed_after_its_start() {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let namespace = format!("commitred-{}", Uuid::new_v4().simple());
    let writer = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
        .await
        .unwrap();
    let initializer = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
        .await
        .unwrap();
    let inspector = TiKvWorkspaceBackend::connect(endpoints, &namespace)
        .await
        .unwrap();
    let jobs = Jobs::default();
    let case_jobs = jobs.clone();
    let mut task = tokio::spawn(bootstrap_wait_case(writer, initializer, case_jobs));
    let result = tokio::time::timeout(CASE_TIMEOUT, &mut task).await;
    if result.is_err() {
        task.abort();
        let _ = task.await;
    }
    jobs.stop_and_wait().await;
    cleanup_namespace(&inspector).await;
    inspector.shutdown().await.unwrap();
    assert!(
        matches!(result, Ok(Ok(()))),
        "bootstrap race contract failed: {result:?}"
    );
}
