// Included inside tikv.rs::tests::commit_failure_candidates.
use super::super::*;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tikv_client::transaction::ResolveLocksOptions;
use tikv_client::{Timestamp, TimestampExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Semaphore, oneshot};
use tokio::task::{AbortHandle, JoinHandle};
use uuid::Uuid;

const STEP_TIMEOUT: Duration = Duration::from_secs(30);
const CASE_TIMEOUT: Duration = Duration::from_secs(150);
const GUARD: &[u8] = b"commit-red/guard";
const TARGET: &[u8] = b"commit-red/target";

struct ArmedPause {
    entered: oneshot::Sender<Timestamp>,
    resume: Arc<Semaphore>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LockObservation {
    start_version: u64,
    key: Vec<u8>,
    returns_value: bool,
}

#[derive(Default)]
pub(in super::super) struct CasTestControl {
    active: AtomicBool,
    attempts: AtomicU64,
    pause: Mutex<Option<ArmedPause>>,
    commit_errors: Mutex<Vec<String>>,
    commit_failure_kinds: Mutex<Vec<&'static str>>,
    observe_locks: AtomicBool,
    lock_events: Mutex<Vec<LockObservation>>,
    bootstrap_started: Mutex<Option<oneshot::Sender<Timestamp>>>,
}

impl CasTestControl {
    pub(in super::super) fn before_bootstrap_lock(&self, timestamp: Timestamp) {
        if let Some(entered) = self.bootstrap_started.lock().unwrap().take() {
            let _ = entered.send(timestamp);
        }
    }
    fn start_lock_observation(&self) {
        assert!(!self.active.swap(true, Ordering::SeqCst));
        assert!(!self.observe_locks.swap(true, Ordering::SeqCst));
        assert!(self.pause.lock().unwrap().is_none());
        self.attempts.store(0, Ordering::SeqCst);
        self.lock_events.lock().unwrap().clear();
    }

    pub(in super::super) fn record_lock(
        &self,
        timestamp: Timestamp,
        key: &[u8],
        returns_value: bool,
    ) {
        if self.observe_locks.load(Ordering::SeqCst) {
            self.lock_events.lock().unwrap().push(LockObservation {
                start_version: timestamp.version(),
                key: key.to_vec(),
                returns_value,
            });
        }
    }

    fn finish_lock_observation(&self) -> (Vec<LockObservation>, u64) {
        assert!(self.observe_locks.swap(false, Ordering::SeqCst));
        assert!(self.active.swap(false, Ordering::SeqCst));
        (
            std::mem::take(&mut *self.lock_events.lock().unwrap()),
            self.attempts.load(Ordering::SeqCst),
        )
    }

    pub(in super::super) fn begin_attempt(&self) {
        if self.active.load(Ordering::SeqCst) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub(in super::super) async fn before_commit(&self, timestamp: Timestamp) {
        let pause = self.pause.lock().unwrap().take();
        if let Some(pause) = pause {
            let _ = pause.entered.send(timestamp);
            tokio::time::timeout(STEP_TIMEOUT, pause.resume.acquire())
                .await
                .expect("commit boundary was never resumed")
                .unwrap()
                .forget();
        }
    }

    pub(in super::super) fn record_commit_result(
        &self,
        result: &Result<Option<Timestamp>, tikv_client::Error>,
    ) {
        if self.active.load(Ordering::SeqCst)
            && let Err(error) = result
        {
            self.commit_errors.lock().unwrap().push(error.to_string());
            self.commit_failure_kinds
                .lock()
                .unwrap()
                .push(commit_failure_kind(error));
        }
    }

    fn arm(&self) -> (oneshot::Receiver<Timestamp>, ResumeOnDrop) {
        assert!(!self.active.swap(true, Ordering::SeqCst));
        let (entered, receiver) = oneshot::channel();
        let resume = Arc::new(Semaphore::new(0));
        assert!(
            self.pause
                .lock()
                .unwrap()
                .replace(ArmedPause {
                    entered,
                    resume: resume.clone(),
                })
                .is_none()
        );
        (receiver, ResumeOnDrop(resume))
    }
}

struct ResumeOnDrop(Arc<Semaphore>);
impl Drop for ResumeOnDrop {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

struct JobDone(Arc<Semaphore>);
impl Drop for JobDone {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

type Retirement = (AbortHandle, Arc<Semaphore>);
#[derive(Clone, Default)]
struct Jobs(Arc<Mutex<Vec<Retirement>>>);
impl Jobs {
    fn spawn<T: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = T> + Send + 'static,
    ) -> Flight<T> {
        let done = Arc::new(Semaphore::new(0));
        let completion = JobDone(done.clone());
        let task = tokio::spawn(async move {
            let _completion = completion;
            future.await
        });
        self.0.lock().unwrap().push((task.abort_handle(), done));
        Flight(task)
    }

    async fn stop_and_wait(&self) {
        let jobs = std::mem::take(&mut *self.0.lock().unwrap());
        for (abort, _) in &jobs {
            abort.abort();
        }
        for (_, done) in jobs {
            tokio::time::timeout(STEP_TIMEOUT, done.acquire())
                .await
                .expect("CAS task did not retire before namespace cleanup")
                .unwrap()
                .forget();
        }
    }
}

struct Flight<T>(JoinHandle<T>);
impl<T> Drop for Flight<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl<T> Flight<T> {
    async fn finish(&mut self) -> T {
        tokio::time::timeout(STEP_TIMEOUT, &mut self.0)
            .await
            .expect("resumed real CAS did not finish")
            .unwrap()
    }
}

fn namespace_range(backend: &TiKvWorkspaceBackend) -> BoundRange {
    let lower = backend.scoped(b"");
    let upper =
        prefix_range_end(&lower).expect("ASCII UUID namespace must have a finite upper bound");
    BoundRange::new(
        Bound::Included(Key::from(lower)),
        Bound::Excluded(Key::from(upper)),
    )
}

async fn scoped_locks(
    backend: &TiKvWorkspaceBackend,
    through: &Timestamp,
) -> Vec<tikv_client::transaction::ProtoLockInfo> {
    let locks = tokio::time::timeout(
        STEP_TIMEOUT,
        backend
            .main_client()
            .await
            .unwrap()
            .scan_locks(through, namespace_range(backend), 16),
    )
    .await
    .expect("scoped lock inventory deadline exceeded")
    .unwrap();
    assert!(
        locks.len() < 16,
        "test lock inventory exceeded its sentinel budget"
    );
    assert!(
        locks
            .iter()
            .all(|lock| lock.key.starts_with(&backend.prefix))
    );
    for lock in &locks {
        eprintln!(
            "real scoped lock inventory: namespace={}, key={}, primary={}, start={}, for_update={}, type={}, ttl={}",
            String::from_utf8_lossy(&backend.prefix),
            String::from_utf8_lossy(&lock.key),
            String::from_utf8_lossy(&lock.primary_lock),
            lock.lock_version,
            lock.lock_for_update_ts,
            lock.lock_type,
            lock.lock_ttl,
        );
    }
    locks
}

async fn revoke_test_pessimistic_locks(
    backend: &TiKvWorkspaceBackend,
    through: &Timestamp,
    locks: &[tikv_client::transaction::ProtoLockInfo],
) {
    assert!(!locks.is_empty() && locks.len() <= 2);
    assert!(locks.iter().all(|lock| lock.lock_type == 5));
    let namespace = std::str::from_utf8(&backend.prefix)
        .unwrap()
        .strip_suffix("/ws:v1/")
        .unwrap();
    let helper = std::env::var("BREWFS_TEST_TIKV_ROLLBACK_FAULT_HELPER")
        .expect("absolute Python rollback fault helper path is required");
    assert!(std::path::Path::new(&helper).is_absolute());
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let inventory = serde_json::json!({
        "namespace": namespace,
        "pd_endpoints": endpoints,
        "through_version": through.version(),
        "locks": locks.iter().map(|lock| serde_json::json!({
            "key": lock.key,
            "primary_lock": lock.primary_lock,
            "lock_version": lock.lock_version,
            "lock_for_update_ts": lock.lock_for_update_ts,
            "lock_type": lock.lock_type,
        })).collect::<Vec<_>>(),
    });
    // Direct server fault only: do not call rollback() on A's transaction,
    // which would change its Rust status before the intended real commit.
    let payload = serde_json::to_vec(&inventory).unwrap();
    let mut child = tokio::process::Command::new("/usr/bin/python3")
        .arg(helper)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("could not start exact scoped rollback fault helper");
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let status = tokio::time::timeout(STEP_TIMEOUT, async {
        stdin.write_all(&payload).await.unwrap();
        stdin.shutdown().await.unwrap();
        drop(stdin);
        child.wait().await.unwrap()
    })
    .await;
    let status = match status {
        Ok(status) => status,
        Err(_) => {
            // Await process retirement before the case performs namespace
            // cleanup; kill_on_drop also covers parent-task cancellation.
            child.kill().await.expect("rollback helper did not retire");
            panic!("exact scoped rollback fault helper deadline exceeded");
        }
    };
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();
    stdout.read_to_end(&mut stdout_bytes).await.unwrap();
    stderr.read_to_end(&mut stderr_bytes).await.unwrap();
    assert!(
        status.success(),
        "exact rollback fault failed: {}",
        String::from_utf8_lossy(&stderr_bytes)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&stdout_bytes)
        .expect("rollback fault helper did not return a JSON receipt");
    eprintln!("real scoped pessimistic rollback receipt: {receipt}");
    assert_eq!(receipt["namespace"].as_str(), Some(namespace));
    assert_eq!(
        receipt["locks_requested"].as_u64(),
        Some(locks.len() as u64)
    );
    assert_eq!(receipt["pd_gc_safepoint_changed"].as_bool(), Some(false));
    let rpc_receipts = receipt["rpc_receipts"].as_array().unwrap();
    assert!(!rpc_receipts.is_empty() && rpc_receipts.len() <= locks.len());
    assert!(rpc_receipts.iter().all(|rpc| {
        rpc["grpc_and_server_success"].as_bool() == Some(true)
            && locks.iter().any(|lock| {
                rpc["start_version"].as_u64() == Some(lock.lock_version)
                    && rpc["for_update_ts"].as_u64() == Some(lock.lock_for_update_ts)
            })
    }));
}

async fn force_test_namespace_locks(backend: &TiKvWorkspaceBackend, through: &Timestamp) {
    // Cleanup after all test tasks have retired. First remove exactly observed
    // pure pessimistic locks, whose SDK cleanup in attempt03 did not clear.
    // For any prewritten locks, preserve the SDK's real transaction resolution.
    // Neither path changes PD's global GC safepoint.
    let locks = scoped_locks(backend, through).await;
    let pessimistic = locks
        .iter()
        .filter(|lock| lock.lock_type == 5)
        .cloned()
        .collect::<Vec<_>>();
    if !pessimistic.is_empty() {
        revoke_test_pessimistic_locks(backend, through, &pessimistic).await;
    }
    if scoped_locks(backend, through).await.is_empty() {
        return;
    }
    tokio::time::timeout(
        STEP_TIMEOUT,
        backend.main_client().await.unwrap().cleanup_locks(
            namespace_range(backend),
            through,
            ResolveLocksOptions {
                async_commit_only: false,
                batch_size: 16,
            },
        ),
    )
    .await
    .expect("scoped forced lock cleanup deadline exceeded")
    .unwrap();
    assert!(scoped_locks(backend, through).await.is_empty());
}

async fn revoked_lock_case(
    a: TiKvWorkspaceBackend,
    b: TiKvWorkspaceBackend,
    inspector: TiKvWorkspaceBackend,
    jobs: Jobs,
) {
    establish_catalog_namespace(&b).await;
    let initial_checks = [
        KvCheck {
            key: GUARD.to_vec(),
            expected: None,
        },
        KvCheck {
            key: TARGET.to_vec(),
            expected: None,
        },
    ];
    let initial_writes = [
        KvWrite::Put {
            key: GUARD.to_vec(),
            value: b"old-guard".to_vec(),
        },
        KvWrite::Put {
            key: TARGET.to_vec(),
            value: b"old-target".to_vec(),
        },
    ];
    assert!(
        b.compare_and_swap(&initial_checks, &initial_writes)
            .await
            .unwrap()
    );
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
    let expiry = b
        .server_time_ns()
        .await
        .unwrap()
        .checked_add(60_000_000_000)
        .unwrap();
    let control = a.test_control.clone();
    let (entered, resume) = control.arm();
    let operation_checks = checks.clone();
    let mut operation = jobs.spawn(async move {
        a.compare_and_swap_before(&operation_checks, &writes, expiry)
            .await
    });
    let timestamp = tokio::time::timeout(STEP_TIMEOUT, entered)
        .await
        .expect("actual locked pre-commit boundary was not reached")
        .unwrap();
    assert_eq!(control.attempts.load(Ordering::SeqCst), 1);
    let locks = scoped_locks(&inspector, &timestamp).await;
    assert_eq!(
        locks.len(),
        2,
        "both checked/write keys must have actual TiKV locks"
    );
    assert!(
        locks
            .iter()
            .all(|lock| lock.lock_version == timestamp.version()),
        "fault injection must only revoke the paused transaction incarnation"
    );
    let mut locked_keys = locks
        .iter()
        .map(|lock| lock.key.clone())
        .collect::<Vec<_>>();
    locked_keys.sort();
    let mut expected_keys = vec![inspector.scoped(GUARD), inspector.scoped(TARGET)];
    expected_keys.sort();
    assert_eq!(locked_keys, expected_keys);
    assert!(
        locks.iter().all(|lock| lock.lock_type == 5),
        "the real fault prerequisite requires actual pure pessimistic locks"
    );
    revoke_test_pessimistic_locks(&inspector, &timestamp, &locks).await;
    assert!(
        scoped_locks(&inspector, &timestamp).await.is_empty(),
        "successful rollback replies must independently prove both locks absent"
    );
    let peer_writes = [
        KvWrite::Put {
            key: GUARD.to_vec(),
            value: b"peer-guard".to_vec(),
        },
        KvWrite::Put {
            key: TARGET.to_vec(),
            value: b"peer-target".to_vec(),
        },
    ];
    assert!(b.compare_and_swap(&checks, &peer_writes).await.unwrap());
    drop(resume);
    let result = operation.finish().await;
    let attempts = control.attempts.load(Ordering::SeqCst);
    let errors = control.commit_errors.lock().unwrap().clone();
    control.active.store(false, Ordering::SeqCst);
    let values = b
        .get_many_consistent(&[GUARD.to_vec(), TARGET.to_vec()])
        .await
        .unwrap();
    assert_eq!(
        values,
        vec![Some(b"peer-guard".to_vec()), Some(b"peer-target".to_vec())]
    );
    eprintln!(
        "real revoked-lock commit receipt: attempts={attempts}, result={result:?}, commit_errors={errors:?}"
    );
    assert!(
        !errors.is_empty(),
        "the real SDK must report the failed prewrite"
    );
    assert!(
        errors[0]
            .to_ascii_lowercase()
            .contains("pessimisticlocknotfound")
            || errors[0]
                .to_ascii_lowercase()
                .contains("pessimistic_lock_not_found"),
        "server fault did not produce the intended typed lock loss: {errors:?}"
    );
    assert!(
        is_retryable(&errors[0]),
        "RED prerequisite: old policy must classify the actual SDK error as retryable"
    );
    assert!(
        matches!(result, Err(WorkspaceError::Backend(_))),
        "commit failure must retain Err rather than turn into a later mismatch: {result:?}"
    );
    assert_eq!(
        attempts, 1,
        "commit failure must not start another CAS transaction"
    );
}

// Fault/lock-order contracts below target established-catalog CAS. Bootstrap
// has its own extra serialization lock and its own separate race contract.
async fn establish_catalog_namespace(backend: &TiKvWorkspaceBackend) {
    use crate::workspace_overlay::catalog::WorkspaceStore;
    crate::workspace_overlay::stores::kv_store::KvWorkspaceStore::new(backend.clone())
        .initialize_workspace_schema()
        .await
        .unwrap();
}

async fn cleanup_namespace(backend: &TiKvWorkspaceBackend) {
    let timestamp = backend
        .main_client()
        .await
        .unwrap()
        .current_timestamp()
        .await
        .unwrap();
    let _ = scoped_locks(backend, &timestamp).await;
    force_test_namespace_locks(backend, &timestamp).await;
    for _ in 0..8 {
        let rows = backend.scan_prefix_bounded(b"", 16).await.unwrap();
        if rows.is_empty() {
            return;
        }
        let checks = rows
            .iter()
            .map(|row| KvCheck {
                key: row.key.clone(),
                expected: Some(row.value.clone()),
            })
            .collect::<Vec<_>>();
        let writes = rows
            .into_iter()
            .map(|row| KvWrite::Delete { key: row.key })
            .collect::<Vec<_>>();
        assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
    }
    panic!("test namespace cleanup exceeded its fixed key budget");
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_TIKV_PD_ENDPOINTS; real scoped lock revocation"]
async fn real_tikv_commit_failure_does_not_replay_as_mismatch() {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .expect("isolated BREWFS_TEST_TIKV_PD_ENDPOINTS is required")
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let namespace = format!("commitred-{}", Uuid::new_v4().simple());
    eprintln!("real commit-failure namespace: {namespace}");
    let a = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
        .await
        .unwrap();
    let b = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
        .await
        .unwrap();
    let inspector = TiKvWorkspaceBackend::connect(endpoints, &namespace)
        .await
        .unwrap();
    let cleanup = inspector.clone();
    let jobs = Jobs::default();
    let case_jobs = jobs.clone();
    let mut task = tokio::spawn(async move { revoked_lock_case(a, b, inspector, case_jobs).await });
    let (result, timed_out) = match tokio::time::timeout(CASE_TIMEOUT, &mut task).await {
        Ok(result) => (result, false),
        Err(_) => {
            task.abort();
            (task.await, true)
        }
    };
    jobs.stop_and_wait().await;
    tokio::time::timeout(STEP_TIMEOUT, cleanup_namespace(&cleanup))
        .await
        .expect("namespace cleanup exceeded its overall deadline");
    assert!(!timed_out, "real commit-failure case deadline exceeded");
    assert!(
        result.is_ok(),
        "real commit-failure contract failed: {result:?}"
    );
}
