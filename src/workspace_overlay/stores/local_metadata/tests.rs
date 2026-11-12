use super::*;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use tokio::sync::Semaphore;

struct Memory {
    rows: Mutex<Values>,
    commits: AtomicUsize,
    now: AtomicI64,
    fail: AtomicBool,
    lost_reply: AtomicBool,
    pause: AtomicBool,
    reached: Semaphore,
    resume: Semaphore,
}
impl Default for Memory {
    fn default() -> Self {
        Self {
            rows: Mutex::new(Values::new()),
            commits: AtomicUsize::new(0),
            now: AtomicI64::new(1),
            fail: AtomicBool::new(false),
            lost_reply: AtomicBool::new(false),
            pause: AtomicBool::new(false),
            reached: Semaphore::new(0),
            resume: Semaphore::new(0),
        }
    }
}
#[async_trait]
impl WorkspaceKvBackend for Arc<Memory> {
    fn name(&self) -> &'static str {
        "local-metadata-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        Ok(self.rows.lock().await.get(key).cloned().flatten())
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let rows = self.rows.lock().await;
        Ok((
            keys.iter()
                .map(|k| rows.get(k).cloned().flatten())
                .collect(),
            self.now.load(Ordering::SeqCst),
        ))
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        limits.validate_keys(keys)?;
        self.get_many_consistent_with_time(keys).await
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        Ok(self
            .rows
            .lock()
            .await
            .iter()
            .filter_map(|(key, value)| {
                value
                    .as_ref()
                    .filter(|_| key.starts_with(prefix))
                    .map(|value| KvEntry {
                        key: key.clone(),
                        value: value.clone(),
                    })
            })
            .collect())
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.compare_and_swap_before(checks, writes, i64::MAX).await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        if self.pause.swap(false, Ordering::SeqCst) {
            self.reached.add_permits(1);
            self.resume.acquire().await.unwrap().forget();
        }
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(WorkspaceError::Backend(
                "injected connection failure".into(),
            ));
        }
        let mut rows = self.rows.lock().await;
        if self.now.load(Ordering::SeqCst) >= deadline {
            return Err(WorkspaceError::Fenced);
        }
        if checks
            .iter()
            .any(|c| rows.get(&c.key).cloned().flatten() != c.expected)
        {
            return Ok(false);
        }
        for write in writes {
            match write {
                KvWrite::Put { key, value } => {
                    rows.insert(key.clone(), Some(value.clone()));
                }
                KvWrite::Delete { key } => {
                    rows.remove(key);
                }
            }
        }
        self.commits.fetch_add(1, Ordering::SeqCst);
        if self.lost_reply.swap(false, Ordering::SeqCst) {
            return Err(WorkspaceError::Backend("lost commit reply".into()));
        }
        Ok(true)
    }
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(self.now.load(Ordering::SeqCst))
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        deadline: i64,
        limits: KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        super::super::kv_backend::validate_bounded_authentication_checks(checks, limits)?;
        if self.now.load(Ordering::SeqCst) >= deadline {
            return Err(WorkspaceError::Fenced);
        }
        let rows = self.rows.lock().await;
        Ok(checks
            .iter()
            .all(|c| rows.get(&c.key).cloned().flatten() == c.expected))
    }
}

fn config(path: &Path) -> LocalMetadataConfig {
    LocalMetadataConfig {
        directory: path.into(),
        sync_on_update: true,
        flush_interval: Duration::from_millis(1000),
        max_operations: 1024,
        max_bytes: 1 << 20,
    }
}
// No timer in deterministic race tests. Production constructor has an idle timer.
fn backend(remote: Arc<Memory>, config: LocalMetadataConfig) -> LocalMetadataBackend {
    let scope = b"test-workspace";
    let (journal, state) = Journal::open(&config, scope).unwrap();
    let remote: Arc<dyn WorkspaceKvBackend> = Arc::new(remote);
    let core = Arc::new(Core {
        remote: remote.clone(),
        config,
        journal: Arc::new(journal),
        state: Arc::new(Mutex::new(state)),
        admission: Mutex::new(()),
        flush: Mutex::new(()),
        receipt_key: b"local-metadata/test".to_vec(),
        failed: AtomicBool::new(false),
    });
    LocalMetadataBackend {
        remote,
        core: Some(core),
    }
}
fn check(value: Option<u8>) -> Vec<KvCheck> {
    vec![KvCheck {
        key: b"inode/1".to_vec(),
        expected: value.map(|v| vec![v]),
    }]
}
fn put(value: u8) -> Vec<KvWrite> {
    vec![KvWrite::Put {
        key: b"inode/1".to_vec(),
        value: vec![value],
    }]
}
async fn edit(b: &LocalMetadataBackend, old: Option<u8>, new: u8) {
    assert!(
        b.compare_and_swap_metadata_before(&check(old), &put(new), 10000)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn repeated_file_edits_are_locally_visible_and_one_remote_commit() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(Memory::default());
    let b = backend(remote.clone(), config(dir.path()));
    for value in 0..100u8 {
        edit(&b, value.checked_sub(1), value).await;
        assert_eq!(b.get(b"inode/1").await.unwrap(), Some(vec![value]));
        assert_eq!(remote.get(b"inode/1").await.unwrap(), None);
    }
    b.flush_metadata().await.unwrap();
    assert_eq!(remote.get(b"inode/1").await.unwrap(), Some(vec![99]));
    assert_eq!(remote.commits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn old_ack_never_clears_new_edit_including_same_value() {
    for (durable, new) in [(true, 1), (true, 2), (false, 1), (false, 2)] {
        let dir = tempfile::tempdir().unwrap();
        let remote = Arc::new(Memory::default());
        let mut cfg = config(dir.path());
        cfg.sync_on_update = durable;
        let b = Arc::new(backend(remote.clone(), cfg));
        edit(&b, None, 1).await;
        remote.pause.store(true, Ordering::SeqCst);
        let core = b.core.clone().unwrap();
        let first = tokio::spawn(async move { core.flush_to(1).await });
        remote.reached.acquire().await.unwrap().forget();
        edit(&b, Some(1), new).await;
        remote.resume.add_permits(1);
        first.await.unwrap().unwrap();
        assert_eq!(b.get(b"inode/1").await.unwrap(), Some(vec![new]));
        assert_eq!(
            b.core
                .as_ref()
                .unwrap()
                .state
                .lock()
                .await
                .pending
                .operations,
            1
        );
        b.flush_metadata().await.unwrap();
        assert_eq!(remote.get(b"inode/1").await.unwrap(), Some(vec![new]));
        assert_eq!(remote.commits.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn uncertain_commit_is_resolved_by_atomic_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(Memory::default());
    let b = backend(remote.clone(), config(dir.path()));
    edit(&b, None, 1).await;
    remote.lost_reply.store(true, Ordering::SeqCst);
    b.flush_metadata().await.unwrap();
    b.flush_metadata().await.unwrap();
    assert_eq!(remote.commits.load(Ordering::SeqCst), 1);
    assert!(!b.core.as_ref().unwrap().state.lock().await.dirty());
}

#[tokio::test]
async fn clean_journal_reopens_after_another_mount_commits() {
    for durable in [true, false] {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let remote = Arc::new(Memory::default());
        for (directory, old, new) in [
            (first_dir.path(), None, 1),
            (second_dir.path(), Some(1), 2),
            (first_dir.path(), Some(2), 3),
        ] {
            let mut cfg = config(directory);
            cfg.sync_on_update = durable;
            let b = LocalMetadataBackend::new(remote.clone(), Some(cfg), b"returning-workspace")
                .await
                .unwrap();
            edit(&b, old, new).await;
            b.flush_metadata().await.unwrap();
            assert_eq!(remote.get(b"inode/1").await.unwrap(), Some(vec![new]));
        }
        assert_eq!(remote.commits.load(Ordering::SeqCst), 3);
    }
}

#[tokio::test]
async fn failed_commit_retains_journal_and_replays_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(Memory::default());
    let b = backend(remote.clone(), config(dir.path()));
    edit(&b, None, 1).await;
    remote.fail.store(true, Ordering::SeqCst);
    assert!(b.flush_metadata().await.is_err());
    drop(b);
    let b = backend(remote.clone(), config(dir.path()));
    assert_eq!(b.get(b"inode/1").await.unwrap(), Some(vec![1]));
    b.flush_metadata().await.unwrap();
    assert_eq!(remote.commits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn committed_but_unacknowledged_batch_recovers_without_reapply() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(Memory::default());
    let b = backend(remote.clone(), config(dir.path()));
    edit(&b, None, 1).await;
    let stale = std::fs::read(dir.path().join("state.bin")).unwrap();
    b.flush_metadata().await.unwrap();
    drop(b);
    std::fs::write(dir.path().join("state.bin"), stale).unwrap();
    let b = backend(remote.clone(), config(dir.path()));
    b.flush_metadata().await.unwrap();
    assert_eq!(remote.commits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn expired_lease_and_foreign_changes_preserve_pending() {
    for expire in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let remote = Arc::new(Memory::default());
        let b = backend(remote.clone(), config(dir.path()));
        edit(&b, None, 1).await;
        if expire {
            remote.now.store(10000, Ordering::SeqCst);
        } else {
            remote
                .rows
                .lock()
                .await
                .insert(b"inode/1".to_vec(), Some(vec![9]));
        }
        assert!(matches!(
            b.flush_metadata().await,
            Err(WorkspaceError::Fenced)
        ));
        assert!(b.core.as_ref().unwrap().state.lock().await.dirty());
        assert_eq!(remote.commits.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn queue_capacity_drains_instead_of_evicting_dirty_values() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(Memory::default());
    let mut cfg = config(dir.path());
    cfg.max_operations = 2;
    let b = backend(remote.clone(), cfg);
    edit(&b, None, 1).await;
    edit(&b, Some(1), 2).await;
    edit(&b, Some(2), 3).await;
    assert_eq!(remote.get(b"inode/1").await.unwrap(), Some(vec![2]));
    assert_eq!(b.get(b"inode/1").await.unwrap(), Some(vec![3]));
    b.flush_metadata().await.unwrap();
    assert_eq!(remote.get(b"inode/1").await.unwrap(), Some(vec![3]));
}

#[tokio::test]
async fn delete_tombstone_and_namespace_barrier_preserve_order() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(Memory::default());
    let b = backend(remote.clone(), config(dir.path()));
    edit(&b, None, 1).await;
    assert!(
        b.compare_and_swap_metadata_before(
            &check(Some(1)),
            &[KvWrite::Delete {
                key: b"inode/1".to_vec()
            }],
            10000
        )
        .await
        .unwrap()
    );
    assert_eq!(b.get(b"inode/1").await.unwrap(), None);
    assert!(b.scan_prefix(b"inode/").await.unwrap().is_empty());
    assert_eq!(remote.commits.load(Ordering::SeqCst), 1);
    edit(&b, None, 3).await;
    assert!(
        b.compare_and_swap_before(&check(Some(3)), &put(4), 10000)
            .await
            .unwrap()
    );
    assert_eq!(remote.get(b"inode/1").await.unwrap(), Some(vec![4]));
}

#[tokio::test]
async fn idle_timer_flushes_without_another_write() {
    for sync_on_update in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let remote = Arc::new(Memory::default());
        let mut cfg = config(dir.path());
        cfg.flush_interval = Duration::from_millis(5);
        cfg.sync_on_update = sync_on_update;
        let b = LocalMetadataBackend::new(remote.clone(), Some(cfg), b"idle")
            .await
            .unwrap();
        edit(&b, None, 1).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while remote.get(b"inode/1").await.unwrap() != Some(vec![1]) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        b.flush_metadata().await.unwrap();
    }
}

#[test]
fn journal_rejects_wrong_scope_corruption_and_second_writer() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let (journal, state) = Journal::open(&cfg, b"one").unwrap();
    journal.persist(&state).unwrap();
    assert!(Journal::open(&cfg, b"one").is_err());
    drop(journal);
    assert!(Journal::open(&cfg, b"two").is_err());
    let path = dir.path().join("state.bin");
    let mut bytes = std::fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    std::fs::write(path, bytes).unwrap();
    assert!(Journal::open(&cfg, b"one").is_err());
}

#[tokio::test]
async fn buffered_updates_are_volatile_until_flush_then_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(Memory::default());
    let mut cfg = config(dir.path());
    cfg.sync_on_update = false;
    let b = backend(remote.clone(), cfg.clone());
    edit(&b, None, 1).await;
    assert_eq!(b.get(b"inode/1").await.unwrap(), Some(vec![1]));
    assert_eq!(remote.get(b"inode/1").await.unwrap(), None);
    assert!(!dir.path().join("state.bin").exists());
    drop(b);
    let b = backend(remote.clone(), cfg.clone());
    assert_eq!(b.get(b"inode/1").await.unwrap(), None);
    edit(&b, None, 2).await;
    b.flush_metadata().await.unwrap();
    drop(b);
    let b = backend(remote.clone(), cfg);
    assert_eq!(b.get(b"inode/1").await.unwrap(), Some(vec![2]));
    assert_eq!(remote.get(b"inode/1").await.unwrap(), Some(vec![2]));
}

#[tokio::test]
async fn remote_mode_remains_synchronous() {
    let remote = Arc::new(Memory::default());
    let b = LocalMetadataBackend::new(remote.clone(), None, b"remote")
        .await
        .unwrap();
    edit(&b, None, 1).await;
    edit(&b, Some(1), 2).await;
    assert_eq!(remote.get(b"inode/1").await.unwrap(), Some(vec![2]));
    assert_eq!(remote.commits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn local_head_authentication_keeps_remote_fences_without_forcing_commit() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(Memory::default());
    let b = backend(remote.clone(), config(dir.path()));
    let limits = KvReadLimits {
        max_records: 1,
        max_key_bytes: 100,
        max_value_bytes: 100,
        max_total_bytes: 1000,
        max_response_bytes: 1024,
        max_data_requests: 1,
    };
    edit(&b, None, 1).await;
    assert!(
        b.authenticate_checks_before_bounded(&check(Some(1)), 10000, limits)
            .await
            .unwrap()
    );
    assert_eq!(remote.commits.load(Ordering::SeqCst), 0);
    assert!(
        !b.authenticate_checks_before_bounded(&check(Some(9)), 10000, limits)
            .await
            .unwrap()
    );
    remote
        .rows
        .lock()
        .await
        .insert(b"inode/1".to_vec(), Some(vec![9]));
    assert!(
        !b.authenticate_checks_before_bounded(&check(Some(1)), 10000, limits)
            .await
            .unwrap()
    );
    remote.now.store(10000, Ordering::SeqCst);
    assert!(matches!(
        b.authenticate_checks_before_bounded(&check(Some(1)), 10000, limits)
            .await,
        Err(WorkspaceError::Fenced)
    ));
}

#[tokio::test]
async fn cancelled_awaiter_does_not_race_journal_installation() {
    let dir = tempfile::tempdir().unwrap();
    let remote = Arc::new(Memory::default());
    let b = Arc::new(backend(remote.clone(), config(dir.path())));
    let (reached, wait) = tokio::sync::oneshot::channel();
    let (resume, pause) = std::sync::mpsc::channel();
    *b.core.as_ref().unwrap().journal.pause.lock().unwrap() = Some((reached, pause));
    let first_backend = b.clone();
    let first = tokio::spawn(async move { edit(&first_backend, None, 1).await });
    wait.await.unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second_backend = b.clone();
    let second = tokio::spawn(async move { edit(&second_backend, Some(1), 2).await });
    resume.send(()).unwrap();
    second.await.unwrap();
    assert_eq!(b.get(b"inode/1").await.unwrap(), Some(vec![2]));
    drop(b);
    let b = backend(remote.clone(), config(dir.path()));
    b.flush_metadata().await.unwrap();
    assert_eq!(remote.get(b"inode/1").await.unwrap(), Some(vec![2]));
}

/// Runs only against an explicitly supplied, disposable local Redis instance.
#[tokio::test]
#[ignore = "requires BREWFS_LOCAL_SYNC_REDIS_URL pointing at disposable local Redis"]
async fn local_metadata_real_redis_pairs() {
    use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
    let url = std::env::var("BREWFS_LOCAL_SYNC_REDIS_URL").unwrap();
    assert!(url.starts_with("redis://127.0.0.1:") || url.starts_with("redis://localhost:"));
    let client = redis::Client::open(url.clone()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    for pair in 0..3 {
        for mode in if pair % 2 == 0 {
            ["remote", "local-journal", "local-buffered"]
        } else {
            ["local-buffered", "local-journal", "remote"]
        } {
            let namespace = format!("local-sync-{}", uuid::Uuid::new_v4());
            let remote = RedisWorkspaceBackend::connect(&url, &namespace)
                .await
                .unwrap();
            let reader = RedisWorkspaceBackend::connect(&url, &namespace)
                .await
                .unwrap();
            let deadline = remote.server_time_ns().await.unwrap() + 60_000_000_000;
            let dir = tempfile::tempdir().unwrap();
            let b = LocalMetadataBackend::new(
                remote,
                (mode != "remote").then(|| {
                    let mut cfg = config(dir.path());
                    cfg.flush_interval = Duration::from_millis(10);
                    cfg.sync_on_update = mode == "local-journal";
                    cfg
                }),
                namespace.as_bytes(),
            )
            .await
            .unwrap();
            let before: String = redis::cmd("INFO")
                .arg("commandstats")
                .query_async(&mut connection)
                .await
                .unwrap();
            let start = std::time::Instant::now();
            let mut samples = Vec::new();
            for value in 0..100u8 {
                let operation = std::time::Instant::now();
                assert!(
                    b.compare_and_swap_metadata_before(
                        &check(value.checked_sub(1)),
                        &put(value),
                        deadline
                    )
                    .await
                    .unwrap()
                );
                assert_eq!(b.get(b"inode/1").await.unwrap(), Some(vec![value]));
                samples.push(operation.elapsed().as_nanos() as u64);
            }
            let active_ns = start.elapsed().as_nanos() as u64;
            b.flush_metadata().await.unwrap();
            let total_ns = start.elapsed().as_nanos() as u64;
            assert_eq!(reader.get(b"inode/1").await.unwrap(), Some(vec![99]));
            let after: String = redis::cmd("INFO")
                .arg("commandstats")
                .query_async(&mut connection)
                .await
                .unwrap();
            println!(
                "LOCAL_SYNC_RESULT {}",
                serde_json::json!({"pair": pair, "mode": mode, "operations":100, "active_ns":active_ns,"total_with_drain_ns":total_ns,"operation_ns":samples,"redis_before":before,"redis_after":after,"final_value":99})
            );
        }
    }
}
