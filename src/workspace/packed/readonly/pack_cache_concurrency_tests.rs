//! Controlled backend gates prove cache admission does not serialize unrelated HEADs.
//! No performance threshold is used: gates stay closed until each assertion is made.
use super::*;
use crate::cadapter::client::ObjectByteStream;
use crate::workspace::packed::PackedWireError;
use crate::workspace::packed::snapshot::SnapshotBudgetLimits;
use bytes::Bytes;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

const SIZE: u64 = 64;

#[derive(Default)]
struct HeadGate {
    open: AtomicBool,
    fail: AtomicBool,
    changed: Notify,
}

impl HeadGate {
    fn release(&self) {
        self.open.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    async fn wait(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.open.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

#[derive(Clone, Default)]
struct GatedBackend {
    gates: Arc<HashMap<String, Arc<HeadGate>>>,
    heads: Arc<Mutex<HashMap<String, usize>>>,
    gets: Arc<AtomicU64>,
    ranges: Arc<AtomicU64>,
    completed_heads: Arc<AtomicU64>,
    entered: Arc<Notify>,
}

impl GatedBackend {
    fn blocked(keys: &[&str]) -> Self {
        Self {
            gates: Arc::new(
                keys.iter()
                    .map(|key| ((*key).to_owned(), Arc::new(HeadGate::default())))
                    .collect(),
            ),
            ..Default::default()
        }
    }

    fn heads(&self, key: &str) -> usize {
        self.heads.lock().unwrap().get(key).copied().unwrap_or(0)
    }

    async fn entered(&self, key: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changed = self.entered.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.heads(key) > 0 {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("controlled HEAD did not start");
    }

    fn release(&self, key: &str) {
        self.gates[key].release();
    }
}

fn body(key: &str) -> Vec<u8> {
    vec![key.as_bytes()[0]; SIZE as usize]
}

#[async_trait::async_trait]
impl ObjectBackend for GatedBackend {
    async fn put_object(&self, _: &str, _: &[u8]) -> Result<()> {
        unreachable!()
    }

    async fn get_object(&self, _: &str) -> Result<Option<Vec<u8>>> {
        unreachable!()
    }

    async fn get_etag(&self, _: &str) -> Result<String> {
        Ok("immutable".into())
    }

    async fn delete_object(&self, _: &str) -> Result<()> {
        unreachable!()
    }

    async fn get_object_size(&self, key: &str) -> Result<Option<u64>> {
        *self
            .heads
            .lock()
            .unwrap()
            .entry(key.to_owned())
            .or_default() += 1;
        self.entered.notify_waiters();
        if let Some(gate) = self.gates.get(key) {
            gate.wait().await;
            ensure!(
                !gate.fail.swap(false, Ordering::AcqRel),
                "injected HEAD failure"
            );
        }
        self.completed_heads.fetch_add(1, Ordering::Relaxed);
        self.entered.notify_waiters();
        match key {
            "limit-error" => return Err(PackedWireError::LimitExceeded("injected".into()).into()),
            "admission-error" => {
                return Err(crate::cadapter::read_observer::ReadBoundaryError::Admission.into());
            }
            "short-error" => {
                return Err(crate::cadapter::read_observer::ReadBoundaryError::Short {
                    expected: 64,
                    received: 12,
                }
                .into());
            }
            "excess-error" => {
                return Err(crate::cadapter::read_observer::ReadBoundaryError::Excess.into());
            }
            _ => {}
        }
        Ok(Some(SIZE))
    }

    async fn get_object_stream(&self, key: &str) -> Result<Option<ObjectByteStream>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        Ok(Some(Box::pin(futures_util::stream::iter([Ok(
            Bytes::from(body(key)),
        )]))))
    }

    async fn get_object_range(&self, key: &str, offset: u64, bytes: &mut [u8]) -> Result<usize> {
        self.ranges.fetch_add(1, Ordering::Relaxed);
        let length = bytes.len();
        bytes.copy_from_slice(&body(key)[offset as usize..offset as usize + length]);
        Ok(length)
    }
}

fn setup(backend: &GatedBackend, control: u64) -> (Arc<PackCache>, ObjectClient<GatedBackend>) {
    let mut limits = SnapshotBudgetLimits::default();
    limits.bytes[SnapshotBudgetPool::Control as usize] = control;
    let cache = PackCache::new(
        16 * SIZE,
        SnapshotMountBudget::new(limits).unwrap(),
        Arc::new(ReadonlyTransport::default()),
        Arc::new(BackendIoCounters::default()),
    );
    (cache, ObjectClient::new(backend.clone()))
}

async fn join_workers(cache: &PackCache) {
    let workers = std::mem::take(&mut *cache.workers.lock().unwrap());
    for worker in workers {
        worker.await.unwrap();
    }
}

async fn clean(cache: &PackCache) {
    cache.drain().await.unwrap();
    cache.transport.drain().await.unwrap();
    assert_eq!(cache.occupancy.live_entries.load(Ordering::Relaxed), 0);
    assert_eq!(
        cache.occupancy.live_reserved_bytes.load(Ordering::Relaxed),
        0
    );
    assert!(cache.budget.state().used.iter().all(|used| *used == 0));
}

#[tokio::test]
async fn cached_object_admission_does_not_wait_for_another_objects_head() {
    let backend = GatedBackend::blocked(&["slow"]);
    let (cache, client) = setup(&backend, 1 << 20);
    assert_eq!(
        cache.read(&client, "warm", 0, 8).await.unwrap(),
        body("warm")[..8]
    );
    join_workers(&cache).await;

    let mut slow = Box::pin(cache.prepare(&client, "slow", 0, 8, None));
    assert!(futures_util::poll!(slow.as_mut()).is_pending());
    backend.entered("slow").await;
    let mut warm = Box::pin(cache.prepare(&client, "warm", 0, 8, None));
    let ready = matches!(
        futures_util::poll!(warm.as_mut()),
        std::task::Poll::Ready(Ok(Some(_)))
    );

    backend.release("slow");
    drop(slow);
    drop(warm);
    clean(&cache).await;
    assert!(
        ready,
        "a completed cache entry waited behind an unrelated HEAD"
    );
}

#[tokio::test]
async fn independent_object_heads_can_overlap_without_increasing_download_slots() {
    let backend = GatedBackend::blocked(&["first", "second"]);
    let (cache, client) = setup(&backend, 1 << 20);
    let mut first = Box::pin(cache.prepare(&client, "first", 0, 8, None));
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    backend.entered("first").await;
    let mut second = Box::pin(cache.prepare(&client, "second", 0, 8, None));
    assert!(futures_util::poll!(second.as_mut()).is_pending());
    let overlaps = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let changed = backend.entered.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if backend.heads("second") == 1 {
                break;
            }
            changed.await;
        }
    })
    .await
    .is_ok();
    assert_eq!(cache.slots.available_permits(), 4);
    backend.release("first");
    backend.release("second");
    drop(first.await.unwrap());
    drop(second.await.unwrap());
    clean(&cache).await;
    assert!(
        overlaps,
        "the second HEAD could not start before releasing the first"
    );
    assert_eq!(backend.gets.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn same_key_followers_share_one_head_and_full_download() {
    let backend = GatedBackend::blocked(&["shared"]);
    let (cache, client) = setup(&backend, 1 << 20);
    let mut requests: Vec<_> = (0..8)
        .map(|_| Box::pin(cache.prepare(&client, "shared", 0, 8, None)))
        .collect();
    for request in &mut requests {
        assert!(futures_util::poll!(request.as_mut()).is_pending());
    }
    backend.entered("shared").await;
    backend.release("shared");
    let mut entries = Vec::new();
    for request in requests {
        entries.push(request.await.unwrap().unwrap());
    }
    assert!(entries.iter().all(|entry| Arc::ptr_eq(&entries[0], entry)));
    assert_eq!(
        cache.read_entry(&entries[0], 0, 8).await.unwrap(),
        body("shared")[..8]
    );
    drop(entries);
    clean(&cache).await;
    assert_eq!(backend.heads("shared"), 1);
    assert_eq!(backend.gets.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn cancelled_head_retains_key_and_slot_until_real_driver_ends() {
    let backend = GatedBackend::blocked(&["shared"]);
    let (cache, client) = setup(&backend, 1 << 20);
    let mut first = Box::pin(cache.prepare(&client, "shared", 0, 8, None));
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    backend.entered("shared").await;
    drop(first);
    assert_eq!(cache.head_slots.available_permits(), 3);
    let retained = cache.budget.state().used[SnapshotBudgetPool::Plans as usize];
    let mut follower = Box::pin(cache.prepare(&client, "shared", 0, 8, None));
    assert!(futures_util::poll!(follower.as_mut()).is_pending());
    backend.release("shared");
    let entry = follower.await.unwrap().unwrap();
    assert_eq!(
        cache.read_entry(&entry, 0, 8).await.unwrap(),
        body("shared")[..8]
    );
    drop(entry);
    clean(&cache).await;
    assert_eq!(retained, 2 * 4096 + 3 * "shared".len() as u64);
    assert_eq!(backend.heads("shared"), 2);
    assert_eq!(backend.gets.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn cancelling_one_follower_does_not_cancel_shared_admission() {
    let backend = GatedBackend::blocked(&["shared"]);
    let (cache, client) = setup(&backend, 1 << 20);
    let mut first = Box::pin(cache.prepare(&client, "shared", 0, 8, None));
    let mut follower = Box::pin(cache.prepare(&client, "shared", 0, 8, None));
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    backend.entered("shared").await;
    assert!(futures_util::poll!(follower.as_mut()).is_pending());
    drop(follower);
    backend.release("shared");
    let entry = first.await.unwrap().unwrap();
    assert_eq!(
        cache.read_entry(&entry, 0, 8).await.unwrap(),
        body("shared")[..8]
    );
    drop(entry);
    clean(&cache).await;
    assert_eq!(backend.heads("shared"), 1);
}

#[tokio::test]
async fn head_failure_releases_same_key_for_followers_retry() {
    let backend = GatedBackend::blocked(&["shared"]);
    backend.gates["shared"].fail.store(true, Ordering::Release);
    let (cache, client) = setup(&backend, 1 << 20);
    let mut first = Box::pin(cache.prepare(&client, "shared", 0, 8, None));
    let mut follower = Box::pin(cache.prepare(&client, "shared", 0, 8, None));
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    backend.entered("shared").await;
    assert!(futures_util::poll!(follower.as_mut()).is_pending());
    backend.release("shared");
    let first_failed = first.await.is_err();
    let entry = follower.await.unwrap().unwrap();
    assert_eq!(
        cache.read_entry(&entry, 0, 8).await.unwrap(),
        body("shared")[..8]
    );
    drop(entry);
    join_workers(&cache).await;
    let heads_after_failure = backend.heads("shared");
    assert_eq!(
        cache.read(&client, "shared", 0, 8).await.unwrap(),
        body("shared")[..8]
    );
    clean(&cache).await;
    assert!(first_failed);
    assert_eq!(heads_after_failure, 2);
    assert_eq!(backend.heads("shared"), 2);
}

#[tokio::test]
async fn pending_heads_are_plan_budgeted_before_network_admission() {
    let backend = GatedBackend::blocked(&["first"]);
    let mut limits = SnapshotBudgetLimits::default();
    let pending_bytes = 4096 + "first".len() as u64;
    limits.bytes[SnapshotBudgetPool::Plans as usize] = pending_bytes + DRIVER_HEADROOM;
    let cache = PackCache::new(
        16 * SIZE,
        SnapshotMountBudget::new(limits).unwrap(),
        Arc::new(ReadonlyTransport::default()),
        Arc::new(BackendIoCounters::default()),
    );
    let client = ObjectClient::new(backend.clone());
    let mut first = Box::pin(cache.prepare(&client, "first", 0, 8, None));
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    backend.entered("first").await;
    let mut other = Box::pin(cache.prepare(&client, "other", 0, 8, None));
    let fallback = matches!(
        futures_util::poll!(other.as_mut()),
        std::task::Poll::Ready(Ok(None))
    );
    let used = cache.budget.state().used[SnapshotBudgetPool::Plans as usize];
    backend.release("first");
    drop(other);
    drop(first.await.unwrap());
    clean(&cache).await;
    assert!(fallback);
    assert_eq!(used, 2 * 4096 + 3 * "first".len() as u64);
    assert_eq!(backend.heads("other"), 0);
}

#[tokio::test]
async fn cache_and_transport_drain_join_abandoned_head_without_new_download() {
    let backend = GatedBackend::blocked(&["first"]);
    let (cache, client) = setup(&backend, 1 << 20);
    let mut first = Box::pin(cache.prepare(&client, "first", 0, 8, None));
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    backend.entered("first").await;
    drop(first);
    let mut drain = Box::pin(async {
        cache.drain().await?;
        cache.transport.drain().await
    });
    let drains_before_head = futures_util::poll!(drain.as_mut()).is_ready();
    let mut other = Box::pin(cache.prepare(&client, "other", 0, 8, None));
    let rejected = matches!(
        futures_util::poll!(other.as_mut()),
        std::task::Poll::Ready(Err(_))
    );
    drop(other);
    backend.release("first");
    if !drains_before_head {
        drain.await.unwrap();
    }
    cache.transport.drain().await.unwrap();
    assert!(
        !drains_before_head,
        "the pending HEAD must stay owned until it ends"
    );
    assert!(rejected);
    assert_eq!(backend.heads("other"), 0);
    assert_eq!(backend.gets.load(Ordering::Relaxed), 0);
    assert!(cache.budget.state().used.iter().all(|used| *used == 0));
}

#[tokio::test]
async fn drain_wakes_pending_followers_and_budget_close_prevents_publication() {
    for close_budget in [false, true] {
        let backend = GatedBackend::blocked(&["first"]);
        let (cache, client) = setup(&backend, 1 << 20);
        let mut first = Box::pin(cache.prepare(&client, "first", 0, 8, None));
        let mut follower = Box::pin(cache.prepare(&client, "first", 0, 8, None));
        assert!(futures_util::poll!(first.as_mut()).is_pending());
        backend.entered("first").await;
        assert!(futures_util::poll!(follower.as_mut()).is_pending());
        if close_budget {
            cache.budget.close();
        } else {
            cache.drain().await.unwrap();
        }
        let follower_woke = matches!(
            futures_util::poll!(follower.as_mut()),
            std::task::Poll::Ready(Err(_))
        );
        drop(follower);
        // An already-issued HEAD remains a real transport owner until the
        // backend returns. The cache must reject publication afterwards.
        backend.release("first");
        assert!(first.await.is_err());
        cache.drain().await.unwrap();
        cache.transport.drain().await.unwrap();
        assert!(follower_woke);
        assert_eq!(backend.gets.load(Ordering::Relaxed), 0);
        assert!(cache.budget.state().used.iter().all(|used| *used == 0));
    }
}

#[tokio::test]
async fn cold_fanout_limits_actual_head_owners_through_registry_publication() {
    let keys: Vec<_> = (0..132).map(|index| format!("key-{index:04}")).collect();
    let borrowed: Vec<_> = keys.iter().map(String::as_str).collect();
    let backend = GatedBackend::blocked(&borrowed);
    let (cache, client) = setup(&backend, 1 << 20);
    let mut requests: Vec<_> = keys
        .iter()
        .map(|key| Box::pin(cache.prepare(&client, key, 0, 8, None)))
        .collect();
    for (index, request) in requests.iter_mut().enumerate() {
        assert!(futures_util::poll!(request.as_mut()).is_pending());
        if index < 4 {
            backend.entered(&keys[index]).await;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(backend.heads.lock().unwrap().values().sum::<usize>(), 4);
    assert_eq!(cache.slots.available_permits(), 4);
    assert_eq!(cache.pending.lock().unwrap().len(), keys.len());
    assert_eq!(
        cache.budget.state().used[SnapshotBudgetPool::Plans as usize],
        keys.iter().map(|key| 4096 + key.len() as u64).sum::<u64>()
            + keys[..4]
                .iter()
                .map(|key| 4096 + 2 * key.len() as u64)
                .sum::<u64>()
    );

    let registry = cache.registry.lock().await;
    for key in &keys {
        backend.release(key);
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let changed = backend.entered.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if backend.completed_heads.load(Ordering::Relaxed) == 4 {
                break;
            }
            changed.await;
        }
    })
    .await
    .unwrap();
    for request in &mut requests {
        assert!(futures_util::poll!(request.as_mut()).is_pending());
    }
    // Completed HEAD results still own their slots while publication is
    // blocked. Backend completion alone must not admit the next 128 probes.
    assert_eq!(cache.head_slots.available_permits(), 0);
    assert_eq!(backend.heads.lock().unwrap().values().sum::<usize>(), 4);
    drop(registry);
    for request in requests {
        drop(request.await.unwrap());
    }
    clean(&cache).await;
    assert_eq!(
        backend.heads.lock().unwrap().values().sum::<usize>(),
        keys.len()
    );
    assert_eq!(cache.pending.lock().unwrap().capacity(), 0);
}

#[tokio::test]
async fn drain_retires_queued_heads_without_starting_them() {
    let keys = ["first", "second", "third", "fourth", "queued"];
    let backend = GatedBackend::blocked(&keys);
    let (cache, client) = setup(&backend, 1 << 20);
    let mut requests: Vec<_> = keys
        .iter()
        .map(|key| Box::pin(cache.prepare(&client, key, 0, 8, None)))
        .collect();
    for (index, request) in requests.iter_mut().enumerate() {
        assert!(futures_util::poll!(request.as_mut()).is_pending());
        if index < 4 {
            backend.entered(keys[index]).await;
        }
    }
    drop(requests);
    let mut drain = Box::pin(async {
        cache.drain().await?;
        cache.transport.drain().await
    });
    assert!(futures_util::poll!(drain.as_mut()).is_pending());
    for key in &keys[..4] {
        backend.release(key);
    }
    drain.await.unwrap();
    cache.transport.drain().await.unwrap();
    assert_eq!(backend.heads("queued"), 0);
    assert_eq!(backend.gets.load(Ordering::Relaxed), 0);
    assert!(cache.budget.state().used.iter().all(|used| *used == 0));
}

#[tokio::test]
async fn completed_heads_release_plans_before_the_next_prepared_cache_hit() {
    let keys: Vec<_> = (0..300).map(|index| format!("key-{index:04}")).collect();
    let borrowed: Vec<_> = keys.iter().map(String::as_str).collect();
    let backend = GatedBackend::blocked(&borrowed);
    let mut limits = SnapshotBudgetLimits::default();
    limits.bytes[SnapshotBudgetPool::Plans as usize] = 5 << 20;
    let cache = PackCache::new(
        300 * SIZE,
        SnapshotMountBudget::new(limits).unwrap(),
        Arc::new(ReadonlyTransport::default()),
        Arc::new(BackendIoCounters::default()),
    );
    let client = ObjectClient::new(backend.clone());
    let mut requests: Vec<_> = keys
        .iter()
        .map(|key| Box::pin(cache.prepare(&client, key, 0, 8, None)))
        .collect();
    for request in &mut requests {
        assert!(futures_util::poll!(request.as_mut()).is_pending());
        tokio::task::yield_now().await;
    }
    assert_eq!(cache.pending.lock().unwrap().len(), 300);
    for key in &keys {
        backend.release(key);
    }
    for request in requests {
        drop(request.await.unwrap());
    }

    // Do not join workers or issue another miss: actual C3 prepared-read
    // admission occurs before PackCache lookup and needs this 4 MiB arena plus
    // 512 KiB headroom. Completed HEADs must already have released their Plans.
    let prepared = cache
        .budget
        .admit_with_headroom(SnapshotBudgetPool::Plans, 4 << 20, 512 << 10)
        .expect("completed HEAD bookkeeping blocked the next prepared read");
    let key = {
        let registry = cache.registry.lock().await;
        keys.iter()
            .find(|key| {
                registry
                    .entries
                    .contains_key(&hex::encode(Sha256::digest(key.as_bytes())))
            })
            .unwrap()
            .clone()
    };
    let heads = backend.heads.lock().unwrap().values().sum::<usize>();
    assert_eq!(
        cache.read(&client, &key, 0, 8).await.unwrap(),
        body(&key)[..8]
    );
    assert_eq!(backend.heads.lock().unwrap().values().sum::<usize>(), heads);
    drop(prepared);
    clean(&cache).await;
}

#[tokio::test]
async fn full_control_budget_still_allows_disk_turnover_after_head() {
    // Include one byte of unused disk: admission must discover the next size,
    // not guess whether eviction is needed from disk==capacity alone.
    for capacity in [SIZE, SIZE + 1] {
        let backend = GatedBackend::default();
        let mut limits = SnapshotBudgetLimits::default();
        limits.bytes[SnapshotBudgetPool::Control as usize] = 4096 + 1;
        let cache = PackCache::new(
            capacity,
            SnapshotMountBudget::new(limits).unwrap(),
            Arc::new(ReadonlyTransport::default()),
            Arc::new(BackendIoCounters::default()),
        );
        let client = ObjectClient::new(backend.clone());
        for key in ["a", "b", "a"] {
            assert_eq!(
                cache.read(&client, key, 0, 8).await.unwrap(),
                body(key)[..8]
            );
            join_workers(&cache).await;
        }
        assert_eq!(backend.gets.load(Ordering::Relaxed), 3);
        assert_eq!(backend.ranges.load(Ordering::Relaxed), 0);
        assert_eq!(cache.evictions.load(Ordering::Relaxed), 2);
        clean(&cache).await;
    }
}

#[tokio::test]
async fn small_plans_budget_selects_range_before_admitting_pending_state() {
    let backend = GatedBackend::default();
    let key = "small";
    let mut limits = SnapshotBudgetLimits::default();
    limits.bytes[SnapshotBudgetPool::Plans as usize] = 4096 + 2 * key.len() as u64;
    let cache = PackCache::new(
        SIZE,
        SnapshotMountBudget::new(limits).unwrap(),
        Arc::new(ReadonlyTransport::default()),
        Arc::new(BackendIoCounters::default()),
    );
    let client = ObjectClient::new(backend.clone());
    assert_eq!(
        cache.read(&client, key, 0, 8).await.unwrap(),
        body(key)[..8]
    );
    assert_eq!(backend.heads(key), 0);
    assert_eq!(backend.ranges.load(Ordering::Relaxed), 1);
    clean(&cache).await;
}

#[tokio::test]
async fn small_plans_fanout_preserves_driver_progress_and_range_fallback() {
    let backend = GatedBackend::blocked(&["first"]);
    let mut limits = SnapshotBudgetLimits::default();
    limits.bytes[SnapshotBudgetPool::Plans as usize] = DRIVER_HEADROOM + 4096 + 5;
    let cache = PackCache::new(
        16 * SIZE,
        SnapshotMountBudget::new(limits).unwrap(),
        Arc::new(ReadonlyTransport::default()),
        Arc::new(BackendIoCounters::default()),
    );
    let client = ObjectClient::new(backend.clone());
    let mut first = Box::pin(cache.read(&client, "first", 0, 8));
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    backend.entered("first").await;
    for index in 0..132 {
        let key = format!("other-{index:03}");
        assert_eq!(
            cache.read(&client, &key, 0, 8).await.unwrap(),
            body(&key)[..8]
        );
    }
    assert_eq!(backend.heads.lock().unwrap().values().sum::<usize>(), 1);
    assert_eq!(backend.ranges.load(Ordering::Relaxed), 132);
    backend.release("first");
    assert_eq!(first.await.unwrap(), body("first")[..8]);
    clean(&cache).await;
}

#[tokio::test]
async fn head_errors_preserve_budget_and_boundary_categories() {
    use crate::workspace::packed::snapshot::observer_backend_error;
    let cases = [
        (
            "limit-error",
            PackedWireError::LimitExceeded("injected".into()),
        ),
        (
            "admission-error",
            PackedWireError::LimitExceeded("typed range allocation/end boundary".into()),
        ),
        (
            "short-error",
            PackedWireError::Truncated {
                what: "packed streamed range",
                need: 64,
                have: 12,
            },
        ),
        (
            "excess-error",
            PackedWireError::Invalid("typed range returned more bytes than requested".into()),
        ),
    ];
    for (key, expected) in cases {
        let backend = GatedBackend::blocked(&[key]);
        let (cache, client) = setup(&backend, 1 << 20);
        let mut first = Box::pin(cache.prepare(&client, key, 0, 8, None));
        let mut follower = Box::pin(cache.prepare(&client, key, 0, 8, None));
        assert!(futures_util::poll!(first.as_mut()).is_pending());
        backend.entered(key).await;
        assert!(futures_util::poll!(follower.as_mut()).is_pending());
        backend.release(key);
        for result in [first.await, follower.await] {
            assert_eq!(
                observer_backend_error(result.err().expect("typed HEAD error")),
                expected
            );
        }
        clean(&cache).await;
        assert_eq!(backend.heads(key), 2);
    }
}

#[test]
fn headroom_admission_checks_exact_capacity_overflow_and_close() {
    for capacity in [10, 11] {
        let mut limits = SnapshotBudgetLimits::default();
        limits.bytes[SnapshotBudgetPool::Plans as usize] = capacity;
        let budget = SnapshotMountBudget::new(limits).unwrap();
        let result = budget.admit_with_headroom(SnapshotBudgetPool::Plans, 1, 10);
        assert_eq!(result.is_ok(), capacity == 11);
        assert_eq!(
            budget.state().used[SnapshotBudgetPool::Plans as usize],
            u64::from(capacity == 11)
        );
        drop(result);
        assert!(
            budget
                .admit_with_headroom(SnapshotBudgetPool::Plans, 1, u64::MAX)
                .is_err()
        );
        budget.close();
        assert!(
            budget
                .admit_with_headroom(SnapshotBudgetPool::Plans, 1, 0)
                .is_err()
        );
        assert!(budget.state().used.iter().all(|used| *used == 0));
    }
}

#[test]
fn concurrent_headroom_admission_does_not_overbook_the_same_free_bytes() {
    let mut limits = SnapshotBudgetLimits::default();
    limits.bytes[SnapshotBudgetPool::Plans as usize] = 11;
    let budget = SnapshotMountBudget::new(limits).unwrap();
    let start = Arc::new(std::sync::Barrier::new(17));
    let finish = Arc::new(std::sync::Barrier::new(17));
    let admitted = Arc::new(AtomicU64::new(0));
    std::thread::scope(|scope| {
        for _ in 0..16 {
            let (budget, start, finish, admitted) = (
                budget.clone(),
                start.clone(),
                finish.clone(),
                admitted.clone(),
            );
            scope.spawn(move || {
                start.wait();
                let owner = budget.admit_with_headroom(SnapshotBudgetPool::Plans, 1, 10);
                if owner.is_ok() {
                    admitted.fetch_add(1, Ordering::Relaxed);
                }
                finish.wait();
                drop(owner);
            });
        }
        start.wait();
        finish.wait();
    });
    assert_eq!(admitted.load(Ordering::Relaxed), 1);
    assert_eq!(budget.state().peak[SnapshotBudgetPool::Plans as usize], 1);
    assert!(budget.state().used.iter().all(|used| *used == 0));
}
