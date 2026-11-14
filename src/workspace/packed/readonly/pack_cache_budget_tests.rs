//! Capacity/owner regressions for the mount-local whole-object cache.
//! These tests use actual temporary files and backend bodies, not a mocked registry.
use super::*;
use crate::cadapter::client::ObjectByteStream;
use crate::workspace::packed::snapshot::SnapshotBudgetLimits;
use bytes::Bytes;

const OBJECT_BYTES: u64 = 64;
const ENTRY_CONTROL_BYTES: u64 = 4096 + 64;

#[derive(Clone, Default)]
struct ImmediateBackend {
    heads: Arc<AtomicU64>,
    gets: Arc<AtomicU64>,
    ranges: Arc<AtomicU64>,
    large_size: usize,
}
impl ImmediateBackend {
    fn body(&self, key: &str) -> Vec<u8> {
        let mut bytes = object_bytes(key);
        if self.large_size > 0 && key == object_key(u64::MAX) {
            bytes.resize(self.large_size, bytes[0]);
        }
        bytes
    }
}

fn object_key(index: u64) -> String {
    format!("{index:064x}")
}

fn object_bytes(key: &str) -> Vec<u8> {
    let index = u64::from_str_radix(&key[48..], 16).unwrap();
    vec![(index % 251) as u8; OBJECT_BYTES as usize]
}

#[async_trait::async_trait]
impl ObjectBackend for ImmediateBackend {
    async fn put_object(&self, _: &str, _: &[u8]) -> Result<()> {
        unreachable!()
    }
    async fn get_object(&self, _: &str) -> Result<Option<Vec<u8>>> {
        unreachable!()
    }
    async fn get_object_size(&self, key: &str) -> Result<Option<u64>> {
        self.heads.fetch_add(1, Ordering::Relaxed);
        Ok(Some(self.body(key).len() as u64))
    }
    async fn get_etag(&self, _: &str) -> Result<String> {
        Ok("immutable".into())
    }
    async fn delete_object(&self, _: &str) -> Result<()> {
        unreachable!()
    }
    async fn get_object_range(&self, key: &str, offset: u64, bytes: &mut [u8]) -> Result<usize> {
        self.ranges.fetch_add(1, Ordering::Relaxed);
        let length = bytes.len();
        bytes.copy_from_slice(&self.body(key)[offset as usize..offset as usize + length]);
        Ok(length)
    }
    async fn get_object_stream(&self, key: &str) -> Result<Option<ObjectByteStream>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        Ok(Some(Box::pin(futures_util::stream::iter([Ok(
            Bytes::from(self.body(key)),
        )]))))
    }
}

fn budget(control_bytes: u64) -> Arc<SnapshotMountBudget> {
    let mut limits = SnapshotBudgetLimits::default();
    limits.bytes[SnapshotBudgetPool::Control as usize] = control_bytes;
    SnapshotMountBudget::new(limits).unwrap()
}

fn persistent(
    directory: &std::path::Path,
    capacity: u64,
    budget: Arc<SnapshotMountBudget>,
) -> Arc<PackCache> {
    PackCache::persistent(
        capacity,
        budget,
        Arc::new(ReadonlyTransport::default()),
        Arc::new(BackendIoCounters::default()),
        directory.to_owned(),
    )
    .unwrap()
}

async fn fill(cache: &Arc<PackCache>, client: &ObjectClient<ImmediateBackend>, key: &str) {
    assert_eq!(
        cache
            .read(client, key, 0, OBJECT_BYTES as usize)
            .await
            .unwrap(),
        object_bytes(key)
    );
    // Reading the available prefix need not wait for the final hard link.
    // Join real workers so assertions cover completed fills, without sleeps.
    let workers = std::mem::take(&mut *cache.workers.lock().unwrap());
    for worker in workers {
        worker.await.unwrap();
    }
}

async fn cached_file(cache: &PackCache, key: &str) -> PathBuf {
    cache.registry.lock().await.entries[&hex::encode(Sha256::digest(key.as_bytes()))]
        .file
        .clone()
}

fn assert_occupancy(cache: &PackCache, entries: u64, reserved: u64) {
    assert_eq!(
        cache.occupancy.live_entries.load(Ordering::Relaxed),
        entries
    );
    assert_eq!(
        cache.occupancy.live_reserved_bytes.load(Ordering::Relaxed),
        reserved
    );
}

#[cfg(target_os = "linux")]
fn assert_only_directory_lock_is_open(directory: &std::path::Path) {
    // Other concurrently running tests own unrelated descriptors. Filter by
    // this unique directory instead of using a process-wide descriptor count.
    let directory = directory.canonicalize().unwrap();
    let files: Vec<_> = std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|row| std::fs::read_link(row.ok()?.path()).ok())
        .filter(|target| target.starts_with(&directory))
        .collect();
    assert_eq!(files, vec![directory.join(".lock")]);
}

#[tokio::test]
async fn budgeted_cache_reuses_more_than_1024_objects_and_restores_all() {
    let count = 1100;
    let directory = tempfile::tempdir().unwrap();
    let budget = budget(8 << 20);
    let backend = ImmediateBackend::default();
    let client = ObjectClient::new(backend.clone());
    let cache = persistent(directory.path(), count * OBJECT_BYTES, budget.clone());
    for index in 0..count {
        fill(&cache, &client, &object_key(index)).await;
    }
    assert_eq!(cache.registry.lock().await.entries.len(), count as usize);
    assert_occupancy(&cache, count, count * OBJECT_BYTES);
    assert_eq!(cache.evictions.load(Ordering::Relaxed), 0);
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Control as usize],
        count * ENTRY_CONTROL_BYTES
    );
    for index in 0..count {
        fill(&cache, &client, &object_key(index)).await;
    }
    assert_eq!(backend.gets.load(Ordering::Relaxed), count);
    assert_eq!(backend.heads.load(Ordering::Relaxed), count);
    #[cfg(target_os = "linux")]
    assert_only_directory_lock_is_open(directory.path());
    cache.drain().await.unwrap();
    assert_occupancy(&cache, 0, 0);
    assert!(budget.state().used.iter().all(|used| *used == 0));
    drop(cache);

    let reopened = persistent(directory.path(), count * OBJECT_BYTES, budget.clone());
    assert_occupancy(&reopened, count, count * OBJECT_BYTES);
    for index in 0..count {
        fill(&reopened, &client, &object_key(index)).await;
    }
    assert_eq!(backend.gets.load(Ordering::Relaxed), count);
    assert_eq!(backend.heads.load(Ordering::Relaxed), count);
    assert_eq!(backend.ranges.load(Ordering::Relaxed), 0);
    reopened.drain().await.unwrap();
    assert!(budget.state().used.iter().all(|used| *used == 0));
}

#[tokio::test]
async fn completed_cache_closes_idle_fds_and_last_reader_owns_temporary_path() {
    let directory = tempfile::tempdir().unwrap();
    let budget = budget(8 << 20);
    let backend = ImmediateBackend::default();
    let client = ObjectClient::new(backend.clone());
    let cache = persistent(directory.path(), 32 * OBJECT_BYTES, budget.clone());
    for index in 0..32 {
        fill(&cache, &client, &object_key(index)).await;
    }
    #[cfg(target_os = "linux")]
    assert_only_directory_lock_is_open(directory.path());
    let key = object_key(0);
    let temporary = cached_file(&cache, &key).await;
    let saved = directory.path().join(format!(
        "{}.pack",
        hex::encode(Sha256::digest(key.as_bytes()))
    ));
    assert!(temporary.exists() && saved.exists());
    let mut reader = cache
        .stream(client.clone(), key.clone(), 0, OBJECT_BYTES, None)
        .await
        .unwrap();
    cache.drain().await.unwrap();
    assert!(
        temporary.exists(),
        "drain cannot delete a live reader's file"
    );
    assert_occupancy(&cache, 1, OBJECT_BYTES);
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Control as usize],
        ENTRY_CONTROL_BYTES
    );
    assert_eq!(
        reader.next().await.unwrap().unwrap().as_ref(),
        object_bytes(&key)
    );
    assert!(reader.next().await.is_none());
    drop(reader);
    assert!(!temporary.exists());
    assert!(
        saved.exists(),
        "the persistent hard link survives temporary cleanup"
    );
    assert_occupancy(&cache, 0, 0);
    assert!(budget.state().used.iter().all(|used| *used == 0));
    drop(cache);
    let reopened = persistent(directory.path(), 32 * OBJECT_BYTES, budget);
    fill(&reopened, &client, &key).await;
    assert_eq!(backend.gets.load(Ordering::Relaxed), 32);
    reopened.drain().await.unwrap();
}

#[tokio::test]
async fn disk_capacity_evicts_only_completed_unpinned_entries() {
    let directory = tempfile::tempdir().unwrap();
    let budget = budget(8 << 20);
    let backend = ImmediateBackend::default();
    let client = ObjectClient::new(backend.clone());
    let cache = persistent(directory.path(), 2 * OBJECT_BYTES, budget.clone());
    let keys: Vec<_> = (0..4).map(object_key).collect();
    for key in &keys[..2] {
        fill(&cache, &client, key).await;
    }
    let first_path = cached_file(&cache, &keys[0]).await;
    let second_path = cached_file(&cache, &keys[1]).await;
    let first = cache
        .stream(client.clone(), keys[0].clone(), 0, OBJECT_BYTES, None)
        .await
        .unwrap();
    fill(&cache, &client, &keys[2]).await;
    assert!(first_path.exists());
    assert!(!second_path.exists());
    assert_eq!(cache.evictions.load(Ordering::Relaxed), 1);
    let third = cache
        .stream(client.clone(), keys[2].clone(), 0, OBJECT_BYTES, None)
        .await
        .unwrap();
    fill(&cache, &client, &keys[3]).await;
    assert_eq!(backend.gets.load(Ordering::Relaxed), 3);
    assert_eq!(backend.ranges.load(Ordering::Relaxed), 1);
    assert_occupancy(&cache, 2, 2 * OBJECT_BYTES);
    drop(first);
    fill(&cache, &client, &keys[3]).await;
    assert!(!first_path.exists());
    assert_eq!(cache.evictions.load(Ordering::Relaxed), 2);
    assert_eq!(cache.registry.lock().await.reserved, 2 * OBJECT_BYTES);
    drop(third);
    cache.drain().await.unwrap();
    assert_occupancy(&cache, 0, 0);
    assert!(budget.state().used.iter().all(|used| *used == 0));
}

#[tokio::test]
async fn replacing_small_objects_retires_sparse_registry_buckets_with_entry_owners() {
    let count = 256;
    let directory = tempfile::tempdir().unwrap();
    let budget = budget(8 << 20);
    let backend = ImmediateBackend {
        large_size: ((count - 1) * OBJECT_BYTES) as usize,
        ..Default::default()
    };
    let client = ObjectClient::new(backend.clone());
    let cache = persistent(directory.path(), count * OBJECT_BYTES, budget.clone());
    for index in 0..count {
        fill(&cache, &client, &object_key(index)).await;
    }
    let high_capacity = cache.registry.lock().await.entries.capacity();
    assert!(high_capacity >= count as usize);
    let key = object_key(u64::MAX);
    let entry = cache
        .prepare(&client, &key, 0, 1, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cache
            .read_entry(&entry, 0, backend.large_size)
            .await
            .unwrap(),
        backend.body(&key)
    );
    let workers = std::mem::take(&mut *cache.workers.lock().unwrap());
    for worker in workers {
        worker.await.unwrap();
    }
    drop(entry);
    let registry = cache.registry.lock().await;
    assert_eq!(registry.entries.len(), 2);
    assert!(registry.entries.capacity() <= 4 * registry.entries.len());
    assert!(registry.entries.capacity() < high_capacity);
    assert_eq!(registry.reserved, count * OBJECT_BYTES);
    drop(registry);
    assert_occupancy(&cache, 2, count * OBJECT_BYTES);
    assert_eq!(cache.evictions.load(Ordering::Relaxed), count - 1);
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Control as usize],
        2 * ENTRY_CONTROL_BYTES
    );
    assert_eq!(budget.state().used[SnapshotBudgetPool::Stored as usize], 0);
    cache.drain().await.unwrap();
    assert_occupancy(&cache, 0, 0);
    assert_eq!(cache.registry.lock().await.entries.capacity(), 0);
    assert!(budget.state().used.iter().all(|used| *used == 0));
}

#[tokio::test]
async fn control_and_stored_exhaustion_keep_bounded_range_fallback() {
    for limited_pool in [SnapshotBudgetPool::Control, SnapshotBudgetPool::Stored] {
        let directory = tempfile::tempdir().unwrap();
        let mut limits = SnapshotBudgetLimits::default();
        limits.bytes[SnapshotBudgetPool::Control as usize] = 2 * ENTRY_CONTROL_BYTES;
        if limited_pool == SnapshotBudgetPool::Stored {
            limits.bytes[SnapshotBudgetPool::Stored as usize] = 512 << 10;
        }
        let budget = SnapshotMountBudget::new(limits).unwrap();
        let backend = ImmediateBackend::default();
        let client = ObjectClient::new(backend.clone());
        let cache = persistent(directory.path(), 100 * OBJECT_BYTES, budget.clone());
        for index in 0..3 {
            fill(&cache, &client, &object_key(index)).await;
        }
        let expected = if limited_pool == SnapshotBudgetPool::Control {
            2
        } else {
            0
        };
        assert_occupancy(&cache, expected, expected * OBJECT_BYTES);
        assert_eq!(backend.gets.load(Ordering::Relaxed), expected);
        assert_eq!(backend.ranges.load(Ordering::Relaxed), 3 - expected);
        assert_eq!(
            budget.state().used[SnapshotBudgetPool::Control as usize],
            expected * ENTRY_CONTROL_BYTES
        );
        assert!(budget.state().rejections > 0);
        assert_eq!(cache.evictions.load(Ordering::Relaxed), 0);
        cache.drain().await.unwrap();
        assert!(budget.state().used.iter().all(|used| *used == 0));
    }
}

#[tokio::test]
async fn persistent_scan_remains_bounded_by_disk_and_control() {
    for control_limited in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        for index in 0..4 {
            let key = object_key(index);
            let path = directory.path().join(format!(
                "{}.pack",
                hex::encode(Sha256::digest(key.as_bytes()))
            ));
            std::fs::write(path, object_bytes(&key)).unwrap();
        }
        let budget = budget(if control_limited {
            2 * ENTRY_CONTROL_BYTES
        } else {
            8 << 20
        });
        let capacity = if control_limited {
            4 * OBJECT_BYTES
        } else {
            2 * OBJECT_BYTES
        };
        let cache = persistent(directory.path(), capacity, budget.clone());
        assert_eq!(cache.registry.lock().await.entries.len(), 2);
        assert_eq!(cache.evictions.load(Ordering::Relaxed), 2);
        assert_occupancy(&cache, 2, 2 * OBJECT_BYTES);
        assert_eq!(
            budget.state().used[SnapshotBudgetPool::Control as usize],
            2 * ENTRY_CONTROL_BYTES
        );
        let packs = std::fs::read_dir(directory.path())
            .unwrap()
            .filter(|row| {
                row.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "pack")
            })
            .count();
        assert_eq!(packs, 2);
        cache.drain().await.unwrap();
        assert_occupancy(&cache, 0, 0);
        assert!(budget.state().used.iter().all(|used| *used == 0));
    }
}
