//! Mount-local immutable-object cache. Downloads publish only written prefixes;
//! callers still authenticate every metadata block/frame above this boundary.
use super::super::snapshot::{SnapshotBudgetPool, SnapshotMountBudget, SnapshotOwnedPermit};
use super::backend_io::BackendIoCounters;
use super::transport::ReadonlyTransport;
use crate::cadapter::client::ObjectBackend;
use crate::cadapter::read_observer::{ReadContext, ReadObserver};
type Observation = Option<(ReadContext, Arc<ReadObserver>)>;
use crate::cadapter::client::ObjectClient;
use anyhow::{Result, ensure};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Notify;

#[derive(Default)]
struct Progress {
    available: u64,
    done: bool,
    error: Option<String>,
}
struct Entry {
    file: PathBuf,
    _temporary: Option<tempfile::NamedTempFile>,
    persisted: Option<PathBuf>,
    progress: Mutex<Progress>,
    changed: Notify,
    size: u64,
    _owner: SnapshotOwnedPermit,
}
#[derive(Default)]
struct Registry {
    entries: HashMap<String, Arc<Entry>>,
    reserved: u64,
    closed: bool,
}
pub(super) struct PackCache {
    registry: tokio::sync::Mutex<Registry>,
    capacity: u64,
    directory: Option<PathBuf>,
    _lock: Option<std::fs::File>,
    pub hits: AtomicU64,
    pub ready_hits: AtomicU64,
    pub inflight_hits: AtomicU64,
    pub local_bytes: AtomicU64,
    pub misses: AtomicU64,
    pub downloaded: AtomicU64,
    budget: Arc<SnapshotMountBudget>,
    transport: Arc<ReadonlyTransport>,
    io: Arc<BackendIoCounters>,
    slots: Arc<tokio::sync::Semaphore>,
    workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
impl PackCache {
    pub fn new(
        capacity: u64,
        budget: Arc<SnapshotMountBudget>,
        transport: Arc<ReadonlyTransport>,
        io: Arc<BackendIoCounters>,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry: Default::default(),
            capacity,
            directory: None,
            _lock: None,
            budget,
            transport,
            io,
            slots: Arc::new(tokio::sync::Semaphore::new(4)),
            hits: AtomicU64::new(0),
            ready_hits: AtomicU64::new(0),
            inflight_hits: AtomicU64::new(0),
            local_bytes: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            downloaded: AtomicU64::new(0),
            workers: Default::default(),
        })
    }
    pub fn persistent(
        capacity: u64,
        budget: Arc<SnapshotMountBudget>,
        transport: Arc<ReadonlyTransport>,
        io: Arc<BackendIoCounters>,
        directory: PathBuf,
    ) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&directory)?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join(".lock"))?;
        if lock.try_lock().is_err() {
            return Ok(Self::new(capacity, budget, transport, io));
        }
        let mut cache = Self::new(capacity, budget, transport, io);
        let inner = Arc::get_mut(&mut cache).unwrap();
        let registry = inner.registry.get_mut();
        for row in std::fs::read_dir(&directory)? {
            let row = row?;
            let name = row.file_name().to_string_lossy().into_owned();
            if name.starts_with(".partial-") {
                if row.file_type()?.is_file() {
                    std::fs::remove_file(row.path())?;
                }
                continue;
            }
            let Some(key) = name.strip_suffix(".pack") else {
                continue;
            };
            if key.len() != 64
                || !key.bytes().all(|c| c.is_ascii_hexdigit())
                || !row.file_type()?.is_file()
            {
                continue;
            }
            let size = row.metadata()?.len();
            let owner = inner
                .budget
                .admit(&[(SnapshotBudgetPool::Control, 4096 + 64)]);
            if registry.reserved.saturating_add(size) > capacity
                || registry.entries.len() >= 1024
                || owner.is_err()
            {
                std::fs::remove_file(row.path())?;
                continue;
            }
            registry.reserved += size;
            registry.entries.insert(
                key.to_owned(),
                Arc::new(Entry {
                    file: row.path(),
                    _temporary: None,
                    persisted: Some(row.path()),
                    size,
                    _owner: owner.unwrap(),
                    changed: Notify::new(),
                    progress: Mutex::new(Progress {
                        available: size,
                        done: true,
                        error: None,
                    }),
                }),
            );
        }
        inner.directory = Some(directory);
        inner._lock = Some(lock);
        Ok(cache)
    }
    async fn prepare<B: ObjectBackend + Clone + 'static>(
        self: &Arc<Self>,
        client: &ObjectClient<B>,
        key: &str,
        offset: u64,
        length: usize,
        observation: Observation,
    ) -> Result<Option<Arc<Entry>>> {
        ensure!(!self.budget.state().closed, "pack cache budget closed");
        let cache_key = hex::encode(Sha256::digest(key.as_bytes()));
        let entry = {
            let mut registry = self.registry.lock().await;
            ensure!(!registry.closed, "pack cache closed");
            if let Some(entry) = registry.entries.get(&cache_key)
                && entry.progress.lock().unwrap().error.is_some()
            {
                // A body error is terminal for its existing readers, not for
                // the immutable object. Keep the partial file charged while
                // any old reader or worker still owns it; new requests can
                // use the ordinary range path until that ownership ends.
                if Arc::strong_count(entry) > 1 {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    drop(registry);
                    return Ok(None);
                }
                let failed = registry.entries.remove(&cache_key).unwrap();
                let size = failed.size;
                drop(failed);
                registry.reserved -= size;
            }
            if let Some(entry) = registry.entries.get(&cache_key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                if offset
                    .checked_add(length as u64)
                    .is_some_and(|end| entry.progress.lock().unwrap().available >= end)
                {
                    self.ready_hits.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.inflight_hits.fetch_add(1, Ordering::Relaxed);
                }
                entry.clone()
            } else {
                self.misses.fetch_add(1, Ordering::Relaxed);
                let head_client = match &observation {
                    Some((context, observer)) => client.clone().with_read_observer(
                        observer.clone(),
                        context.engine,
                        context.phase,
                        context.origin,
                    ),
                    None => client.clone(),
                };
                let head_context = observation.as_ref().map(|(context, _)| *context);
                let head_key = key.to_owned();
                let io = self.io.clone();
                let head = self
                    .transport
                    .run(&self.budget, key.len(), 0, || async move {
                        io.head_started();
                        let result = match head_context {
                            Some(context) => {
                                head_client
                                    .typed_object_size(context.class, &head_key)
                                    .await
                            }
                            None => head_client.get_object_size(&head_key).await,
                        }
                        .and_then(|size| size.ok_or_else(|| anyhow::anyhow!("missing pack")));
                        io.finish_open(result)
                    })
                    .await?;
                let size = head.value;
                if size > self.capacity {
                    drop(registry);
                    return Ok(None);
                }
                while registry.reserved + size > self.capacity || registry.entries.len() >= 1024 {
                    let victim = registry
                        .entries
                        .iter()
                        .find(|(_, entry)| {
                            Arc::strong_count(entry) == 1 && entry.progress.lock().unwrap().done
                        })
                        .map(|(key, _)| key.clone());
                    let Some(victim) = victim else {
                        drop(registry);
                        return Ok(None);
                    };
                    let removed = registry.entries.remove(&victim).unwrap();
                    registry.reserved -= removed.size;
                    if let Some(path) = &removed.persisted
                        && path.exists()
                    {
                        std::fs::remove_file(path)?;
                    }
                }
                let Ok(slot) = self.slots.clone().try_acquire_owned() else {
                    drop(registry);
                    return Ok(None);
                };
                let Ok(buffer) = self.budget.admit(&[(SnapshotBudgetPool::Stored, 1 << 20)]) else {
                    drop(registry);
                    return Ok(None);
                };
                let Ok(owner) = self
                    .budget
                    .admit(&[(SnapshotBudgetPool::Control, 4096 + key.len() as u64)])
                else {
                    drop(registry);
                    return Ok(None);
                };
                let temporary = match &self.directory {
                    Some(directory) => tempfile::Builder::new()
                        .prefix(".partial-")
                        .tempfile_in(directory)?,
                    None => tempfile::NamedTempFile::new()?,
                };
                let entry = Arc::new(Entry {
                    file: temporary.path().to_owned(),
                    _temporary: Some(temporary),
                    persisted: self
                        .directory
                        .as_ref()
                        .map(|directory| directory.join(format!("{cache_key}.pack"))),
                    _owner: owner,
                    progress: Default::default(),
                    changed: Notify::new(),
                    size,
                });
                registry.reserved += size;
                registry.entries.insert(cache_key, entry.clone());
                let cache = self.clone();
                let task_entry = entry.clone();
                let (client, context) = match observation {
                    Some((context, observer)) => (
                        client.clone().with_read_observer(
                            observer,
                            context.engine,
                            context.phase,
                            context.origin,
                        ),
                        Some(context),
                    ),
                    None => (client.clone(), None),
                };
                let key = key.to_owned();
                let worker = tokio::spawn(async move {
                    let result: Result<()> = async {
                        let _slot = slot;
                        let _buffer = buffer;
                        let key_len = key.len();
                        let io = cache.io.clone();
                        let mut stream = cache
                            .transport
                            .open_body(&cache.budget, key_len, || async move {
                                io.full_started(size);
                                let result = client
                                    .backend_object_stream(&key, Some(size), context)
                                    .await
                                    .and_then(|stream| {
                                        stream.ok_or_else(|| anyhow::anyhow!("missing pack body"))
                                    });
                                io.observe_stream(result)
                            })
                            .await?;
                        let mut file = tokio::fs::OpenOptions::new()
                            .write(true)
                            .open(&task_entry.file)
                            .await?;
                        let mut written = 0u64;
                        while let Some(chunk) = stream.next().await {
                            let chunk = chunk?;
                            if written + chunk.len() as u64 > size {
                                cache.io.failures.fetch_add(1, Ordering::Relaxed);
                                anyhow::bail!("pack body overflow");
                            }
                            file.write_all(&chunk).await?;
                            file.flush().await?;
                            written += chunk.len() as u64;
                            cache
                                .downloaded
                                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                            task_entry.progress.lock().unwrap().available = written;
                            task_entry.changed.notify_waiters();
                        }
                        if written != size {
                            cache.io.failures.fetch_add(1, Ordering::Relaxed);
                            anyhow::bail!("short pack body");
                        }
                        if let Some(path) = &task_entry.persisted {
                            file.sync_data().await?;
                            tokio::fs::hard_link(&task_entry.file, path).await?;
                        }
                        Ok(())
                    }
                    .await;
                    let mut progress = task_entry.progress.lock().unwrap();
                    progress.error = result.err().map(|error| error.to_string());
                    progress.done = true;
                    drop(progress);
                    task_entry.changed.notify_waiters();
                });
                let mut workers = self.workers.lock().unwrap();
                workers.retain(|worker| !worker.is_finished());
                workers.push(worker);
                entry
            }
        };
        Ok(Some(entry))
    }
    pub async fn read<B: ObjectBackend + Clone + 'static>(
        self: &Arc<Self>,
        client: &ObjectClient<B>,
        key: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>> {
        match self.prepare(client, key, offset, length, None).await? {
            Some(entry) => self.read_entry(&entry, offset, length).await,
            None => self.range_fallback(client, key, offset, length).await,
        }
    }

    pub async fn stream<B: ObjectBackend + Clone + 'static>(
        self: &Arc<Self>,
        client: ObjectClient<B>,
        key: String,
        offset: u64,
        length: u64,
        observation: Observation,
    ) -> Result<crate::cadapter::client::ObjectByteStream> {
        let count = usize::try_from(length)?;
        let Some(entry) = self
            .prepare(&client, &key, offset, count, observation.clone())
            .await?
        else {
            let io = self.io.clone();
            return self
                .transport
                .open_body(&self.budget, key.len(), || async move {
                    io.range_started(length);
                    let result = match observation {
                        Some((context, observer)) => {
                            client
                                .backend_range_stream_observed(
                                    &key, offset, length, context, observer,
                                )
                                .await
                        }
                        None => client.get_object_range_stream(&key, offset, length).await,
                    };
                    io.observe_stream(result)
                })
                .await;
        };
        Ok(Box::pin(futures_util::stream::try_unfold(
            (self.clone(), entry, offset, length),
            |(cache, entry, offset, remaining)| async move {
                if remaining == 0 {
                    return Ok(None);
                }
                let length = remaining.min(64 << 10) as usize;
                let bytes = cache.read_entry(&entry, offset, length).await?;
                Ok(Some((
                    bytes::Bytes::from(bytes),
                    (
                        cache,
                        entry,
                        offset + length as u64,
                        remaining - length as u64,
                    ),
                )))
            },
        )))
    }
    async fn read_entry(&self, entry: &Entry, offset: u64, length: usize) -> Result<Vec<u8>> {
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| anyhow::anyhow!("pack range overflow"))?;
        ensure!(end <= entry.size, "pack range exceeds object");
        loop {
            let changed = entry.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let progress = entry.progress.lock().unwrap();
                if let Some(error) = &progress.error {
                    anyhow::bail!("{error}");
                }
                if progress.available >= end {
                    break;
                }
            }
            changed.await;
        }
        let mut file = tokio::fs::File::open(&entry.file).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut output = vec![0; length];
        file.read_exact(&mut output).await?;
        self.local_bytes.fetch_add(length as u64, Ordering::Relaxed);
        Ok(output)
    }
    async fn range_fallback<B: ObjectBackend + Clone + 'static>(
        &self,
        client: &ObjectClient<B>,
        key: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>> {
        let client = client.clone();
        let owned_key = key.to_owned();
        let io = self.io.clone();
        let output = self
            .transport
            .run(&self.budget, key.len(), length as u64, || async move {
                io.range_started(length as u64);
                let result = async {
                    let mut bytes = vec![0; length];
                    let actual = client
                        .get_object_range(&owned_key, offset, &mut bytes)
                        .await?;
                    ensure!(actual <= bytes.len(), "pack fallback range overflow");
                    io.received_bytes
                        .fetch_add(actual as u64, Ordering::Relaxed);
                    bytes.truncate(actual);
                    Ok(bytes)
                }
                .await;
                io.finish_open(result)
            })
            .await?;
        Ok(output.value.clone())
    }
    pub async fn drain(&self) -> Result<()> {
        self.registry.lock().await.closed = true;
        let workers = std::mem::take(&mut *self.workers.lock().unwrap());
        for worker in workers {
            worker.await?;
        }
        self.registry.lock().await.entries.clear();
        Ok(())
    }
}

impl std::fmt::Debug for PackCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PackCache")
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::client::ObjectByteStream;
    use bytes::Bytes;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    #[derive(Clone)]
    struct Backend {
        gets: Arc<AtomicU64>,
        release: Arc<Notify>,
        fail_tail: Arc<AtomicBool>,
        reported_size: u64,
    }
    #[async_trait::async_trait]
    impl ObjectBackend for Backend {
        async fn put_object(&self, _: &str, _: &[u8]) -> Result<()> {
            unreachable!()
        }
        async fn get_object(&self, _: &str) -> Result<Option<Vec<u8>>> {
            unreachable!()
        }
        async fn get_object_size(&self, _: &str) -> Result<Option<u64>> {
            Ok(Some(self.reported_size))
        }
        async fn get_etag(&self, _: &str) -> Result<String> {
            Ok("immutable".into())
        }
        async fn delete_object(&self, _: &str) -> Result<()> {
            unreachable!()
        }
        async fn get_object_range(&self, _: &str, offset: u64, bytes: &mut [u8]) -> Result<usize> {
            for (i, byte) in bytes.iter_mut().enumerate() {
                *byte = (offset as usize + i) as u8;
            }
            Ok(bytes.len())
        }
        async fn get_object_stream(&self, _: &str) -> Result<Option<ObjectByteStream>> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            let release = self.release.clone();
            let fail = self.fail_tail.load(Ordering::Relaxed);
            Ok(Some(Box::pin(
                futures_util::stream::iter(vec![Ok(Bytes::from((0..64u8).collect::<Vec<_>>()))])
                    .chain(futures_util::stream::once(async move {
                        release.notified().await;
                        ensure!(!fail, "injected tail failure");
                        Ok(Bytes::from((64..128u8).collect::<Vec<_>>()))
                    })),
            )))
        }
    }
    fn setup(fail_tail: bool, capacity: u64) -> (Arc<PackCache>, ObjectClient<Backend>, Backend) {
        let backend = Backend {
            gets: Default::default(),
            release: Default::default(),
            fail_tail: Arc::new(AtomicBool::new(fail_tail)),
            reported_size: 128,
        };
        let cache = PackCache::new(
            capacity,
            SnapshotMountBudget::defaults(),
            Arc::new(ReadonlyTransport::default()),
            Arc::new(BackendIoCounters::default()),
        );
        (cache, ObjectClient::new(backend.clone()), backend)
    }
    #[tokio::test]
    async fn prefix_reads_before_eof_and_followers_share_one_download() {
        let (cache, client, backend) = setup(false, 128);
        let first =
            tokio::time::timeout(Duration::from_secs(2), cache.read(&client, "pack", 0, 64))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(first, (0..64u8).collect::<Vec<_>>());
        assert_eq!(cache.downloaded.load(Ordering::Relaxed), 64);
        assert_eq!(
            cache.read(&client, "pack", 8, 8).await.unwrap(),
            (8..16u8).collect::<Vec<_>>()
        );
        backend.release.notify_one();
        assert_eq!(
            cache.read(&client, "pack", 64, 64).await.unwrap(),
            (64..128u8).collect::<Vec<_>>()
        );
        assert_eq!(backend.gets.load(Ordering::Relaxed), 1);
        cache.drain().await.unwrap();
        assert!(cache.read(&client, "pack", 0, 1).await.is_err());
    }
    #[tokio::test]
    async fn tail_failure_wakes_waiter_without_returning_partial_tail() {
        let (cache, client, backend) = setup(true, 128);
        cache.read(&client, "pack", 0, 1).await.unwrap();
        let mut tail = cache
            .stream(client.clone(), "pack".into(), 64, 64, None)
            .await
            .unwrap();
        backend.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(2), tail.next())
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_err());
        cache.drain().await.unwrap();
    }

    #[tokio::test]
    async fn failed_download_retries_without_releasing_a_pinned_disk_reservation() {
        let (cache, client, backend) = setup(true, 128);
        assert_eq!(cache.read(&client, "pack", 0, 1).await.unwrap(), vec![0]);
        // Keep an old response alive even after another waiter observes the
        // error. Its partial file must remain charged to the full disk budget.
        let mut pinned = cache
            .stream(client.clone(), "pack".into(), 64, 64, None)
            .await
            .unwrap();
        let mut waiter = cache
            .stream(client.clone(), "pack".into(), 64, 64, None)
            .await
            .unwrap();
        backend.release.notify_one();
        let error = tokio::time::timeout(Duration::from_secs(2), waiter.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("injected tail failure"));
        drop(waiter);
        let worker = cache.workers.lock().unwrap().pop().unwrap();
        worker.await.unwrap();
        let failed_file = {
            let registry = cache.registry.lock().await;
            assert_eq!(registry.reserved, 128);
            registry.entries.values().next().unwrap().file.clone()
        };

        // The transport remains usable after an ordinary body failure. A new
        // request can recover through the range path while the old entry pins
        // all disk space, without reusing its error or starting another fill.
        backend.fail_tail.store(false, Ordering::Relaxed);
        let recovered =
            tokio::time::timeout(Duration::from_secs(2), cache.read(&client, "pack", 64, 64))
                .await
                .unwrap()
                .expect("a failed fill must not permanently poison later requests");
        assert_eq!(recovered, (64..128u8).collect::<Vec<_>>());
        assert_eq!(backend.gets.load(Ordering::Relaxed), 1);
        assert_eq!(cache.registry.lock().await.reserved, 128);
        assert!(failed_file.exists(), "a pinned file remains charged");
        assert_eq!(cache.io.full_gets.load(Ordering::Relaxed), 1);
        assert_eq!(cache.io.range_gets.load(Ordering::Relaxed), 1);
        assert_eq!(cache.io.received_bytes.load(Ordering::Relaxed), 128);
        assert_eq!(cache.io.failures.load(Ordering::Relaxed), 1);
        assert!(
            pinned
                .next()
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("injected tail failure")
        );
        drop(pinned);
        assert_eq!(cache.io.failures.load(Ordering::Relaxed), 1);

        // Once every old reader has released the failed entry, reclaim its
        // file and reservation before admitting one replacement download.
        assert_eq!(cache.read(&client, "pack", 0, 1).await.unwrap(), vec![0]);
        assert!(!failed_file.exists());
        assert_eq!(backend.gets.load(Ordering::Relaxed), 2);
        assert_eq!(cache.registry.lock().await.reserved, 128);
        backend.release.notify_one();
        assert_eq!(
            cache.read(&client, "pack", 64, 64).await.unwrap(),
            (64..128u8).collect::<Vec<_>>()
        );
        cache.drain().await.unwrap();
    }
    #[tokio::test]
    async fn object_larger_than_budget_uses_range_without_starting_download() {
        let (cache, client, backend) = setup(false, 64);
        assert_eq!(
            cache.read(&client, "pack", 8, 8).await.unwrap(),
            (8..16u8).collect::<Vec<_>>()
        );
        assert_eq!(backend.gets.load(Ordering::Relaxed), 0);
        assert_eq!(cache.io.heads.load(Ordering::Relaxed), 1);
        assert_eq!(cache.io.full_gets.load(Ordering::Relaxed), 0);
        assert_eq!(cache.io.range_gets.load(Ordering::Relaxed), 1);
        assert_eq!(cache.io.received_bytes.load(Ordering::Relaxed), 8);
        cache.drain().await.unwrap();
    }

    #[tokio::test]
    async fn malformed_full_bodies_count_received_bytes_before_length_rejection() {
        for reported_size in [64, 256] {
            let (cache, _, backend) = setup(false, 256);
            let backend = Backend {
                reported_size,
                ..backend
            };
            let client = ObjectClient::new(backend.clone());
            let entry = cache
                .prepare(&client, "pack", 0, 1, None)
                .await
                .unwrap()
                .unwrap();
            backend.release.notify_one();
            let worker = cache.workers.lock().unwrap().pop().unwrap();
            worker.await.unwrap();
            assert!(cache.read_entry(&entry, 0, 1).await.is_err());
            assert_eq!(cache.io.heads.load(Ordering::Relaxed), 1);
            assert_eq!(cache.io.full_gets.load(Ordering::Relaxed), 1);
            assert_eq!(
                cache.io.requested_bytes.load(Ordering::Relaxed),
                reported_size
            );
            assert_eq!(cache.io.received_bytes.load(Ordering::Relaxed), 128);
            assert_eq!(cache.io.failures.load(Ordering::Relaxed), 1);
            drop(entry);
            cache.drain().await.unwrap();
        }
    }
    #[tokio::test]
    async fn completed_download_is_reused_after_remount_without_get() {
        let directory = tempfile::tempdir().unwrap();
        let backend = Backend {
            gets: Default::default(),
            release: Default::default(),
            fail_tail: Arc::new(AtomicBool::new(false)),
            reported_size: 128,
        };
        let client = ObjectClient::new(backend.clone());
        let first = PackCache::persistent(
            128,
            SnapshotMountBudget::defaults(),
            Arc::new(ReadonlyTransport::default()),
            Arc::new(BackendIoCounters::default()),
            directory.path().to_owned(),
        )
        .unwrap();
        first.read(&client, "pack", 0, 64).await.unwrap();
        backend.release.notify_one();
        first.drain().await.unwrap();
        drop(first);
        let second = PackCache::persistent(
            128,
            SnapshotMountBudget::defaults(),
            Arc::new(ReadonlyTransport::default()),
            Arc::new(BackendIoCounters::default()),
            directory.path().to_owned(),
        )
        .unwrap();
        assert_eq!(
            second.read(&client, "pack", 64, 64).await.unwrap(),
            (64..128u8).collect::<Vec<_>>()
        );
        assert_eq!(backend.gets.load(Ordering::Relaxed), 1);
        second.drain().await.unwrap();
    }

    #[tokio::test]
    async fn stream_pins_object_until_last_chunk_and_counts_one_request() {
        let (cache, client, backend) = setup(false, 128);
        let mut stream = cache
            .stream(client.clone(), "pack".into(), 0, 128, None)
            .await
            .unwrap();
        backend.release.notify_one();
        assert_eq!(stream.next().await.unwrap().unwrap().len(), 128);
        // The stream still owns its entry until EOF, so a competing pack uses
        // the range fallback rather than evicting the pinned local file.
        assert_eq!(
            cache.read(&client, "other", 0, 8).await.unwrap(),
            (0..8u8).collect::<Vec<_>>()
        );
        assert_eq!(backend.gets.load(Ordering::Relaxed), 1);
        assert!(stream.next().await.is_none());
        cache.drain().await.unwrap();
    }
}
