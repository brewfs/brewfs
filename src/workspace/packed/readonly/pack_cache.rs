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
use std::sync::{Arc, Mutex, Weak};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Notify;

// Four HEADs and four body drivers, each with transport's maximum 4096-byte
// key. Pending admissions leave this space free for their downstream drivers.
const DRIVER_HEADROOM: u64 = 8 * (4096 + 2 * 4096);

#[derive(Default)]
struct Progress {
    available: u64,
    done: bool,
    error: Option<String>,
}
struct Entry {
    file: PathBuf,
    _temporary: Option<tempfile::TempPath>,
    persisted: Option<PathBuf>,
    progress: Mutex<Progress>,
    changed: Notify,
    size: u64,
    _owner: SnapshotOwnedPermit,
    _occupancy: EntryOccupancy,
}

// The table is weak: only real callers and transport drivers retain Plans.
// Completed HEADs leave no task header or cached error holding temporary Plans.
struct Pending {
    cache: Weak<PackCache>,
    cache_key: String,
    lock: Arc<tokio::sync::Mutex<()>>,
    _owner: SnapshotOwnedPermit,
}
impl Drop for Pending {
    fn drop(&mut self) {
        let Some(cache) = self.cache.upgrade() else {
            return;
        };
        let mut pending = cache.pending.lock().unwrap();
        if pending
            .get(&self.cache_key)
            .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), self))
        {
            pending.remove(&self.cache_key);
            // Retire bucket storage before this real owner releases Plans.
            if pending.capacity() > pending.len().saturating_mul(4) {
                pending.shrink_to_fit();
            }
        }
    }
}

enum Lookup {
    Hit(Arc<Entry>),
    Miss,
    Fallback,
}

// These gauges follow actual Entry ownership, including a reader that outlives
// registry drain. They observe accounting; admission still uses the real
// disk reservation and SnapshotOwnedPermit below.
#[derive(Default)]
pub(super) struct CacheOccupancy {
    pub live_entries: AtomicU64,
    pub live_reserved_bytes: AtomicU64,
}
impl CacheOccupancy {
    fn retain(self: &Arc<Self>, bytes: u64) -> EntryOccupancy {
        self.live_entries.fetch_add(1, Ordering::Relaxed);
        self.live_reserved_bytes.fetch_add(bytes, Ordering::Relaxed);
        EntryOccupancy {
            counters: self.clone(),
            bytes,
        }
    }
}
struct EntryOccupancy {
    counters: Arc<CacheOccupancy>,
    bytes: u64,
}
impl Drop for EntryOccupancy {
    fn drop(&mut self) {
        self.counters.live_entries.fetch_sub(1, Ordering::Relaxed);
        self.counters
            .live_reserved_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}
#[derive(Default)]
struct Registry {
    entries: HashMap<String, Arc<Entry>>,
    reserved: u64,
    closed: bool,
}
impl Registry {
    fn shrink_after_retirement(&mut self) {
        // HashMap buckets must not retain a large high-water allocation after
        // most Entry permits retire. Check per removal, but rebuild only at
        // a sparse threshold, while the removed entry still owns its permit.
        if self.entries.capacity() > self.entries.len().saturating_mul(4) {
            self.entries.shrink_to_fit();
        }
    }
}
pub(super) struct PackCache {
    registry: tokio::sync::Mutex<Registry>,
    // Lock order: registry -> pending/workers. Pending::drop takes only pending.
    pending: Mutex<HashMap<String, Weak<Pending>>>,
    closed: Notify,
    capacity: u64,
    directory: Option<PathBuf>,
    _lock: Option<std::fs::File>,
    pub hits: AtomicU64,
    pub ready_hits: AtomicU64,
    pub inflight_hits: AtomicU64,
    pub local_bytes: AtomicU64,
    pub misses: AtomicU64,
    pub downloaded: AtomicU64,
    pub occupancy: Arc<CacheOccupancy>,
    pub evictions: AtomicU64,
    budget: Arc<SnapshotMountBudget>,
    transport: Arc<ReadonlyTransport>,
    io: Arc<BackendIoCounters>,
    slots: Arc<tokio::sync::Semaphore>,
    head_slots: Arc<tokio::sync::Semaphore>,
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
            pending: Default::default(),
            closed: Notify::new(),
            capacity,
            directory: None,
            _lock: None,
            budget,
            transport,
            io,
            slots: Arc::new(tokio::sync::Semaphore::new(4)),
            head_slots: Arc::new(tokio::sync::Semaphore::new(4)),
            hits: AtomicU64::new(0),
            ready_hits: AtomicU64::new(0),
            inflight_hits: AtomicU64::new(0),
            local_bytes: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            downloaded: AtomicU64::new(0),
            occupancy: Default::default(),
            evictions: AtomicU64::new(0),
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
            if registry.reserved.saturating_add(size) > capacity || owner.is_err() {
                std::fs::remove_file(row.path())?;
                inner.evictions.fetch_add(1, Ordering::Relaxed);
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
                    _occupancy: inner.occupancy.retain(size),
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
        let pending = {
            let mut registry = self.registry.lock().await;
            ensure!(!registry.closed, "pack cache closed");
            match self.lookup(&mut registry, &cache_key, offset, length) {
                Lookup::Hit(entry) => return Ok(Some(entry)),
                Lookup::Fallback => return Ok(None),
                Lookup::Miss => {}
            }
            let mut table = self.pending.lock().unwrap();
            if let Some(existing) = table.get(&cache_key).and_then(Weak::upgrade) {
                existing
            } else {
                let Ok(owner) = self.budget.admit_with_headroom(
                    SnapshotBudgetPool::Plans,
                    4096 + key.len() as u64,
                    DRIVER_HEADROOM,
                ) else {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                };
                let pending = Arc::new(Pending {
                    cache: Arc::downgrade(self),
                    cache_key: cache_key.clone(),
                    lock: Arc::new(tokio::sync::Mutex::new(())),
                    _owner: owner,
                });
                table.insert(cache_key.clone(), Arc::downgrade(&pending));
                pending
            }
        };

        let closed = self.closed.notified();
        tokio::pin!(closed);
        closed.as_mut().enable();
        ensure!(!self.head_slots.is_closed(), "pack cache closed");
        let guard = tokio::select! {
            guard = pending.lock.clone().lock_owned() => guard,
            () = closed => anyhow::bail!("pack cache closed"),
            () = self.budget.wait_closed() => anyhow::bail!("pack cache budget closed"),
        };
        {
            let mut registry = self.registry.lock().await;
            ensure!(!registry.closed, "pack cache closed");
            // The previous owner may have published while this caller waited
            // for the same key. Hits need no new HEAD or download worker.
            match self.lookup(&mut registry, &cache_key, offset, length) {
                Lookup::Hit(entry) => return Ok(Some(entry)),
                Lookup::Fallback => return Ok(None),
                Lookup::Miss => {}
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let slot = tokio::select! {
            slot = self.head_slots.clone().acquire_owned() => slot?,
            () = self.budget.wait_closed() => anyhow::bail!("pack cache budget closed"),
        };
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
        // Form the ownership bundle before run: admission failure or dropping
        // an unpolled factory must release the same complete set of owners.
        let guards = (guard, slot, pending);
        let head = self
            .transport
            .run(&self.budget, key.len(), 0, || async move {
                // The real detached transport driver owns these guards after
                // caller cancellation. Successful results retain them through
                // registry publication, preventing a second HEAD in that gap.
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
                io.finish_open(result).map(|size| (size, guards))
            })
            .await?;
        let result = self
            .prepare_body(client, key, &cache_key, head.value.0, observation)
            .await;
        drop(head);
        result
    }

    fn lookup(
        &self,
        registry: &mut Registry,
        cache_key: &str,
        offset: u64,
        length: usize,
    ) -> Lookup {
        if let Some(entry) = registry.entries.get(cache_key)
            && entry.progress.lock().unwrap().error.is_some()
        {
            // A failed partial file stays charged while a reader/worker pins
            // it. Once unpinned, retire it before admitting a replacement.
            if Arc::strong_count(entry) > 1 {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return Lookup::Fallback;
            }
            let failed = registry.entries.remove(cache_key).unwrap();
            registry.reserved -= failed.size;
            registry.shrink_after_retirement();
            drop(failed);
        }
        if let Some(entry) = registry.entries.get(cache_key) {
            self.record_hit(entry, offset, length);
            Lookup::Hit(entry.clone())
        } else {
            Lookup::Miss
        }
    }

    fn record_hit(&self, entry: &Entry, offset: u64, length: usize) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        if offset
            .checked_add(length as u64)
            .is_some_and(|end| entry.progress.lock().unwrap().available >= end)
        {
            self.ready_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inflight_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn prepare_body<B: ObjectBackend + Clone + 'static>(
        self: &Arc<Self>,
        client: &ObjectClient<B>,
        key: &str,
        cache_key: &str,
        size: u64,
        observation: Observation,
    ) -> Result<Option<Arc<Entry>>> {
        let mut registry = self.registry.lock().await;
        ensure!(!registry.closed, "pack cache closed");
        ensure!(!self.budget.state().closed, "pack cache budget closed");
        if size > self.capacity {
            drop(registry);
            return Ok(None);
        }
        while registry.reserved + size > self.capacity {
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
            registry.shrink_after_retirement();
            self.evictions.fetch_add(1, Ordering::Relaxed);
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
        // The download worker opens the path itself. Retain automatic
        // path cleanup, without retaining one unused FD per entry.
        let temporary = match &self.directory {
            Some(directory) => tempfile::Builder::new()
                .prefix(".partial-")
                .tempfile_in(directory)?,
            None => tempfile::NamedTempFile::new()?,
        }
        .into_temp_path();
        let entry = Arc::new(Entry {
            file: temporary.to_path_buf(),
            _temporary: Some(temporary),
            persisted: self
                .directory
                .as_ref()
                .map(|directory| directory.join(format!("{cache_key}.pack"))),
            _owner: owner,
            _occupancy: self.occupancy.retain(size),
            progress: Default::default(),
            changed: Notify::new(),
            size,
        });
        registry.reserved += size;
        registry.entries.insert(cache_key.to_owned(), entry.clone());
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
        let workers = {
            let mut registry = self.registry.lock().await;
            registry.closed = true;
            self.head_slots.close();
            // A HEAD result cannot publish a body worker after closure. Real
            // detached HEADs are drained by the enclosing readonly transport.
            self.closed.notify_waiters();
            std::mem::take(&mut *self.workers.lock().unwrap())
        };
        let mut workers = workers;
        let mut failure = None;
        while let Some(worker) = workers.pop() {
            if workers.capacity() > workers.len().saturating_mul(4) {
                workers.shrink_to_fit();
            }
            if let Err(error) = worker.await {
                failure.get_or_insert(error);
            }
        }
        let mut registry = self.registry.lock().await;
        // Retire the registry allocation too; response-owned entries retain
        // their own permits and occupancy until their last reader exits.
        registry.entries = HashMap::new();
        registry.reserved = 0;
        if let Some(error) = failure {
            return Err(error.into());
        }
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
#[path = "pack_cache_budget_tests.rs"]
mod budget_tests;

#[cfg(test)]
#[path = "pack_cache_concurrency_tests.rs"]
mod concurrency_tests;

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
        assert_eq!(cache.occupancy.live_entries.load(Ordering::Relaxed), 1);
        assert_eq!(
            cache.occupancy.live_reserved_bytes.load(Ordering::Relaxed),
            128
        );
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
        assert_eq!(cache.occupancy.live_entries.load(Ordering::Relaxed), 0);
        assert_eq!(
            cache.occupancy.live_reserved_bytes.load(Ordering::Relaxed),
            0
        );
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
        assert_eq!(cache.occupancy.live_entries.load(Ordering::Relaxed), 1);
        assert_eq!(
            cache.occupancy.live_reserved_bytes.load(Ordering::Relaxed),
            128
        );
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
        assert_eq!(cache.occupancy.live_entries.load(Ordering::Relaxed), 0);
        assert_eq!(
            cache.occupancy.live_reserved_bytes.load(Ordering::Relaxed),
            0
        );
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
