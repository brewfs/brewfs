//! File-map oracles use deterministic MemoryLower bytes and real RustFS for
//! every physical upper PUT/GET/DELETE. The packed-lower integration suite
//! separately exercises authenticated packed objects through EntityData.
//! Metadata-only checks use a capability stub that panics on any object I/O.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::Notify;

use super::*;
use crate::cadapter::client::ObjectByteStream;
use crate::cadapter::s3::S3Backend;
use crate::workspace_overlay::entity::backend::{
    CreateAllocation, RowSnapshot, SliceAllocation, TransactionAuthority,
};
use crate::workspace_overlay::entity::keys::EntityKeys;
use crate::workspace_overlay::entity::lifecycle::{Lifecycle, WriterLease, WriterSession};
use crate::workspace_overlay::entity::memory::MemoryEntityBackend;
use crate::workspace_overlay::entity::model::{EntityDentry, LowerBinding};
use crate::workspace_overlay::entity::namespace::{CreateRequest, Credentials};
use crate::workspace_overlay::entity::redis::RedisEntityBackend;
use crate::workspace_overlay::entity::resolver::ImmutableLower;
use crate::workspace_overlay::entity::resolver::test_support::MemoryLower;
use crate::workspace_overlay::entity::test_objects;

struct Objects {
    inner: S3Backend,
    puts: AtomicUsize,
    fail_put: AtomicUsize,
    pause_put: AtomicUsize,
    gets: AtomicUsize,
    pause_get: AtomicUsize,
    deletes: AtomicUsize,
    fail_delete: AtomicBool,
    entered: Notify,
    resume: Notify,
}

impl Objects {
    fn new(inner: S3Backend) -> Self {
        Self {
            inner,
            puts: AtomicUsize::new(0),
            fail_put: AtomicUsize::new(0),
            pause_put: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
            pause_get: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
            fail_delete: AtomicBool::new(false),
            entered: Notify::new(),
            resume: Notify::new(),
        }
    }

    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.entered.notified())
            .await
            .expect("upload did not reach pause");
    }
}

#[async_trait]
impl ObjectBackend for Objects {
    fn forbids_mutation_replay(&self) -> bool {
        self.inner.forbids_mutation_replay()
    }
    async fn put_object(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object(key, bytes).await
    }
    async fn put_object_create_only_durable(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        let number = self.puts.fetch_add(1, Ordering::SeqCst) + 1;
        self.inner
            .put_object_create_only_durable(key, bytes)
            .await?;
        if self.pause_put.load(Ordering::SeqCst) == number {
            let resumed = self.resume.notified();
            tokio::pin!(resumed);
            resumed.as_mut().enable();
            self.entered.notify_one();
            resumed.await;
        }
        if self.fail_put.load(Ordering::SeqCst) == number {
            anyhow::bail!("injected ambiguous durable PUT reply");
        }
        Ok(())
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.inner.get_object_range(key, offset, buf).await
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        let number = self.gets.fetch_add(1, Ordering::SeqCst) + 1;
        let result = self
            .inner
            .get_object_range_stream(key, offset, length)
            .await;
        if self.pause_get.load(Ordering::SeqCst) == number {
            let resumed = self.resume.notified();
            tokio::pin!(resumed);
            resumed.as_mut().enable();
            self.entered.notify_one();
            resumed.await;
        }
        result
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(key).await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        self.inner.delete_object(key).await?;
        if self.fail_delete.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected ambiguous DELETE reply");
        }
        Ok(())
    }
}

struct NoObjectIo;

#[async_trait]
impl ObjectBackend for NoObjectIo {
    async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        panic!("metadata-only test attempted object PUT")
    }
    async fn put_object_create_only_durable(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        panic!("metadata-only test attempted durable object PUT")
    }
    async fn get_object(&self, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
        panic!("metadata-only test attempted object GET")
    }
    async fn get_object_range(&self, _: &str, _: u64, _: &mut [u8]) -> anyhow::Result<usize> {
        panic!("metadata-only test attempted range GET")
    }
    async fn get_object_size_bounded(&self, _: &str) -> anyhow::Result<Option<u64>> {
        panic!("metadata-only test attempted object HEAD")
    }
    async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
        panic!("metadata-only test attempted object ETag")
    }
    async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
        panic!("metadata-only test attempted object DELETE")
    }
}

struct CountingRangeObjects {
    bytes: Option<Vec<u8>>,
    heads: AtomicUsize,
    gets: AtomicUsize,
}

#[async_trait]
impl ObjectBackend for CountingRangeObjects {
    async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("test object backend is read-only")
    }

    async fn get_object(&self, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
        panic!("entity reads must use bounded streaming GET")
    }

    async fn get_object_range(
        &self,
        _: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        let start = usize::try_from(offset)?;
        let Some(source) = self.bytes.as_deref().and_then(|bytes| bytes.get(start..)) else {
            return Ok(0);
        };
        let source = &source[..source.len().min(buf.len())];
        buf[..source.len()].copy_from_slice(source);
        Ok(source.len())
    }

    async fn get_object_range_stream(
        &self,
        _: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let start = usize::try_from(offset)?;
        let bytes = self
            .bytes
            .as_deref()
            .and_then(|bytes| bytes.get(start..))
            .unwrap_or_default();
        let bytes = bytes[..bytes.len().min(usize::try_from(length)?)].to_vec();
        Ok(Box::pin(futures_util::stream::once(async move {
            Ok(Bytes::from(bytes))
        })))
    }

    async fn get_object_size_bounded(&self, _: &str) -> anyhow::Result<Option<u64>> {
        self.heads.fetch_add(1, Ordering::SeqCst);
        Ok(self.bytes.as_ref().map(|bytes| bytes.len() as u64))
    }

    async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
        Ok("test-etag".into())
    }

    async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
        anyhow::bail!("test object backend is read-only")
    }
}

struct Fixture<B, O = Objects> {
    backend: Arc<B>,
    keys: EntityKeys,
    session: WriterSession,
    namespace: Arc<EntityNamespace<B, MemoryLower>>,
    data: Arc<EntityData<B, MemoryLower, O>>,
    objects: Arc<O>,
}

/// Count calls at the metadata boundary while executing actual file semantics.
/// Object I/O still goes through the shared real RustFS client.
struct MeteredBackend<B> {
    inner: B,
    reads: AtomicUsize,
    commits: AtomicUsize,
    reservations: AtomicUsize,
    scans: AtomicUsize,
}

impl<B> MeteredBackend<B> {
    fn new(inner: B) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
            commits: AtomicUsize::new(0),
            reservations: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
        }
    }

    fn take_counts(&self) -> (usize, usize, usize, usize) {
        (
            self.reads.swap(0, Ordering::SeqCst),
            self.commits.swap(0, Ordering::SeqCst),
            self.reservations.swap(0, Ordering::SeqCst),
            self.scans.swap(0, Ordering::SeqCst),
        )
    }
}

#[async_trait]
impl<B: EntityBackend> EntityBackend for MeteredBackend<B> {
    async fn read(&self, keys: &[Vec<u8>]) -> Result<RowSnapshot> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read(keys).await
    }

    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        self.inner.commit(authority, plan).await
    }

    async fn commit_with_inode_allocation(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
        create: &CreateAllocation,
    ) -> Result<Option<i64>> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        self.inner
            .commit_with_inode_allocation(authority, plan, create)
            .await
    }

    async fn reserve_slices(
        &self,
        authority: &TransactionAuthority,
        allocation: &SliceAllocation,
    ) -> Result<Option<Vec<u64>>> {
        self.reservations.fetch_add(1, Ordering::SeqCst);
        self.inner.reserve_slices(authority, allocation).await
    }

    async fn scan_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_keys_page(prefix, after, limit).await
    }

    async fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_prefix(prefix, limit).await
    }
    async fn scan_prefix_batch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_prefix_batch(prefix, limit).await
    }
}

async fn exercise_batched_file_io<B: EntityBackend + 'static>(
    fixture: &Fixture<MeteredBackend<B>>,
) {
    let handle = new_file(fixture, b"batched").await;
    fixture.backend.take_counts();
    // Three chunks still use one reservation and one atomic publication.
    let original = Bytes::from_static(b"abcdefghijklmnopqrstuvwx");
    let result = fixture
        .data
        .write(handle.clone(), 0, original.clone(), false)
        .await
        .unwrap();
    assert_eq!((result.written, result.size), (24, 24));
    assert_eq!(fixture.backend.take_counts(), (1, 1, 1, 0));
    assert_eq!(fixture.objects.puts.load(Ordering::SeqCst), 3);
    assert_eq!(
        fixture
            .data
            .attr(handle.clone())
            .await
            .unwrap()
            .data_version,
        1
    );
    assert_eq!(fixture.backend.take_counts(), (1, 0, 0, 0));
    assert_eq!(
        fixture.data.read(handle.clone(), 0, 24).await.unwrap(),
        original
    );
    // Slice identities known from publication should nominate ownership and
    // authentication rows in the initial batch. Keep the post-transport fence.
    assert_eq!(fixture.backend.take_counts(), (2, 0, 0, 0));
    fixture.data.fsync(handle.clone()).await.unwrap();
    assert_eq!(fixture.backend.take_counts(), (1, 1, 0, 0));
    assert!(
        fixture
            .data
            .read(handle.clone(), 24, 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(fixture.backend.take_counts(), (1, 0, 0, 0));
    // A new reader must discover the identities once, then batch later reads.
    let cold = EntityData::new(fixture.namespace.clone(), fixture.objects.clone(), 8)
        .await
        .unwrap();
    fixture.backend.take_counts();
    for reads in [3, 2] {
        assert_eq!(cold.read(handle.clone(), 0, 24).await.unwrap(), original);
        assert_eq!(fixture.backend.take_counts(), (reads, 0, 0, 0));
    }
    let replacement = Bytes::from_static(b"ZYXWVUTSRQPONMLKJIHGFEDC");
    fixture
        .data
        .write(handle.clone(), 0, replacement.clone(), false)
        .await
        .unwrap();
    assert_eq!(fixture.backend.take_counts(), (2, 1, 1, 0));
    assert_eq!(
        fixture.data.read(handle.clone(), 0, 24).await.unwrap(),
        replacement
    );
    assert_eq!(fixture.backend.take_counts(), (2, 0, 0, 0));
    // Another reader's stale identities must never substitute cached values
    // for the newly published maps, ownership, or object authentication rows.
    for reads in [3, 2] {
        assert_eq!(cold.read(handle.clone(), 0, 24).await.unwrap(), replacement);
        assert_eq!(fixture.backend.take_counts(), (reads, 0, 0, 0));
    }
    let resized = fixture.data.truncate(handle.clone(), 32).await.unwrap();
    assert_eq!((resized.size, resized.data_version), (32, 3));
    assert_eq!(fixture.backend.take_counts(), (1, 1, 0, 0));
    let mut expected = replacement.to_vec();
    expected.resize(32, 0);
    assert_eq!(fixture.data.read(handle, 0, 32).await.unwrap(), expected);
}

#[tokio::test]
async fn eof_after_visible_cut_ignores_oversized_invisible_tail_metadata() {
    use crate::workspace_overlay::entity::{backend::MAX_TRANSACTION_BYTES, model::ChunkSpan};

    const CHUNK_SIZE: u64 = 4096;
    const CHUNKS: u64 = 64;
    let backend = Arc::new(MemoryEntityBackend::new(1000));
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let mut lower = MemoryLower::empty();
    lower.add_file(1, b"base", 2, 24);
    let lower = Arc::new(lower);
    let digest = lower.binding().manifest_digest;
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    lifecycle.provision(lower.binding()).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), 1_000_000).await.unwrap();
    let namespace = EntityNamespace::new(backend.clone(), lower, keys.clone(), session.clone());
    let objects = Arc::new(NoObjectIo);
    let data = EntityData::new(namespace.clone(), objects.clone(), CHUNK_SIZE)
        .await
        .unwrap();
    let fixture = Fixture {
        backend,
        keys,
        session,
        namespace,
        data,
        objects,
    };
    let handle = new_file(&fixture, b"cut").await;
    let authority = lifecycle.authority(&fixture.session).await.unwrap();
    let mut inode = fixture.data.attr(handle.clone()).await.unwrap();
    let before = codec::encode(&UpperInode::Present(inode.clone())).unwrap();
    inode.size = CHUNKS * CHUNK_SIZE;
    inode.data_version += 1;
    let grown = codec::encode(&UpperInode::Present(inode.clone())).unwrap();
    assert_eq!(
        fixture
            .backend
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![RowCheck {
                        key: fixture.keys.inode(inode.ino),
                        expected: Some(before)
                    }],
                    writes: vec![RowWrite::Put {
                        key: fixture.keys.inode(inode.ino),
                        value: grown.clone()
                    }],
                }
            )
            .await
            .unwrap(),
        CommitOutcome::Committed
    );

    // Valid immutable lower ranges alternate with holes. No upper object is
    // involved, and every individual map fits its row/transaction limits.
    let map = ChunkMap::new(
        CHUNK_SIZE,
        (0..CHUNK_SIZE)
            .map(|offset| ChunkSpan {
                offset,
                length: 1,
                source: if offset % 2 == 0 {
                    ChunkSource::LowerRange {
                        binding_digest: digest,
                        ino: 2,
                        file_offset: 0,
                    }
                } else {
                    ChunkSource::Hole
                },
            })
            .collect(),
    )
    .unwrap();
    let bytes = codec::encode(&map).unwrap();
    assert!(bytes.len() * CHUNKS as usize > MAX_TRANSACTION_BYTES);
    let chunk_keys: Vec<_> = (0..CHUNKS)
        .map(|index| fixture.keys.chunk(inode.ino, index))
        .collect();
    for batch in chunk_keys.chunks(4) {
        assert_eq!(
            fixture
                .backend
                .commit(
                    &authority,
                    &EntityPlan {
                        checks: batch
                            .iter()
                            .map(|key| RowCheck {
                                key: key.clone(),
                                expected: None
                            })
                            .collect(),
                        writes: batch
                            .iter()
                            .map(|key| RowWrite::Put {
                                key: key.clone(),
                                value: bytes.clone()
                            })
                            .collect(),
                    }
                )
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
    }
    // The normal shrink protocol exposes its cut before bounded map cleanup.
    let cut = prepare_resize(
        &fixture.keys,
        inode.ino,
        Some(grown),
        &inode,
        0,
        CHUNK_SIZE,
        1000,
    )
    .unwrap();
    assert_eq!(
        fixture.backend.commit(&authority, &cut).await.unwrap(),
        CommitOutcome::Committed
    );
    assert!(matches!(
        fixture.backend.read(&chunk_keys).await,
        Err(EntityError::Invalid(_))
    ));
    assert!(
        fixture
            .data
            .read(handle, 0, (CHUNKS * CHUNK_SIZE) as usize)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn batched_file_io_has_one_publication_and_preserves_bytes() {
    let fixture = attach(
        Arc::new(MeteredBackend::new(MemoryEntityBackend::new(1000))),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        1_000_000,
    )
    .await;
    exercise_batched_file_io(&fixture).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and RustFS via the entity data test harness"]
async fn redis_batched_file_io_has_one_publication_and_preserves_bytes() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let volume = Uuid::new_v4();
    let backend = RedisEntityBackend::connect(&url, volume).await.unwrap();
    let fixture = attach(
        Arc::new(MeteredBackend::new(backend)),
        EntityKeys::new(volume, Uuid::new_v4()),
        60_000_000_000,
    )
    .await;
    exercise_batched_file_io(&fixture).await;
}

#[tokio::test]
#[ignore = "requires isolated RustFS via the entity data test harness"]
async fn batched_upper_read_rechecks_lease_after_get() {
    let fixture = fixture().await;
    let handle = new_file(&fixture, b"fenced-read").await;
    fixture
        .data
        .write(handle.clone(), 0, Bytes::from_static(b"payload"), false)
        .await
        .unwrap();
    fixture.objects.pause_get.store(1, Ordering::SeqCst);
    let data = fixture.data.clone();
    let reading = tokio::spawn(async move { data.read(handle, 0, 7).await });
    fixture.objects.entered().await;
    fixture.backend.set_time(2_000_000).unwrap();
    fixture.objects.resume.notify_one();
    assert_eq!(reading.await.unwrap(), Err(EntityError::Fenced));
}

#[tokio::test]
#[ignore = "requires isolated RustFS via the entity data test harness"]
async fn batched_read_does_not_validate_unused_stale_slice_hints() {
    let fixture = attach(
        Arc::new(MeteredBackend::new(MemoryEntityBackend::new(1000))),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        1_000_000,
    )
    .await;
    let handle = new_file(&fixture, b"stale-hints").await;
    fixture
        .data
        .write(handle.clone(), 0, Bytes::from_static(b"old"), false)
        .await
        .unwrap();
    let snapshot = fixture
        .backend
        .inner
        .read(&[fixture.keys.chunk(handle.ino(), 0)])
        .await
        .unwrap();
    let map: ChunkMap = codec::decode(snapshot.rows[0].as_deref().unwrap()).unwrap();
    let descriptor_key = fixture
        .keys
        .object_descriptor(*map.upper_slices().unwrap().first().unwrap());
    let expected = fixture
        .backend
        .inner
        .read(std::slice::from_ref(&descriptor_key))
        .await
        .unwrap()
        .rows
        .remove(0);
    let reader = EntityData::new(fixture.namespace.clone(), fixture.objects.clone(), 8)
        .await
        .unwrap();
    assert_eq!(reader.read(handle.clone(), 0, 3).await.unwrap(), b"old");
    fixture
        .data
        .write(handle.clone(), 0, Bytes::from_static(b"new"), false)
        .await
        .unwrap();
    let authority = fixture.namespace.authority().await.unwrap();
    let next_get = fixture.objects.gets.load(Ordering::SeqCst) + 1;
    fixture.objects.pause_get.store(next_get, Ordering::SeqCst);
    fixture.backend.take_counts();
    let reading = tokio::spawn(async move { reader.read(handle, 0, 3).await });
    fixture.objects.entered().await;
    // This old descriptor is unrelated to the freshly observed map. Changing
    // it during GET must not invalidate the read or cause a second object GET.
    assert_eq!(
        fixture
            .backend
            .inner
            .commit(
                &authority,
                &EntityPlan {
                    checks: vec![RowCheck {
                        key: descriptor_key.clone(),
                        expected
                    }],
                    writes: vec![RowWrite::Delete {
                        key: descriptor_key
                    }],
                }
            )
            .await
            .unwrap(),
        CommitOutcome::Committed,
    );
    fixture.objects.resume.notify_one();
    assert_eq!(reading.await.unwrap().unwrap(), b"new");
    assert_eq!(fixture.backend.take_counts(), (3, 0, 0, 0));
    assert_eq!(fixture.objects.gets.load(Ordering::SeqCst), next_get);
}

#[tokio::test]
#[ignore = "requires isolated Redis and RustFS via the entity data test harness"]
async fn redis_batched_read_ignores_corrupt_unused_hint_rows() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let volume = Uuid::new_v4();
    let fixture = attach(
        Arc::new(MeteredBackend::new(
            RedisEntityBackend::connect(&url, volume).await.unwrap(),
        )),
        EntityKeys::new(volume, Uuid::new_v4()),
        60_000_000_000,
    )
    .await;
    let mut connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let handle = new_file(&fixture, b"corrupt-hint").await;
    let reader = EntityData::new(fixture.namespace.clone(), fixture.objects.clone(), 8)
        .await
        .unwrap();
    for (bytes, required) in [(b"before", false), (b"after!", true)] {
        fixture
            .data
            .write(handle.clone(), 0, Bytes::copy_from_slice(bytes), false)
            .await
            .unwrap();
        assert_eq!(reader.read(handle.clone(), 0, 6).await.unwrap(), bytes);
        let snapshot = fixture
            .backend
            .inner
            .read(&[fixture.keys.chunk(handle.ino(), 0)])
            .await
            .unwrap();
        let map: ChunkMap = codec::decode(snapshot.rows[0].as_deref().unwrap()).unwrap();
        let key = fixture
            .keys
            .object_descriptor(*map.upper_slices().unwrap().first().unwrap());
        redis::pipe()
            .cmd("DEL")
            .arg(&key)
            .ignore()
            .cmd("LPUSH")
            .arg(&key)
            .arg("wrong type")
            .ignore()
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
        if required {
            assert!(matches!(
                reader.read(handle.clone(), 0, 6).await,
                Err(EntityError::Corrupt(_))
            ));
        } else {
            // Replace the map without consulting its now-unused descriptor.
            fixture
                .data
                .write(handle.clone(), 0, Bytes::from_static(b"after!"), false)
                .await
                .unwrap();
            fixture.backend.take_counts();
            assert_eq!(reader.read(handle.clone(), 0, 6).await.unwrap(), b"after!");
            // Failed speculative read, ordinary map/dependency reads, final fence.
            assert_eq!(fixture.backend.take_counts(), (4, 0, 0, 0));
        }
    }
}

async fn exercise_fragmented_publication<B: EntityBackend + 'static>(
    fixture: &Fixture<MeteredBackend<B>>,
) {
    use crate::workspace_overlay::entity::{
        model::ChunkSpan,
        ownership::{SliceState, chunk_membership_keys},
    };
    use std::collections::BTreeSet;

    const CHUNKS: u64 = 64;
    const SLICES: u64 = 32;
    let handle = new_file(fixture, b"fragmented").await;
    let lifecycle = Lifecycle::new(fixture.backend.clone(), fixture.keys.clone());
    let authority = lifecycle.authority(&fixture.session).await.unwrap();
    let ownership = Ownership::new(fixture.backend.clone(), fixture.keys.clone());
    let tokens = ownership
        .reserve_uploads(&fixture.session, &authority, &[1; SLICES as usize])
        .await
        .unwrap();
    let mut receipts = Vec::new();
    let mut descriptors = Vec::new();
    for (position, token) in tokens.into_iter().enumerate() {
        let bytes = [b'a' + position as u8 % 26];
        fixture
            .objects
            .put_object_create_only_durable(&fixture.keys.object(token.slice_id()), &bytes)
            .await
            .unwrap();
        descriptors.push(ObjectDescriptor {
            namespace_id: fixture.keys.namespace_id(),
            slice_id: token.slice_id(),
            size: 1,
            sha256: Sha256::digest(bytes).into(),
        });
        receipts.push(token.confirm_durable_put());
    }
    let old_ids: Vec<_> = receipts.iter().map(DurableUpload::slice_id).collect();
    let fragmented = ChunkMap::new(
        SLICES,
        old_ids
            .iter()
            .enumerate()
            .map(|(offset, id)| ChunkSpan {
                offset: offset as u64,
                length: 1,
                source: ChunkSource::Data {
                    slice_id: *id,
                    slice_offset: 0,
                },
            })
            .collect(),
    )
    .unwrap();
    let holes = ChunkMap::holes(SLICES).unwrap();
    let mut inode = fixture.data.attr(handle.clone()).await.unwrap();
    // Build a valid shared-slice fixture through ordinary bounded publication:
    // Pending precedes each real PUT; every map/refcount/descriptor is atomic.
    for start in (0..CHUNKS).step_by(8) {
        let authority = lifecycle.authority(&fixture.session).await.unwrap();
        let replacements: Vec<_> = (start..start + 8)
            .map(|index| (index, holes.clone(), fragmented.clone()))
            .collect();
        let mut keys = BTreeSet::new();
        for (index, old, new) in &replacements {
            keys.extend(chunk_membership_keys(&fixture.keys, inode.ino, *index, old, new).unwrap());
        }
        let keys: Vec<_> = keys.into_iter().collect();
        let snapshot = fixture.backend.read(&keys).await.unwrap();
        let rows = keys.into_iter().zip(snapshot.rows).collect();
        let mut plan = plan_chunk_memberships_from_rows(
            &fixture.keys,
            inode.ino,
            &replacements,
            &rows,
            if start == 0 { &receipts } else { &[] },
        )
        .unwrap();
        for (index, _, new) in &replacements {
            let key = fixture.keys.chunk(inode.ino, *index);
            plan.checks.push(RowCheck {
                key: key.clone(),
                expected: None,
            });
            plan.writes.push(RowWrite::Put {
                key,
                value: codec::encode(new).unwrap(),
            });
        }
        if start == 0 {
            for descriptor in &descriptors {
                let key = fixture.keys.object_descriptor(descriptor.slice_id);
                plan.checks.push(RowCheck {
                    key: key.clone(),
                    expected: None,
                });
                plan.writes.push(RowWrite::Put {
                    key,
                    value: codec::encode(descriptor).unwrap(),
                });
            }
        }
        let old = codec::encode(&UpperInode::Present(inode.clone())).unwrap();
        inode.size = (start + 8) * SLICES;
        inode.data_version += 1;
        plan.checks.push(RowCheck {
            key: fixture.keys.inode(inode.ino),
            expected: Some(old),
        });
        plan.checks
            .push(absent_intent_check(&fixture.keys, inode.ino));
        plan.writes.push(RowWrite::Put {
            key: fixture.keys.inode(inode.ino),
            value: codec::encode(&UpperInode::Present(inode.clone())).unwrap(),
        });
        assert_eq!(
            fixture.backend.commit(&authority, &plan).await.unwrap(),
            CommitOutcome::Committed
        );
    }
    drop(receipts);
    let old_rows = fixture
        .backend
        .read(
            &old_ids
                .iter()
                .map(|id| fixture.keys.slice(*id))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    for row in old_rows.rows {
        let row: SliceOwnership = codec::decode(row.as_deref().unwrap()).unwrap();
        assert_eq!(row.current_refs, CHUNKS);
        assert!(!row.retirement_hold);
    }
    assert_eq!(fixture.objects.puts.load(Ordering::SeqCst), SLICES as usize);
    fixture.backend.take_counts();

    // All 2048 old edges need checks and deletion: one transaction cannot fit
    // the 4096-row limit, but two 32-chunk publications fit without re-upload.
    let expected = Bytes::from(vec![b'Z'; (CHUNKS * SLICES) as usize]);
    let outcome = fixture
        .data
        .write(handle.clone(), 0, expected.clone(), false)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        WriteOutcome {
            offset: 0,
            written: expected.len(),
            size: expected.len() as u64
        }
    );
    assert_eq!(fixture.backend.take_counts(), (6, 2, 1, 0));
    assert_eq!(
        fixture.objects.puts.load(Ordering::SeqCst),
        (SLICES + CHUNKS) as usize
    );
    assert_eq!(
        fixture
            .data
            .attr(handle.clone())
            .await
            .unwrap()
            .data_version,
        inode.data_version + 2
    );
    fixture.data.fsync(handle.clone()).await.unwrap();
    assert_eq!(
        fixture.data.read(handle, 0, expected.len()).await.unwrap(),
        expected
    );

    let old_rows = fixture
        .backend
        .read(
            &old_ids
                .iter()
                .map(|id| fixture.keys.slice(*id))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    for row in old_rows.rows {
        let row: SliceOwnership = codec::decode(row.as_deref().unwrap()).unwrap();
        assert_eq!(row.state, SliceState::Available);
        assert_eq!(row.current_refs, 0);
        assert!(row.retirement_hold);
    }
    let old_edges: Vec<_> = (0..CHUNKS)
        .flat_map(|index| {
            old_ids
                .iter()
                .map(move |id| fixture.keys.owner(*id, inode.ino, index))
        })
        .collect();
    assert!(
        fixture
            .backend
            .read(&old_edges)
            .await
            .unwrap()
            .rows
            .iter()
            .all(Option::is_none)
    );
    let maps = fixture
        .backend
        .read(
            &(0..CHUNKS)
                .map(|index| fixture.keys.chunk(inode.ino, index))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    let mut new_ids = BTreeSet::new();
    for row in maps.rows {
        let map: ChunkMap = codec::decode(row.as_deref().unwrap()).unwrap();
        let ids = map.upper_slices().unwrap();
        assert_eq!(ids.len(), 1);
        new_ids.extend(ids);
    }
    assert_eq!(new_ids.len(), CHUNKS as usize);
    let new_rows = fixture
        .backend
        .read(
            &new_ids
                .into_iter()
                .map(|id| fixture.keys.slice(id))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    for row in new_rows.rows {
        let row: SliceOwnership = codec::decode(row.as_deref().unwrap()).unwrap();
        assert_eq!(row.state, SliceState::Available);
        assert_eq!(row.current_refs, 1);
        assert!(!row.staging_hold);
    }
}

#[tokio::test]
#[ignore = "requires isolated RustFS via the entity data test harness"]
async fn fragmented_write_splits_publication_without_reupload_or_lost_refs() {
    let fixture = attach_objects(
        Arc::new(MeteredBackend::new(MemoryEntityBackend::new(1000))),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        60_000_000_000,
        Arc::new(Objects::new(test_objects::backend().await)),
        32,
    )
    .await;
    exercise_fragmented_publication(&fixture).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and RustFS via the entity data test harness"]
async fn redis_fragmented_write_splits_publication_without_reupload_or_lost_refs() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let volume = Uuid::new_v4();
    let fixture = attach_objects(
        Arc::new(MeteredBackend::new(
            RedisEntityBackend::connect(&url, volume).await.unwrap(),
        )),
        EntityKeys::new(volume, Uuid::new_v4()),
        60_000_000_000,
        Arc::new(Objects::new(test_objects::backend().await)),
        32,
    )
    .await;
    exercise_fragmented_publication(&fixture).await;
}

fn root() -> Credentials {
    Credentials {
        uid: 0,
        gid: 0,
        groups: vec![0],
        umask: 0o022,
    }
}

#[tokio::test]
async fn independent_reads_and_attr_progress_while_transport_is_paused() {
    struct PausedLower {
        inner: MemoryLower,
        entered: tokio::sync::mpsc::UnboundedSender<u64>,
        resume: [tokio::sync::Semaphore; 2],
    }

    #[async_trait]
    impl ImmutableLower for PausedLower {
        fn binding(&self) -> &LowerBinding {
            self.inner.binding()
        }
        async fn inode(&self, ino: i64) -> Result<Option<EntityInode>> {
            self.inner.inode(ino).await
        }
        async fn dentry(&self, parent: i64, name: &[u8]) -> Result<Option<EntityDentry>> {
            self.inner.dentry(parent, name).await
        }
        async fn xattr(&self, ino: i64, name: &[u8]) -> Result<Option<Vec<u8>>> {
            self.inner.xattr(ino, name).await
        }
        async fn chunk(&self, ino: i64, index: u64, size: u64) -> Result<Option<ChunkMap>> {
            self.inner.chunk(ino, index, size).await
        }
        async fn readdir(&self, ino: i64) -> Result<Vec<(Vec<u8>, EntityDentry)>> {
            self.inner.readdir(ino).await
        }
    }

    #[async_trait]
    impl ImmutableLowerData for PausedLower {
        async fn read_range(&self, ino: i64, offset: u64, length: usize) -> Result<Vec<u8>> {
            self.entered.send(offset).unwrap();
            self.resume[offset as usize / 8]
                .acquire()
                .await
                .unwrap()
                .forget();
            self.inner.read_range(ino, offset, length).await
        }
    }

    let backend = Arc::new(MemoryEntityBackend::new(1000));
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let mut inner = MemoryLower::empty();
    inner.add_file(1, b"base", 2, 24);
    let (entered, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let lower = Arc::new(PausedLower {
        inner,
        entered,
        resume: std::array::from_fn(|_| tokio::sync::Semaphore::new(0)),
    });
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    lifecycle.provision(lower.binding()).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), 1_000_000).await.unwrap();
    let namespace = EntityNamespace::new(backend, lower.clone(), keys, session);
    let data = EntityData::new(namespace.clone(), Arc::new(NoObjectIo), 8)
        .await
        .unwrap();
    let first = Arc::new(namespace.open(1, b"base", 6, &root()).await.unwrap());
    let second = Arc::new(namespace.open(1, b"base", 4, &root()).await.unwrap());
    let read_first = {
        let data = data.clone();
        let handle = first.clone();
        tokio::spawn(async move { data.read(handle, 0, 8).await })
    };
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap(),
        Some(0)
    );
    let read_second = {
        let data = data.clone();
        tokio::spawn(async move { data.read(second, 8, 8).await })
    };
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("a disjoint read must enter transport while the first read is paused"),
        Some(8)
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), data.attr(first.clone()))
            .await
            .expect("fstat must progress while read transport is paused")
            .unwrap()
            .size,
        24
    );

    lower.resume[1].add_permits(1);
    assert_eq!(
        read_second.await.unwrap().unwrap(),
        test_lower_bytes(2, 8, 8)
    );

    // Only the cancelled caller's owned read now holds the inode barrier.
    read_first.abort();
    assert!(read_first.await.unwrap_err().is_cancelled());
    let mut truncate = tokio::spawn(async move { data.truncate(first, 0).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut truncate)
            .await
            .is_err()
    );
    lower.resume[0].add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), truncate)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .size,
        0
    );
}

async fn attach<B: EntityBackend + 'static>(
    backend: Arc<B>,
    keys: EntityKeys,
    ttl_ns: i64,
) -> Fixture<B> {
    attach_objects(
        backend,
        keys,
        ttl_ns,
        Arc::new(Objects::new(test_objects::backend().await)),
        8,
    )
    .await
}

async fn attach_objects<B: EntityBackend + 'static, O: ObjectBackend + 'static>(
    backend: Arc<B>,
    keys: EntityKeys,
    ttl_ns: i64,
    objects: Arc<O>,
    chunk_size: u64,
) -> Fixture<B, O> {
    let mut lower = MemoryLower::empty();
    lower.add_file(1, b"base", 2, 24);
    let lower = Arc::new(lower);
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    lifecycle.provision(lower.binding()).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), ttl_ns).await.unwrap();
    let namespace = EntityNamespace::new(backend.clone(), lower, keys.clone(), session.clone());
    let data = EntityData::new(namespace.clone(), objects.clone(), chunk_size)
        .await
        .unwrap();
    Fixture {
        backend,
        keys,
        session,
        namespace,
        data,
        objects,
    }
}

async fn fixture() -> Fixture<MemoryEntityBackend> {
    attach(
        Arc::new(MemoryEntityBackend::new(1000)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        1_000_000,
    )
    .await
}

async fn fragmented_small_write_fixture(
    chunk_size: u64,
) -> (Fixture<MemoryEntityBackend>, Arc<EntityHandle>, Vec<u8>) {
    let fixture = attach_objects(
        Arc::new(MemoryEntityBackend::new(1000)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        1_000_000,
        Arc::new(Objects::new(test_objects::backend().await)),
        chunk_size,
    )
    .await;
    let handle = new_file(&fixture, b"many-small-writes").await;
    let expected: Vec<u8> = (0..1500).map(|i| (i % 251) as u8).collect();
    for (offset, byte) in expected.iter().enumerate() {
        assert_eq!(
            fixture
                .data
                .write(
                    handle.clone(),
                    offset as u64,
                    Bytes::copy_from_slice(&[*byte]),
                    false
                )
                .await
                .unwrap()
                .written,
            1
        );
    }
    (fixture, handle, expected)
}

#[tokio::test]
#[ignore = "requires isolated RustFS via the entity data test harness"]
async fn fragmented_small_writes_remain_readable_in_large_requests() {
    for chunk_size in [4 * 1024 * 1024, 1024] {
        let (fixture, handle, expected) = fragmented_small_write_fixture(chunk_size).await;
        // The same successfully published file supports both small reads and
        // an ordinary request spanning every independently owned object.
        for offset in [0, 749, 1499] {
            assert_eq!(
                fixture.data.read(handle.clone(), offset, 1).await.unwrap(),
                vec![expected[offset as usize]]
            );
        }
        assert_eq!(
            fixture
                .data
                .read(handle.clone(), 0, expected.len())
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            fixture.data.read(handle, 500, 4096).await.unwrap(),
            expected[500..]
        );
    }
}

#[tokio::test]
#[ignore = "requires isolated RustFS via the entity data test harness"]
async fn fragmented_small_write_read_is_fenced_between_bounded_groups() {
    let (fixture, handle, expected) = fragmented_small_write_fixture(4 * 1024 * 1024).await;
    // The first half fits the metadata bound; stop the first object fetch in
    // the next group, after the first group's bytes have already been read.
    fixture.objects.pause_get.store(751, Ordering::SeqCst);
    let data = fixture.data.clone();
    let mut reading = tokio::spawn(async move { data.read(handle, 0, expected.len()).await });
    tokio::select! {
        result = &mut reading => panic!("fragmented read failed before reaching its second group: {result:?}"),
        entered = tokio::time::timeout(Duration::from_secs(60), fixture.objects.entered.notified()) => {
            entered.expect("fragmented read did not reach its second group");
        }
    }
    fixture.backend.set_time(2_000_000).unwrap();
    fixture.objects.resume.notify_one();
    assert_eq!(reading.await.unwrap(), Err(EntityError::Fenced));
}

#[tokio::test]
#[ignore = "requires isolated RustFS via the entity data test harness"]
async fn fragmented_small_write_read_retries_changed_version_between_groups() {
    let (fixture, handle, expected) = fragmented_small_write_fixture(4 * 1024 * 1024).await;
    let inode = fixture.data.attr(handle.clone()).await.unwrap();
    let cut = prepare_resize(
        &fixture.keys,
        inode.ino,
        Some(codec::encode(&UpperInode::Present(inode.clone())).unwrap()),
        &inode,
        750,
        fixture.data.chunk_size,
        1000,
    )
    .unwrap();
    fixture.objects.pause_get.store(751, Ordering::SeqCst);
    let data = fixture.data.clone();
    let mut reading = tokio::spawn(async move { data.read(handle, 0, 1500).await });
    tokio::select! {
        result = &mut reading => panic!("fragmented read failed before reaching its second group: {result:?}"),
        entered = tokio::time::timeout(Duration::from_secs(60), fixture.objects.entered.notified()) => {
            entered.expect("fragmented read did not reach its second group");
        }
    }
    // Inject a committed remote cut after one group's bytes were collected.
    // The reader must discard that view and retry against the new file size.
    let authority = fixture.namespace.authority().await.unwrap();
    assert_eq!(
        fixture.backend.commit(&authority, &cut).await.unwrap(),
        CommitOutcome::Committed
    );
    fixture.objects.resume.notify_one();
    assert_eq!(reading.await.unwrap().unwrap(), expected[..750]);
}

async fn new_file<B: EntityBackend + 'static, O: ObjectBackend + 'static>(
    fixture: &Fixture<B, O>,
    name: &[u8],
) -> Arc<EntityHandle> {
    fixture
        .namespace
        .create(
            CreateRequest {
                parent: 1,
                name: name.to_vec(),
                kind: EntityKind::File,
                mode: 0o666,
                rdev: 0,
                symlink_target: None,
            },
            root(),
        )
        .await
        .unwrap();
    Arc::new(fixture.namespace.open(1, name, 6, &root()).await.unwrap())
}

#[tokio::test]
async fn readdirplus_reuses_attributes_captured_by_opendir() {
    use asyncfuse::raw::{Filesystem, Request};
    let fixture = attach_objects(
        Arc::new(MeteredBackend::new(MemoryEntityBackend::new(1000))),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        1_000_000,
        Arc::new(NoObjectIo),
        8,
    )
    .await;
    for n in 0..8 {
        new_file(&fixture, format!("child-{n}").as_bytes()).await;
    }
    let fuse =
        crate::fuse::entity::EntityFuse::new(fixture.namespace.clone(), fixture.data.clone(), 1)
            .unwrap();
    let opened = fuse.opendir(Request::default(), 1, 0).await.unwrap();
    fixture.backend.take_counts();
    let entries: Vec<_> = fuse
        .readdirplus(Request::default(), 1, opened.fh, 0, 0)
        .await
        .unwrap()
        .entries
        .collect()
        .await;
    assert_eq!(entries.len(), 11); // dot, dot-dot, lower file, eight upper files
    for entry in entries {
        let entry = entry.unwrap();
        assert_eq!(entry.attr.ino, entry.inode);
        assert_eq!(entry.attr.kind, entry.kind);
        if entry.name == "base" {
            assert_eq!(entry.attr.size, 24);
        }
    }
    // One fresh directory access check; attributes were already batched at open.
    assert_eq!(fixture.backend.take_counts(), (1, 0, 0, 0));
}

#[tokio::test]
async fn readdirplus_captures_actual_parent_attributes() {
    use asyncfuse::raw::{Filesystem, Request};
    let fixture = attach_objects(
        Arc::new(MeteredBackend::new(MemoryEntityBackend::new(1000))),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        1_000_000,
        Arc::new(NoObjectIo),
        8,
    )
    .await;
    let fuse =
        crate::fuse::entity::EntityFuse::new(fixture.namespace.clone(), fixture.data.clone(), 1)
            .unwrap();
    let parent = fuse
        .mkdir(
            Request::default(),
            1,
            std::ffi::OsStr::new("parent"),
            0o750,
            0,
        )
        .await
        .unwrap()
        .attr;
    let child = fuse
        .mkdir(
            Request::default(),
            parent.ino,
            std::ffi::OsStr::new("child"),
            0o711,
            0,
        )
        .await
        .unwrap()
        .attr;
    let opened = fuse
        .opendir(Request::default(), child.ino, 0)
        .await
        .unwrap();
    fixture.backend.take_counts();
    let entries: Vec<_> = fuse
        .readdirplus(Request::default(), child.ino, opened.fh, 0, 0)
        .await
        .unwrap()
        .entries
        .collect()
        .await;
    assert_eq!(entries.len(), 2);
    let dot = entries[0].as_ref().unwrap();
    let parent_entry = entries[1].as_ref().unwrap();
    assert_eq!(dot.attr.ino, child.ino);
    assert_eq!(dot.attr.perm, child.perm);
    assert_eq!(parent_entry.attr.ino, parent.ino);
    assert_eq!(parent_entry.attr.perm, parent.perm);
    assert_eq!(parent_entry.attr.nlink, 3);
    assert_eq!(fixture.backend.take_counts(), (1, 0, 0, 0));
}

#[tokio::test]
async fn clean_fuse_release_avoids_metadata_barrier_but_explicit_fsync_keeps_it() {
    use asyncfuse::raw::{Filesystem, Request};
    let fixture = attach_objects(
        Arc::new(MeteredBackend::new(MemoryEntityBackend::new(1000))),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        1_000_000,
        Arc::new(NoObjectIo),
        8,
    )
    .await;
    let fuse =
        crate::fuse::entity::EntityFuse::new(fixture.namespace.clone(), fixture.data.clone(), 1)
            .unwrap();
    for flags in [libc::O_RDONLY as u32, libc::O_RDWR as u32] {
        let opened = fuse.open(Request::default(), 3, flags).await.unwrap();
        fixture.backend.take_counts();
        fuse.fsync(Request::default(), 3, opened.fh, false)
            .await
            .unwrap();
        assert_eq!(fixture.backend.take_counts(), (1, 1, 0, 0));
        fuse.release(Request::default(), 3, opened.fh, flags, 0, true)
            .await
            .unwrap();
        assert_eq!(fixture.backend.take_counts(), (0, 0, 0, 0));
    }
    let opened = fuse
        .open(Request::default(), 3, libc::O_RDWR as u32)
        .await
        .unwrap();
    fuse.setattr(
        Request::default(),
        3,
        Some(opened.fh),
        asyncfuse::SetAttr {
            size: Some(40),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    fixture.backend.take_counts();
    fuse.release(
        Request::default(),
        3,
        opened.fh,
        libc::O_RDWR as u32,
        0,
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        fixture.backend.take_counts(),
        (1, 1, 0, 0),
        "truncate keeps the release barrier"
    );

    let opened = fuse.open(Request::default(), 3, 0).await.unwrap();
    fixture
        .session
        .remember_io_failure(2, "injected earlier uncertain write".into());
    assert_eq!(
        fuse.release(Request::default(), 3, opened.fh, 0, 0, true)
            .await
            .unwrap_err(),
        asyncfuse::Errno::from(libc::EIO),
        "even a clean handle must report the inode's sticky error"
    );
}

async fn exercise_copy_up<B: EntityBackend + 'static>(fixture: &Fixture<B>) {
    let handle = Arc::new(
        fixture
            .namespace
            .open(1, b"base", 6, &root())
            .await
            .unwrap(),
    );
    let file = &fixture.data;
    let mut expected = test_lower_bytes(2, 0, 24);
    assert_eq!(file.read(handle.clone(), 0, 40).await.unwrap(), expected);
    let written = file
        .write(handle.clone(), 6, Bytes::from_static(b"WXYZ"), false)
        .await
        .unwrap();
    assert_eq!(
        written,
        WriteOutcome {
            offset: 6,
            written: 4,
            size: 24
        }
    );
    expected[6..10].copy_from_slice(b"WXYZ");
    assert_eq!(file.read(handle.clone(), 0, 40).await.unwrap(), expected);
    file.write(handle.clone(), 30, Bytes::from_static(b"tail"), false)
        .await
        .unwrap();
    expected.resize(30, 0);
    expected.extend_from_slice(b"tail");
    assert_eq!(file.read(handle.clone(), 0, 40).await.unwrap(), expected);
    // The boundary chunk is a mixture of LowerRange and Data. Shrink/extend
    // must remove both kinds beyond the cut, including already present maps.
    file.truncate(handle.clone(), 7).await.unwrap();
    file.truncate(handle.clone(), 34).await.unwrap();
    expected.truncate(7);
    expected.resize(34, 0);
    assert_eq!(file.read(handle.clone(), 0, 40).await.unwrap(), expected);
    assert_eq!(
        file.attr(handle.clone()).await.unwrap().lower_visible_size,
        7
    );
    file.write(handle.clone(), 9, Bytes::from_static(b"new"), false)
        .await
        .unwrap();
    expected[9..12].copy_from_slice(b"new");
    assert_eq!(file.read(handle.clone(), 0, 40).await.unwrap(), expected);
    file.fsync(handle).await.unwrap();
    assert!(
        fixture
            .backend
            .read(&[fixture.keys.truncate(2)])
            .await
            .unwrap()
            .rows[0]
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn durable_create_only_put_accepts_identical_retry_and_rejects_replacement() {
    let objects = test_objects::backend().await;
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let key = keys.object(1);
    objects
        .put_object_create_only_durable(&key, b"original")
        .await
        .unwrap();
    objects
        .put_object_create_only_durable(&key, b"original")
        .await
        .unwrap();
    assert!(
        objects
            .put_object_create_only_durable(&key, b"replaced")
            .await
            .is_err()
    );
    assert_eq!(
        objects.get_object(&key).await.unwrap().unwrap(),
        b"original"
    );
    objects.delete_object(&key).await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn copy_up_overwrite_sparse_extend_and_shrink_never_resurrect_data() {
    exercise_copy_up(&fixture().await).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn concurrent_append_assigns_disjoint_offsets_and_preserves_every_record() {
    let fixture = fixture().await;
    let handle = new_file(&fixture, b"append").await;
    let mut tasks = Vec::new();
    for byte in 0..32_u8 {
        let data = fixture.data.clone();
        let handle = handle.clone();
        tasks.push(tokio::spawn(async move {
            (
                byte,
                data.write(handle, 0, Bytes::from(vec![byte; 4]), true)
                    .await
                    .unwrap(),
            )
        }));
    }
    let mut expected = vec![0; 128];
    let mut offsets = std::collections::BTreeSet::new();
    for task in tasks {
        let (byte, result) = task.await.unwrap();
        assert_eq!(result.written, 4);
        assert!(offsets.insert(result.offset));
        expected[result.offset as usize..result.offset as usize + 4].fill(byte);
    }
    assert_eq!(
        offsets.into_iter().collect::<Vec<_>>(),
        (0..32).map(|n| n * 4).collect::<Vec<_>>()
    );
    fixture.data.fsync(handle.clone()).await.unwrap();
    assert_eq!(fixture.data.read(handle, 0, 128).await.unwrap(), expected);
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn paused_upload_is_invisible_and_cancellation_cannot_release_file_barrier() {
    let fixture = fixture().await;
    let handle = new_file(&fixture, b"waiting").await;
    let other = new_file(&fixture, b"independent").await;
    fixture.objects.pause_put.store(1, Ordering::SeqCst);
    let data = fixture.data.clone();
    let writer_handle = handle.clone();
    let caller = tokio::spawn(async move {
        data.write(writer_handle, 0, Bytes::from_static(b"waiting"), false)
            .await
    });
    fixture.objects.entered().await;
    let snapshot = fixture
        .backend
        .read(&[
            fixture.keys.inode(handle.ino()),
            fixture.keys.chunk(handle.ino(), 0),
        ])
        .await
        .unwrap();
    let UpperInode::Present(inode) = codec::decode(snapshot.rows[0].as_deref().unwrap()).unwrap()
    else {
        panic!("missing inode")
    };
    assert_eq!(inode.size, 0);
    assert!(snapshot.rows[1].is_none());
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let data = fixture.data.clone();
    let barrier_handle = handle.clone();
    let barrier = tokio::spawn(async move { data.fsync(barrier_handle).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        fixture
            .data
            .write(other, 0, Bytes::from_static(b"other"), false),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        !barrier.is_finished(),
        "fsync overtook a still-running accepted write"
    );
    fixture.objects.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), barrier)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(fixture.data.read(handle, 0, 8).await.unwrap(), b"waiting");
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn short_write_reports_confirmed_prefix_and_later_fsync_reports_upload_failure() {
    let fixture = fixture().await;
    let handle = new_file(&fixture, b"short").await;
    fixture.objects.fail_put.store(2, Ordering::SeqCst);
    let result = fixture
        .data
        .write(
            handle.clone(),
            0,
            Bytes::from_static(b"abcdefghijkl"),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        result,
        WriteOutcome {
            offset: 0,
            written: 8,
            size: 8
        }
    );
    assert_eq!(
        fixture.data.read(handle.clone(), 0, 16).await.unwrap(),
        b"abcdefgh"
    );
    assert!(matches!(
        fixture.data.fsync(handle.clone()).await,
        Err(EntityError::Backend(_))
    ));
    // A second adapter must not turn a short/uncertain write into clean fsync.
    let second = EntityData::new(fixture.namespace.clone(), fixture.objects.clone(), 8)
        .await
        .unwrap();
    assert!(matches!(
        second.fsync(handle).await,
        Err(EntityError::Backend(_))
    ));
    assert_eq!(fixture.objects.puts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn canceled_failed_write_keeps_error_for_fsync() {
    let fixture = fixture().await;
    let handle = new_file(&fixture, b"canceled").await;
    fixture.objects.pause_put.store(1, Ordering::SeqCst);
    fixture.objects.fail_put.store(1, Ordering::SeqCst);
    let data = fixture.data.clone();
    let writer_handle = handle.clone();
    let caller = tokio::spawn(async move {
        data.write(writer_handle, 0, Bytes::from_static(b"failed"), false)
            .await
    });
    fixture.objects.entered().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    fixture.objects.resume.notify_one();
    assert!(matches!(
        fixture.data.fsync(handle.clone()).await,
        Err(EntityError::Backend(_))
    ));
    assert!(fixture.data.read(handle, 0, 8).await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn upper_reads_reject_missing_corrupted_and_oversized_objects() {
    let fixture = fixture().await;
    let handle = new_file(&fixture, b"authenticated").await;
    fixture
        .data
        .write(handle.clone(), 0, Bytes::from_static(b"payload"), false)
        .await
        .unwrap();
    let snapshot = fixture
        .backend
        .read(&[fixture.keys.chunk(handle.ino(), 0)])
        .await
        .unwrap();
    let map: ChunkMap = codec::decode(snapshot.rows[0].as_deref().unwrap()).unwrap();
    let slice = *map.upper_slices().unwrap().first().unwrap();
    let key = fixture.keys.object(slice);
    assert!(key.starts_with("workspace-entity-v1/"));
    assert!(!key.starts_with("chunks/"));
    for damaged in [b"PAYLOAD".as_slice(), b"payload-extra"] {
        fixture
            .objects
            .inner
            .put_object(&key, damaged)
            .await
            .unwrap();
        assert!(matches!(
            fixture.data.read(handle.clone(), 0, 7).await,
            Err(EntityError::Corrupt(_))
        ));
    }
    fixture.objects.inner.delete_object(&key).await.unwrap();
    assert!(matches!(
        fixture.data.read(handle, 0, 7).await,
        Err(EntityError::Corrupt(_))
    ));
}

#[tokio::test]
async fn upper_object_read_uses_bounded_get_without_metadata_head() {
    let bytes = b"authenticated payload".to_vec();
    let objects = Arc::new(CountingRangeObjects {
        bytes: Some(bytes.clone()),
        heads: AtomicUsize::new(0),
        gets: AtomicUsize::new(0),
    });
    let fixture = attach_objects(
        Arc::new(MemoryEntityBackend::new(1000)),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        1_000_000,
        objects.clone(),
        8,
    )
    .await;
    let descriptor = ObjectDescriptor {
        namespace_id: fixture.keys.namespace_id(),
        slice_id: 1,
        size: bytes.len() as u64,
        sha256: Sha256::digest(&bytes).into(),
    };

    assert_eq!(fixture.data.read_object(&descriptor).await.unwrap(), bytes);
    assert_eq!(objects.heads.load(Ordering::SeqCst), 0);
    assert_eq!(objects.gets.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn upper_object_read_rejects_missing_short_corrupt_and_extended_payloads() {
    let expected = b"authenticated payload";
    for bytes in [
        None,
        Some(Vec::new()),
        Some(expected[..expected.len() - 1].to_vec()),
        Some(vec![b'x'; expected.len()]),
        Some([expected.as_slice(), b"extra"].concat()),
    ] {
        let objects = Arc::new(CountingRangeObjects {
            bytes,
            heads: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
        });
        let fixture = attach_objects(
            Arc::new(MemoryEntityBackend::new(1000)),
            EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
            1_000_000,
            objects,
            8,
        )
        .await;
        let descriptor = ObjectDescriptor {
            namespace_id: fixture.keys.namespace_id(),
            slice_id: 1,
            size: expected.len() as u64,
            sha256: Sha256::digest(expected).into(),
        };
        assert!(matches!(
            fixture.data.read_object(&descriptor).await,
            Err(EntityError::Corrupt(_))
        ));
    }
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn unlinked_open_file_retains_data_and_readonly_handles_cannot_mutate() {
    let fixture = fixture().await;
    let handle = new_file(&fixture, b"live").await;
    let readonly = Arc::new(
        fixture
            .namespace
            .open(1, b"live", 4, &root())
            .await
            .unwrap(),
    );
    assert_eq!(
        fixture
            .data
            .write(readonly.clone(), 0, Bytes::from_static(b"no"), false)
            .await
            .unwrap_err(),
        EntityError::Permission
    );
    assert_eq!(
        fixture.data.truncate(readonly, 0).await.unwrap_err(),
        EntityError::Permission
    );
    fixture
        .namespace
        .unlink(1, b"live".to_vec(), false, root())
        .await
        .unwrap();
    fixture
        .data
        .write(handle.clone(), 0, Bytes::from_static(b"alive"), false)
        .await
        .unwrap();
    fixture.data.fsync(handle.clone()).await.unwrap();
    assert_eq!(
        fixture.data.read(handle.clone(), 0, 8).await.unwrap(),
        b"alive"
    );
    assert_eq!(fixture.data.attr(handle.clone()).await.unwrap().nlink, 0);
    assert!(
        fixture
            .backend
            .read(&[fixture.keys.orphan(handle.ino())])
            .await
            .unwrap()
            .rows[0]
            .is_some()
    );
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn read_only_fsync_and_reads_are_noatime_and_invalid_ranges_do_not_poison_file() {
    let fixture = fixture().await;
    let readonly = Arc::new(
        fixture
            .namespace
            .open(1, b"base", 4, &root())
            .await
            .unwrap(),
    );
    let before = fixture
        .backend
        .scan_prefix(fixture.keys.volume_prefix(), 4096)
        .await
        .unwrap();
    let attr = fixture.data.attr(readonly.clone()).await.unwrap();
    assert_eq!(
        fixture.data.read(readonly.clone(), 0, 24).await.unwrap(),
        test_lower_bytes(2, 0, 24)
    );
    fixture.data.fsync(readonly.clone()).await.unwrap();
    assert_eq!(fixture.data.attr(readonly).await.unwrap(), attr);
    assert_eq!(
        fixture
            .backend
            .scan_prefix(fixture.keys.volume_prefix(), 4096)
            .await
            .unwrap(),
        before
    );
    let writable = Arc::new(
        fixture
            .namespace
            .open(1, b"base", 6, &root())
            .await
            .unwrap(),
    );
    assert!(matches!(
        fixture
            .data
            .write(
                writable.clone(),
                u64::MAX,
                Bytes::from_static(b"invalid"),
                false
            )
            .await,
        Err(EntityError::Overflow)
    ));
    fixture.data.fsync(writable.clone()).await.unwrap();
    fixture
        .data
        .write(writable.clone(), 0, Bytes::from_static(b"ok"), false)
        .await
        .unwrap();
    fixture.data.fsync(writable).await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn discard_reserves_before_delete_and_reconciles_an_ambiguous_reply() {
    let fixture = fixture().await;
    let handle = new_file(&fixture, b"discarded").await;
    fixture
        .data
        .write(
            handle.clone(),
            0,
            Bytes::from_static(b"two objects!"),
            false,
        )
        .await
        .unwrap();
    fixture.data.fsync(handle.clone()).await.unwrap();
    drop(handle);
    let maintenance = Lifecycle::new(fixture.backend.clone(), fixture.keys.clone())
        .enter_maintenance(&fixture.session)
        .await
        .unwrap();
    fixture.objects.fail_delete.store(true, Ordering::SeqCst);
    let mut saw_error = false;
    for _ in 0..32 {
        match fixture.data.discard_step(&maintenance).await {
            Ok(false) => {}
            Ok(true) => panic!("discard completed before injected DELETE failure"),
            Err(EntityError::Backend(_)) => {
                saw_error = true;
                break;
            }
            Err(error) => panic!("unexpected discard error: {error}"),
        }
    }
    assert!(saw_error);
    assert_eq!(fixture.objects.deletes.load(Ordering::SeqCst), 1);
    assert!(
        fixture
            .backend
            .read(&[fixture.keys.lower_pin()])
            .await
            .unwrap()
            .rows[0]
            .is_some()
    );
    let mut complete = false;
    for _ in 0..32 {
        if fixture.data.discard_step(&maintenance).await.unwrap() {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert!(fixture.data.discard_step(&maintenance).await.unwrap());
    assert_eq!(fixture.objects.deletes.load(Ordering::SeqCst), 3);
    assert!(
        fixture
            .backend
            .read(&[fixture.keys.lower_pin()])
            .await
            .unwrap()
            .rows[0]
            .is_none()
    );
    for slice in [1, 2] {
        assert!(
            fixture
                .objects
                .get_object_size_bounded(&fixture.keys.object(slice))
                .await
                .unwrap()
                .is_none()
        );
        let row = fixture
            .backend
            .read(&[fixture.keys.slice(slice)])
            .await
            .unwrap();
        let owner: SliceOwnership = codec::decode(row.rows[0].as_deref().unwrap()).unwrap();
        assert!(matches!(
            owner.state,
            super::super::ownership::SliceState::Deleted { .. }
        ));
    }
}

#[tokio::test]
async fn chunk_geometry_is_fixed_and_expired_empty_io_is_fenced() {
    let fixture = attach_objects(
        Arc::new(MeteredBackend::new(MemoryEntityBackend::new(1000))),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        1_000_000,
        Arc::new(NoObjectIo),
        8,
    )
    .await;
    assert!(matches!(
        EntityData::new(fixture.namespace.clone(), fixture.objects.clone(), 16).await,
        Err(EntityError::Invalid(_))
    ));
    let handle = new_file(&fixture, b"empty").await;
    fixture.backend.take_counts();
    assert_eq!(fixture.data.attr(handle.clone()).await.unwrap().size, 0);
    assert_eq!(fixture.backend.take_counts(), (1, 0, 0, 0));
    assert!(
        fixture
            .data
            .read(handle.clone(), 0, 8)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(fixture.backend.take_counts(), (1, 0, 0, 0));
    fixture.backend.inner.set_time(2_000_000).unwrap();
    assert_eq!(
        fixture.data.attr(handle.clone()).await.unwrap_err(),
        EntityError::Fenced
    );
    assert_eq!(
        fixture.data.read(handle.clone(), 0, 0).await.unwrap_err(),
        EntityError::Fenced
    );
    assert_eq!(
        fixture
            .data
            .write(handle.clone(), 0, Bytes::new(), false)
            .await
            .unwrap_err(),
        EntityError::Fenced
    );
    assert!(fixture.data.fsync(handle).await.is_err());
}

/// Force a real authority-row conflict after allocation reads its lease. The
/// underlying allocator, rather than this wrapper, rejects the stale authority.
struct RenewDuringAllocationBackend {
    inner: MemoryEntityBackend,
    keys: EntityKeys,
    renew_allocations: AtomicUsize,
    allocations: AtomicUsize,
}

impl RenewDuringAllocationBackend {
    fn new(keys: EntityKeys) -> Self {
        Self {
            inner: MemoryEntityBackend::new(1000),
            keys,
            renew_allocations: AtomicUsize::new(0),
            allocations: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl EntityBackend for RenewDuringAllocationBackend {
    async fn read(&self, keys: &[Vec<u8>]) -> Result<RowSnapshot> {
        self.inner.read(keys).await
    }

    async fn commit_with_inode_allocation(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
        create: &CreateAllocation,
    ) -> Result<Option<i64>> {
        self.inner
            .commit_with_inode_allocation(authority, plan, create)
            .await
    }

    async fn reserve_slices(
        &self,
        authority: &TransactionAuthority,
        allocation: &SliceAllocation,
    ) -> Result<Option<Vec<u64>>> {
        {
            self.allocations.fetch_add(1, Ordering::SeqCst);
            if self
                .renew_allocations
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                let lease_key = self.keys.lease();
                let snapshot = self.inner.read(std::slice::from_ref(&lease_key)).await?;
                let previous = snapshot.rows[0].clone().unwrap();
                let mut lease: WriterLease = codec::decode(&previous)?;
                lease.expires_ns += 1_000_000;
                assert_eq!(
                    self.inner
                        .commit(
                            authority,
                            &EntityPlan {
                                checks: vec![RowCheck {
                                    key: lease_key.clone(),
                                    expected: Some(previous),
                                }],
                                writes: vec![RowWrite::Put {
                                    key: lease_key,
                                    value: codec::encode(&lease)?,
                                }],
                            },
                        )
                        .await?,
                    CommitOutcome::Committed
                );
            }
        }
        self.inner.reserve_slices(authority, allocation).await
    }

    async fn scan_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        self.inner.scan_keys_page(prefix, after, limit).await
    }

    async fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix(prefix, limit).await
    }
    async fn scan_prefix_batch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix_batch(prefix, limit).await
    }

    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        self.inner.commit(authority, plan).await
    }
}

#[tokio::test]
async fn reservation_contention_is_bounded_and_does_not_poison_fsync() {
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let backend = Arc::new(RenewDuringAllocationBackend::new(keys.clone()));
    let fixture = attach_objects(backend.clone(), keys, 1_000_000, Arc::new(NoObjectIo), 8).await;
    let handle = new_file(&fixture, b"contention").await;
    backend
        .renew_allocations
        .store(MAX_RETRIES, Ordering::SeqCst);
    assert_eq!(
        fixture
            .data
            .write(handle.clone(), 0, Bytes::from_static(b"once"), false)
            .await
            .unwrap_err(),
        EntityError::Busy
    );
    assert_eq!(backend.allocations.load(Ordering::SeqCst), MAX_RETRIES);
    assert_eq!(fixture.data.attr(handle.clone()).await.unwrap().size, 0);
    // NoObjectIo would panic if a failed reservation reached any object call.
    fixture.data.fsync(handle.clone()).await.unwrap();
    backend.inner.set_time(100_000_000).unwrap();
    assert_eq!(
        fixture.data.fsync(handle).await.unwrap_err(),
        EntityError::Fenced
    );
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn reservation_renews_authority_before_retrying_known_allocation_conflict() {
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let backend = Arc::new(RenewDuringAllocationBackend::new(keys.clone()));
    let fixture = attach(backend.clone(), keys, 1_000_000).await;
    let handle = new_file(&fixture, b"renewed").await;
    backend.renew_allocations.store(1, Ordering::SeqCst);
    let written = fixture
        .data
        .write(handle.clone(), 0, Bytes::from_static(b"once"), false)
        .await
        .unwrap();
    assert_eq!(written.written, 4);
    assert_eq!(backend.allocations.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.objects.puts.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.data.read(handle.clone(), 0, 8).await.unwrap(),
        b"once"
    );
    fixture.data.fsync(handle).await.unwrap();
}

struct LostReplyBackend {
    inner: MemoryEntityBackend,
    lose: AtomicBool,
    publications: AtomicUsize,
}

#[async_trait]
impl EntityBackend for LostReplyBackend {
    async fn read(&self, keys: &[Vec<u8>]) -> Result<RowSnapshot> {
        self.inner.read(keys).await
    }
    async fn commit_with_inode_allocation(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
        create: &CreateAllocation,
    ) -> Result<Option<i64>> {
        // This adapter only loses current-chunk publication replies; creating
        // an empty inode must not consume that independently armed fault.
        self.inner
            .commit_with_inode_allocation(authority, plan, create)
            .await
    }
    async fn reserve_slices(
        &self,
        authority: &TransactionAuthority,
        allocation: &SliceAllocation,
    ) -> Result<Option<Vec<u64>>> {
        self.inner.reserve_slices(authority, allocation).await
    }
    async fn scan_keys_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>> {
        self.inner.scan_keys_page(prefix, after, limit).await
    }

    async fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix(prefix, limit).await
    }
    async fn scan_prefix_batch(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_prefix_batch(prefix, limit).await
    }
    async fn commit(
        &self,
        authority: &TransactionAuthority,
        plan: &EntityPlan,
    ) -> Result<CommitOutcome> {
        let result = self.inner.commit(authority, plan).await?;
        if result == CommitOutcome::Committed
            && plan
                .writes
                .iter()
                .any(|write| write.key().windows(7).any(|part| part == b"/chunk/"))
        {
            self.publications.fetch_add(1, Ordering::SeqCst);
            if self.lose.swap(false, Ordering::SeqCst) {
                return Err(EntityError::Backend("injected lost committed reply".into()));
            }
        }
        Ok(result)
    }
}

#[tokio::test]
#[ignore = "requires isolated RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn uncertain_file_publication_is_not_replayed_or_hidden_by_fsync() {
    let backend = Arc::new(LostReplyBackend {
        inner: MemoryEntityBackend::new(1000),
        lose: AtomicBool::new(false),
        publications: AtomicUsize::new(0),
    });
    let fixture = attach(
        backend.clone(),
        EntityKeys::new(Uuid::new_v4(), Uuid::new_v4()),
        1_000_000,
    )
    .await;
    let handle = new_file(&fixture, b"uncertain").await;
    backend.lose.store(true, Ordering::SeqCst);
    assert!(matches!(
        fixture
            .data
            .write(handle.clone(), 0, Bytes::from_static(b"once"), false)
            .await,
        Err(EntityError::Backend(_))
    ));
    assert_eq!(backend.publications.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.objects.puts.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.data.read(handle.clone(), 0, 8).await.unwrap(),
        b"once"
    );
    assert!(matches!(
        fixture.data.fsync(handle).await,
        Err(EntityError::Backend(_))
    ));
}

#[tokio::test]
#[ignore = "requires isolated Redis via BREWFS_TEST_REDIS_URL and RustFS via BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT/BUCKET and AWS credentials"]
async fn redis_entity_data_copy_up_durability_and_truncate_contract() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let volume = Uuid::new_v4();
    let keys = EntityKeys::new(volume, Uuid::new_v4());
    let backend = Arc::new(RedisEntityBackend::connect(&url, volume).await.unwrap());
    let outcome = AssertUnwindSafe(async {
        let fixture = attach(backend.clone(), keys.clone(), 60_000_000_000).await;
        exercise_copy_up(&fixture).await;
        let handle = new_file(&fixture, b"upper").await;
        fixture
            .data
            .write(handle.clone(), 0, Bytes::from_static(b"durable"), false)
            .await
            .unwrap();
        fixture.data.fsync(handle.clone()).await.unwrap();
        let reattached = EntityData::new(fixture.namespace.clone(), fixture.objects.clone(), 8)
            .await
            .unwrap();
        assert_eq!(reattached.read(handle, 0, 8).await.unwrap(), b"durable");
        assert!(!fixture.session.is_closed());
    })
    .catch_unwind()
    .await;
    let rows = backend
        .scan_prefix(keys.volume_prefix(), 4096)
        .await
        .unwrap();
    if let Some((key, value)) = rows.first() {
        let authority = TransactionAuthority::from_validated(
            vec![RowCheck {
                key: key.clone(),
                expected: Some(value.clone()),
            }],
            None,
        )
        .unwrap();
        let plan = EntityPlan {
            checks: rows
                .iter()
                .map(|(key, value)| RowCheck {
                    key: key.clone(),
                    expected: Some(value.clone()),
                })
                .collect(),
            writes: rows
                .into_iter()
                .map(|(key, _)| RowWrite::Delete { key })
                .collect(),
        };
        assert_eq!(
            backend.commit(&authority, &plan).await.unwrap(),
            CommitOutcome::Committed
        );
    }
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
