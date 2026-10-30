//! Local contract tests use a real temporary filesystem object repository;
//! the entity data harness separately runs the production S3 adapter.

use super::*;
use crate::workspace_overlay::entity::local_test_objects::DurableLocalObjects as LocalFsBackend;

struct BlockedObjects {
    inner: LocalFsBackend,
    release: tokio::sync::Semaphore,
    started: AtomicUsize,
}

#[async_trait]
impl ObjectBackend for BlockedObjects {
    fn forbids_mutation_replay(&self) -> bool {
        true
    }
    async fn put_object(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object(key, bytes).await
    }
    async fn put_object_create_only_durable(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.release.acquire().await?.forget();
        self.inner.put_object_create_only_durable(key, bytes).await
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        output: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.inner.get_object_range(key, offset, output).await
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        self.inner
            .get_object_range_stream(key, offset, length)
            .await
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(key).await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete_object(key).await
    }
}

#[tokio::test]
async fn entity_commit_before_upload_publishes_while_remote_is_blocked() {
    let remote_directory = tempfile::tempdir().unwrap();
    let objects = Arc::new(BlockedObjects {
        inner: LocalFsBackend::new(remote_directory.path()),
        release: tokio::sync::Semaphore::new(0),
        started: AtomicUsize::new(0),
    });
    let spool = tempfile::tempdir().unwrap();
    let fixture = local_fixture(objects, spool.path(), 1 << 20).await;
    let handle = new_file(&fixture, b"asynchronous").await;
    // Regression baseline: both experimental lower variants currently use
    // EntityData's unconditional remote PUT before this metadata publication.
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        fixture
            .data
            .write(handle.clone(), 0, Bytes::from_static(b"local ack"), false),
    )
    .await
    .expect("commit_before_upload must publish after local staging while S3 is blocked")
    .unwrap();
    assert_eq!(result.written, 9);
    assert_eq!(fixture.data.attr(handle).await.unwrap().size, 9);
    assert_eq!(
        fixture.data.writeback_status().await.unwrap().pending_bytes,
        9
    );
    fixture.objects.release.add_permits(8);
    fixture
        .data
        .writeback
        .as_ref()
        .unwrap()
        .drain(Duration::from_secs(5))
        .await
        .unwrap();
}

#[tokio::test]
async fn local_writeback_overlaps_bounded_uploads_without_blocking_publication() {
    let remote = tempfile::tempdir().unwrap();
    let spool = tempfile::tempdir().unwrap();
    let objects = Arc::new(BlockedObjects {
        inner: LocalFsBackend::new(remote.path()),
        release: tokio::sync::Semaphore::new(0),
        started: AtomicUsize::new(0),
    });
    let fixture = local_fixture(objects.clone(), spool.path(), 1 << 20).await;
    // Blocked PUTs must not prevent new local receipts. A full discovery batch
    // must overlap up to eight PUTs once the initial batch completes.
    for index in 0..20 {
        let handle = new_file(&fixture, format!("parallel-{index}").as_bytes()).await;
        tokio::time::timeout(
            Duration::from_secs(2),
            fixture
                .data
                .write(handle, 0, Bytes::from_static(b"parallel"), false),
        )
        .await
        .unwrap()
        .unwrap();
        if index == 0 {
            // Fix the first discovery batch at one object before adding debt.
            // Otherwise a partially filled initial batch could remain blocked
            // after the single permit below, depending on task scheduling.
            tokio::time::timeout(Duration::from_secs(2), async {
                while objects.started.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
        }
    }
    // Complete the single-object discovery batch, then observe a full window.
    objects.release.add_permits(1);
    let overlapped = tokio::time::timeout(Duration::from_secs(2), async {
        while objects.started.load(Ordering::SeqCst) < 9 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    let in_flight = objects.started.load(Ordering::SeqCst) - 1;
    objects.release.add_permits(32);
    fixture
        .data
        .drain_remote(Duration::from_secs(10))
        .await
        .unwrap();
    assert!(overlapped, "independent immutable uploads must overlap");
    assert_eq!(
        in_flight, 8,
        "the uploader must bound retained payloads and PUTs"
    );
    assert_eq!(
        fixture
            .data
            .writeback_status()
            .await
            .unwrap()
            .pending_objects,
        0
    );
}

async fn local_fixture(
    objects: Arc<BlockedObjects>,
    path: &std::path::Path,
    capacity: u64,
) -> Fixture<MemoryEntityBackend, BlockedObjects> {
    let backend = Arc::new(MemoryEntityBackend::new(1000));
    let keys = EntityKeys::new(Uuid::new_v4(), Uuid::new_v4());
    let lower = Arc::new(MemoryLower::empty());
    let lifecycle = Lifecycle::new(backend.clone(), keys.clone());
    lifecycle.provision(lower.binding()).await.unwrap();
    let session = lifecycle.acquire(Uuid::new_v4(), 1_000_000).await.unwrap();
    let namespace = EntityNamespace::new(backend.clone(), lower, keys.clone(), session.clone());
    let data = EntityData::new_with_writeback(
        namespace.clone(),
        objects.clone(),
        8,
        Some(WritebackConfig {
            directory: path.into(),
            capacity_bytes: capacity,
            min_free_bytes: 0,
        }),
    )
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

#[tokio::test]
async fn local_writeback_overwrite_truncate_and_missing_remote_are_not_replayed() {
    let remote = tempfile::tempdir().unwrap();
    let spool = tempfile::tempdir().unwrap();
    let objects = Arc::new(BlockedObjects {
        inner: LocalFsBackend::new(remote.path()),
        release: tokio::sync::Semaphore::new(0),
        started: AtomicUsize::new(0),
    });
    let fixture = local_fixture(objects.clone(), spool.path(), 1 << 20).await;
    let handle = new_file(&fixture, b"mutated").await;
    fixture
        .data
        .write(handle.clone(), 0, Bytes::from_static(b"abcdefgh"), false)
        .await
        .unwrap();
    fixture.data.fsync(handle.clone()).await.unwrap();
    assert_eq!(
        fixture.data.read(handle.clone(), 0, 8).await.unwrap(),
        b"abcdefgh"
    );
    // An independent remote-only reader must report absence/authentication failure.
    let descriptor: ObjectDescriptor = codec::decode(
        fixture
            .backend
            .read(&[fixture.keys.object_descriptor(1)])
            .await
            .unwrap()
            .rows[0]
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    assert!(
        objects
            .inner
            .get_object(&fixture.keys.object(descriptor.slice_id))
            .await
            .unwrap()
            .is_none()
    );
    fixture
        .data
        .write(handle.clone(), 0, Bytes::from_static(b"XYZ"), false)
        .await
        .unwrap();
    fixture.data.truncate(handle.clone(), 5).await.unwrap();
    assert_eq!(
        fixture.data.read(handle.clone(), 0, 8).await.unwrap(),
        b"XYZde"
    );
    assert!(matches!(
        fixture
            .data
            .writeback
            .as_ref()
            .unwrap()
            .drain(Duration::from_millis(50))
            .await,
        Err(EntityError::Busy)
    ));
    objects.release.add_permits(8);
    fixture
        .data
        .writeback
        .as_ref()
        .unwrap()
        .drain(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        fixture.data.read(handle.clone(), 0, 8).await.unwrap(),
        b"XYZde"
    );
    drop(handle);
    fixture
        .data
        .drain_remote(Duration::from_secs(5))
        .await
        .unwrap();
    Lifecycle::new(fixture.backend, fixture.keys)
        .enter_maintenance(&fixture.session)
        .await
        .unwrap();
}

#[tokio::test]
async fn local_writeback_capacity_failure_does_not_publish() {
    let remote = tempfile::tempdir().unwrap();
    let spool = tempfile::tempdir().unwrap();
    let objects = Arc::new(BlockedObjects {
        inner: LocalFsBackend::new(remote.path()),
        release: tokio::sync::Semaphore::new(0),
        started: AtomicUsize::new(0),
    });
    let fixture = local_fixture(objects, spool.path(), 4096).await;
    let handle = new_file(&fixture, b"capacity").await;
    assert!(matches!(
        fixture
            .data
            .write(handle.clone(), 0, Bytes::from_static(b"full"), false)
            .await,
        Err(EntityError::Busy)
    ));
    assert_eq!(fixture.data.attr(handle).await.unwrap().size, 0);
    assert_eq!(
        fixture
            .data
            .writeback_status()
            .await
            .unwrap()
            .pending_objects,
        0
    );
}

#[tokio::test]
async fn local_writeback_restart_recovers_original_objects_with_expired_lease() {
    let remote = tempfile::tempdir().unwrap();
    let spool = tempfile::tempdir().unwrap();
    let objects = Arc::new(BlockedObjects {
        inner: LocalFsBackend::new(remote.path()),
        release: tokio::sync::Semaphore::new(0),
        started: AtomicUsize::new(0),
    });
    // Failed transport exits promptly so dropping this runtime releases its lock.
    objects.release.close();
    let fixture = local_fixture(objects, spool.path(), 1 << 20).await;
    let handle = new_file(&fixture, b"restart").await;
    fixture
        .data
        .write(handle.clone(), 0, Bytes::from_static(b"old-data"), false)
        .await
        .unwrap();
    fixture
        .data
        .write(handle.clone(), 0, Bytes::from_static(b"new"), false)
        .await
        .unwrap();
    fixture.data.truncate(handle.clone(), 3).await.unwrap();
    let ino = fixture.data.attr(handle.clone()).await.unwrap().ino;
    let before = fixture
        .backend
        .read(&[fixture.keys.inode(ino), fixture.keys.chunk(ino, 0)])
        .await
        .unwrap()
        .rows;
    let keys = fixture.keys.clone();
    let backend = fixture.backend.clone();
    assert!(matches!(
        EntityWriteback::open(
            backend.clone(),
            Arc::new(LocalFsBackend::new(remote.path())),
            keys.clone(),
            WritebackConfig {
                directory: spool.path().into(),
                capacity_bytes: 1 << 20,
                min_free_bytes: 0,
            },
        ),
        Err(EntityError::Busy)
    ));
    backend.set_time(2_000_000).unwrap();
    drop(handle);
    drop(fixture);
    tokio::time::sleep(Duration::from_millis(150)).await;
    // Model a successful remote PUT whose reply was lost: create-only recovery
    // must authenticate this existing object rather than blindly repeat PUT.
    LocalFsBackend::new(remote.path())
        .put_object_create_only_durable(&keys.object(1), b"old-data")
        .await
        .unwrap();
    let recovered = EntityWriteback::open(
        backend.clone(),
        Arc::new(LocalFsBackend::new(remote.path())),
        keys.clone(),
        WritebackConfig {
            directory: spool.path().into(),
            capacity_bytes: 1 << 20,
            min_free_bytes: 0,
        },
    )
    .unwrap();
    assert_eq!(recovered.status().await.unwrap().pending_objects, 2);
    recovered.upload_pass().await.unwrap();
    assert_eq!(recovered.status().await.unwrap().pending_objects, 0);
    assert_eq!(
        backend
            .read(&[keys.inode(ino), keys.chunk(ino, 0)])
            .await
            .unwrap()
            .rows,
        before
    );
    assert_eq!(
        LocalFsBackend::new(remote.path())
            .get_object(&keys.object(1))
            .await
            .unwrap()
            .unwrap(),
        b"old-data"
    );
    assert_eq!(
        LocalFsBackend::new(remote.path())
            .get_object(&keys.object(2))
            .await
            .unwrap()
            .unwrap(),
        b"new"
    );
}

#[tokio::test]
async fn local_writeback_corrupt_or_missing_stage_keeps_debt() {
    for missing in [false, true] {
        let remote = tempfile::tempdir().unwrap();
        let spool = tempfile::tempdir().unwrap();
        let objects = Arc::new(BlockedObjects {
            inner: LocalFsBackend::new(remote.path()),
            release: tokio::sync::Semaphore::new(0),
            started: AtomicUsize::new(0),
        });
        objects.release.close();
        let fixture = local_fixture(objects, spool.path(), 1 << 20).await;
        let handle = new_file(&fixture, b"damage").await;
        fixture
            .data
            .write(handle.clone(), 0, Bytes::from_static(b"original"), false)
            .await
            .unwrap();
        let path = spool
            .path()
            .join(format!(
                "{}-{}",
                fixture.keys.volume_id(),
                fixture.keys.namespace_id()
            ))
            .join("1.stage");
        if missing {
            std::fs::remove_file(path).unwrap();
        } else {
            std::fs::write(path, b"invalid staging header").unwrap();
        }
        assert!(fixture.data.read(handle.clone(), 0, 8).await.is_err());
        let queue = fixture.data.writeback.as_ref().unwrap();
        assert!(queue.upload_pass().await.is_err());
        assert_eq!(queue.status().await.unwrap().pending_objects, 1);
        drop(handle);
        assert!(matches!(
            Lifecycle::new(fixture.backend, fixture.keys)
                .enter_maintenance(&fixture.session)
                .await,
            Err(EntityError::Busy)
        ));
    }
}

#[tokio::test]
async fn local_writeback_unlink_before_upload_does_not_resurrect_name() {
    let remote = tempfile::tempdir().unwrap();
    let spool = tempfile::tempdir().unwrap();
    let objects = Arc::new(BlockedObjects {
        inner: LocalFsBackend::new(remote.path()),
        release: tokio::sync::Semaphore::new(0),
        started: AtomicUsize::new(0),
    });
    let fixture = local_fixture(objects.clone(), spool.path(), 1 << 20).await;
    let handle = new_file(&fixture, b"gone").await;
    fixture
        .data
        .write(handle.clone(), 0, Bytes::from_static(b"unlinked"), false)
        .await
        .unwrap();
    fixture
        .namespace
        .unlink(1, b"gone".to_vec(), false, root())
        .await
        .unwrap();
    assert_eq!(
        fixture.data.read(handle.clone(), 0, 8).await.unwrap(),
        b"unlinked"
    );
    objects.release.add_permits(4);
    fixture
        .data
        .writeback
        .as_ref()
        .unwrap()
        .drain(Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(
        fixture.namespace.open(1, b"gone", 4, &root()).await,
        Err(EntityError::NotFound)
    ));
    assert_eq!(fixture.data.read(handle, 0, 8).await.unwrap(), b"unlinked");
}
