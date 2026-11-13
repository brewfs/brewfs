//! Actual lower payload reads and capture files exercise write admission and
//! cancellation. These tests do not certify a filesystem physical quota.

use super::*;
use crate::cadapter::client::{ObjectBackend, ObjectByteStream};
use crate::cadapter::read_observer::{ReadClass, ReadContext, ReadObserver};
use crate::workspace::packed::PackedWireError;
use crate::workspace::packed::snapshot::SnapshotBudgetPool;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::atomic::AtomicU64;

#[derive(Default)]
struct PayloadReadProbe {
    reads: AtomicU64,
    hold: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    returned: AtomicBool,
}

struct ReleasePayloadRead(Arc<PayloadReadProbe>);
impl Drop for ReleasePayloadRead {
    fn drop(&mut self) {
        self.0.resume.notify_one();
    }
}

#[derive(Clone)]
struct HeldPayloadBackend {
    inner: LocalFsBackend,
    probe: Arc<PayloadReadProbe>,
}

#[async_trait]
impl ObjectBackend for HeldPayloadBackend {
    async fn put_object(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object(key, bytes).await
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        bytes: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.inner.get_object_range(key, offset, bytes).await
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
    async fn get_object_range_stream_observed(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<ObjectByteStream> {
        let payload = matches!(
            context.class,
            ReadClass::PackedPayload | ReadClass::ExternalPayload
        );
        if payload {
            self.probe.reads.fetch_add(1, Ordering::SeqCst);
            if self.probe.hold.swap(false, Ordering::SeqCst) {
                self.probe.entered.notify_one();
                self.probe.resume.notified().await;
            }
        }
        let result = self
            .inner
            .get_object_range_stream_observed(key, offset, length, context, observer)
            .await;
        if payload {
            self.probe.returned.store(true, Ordering::SeqCst);
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
        self.inner.delete_object(key).await
    }
}

async fn payload_fixture() -> (Fixture, Arc<PayloadReadProbe>) {
    let (directory, client, snapshot, lower_proof, payload) = packed().await;
    let backend = PinMemoryBackend::default();
    backend.now.store(1_000_000_000, Ordering::SeqCst);
    let budget = SnapshotMountBudget::defaults();
    let store = Arc::new(
        KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    let initial = request(store.as_ref(), lower_proof).await;
    let binding = store
        .install_packed_lower_binding(initial.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: binding.head_epoch,
        ..initial.guard
    };
    let reader = store
        .clone()
        .open_packed_reader_session(
            guard.clone(),
            budget.clone(),
            PackedReaderLeaseOptions::default(),
        )
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&reader.mount_budget(), &budget));
    let probe = Arc::new(PayloadReadProbe::default());
    let payload_probe = probe.clone();
    let lower = Arc::new(
        PackedReadonlyMeta::from_snapshot_budget(
            client.clone().map_backend(move |inner| HeldPayloadBackend {
                inner,
                probe: payload_probe,
            }),
            snapshot,
            CHUNK,
            0,
            budget.clone(),
        )
        .unwrap(),
    );
    // The first real lower plan lazily starts the mount-owned coordinator.
    // Its queue/registry Roots persist while this fixture retains the lower;
    // establish them before the capture-release baseline, without fetching
    // payload, submitting a frame, or arming the held-read probe.
    let roots_before_plan = budget.state().used[SnapshotBudgetPool::Roots as usize];
    drop(
        lower
            .prepare_unified_read(400, 0, 0, CHUNK)
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(probe.reads.load(Ordering::SeqCst), 0);
    assert!(budget.state().used[SnapshotBudgetPool::Roots as usize] > roots_before_plan);
    let upper = Arc::new(InMemoryBlockStore::new());
    let layout = ChunkLayout {
        chunk_size: CHUNK,
        block_size: CHUNK as u32,
    };
    let authority = Arc::new(PinnedCatalogPackedBindingAuthority {
        store: store.clone(),
        reader: reader.clone(),
    });
    let meta = Arc::new(
        WorkspaceMetaLayer::with_chunk_size(
            store.clone(),
            ViewContext {
                workspace_id: guard.workspace_id,
                head_layer_id: guard.expected_head_layer_id,
                head_epoch: guard.expected_head_epoch,
                lease_id: guard.lease_id,
                holder_generation: guard.holder_generation,
            },
            CHUNK,
        )
        .with_packed_lower(
            binding.binding.clone(),
            lower,
            authority,
            upper.clone(),
            layout,
        )
        .unwrap(),
    );
    meta.initialize().await.unwrap();
    let provider: Arc<dyn WorkspaceReadPlanProvider> = meta.clone();
    let vfs = VFS::from_readonly_components_with_provider(
        VFSConfig::new(layout),
        upper,
        meta.clone(),
        provider,
    )
    .unwrap();
    (
        Fixture {
            directory,
            client,
            store,
            backend,
            reader,
            budget,
            meta,
            vfs,
            guard,
            payload,
            binding,
        },
        probe,
    )
}

async fn begin_payload_capture(
    f: &Fixture,
) -> (
    Arc<
        crate::workspace::stores::kv_store::packed_native_freeze::PackedNativeQuiesceFence<
            PinMemoryBackend,
        >,
    >,
    crate::vfs::fs::PackedVfsDrainFence<InMemoryBlockStore, NativeMeta>,
) {
    let local = f.vfs.quiesce_packed_vfs().await.unwrap();
    let layers = f
        .store
        .load_layer_chain(f.guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let native = Arc::new(
        f.store
            .clone()
            .begin_packed_native_quiesce(
                f.guard.clone(),
                layers,
                JournalId::new(),
                LayerId::new(),
                f.budget.clone(),
            )
            .await
            .unwrap(),
    );
    (native, local)
}

fn held_payload_file(parent: &Path) -> File {
    let directories: Vec<_> = std::fs::read_dir(parent)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_dir())
        .collect();
    assert_eq!(directories.len(), 1, "exactly one owned capture directory");
    File::open(directories[0].join("inode-0000000000000190")).unwrap()
}

async fn assert_payload_scratch_released(
    parent: &Path,
    budget: &SnapshotMountBudget,
    baseline: [u64; 8],
) {
    let released = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let has_owned_directory = std::fs::read_dir(parent)
                .unwrap()
                .any(|entry| entry.unwrap().file_type().unwrap().is_dir());
            if !has_owned_directory && budget.state().used == baseline {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        released.is_ok(),
        "scratch release timeout: owned_directories={} baseline={baseline:?} actual={:?}",
        std::fs::read_dir(parent)
            .unwrap()
            .filter(|entry| entry.as_ref().unwrap().file_type().unwrap().is_dir())
            .count(),
        budget.state().used,
    );
    assert_eq!(
        std::fs::read(parent.join("sibling-evidence")).unwrap(),
        b"keep"
    );
    assert!(!budget.state().closed);
}

fn scratch() -> tempfile::TempDir {
    let parent = tempfile::tempdir().unwrap();
    std::fs::write(parent.path().join("sibling-evidence"), b"keep").unwrap();
    parent
}

#[tokio::test]
async fn native_payload_budget_rejects_low_quota_before_actual_payload_fetch() {
    let (f, probe) = payload_fixture().await;
    let (native, local) = begin_payload_capture(&f).await;
    let parent = scratch();
    let baseline = f.budget.state().used;
    let before = f.backend.rows.lock().await.clone();
    let mut bounded = limits();
    bounded.max_payload_disk_bytes = CHUNK - 1;
    let result = FrozenNativeArtifact::capture(
        native.clone(),
        local,
        parent.path().to_path_buf(),
        bounded,
        CancellationToken::new(),
    )
    .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("payload quota below first data span accepted"),
    };
    assert_payload_scratch_released(parent.path(), &f.budget, baseline).await;
    assert!(
        matches!(error, PackedWireError::LimitExceeded(_)),
        "{error}"
    );
    assert_eq!(
        probe.reads.load(Ordering::SeqCst),
        0,
        "quota rejected after actual payload fetch"
    );
    assert_eq!(*f.backend.rows.lock().await, before);
    native.validate().await.unwrap();
    drop(native);
    f.meta.shutdown_session().await.unwrap();
}

#[tokio::test]
async fn native_payload_budget_token_cancel_retains_actual_read_then_performs_no_write() {
    let (f, probe) = payload_fixture().await;
    let (native, local) = begin_payload_capture(&f).await;
    let parent = scratch();
    let baseline = f.budget.state().used;
    let before = f.backend.rows.lock().await.clone();
    let cancel = CancellationToken::new();
    probe.hold.store(true, Ordering::SeqCst);
    let _release = ReleasePayloadRead(probe.clone());
    let capture_native = native.clone();
    let capture_parent = parent.path().to_path_buf();
    let capture_cancel = cancel.clone();
    let mut capture = tokio::spawn(async move {
        FrozenNativeArtifact::capture(
            capture_native,
            local,
            capture_parent,
            limits(),
            capture_cancel,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), probe.entered.notified())
        .await
        .unwrap();
    let file = held_payload_file(parent.path());
    assert_eq!(file.metadata().unwrap().blocks(), 0);
    cancel.cancel();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut capture)
            .await
            .is_err(),
        "cancel released capture while actual backend read was held"
    );
    assert!(f.budget.state().used[SnapshotBudgetPool::Raw as usize] >= 2 * (64 << 10));
    assert!(!probe.returned.load(Ordering::SeqCst));
    assert_eq!(
        f.store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .len(),
        1
    );
    probe.resume.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(3), capture)
        .await
        .unwrap()
        .unwrap();
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("cancelled capture succeeded"),
    };
    assert_payload_scratch_released(parent.path(), &f.budget, baseline).await;
    assert!(error.to_string().contains("cancel"), "{error}");
    assert!(probe.returned.load(Ordering::SeqCst));
    assert_eq!(
        file.metadata().unwrap().blocks(),
        0,
        "payload was written after a cancelled actual read returned"
    );
    assert_eq!(*f.backend.rows.lock().await, before);
    drop(file);
    native.validate().await.unwrap();
    drop(native);
    f.meta.shutdown_session().await.unwrap();
}

#[tokio::test]
async fn native_payload_budget_caller_abort_stops_new_write_without_cancelling_parent_token() {
    let (f, probe) = payload_fixture().await;
    let (native, local) = begin_payload_capture(&f).await;
    let parent = scratch();
    let baseline = f.budget.state().used;
    let before = f.backend.rows.lock().await.clone();
    let cancel = CancellationToken::new();
    probe.hold.store(true, Ordering::SeqCst);
    let _release = ReleasePayloadRead(probe.clone());
    let capture_native = native.clone();
    let capture_parent = parent.path().to_path_buf();
    let capture_cancel = cancel.clone();
    let capture = tokio::spawn(async move {
        FrozenNativeArtifact::capture(
            capture_native,
            local,
            capture_parent,
            limits(),
            capture_cancel,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), probe.entered.notified())
        .await
        .unwrap();
    let file = held_payload_file(parent.path());
    assert_eq!(file.metadata().unwrap().blocks(), 0);
    capture.abort();
    match capture.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("aborted caller completed"),
    }
    assert!(
        !cancel.is_cancelled(),
        "capture abort cancelled caller's shared parent token"
    );
    assert!(!probe.returned.load(Ordering::SeqCst));
    assert!(f.budget.state().used[SnapshotBudgetPool::Raw as usize] >= 2 * (64 << 10));
    assert_eq!(
        f.store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .len(),
        1
    );
    probe.resume.notify_one();
    assert_payload_scratch_released(parent.path(), &f.budget, baseline).await;
    assert!(probe.returned.load(Ordering::SeqCst));
    assert_eq!(
        file.metadata().unwrap().blocks(),
        0,
        "abandoned capture wrote payload after its actual read returned"
    );
    assert_eq!(*f.backend.rows.lock().await, before);
    drop(file);
    native.validate().await.unwrap();
    drop(native);
    f.meta.shutdown_session().await.unwrap();
}
