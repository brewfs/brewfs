//! Real authenticated lower operations and the real persistent pin store.

use super::*;
use crate::cadapter::client::{ObjectBackend, ObjectByteStream, ObjectClient};
use crate::cadapter::localfs::LocalFsBackend;
use crate::chunk::read_plan::{WorkspaceReadPlanProvider, execute_unified_into};
use crate::chunk::{BlockKey, BlockStore, ChunkLayout};
use crate::meta::layer::MetaLayer;
use crate::workspace::meta_layer::WorkspaceMetaLayer;
use crate::workspace::model::ViewContext;
use crate::workspace::packed::PackedReadonlyMeta;
use crate::workspace::packed_reader_lifecycle::{PackedReaderLeaseOptions, PackedReaderSession};
use futures_util::{Stream, StreamExt, stream};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize};
use std::task::{Context, Poll};

#[derive(Default)]
struct Probe {
    mode: AtomicU8,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    terminal: AtomicBool,
    destroyed_bodies: AtomicUsize,
}

struct ActualBody {
    inner: ObjectByteStream,
    probe: Arc<Probe>,
}
impl Stream for ActualBody {
    type Item = anyhow::Result<bytes::Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
impl Drop for ActualBody {
    fn drop(&mut self) {
        self.probe.destroyed_bodies.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct HeldBackend {
    inner: LocalFsBackend,
    probe: Arc<Probe>,
}
#[async_trait]
impl ObjectBackend for HeldBackend {
    async fn put_object(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object(key, bytes).await
    }
    async fn get_object(&self, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
        anyhow::bail!("readonly transport must use bounded range bodies")
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
        match self.probe.mode.swap(0, Ordering::SeqCst) {
            1 => {
                // This is the actual backend invocation, already started;
                // only its server-response control can complete it.
                self.probe.entered.notify_one();
                self.probe.resume.notified().await;
                let result = self
                    .inner
                    .get_object_range_stream(key, offset, length)
                    .await;
                self.probe.terminal.store(true, Ordering::SeqCst);
                result
            }
            2 => {
                let inner = self
                    .inner
                    .get_object_range_stream(key, offset, length)
                    .await?;
                let probe = self.probe.clone();
                let hold = stream::once(async move {
                    probe.entered.notify_one();
                    probe.resume.notified().await;
                    Ok(bytes::Bytes::new())
                });
                Ok(Box::pin(ActualBody {
                    inner: Box::pin(hold.chain(inner)),
                    probe: self.probe.clone(),
                }))
            }
            _ => {
                self.inner
                    .get_object_range_stream(key, offset, length)
                    .await
            }
        }
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
        anyhow::bail!("readonly fixture never deletes objects")
    }
}

struct NoUpperData;
#[async_trait]
impl BlockStore for NoUpperData {
    async fn write_fresh_range(&self, _: BlockKey, _: u64, _: &[u8]) -> anyhow::Result<u64> {
        anyhow::bail!("fixture only reads its genuine lower")
    }
    async fn read_range(&self, _: BlockKey, _: u64, _: &mut [u8]) -> anyhow::Result<()> {
        anyhow::bail!("unexpected upper fetch")
    }
    async fn delete_range(&self, _: BlockKey, _: u64) -> anyhow::Result<()> {
        anyhow::bail!("fixture never deletes upper data")
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    meta: Arc<WorkspaceMetaLayer<KvWorkspaceStore<PinMemoryBackend>>>,
    lower: Arc<PackedReadonlyMeta<HeldBackend>>,
    store: Arc<KvWorkspaceStore<PinMemoryBackend>>,
    backend: PinMemoryBackend,
    budget: Arc<SnapshotMountBudget>,
    reader: Arc<dyn PackedReaderSession>,
    probe: Arc<Probe>,
}

async fn fixture() -> Fixture {
    let (directory, _client, snapshot, proof, _) = packed().await;
    let backend = PinMemoryBackend::default();
    let budget = SnapshotMountBudget::defaults();
    let store = Arc::new(
        KvWorkspaceStore::new(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    let request = request(store.as_ref(), proof).await;
    let record = store
        .install_packed_lower_binding(request.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: record.head_epoch,
        ..request.guard
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
    let probe = Arc::new(Probe::default());
    let lower = Arc::new(
        PackedReadonlyMeta::from_snapshot_budget(
            ObjectClient::new(HeldBackend {
                inner: LocalFsBackend::new(directory.path().join("objects")),
                probe: probe.clone(),
            }),
            snapshot,
            4096,
            0,
            budget.clone(),
        )
        .unwrap(),
    );
    let view = ViewContext {
        workspace_id: guard.workspace_id,
        head_layer_id: guard.expected_head_layer_id,
        head_epoch: guard.expected_head_epoch,
        lease_id: guard.lease_id,
        holder_generation: guard.holder_generation,
    };
    let authority = Arc::new(
        crate::workspace::meta_layer::PinnedCatalogPackedBindingAuthority {
            store: store.clone(),
            reader: reader.clone(),
        },
    );
    let meta = Arc::new(
        WorkspaceMetaLayer::with_chunk_size(store.clone(), view, 4096)
            .with_packed_lower(
                record.binding,
                lower.clone(),
                authority,
                Arc::new(NoUpperData),
                ChunkLayout {
                    chunk_size: 4096,
                    block_size: 4096,
                },
            )
            .unwrap(),
    );
    meta.initialize().await.unwrap();
    Fixture {
        _directory: directory,
        meta,
        lower,
        store,
        backend,
        budget,
        reader,
        probe,
    }
}

async fn wait_entered(probe: &Probe) {
    tokio::time::timeout(Duration::from_secs(2), probe.entered.notified())
        .await
        .unwrap();
}

async fn assert_waiting_pin(
    f: &Fixture,
    close: &mut tokio::task::JoinHandle<Result<(), crate::meta::store::MetaError>>,
) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match f.reader.retain_request() {
                Err(WorkspaceError::Fenced) => break,
                Ok(owner) => drop(owner),
                Err(error) => panic!("unexpected shutdown admission result: {error}"),
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut *close)
            .await
            .is_err(),
        "actual backend still held but shutdown returned"
    );
    assert!(
        !f.budget.state().closed,
        "shared budget closed before pin cleanup admission"
    );
    assert!(!f.probe.terminal.load(Ordering::SeqCst));
    assert_eq!(
        f.store
            .packed_reader_pin_roots()
            .await
            .unwrap()
            .bindings
            .len(),
        1,
        "persistent lower pin disappeared before transport drain"
    );
}

async fn assert_released(f: &Fixture) {
    assert!(f.budget.state().closed);
    let rows = f.backend.rows.lock().await;
    let pin = PackedReaderPin::decode(rows.get(&pin_key(0)).unwrap()).unwrap();
    assert_eq!(pin.state, PackedReaderPinState::Released);
    assert!(f.lower.stat_fresh(400).await.is_err());
}

#[tokio::test]
async fn packed_workspace_shutdown_waits_cancelled_metadata_response_and_retry_after_cancelled_waiter()
 {
    let f = fixture().await;
    f.probe.mode.store(1, Ordering::SeqCst);
    let meta = f.meta.clone();
    let read = tokio::spawn(async move { meta.stat_fresh(400).await });
    wait_entered(&f.probe).await;
    read.abort();
    assert!(read.await.unwrap_err().is_cancelled());
    let meta = f.meta.clone();
    let mut first = tokio::spawn(async move { meta.shutdown_session().await });
    assert_waiting_pin(&f, &mut first).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let meta = f.meta.clone();
    let mut second = tokio::spawn(async move { meta.shutdown_session().await });
    assert_waiting_pin(&f, &mut second).await;
    f.probe.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(f.probe.terminal.load(Ordering::SeqCst));
    assert_released(&f).await;
}

#[tokio::test]
async fn packed_workspace_shutdown_waits_cancelled_actual_frame_get_before_pin_release() {
    let f = fixture().await;
    let prepared = f
        .meta
        .prepare_unified_read(400, 0, 0, 4096)
        .await
        .unwrap()
        .unwrap();
    f.probe.mode.store(1, Ordering::SeqCst);
    let read = tokio::spawn(async move {
        let mut output = vec![0; 4096];
        execute_unified_into(prepared.fetcher.as_ref(), 0, &prepared.plan, &mut output).await
    });
    wait_entered(&f.probe).await;
    read.abort();
    assert!(read.await.unwrap_err().is_cancelled());
    let meta = f.meta.clone();
    let mut close = tokio::spawn(async move { meta.shutdown_session().await });
    assert_waiting_pin(&f, &mut close).await;
    f.probe.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(2), close)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(f.probe.terminal.load(Ordering::SeqCst));
    assert_released(&f).await;
}

#[tokio::test]
async fn packed_workspace_shutdown_destroys_actual_response_body_before_pin_and_budget_retirement()
{
    let f = fixture().await;
    let prepared = f
        .meta
        .prepare_unified_read(400, 0, 0, 4096)
        .await
        .unwrap()
        .unwrap();
    f.probe.mode.store(2, Ordering::SeqCst);
    let read = tokio::spawn(async move {
        let mut output = vec![0; 4096];
        execute_unified_into(prepared.fetcher.as_ref(), 0, &prepared.plan, &mut output).await
    });
    wait_entered(&f.probe).await;
    read.abort();
    assert!(read.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), f.meta.shutdown_session())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        f.probe.destroyed_bodies.load(Ordering::SeqCst),
        1,
        "lower pin released while actual userspace body still owned"
    );
    assert_released(&f).await;
}
