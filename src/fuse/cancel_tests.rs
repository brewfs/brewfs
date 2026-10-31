//! PM10 production-adapter tests. LocalFS transport has no HTTP capability.
use super::*;
use crate::cadapter::client::{ObjectBackend, ObjectByteStream, ObjectClient};
use crate::cadapter::localfs::LocalFsBackend;
use crate::cadapter::read_observer::{Engine, Ledger, Origin, Phase, ReadClass, ReadObserver};
use crate::vfs::config::VFSConfig;
use crate::workspace_overlay::packed_v3::wire005::{
    AuthenticatedV3Snapshot, V3BudgetLimits, V3BudgetPool, V3MountBudget, V3ProducerOptions,
    V3SourceConsistency, V3SourceFileLimits, V3SourceHardlinkPolicy, V3SourceNamespaceInventory,
    V3SourceNamespaceOptions,
};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, PackedCodec, PackedV3BlockStore, PackedV3ReadonlyMeta, SizeClassTable,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use tokio::sync::Notify;

#[path = "cancel_tests/actual_outer_worker_future_layout_tests.rs"]
mod actual_outer_worker_future_layout_tests;

#[path = "cancel_tests/shared_existing_api_tests.rs"]
mod shared_existing_api_tests;

#[path = "cancel_tests/control_stats_existing_api_tests.rs"]
mod control_stats_existing_api_tests;

#[path = "cancel_tests/shutdown_read_errno_existing_api_tests.rs"]
mod shutdown_read_errno_existing_api_tests;

#[path = "cancel_tests/client_fence_roots_existing_api_tests.rs"]
mod client_fence_roots_existing_api_tests;

#[path = "cancel_tests/client_close_fence_existing_api_tests.rs"]
mod client_close_fence_existing_api_tests;

#[derive(Default)]
struct Gate {
    mode: AtomicU8,
    entered: Notify,
    dropped: AtomicUsize,
    payload_ranges: std::sync::Mutex<Vec<(String, u64, u64)>>,
    released: AtomicBool,
    resumed: Notify,
    payload_bodies: AtomicUsize,
}
struct BodyDrop(Arc<Gate>);
impl Drop for BodyDrop {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Clone)]
struct PausedBackend {
    inner: LocalFsBackend,
    gate: Arc<Gate>,
}
#[async_trait]
impl ObjectBackend for PausedBackend {
    async fn put_object(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object(key, data).await
    }
    async fn put_object_create_only(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        self.inner.put_object_create_only(key, data).await
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        data: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.inner.get_object_range(key, offset, data).await
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        let stream = self
            .inner
            .get_object_range_stream(key, offset, length)
            .await?;
        let mode = self.gate.mode.load(Ordering::SeqCst);
        if length <= 256 * 1024 || mode == 0 {
            return Ok(stream);
        }
        let gate = self.gate.clone();
        if mode == 3 {
            use futures_util::StreamExt;
            gate.payload_bodies.fetch_add(1, Ordering::SeqCst);
            gate.entered.notify_one();
            let owner = BodyDrop(gate.clone());
            return Ok(Box::pin(futures_util::stream::unfold(
                (stream, gate, owner),
                |(mut source, gate, owner)| async move {
                    loop {
                        let notified = gate.resumed.notified();
                        tokio::pin!(notified);
                        notified.as_mut().enable();
                        if gate.released.load(Ordering::SeqCst) {
                            break;
                        }
                        notified.await;
                    }
                    source
                        .next()
                        .await
                        .map(|chunk| (chunk, (source, gate, owner)))
                },
            )));
        }
        let range = (key.to_owned(), offset, length);
        Ok(Box::pin(futures_util::stream::once(async move {
            let _source = stream;
            let _drop = BodyDrop(gate.clone());
            gate.payload_ranges.lock().unwrap().push(range);
            gate.entered.notify_one();
            if mode == 1 {
                std::future::pending::<()>().await;
            }
            anyhow::bail!("controlled payload body failure")
        })))
    }
    async fn get_object_size(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size(key).await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete_object(key).await
    }
}
type TestVfs = VFS<PackedV3BlockStore<PausedBackend>, PackedV3ReadonlyMeta<PausedBackend>>;
struct Fixture {
    fs: TestVfs,
    ino: u64,
    gate: Arc<Gate>,
    observer: Arc<ReadObserver>,
    budget: Arc<V3MountBudget>,
    _source: tempfile::TempDir,
    _spool: tempfile::TempDir,
    _objects: tempfile::TempDir,
}
fn request(unique: u64) -> Request {
    Request {
        unique,
        uid: 0,
        gid: 0,
        pid: std::process::id(),
    }
}
async fn fixture() -> Fixture {
    fixture_with_budget(V3MountBudget::defaults()).await
}

#[tokio::test]
async fn readonly_preparation_concrete_future_fits_fixed_roots_reservation() {
    let f = fixture().await;
    let prepare = Filesystem::prepare_unmount(&f.fs);
    // The vendor factory also retains two thin Arcs and its state tag. Its
    // concrete future is checked again before boxing or physical mounting.
    let fs_future_bytes = std::mem::size_of_val(&prepare);
    eprintln!("actual_readonly_prepare_future_bytes={fs_future_bytes}");
    assert!(fs_future_bytes + 32 <= 512 - 304 - 32);
    prepare.await.expect("readonly preparation failed");
}
async fn fixture_with_budget(budget: Arc<V3MountBudget>) -> Fixture {
    fixture_with_source_files(budget, &[("payload", 512 * 1024, 0x75)]).await
}
fn constrained_plans_budget() -> Arc<V3MountBudget> {
    constrained_plans_budget_with_capacity(4 << 20)
}
fn constrained_plans_budget_with_capacity(plans_bytes: u64) -> Arc<V3MountBudget> {
    let limits = V3BudgetLimits {
        bytes: [
            16 << 20,
            64 << 20,
            plans_bytes,
            1 << 20,
            32 << 20,
            8 << 20,
            8 << 20,
            32 << 20,
        ],
        ..V3BudgetLimits::default()
    };
    let budget = V3MountBudget::new(limits).unwrap();
    budget.validate_frame_capability(8 << 20).unwrap();
    budget
}
async fn fixture_with_source_files(
    budget: Arc<V3MountBudget>,
    files: &[(&str, usize, u8)],
) -> Fixture {
    let source = tempfile::tempdir().unwrap();
    let spool = tempfile::tempdir().unwrap();
    let objects = tempfile::tempdir().unwrap();
    for &(name, size, byte) in files {
        std::fs::write(source.path().join(name), vec![byte; size]).unwrap();
    }
    let gate = Arc::new(Gate::default());
    let client = ObjectClient::new(PausedBackend {
        inner: LocalFsBackend::new(objects.path()),
        gate: gate.clone(),
    });
    let inventory = V3SourceNamespaceInventory::capture(
        source.path(),
        spool.path(),
        V3SourceNamespaceOptions {
            root_inode: 1,
            consistency: V3SourceConsistency::BestEffortDetected,
            hardlink_policy: V3SourceHardlinkPolicy::VisibleLinks,
            file_limits: V3SourceFileLimits::default(),
        },
    )
    .await
    .unwrap();
    let built = inventory
        .build_snapshot(
            client.clone(),
            "cancel-read".into(),
            V3ProducerOptions {
                snapshot_id: [1; 32],
                root_dir_key: [2; 32],
                root_inode: 1,
                profile: AccessProfile::RandomSmallFile,
                // Cancellation fixtures deliberately keep one 512 KiB body.
                size_classes: SizeClassTable {
                    max_random_frame_raw_bytes: 4 * 1024 * 1024,
                    ..SizeClassTable::default()
                },
                build_policy: Default::default(),
                metadata_codec: PackedCodec::Raw,
                data_codec: PackedCodec::Raw,
            },
        )
        .await
        .unwrap();
    let snapshot = AuthenticatedV3Snapshot::open(&client, &built.reference)
        .await
        .unwrap();
    let layout = crate::chunk::ChunkLayout::default();
    let observer = Arc::new(ReadObserver::with_mount_budget(&budget).unwrap());
    let client = client.with_read_observer(
        observer.clone(),
        Engine::PackedV3,
        Phase::Runtime,
        Origin::Demand,
    );
    let meta = Arc::new(
        PackedV3ReadonlyMeta::from_v3_budget(
            client,
            snapshot,
            layout.chunk_size,
            0,
            budget.clone(),
        )
        .unwrap(),
    );
    let store = Arc::new(meta.block_store(layout.block_size).unwrap());
    let fs = VFS::from_workspace_components(VFSConfig::new(layout), store, meta).unwrap();
    let ino = Filesystem::lookup(&fs, request(1), 1, OsStr::new("payload"))
        .await
        .unwrap()
        .attr
        .ino;
    Fixture {
        fs,
        ino,
        gate,
        observer,
        budget,
        _source: source,
        _spool: spool,
        _objects: objects,
    }
}
fn assert_cancelled_read(observer: &ReadObserver) {
    let snapshot = observer.snapshot();
    assert!(!snapshot.overflowed);
    assert!(snapshot.rows.values().all(|row| row.conserved()));
    let reads: Vec<_> = snapshot
        .rows
        .iter()
        .filter(|((ledger, context), _)| {
            *ledger == Ledger::LogicalOperation
                && context.class == ReadClass::LogicalRead
                && context.origin == Origin::Demand
        })
        .map(|(_, row)| row)
        .collect();
    assert!(!reads.is_empty());
    assert_eq!(
        reads.iter().map(|row| row.logical_delivered).sum::<u64>(),
        0
    );
    assert!(
        reads
            .iter()
            .map(|row| row.failed + row.cancelled)
            .sum::<u64>()
            > 0
    );
    assert_eq!(reads.iter().map(|row| row.inflight).sum::<u64>(), 0);
}

#[tokio::test]
async fn packed_prepare_admission_returns_enomem_and_next_read_recovers() {
    use crate::workspace_overlay::packed_v3::wire005::V3BudgetPool;
    let f = fixture_with_budget(constrained_plans_budget()).await;
    let opened = Filesystem::open(&f.fs, request(81), f.ino, libc::O_RDONLY as u32)
        .await
        .unwrap();
    let baseline = f.budget.state().used;
    let held = f
        .budget
        .admit(&[(V3BudgetPool::Plans, f.budget.capacity(V3BudgetPool::Plans))])
        .unwrap();
    let rejected = Filesystem::read(&f.fs, request(82), f.ino, opened.fh, 0, 8192).await;
    assert_eq!(rejected.err(), Some(Errno::from(libc::ENOMEM)));
    assert_cancelled_read(&f.observer);
    let snapshot = f.observer.snapshot();
    let admission_failures: u64 = snapshot
        .rows
        .iter()
        .filter(|((ledger, context), _)| {
            *ledger == Ledger::LogicalOperation
                && context.class == ReadClass::LogicalRead
                && context.origin == Origin::Demand
        })
        .map(|(_, row)| {
            row.failure_reasons[&crate::cadapter::read_observer::FailureClass::Admission]
        })
        .sum();
    assert_eq!(
        admission_failures, 1,
        "budget rejection was classified as backend failure"
    );
    drop(held);
    assert_eq!(
        f.budget.state().used,
        baseline,
        "failed read leaked admission owners"
    );
    let recovered = Filesystem::read(&f.fs, request(83), f.ino, opened.fh, 0, 8192)
        .await
        .unwrap();
    assert_eq!(recovered.data.as_ref(), &[0x75; 8192]);
    drop(recovered);
    f.fs.close(opened.fh).await.unwrap();
}

#[tokio::test]
async fn packed_interrupt_unknown_unique_returns_eagain() {
    let f = fixture().await;
    assert_eq!(
        Filesystem::interrupt(&f.fs, request(2), 999)
            .await
            .unwrap_err(),
        Errno::from(libc::EAGAIN)
    );
}

#[tokio::test]
async fn packed_interrupt_drops_entire_payload_future_and_preserves_real_handle() {
    let f = fixture().await;
    let opened = Filesystem::open(&f.fs, request(2), f.ino, libc::O_RDONLY as u32)
        .await
        .unwrap();
    f.gate.mode.store(1, Ordering::SeqCst);
    let fs = f.fs.clone();
    let ino = f.ino;
    let reader =
        tokio::spawn(
            async move { Filesystem::read(&fs, request(41), ino, opened.fh, 0, 8192).await },
        );
    tokio::time::timeout(Duration::from_secs(2), f.gate.entered.notified())
        .await
        .expect("real payload body was not reached");
    Filesystem::interrupt(&f.fs.clone(), request(42), 41)
        .await
        .unwrap();
    let read = tokio::time::timeout(Duration::from_secs(2), reader)
        .await
        .expect("FUSE unique did not cancel full read future")
        .unwrap();
    assert_eq!(read.err(), Some(Errno::from(libc::EINTR)));
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.fs.open_file_handle_count(),
        1,
        "interrupt closed the caller-owned fh"
    );
    assert_cancelled_read(&f.observer);
    assert_eq!(
        Filesystem::interrupt(&f.fs, request(43), 41)
            .await
            .unwrap_err(),
        Errno::from(libc::EAGAIN)
    );
    f.gate.mode.store(0, Ordering::SeqCst);
    assert_eq!(
        Filesystem::read(&f.fs, request(44), f.ino, opened.fh, 0, 8192)
            .await
            .unwrap()
            .data
            .as_ref(),
        &[0x75; 8192]
    );
    f.fs.close(opened.fh).await.unwrap();
}

#[tokio::test]
async fn packed_interrupt_closes_temporary_fh_before_cancelled_reply() {
    let f = fixture().await;
    f.gate.mode.store(1, Ordering::SeqCst);
    let fs = f.fs.clone();
    let ino = f.ino;
    let reader =
        tokio::spawn(async move { Filesystem::read(&fs, request(51), ino, 0, 0, 8192).await });
    tokio::time::timeout(Duration::from_secs(2), f.gate.entered.notified())
        .await
        .unwrap();
    assert_eq!(f.fs.open_file_handle_count(), 1);
    Filesystem::interrupt(&f.fs, request(52), 51).await.unwrap();
    let read = tokio::time::timeout(Duration::from_secs(2), reader)
        .await
        .expect("temporary-fh read was not cancelled")
        .unwrap();
    assert_eq!(read.err(), Some(Errno::from(libc::EINTR)));
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.fs.open_file_handle_count(),
        0,
        "temporary fh survived final cancelled reply"
    );
    assert_cancelled_read(&f.observer);
}

#[tokio::test]
async fn packed_failed_read_releases_temporary_fh_and_unique() {
    let f = fixture().await;
    f.gate.mode.store(2, Ordering::SeqCst);
    assert_eq!(
        Filesystem::read(&f.fs, request(61), f.ino, 0, 0, 8192)
            .await
            .err(),
        Some(Errno::from(libc::EIO))
    );
    assert_eq!(
        f.fs.open_file_handle_count(),
        0,
        "error path leaked a temporary handle"
    );
    assert_eq!(
        Filesystem::interrupt(&f.fs, request(62), 61)
            .await
            .unwrap_err(),
        Errno::from(libc::EAGAIN)
    );
}

#[tokio::test]
async fn packed_outer_read_task_drop_removes_unique_and_schedules_temporary_close() {
    let f = fixture().await;
    f.gate.mode.store(1, Ordering::SeqCst);
    let fs = f.fs.clone();
    let ino = f.ino;
    let reader =
        tokio::spawn(async move { Filesystem::read(&fs, request(71), ino, 0, 0, 8192).await });
    tokio::time::timeout(Duration::from_secs(2), f.gate.entered.notified())
        .await
        .unwrap();
    // This drops a userspace VFS future; no io_uring/kernel memory is involved.
    reader.abort();
    assert!(reader.await.unwrap_err().is_cancelled());
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        Filesystem::interrupt(&f.fs, request(72), 71)
            .await
            .unwrap_err(),
        Errno::from(libc::EAGAIN)
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.fs.open_file_handle_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("FileGuard drop failed to finish owned temporary close");
    assert_cancelled_read(&f.observer);
}

#[tokio::test]
async fn packed_destroy_waits_for_pending_unique_owners_and_temporary_handles() {
    let f = fixture_with_source_files(
        V3MountBudget::defaults(),
        &[
            ("payload", 512 * 1024, 0x75),
            ("payload-large", 2 * 1024 * 1024, 0x76),
        ],
    )
    .await;
    let first = Filesystem::lookup(&f.fs, request(78), 1, OsStr::new("payload"))
        .await
        .unwrap();
    let second = Filesystem::lookup(&f.fs, request(79), 1, OsStr::new("payload-large"))
        .await
        .unwrap();
    assert_eq!(first.attr.ino, f.ino);
    assert_ne!(first.attr.ino, second.attr.ino);
    assert_eq!(first.attr.size, 512 * 1024);
    assert_eq!(second.attr.size, 2 * 1024 * 1024);
    let layout = |size| {
        crate::workspace_overlay::packed_v3::choose_frame_layout(
            size,
            None,
            AccessProfile::RandomSmallFile,
            SizeClassTable::default(),
        )
        .unwrap()
    };
    assert_ne!(
        layout(first.attr.size).size_class,
        layout(second.attr.size).size_class
    );
    f.gate.mode.store(1, Ordering::SeqCst);
    let mut readers = Vec::new();
    // Start the second distinct frame only after the first body is pending,
    // so a coalescing window cannot turn these reads into one payload body.
    for (unique, ino) in [(81, first.attr.ino), (82, second.attr.ino)] {
        let fs = f.fs.clone();
        readers.push(tokio::spawn(async move {
            Filesystem::read(&fs, request(unique), ino, 0, 0, 8192).await
        }));
        tokio::time::timeout(Duration::from_secs(2), f.gate.entered.notified())
            .await
            .expect("production packed payload was not pending");
    }
    {
        let ranges = f.gate.payload_ranges.lock().unwrap();
        assert_eq!(ranges.len(), 2);
        assert_ne!(
            ranges[0], ranges[1],
            "reads shared one physical payload range"
        );
    }
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 0);
    assert_eq!(f.fs.open_file_handle_count(), 2);
    tokio::time::timeout(
        Duration::from_secs(2),
        Filesystem::destroy(&f.fs, request(83)),
    )
    .await
    .expect("destroy waited forever for readonly userspace reads");
    // Observe real destroy completion before checking owners and handles.
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 2);
    assert_eq!(f.fs.open_file_handle_count(), 0);
    for reader in readers {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), reader)
                .await
                .unwrap()
                .unwrap()
                .err(),
            Some(Errno::from(libc::EIO))
        );
    }
    for unique in [81, 82] {
        assert_eq!(
            Filesystem::interrupt(&f.fs, request(84), unique)
                .await
                .unwrap_err(),
            Errno::from(libc::EAGAIN)
        );
    }
    for (unique, ino) in [(85, first.attr.ino), (86, second.attr.ino)] {
        assert_eq!(
            Filesystem::read(&f.fs, request(unique), ino, 0, 0, 8192)
                .await
                .err(),
            Some(Errno::from(libc::EIO))
        );
    }
    assert_eq!(f.budget.state().used[V3BudgetPool::Control as usize], 0);
    assert_cancelled_read(&f.observer);
}

#[tokio::test]
async fn g06_existing_api_destroy_retires_actual_pending_read_and_temporary_handle() {
    let f = fixture().await;
    f.gate.mode.store(1, Ordering::SeqCst);
    let fs = f.fs.clone();
    let ino = f.ino;
    let reader =
        tokio::spawn(async move { Filesystem::read(&fs, request(811), ino, 0, 0, 8192).await });
    tokio::time::timeout(Duration::from_secs(2), f.gate.entered.notified())
        .await
        .expect("real payload body did not become pending");
    assert_eq!(f.gate.payload_ranges.lock().unwrap().len(), 1);
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 0);
    assert_eq!(f.fs.open_file_handle_count(), 1);
    tokio::time::timeout(
        Duration::from_secs(2),
        Filesystem::destroy(&f.fs, request(812)),
    )
    .await
    .expect("destroy did not finish");
    assert_eq!(f.gate.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(f.fs.open_file_handle_count(), 0);
    assert_eq!(reader.await.unwrap().err(), Some(Errno::from(libc::EIO)));
    assert_eq!(
        Filesystem::interrupt(&f.fs, request(813), 811)
            .await
            .unwrap_err(),
        Errno::from(libc::EAGAIN)
    );
    assert_eq!(
        Filesystem::read(&f.fs, request(814), ino, 0, 0, 8192)
            .await
            .err(),
        Some(Errno::from(libc::EIO))
    );
    assert_eq!(f.budget.state().used[V3BudgetPool::Control as usize], 0);
    assert_cancelled_read(&f.observer);
}

#[tokio::test]
async fn packed_minimum_control_admits_handle_real_request_charge_and_read() {
    let mut limits = V3BudgetLimits::default();
    limits.bytes[V3BudgetPool::Control as usize] = 32768;
    let f = fixture_with_budget(V3MountBudget::new(limits).unwrap()).await;
    let opened = Filesystem::open(&f.fs, request(91), f.ino, libc::O_RDONLY as u32)
        .await
        .unwrap();
    let request_owner = Filesystem::reserve_request_memory(&f.fs, 40)
        .unwrap()
        .unwrap();
    let reply = Filesystem::read(&f.fs, request(92), f.ino, opened.fh, 0, 8192)
        .await
        .expect("minimum admitted profile cannot serve its first valid read");
    assert_eq!(reply.data.as_ref(), &[0x75; 8192]);
    assert!(f.budget.state().peak[V3BudgetPool::Control as usize] <= 32768);
    drop(reply);
    drop(request_owner);
    f.fs.close(opened.fh).await.unwrap();
    assert_eq!(f.budget.state().used[V3BudgetPool::Control as usize], 0);
}

#[path = "cancel_tests/minimum_roots_inline_hooks_existing_api_tests.rs"]
mod minimum_roots_inline_hooks_existing_api_tests;
